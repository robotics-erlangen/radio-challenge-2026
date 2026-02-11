use crate::conn_stats::{ConnectionStatTracker, ConnectionStats};
use crate::packet::datagrams::{
    CommandDatagram, CommandDatagramType, ResponseDatagram, ResponseDatagramType,
};
use crate::packet::deku_helpers::FixedSizePacking;
use crate::packet::{
    DATAGRAM_CHUNK_SIZE, RadioCommandPacket, RadioCommandPayload, RadioPacketPayload,
    RadioResponsePacket, RegularCommandData, RegularResponseData,
};
use deku::DekuContainerRead;
use std::collections::VecDeque;

pub(crate) struct ConnectionCore {
    // General
    counter: u8,
    stat_tracker: ConnectionStatTracker,

    // Datagrams
    datagram_queue: VecDeque<CommandDatagram>,
    datagram_chunk_queue: VecDeque<(CommandDatagramType, [u8; DATAGRAM_CHUNK_SIZE])>,
    sent_datagram_packet: Option<RadioCommandPayload>,
    cycles_since_datagram_send: u8,
    seqnum_last_sent: u8,
    seqnum_last_received: u8,

    datagram_rx_type: Option<ResponseDatagramType>,
    datagram_rx_buf: Vec<u8>,
}

pub(crate) enum PacketRxResult {
    Regular(RegularResponseData),
    Datagram(ResponseDatagram),
    IncompleteDatagram,
}

impl ConnectionCore {
    pub(crate) fn new() -> Self {
        Self {
            counter: 0,
            stat_tracker: ConnectionStatTracker::new(100),
            datagram_queue: VecDeque::new(),
            datagram_chunk_queue: VecDeque::new(),
            sent_datagram_packet: None,
            cycles_since_datagram_send: 0,
            seqnum_last_sent: 1,
            seqnum_last_received: 1,
            datagram_rx_type: None,
            datagram_rx_buf: Vec::new(),
        }
    }

    pub(crate) fn stats(&self) -> ConnectionStats {
        self.stat_tracker.get()
    }

    /// This method has to be called for every received packet to update datagram seqnums
    pub(crate) fn packet_received(&mut self, bytes: &[u8]) -> PacketRxResult {
        let packet = RadioResponsePacket::from_bytes((bytes, 0)).unwrap().1;

        // Update seqnum_last_received
        if let RadioPacketPayload::Datagram { seqnum, .. } = &packet.payload {
            self.seqnum_last_received = *seqnum;
        }

        // Update the stat tracker
        self.stat_tracker.received();

        // Handle ack
        if let Some(RadioPacketPayload::Datagram { seqnum, .. }) =
            self.sent_datagram_packet.as_ref()
        {
            let seqnum = *seqnum;
            if seqnum == packet.acknum {
                self.sent_datagram_packet = None;
            }
        }

        match packet.payload {
            RadioPacketPayload::Regular(data) => PacketRxResult::Regular(data),
            RadioPacketPayload::Datagram {
                datagram_type,
                data,
                ..
            } => {
                // Clear buffer if replacing different uncompleted datagram
                if let Some(rx_type) = self.datagram_rx_type.as_ref()
                    && *rx_type != datagram_type
                {
                    self.datagram_rx_buf.clear();
                }

                // Insert new chunk
                self.datagram_rx_type = Some(datagram_type);
                self.datagram_rx_buf.extend_from_slice(&data);

                if self.datagram_rx_buf.len() >= datagram_type.get_datagram_size() {
                    // Full datagram received
                    let full_datagram = (datagram_type, self.datagram_rx_buf.as_slice())
                        .try_into()
                        .unwrap();
                    self.datagram_rx_type = None;
                    self.datagram_rx_buf.clear();
                    PacketRxResult::Datagram(full_datagram)
                } else {
                    PacketRxResult::IncompleteDatagram
                }
            }
        }
    }

    pub(crate) fn queue_datagram(&mut self, datagram: CommandDatagram) {
        self.datagram_queue.push_back(datagram);
    }

    /// Prepares a packet for sending. If a datagram packet is available, it will be used instead.
    pub(crate) fn next_packet(&mut self, regular_data: RegularCommandData) -> Vec<u8> {
        let payload = if let Some(sent_datagram_packet) = &self.sent_datagram_packet
            && self.cycles_since_datagram_send >= 1
        {
            // Resend the last datagram packet if an ack was not received within resend_cooldown_cycles
            self.cycles_since_datagram_send = 0;
            sent_datagram_packet.clone()
        } else if self.sent_datagram_packet.is_none()
            && let Some(next_chunk) = self.get_next_datagram_chunk()
        {
            // Construct new datagram packet
            self.seqnum_last_sent = 1 - self.seqnum_last_sent;
            let payload = RadioCommandPayload::Datagram {
                seqnum: self.seqnum_last_sent,
                datagram_type: next_chunk.0,
                data: next_chunk.1,
            };

            self.sent_datagram_packet = Some(payload.clone());
            self.cycles_since_datagram_send = 0;
            payload
        } else {
            // Send regular packet
            self.cycles_since_datagram_send = self.cycles_since_datagram_send.saturating_add(1);
            RadioCommandPayload::Regular(regular_data)
        };

        self.counter = (self.counter + 1) % (2u8.pow(6));
        self.stat_tracker.sent();
        RadioCommandPacket {
            counter: self.counter,
            acknum: self.seqnum_last_received,
            payload,
        }
        .pack_padded()
    }

    fn get_next_datagram_chunk(
        &mut self,
    ) -> Option<(CommandDatagramType, [u8; DATAGRAM_CHUNK_SIZE])> {
        if let Some(next_chunk) = self.datagram_chunk_queue.pop_front() {
            // Next chunk available
            Some(next_chunk)
        } else if let Some(next_datagram) = self.datagram_queue.pop_front() {
            // No next chunk, but another queued datagram -> ingest next datagram
            let (datagram_type, data) = next_datagram.into();

            // Still submit an empty chunk when there is no payload
            if data.is_empty() {
                self.datagram_chunk_queue
                    .push_back((datagram_type, [0u8; DATAGRAM_CHUNK_SIZE]))
            } else {
                self.datagram_chunk_queue
                    .extend(data.chunks(DATAGRAM_CHUNK_SIZE).map(|chunk| {
                        let mut datagram_chunk = [0u8; DATAGRAM_CHUNK_SIZE];
                        datagram_chunk[..chunk.len()].copy_from_slice(chunk);
                        (datagram_type, datagram_chunk)
                    }));
            }

            self.datagram_chunk_queue.pop_front()
        } else {
            // No chunks or datagrams left
            None
        }
    }
}
