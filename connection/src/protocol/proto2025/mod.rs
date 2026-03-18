use crate::conn_stats::{ConnectionStatTracker, ConnectionStats};
use crate::protocol::deku_helpers::{PacketPacking, PacketUnpacking};
use crate::protocol::proto2025::packet::*;
use crate::protocol::{PacketRxResult, RadioProtocol};
use datagrams::{CommandDatagram, CommandDatagramType, ResponseDatagram, ResponseDatagramType};
use std::collections::VecDeque;

pub mod datagrams;
pub mod packet;

pub struct RadioProtocol2025 {
    // General
    counter: u8,
    stat_tracker: ConnectionStatTracker,

    // Datagrams
    datagram_queue: VecDeque<CommandDatagram>,
    datagram_chunk_queue: VecDeque<(CommandDatagramType, [u8; DATAGRAM_CHUNK_SIZE])>,
    sent_datagram_packet: Option<DatagramPayload<CommandDatagramType>>,
    cycles_since_datagram_send: u8,
    seqnum_last_sent: u8,
    seqnum_last_received: u8,

    datagram_rx_type: Option<ResponseDatagramType>,
    datagram_rx_buf: Vec<u8>,
}

impl RadioProtocol2025 {
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

impl Default for RadioProtocol2025 {
    fn default() -> Self {
        Self::new()
    }
}

impl<RC: PacketPacking<PAYLOAD_SIZE>, RR: PacketUnpacking<PAYLOAD_SIZE>>
    RadioProtocol<RC, RR, CommandDatagram, ResponseDatagram> for RadioProtocol2025
{
    fn stats(&self) -> ConnectionStats {
        self.stat_tracker.get()
    }

    /// Unpacks a packet and updates internal connection state
    fn packet_received(&mut self, bytes: &[u8]) -> PacketRxResult<RR, ResponseDatagram> {
        let header = PacketHeader::unpack(bytes).unwrap();

        // Update the stat tracker
        self.stat_tracker.received();

        // Handle ack
        if let Some(sent_datagram_packet) = &self.sent_datagram_packet
            && sent_datagram_packet.seqnum == header.acknum
        {
            self.sent_datagram_packet = None;
        }

        if header.datagram {
            let datagram_payload =
                DatagramPayload::<ResponseDatagramType>::unpack(&bytes[1..]).unwrap();
            let datagram_type = datagram_payload.datagram_type;
            let data_chunk = datagram_payload.data;

            // Update seqnum_last_received
            self.seqnum_last_received = datagram_payload.seqnum;

            // Clear buffer if replacing different uncompleted datagram
            if let Some(rx_type) = self.datagram_rx_type.as_ref()
                && *rx_type != datagram_type
            {
                self.datagram_rx_buf.clear();
            }

            // Insert new chunk
            self.datagram_rx_type = Some(datagram_type);
            self.datagram_rx_buf.extend_from_slice(&data_chunk);

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
        } else {
            let regular_payload = RR::unpack(&bytes[1..]).unwrap();
            PacketRxResult::Regular(regular_payload)
        }
    }

    fn queue_datagram(&mut self, datagram: CommandDatagram) {
        self.datagram_queue.push_back(datagram);
    }

    /// Prepares a packet for sending. If a datagram packet is available, it will be used instead.
    fn next_packet(&mut self, regular_data: RC) -> Vec<u8> {
        let mut packet = vec![0; PACKET_SIZE];

        self.counter = (self.counter + 1) % (2u8.pow(6));
        let mut header = PacketHeader {
            counter: self.counter,
            acknum: self.seqnum_last_received,
            datagram: false, // Might be overwritten later
        };

        if let Some(sent_datagram_packet) = &self.sent_datagram_packet
            && self.cycles_since_datagram_send >= 1
        {
            // Resend the last datagram packet if an ack was not received within resend_cooldown_cycles
            self.cycles_since_datagram_send = 0;

            header.datagram = true;
            header.pack_to_slice(&mut packet[..1]);
            sent_datagram_packet.pack_to_slice(&mut packet[1..]);
        } else if self.sent_datagram_packet.is_none()
            && let Some(next_chunk) = self.get_next_datagram_chunk()
        {
            // Construct new datagram packet
            self.seqnum_last_sent = 1 - self.seqnum_last_sent;
            let payload = DatagramPayload::<CommandDatagramType> {
                seqnum: self.seqnum_last_sent,
                datagram_type: next_chunk.0,
                data: next_chunk.1,
            };

            self.sent_datagram_packet = Some(payload.clone());
            self.cycles_since_datagram_send = 0;

            header.datagram = true;
            header.pack_to_slice(&mut packet[..1]);
            payload.pack_to_slice(&mut packet[1..]);
        } else {
            // Send regular packet
            self.cycles_since_datagram_send = self.cycles_since_datagram_send.saturating_add(1);

            header.pack_to_slice(&mut packet[..1]);
            regular_data.pack_to_slice(&mut packet[1..]);
        };

        self.stat_tracker.sent();
        packet
    }
}
