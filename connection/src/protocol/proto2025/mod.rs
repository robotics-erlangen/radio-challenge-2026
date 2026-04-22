use crate::conn_stats::{ConnectionStatTracker, ConnectionStats};
use crate::protocol::deku_helpers::{PacketPacking, PacketUnpacking};
use crate::protocol::proto2025::packet::*;
use crate::protocol::{PacketRxResult, RadioProtocol};
use datagrams::{CommandDatagram, CommandDatagramType, ResponseDatagram, ResponseDatagramType};
use std::collections::VecDeque;
use std::time::Instant;

pub mod datagrams;
pub mod packet;

/// Marker trait for all valid proto2025 input/output types. Used as a more accurate restriction on top of the packed size.
trait RadioProtocol2025Payload {}
impl RadioProtocol2025Payload for RegularCommandPayload {}
impl RadioProtocol2025Payload for RegularResponsePayload {}
impl RadioProtocol2025Payload for [u8; PAYLOAD_SIZE] {}
impl RadioProtocol2025Payload for Box<[u8; PAYLOAD_SIZE]> {}

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
            stat_tracker: ConnectionStatTracker::new(100, 5),
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

impl<
    RC: PacketPacking<PAYLOAD_SIZE> + RadioProtocol2025Payload,
    RR: PacketUnpacking<PAYLOAD_SIZE> + RadioProtocol2025Payload,
> RadioProtocol<RC, RR, CommandDatagram, ResponseDatagram> for RadioProtocol2025
{
    const RESPONSE_PACKET_SIZE: usize = PAYLOAD_SIZE + 1; // +1 for header

    fn stats(&self) -> ConnectionStats {
        self.stat_tracker.get()
    }

    /// Unpacks a packet and updates internal connection state
    fn packet_received(
        &mut self,
        bytes: &[u8],
        timestamp: Instant,
    ) -> PacketRxResult<RR, ResponseDatagram> {
        let header = PacketHeader::unpack(bytes).unwrap();

        // Update the stat tracker
        self.stat_tracker.received(header.counter as u32, timestamp);

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

        let mut header = PacketHeader {
            counter: self.counter,
            acknum: self.seqnum_last_received,
            datagram: false, // Might be overwritten later
        };
        self.counter = (self.counter + 1) % (2u8.pow(6));

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

        self.stat_tracker
            .sent(header.counter as u32, Instant::now());
        packet
    }
}

#[cfg(test)]
mod tests {
    use crate::protocol::deku_helpers::{PacketPacking, PacketUnpacking};
    use crate::protocol::proto2025::RadioProtocol2025;
    use crate::protocol::proto2025::datagrams::{
        CommandDatagram, CommandDatagramType, EchoDatagram, ResponseDatagram, ResponseDatagramType,
    };
    use crate::protocol::proto2025::packet::{
        DatagramPayload, PACKET_SIZE, PAYLOAD_SIZE, PacketHeader,
    };
    use crate::protocol::{PacketRxResult, RadioProtocol};
    use std::time::Instant;

    // Type aliases to reduce the bloat of the fully qualified call syntax.
    // The called functions never use all generics at once, so rust can't choose
    // the right trait implementation without it.
    type R = [u8; PAYLOAD_SIZE];
    type DC = CommandDatagram;
    type DR = ResponseDatagram;

    #[test]
    fn simple_send() {
        let mut proto = RadioProtocol2025::new();
        let test_payload: [u8; PAYLOAD_SIZE] = std::array::from_fn(|i| i as u8);

        let send_buf = RadioProtocol::<R, R, DC, DR>::next_packet(&mut proto, test_payload);

        let header = PacketHeader::unpack(&send_buf[..1]).unwrap();

        assert_eq!(send_buf.len(), PACKET_SIZE);
        assert_eq!(send_buf[1..], test_payload);
        assert_eq!(
            header,
            PacketHeader {
                counter: 0,
                acknum: 1,
                datagram: false,
            }
        );
    }

    /// Full send- and receive cycle of a large multi-packet datagram
    #[test]
    fn datagram_echo() {
        let mut proto = RadioProtocol2025::new();

        // Build the test packets
        let test_payload: [u8; PAYLOAD_SIZE] = std::array::from_fn(|i| i as u8);
        let test_datagram = EchoDatagram {
            data: std::array::from_fn(|i| i as u8),
        };

        RadioProtocol::<R, R, DC, DR>::queue_datagram(
            &mut proto,
            CommandDatagram::Echo(test_datagram),
        );

        // ======== Send the echo datagram packet ========

        let mut commands_without_datagram = 0;
        let mut total_commands = 0;
        let mut expected_next_byte = 0u8;
        let mut seqnum_last_recv = 0u8;
        // Continue sending until there are no more queued chunks
        while commands_without_datagram < 10 && total_commands < 500 {
            let send_buf = RadioProtocol::<R, R, DC, DR>::next_packet(&mut proto, test_payload);
            total_commands += 1;

            let header = PacketHeader::unpack(&send_buf[..1]).unwrap();

            if header.datagram {
                commands_without_datagram = 0;

                // Validate the command datagram packet
                assert_eq!(
                    send_buf.len(),
                    PACKET_SIZE,
                    "Sent a command datagram packet with the wrong size"
                );
                let datagram_payload =
                    DatagramPayload::<CommandDatagramType>::unpack(&send_buf[1..]).unwrap();
                let payload_data = datagram_payload.data;
                assert_eq!(
                    datagram_payload.datagram_type,
                    CommandDatagramType::Echo,
                    "Sent wrong datagram type"
                );
                for byte in payload_data.iter() {
                    if expected_next_byte < 100 {
                        assert_eq!(
                            *byte, expected_next_byte,
                            "Sent wrong datagram chunk. Could also be a resend of a previous chunk."
                        );
                    }
                    expected_next_byte += 1;
                }

                // Simulate receiving ack
                seqnum_last_recv = datagram_payload.seqnum;
                let mut ack_packet = [0u8; PACKET_SIZE];
                let ack_header = PacketHeader {
                    counter: header.counter,
                    acknum: seqnum_last_recv,
                    datagram: false,
                };
                ack_header.pack_to_slice(&mut ack_packet); // The rest is filled with 0

                assert!(matches!(
                    RadioProtocol::<R, R, DC, DR>::packet_received(
                        &mut proto,
                        &ack_packet,
                        Instant::now()
                    ),
                    PacketRxResult::Regular(_)
                ));
            } else {
                commands_without_datagram += 1;
            }
        }

        assert!(total_commands < 500, "Infinitely sending datagram chunks");
        assert!(
            expected_next_byte >= 100,
            "Didn't finish sending the echo packet"
        );

        // ======== Send the response datagram ========

        let mut received_datagram = None;
        let mut response_sends = 0;
        let mut next_response_base_byte = 0;
        let mut resp_seqnum = 0;
        while response_sends < 500 && received_datagram.is_none() {
            // Simulate a command packet to respond to
            let send_buf = RadioProtocol::<R, R, DC, DR>::next_packet(&mut proto, test_payload);
            response_sends += 1;

            // Validate the command packet
            let header = PacketHeader::unpack(&send_buf[..1]).unwrap();
            assert_eq!(
                send_buf.len(),
                PACKET_SIZE,
                "Sent a regular packet with the wrong size"
            );
            assert!(
                !header.datagram,
                "Sent datagram chunk when expecting regular packet"
            );
            assert_eq!(send_buf[1..], test_payload, "Sent wrong regular packet");

            let mut resp_packet = [0u8; PACKET_SIZE];
            let resp_header = PacketHeader {
                counter: header.counter,
                acknum: seqnum_last_recv, // Keep sending the last acknum
                datagram: true,
            };
            let resp_payload = DatagramPayload::<ResponseDatagramType> {
                seqnum: resp_seqnum, // Irrelevant for this test
                datagram_type: ResponseDatagramType::Echo,
                data: std::array::from_fn(|i| next_response_base_byte + i as u8),
            };
            next_response_base_byte += resp_payload.data.len() as u8;
            resp_header.pack_to_slice(&mut resp_packet[0..1]);
            resp_payload.pack_to_slice(&mut resp_packet[1..]);
            resp_seqnum = 1 - resp_seqnum;

            let response: PacketRxResult<[u8; PAYLOAD_SIZE], ResponseDatagram> =
                RadioProtocol::<R, R, DC, DR>::packet_received(
                    &mut proto,
                    &resp_packet,
                    Instant::now(),
                );
            assert!(
                match response {
                    PacketRxResult::IncompleteDatagram => {
                        true
                    }
                    PacketRxResult::Datagram(ResponseDatagram::Echo(d)) => {
                        received_datagram = Some(d);
                        true
                    }
                    _ => false,
                },
                "Processed a response datagram chunk incorrectly"
            )
        }

        assert!(
            received_datagram.is_some(),
            "Didn't receive the full echo response"
        );
        assert_eq!(
            received_datagram.unwrap().data,
            std::array::from_fn(|i| i as u8),
            "Received wrong echo data"
        );
    }

    /// 0-sized datagrams like ReadRobotId should be handled correctly by sending a dummy chunk.
    #[test]
    fn empty_datagram() {
        let mut proto = RadioProtocol2025::new();
        let test_payload: [u8; PAYLOAD_SIZE] = std::array::from_fn(|i| i as u8);
        let test_datagram = CommandDatagram::ReadRobotId;

        RadioProtocol::<R, R, DC, DR>::queue_datagram(&mut proto, test_datagram);

        let send_buf = RadioProtocol::<R, R, DC, DR>::next_packet(&mut proto, test_payload);
        let header = PacketHeader::unpack(&send_buf[..1]).unwrap();

        assert!(header.datagram, "Expected a datagram packet");

        let datagram_payload =
            DatagramPayload::<CommandDatagramType>::unpack(&send_buf[1..]).unwrap();
        assert_eq!(
            datagram_payload.datagram_type,
            CommandDatagramType::ReadRobotId
        );
        assert_eq!(
            datagram_payload.data,
            [0u8; crate::protocol::proto2025::packet::DATAGRAM_CHUNK_SIZE],
            "Expected an empty chunk. Still works, but there is probably a bug somewhere."
        );
    }

    /// Datagrams must be retransmitted if no ack is received in time
    #[test]
    fn datagram_resend() {
        let mut proto = RadioProtocol2025::new();
        let test_payload: [u8; PAYLOAD_SIZE] = std::array::from_fn(|i| i as u8);

        RadioProtocol::<R, R, DC, DR>::queue_datagram(&mut proto, CommandDatagram::ReadRobotId);

        // Expect initial send within 10 cycles
        let mut sent_initial = false;
        for _ in 0..10 {
            let send_buf = RadioProtocol::<R, R, DC, DR>::next_packet(&mut proto, test_payload);
            let header = PacketHeader::unpack(&send_buf[..1]).unwrap();
            if header.datagram {
                let init_payload =
                    DatagramPayload::<CommandDatagramType>::unpack(&send_buf[1..]).unwrap();
                assert_eq!(init_payload.datagram_type, CommandDatagramType::ReadRobotId);
                sent_initial = true;
                break;
            }
        }
        assert!(sent_initial, "Didn't send the initial datagram");

        // Expect resend within 10 cycles
        let mut got_resend = false;
        for _ in 0..10 {
            let send_buf = RadioProtocol::<R, R, DC, DR>::next_packet(&mut proto, test_payload);
            let header = PacketHeader::unpack(&send_buf[..1]).unwrap();
            if header.datagram {
                let payload =
                    DatagramPayload::<CommandDatagramType>::unpack(&send_buf[1..]).unwrap();
                assert_eq!(
                    payload.datagram_type,
                    CommandDatagramType::ReadRobotId,
                    "Resend should have the same datagram type"
                );
                got_resend = true;
            }
        }

        assert!(got_resend, "Didn't resend a \"lost\" datagram");
    }

    /// Multiple datagrams should be queueable at once. Depends on empty_datagrams
    #[test]
    fn datagram_queue() {
        let mut proto = RadioProtocol2025::new();
        let test_payload: [u8; PAYLOAD_SIZE] = std::array::from_fn(|i| i as u8);
        let test_datagram1 = CommandDatagram::ReadRobotId;
        let test_datagram2 = CommandDatagram::ReadBoardIds;

        RadioProtocol::<R, R, DC, DR>::queue_datagram(&mut proto, test_datagram1);
        RadioProtocol::<R, R, DC, DR>::queue_datagram(&mut proto, test_datagram2);

        assert_eq!(proto.datagram_queue.len(), 2);

        let mut seen_datagram_types = Vec::new();
        for _ in 0..1000 {
            let send_buf = RadioProtocol::<R, R, DC, DR>::next_packet(&mut proto, test_payload);
            let header = PacketHeader::unpack(&send_buf[..1]).unwrap();

            if header.datagram {
                let datagram_payload =
                    DatagramPayload::<CommandDatagramType>::unpack(&send_buf[1..]).unwrap();
                if !seen_datagram_types.contains(&datagram_payload.datagram_type) {
                    seen_datagram_types.push(datagram_payload.datagram_type);
                }

                let mut ack_packet = [0u8; PACKET_SIZE];
                let ack_header = PacketHeader {
                    counter: header.counter,
                    acknum: datagram_payload.seqnum,
                    datagram: false,
                };
                ack_header.pack_to_slice(&mut ack_packet);

                RadioProtocol::<R, R, DC, DR>::packet_received(
                    &mut proto,
                    &ack_packet,
                    Instant::now(),
                );
            }

            if seen_datagram_types.len() == 2 {
                break;
            }
        }

        assert!(
            seen_datagram_types.contains(&CommandDatagramType::ReadRobotId)
                && seen_datagram_types.contains(&CommandDatagramType::ReadBoardIds),
            "Didn't receive both datagram types"
        );
    }
}
