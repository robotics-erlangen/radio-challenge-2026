use crate::conn_stats::ConnectionStats;
use std::fmt::{Debug, Formatter};
use std::time::Instant;

pub mod deku_helpers;
pub mod proto2025;

/// Protocol/Connection state for a single robot.
/// Generics: RegularCommand, RegularResponse, DatagramCommand, DatagramResponse
pub trait RadioProtocol<RC, RR, DC, DR> {
    const RESPONSE_PACKET_SIZE: usize;

    /// Returns some basic performance statistics about the connection.
    fn stats(&self) -> ConnectionStats;

    /// Unpacks a packet and updates internal connection state.
    // TODO: Replace &[u8] with &[u8; RESPONSE_PACKET_SIZE]
    fn packet_received(&mut self, bytes: &[u8], timestamp: Instant) -> PacketRxResult<RR, DR>;
    /// Queues a datagram for sending. It will be gradually transmitted over the next calls to `next_packet`.
    fn queue_datagram(&mut self, datagram: DC);
    /// Packs a packet for sending. If a datagram is available, it will be used instead.
    fn next_packet(&mut self, regular_data: RC) -> Vec<u8>;
}

pub enum PacketRxResult<RR, DR> {
    Regular(RR),
    Datagram(DR),
    IncompleteDatagram,
}

// Conditional Debug impl so that the Debug bound doesn't infect the trait.
impl<RR: Debug, DR: Debug> Debug for PacketRxResult<RR, DR> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Regular(arg0) => f.debug_tuple("Regular").field(arg0).finish(),
            Self::Datagram(arg0) => f.debug_tuple("Datagram").field(arg0).finish(),
            Self::IncompleteDatagram => f.write_str("IncompleteDatagram"),
        }
    }
}
