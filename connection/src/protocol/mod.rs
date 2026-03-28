use crate::conn_stats::ConnectionStats;

pub mod deku_helpers;
pub mod proto2025;

/// Generics: RegularCommand, RegularResponse, DatagramCommand, DatagramResponse
pub trait RadioProtocol<RC, RR, DC, DR> {
    const RESPONSE_PACKET_SIZE: usize;

    fn stats(&self) -> ConnectionStats;

    // TODO: Replace with a statically sized array
    fn packet_received(&mut self, bytes: &[u8]) -> PacketRxResult<RR, DR>;
    fn queue_datagram(&mut self, datagram: DC);
    fn next_packet(&mut self, regular_data: RC) -> Vec<u8>;
}

pub enum PacketRxResult<RR, DR> {
    Regular(RR),
    Datagram(DR),
    IncompleteDatagram,
}
