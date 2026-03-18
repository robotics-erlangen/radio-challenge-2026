#[cfg(feature = "serial")]
use mio_serial::SerialPortInfo;
use std::fmt::{Display, Formatter};
#[cfg(feature = "udp")]
use std::net::SocketAddr;
use std::time::Duration;

use protocol::proto2025::packet::PACKET_SIZE;

pub mod cache;
pub mod conn_stats;
pub mod pool;
pub mod protocol;
#[cfg(feature = "serial")]
pub mod serial;
#[cfg(feature = "udp")]
pub mod udp;

const DEFAULT_SEND_PERIOD: Duration = Duration::from_millis(10);
const DEFAULT_TIMEOUT: Duration = Duration::from_millis(1000);

#[derive(Clone, Debug)]
pub enum RobotMessage<RR, DR> {
    Connected(u8, RobotTransceiverAddress),
    Disconnected(u8),
    PacketReceived(u8, RR),
    DatagramReceived(u8, DR),
}

#[derive(Clone, Debug)]
pub enum TransceiverMessage {
    Connected(RobotTransceiverAddress, u8),
    Disconnected(RobotTransceiverAddress),
    PacketReceived(RobotTransceiverAddress, [u8; PACKET_SIZE]),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RobotTransceiverAddress {
    #[cfg(feature = "serial")]
    Serial(SerialPortInfo),
    #[cfg(feature = "udp")]
    Udp(SocketAddr),
}

impl Display for RobotTransceiverAddress {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            #[cfg(feature = "serial")]
            RobotTransceiverAddress::Serial(port) => f.write_str(&port.port_name),
            #[cfg(feature = "udp")]
            RobotTransceiverAddress::Udp(ip) => f.write_fmt(format_args!("{ip}")),
        }
    }
}

#[cfg(feature = "serial")]
impl From<SerialPortInfo> for RobotTransceiverAddress {
    fn from(value: SerialPortInfo) -> Self {
        Self::Serial(value)
    }
}

#[cfg(feature = "udp")]
impl From<SocketAddr> for RobotTransceiverAddress {
    fn from(value: SocketAddr) -> Self {
        Self::Udp(value)
    }
}
