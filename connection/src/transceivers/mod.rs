#[cfg(feature = "serial")]
use mio_serial::SerialPortInfo;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
#[cfg(feature = "udp")]
use std::net::SocketAddr;
use std::time::Instant;

#[cfg(feature = "serial")]
pub mod serial;
#[cfg(feature = "udp")]
pub mod udp;

#[derive(Clone, Debug)]
pub enum TransceiverEvent {
    Connected(RobotTransceiverAddress, u8),
    Disconnected(RobotTransceiverAddress),
    PacketReceived(RobotTransceiverAddress, Box<[u8]>, Instant), // TODO: Replace with a statically sized array when feature(generic_const_exprs) lands
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RobotTransceiverAddress {
    #[cfg(feature = "serial")]
    Serial(SerialPortInfo),
    #[cfg(feature = "udp")]
    Udp(SocketAddr),
}

impl Hash for RobotTransceiverAddress {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            #[cfg(feature = "serial")]
            RobotTransceiverAddress::Serial(port) => port.port_name.hash(state),
            #[cfg(feature = "udp")]
            RobotTransceiverAddress::Udp(ip) => ip.hash(state),
        }
    }
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
