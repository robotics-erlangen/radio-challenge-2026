#[cfg(feature = "serial")]
use mio_serial::SerialPortInfo;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
#[cfg(feature = "udp")]
use std::net::SocketAddr;
use std::time::{Duration, Instant};

pub mod cache;
pub mod conn_stats;
mod dual_map;
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
    PacketReceived(u8, RR, Instant),
    DatagramReceived(u8, DR, Instant),
}

#[derive(Clone, Debug)]
pub enum TransceiverMessage {
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

#[derive(Clone, Debug, Default)]
pub struct RobotIdFilter {
    whitelist: Option<u32>,
    blacklist: Option<u32>,
}

impl RobotIdFilter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_whitelist(mut self, ids: impl IntoIterator<Item = u8>) -> Self {
        let mut bits = 0u32;
        for id in ids {
            if id < 32 {
                bits |= 1 << id;
            }
        }
        self.whitelist = Some(bits);
        self
    }

    pub fn with_blacklist(mut self, ids: impl IntoIterator<Item = u8>) -> Self {
        let mut bits = 0u32;
        for id in ids {
            if id < 32 {
                bits |= 1 << id;
            }
        }
        self.blacklist = Some(bits);
        self
    }

    pub fn apply(&self, id: u8) -> bool {
        if id > 31 {
            return false;
        }
        let bit = 1 << id;
        self.whitelist.is_none_or(|w| (w & bit) == 1)
            && self.blacklist.is_none_or(|b| (b & bit) == 0)
    }
}
