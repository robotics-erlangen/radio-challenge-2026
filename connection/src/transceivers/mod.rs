use crate::RobotIdFilter;
use crate::driver::TokenAllocator;
use log::error;
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

pub trait Transceiver {
    fn set_id_filter(&mut self, id_filter: RobotIdFilter);

    fn next_timeout(&self) -> Instant;

    fn send_packet(&mut self, addr: &RobotTransceiverAddress, packet: &[u8]);

    fn mio_timeout(
        &mut self,
        now: Instant,
        poll: &mut mio::Poll,
        token_allocator: &mut TokenAllocator,
        events_out: &mut Vec<TransceiverEvent>,
    );

    fn mio_event(
        &mut self,
        event: mio::event::Event,
        poll: &mut mio::Poll,
        token_allocator: &mut TokenAllocator,
        events_out: &mut Vec<TransceiverEvent>,
    );
}

pub struct TransceiverGroup {
    #[cfg(feature = "serial")]
    pub serial: Option<serial::SerialTransceiver>,
    #[cfg(feature = "udp")]
    pub udp: Option<udp::UdpTransceiver>,
}

impl TransceiverGroup {
    pub fn init_all(
        poll: &mut mio::Poll,
        token_allocator: &mut TokenAllocator,
        packet_size: usize,
    ) -> Self {
        Self {
            #[cfg(feature = "serial")]
            serial: serial::SerialTransceiver::start(poll, token_allocator, packet_size)
                .inspect_err(|e| error!("Failed to initialize serial transceiver: {e}"))
                .ok(),
            #[cfg(feature = "udp")]
            udp: udp::UdpTransceiver::start(poll, token_allocator, packet_size)
                .inspect_err(|e| error!("Failed to initialize udp transceiver: {e}"))
                .ok(),
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = &dyn Transceiver> {
        let mut transceivers: Vec<&'_ dyn Transceiver> = Vec::new();
        #[cfg(feature = "serial")]
        if let Some(serial) = &self.serial {
            transceivers.push(serial);
        }
        #[cfg(feature = "udp")]
        if let Some(udp) = &self.udp {
            transceivers.push(udp);
        }
        transceivers.into_iter()
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut dyn Transceiver> {
        let mut transceivers: Vec<&'_ mut dyn Transceiver> = Vec::new();
        #[cfg(feature = "serial")]
        if let Some(serial) = &mut self.serial {
            transceivers.push(serial);
        }
        #[cfg(feature = "udp")]
        if let Some(udp) = &mut self.udp {
            transceivers.push(udp);
        }
        transceivers.into_iter()
    }
}

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
