use crate::transceivers::{RobotTransceiverAddress, TransceiverError};
use std::sync::Arc;
use std::time::Instant;

// Crash when no transceiver feature is enabled. It wouldn't break anything, but it also just doesn't make any sense.
#[cfg(not(any(feature = "udp", feature = "serial")))]
compile_error!(
    "No transceiver enabled. Enable at least one of the following features: udp, serial"
);

pub mod driver;
pub mod periodic;
pub mod protocol;
mod transceivers;
pub mod utils;

#[derive(Clone, Debug)]
pub enum ConnectionDriverEvent<RR, DR> {
    Connected(u8, RobotTransceiverAddress),
    Disconnected(u8),
    PacketReceived(u8, RR, Instant),
    DatagramReceived(u8, DR, Instant),
    TransceiverError(Arc<TransceiverError>), // Needs to be Arc to allow cloning the wrapped io::Error
}
