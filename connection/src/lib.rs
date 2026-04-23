use crate::transceivers::{RobotTransceiverAddress, TransceiverError};
use std::sync::Arc;
use std::time::Instant;

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
