use crate::transceivers::{RobotTransceiverAddress, TransceiverError};
use std::time::{Duration, Instant};

pub mod cache;
pub mod conn_stats;
pub mod driver;
mod dual_map;
pub mod periodic;
pub mod protocol;
mod transceivers;

const DEFAULT_TIMEOUT: Duration = Duration::from_millis(1000);

#[derive(Debug)]
pub enum ConnectionDriverEvent<RR, DR> {
    Connected(u8, RobotTransceiverAddress),
    Disconnected(u8),
    PacketReceived(u8, RR, Instant),
    DatagramReceived(u8, DR, Instant),
    TransceiverError(TransceiverError),
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
        self.whitelist.is_none_or(|w| (w & bit) != 0)
            && self.blacklist.is_none_or(|b| (b & bit) == 0)
    }
}
