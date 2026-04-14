use crate::transceivers::{RobotTransceiverAddress, TransceiverError};
use std::time::Instant;

pub mod cache;
pub mod conn_stats;
pub mod driver;
mod dual_map;
pub mod periodic;
pub mod protocol;
mod transceivers;

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

#[cfg(test)]
mod tests {
    use super::RobotIdFilter;

    #[test]
    fn whitelist_blacklist_conflict() {
        let filter = RobotIdFilter::new()
            .with_whitelist([1, 3, 5])
            .with_blacklist([3]);
        assert!(filter.apply(1));
        assert!(!filter.apply(3)); // Needs to pass both lists
        assert!(filter.apply(5));
        assert!(!filter.apply(2));
    }

    #[test]
    fn no_whitelist() {
        let filter = RobotIdFilter::new().with_blacklist([2, 4]);
        assert!(filter.apply(1));
        assert!(!filter.apply(2));
        assert!(filter.apply(3));
        assert!(!filter.apply(4));
    }

    #[test]
    fn no_blacklist() {
        let filter = RobotIdFilter::new().with_whitelist([1, 3]);
        assert!(filter.apply(1));
        assert!(!filter.apply(2));
        assert!(filter.apply(3));
        assert!(!filter.apply(4));
    }

    #[test]
    fn default_all_allowed() {
        let filter = RobotIdFilter::new();
        for id in 0..32 {
            assert!(filter.apply(id));
        }
    }

    #[test]
    fn out_of_range() {
        // Creation doesn't crash
        let filter = RobotIdFilter::new().with_whitelist([0, 255]);
        assert!(filter.apply(0)); // In-range part still works
        assert!(!filter.apply(255)); // Out-of-range should be rejected
    }
}
