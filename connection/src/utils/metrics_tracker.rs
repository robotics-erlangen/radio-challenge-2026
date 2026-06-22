use std::collections::HashMap;
use std::time::{Duration, Instant};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetricsSample {
    /// When the command packet was sent. Used to reorder the events for plotting,
    /// since losses can only be emitted with a significant delay, after the timeout.
    pub sent_time: Instant,
    /// Measured round trip time, None if the packet or the response was lost
    pub rtt: Option<Duration>,
}

/// Helper struct for protocol implementations that tracks the RTT (or loss)
/// of sent packets, assuming each packet shares a counter with its response
pub(crate) struct MetricsTracker {
    /// The maximum number of simultaneous pending packets before the oldest one is considered lost
    max_pending: usize,
    /// All currently sent packets by counter and when they were sent
    pending_sent_times: HashMap<u32, Instant>,
    /// Callback to emit the raw rtt/loss events
    // TODO: Get rid of the Send + Sync bounds, without making the struct !Sync. Maybe with generics? Maybe as return values?
    sample_callback: Box<dyn Fn(MetricsSample) + Send + Sync>,
}

impl MetricsTracker {
    pub(crate) fn new(
        max_pending: usize,
        sample_callback: Box<dyn Fn(MetricsSample) + Send + Sync>,
    ) -> Self {
        Self {
            max_pending,
            pending_sent_times: HashMap::with_capacity(max_pending),
            sample_callback,
        }
    }

    fn emit_sample(&mut self, sample: Option<Duration>, sent_time: Instant) {
        (self.sample_callback)(MetricsSample {
            sent_time,
            rtt: sample,
        });
    }

    pub(crate) fn sent(&mut self, counter: u32, time: Instant) {
        // When necessary, remove the oldest pending packets and mark them as lost. Breaks if max_pending > counter.max
        while self.pending_sent_times.len() >= self.max_pending {
            let oldest_counter = self
                .pending_sent_times
                .iter()
                .min_by_key(|(_, timestamp)| *timestamp)
                .map(|(&counter, _)| counter);

            if let Some(counter_to_remove) = oldest_counter {
                let sent_time = self.pending_sent_times.remove(&counter_to_remove).unwrap();
                self.emit_sample(None, sent_time);
            } else {
                break;
            }
        }

        self.pending_sent_times.insert(counter, time);
    }

    pub(crate) fn received(&mut self, counter: u32, curr_time: Instant) {
        if let Some(sent_time) = self.pending_sent_times.remove(&counter) {
            self.emit_sample(
                Some(curr_time.saturating_duration_since(sent_time)),
                sent_time,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{MetricsSample, MetricsTracker};
    use flume::{Receiver, TryRecvError};
    use std::time::{Duration, Instant};

    fn new_test_tracker(
        capacity: usize,
        max_pending: usize,
    ) -> (MetricsTracker, Receiver<MetricsSample>) {
        let (sender, receiver) = flume::bounded(capacity);
        let tracker = MetricsTracker::new(
            max_pending,
            Box::new(move |sample| sender.send(sample).unwrap()),
        );
        (tracker, receiver)
    }

    fn sample(
        base_time: Instant,
        sent_millis: u32,
        received_millis: u32,
    ) -> Result<MetricsSample, TryRecvError> {
        assert!(received_millis >= sent_millis);
        Ok(MetricsSample {
            sent_time: base_time + Duration::from_millis(sent_millis as u64),
            rtt: Some(Duration::from_millis(
                (received_millis - sent_millis) as u64,
            )),
        })
    }

    fn loss(base_time: Instant, sent_millis: u32) -> Result<MetricsSample, TryRecvError> {
        Ok(MetricsSample {
            sent_time: base_time + Duration::from_millis(sent_millis as u64),
            rtt: None,
        })
    }

    #[test]
    fn typical_usage() {
        let (mut tracker, receiver) = new_test_tracker(10, 5);
        let start = Instant::now();

        for i in 0..10 {
            let send_time = start + Duration::from_millis(i * 10);
            tracker.sent(i as u32, send_time);
            let recv_time = send_time + Duration::from_millis(5 + (i % 3));
            tracker.received(i as u32, recv_time);
        }

        for i in 0..10 {
            assert_eq!(
                receiver.try_recv(),
                sample(start, i * 10, (i * 10) + (5 + (i % 3)))
            )
        }
        assert!(receiver.is_empty());
    }

    #[test]
    fn out_of_order_receive() {
        let (mut tracker, receiver) = new_test_tracker(10, 5);
        let start = Instant::now();

        tracker.sent(1, start);
        tracker.sent(2, start + Duration::from_millis(5));
        tracker.sent(3, start + Duration::from_millis(10));

        tracker.received(2, start + Duration::from_millis(15));
        tracker.received(3, start + Duration::from_millis(30));
        tracker.received(1, start + Duration::from_millis(30));

        assert_eq!(receiver.try_recv(), sample(start, 5, 15)); // 2
        assert_eq!(receiver.try_recv(), sample(start, 10, 30)); // 3
        assert_eq!(receiver.try_recv(), sample(start, 0, 30)); // 1
        assert!(receiver.is_empty());
    }

    #[test]
    fn lost_packet() {
        let (mut tracker, receiver) = new_test_tracker(10, 3);
        let start = Instant::now();

        // Send 4 packets but max_pending is 3
        tracker.sent(1, start);
        tracker.sent(2, start + Duration::from_millis(5));
        tracker.sent(3, start + Duration::from_millis(10));
        tracker.sent(4, start + Duration::from_millis(15)); // This causes packet 1 to be evicted as lost

        tracker.received(3, start + Duration::from_millis(20));
        tracker.received(4, start + Duration::from_millis(25));

        assert_eq!(receiver.try_recv(), loss(start, 0)); // 1
        assert_eq!(receiver.try_recv(), sample(start, 10, 20)); // 3
        assert_eq!(receiver.try_recv(), sample(start, 15, 25)); // 4
        assert!(receiver.is_empty()) // 2 is still pending
    }

    #[test]
    fn receive_unknown_counter() {
        let (mut tracker, receiver) = new_test_tracker(10, 5);
        let start = Instant::now();

        tracker.sent(1, start);

        // Receive a packet we never sent - should be silently ignored
        tracker.received(999, start + Duration::from_millis(5));
        assert!(receiver.is_empty());

        tracker.received(1, start + Duration::from_millis(10));
        assert_eq!(receiver.try_recv(), sample(start, 0, 10));

        assert!(receiver.is_empty());
    }

    // Counter-wrapping doesn't affect the current hashmap-based implementation, but that could change in the future.
    #[test]
    fn counter_wrapping() {
        let (mut tracker, receiver) = new_test_tracker(10, 5);
        let start = Instant::now();

        let counter1 = u32::MAX;
        let counter2 = 0u32;

        tracker.sent(counter1, start);
        tracker.sent(counter2, start + Duration::from_millis(5));

        tracker.received(counter1, start + Duration::from_millis(10));
        tracker.received(counter2, start + Duration::from_millis(20));

        assert_eq!(receiver.try_recv(), sample(start, 0, 10));
        assert_eq!(receiver.try_recv(), sample(start, 5, 20));
        assert!(receiver.is_empty());
    }

    #[test]
    fn non_monotonic_timestamps() {
        let (mut tracker, receiver) = new_test_tracker(10, 5);
        let start = Instant::now();

        tracker.sent(1, start);
        tracker.sent(2, start + Duration::from_millis(15));
        tracker.sent(3, start + Duration::from_millis(5));

        tracker.received(1, start + Duration::from_millis(20));
        tracker.received(2, start + Duration::from_millis(15));
        tracker.received(3, start + Duration::from_millis(10));

        assert_eq!(receiver.try_recv(), sample(start, 0, 20));
        assert_eq!(receiver.try_recv(), sample(start, 15, 15));
        assert_eq!(receiver.try_recv(), sample(start, 5, 10));
        assert!(receiver.is_empty());
    }
}
