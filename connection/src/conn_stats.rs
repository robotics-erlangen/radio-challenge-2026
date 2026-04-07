use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ConnectionStats {
    pub round_trip_packet_loss: f32,
    pub rtt_avg: Duration,
    pub rtt_stddev: Duration,
    pub rtt_p99: Duration,
    pub rtt_p90: Duration,
    pub rtt_p75: Duration,
}

#[derive(Debug)]
pub(crate) struct ConnectionStatTracker {
    history_length: usize,
    rtt_history: VecDeque<Option<Duration>>,
    max_pending: usize,
    pending_sent_times: HashMap<u32, Instant>,
}

impl ConnectionStatTracker {
    pub(crate) fn new(history_length: usize, max_pending: usize) -> Self {
        Self {
            history_length,
            rtt_history: VecDeque::with_capacity(history_length),
            max_pending,
            pending_sent_times: HashMap::with_capacity(max_pending),
        }
    }

    fn push_sample(&mut self, sample: Option<Duration>) {
        if self.rtt_history.len() == self.history_length {
            self.rtt_history.pop_front();
        }
        self.rtt_history.push_back(sample);
    }

    pub(crate) fn sent(&mut self, counter: u32, time: Instant) {
        // When neccessary, remove the oldest pending packets and mark them as lost
        while self.pending_sent_times.len() >= self.max_pending {
            let oldest_counter = self
                .pending_sent_times
                .iter()
                .min_by_key(|(_, timestamp)| *timestamp)
                .map(|(&counter, _)| counter);

            if let Some(counter_to_remove) = oldest_counter {
                self.pending_sent_times.remove(&counter_to_remove);
                self.push_sample(None);
            } else {
                break;
            }
        }

        self.pending_sent_times.insert(counter, time);
    }

    pub(crate) fn received(&mut self, counter: u32, time: Instant) {
        if let Some(sent_time) = self.pending_sent_times.remove(&counter) {
            self.push_sample(Some(time.saturating_duration_since(sent_time)));
        }
    }

    pub(crate) fn get(&self) -> ConnectionStats {
        if self.rtt_history.is_empty() {
            return ConnectionStats::default();
        }

        // Get a sorted buffer of all valid rtt measurements
        let mut rtt_samples = self
            .rtt_history
            .iter()
            .filter_map(|p| p.as_ref())
            .copied()
            .collect::<Vec<_>>();
        rtt_samples.sort_unstable();

        if rtt_samples.is_empty() {
            return ConnectionStats {
                round_trip_packet_loss: 1.0,
                ..ConnectionStats::default()
            };
        }

        let rtt_avg = rtt_samples.iter().sum::<Duration>() / rtt_samples.len() as u32;
        let rtt_stddev = rtt_samples
            .iter()
            .map(|rtt| rtt.abs_diff(rtt_avg))
            .sum::<Duration>()
            / rtt_samples.len() as u32;

        ConnectionStats {
            round_trip_packet_loss: (self.rtt_history.len() - rtt_samples.len()) as f32
                / self.rtt_history.len() as f32,
            rtt_avg,
            rtt_stddev,
            rtt_p99: rtt_samples[(rtt_samples.len() as f32 * 0.99) as usize],
            rtt_p90: rtt_samples[(rtt_samples.len() as f32 * 0.90) as usize],
            rtt_p75: rtt_samples[(rtt_samples.len() as f32 * 0.75) as usize],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ConnectionStatTracker;
    use crate::conn_stats::ConnectionStats;
    use std::time::{Duration, Instant};

    #[test]
    fn empty_history_default() {
        let tracker = ConnectionStatTracker::new(10, 10);
        assert_eq!(tracker.get(), ConnectionStats::default());
    }

    #[test]
    fn typical_usage() {
        let mut tracker = ConnectionStatTracker::new(100, 5);
        let start = Instant::now();

        for i in 0..10 {
            let send_time = start + Duration::from_millis(i * 10);
            tracker.sent(i as u32, send_time);
            let recv_time = send_time + Duration::from_millis(5 + (i % 3));
            tracker.received(i as u32, recv_time);
        }

        let stats = tracker.get();
        assert_eq!(stats.round_trip_packet_loss, 0.0);
        assert!(stats.rtt_avg >= Duration::from_millis(5));
        assert!(stats.rtt_avg <= Duration::from_millis(7));
    }

    #[test]
    fn percentiles() {
        let mut tracker = ConnectionStatTracker::new(100, 5);
        let start = Instant::now();

        // Send 100 packets with increasing RTTs from 0ms to 99ms. i=rtt
        for i in 0..100 {
            let send_time = start + Duration::from_millis(i as u64);
            tracker.sent(i, send_time);
            let recv_time = send_time + Duration::from_millis(i as u64);
            tracker.received(i, recv_time);
        }

        assert_eq!(
            tracker.get(),
            ConnectionStats {
                round_trip_packet_loss: 0.0,
                rtt_avg: Duration::from_micros(49500), // 49.5ms (Not 50 because 0 is included but 100 is not)
                rtt_stddev: Duration::from_millis(25),
                rtt_p99: Duration::from_millis(99),
                rtt_p90: Duration::from_millis(90),
                rtt_p75: Duration::from_millis(75),
            }
        );
    }

    #[test]
    fn out_of_order_receive() {
        let mut tracker = ConnectionStatTracker::new(16, 16);
        let start = Instant::now();

        tracker.sent(1, start);
        tracker.sent(2, start + Duration::from_millis(5));
        tracker.sent(3, start + Duration::from_millis(10));

        tracker.received(2, start + Duration::from_millis(15)); // RTT 10
        tracker.received(3, start + Duration::from_millis(30)); // RTT 20
        tracker.received(1, start + Duration::from_millis(30)); // RTT 30

        let stats = tracker.get();
        assert_eq!(stats.round_trip_packet_loss, 0.0);
        assert_eq!(stats.rtt_avg, Duration::from_millis(20));
    }

    #[test]
    fn lost_packet() {
        let mut tracker = ConnectionStatTracker::new(16, 3);
        let start = Instant::now();

        // Send 4 packets but max_pending is 3
        tracker.sent(1, start);
        tracker.sent(2, start + Duration::from_millis(5));
        tracker.sent(3, start + Duration::from_millis(10));
        tracker.sent(4, start + Duration::from_millis(15)); // This causes packet 1 to be evicted as lost

        tracker.received(3, start + Duration::from_millis(20));
        tracker.received(4, start + Duration::from_millis(25));

        let stats = tracker.get();
        // One loss (packet 1), two successful samples (3 and 4), one still pending (2)
        assert_eq!(stats.round_trip_packet_loss, 1.0 / 3.0);
    }

    #[test]
    fn receive_unknown_counter() {
        let mut tracker = ConnectionStatTracker::new(16, 16);
        let start = Instant::now();

        tracker.sent(1, start);
        tracker.received(1, start + Duration::from_millis(10));

        // Receive a packet we never sent - should be silently ignored
        tracker.received(999, start + Duration::from_millis(123));

        let stats = tracker.get();
        assert_eq!(stats.round_trip_packet_loss, 0.0);
        assert_eq!(stats.rtt_avg, Duration::from_millis(10)); // Still the rtt from packet 1
    }

    #[test]
    fn counter_wrapping() {
        let mut tracker = ConnectionStatTracker::new(16, 16);
        let start = Instant::now();

        let counter1 = u32::MAX;
        let counter2 = 0u32;

        tracker.sent(counter1, start);
        tracker.sent(counter2, start + Duration::from_millis(5));

        tracker.received(counter1, start + Duration::from_millis(4));
        tracker.received(counter2, start + Duration::from_millis(11));

        let stats = tracker.get();
        assert_eq!(stats.round_trip_packet_loss, 0.0);
        assert_eq!(stats.rtt_avg, Duration::from_millis(5));
    }
}
