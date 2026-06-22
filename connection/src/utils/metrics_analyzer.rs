use crate::ConnectionDriverEvent;
use log::warn;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::time::Duration;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ConnectionMetrics {
    pub round_trip_packet_loss: f32,
    pub rtt_avg: Duration,
    pub rtt_stddev: Duration,
    pub rtt_p99: Duration,
    pub rtt_p90: Duration,
    pub rtt_p75: Duration,
}

/// Helper struct for aggregating and analyzing connection metrics from a [ConnectionDriverEvent] stream.
///
/// Similar to the [ConnectionStateCache](super::cache::ConnectionStateCache).
pub struct MetricsAnalyzer {
    history_length: usize,
    rtt_histories: HashMap<u8, VecDeque<Option<Duration>>>,
}

impl MetricsAnalyzer {
    pub fn new(history_length: usize) -> Self {
        Self {
            history_length,
            rtt_histories: HashMap::new(),
        }
    }

    pub fn update<RR, DR>(&mut self, driver_event: &ConnectionDriverEvent<RR, DR>) {
        match driver_event {
            ConnectionDriverEvent::Connected(robot_id, _) => {
                self.rtt_histories
                    .insert(*robot_id, VecDeque::with_capacity(self.history_length));
            }
            ConnectionDriverEvent::Disconnected(robot_id) => {
                self.rtt_histories.remove(robot_id);
            }
            ConnectionDriverEvent::MetricsSample(robot_id, sample) => {
                match self.rtt_histories.entry(*robot_id) {
                    Entry::Occupied(mut entry) => {
                        let history = entry.get_mut();
                        if history.len() == self.history_length {
                            history.pop_front();
                        }
                        history.push_back(sample.rtt);
                    }
                    Entry::Vacant(entry) => {
                        warn!("Received a MetricsSample without a previous Connection event");
                        entry
                            .insert(VecDeque::with_capacity(self.history_length))
                            .push_back(sample.rtt);
                    }
                }
            }
            _ => {}
        }
    }

    pub fn get(&self, robot_id: u8) -> Option<ConnectionMetrics> {
        let rtt_history = self.rtt_histories.get(&robot_id)?;

        if rtt_history.is_empty() {
            return Some(ConnectionMetrics::default());
        }

        // Get a sorted buffer of all valid rtt measurements
        let mut rtt_samples = rtt_history
            .iter()
            .filter_map(|p| p.as_ref())
            .copied()
            .collect::<Vec<_>>();
        rtt_samples.sort_unstable();

        if rtt_samples.is_empty() {
            return Some(ConnectionMetrics {
                round_trip_packet_loss: 1.0,
                ..ConnectionMetrics::default()
            });
        }

        let rtt_avg = rtt_samples.iter().sum::<Duration>() / rtt_samples.len() as u32;
        let rtt_stddev = rtt_samples
            .iter()
            .map(|rtt| rtt.abs_diff(rtt_avg))
            .sum::<Duration>()
            / rtt_samples.len() as u32;

        Some(ConnectionMetrics {
            round_trip_packet_loss: (rtt_history.len() - rtt_samples.len()) as f32
                / rtt_history.len() as f32,
            rtt_avg,
            rtt_stddev,
            rtt_p99: rtt_samples[(rtt_samples.len() as f32 * 0.99) as usize],
            rtt_p90: rtt_samples[(rtt_samples.len() as f32 * 0.90) as usize],
            rtt_p75: rtt_samples[(rtt_samples.len() as f32 * 0.75) as usize],
        })
    }

    /// Iterate over the aggregated metrics for all known robots.
    /// Does NOT cache the results, so it can get very expensive with many robots.
    pub fn iter(&self) -> impl Iterator<Item = (u8, ConnectionMetrics)> {
        self.rtt_histories
            .keys()
            .filter_map(|&robot_id| self.get(robot_id).map(|stats| (robot_id, stats)))
    }
}

#[cfg(test)]
mod tests {
    use crate::ConnectionDriverEvent;
    use crate::transceivers::RobotTransceiverAddress;
    use crate::utils::metrics_analyzer::{ConnectionMetrics, MetricsAnalyzer};
    use crate::utils::metrics_tracker::MetricsSample;
    use std::time::{Duration, Instant};

    // Shorthand to avoid the useless generics
    type DriverEvent = ConnectionDriverEvent<(), ()>;

    #[test]
    fn unknown_id_none() {
        let analyzer = MetricsAnalyzer::new(10);
        assert_eq!(analyzer.get(0), None);
    }
    #[test]
    fn empty_history_default() {
        let mut analyzer = MetricsAnalyzer::new(10);
        analyzer.update(&DriverEvent::Connected(0, RobotTransceiverAddress::Test(0)));
        assert_eq!(analyzer.get(0), Some(ConnectionMetrics::default()));
    }

    #[test]
    fn percentiles() {
        let mut analyzer = MetricsAnalyzer::new(100);
        let start = Instant::now();

        analyzer.update(&DriverEvent::Connected(0, RobotTransceiverAddress::Test(0)));

        // Send 100 packets with increasing RTTs from 0ms to 99ms. i=rtt
        for i in 0..100 {
            analyzer.update(&DriverEvent::MetricsSample(
                0,
                MetricsSample {
                    sent_time: start + Duration::from_millis(i as u64),
                    rtt: Some(Duration::from_millis(i as u64)),
                },
            ));
        }

        assert_eq!(
            analyzer.get(0),
            Some(ConnectionMetrics {
                round_trip_packet_loss: 0.0,
                rtt_avg: Duration::from_micros(49500), // 49.5ms (Not 50 because 0 is included but 100 is not)
                rtt_stddev: Duration::from_millis(25),
                rtt_p99: Duration::from_millis(99),
                rtt_p90: Duration::from_millis(90),
                rtt_p75: Duration::from_millis(75),
            })
        );
    }
}
