use std::collections::VecDeque;
use std::time::{Duration, Instant};

#[derive(Debug, Default, Clone)]
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
    capacity: usize,
    rtt_history: VecDeque<Option<Duration>>,
    sent_time: Option<Instant>,
}

impl ConnectionStatTracker {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            rtt_history: VecDeque::with_capacity(capacity),
            sent_time: None,
        }
    }

    fn push_sample(&mut self, sample: Option<Duration>) {
        if self.rtt_history.len() == self.capacity {
            self.rtt_history.pop_front();
        }
        self.rtt_history.push_back(sample);
    }

    pub(crate) fn sent(&mut self) {
        if self.sent_time.is_some() {
            // No response to the last command -> mark as lost
            self.push_sample(None)
        }
        self.sent_time = Some(Instant::now());
    }

    pub(crate) fn received(&mut self) {
        if let Some(sent_time) = self.sent_time.take() {
            self.push_sample(Some(sent_time.elapsed()));
        }
    }

    pub(crate) fn get(&self) -> ConnectionStats {
        if self.rtt_history.is_empty() {
            return ConnectionStats::default();
        }

        // Get a sorted buffer of all valid rtt measurements
        let mut rtt_buf = self
            .rtt_history
            .iter()
            .filter_map(|p| p.as_ref())
            .copied()
            .collect::<Vec<_>>();
        rtt_buf.sort_unstable();

        if rtt_buf.is_empty() {
            return ConnectionStats {
                round_trip_packet_loss: 1.0,
                ..ConnectionStats::default()
            };
        }

        let rtt_avg = rtt_buf.iter().sum::<Duration>() / rtt_buf.len() as u32;
        let rtt_stddev = rtt_buf
            .iter()
            .map(|rtt| rtt.abs_diff(rtt_avg))
            .sum::<Duration>()
            / rtt_buf.len() as u32;

        ConnectionStats {
            round_trip_packet_loss: (self.rtt_history.len() - rtt_buf.len()) as f32
                / self.rtt_history.len() as f32,
            rtt_avg,
            rtt_stddev,
            rtt_p99: rtt_buf[(rtt_buf.len() as f32 * 0.99) as usize],
            rtt_p90: rtt_buf[(rtt_buf.len() as f32 * 0.90) as usize],
            rtt_p75: rtt_buf[(rtt_buf.len() as f32 * 0.75) as usize],
        }
    }
}
