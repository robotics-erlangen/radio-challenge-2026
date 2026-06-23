use crate::ConnectionDriverEvent;
use log::{debug, info, warn};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fs::File;
use std::io::Write;
use std::io::{self, BufWriter};
use std::path::PathBuf;
use std::time::Instant;

/// Helper struct for logging connection metrics from a [ConnectionDriverEvent] stream to .csv files.
pub struct MetricsLogger {
    ref_instant: Instant,
    base_dir: PathBuf,
    open_files: HashMap<u8, BufWriter<File>>,
}

impl MetricsLogger {
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        let path_buf = base_dir.into();
        std::fs::create_dir_all(&path_buf).unwrap();
        info!(
            "Metrics logs will be written to {:?}",
            path_buf.to_string_lossy()
        );
        Self {
            ref_instant: Instant::now(),
            base_dir: path_buf,
            open_files: HashMap::new(),
        }
    }

    fn next_logfile(&self, robot_id: u8) -> io::Result<BufWriter<File>> {
        let files = std::fs::read_dir(&self.base_dir)?;

        // Find the highest existing file number for this robot
        let highest_prev_lognum = files
            .filter_map(|f| f.ok())
            .filter_map(|f| f.file_name().into_string().ok())
            .filter_map(|s| {
                s.strip_prefix(&format!("robot_{}_metrics_", robot_id))?
                    .strip_suffix(".csv")?
                    .parse::<u32>()
                    .ok()
            })
            .max();

        let lognum = highest_prev_lognum.map_or(0, |n| n + 1);
        let file_path = self
            .base_dir
            .join(format!("robot_{}_metrics_{}.csv", robot_id, lognum));

        debug!(
            "Creating metrics log file for robot {robot_id}: {:?}",
            file_path.to_string_lossy()
        );

        let mut writer = BufWriter::new(File::create_new(file_path)?);
        writeln!(writer, "sent_time_us,rtt_us")?;
        Ok(writer)
    }

    pub fn update<RR, DR>(&mut self, driver_event: &ConnectionDriverEvent<RR, DR>) {
        match driver_event {
            ConnectionDriverEvent::Connected(robot_id, _) => match self.next_logfile(*robot_id) {
                Ok(file) => {
                    self.open_files.insert(*robot_id, file);
                }
                Err(e) => {
                    warn!("Failed to create metrics log file for robot {robot_id}: {e}");
                }
            },
            ConnectionDriverEvent::Disconnected(robot_id) => {
                self.open_files.remove(robot_id);
            }
            ConnectionDriverEvent::MetricsSample(robot_id, sample) => {
                match self.open_files.entry(*robot_id) {
                    Entry::Occupied(mut entry) => {
                        let sent_us = sample
                            .sent_time
                            .duration_since(self.ref_instant)
                            .as_micros();
                        let rtt_us = sample.rtt.map(|rtt| rtt.as_micros()).unwrap_or(0);

                        if let Err(e) = writeln!(entry.get_mut(), "{sent_us},{rtt_us}") {
                            warn!("Failed to write to metrics log for robot {robot_id}: {e}");
                        };
                    }
                    Entry::Vacant(_) => {
                        warn!("Ignoring MetricsSample for uninitialized robot {robot_id}");
                    }
                }
            }
            _ => {}
        }
    }
}
