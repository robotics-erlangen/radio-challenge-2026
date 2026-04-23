use crate::driver::ConnectionDriver;
use crate::protocol::RadioProtocol;
use crate::utils::conn_stats::ConnectionStats;
use crate::{ConnectionDriverEvent, RobotTransceiverAddress};
use flume::Sender;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const DEFAULT_SEND_PERIOD: Duration = Duration::from_millis(10);

pub struct PeriodicConnectionDriver<
    RC: Clone + Default + Send + Sync + 'static,
    RR: Send + Sync + 'static,
    DC: Send + Sync + 'static,
    DR: Send + Sync + 'static,
    P: RadioProtocol<RC, RR, DC, DR> + Default + Send + Sync + 'static,
> {
    inner: Arc<ConnectionDriver<RC, RR, DC, DR, P>>,
    packets: Arc<RwLock<HashMap<u8, RC>>>,
    thread: Option<JoinHandle<()>>,
    thread_control_channel: Sender<PeriodicConnectionDriverControlMessage>,
    send_period: Duration,
}

enum PeriodicConnectionDriverControlMessage {
    SetSendPeriod(Duration),
    Stop,
}

impl<
    RC: Clone + Default + Send + Sync + 'static,
    RR: Send + Sync + 'static,
    DC: Send + Sync + 'static,
    DR: Send + Sync + 'static,
    P: RadioProtocol<RC, RR, DC, DR> + Default + Send + Sync + 'static,
> PeriodicConnectionDriver<RC, RR, DC, DR, P>
{
    pub fn new(conn_driver: ConnectionDriver<RC, RR, DC, DR, P>) -> Self {
        let inner = Arc::new(conn_driver);
        let packets = Arc::new(RwLock::new(HashMap::<u8, RC>::new()));
        let (thread_control_sender, thread_control_receiver) = flume::bounded(100);

        let thread = {
            let inner = inner.clone();
            let packets = packets.clone();

            thread::spawn(move || {
                let mut send_period = DEFAULT_SEND_PERIOD;
                let mut next_send_time = Instant::now();
                loop {
                    // Send packets
                    for robot_id in inner.connected_robots() {
                        let packet = packets
                            .read()
                            .unwrap()
                            .get(&robot_id)
                            .cloned()
                            .unwrap_or_else(RC::default);
                        inner.send_packet(robot_id, packet.clone());
                    }

                    // Handle control messages
                    while let Ok(msg) = thread_control_receiver.try_recv() {
                        match msg {
                            PeriodicConnectionDriverControlMessage::SetSendPeriod(val) => {
                                send_period = val
                            }
                            PeriodicConnectionDriverControlMessage::Stop => return,
                        }
                    }

                    next_send_time += send_period;
                    if Instant::now() < next_send_time {
                        thread::sleep(next_send_time - Instant::now());
                    } else {
                        // Reset the timer when the last cycle took too long
                        next_send_time = Instant::now();
                    }
                }
            })
        };

        Self {
            inner,
            packets,
            thread: Some(thread),
            thread_control_channel: thread_control_sender,
            send_period: DEFAULT_SEND_PERIOD,
        }
    }

    pub fn set_send_period(&self, period: Duration) {
        self.thread_control_channel
            .send(PeriodicConnectionDriverControlMessage::SetSendPeriod(
                period,
            ))
            .unwrap();
    }
    pub fn send_period(&self) -> Duration {
        self.send_period
    }

    pub fn set_regular_packet(&self, robot_id: u8, packet: RC) {
        self.packets.write().unwrap().insert(robot_id, packet);
    }
    pub fn queue_datagram(&self, robot_id: u8, datagram: DC) {
        self.inner.queue_datagram(robot_id, datagram);
    }

    pub fn recv(&self) -> ConnectionDriverEvent<RR, DR> {
        self.inner.recv()
    }
    pub fn try_recv(&self) -> Result<ConnectionDriverEvent<RR, DR>, flume::TryRecvError> {
        self.inner.try_recv()
    }
    pub fn recv_async(&'_ self) -> flume::r#async::RecvFut<'_, ConnectionDriverEvent<RR, DR>> {
        self.inner.recv_async()
    }

    // TODO: Emit stats as a ConnectionStatistics message
    pub fn connection_stats(&self, robot_id: u8) -> Option<ConnectionStats> {
        self.inner.connection_stats(robot_id)
    }

    pub fn has_robot(&self, robot_id: u8) -> bool {
        self.inner.has_robot(robot_id)
    }
    pub fn connected_robots(&self) -> Vec<u8> {
        self.inner.connected_robots()
    }
    pub fn transceiver_addr(&self, robot_id: u8) -> Option<RobotTransceiverAddress> {
        self.inner.transceiver_addr(robot_id)
    }
}

impl<
    RC: Clone + Default + Send + Sync + 'static,
    RR: Send + Sync + 'static,
    DC: Send + Sync + 'static,
    DR: Send + Sync + 'static,
    P: RadioProtocol<RC, RR, DC, DR> + Default + Send + Sync + 'static,
> Drop for PeriodicConnectionDriver<RC, RR, DC, DR, P>
{
    fn drop(&mut self) {
        self.thread_control_channel
            .send(PeriodicConnectionDriverControlMessage::Stop)
            .unwrap();
        self.thread.take().unwrap().join().unwrap();
    }
}
