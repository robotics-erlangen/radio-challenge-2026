use crate::conn_core::{ConnectionCore, PacketRxResult};
use crate::conn_stats::ConnectionStats;
use crate::packet::RegularCommandData;
use crate::packet::datagrams::CommandDatagram;
#[cfg(feature = "serial")]
use crate::serial::SerialTransceiver;
#[cfg(feature = "udp")]
use crate::udp::UdpTransceiver;
use crate::{DEFAULT_SEND_PERIOD, RobotMessage, RobotTransceiverAddress, TransceiverMessage};
use flume::{Receiver, Sender};
use log::info;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant};

pub struct PeriodicConnectionPool {
    inner: Arc<ConnectionPool>,
    packets: Arc<RwLock<HashMap<u8, RegularCommandData>>>,
    thread: Option<thread::JoinHandle<()>>,
    thread_control_channel: Sender<PeriodicConnectionPoolControlMessage>,
    send_period: Duration,
}

enum PeriodicConnectionPoolControlMessage {
    SetSendPeriod(Duration),
    Stop,
}

impl PeriodicConnectionPool {
    pub fn new(conn_pool: ConnectionPool) -> Self {
        let inner = Arc::new(conn_pool);
        let packets = Arc::new(RwLock::new(HashMap::<u8, RegularCommandData>::new()));
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
                            .unwrap_or_else(RegularCommandData::default);
                        inner.send_packet(robot_id, packet.clone());
                    }

                    // Handle control messages
                    while let Ok(msg) = thread_control_receiver.try_recv() {
                        match msg {
                            PeriodicConnectionPoolControlMessage::SetSendPeriod(val) => {
                                send_period = val
                            }
                            PeriodicConnectionPoolControlMessage::Stop => return,
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
            .send(PeriodicConnectionPoolControlMessage::SetSendPeriod(period))
            .unwrap();
    }
    pub fn send_period(&self) -> Duration {
        self.send_period
    }

    pub fn set_regular_packet(&self, robot_id: u8, packet: RegularCommandData) {
        self.packets.write().unwrap().insert(robot_id, packet);
    }
    pub fn queue_datagram(&self, robot_id: u8, datagram: CommandDatagram) {
        self.inner.queue_datagram(robot_id, datagram);
    }

    pub fn recv(&self) -> RobotMessage {
        self.inner.recv()
    }
    pub fn try_recv(&self) -> Result<RobotMessage, flume::TryRecvError> {
        self.inner.try_recv()
    }
    pub fn recv_async(&'_ self) -> flume::r#async::RecvFut<'_, RobotMessage> {
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

impl Drop for PeriodicConnectionPool {
    fn drop(&mut self) {
        self.thread_control_channel
            .send(PeriodicConnectionPoolControlMessage::Stop)
            .unwrap();
        self.thread.take().unwrap().join().unwrap();
    }
}

pub struct ConnectionPool {
    /// Merged message stream from all connections
    out_channel: Receiver<RobotMessage>,
    #[cfg(feature = "serial")]
    serial_transceiver: SerialTransceiver,
    #[cfg(feature = "udp")]
    udp_transceiver: UdpTransceiver,
    active_connections: Arc<RwLock<HashMap<u8, (RobotTransceiverAddress, ConnectionCore)>>>,
    /// List of connections to duplicate robot ids. Will be promoted if the active one disconnects.
    #[allow(dead_code)]
    idle_connections: Arc<RwLock<Vec<(u8, RobotTransceiverAddress)>>>,
}

impl ConnectionPool {
    pub fn start() -> Self {
        info!("Starting connection pool");

        let (sender, receiver) = flume::bounded(100);
        let active_connections = Arc::new(RwLock::new(HashMap::new()));
        let idle_connections = Arc::new(RwLock::new(Vec::new()));

        let msg_handler = {
            let sender = sender.clone();
            let active_connections = active_connections.clone();
            let idle_connections = idle_connections.clone();

            Arc::new(move |msg: TransceiverMessage| match msg {
                TransceiverMessage::Connected(addr, robot_id) => {
                    if let Entry::Vacant(e) = active_connections.write().unwrap().entry(robot_id) {
                        e.insert((addr.clone(), ConnectionCore::new()));
                        sender
                            .send(RobotMessage::Connected(robot_id, addr))
                            .unwrap();
                    } else {
                        idle_connections.write().unwrap().push((robot_id, addr));
                    }
                }
                TransceiverMessage::Disconnected(addr) => {
                    let mut active = active_connections.write().unwrap();
                    let mut idle = idle_connections.write().unwrap();

                    idle.retain(|(_, a)| *a != addr);

                    // There should only ever be one active connection per address, but that isn't actually enforced elsewhere
                    let removed_active = active
                        .extract_if(|_, (a, _)| *a == addr)
                        .map(|(id, _)| id)
                        .collect::<Vec<_>>();
                    for id in removed_active {
                        sender.send(RobotMessage::Disconnected(id)).unwrap();
                        // Replace with idle connection where possible
                        if let Some(idx) = idle.iter().position(|(i, _)| *i == id) {
                            let (_, new_addr) = idle.remove(idx);
                            active.insert(id, (new_addr.clone(), ConnectionCore::new()));
                            sender.send(RobotMessage::Connected(id, new_addr)).unwrap();
                        }
                    }
                }
                TransceiverMessage::PacketReceived(addr, bytes) => {
                    if let Some((&robot_id, (_, proto))) = active_connections
                        .write()
                        .unwrap()
                        .iter_mut()
                        .find(|(_, (a, _))| *a == addr)
                    {
                        match proto.packet_received(&bytes) {
                            PacketRxResult::Regular(packet) => sender
                                .send(RobotMessage::PacketReceived(robot_id, packet))
                                .unwrap(),
                            PacketRxResult::Datagram(dgram) => sender
                                .send(RobotMessage::DatagramReceived(robot_id, dgram))
                                .unwrap(),
                            PacketRxResult::IncompleteDatagram => {}
                        }
                    };
                }
            })
        };

        ConnectionPool {
            out_channel: receiver,
            #[cfg(feature = "serial")]
            serial_transceiver: SerialTransceiver::start(msg_handler.clone()).unwrap(),
            #[cfg(feature = "udp")]
            udp_transceiver: UdpTransceiver::start(msg_handler.clone()).unwrap(),
            active_connections,
            idle_connections,
        }
    }

    // TODO: New send api that returns protocol references
    pub fn send_packet(&self, robot_id: u8, packet: RegularCommandData) {
        if let Some((addr, proto)) = self.active_connections.write().unwrap().get_mut(&robot_id) {
            let bytes = proto.next_packet(packet);
            match addr {
                #[cfg(feature = "serial")]
                RobotTransceiverAddress::Serial(port_info) => self
                    .serial_transceiver
                    .send(port_info.port_name.clone(), &bytes),
                #[cfg(feature = "udp")]
                RobotTransceiverAddress::Udp(addr) => self.udp_transceiver.send(*addr, bytes),
            }
        }
    }

    pub fn queue_datagram(&self, robot_id: u8, datagram: CommandDatagram) {
        if let Some((_addr, proto)) = self.active_connections.write().unwrap().get_mut(&robot_id) {
            proto.queue_datagram(datagram);
        }
    }

    pub fn recv(&self) -> RobotMessage {
        self.out_channel.recv().unwrap()
    }
    pub fn try_recv(&self) -> Result<RobotMessage, flume::TryRecvError> {
        self.out_channel.try_recv()
    }
    pub fn recv_async(&'_ self) -> flume::r#async::RecvFut<'_, RobotMessage> {
        self.out_channel.recv_async()
    }

    pub fn connection_stats(&self, robot_id: u8) -> Option<ConnectionStats> {
        self.active_connections
            .read()
            .unwrap()
            .get(&robot_id)
            .map(|(_, proto)| proto.stats())
    }

    pub fn has_robot(&self, robot_id: u8) -> bool {
        self.active_connections
            .read()
            .unwrap()
            .contains_key(&robot_id)
    }
    pub fn connected_robots(&self) -> Vec<u8> {
        self.active_connections
            .read()
            .unwrap()
            .keys()
            .copied()
            .collect()
    }
    pub fn transceiver_addr(&self, robot_id: u8) -> Option<RobotTransceiverAddress> {
        self.active_connections
            .read()
            .unwrap()
            .get(&robot_id)
            .map(|(addr, _proto)| addr.clone())
    }
}
