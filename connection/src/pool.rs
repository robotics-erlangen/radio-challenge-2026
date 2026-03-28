use crate::conn_stats::ConnectionStats;
use crate::protocol::{PacketRxResult, RadioProtocol};
#[cfg(feature = "serial")]
use crate::serial::SerialTransceiver;
#[cfg(feature = "udp")]
use crate::udp::UdpTransceiver;
use crate::{
    DEFAULT_SEND_PERIOD, RobotIdFilter, RobotMessage, RobotTransceiverAddress, TransceiverMessage,
};
use flume::{Receiver, Sender};
use log::info;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::marker::PhantomData;
use std::sync::{Arc, OnceLock, RwLock};
use std::thread;
use std::time::{Duration, Instant};

pub struct PeriodicConnectionPool<
    RC: Clone + Default + Send + Sync + 'static,
    RR: Send + Sync + 'static,
    DC: Send + Sync + 'static,
    DR: Send + Sync + 'static,
    P: RadioProtocol<RC, RR, DC, DR> + Default + Send + Sync + 'static,
> {
    inner: Arc<ConnectionPool<RC, RR, DC, DR, P>>,
    packets: Arc<RwLock<HashMap<u8, RC>>>,
    thread: Option<thread::JoinHandle<()>>,
    thread_control_channel: Sender<PeriodicConnectionPoolControlMessage>,
    send_period: Duration,
}

enum PeriodicConnectionPoolControlMessage {
    SetSendPeriod(Duration),
    Stop,
}

impl<
    RC: Clone + Default + Send + Sync + 'static,
    RR: Send + Sync + 'static,
    DC: Send + Sync + 'static,
    DR: Send + Sync + 'static,
    P: RadioProtocol<RC, RR, DC, DR> + Default + Send + Sync + 'static,
> PeriodicConnectionPool<RC, RR, DC, DR, P>
{
    pub fn new(conn_pool: ConnectionPool<RC, RR, DC, DR, P>) -> Self {
        let inner = Arc::new(conn_pool);
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

    pub fn set_regular_packet(&self, robot_id: u8, packet: RC) {
        self.packets.write().unwrap().insert(robot_id, packet);
    }
    pub fn queue_datagram(&self, robot_id: u8, datagram: DC) {
        self.inner.queue_datagram(robot_id, datagram);
    }

    pub fn recv(&self) -> RobotMessage<RR, DR> {
        self.inner.recv()
    }
    pub fn try_recv(&self) -> Result<RobotMessage<RR, DR>, flume::TryRecvError> {
        self.inner.try_recv()
    }
    pub fn recv_async(&'_ self) -> flume::r#async::RecvFut<'_, RobotMessage<RR, DR>> {
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
> Drop for PeriodicConnectionPool<RC, RR, DC, DR, P>
{
    fn drop(&mut self) {
        self.thread_control_channel
            .send(PeriodicConnectionPoolControlMessage::Stop)
            .unwrap();
        self.thread.take().unwrap().join().unwrap();
    }
}

pub struct ConnectionPool<
    RC,
    RR: Send + 'static,
    DC,
    DR: Send + 'static,
    P: RadioProtocol<RC, RR, DC, DR> + Default + Send + Sync + 'static,
> {
    inner: Arc<ConnectionPoolInner<P>>,
    /// Merged message stream from all connections
    out_channel: Receiver<RobotMessage<RR, DR>>,
    _phantom_data: PhantomData<(RC, RR, DC, DR)>,
}

struct ConnectionPoolInner<P> {
    #[cfg(feature = "serial")]
    serial_transceiver: OnceLock<SerialTransceiver>,
    #[cfg(feature = "udp")]
    udp_transceiver: OnceLock<UdpTransceiver>,
    active_connections: RwLock<HashMap<u8, (RobotTransceiverAddress, P)>>, // TODO: More granular locking (dashmap?)
}

impl<P> ConnectionPoolInner<P> {
    fn update_transceiver_blacklists(&self) {
        let filter = RobotIdFilter::new()
            .with_blacklist(self.active_connections.read().unwrap().keys().copied());
        #[cfg(feature = "serial")]
        self.serial_transceiver
            .get()
            .unwrap()
            .set_id_filter(filter.clone());
        #[cfg(feature = "udp")]
        self.udp_transceiver
            .get()
            .unwrap()
            .set_id_filter(filter.clone());
    }
}

impl<
    RC,
    RR: Send + 'static,
    DC,
    DR: Send + 'static,
    P: RadioProtocol<RC, RR, DC, DR> + Default + Send + Sync + 'static,
> ConnectionPool<RC, RR, DC, DR, P>
{
    pub fn start() -> Self {
        info!("Starting connection pool");

        let (sender, receiver) = flume::bounded(100);
        let pool_inner = Arc::new(ConnectionPoolInner {
            #[cfg(feature = "serial")]
            serial_transceiver: OnceLock::new(),
            #[cfg(feature = "udp")]
            udp_transceiver: OnceLock::new(),
            active_connections: RwLock::new(HashMap::new()),
        });

        let msg_handler = {
            let pool = pool_inner.clone();
            let sender = sender.clone();

            Arc::new(move |msg: TransceiverMessage| match msg {
                TransceiverMessage::Connected(addr, robot_id) => {
                    // Register the new connection
                    if let Entry::Vacant(e) =
                        pool.active_connections.write().unwrap().entry(robot_id)
                    {
                        e.insert((addr.clone(), P::default()));
                        sender
                            .send(RobotMessage::Connected(robot_id, addr))
                            .unwrap();
                    }
                    pool.update_transceiver_blacklists();
                }
                TransceiverMessage::Disconnected(addr) => {
                    // There should only ever be one active connection per address, but that isn't actually enforced elsewhere
                    let removed_active = pool
                        .active_connections
                        .write()
                        .unwrap()
                        .extract_if(|_, (a, _)| *a == addr)
                        .map(|(id, _)| id)
                        .collect::<Vec<_>>();
                    for id in removed_active {
                        sender.send(RobotMessage::Disconnected(id)).unwrap();
                    }
                    pool.update_transceiver_blacklists();
                }
                TransceiverMessage::PacketReceived(addr, bytes, received_on) => {
                    if let Some((&robot_id, (_, proto))) = pool
                        .active_connections
                        .write()
                        .unwrap()
                        .iter_mut()
                        .find(|(_, (a, _))| *a == addr)
                    {
                        match proto.packet_received(&bytes) {
                            PacketRxResult::Regular(packet) => sender
                                .send(RobotMessage::PacketReceived(robot_id, packet, received_on))
                                .unwrap(),
                            PacketRxResult::Datagram(dgram) => sender
                                .send(RobotMessage::DatagramReceived(robot_id, dgram, received_on))
                                .unwrap(),
                            PacketRxResult::IncompleteDatagram => {}
                        }
                    };
                }
            })
        };

        _ = pool_inner
            .serial_transceiver
            .set(SerialTransceiver::start(msg_handler.clone(), P::RESPONSE_PACKET_SIZE).unwrap());
        _ = pool_inner
            .udp_transceiver
            .set(UdpTransceiver::start(msg_handler.clone(), P::RESPONSE_PACKET_SIZE).unwrap());

        ConnectionPool {
            inner: pool_inner,
            out_channel: receiver,
            _phantom_data: PhantomData,
        }
    }

    pub fn send_packet(&self, robot_id: u8, packet: RC) {
        let mut conns = self.inner.active_connections.write().unwrap();
        if let Some((addr, proto)) = conns.get_mut(&robot_id) {
            let bytes = proto.next_packet(packet);
            match addr {
                #[cfg(feature = "serial")]
                RobotTransceiverAddress::Serial(port_info) => self
                    .inner
                    .serial_transceiver
                    .get()
                    .unwrap()
                    .send(port_info.port_name.clone(), &bytes),
                #[cfg(feature = "udp")]
                RobotTransceiverAddress::Udp(addr) => {
                    self.inner.udp_transceiver.get().unwrap().send(*addr, bytes)
                }
            }
        }
    }

    pub fn queue_datagram(&self, robot_id: u8, datagram: DC) {
        let mut conns = self.inner.active_connections.write().unwrap();
        if let Some((_addr, proto)) = conns.get_mut(&robot_id) {
            proto.queue_datagram(datagram);
        }
    }

    pub fn recv(&self) -> RobotMessage<RR, DR> {
        self.out_channel.recv().unwrap()
    }
    pub fn try_recv(&self) -> Result<RobotMessage<RR, DR>, flume::TryRecvError> {
        self.out_channel.try_recv()
    }
    pub fn recv_async(&'_ self) -> flume::r#async::RecvFut<'_, RobotMessage<RR, DR>> {
        self.out_channel.recv_async()
    }

    pub fn connection_stats(&self, robot_id: u8) -> Option<ConnectionStats> {
        let conns = self.inner.active_connections.read().unwrap();
        conns.get(&robot_id).map(|(_, proto)| proto.stats())
    }

    pub fn has_robot(&self, robot_id: u8) -> bool {
        let conns = self.inner.active_connections.read().unwrap();
        conns.contains_key(&robot_id)
    }
    pub fn connected_robots(&self) -> Vec<u8> {
        let conns = self.inner.active_connections.read().unwrap();
        conns.keys().copied().collect()
    }
    pub fn transceiver_addr(&self, robot_id: u8) -> Option<RobotTransceiverAddress> {
        let conns = self.inner.active_connections.read().unwrap();
        conns.get(&robot_id).map(|(addr, _proto)| addr.clone())
    }
}
