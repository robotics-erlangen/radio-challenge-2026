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
use log::{error, info};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::io::ErrorKind;
use std::marker::PhantomData;
use std::sync::{Arc, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

pub struct PeriodicConnectionPool<
    RC: Clone + Default + Send + Sync + 'static,
    RR: Send + Sync + 'static,
    DC: Send + Sync + 'static,
    DR: Send + Sync + 'static,
    P: RadioProtocol<RC, RR, DC, DR> + Default + Send + Sync + 'static,
> {
    inner: Arc<ConnectionDriver<RC, RR, DC, DR, P>>,
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
    pub fn new(conn_pool: ConnectionDriver<RC, RR, DC, DR, P>) -> Self {
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

// ================================================================================================
// ================================================================================================
// ================================================================================================

const WAKER_TOKEN: mio::Token = mio::Token(0);

pub struct TokenAllocator(usize);

impl TokenAllocator {
    pub fn new_token(&mut self) -> mio::Token {
        let token = mio::Token(self.0);
        self.0 += 1;
        token
    }
}

enum ConnectionDriverControlMessage {
    Send(RobotTransceiverAddress, Vec<u8>),
    Stop,
}

pub struct ConnectionDriver<
    RC,
    RR: Send + 'static,
    DC,
    DR: Send + 'static,
    P: RadioProtocol<RC, RR, DC, DR> + Default + Send + Sync + 'static,
> {
    thread: Option<JoinHandle<()>>,
    /// ALWAYS CALL THE WAKER AFTER SUBMITTING A MESSAGE
    thread_control_channel: Sender<ConnectionDriverControlMessage>,
    thread_control_waker: mio::Waker,

    // TODO: Key active connections by both id and transceiver address, like in the serial transceiver. Maybe create DoubleHashMap abstraction?
    active_connections: Arc<RwLock<HashMap<u8, (RobotTransceiverAddress, P)>>>,

    /// Merged message stream from all connections
    out_channel: Receiver<RobotMessage<RR, DR>>,
    _phantom_data: PhantomData<(RC, RR, DC, DR)>,
}

impl<
    RC,
    RR: Send + 'static,
    DC,
    DR: Send + 'static,
    P: RadioProtocol<RC, RR, DC, DR> + Default + Send + Sync + 'static,
> Drop for ConnectionDriver<RC, RR, DC, DR, P>
{
    fn drop(&mut self) {
        _ = self
            .thread_control_channel
            .send(ConnectionDriverControlMessage::Stop);
        _ = self.thread_control_waker.wake();
        // Immediately dropping the waker can cause the event to be lost
        _ = self.thread.take().unwrap().join();
    }
}

impl<
    RC,
    RR: Send + 'static,
    DC,
    DR: Send + 'static,
    P: RadioProtocol<RC, RR, DC, DR> + Default + Send + Sync + 'static,
> ConnectionDriver<RC, RR, DC, DR, P>
{
    pub fn start() -> Self {
        info!("Starting connection driver");

        // Mio setup
        let poll = mio::Poll::new().unwrap();
        let waker = mio::Waker::new(poll.registry(), WAKER_TOKEN).unwrap();

        let (message_sender, message_receiver) = flume::bounded(100);
        let (thread_control_sender, thread_control_receiver) = flume::bounded(100);
        let active_connections = Arc::new(RwLock::new(HashMap::new()));

        let thread = {
            let active_connections = active_connections.clone();
            thread::spawn(move || {
                Self::mio_thread(
                    poll,
                    active_connections,
                    thread_control_receiver,
                    message_sender,
                )
            })
        };

        ConnectionDriver {
            thread: Some(thread),
            thread_control_channel: thread_control_sender,
            thread_control_waker: waker,
            active_connections,
            out_channel: message_receiver,
            _phantom_data: PhantomData,
        }
    }

    pub fn send_packet(&self, robot_id: u8, packet: RC) {
        let mut conns = self.active_connections.write().unwrap();
        // Feed to the protocol, then send the bytes to the driver thread
        if let Some((addr, proto)) = conns.get_mut(&robot_id) {
            let bytes = proto.next_packet(packet);
            self.thread_control_channel
                .send(ConnectionDriverControlMessage::Send(addr.clone(), bytes))
                .unwrap();
            self.thread_control_waker.wake().unwrap();
        }
    }

    pub fn queue_datagram(&self, robot_id: u8, datagram: DC) {
        let mut conns = self.active_connections.write().unwrap();
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
        let conns = self.active_connections.read().unwrap();
        conns.get(&robot_id).map(|(_, proto)| proto.stats())
    }

    pub fn has_robot(&self, robot_id: u8) -> bool {
        let conns = self.active_connections.read().unwrap();
        conns.contains_key(&robot_id)
    }
    pub fn connected_robots(&self) -> Vec<u8> {
        let conns = self.active_connections.read().unwrap();
        conns.keys().copied().collect()
    }
    pub fn transceiver_addr(&self, robot_id: u8) -> Option<RobotTransceiverAddress> {
        let conns = self.active_connections.read().unwrap();
        conns.get(&robot_id).map(|(addr, _proto)| addr.clone())
    }

    fn mio_thread(
        mut poll: mio::Poll,
        active_connections: Arc<RwLock<HashMap<u8, (RobotTransceiverAddress, P)>>>,
        control_channel: Receiver<ConnectionDriverControlMessage>,
        message_sender: Sender<RobotMessage<RR, DR>>,
    ) {
        let mut events = mio::Events::with_capacity(64);

        let mut token_allocator = TokenAllocator(1); // 0 is reserved for the waker
        #[cfg(feature = "serial")]
        let mut serial_transceiver =
            SerialTransceiver::start(&mut poll, &mut token_allocator, P::RESPONSE_PACKET_SIZE)
                .unwrap();
        #[cfg(feature = "udp")]
        let mut udp_transceiver =
            UdpTransceiver::start(&mut poll, &mut token_allocator, P::RESPONSE_PACKET_SIZE)
                .unwrap();

        loop {
            // Get the closest transceiver timeout
            let transceiver_timeouts = vec![
                #[cfg(feature = "serial")]
                serial_transceiver.next_timeout(),
                #[cfg(feature = "udp")]
                udp_transceiver.next_timeout(),
            ];
            let next_timeout = transceiver_timeouts.into_iter().min();

            // Convert timeout instant to duration from now
            let timeout_dur = next_timeout.map(|i| i.saturating_duration_since(Instant::now()));

            if let Err(err) = poll.poll(&mut events, timeout_dur) {
                match err.kind() {
                    ErrorKind::Interrupted => continue,
                    ErrorKind::TimedOut => {
                        // Poll usually returns Ok(()) on timeout, but that isn't guaranteed,
                        // so we still need to cover this case.
                    }
                    _ => error!("Unexpected socket poll error: {}", err),
                }
            }

            // Temporary storage for any emitted transceiver messages
            let mut transceiver_messages = Vec::new();

            // Handle timeouts
            if events.is_empty() {
                // Assume a timeout happened. Poll usually returns Ok(()) on timeout, so we can't check for it explicitly.
                let now = Instant::now();

                #[cfg(feature = "serial")]
                serial_transceiver.mio_timeout(
                    now,
                    |msg| transceiver_messages.push(msg),
                    &mut poll,
                    &mut token_allocator,
                );
                #[cfg(feature = "udp")]
                udp_transceiver.mio_timeout(now, |msg| transceiver_messages.push(msg));
            }

            // Handle mio events and control messages
            for event in events.iter() {
                match event.token() {
                    WAKER_TOKEN => {
                        // Process any incoming control messages
                        while let Ok(msg) = control_channel.try_recv() {
                            match msg {
                                ConnectionDriverControlMessage::Send(addr, bytes) => match addr {
                                    #[cfg(feature = "serial")]
                                    RobotTransceiverAddress::Serial(port_info) => {
                                        serial_transceiver
                                            .send_packet(port_info.port_name.clone(), &bytes)
                                    }
                                    #[cfg(feature = "udp")]
                                    RobotTransceiverAddress::Udp(addr) => {
                                        udp_transceiver.send_packet(addr, bytes)
                                    }
                                },
                                ConnectionDriverControlMessage::Stop => return,
                            }
                        }
                    }
                    _ => {
                        // Call handlers on every transceiver
                        #[cfg(feature = "serial")]
                        serial_transceiver
                            .mio_event(event.clone(), |msg| transceiver_messages.push(msg));
                        #[cfg(feature = "udp")]
                        udp_transceiver
                            .mio_event(event.clone(), |msg| transceiver_messages.push(msg));
                    }
                }
            }

            // Process collected transceiver messages
            let mut update_transceiver_blacklists = false;
            for msg in transceiver_messages {
                match msg {
                    TransceiverMessage::Connected(addr, robot_id) => {
                        // Register the new connection
                        if let Entry::Vacant(e) =
                            active_connections.write().unwrap().entry(robot_id)
                        {
                            e.insert((addr.clone(), P::default()));
                            message_sender
                                .send(RobotMessage::Connected(robot_id, addr))
                                .unwrap();
                        }
                        update_transceiver_blacklists = true;
                    }
                    TransceiverMessage::Disconnected(addr) => {
                        // There isn't any explicit protection against duplicate transceiver addresses, so just removing the first one could break
                        let removed_active = active_connections
                            .write()
                            .unwrap()
                            .extract_if(|_, (a, _)| *a == addr)
                            .map(|(id, _)| id)
                            .collect::<Vec<_>>();
                        for id in removed_active {
                            message_sender.send(RobotMessage::Disconnected(id)).unwrap();
                        }
                        update_transceiver_blacklists = true;
                    }
                    TransceiverMessage::PacketReceived(addr, bytes, received_on) => {
                        if let Some((&robot_id, (_, proto))) = active_connections
                            .write()
                            .unwrap()
                            .iter_mut()
                            .find(|(_, (a, _))| *a == addr)
                        {
                            match proto.packet_received(&bytes) {
                                PacketRxResult::Regular(packet) => message_sender
                                    .send(RobotMessage::PacketReceived(
                                        robot_id,
                                        packet,
                                        received_on,
                                    ))
                                    .unwrap(),
                                PacketRxResult::Datagram(dgram) => message_sender
                                    .send(RobotMessage::DatagramReceived(
                                        robot_id,
                                        dgram,
                                        received_on,
                                    ))
                                    .unwrap(),
                                PacketRxResult::IncompleteDatagram => {}
                            }
                        };
                    }
                }
            }

            if update_transceiver_blacklists {
                let filter = RobotIdFilter::new()
                    .with_blacklist(active_connections.read().unwrap().keys().copied());
                #[cfg(feature = "serial")]
                {
                    serial_transceiver.id_filter = filter.clone();
                }
                #[cfg(feature = "udp")]
                {
                    udp_transceiver.id_filter = filter.clone();
                }
            }
        }
    }
}
