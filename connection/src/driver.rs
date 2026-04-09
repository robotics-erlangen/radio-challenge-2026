use crate::conn_stats::ConnectionStats;
use crate::dual_map::DualHashMap;
use crate::protocol::{PacketRxResult, RadioProtocol};
#[cfg(feature = "serial")]
use crate::serial::SerialTransceiver;
#[cfg(feature = "udp")]
use crate::udp::UdpTransceiver;
use crate::{ConnectionDriverEvent, RobotIdFilter, RobotTransceiverAddress, TransceiverEvent};
use flume::{Receiver, Sender, TrySendError};
use log::{error, info};
use std::io::ErrorKind;
use std::marker::PhantomData;
use std::sync::{Arc, RwLock};
use std::thread::{self, JoinHandle};
use std::time::Instant;

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

    active_connections: Arc<RwLock<DualHashMap<u8, RobotTransceiverAddress, P>>>,

    /// Merged message stream from all connections
    out_channel: Receiver<ConnectionDriverEvent<RR, DR>>,
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
        let active_connections = Arc::new(RwLock::new(DualHashMap::new()));

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
        let (addr, bytes) = {
            let mut conns = self.active_connections.write().unwrap();
            if let Some((addr, proto)) = conns.get_prim_mut(&robot_id) {
                (addr.clone(), proto.next_packet(packet))
            } else {
                // No connection for this robot id
                return;
            }
        };
        // Send the packet to the mio thread. It is important to release active_connections before sending because the bounded channel blocks if full and the thread can't process the messages while active_connections is locked.
        self.thread_control_channel
            .send(ConnectionDriverControlMessage::Send(addr.clone(), bytes))
            .unwrap();
        self.thread_control_waker.wake().unwrap();
    }

    pub fn queue_datagram(&self, robot_id: u8, datagram: DC) {
        let mut conns = self.active_connections.write().unwrap();
        if let Some((_addr, proto)) = conns.get_prim_mut(&robot_id) {
            proto.queue_datagram(datagram);
        }
    }

    pub fn recv(&self) -> ConnectionDriverEvent<RR, DR> {
        self.out_channel.recv().unwrap()
    }
    pub fn try_recv(&self) -> Result<ConnectionDriverEvent<RR, DR>, flume::TryRecvError> {
        self.out_channel.try_recv()
    }
    pub fn recv_async(&'_ self) -> flume::r#async::RecvFut<'_, ConnectionDriverEvent<RR, DR>> {
        self.out_channel.recv_async()
    }

    pub fn connection_stats(&self, robot_id: u8) -> Option<ConnectionStats> {
        let conns = self.active_connections.read().unwrap();
        conns
            .get_prim(&robot_id)
            .map(|(_addr, proto)| proto.stats())
    }

    pub fn has_robot(&self, robot_id: u8) -> bool {
        let conns = self.active_connections.read().unwrap();
        conns.contains_prim(&robot_id)
    }
    pub fn connected_robots(&self) -> Vec<u8> {
        let conns = self.active_connections.read().unwrap();
        conns.keys().map(|(id, _addr)| *id).collect()
    }
    pub fn transceiver_addr(&self, robot_id: u8) -> Option<RobotTransceiverAddress> {
        let conns = self.active_connections.read().unwrap();
        conns.get_prim(&robot_id).map(|(addr, _proto)| addr.clone())
    }

    fn mio_thread(
        mut poll: mio::Poll,
        active_connections: Arc<RwLock<DualHashMap<u8, RobotTransceiverAddress, P>>>,
        control_channel: Receiver<ConnectionDriverControlMessage>,
        message_sender: Sender<ConnectionDriverEvent<RR, DR>>,
    ) {
        let mut events = mio::Events::with_capacity(64);

        let mut token_allocator = TokenAllocator(WAKER_TOKEN.0 + 1); // The waker is usually 0
        #[cfg(feature = "serial")]
        let mut serial_transceiver =
            SerialTransceiver::start(&mut poll, &mut token_allocator, P::RESPONSE_PACKET_SIZE)
                .expect("Failed to start serial transceiver");
        #[cfg(feature = "udp")]
        let mut udp_transceiver =
            UdpTransceiver::start(&mut poll, &mut token_allocator, P::RESPONSE_PACKET_SIZE)
                .expect("Failed to start UDP transceiver");

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
            let now = Instant::now();
            if next_timeout.is_some_and(|t| t <= now) {
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
                                        serial_transceiver.send_packet(&port_info.port_name, &bytes)
                                    }
                                    #[cfg(feature = "udp")]
                                    RobotTransceiverAddress::Udp(addr) => {
                                        udp_transceiver.send_packet(addr, &bytes)
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
                    TransceiverEvent::Connected(addr, robot_id) => {
                        let mut active_connections = active_connections.write().unwrap();
                        if !active_connections.contains_prim(&robot_id) {
                            active_connections.insert(robot_id, addr.clone(), P::default());
                            // Block on full channel because Connected messages can't be lost
                            message_sender
                                .send(ConnectionDriverEvent::Connected(robot_id, addr))
                                .expect("ConnectionDriver message receiver dropped before stopping the mio thread");
                        }
                        update_transceiver_blacklists = true;
                    }
                    TransceiverEvent::Disconnected(addr) => {
                        if let Some((robot_id, _proto)) =
                            active_connections.write().unwrap().remove_sec(&addr)
                        {
                            // Block on full channel because Disconnected messages can't be lost
                            message_sender
                                .send(ConnectionDriverEvent::Disconnected(robot_id))
                                .expect("ConnectionDriver message receiver dropped before stopping the mio thread");
                        }
                        update_transceiver_blacklists = true;
                    }
                    TransceiverEvent::PacketReceived(addr, bytes, received_on) => {
                        if let Some((&robot_id, proto)) =
                            active_connections.write().unwrap().get_sec_mut(&addr)
                        {
                            match proto.packet_received(&bytes, received_on) {
                                // Don't block if the channel is full because some package loss is acceptable for regular packets
                                PacketRxResult::Regular(packet) => match message_sender
                                    .try_send(ConnectionDriverEvent::PacketReceived(
                                        robot_id,
                                        packet,
                                        received_on,
                                    )) {
                                        Ok(_) => {}
                                        Err(TrySendError::Full(_)) => error!("Dropping received packet from robot {} because the message channel is full", robot_id),
                                        Err(_) => panic!("ConnectionDriver message receiver dropped before stopping the mio thread"),
                                    },
                                // Block on full channel because Datagram messages can't be lost
                                PacketRxResult::Datagram(dgram) => message_sender
                                    .send(ConnectionDriverEvent::DatagramReceived(
                                        robot_id,
                                        dgram,
                                        received_on,
                                    ))
                                    .expect("ConnectionDriver message receiver dropped before stopping the mio thread"),
                                PacketRxResult::IncompleteDatagram => {}
                            }
                        };
                    }
                }
            }

            if update_transceiver_blacklists {
                let filter = RobotIdFilter::new().with_blacklist(
                    active_connections
                        .read()
                        .unwrap()
                        .keys()
                        .map(|(id, _addr)| *id),
                );
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
