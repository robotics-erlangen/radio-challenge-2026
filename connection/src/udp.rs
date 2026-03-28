use crate::{DEFAULT_TIMEOUT, RobotIdFilter, RobotTransceiverAddress, TransceiverMessage};
use flume::{Receiver, Sender};
use log::{error, trace, warn};
use mio::Interest;
use mio::net::UdpSocket;
use network_interface::{NetworkInterface, NetworkInterfaceConfig};
use socket2::{Domain, Protocol, Socket, Type};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::io::ErrorKind;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::ops::Range;
use std::sync::{Arc, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use std::{io, thread};

// Protocol breakdown:
// 1. The host repeatedly sends out empty beacon packets to a fixed multicast address on all interfaces.
//    Doing the discovery this way has the advantage that the router can convert them to individual unicast packets to each interested robot.
//    This means that the airtime scales linearly with the number of waiting robots (even down to 0),
//    but that is still much better than using slow wifi broadcasts.
// 2. Each robot ready to accept a connection subscribes to the multicast group and responds to all beacons with its robot id.
// 3. The host starts sending a continuous command stream to every known robot on a set port (not a direct response).
// 4. After receiving the first command packet from a host it has previously responded to, a robot will unsubscribe
//    from the multicast group and start the regular communication by responding to incoming packets.
// Both sides have a fixed timeout before they will assume a disconnection:
// - Hosts will forget the connection and stop sending command packets
// - Robots will resubscribe to the beacon multicast group and wait for a new host

// Address definitions
const BEACON_ADDR_V4: SocketAddr =
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(239, 0, 0, 1), 11000));
const BEACON_ADDR_V6: SocketAddr = SocketAddr::V6(SocketAddrV6::new(
    Ipv6Addr::from_bits(0xFF15_0000_0000_0045_5246_6F72_6365_0000), // "ERForce" in hex
    11000,
    0,
    0,
));
const DATA_PORT: u16 = 11001;
// Multiple ports so that multiple instances can run on the same host
const DISCOVERY_BIND_RANGE: Range<u16> = 12000..12010;
const DATA_BIND_RANGE: Range<u16> = 12010..12020;

// Mio event tokens
const WAKER_TOKEN: mio::Token = mio::Token(0);
const DISCOVERY_V4_TOKEN: mio::Token = mio::Token(1);
const DISCOVERY_V6_TOKEN: mio::Token = mio::Token(2);
const DATA_V4_TOKEN: mio::Token = mio::Token(3);
const DATA_V6_TOKEN: mio::Token = mio::Token(4);

fn bind_from_range(port_range: Range<u16>) -> Option<(UdpSocket, UdpSocket)> {
    port_range.into_iter().find_map(|port| {
        Some((
            bind_ipv4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port)).ok()?,
            bind_ipv6(SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, port, 0, 0)).ok()?,
        ))
    })
}
fn bind_ipv4(addr: SocketAddrV4) -> io::Result<UdpSocket> {
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_nonblocking(true)?; // Mio doesn't set nonblocking when converting from std
    socket.bind(&addr.into())?;
    Ok(UdpSocket::from_std(socket.into()))
}
fn bind_ipv6(addr: SocketAddrV6) -> io::Result<UdpSocket> {
    let socket = Socket::new(Domain::IPV6, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_nonblocking(true)?; // Mio doesn't set nonblocking when converting from std
    socket.set_only_v6(true)?; // By default, linux binds ipv6 sockets as dual-stack
    socket.bind(&addr.into())?;
    Ok(UdpSocket::from_std(socket.into()))
}

#[derive(Debug)]
pub struct UdpTransceiver {
    thread: Option<JoinHandle<()>>,
    /// ALWAYS CALL THE WAKER AFTER SUBMITTING A MESSAGE
    thread_control_channel: Sender<UdpControlMessage>,
    thread_control_waker: mio::Waker,

    connection_timeouts: Arc<RwLock<HashMap<SocketAddr, Instant>>>,
    // Stored here for easy access, the actual timeout used by the thread is updated via control messages
    timeout: Duration,
}

impl UdpTransceiver {
    pub fn start(
        msg_callback: Arc<dyn Fn(TransceiverMessage) + Send + Sync>,
        packet_size: usize,
    ) -> io::Result<Self> {
        let connection_timeouts = Arc::new(RwLock::new(HashMap::new()));

        // Bind sockets
        let mut discovery_sockets = bind_from_range(DISCOVERY_BIND_RANGE).unwrap();
        let mut data_sockets = bind_from_range(DATA_BIND_RANGE).unwrap();

        // Set up mio polling. Has to be done outside the mio thread because the waker has to be registered at creation.
        let poll = mio::Poll::new()?;
        let waker = mio::Waker::new(poll.registry(), WAKER_TOKEN)?;

        // The sockets could also be registered within the thread, but doing it here keeps all the registration in one place.
        poll.registry().register(
            &mut discovery_sockets.0,
            DISCOVERY_V4_TOKEN,
            Interest::READABLE,
        )?;
        poll.registry().register(
            &mut discovery_sockets.1,
            DISCOVERY_V6_TOKEN,
            Interest::READABLE,
        )?;
        poll.registry()
            .register(&mut data_sockets.0, DATA_V4_TOKEN, Interest::READABLE)?;
        poll.registry()
            .register(&mut data_sockets.1, DATA_V6_TOKEN, Interest::READABLE)?;

        // Start mio thread
        let (thread_control_sender, thread_control_receiver) = flume::unbounded();
        let thread = {
            let connection_timeouts = connection_timeouts.clone();
            thread::spawn(move || {
                udp_mio_thread(
                    poll,
                    discovery_sockets,
                    data_sockets,
                    packet_size,
                    connection_timeouts,
                    msg_callback,
                    thread_control_receiver,
                )
            })
        };

        Ok(Self {
            thread: Some(thread),
            thread_control_channel: thread_control_sender,
            thread_control_waker: waker,
            connection_timeouts,
            timeout: DEFAULT_TIMEOUT,
        })
    }

    pub fn set_id_filter(&self, filter: RobotIdFilter) {
        self.thread_control_channel
            .send(UdpControlMessage::SetIdFilter(filter))
            .unwrap();
        self.thread_control_waker.wake().unwrap();
    }

    pub fn set_timeout(&mut self, timeout: Duration) {
        self.timeout = timeout;
        self.thread_control_channel
            .send(UdpControlMessage::SetTimeout(timeout))
            .unwrap();
        self.thread_control_waker.wake().unwrap();
    }
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    pub fn send(&self, addr: SocketAddr, bytes: Vec<u8>) {
        // Send write commands to the io thread to avoid concurrent socket access
        self.thread_control_channel
            .send(UdpControlMessage::Write(addr, bytes))
            .unwrap();
        self.thread_control_waker.wake().unwrap();
    }

    pub fn connected_robots(&self) -> Vec<SocketAddr> {
        self.connection_timeouts
            .read()
            .unwrap()
            .keys()
            .copied()
            .collect()
    }
}

impl Drop for UdpTransceiver {
    fn drop(&mut self) {
        _ = self.thread_control_channel.send(UdpControlMessage::Stop);
        _ = self.thread_control_waker.wake();
        // Immediately dropping the waker can cause the event to be lost
        _ = self.thread.take().unwrap().join();
    }
}

enum UdpControlMessage {
    Write(SocketAddr, Vec<u8>),
    SetIdFilter(RobotIdFilter),
    SetTimeout(Duration),
    Stop,
}

fn udp_mio_thread(
    mut poll: mio::Poll,
    (discovery_socket_v4, discovery_socket_v6): (UdpSocket, UdpSocket),
    (data_socket_v4, data_socket_v6): (UdpSocket, UdpSocket),
    packet_size: usize,
    connection_timeouts: Arc<RwLock<HashMap<SocketAddr, Instant>>>,
    msg_callback: Arc<dyn Fn(TransceiverMessage) + Send + Sync>,
    control_channel: Receiver<UdpControlMessage>,
) {
    let mut configured_id_filter = RobotIdFilter::default();
    let mut configured_timeout = DEFAULT_TIMEOUT;
    let mut rx_buf = vec![0u8; packet_size].into_boxed_slice();

    let mut events = mio::Events::with_capacity(64);

    // Listen to io events and periodically send beacon packets
    let mut next_beacon_time = Instant::now() + Duration::from_secs(1);
    loop {
        let next_conn_timeout = connection_timeouts.read().unwrap().values().min().copied();
        let poll_deadline = next_conn_timeout.map_or(next_beacon_time, |t| t.min(next_beacon_time));
        let poll_timeout = poll_deadline.saturating_duration_since(Instant::now());

        if let Err(err) = poll.poll(&mut events, Some(poll_timeout)) {
            match err.kind() {
                ErrorKind::Interrupted => continue,
                ErrorKind::TimedOut => {
                    // Poll usually returns Ok(()) on timeout, but that isn't guaranteed,
                    // so we still need to cover this case.
                }
                _ => error!("Unexpected socket poll error: {}", err),
            }
        }

        if events.is_empty() {
            // Assume a timeout happened. Poll usually returns Ok(()) on timeout, so we can't check for it explicitly.
            let now = Instant::now();

            // Check if any connection has timed out
            if next_conn_timeout.is_some_and(|t| t < now) {
                connection_timeouts.write().unwrap().retain(|&addr, t| {
                    if *t < now {
                        msg_callback(TransceiverMessage::Disconnected(addr.into()));
                        false
                    } else {
                        true
                    }
                });
            }

            // Send beacon packets
            if now >= next_beacon_time {
                send_beacon_packets(&discovery_socket_v4, &discovery_socket_v6);
                next_beacon_time += Duration::from_secs(1);
            }

            continue;
        }

        for event in events.iter() {
            match event.token() {
                WAKER_TOKEN => {
                    // Process any incoming control messages
                    while let Ok(msg) = control_channel.try_recv() {
                        match msg {
                            UdpControlMessage::Write(addr, bytes) => {
                                // Check if the socket address is known
                                if !connection_timeouts.read().unwrap().contains_key(&addr) {
                                    continue;
                                }

                                let result = if addr.is_ipv4() {
                                    data_socket_v4.send_to(&bytes, addr)
                                } else {
                                    data_socket_v6.send_to(&bytes, addr)
                                };

                                if let Err(e) = result {
                                    warn!("Failed to send udp data packet to {addr}: {e}");
                                } else {
                                    trace!("Sent udp data packet to {addr}");
                                }
                            }
                            UdpControlMessage::SetIdFilter(val) => {
                                configured_id_filter = val;
                            }
                            UdpControlMessage::SetTimeout(val) => {
                                configured_timeout = val;
                            }
                            UdpControlMessage::Stop => return,
                        }
                    }
                }
                DISCOVERY_V4_TOKEN => receive_discovery_packets(
                    &discovery_socket_v4,
                    &mut connection_timeouts.write().unwrap(),
                    &configured_id_filter,
                    configured_timeout,
                    msg_callback.clone(),
                ),
                DISCOVERY_V6_TOKEN => receive_discovery_packets(
                    &discovery_socket_v6,
                    &mut connection_timeouts.write().unwrap(),
                    &configured_id_filter,
                    configured_timeout,
                    msg_callback.clone(),
                ),
                DATA_V4_TOKEN => receive_data_packets(
                    &data_socket_v4,
                    &mut rx_buf,
                    &mut connection_timeouts.write().unwrap(),
                    configured_timeout,
                    msg_callback.clone(),
                ),
                DATA_V6_TOKEN => receive_data_packets(
                    &data_socket_v6,
                    &mut rx_buf,
                    &mut connection_timeouts.write().unwrap(),
                    configured_timeout,
                    msg_callback.clone(),
                ),
                _ => warn!("Unexpected mio token: {event:?}"),
            }
        }
    }
}

fn send_beacon_packets(socket_v4: &UdpSocket, socket_v6: &UdpSocket) {
    let interfaces = NetworkInterface::show().unwrap();
    trace!(
        "Sending udp beacon packets: {}",
        interfaces
            .iter()
            .map(|i| i.name.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );

    for iface in interfaces {
        #[allow(clippy::collapsible_if)]
        if let Some(network_interface::Addr::V4(addr)) =
            iface.addr.iter().find(|a| a.ip().is_ipv4())
        {
            if socket2::SockRef::from(&socket_v4)
                .set_multicast_if_v4(&addr.ip)
                .is_ok()
            {
                if let Err(e) = socket_v4.send_to(&[], BEACON_ADDR_V4) {
                    warn!(
                        "Failed to send ipv4 beacon packet to interface {}: {e}",
                        iface.name
                    );
                }
            }
        } else if iface.addr.iter().any(|a| a.ip().is_ipv6()) {
            if socket2::SockRef::from(&socket_v6)
                .set_multicast_if_v6(iface.index)
                .is_ok()
            {
                if let Err(e) = socket_v6.send_to(&[], BEACON_ADDR_V6) {
                    warn!(
                        "Failed to send ipv6 beacon packet to interface {}: {e}",
                        iface.name
                    );
                }
            }
        }
    }
}

fn receive_discovery_packets(
    socket: &UdpSocket,
    connection_timeouts: &mut HashMap<SocketAddr, Instant>,
    configured_id_filter: &RobotIdFilter,
    configured_timeout: Duration,
    msg_callback: Arc<dyn Fn(TransceiverMessage) + Send + Sync>,
) {
    let mut rx_buf = [0u8; 1];
    loop {
        match socket.recv_from(&mut rx_buf) {
            Ok((_, src_addr)) => {
                let robot_id = rx_buf[0];
                if !configured_id_filter.apply(robot_id) {
                    continue;
                }

                // Construct the data address by replacing the port of the source address
                let data_addr = match src_addr {
                    SocketAddr::V4(addrv4) => {
                        SocketAddr::V4(SocketAddrV4::new(*addrv4.ip(), DATA_PORT))
                    }
                    SocketAddr::V6(addrv6) => SocketAddr::V6(SocketAddrV6::new(
                        *addrv6.ip(),
                        DATA_PORT,
                        0,
                        addrv6.scope_id(),
                    )),
                };

                // Insert into the connection map
                if let Entry::Vacant(e) = connection_timeouts.entry(data_addr) {
                    e.insert(Instant::now() + configured_timeout);
                    msg_callback(TransceiverMessage::Connected(
                        RobotTransceiverAddress::Udp(data_addr),
                        robot_id,
                    ));
                }
            }
            Err(e) if e.kind() == ErrorKind::WouldBlock => return,
            Err(e) => error!("Unexpected discovery socket rx error: {e}"),
        }
    }
}

fn receive_data_packets(
    socket: &UdpSocket,
    rx_buf: &mut [u8],
    connection_timeouts: &mut HashMap<SocketAddr, Instant>,
    configured_timeout: Duration,
    msg_callback: Arc<dyn Fn(TransceiverMessage) + Send + Sync>,
) {
    loop {
        match socket.recv_from(rx_buf) {
            Ok((_, src_addr)) => {
                let Some(connection_timeout) = connection_timeouts.get_mut(&src_addr) else {
                    continue;
                };

                *connection_timeout = Instant::now() + configured_timeout;
                trace!("Received udp packet from {src_addr}");
                msg_callback(TransceiverMessage::PacketReceived(
                    RobotTransceiverAddress::Udp(src_addr),
                    rx_buf.into(),
                    Instant::now(),
                ));
            }
            Err(e) if e.kind() == ErrorKind::WouldBlock => return,
            Err(e) => error!("Unexpected data socket rx error: {e}"),
        }
    }
}
