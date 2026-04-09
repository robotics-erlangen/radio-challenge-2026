use crate::driver::TokenAllocator;
use crate::{DEFAULT_TIMEOUT, RobotIdFilter, RobotTransceiverAddress, TransceiverEvent};
use log::{error, trace, warn};
use mio::Interest;
use mio::net::UdpSocket;
use network_interface::{NetworkInterface, NetworkInterfaceConfig};
use socket2::{Domain, Protocol, Socket, Type};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::io;
use std::io::ErrorKind;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::ops::Range;
use std::time::{Duration, Instant};

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
    Ipv6Addr::from_bits(0xFF15_0000_0000_0045_5246_6F72_6365_0000), // "ERForce" in hex + 0000 protocol id
    11000,
    0,
    0,
));
const DATA_PORT: u16 = 11001;
// Multiple ports so that multiple instances can run on the same host
const DISCOVERY_BIND_RANGE: Range<u16> = 12000..12010;
const DATA_BIND_RANGE: Range<u16> = 12010..12020;

/// Tries to bind a pair of ipv4 and ipv6 udp sockets to the same port, returning the last error if all ports in the given range fail.
fn bind_from_range(port_range: Range<u16>) -> io::Result<(UdpSocket, UdpSocket)> {
    let mut last_err = io::Error::new(ErrorKind::InvalidInput, "empty port range");

    port_range
        .into_iter()
        .find_map(|port| {
            Some((
                bind_ipv4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port))
                    .map_err(|e| last_err = e)
                    .ok()?,
                bind_ipv6(SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, port, 0, 0))
                    .map_err(|e| last_err = e)
                    .ok()?,
            ))
        })
        .ok_or(last_err)
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
    connection_timeouts: HashMap<SocketAddr, Instant>,

    // IO resources
    discovery_socket_v4: RegisteredSocket,
    discovery_socket_v6: RegisteredSocket,
    data_socket_v4: RegisteredSocket,
    data_socket_v6: RegisteredSocket,
    rx_buf: Box<[u8]>, // TODO: Make rx_buf a statically sized array when feature(generic_const_exprs) lands

    // Cached timeouts
    next_beacon_time: Instant,
    next_conn_timeout: Option<Instant>,

    // Configuration
    pub timeout: Duration,
    pub id_filter: RobotIdFilter,
}

#[derive(Debug)]
struct RegisteredSocket {
    socket: UdpSocket,
    token: mio::Token,
}

impl UdpTransceiver {
    pub fn start(
        poll: &mut mio::Poll,
        token_allocator: &mut TokenAllocator,
        packet_size: usize,
    ) -> io::Result<Self> {
        // Allocate mio tokens
        let discovery_v4_token = token_allocator.new_token();
        let discovery_v6_token = token_allocator.new_token();
        let data_v4_token = token_allocator.new_token();
        let data_v6_token = token_allocator.new_token();

        // Bind sockets
        let mut discovery_sockets = bind_from_range(DISCOVERY_BIND_RANGE)?;
        let mut data_sockets = bind_from_range(DATA_BIND_RANGE)?;

        // Register the sockets to the caller's poll instance
        poll.registry().register(
            &mut discovery_sockets.0,
            discovery_v4_token,
            Interest::READABLE,
        )?;
        poll.registry().register(
            &mut discovery_sockets.1,
            discovery_v6_token,
            Interest::READABLE,
        )?;
        poll.registry()
            .register(&mut data_sockets.0, data_v4_token, Interest::READABLE)?;
        poll.registry()
            .register(&mut data_sockets.1, data_v6_token, Interest::READABLE)?;

        Ok(Self {
            connection_timeouts: HashMap::new(),

            discovery_socket_v4: RegisteredSocket {
                socket: discovery_sockets.0,
                token: discovery_v4_token,
            },
            discovery_socket_v6: RegisteredSocket {
                socket: discovery_sockets.1,
                token: discovery_v6_token,
            },
            data_socket_v4: RegisteredSocket {
                socket: data_sockets.0,
                token: data_v4_token,
            },
            data_socket_v6: RegisteredSocket {
                socket: data_sockets.1,
                token: data_v6_token,
            },
            rx_buf: vec![0u8; packet_size].into_boxed_slice(),

            next_beacon_time: Instant::now() + Duration::from_secs(1),
            next_conn_timeout: None,

            timeout: DEFAULT_TIMEOUT,
            id_filter: RobotIdFilter::default(),
        })
    }

    pub fn connected_robots(&self) -> Vec<SocketAddr> {
        self.connection_timeouts.keys().copied().collect()
    }

    pub fn next_timeout(&self) -> Instant {
        self.next_conn_timeout
            .map_or(self.next_beacon_time, |t| t.min(self.next_beacon_time))
    }

    pub fn send_packet(&mut self, addr: SocketAddr, bytes: &[u8]) {
        // Check if the socket address is known
        if !self.connection_timeouts.contains_key(&addr) {
            return;
        }

        let result = if addr.is_ipv4() {
            self.data_socket_v4.socket.send_to(bytes, addr)
        } else {
            self.data_socket_v6.socket.send_to(bytes, addr)
        };

        if let Err(e) = result {
            warn!("Failed to send udp data packet to {addr}: {e}");
        } else {
            trace!("Sent udp data packet to {addr}");
        }
    }

    // ======== Timeout handler ========

    pub fn mio_timeout(&mut self, now: Instant, mut msg_callback: impl FnMut(TransceiverEvent)) {
        // Check if any connection has timed out
        if self.next_conn_timeout.is_some_and(|t| t < now) {
            self.connection_timeouts.retain(|&addr, t| {
                if *t < now {
                    msg_callback(TransceiverEvent::Disconnected(addr.into()));
                    false
                } else {
                    true
                }
            });
            self.next_conn_timeout = self.connection_timeouts.values().min().copied();
        }

        // Send beacon packets
        if now >= self.next_beacon_time {
            self.send_beacon_packets();
            self.next_beacon_time += Duration::from_secs(1);
        }
    }

    fn send_beacon_packets(&self) {
        let interfaces = NetworkInterface::show().expect("Failed to list network interfaces");
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
                if socket2::SockRef::from(&self.discovery_socket_v4.socket)
                    .set_multicast_if_v4(&addr.ip)
                    .is_ok()
                {
                    if let Err(e) = self.discovery_socket_v4.socket.send_to(&[], BEACON_ADDR_V4) {
                        warn!(
                            "Failed to send ipv4 beacon packet to interface {}: {e}",
                            iface.name
                        );
                    }
                }
            } else if iface.addr.iter().any(|a| a.ip().is_ipv6()) {
                if socket2::SockRef::from(&self.discovery_socket_v6.socket)
                    .set_multicast_if_v6(iface.index)
                    .is_ok()
                {
                    if let Err(e) = self.discovery_socket_v6.socket.send_to(&[], BEACON_ADDR_V6) {
                        warn!(
                            "Failed to send ipv6 beacon packet to interface {}: {e}",
                            iface.name
                        );
                    }
                }
            }
        }
    }

    // ======== Readable event handler ========

    pub fn mio_event(
        &mut self,
        event: mio::event::Event,
        msg_callback: impl FnMut(TransceiverEvent),
    ) {
        let token = event.token();

        if token == self.discovery_socket_v4.token {
            self.receive_discovery_packets(false, msg_callback);
        } else if token == self.discovery_socket_v6.token {
            self.receive_discovery_packets(true, msg_callback);
        } else if token == self.data_socket_v4.token {
            self.receive_data_packets(false, msg_callback);
        } else if token == self.data_socket_v6.token {
            self.receive_data_packets(true, msg_callback);
        } else {
            // Ignore unknown tokens, they might be for other transceivers
            return;
        }

        self.next_conn_timeout = self.connection_timeouts.values().min().copied();
    }

    fn receive_discovery_packets(
        &mut self,
        v6: bool,
        mut msg_callback: impl FnMut(TransceiverEvent),
    ) {
        let mut rx_buf = [0u8; 1];
        let socket = if v6 {
            &self.discovery_socket_v6.socket
        } else {
            &self.discovery_socket_v4.socket
        };

        loop {
            match socket.recv_from(&mut rx_buf) {
                Ok((_, src_addr)) => {
                    let robot_id = rx_buf[0];
                    if !self.id_filter.apply(robot_id) {
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
                    if let Entry::Vacant(e) = self.connection_timeouts.entry(data_addr) {
                        e.insert(Instant::now() + self.timeout);
                        msg_callback(TransceiverEvent::Connected(
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

    fn receive_data_packets(&mut self, v6: bool, mut msg_callback: impl FnMut(TransceiverEvent)) {
        let socket = if v6 {
            &self.data_socket_v6.socket
        } else {
            &self.data_socket_v4.socket
        };

        loop {
            match socket.recv_from(&mut self.rx_buf) {
                Ok((_, src_addr)) => {
                    let Some(connection_timeout) = self.connection_timeouts.get_mut(&src_addr)
                    else {
                        continue;
                    };

                    *connection_timeout = Instant::now() + self.timeout;
                    trace!("Received udp packet from {src_addr}");
                    msg_callback(TransceiverEvent::PacketReceived(
                        RobotTransceiverAddress::Udp(src_addr),
                        self.rx_buf.clone(),
                        Instant::now(),
                    ));
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => return,
                Err(e) => error!("Unexpected data socket rx error: {e}"),
            }
        }
    }
}
