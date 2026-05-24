mod serial;

use network_interface::{NetworkInterface, NetworkInterfaceConfig};
use socket2::{Domain, Protocol, Socket, Type};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6, UdpSocket};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use std::{io, thread};

use crate::serial::start_udp_serial_bridge;

const BEACON_ADDR_V4: SocketAddrV4 = SocketAddrV4::new(Ipv4Addr::new(239, 0, 0, 1), 11000);
const BEACON_ADDR_V6: SocketAddrV6 = SocketAddrV6::new(
    Ipv6Addr::from_bits(0xFF15_0000_0000_0045_5246_6F72_6365_0000),
    11000,
    0,
    0,
); // "ERForce" in hex
const DATA_PORT: u16 = 11001;
const PACKET_SIZE: usize = 29;

const CONNECTION_TIMEOUT: Duration = Duration::from_secs(1);

fn bind_ipv4(port: u16) -> io::Result<UdpSocket> {
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    socket.bind(&SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port).into())?;
    Ok(socket.into())
}
fn bind_ipv6(port: u16) -> io::Result<UdpSocket> {
    let socket = Socket::new(Domain::IPV6, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_only_v6(true)?; // By default, linux binds ipv6 sockets as dual-stack
    socket.bind(&SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, port, 0, 0).into())?;
    Ok(socket.into())
}

fn main() {
    let last_received_time = Arc::new(RwLock::new(Instant::now()));
    let robot_id = start_udp_serial_bridge(last_received_time.clone());

    // ======== Discovery loop ========

    let discovery_socket_v4 = bind_ipv4(BEACON_ADDR_V4.port()).unwrap();
    discovery_socket_v4
        .set_read_timeout(Some(Duration::from_millis(500)))
        .unwrap();
    let discovery_socket_v6 = bind_ipv6(BEACON_ADDR_V6.port()).unwrap();
    discovery_socket_v6
        .set_read_timeout(Some(Duration::from_millis(500)))
        .unwrap();

    let mut discovery_connected = true;
    let mut next_if_update = Instant::now();
    loop {
        next_if_update += Duration::from_secs(5);

        if last_received_time.read().unwrap().elapsed() > CONNECTION_TIMEOUT {
            // Continuously join beacon multicast groups while not connected to include new interfaces
            for iface in NetworkInterface::show().unwrap() {
                if let Some(network_interface::Addr::V4(addr)) =
                    iface.addr.iter().find(|a| a.ip().is_ipv4())
                {
                    _ = discovery_socket_v4.join_multicast_v4(BEACON_ADDR_V4.ip(), &addr.ip);
                } else if iface.addr.iter().any(|a| a.ip().is_ipv6()) {
                    _ = discovery_socket_v6.join_multicast_v6(BEACON_ADDR_V6.ip(), iface.index);
                }
            }
            discovery_connected = true;
        } else if discovery_connected {
            // Leave beacon multicast groups while actively connected to reduce network load
            for iface in NetworkInterface::show().unwrap() {
                if let Some(network_interface::Addr::V4(addr)) =
                    iface.addr.iter().find(|a| a.ip().is_ipv4())
                {
                    _ = discovery_socket_v4.leave_multicast_v4(BEACON_ADDR_V4.ip(), &addr.ip);
                } else if iface.addr.iter().any(|a| a.ip().is_ipv6()) {
                    _ = discovery_socket_v6.leave_multicast_v6(BEACON_ADDR_V6.ip(), iface.index);
                }
            }
            discovery_connected = false;
        }

        // Respond to discovery packets
        // TODO: Log send errors
        thread::scope(|s| {
            s.spawn(|| {
                while Instant::now() < next_if_update {
                    if let Ok((_, src)) = discovery_socket_v4.recv_from(&mut []) {
                        _ = discovery_socket_v4.send_to(&[robot_id], src);
                    }
                }
            });
            while Instant::now() < next_if_update {
                if let Ok((_, src)) = discovery_socket_v6.recv_from(&mut []) {
                    _ = discovery_socket_v6.send_to(&[robot_id], src);
                }
            }
        });
    }
}
