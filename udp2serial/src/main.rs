use network_interface::{NetworkInterface, NetworkInterfaceConfig};
use socket2::{Domain, Protocol, Socket, Type};
use std::io::{ErrorKind, Write};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6, UdpSocket};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use std::{io, thread};

const BEACON_ADDR_V4: SocketAddrV4 = SocketAddrV4::new(Ipv4Addr::new(239, 0, 0, 1), 11000);
const BEACON_ADDR_V6: SocketAddrV6 = SocketAddrV6::new(
    Ipv6Addr::from_bits(0xFF15_0000_0000_0045_5246_6F72_6365_0000),
    11000,
    0,
    0,
); // "ERForce" in hex
const DATA_PORT: u16 = 11001;
const PACKET_SIZE: usize = 29;

const BAUD_RATE: u32 = 921600;
const TIMEOUT: Duration = Duration::from_secs(1);

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
    // I just want a somewhat random number without adding extra dependencies
    // TODO: Actually read the id from the robot
    let random_id = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        % 16) as u8;

    let available_ports = serialport::available_ports().expect("Failed to enumerate serial ports");
    println!("Available ports: {:?}", available_ports);
    let selected_port_name = &available_ports
        .first()
        .expect("No serial port found")
        .port_name;
    println!("Selected port: {selected_port_name}");

    let mut read_port = serialport::new(selected_port_name, BAUD_RATE)
        .timeout(Duration::from_millis(100))
        .open()
        .unwrap_or_else(|_| panic!("Failed to open serial port {selected_port_name}"));

    let rx_socket_v4 = bind_ipv4(DATA_PORT).unwrap();
    let rx_socket_v6 = bind_ipv6(DATA_PORT).unwrap();
    let tx_socket_v4 = rx_socket_v4.try_clone().unwrap();
    let tx_socket_v6 = rx_socket_v6.try_clone().unwrap();

    let last_command_addr = Arc::new(RwLock::new(None));
    let last_received_time = Arc::new(RwLock::new(Instant::now()));

    // Udp -> Serial
    let spawn_receive_thread = |socket: UdpSocket| {
        let mut write_port = read_port.try_clone().unwrap();
        let last_command_addr = last_command_addr.clone();
        let last_received_time = last_received_time.clone();
        thread::spawn(move || {
            let mut rx_buf = [0u8; PACKET_SIZE];
            loop {
                let Ok((_, addr)) = socket.recv_from(&mut rx_buf) else {
                    continue;
                };

                if last_received_time.read().unwrap().elapsed() > TIMEOUT {
                    // New connection
                    println!("Receiving packets from {:?}", addr);
                    *last_command_addr.write().unwrap() = Some(addr);
                } else if *last_command_addr.read().unwrap() != Some(addr) {
                    // Different connection active
                    continue;
                }
                *last_received_time.write().unwrap() = Instant::now();

                let mut bytes = rx_buf.to_vec();
                let checksum = bytes.iter().fold(0u8, |sum, &x| sum ^ x);
                bytes.push(checksum);

                let mut encoded_bytes = cobs::encode_vec(&bytes);
                encoded_bytes.push(0);

                write_port.write_all(&encoded_bytes).unwrap();
            }
        })
    };
    spawn_receive_thread(rx_socket_v4);
    spawn_receive_thread(rx_socket_v6);

    // Serial -> Udp
    {
        let last_received_time = last_received_time.clone();
        thread::spawn(move || {
            let mut packet_buffer = Vec::<u8>::new();
            let mut read_byte = [0u8];
            let mut rtt_avg = Duration::from_secs(0);
            let mut last_rtt_print = Instant::now();

            loop {
                // Read raw bytes
                // +1 for checksum, +1 for cobs overhead byte
                while packet_buffer.len() < PACKET_SIZE + 2 {
                    // Read one byte from serial
                    if let Err(e) = read_port.read_exact(&mut read_byte) {
                        match e.kind() {
                            ErrorKind::TimedOut => continue,
                            e => {
                                panic!("{e}")
                            }
                        }
                    }

                    // Handle cobs packet framing byte
                    if read_byte[0] == 0 {
                        packet_buffer.clear();
                    } else {
                        packet_buffer.push(read_byte[0]);
                    }
                }

                rtt_avg = (19 * rtt_avg + last_received_time.read().unwrap().elapsed()) / 20;
                if last_rtt_print.elapsed() > Duration::from_secs(1) {
                    println!("Serial RTT: {}ms", rtt_avg.as_millis());
                    last_rtt_print = Instant::now();
                }

                // Decode raw bytes
                let Ok(decoded) = cobs::decode_vec(&packet_buffer) else {
                    packet_buffer.clear();
                    continue;
                };
                packet_buffer.clear();

                // Check checksum
                let checksum = &decoded[0..decoded.len() - 1]
                    .iter()
                    .fold(0u8, |sum, &x| sum ^ x);
                if decoded.last().unwrap() != checksum {
                    continue;
                }

                if let Some(addr) = *last_command_addr.read().unwrap() {
                    if addr.is_ipv4() {
                        tx_socket_v4.send_to(&decoded, addr).unwrap();
                    } else {
                        tx_socket_v6.send_to(&decoded, addr).unwrap();
                    }
                }
            }
        });
    }

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

        // TODO: Filter network interfaces (exclude loopback?)
        if last_received_time.read().unwrap().elapsed() > TIMEOUT {
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
                        _ = discovery_socket_v4.send_to(&[random_id], src);
                    }
                }
            });
            while Instant::now() < next_if_update {
                if let Ok((_, src)) = discovery_socket_v6.recv_from(&mut []) {
                    _ = discovery_socket_v6.send_to(&[random_id], src);
                }
            }
        });
    }
}
