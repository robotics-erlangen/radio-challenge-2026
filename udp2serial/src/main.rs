use network_interface::{NetworkInterface, NetworkInterfaceConfig};
use serialport::SerialPort;
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

    println!("Querying robot ID...");
    let robot_id = query_robot_id(&mut read_port);
    println!("Robot ID: {robot_id}");

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

                if last_received_time.read().unwrap().elapsed() > CONNECTION_TIMEOUT {
                    // New connection
                    println!("Receiving packets from {:?}", addr);
                    *last_command_addr.write().unwrap() = Some(addr);
                } else if *last_command_addr.read().unwrap() != Some(addr) {
                    // Different connection source active
                    continue;
                }
                *last_received_time.write().unwrap() = Instant::now();

                let mut bytes = Vec::with_capacity(1 + rx_buf.len());
                bytes.push(0); // Message type = packet
                bytes.extend_from_slice(&rx_buf);
                write_serial_packet(&mut write_port, bytes).unwrap();
            }
        })
    };
    spawn_receive_thread(rx_socket_v4);
    spawn_receive_thread(rx_socket_v6);

    // Serial -> Udp
    {
        let last_received_time = last_received_time.clone();
        thread::spawn(move || {
            let mut rtt_avg = Duration::from_secs(0);
            let mut last_rtt_print = Instant::now();
            let mut count = 0;

            loop {
                // No timeout because this simple forwarding thread is independent of the outgoing udp connection
                let decoded = read_serial_packet(&mut read_port, None).unwrap();
                count += 1;

                rtt_avg = (19 * rtt_avg + last_received_time.read().unwrap().elapsed()) / 20;
                if last_rtt_print.elapsed() > Duration::from_secs(1) {
                    println!("Serial RTT: {}us; {count} responses/s", rtt_avg.as_micros());
                    count = 0;
                    last_rtt_print = Instant::now();
                }

                if let Some(addr) = *last_command_addr.read().unwrap() {
                    if addr.is_ipv4() {
                        tx_socket_v4.send_to(&decoded[1..], addr).unwrap();
                    } else {
                        tx_socket_v6.send_to(&decoded[1..], addr).unwrap();
                    }
                }
            }
        });
    }

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

fn query_robot_id(port: &mut Box<dyn SerialPort>) -> u8 {
    loop {
        let mut request_buf = [0u8; 1 + PACKET_SIZE]; // +1 for serial msg type
        request_buf[0] = 1; // Robot ID request message type

        let response = write_serial_packet(port, request_buf.to_vec())
            .and_then(|_| read_serial_packet(port, Some(Duration::from_millis(100))));

        match response {
            Ok(response) if response[0] == 1 => {
                return response[1]; // Return the robot ID (second byte of the response)
            }
            Ok(response) => {
                eprintln!(
                    "Unexpected message type in robot ID response: {:?} (expected 1)",
                    response[0]
                );
            }
            Err(e) => {
                eprintln!("Error querying robot ID: {e}");
            }
        }
        // Sleep and retry if failed
        thread::sleep(Duration::from_secs(1));
    }
}

fn checksum(data: &[u8]) -> u8 {
    data.iter().fold(0u8, |sum, &x| sum ^ x)
}

// Adds a checksum to the data, encodes it using COBS, and writes it to the serial port.
// It does not handle the serial message type, so the expected data layout is [msg_type, packet...]
fn write_serial_packet(port: &mut Box<dyn SerialPort>, mut data: Vec<u8>) -> io::Result<()> {
    data.push(checksum(&data));

    let mut encoded_packet = cobs::encode_vec(&data);
    encoded_packet.push(0); // COBS delimiter byte

    port.write_all(&encoded_packet)
}

// Reads a packet from the serial port, decodes it using COBS, and checks the checksum.
// It returns the decoded packet data without the checksum. The expected output data layout is [msg_type, packet...]
fn read_serial_packet(
    port: &mut Box<dyn SerialPort>,
    timeout: Option<Duration>,
) -> io::Result<Vec<u8>> {
    let mut packet_buffer = Vec::<u8>::new();
    let mut read_byte = [0u8];
    let timeout_instant = timeout.map(|d| Instant::now() + d);

    // Read raw bytes
    // +1 for msg type, +1 for checksum, +1 for cobs overhead byte
    while packet_buffer.len() < PACKET_SIZE + 3 {
        // Check for timeout
        if timeout_instant.is_some_and(|i| i < Instant::now()) {
            return Err(io::Error::new(
                ErrorKind::TimedOut,
                "Timed out while waiting for serial packet",
            ));
        }

        // Read one byte from serial
        if let Err(e) = port.read_exact(&mut read_byte) {
            if e.kind() == ErrorKind::TimedOut {
                // No data available right now, retry
                continue;
            } else {
                return Err(e);
            }
        }

        // Handle cobs packet framing byte
        if read_byte[0] == 0 {
            packet_buffer.clear();
        } else {
            packet_buffer.push(read_byte[0]);
        }
    }

    // Decode raw bytes
    let mut decoded = match cobs::decode_vec(&packet_buffer) {
        Ok(decoded) => decoded,
        Err(e) => {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                format!("Failed to decode COBS packet: {e}"),
            ));
        }
    };

    // Check checksum
    let calc_checksum = checksum(&decoded[0..decoded.len() - 1]);
    if *decoded.last().unwrap() != calc_checksum {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "Checksum mismatch in serial packet",
        ));
    }
    decoded.truncate(decoded.len() - 1); // Remove checksum byte

    Ok(decoded)
}
