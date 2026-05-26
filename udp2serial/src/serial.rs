use serialport::SerialPort;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use std::{io, thread};

use crate::{CONNECTION_TIMEOUT, DATA_PORT, PACKET_SIZE};

const BAUD_RATE: u32 = 921600;

pub fn start_udp_serial_bridge(last_received_time: Arc<RwLock<Instant>>) -> u8 {
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

    let rx_socket = crate::bind_dual_stack(DATA_PORT).unwrap();
    let tx_socket = rx_socket.try_clone().unwrap();

    let last_command_addr = Arc::new(RwLock::new(None));

    // Udp -> Serial
    {
        let mut write_port = read_port.try_clone().unwrap();
        let last_command_addr = last_command_addr.clone();
        let last_received_time = last_received_time.clone();
        thread::spawn(move || {
            let mut rx_buf = [0u8; PACKET_SIZE];
            loop {
                let Ok((_, addr)) = rx_socket.recv_from(&mut rx_buf) else {
                    continue;
                };

                if last_received_time.read().unwrap().elapsed() > CONNECTION_TIMEOUT {
                    // New connection
                    println!(
                        "Receiving packets from {}:{}",
                        addr.ip().to_canonical(),
                        addr.port()
                    );
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
        });
    }

    // Serial -> Udp
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
                tx_socket.send_to(&decoded[1..], addr).unwrap();
            }
        }
    });

    robot_id
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
                io::ErrorKind::TimedOut,
                "Timed out while waiting for serial packet",
            ));
        }

        // Read one byte from serial
        if let Err(e) = port.read_exact(&mut read_byte) {
            if e.kind() == io::ErrorKind::TimedOut {
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
                io::ErrorKind::InvalidData,
                format!("Failed to decode COBS packet: {e}"),
            ));
        }
    };

    // Check checksum
    let calc_checksum = checksum(&decoded[0..decoded.len() - 1]);
    if *decoded.last().unwrap() != calc_checksum {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Checksum mismatch in serial packet",
        ));
    }
    decoded.truncate(decoded.len() - 1); // Remove checksum byte

    Ok(decoded)
}
