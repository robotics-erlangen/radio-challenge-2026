use crate::driver::TokenAllocator;
use crate::dual_map::DualHashMap;
use crate::{DEFAULT_TIMEOUT, RobotIdFilter, RobotTransceiverAddress, TransceiverMessage};
use log::{error, trace, warn};
use mio::Interest;
pub use mio_serial::SerialPortInfo;
use mio_serial::{SerialPortType, SerialStream};
use std::io;
use std::io::{ErrorKind, Read, Write};
use std::time::{Duration, Instant};

const DEFAULT_PROBE_PERIOD: Duration = Duration::from_millis(2000);
const BAUD_RATE: u32 = 921600;

#[derive(Debug)]
pub struct SerialTransceiver {
    active_connections: DualHashMap<mio::Token, String, SerialConnectionState>,
    active_discovery_ports: Option<Vec<(mio::Token, SerialPortInfo, SerialStream)>>,

    // Timeouts
    next_discovery_time: Instant,
    next_conn_timeout: Option<Instant>, //TODO: Somehow enforce updating this cache with active_connections.values().map(|s| s.timeout).min()

    // Config
    pub probe_period: Duration,
    pub id_filter: RobotIdFilter,
    pub timeout: Duration,
    packet_size: usize,
}

impl SerialTransceiver {
    pub fn start(
        _poll: &mut mio::Poll,
        _token_allocator: &mut TokenAllocator,
        packet_size: usize,
    ) -> io::Result<Self> {
        Ok(Self {
            active_connections: DualHashMap::new(),
            active_discovery_ports: None,
            next_discovery_time: Instant::now(),
            next_conn_timeout: None,
            probe_period: DEFAULT_PROBE_PERIOD,
            id_filter: RobotIdFilter::default(),
            timeout: DEFAULT_TIMEOUT,
            packet_size,
        })
    }

    pub fn next_timeout(&self) -> Instant {
        self.next_conn_timeout
            .map(|d| d.min(self.next_discovery_time))
            .unwrap_or(self.next_discovery_time)
    }

    pub fn send_packet(&mut self, port_name: String, packet: &[u8]) {
        // Check if the connection is active
        if let Some((_token, state)) = self.active_connections.get_sec_mut(&port_name) {
            // Encode the packet
            let checksum = packet.iter().fold(0u8, |sum, &x| sum ^ x);
            let mut packet_bytes = packet.to_vec();
            packet_bytes.push(checksum);

            let mut encoded_bytes = cobs::encode_vec(&packet_bytes);
            encoded_bytes.push(0);

            // Write the encoded packet
            if let Err(e) = state.port.write_all(&encoded_bytes) {
                warn!("Failed to send serial packet to {port_name}: {e}");
            } else {
                trace!("Sent serial packet to {port_name}");
            }
        };
    }

    // ======== Timeout handler ========

    pub fn mio_timeout(
        &mut self,
        now: Instant,
        mut msg_callback: impl FnMut(TransceiverMessage),
        poll: &mut mio::Poll,
        token_allocator: &mut TokenAllocator,
    ) {
        // Handle discovery
        if self.next_discovery_time < now {
            if let Some(ports) = self.active_discovery_ports.take() {
                self.end_discovery(poll, ports, &mut msg_callback);
                self.next_discovery_time += self.probe_period - Duration::from_millis(500);
            } else {
                self.active_discovery_ports = Some(self.start_discovery(poll, token_allocator));
                self.next_discovery_time += Duration::from_millis(500);
            }
        }

        // Check if any connection has timed out
        if self.next_conn_timeout.is_some_and(|t| t < now) {
            self.active_connections.retain(|_, _, state| {
                if state.timeout < now {
                    msg_callback(TransceiverMessage::Disconnected(
                        state.port_info.clone().into(),
                    ));
                    false
                } else {
                    true
                }
            });
            self.next_conn_timeout = self.active_connections.values().map(|s| s.timeout).min();
        }
    }

    fn start_discovery(
        &self,
        poll: &mio::Poll,
        token_allocator: &mut TokenAllocator,
    ) -> Vec<(mio::Token, SerialPortInfo, SerialStream)> {
        let new_ports = mio_serial::available_ports()
            .unwrap()
            .into_iter()
            // Filter by port info
            .filter(|p| match p.port_type.clone() {
                SerialPortType::UsbPort(details) => details.product.is_some_and(|p| {
                    p.to_ascii_lowercase().contains("uart")
                        || p.to_ascii_lowercase().contains("serial")
                }),
                SerialPortType::PciPort => true,
                _ => false,
            })
            // Filter for new ports
            .filter(|p| !self.active_connections.contains_sec(&p.port_name))
            // Open the ports
            .filter_map(|p| {
                SerialStream::open(&mio_serial::new(&p.port_name, BAUD_RATE))
                    .inspect_err(|e| warn!("Failed to open serial port {}: {}", p.port_name, e))
                    .ok()
                    .map(|a| (p, a))
            })
            .collect::<Vec<_>>();

        let mut discovery_ports = Vec::new();
        for (info, mut port) in new_ports {
            // Send init message
            if let Err(e) = port.write_all("start-connection\r".as_bytes()) {
                warn!(
                    "Failed to send start-connection command to {}: {e}",
                    info.port_name
                );
                continue;
            }
            println!("Sent start-connection command to {}", info.port_name);

            // Register the port with mio and remember it for discovery
            let token = token_allocator.new_token();
            if poll
                .registry()
                .register(&mut port, token, Interest::READABLE)
                .is_ok()
            {
                discovery_ports.push((token, info, port));
            }
        }
        discovery_ports
    }

    fn end_discovery(
        &mut self,
        poll: &mio::Poll,
        discovery_ports: Vec<(mio::Token, SerialPortInfo, SerialStream)>,
        msg_callback: &mut impl FnMut(TransceiverMessage),
    ) {
        for (token, port_info, mut port) in discovery_ports {
            // Read response
            let mut buf = Vec::new();
            let mut chunk = [0u8; 10];
            loop {
                match port.read(&mut chunk) {
                    Ok(read_bytes) => {
                        buf.extend_from_slice(&chunk[..read_bytes]);
                        continue;
                    }
                    Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                    Err(e) => {
                        error!(
                            "Unexpected serial read error on port {}: {e}",
                            port_info.port_name
                        )
                    }
                }
            }
            let string = String::from_utf8(buf).unwrap();

            // Parse response
            let mut robot_id = None;
            for line in string.lines() {
                if let Some(("robot_id", value)) = line.split_once(' ') {
                    robot_id = value.parse::<u8>().ok();
                };
            }

            // Accept or reject the connection
            if let Some(robot_id) = robot_id
                && self.id_filter.apply(robot_id)
            {
                let state = SerialConnectionState {
                    port,
                    port_info: port_info.clone(),
                    rx_buf: vec![0; self.packet_size + 2].into_boxed_slice(), // +1 for checksum, +1 for cobs overhead byte. TODO: Replace with a statically sized array when feature(generic_const_exprs) lands
                    rx_buf_pos: 0,
                    timeout: Instant::now() + self.timeout, // TODO: Maybe separate this? This timeout is for the serial console, not for the normal connection
                };
                self.active_connections
                    .insert(token, port_info.port_name.clone(), state);
                msg_callback(TransceiverMessage::Connected(
                    RobotTransceiverAddress::Serial(port_info),
                    robot_id,
                ));
            } else {
                poll.registry().deregister(&mut port).unwrap();
            }
        }
    }

    // ======== Readable event handler ========

    pub fn mio_event(
        &mut self,
        event: mio::event::Event,
        msg_callback: impl FnMut(TransceiverMessage),
    ) {
        // Filter out discovery tokens, they will be handled separately
        if let Some((_port_name, conn)) = self.active_connections.get_prim_mut(&event.token()) {
            conn.receive_serial_packets(self.timeout, msg_callback);
            if self.next_conn_timeout.is_some_and(|old| conn.timeout < old) {
                self.next_conn_timeout = Some(conn.timeout);
            }
        }
    }
}

// TODO: Make rx_buf a statically sized array when feature(generic_const_exprs) lands
#[derive(Debug)]
struct SerialConnectionState {
    port: SerialStream,
    port_info: SerialPortInfo,
    rx_buf: Box<[u8]>,
    rx_buf_pos: usize,
    timeout: Instant,
}

impl SerialConnectionState {
    fn receive_serial_packets(
        &mut self,
        configured_timeout: Duration,
        mut msg_callback: impl FnMut(TransceiverMessage),
    ) {
        loop {
            match self.port.read(&mut self.rx_buf[self.rx_buf_pos..]) {
                Ok(read_bytes) => {
                    // Handle read
                    let old_pos = self.rx_buf_pos;
                    let new_pos = self.rx_buf_pos + read_bytes;

                    // Reset on null byte (cobs packet framing)
                    let zero_idx = self.rx_buf[old_pos..new_pos].iter().position(|&x| x == 0);
                    if let Some(rel_idx) = zero_idx {
                        let idx = old_pos + rel_idx;
                        self.rx_buf.copy_within(idx..new_pos, 0);
                        self.rx_buf_pos = new_pos - idx;
                        continue;
                    }

                    // Check if the packet is completed
                    if self.rx_buf_pos < self.rx_buf.len() {
                        continue;
                    }

                    // Decode cobs framing
                    let mut decoded = vec![0u8; self.rx_buf.len() - 1].into_boxed_slice(); // -1 for the removed cobs overhead byte. TODO: Replace with a statically sized array when feature(generic_const_exprs) lands
                    if cobs::decode(&self.rx_buf, &mut decoded).is_err() {
                        self.rx_buf_pos = 0;
                        continue;
                    };
                    self.rx_buf_pos = 0;

                    // Verify checksum
                    let checksum = &decoded[0..decoded.len() - 1]
                        .iter()
                        .fold(0u8, |sum, &x| sum ^ x);
                    if decoded.last().unwrap() != checksum {
                        self.rx_buf_pos = 0;
                        continue;
                    }

                    self.timeout = Instant::now() + configured_timeout;
                    msg_callback(TransceiverMessage::PacketReceived(
                        RobotTransceiverAddress::Serial(self.port_info.clone()),
                        (&decoded[..decoded.len() - 1]).into(),
                        Instant::now(),
                    ))
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => return,
                Err(e) => {
                    error!(
                        "Unexpected serial read error on port {}: {e}",
                        self.port_info.port_name
                    );
                    return;
                }
            }
        }
    }
}
