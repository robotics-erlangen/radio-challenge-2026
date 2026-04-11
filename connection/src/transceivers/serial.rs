use crate::driver::TokenAllocator;
use crate::dual_map::DualHashMap;
use crate::transceivers::{IoToTransceiverError, Transceiver, TransceiverError, TransceiverEvent};
use crate::{DEFAULT_TIMEOUT, RobotIdFilter, RobotTransceiverAddress};
use log::trace;
use mio::event::Event;
use mio::{Interest, Poll};
pub use mio_serial::SerialPortInfo;
use mio_serial::{SerialPortType, SerialStream};
use std::io;
use std::io::{ErrorKind, Read, Write};
use std::time::{Duration, Instant};

const BAUD_RATE: u32 = 921600;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct SerialTransceiverConfig {
    pub probe_period: Duration,
}

#[derive(Debug)]
pub struct SerialTransceiver {
    active_connections: DualHashMap<mio::Token, String, SerialConnectionState>,
    active_discovery_ports: Option<Vec<(mio::Token, SerialPortInfo, SerialStream)>>,

    // Timeouts
    next_discovery_time: Instant,
    next_conn_timeout: Option<Instant>, //TODO: Somehow enforce updating this cache with active_connections.values().map(|s| s.timeout).min()

    // Config. Public because it could be set directly, but usually the Transceiver trait functions are used instead.
    pub id_filter: RobotIdFilter,
    pub timeout: Duration,
    config: SerialTransceiverConfig,
    packet_size: usize,
}

impl Transceiver for SerialTransceiver {
    fn set_id_filter(&mut self, id_filter: RobotIdFilter) {
        self.id_filter = id_filter;
    }

    fn next_timeout(&self) -> Instant {
        self.next_conn_timeout
            .map(|d| d.min(self.next_discovery_time))
            .unwrap_or(self.next_discovery_time)
    }

    fn send_packet(
        &mut self,
        addr: &RobotTransceiverAddress,
        packet: &[u8],
    ) -> Result<(), TransceiverError> {
        let RobotTransceiverAddress::Serial(port_info) = addr else {
            return Ok(()); // Skipping other addresses is expected
        };
        let port_name = &port_info.port_name;

        // Check if the connection is active
        if let Some((_token, state)) = self.active_connections.get_sec_mut(port_name) {
            // Calculate checksum
            let checksum = packet.iter().fold(0u8, |sum, &x| sum ^ x);
            let mut packet_bytes = packet.to_vec();
            packet_bytes.push(checksum);

            // Encode the packet
            let mut encoded_bytes = cobs::encode_vec(&packet_bytes);
            encoded_bytes.push(0);

            // Write the encoded packet
            match state.port.write_all(&encoded_bytes) {
                Ok(_) => trace!("Sent serial data packet to {port_name}"),
                Err(e) => {
                    return Err(e.to_error(format!(
                        "Failed to send serial data packet to {}",
                        port_info.port_name
                    )));
                }
            }
        }

        Ok(())
    }

    fn mio_timeout(
        &mut self,
        now: Instant,
        poll: &mut Poll,
        token_allocator: &mut TokenAllocator,
        events_out: &mut Vec<TransceiverEvent>,
    ) {
        // Handle discovery
        if self.next_discovery_time < now {
            if let Some(ports) = self.active_discovery_ports.take() {
                self.end_discovery(poll, ports, events_out);
                self.next_discovery_time += self
                    .config
                    .probe_period
                    .saturating_sub(Duration::from_millis(500));
            } else {
                self.active_discovery_ports =
                    Some(self.start_discovery(poll, token_allocator, events_out));
                self.next_discovery_time += Duration::from_millis(500);
            }
        }

        // Check if any connection has timed out
        if self.next_conn_timeout.is_some_and(|t| t < now) {
            self.active_connections.retain(|_, _, state| {
                if state.timeout < now {
                    events_out.push(TransceiverEvent::Disconnected(
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

    fn mio_event(
        &mut self,
        event: Event,
        _poll: &mut Poll,
        _token_allocator: &mut TokenAllocator,
        events_out: &mut Vec<TransceiverEvent>,
    ) {
        // Filter out discovery tokens, they will be handled separately
        if let Some((_port_name, conn)) = self.active_connections.get_prim_mut(&event.token()) {
            conn.receive_serial_packets(self.timeout, events_out);
            if self.next_conn_timeout.is_some_and(|old| conn.timeout < old) {
                self.next_conn_timeout = Some(conn.timeout);
            }
        }
    }
}

impl SerialTransceiver {
    pub fn start(packet_size: usize, config: SerialTransceiverConfig) -> io::Result<Self> {
        Ok(Self {
            active_connections: DualHashMap::new(),
            active_discovery_ports: None,
            next_discovery_time: Instant::now(),
            next_conn_timeout: None,
            id_filter: RobotIdFilter::default(),
            timeout: DEFAULT_TIMEOUT,
            config,
            packet_size,
        })
    }

    // ======== Timeout handling ========

    fn start_discovery(
        &self,
        poll: &Poll,
        token_allocator: &mut TokenAllocator,
        events_out: &mut Vec<TransceiverEvent>,
    ) -> Vec<(mio::Token, SerialPortInfo, SerialStream)> {
        let new_ports = mio_serial::available_ports()
            .expect("Failed to list serial ports") // TODO: Gracefully handle serial port list errors
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
            .filter(|port_info| !self.active_connections.contains_sec(&port_info.port_name))
            // Open the ports
            .filter_map(|port_info| {
                match SerialStream::open(&mio_serial::new(&port_info.port_name, BAUD_RATE)) {
                    Ok(s) => Some((port_info, s)),
                    Err(e) => {
                        events_out.push(io::Error::from(e).to_event(format!(
                            "Failed to open serial port {}",
                            port_info.port_name
                        )));
                        None
                    }
                }
            })
            .collect::<Vec<_>>();

        let mut discovery_ports = Vec::new();
        for (port_info, mut port) in new_ports {
            // Send init message
            if let Err(e) = port.write_all("start-connection\r".as_bytes()) {
                events_out.push(e.to_event(format!(
                    "Failed to send start-connection command to {}",
                    port_info.port_name
                )));
                continue;
            }
            trace!("Sent start-connection command to {}", port_info.port_name);

            // Register the port with mio and remember it for discovery
            let token = token_allocator.new_token();
            match poll
                .registry()
                .register(&mut port, token, Interest::READABLE)
            {
                Ok(_) => discovery_ports.push((token, port_info, port)),
                Err(e) => events_out.push(e.to_event(format!(
                    "Failed to register serial port {} with mio",
                    port_info.port_name
                ))),
            }
        }
        discovery_ports
    }

    fn end_discovery(
        &mut self,
        poll: &Poll,
        discovery_ports: Vec<(mio::Token, SerialPortInfo, SerialStream)>,
        events_out: &mut Vec<TransceiverEvent>,
    ) {
        'ports: for (token, port_info, mut port) in discovery_ports {
            // Read response
            let mut buf = Vec::new();
            let mut chunk = [0u8; 10];
            loop {
                // Read a single data chunk and append it to the buffer. The chunking is necessary because the read call requires a fixed-size buffer.
                match port.read(&mut chunk) {
                    Ok(read_bytes) if read_bytes > 0 => {
                        buf.extend_from_slice(&chunk[..read_bytes]);
                    }
                    Ok(_) /*if read_bytes == 0*/ => break,
                    Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                    Err(e) => {
                        events_out.push(e.to_event(format!(
                            "Unexpected serial read error on port {}",
                            port_info.port_name
                        )));
                        continue 'ports;
                    }
                }
            }
            let Ok(string) = String::from_utf8(buf) else {
                continue;
            };

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
                    timeout: Instant::now() + self.timeout,
                };
                self.next_conn_timeout = Some(
                    self.next_conn_timeout
                        .map(|t| t.min(state.timeout))
                        .unwrap_or(state.timeout),
                );
                self.active_connections
                    .insert(token, port_info.port_name.clone(), state);
                events_out.push(TransceiverEvent::Connected(
                    RobotTransceiverAddress::Serial(port_info),
                    robot_id,
                ));
            } else if let Err(e) = poll.registry().deregister(&mut port) {
                events_out.push(e.to_event(format!(
                    "Failed to deregister serial port {} after being discarded during discovery",
                    port_info.port_name
                )));
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
        events_out: &mut Vec<TransceiverEvent>,
    ) {
        loop {
            match self.port.read(&mut self.rx_buf[self.rx_buf_pos..]) {
                Ok(read_bytes) => {
                    // Handle read
                    let old_pos = self.rx_buf_pos;
                    self.rx_buf_pos += read_bytes;

                    // Reset on null byte (cobs packet framing)
                    let zero_idx = self.rx_buf[old_pos..self.rx_buf_pos]
                        .iter()
                        .position(|&x| x == 0);
                    if let Some(rel_idx) = zero_idx {
                        let idx = old_pos + rel_idx;
                        self.rx_buf.copy_within(idx..self.rx_buf_pos, 0);
                        self.rx_buf_pos -= idx;
                        continue;
                    }

                    // Check if the packet is completed
                    if self.rx_buf_pos < self.rx_buf.len() {
                        continue;
                    }
                    self.rx_buf_pos = 0;

                    // Decode cobs framing
                    let mut decoded = vec![0u8; self.rx_buf.len() - 1].into_boxed_slice(); // -1 for the removed cobs overhead byte. TODO: Replace with a statically sized array when feature(generic_const_exprs) lands
                    if cobs::decode(&self.rx_buf, &mut decoded).is_err() {
                        continue;
                    };

                    // Verify checksum
                    let checksum = &decoded[0..decoded.len() - 1]
                        .iter()
                        .fold(0u8, |sum, &x| sum ^ x);
                    if decoded.last().unwrap() != checksum {
                        continue;
                    }

                    self.timeout = Instant::now() + configured_timeout;
                    events_out.push(TransceiverEvent::PacketReceived(
                        RobotTransceiverAddress::Serial(self.port_info.clone()),
                        (&decoded[..decoded.len() - 1]).into(),
                        Instant::now(),
                    ));
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => return,
                Err(e) => {
                    events_out.push(e.to_event(format!(
                        "Unexpected serial read error on port {}",
                        self.port_info.port_name
                    )));
                    return;
                }
            }
        }
    }
}
