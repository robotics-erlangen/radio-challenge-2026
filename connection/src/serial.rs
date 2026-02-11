use crate::packet::PACKET_SIZE;
use crate::{DEFAULT_TIMEOUT, RobotTransceiverAddress, TransceiverMessage};
use flume::{Receiver, Sender};
use log::{error, trace, warn};
use mio::Interest;
pub use mio_serial::SerialPortInfo;
use mio_serial::{SerialPortType, SerialStream};
use std::collections::HashMap;
use std::io::{ErrorKind, Read, Write};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use std::{io, thread};

const DEFAULT_PROBE_PERIOD: Duration = Duration::from_millis(2000);
const BAUD_RATE: u32 = 921600;
// +1 for checksum, +1 for cobs overhead byte
const SERIAL_PACKET_SIZE: usize = PACKET_SIZE + 2;

const WAKER_TOKEN: mio::Token = mio::Token(0);

#[derive(Debug)]
pub struct SerialTransceiver {
    thread: Option<JoinHandle<()>>,
    thread_control_channel: Sender<SerialControlMessage>,
    thread_control_waker: mio::Waker,

    probe_period: Duration,
    timeout: Duration,
}

impl SerialTransceiver {
    pub fn start(msg_callback: Arc<dyn Fn(TransceiverMessage) + Send + Sync>) -> io::Result<Self> {
        let poll = mio::Poll::new()?;
        let waker = mio::Waker::new(poll.registry(), WAKER_TOKEN)?;

        let (thread_control_sender, thread_control_receiver) = flume::unbounded();
        let thread =
            thread::spawn(move || serial_mio_thread(poll, thread_control_receiver, msg_callback));

        Ok(Self {
            thread: Some(thread),
            thread_control_channel: thread_control_sender,
            thread_control_waker: waker,
            probe_period: DEFAULT_PROBE_PERIOD,
            timeout: DEFAULT_TIMEOUT,
        })
    }

    pub fn set_probe_period(&mut self, period: Duration) {
        self.probe_period = period;
        self.thread_control_channel
            .send(SerialControlMessage::SetProbePeriod(period))
            .unwrap();
        self.thread_control_waker.wake().unwrap();
    }
    pub fn probe_period(&self) -> Duration {
        self.probe_period
    }

    pub fn set_timeout(&mut self, timeout: Duration) {
        self.timeout = timeout;
        self.thread_control_channel
            .send(SerialControlMessage::SetTimeout(timeout))
            .unwrap();
        self.thread_control_waker.wake().unwrap();
    }
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    fn encode_packet(packet: &[u8]) -> Vec<u8> {
        let mut bytes = packet.to_vec();

        let checksum = bytes.iter().fold(0u8, |sum, &x| sum ^ x);
        bytes.push(checksum);

        let mut encoded_bytes = cobs::encode_vec(&bytes);
        encoded_bytes.push(0);
        encoded_bytes
    }

    pub fn send(&self, port_name: String, packet: &[u8]) {
        self.thread_control_channel
            .send(SerialControlMessage::Write(
                port_name,
                Self::encode_packet(packet),
            ))
            .unwrap();
        self.thread_control_waker.wake().unwrap();
    }

    /// More efficient version of `send` that only wakes the io thread once
    pub fn send_batch<'a>(&self, packets: impl IntoIterator<Item = (String, &'a [u8])>) {
        for (port_name, packet) in packets {
            self.thread_control_channel
                .send(SerialControlMessage::Write(
                    port_name,
                    Self::encode_packet(packet),
                ))
                .unwrap();
        }
        self.thread_control_waker.wake().unwrap();
    }
}

impl Drop for SerialTransceiver {
    fn drop(&mut self) {
        _ = self.thread_control_channel.send(SerialControlMessage::Stop);
        _ = self.thread_control_waker.wake();
        // Immediately dropping the waker can cause the event to be lost
        _ = self.thread.take().unwrap().join();
    }
}

enum SerialControlMessage {
    Write(String, Vec<u8>),
    SetProbePeriod(Duration),
    SetTimeout(Duration),
    Stop,
}

#[derive(Debug)]
struct SerialConnectionState {
    port: SerialStream,
    port_info: SerialPortInfo,
    // TODO: Support receiving variable sized packets
    rx_buf: [u8; PACKET_SIZE],
    rx_buf_pos: usize,
    timeout: Instant,
}

enum SerialDiscoveryStage {
    Waiting(Instant),
    Collecting(Instant, HashMap<mio::Token, (SerialPortInfo, SerialStream)>),
}

fn serial_mio_thread(
    mut poll: mio::Poll,
    control_channel: Receiver<SerialControlMessage>,
    msg_callback: Arc<dyn Fn(TransceiverMessage) + Send + Sync>,
) {
    let mut active_connections: HashMap<mio::Token, SerialConnectionState> = HashMap::new();
    let mut port_names: HashMap<String, mio::Token> = HashMap::new();
    let mut discovery_stage = SerialDiscoveryStage::Waiting(Instant::now());
    let mut next_token_num: usize = 1;

    let mut configured_probe_period = DEFAULT_PROBE_PERIOD;
    let mut configured_timeout = DEFAULT_TIMEOUT;

    let mut events = mio::Events::with_capacity(64);

    // Listen to io events and periodically probe for new connections
    loop {
        let next_discovery_deadline = match discovery_stage {
            SerialDiscoveryStage::Waiting(i) => i,
            SerialDiscoveryStage::Collecting(i, _) => i,
        };
        let next_conn_deadline = active_connections.values().map(|s| s.timeout).min();
        let timeout = next_conn_deadline
            .map(|d| d.min(next_discovery_deadline))
            .unwrap_or(next_discovery_deadline)
            .saturating_duration_since(Instant::now());

        if let Err(err) = poll.poll(&mut events, Some(timeout)) {
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

            // Handle discovery
            if next_discovery_deadline < now {
                discovery_stage = match discovery_stage {
                    SerialDiscoveryStage::Waiting(time) => start_discovery(
                        &poll,
                        &active_connections,
                        &mut next_token_num,
                        time + Duration::from_millis(500),
                    ),
                    SerialDiscoveryStage::Collecting(time, ports) => end_discovery(
                        &poll,
                        ports,
                        &mut active_connections,
                        &mut port_names,
                        msg_callback.clone(),
                        configured_timeout,
                        time + configured_probe_period - Duration::from_millis(500),
                    ),
                }
            }

            // Check if any connection has timed out
            if next_conn_deadline.is_some_and(|t| t < now) {
                active_connections.retain(|_, state| {
                    if state.timeout < now {
                        port_names.remove(state.port_info.port_name.as_str());
                        msg_callback(TransceiverMessage::Disconnected(
                            state.port_info.clone().into(),
                        ));
                        false
                    } else {
                        true
                    }
                });
            }

            continue;
        }

        for event in events.iter() {
            match event.token() {
                WAKER_TOKEN => {
                    while let Ok(msg) = control_channel.try_recv() {
                        match msg {
                            SerialControlMessage::Write(port_name, bytes) => {
                                // Check if the connection is active and lock access to the port
                                let Some(state) = port_names
                                    .get(&port_name)
                                    .and_then(|token| active_connections.get_mut(token))
                                else {
                                    continue;
                                };

                                // TODO: Handle would_block errors per port and resume after writeable event
                                if let Err(e) = state.port.write_all(&bytes) {
                                    warn!("Failed to send serial packet to {port_name}: {e}");
                                } else {
                                    trace!("Sent serial packet to {port_name}");
                                }
                            }
                            SerialControlMessage::SetProbePeriod(val) => {
                                configured_probe_period = val
                            }
                            SerialControlMessage::SetTimeout(val) => {
                                configured_timeout = val;
                            }
                            SerialControlMessage::Stop => return,
                        }
                    }
                }
                serial_token => {
                    // Filter out discovery tokens, they will be handled separately
                    if let Some(conn) = active_connections.get_mut(&serial_token) {
                        receive_serial_packets(conn, configured_timeout, msg_callback.clone())
                    }
                }
            }
        }
    }
}

fn start_discovery(
    poll: &mio::Poll,
    active_connections: &HashMap<mio::Token, SerialConnectionState>,
    next_token_num: &mut usize,
    end_time: Instant,
) -> SerialDiscoveryStage {
    let new_ports = mio_serial::available_ports()
        .unwrap()
        .into_iter()
        // Filter by port info
        .filter(|p| match p.port_type.clone() {
            SerialPortType::UsbPort(details) => details.product.is_some_and(|p| {
                p.to_ascii_lowercase().contains("uart") || p.to_ascii_lowercase().contains("serial")
            }),
            SerialPortType::PciPort => true,
            _ => false,
        })
        // Filter for new ports
        .filter(|p| {
            !active_connections
                .iter()
                .any(|(_, state)| state.port_info.port_name == p.port_name)
        })
        // Open the ports
        .filter_map(|p| {
            SerialStream::open(&mio_serial::new(&p.port_name, BAUD_RATE))
                .inspect_err(|e| warn!("Failed to open serial port {}: {}", p.port_name, e))
                .ok()
                .map(|a| (p, a))
        })
        .collect::<Vec<_>>();

    let mut discovery_ports = HashMap::new();
    for (info, mut port) in new_ports {
        if let Err(e) = port.write_all("start-connection\r".as_bytes()) {
            warn!(
                "Failed to send start-connection command to {}: {e}",
                info.port_name
            );
            continue;
        }
        println!("Sent start-connection command to {}", info.port_name);

        let token = mio::Token(*next_token_num);
        if poll
            .registry()
            .register(&mut port, token, Interest::READABLE)
            .is_ok()
        {
            *next_token_num += 1;
            discovery_ports.insert(token, (info, port));
        }
    }
    SerialDiscoveryStage::Collecting(end_time, discovery_ports)
}

fn end_discovery(
    poll: &mio::Poll,
    discovery_ports: HashMap<mio::Token, (SerialPortInfo, SerialStream)>,
    active_connections: &mut HashMap<mio::Token, SerialConnectionState>,
    port_names: &mut HashMap<String, mio::Token>,
    msg_callback: Arc<dyn Fn(TransceiverMessage) + Send + Sync>,
    initial_timeout: Duration,
    next_start: Instant,
) -> SerialDiscoveryStage {
    for (token, (port_info, mut port)) in discovery_ports {
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
        if let Some(robot_id) = robot_id {
            let state = SerialConnectionState {
                port,
                port_info: port_info.clone(),
                rx_buf: [0; PACKET_SIZE],
                rx_buf_pos: 0,
                timeout: Instant::now() + initial_timeout,
            };
            active_connections.insert(token, state);
            port_names.insert(port_info.port_name.clone(), token);
            msg_callback(TransceiverMessage::Connected(
                RobotTransceiverAddress::Serial(port_info),
                robot_id,
            ));
        } else {
            poll.registry().deregister(&mut port).unwrap();
        }
    }
    SerialDiscoveryStage::Waiting(next_start)
}

fn receive_serial_packets(
    state: &mut SerialConnectionState,
    configured_timeout: Duration,
    msg_callback: Arc<dyn Fn(TransceiverMessage) + Send + Sync>,
) {
    loop {
        match state.port.read(&mut state.rx_buf[state.rx_buf_pos..]) {
            Ok(read_bytes) => {
                // Handle read
                let old_pos = state.rx_buf_pos;
                let new_pos = state.rx_buf_pos + read_bytes;

                // Reset on null byte (cobs packet framing)
                let zero_idx = state.rx_buf[old_pos..new_pos].iter().position(|&x| x == 0);
                if let Some(rel_idx) = zero_idx {
                    let idx = old_pos + rel_idx;
                    state.rx_buf.copy_within(idx..new_pos, 0);
                    state.rx_buf_pos = new_pos - idx;
                    continue;
                }

                // Check if the packet is completed
                if state.rx_buf_pos < state.rx_buf.len() {
                    continue;
                }

                // Decode cobs framing
                let mut decoded = [0u8; SERIAL_PACKET_SIZE - 1];
                if cobs::decode(&state.rx_buf, &mut decoded).is_err() {
                    state.rx_buf_pos = 0;
                    continue;
                };
                state.rx_buf_pos = 0;

                // Verify checksum
                let checksum = &decoded[0..decoded.len() - 1]
                    .iter()
                    .fold(0u8, |sum, &x| sum ^ x);
                if decoded.last().unwrap() != checksum {
                    state.rx_buf_pos = 0;
                    continue;
                }

                state.timeout = Instant::now() + configured_timeout;
                msg_callback(TransceiverMessage::PacketReceived(
                    RobotTransceiverAddress::Serial(state.port_info.clone()),
                    (&decoded[0..PACKET_SIZE]).try_into().unwrap(),
                ))
            }
            Err(e) if e.kind() == ErrorKind::WouldBlock => return,
            Err(e) => {
                error!(
                    "Unexpected serial read error on port {}: {e}",
                    state.port_info.port_name
                );
                return;
            }
        }
    }
}
