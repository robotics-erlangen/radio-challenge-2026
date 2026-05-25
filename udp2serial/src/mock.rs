use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant};

use crate::{CONNECTION_TIMEOUT, DATA_PORT, PACKET_SIZE};

pub fn start_mock_responder(
    last_received_time: Arc<RwLock<Instant>>,
    sim_response_time: Duration,
    sim_command_loss: f32,
    sim_response_loss: f32,
) -> u8 {
    let socket = crate::bind_dual_stack(DATA_PORT).unwrap();

    // Try to get a unique id from the hostname (trailing digits: pi10 -> 10)
    let hardware_id = (|| {
        let hostname = hostname::get().ok()?.into_string().ok()?;
        let start = hostname
            .bytes()
            .rposition(|b| !b.is_ascii_digit())
            .map_or(0, |i| i + 1);
        let id = hostname[start..].parse::<u8>().ok()?;

        println!("Running mock responder with hostname id {id} (from {hostname})");
        Some(id)
    })()
    .unwrap_or_else(|| {
        // Couldn't get the id from the hostname -> use a "random" number
        let id = (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
            % 16) as u8;
        println!("Running mock responder with random id {id}");
        id
    });

    thread::spawn(move || {
        let mut rx_buf = [0u8; PACKET_SIZE];
        let mut tx_buf = [0u8; PACKET_SIZE];
        let mut last_command_addr = None;

        let mut last_rx_counter = 0u8;
        let mut packet_loss_history = 0u128;

        let mut sim_command_loss_acc = 1f32;
        let mut sim_response_loss_acc = 1f32;
        loop {
            let Ok((_, addr)) = socket.recv_from(&mut rx_buf) else {
                continue;
            };

            if last_received_time.read().unwrap().elapsed() > CONNECTION_TIMEOUT {
                // New connection
                println!("Receiving packets from {:?}", addr);
                last_command_addr = Some(addr);
            } else if last_command_addr != Some(addr) {
                // Different connection source active
                continue;
            }
            *last_received_time.write().unwrap() = Instant::now();

            // Apply simulated command packet loss
            sim_command_loss_acc += sim_command_loss;
            if sim_command_loss_acc >= 1.0 {
                sim_command_loss_acc -= 1.0;
                continue;
            }

            // Apply simulated delay
            std::thread::sleep(sim_response_time);

            // Layout: (lsb)[counter: 6bits][acknumber: 1bit][datagram: 1bit]
            let rx_counter = rx_buf[0] & 0b00111111;
            let counter_diff = (rx_counter as i8 - last_rx_counter as i8).rem_euclid(64) as u8;
            last_rx_counter = rx_counter;

            // Update the packet loss history pushing a 1 for the received packet 0s for any skipped counters
            if counter_diff > 0 {
                packet_loss_history <<= counter_diff; // Shift for the new packet, and any missed packets in between
                packet_loss_history &= !((1u128 << counter_diff) - 1); // Clear the bits that were shifted out
                packet_loss_history |= 1u128; // Set the bit for the new received packet
            }

            // Apply simulated repsonse packet loss after registering the command loss
            sim_response_loss_acc += sim_response_loss;
            if sim_response_loss_acc >= 1.0 {
                sim_response_loss_acc -= 1.0;
                continue;
            }

            // Command (rx) packet loss = ratio of 0s in the last 100 bits
            packet_loss_history &= (1u128 << 100) - 1; // Clear bits 100..128 beacuse the history should only count the last 100 packets
            let missing_in_window = 100 - packet_loss_history.count_ones(); // 100-ones because the cleared 0s should not influence the count
            let rx_packet_loss = missing_in_window as f32 / 100.0;

            // Copy the counter from the command message header, without the ack and datagram bits.
            // Layout: (lsb)[counter: 6bits][acknumber: 1bit][datagram: 1bit]
            tx_buf[0] = rx_buf[0] & 0b00111111;
            // Set the packet loss in the second byte of the response message (third byte including the header)
            tx_buf[2] = (rx_packet_loss * u8::MAX as f32) as u8;

            if let Some(addr) = last_command_addr {
                socket.send_to(&tx_buf, addr).unwrap();
            }
        }
    });

    hardware_id
}
