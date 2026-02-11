use crate::packet::RegularResponseData;
use crate::{RobotMessage, RobotTransceiverAddress};
use std::collections::HashMap;

/// Cache that can keep track of the transient data that is sent with ConnectionPool messages, like received responses and the connection type
#[derive(Clone, Debug, Default)]
pub struct ConnectionStateCache {
    connected_robots: HashMap<u8, RobotConnState>,
}

#[derive(Clone, Debug)]
struct RobotConnState {
    transceiver_address: RobotTransceiverAddress,
    latest_response: Option<RegularResponseData>,
}

impl ConnectionStateCache {
    pub fn update(&mut self, msg: RobotMessage) {
        match msg {
            RobotMessage::Connected(robot_id, addr) => {
                self.connected_robots.insert(
                    robot_id,
                    RobotConnState {
                        transceiver_address: addr,
                        latest_response: None,
                    },
                );
            }
            RobotMessage::Disconnected(robot_id, ..) => {
                self.connected_robots.remove(&robot_id);
            }
            RobotMessage::PacketReceived(robot_id, packet) => {
                if let Some(conn_state) = self.connected_robots.get_mut(&robot_id) {
                    conn_state.latest_response = Some(packet);
                }
            }
            _ => {}
        }
    }

    pub fn connected_robots(&self) -> Vec<u8> {
        self.connected_robots.keys().copied().collect()
    }

    pub fn transceiver_address(&self, robot_id: u8) -> Option<&RobotTransceiverAddress> {
        self.connected_robots
            .get(&robot_id)
            .map(|state| &state.transceiver_address)
    }

    pub fn latest_response(&self, robot_id: u8) -> Option<&RegularResponseData> {
        self.connected_robots
            .get(&robot_id)
            .and_then(|state| state.latest_response.as_ref())
    }
}
