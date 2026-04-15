use crate::{ConnectionDriverEvent, RobotTransceiverAddress};
use std::collections::HashMap;

/// Cache that can keep track of the transient data that is sent with ConnectionDriver messages, like received responses and the connection type
#[derive(Clone, Debug, Default)]
pub struct ConnectionStateCache<RR> {
    connected_robots: HashMap<u8, RobotConnState<RR>>,
}

#[derive(Clone, Debug)]
struct RobotConnState<RR> {
    transceiver_address: RobotTransceiverAddress,
    latest_response: Option<RR>,
}

impl<RR> ConnectionStateCache<RR> {
    pub fn update<DR>(&mut self, msg: ConnectionDriverEvent<RR, DR>) {
        match msg {
            ConnectionDriverEvent::Connected(robot_id, addr) => {
                self.connected_robots.insert(
                    robot_id,
                    RobotConnState {
                        transceiver_address: addr,
                        latest_response: None,
                    },
                );
            }
            ConnectionDriverEvent::Disconnected(robot_id, ..) => {
                self.connected_robots.remove(&robot_id);
            }
            ConnectionDriverEvent::PacketReceived(robot_id, packet, _) => {
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

    pub fn latest_response(&self, robot_id: u8) -> Option<&RR> {
        self.connected_robots
            .get(&robot_id)
            .and_then(|state| state.latest_response.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::ConnectionStateCache;
    use crate::{ConnectionDriverEvent, RobotTransceiverAddress};
    use std::time::Instant;

    fn conn_event(robot_id: u8) -> ConnectionDriverEvent<u32, ()> {
        ConnectionDriverEvent::Connected(robot_id, RobotTransceiverAddress::Test(robot_id))
    }
    fn recv_event(robot_id: u8, value: u32) -> ConnectionDriverEvent<u32, ()> {
        ConnectionDriverEvent::PacketReceived(robot_id, value, Instant::now())
    }

    #[test]
    fn connect() {
        let mut cache = ConnectionStateCache::<()>::default();
        let robot_id = 3;
        let addr = RobotTransceiverAddress::Test(0);

        cache.update::<()>(ConnectionDriverEvent::Connected(robot_id, addr.clone()));

        assert_eq!(cache.connected_robots(), vec![robot_id]);
        assert_eq!(cache.transceiver_address(robot_id), Some(&addr));
        assert_eq!(cache.latest_response(robot_id), None);
    }

    #[test]
    fn receive() {
        let mut cache = ConnectionStateCache::<u32>::default();
        let robot_id = 7;

        cache.update::<()>(conn_event(robot_id));
        cache.update::<()>(recv_event(robot_id, 42));
        cache.update::<()>(recv_event(robot_id, 43));

        assert_eq!(cache.latest_response(robot_id), Some(&43u32));
    }

    #[test]
    fn ignore_unknown_receive() {
        let mut cache = ConnectionStateCache::<u32>::default();
        let robot_id = 9;

        cache.update::<()>(recv_event(robot_id, 11));

        assert!(cache.connected_robots().is_empty());
        assert_eq!(cache.latest_response(robot_id), None);
    }

    #[test]
    fn disconnect() {
        let mut cache = ConnectionStateCache::<u32>::default();
        let robot_id = 5;

        cache.update::<()>(conn_event(robot_id));
        cache.update::<()>(recv_event(robot_id, 99));
        cache.update::<()>(ConnectionDriverEvent::Disconnected(robot_id));

        assert!(cache.connected_robots().is_empty());
        assert_eq!(cache.transceiver_address(robot_id), None);
        assert_eq!(cache.latest_response(robot_id), None);
    }

    #[test]
    fn multiple_robots() {
        let mut cache = ConnectionStateCache::<u32>::default();
        let robot_id1 = 1;
        let robot_id2 = 2;

        cache.update::<()>(conn_event(robot_id1));
        cache.update::<()>(conn_event(robot_id2));
        cache.update::<()>(recv_event(robot_id1, 42));
        cache.update::<()>(recv_event(robot_id2, 43));

        let mut connected_robots = cache.connected_robots();
        connected_robots.sort_unstable();
        assert_eq!(connected_robots, vec![robot_id1, robot_id2]);
        assert_eq!(cache.latest_response(robot_id1), Some(&42u32));
        assert_eq!(cache.latest_response(robot_id2), Some(&43u32));
    }
}
