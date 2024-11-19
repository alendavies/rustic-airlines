use messages::{Ack, Ack2, Digest, GossipMessage, Syn};
use rand::{seq::IteratorRandom, thread_rng};
use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    net::Ipv4Addr,
};
use structures::{EndpointState, HeartbeatState};
pub mod messages;
pub mod structures;

pub struct Gossiper {
    pub endpoints_state: HashMap<Ipv4Addr, EndpointState>,
}

#[derive(Debug)]
pub enum GossipError {
    SynError,
}

impl fmt::Display for GossipError {
    /// Implementation of the `fmt` method to convert the error into a readable string.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let description = match self {
            GossipError::SynError => "Syn error occurred",
        };
        write!(f, "{}", description)
    }
}

impl Gossiper {
    pub fn new() -> Self {
        Self {
            endpoints_state: HashMap::new(),
        }
    }

    pub fn heartbeat(&mut self, ip: Ipv4Addr) {
        self.endpoints_state
            .get_mut(&ip)
            .unwrap()
            .heartbeat_state
            .inc_version();
    }

    pub fn with_endpoint_state(mut self, ip: Ipv4Addr) -> Self {
        self.endpoints_state.insert(ip, EndpointState::default());
        self
    }

    pub fn with_seeds(mut self, seeds_ip: Vec<Ipv4Addr>) -> Self {
        // init seed with default state
        for ip in seeds_ip {
            self.endpoints_state.insert(ip, EndpointState::default());
        }
        self
    }

    pub fn pick_ips(&self, exclude: Ipv4Addr) -> Vec<&Ipv4Addr> {
        let mut rng = thread_rng();
        let ips: Vec<&Ipv4Addr> = self
            .endpoints_state
            .keys()
            .filter(|&key| *key != exclude)
            .choose_multiple(&mut rng, 3);
        ips
    }

    pub fn create_syn(&self, from: Ipv4Addr) -> GossipMessage {
        let digests: Vec<Digest> = self
            .endpoints_state
            .iter()
            .map(|(k, v)| Digest::from_heartbeat_state(*k, &v.heartbeat_state))
            .collect();

        let syn = Syn::new(digests);

        GossipMessage {
            from,
            payload: messages::Payload::Syn(syn),
        }
    }

    pub fn handle_syn(&self, syn: &Syn) -> Ack {
        let mut stale_digests = Vec::new();
        let mut updated_info = BTreeMap::new();

        for digest in &syn.digests {
            if let Some(my_state) = self.endpoints_state.get(&digest.address) {
                let my_digest =
                    Digest::from_heartbeat_state(digest.address, &my_state.heartbeat_state);

                if digest.generation == my_digest.generation && digest.version == my_digest.version
                {
                    continue;
                }

                if digest.generation != my_digest.generation {
                    // Si la generacion del digest es mayor a la mía, entonces el mio está desactualizado
                    // le mando mi digest
                    if digest.generation > my_digest.generation {
                        stale_digests.push(my_digest.clone());
                    }
                    // si el de él está desactualizado, le mando la info para que lo actualice
                    else if digest.generation < my_digest.generation {
                        updated_info.insert(my_digest.clone(), my_state.application_state.clone());
                    }
                } else {
                    // Si la versión del digest es mayor a la mía, entonces el mio está desactualizado
                    // le mando mi digest
                    if digest.version > my_digest.version {
                        stale_digests.push(my_digest);
                    }
                    // si el de él está desactualizado, le mando la info para que lo actualice
                    else if digest.version < my_digest.version {
                        updated_info.insert(my_digest, my_state.application_state.clone());
                    }
                }
            } else {
                // si no tengo info de ese nodo, entonces mi digest está desactualizado
                // le mando el digest correspondiente a ese nodo con version y generacion en 0
                stale_digests.push(Digest::from_heartbeat_state(
                    digest.address,
                    &HeartbeatState::new(0, 0),
                ));
            }
        }

        Ack {
            stale_digests,
            updated_info,
        }
    }

    pub fn handle_ack(&mut self, ack: &Ack) -> Ack2 {
        let mut updated_info = BTreeMap::new();

        for digest in &ack.stale_digests {
            let my_state = self.endpoints_state.get(&digest.address).unwrap();

            let my_digest = Digest::from_heartbeat_state(digest.address, &my_state.heartbeat_state);

            if digest.generation == my_digest.generation && digest.version == my_digest.version {
                continue;
            }

            // si la generacion en el digest es menor a la mía, le mando la info para que lo actualice
            if digest.generation < my_digest.generation {
                updated_info.insert(my_digest, my_state.application_state.clone());
            }
            // si la version en el digest es menor a la mía, le mando la info para que lo actualice
            else if digest.version < my_digest.version {
                updated_info.insert(my_digest, my_state.application_state.clone());
            }
        }

        for info in &ack.updated_info {
            let my_state = self.endpoints_state.get(&info.0.address).unwrap();

            // por las dudas chequeo que efectivamente sea info más actualizada que la que tengo
            if info.0.version > my_state.heartbeat_state.version
                || info.0.generation > my_state.heartbeat_state.generation
            {
                // la actualizo
                self.endpoints_state.insert(
                    info.0.address,
                    EndpointState::new(
                        info.1.clone(),
                        HeartbeatState::new(info.0.generation, info.0.version),
                    ),
                );
            }
        }

        Ack2 { updated_info }
    }

    pub fn handle_ack2(&mut self, ack2: &Ack2) {
        for info in &ack2.updated_info {
            if let Some(my_state) = self.endpoints_state.get(&info.0.address) {
                // por las dudas chequeo que efectivamente sea info más actualizada que la que tengo
                if info.0.version > my_state.heartbeat_state.version
                    || info.0.generation > my_state.heartbeat_state.generation
                {
                    // la actualizo
                    self.endpoints_state.insert(
                        info.0.address,
                        EndpointState::new(
                            info.1.clone(),
                            HeartbeatState::new(info.0.generation, info.0.version),
                        ),
                    );
                }
            } else {
                self.endpoints_state.insert(
                    info.0.address,
                    EndpointState::new(
                        info.1.clone(),
                        HeartbeatState::new(info.0.generation, info.0.version),
                    ),
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use messages::Payload;

    use crate::structures::{ApplicationState, NodeStatus};

    use super::*;

    #[test]
    fn incoming_syn_same_generation_lower_version() {
        // if the incoming version is lower, the returned ack
        // should contain the updated info
        let ip = Ipv4Addr::from_str("127.0.0.2").unwrap();

        let syn = Syn::new(vec![Digest::new(ip, 3, 2)]);

        let local_state: HashMap<Ipv4Addr, EndpointState> = HashMap::from([(
            ip,
            EndpointState::new(
                ApplicationState::new(NodeStatus::Normal, 6),
                HeartbeatState::new(3, 3),
            ),
        )]);

        let gossiper = Gossiper {
            endpoints_state: local_state.clone(),
        };

        let ack = gossiper.handle_syn(&syn);

        assert!(ack.stale_digests.is_empty());
        assert_eq!(
            ack.updated_info,
            BTreeMap::from([(
                Digest::from_heartbeat_state(
                    ip,
                    &gossiper.endpoints_state.get(&ip).unwrap().heartbeat_state
                ),
                gossiper
                    .endpoints_state
                    .get(&ip)
                    .unwrap()
                    .application_state
                    .clone()
            )])
        );
    }

    #[test]
    fn incoming_syn_lower_generation() {
        // if the incoming generation is lower, the returned ack
        // shold containe the updated info
        let ip = Ipv4Addr::from_str("127.0.0.2").unwrap();

        let syn = Syn::new(vec![Digest::new(ip, 2, 5)]);

        let local_state: HashMap<Ipv4Addr, EndpointState> = HashMap::from([(
            ip,
            EndpointState::new(
                ApplicationState::new(NodeStatus::Normal, 6),
                HeartbeatState::new(3, 3),
            ),
        )]);

        let gossiper = Gossiper {
            endpoints_state: local_state.clone(),
        };

        let ack = gossiper.handle_syn(&syn);

        assert!(ack.stale_digests.is_empty());
        assert_eq!(
            ack.updated_info,
            BTreeMap::from([(
                Digest::from_heartbeat_state(
                    ip,
                    &gossiper.endpoints_state.get(&ip).unwrap().heartbeat_state
                ),
                gossiper
                    .endpoints_state
                    .get(&ip)
                    .unwrap()
                    .application_state
                    .clone()
            )])
        );
    }

    #[test]
    fn incoming_syn_higher_generation() {
        // if the incoming generation is higher, the return ack
        // should contain the local stale digest
        let ip = Ipv4Addr::from_str("127.0.0.2").unwrap();

        let syn = Syn::new(vec![Digest::new(ip, 7, 3)]);

        let local_state: HashMap<Ipv4Addr, EndpointState> = HashMap::from([(
            ip,
            EndpointState::new(
                ApplicationState::new(NodeStatus::Normal, 6),
                HeartbeatState::new(4, 8),
            ),
        )]);

        let gossiper = Gossiper {
            endpoints_state: local_state.clone(),
        };

        let ack = gossiper.handle_syn(&syn);

        assert_eq!(
            ack.stale_digests,
            vec![Digest::from_heartbeat_state(
                ip,
                &gossiper.endpoints_state.get(&ip).unwrap().heartbeat_state
            )]
        );
        assert!(ack.updated_info.is_empty());
    }

    #[test]
    fn incoming_syn_higher_version_same_generation() {
        // if the incoming digest version is higher, the return ack
        // should contain the local stale digest
        let ip = Ipv4Addr::from_str("127.0.0.2").unwrap();

        let syn = Syn::new(vec![Digest::new(ip, 7, 3)]);

        let local_state: HashMap<Ipv4Addr, EndpointState> = HashMap::from([(
            ip,
            EndpointState::new(
                ApplicationState::new(NodeStatus::Normal, 6),
                HeartbeatState::new(7, 2),
            ),
        )]);

        let gossiper = Gossiper {
            endpoints_state: local_state.clone(),
        };

        let ack = gossiper.handle_syn(&syn);

        assert_eq!(
            ack.stale_digests,
            vec![Digest::from_heartbeat_state(
                ip,
                &gossiper.endpoints_state.get(&ip).unwrap().heartbeat_state
            )]
        );
        assert!(ack.updated_info.is_empty(),);
    }

    #[test]
    fn incoming_ack_stale_digest_lower_generation() {
        // if there is incoming stale digest in the ack, the returned ack2 should
        // contain the updated state
        let ip = Ipv4Addr::from_str("127.0.0.2").unwrap();

        let ack = Ack::new(vec![Digest::new(ip, 6, 2)], BTreeMap::new());

        let local_state: HashMap<Ipv4Addr, EndpointState> = HashMap::from([(
            ip,
            EndpointState::new(
                ApplicationState::new(NodeStatus::Normal, 6),
                HeartbeatState::new(7, 2),
            ),
        )]);

        let mut gossiper = Gossiper {
            endpoints_state: local_state.clone(),
        };

        let ack2 = gossiper.handle_ack(&ack);

        assert_eq!(
            ack2.updated_info,
            BTreeMap::from([(
                Digest::from_heartbeat_state(
                    ip,
                    &gossiper.endpoints_state.get(&ip).unwrap().heartbeat_state
                ),
                gossiper
                    .endpoints_state
                    .get(&ip)
                    .unwrap()
                    .application_state
                    .clone()
            )])
        )
    }

    #[test]
    fn incoming_ack_stale_digest_same_generation_lower_version() {
        let ip = Ipv4Addr::from_str("127.0.0.2").unwrap();

        let ack = Ack::new(vec![Digest::new(ip, 7, 2)], BTreeMap::new());

        let local_state: HashMap<Ipv4Addr, EndpointState> = HashMap::from([(
            ip,
            EndpointState::new(
                ApplicationState::new(NodeStatus::Normal, 6),
                HeartbeatState::new(7, 3),
            ),
        )]);

        let mut gossiper = Gossiper {
            endpoints_state: local_state.clone(),
        };

        let ack2 = gossiper.handle_ack(&ack);

        assert_eq!(
            ack2.updated_info,
            BTreeMap::from([(
                Digest::from_heartbeat_state(
                    ip,
                    &gossiper.endpoints_state.get(&ip).unwrap().heartbeat_state
                ),
                gossiper
                    .endpoints_state
                    .get(&ip)
                    .unwrap()
                    .application_state
                    .clone()
            )])
        )
    }

    #[test]
    fn incoming_ack_stale_digest_lower_generation_greater_version() {
        let ip = Ipv4Addr::from_str("127.0.0.2").unwrap();

        let ack = Ack::new(vec![Digest::new(ip, 6, 9)], BTreeMap::new());

        let local_state: HashMap<Ipv4Addr, EndpointState> = HashMap::from([(
            ip,
            EndpointState::new(
                ApplicationState::new(NodeStatus::Normal, 6),
                HeartbeatState::new(7, 3),
            ),
        )]);

        let mut gossiper = Gossiper {
            endpoints_state: local_state.clone(),
        };

        let ack2 = gossiper.handle_ack(&ack);

        assert_eq!(
            ack2.updated_info,
            BTreeMap::from([(
                Digest::from_heartbeat_state(
                    ip,
                    &gossiper.endpoints_state.get(&ip).unwrap().heartbeat_state
                ),
                gossiper
                    .endpoints_state
                    .get(&ip)
                    .unwrap()
                    .application_state
                    .clone()
            )])
        )
    }

    #[test]
    fn incoming_ack_updated_info_higher_generation_higher_version() {
        // if there is incoming updated info in the ack, the local state
        // should be updated
        let ip = Ipv4Addr::from_str("127.0.0.2").unwrap();

        let ack = Ack::new(
            Vec::new(),
            BTreeMap::from([(
                Digest::new(ip, 8, 7),
                ApplicationState::new(NodeStatus::Leaving, 9),
            )]),
        );

        let local_state: HashMap<Ipv4Addr, EndpointState> = HashMap::from([(
            ip,
            EndpointState::new(
                ApplicationState::new(NodeStatus::Normal, 6),
                HeartbeatState::new(7, 2),
            ),
        )]);

        let mut gossiper = Gossiper {
            endpoints_state: local_state.clone(),
        };

        let ack2 = gossiper.handle_ack(&ack);

        assert!(ack2.updated_info.is_empty());
        assert_eq!(
            gossiper.endpoints_state.get(&ip).unwrap().heartbeat_state,
            HeartbeatState::new(8, 7)
        );
        assert_eq!(
            gossiper.endpoints_state.get(&ip).unwrap().application_state,
            ApplicationState::new(NodeStatus::Leaving, 9)
        );
    }

    #[test]
    fn incoming_ack_updated_info_same_generation_higher_version() {
        // if there is incoming updated info in the ack, the local state
        // should be updated
        let ip = Ipv4Addr::from_str("127.0.0.2").unwrap();

        let ack = Ack::new(
            Vec::new(),
            BTreeMap::from([(
                Digest::new(ip, 7, 7),
                ApplicationState::new(NodeStatus::Leaving, 9),
            )]),
        );

        let local_state: HashMap<Ipv4Addr, EndpointState> = HashMap::from([(
            ip,
            EndpointState::new(
                ApplicationState::new(NodeStatus::Normal, 6),
                HeartbeatState::new(7, 2),
            ),
        )]);

        let mut gossiper = Gossiper {
            endpoints_state: local_state.clone(),
        };

        let ack2 = gossiper.handle_ack(&ack);

        assert!(ack2.updated_info.is_empty());
        assert_eq!(
            gossiper.endpoints_state.get(&ip).unwrap().heartbeat_state,
            HeartbeatState::new(7, 7)
        );
        assert_eq!(
            gossiper.endpoints_state.get(&ip).unwrap().application_state,
            ApplicationState::new(NodeStatus::Leaving, 9)
        );
    }

    #[test]
    fn incoming_ack_updated_info_and_stale_digest() {
        let ip_1 = Ipv4Addr::from_str("127.0.0.2").unwrap();
        let ip_2 = Ipv4Addr::from_str("127.0.0.7").unwrap();

        // ack with one stale digest (ip_1) and one updated info (ip_2)
        let ack = Ack::new(
            vec![Digest::new(ip_1, 6, 3)],
            BTreeMap::from([(
                Digest::new(ip_2, 8, 7),
                ApplicationState::new(NodeStatus::Removing, 9),
            )]),
        );

        let local_state: HashMap<Ipv4Addr, EndpointState> = HashMap::from([
            (
                ip_1,
                EndpointState::new(
                    ApplicationState::new(NodeStatus::Bootstrap, 2),
                    HeartbeatState::new(7, 2),
                ),
            ),
            (
                ip_2,
                EndpointState::new(
                    ApplicationState::new(NodeStatus::Normal, 6),
                    HeartbeatState::new(7, 2),
                ),
            ),
        ]);

        let mut gossiper = Gossiper {
            endpoints_state: local_state.clone(),
        };

        let ack2 = gossiper.handle_ack(&ack);

        // the ack2 should contain the updated info for ip_1
        assert_eq!(
            ack2.updated_info,
            BTreeMap::from([(
                Digest::from_heartbeat_state(
                    ip_1,
                    &gossiper.endpoints_state.get(&ip_1).unwrap().heartbeat_state
                ),
                gossiper
                    .endpoints_state
                    .get(&ip_1)
                    .unwrap()
                    .application_state
                    .clone()
            )])
        );
        // the local_state should be updated for ip_2
        assert_eq!(
            gossiper.endpoints_state.get(&ip_2).unwrap().heartbeat_state,
            HeartbeatState::new(8, 7)
        );
        assert_eq!(
            gossiper
                .endpoints_state
                .get(&ip_2)
                .unwrap()
                .application_state,
            ApplicationState::new(NodeStatus::Removing, 9)
        );
    }

    #[test]
    fn incoming_ack2_updated_info() {
        let ip_1 = Ipv4Addr::from_str("127.0.0.2").unwrap();
        let ip_2 = Ipv4Addr::from_str("127.0.0.7").unwrap();

        let ack2 = Ack2::new(BTreeMap::from([
            (
                Digest::new(ip_1, 7, 6),
                ApplicationState::new(NodeStatus::Normal, 7),
            ),
            (
                Digest::new(ip_2, 8, 7),
                ApplicationState::new(NodeStatus::Removing, 9),
            ),
        ]));

        let local_state: HashMap<Ipv4Addr, EndpointState> = HashMap::from([
            (
                ip_1,
                EndpointState::new(
                    ApplicationState::new(NodeStatus::Bootstrap, 2),
                    HeartbeatState::new(7, 2),
                ),
            ),
            (
                ip_2,
                EndpointState::new(
                    ApplicationState::new(NodeStatus::Normal, 6),
                    HeartbeatState::new(7, 2),
                ),
            ),
        ]);

        let mut gossiper = Gossiper {
            endpoints_state: local_state.clone(),
        };

        gossiper.handle_ack2(&ack2);

        // the local_state should be updated for both ips
        assert_eq!(
            gossiper.endpoints_state.get(&ip_1).unwrap().heartbeat_state,
            HeartbeatState::new(7, 6)
        );
        assert_eq!(
            gossiper
                .endpoints_state
                .get(&ip_1)
                .unwrap()
                .application_state,
            ApplicationState::new(NodeStatus::Normal, 7)
        );
        assert_eq!(
            gossiper.endpoints_state.get(&ip_2).unwrap().heartbeat_state,
            HeartbeatState::new(8, 7)
        );
        assert_eq!(
            gossiper
                .endpoints_state
                .get(&ip_2)
                .unwrap()
                .application_state,
            ApplicationState::new(NodeStatus::Removing, 9)
        );
    }

    #[test]
    fn new_digest_in_syn() {
        let new_ip = Ipv4Addr::from_str("127.0.0.7").unwrap();

        let syn = Syn::new(vec![Digest::new(new_ip, 1, 1)]);

        let local_state: HashMap<Ipv4Addr, EndpointState> = HashMap::new();

        let gossiper = Gossiper {
            endpoints_state: local_state.clone(),
        };

        let ack = gossiper.handle_syn(&syn);

        assert_eq!(ack.stale_digests, vec![Digest::new(new_ip, 0, 0)]);
        assert!(ack.updated_info.is_empty(),);
    }

    #[test]
    fn new_state_in_ack() {
        let new_ip = Ipv4Addr::from_str("127.0.0.7").unwrap();

        let ack = Ack2::new(BTreeMap::from([(
            Digest::new(new_ip, 1, 1),
            ApplicationState::new(NodeStatus::Bootstrap, 1),
        )]));

        let local_state: HashMap<Ipv4Addr, EndpointState> = HashMap::new();

        let mut gossiper = Gossiper {
            endpoints_state: local_state.clone(),
        };

        let _ = gossiper.handle_ack2(&ack);

        assert_eq!(
            gossiper
                .endpoints_state
                .get(&new_ip)
                .unwrap()
                .heartbeat_state,
            HeartbeatState::new(1, 1)
        );
        assert_eq!(
            gossiper
                .endpoints_state
                .get(&new_ip)
                .unwrap()
                .application_state,
            ApplicationState::new(NodeStatus::Bootstrap, 1)
        );
    }

    #[test]
    fn test_gossip_flow() {
        let client_ip = Ipv4Addr::from_str("127.0.0.1").unwrap();
        let server_ip = Ipv4Addr::from_str("127.0.0.2").unwrap();
        let another_ip = Ipv4Addr::from_str("127.0.0.3").unwrap();
        let new_ip = Ipv4Addr::from_str("127.0.0.4").unwrap();

        let client_state: HashMap<Ipv4Addr, EndpointState> = HashMap::from([
            (
                client_ip,
                EndpointState::new(
                    ApplicationState::new(NodeStatus::Bootstrap, 2),
                    HeartbeatState::new(7, 2),
                ),
            ),
            (
                server_ip,
                EndpointState::new(
                    ApplicationState::new(NodeStatus::Normal, 6),
                    HeartbeatState::new(8, 3),
                ),
            ),
            (
                another_ip,
                EndpointState::new(
                    ApplicationState::new(NodeStatus::Normal, 8),
                    HeartbeatState::new(4, 1),
                ),
            ),
            (
                new_ip,
                EndpointState::new(
                    ApplicationState::new(NodeStatus::Bootstrap, 1),
                    HeartbeatState::new(1, 1),
                ),
            ),
        ]);

        let server_state: HashMap<Ipv4Addr, EndpointState> = HashMap::from([
            (
                client_ip,
                EndpointState::new(
                    ApplicationState::new(NodeStatus::Bootstrap, 2),
                    HeartbeatState::new(6, 1),
                ),
            ),
            (
                server_ip,
                EndpointState::new(
                    ApplicationState::new(NodeStatus::Normal, 7),
                    HeartbeatState::new(8, 8),
                ),
            ),
            (
                another_ip,
                EndpointState::new(
                    ApplicationState::new(NodeStatus::Normal, 8),
                    HeartbeatState::new(7, 2),
                ),
            ),
        ]);

        // client sends syn to server
        let syn = Syn::new(vec![
            Digest::new(client_ip, 7, 2),
            Digest::new(server_ip, 8, 3),
            Digest::new(another_ip, 4, 1),
            Digest::new(new_ip, 1, 1),
        ]);

        let mut gossiper_server = Gossiper {
            endpoints_state: server_state.clone(),
        };

        // server handles syn and sends ack to client
        let ack = gossiper_server.handle_syn(&syn);

        assert_eq!(
            ack,
            Ack::new(
                vec![Digest::new(client_ip, 6, 1), Digest::new(new_ip, 0, 0)],
                BTreeMap::from([
                    (
                        Digest::new(server_ip, 8, 8),
                        ApplicationState::new(NodeStatus::Normal, 7)
                    ),
                    (
                        Digest::new(another_ip, 7, 2),
                        ApplicationState::new(NodeStatus::Normal, 8)
                    ),
                ])
            )
        );

        let mut gossiper_client = Gossiper {
            endpoints_state: client_state.clone(),
        };

        // client handles ack, updates its state and sends ack2 to server
        let ack2 = gossiper_client.handle_ack(&ack);

        assert_eq!(
            ack2,
            Ack2::new(BTreeMap::from([
                (
                    Digest::new(client_ip, 7, 2),
                    ApplicationState::new(NodeStatus::Bootstrap, 2)
                ),
                (
                    Digest::new(new_ip, 1, 1),
                    ApplicationState::new(NodeStatus::Bootstrap, 1)
                ),
            ]))
        );

        // server handles ack2 and updates its state
        gossiper_server.handle_ack2(&ack2);

        assert_eq!(
            gossiper_server.endpoints_state,
            gossiper_client.endpoints_state
        );
    }

    #[test]
    fn string_as_bytes() {
        let syn = Syn {
            digests: vec![
                Digest::new(Ipv4Addr::new(127, 0, 0, 1), 1, 15),
                Digest::new(Ipv4Addr::new(127, 0, 0, 2), 10, 15),
                Digest::new(Ipv4Addr::new(127, 0, 0, 3), 3, 15),
            ],
        };

        let gossip_msg = GossipMessage {
            from: Ipv4Addr::new(127, 0, 0, 1),
            payload: Payload::Syn(syn),
        };

        let syn_bytes = gossip_msg.as_bytes();
        let string = format!("{}", std::str::from_utf8(syn_bytes.as_slice()).unwrap());
        let string_bytes = string.as_bytes();

        assert_eq!(syn_bytes, string_bytes);
    }
}
