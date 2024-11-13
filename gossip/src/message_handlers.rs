use std::{
    collections::{BTreeMap, HashMap},
    net::Ipv4Addr,
};

use crate::{
    messages::{Ack, Ack2, Digest, Syn},
    structures::{EndpointState, HeartbeatState},
};

pub fn handle_syn(syn: Syn, state: &mut HashMap<Ipv4Addr, EndpointState>) -> Ack {
    let mut stale_digests = Vec::new();
    let mut updated_info = BTreeMap::new();

    for digest in syn.digests {
        if let Some(my_state) = state.get(&digest.address) {
            let my_digest = Digest::from_heartbeat_state(digest.address, &my_state.heartbeat_state);

            if digest.generation == my_digest.generation && digest.version == my_digest.version {
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

pub fn handle_ack(ack: Ack, state: &mut HashMap<Ipv4Addr, EndpointState>) -> Ack2 {
    let mut updated_info = BTreeMap::new();

    for digest in ack.stale_digests {
        let my_state = state.get(&digest.address).unwrap();

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

    for info in ack.updated_info {
        let my_state = state.get(&info.0.address).unwrap();

        // por las dudas chequeo que efectivamente sea info más actualizada que la que tengo
        if info.0.version > my_state.heartbeat_state.version
            || info.0.generation > my_state.heartbeat_state.generation
        {
            // la actualizo
            state.insert(
                info.0.address,
                EndpointState::new(
                    info.1,
                    HeartbeatState::new(info.0.generation, info.0.version),
                ),
            );
        }
    }

    Ack2 { updated_info }
}

pub fn handle_ack2(ack2: Ack2, state: &mut HashMap<Ipv4Addr, EndpointState>) {
    for info in ack2.updated_info {
        if let Some(my_state) = state.get(&info.0.address) {
            // por las dudas chequeo que efectivamente sea info más actualizada que la que tengo
            if info.0.version > my_state.heartbeat_state.version
                || info.0.generation > my_state.heartbeat_state.generation
            {
                // la actualizo
                state.insert(
                    info.0.address,
                    EndpointState::new(
                        info.1,
                        HeartbeatState::new(info.0.generation, info.0.version),
                    ),
                );
            }
        } else {
            state.insert(
                info.0.address,
                EndpointState::new(
                    info.1,
                    HeartbeatState::new(info.0.generation, info.0.version),
                ),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::structures::{ApplicationState, NodeStatus};

    use super::*;

    #[test]
    fn incoming_syn_same_generation_lower_version() {
        // if the incoming version is lower, the returned ack
        // should contain the updated info
        let ip = Ipv4Addr::from_str("127.0.0.2").unwrap();

        let syn = Syn::new(vec![Digest::new(ip, 3, 2)]);

        let mut local_state: HashMap<Ipv4Addr, EndpointState> = HashMap::from([(
            ip,
            EndpointState::new(
                ApplicationState::new(NodeStatus::Normal, 6),
                HeartbeatState::new(3, 3),
            ),
        )]);

        let ack = handle_syn(syn, &mut local_state);

        assert!(ack.stale_digests.is_empty());
        assert_eq!(
            ack.updated_info,
            BTreeMap::from([(
                Digest::from_heartbeat_state(ip, &local_state.get(&ip).unwrap().heartbeat_state),
                local_state.get(&ip).unwrap().application_state.clone()
            )])
        );
    }

    #[test]
    fn incoming_syn_lower_generation() {
        // if the incoming generation is lower, the returned ack
        // shold containe the updated info
        let ip = Ipv4Addr::from_str("127.0.0.2").unwrap();

        let syn = Syn::new(vec![Digest::new(ip, 2, 5)]);

        let mut local_state: HashMap<Ipv4Addr, EndpointState> = HashMap::from([(
            ip,
            EndpointState::new(
                ApplicationState::new(NodeStatus::Normal, 6),
                HeartbeatState::new(3, 3),
            ),
        )]);

        let ack = handle_syn(syn, &mut local_state);

        assert!(ack.stale_digests.is_empty());
        assert_eq!(
            ack.updated_info,
            BTreeMap::from([(
                Digest::from_heartbeat_state(ip, &local_state.get(&ip).unwrap().heartbeat_state),
                local_state.get(&ip).unwrap().application_state.clone()
            )])
        );
    }

    #[test]
    fn incoming_syn_higher_generation() {
        // if the incoming generation is higher, the return ack
        // should contain the local stale digest
        let ip = Ipv4Addr::from_str("127.0.0.2").unwrap();

        let syn = Syn::new(vec![Digest::new(ip, 7, 3)]);

        let mut local_state: HashMap<Ipv4Addr, EndpointState> = HashMap::from([(
            ip,
            EndpointState::new(
                ApplicationState::new(NodeStatus::Normal, 6),
                HeartbeatState::new(4, 8),
            ),
        )]);

        let ack = handle_syn(syn, &mut local_state);

        assert_eq!(
            ack.stale_digests,
            vec![Digest::from_heartbeat_state(
                ip,
                &local_state.get(&ip).unwrap().heartbeat_state
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

        let mut local_state: HashMap<Ipv4Addr, EndpointState> = HashMap::from([(
            ip,
            EndpointState::new(
                ApplicationState::new(NodeStatus::Normal, 6),
                HeartbeatState::new(7, 2),
            ),
        )]);

        let ack = handle_syn(syn, &mut local_state);

        assert_eq!(
            ack.stale_digests,
            vec![Digest::from_heartbeat_state(
                ip,
                &local_state.get(&ip).unwrap().heartbeat_state
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

        let mut local_state: HashMap<Ipv4Addr, EndpointState> = HashMap::from([(
            ip,
            EndpointState::new(
                ApplicationState::new(NodeStatus::Normal, 6),
                HeartbeatState::new(7, 2),
            ),
        )]);

        let ack2 = handle_ack(ack, &mut local_state);

        assert_eq!(
            ack2.updated_info,
            BTreeMap::from([(
                Digest::from_heartbeat_state(ip, &local_state.get(&ip).unwrap().heartbeat_state),
                local_state.get(&ip).unwrap().application_state.clone()
            )])
        )
    }

    #[test]
    fn incoming_ack_stale_digest_same_generation_lower_version() {
        let ip = Ipv4Addr::from_str("127.0.0.2").unwrap();

        let ack = Ack::new(vec![Digest::new(ip, 7, 2)], BTreeMap::new());

        let mut local_state: HashMap<Ipv4Addr, EndpointState> = HashMap::from([(
            ip,
            EndpointState::new(
                ApplicationState::new(NodeStatus::Normal, 6),
                HeartbeatState::new(7, 3),
            ),
        )]);

        let ack2 = handle_ack(ack, &mut local_state);

        assert_eq!(
            ack2.updated_info,
            BTreeMap::from([(
                Digest::from_heartbeat_state(ip, &local_state.get(&ip).unwrap().heartbeat_state),
                local_state.get(&ip).unwrap().application_state.clone()
            )])
        )
    }

    #[test]
    fn incoming_ack_stale_digest_lower_generation_greater_version() {
        let ip = Ipv4Addr::from_str("127.0.0.2").unwrap();

        let ack = Ack::new(vec![Digest::new(ip, 6, 9)], BTreeMap::new());

        let mut local_state: HashMap<Ipv4Addr, EndpointState> = HashMap::from([(
            ip,
            EndpointState::new(
                ApplicationState::new(NodeStatus::Normal, 6),
                HeartbeatState::new(7, 3),
            ),
        )]);

        let ack2 = handle_ack(ack, &mut local_state);

        assert_eq!(
            ack2.updated_info,
            BTreeMap::from([(
                Digest::from_heartbeat_state(ip, &local_state.get(&ip).unwrap().heartbeat_state),
                local_state.get(&ip).unwrap().application_state.clone()
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

        let mut local_state: HashMap<Ipv4Addr, EndpointState> = HashMap::from([(
            ip,
            EndpointState::new(
                ApplicationState::new(NodeStatus::Normal, 6),
                HeartbeatState::new(7, 2),
            ),
        )]);

        let ack2 = handle_ack(ack, &mut local_state);

        assert!(ack2.updated_info.is_empty());
        assert_eq!(
            local_state.get(&ip).unwrap().heartbeat_state,
            HeartbeatState::new(8, 7)
        );
        assert_eq!(
            local_state.get(&ip).unwrap().application_state,
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

        let mut local_state: HashMap<Ipv4Addr, EndpointState> = HashMap::from([(
            ip,
            EndpointState::new(
                ApplicationState::new(NodeStatus::Normal, 6),
                HeartbeatState::new(7, 2),
            ),
        )]);

        let ack2 = handle_ack(ack, &mut local_state);

        assert!(ack2.updated_info.is_empty());
        assert_eq!(
            local_state.get(&ip).unwrap().heartbeat_state,
            HeartbeatState::new(7, 7)
        );
        assert_eq!(
            local_state.get(&ip).unwrap().application_state,
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

        let mut local_state: HashMap<Ipv4Addr, EndpointState> = HashMap::from([
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

        let ack2 = handle_ack(ack, &mut local_state);

        // the ack2 should contain the updated info for ip_1
        assert_eq!(
            ack2.updated_info,
            BTreeMap::from([(
                Digest::from_heartbeat_state(
                    ip_1,
                    &local_state.get(&ip_1).unwrap().heartbeat_state
                ),
                local_state.get(&ip_1).unwrap().application_state.clone()
            )])
        );
        // the local_state should be updated for ip_2
        assert_eq!(
            local_state.get(&ip_2).unwrap().heartbeat_state,
            HeartbeatState::new(8, 7)
        );
        assert_eq!(
            local_state.get(&ip_2).unwrap().application_state,
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

        let mut local_state: HashMap<Ipv4Addr, EndpointState> = HashMap::from([
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

        handle_ack2(ack2, &mut local_state);

        // the local_state should be updated for both ips
        assert_eq!(
            local_state.get(&ip_1).unwrap().heartbeat_state,
            HeartbeatState::new(7, 6)
        );
        assert_eq!(
            local_state.get(&ip_1).unwrap().application_state,
            ApplicationState::new(NodeStatus::Normal, 7)
        );
        assert_eq!(
            local_state.get(&ip_2).unwrap().heartbeat_state,
            HeartbeatState::new(8, 7)
        );
        assert_eq!(
            local_state.get(&ip_2).unwrap().application_state,
            ApplicationState::new(NodeStatus::Removing, 9)
        );
    }

    #[test]
    fn new_digest_in_syn() {
        let new_ip = Ipv4Addr::from_str("127.0.0.7").unwrap();

        let syn = Syn::new(vec![Digest::new(new_ip, 1, 1)]);

        let mut local_state: HashMap<Ipv4Addr, EndpointState> = HashMap::new();

        let ack = handle_syn(syn, &mut local_state);

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

        let mut local_state: HashMap<Ipv4Addr, EndpointState> = HashMap::new();

        let _ = handle_ack2(ack, &mut local_state);

        assert_eq!(
            local_state.get(&new_ip).unwrap().heartbeat_state,
            HeartbeatState::new(1, 1)
        );
        assert_eq!(
            local_state.get(&new_ip).unwrap().application_state,
            ApplicationState::new(NodeStatus::Bootstrap, 1)
        );
    }

    #[test]
    fn test_gossip_flow() {
        let client_ip = Ipv4Addr::from_str("127.0.0.1").unwrap();
        let server_ip = Ipv4Addr::from_str("127.0.0.2").unwrap();
        let another_ip = Ipv4Addr::from_str("127.0.0.3").unwrap();
        let new_ip = Ipv4Addr::from_str("127.0.0.4").unwrap();

        let mut client_state: HashMap<Ipv4Addr, EndpointState> = HashMap::from([
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

        let mut server_state: HashMap<Ipv4Addr, EndpointState> = HashMap::from([
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

        // server handles syn and sends ack to client
        let ack = handle_syn(syn, &mut server_state);

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

        // client handles ack, updates its state and sends ack2 to server
        let ack2 = handle_ack(ack, &mut client_state);

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
        handle_ack2(ack2, &mut server_state);

        assert_eq!(server_state, client_state);
    }
}
