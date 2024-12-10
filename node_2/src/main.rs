use core::panic;
use std::{
    env,
    net::{IpAddr, Ipv4Addr, ToSocketAddrs},
    sync::mpsc::{self},
    thread,
    time::Duration,
};

use clients_link::{ClientResponse, ClientsLink};
use gossip::{
    messages::{GossipMessageWithDestination, GossipMessageWithOrigin},
    Gossiper,
};
use internode_protocol::{
    internode_link::{
        InternodeLink, InternodeMessageWithDestinationAddress, InternodeMessageWithOriginAddress,
    },
    messages::{InternodeMessage, InternodeMessageContent},
};
use storage_engine::{CsvStorageEngine, StorageEngine};

fn main() {
    let engine = CsvStorageEngine;
    let node = Node::new(engine);
    node.start();
}

// struct OpenQueries;

struct ClientInfo {
    keyspace: String,
}

pub struct Node {
    storage: Box<dyn StorageEngine>,
    // open_queries: OpenQueries,
}

impl Node {
    pub fn new(storage: impl StorageEngine + 'static) -> Self {
        Self {
            storage: Box::new(storage),
        }
    }

    pub fn start(&self) {
        let hostname = env::var("HOSTNAME").unwrap();

        let own: Vec<_> = format!("{}:0", hostname)
            .to_socket_addrs()
            .unwrap()
            .filter(|x| x.is_ipv4())
            .collect();
        let own = own.first().unwrap().ip();
        let self_ip: Ipv4Addr;

        if let IpAddr::V4(y) = own {
            self_ip = y;
        } else {
            panic!()
        }

        dbg!(self_ip);

        let seed: Vec<_> = "seed:0"
            .to_socket_addrs()
            .unwrap()
            .filter(|x| x.is_ipv4())
            .collect();
        let seed = seed.first().unwrap().ip();
        let seed_ip: Ipv4Addr;

        if let IpAddr::V4(y) = seed {
            seed_ip = y;
        } else {
            panic!()
        }

        dbg!(seed_ip);

        println!("Node started with hostname: {}", hostname);

        let (tx_internode_inbound, rx_internode_inbound) =
            mpsc::channel::<InternodeMessageWithOriginAddress>();
        let (tx_internode_outbound, rx_internode_outbound) =
            mpsc::channel::<InternodeMessageWithDestinationAddress>();

        let internode = thread::spawn(move || {
            let link = InternodeLink::new(rx_internode_outbound, tx_internode_inbound);
            link.start();
        });

        let (tx_gossip_outbound, rx_gossip_outbound) =
            mpsc::channel::<GossipMessageWithDestination>();
        let (tx_gossip_inbound, rx_gossip_inbound) = mpsc::channel::<GossipMessageWithOrigin>();

        let a: Vec<_> = "seed:9999"
            .to_socket_addrs()
            .unwrap()
            .filter(|x| x.is_ipv4())
            .collect();
        let a = a.first().unwrap().ip();
        let x: Ipv4Addr;

        if let IpAddr::V4(y) = a {
            x = y;
        } else {
            panic!()
        }

        let gossip_handler = thread::spawn(move || {
            let gossiper =
                Gossiper::new(self_ip, rx_gossip_inbound, tx_gossip_outbound).with_seeds(vec![x]);
            gossiper.start();
        });

        let tx_internode_outbound_clone = tx_internode_outbound.clone();

        // receives messages from the gossiper, wraps them and sends them to the internode link
        let gossip_outbound_queue = thread::spawn(move || {
            for msg in rx_gossip_outbound {
                // send the gossip message to the internode link
                tx_internode_outbound_clone
                    .send(InternodeMessageWithDestinationAddress {
                        to: IpAddr::V4(msg.to),
                        message: InternodeMessage::new(InternodeMessageContent::Gossip(
                            msg.message,
                        )),
                    })
                    .unwrap();
            }
        });

        let (tx_client_inbound, rx_client_inbound) = mpsc::channel();
        // let (tx_client_outbound, rx_client_outbound) = mpsc::channel();

        let client = thread::spawn(move || {
            let link = ClientsLink::new(tx_client_inbound);
            link.start();
        });

        let client_request_queue = thread::spawn(move || {
            for msg in rx_client_inbound {
                dbg!(&msg);
                thread::sleep(Duration::from_secs(2));
                msg.reply_channel.send(ClientResponse).unwrap();
            }
        });

        // receives messages from other nodes, unwraps them and decides what to do with them
        let internode_inbound_queue = thread::spawn(move || {
            for msg in rx_internode_inbound {
                // handle messages coming from other nodes
                match msg.message.content {
                    InternodeMessageContent::Gossip(gossip_message) => {
                        let msg = GossipMessageWithOrigin {
                            from: if let IpAddr::V4(ip) = msg.from {
                                ip
                            } else {
                                panic!()
                            },
                            message: gossip_message,
                        };

                        tx_gossip_inbound.send(msg).unwrap()
                    } // check if query can be closed, if yes, send to tx_client_outbound
                }
            }
        });

        client.join().unwrap();
        client_request_queue.join().unwrap();
        internode.join().unwrap();
        gossip_handler.join().unwrap();
        gossip_outbound_queue.join().unwrap();
        internode_inbound_queue.join().unwrap();
    }
}
