use std::{
    net::{IpAddr, Ipv4Addr},
    str::FromStr,
    sync::mpsc::{self, Receiver},
    thread,
};

use gossip::{messages::GossipMessage, Gossiper};
use internode_protocol::{
    internode_link::InternodeLink,
    messages::{InternodeMessage, InternodeMessageContent},
};

fn main() {
    let node = Node::new();
    node.start();
}

pub struct Node {}

impl Node {
    pub fn new() -> Self {
        Self {}
    }

    pub fn start(&self) {
        dbg!("Node started!");

        let (tx_internode_inbound, rx_internode_inbound) = mpsc::channel::<InternodeMessage>();
        let (tx_internode_outbound, rx_internode_outbound) = mpsc::channel::<InternodeMessage>();

        let internode = thread::spawn(move || {
            let link = InternodeLink::new(rx_internode_outbound, tx_internode_inbound);
            link.start();
        });

        let (tx_gossip_outbound, rx_gossip_outbound) = mpsc::channel::<GossipMessage>();
        let (tx_gossip_inbound, rx_gossip_inbound) = mpsc::channel::<GossipMessage>();

        let gossip_handler = thread::spawn(move || {
            let gossiper = Gossiper::new(rx_gossip_inbound, tx_gossip_outbound).with_seeds(vec![
                Ipv4Addr::from_str("192.168.0.72").unwrap(),
                Ipv4Addr::from_str("192.168.0.70").unwrap(),
            ]);
            gossiper.start();
        });

        let tx_internode_outbound_clone = tx_internode_outbound.clone();

        // receives messages from the gossiper, wraps them and sends them to the internode link
        let outbound_queue = thread::spawn(move || {
            for msg in rx_gossip_outbound {
                // send the gossip message to the internode link
                tx_internode_outbound_clone
                    .send(InternodeMessage::new(
                        IpAddr::V4(msg.to),
                        InternodeMessageContent::Gossip(msg),
                    ))
                    .unwrap();
            }
        });

        // receives messages from other nodes, unwraps them and decides what to do with them
        let internode_inbound_queue = thread::spawn(move || {
            for msg in rx_internode_inbound {
                // handle messages coming from other nodes
                match msg.content {
                    InternodeMessageContent::Gossip(gossip_message) => {
                        tx_gossip_inbound.send(gossip_message).unwrap()
                    }
                    InternodeMessageContent::Dummy(ref m) => {
                        dbg!(m);
                    } // handle other types of messages
                }
            }
        });

        internode.join().unwrap();
        gossip_handler.join().unwrap();
        outbound_queue.join().unwrap();
        internode_inbound_queue.join().unwrap();
    }
}
