use std::{collections::HashMap, net::Ipv4Addr};

use messages::{Ack, Ack2, Syn};
use structures::EndpointState;

pub mod message_handlers;
pub mod messages;
pub mod structures;

pub struct Gossiper {
    endpoints_state: HashMap<Ipv4Addr, EndpointState>,
}

impl Gossiper {
    pub fn new() -> Self {
        todo!()
    }

    pub fn handle_syn(syn: Syn) -> Ack {
        todo!()
    }

    pub fn handle_ack(ack: Ack) -> Ack2 {
        todo!()
    }

    pub fn handle_ack2(ack2: Ack2) {
        todo!()
    }
}
