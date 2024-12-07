use std::net::IpAddr;

use gossip::messages::GossipMessage;

pub struct InternodeMessage {
    /// The IP address of the destination node.
    pub to: IpAddr,
    /// The content of the message.
    pub content: InternodeMessageContent,
}

impl InternodeMessage {
    /// Creates a new internode message.
    pub fn new(to: IpAddr, content: InternodeMessageContent) -> Self {
        Self { to, content }
    }
}

pub enum InternodeMessageContent {
    Gossip(GossipMessage),
    Dummy(String),
}
