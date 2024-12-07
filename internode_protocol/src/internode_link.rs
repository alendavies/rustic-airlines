use std::{
    io::Read,
    net::{IpAddr, Ipv4Addr, TcpListener},
    str::FromStr,
    sync::mpsc::{Receiver, Sender},
};

use crate::messages::{InternodeMessage, InternodeMessageContent};

pub struct InternodeLink {
    /// Receiving stub from where we will receive messages to forward to other nodes
    rx: Receiver<InternodeMessage>,
    /// Sending stub to send messages received from other nodes
    tx: Sender<InternodeMessage>,
    // open connections
}

impl InternodeLink {
    pub fn new(rx: Receiver<InternodeMessage>, tx: Sender<InternodeMessage>) -> Self {
        Self { rx, tx }
    }

    pub fn start(&self) {
        // two threads, one for sending and one for receiving
        // todo!()

        let socket = TcpListener::bind("0.0.0.0:9999").unwrap();

        for stream in socket.incoming() {
            let mut stream = stream.unwrap();
            let mut buffer = [0; 1024];

            stream.read(&mut buffer).unwrap();

            self.tx
                .send(InternodeMessage::new(
                    IpAddr::V4(Ipv4Addr::from_str("0.0.0.0").unwrap()),
                    InternodeMessageContent::Dummy("recibí algo".to_string()),
                ))
                .unwrap();
        }

        // for msg in &self.rx {
        //     let mut stream = TcpStream::connect("node1:9999").unwrap();
        //     stream.write(b"you are the f host!").unwrap();
        // }
    }
}
