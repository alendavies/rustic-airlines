use std::{
    io::{Read, Write},
    net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener, TcpStream},
    str::FromStr,
    sync::mpsc::{Receiver, Sender},
    thread,
};

use gossip::messages::GossipMessage;

use crate::{
    messages::{InternodeMessage, InternodeMessageContent},
    Serializable,
};

pub struct InternodeLink {
    /// Receiving stub from where we will receive messages to forward to other nodes
    rx: Receiver<InternodeMessageWithDestinationAddress>,
    /// Sending stub to send messages received from other nodes
    tx: Sender<InternodeMessageWithOriginAddress>,
    // open connections
}

#[derive(Debug)]
pub struct InternodeMessageWithOriginAddress {
    pub message: InternodeMessage,
    pub from: IpAddr,
}

#[derive(Debug)]
pub struct InternodeMessageWithDestinationAddress {
    pub message: InternodeMessage,
    pub to: IpAddr,
}

impl InternodeLink {
    pub fn new(
        rx: Receiver<InternodeMessageWithDestinationAddress>,
        tx: Sender<InternodeMessageWithOriginAddress>,
    ) -> Self {
        Self { rx, tx }
    }

    pub fn start(self) {
        // two threads, one for sending and one for receiving
        // todo!()

        let tx_clone = self.tx.clone();

        let writer = thread::spawn(move || {
            for msg in &self.rx {
                //println!("Internode link sending: {:?} to {:?}", msg.message, msg.to);

                if let Some(mut s) = TcpStream::connect(SocketAddr::new(msg.to, 9999)).ok() {
                    s.write(&msg.message.as_bytes()).unwrap();
                }
            }
        });

        let reader = thread::spawn(move || {
            let socket = TcpListener::bind("0.0.0.0:9999").unwrap();

            for stream in socket.incoming() {
                let mut stream = stream.unwrap();
                let mut buffer = [0; 1024];

                match stream.read(&mut buffer) {
                    Ok(0) => {}
                    Ok(n) => {
                        let msg = InternodeMessage::from_bytes(&buffer[..n]).unwrap();
                        let msg = InternodeMessageWithOriginAddress {
                            message: msg,
                            from: stream.peer_addr().unwrap().ip(),
                        };
                        //println!("Internode link received: {:?}.", &msg);

                        tx_clone.send(msg).unwrap();
                    }
                    Err(_) => todo!(),
                }

                //let bytes_read = stream.read(&mut buffer).unwrap();
                //let string = String::from_utf8(buffer[..bytes_read].to_vec()).unwrap();
                //println!("Internode link received: {:?}.", &string);
            }
        });

        reader.join().unwrap();
        writer.join().unwrap();
    }
}
