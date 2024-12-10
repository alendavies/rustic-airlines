use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::mpsc::{self, Sender},
    thread,
};

use native_protocol::{frame::Frame, Serializable};
use query_creator::{clauses::select_cql::Select, CreateClientResponse, Query, QueryCreator};

pub struct ClientResponse;

#[derive(Debug)]
pub enum ClientRequestEnum {
    Select { keyspace: String, query: Select },
}

#[derive(Debug)]
pub struct ClientRequest {
    pub query: ClientRequestEnum,
    pub reply_channel: Sender<ClientResponse>,
}

pub struct ClientsLink {
    // rx: Receiver<ClientResponse>,
    tx: Sender<ClientRequest>,
    // connections: Arc<RwLock<HashMap<SocketAddr, TcpStream>>>,
}

fn handle_connection(mut stream: TcpStream, tx: Sender<ClientRequest>) {
    let mut buffer = [0; 1024];
    let current_keyspace = None;

    while let Ok(n) = stream.read(&mut buffer) {
        if n == 0 {
            dbg!("Connection closed by the client.");
            break;
        }

        match Frame::from_bytes(&buffer[..n]).unwrap() {
            Frame::Startup => {
                let response = Frame::Ready.to_bytes().unwrap();
                println!("STARTUP from {:?}", &stream.peer_addr().unwrap());
                stream.write(&response).unwrap();
            }
            Frame::Query(query) => {
                dbg!(&query);

                // 1. Parse the query.
                let parsed_query = QueryCreator::new().handle_query(query.query).unwrap();

                // 2. Wrap it with the current keyspace.
                let q = match parsed_query {
                    Query::Select(select) => ClientRequestEnum::Select {
                        keyspace: current_keyspace.clone().unwrap_or_default(),
                        query: select,
                    },
                    _ => todo!(),
                };

                // 3. Open reply channel.
                let (tx_reply, rx_reply) = mpsc::channel();

                // 4. Create the client request to send to the node.
                let request = ClientRequest {
                    query: q,
                    reply_channel: tx_reply,
                };

                // 5. Send the request to the node.
                tx.send(request).unwrap();

                // 6. Await for response from the node.
                let response = rx_reply.recv().unwrap();

                // 7. Create response for the client.
                let response =
                    Frame::Result(native_protocol::messages::result::result_::Result::Void)
                        .to_bytes()
                        .unwrap();

                // 8. Send response to client.
                stream.write(&response).unwrap();
            }
            _ => todo!(),
        }
    }
}

impl ClientsLink {
    pub fn new(tx: Sender<ClientRequest>) -> Self {
        Self { tx }
    }

    pub fn start(self) {
        let socket = TcpListener::bind("0.0.0.0:9998").unwrap();

        for stream in socket.incoming() {
            let stream = stream.unwrap();

            let tx_clone = self.tx.clone();

            thread::spawn(move || {
                handle_connection(stream, tx_clone);
            });
        }
    }
}
