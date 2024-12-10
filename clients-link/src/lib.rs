use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::{
        mpsc::{Receiver, Sender},
        Arc,
    },
    thread,
};

use native_protocol::{frame::Frame, Serializable};
use query_creator::{clauses::select_cql::Select, Query, QueryCreator};

pub struct ClientResponse;

#[derive(Debug)]
pub enum ClientRequest {
    Select { keyspace: String, query: Select },
}

pub struct ClientsLink {
    rx: Receiver<ClientResponse>,
    tx: Sender<ClientRequest>,
}

impl ClientsLink {
    pub fn new(rx: Receiver<ClientResponse>, tx: Sender<ClientRequest>) -> Self {
        Self { rx, tx }
    }

    pub fn start(self) {
        let arc_tx = Arc::new(self.tx);

        let listener_thread = thread::spawn(move || {
            let socket = TcpListener::bind("0.0.0.0:9998").unwrap();

            for stream in socket.incoming() {
                let mut stream = stream.unwrap();
                let mut buffer = [0; 1024];

                match stream.read(&mut buffer) {
                    Ok(0) => {}
                    Ok(n) => match Frame::from_bytes(&buffer[..n]).unwrap() {
                        Frame::Startup => {
                            let response = Frame::Ready.to_bytes().unwrap();
                            println!("STARTUP from {:?}", &stream.peer_addr().unwrap());

                            stream.write(&response).unwrap();
                            stream.flush().unwrap();
                        }
                        Frame::Query(query) => {
                            dbg!(&query);

                            // dummy query
                            let parsed_query =
                                QueryCreator::new().handle_query(query.query).unwrap();

                            let request = match parsed_query {
                                Query::Select(select) => ClientRequest::Select {
                                    keyspace: "dummy_keyspace".to_string(),
                                    query: select,
                                },
                                _ => todo!(),
                            };

                            arc_tx.send(request).unwrap();
                        }
                        _ => todo!(),
                    },
                    Err(_) => panic!(),
                }
            }
        });

        listener_thread.join().unwrap();
    }
}
