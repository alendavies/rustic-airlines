use std::{
    collections::HashMap,
    io::{Read, Write},
    net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream},
    sync::{
        mpsc::{Receiver, Sender},
        Arc, RwLock,
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
    connections: Arc<RwLock<HashMap<SocketAddr, TcpStream>>>,
}

impl ClientsLink {
    pub fn new(rx: Receiver<ClientResponse>, tx: Sender<ClientRequest>) -> Self {
        Self {
            rx,
            tx,
            connections: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn start(self) {
        let arc_tx = Arc::new(self.tx);
        let arc_conns = self.connections.clone();

        let listener_thread = thread::spawn(move || {
            let socket = TcpListener::bind("0.0.0.0:9998").unwrap();

            for stream in socket.incoming() {
                let mut stream = stream.unwrap();
                let mut buffer = [0; 1024];

                let read = stream.read(&mut buffer);

                dbg!(&read);

                match read {
                    Ok(0) => {}
                    Ok(n) => match Frame::from_bytes(&buffer[..n]).unwrap() {
                        Frame::Startup => {
                            let response = Frame::Ready.to_bytes().unwrap();
                            println!("STARTUP from {:?}", &stream.peer_addr().unwrap());
                            let arc_conns_clone = arc_conns.clone();
                            let arc_tx_clone = arc_tx.clone();

                            thread::spawn(move || {
                                let mut buffer = [0; 1024];
                                stream.write(&response).unwrap();

                                let read = stream.read(&mut buffer);

                                match read {
                                    Ok(0) => todo!(),
                                    Ok(n) => {
                                        // 1. send the query to the node
                                        match Frame::from_bytes(&buffer[..n]).unwrap() {
                                            Frame::Query(query) => {
                                                dbg!(&query);

                                                // dummy query
                                                let parsed_query = QueryCreator::new()
                                                    .handle_query(query.query)
                                                    .unwrap();

                                                let request = match parsed_query {
                                                    Query::Select(select) => {
                                                        ClientRequest::Select {
                                                            keyspace: "dummy_keyspace".to_string(),
                                                            query: select,
                                                        }
                                                    }
                                                    _ => todo!(),
                                                };

                                                arc_tx_clone.send(request).unwrap();
                                            }
                                            _ => todo!(),
                                        }

                                        // 2. save the stream for future reuse
                                        arc_conns_clone
                                            .write()
                                            .unwrap()
                                            .insert(stream.peer_addr().unwrap(), stream);
                                    }
                                    Err(_) => todo!(),
                                }
                            });
                        }
                        _ => todo!(),
                        // Frame::Query(query) => {
                        // }
                        _ => todo!(),
                    },
                    Err(_) => panic!(),
                }
            }
        });

        listener_thread.join().unwrap();
    }
}
