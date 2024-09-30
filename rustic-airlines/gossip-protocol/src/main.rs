use messages::{Ack, Ack2, Digest, Syn};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use std::vec;
use std::{thread, time::Duration};
mod messages;

#[derive(Debug, Clone)]
struct Gossiper {
    endpoint_states: Arc<Mutex<HashMap<Ipv4Addr, EndpointState>>>,
}

impl Gossiper {
    fn new(endpoint_states: HashMap<Ipv4Addr, EndpointState>, ip: Ipv4Addr) -> Arc<Self> {
        let gossiper = Arc::new(Self {
            endpoint_states: Arc::new(Mutex::new(endpoint_states)),
        });

        let gossiper_clone = Arc::clone(&gossiper);
        thread::spawn(move || loop {
            gossiper_clone.send(ip);
            thread::sleep(Duration::from_secs(1));
        });

        let gossiper_clone = Arc::clone(&gossiper);
        thread::spawn(move || loop {
            gossiper_clone.listen(ip);
            thread::sleep(Duration::from_secs(1));
        });

        gossiper
    }

    fn send(&self, ip: Ipv4Addr) {
        let mut socket = TcpStream::connect(SocketAddr::new(IpAddr::V4(ip), 8080)).unwrap();

        let digests: Vec<Digest> = {
            let states = self.endpoint_states.lock().unwrap();
            states
                .iter()
                .map(|(ip, state)| {
                    let heartbeat_state = state.heartbeat_state.lock().unwrap();
                    Digest::new(*ip, heartbeat_state.generation, heartbeat_state.version)
                })
                .collect()
        };

        let syn = Syn::new(digests);

        // send syn
        socket.write(&syn.as_bytes()).unwrap();

        let mut buff = [0; 2048];

        // wait ack
        socket.read(&mut buff).unwrap();

        let ack = Ack::from_bytes(buff.to_vec()).unwrap();

        // comparar lo que vino en el ack con lo que tengo
        // actualizar lo que tengo con lo que vino en el ack
        for ack_digest in ack.stale_digests {
            let mut states = self.endpoint_states.lock().unwrap();

            let version = states
                .get(&ack_digest.address)
                .unwrap()
                .heartbeat_state
                .lock()
                .unwrap()
                .version;

            let generation = states
                .get(&ack_digest.address)
                .unwrap()
                .heartbeat_state
                .lock()
                .unwrap()
                .generation;

            if version < ack_digest.version {
                // actualizar
                states
                    .get(&ack_digest.address)
                    .unwrap()
                    .heartbeat_state
                    .lock()
                    .unwrap()
                    .version = ack_digest.version;
            }
            if ack_digest.generation < generation {
                // actualizar
                states
                    .get(&ack_digest.address)
                    .unwrap()
                    .heartbeat_state
                    .lock()
                    .unwrap()
                    .generation = ack_digest.generation;
            }
        }

        // crear ack2 con la info pedida
        let updated_info = HashMap::new();

        let ack2 = Ack2::new(updated_info);

        socket.write(&ack2.as_bytes()).unwrap();
    }

    fn listen(&self, ip: Ipv4Addr) {
        let mut socket = TcpStream::connect(SocketAddr::new(IpAddr::V4(ip), 8080)).unwrap();

        let mut buff = [0; 2048];

        // wait syn
        socket.read(&mut buff).unwrap();

        let syn = Syn::from_bytes(buff.to_vec()).unwrap();

        // comparar lo que vino en el syn con lo que tengo

        for digest in syn.digests {
            let states = self.endpoint_states.lock().unwrap();

            let version = states
                .get(&digest.address)
                .unwrap()
                .heartbeat_state
                .lock()
                .unwrap()
                .version;

            let generation = states
                .get(&digest.address)
                .unwrap()
                .heartbeat_state
                .lock()
                .unwrap()
                .generation;

            // comparar y actualizar
        }

        let digests = vec![];
        let updated_info = HashMap::new();

        // send ack con la info pedida
        socket
            .write(&Ack::new(digests, updated_info).as_bytes())
            .unwrap();

        // wait ack2
        socket.read(&mut buff).unwrap();

        let ack2 = Ack2::from_bytes(buff.to_vec()).unwrap();

        // comparar lo que vino en el ack2 con lo que tengo
        // actualizar lo que tengo con lo que vino en el ack2

        for info in ack2.updated_info {
            // actualizar
        }
    }
}

#[derive(Clone, Debug)]
struct Node {
    ip: Ipv4Addr,
    endpoint_states: HashMap<Ipv4Addr, EndpointState>,
}

impl Node {
    fn new(ip: Ipv4Addr) -> Node {
        let endpoint_states = HashMap::new();
        let gossiper = Gossiper::new(endpoint_states, ip);
        Node {
            ip,
            endpoint_states: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
struct EndpointState {
    heartbeat_state: Arc<Mutex<HeartbeatState>>,
    application_state: ApplicationState,
}

impl EndpointState {
    fn new() -> Self {
        let application_state = ApplicationState::new();
        let heartbeat_state = Arc::new(Mutex::new(HeartbeatState::new()));

        let cloned = heartbeat_state.clone();

        thread::spawn(move || {
            loop {
                thread::sleep(Duration::from_secs(1));
                let mut state = cloned.lock().unwrap(); // Incrementa cada segundo
                state.inc_version();
            }
        });

        Self {
            heartbeat_state,
            application_state,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct HeartbeatState {
    generation: u128,
    version: u32,
}

impl HeartbeatState {
    fn new() -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let version = 0;

        Self {
            generation: timestamp,
            version,
        }
    }

    fn inc_version(&mut self) {
        self.version += 1;
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Status {
    Bootstrap,
    Normal,
    Leaving,
    Removing,
}

#[derive(Debug, Clone)]
struct ApplicationState {
    status: Status,
    version: u32,
}

impl ApplicationState {
    fn new() -> Self {
        Self {
            status: Status::Bootstrap,
            version: 0,
        }
    }
}

fn main() {
    let endpoint = EndpointState::new();

    let mut node_1 = Node::new(String::from("127.0.0.1"));
    let mut node_2 = Node::new(String::from("127.0.0.2"));
    let mut node_3 = Node::new(String::from("127.0.0.3"));
    let mut node_4 = Node::new(String::from("127.0.0.4"));
    let mut node_5 = Node::new(String::from("127.0.0.5"));
    let mut node_6 = Node::new(String::from("127.0.0.6"));
    let mut node_7 = Node::new(String::from("127.0.0.7"));

    // let mut cluster = Cluster::new(vec![node_1, node_2, node_3, node_4, node_5, node_6]);

    let mut gossip = Gossiper::new(HashMap::new());

    thread::sleep(Duration::from_secs(2));
    gossip.lock().unwrap().cluster.add_node(node_7);
    //println!("{:?}", gossip.lock().unwrap().cluster);
    thread::sleep(Duration::from_secs(2));
    gossip
        .lock()
        .unwrap()
        .cluster
        .remove_node(String::from("127.0.0.1"));
    gossip
        .lock()
        .unwrap()
        .cluster
        .remove_node(String::from("127.0.0.2"));
    gossip
        .lock()
        .unwrap()
        .cluster
        .remove_node(String::from("127.0.0.3"));
    thread::sleep(Duration::from_secs(2));
    thread::sleep(Duration::from_secs(2));
}
