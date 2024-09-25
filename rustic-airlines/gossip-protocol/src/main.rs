use rand::Rng;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use std::{thread, time::Duration};

struct Gossip {
    nodes: HashMap<u32, Node>,
    last_id: u32,
}

impl Gossip {
    fn new() -> Gossip {
        Gossip {
            nodes: HashMap::new(),
            last_id: 0,
        }
    }

    fn add_node(&mut self, node: Node) {
        self.last_id += 1;
        self.nodes.insert(self.last_id, node);
        self.sort_neighbours();
    }

    fn sort_neighbours(&mut self) {
        let mut rng = rand::thread_rng(); // Get a random number generator
        let len = self.nodes.len() as i32;

        for (id, node) in &mut self.nodes.clone() {
            let rand_num: i32 = rng.gen_range(0..len);

            if node.neighbours.len() == 2 {
                continue;
            }

            let neighbours: Vec<_> = self.nodes.iter().map(|(id, _)| id).collect();
            let new_neighbour = neighbours[rand_num as usize];

            if !node.neighbours.contains(&new_neighbour) && node.id != *new_neighbour {
                node.add_neighbour(*new_neighbour);
            }
        }
    }
}

#[derive(PartialEq, Clone)]
struct Node {
    id: u32,
    neighbours: Vec<u32>,
    endpoint_state: EndpointState,
}

impl Node {
    fn new(id: u32) -> Node {
        // let nodes = rand(Gossip.nodes);
        let endpoint_state = EndpointState::new();
        Node {
            id,
            neighbours: vec![],
            endpoint_state,
        }
    }

    fn add_neighbour(&mut self, node_id: u32) {
        self.neighbours.push(node_id);
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

impl PartialEq for EndpointState {
    fn eq(&self, other: &Self) -> bool {
        // Intentar obtener el lock y comparar los contenidos de los Mutex
        let self_heartbeat = self.heartbeat_state.lock().unwrap();
        let other_heartbeat = other.heartbeat_state.lock().unwrap();

        // Comparar el contenido de `heartbeat_state` y `application_state`
        *self_heartbeat == *other_heartbeat && self.application_state == other.application_state
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct HeartbeatState {
    generation: SystemTime,
    version: u64,
}

impl HeartbeatState {
    fn new() -> Self {
        let timestamp = SystemTime::now();
        let version = 0;

        Self {
            generation: timestamp,
            version,
        }
    }

    fn inc_version(&mut self) {
        self.version += 1;
        dbg!(self.version);
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Status {
    Bootstrap,
    Normal,
    Leaving,
    Removing,
}

#[derive(Debug, Clone, PartialEq)]
struct ApplicationState {
    status: Status,
}

impl ApplicationState {
    fn new() -> Self {
        let status = Status::Bootstrap;
        Self { status }
    }
}

fn main() {
    let endpoint = EndpointState::new();
    print!("{:?}\n", endpoint);

    thread::sleep(Duration::from_secs(5));
}
