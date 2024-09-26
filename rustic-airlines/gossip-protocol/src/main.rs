use rand::Rng;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use std::{thread, time::Duration};
mod messages;

#[derive(Debug, Clone)]
struct Cluster {
    nodes: HashMap<String, Node>,
}

impl Cluster {
    fn new(nodes: Vec<Node>) -> Cluster {
        let mut cluster = Cluster {
            nodes: HashMap::new(),
        };

        for node in nodes {
            cluster.add_node(node);
        }

        cluster
    }

    fn add_node(&mut self, node: Node) {
        self.nodes.insert(node.ip.clone(), node);
    }

    fn remove_node(&mut self, ip: String) {
        self.nodes.remove(&ip);
    }
}

#[derive(Debug, Clone)]
struct Gossip {
    cluster: Cluster,
}

impl Gossip {
    fn new(cluster: Cluster) -> Arc<Mutex<Gossip>> {
        let gossip = Gossip { cluster: cluster };

        // Usa Arc<Mutex<Gossip>> para que sea mutable y compartido entre hilos
        let gossip_arc = Arc::new(Mutex::new(gossip));
        let gossip_clone = Arc::clone(&gossip_arc); // Clonamos el Arc para moverlo al hilo

        // Iniciar el thread
        thread::spawn(move || {
            loop {
                thread::sleep(Duration::from_secs(1));

                // Bloquea el mutex y accede a gossip de manera mutable
                let mut gossip = gossip_clone.lock().unwrap();
                gossip.start_protocol(); // Ejecuta el protocolo cada segundo
            }
        });

        // Devolver Arc<Mutex<Gossip>> para que pueda modificarse desde fuera
        gossip_arc
    }

    fn start_protocol(&mut self) {
        let mut rng = rand::thread_rng(); // Get a random number generator
        let len = self.cluster.nodes.len() as i32;
        let nodes_to_gossip: Vec<Node> = self
            .cluster
            .nodes
            .iter()
            .map(|(_, node)| node.clone())
            .collect();

        let amount_to_gossip: usize = rng.gen_range(1..4); // Random number between 1 and 3

        println!("NEW GOSSIP\n");
        for (_, node) in &self.cluster.nodes {
            let mut nodes_gossiped: Vec<&Node> = vec![];

            while nodes_gossiped.len() < amount_to_gossip {
                let node_to_connect: usize = rng.gen_range(0..len) as usize;

                if *node == nodes_to_gossip[node_to_connect] {
                    continue;
                }
                if nodes_gossiped.contains(&&nodes_to_gossip[node_to_connect]) {
                    continue;
                }
                nodes_gossiped.push(&nodes_to_gossip[node_to_connect]);
            }

            for node_to_gossip in nodes_gossiped {
                //self.connecct_nodes(node, node_to_gossip);
                println!(
                    "{:?} ({:?}) se conecto con {:?} ({:?})",
                    node.ip,
                    node.endpoint_state.heartbeat_state.lock().unwrap().version,
                    node_to_gossip.ip,
                    node_to_gossip
                        .endpoint_state
                        .heartbeat_state
                        .lock()
                        .unwrap()
                        .version
                )
            }
        }
        println!("\n");
    }

    // fn sort_neighbours(&mut self) {
    //     let mut rng = rand::thread_rng(); // Get a random number generator
    //     let len = self.nodes.len() as i32;

    //     for (id, node) in &mut self.nodes.clone() {
    //         let rand_num: i32 = rng.gen_range(0..len);

    //         if node.neighbours.len() == 2 {
    //             continue;
    //         }

    //         let neighbours: Vec<_> = self.nodes.iter().map(|(id, _)| id).collect();
    //         let new_neighbour = neighbours[rand_num as usize];

    //         if !node.neighbours.contains(&new_neighbour) && node.id != *new_neighbour {
    //             node.add_neighbour(*new_neighbour);
    //         }
    //     }
    // }
}

#[derive(Clone, Debug)]
struct Node {
    ip: String,
    endpoint_state: EndpointState,
}

impl Node {
    fn new(ip: String) -> Node {
        // let nodes = rand(Gossip.nodes);
        let endpoint_state = EndpointState::new();
        Node { ip, endpoint_state }
    }
}

impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        // Intentar obtener el lock y comparar los contenidos de los Mutex
        self.ip == other.ip
    }
}

#[derive(Debug, Clone)]
struct EndpointState {
    heartbeat_state: Arc<Mutex<HeartbeatState>>,
    application_state: AppState,
}

impl EndpointState {
    fn new() -> Self {
        let application_state = AppState::new();
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

// impl PartialEq for EndpointState {
//     fn eq(&self, other: &Self) -> bool {
//         // Intentar obtener el lock y comparar los contenidos de los Mutex
//         let self_heartbeat = self.heartbeat_state.lock().unwrap();
//         let other_heartbeat = other.heartbeat_state.lock().unwrap();

//         // Comparar el contenido de `heartbeat_state` y `application_state`
//         *self_heartbeat == *other_heartbeat && self.application_state == other.application_state
//     }
// }

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
        // dbg!(self.version);
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Status {
    Bootstrap,
    Normal,
    Leaving,
    Removing,
}

#[derive(Debug, Hash, Eq, PartialEq, Clone)]
pub enum ApplicationState {
    Status,
    Load,
    Schema,
    Dc,
    Rack,
    ReleaseVersion,
    RemovalCoordinator,
    Severity,
    NetVersion,
    HostId,
    Tokens,
    RpcReady,
    InternalAddressAndPort, // Replacement for INTERNAL_IP with up to two ports
    NativeAddressAndPort,   // Replacement for RPC_ADDRESS
}

#[derive(Debug, Clone)]
struct VersionedValue {
    value: f64,   // For example, node load or other metric
    version: u64, // The version of the information
}

#[derive(Debug, Clone)]
struct AppState {
    state_map: HashMap<ApplicationState, VersionedValue>,
}

impl AppState {
    fn new() -> Self {
        Self {
            state_map: HashMap::new(),
        }
    }
}

fn main() {
    let endpoint = EndpointState::new();
    //print!("{:?}\n", endpoint);

    let node_1 = Node::new(String::from("127.0.0.1"));
    let node_2 = Node::new(String::from("127.0.0.2"));
    let node_3 = Node::new(String::from("127.0.0.3"));
    let node_4 = Node::new(String::from("127.0.0.4"));
    let node_5 = Node::new(String::from("127.0.0.5"));
    let node_6 = Node::new(String::from("127.0.0.6"));
    let node_7 = Node::new(String::from("127.0.0.7"));

    let mut cluster = Cluster::new(vec![node_1, node_2, node_3, node_4, node_5, node_6]);

    let mut gossip = Gossip::new(cluster.clone());

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
