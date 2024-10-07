use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::net::{Ipv4Addr, TcpListener, TcpStream, SocketAddrV4};
use std::path::Path;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::thread;
use partitioner::Partitioner;
use query_coordinator::clauses::insert_sql::Insert;
use query_coordinator::QueryCoordinator;
use query_coordinator::Query;
mod query_execution;
use query_execution::QueryExecution;
mod errors;
use errors::NodeError;

pub struct Node {
    ip: Ipv4Addr,
    seeds_node: Vec<Ipv4Addr>,
    port: u16,
    partitioner: Partitioner,
}

impl Node {
    pub fn new(ip: Ipv4Addr, seeds_node: Vec<Ipv4Addr>) -> Result<Node, NodeError> {
        let mut partitioner = Partitioner::new();
        partitioner.add_node(ip)?;
        Ok(Node {
            ip,
            seeds_node,
            port: 0,
            partitioner,
        })
    }

    pub fn get_ip(&self) -> Ipv4Addr {
        self.ip
    }

    pub fn is_seed(&self) -> bool {
        self.seeds_node.contains(&self.get_ip())
    }

    pub fn get_partitioner(&self) -> Partitioner {
        self.partitioner.clone()
    }

    pub fn start(
        node: Arc<Mutex<Node>>,
        port: u16,
        connections: Arc<Mutex<Vec<TcpStream>>>,
    ) -> Result<(), NodeError> {
        let address = {
            let mut node_guard = node.lock()?;
            node_guard.port = port;
            SocketAddrV4::new(node_guard.ip, port)
        };

        let is_seed = node.lock()?.is_seed();
        let seed_ip = node.lock()?.seeds_node[0];

        {
            let mut node_guard = node.lock()?;
            if !is_seed {
                println!("El nodo NO es semilla");
                if let Ok(mut stream) = node_guard.connect(node_guard.seeds_node[0], Arc::clone(&connections)) {
                    let message = format!("IP {}", node_guard.ip.to_string());
                    node_guard.send_message(&mut stream, &message)?;
                    node_guard.partitioner.add_node(seed_ip)?;
                }
            } else {
                println!("El Nodo ES semilla");
            }
        }

        node.lock()?.setup_keyspaces()?;
        let listener = TcpListener::bind(address)?;

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let mut connections_guard = connections.lock()?;
                    connections_guard.push(stream.try_clone()?);

                    let node_clone = Arc::clone(&node);
                    let stream_clone = stream.try_clone()?;
                    let connections_clone = Arc::clone(&connections);

                    thread::spawn(move || {
                        if let Err(e) = Node::handle_incoming_messages(node_clone, stream_clone, connections_clone, is_seed) {
                            eprintln!("Error handling incoming message: {:?}", e);
                        }
                    });
                }
                Err(e) => {
                    eprintln!("Error al aceptar conexión: {:?}", e);
                }
            }
        }
        Ok(())
    }

    pub fn setup_keyspaces(&self) -> Result<(), NodeError> {
        let ip_str = self.ip.to_string().replace(".", "_");
        let base_dir = format!("keyspaces_{}", ip_str);

        if !Path::new(&base_dir).exists() {
            fs::create_dir(&base_dir)?;
        }

        let keyspace_dir = format!("{}/PLANES", base_dir);
        if !Path::new(&keyspace_dir).exists() {
            fs::create_dir(&keyspace_dir)?;
        }

        let table_file = format!("{}/airports.csv", keyspace_dir);
        if !Path::new(&table_file).exists() {
            fs::write(&table_file, "id,name,location\n")?;
        }

        Ok(())
    }

    pub fn connect(&self, peer_ip: Ipv4Addr, connections: Arc<Mutex<Vec<TcpStream>>>) -> Result<TcpStream, NodeError> {
        let address = SocketAddrV4::new(peer_ip, self.port);
        let stream = TcpStream::connect(address)?;
        {
            let mut connections_guard = connections.lock()?;
            connections_guard.push(stream.try_clone()?);
        }
        Ok(stream)
    }

    pub fn send_message(&self, stream: &mut TcpStream, message: &str) -> Result<(), NodeError> {
        stream.write_all(message.as_bytes())?;
        stream.write_all(b"\n")?;
        Ok(())
    }

    pub fn handle_incoming_messages(
        node: Arc<Mutex<Node>>,
        stream: TcpStream,
        connections: Arc<Mutex<Vec<TcpStream>>>,
        is_seed: bool,
    ) -> Result<(), NodeError> {
        let mut reader = BufReader::new(stream.try_clone().map_err(NodeError::IoError)?);
        let mut buffer = String::new();

        loop {
           
            Node::execute_initial_insert(node.clone(), connections.clone())?;
            
            buffer.clear();
            let bytes_read = reader.read_line(&mut buffer).map_err(NodeError::IoError)?;
            if bytes_read == 0 {
                println!("Conexión cerrada por el peer.");
                break;
            }

            let tokens: Vec<&str> = buffer.trim().split_whitespace().collect();
            if tokens.is_empty() {
                continue;
            }

            let command = tokens[0];
            match command {
                "IP" => Node::handle_ip_command(&node, tokens, connections.clone(), is_seed)?,
                "INSERT" => Node::handle_insert_command(&node, tokens, connections.clone())?,
                _ => println!("Comando desconocido: {}", command),
            }
        }

        Ok(())
    }

    // Función para verificar si el particionador está lleno y el nodo es una semilla
    fn initial_condition(node: &Arc<Mutex<Node>>) -> Result<bool, NodeError> {
        let lock_node = node.lock().map_err(|_| NodeError::LockError)?;
        Ok(lock_node.get_partitioner().get_nodes().len() == 4 && lock_node.is_seed())
    }

   // Función para ejecutar múltiples inserciones iniciales cuando el particionador está lleno
fn execute_initial_insert(node: Arc<Mutex<Node>>, connections: Arc<Mutex<Vec<TcpStream>>>) -> Result<(), NodeError> {
    if !Node::initial_condition(&node)? {
        return Ok(());
    }
    
    let queries = vec![
        "INSERT INTO airports (id, name, location) VALUES (1, 'Express Airport', 'New York')",
        "INSERT INTO airports (id, name, location) VALUES (2, 'Skyway International', 'Los Angeles')",
        "INSERT INTO airports (id, name, location) VALUES (3, 'Oceanview Airport', 'Miami')",
        "INSERT INTO airports (id, name, location) VALUES (4, 'Mountain Top Airfield', 'Denver')",
        "INSERT INTO airports (id, name, location) VALUES (5, 'Central Hub', 'Chicago')",
        "INSERT INTO airports (id, name, location) VALUES (6, 'Desert Sky', 'Phoenix')",
        "INSERT INTO airports (id, name, location) VALUES (7, 'Lakeside Gateway', 'Minneapolis')",
        "INSERT INTO airports (id, name, location) VALUES (8, 'Bay Area Field', 'San Francisco')",
        "INSERT INTO airports (id, name, location) VALUES (9, 'Riverbend Airport', 'Memphis')",
        "INSERT INTO airports (id, name, location) VALUES (10, 'Hilltop Airstrip', 'Austin')",
        "INSERT INTO airports (id, name, location) VALUES (11, 'Forest Glade Airpark', 'Seattle')",
        "INSERT INTO airports (id, name, location) VALUES (12, 'Sunshine Terminal', 'Orlando')",
        "INSERT INTO airports (id, name, location) VALUES (13, 'Windy Plains', 'Kansas City')",
        "INSERT INTO airports (id, name, location) VALUES (14, 'Northern Lights Airport', 'Anchorage')",
        "INSERT INTO airports (id, name, location) VALUES (15, 'Golden Gate Airfield', 'San Francisco')",
    ];

    for query_str in queries {
        let query = QueryCoordinator::new()
            .handle_query(query_str.to_string())
            .map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections.clone()).execute(query)?;
    }

    Ok(())
}


    // Función para manejar el comando "IP"
    fn handle_ip_command(
        node: &Arc<Mutex<Node>>,
        tokens: Vec<&str>,
        connections: Arc<Mutex<Vec<TcpStream>>>,
        is_seed: bool,
    ) -> Result<(), NodeError> {

        let new_ip = Ipv4Addr::from_str(tokens.get(1).ok_or(NodeError::OtherError)?)
            .map_err(|_| NodeError::OtherError)?;
        let mut lock_node = node.lock().map_err(|_| NodeError::LockError)?;
        let self_ip = lock_node.get_ip();

        if self_ip != new_ip && !lock_node.partitioner.contains_node(&new_ip) {
            lock_node.partitioner.add_node(new_ip)?;
        }

        if is_seed {
            for ip in lock_node.get_partitioner().get_nodes() {
                if new_ip != ip && self_ip != ip {
                    lock_node.forward_message(connections.clone(), new_ip, ip)?;
                    lock_node.forward_message(connections.clone(), ip, new_ip)?;
                }
            }
        }

        Ok(())
    }

    // Función para manejar el comando "INSERT"
    fn handle_insert_command(
        node: &Arc<Mutex<Node>>,
        tokens: Vec<&str>,
        connections: Arc<Mutex<Vec<TcpStream>>>,
    ) -> Result<(), NodeError> {
        let query_str = tokens.get(1..).ok_or(NodeError::OtherError)?.join(" ");
        let query = Insert::deserialize(&query_str).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections).execute(Query::Insert(query))
    }

    fn forward_message(
        &self,
        connections: Arc<Mutex<Vec<TcpStream>>>,
        new_ip: Ipv4Addr,
        target_ip: Ipv4Addr,
    ) -> Result<(), NodeError> {
        let mut tcp = self.connect(target_ip, Arc::clone(&connections))?;
        let message = format!("IP {}", new_ip);
        self.send_message(&mut tcp, &message)?;
        Ok(())
    }
}
