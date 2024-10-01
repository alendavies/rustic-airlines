use std::{env, vec};
use std::io::{BufRead, BufReader, Write};
use std::net::{Ipv4Addr, TcpListener, TcpStream, SocketAddrV4};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::thread;
use partitioner::Partitioner;

struct Node {
    ip: Ipv4Addr,
    nodes_that_knows: Vec<Ipv4Addr>,
    seeds_node: Vec<Ipv4Addr>,
    port: u16,
    partitioner: Partitioner,
}

impl Node {
    pub fn new(ip: Ipv4Addr) -> Node {
        let mut partitioner = Partitioner::new();
        partitioner.add_node(ip);
        Node {
            ip,
            nodes_that_knows: vec![],
            seeds_node: vec![Ipv4Addr::from_str("127.0.0.1").expect("No se pudo crear la semilla")],
            port: 0,
            partitioner,
        }
    }

    pub fn get_ip(&self) -> Ipv4Addr {
        self.ip
    }

    pub fn is_seed(&self) -> bool {
        self.seeds_node.contains(&self.get_ip())
    }

    pub fn start(
        node: Arc<Mutex<Node>>,
        port: u16,
        connections: Arc<Mutex<Vec<TcpStream>>>,
    ) -> std::io::Result<()> {
        let address;
        {
            let mut node_guard = node.lock().unwrap();
            node_guard.port = port;
            address = SocketAddrV4::new(node_guard.ip, port);
        }

        let is_seed;
        {
            let node_guard = node.lock().unwrap();
            is_seed = node_guard.is_seed();
        }

        let seed_ip;
        {
            let node_guard = node.lock().unwrap();
            seed_ip = node_guard.seeds_node[0];
        }

        {
            let mut node_guard = node.lock().unwrap();
            if !is_seed {
                println!("El nodo NO es semilla");
                if let Ok(mut stream) = node_guard.connect(node_guard.seeds_node[0], Arc::clone(&connections)) {
                    node_guard.nodes_that_knows.push(seed_ip);
                    node_guard.partitioner.add_node(seed_ip);
                    let message = node_guard.ip.to_string();
                    node_guard.send_message(&mut stream, &message)?;
                }
            } else {
                println!("El Nodo ES semilla");
            }
        }

        let listener = TcpListener::bind(address)?;

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let mut connections_guard = connections.lock().unwrap();
                    connections_guard.push(stream.try_clone()?);

                    let node_clone = Arc::clone(&node);
                    let stream_clone = stream.try_clone()?;
                    let connections_clone = Arc::clone(&connections);

                    thread::spawn(move || {
                        if is_seed {
                            Node::handle_incoming_messages_like_seed(node_clone, stream_clone, Arc::clone(&connections_clone))
                                .expect("Error al manejar mensajes entrantes");
                        } else {
                            Node::handle_incoming_messages(node_clone, stream_clone)
                                .expect("Error al manejar mensajes entrantes");
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

    pub fn connect(&self, peer_ip: Ipv4Addr, connections: Arc<Mutex<Vec<TcpStream>>>) -> std::io::Result<TcpStream> {
        let address = SocketAddrV4::new(peer_ip, self.port);
        let stream = TcpStream::connect(address)?;
        {
            let mut connections_guard = connections.lock().unwrap();
            connections_guard.push(stream.try_clone()?);
        }
        Ok(stream)
    }

    pub fn send_message(&self, stream: &mut TcpStream, message: &str) -> std::io::Result<()> {
        stream.write_all(message.as_bytes())?;
        stream.write_all(b"\n")?;
        Ok(())
    }

    pub fn handle_incoming_messages(node: Arc<Mutex<Node>>, stream: TcpStream) -> std::io::Result<()> {
        let mut reader = BufReader::new(stream.try_clone()?);
        let mut buffer = String::new();

        loop {
            buffer.clear();
            let bytes_read = reader.read_line(&mut buffer)?;
            if bytes_read == 0 {
                println!("Conexión cerrada por el peer.");
                break;
            }

            let mut lock_node = node.lock().unwrap();
            let client_ip = Ipv4Addr::from_str(&buffer.trim()).unwrap();

            if lock_node.get_ip() != client_ip {
                if !lock_node.nodes_that_knows.contains(&client_ip) {
                    lock_node.nodes_that_knows.push(client_ip);
                    lock_node.partitioner.add_node(client_ip);
                }
            }
        }

        Ok(())
    }

    fn forward_message(
        &self,
        connections: Arc<Mutex<Vec<TcpStream>>>,
        new_ip: Ipv4Addr,
        target_ip: Ipv4Addr,
    ) -> std::io::Result<()> {
        let mut tcp = self.connect(target_ip, Arc::clone(&connections))?;
        self.send_message(&mut tcp, &new_ip.to_string())?;
        Ok(())
    }

    pub fn handle_incoming_messages_like_seed(
        node: Arc<Mutex<Node>>,
        stream: TcpStream,
        connections: Arc<Mutex<Vec<TcpStream>>>,
    ) -> std::io::Result<()> {
        let mut reader = BufReader::new(stream.try_clone()?);
        let mut buffer = String::new();

        loop {
            buffer.clear();
            let bytes_read = reader.read_line(&mut buffer)?;
            if bytes_read == 0 {
                println!("Conexión cerrada por el peer.");
                break;
            }

            let new_ip = Ipv4Addr::from_str(&buffer.trim()).unwrap();
            let nodes_that_knows;
            {
                let mut lock_node = node.lock().unwrap();
                if lock_node.get_ip() != new_ip {
                    if !lock_node.nodes_that_knows.contains(&new_ip) {
                        lock_node.nodes_that_knows.push(new_ip);
                        lock_node.partitioner.add_node(new_ip);
                    }
                }
                nodes_that_knows = lock_node.nodes_that_knows.clone();
            }

            for ip in &nodes_that_knows {
                if new_ip != *ip {
                    node.lock().unwrap().forward_message(Arc::clone(&connections), new_ip, *ip)?;
                    node.lock().unwrap().forward_message(Arc::clone(&connections), *ip, new_ip)?;
                }
            }
        }

        Ok(())
    }
}

fn main() -> Result<(), String> {
    let args: Vec<String> = env::args().collect();

    if args.len() != 2 {
        return Err("Uso: programa <ip_Node>".to_string());
    }

    let node_ip = Ipv4Addr::from_str(&args[1]).map_err(|_| "IP no válida".to_string())?;
    let node = Arc::new(Mutex::new(Node::new(node_ip)));
    let connections = Arc::new(Mutex::new(Vec::new()));
    Node::start(Arc::clone(&node), 8080, Arc::clone(&connections)).map_err(|e| e.to_string())?;
    Ok(())
}
