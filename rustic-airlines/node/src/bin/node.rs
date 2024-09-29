use std::{env, vec};
use std::io::{BufRead, BufReader, Write};
use std::net::{Ipv4Addr, TcpListener, TcpStream, SocketAddrV4};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::thread;

struct Node {
    ip: Ipv4Addr,
    nodes_that_knows: Vec<Ipv4Addr>,
    seeds_node: Vec<Ipv4Addr>
}

impl Node {
    /// Constructor para crear un Node a partir de un `String` o `&str` que representa una IP
    pub fn new(ip: Ipv4Addr) -> Node {
        Node {
            ip, 
            nodes_that_knows: vec![], 
            seeds_node: vec![Ipv4Addr::from_str("127.0.0.1").expect("No se pudo crear la semilla")]
        }
    }

    /// Método para obtener la IP del Node
    pub fn get_ip(&self) -> Ipv4Addr {
        self.ip
    }

    pub fn is_seed(&self) -> bool {
        return self.seeds_node.contains(&self.get_ip())
    }

    /// Función para iniciar el servidor en una dirección IPv4 y escuchar conexiones entrantes.
    pub fn start(node: Arc<Mutex<Node>>, port: u16, connections: Arc<Mutex<Vec<TcpStream>>>) -> std::io::Result<()> {
        let address;
        {
            // Obtener la IP dentro de un bloque que bloquee el mutex
            let node_guard = node.lock().unwrap();
            address = SocketAddrV4::new(node_guard.ip, port);
        }

        // Si el nodo no es semilla, conectarse al nodo semilla y enviar mensaje
        {
            let node_guard = node.lock().unwrap();

            if !node_guard.is_seed() {
                println!("El nodo NO es semilla");

                if let Ok(mut stream) = node_guard.connect(node_guard.seeds_node[0], port, Arc::clone(&connections)) {

                    let message = node_guard.ip.to_string();
                    node_guard.send_message(&mut stream, &message)?;

                } else {
                    println!("No se pudo conectar al nodo semilla");
                }
            }else{
                println!("El Nodo ES semilla");

            }
        }

        println!("Node escuchando en {}", address);
        let listener = TcpListener::bind(address)?;
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let mut connections_guard = connections.lock().unwrap();
                    connections_guard.push(stream.try_clone()?);

                    // Clonamos el Arc para el nodo y conexiones
                    let node_clone = Arc::clone(&node);
                    let stream_clone = stream.try_clone()?;
                    thread::spawn(move || {
                        Node::handle_incoming_messages(node_clone, stream_clone).expect("Error al manejar mensajes entrantes");
                    });
                }
                Err(e) => {
                    eprintln!("Error al aceptar conexión: {:?}", e);
                }
            }
        }
        Ok(())
    }

    /// Función para conectarse a otro Node usando Ipv4Addr.
    pub fn connect(&self, peer_ip: Ipv4Addr, port: u16, connections: Arc<Mutex<Vec<TcpStream>>>) -> std::io::Result<TcpStream> {
        let address = SocketAddrV4::new(peer_ip, port);
        let stream = TcpStream::connect(address)?;
        {
            let mut connections_guard = connections.lock().unwrap();
            connections_guard.push(stream.try_clone()?);
        }

        Ok(stream)
    }

    /// Función para enviar mensajes a otro Node.
    pub fn send_message(&self, stream: &mut TcpStream, message: &str) -> std::io::Result<()> {
        stream.write_all(message.as_bytes())?;
        stream.write_all(b"\n")?;
        Ok(())
    }
/// Función para manejar los mensajes entrantes de otro Node.
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
        // Imprimir las direcciones de conexión
        println!("Ip que se comunico conmigo: {}", client_ip);
        
        
        // Verificar si el nodo es semilla y que el peer no sea el propio nodo
        if lock_node.get_ip() != client_ip {
            if !lock_node.nodes_that_knows.contains(&client_ip) {
                lock_node.nodes_that_knows.push(client_ip);
                println!("Ahora conozco al nodo {}", client_ip);
            }
        }

        println!("Todos los nodos que conozco = {:?}", lock_node.nodes_that_knows);
        // Aquí podrías agregar lógica adicional para manejar los mensajes
    }

    Ok(())
}


}

fn main() -> Result<(), String> {
    // Obtener los argumentos de la línea de comandos
    let args: Vec<String> = env::args().collect();

    if args.len() != 2 {
        return Err("Uso: programa <ip_Node>".to_string());
    }

    let node_ip = Ipv4Addr::from_str(&args[1]).map_err(|_| "IP no válida".to_string())?;
    let node = Arc::new(Mutex::new(Node::new(node_ip)));

    // Crear una lista de conexiones compartida entre hilos
    let connections = Arc::new(Mutex::new(Vec::new()));

    // Iniciar el nodo en el puerto 8080
    Node::start(Arc::clone(&node), 8080, Arc::clone(&connections)).map_err(|e| e.to_string())?;

    Ok(())
}
