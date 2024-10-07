use std::env;
use std::net::Ipv4Addr;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

// Asegúrate de que Node esté disponible desde la librería "node"
use node::Node; // Esto asume que Node está definido en el crate "node"
fn main() -> Result<(), String> {
    let args: Vec<String> = env::args().collect();

    if args.len() != 2 {
        return Err("Uso: programa <ip_Node>".to_string());
    }

    let node_ip = Ipv4Addr::from_str(&args[1]).map_err(|_| "IP no válida".to_string())?;
    let node = Arc::new(Mutex::new(Node::new(node_ip, vec![Ipv4Addr::from_str("127.0.0.1").expect("No se pudo crear la semilla")]).unwrap()));
    let connections = Arc::new(Mutex::new(Vec::new()));
    Node::start(Arc::clone(&node), 8080, Arc::clone(&connections)).map_err(|e| e.to_string())?;
    Ok(())
}