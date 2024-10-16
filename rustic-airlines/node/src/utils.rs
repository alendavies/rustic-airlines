use std::collections::HashMap;
use std::io::Write;
use std::net::{TcpStream, SocketAddrV4, Ipv4Addr};
use std::sync::{Arc, Mutex};
use crate::errors::NodeError;
/// Define el tipo de error de nodo para manejar los posibles errores en las funciones de red

/// Envía un mensaje a través de un TcpStream protegido por Arc<Mutex>
/// Retorna un Result para manejar posibles errores
pub fn send_message(stream: &Arc<Mutex<TcpStream>>, message: &str) -> Result<(), NodeError> {
    let mut stream_guard = stream.lock().map_err(|_| NodeError::LockError)?; // Bloquea el Mutex y obtiene el TcpStream
    if let Err(e) = stream_guard.write_all(message.as_bytes()) {
        println!("Error al enviar mensaje: {:?}. Eliminando conexión.", e);
        return Err(NodeError::IoError(e));
    }
    stream_guard.write_all(b"\n").map_err(NodeError::IoError)?;
    stream_guard.flush().map_err(NodeError::IoError)?; // Forzamos el envío inmediato del mensaje
    Ok(())
}

/// Conecta a un `peer_id` y `puerto`, agrega la conexión al HashMap de conexiones, y maneja errores
pub fn connect(
    peer_id: Ipv4Addr,
    port: u16,
    connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
) -> Result<Arc<Mutex<TcpStream>>, NodeError> {

    let peer_socket = SocketAddrV4::new(peer_id, port);
    let peer_addr = peer_socket.to_string();

    // // Intenta obtener la conexión si ya existe y está activa
    // {
    //     let mut connections_guard = connections.lock().map_err(|_| NodeError::LockError)?;
    //     if let Some(existing_stream) = connections_guard.get(&peer_addr) {
    //         let mut stream_guard = existing_stream.lock().map_err(|_| NodeError::LockError)?;
    //         if stream_guard.write(&[]).is_ok() {
    //             println!("Conexión existente reutilizada: {:?}", existing_stream);
    //             return Ok(Arc::clone(existing_stream)); // Devuelve la conexión existente si está activa
    //         } else {
    //             println!("Conexión existente inactiva, eliminando y reconectando...");
    //             connections_guard.remove(&peer_addr); // Elimina la conexión rota
    //         }
    //     }
    // }

    // Si no existe o está inactiva, crea una nueva conexión y agrégala al HashMap
    let stream = TcpStream::connect(peer_socket).map_err(NodeError::IoError)?;
    let stream = Arc::new(Mutex::new(stream));
    {
        let mut connections_guard = connections.lock().map_err(|_| NodeError::LockError)?;
        connections_guard.insert(peer_addr.clone(), Arc::clone(&stream));
    }
    Ok(stream)
}
