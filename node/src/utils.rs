use crate::errors::NodeError;
use crate::internode_protocol::message::InternodeMessage;
use crate::internode_protocol::InternodeSerializable;
use std::collections::HashMap;
use std::io::Write;
use std::net::{Ipv4Addr, SocketAddrV4, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Attempts to connect to a peer and send a message over the `TcpStream`.
///
/// If a connection to the peer already exists, it checks the connection status and tries to send
/// the message. If the connection is broken, it reconnects, updates the shared `HashMap`, and
/// attempts to resend the message. Ensures thread-safe access to the stream and the connections
/// map.
///
/// # Parameters
/// - `peer_id`: The IPv4 address of the peer to connect to.
/// - `port`: The port number for the connection.
/// - `connections`: A shared `HashMap` containing active connections.
/// - `message`: The message to send as a `&str`.
///
/// # Returns
/// A `Result` indicating success or failure, with `Ok(())` on success or `NodeError` on failure.
pub fn connect_and_send_message(
    peer_id: Ipv4Addr,
    port: u16,
    connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
    message: InternodeMessage,
) -> Result<(), NodeError> {
    let peer_socket = SocketAddrV4::new(peer_id, port);
    let peer_addr = peer_socket.to_string();

    // Intentar reutilizar una conexión existente
    if let Some(existing_stream) = {
        let connections_guard = connections.lock().map_err(|_| NodeError::LockError)?;
        connections_guard.get(&peer_addr).cloned()
    } {
        let mut stream_guard = existing_stream.lock().map_err(|_| NodeError::LockError)?;
        if stream_guard.write_all(&message.as_bytes()).is_err() {
            println!("Error al escribir en el stream");
            return Err(NodeError::IoError(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Error al escribir en el stream",
            )));
        }
        if stream_guard.flush().is_err() {
            println!("Error al hacer flush en el stream");
            return Err(NodeError::IoError(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Error al hacer flush en el stream",
            )));
        }
        return Ok(());
    }

    // Si no hay conexión, intentar conectar una vez
    let stream = TcpStream::connect((peer_id, port))
        .map_err(|e| {
            eprintln!("Error al intentar conectar con {:?}: {:?}", peer_addr, e);
            NodeError::IoError(e)
        })
        .unwrap();
    let stream = Arc::new(Mutex::new(stream));

    // Añadir la nueva conexión al HashMap
    {
        let mut connections_guard = connections.lock().map_err(|_| NodeError::LockError)?;
        connections_guard.insert(peer_addr.clone(), Arc::clone(&stream));
    }

    // Intentar enviar el mensaje a través de la nueva conexión
    {
        let mut stream_guard = stream.lock().map_err(|_| NodeError::LockError)?;
        stream_guard
            .write_all(&message.as_bytes())
            .map_err(|e| {
                eprintln!("Error al escribir en el stream: {:?}", e);
                NodeError::IoError(e)
            })
            .unwrap();
        stream_guard
            .flush()
            .map_err(|e| {
                eprintln!("Error al hacer flush en el stream: {:?}", e);
                NodeError::IoError(e)
            })
            .unwrap();
    }
    Ok(())
}
