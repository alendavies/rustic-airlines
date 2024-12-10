use crate::errors::NodeError;
use crate::internode_protocol::message::InternodeMessage;
use crate::internode_protocol::InternodeSerializable;
use std::collections::HashMap;
use std::io::Write;
use std::net::{Ipv4Addr, SocketAddrV4, TcpStream};
use std::sync::{Arc, Mutex};

/// Attempts to connect to a peer and send a message over the `TcpStream`.
///
/// # Purpose
/// This function manages communication with a peer node in a distributed system.
/// It reuses existing connections when available, attempts to reconnect if a connection is broken,
/// and ensures thread-safe access to the shared connections map while sending the message.
///
/// # Parameters
/// - `peer_id: Ipv4Addr`
///   - The IPv4 address of the peer to connect to.
/// - `port: u16`
///   - The port number on which the peer is listening for incoming connections.
/// - `connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>`
///   - A thread-safe map of active TCP connections to other nodes.
///     - Keys are peer addresses (in `String` format).
///     - Values are `Arc<Mutex<TcpStream>>`, allowing thread-safe access and sharing of streams.
/// - `message: InternodeMessage`
///   - The message to send to the peer, serialized using the `InternodeSerializable` trait.
///
/// # Returns
/// - `Result<(), NodeError>`:
///   - Returns `Ok(())` on successful connection and message transmission.
///   - Returns `Err(NodeError)` if an error occurs during connection or message handling.
///
/// # Behavior
/// 1. **Existing Connection Handling**:
///    - Checks if a connection to the peer already exists in the `connections` map.
///    - If an existing connection is found:
///      - Acquires a lock on the `TcpStream` and attempts to send the message.
///      - Ensures the stream is flushed after writing.
///      - Returns `Err(NodeError::IoError)` if any errors occur during this process.
/// 2. **New Connection Handling**:
///    - If no existing connection is found, attempts to establish a new `TcpStream` connection to the peer.
///    - Adds the new connection to the `connections` map for future reuse.
///    - Sends the message through the newly established connection and ensures the stream is flushed.
/// 3. **Thread Safety**:
///    - Uses `Mutex` locks to ensure safe access to the shared `connections` map and individual streams.
///
/// # Errors
/// - Returns `NodeError::LockError` if the `Mutex` lock on the `connections` map or a `TcpStream` fails.
/// - Returns `NodeError::IoError` for I/O errors during connection, writing, or flushing operations.
///
/// # Notes
/// - **Efficient Reuse**:
///   - This function optimizes network usage by reusing existing connections where possible.
/// - **Logging**:
///   - Logs errors to `stderr` for debugging purposes but does not expose them in the return type.
/// - **Thread-Safe Design**:
///   - The function ensures thread safety for shared resources, making it suitable for concurrent environments.
///
/// # Importance
/// This function is critical for maintaining efficient and reliable communication between nodes in a distributed system.
/// By managing connections dynamically and reusing streams, it minimizes overhead and improves resilience to network issues.

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
            return Err(NodeError::IoError(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Error al escribir en el stream",
            )));
        }
        if stream_guard.flush().is_err() {
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
        .map_err(|e| e)?;

    let stream = Arc::new(Mutex::new(stream));

    // Añadir la nueva conexión al HashMap
    {
        let mut connections_guard = connections.lock().map_err(|_| NodeError::LockError)?;
        connections_guard.insert(peer_addr.clone(), Arc::clone(&stream));
    }

    // Intentar enviar el mensaje a través de la nueva conexión
    {
        let mut stream_guard = stream.lock().map_err(|_| NodeError::LockError)?;
        stream_guard.write_all(&message.as_bytes()).map_err(|e| {
            eprintln!("Error al escribir en el stream: {:?}", e);
            NodeError::IoError(e)
        })?;
        stream_guard.flush().map_err(|e| {
            eprintln!("Error al hacer flush en el stream: {:?}", e);
            NodeError::IoError(e)
        })?;
    }
    Ok(())
}
