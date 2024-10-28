use crate::errors::NodeError;
use std::collections::HashMap;
use std::io::Write;
use std::net::{Ipv4Addr, SocketAddrV4, TcpStream};
use std::sync::{Arc, Mutex};

/// Sends a message over a `TcpStream` protected by an `Arc<Mutex<TcpStream>>`.
///
/// This function locks the stream using a mutex, writes the message as bytes, appends a newline,
/// and flushes the buffer to ensure the message is sent immediately. If any error occurs during
/// this process, the error is captured and returned as `NodeError`.
///
/// # Parameters
/// - `stream`: An `Arc<Mutex<TcpStream>>` protecting the stream for thread-safe access.
/// - `message`: The message to send, represented as a `&str`.
///
/// # Returns
/// A `Result` indicating success or failure. Returns `Ok(())` on success or `NodeError` on failure.
///
/// # Example
/// ```rust
/// let message = "Hello, node!";
/// send_message(&stream, message)?;
/// ```
pub fn send_message(stream: &Arc<Mutex<TcpStream>>, message: &str) -> Result<(), NodeError> {
    let mut stream_guard = stream.lock().map_err(|_| NodeError::LockError)?; // Lock the stream
    if let Err(e) = stream_guard.write_all(message.as_bytes()) {
        println!("Error sending message: {:?}. Removing connection.", e);
        return Err(NodeError::IoError(e));
    }
    // Append a newline and flush the stream to ensure the message is sent immediately
    stream_guard.write_all(b"\n").map_err(NodeError::IoError)?;
    stream_guard.flush().map_err(NodeError::IoError)?;
    Ok(())
}

/// Establishes a connection to a `peer_id` and `port`, and adds the connection to a shared `HashMap`.
///
/// This function connects to a remote peer at the specified `peer_id` and `port`. If the connection
/// is successful, the `TcpStream` is wrapped in an `Arc<Mutex<TcpStream>>` for thread-safe access
/// and added to the provided `connections` map. If the connection fails, the error is captured and
/// returned as a `NodeError`.
///
/// # Parameters
/// - `peer_id`: The IPv4 address of the peer to connect to.
/// - `port`: The port number to connect on.
/// - `connections`: An `Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>` that stores active connections.
///
/// # Returns
/// A `Result` containing an `Arc<Mutex<TcpStream>>` on success or `NodeError` on failure.
///
/// # Example
/// ```rust
/// let stream = connect(peer_ip, 8080, connections)?;
/// ```
pub fn connect(
    peer_id: Ipv4Addr,
    port: u16,
    connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
) -> Result<Arc<Mutex<TcpStream>>, NodeError> {
    let peer_socket = SocketAddrV4::new(peer_id, port);
    let peer_addr = peer_socket.to_string();

    // Connect to the peer
    let stream = TcpStream::connect(peer_socket).map_err(NodeError::IoError)?;
    let stream = Arc::new(Mutex::new(stream));
    {
        // Add the new connection to the shared HashMap
        let mut connections_guard = connections.lock().map_err(|_| NodeError::LockError)?;
        connections_guard.insert(peer_addr.clone(), Arc::clone(&stream));
    }
    Ok(stream)
}
