use crate::errors::NodeError;
use crate::messages::InternodeMessage;
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

    // Attempt to retrieve and use an existing connection
    {
        let connections_guard = connections.lock().map_err(|_| NodeError::LockError)?;
        if let Some(existing_stream) = connections_guard.get(&peer_addr) {
            // Try to acquire the lock and send the message
            if let Ok(mut stream) = existing_stream.lock() {
                // if stream.write_all(message.as_bytes()).is_ok()
                if stream.write_all(&message.as_bytes()).is_ok()
                    // && stream.write_all(b"\n").is_ok()
                    && stream.flush().is_ok()
                {
                    //println!("Reutilizamos TCP ");
                    return Ok(());
                } else {
                    // println!(
                    //     "Conexión rota detectada para {:?}. Intentando reconectar...",
                    //     peer_addr
                    // );
                }
            }
        }
    }

    // Reconnect if no active connection exists or if the previous attempt failed
    let stream = TcpStream::connect_timeout(&peer_socket.into(), Duration::from_secs(5))
        .map_err(NodeError::IoError)?;
    let stream = Arc::new(Mutex::new(stream));

    // Add the new connection to the shared HashMap
    let mut connections_guard = connections.lock().map_err(|_| NodeError::LockError)?;
    connections_guard.insert(peer_addr.clone(), Arc::clone(&stream));

    // Attempt to send the message on the new connection
    {
        let mut stream_guard = stream.lock().map_err(|_| NodeError::LockError)?;
        stream_guard
            // .write_all(message.as_bytes())
            .write(&message.as_bytes())
            .map_err(NodeError::IoError)?;
        // stream_guard.write_all(b"\n").map_err(NodeError::IoError)?;
        stream_guard.flush().map_err(NodeError::IoError)?;
    }

    Ok(())
}
