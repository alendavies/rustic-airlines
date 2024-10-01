use std::io::{BufRead, BufReader, Write, Read};
use std::net::{TcpListener, TcpStream, SocketAddr};
use std::sync::{Arc, Mutex};
use std::thread;
use std::collections::HashMap;
use crate::client::client_receive;
use crate::client_info::{ClientInfo, ClientsMap};
use crate::enums::ClientState;
use crate::frame::enums::opcode::Opcode;
use crate::frame::frame::Frame;


mod frame;
mod client;
mod enums;
mod client_info;

// This is only used for testing, we should have a mechanism to define whether auth is required.
const REQUIRES_AUTH: bool = false;

fn main() -> Result<(), ()> {
    let address = "127.0.0.1:8081";
    let clients = Arc::new(Mutex::new(HashMap::new()));

    server_run(&address, clients).unwrap();
    Ok(())
}

/// Accepts connections and handles clients on different threads
fn server_run(address: &str, clients: ClientsMap) -> std::io::Result<()> {
    let listener = TcpListener::bind(address)?;

    for stream in listener.incoming() {
        match stream {
            Ok(client_stream) => {
                let clients_clone = Arc::clone(&clients);
                thread::spawn(move || {
                    handle_client(client_stream, clients_clone).unwrap_or_else(|error| {
                        eprintln!("Error manejando cliente: {:?}", error);
                    });
                });
            }
            Err(e) => {
                eprintln!("Error al aceptar conexión: {:?}", e);
            }
        }
    }
    Ok(())
}

/// Manages the connection to clients
fn handle_client(mut stream: TcpStream, clients: ClientsMap) -> std::io::Result<()> {
    let socket_addr = stream.peer_addr()?.to_string();
    println!("New client connected: {}", socket_addr);

    {
        let mut clients_guard = clients.lock().unwrap();
        clients_guard.insert(socket_addr.clone(), ClientInfo::new(stream.try_clone()?));
    }

    loop {
        let frame = match client_receive(stream.try_clone()?) {
            Ok(frame) => frame,
            Err(e) => {
                eprintln!("Error while receiving message: {}", e);
                break;
            }
        };

        let opcode = frame.header().opcode();
        let mut clients_guard = clients.lock().unwrap();
        let client_info = clients_guard.get_mut(&socket_addr).unwrap();

        match client_info.state() {
            ClientState::Startup => match opcode {
                Opcode::Startup | Opcode::Options => {
                    println!("Processing {:?} in Startup state", opcode);
                    if REQUIRES_AUTH = false {
                        client_info.successful_auth();
                    }
                    else {
                        client_info.start_auth();
                        // We send the AUTH_CHALLENGE frame here
                    }
                }
                _ => {
                    println!("Error: Invalid opcode in Startup state");
                    // We send the error frame here
                }
            },
            ClientState::Authentication => match opcode {
                Opcode::AuthResponse => {
                    println!("Processing AUTH_RESPONSE");
                    // We'd send
                    client_info.successful_auth();
                    println!("Authentication successful, transitioning to Authenticated state");
                }
                _ => {
                    println!("Error: Invalid opcode in Authentication state");
                    // We send the error frame here
                }
            },
            ClientState::Authenticated => match opcode {
                Opcode::Startup | Opcode::AuthResponse => {
                    println!("Error: Invalid opcode in Authenticated state");
                    // We send the error frame here
                }
                _ => {
                    println!("Processing {:?} in Authenticated state", opcode);
                    // Other opcodes go here.
                }
            },
        }
    }

    // Remove client from the map on disconnection
    clients.lock().unwrap().remove(&socket_addr);
    println!("Client disconnected: {}", socket_addr);
    Ok(())
}


/// Sends a frame to a client identified by its 'username'
fn send_frame_to_client(
    frame: &Frame,
    clients_guard: &mut HashMap<String, TcpStream>,
    username: &str
) -> std::io::Result<()> {

    println!("Sending frame to client with username: {}", username);

    if let Some(client) = clients_guard.get_mut(username) {

        let mut client_stream = client.try_clone()?;

        let frame_bytes = frame.serialize();
        client_stream.write_all(&frame_bytes)?;
        client_stream.flush()?;

    } else {
        eprintln!("Client with nickname '{}' not found", username);
    }

    Ok(())
}


