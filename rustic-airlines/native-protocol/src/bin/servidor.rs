use std::io::{BufRead, BufReader, Write, Read};
use std::net::{TcpListener, TcpStream, SocketAddr};
use std::sync::{Arc, Mutex};
use std::thread;
use std::collections::HashMap;
use crate::cliente::client_receive;
use crate::frame::enums::opcode::Opcode;
use crate::frame::frame::Frame;

mod frame;
mod cliente;


fn main() -> Result<(), ()> {
    let address = "127.0.0.1:8081";
    let clients = Arc::new(Mutex::new(HashMap::new())); // Lista de clientes

    // Inicia el servidor
    server_run(&address, clients).unwrap();
    Ok(())
}

/// Función principal del servidor, acepta conexiones y maneja cada cliente en un hilo separado
fn server_run(address: &str, clients: Arc<Mutex<HashMap<String,TcpStream>>>) -> std::io::Result<()> {
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

/// Maneja la conexión de un cliente
fn handle_client(mut stream: TcpStream, clients: Arc<Mutex<HashMap<String, TcpStream>>>) -> std::io::Result<()> {
    let socket_addr = stream.peer_addr()?;
    println!("New client connected: {}", socket_addr);

    clients.lock().unwrap().insert(socket_addr.to_string(), stream.try_clone()?);

    loop {
        // Podemos usar tranquilamente la misma función de recepción del cliente
        match client_receive(stream.try_clone()?) {
            Ok(frame) => {
                println!(
                    "Received message with version: {:?}, opcode: {:?}, body length: {}",
                    frame.header().version(), frame.header().opcode(), frame.header().body_length()
                );

                // Y vemos a donde vamos segun el opcode
                match frame.header().opcode() {
                    Opcode::Startup => println!("Processing STARTUP message"),
                    _ => println!("Unknown message type"),
                }
            },
            Err(e) => {
                eprintln!("Error while receiving message: {}", e);
                break;
            }
        }
    }

    // Al finalizar la conexión, remover el cliente del HashMap
    clients.lock().unwrap().remove(&socket_addr.to_string());

    println!("Client disconnected: {}", socket_addr);
    Ok(())
}

/// Envía un mensaje a un cliente en particular identificado por su nickname
fn enviar_frame_a_cliente(
    frame: &Frame,
    clients_guard: &mut HashMap<String, TcpStream>,
    nickname: &str
) -> std::io::Result<()> {

    println!("Sending frame to client with nickname: {}", nickname);

    // Buscamos el cliente por su nickname
    if let Some(client) = clients_guard.get_mut(nickname) {

        let mut client_stream = client.try_clone()?;

        let frame_bytes = frame.serialize();
        client_stream.write_all(&frame_bytes)?;
        client_stream.flush()?;

    } else {
        // Si no encontramos el cliente con ese nickname, devolvemos un error
        eprintln!("Cliente con nickname '{}' no encontrado", nickname);
    }

    Ok(())
}


