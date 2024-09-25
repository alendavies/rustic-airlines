use std::io::{BufRead, BufReader, Write, Read};
use std::net::{TcpListener, TcpStream, SocketAddr};
use std::sync::{Arc, Mutex};
use std::thread;
use std::collections::HashMap;


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

/// Procesa el mensaje que llega del cliente según el protocolo nativo
fn handle_incoming_message(mut stream: TcpStream) -> std::io::Result<()> {
    let mut buffer = [0u8; 9]; // Leer solo el encabezado primero
    stream.read_exact(&mut buffer)?;

    let version = buffer[0];
    let flags = buffer[1];
    let stream_id = u16::from_be_bytes([buffer[2], buffer[3]]);
    let opcode = buffer[4];
    let body_length = u32::from_be_bytes([buffer[5], buffer[6], buffer[7], buffer[8]]);

    println!("Received message with version: {}, opcode: {}, body length: {}",
             version, opcode, body_length);

    // Leer el cuerpo si es necesario
    let mut body = vec![0u8; body_length as usize];
    stream.read_exact(&mut body)?;

    // Aquí puedes procesar el cuerpo del mensaje según el tipo (e.g., STARTUP)
    match opcode {
        0x01 => println!("Processing STARTUP message"), // Ejemplo para STARTUP
        _ => println!("Unknown message type"),
    }

    Ok(())
}

/// Maneja la conexión de un cliente
fn handle_client(mut stream: TcpStream, clients: Arc<Mutex<HashMap<String, TcpStream>>>) -> std::io::Result<()> {
    let socket_addr = stream.peer_addr()?;
    println!("New client connected: {}", socket_addr);

    // Añadir el cliente al HashMap compartido
    clients.lock().unwrap().insert(socket_addr.to_string(), stream.try_clone()?);

    let reader = BufReader::new(stream.try_clone()?);

    // Leer cada línea enviada por el cliente
    for line in reader.lines() {
        let line = line?;

        println!("Received from client {}: {}", socket_addr, line);

        // Procesar el mensaje como un protocolo nativo si es necesario
        handle_incoming_message(stream.try_clone()?)?;  // Procesar el mensaje usando la función handle_incoming_message

        // Después de procesar el mensaje, solicitamos el siguiente mensaje
    }

    // Al finalizar la conexión, remover el cliente del HashMap
    clients.lock().unwrap().remove(&socket_addr.to_string());

    println!("Client disconnected: {}", socket_addr);
    Ok(())
}



fn obtener_primera_y_resto(oracion: &str) -> Option<(&str, &str)> {
    // Usamos split_once para dividir la oración por el primer espacio
    if let Some((primera_palabra, resto)) = oracion.split_once(' ') {
        // Si hay una palabra y un resto de la oración, devolvemos ambos
        Some((primera_palabra, resto.trim_start()))
    } else if !oracion.is_empty() {
        // Si no hay espacio, devolvemos toda la oración como la primera palabra
        Some((oracion, ""))
    } else {
        // Si la oración está vacía, devolvemos None
        None
    }
}


/// Envía un mensaje a un cliente en particular identificado por su nickname
fn enviar_mensaje_a_cliente(
    message: &str,
    clients_guard: &mut HashMap<String, TcpStream>,
    nickname: &str
) -> std::io::Result<()> {

    println!("lo encontramos");

    // Buscamos el cliente por su nickname
    if let Some(client) = clients_guard.get_mut(nickname) {
        // Si encontramos el cliente, clonamos su stream y enviamos el mensaje
        let mut client_stream = client.try_clone()?;
        client_stream.write_all(message.as_bytes())?;
        client_stream.flush()?;  // Aseguramos que el mensaje se envía de inmediato
    } else {
        // Si no encontramos el cliente con ese nickname, devolvemos un error
        eprintln!("Cliente con nickname '{}' no encontrado", nickname);
    }
    
    Ok(())
}


