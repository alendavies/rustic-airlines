use std::io::{stdin, BufRead, BufReader, Write};
use std::net::TcpStream;
use std::thread;
use std::collections::HashMap;

fn main() -> Result<(), ()> {
    let address = "127.0.0.1:8081";
    
    // Conectar el socket
    let socket = TcpStream::connect(address).expect("No se pudo conectar al servidor");

    // Clonamos el socket porque lo vamos a usar en dos hilos, uno para enviar y otro para recibir
    let socket_reader = socket.try_clone().expect("No se pudo clonar el socket");

    // Hilo para enviar mensajes desde stdin
    let sender_thread = thread::spawn(move || {
        client_send(socket).expect("Error en el envío de mensajes");
    });

    // Hilo para recibir mensajes del servidor
    let receiver_thread = thread::spawn(move || {
        client_receive(socket_reader).expect("Error en la recepción de mensajes");
    });

    // Esperamos que ambos hilos terminen
    sender_thread.join().unwrap();
    receiver_thread.join().unwrap();

    Ok(())
}

/// Enviar mensajes al servidor desde stdin
fn client_send(mut socket: TcpStream) -> std::io::Result<()> {
    let stdin = stdin();
    let reader = BufReader::new(stdin);

    let startup_message = serialize_startup_message();
    socket.write_all(&startup_message)?;

    for line in reader.lines() {
        let line = line?;
        if line == "exit" {
            break;
        }
        // Mover cursor hacia arriba y borrar la línea
        //print!("\x1B[A\x1B[K");
        // Mover el cursor al principio de la línea
        //print!("\r");
        // Enviar el mensaje al servidor
        socket.write_all(line.as_bytes())?;
        socket.write_all(b"\n")?;
    }
    Ok(())
}

fn serialize_startup_message() -> Vec<u8> {
    // Mapa con las opciones (ejemplo: versión de CQL y sin compresión)
    let mut options = HashMap::new();
    options.insert("CQL_VERSION".to_string(), "3.0.0".to_string());
    options.insert("COMPRESSION".to_string(), "none".to_string());

    // Serializar el body
    let mut body : Vec<u8>= vec![];

    // Número de pares clave-valor en el mapa
    body.extend((options.len() as u16).to_be_bytes().iter()); // Tamaño del mapa

    for (key, value) in options {
        // Serializar la clave (longitud + contenido)
        body.extend((key.len() as u16).to_be_bytes().iter());
        body.extend(key.as_bytes());


        // Serializar el valor (longitud + contenido)
        body.extend((value.len() as u16).to_be_bytes().iter());
        body.extend(value.as_bytes());
    }

    // Construir el encabezado del mensaje
    let mut message = vec![];

    let version: u8 = 0x04;  // Versión del protocolo (4 en este caso)
    let flags: u8 = 0x00;    // Sin flags
    let stream: u16 = 0x0000; // Stream ID (0 por simplicidad)
    let opcode: u8 = 0x01;   // Opcode para `STARTUP`
    let body_length: u32 = body.len() as u32; // Longitud del body

    // Escribir el encabezado
    message.push(version);
    message.push(flags);
    message.extend(&stream.to_be_bytes()); // 2 bytes para el Stream ID
    message.push(opcode);
    message.extend(&body_length.to_be_bytes()); // 4 bytes para la longitud del body

    // Añadir el body
    message.extend(body);

    message
}


/// Recibir mensajes del servidor y mostrarlos en pantalla
fn client_receive(socket: TcpStream) -> std::io::Result<()> {
    let mut reader = BufReader::new(socket);

    let mut buffer = String::new();
    loop {
        // Leer cada línea enviada por el servidor
        match reader.read_line(&mut buffer) {
            Ok(0) => {
                // Si leemos 0 bytes, significa que el servidor cerró la conexión
                println!("Conexión cerrada por el servidor.");
                break;
            }
            Ok(_) => {
                // Mostramos el mensaje recibido
                print!("{buffer}");
                buffer.clear(); // Limpiamos el buffer para el próximo mensaje
            }
            Err(e) => {
                eprintln!("Error al recibir mensaje: {}", e);
                break;
            }
        }
    }
    Ok(())
}
