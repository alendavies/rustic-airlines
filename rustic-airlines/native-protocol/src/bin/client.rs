use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
mod frame;
use crate::frame::frame::Frame;
use crate::frame::frame_builder::{create_auth_response_frame, create_startup_frame};
use crate::frame::header::FrameHeader;

fn main() -> Result<(), ()> {
    /*let address = "127.0.0.1:8081";
    
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
    receiver_thread.join().unwrap();*/

    Ok(())
}

/// Send frame
pub fn client_send(mut socket: TcpStream, frame: Frame) -> std::io::Result<()> {

    let serialized_frame = frame.to_bytes();

    socket.write_all(&serialized_frame)?;

    Ok(())
}


/// Receive frame
pub fn client_receive(socket: TcpStream) -> std::io::Result<Frame> {
    let mut reader = BufReader::new(socket);

    let mut header_buffer = [0; 9];
    reader.read_exact(&mut header_buffer)?;

    let header = FrameHeader::from_bytes(&header_buffer).unwrap();

    let body_length = *header.body_length() as usize;
    let mut body_buffer = vec![0; body_length];

    reader.read_exact(&mut body_buffer)?;

    let body = String::from_utf8(body_buffer).unwrap();

    let frame = Frame::new(header, body);
    Ok(frame)
}

fn send_startup(socket: TcpStream) -> std::io::Result<()> {

    let startup_frame = create_startup_frame();

    // Send STARTUP Frame
    client_send(socket, startup_frame)?;
    println!("Sent STARTUP message");

    Ok(())
}


fn send_auth_response(socket: TcpStream) -> std::io::Result<()> {
    let auth_body = String::from("my_auth_token"); // Example auth token

    let auth_response_frame = create_auth_response_frame(auth_body);

    client_send(socket, auth_response_frame)?;
    println!("Sent AUTH_RESPONSE message");

    Ok(())
}