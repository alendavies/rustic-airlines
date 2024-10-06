use std::collections::HashMap;
use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use crate::enums::ClientState;

/// The server stores the different clients using the struct ClientInfo.
/// This will include their connection stream, and stored queries waiting for execution.
/// It always starts in the state Startup.

pub struct ClientInfo {
    stream: TcpStream,
    state: ClientState,
}

impl ClientInfo {
    pub fn new(stream: TcpStream) -> Self {
        ClientInfo {
            stream,
            state: ClientState::Startup,
        }
    }

    pub fn start_auth(&mut self) {
        self.state = ClientState::Authentication
    }
    pub fn successful_auth(&mut self) {
        self.state = ClientState::Authenticated
    }

    pub fn state(&self) -> &ClientState {
        &self.state
    }

    pub fn stream(&self) -> &TcpStream {
        &self.stream
    }
}

pub type ClientsMap = Arc<Mutex<HashMap<String, ClientInfo>>>;

fn main() {}