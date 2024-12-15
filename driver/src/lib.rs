use std::{
    env,
    io::{Read, Write},
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream},
    sync::Arc,
};
pub mod server;
mod tls;

use native_protocol::{
    self,
    frame::Frame,
    messages::{
        self,
        auth::AuthResponse,
        query::{Consistency, Query, QueryParams},
    },
    types::Bytes,
    Serializable,
};
use rustls::{ClientConnection, StreamOwned};
use tls::configure_client;

pub struct CassandraClient {
    stream: StreamOwned<ClientConnection, TcpStream>,
}

const NATIVE_PORT: u16 = 0x4645;

#[derive(Debug)]
pub struct ClientError;

#[derive(Debug)]
pub enum QueryResult {
    Result(messages::result::result_::Result),
    Error(messages::error::Error),
}

impl CassandraClient {
    /// Creates a connection with the node at `ip`.
    pub fn connect(ip: Ipv4Addr) -> Result<Self, ClientError> {
        // Configurar TLS sin verificación de certificados
        let config = configure_client();
        let config = Arc::new(config);

        let server_name = rustls::pki_types::ServerName::try_from("databaseserver").unwrap();
        let conn = ClientConnection::new(config, server_name).unwrap();

        let addr = if let Ok(var) = env::var("NODE_ADDR") {
            var.parse().map_err(|_| ClientError)?
        } else {
            SocketAddr::new(IpAddr::V4(ip), NATIVE_PORT)
        };

        let sock = TcpStream::connect(addr).unwrap();
        sock.set_read_timeout(Some(std::time::Duration::from_secs(3)))
            .map_err(|_| ClientError)?;
        sock.set_write_timeout(Some(std::time::Duration::from_secs(3)))
            .map_err(|_| ClientError)?;
        let tls = StreamOwned::new(conn, sock);

        Ok(Self { stream: tls })
    }

    /// Execute a query.
    pub fn execute(
        &mut self,
        query: &str,
        consistency_str: &str,
    ) -> Result<QueryResult, ClientError> {
        let consistency = Consistency::from_string(consistency_str).map_err(|_| ClientError)?;
        let result = self.send_query(query, consistency)?;
        match result {
            Frame::Result(res) => Ok(QueryResult::Result(res)),
            Frame::Error(err) => Ok(QueryResult::Error(err)),
            _ => Err(ClientError),
        }
    }

    pub fn startup(&mut self) -> Result<(), ClientError> {
        let startup = Frame::Startup;

        self.stream
            .write_all(&startup.to_bytes().map_err(|_| ClientError)?)
            .map_err(|_| ClientError)?;

        let mut result = [0u8; 2048];
        let _ = self.stream.read(&mut result).map_err(|_| ClientError)?;

        let response = Frame::from_bytes(&result).map_err(|_| ClientError)?;

        match response {
            Frame::Authenticate(_) => {
                let auth_response = Frame::AuthResponse(AuthResponse::new(Bytes::Vec(
                    "admin".to_string().as_bytes().to_vec(),
                )));

                self.stream
                    .write_all(&auth_response.to_bytes().map_err(|_| ClientError)?)
                    .map_err(|_| ClientError)?;

                let mut result = [0u8; 2048];

                let _ = self.stream.read(&mut result).map_err(|_| ClientError)?;

                let response = Frame::from_bytes(&result).map_err(|_| ClientError)?;

                match response {
                    Frame::AuthSuccess(_) => return Ok(()),
                    _ => return Err(ClientError),
                }
            }
            Frame::Ready => return Ok(()),
            _ => return Err(ClientError),
        }
    }

    fn send_query(
        &mut self,
        cql_query: &str,
        consistency: Consistency,
    ) -> Result<Frame, ClientError> {
        let params = QueryParams::new(consistency, vec![]);
        let query = Query::new(cql_query.to_string(), params);
        let query = Frame::Query(query);

        // Escribir la consulta en el stream
        self.stream
            .write_all(query.to_bytes().map_err(|_| ClientError)?.as_slice())
            .map_err(|_| ClientError)?;

        let mut result = [0u8; 850000];

        self.stream.read(&mut result).map_err(|_| ClientError)?;

        // Decodificar la respuesta
        let result = Frame::from_bytes(&result).map_err(|_| ClientError)?;
        Ok(result)
    }
}
