use std::{
    env,
    io::{Read, Write},
    net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4, TcpStream},
    str::FromStr,
};
pub mod server;

use native_protocol::{
    self,
    frame::Frame,
    messages::{
        self,
        query::{Consistency, Query, QueryParams},
    },
    Serializable,
};

pub struct CassandraClient {
    stream: TcpStream,
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
        let addr = if let Ok(var) = env::var("NODE_ADDR") {
            var.parse().map_err(|_| ClientError)?
        } else {
            SocketAddr::new(IpAddr::V4(ip), NATIVE_PORT)
        };

        let stream = TcpStream::connect(addr).map_err(|e| {
            eprintln!("Error al conectar: {:?}", e);
            ClientError
        })?;

        Ok(Self { stream })
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

        let ready = Frame::from_bytes(&result).map_err(|_| ClientError)?;

        match ready {
            Frame::Ready => Ok(()),
            _ => Err(ClientError),
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

        self.stream
            .write_all(query.to_bytes().map_err(|_| ClientError)?.as_slice())
            .map_err(|_| ClientError)?;

        let mut result = [0u8; 850000];
        self.stream.read(&mut result).map_err(|_| ClientError)?;
        // dbg!(&String::from_utf8(result.to_vec()).unwrap());
        let result = Frame::from_bytes(&result).map_err(|_| ClientError)?;
        Ok(result)
    }
}
