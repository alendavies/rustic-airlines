// Local modules first
mod errors;
mod internode_protocol_handler;
mod keyspace;
mod open_query_handler;
mod query_execution;
mod table;
mod utils;

// Standard libraries
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Ipv4Addr, SocketAddrV4, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

// Internal project libraries
use crate::table::Table;
use crate::utils::{connect, send_message};

// External libraries
use driver::server::{handle_client_request, Request};
use errors::NodeError;
use internode_protocol_handler::InternodeProtocolHandler;
use keyspace::Keyspace;
use native_protocol::frame::Frame;
use native_protocol::messages::error;
use native_protocol::Serializable;
use open_query_handler::OpenQueryHandler;
use partitioner::Partitioner;
use query_creator::clauses::keyspace::create_keyspace_cql::CreateKeyspace;
use query_creator::clauses::table::create_table_cql::CreateTable;
use query_creator::clauses::types::column::Column;
use query_creator::errors::CQLError;
use query_creator::{GetTableName, Query};
use query_creator::{NeededResponses, QueryCreator};
use query_execution::QueryExecution;

const CLIENT_NODE_PORT: u16 = 0x4645; // Hexadecimal of "FE" (FERRUM) = 17989
const INTERNODE_PORT: u16 = 0x554D; // Hexadecimal of "UM" (FERRUM) = 21837

/// Represents a node within the distributed network.
/// The node can manage keyspaces, tables, and handle connections between nodes and clients.
pub struct Node {
    ip: Ipv4Addr,
    seeds_nodes: Vec<Ipv4Addr>,
    partitioner: Partitioner,
    open_query_handler: OpenQueryHandler,
    keyspaces: Vec<Keyspace>,
    actual_keyspace: Option<Keyspace>,
    //aux: bool
}

impl Node {
    /// Creates a new node with the given IP and a list of seed nodes.
    ///
    /// # Arguments
    ///
    /// * `ip` - The IP address of the node.
    /// * `seeds_nodes` - A vector of IP addresses of the seed nodes.
    ///
    /// # Returns
    /// Returns a `Node` instance or a `NodeError` if it fails.
    pub fn new(ip: Ipv4Addr, seeds_nodes: Vec<Ipv4Addr>) -> Result<Node, NodeError> {
        let mut partitioner = Partitioner::new();
        partitioner.add_node(ip)?;
        Ok(Node {
            ip,
            seeds_nodes,
            partitioner,
            open_query_handler: OpenQueryHandler::new(),
            keyspaces: vec![],
            actual_keyspace: None,
            //aux: true,
        })
    }

    /// Adds a new open query in the node.
    ///
    /// # Arguments
    ///
    /// * `query` - The query to be opened.
    /// * `connection` - The TCP connection with the client.
    ///
    /// # Returns
    /// Returns the ID of the open query or a `NodeError`.
    pub fn add_open_query(
        &mut self,
        query: Query,
        connection: TcpStream,
        table: Option<Table>,
    ) -> Result<i32, NodeError> {
        let all_nodes = self.get_how_many_nodes_i_know();

        let replication_factor = self.get_replication_factor().unwrap_or(1);

        let needed_responses = match query.needed_responses() {
            query_creator::NeededResponseCount::AllNodes => all_nodes,
            query_creator::NeededResponseCount::Specific(specific_value) => {
                specific_value as usize * replication_factor as usize
            }
        };
        Ok(self.open_query_handler.new_open_query(
            needed_responses as i32,
            connection,
            query,
            table,
        ))
    }

    fn is_seed(&self) -> bool {
        self.seeds_nodes.contains(&self.ip)
    }

    fn get_ip(&self) -> Ipv4Addr {
        self.ip
    }

    fn get_ip_string(&self) -> String {
        self.ip.to_string()
    }

    fn get_how_many_nodes_i_know(&self) -> usize {
        self.partitioner.get_nodes().len() - 1
    }

    fn get_partitioner(&self) -> Partitioner {
        self.partitioner.clone()
    }

    // Method to check if there is no current keyspace
    fn has_no_actual_keyspace(&self) -> bool {
        self.actual_keyspace.is_none()
    }

    // Method to get the name of the current keyspace if it exists
    fn actual_keyspace_name(&self) -> Option<String> {
        self.actual_keyspace
            .as_ref() // Converts Option<CreateKeyspace> to Option<&CreateKeyspace>
            .map(|keyspace| keyspace.get_name()) // Gets the name if it exists // If None, returns an error
    }

    fn get_replication_factor(&self) -> Option<u32> {
        if let Some(keyspace) = self.actual_keyspace.clone() {
            Some(keyspace.get_replication_factor())
        } else {
            None
        }
    }
    fn get_open_handle_query(&mut self) -> &mut OpenQueryHandler {
        &mut self.open_query_handler
    }

    fn add_keyspace(&mut self, new_keyspace: CreateKeyspace) -> Result<(), NodeError> {
        let new_keyspace = Keyspace::new(new_keyspace);
        if self.keyspaces.contains(&new_keyspace) {
            return Err(NodeError::KeyspaceError);
        }
        self.keyspaces.push(new_keyspace.clone());
        self.actual_keyspace = Some(new_keyspace);
        Ok(())
    }

    fn remove_keyspace(&mut self, keyspace_name: String) -> Result<(), NodeError> {
        let mut keyspaces = self.keyspaces.clone();

        let index = keyspaces
            .iter()
            .position(|keyspace| keyspace.get_name() == keyspace_name)
            .ok_or(NodeError::KeyspaceError)?;

        keyspaces.remove(index);
        if self.actual_keyspace_name().is_some()
            && self
                .actual_keyspace_name()
                .ok_or(NodeError::KeyspaceError)?
                == keyspace_name
        {
            self.actual_keyspace = None;
        }

        self.keyspaces = keyspaces;
        Ok(())
    }

    fn set_actual_keyspace(&mut self, keyspace_name: String) -> Result<(), NodeError> {
        // Clonar la lista de keyspaces para búsqueda
        let mut keyspaces = self.keyspaces.clone();

        // Buscar el índice del keyspace con el nombre dado
        let index = keyspaces
            .iter()
            .position(|keyspace| keyspace.get_name() == keyspace_name)
            .ok_or(NodeError::KeyspaceError)?;

        // Configurar el keyspace actual usando el índice encontrado
        self.actual_keyspace = Some(keyspaces.remove(index));

        Ok(())
    }

    fn add_table(&mut self, new_table: CreateTable) -> Result<(), NodeError> {
        let mut new_keyspace = self
            .actual_keyspace
            .clone()
            .ok_or(NodeError::KeyspaceError)?;
        new_keyspace.add_table(Table::new(new_table))?;
        self.actual_keyspace = Some(new_keyspace);
        Ok(())
    }

    fn get_table(&self, table_name: String) -> Result<Table, NodeError> {
        self.actual_keyspace
            .clone()
            .ok_or(NodeError::KeyspaceError)?
            .get_table(&table_name) // Clones the found value to return it
    }

    fn remove_table(&mut self, table_name: String) -> Result<(), NodeError> {
        let keyspace = self
            .actual_keyspace
            .as_mut()
            .ok_or(NodeError::KeyspaceError)?;
        keyspace.remove_table(&table_name)?;
        Ok(())
    }

    fn update_table(&mut self, new_table: CreateTable) -> Result<(), NodeError> {
        // Gets a mutable reference to `actual_keyspace` if it exists
        let keyspace = self
            .actual_keyspace
            .as_mut()
            .ok_or(NodeError::KeyspaceError)?;

        // Finds the position of the table in the `tables` vector of the `Keyspace`
        let index = keyspace
            .tables
            .iter()
            .position(|table| table.get_name() == new_table.get_name())
            .ok_or(NodeError::CQLError(CQLError::InvalidTable))?;

        // Replaces the existing table at the found position with the new table
        keyspace.tables[index] = Table::new(new_table);

        Ok(())
    }

    fn update_keyspace(&mut self, new_keyspace: Keyspace) -> Result<(), NodeError> {
        // Finds the position of the keyspace in the `keyspaces`
        let index = self
            .keyspaces
            .iter()
            .position(|table| table.get_name() == new_keyspace.get_name())
            .ok_or(NodeError::KeyspaceError)?;

        // Replaces the existing table at the found position with the new table
        self.keyspaces[index] = new_keyspace.clone();

        // Updates the current keyspace if it's the same
        if self.actual_keyspace.is_some()
            && self
                .actual_keyspace
                .clone()
                .ok_or(NodeError::KeyspaceError)?
                == new_keyspace
        {
            self.actual_keyspace = Some(new_keyspace);
        }
        Ok(())
    }
    fn table_already_exist(&self, table_name: String) -> Result<bool, NodeError> {
        // Gets a reference to `actual_keyspace` if it exists; otherwise, returns an error
        let keyspace = self
            .actual_keyspace
            .as_ref()
            .ok_or(NodeError::KeyspaceError)?;

        for table in keyspace.get_tables() {
            if table.get_name() == table_name {
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Starts the primary internode and client connection handlers for a `Node`.
    ///
    /// This function sets up the initial internode communication, establishing a handshake
    /// with a seed node if the current node is not a seed. It then spawns two threads:
    /// one to handle connections with other nodes (internode connections) and another to
    /// manage client connections. Both threads are joined, ensuring that any errors encountered
    /// are captured and handled appropriately.
    ///
    /// # Parameters
    /// - `node`: An `Arc<Mutex<Node>>` representing the node for which the communication is being established.
    /// - `connections`: An `Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>` storing active TCP connections,
    ///   allowing both node and client connections to be shared and synchronized.
    ///
    /// # Returns
    /// * `Result<(), NodeError>` - Returns `Ok(())` if both threads execute and join successfully, or
    ///   `NodeError` if an error occurs during the connection setup, handshake, or in joining the threads.
    ///
    /// # Workflow
    /// 1. **Initialization and Seed Connection**:
    ///     - Acquires a lock on `node` to check if the node is a seed and retrieve the seed IP and node's IP.
    ///     - If the node is not a seed, attempts to connect to the seed node's IP using the `INTERNODE_PORT`.
    ///     - Sends a handshake message (`HANDSHAKE` query) to the seed node, establishing initial communication
    ///       and adding the seed node to the partitioner.
    /// 2. **Thread Creation**:
    ///     - Creates two threads:
    ///         * **Node Connection Handler**: Manages internode connections and handles commands and messages
    ///           between nodes.
    ///         * **Client Connection Handler**: Manages connections with clients that send queries to the node.
    /// 3. **Thread Execution**:
    ///     - Each thread is executed and joined. Errors during the thread's execution are printed to `stderr`.
    ///
    /// # Errors
    /// - This function returns `NodeError` in the following cases:
    ///   - `NodeError::InternodeError`: If there is an error in the internode thread handling connections.
    ///   - `NodeError::ClientError`: If there is an error in the client thread handling connections.
    ///   - `NodeError::LockError`: If there is an issue locking the node for accessing seed and IP information.
    pub fn start(
        node: Arc<Mutex<Node>>,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
    ) -> Result<(), NodeError> {
        let is_seed;
        let seed_ip;
        let self_ip;
        {
            let mut node_guard = node.lock()?;
            is_seed = node_guard.is_seed();
            seed_ip = node_guard.seeds_nodes[0];
            self_ip = node_guard.get_ip();

            if !is_seed {
                if let Ok(stream) = connect(seed_ip, INTERNODE_PORT, Arc::clone(&connections)) {
                    let stream = Arc::new(Mutex::new(stream)); // Encapsulates in Arc<Mutex<TcpStream>>
                    let message = InternodeProtocolHandler::create_protocol_message(
                        &node_guard.get_ip_string(),
                        0,
                        "HANDSHAKE",
                        "_",
                        true,
                        false,
                    );
                    let mut stream_guard = stream.lock()?;
                    send_message(&mut stream_guard, &message)?;
                    node_guard.partitioner.add_node(seed_ip)?;
                }
            }
        }

        // Creates a thread to handle node connections
        let node_connections_node = Arc::clone(&node);
        let node_connections = Arc::clone(&connections);
        let self_ip_node = self_ip.clone();
        let handle_node_thread = thread::spawn(move || {
            Self::handle_node_connections(
                node_connections_node,
                node_connections,
                self_ip_node,
                is_seed,
            )
            .unwrap_or_else(|err| {
                eprintln!("Error in internode connections: {:?}", err); // Or handle the error as needed
            });
        });

        // Creates a thread to handle client connections
        let client_connections_node = Arc::clone(&node);
        let client_connections = Arc::clone(&connections);
        let self_ip_client = self_ip;

        let handle_client_thread = thread::spawn(move || {
            Self::handle_client_connections(
                client_connections_node,
                client_connections,
                self_ip_client,
            )
            .unwrap_or_else(|e| eprintln!("Error in client connections: {:?}", e));
        });

        handle_node_thread
            .join()
            .map_err(|_| NodeError::InternodeError)?;
        handle_client_thread
            .join()
            .map_err(|_| NodeError::ClientError)?;

        Ok(())
    }

    fn handle_node_connections(
        node: Arc<Mutex<Node>>,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        self_ip: std::net::Ipv4Addr,
        is_seed: bool,
    ) -> Result<(), NodeError> {
        let socket = SocketAddrV4::new(self_ip, INTERNODE_PORT);
        let listener = TcpListener::bind(socket)?;

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let node_clone = Arc::clone(&node);
                    let stream = Arc::new(Mutex::new(stream)); // Encapsulates the stream in Arc<Mutex<TcpStream>>
                    let connections_clone = Arc::clone(&connections);

                    thread::spawn(move || {
                        if let Err(e) = Node::handle_incoming_internode_messages(
                            node_clone,
                            stream,
                            connections_clone,
                            is_seed,
                        ) {
                            eprintln!("{:?}", e);
                        }
                    });
                }
                Err(e) => {
                    eprintln!("Error accepting internode connection: {:?}", e);
                }
            }
        }

        Ok(())
    }

    fn handle_client_connections(
        node: Arc<Mutex<Node>>,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        self_ip: std::net::Ipv4Addr,
    ) -> Result<(), NodeError> {
        let socket = SocketAddrV4::new(self_ip, CLIENT_NODE_PORT); // Specific port for clients
        let listener = TcpListener::bind(socket)?;

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let node_clone = Arc::clone(&node);
                    let stream = Arc::new(Mutex::new(stream.try_clone()?)); // Cloning the stream
                    let connections_clone = Arc::clone(&connections);

                    thread::spawn(move || {
                        match Node::handle_incoming_client_messages(
                            node_clone,
                            stream.clone(),
                            connections_clone,
                        ) {
                            Ok(_) => {}
                            Err(e) => {
                                eprintln!("Error handling query: [{:?}]", e);

                                let frame =
                                    Frame::Error(error::Error::ServerError("error ".to_string()));

                                if let Ok(mut client_stream) = stream.lock() {

                                    let frame_bytes_result = &frame.to_bytes();
                                    let mut frame_bytes = &vec![];
                                    if let Ok(value) = frame_bytes_result {
                                        frame_bytes = value;
                                    }

                                    if let Err(write_err) = client_stream.write(&frame_bytes) {
                                        eprintln!(
                                            "Error writing to client stream: {:?}",
                                            write_err
                                        );
                                    }
                                } else {
                                    eprintln!("Error locking client stream");
                                }
                            }
                        };
                    });
                }
                Err(e) => {
                    eprintln!("Error accepting client connection: {:?}", e);
                }
            }
        }

        Ok(())
    }

    fn forward_message(
        &self,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        sent_ip: Ipv4Addr,
        target_ip: Ipv4Addr,
    ) -> Result<(), NodeError> {
        // First, try to reuse or establish a connection
        let mut tcp = connect(target_ip, INTERNODE_PORT, Arc::clone(&connections))?;

        let message = InternodeProtocolHandler::create_protocol_message(
            &sent_ip.to_string(),
            0,
            "HANDSHAKE",
            "_",
            true,
            false,
        );

        send_message(&mut tcp, &message)?;
        Ok(())
    }

    // Receives packets from the client
    fn handle_incoming_client_messages(
        node: Arc<Mutex<Node>>,
        stream: Arc<Mutex<TcpStream>>,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
    ) -> Result<(), NodeError> {
        // Clone the stream under Mutex protection and create the reader
        let mut stream_guard = stream.lock()?;

        let mut reader = BufReader::new(stream_guard.try_clone().map_err(NodeError::IoError)?);

        loop {
            // Clean the buffer
            // let mut buffer = String::new();

            let mut buffer = [0; 2048];

            // Execute initial inserts if necessary

            // Try to read a line
            // let bytes_read = reader.read_line(&mut buffer);
            let bytes_read = reader.read(&mut buffer);

            match bytes_read {
                Ok(0) => {
                    // Connection closed
                    break;
                }
                Ok(_) => {
                    let query = handle_client_request(&buffer);

                    match query {
                        Request::Startup => {
                            // let mut stream_guard = stream.lock()?;
                            stream_guard.write(Frame::Ready.to_bytes()?.as_slice())?;
                            stream_guard.flush()?;
                        }
                        Request::Query(query) => {
                            // Handle the query
                            let query_str = &query.query;
                            let client_stream = stream_guard.try_clone()?;

                            let result = Node::handle_query_execution(
                                query_str,
                                &node,
                                connections.clone(),
                                client_stream,
                            );

                            if let Err(e) = result {
                                eprintln!("{:?} when client sent {:?}", e, query_str);
                                return Err(e);
                            }
                        }
                    };
                }
                Err(_) => {
                    // Another type of error
                    return Err(NodeError::OtherError);
                }
            }
        }

        Ok(())
    }

    fn handle_incoming_internode_messages(
        node: Arc<Mutex<Node>>,
        mut stream: Arc<Mutex<TcpStream>>,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        is_seed: bool,
    ) -> Result<(), NodeError> {
        // Clone the stream under Mutex protection and create the reader
        let mut reader = {
            let stream_guard = stream.lock()?;
            BufReader::new(stream_guard.try_clone().map_err(NodeError::IoError)?)
        };

        let internode_protocol_handler = InternodeProtocolHandler::new();

        loop {
            // Clean the buffer
            let mut buffer = String::new();

            // Execute initial inserts if necessary

            // Self::execute_querys(&node, connections.clone())?;

            // Try to read a line
            let bytes_read = reader.read_line(&mut buffer);
            match bytes_read {
                Ok(0) => {
                    // Connection closed
                    break;
                }
                Ok(_) => {
                    // Process the command with the protocol, passing the buffer and the necessary parameters
                    let result = internode_protocol_handler.handle_command(
                        &node,
                        &buffer.trim().to_string(),
                        &mut stream,
                        connections.clone(),
                        is_seed,
                    );

                    // If there's an error handling the command, exit the loop
                    if let Err(e) = result {
                        eprintln!("{:?} when other node sent me {:?}", e, buffer);
                        break;
                    }
                }
                Err(_) => {
                    // Another type of error
                    return Err(NodeError::OtherError);
                }
            }
        }

        Ok(())
    }

    fn handle_query_execution(
        query_str: &str,
        node: &Arc<Mutex<Node>>,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        client_connection: TcpStream,
    ) -> Result<(), NodeError> {
        let query = QueryCreator::new()
            .handle_query(query_str.to_string())
            .map_err(NodeError::CQLError)?;

        let open_query_id;
        {
            let mut guard_node = node.lock()?;
            let table_name = query.get_table_name();
            let table = {
                if let Some(table) = table_name {
                    guard_node.get_table(table).ok()
                } else {
                    None
                }
            };

            open_query_id =
                guard_node.add_open_query(query.clone(), client_connection.try_clone()?, table)?;
        }

        let response = QueryExecution::new(node.clone(), connections.clone()).execute(
            query.clone(),
            false,
            false,
            open_query_id,
        )?;

        if let Some((finished_responses, content)) = response {
            let mut guard_node = node.lock()?;
            let keyspace_name = guard_node
                .actual_keyspace_name()
                .ok_or(NodeError::KeyspaceError)?;
            let table_name = query.get_table_name();
            let table = {
                if let Some(table) = table_name {
                    guard_node.get_table(table).ok()
                } else {
                    None
                }
            };
            let columns: Vec<Column> = {
                if let Some(table) = table {
                    table.get_columns()
                } else {
                    vec![]
                }
            };
            let query_handler = guard_node.get_open_handle_query();

            for _ in [..finished_responses] {
                InternodeProtocolHandler::add_response_to_open_query_and_send_response_if_closed(
                    query_handler,
                    &content,
                    open_query_id,
                    keyspace_name.clone(),
                    columns.clone(),
                )?;
            }
        }

        Ok(())
    }
}
