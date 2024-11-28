// Local modules first
mod errors;
mod internode_protocol;
mod internode_protocol_handler;
mod keyspace;
mod open_query_handler;
mod query_execution;
pub mod storage_engine;
pub mod table;
mod utils;

// Standard libraries
use std::collections::HashMap;
use std::io::{BufReader, Read, Write};
use std::net::{Ipv4Addr, SocketAddrV4, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use std::{thread, vec};

// Internal project libraries
use crate::table::Table;

// External libraries
use chrono::Utc;
use driver::server::{handle_client_request, Request};
use errors::NodeError;
use gossip::structures::NodeStatus;
use gossip::Gossiper;
use internode_protocol::message::{InternodeMessage, InternodeMessageContent};
use internode_protocol::response::{
    InternodeResponse, InternodeResponseContent, InternodeResponseStatus,
};
use internode_protocol::InternodeSerializable;
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
use query_creator::{GetTableName, GetUsedKeyspace, Query};
use query_creator::{NeededResponses, QueryCreator};
use query_execution::QueryExecution;
use storage_engine::StorageEngine;
use utils::connect_and_send_message;

const CLIENT_NODE_PORT: u16 = 0x4645; // Hexadecimal of "FE" (FERRUM) = 17989
const INTERNODE_PORT: u16 = 0x554D; // Hexadecimal of "UM" (FERRUM) = 21837

/// Represents a node within the distributed network.
/// The node can manage keyspaces, tables, and handle connections between nodes and clients.
///
pub struct Node {
    ip: Ipv4Addr,
    partitioner: Partitioner,
    open_query_handler: OpenQueryHandler,
    keyspaces: Vec<Keyspace>,
    clients_keyspace: HashMap<i32, Option<String>>,
    last_client_id: i32,
    gossiper: Gossiper,
    storage_path: PathBuf,
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
    pub fn new(
        ip: Ipv4Addr,
        seeds_nodes: Vec<Ipv4Addr>,
        storage_path: PathBuf,
    ) -> Result<Node, NodeError> {
        let mut partitioner = Partitioner::new();
        partitioner.add_node(ip)?;

        let storage_engine = StorageEngine::new(storage_path.clone(), ip.to_string());
        storage_engine.reset_folders()?;

        for seed_ip in seeds_nodes.clone() {
            if seed_ip != ip {
                partitioner.add_node(seed_ip)?;
            }
        }

        Ok(Node {
            ip,
            partitioner,
            open_query_handler: OpenQueryHandler::new(),
            keyspaces: vec![],
            clients_keyspace: HashMap::new(),
            last_client_id: 0,
            storage_path,
            gossiper: Gossiper::new()
                .with_endpoint_state(ip)
                .with_seeds(seeds_nodes),
        })
    }
    /// Starts the gossip process for the node.
    /// Opens 3 connections with 3 other nodes.
    pub fn start_gossip(
        node: Arc<Mutex<Node>>,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
    ) -> Result<(), NodeError> {
        let _ = thread::spawn(move || {
            let initial_gossip = Instant::now();
            loop {
                {
                    {
                        let mut node_guard = node.lock().unwrap();
                        let ip = node_guard.ip;
                        if initial_gossip.elapsed().as_millis() > 1500 {
                            node_guard
                                .gossiper
                                .change_status(ip, NodeStatus::Normal)
                                .ok();
                        }
                        node_guard.gossiper.heartbeat(ip);
                    }
                    let ips: Vec<Ipv4Addr>;
                    let syn;
                    {
                        let node_guard = node.lock().unwrap();
                        ips = node_guard
                            .gossiper
                            .pick_ips(node_guard.get_ip())
                            .iter()
                            .map(|x| **x)
                            .collect();
                        syn = node_guard.gossiper.create_syn(node_guard.ip);
                    }
                    let mut node_guard = node.lock().unwrap();
                    for ip in ips {
                        let connections_clone = Arc::clone(&connections);
                        let msg = InternodeMessage::new(
                            ip.clone(),
                            InternodeMessageContent::Gossip(syn.clone()),
                        );
                        if connect_and_send_message(ip, INTERNODE_PORT, connections_clone, msg)
                            .is_err()
                        {
                            node_guard.gossiper.change_status(ip, NodeStatus::Dead).ok();
                        }
                    }
                }
                // After each gossip round, update the partitioner
                {
                    // Bloqueo del mutex solo para extraer lo necesario
                    let (storage_path, self_ip, keyspaces) = {
                        let node_guard = match node.lock() {
                            Ok(guard) => guard,
                            Err(_) => return NodeError::LockError,
                        };

                        (
                            node_guard.storage_path.clone(), // Clonar el path de almacenamiento
                            node_guard.get_ip().to_string(), // Clonar el IP
                            node_guard.keyspaces.clone(), // Clonar los keyspaces desde el guard     // Referencia mutable al particionador
                        )
                    };
                    let mut node_guard = node.lock().unwrap();
                    let endpoints_states = &node_guard.gossiper.endpoints_state.clone();
                    let partitioner = &mut node_guard.partitioner;
                    let mut needs_to_redistribute = false;

                    for (ip, state) in endpoints_states {
                        let is_in_partitioner: bool;
                        let result = partitioner.node_already_in_partitioner(ip);
                        if let Ok(is_in) = result {
                            is_in_partitioner = is_in;
                        } else {
                            return NodeError::PartitionerError(
                                partitioner::errors::PartitionerError::HashError,
                            );
                        }

                        if state.application_state.status.is_dead() {
                            if is_in_partitioner {
                                needs_to_redistribute = true;
                                partitioner.remove_node(*ip).ok();
                            }
                        } else {
                            if !is_in_partitioner {
                                needs_to_redistribute = true;
                                partitioner.add_node(*ip).ok();
                            }
                        }
                    }
                    if needs_to_redistribute {
                        storage_engine::StorageEngine::new(storage_path.clone(), self_ip.clone())
                            .redistribute_data(keyspaces.clone(), partitioner, connections.clone())
                            .ok();
                    }
                }
                {}
                thread::sleep(std::time::Duration::from_secs(1));
            }
        });
        //handle.join().unwrap();
        Ok(())
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
        consistency_level: &str,
        connection: TcpStream,
        table: Option<Table>,
        keyspace: Option<Keyspace>,
    ) -> Result<i32, NodeError> {
        let all_nodes = self.get_how_many_nodes_i_know();

        let replication_factor = {
            if let Some(value) = keyspace.clone() {
                value.get_replication_factor()
            } else {
                1
            }
        };

        let needed_responses = match query.needed_responses() {
            query_creator::NeededResponseCount::One => 1,
            query_creator::NeededResponseCount::Specific(specific_value) => {
                let calculated_responses = specific_value as usize * replication_factor as usize;
                if calculated_responses > all_nodes {
                    all_nodes
                } else {
                    calculated_responses
                }
            }
        };

        Ok(self.open_query_handler.new_open_query(
            needed_responses as i32,
            connection,
            query,
            consistency_level,
            table,
            keyspace,
        ))
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

    fn get_open_handle_query(&mut self) -> &mut OpenQueryHandler {
        &mut self.open_query_handler
    }

    fn generate_client_id(&mut self) -> i32 {
        self.last_client_id += 1;
        self.clients_keyspace.insert(self.last_client_id, None);
        self.last_client_id
    }

    fn add_keyspace(&mut self, new_keyspace: CreateKeyspace) -> Result<(), NodeError> {
        let new_keyspace = Keyspace::new(new_keyspace);
        if self.keyspaces.contains(&new_keyspace) {
            return Err(NodeError::KeyspaceError);
        }
        self.keyspaces.push(new_keyspace.clone());
        Ok(())
    }

    fn remove_keyspace(&mut self, keyspace_name: String) -> Result<(), NodeError> {
        // Clona los keyspaces para evitar problemas de referencia mutable
        let mut keyspaces = self.keyspaces.clone();

        // Busca el índice del keyspace a eliminar y, si no existe, retorna un error
        let index = keyspaces
            .iter()
            .position(|keyspace| keyspace.get_name() == keyspace_name)
            .ok_or(NodeError::KeyspaceError)?;

        // Elimina el keyspace encontrado
        keyspaces.remove(index);

        // Recorre los clients_keyspace para encontrar y actualizar keyspaces coincidentes
        for (_, client_keyspace) in self.clients_keyspace.iter_mut() {
            if let Some(ref keyspace) = client_keyspace {
                if keyspace == &keyspace_name {
                    *client_keyspace = None;
                }
            }
        }

        // Actualiza los keyspaces después de la eliminación
        self.keyspaces = keyspaces;
        Ok(())
    }

    fn set_actual_keyspace(
        &mut self,
        keyspace_name: String,
        client_id: i32,
    ) -> Result<(), NodeError> {
        // Clona la lista de keyspaces para búsqueda

        // Configurar el keyspace actual del cliente usando el índice encontrado
        self.clients_keyspace.insert(client_id, Some(keyspace_name));

        Ok(())
    }

    fn update_keyspace(&mut self, client_id: i32, new_keyspace: Keyspace) {
        let new_key_name = new_keyspace.clone().get_name().clone();
        self.clients_keyspace
            .insert(client_id, Some(new_key_name.clone()));

        for (i, keyspace) in self.keyspaces.clone().iter().enumerate() {
            if new_key_name == keyspace.get_name() {
                self.keyspaces[i] = new_keyspace.clone();
            }
        }
    }

    fn add_table(&mut self, new_table: CreateTable, keyspace_name: &str) -> Result<(), NodeError> {
        // Encuentra el índice del Keyspace en el Vec
        if let Some(index) = self
            .keyspaces
            .iter()
            .position(|k| k.get_name() == keyspace_name)
        {
            // Obtenemos una referencia mutable del Keyspace en el índice encontrado
            let keyspace = &mut self.keyspaces[index];

            // Modifica el Keyspace agregando la nueva tabla
            for table in &keyspace.get_tables() {
                if table.get_name() == new_table.get_name() {
                    return Err(NodeError::CQLError(CQLError::TableAlreadyExist));
                }
            }
            keyspace.add_table(Table::new(new_table))?;
        } else {
            // Retorna un error si el Keyspace no se encuentra
            return Err(NodeError::KeyspaceError);
        }
        Ok(())
    }

    fn get_table(&self, table_name: String, client_keyspace: Keyspace) -> Result<Table, NodeError> {
        // Busca y devuelve la tabla solicitada
        client_keyspace.get_table(&table_name)
    }

    fn remove_table(&mut self, table_name: String, open_query_id: i32) -> Result<(), NodeError> {
        // Obtiene el keyspace actual del cliente
        let keyspace_name = self
            .get_open_handle_query()
            .get_keyspace_of_query(open_query_id)?
            .ok_or(NodeError::KeyspaceError)?
            .get_name();

        let keyspace = self
            .keyspaces
            .iter_mut()
            .find(|k| &k.get_name() == &keyspace_name)
            .ok_or(NodeError::KeyspaceError)?;
        // Remueve la tabla solicitada del keyspace del cliente
        keyspace.remove_table(&table_name)?;
        Ok(())
    }

    fn update_table(
        &mut self,
        keyspace_name: &str,
        new_table: CreateTable,
    ) -> Result<(), NodeError> {
        // Encuentra el índice del Keyspace en el Vec
        if let Some(index) = self
            .keyspaces
            .iter()
            .position(|k| k.get_name() == keyspace_name)
        {
            // Obtenemos una referencia mutable al Keyspace en el índice encontrado
            let keyspace = &mut self.keyspaces[index];

            // Encuentra la posición de la tabla a actualizar en el keyspace
            let table_index = keyspace
                .tables
                .iter()
                .position(|table| table.get_name() == new_table.get_name())
                .ok_or(NodeError::CQLError(CQLError::InvalidTable))?;

            // Reemplaza la tabla existente con la nueva en el Keyspace
            keyspace.tables[table_index] = Table::new(new_table);
            Ok(())
        } else {
            // Retorna un error si el Keyspace no se encuentra
            Err(NodeError::KeyspaceError)
        }
    }

    fn table_already_exist(
        &mut self,
        table_name: String,
        keyspace_name: String,
    ) -> Result<bool, NodeError> {
        let keyspace = self
            .get_keyspace(&keyspace_name)?
            .ok_or(NodeError::KeyspaceError)?;
        // Verifica si la tabla ya existe en el keyspace del cliente
        for table in keyspace.get_tables() {
            if table.get_name() == table_name {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn get_client_keyspace(&self, client_id: i32) -> Result<Option<Keyspace>, NodeError> {
        let keyspace_name = self
            .clients_keyspace
            .get(&client_id)
            .ok_or(NodeError::InternodeProtocolError)
            .cloned()?;
        if let Some(value) = keyspace_name {
            Ok(self
                .keyspaces
                .iter()
                .find(|k| k.get_name() == value)
                .cloned())
        } else {
            Ok(None)
        }
    }

    fn get_keyspace(&self, keyspace_name: &str) -> Result<Option<Keyspace>, NodeError> {
        Ok(self
            .keyspaces
            .iter()
            .find(|k| k.get_name() == keyspace_name)
            .cloned())
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
        let self_ip;
        {
            let node_guard = node.lock()?;
            self_ip = node_guard.get_ip();

            // if !is_seed {
            //     let message = InternodeProtocolHandler::create_protocol_message(
            //         &node_guard.get_ip_string(),
            //         0,
            //         "HANDSHAKE",
            //         "_",
            //         true,
            //         false,
            //         0,
            //         "None",
            //     );
            //     connect_and_send_message(
            //         seed_ip,
            //         INTERNODE_PORT,
            //         Arc::clone(&connections),
            //         &message,
            //     )?;
            //     node_guard.partitioner.add_node(seed_ip)?;
            // }
        }

        // Creates a thread to handle node connections
        let node_connections_node = Arc::clone(&node);
        let node_connections = Arc::clone(&connections);
        let self_ip_node = self_ip.clone();
        let handle_node_thread = thread::spawn(move || {
            Self::handle_node_connections(node_connections_node, node_connections, self_ip_node)
                .unwrap_or_else(|err| {
                    eprintln!("Error in internode connections: {:?}", err); // Or handle the error as needed
                });
        });

        // Creates a thread to handle gossip
        let gossip_connections = Arc::clone(&connections);
        let node_gossip = Arc::clone(&node);
        Self::start_gossip(node_gossip, gossip_connections).unwrap_or_else(|err| {
            eprintln!("Error in gossip: {:?}", err); // Or handle the error as needed
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

    // Receives packets from the client
    fn handle_incoming_client_messages(
        node: Arc<Mutex<Node>>,
        stream: Arc<Mutex<TcpStream>>,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
    ) -> Result<(), NodeError> {
        // Clone the stream under Mutex protection and create the reader
        let mut stream_guard = stream.lock()?;

        let mut reader = BufReader::new(stream_guard.try_clone().map_err(NodeError::IoError)?);

        let client_id = { node.lock()?.generate_client_id() };

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
                            stream_guard.write(Frame::Ready.to_bytes()?.as_slice())?;
                            stream_guard.flush()?;
                        }
                        Request::Query(query) => {
                            // Handle the query
                            let query_str = query.get_query();
                            let query_consistency_level: &str = &query.get_consistency();
                            let client_stream = stream_guard.try_clone()?;

                            let result = Node::handle_query_execution(
                                query_str,
                                query_consistency_level,
                                &node,
                                connections.clone(),
                                client_stream,
                                client_id,
                            );

                            if let Err(e) = result {
                                let frame = Frame::Error(error::Error::ServerError(e.to_string()));

                                let frame_bytes_result = &frame.to_bytes();
                                let mut frame_bytes = &vec![];
                                if let Ok(value) = frame_bytes_result {
                                    frame_bytes = value;
                                }
                                stream_guard.write(&frame_bytes)?;
                                stream_guard.flush()?;
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
        stream: Arc<Mutex<TcpStream>>,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
    ) -> Result<(), NodeError> {
        // Clone the stream under Mutex protection and create the reader
        let mut reader = {
            let stream_guard = stream.lock()?;
            BufReader::new(stream_guard.try_clone().map_err(NodeError::IoError)?)
        };

        let internode_protocol_handler = InternodeProtocolHandler::new();

        loop {
            // Clean the buffer
            let mut buffer = [0u8; 850000];

            // Execute initial inserts if necessary

            // Self::execute_querys(&node, connections.clone())?;

            // Try to read a line
            let bytes_read = reader.read(&mut buffer);
            let result = InternodeMessage::from_bytes(&buffer);

            let message;

            match result {
                Ok(value) => {
                    message = value;
                }
                Err(_) => {
                    //println!("error al procesar mensaje internodo");
                    // println!("Error al crear los bytes: {:?}", e);

                    continue;
                }
            }

            match bytes_read {
                Ok(0) => {
                    // Connection closed
                    break;
                }
                Ok(_) => {
                    // Process the command with the protocol, passing the buffer and the necessary parameters
                    let result = internode_protocol_handler.handle_command(
                        &node,
                        message.clone(),
                        connections.clone(),
                    );

                    // If there's an error handling the command, exit the loop
                    if let Err(e) = result {
                        eprintln!("{:?} when other node sent me {:?}", e, message);
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

    pub fn current_timestamp() -> i64 {
        Utc::now().timestamp()
    }

    fn handle_query_execution(
        query_str: &str,
        consistency_level: &str,
        node: &Arc<Mutex<Node>>,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        client_connection: TcpStream,
        client_id: i32,
    ) -> Result<(), NodeError> {
        let query = QueryCreator::new()
            .handle_query(query_str.to_string())
            .map_err(NodeError::CQLError)?;

        let open_query_id;
        let self_ip: Ipv4Addr;
        let storage_path;
        {
            let mut guard_node = node.lock()?;
            let keyspace;
            // Obtener el keyspace especificado o el actual del cliente
            if let Some(keyspace_name) = query.get_used_keyspace() {
                keyspace = guard_node.get_keyspace(&keyspace_name)?
            } else {
                keyspace = guard_node.get_client_keyspace(client_id)?;
            }

            // Intentar obtener el nombre de la tabla y buscar la tabla correspondiente en el keyspace
            let table = query.get_table_name().and_then(|table_name| {
                keyspace
                    .clone()
                    .and_then(|k| guard_node.get_table(table_name, k).ok())
            });

            // Agregar la consulta abierta
            open_query_id = guard_node.add_open_query(
                query.clone(),
                consistency_level,
                client_connection.try_clone()?,
                table,
                keyspace,
            )?;
            self_ip = guard_node.get_ip();
            storage_path = guard_node.storage_path.clone();
        }
        let timestamp = Self::current_timestamp();

        let response =
            QueryExecution::new(node.clone(), connections.clone(), storage_path.clone())?.execute(
                query.clone(),
                false,
                false,
                open_query_id,
                client_id,
                Some(timestamp),
            )?;

        if let Some(((finished_responses, failed_nodes), content)) = response {
            let mut guard_node = node.lock()?;
            // Obtener el keyspace especificado o el actual del cliente

            let keyspace = guard_node
                .get_open_handle_query()
                .get_keyspace_of_query(open_query_id)?
                .clone();

            // Intentar obtener el nombre de la tabla y buscar la tabla correspondiente en el keyspace
            let table = query.get_table_name().and_then(|table_name| {
                keyspace
                    .clone()
                    .and_then(|k| guard_node.get_table(table_name, k).ok())
            });
            let columns: Vec<Column> = {
                if let Some(table) = table.clone() {
                    table.get_columns()
                } else {
                    vec![]
                }
            };

            let keyspace_name: String = if let Some(key) = keyspace.clone() {
                key.get_name()
            } else {
                "".to_string()
            };

            let partitioner = guard_node.get_partitioner();
            let query_handler = guard_node.get_open_handle_query();

            for _ in 0..finished_responses {
                let mut select_columns: Vec<String> = vec![];
                let mut values: Vec<Vec<String>> = vec![];
                let mut complete_columns: Vec<String> = vec![];
                if let Some(cont) = content.content.clone() {
                    complete_columns = cont.columns.clone();
                    select_columns = cont.select_columns.clone();
                    values = cont.values.clone();
                }

                InternodeProtocolHandler::add_ok_response_to_open_query_and_send_response_if_closed(
                    query_handler,
                    // TODO: convertir el content al content de la response
                    &InternodeResponse::new(open_query_id as u32, InternodeResponseStatus::Ok, Some(InternodeResponseContent{
                        columns: complete_columns,
                        select_columns:  select_columns,
                        values: values,
                    })),
                    open_query_id,
                    keyspace_name.clone(),
                    table.clone(),
                    columns.clone(),
                    self_ip,
                    self_ip,
                    connections.clone(),
                    partitioner.clone(),
                    storage_path.clone()

                )?;
            }
            for _ in 0..failed_nodes {
                InternodeProtocolHandler::add_error_response_to_open_query_and_send_response_if_closed(
                    query_handler,
                    open_query_id,

                )?;
            }
        }

        Ok(())
    }
}
