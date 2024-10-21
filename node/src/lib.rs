use keyspace::Keyspace;
use native_protocol::frame::Frame;
use native_protocol::Serializable;
use open_query_handler::OpenQueryHandler;
use partitioner::Partitioner;
use query_coordinator::clauses::keyspace::create_keyspace_cql::CreateKeyspace;
use query_coordinator::clauses::table::create_table_cql::CreateTable;
use query_coordinator::errors::CQLError;
use query_coordinator::Query;
use query_coordinator::{NeededResponses, QueryCoordinator};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Ipv4Addr, SocketAddrV4, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread::{self};
mod query_execution;
use query_execution::QueryExecution;
mod internode_protocol_handler;
use internode_protocol_handler::InternodeProtocolHandler;
mod errors;
use errors::NodeError;
mod keyspace;
mod open_query_handler;
mod table;
mod utils;
use crate::table::Table;
use crate::utils::{connect, send_message};
use driver::server::{handle_client_request, Request};

const CLIENT_NODE_PORT: u16 = 0x4645; // Hexadecimal de "FE" (FERRUM) = 17989
const INTERNODE_PORT: u16 = 0x554D; // Hexadecimal de "UM" (FERRUM) = 21837

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

    // pub fn add_response_to_open_query(&mut self, open_query_id: i32, response: String)->bool{
    //     self.open_query_handler.add_response(open_query_id, response)
    // }

    pub fn add_open_query(&mut self, needed_responses: i32, connection: TcpStream) -> i32 {
        self.open_query_handler
            .new_open_query(needed_responses, connection)
    }

    pub fn remove_open_query(&mut self, id: i32) {
        self.open_query_handler.remove_query(&id);
    }

    pub fn is_seed(&self) -> bool {
        self.seeds_nodes.contains(&self.ip)
    }

    pub fn get_ip(&self) -> Ipv4Addr {
        self.ip
    }

    pub fn get_ip_string(&self) -> String {
        self.ip.to_string()
    }

    pub fn get_how_many_nodes_i_know(&self) -> usize {
        self.partitioner.get_nodes().len() - 1
    }
    pub fn get_partitioner(&self) -> Partitioner {
        self.partitioner.clone()
    }

    // Método para verificar si no hay keyspace actual
    pub fn has_no_actual_keyspace(&self) -> bool {
        self.actual_keyspace.is_none()
    }

    // Método para obtener el nombre del keyspace actual si existe
    pub fn actual_keyspace_name(&self) -> Result<String, NodeError> {
        self.actual_keyspace
            .as_ref() // Convierte Option<CreateKeyspace> en Option<&CreateKeyspace>
            .map(|keyspace| keyspace.get_name()) // Obtiene el nombre si existe
            .ok_or(NodeError::OtherError) // Si es None, devuelve un error
    }

    pub fn get_open_hanlde_query(&mut self) -> &mut OpenQueryHandler {
        &mut self.open_query_handler
    }

    pub fn add_keyspace(&mut self, new_keyspace: CreateKeyspace) -> Result<(), NodeError> {
        let new_keyspace = Keyspace::new(new_keyspace);
        if self.keyspaces.contains(&new_keyspace) {
            return Err(NodeError::CQLError(CQLError::InvalidTable));
        }
        self.keyspaces.push(new_keyspace.clone());
        self.actual_keyspace = Some(new_keyspace);
        Ok(())
    }

    pub fn remove_keyspace(&mut self, keyspace_name: String) -> Result<(), NodeError> {
        let mut keyspaces = self.keyspaces.clone();

        let index = keyspaces
            .iter()
            .position(|keyspace| keyspace.get_name() == keyspace_name)
            .ok_or(NodeError::CQLError(CQLError::InvalidTable))?;

        keyspaces.remove(index);
        if self.actual_keyspace_name().is_ok() && self.actual_keyspace_name()? == keyspace_name {
            self.actual_keyspace = None;
        }

        self.keyspaces = keyspaces;
        Ok(())
    }

    pub fn add_table(&mut self, new_table: CreateTable) -> Result<(), NodeError> {
        let mut new_keyspace = self.actual_keyspace.clone().ok_or(NodeError::OtherError)?;
        new_keyspace.add_table(Table::new(new_table))?;
        self.actual_keyspace = Some(new_keyspace);
        Ok(())
    }

    pub fn get_table(&self, table_name: String) -> Result<Table, NodeError> {
        self.actual_keyspace
            .clone()
            .ok_or(NodeError::OtherError)?
            .get_table(&table_name) // Clona el valor encontrado para devolverlo
    }

    pub fn remove_table(&mut self, table_name: String) -> Result<(), NodeError> {
        let keyspace = self.actual_keyspace.as_mut().ok_or(NodeError::OtherError)?;
        keyspace.remove_table(&table_name)?;
        Ok(())
    }

    pub fn update_table(&mut self, new_table: CreateTable) -> Result<(), NodeError> {
        // Obtiene una referencia mutable a `actual_keyspace` si existe
        let keyspace = self.actual_keyspace.as_mut().ok_or(NodeError::OtherError)?;

        // Encuentra la posición de la tabla en el vector `tables` del `Keyspace`
        let index = keyspace
            .tables
            .iter()
            .position(|table| table.get_name() == new_table.get_name())
            .ok_or(NodeError::CQLError(CQLError::InvalidTable))?;

        // Reemplaza la tabla existente en la posición encontrada con la nueva tabla
        keyspace.tables[index] = Table::new(new_table);

        Ok(())
    }

    pub fn update_keyspace(&mut self, new_keyspace: Keyspace) -> Result<(), NodeError> {
        // Encuentra la posición del keyspace en kesyapces
        let index = self
            .keyspaces
            .iter()
            .position(|table| table.get_name() == new_keyspace.get_name())
            .ok_or(NodeError::CQLError(CQLError::InvalidTable))?;

        // Reemplaza la tabla existente en la posición encontrada con la nueva tabla
        self.keyspaces[index] = new_keyspace.clone();

        //Actualizamos el actual si es que era ese
        if self.actual_keyspace.is_some()
            && self.actual_keyspace.clone().ok_or(NodeError::OtherError)? == new_keyspace
        {
            self.actual_keyspace = Some(new_keyspace);
        }
        Ok(())
    }
    pub fn table_already_exist(&self, table_name: String) -> Result<bool, NodeError> {
        // Obtiene una referencia a `actual_keyspace` si existe; si no, devuelve un error
        let keyspace = self.actual_keyspace.as_ref().ok_or(NodeError::OtherError)?;

        for table in keyspace.get_tables() {
            if table.get_name() == table_name {
                return Ok(true);
            }
        }

        Ok(false)
    }

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
                    let stream = Arc::new(Mutex::new(stream)); // Encapsulamos en Arc<Mutex<TcpStream>>
                    let message = InternodeProtocolHandler::create_protocol_message(
                        &node_guard.get_ip_string(),
                        0,
                        "HANDSHAKE",
                        "_",
                        true,
                    );
                    let mut stream_guard = stream.lock()?;
                    send_message(&mut stream_guard, &message)?;
                    node_guard.partitioner.add_node(seed_ip)?;
                }
            }
        }

        // Crea un hilo para manejar las conexiones de nodos
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
            .unwrap_or_else(|e| eprintln!("Error in node connection handler: {:?}", e));
        });

        // Crea un hilo para manejar las conexiones de clientes
        let client_connections_node = Arc::clone(&node);
        let client_connections = Arc::clone(&connections);
        let self_ip_client = self_ip;

        let handle_client_thread = thread::spawn(move || {
            Self::handle_client_connections(
                client_connections_node,
                client_connections,
                self_ip_client,
            )
            .unwrap_or_else(|e| eprintln!("Error in client connection handler: {:?}", e));
        });

        handle_node_thread
            .join()
            .map_err(|_| NodeError::OtherError)?;
        handle_client_thread
            .join()
            .map_err(|_| NodeError::OtherError)?;

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
                    let stream = Arc::new(Mutex::new(stream)); // Encapsulamos el stream en Arc<Mutex<TcpStream>>
                    let connections_clone = Arc::clone(&connections);

                    thread::spawn(move || {
                        if let Err(e) = Node::handle_incoming_internode_messages(
                            node_clone,
                            stream,
                            connections_clone,
                            is_seed,
                        ) {
                            eprintln!("Error handling incoming node message: {:?}", e);
                        }
                    });
                }
                Err(e) => {
                    eprintln!("Error accepting node connection: {:?}", e);
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
        let socket = SocketAddrV4::new(self_ip, CLIENT_NODE_PORT); // Puerto específico para clientes
        let listener = TcpListener::bind(socket)?;

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let node_clone = Arc::clone(&node);
                    let stream = Arc::new(Mutex::new(stream));
                    let connections_clone = Arc::clone(&connections);

                    thread::spawn(move || {
                        match Node::handle_incoming_client_messages(
                            node_clone,
                            stream,
                            connections_clone,
                        ) {
                            Ok(_) => {}
                            Err(_) => todo!(),
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
        // Primero intentamos reutilizar o establecer una conexión
        let mut tcp = connect(target_ip, INTERNODE_PORT, Arc::clone(&connections))?;

        let message = InternodeProtocolHandler::create_protocol_message(
            &sent_ip.to_string(),
            0,
            "HANDSHAKE",
            "_",
            true,
        );

        send_message(&mut tcp, &message)?;
        Ok(())
    }

    // recibe los paquetes desde el cliente
    pub fn handle_incoming_client_messages(
        node: Arc<Mutex<Node>>,
        stream: Arc<Mutex<TcpStream>>,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
    ) -> Result<(), NodeError> {
        // Clona el stream bajo protección Mutex y crea el lector
        let mut stream_guard = stream.lock()?;

        let mut reader = BufReader::new(stream_guard.try_clone().map_err(NodeError::IoError)?);

        loop {
            // Limpiamos el buffer
            // let mut buffer = String::new();

            let mut buffer = [0; 2048];

            // Ejecuta inserciones iniciales si es necesario

            // Intentamos leer una línea
            // let bytes_read = reader.read_line(&mut buffer);
            let bytes_read = reader.read(&mut buffer);

            match bytes_read {
                Ok(0) => {
                    // Conexión cerrada
                    break;
                }
                Ok(_) => {
                    let query = handle_client_request(&buffer);

                    match query {
                        Request::Startup => {
                            // let mut stream_guard = stream.lock()?;
                            stream_guard.write(Frame::Ready.to_bytes().as_slice())?;
                            stream_guard.flush()?;
                        }
                        Request::Query(query) => {
                            // handle la query
                            let query_str = &query.query;
                            let client_stream = stream_guard.try_clone()?;

                            Node::handle_query_execution(
                                query_str,
                                &node,
                                connections.clone(),
                                client_stream,
                            )?;
                        }
                    };
                }
                Err(e) => {
                    // Otro tipo de error
                    eprintln!("Error de lectura en handle_incoming_messages: {:?}", e);
                    break;
                }
            }
        }

        Ok(())
    }

    pub fn handle_incoming_internode_messages(
        node: Arc<Mutex<Node>>,
        mut stream: Arc<Mutex<TcpStream>>,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        is_seed: bool,
    ) -> Result<(), NodeError> {
        // Clona el stream bajo protección Mutex y crea el lector
        let mut reader = {
            let stream_guard = stream.lock()?;
            BufReader::new(stream_guard.try_clone().map_err(NodeError::IoError)?)
        };

        let internode_protocol_handler = InternodeProtocolHandler::new();

        loop {
            // Limpiamos el buffer
            let mut buffer = String::new();

            // Ejecuta inserciones iniciales si es necesario

            // Self::execute_querys(&node, connections.clone())?;

            // Intentamos leer una línea
            let bytes_read = reader.read_line(&mut buffer);
            match bytes_read {
                Ok(0) => {
                    // Conexión cerrada
                    break;
                }
                Ok(_) => {
                    // Procesa el comando con el protocolo, pasándole el buffer y los parámetros necesarios
                    let buffer_cop = buffer.clone();
                    let result = internode_protocol_handler.handle_command(
                        &node,
                        &buffer.trim().to_string(),
                        &mut stream,
                        connections.clone(),
                        is_seed,
                    );

                    // Si hay un error al manejar el comando, salimos del bucle
                    if let Err(e) = result {
                        eprintln!(
                            "Error handling command: {:?} cuando le pase {:?}",
                            e, buffer_cop
                        );
                        break;
                    }
                }
                Err(e) => {
                    // Otro tipo de error
                    eprintln!("Error de lectura en handle_incoming_messages: {:?}", e);
                    break;
                }
            }
        }

        Ok(())
    }

    fn handle_query_execution(
        query_str: &str,
        node: &Arc<Mutex<Node>>,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        mut client_connection: TcpStream,
    ) -> Result<(), NodeError> {
        let query = QueryCoordinator::new()
            .handle_query(query_str.to_string())
            .map_err(NodeError::CQLError)?;

        let query_id;
        {
            let mut guard_node = node.lock()?;
            let all_nodes = guard_node.get_how_many_nodes_i_know();
            let needed_responses = match query.needed_responses() {
                query_coordinator::NeededResponseCount::AllNodes => all_nodes,
                query_coordinator::NeededResponseCount::Specific(specific_value) => {
                    specific_value as usize
                }
            };
            query_id =
                guard_node.add_open_query(needed_responses as i32, client_connection.try_clone()?);
        }

        let response = QueryExecution::new(node.clone(), connections.clone()).execute(
            query.clone(),
            false,
            query_id,
        )?;

        if let Some(value) = response {
            //creamos mensaje de respuesta del native
            client_connection.write(value.as_bytes())?;
            client_connection.flush()?;
        }

        Ok(())
    }
}
