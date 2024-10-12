use std::io::{BufRead, BufReader, Write};
use std::net::{Ipv4Addr, TcpListener, TcpStream, SocketAddrV4};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::thread;
use keyspace::Keyspace;
use partitioner::Partitioner;
use query_coordinator::clauses::insert_sql::Insert;
use query_coordinator::clauses::keyspace::create_keyspace_cql::CreateKeyspace;
use query_coordinator::clauses::keyspace::drop_keyspace_cql::DropKeyspace;
use query_coordinator::clauses::keyspace::alter_keyspace_cql::AlterKeyspace;
use query_coordinator::clauses::table::alter_table_cql::AlterTable;
use query_coordinator::clauses::table::create_table_cql::CreateTable;
use query_coordinator::clauses::table::drop_table_cql::DropTable;
use query_coordinator::errors::CQLError;
use query_coordinator::QueryCoordinator;
use query_coordinator::Query;
mod query_execution;
use query_execution::QueryExecution;
mod errors;
use errors::NodeError;
mod table;
mod keyspace;
use crate::table::Table;

pub struct Node {
    ip: Ipv4Addr,
    seeds_node: Vec<Ipv4Addr>,
    port: u16,
    partitioner: Partitioner,
    keyspaces: Vec<Keyspace>,
    actual_keyspace: Option<Keyspace>
}

impl Node {
    pub fn new(ip: Ipv4Addr, seeds_node: Vec<Ipv4Addr>) -> Result<Node, NodeError> {
        let mut partitioner = Partitioner::new();
        partitioner.add_node(ip)?;
        Ok(Node {
            ip,
            seeds_node,
            port: 0,
            partitioner,
            keyspaces: vec![],
            actual_keyspace: None
        })
    }

    pub fn get_ip(&self) -> Ipv4Addr {
        self.ip
    }

    pub fn is_seed(&self) -> bool {
        self.seeds_node.contains(&self.get_ip())
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
            .as_ref()  // Convierte Option<CreateKeyspace> en Option<&CreateKeyspace>
            .map(|keyspace| keyspace.get_name())  // Obtiene el nombre si existe
            .ok_or(NodeError::OtherError)  // Si es None, devuelve un error
    }


    pub fn add_keyspace(&mut self, new_keyspace: CreateKeyspace) -> Result<(), NodeError> {

        let new_keyspace = Keyspace::new(new_keyspace);
        if self.keyspaces.contains(&new_keyspace){
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
        if self.actual_keyspace_name().is_ok() && self.actual_keyspace_name()? == keyspace_name{
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
        self.actual_keyspace.clone().ok_or(NodeError::OtherError)?.get_table(&table_name) // Clona el valor encontrado para devolverlo
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
        let index = self.keyspaces
            .iter()
            .position(|table| table.get_name() == new_keyspace.get_name())
            .ok_or(NodeError::CQLError(CQLError::InvalidTable))?;
        
        // Reemplaza la tabla existente en la posición encontrada con la nueva tabla
        self.keyspaces[index] = new_keyspace.clone();

        //Actualizamos el actual si es que era ese
        if self.actual_keyspace.is_some() && self.actual_keyspace.clone().ok_or(NodeError::OtherError)? == new_keyspace{
            self.actual_keyspace = Some(new_keyspace);
        }
        Ok(())
    }
    pub fn table_already_exist(&self, table_name: String) -> Result<bool, NodeError> {
        // Obtiene una referencia a `actual_keyspace` si existe; si no, devuelve un error
        let keyspace = self.actual_keyspace.as_ref().ok_or(NodeError::OtherError)?;
    
        for table in keyspace.get_tables(){
            if table.get_name() == table_name{
                return  Ok(true);
            }
        }
    
        Ok(false)
    }
    
    pub fn start(
        node: Arc<Mutex<Node>>,
        port: u16,
        connections: Arc<Mutex<Vec<TcpStream>>>,
    ) -> Result<(), NodeError> {
        let address = {
            let mut node_guard = node.lock()?;
            node_guard.port = port;
            SocketAddrV4::new(node_guard.ip, port)
        };

        let is_seed = node.lock()?.is_seed();
        let seed_ip = node.lock()?.seeds_node[0];

        {
            let mut node_guard = node.lock()?;
            if !is_seed {
                println!("El nodo NO es semilla");
                if let Ok(mut stream) = node_guard.connect(node_guard.seeds_node[0], Arc::clone(&connections)) {
                    let message = format!("IP {}", node_guard.ip.to_string());
                    node_guard.send_message(&mut stream, &message)?;
                    node_guard.partitioner.add_node(seed_ip)?;
                }
            } else {
                println!("El Nodo ES semilla");
            }
        }

        let listener = TcpListener::bind(address)?;
        
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let mut connections_guard = connections.lock()?;
                    connections_guard.push(stream.try_clone()?);

                    let node_clone = Arc::clone(&node);
                    let stream_clone = stream.try_clone()?;
                    let connections_clone = Arc::clone(&connections);

                    thread::spawn(move || {
                        if let Err(e) = Node::handle_incoming_messages(node_clone, stream_clone, connections_clone, is_seed) {
                            eprintln!("Error handling incoming message: {:?}", e);
                        }
                    });
                }
                Err(e) => {
                    eprintln!("Error al aceptar conexión: {:?}", e);
                }
            }
        }
        Ok(())
    }

    pub fn connect(&self, peer_ip: Ipv4Addr, connections: Arc<Mutex<Vec<TcpStream>>>) -> Result<TcpStream, NodeError> {
        let address = SocketAddrV4::new(peer_ip, self.port);
        let stream = TcpStream::connect(address)?;
        {
            let mut connections_guard = connections.lock()?;
            connections_guard.push(stream.try_clone()?);
        }
        Ok(stream)
    }

    pub fn send_message(&self, stream: &mut TcpStream, message: &str) -> Result<(), NodeError> {
        stream.write_all(message.as_bytes())?;
        stream.write_all(b"\n")?;
        Ok(())
    }

    pub fn handle_incoming_messages(
        node: Arc<Mutex<Node>>,
        stream: TcpStream,
        connections: Arc<Mutex<Vec<TcpStream>>>,
        is_seed: bool,
    ) -> Result<(), NodeError> {
        let mut reader = BufReader::new(stream.try_clone().map_err(NodeError::IoError)?);
        let mut buffer = String::new();

        loop {
           
            Node::execute_initial_insert(node.clone(), connections.clone())?;
            
            buffer.clear();
            let bytes_read = reader.read_line(&mut buffer).map_err(NodeError::IoError)?;
            if bytes_read == 0 {
                println!("Conexión cerrada por el peer.");
                break;
            }

            let tokens: Vec<&str> = buffer.trim().split_whitespace().collect();
            if tokens.is_empty() {
                continue;
            }

            let command = tokens[0];
            
            match command {
                "IP" => Node::handle_ip_command(&node, tokens, connections.clone(), is_seed)?,
                "INSERT" => Node::handle_insert_command(&node, tokens, connections.clone(), false)?,
                "INSERT_INTERNODE" => Node::handle_insert_command(&node, tokens, connections.clone(), true)?,
                "CREATE_TABLE" => Node::handle_create_table_command(&node, tokens, connections.clone(),false)?,
                "CREATE_TABLE_INTERNODE" => Node::handle_create_table_command(&node, tokens, connections.clone(),true)?,
                "DROP_TABLE" => Node::handle_drop_table_command(&node, tokens, connections.clone(),false)?,
                "DROP_TABLE_INTERNODE" => Node::handle_drop_table_command(&node, tokens, connections.clone(),true)?,
                "ALTER_TABLE" => Node::handle_alter_table_command(&node, tokens, connections.clone(),false)?,
                "ALTER_TABLE_INTERNODE" => Node::handle_alter_table_command(&node, tokens, connections.clone(),true)?,
                "CREATE_KEYSPACE" => Node::handle_create_keyspace_command(&node, tokens, connections.clone(),false)?,
                "CREATE_KEYSPACE_INTERNODE" => Node::handle_create_keyspace_command(&node, tokens, connections.clone(),true)?,
                "DROP_KEYSPACE" => Node::handle_drop_keyspace_command(&node, tokens, connections.clone(),false)?,
                "DROP_KEYSPACE_INTERNODE" => Node::handle_drop_keyspace_command(&node, tokens, connections.clone(),true)?,
                "ALTER_KEYSPACE" => Node::handle_alter_keyspace_command(&node, tokens, connections.clone(),false)?,
                "ALTER_KEYSPACE_INTERNODE" => Node::handle_alter_keyspace_command(&node, tokens, connections.clone(),true)?,
                _ => println!("Comando desconocido: {}", command),
            }
        
            // let node = node.lock()?;
            // // Imprimir un string vacío si `node.actual_keyspace` es `None`
            // println!(
            //     "El actual keyspace es {:?}",
            //     node.actual_keyspace.as_ref().map(|ks| format!("{:?}", ks)).unwrap_or_else(|| "".to_string())
            // );
    
            // // Imprimir las tablas, mostrando un string vacío si `node.actual_keyspace` es `None`
            // println!(
            //     "Las tablas que tiene son {:?}",
            //     node.actual_keyspace.as_ref().map(|ks| format!("{:?}", ks.tables)).unwrap_or_else(|| "".to_string())
            // );
    
        
        }
        Ok(())
        
    }

    // Función para verificar si el particionador está lleno y el nodo es una semilla
    fn initial_condition(node: &Arc<Mutex<Node>>) -> Result<bool, NodeError> {
        let lock_node = node.lock().map_err(|_| NodeError::LockError)?;
        Ok(lock_node.get_partitioner().get_nodes().len() == 4 && lock_node.is_seed())
    }

    // Función para ejecutar múltiples inserciones iniciales cuando el particionador está lleno
    fn execute_initial_insert(node: Arc<Mutex<Node>>, connections: Arc<Mutex<Vec<TcpStream>>>) -> Result<(), NodeError> {
        if !Node::initial_condition(&node)? {
            return Ok(());
        }

        let queries = vec![
            "CREATE KEYSPACE world WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}",
            "CREATE TABLE people (id INT PRIMARY KEY, name TEXT, weight INT)",
            "CREATE TABLE city (id INT PRIMARY KEY, name TEXT, country TEXT)",
            "INSERT INTO people (id, name, weight) VALUES (1,'Lorenzo', 39)",
            "INSERT INTO people (id, name, weight) VALUES (2,'Maggie', 67)",
            "INSERT INTO people (id, name, weight) VALUES (1,'Palta', 41)",
            "INSERT INTO city (id, name, country) VALUES (5,'Fucking', 'Brazil')",
            "INSERT INTO people (id, name, weight) VALUES (7,'Nashville',32)",
            "DROP TABLE people",
            "ALTER KEYSPACE world WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 4}"

        ];

        for query_str in queries {
            let query = QueryCoordinator::new()
                .handle_query(query_str.to_string())
                .map_err(NodeError::CQLError)?;
            QueryExecution::new(node.clone(), connections.clone()).execute(query, false)?;
        }

        Ok(())
    }


   
    // Función para manejar el comando "IP"
    fn handle_ip_command(
        node: &Arc<Mutex<Node>>,
        tokens: Vec<&str>,
        connections: Arc<Mutex<Vec<TcpStream>>>,
        is_seed: bool,
    ) -> Result<(), NodeError> {

        let new_ip = Ipv4Addr::from_str(tokens.get(1).ok_or(NodeError::OtherError)?)
            .map_err(|_| NodeError::OtherError)?;
        let mut lock_node = node.lock().map_err(|_| NodeError::LockError)?;
        let self_ip = lock_node.get_ip();

        if self_ip != new_ip && !lock_node.partitioner.contains_node(&new_ip) {
            lock_node.partitioner.add_node(new_ip)?;
        }

        if is_seed {
            for ip in lock_node.get_partitioner().get_nodes() {
                if new_ip != ip && self_ip != ip {
                    lock_node.forward_message(connections.clone(), new_ip, ip)?;
                    lock_node.forward_message(connections.clone(), ip, new_ip)?;
                }
            }
        }

        Ok(())
    }

    // Función para manejar el comando "INSERT"
    fn handle_insert_command(
        node: &Arc<Mutex<Node>>,
        tokens: Vec<&str>,
        connections: Arc<Mutex<Vec<TcpStream>>>,
        internode: bool
    ) -> Result<(), NodeError> {
        let query_str = tokens.get(1..).ok_or(NodeError::OtherError)?.join(" ");
        let query = Insert::deserialize(&query_str).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections).execute(Query::Insert(query),internode)
    }

     // Función para manejar el comando "IP"
     fn handle_create_table_command(
        node: &Arc<Mutex<Node>>,
        tokens: Vec<&str>,
        connections: Arc<Mutex<Vec<TcpStream>>>,
        internode: bool,
    ) -> Result<(), NodeError> {
        let query_str = tokens.get(1..).ok_or(NodeError::OtherError)?.join(" ");
        let query = CreateTable::deserialize(&query_str).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections).execute(Query::CreateTable(query),internode)
    }


     // Función para manejar el comando "IP"
     fn handle_drop_table_command(
        node: &Arc<Mutex<Node>>,
        tokens: Vec<&str>,
        connections: Arc<Mutex<Vec<TcpStream>>>,
        internode: bool,
    ) -> Result<(), NodeError> {
        let query_str = tokens.get(1..).ok_or(NodeError::OtherError)?.join(" ");
        let query = DropTable::deserialize(&query_str).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections).execute(Query::DropTable(query),internode)
    }


    // Función para manejar el comando "IP"
    fn handle_alter_table_command(
        node: &Arc<Mutex<Node>>,
        tokens: Vec<&str>,
        connections: Arc<Mutex<Vec<TcpStream>>>,
        internode: bool,
    ) -> Result<(), NodeError> {
        let query_str = tokens.get(1..).ok_or(NodeError::OtherError)?.join(" ");
        let query = AlterTable::deserialize(&query_str).map_err(NodeError::CQLError)?;
        
        QueryExecution::new(node.clone(), connections).execute(Query::AlterTable(query),internode)
    }

    // Función para manejar el comando "IP"
    fn handle_create_keyspace_command(
        node: &Arc<Mutex<Node>>,
        tokens: Vec<&str>,
        connections: Arc<Mutex<Vec<TcpStream>>>,
        internode: bool,
    ) -> Result<(), NodeError> {
        let query_str = tokens.get(1..).ok_or(NodeError::OtherError)?.join(" ");
        let query = CreateKeyspace::deserialize(&query_str).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections).execute(Query::CreateKeyspace(query),internode)
    }


     // Función para manejar el comando "IP"
     fn handle_drop_keyspace_command(
        node: &Arc<Mutex<Node>>,
        tokens: Vec<&str>,
        connections: Arc<Mutex<Vec<TcpStream>>>,
        internode: bool,
    ) -> Result<(), NodeError> {

        let query_str = tokens.get(1..).ok_or(NodeError::OtherError)?.join(" ");
        let query = DropKeyspace::deserialize(&query_str).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections).execute(Query::DropKeyspace(query),internode)
    }


    // Función para manejar el comando "IP"
    fn handle_alter_keyspace_command(
        node: &Arc<Mutex<Node>>,
        tokens: Vec<&str>,
        connections: Arc<Mutex<Vec<TcpStream>>>,
        internode: bool,
    ) -> Result<(), NodeError> {
        let query_str = tokens.get(1..).ok_or(NodeError::OtherError)?.join(" ");
        let query = AlterKeyspace::deserialize(&query_str).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections).execute(Query::AlterKeyspace(query),internode)
        
        
    }

    fn forward_message(
        &self,
        connections: Arc<Mutex<Vec<TcpStream>>>,
        new_ip: Ipv4Addr,
        target_ip: Ipv4Addr,
    ) -> Result<(), NodeError> {
        let mut tcp = self.connect(target_ip, Arc::clone(&connections))?;
        let message = format!("IP {}", new_ip);
        self.send_message(&mut tcp, &message)?;
        Ok(())
    }
}
