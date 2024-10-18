use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::net::{Ipv4Addr, SocketAddrV4, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use keyspace::Keyspace;
use open_query_handler::OpenQueryHandler;
use partitioner::Partitioner;
use query_coordinator::clauses::keyspace::create_keyspace_cql::CreateKeyspace;
use query_coordinator::clauses::table::create_table_cql::CreateTable;
use query_coordinator::errors::CQLError;
use query_coordinator::{NeededResponses, QueryCoordinator};
use query_coordinator::Query;
mod query_execution;
use query_execution::QueryExecution;
mod internode_protocol_handler;
use internode_protocol_handler::InternodeProtocolHandler;
mod errors;
use errors::NodeError;
mod table;
mod keyspace;
mod utils;
mod open_query_handler;
use crate::utils::{send_message, connect};
use crate::table::Table;
use std::thread::sleep;
use std::time::Duration;



const  _CLIENT_NODE_PORT_1: u16 = 0x4645; // Hexadecimal de "FE" (FERRUM) = 17989
const  INTERNODE_PORT: u16 = 0x554D; // Hexadecimal de "UM" (FERRUM) = 21837

pub struct Node {
    ip: Ipv4Addr,
    seeds_nodes: Vec<Ipv4Addr>,
    partitioner: Partitioner,
    open_query_handler: OpenQueryHandler,
    keyspaces: Vec<Keyspace>,
    actual_keyspace: Option<Keyspace>,
    aux: bool
}

impl Node {
    pub fn new(ip:Ipv4Addr, seeds_nodes: Vec<Ipv4Addr>) -> Result<Node, NodeError> {
        let mut partitioner = Partitioner::new();
        partitioner.add_node(ip)?;
        Ok(Node {
            ip,
            seeds_nodes,
            partitioner,
            open_query_handler: OpenQueryHandler::new(),
            keyspaces: vec![],
            actual_keyspace: None,
            aux: true,
        })
    }

    // pub fn add_response_to_open_query(&mut self, open_query_id: i32, response: String)->bool{
    //     self.open_query_handler.add_response(open_query_id, response)
    // }

    pub fn add_open_query(&mut self, needed_responses: i32) -> i32{
        self.open_query_handler.new_open_query(needed_responses)
    }

    pub fn remove_open_query(&mut self, id: i32) {
        self.open_query_handler.remove_query(&id);
    }

    pub fn is_seed(&self) -> bool {
        self.seeds_nodes.contains(&self.ip)
    }

    pub fn get_ip(&self)->Ipv4Addr{
        self.ip
    }

    pub fn get_ip_string(&self)->String{
        self.ip.to_string()
    }

    pub fn get_how_many_nodes_i_know(&self) -> usize{
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
            .as_ref()  // Convierte Option<CreateKeyspace> en Option<&CreateKeyspace>
            .map(|keyspace| keyspace.get_name())  // Obtiene el nombre si existe
            .ok_or(NodeError::OtherError)  // Si es None, devuelve un error
    }


    pub fn get_open_hanlde_query(&mut self)-> &mut OpenQueryHandler{
        &mut self.open_query_handler
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
    connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>, // Modificación aquí
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
                let message = InternodeProtocolHandler::create_protocol_message(&node_guard.get_ip_string(), 0,"HANDSHAKE", "_", true);
                
                // Usamos el Mutex para enviar el mensaje de forma segura
                let mut stream_guard = stream.lock()?;
                send_message(&mut stream_guard, &message)?;
                node_guard.partitioner.add_node(seed_ip)?;
                
            }
        } 
    }

    let socket = SocketAddrV4::new(self_ip, INTERNODE_PORT);
    let listener = TcpListener::bind(socket)?;

    for stream in listener.incoming() {
        
        match stream {
            Ok(stream) => {
                let node_clone = Arc::clone(&node);
                let stream = Arc::new(Mutex::new(stream)); // Encapsulamos el stream en Arc<Mutex<TcpStream>>
                let connections_clone = Arc::clone(&connections);

                thread::spawn(move || {
                    if let Err(e) = Node::handle_incoming_messages(node_clone, stream, connections_clone, is_seed) {
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

    fn forward_message(
        &self,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        sent_ip: Ipv4Addr,
        target_ip: Ipv4Addr,
    ) -> Result<(), NodeError> {
        // Primero intentamos reutilizar o establecer una conexión
        let mut tcp = connect(target_ip, INTERNODE_PORT,Arc::clone(&connections))?;

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



    pub fn handle_incoming_messages(
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
            
            Self::execute_querys(&node, connections.clone())?;
                

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
                        eprintln!("Error handling command: {:?} cuando le pase {:?}", e, buffer_cop);
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
        
    // Función para verificar si el particionador está lleno y el nodo es una semilla
    fn condition(node: &Arc<Mutex<Node>>) -> Result<bool, NodeError> {
        let mut lock_node = node.lock().map_err(|_| NodeError::LockError)?;
        let a =lock_node.get_partitioner().get_nodes().len() == 4 && lock_node.is_seed() && lock_node.aux == true;
        if a {lock_node.aux = false;}
        Ok(a)
    }

    // Función para ejecutar múltiples inserciones iniciales cuando el particionador está lleno
    fn execute_querys(node: &Arc<Mutex<Node>>, connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>) -> Result<(), NodeError> {

        if !Node::condition(&node)? {
            return Ok(());
        }

        let queries = vec![
            "CREATE KEYSPACE world WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}",
            "CREATE TABLE people (id INT PRIMARY KEY, name TEXT, weight INT)",
            "INSERT INTO people (id, name, weight) VALUES (1,'Lorenzo', 39)",
            "INSERT INTO people (id, name, weight) VALUES (2,'Lorenzo', 67)",
            "INSERT INTO people (id, name, weight) VALUES (3,'Lorenzo',32)",
            "INSERT INTO people (id, name, weight) VALUES (4,'Maggie', 39)",
            "INSERT INTO people (id, name, weight) VALUES (5,'parafresco', 67)",
            "INSERT INTO people (id, name, weight) VALUES (6,'Maggie',32)",
            "INSERT INTO people (id, name, weight) VALUES (7,'Maggie', 39)",
            "INSERT INTO people (id, name, weight) VALUES (8,'Maggie', 67)",
            "INSERT INTO people (id, name, weight) VALUES (9,'Maggie',32)",
            "UPDATE people SET name = 'Pablo', weight = 8  WHERE id = 11",
            "UPDATE people SET name = 'Nestum' WHERE id = 8",
            "DELETE FROM people WHERE id = 5",
            //"SELECT id,name FROM people WHERE id = 3",
        ];

        for query_str in queries {
            sleep(Duration::from_millis(50));
            let query = QueryCoordinator::new()
                .handle_query(query_str.to_string())
                .map_err(NodeError::CQLError)?;


            let query_id;
            {
                let mut guard_node = node.lock()?;
                        let all_nodes = guard_node.get_how_many_nodes_i_know();
                        let needed_responses;
                        match  query.needed_responses(){
                            query_coordinator::NeededResponseCount::AllNodes => {
                                needed_responses = all_nodes
                            }
                            query_coordinator::NeededResponseCount::Specific(specific_value) => {
                                needed_responses = specific_value as usize
                            }
                        };
                        query_id = guard_node.add_open_query(needed_responses as i32);
            }

                let result = QueryExecution::new(node.clone(), connections.clone()).execute(query.clone(), false, query_id)?;
                

                // // Maneja el resultado
                // match result {
                //     Ok(message) => {
                        
                //         println!("{}", message); // Imprime el mensaje si es exitoso
                //     }
                //     Err(e) => {
                //         let mut guard_node = node.lock()?;
                //         guard_node.remove_open_query(query_id);
                //         println!("{:?} in {:?}", e, query_str)}, // Retorna el error si ocurrió uno
                // }
        }
        Ok(())
    }

}
