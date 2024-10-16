// internode_protocol_handler.rs
use std::sync::{Arc, Mutex};
use std::net::{ Ipv4Addr, TcpStream};
use crate::{Node, NodeError, Query, QueryExecution};
use query_coordinator::clauses::{insert_sql::Insert, delete_sql::Delete, select_sql::Select, update_sql::Update};
use query_coordinator::clauses::table::{create_table_cql::CreateTable, drop_table_cql::DropTable, alter_table_cql::AlterTable};
use query_coordinator::clauses::keyspace::{create_keyspace_cql::CreateKeyspace, drop_keyspace_cql::DropKeyspace, alter_keyspace_cql::AlterKeyspace};
use std::collections::HashMap;




pub struct InternodeProtocolHandler {
    node: Arc<Mutex<Node>>, // Almacenamiento de conexiones activas
}

impl InternodeProtocolHandler {
    pub fn new(node: Arc<Mutex<Node>>) -> Self {
        InternodeProtocolHandler {
            node,
        }
    }

    pub fn create_protocol_message(id: &str, query_type: &str, structure: &str, internode: bool) -> String {
        format!("{} - {} - {} - {}", id, query_type, structure, internode)
    }
    

    pub fn create_protocol_response(status: &str, content: &str) -> String {
        format!("{} - {}", status, content)
    }
    

    pub fn handle_command(
        &self,
        message: &str,
        connections: Arc<Mutex<HashMap<String,  Arc<Mutex<TcpStream>>>>>,
        is_seed: bool,
    ) -> Result<(), NodeError> {

        // Split message using the protocol format NODO_ID - QUERY - STRUCTURE - INTERNODE
        let cleaned_message = message.trim_end();
        let parts: Vec<&str> = cleaned_message.splitn(4, " - ").collect();

    
        if parts.len() < 4 {
            return Err(NodeError::OtherError);
        }
        
        let nodo_id = parts[0];
        let query_type = parts[1];
        let structure = parts[2];
        let internode = parts[3] == "true";

        //println!("recibi de {:?} un mensaje tipo {:?}", nodo_id, query_type);
        
        // {
        //     let mut connections_guard = connections.lock()?;
        //     connections_guard.insert(nodo_id.to_string(), stream.try_clone()?);
        // }

        let result = match query_type {
            "HANDSHAKE" => Self::handle_introduction_command(&self.node, nodo_id, connections.clone(), is_seed),
            "CREATE_TABLE" => Self::handle_create_table_command(&self.node, structure, connections.clone(), internode),
            "DROP_TABLE" => Self::handle_drop_table_command(&self.node, structure, connections.clone(), internode),
            "ALTER_TABLE" => Self::handle_alter_table_command(&self.node, structure, connections.clone(), internode),
            "CREATE_KEYSPACE" => Self::handle_create_keyspace_command(&self.node, structure, connections.clone(), internode),
            "DROP_KEYSPACE" => Self::handle_drop_keyspace_command(&self.node, structure, connections.clone(), internode),
            "ALTER_KEYSPACE" => Self::handle_alter_keyspace_command(&self.node, structure, connections.clone(), internode),
            "INSERT" => Self::handle_insert_command(&self.node, structure, connections.clone(), internode),
            "UPDATE" => Self::handle_update_command(&self.node, structure, connections.clone(), internode),
            "DELETE" => Self::handle_delete_command(&self.node, structure, connections.clone(), internode),
            "SELECT" => Self::handle_select_command(&self.node, structure, connections.clone(), internode),
            _ => Err(NodeError::OtherError),
        };
        
        println!("{:?}", result);

        // match result {
        //     Ok(response) => {
        //         send_message(stream, &response)?;  // Utiliza el `stream` original para enviar la respuesta
        //     }
        //     Err(e) => {
        //         let error_message = format!("Error: {:?}", e);
        //         send_message(stream, &error_message)?;  // Envía el mensaje de error
        //     }
        // }
    
        Ok(())
    }
    
    
    fn handle_introduction_command(
        node: &Arc<Mutex<Node>>,
        nodo_id: &str,
        connections: Arc<Mutex<HashMap<String,  Arc<Mutex<TcpStream>>>>>,
        is_seed: bool,
    ) -> Result<String, NodeError> {
        let mut lock_node = node.lock().map_err(|_| NodeError::LockError)?;
        let self_ip = lock_node.get_ip();
    
        // Convierte nodo_id a SocketAddrV4
        let new_ip: Ipv4Addr = nodo_id.parse().map_err(|_| NodeError::OtherError)?;
    
        if self_ip != new_ip && !lock_node.partitioner.contains_node(&new_ip) {
            lock_node.partitioner.add_node(new_ip)?;
        }
    
        if is_seed {
            for socket in lock_node.get_partitioner().get_nodes() {
                if new_ip != socket && self_ip != socket {
                    //println!("Voy a hacer que se conozcan {:?} con {:?}", new_socket.to_string(), socket.to_string());
                    lock_node.forward_message(connections.clone(), socket,new_ip)?;
                    lock_node.forward_message(connections.clone(), new_ip,socket)?;
                }
            }
        }
        Ok(Self::create_protocol_response("OK", ""))
    }
    

    fn handle_insert_command(
        node: &Arc<Mutex<Node>>,
        structure: &str,
        connections: Arc<Mutex<HashMap<String,  Arc<Mutex<TcpStream>>>>>,
        internode: bool
    ) -> Result<String, NodeError> {
        let query = Insert::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections).execute(Query::Insert(query), internode)
    }

    fn handle_create_table_command(
        node: &Arc<Mutex<Node>>,
        structure: &str,
        connections: Arc<Mutex<HashMap<String,  Arc<Mutex<TcpStream>>>>>,
        internode: bool,
    ) -> Result<String, NodeError> {
        let query = CreateTable::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections).execute(Query::CreateTable(query), internode)
    }

    fn handle_drop_table_command(
        node: &Arc<Mutex<Node>>,
        structure: &str,
        connections: Arc<Mutex<HashMap<String,  Arc<Mutex<TcpStream>>>>>,
        internode: bool,
    ) -> Result<String, NodeError> {
        let query = DropTable::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections).execute(Query::DropTable(query), internode)
    }

    fn handle_alter_table_command(
        node: &Arc<Mutex<Node>>,
        structure: &str,
        connections: Arc<Mutex<HashMap<String,  Arc<Mutex<TcpStream>>>>>,
        internode: bool,
    ) -> Result<String, NodeError> {
        let query = AlterTable::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections).execute(Query::AlterTable(query), internode)
    }

    fn handle_create_keyspace_command(
        node: &Arc<Mutex<Node>>,
        structure: &str,
        connections: Arc<Mutex<HashMap<String,  Arc<Mutex<TcpStream>>>>>,
        internode: bool,
    ) -> Result<String, NodeError> {
        let query = CreateKeyspace::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections).execute(Query::CreateKeyspace(query), internode)
    }

    fn handle_drop_keyspace_command(
        node: &Arc<Mutex<Node>>,
        structure: &str,
        connections: Arc<Mutex<HashMap<String,  Arc<Mutex<TcpStream>>>>>,
        internode: bool,
    ) -> Result<String, NodeError> {
        let query = DropKeyspace::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections).execute(Query::DropKeyspace(query), internode)
    }

    fn handle_alter_keyspace_command(
        node: &Arc<Mutex<Node>>,
        structure: &str,
        connections: Arc<Mutex<HashMap<String,  Arc<Mutex<TcpStream>>>>>,
        internode: bool,
    ) -> Result<String, NodeError> {
        let query = AlterKeyspace::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections).execute(Query::AlterKeyspace(query), internode)
    }

    fn handle_update_command(
        node: &Arc<Mutex<Node>>,
        structure: &str,
        connections: Arc<Mutex<HashMap<String,  Arc<Mutex<TcpStream>>>>>,
        internode: bool,
    ) -> Result<String, NodeError> {
        let query = Update::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections).execute(Query::Update(query), internode)
    }

    fn handle_delete_command(
        node: &Arc<Mutex<Node>>,
        structure: &str,
        connections: Arc<Mutex<HashMap<String,  Arc<Mutex<TcpStream>>>>>,
        internode: bool,
    ) -> Result<String, NodeError> {
        let query = Delete::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections).execute(Query::Delete(query), internode)
    }

    fn handle_select_command(
        node: &Arc<Mutex<Node>>,
        structure: &str,
        connections: Arc<Mutex<HashMap<String,  Arc<Mutex<TcpStream>>>>>,
        internode: bool,
    ) -> Result<String, NodeError> {
        let query = Select::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections).execute(Query::Select(query), internode)
    }
}
