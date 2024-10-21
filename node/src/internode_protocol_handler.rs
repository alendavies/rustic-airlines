use crate::utils::{connect, send_message};
use crate::{Node, NodeError, Query, QueryExecution, INTERNODE_PORT};
use native_protocol::frame::Frame;
use native_protocol::messages::result::result;
use native_protocol::Serializable;
use query_coordinator::clauses::keyspace::{
    alter_keyspace_cql::AlterKeyspace, create_keyspace_cql::CreateKeyspace,
    drop_keyspace_cql::DropKeyspace,
};
use query_coordinator::clauses::table::{
    alter_table_cql::AlterTable, create_table_cql::CreateTable, drop_table_cql::DropTable,
};
use query_coordinator::clauses::{
    delete_sql::Delete, insert_sql::Insert, select_sql::Select, update_sql::Update,
};
use std::collections::btree_map::Values;
use std::collections::HashMap;
use std::io::Write;
use std::net::{Ipv4Addr, TcpStream};
use std::sync::{Arc, Mutex};

pub struct InternodeProtocolHandler {}

impl InternodeProtocolHandler {
    pub fn new() -> Self {
        InternodeProtocolHandler {}
    }

    pub fn create_protocol_message(
        id: &str,
        open_query_id: i32,
        query_type: &str,
        structure: &str,
        internode: bool,
    ) -> String {
        format!(
            "QUERY - {} - {} - {} - {} - {}",
            id, open_query_id, query_type, structure, internode
        )
    }

    pub fn create_protocol_response(status: &str, content: &str, open_query_id: i32) -> String {
        format!("RESPONSE - {} - {} - {}", open_query_id, status, content)
    }

    pub fn handle_command(
        &self,
        node: &Arc<Mutex<Node>>,
        message: &str,
        _stream: &mut Arc<Mutex<TcpStream>>,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        is_seed: bool,
    ) -> Result<(), NodeError> {
        let cleaned_message = message.trim_end();
        let parts: Vec<&str> = cleaned_message.splitn(2, " - ").collect();

        if parts.len() < 2 {
            return Err(NodeError::OtherError);
        }

        match parts[0] {
            "QUERY" => {
                self.handle_query_command(node, parts[1], connections, is_seed)?;
                Ok(())
            }
            "RESPONSE" => self.handle_response_command(node, parts[1]),
            _ => Err(NodeError::OtherError),
        }
    }

    fn handle_query_command(
        &self,
        node: &Arc<Mutex<Node>>,
        message: &str,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        is_seed: bool,
    ) -> Result<(), NodeError> {
        let parts: Vec<&str> = message.splitn(5, " - ").collect();

        if parts.len() < 5 {
            return Err(NodeError::OtherError);
        }

        let nodo_id = parts[0];
        let open_query_id: i32 = parts[1].parse().map_err(|_| NodeError::OtherError)?;
        let query_type = parts[2];
        let structure = parts[3];
        let internode = parts[4] == "true";

        let result = match query_type {
            "HANDSHAKE" => {
                Self::handle_introduction_command(node, nodo_id, connections.clone(), is_seed)
            }
            "CREATE_TABLE" => Self::handle_create_table_command(
                node,
                structure,
                connections.clone(),
                internode,
                open_query_id,
            ),
            "DROP_TABLE" => Self::handle_drop_table_command(
                node,
                structure,
                connections.clone(),
                internode,
                open_query_id,
            ),
            "ALTER_TABLE" => Self::handle_alter_table_command(
                node,
                structure,
                connections.clone(),
                internode,
                open_query_id,
            ),
            "CREATE_KEYSPACE" => Self::handle_create_keyspace_command(
                node,
                structure,
                connections.clone(),
                internode,
                open_query_id,
            ),
            "DROP_KEYSPACE" => Self::handle_drop_keyspace_command(
                node,
                structure,
                connections.clone(),
                internode,
                open_query_id,
            ),
            "ALTER_KEYSPACE" => Self::handle_alter_keyspace_command(
                node,
                structure,
                connections.clone(),
                internode,
                open_query_id,
            ),
            "INSERT" => Self::handle_insert_command(
                node,
                structure,
                connections.clone(),
                internode,
                open_query_id,
            ),
            "UPDATE" => Self::handle_update_command(
                node,
                structure,
                connections.clone(),
                internode,
                open_query_id,
            ),
            "DELETE" => Self::handle_delete_command(
                node,
                structure,
                connections.clone(),
                internode,
                open_query_id,
            ),
            "SELECT" => Self::handle_select_command(
                node,
                structure,
                connections.clone(),
                internode,
                open_query_id,
            ),
            _ => Err(NodeError::OtherError),
        };

        let response = result?;
        if let Some(value) = response {
            let peer_id: Ipv4Addr = nodo_id.parse().map_err(|_| NodeError::OtherError)?;
            let stream = connect(peer_id, INTERNODE_PORT, connections)?;
            send_message(&stream, &value)?;
        }
        Ok(())
    }

    fn handle_response_command(
        &self,
        node: &Arc<Mutex<Node>>,
        message: &str,
    ) -> Result<(), NodeError> {
        let mut guard_node = node.lock()?;
        let query_handler = guard_node.get_open_hanlde_query();
        let parts: Vec<&str> = message.splitn(3, " - ").collect();

        if parts.len() < 3 {
            return Err(NodeError::OtherError);
        }

        let open_query_id: i32 = parts[0].parse().map_err(|_| NodeError::OtherError)?;
        let status = parts[1];
        let content = parts[2];

        if status != "OK" {
            return Err(NodeError::OtherError);
        }

        // let (query_close, responses) =
        if let Some(open_query) =
            query_handler.add_response_and_get_if_closed(open_query_id, content.to_string())
        {
            println!(
                "cerro la query y el contendio es {:?}",
                content.replace("/", "\n")
            );
            // let mut connection = query_handler.get_connection_mut(open_query_id)?;
            let mut connection = open_query.get_connection();
            let frame = Frame::Result(result::Result::SetKeyspace("OK".to_string())).to_bytes();
            connection.write(&frame)?;
        }

        // Implementar lógica para manejar las respuestas según sea necesario
        Ok(())
    }

    fn handle_introduction_command(
        node: &Arc<Mutex<Node>>,
        nodo_id: &str,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        is_seed: bool,
    ) -> Result<Option<String>, NodeError> {
        let mut lock_node = node.lock().map_err(|_| NodeError::LockError)?;
        let self_ip = lock_node.get_ip();

        let new_ip: Ipv4Addr = nodo_id.parse().map_err(|_| NodeError::OtherError)?;

        if self_ip != new_ip && !lock_node.partitioner.contains_node(&new_ip) {
            lock_node.partitioner.add_node(new_ip)?;
        }

        if is_seed {
            for ip in lock_node.get_partitioner().get_nodes() {
                if new_ip != ip && self_ip != ip && is_seed {
                    lock_node.forward_message(connections.clone(), ip, new_ip)?;
                    lock_node.forward_message(connections.clone(), new_ip, ip)?;
                }
            }
        }
        Ok(None)
    }

    fn handle_insert_command(
        node: &Arc<Mutex<Node>>,
        structure: &str,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        internode: bool,
        open_query_id: i32,
    ) -> Result<Option<String>, NodeError> {
        let query = Insert::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections).execute(
            Query::Insert(query),
            internode,
            open_query_id,
        )
    }

    fn handle_create_table_command(
        node: &Arc<Mutex<Node>>,
        structure: &str,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        internode: bool,
        open_query_id: i32,
    ) -> Result<Option<String>, NodeError> {
        let query = CreateTable::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections).execute(
            Query::CreateTable(query),
            internode,
            open_query_id,
        )
    }

    fn handle_drop_table_command(
        node: &Arc<Mutex<Node>>,
        structure: &str,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        internode: bool,
        open_query_id: i32,
    ) -> Result<Option<String>, NodeError> {
        let query = DropTable::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections).execute(
            Query::DropTable(query),
            internode,
            open_query_id,
        )
    }

    fn handle_alter_table_command(
        node: &Arc<Mutex<Node>>,
        structure: &str,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        internode: bool,
        open_query_id: i32,
    ) -> Result<Option<String>, NodeError> {
        let query = AlterTable::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections).execute(
            Query::AlterTable(query),
            internode,
            open_query_id,
        )
    }

    fn handle_create_keyspace_command(
        node: &Arc<Mutex<Node>>,
        structure: &str,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        internode: bool,
        open_query_id: i32,
    ) -> Result<Option<String>, NodeError> {
        let query = CreateKeyspace::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections).execute(
            Query::CreateKeyspace(query),
            internode,
            open_query_id,
        )
    }

    fn handle_drop_keyspace_command(
        node: &Arc<Mutex<Node>>,
        structure: &str,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        internode: bool,
        open_query_id: i32,
    ) -> Result<Option<String>, NodeError> {
        let query = DropKeyspace::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections).execute(
            Query::DropKeyspace(query),
            internode,
            open_query_id,
        )
    }

    fn handle_alter_keyspace_command(
        node: &Arc<Mutex<Node>>,
        structure: &str,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        internode: bool,
        open_query_id: i32,
    ) -> Result<Option<String>, NodeError> {
        let query = AlterKeyspace::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections).execute(
            Query::AlterKeyspace(query),
            internode,
            open_query_id,
        )
    }

    fn handle_update_command(
        node: &Arc<Mutex<Node>>,
        structure: &str,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        internode: bool,
        open_query_id: i32,
    ) -> Result<Option<String>, NodeError> {
        let query = Update::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections).execute(
            Query::Update(query),
            internode,
            open_query_id,
        )
    }

    fn handle_delete_command(
        node: &Arc<Mutex<Node>>,
        structure: &str,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        internode: bool,
        open_query_id: i32,
    ) -> Result<Option<String>, NodeError> {
        let query = Delete::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections).execute(
            Query::Delete(query),
            internode,
            open_query_id,
        )
    }

    fn handle_select_command(
        node: &Arc<Mutex<Node>>,
        structure: &str,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        internode: bool,
        open_query_id: i32,
    ) -> Result<Option<String>, NodeError> {
        let query = Select::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections).execute(
            Query::Select(query),
            internode,
            open_query_id,
        )
    }
}
