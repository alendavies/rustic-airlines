use crate::open_query_handler::OpenQueryHandler;
use crate::utils::{connect, send_message};
use crate::{Node, NodeError, Query, QueryExecution, INTERNODE_PORT};
use native_protocol::frame::Frame;
use native_protocol::messages::error;
use native_protocol::Serializable;
use query_creator::clauses::keyspace::{
    alter_keyspace_cql::AlterKeyspace, create_keyspace_cql::CreateKeyspace,
    drop_keyspace_cql::DropKeyspace,
};
use query_creator::clauses::table::{
    alter_table_cql::AlterTable, create_table_cql::CreateTable, drop_table_cql::DropTable,
};
use query_creator::clauses::types::column::Column;
use query_creator::clauses::use_cql::Use;
use query_creator::clauses::{
    delete_cql::Delete, insert_cql::Insert, select_cql::Select, update_cql::Update,
};
use query_creator::CreateClientResponse;
use std::collections::HashMap;
use std::io::Write;
use std::net::{Ipv4Addr, TcpStream};
use std::sync::{Arc, Mutex};

/// Struct that represents the handler for internode communication protocol.
pub struct InternodeProtocolHandler {}

impl InternodeProtocolHandler {
    /// Creates a new `InternodeProtocolHandler`.
    pub fn new() -> Self {
        InternodeProtocolHandler {}
    }

    /// Creates a protocol message for querying between nodes.
    ///
    /// # Parameters
    /// - `id`: The ID of the node.
    /// - `open_query_id`: The ID of the open query.
    /// - `query_type`: The type of the query being executed.
    /// - `structure`: The structure of the query in string format.
    /// - `internode`: A boolean indicating whether the query is between nodes.
    ///
    /// # Returns
    /// A formatted string representing the query message.
    pub fn create_protocol_message(
        id: &str,
        open_query_id: i32,
        query_type: &str,
        structure: &str,
        internode: bool,
        replication: bool,
    ) -> String {
        format!(
            "QUERY - {} - {} - {} - {} - {} - {}",
            id, open_query_id, query_type, structure, internode, replication
        )
    }

    /// Creates a protocol response to a query.
    ///
    /// # Parameters
    /// - `status`: The status of the response (e.g., "OK").
    /// - `content`: The content of the response.
    /// - `open_query_id`: The ID of the open query related to the response.
    ///
    /// # Returns
    /// A formatted string representing the response message.
    pub fn create_protocol_response(status: &str, content: &str, open_query_id: i32) -> String {
        format!("RESPONSE - {} - {} - {}", open_query_id, status, content)
    }

    /// Handles an incoming command from a node or client.
    ///
    /// # Parameters
    /// - `node`: The node receiving the command.
    /// - `message`: The message received.
    /// - `_stream`: The stream used for communication.
    /// - `connections`: A collection of current TCP connections.
    /// - `is_seed`: A boolean indicating if the current node is a seed.
    ///
    /// # Returns
    /// A result that either indicates success or returns a `NodeError`.
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
            return Err(NodeError::InternodeProtocolError);
        }

        match parts[0] {
            "QUERY" => {
                self.handle_query_command(node, parts[1], connections, is_seed)?;
                Ok(())
            }
            "RESPONSE" => {
                self.handle_response_command(node, parts[1])?;
                Ok(())
            }
            _ => Err(NodeError::InternodeProtocolError),
        }
    }

    /// Handles a query command received from another node.
    fn handle_query_command(
        &self,
        node: &Arc<Mutex<Node>>,
        message: &str,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        is_seed: bool,
    ) -> Result<(), NodeError> {
        let parts: Vec<&str> = message.splitn(6, " - ").collect();

        if parts.len() < 6 {
            return Err(NodeError::InternodeProtocolError);
        }

        let nodo_id = parts[0];
        let open_query_id: i32 = parts[1]
            .parse()
            .map_err(|_| NodeError::InternodeProtocolError)?;
        let query_type = parts[2];
        let structure = parts[3];
        let internode = parts[4] == "true";
        let replication = parts[5] == "true";

        let result: Result<Option<(i32, String)>, NodeError> = match query_type {
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
                replication,
                open_query_id,
            ),
            "UPDATE" => Self::handle_update_command(
                node,
                structure,
                connections.clone(),
                internode,
                replication,
                open_query_id,
            ),
            "DELETE" => Self::handle_delete_command(
                node,
                structure,
                connections.clone(),
                internode,
                replication,
                open_query_id,
            ),
            "SELECT" => Self::handle_select_command(
                node,
                structure,
                connections.clone(),
                internode,
                replication,
                open_query_id,
            ),
            "USE" => Self::handle_use_command(
                node,
                structure,
                connections.clone(),
                internode,
                open_query_id,
            ),
            _ => Err(NodeError::InternodeProtocolError),
        };

        let response: Option<(i32, String)> = result?;
        if let Some(responses) = response {
            let (_, value): (i32, String) = responses;
            let peer_id: Ipv4Addr = nodo_id
                .parse()
                .map_err(|_| NodeError::InternodeProtocolError)?;
            let stream: Arc<Mutex<TcpStream>> = connect(peer_id, INTERNODE_PORT, connections)?;
            send_message(&stream, &value)?;
        }
        Ok(())
    }

    /// Handles a response command from another node.
    fn handle_response_command(
        &self,
        node: &Arc<Mutex<Node>>,
        message: &str,
    ) -> Result<(), NodeError> {
        let mut guard_node = node.lock()?;
        let keyspace_name = guard_node
            .actual_keyspace_name()
            .ok_or(NodeError::KeyspaceError)?
            .clone();

        let query_handler = guard_node.get_open_handle_query();

        let parts: Vec<&str> = message.splitn(3, " - ").collect();
        if parts.len() < 3 {
            return Err(NodeError::InternodeProtocolError);
        }

        let open_query_id: i32 = parts[0]
            .parse()
            .map_err(|_| NodeError::InternodeProtocolError)?;
        let status = parts[1];
        let content = parts[2];

        match status {
            "OK" => {
                self.process_ok_response(query_handler, content, open_query_id, keyspace_name)?;
            }
            "ERROR" => {
                // Aquí puedes agregar la lógica para manejar el caso "ERROR".
                // Por ejemplo, puedes retornar un error específico o realizar otra acción.
                self.process_error_response(query_handler, open_query_id)?;
            }
            _ => {
                // En caso de que el estado no sea ni "OK" ni "ERROR", podemos manejarlo
                // como un error de protocolo o una situación inesperada.
                return Err(NodeError::InternodeProtocolError);
            }
        }

        Ok(())
    }

    /// Procesa la respuesta cuando el estado es "OK"
    fn process_ok_response(
        &self,
        query_handler: &mut OpenQueryHandler,
        content: &str,
        open_query_id: i32,
        keyspace_name: String,
    ) -> Result<(), NodeError> {
        let open_query = query_handler
            .get_query_mut(&open_query_id)
            .ok_or(NodeError::OtherError)?;

        let columns = {
            if let Some(table) = open_query.get_table() {
                table.get_columns()
            } else {
                vec![]
            }
        };

        Self::add_response_to_open_query_and_send_response_if_closed(
            query_handler,
            content,
            open_query_id,
            keyspace_name,
            columns,
        )?;

        Ok(())
    }

    /// Procesa la respuesta cuando el estado es "ERROR"
    fn process_error_response(
        &self,
        query_handler: &mut OpenQueryHandler,
        open_query_id: i32,
    ) -> Result<(), NodeError> {
        Self::close_query_and_send_error_frame(query_handler, open_query_id)
    }

    pub fn add_response_to_open_query_and_send_response_if_closed(
        query_handler: &mut OpenQueryHandler,
        content: &str,
        open_query_id: i32,
        keyspace_name: String,
        columns: Vec<Column>,
    ) -> Result<(), NodeError> {
        if let Some(open_query) =
            query_handler.add_response_and_get_if_closed(open_query_id, content.to_string())
        {
            let mut connection = open_query.get_connection();

            let keyspace_name = keyspace_name;

            let frame = open_query.get_query().create_client_response(
                columns,
                keyspace_name,
                content.split("/").map(|s| s.to_string()).collect(),
            )?;
            println!("ya termino la query, voy a mandar el frame {:?}", frame);
            connection.write(&frame.to_bytes())?;
            Ok(())
        } else {
            Ok(())
        }
    }

    pub fn close_query_and_send_error_frame(
        query_handler: &mut OpenQueryHandler,
        open_query_id: i32,
    ) -> Result<(), NodeError> {
        // Verificamos si la consulta está cerrada y necesitamos enviar un frame de error.
        if let Some(open_query) = query_handler.close_query_and_get_if_closed(open_query_id) {
            let mut connection = open_query.get_connection();

            // Crear un frame de error para el cliente.
            let error_frame = Frame::Error(error::Error::ServerError("".to_string()));

            connection.write(&error_frame.to_bytes())?;
            Ok(())
        } else {
            Ok(())
        }
    }

    /// Handles the introduction command, which is used for the "HANDSHAKE" protocol.
    fn handle_introduction_command(
        node: &Arc<Mutex<Node>>,
        nodo_id: &str,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        is_seed: bool,
    ) -> Result<Option<(i32, String)>, NodeError> {
        let mut lock_node = node.lock().map_err(|_| NodeError::LockError)?;
        let self_ip = lock_node.get_ip();

        let new_ip: Ipv4Addr = nodo_id
            .parse()
            .map_err(|_| NodeError::InternodeProtocolError)?;

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

    /// Handles an `INSERT` command.
    fn handle_insert_command(
        node: &Arc<Mutex<Node>>,
        structure: &str,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        internode: bool,
        replication: bool,
        open_query_id: i32,
    ) -> Result<Option<(i32, String)>, NodeError> {
        let query = Insert::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections).execute(
            Query::Insert(query),
            internode,
            replication,
            open_query_id,
        )
    }

    /// Handles a `CREATE_TABLE` command.
    fn handle_create_table_command(
        node: &Arc<Mutex<Node>>,
        structure: &str,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        internode: bool,
        open_query_id: i32,
    ) -> Result<Option<(i32, String)>, NodeError> {
        let query = CreateTable::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections).execute(
            Query::CreateTable(query),
            internode,
            false,
            open_query_id,
        )
    }

    /// Handles a `DROP_TABLE` command.
    fn handle_drop_table_command(
        node: &Arc<Mutex<Node>>,
        structure: &str,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        internode: bool,
        open_query_id: i32,
    ) -> Result<Option<(i32, String)>, NodeError> {
        let query = DropTable::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections).execute(
            Query::DropTable(query),
            internode,
            false,
            open_query_id,
        )
    }

    /// Handles an `ALTER_TABLE` command.
    fn handle_alter_table_command(
        node: &Arc<Mutex<Node>>,
        structure: &str,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        internode: bool,
        open_query_id: i32,
    ) -> Result<Option<(i32, String)>, NodeError> {
        let query = AlterTable::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections).execute(
            Query::AlterTable(query),
            internode,
            false,
            open_query_id,
        )
    }

    /// Handles a `CREATE_KEYSPACE` command.
    fn handle_create_keyspace_command(
        node: &Arc<Mutex<Node>>,
        structure: &str,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        internode: bool,
        open_query_id: i32,
    ) -> Result<Option<(i32, String)>, NodeError> {
        let query = CreateKeyspace::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections).execute(
            Query::CreateKeyspace(query),
            internode,
            false,
            open_query_id,
        )
    }

    /// Handles a `DROP_KEYSPACE` command.
    fn handle_drop_keyspace_command(
        node: &Arc<Mutex<Node>>,
        structure: &str,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        internode: bool,
        open_query_id: i32,
    ) -> Result<Option<(i32, String)>, NodeError> {
        let query = DropKeyspace::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections).execute(
            Query::DropKeyspace(query),
            internode,
            false,
            open_query_id,
        )
    }

    /// Handles an `ALTER_KEYSPACE` command.
    fn handle_alter_keyspace_command(
        node: &Arc<Mutex<Node>>,
        structure: &str,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        internode: bool,
        open_query_id: i32,
    ) -> Result<Option<(i32, String)>, NodeError> {
        let query = AlterKeyspace::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections).execute(
            Query::AlterKeyspace(query),
            internode,
            false,
            open_query_id,
        )
    }

    /// Handles an `UPDATE` command.
    fn handle_update_command(
        node: &Arc<Mutex<Node>>,
        structure: &str,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        internode: bool,
        replication: bool,
        open_query_id: i32,
    ) -> Result<Option<(i32, String)>, NodeError> {
        let query = Update::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections).execute(
            Query::Update(query),
            internode,
            replication,
            open_query_id,
        )
    }

    /// Handles a `DELETE` command.
    fn handle_delete_command(
        node: &Arc<Mutex<Node>>,
        structure: &str,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        internode: bool,
        replication: bool,
        open_query_id: i32,
    ) -> Result<Option<(i32, String)>, NodeError> {
        let query = Delete::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections).execute(
            Query::Delete(query),
            internode,
            replication,
            open_query_id,
        )
    }

    /// Handles a `SELECT` command.
    fn handle_select_command(
        node: &Arc<Mutex<Node>>,
        structure: &str,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        internode: bool,
        replication: bool,
        open_query_id: i32,
    ) -> Result<Option<(i32, String)>, NodeError> {
        let query = Select::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections).execute(
            Query::Select(query),
            internode,
            replication,
            open_query_id,
        )
    }

    /// Handles an `INSERT` command.
    fn handle_use_command(
        node: &Arc<Mutex<Node>>,
        structure: &str,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        internode: bool,
        open_query_id: i32,
    ) -> Result<Option<(i32, String)>, NodeError> {
        let query = Use::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections).execute(
            Query::Use(query),
            internode,
            false,
            open_query_id,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Node, NodeError};
    use std::collections::HashMap;
    use std::net::{TcpListener, TcpStream};
    use std::sync::{Arc, Mutex};

    // Función auxiliar para crear un nodo para pruebas
    fn create_test_node() -> Arc<Mutex<Node>> {
        let ip = std::net::Ipv4Addr::new(127, 0, 0, 1);
        let seeds_nodes = vec![std::net::Ipv4Addr::new(127, 0, 0, 2)];
        let node = Node::new(ip, seeds_nodes).expect("Error creating node for test");
        Arc::new(Mutex::new(node))
    }

    #[test]
    fn test_create_protocol_message() {
        let message = InternodeProtocolHandler::create_protocol_message(
            "127.0.0.1",
            1,
            "CREATE_TABLE",
            "table_structure",
            true,
            true,
        );
        assert_eq!(
            message,
            "QUERY - 127.0.0.1 - 1 - CREATE_TABLE - table_structure - true - true"
        );
    }

    #[test]
    fn test_create_protocol_response() {
        let response = InternodeProtocolHandler::create_protocol_response("OK", "content", 1);
        assert_eq!(response, "RESPONSE - 1 - OK - content");
    }

    #[test]
    fn test_handle_invalid_command() {
        // Crear un listener para aceptar conexiones
        let _listener = TcpListener::bind("127.0.0.1:8080").unwrap();

        let node = create_test_node();
        let handler = InternodeProtocolHandler::new();
        let connections = Arc::new(Mutex::new(HashMap::new()));

        // Crear un TcpStream real conectándose al listener
        let client_stream = TcpStream::connect("127.0.0.1:8080").unwrap();

        let result = handler.handle_command(
            &node,
            "INVALID - command",
            &mut Arc::new(Mutex::new(client_stream)),
            connections,
            false,
        );
        assert!(matches!(result, Err(NodeError::InternodeProtocolError)));
    }

    #[test]
    fn test_handle_valid_command() {
        // Crear un listener para aceptar conexiones
        let _listener = TcpListener::bind("127.0.0.4:8080").unwrap();

        let node = create_test_node();
        let handler = InternodeProtocolHandler::new();
        let connections = Arc::new(Mutex::new(HashMap::new()));

        // Crear un TcpStream real conectándose al listener
        let client_stream = TcpStream::connect("127.0.0.4:8080").unwrap();

        let result = handler.handle_command(
            &node,
            "QUERY - 127.0.0.1 - 1 - HANDSHAKE - structure - true - true",
            &mut Arc::new(Mutex::new(client_stream)),
            connections,
            true,
        );
        assert!(result.is_ok());
    }
}
