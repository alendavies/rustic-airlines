use crate::open_query_handler::OpenQueryHandler;
use crate::utils::connect_and_send_message;
use crate::{Node, NodeError, Query, QueryExecution, INTERNODE_PORT};
use gossip::messages::GossipMessage;
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
/// Struct that represents the handler for internode communication protocol.
pub struct InternodeProtocolHandler {}

impl InternodeProtocolHandler {
    /// Creates a new `InternodeProtocolHandler` for handling internode commands
    /// and responses between nodes in a distributed setting.
    pub fn new() -> Self {
        InternodeProtocolHandler {}
    }

    /// Creates a formatted protocol message for sending a query between nodes.
    ///
    /// # Parameters
    /// - `id`: The identifier of the node sending the message.
    /// - `open_query_id`: The unique ID of the open query, allowing tracking across nodes.
    /// - `query_type`: The type of query being executed (e.g., "SELECT", "INSERT").
    /// - `structure`: The serialized string structure of the query.
    /// - `internode`: Boolean flag indicating if the query originates from another node.
    /// - `replication`: Boolean flag indicating if replication is required.
    ///
    /// # Returns
    /// * `String` - A formatted string representing the protocol message, including
    ///   node ID, query ID, query type, serialized query structure, internode status,
    ///   and replication status.
    pub fn create_protocol_message(
        id: &str,
        open_query_id: i32,
        query_type: &str,
        structure: &str,
        internode: bool,
        replication: bool,
        client_id: i32,
        keyspace_name: &str,
    ) -> String {
        format!(
            "QUERY - {} - {} - {} - {} - {} - {} - {} - {}",
            id,
            open_query_id,
            query_type,
            structure,
            internode,
            replication,
            client_id,
            keyspace_name
        )
    }

    /// Creates a response message for a query, used for communication between nodes.
    ///
    /// # Parameters
    /// - `status`: The status of the response (e.g., "OK" or "ERROR").
    /// - `content`: The content of the response message.
    /// - `open_query_id`: The ID of the open query related to this response.
    ///
    /// # Returns
    /// * `String` - A formatted string representing the response message, including
    ///   the query ID, status, and content of the response.
    pub fn create_protocol_response(status: &str, content: &str, open_query_id: i32) -> String {
        format!("RESPONSE - {} - {} - {}", open_query_id, status, content)
    }

    /// Handles an incoming command from a node or client, distinguishing between query commands
    /// and response commands, and delegating to the appropriate handler.
    ///
    /// # Parameters
    /// - `node`: An `Arc<Mutex<Node>>` representing the node receiving the command.
    /// - `message`: The incoming message string to be processed.
    /// - `_stream`: A mutable reference to the TCP stream used for communication.
    /// - `connections`: A thread-safe collection of active TCP connections with other nodes.
    /// - `is_seed`: Boolean flag indicating if the current node is a seed node.
    ///
    /// # Returns
    /// * `Result<(), NodeError>` - Returns `Ok(())` on successful processing of the command,
    ///   or `NodeError` if there is an issue in parsing or handling the command.
    ///
    /// # Errors
    /// This function may return `NodeError::InternodeProtocolError` if:
    /// - The incoming command format is invalid.
    /// - The command type is unrecognized.
    pub fn handle_command(
        &self,
        node: &Arc<Mutex<Node>>,
        message: &str,
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

            "GOSSIP" => {
                self.handle_gossip_command(node, parts[1], connections)?;
                Ok(())
            }
            _ => Err(NodeError::InternodeProtocolError),
        }
    }

    /// Adds a response to an open query and, if all expected responses have been received,
    /// sends a complete response back to the client.
    ///
    /// # Parameters
    /// - `query_handler`: A mutable reference to the `OpenQueryHandler` managing open queries.
    /// - `content`: The response content received from another node.
    /// - `open_query_id`: The ID of the open query being handled.
    /// - `keyspace_name`: The name of the keyspace associated with this query.
    /// - `columns`: The list of columns in the response, if applicable.
    ///
    /// # Returns
    /// * `Result<(), NodeError>` - Returns `Ok(())` on successful handling of the response,
    ///   or `NodeError` if there is an issue in processing the query.
    ///
    /// # Errors
    /// - `NodeError::OtherError` may be returned if the open query cannot be retrieved.
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

            let frame = open_query.get_query().create_client_response(
                columns,
                keyspace_name,
                content.split("/").map(|s| s.to_string()).collect(),
            )?;

            println!("Returning frame to client: {:?}", frame);

            connection.write(&frame.to_bytes()?)?;
            Ok(())
        } else {
            Ok(())
        }
    }

    /// Closes an open query and sends an error response back to the client.
    ///
    /// # Parameters
    /// - `query_handler`: A mutable reference to the `OpenQueryHandler` managing open queries.
    /// - `open_query_id`: The ID of the open query being closed due to an error.
    ///
    /// # Returns
    /// * `Result<(), NodeError>` - Returns `Ok(())` on successful error handling,
    ///   or `NodeError` if there is an issue in processing the query.
    ///
    /// # Errors
    /// - This function returns `NodeError` if there is a failure in sending the error response.
    pub fn close_query_and_send_error_frame(
        query_handler: &mut OpenQueryHandler,
        open_query_id: i32,
    ) -> Result<(), NodeError> {
        if let Some(open_query) = query_handler.close_query_and_get_if_closed(open_query_id) {
            let mut connection = open_query.get_connection();

            let error_frame = Frame::Error(error::Error::ServerError(
                "A node failed to execute the request.".to_string(),
            ));

            connection.write(&error_frame.to_bytes()?)?;
            Ok(())
        } else {
            Ok(())
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
        let parts: Vec<&str> = message.splitn(8, " - ").collect();
        if parts.len() < 7 {
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
        let client_id: i32 = parts[6]
            .parse()
            .map_err(|_| NodeError::InternodeProtocolError)?;
        let keyspace_name = parts[7];

        if keyspace_name != "None" {
            {
                let mut guard_node = node.lock()?;
                let k = guard_node.get_keyspace(keyspace_name)?;
                guard_node
                    .get_open_handle_query()
                    .set_keyspace_of_query(open_query_id, k.ok_or(NodeError::KeyspaceError)?);
            }
        }
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
                client_id,
            ),
            "DROP_TABLE" => Self::handle_drop_table_command(
                node,
                structure,
                connections.clone(),
                internode,
                open_query_id,
                client_id,
            ),
            "ALTER_TABLE" => Self::handle_alter_table_command(
                node,
                structure,
                connections.clone(),
                internode,
                open_query_id,
                client_id,
            ),
            "CREATE_KEYSPACE" => Self::handle_create_keyspace_command(
                node,
                structure,
                connections.clone(),
                internode,
                open_query_id,
                client_id,
            ),
            "DROP_KEYSPACE" => Self::handle_drop_keyspace_command(
                node,
                structure,
                connections.clone(),
                internode,
                open_query_id,
                client_id,
            ),
            "ALTER_KEYSPACE" => Self::handle_alter_keyspace_command(
                node,
                structure,
                connections.clone(),
                internode,
                open_query_id,
                client_id,
            ),
            "INSERT" => Self::handle_insert_command(
                node,
                structure,
                connections.clone(),
                internode,
                replication,
                open_query_id,
                client_id,
            ),
            "UPDATE" => Self::handle_update_command(
                node,
                structure,
                connections.clone(),
                internode,
                replication,
                open_query_id,
                client_id,
            ),
            "DELETE" => Self::handle_delete_command(
                node,
                structure,
                connections.clone(),
                internode,
                replication,
                open_query_id,
                client_id,
            ),
            "SELECT" => Self::handle_select_command(
                node,
                structure,
                connections.clone(),
                internode,
                replication,
                open_query_id,
                client_id,
            ),
            "USE" => Self::handle_use_command(
                node,
                structure,
                connections.clone(),
                internode,
                open_query_id,
                client_id,
            ),
            _ => Err(NodeError::InternodeProtocolError),
        };

        let response: Option<(i32, String)> = result?;
        if let Some(responses) = response {
            let (_, value): (i32, String) = responses;
            let peer_id: Ipv4Addr = nodo_id
                .parse()
                .map_err(|_| NodeError::InternodeProtocolError)?;
            connect_and_send_message(peer_id, INTERNODE_PORT, connections, &value)?;
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

        let keyspace = query_handler.get_keyspace_of_query(open_query_id)?;

        let keyspace_name = if let Some(value) = keyspace {
            value.get_name()
        } else {
            "".to_string()
        };

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

    /// Handles a gossip command from another node.
    fn handle_gossip_command(
        &self,
        node: &Arc<Mutex<Node>>,
        message: &str,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
    ) -> Result<(), NodeError> {
        let mut guard_node = node.lock()?;

        let bytes = message.as_bytes();

        let gossip_message =
            GossipMessage::from_bytes(bytes).map_err(|_| NodeError::GossipError)?;

        match gossip_message.payload {
            gossip::messages::Payload::Syn(syn) => {
                let ack = guard_node.gossiper.handle_syn(syn);
                let msg = GossipMessage {
                    from: guard_node.ip,
                    payload: gossip::messages::Payload::Ack(ack),
                };
                let bytes = msg.as_bytes();

                let message = std::str::from_utf8(bytes.as_slice()).unwrap();
                connect_and_send_message(
                    gossip_message.from,
                    INTERNODE_PORT,
                    connections,
                    format!("GOSSIP - {}", message).as_str(),
                )
                .unwrap();
            }
            gossip::messages::Payload::Ack(ack) => {
                let ack2 = guard_node.gossiper.handle_ack(ack);
                let msg = GossipMessage {
                    from: guard_node.ip,
                    payload: gossip::messages::Payload::Ack2(ack2),
                };
                let bytes = msg.as_bytes();

                let message = std::str::from_utf8(bytes.as_slice()).unwrap();
                connect_and_send_message(
                    gossip_message.from,
                    INTERNODE_PORT,
                    connections,
                    format!("GOSSIP - {}", message).as_str(),
                )
                .unwrap();
            }
            gossip::messages::Payload::Ack2(ack2) => {
                guard_node.gossiper.handle_ack2(ack2);
            }
        };

        // TODO
        // informar al partitioner que un nodo se ha unido

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
        let open_query;

        if let Some(value) = query_handler.get_query_mut(&open_query_id) {
            open_query = value;
        } else {
            // Si es `None`, retorna `Ok(())`.
            return Ok(());
        }

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
        client_id: i32,
    ) -> Result<Option<(i32, String)>, NodeError> {
        let query = Insert::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections).execute(
            Query::Insert(query),
            internode,
            replication,
            open_query_id,
            client_id,
        )
    }

    /// Handles a `CREATE_TABLE` command.
    fn handle_create_table_command(
        node: &Arc<Mutex<Node>>,
        structure: &str,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        internode: bool,
        open_query_id: i32,
        client_id: i32,
    ) -> Result<Option<(i32, String)>, NodeError> {
        let query = CreateTable::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections).execute(
            Query::CreateTable(query),
            internode,
            false,
            open_query_id,
            client_id,
        )
    }

    /// Handles a `DROP_TABLE` command.
    fn handle_drop_table_command(
        node: &Arc<Mutex<Node>>,
        structure: &str,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        internode: bool,
        open_query_id: i32,
        client_id: i32,
    ) -> Result<Option<(i32, String)>, NodeError> {
        let query = DropTable::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections).execute(
            Query::DropTable(query),
            internode,
            false,
            open_query_id,
            client_id,
        )
    }

    /// Handles an `ALTER_TABLE` command.
    fn handle_alter_table_command(
        node: &Arc<Mutex<Node>>,
        structure: &str,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        internode: bool,
        open_query_id: i32,
        client_id: i32,
    ) -> Result<Option<(i32, String)>, NodeError> {
        let query = AlterTable::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections).execute(
            Query::AlterTable(query),
            internode,
            false,
            open_query_id,
            client_id,
        )
    }

    /// Handles a `CREATE_KEYSPACE` command.
    fn handle_create_keyspace_command(
        node: &Arc<Mutex<Node>>,
        structure: &str,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        internode: bool,
        open_query_id: i32,
        client_id: i32,
    ) -> Result<Option<(i32, String)>, NodeError> {
        let query = CreateKeyspace::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections).execute(
            Query::CreateKeyspace(query),
            internode,
            false,
            open_query_id,
            client_id,
        )
    }

    /// Handles a `DROP_KEYSPACE` command.
    fn handle_drop_keyspace_command(
        node: &Arc<Mutex<Node>>,
        structure: &str,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        internode: bool,
        open_query_id: i32,
        client_id: i32,
    ) -> Result<Option<(i32, String)>, NodeError> {
        let query = DropKeyspace::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections).execute(
            Query::DropKeyspace(query),
            internode,
            false,
            open_query_id,
            client_id,
        )
    }

    /// Handles an `ALTER_KEYSPACE` command.
    fn handle_alter_keyspace_command(
        node: &Arc<Mutex<Node>>,
        structure: &str,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        internode: bool,
        open_query_id: i32,
        client_id: i32,
    ) -> Result<Option<(i32, String)>, NodeError> {
        let query = AlterKeyspace::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections).execute(
            Query::AlterKeyspace(query),
            internode,
            false,
            open_query_id,
            client_id,
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
        client_id: i32,
    ) -> Result<Option<(i32, String)>, NodeError> {
        let query = Update::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections).execute(
            Query::Update(query),
            internode,
            replication,
            open_query_id,
            client_id,
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
        client_id: i32,
    ) -> Result<Option<(i32, String)>, NodeError> {
        let query = Delete::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections).execute(
            Query::Delete(query),
            internode,
            replication,
            open_query_id,
            client_id,
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
        client_id: i32,
    ) -> Result<Option<(i32, String)>, NodeError> {
        let query = Select::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections).execute(
            Query::Select(query),
            internode,
            replication,
            open_query_id,
            client_id,
        )
    }

    /// Handles an `INSERT` command.
    fn handle_use_command(
        node: &Arc<Mutex<Node>>,
        structure: &str,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        internode: bool,
        open_query_id: i32,
        client_id: i32,
    ) -> Result<Option<(i32, String)>, NodeError> {
        let query = Use::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections).execute(
            Query::Use(query),
            internode,
            false,
            open_query_id,
            client_id,
        )
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::{Node, NodeError};
//     use std::collections::HashMap;
//     use std::net::{TcpListener, TcpStream};
//     use std::sync::{Arc, Mutex};

//     // // Función auxiliar para crear un nodo para pruebas
//     // fn create_test_node() -> Arc<Mutex<Node>> {
//     //     let ip = std::net::Ipv4Addr::new(127, 0, 0, 1);
//     //     let seeds_nodes = vec![std::net::Ipv4Addr::new(127, 0, 0, 2)];
//     //     let node = Node::new(ip, seeds_nodes).expect("Error creating node for test");
//     //     Arc::new(Mutex::new(node))
//     // }

//     #[test]
//     fn test_create_protocol_message() {
//         let message = InternodeProtocolHandler::create_protocol_message(
//             "127.0.0.1",
//             1,
//             "CREATE_TABLE",
//             "table_structure",
//             true,
//             true,
//             1,
//             "a",
//         );
//         assert_eq!(
//             message,
//             "QUERY - 127.0.0.1 - 1 - CREATE_TABLE - table_structure - true - true - 1 - a"
//         );
//     }

//     #[test]
//     fn test_create_protocol_response() {
//         let response = InternodeProtocolHandler::create_protocol_response("OK", "content", 1);
//         assert_eq!(response, "RESPONSE - 1 - OK - content - 2");
//     }

//     // #[test]
//     // fn test_handle_invalid_command() {
//     //     // Crear un listener para aceptar conexiones
//     //     let _listener = TcpListener::bind("127.0.0.1:8080").unwrap();

//     //     let node = create_test_node();
//     //     let handler = InternodeProtocolHandler::new();
//     //     let connections = Arc::new(Mutex::new(HashMap::new()));

//     //     // Crear un TcpStream real conectándose al listener
//     //     let client_stream = TcpStream::connect("127.0.0.1:8080").unwrap();

//     //     let result = handler.handle_command(
//     //         &node,
//     //         "INVALID - command",
//     //         &mut Arc::new(Mutex::new(client_stream)),
//     //         connections,
//     //         false,
//     //     );
//     //     assert!(matches!(result, Err(NodeError::InternodeProtocolError)));
//     // }

//     // #[test]
//     // fn test_handle_valid_command() {
//     //     // Crear un listener para aceptar conexiones
//     //     let _listener = TcpListener::bind("127.0.0.4:8080").unwrap();

//     //     let node = create_test_node();
//     //     let handler = InternodeProtocolHandler::new();
//     //     let connections = Arc::new(Mutex::new(HashMap::new()));

//     //     // Crear un TcpStream real conectándose al listener
//     //     let client_stream = TcpStream::connect("127.0.0.4:8080").unwrap();

//     //     let result = handler.handle_command(
//     //         &node,
//     //         "QUERY - 127.0.0.1 - 1 - HANDSHAKE - structure - true - true",
//     //         &mut Arc::new(Mutex::new(client_stream)),
//     //         connections,
//     //         true,
//     //     );
//     //     assert!(result.is_ok());
//     // }
// }
