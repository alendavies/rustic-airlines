use crate::messages::{
    InternodeMessage, InternodeMessageContent, InternodeQuery, InternodeResponse,
    InternodeResponseContent, InternodeResponseStatus,
};
use crate::open_query_handler::OpenQueryHandler;
use crate::utils::connect_and_send_message;
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
/// Struct that represents the handler for internode communication protocol.
pub struct InternodeProtocolHandler {}

impl InternodeProtocolHandler {
    /// Creates a new `InternodeProtocolHandler` for handling internode commands
    /// and responses between nodes in a distributed setting.
    pub fn new() -> Self {
        InternodeProtocolHandler {}
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
        message: InternodeMessage,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
    ) -> Result<(), NodeError> {
        match message.content {
            InternodeMessageContent::Query(query) => {
                self.handle_query_command(node, query, connections, message.from)?;
                Ok(())
            }
            InternodeMessageContent::Response(response) => {
                self.handle_response_command(node, &response, message.from)?;
                Ok(())
            }
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
    pub fn add_ok_response_to_open_query_and_send_response_if_closed(
        query_handler: &mut OpenQueryHandler,
        response: &InternodeResponse,
        open_query_id: i32,
        keyspace_name: String,
        columns: Vec<Column>,
        from: Ipv4Addr,
    ) -> Result<(), NodeError> {
        if let Some(open_query) =
            query_handler.add_ok_response_and_get_if_closed(open_query_id, response.clone(), from)
        {
            let contents_of_different_nodes = open_query.get_acumulated_responses();
            //here we have to determinated the more new row
            // and do READ REPAIR

            for (_, c) in contents_of_different_nodes.iter().enumerate() {
                if let Some(cont) = c.clone().1.content {
                    println!(
                        "la respuesta del nodo {:?} trajo los valores {:?}",
                        c.0, cont.values
                    );
                }
            }
            let rows = if let Some(content) = &response.content {
                Self::filter_and_join_columns(content)
            } else {
                vec![]
            };

            //let rows = Self::read_repair(contents_of_different_nodes, columns);

            let mut connection = open_query.get_connection();

            let frame =
                open_query
                    .get_query()
                    .create_client_response(columns, keyspace_name, rows)?;

            println!("Returning frame to client: {:?}", frame);

            connection.write(&frame.to_bytes()?)?;
            connection.flush()?;

            Ok(())
        } else {
            Ok(())
        }
    }

    fn read_repair(contents_of_different_nodes: Vec<InternodeResponse>, columns: Vec<Column>) {
        // Identificar índices de claves primarias y columnas de clustering
        let primary_key_indices: Vec<usize> = columns
            .iter()
            .enumerate()
            .filter_map(|(index, column)| {
                if column.is_partition_key {
                    Some(index)
                } else {
                    None
                }
            })
            .collect();

        let clustering_column_indices: Vec<usize> = columns
            .iter()
            .enumerate()
            .filter_map(|(index, column)| {
                if column.is_clustering_column {
                    Some(index)
                } else {
                    None
                }
            })
            .collect();

        // Crear un HashMap para rastrear los timestamps más recientes por clave y IP
        let latest_versions: HashMap<String, (String, i64)> = HashMap::new();

        // Iterar sobre las respuestas de diferentes nodos
        for response in &contents_of_different_nodes {
            if let Some(content) = &response.content {
                // Recorrer los valores de la respuesta
                for value in &content.values {
                    // Construir una clave única a partir de las claves primarias y clustering columns
                    let mut key_components: Vec<String> = Vec::new();
                    for &index in &primary_key_indices {
                        key_components.push(value[index].clone());
                    }
                    for &index in &clustering_column_indices {
                        key_components.push(value[index].clone());
                    }
                    let key = key_components.join("|");

                    // Obtener el timestamp de la última celda
                    let timestamp_index = value.len() - 1;
                    let timestamp = value[timestamp_index].parse::<i64>().unwrap_or_else(|_| 0); // Manejar errores de parseo

                    // Comparar con la entrada existente en el HashMap
                    if let Some((existing_ip, existing_timestamp)) = latest_versions.get(&key) {
                        if *existing_timestamp < timestamp {
                            // Actualizar el registro si el timestamp es más reciente
                            //latest_versions.insert(key, (response.node_ip.clone(), timestamp));
                        }
                    } else {
                        // Insertar si no existe en el HashMap
                        //latest_versions.insert(key, (response.node_ip.clone(), timestamp));
                    }
                }
            }
        }

        // Identificar nodos desactualizados y enviar INSERTs
        for response in &contents_of_different_nodes {
            if let Some(content) = &response.content {
                for value in &content.values {
                    let mut key_components: Vec<String> = Vec::new();
                    for &index in &primary_key_indices {
                        key_components.push(value[index].clone());
                    }
                    for &index in &clustering_column_indices {
                        key_components.push(value[index].clone());
                    }
                    let key = key_components.join("|");

                    // Verificar si el nodo está desactualizado
                    if let Some((latest_ip, latest_timestamp)) = latest_versions.get(&key) {
                        let timestamp_index = value.len() - 1;
                        let current_timestamp =
                            value[timestamp_index].parse::<i64>().unwrap_or_else(|_| 0);

                        // if &response != latest_ip && current_timestamp < *latest_timestamp {
                        //     // Nodo desactualizado: preparar el INSERT
                        //     println!(
                        //         "Nodo desactualizado: IP = {}, Clave = {}, Último timestamp = {}",
                        //         response.node_ip, key, latest_timestamp
                        //     );

                        // Aquí puedes completar con la lógica para enviar el INSERT
                    }
                }
            }
        }
    }

    fn filter_and_join_columns(content: &InternodeResponseContent) -> Vec<String> {
        // Crear el encabezado con las columnas seleccionadas
        let mut result = vec![content.select_columns.join(",")];

        // Obtener los índices de las columnas seleccionadas
        let selected_indices: Vec<usize> = content
            .select_columns
            .iter()
            .filter_map(|col| content.columns.iter().position(|c| c == col))
            .collect();

        // Procesar cada fila de valores
        let filtered_rows: Vec<String> = content
            .values
            .iter()
            .map(|row| {
                selected_indices
                    .iter()
                    .map(|&i| row.get(i).unwrap_or(&String::new()).to_string()) // Crear copias de los valores
                    .collect::<Vec<String>>()
                    .join(",")
            })
            .collect();

        // Agregar los valores procesados al resultado
        result.extend(filtered_rows);

        result
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
    pub fn add_error_response_to_open_query_and_send_response_if_closed(
        query_handler: &mut OpenQueryHandler,
        open_query_id: i32,
    ) -> Result<(), NodeError> {
        if let Some(open_query) = query_handler.add_error_response_and_get_if_closed(open_query_id)
        {
            let mut connection = open_query.get_connection();

            let error_frame = Frame::Error(error::Error::ServerError(
                "A node failed to execute the request of the coordinator.".to_string(),
            ));

            println!("Returning frame to client: {:?}", error_frame);

            connection.write(&error_frame.to_bytes()?)?;
            connection.flush()?;
            Ok(())
        } else {
            Ok(())
        }
    }

    /// Handles a query command received from another node.
    fn handle_query_command(
        &self,
        node: &Arc<Mutex<Node>>,
        query: InternodeQuery,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        node_ip: Ipv4Addr,
    ) -> Result<(), NodeError> {
        if query.keyspace_name != "None" {
            {
                let mut guard_node = node.lock()?;
                let k = guard_node.get_keyspace(query.keyspace_name.as_str())?;
                guard_node.get_open_handle_query().set_keyspace_of_query(
                    query.open_query_id as i32,
                    k.ok_or(NodeError::KeyspaceError)?,
                );
            }
        }
        let query_split: Vec<&str> = query.query_string.split_whitespace().collect();

        let result: Result<Option<((i32, i32), InternodeResponse)>, NodeError> =
            match query_split[0] {
                "CREATE" => match query_split[1] {
                    "TABLE" => Self::handle_create_table_command(
                        node,
                        &query.query_string,
                        connections.clone(),
                        true,
                        query.open_query_id as i32,
                        query.client_id as i32,
                    ),
                    "KEYSPACE" => Self::handle_create_keyspace_command(
                        node,
                        &query.query_string,
                        connections.clone(),
                        true,
                        query.open_query_id as i32,
                        query.client_id as i32,
                    ),
                    _ => Err(NodeError::InternodeProtocolError),
                },
                "DROP" => match query_split[1] {
                    "TABLE" => Self::handle_drop_table_command(
                        node,
                        &query.query_string,
                        connections.clone(),
                        true,
                        query.open_query_id as i32,
                        query.client_id as i32,
                    ),
                    "KEYSPACE" => Self::handle_drop_keyspace_command(
                        node,
                        &query.query_string,
                        connections.clone(),
                        true,
                        query.open_query_id as i32,
                        query.client_id as i32,
                    ),
                    _ => Err(NodeError::InternodeProtocolError),
                },
                "ALTER" => match query_split[1] {
                    "TABLE" => Self::handle_alter_table_command(
                        node,
                        &query.query_string,
                        connections.clone(),
                        true,
                        query.open_query_id as i32,
                        query.client_id as i32,
                    ),
                    "KEYSPACE" => Self::handle_alter_keyspace_command(
                        node,
                        &query.query_string,
                        connections.clone(),
                        true,
                        query.open_query_id as i32,
                        query.client_id as i32,
                    ),
                    _ => Err(NodeError::InternodeProtocolError),
                },
                "INSERT" => Self::handle_insert_command(
                    node,
                    &query.query_string,
                    connections.clone(),
                    true,
                    query.replication,
                    query.open_query_id as i32,
                    query.client_id as i32,
                    query.timestamp,
                ),
                "UPDATE" => Self::handle_update_command(
                    node,
                    &query.query_string,
                    connections.clone(),
                    true,
                    query.replication,
                    query.open_query_id as i32,
                    query.client_id as i32,
                    query.timestamp,
                ),
                "DELETE" => Self::handle_delete_command(
                    node,
                    &query.query_string,
                    connections.clone(),
                    true,
                    query.replication,
                    query.open_query_id as i32,
                    query.client_id as i32,
                    query.timestamp,
                ),
                "SELECT" => Self::handle_select_command(
                    node,
                    &query.query_string,
                    connections.clone(),
                    true,
                    query.replication,
                    query.open_query_id as i32,
                    query.client_id as i32,
                ),
                "USE" => Self::handle_use_command(
                    node,
                    &query.query_string,
                    connections.clone(),
                    true,
                    query.open_query_id as i32,
                    query.client_id as i32,
                ),
                _ => Err(NodeError::InternodeProtocolError),
            };

        let response: Option<((i32, i32), InternodeResponse)> = result?;

        if let Some(responses) = response {
            let (_, value): ((i32, i32), InternodeResponse) = responses;

            connect_and_send_message(
                node_ip,
                INTERNODE_PORT,
                connections,
                InternodeMessage {
                    from: node_ip,
                    content: InternodeMessageContent::Response(value),
                },
            )?;
        }

        Ok(())
    }

    /// Handles a response command from another node.
    fn handle_response_command(
        &self,
        node: &Arc<Mutex<Node>>,
        response: &InternodeResponse,
        from: Ipv4Addr,
    ) -> Result<(), NodeError> {
        let mut guard_node = node.lock()?;

        let query_handler = guard_node.get_open_handle_query();

        let keyspace = query_handler.get_keyspace_of_query(response.open_query_id as i32)?;

        let keyspace_name = if let Some(value) = keyspace {
            value.get_name()
        } else {
            "".to_string()
        };

        match response.status {
            InternodeResponseStatus::Ok => {
                self.process_ok_response(
                    query_handler,
                    response,
                    response.open_query_id as i32,
                    keyspace_name,
                    from,
                )?;
            }
            InternodeResponseStatus::Error => {
                // Aquí puedes agregar la lógica para manejar el caso "ERROR".
                // Por ejemplo, puedes retornar un error específico o realizar otra acción.
                self.process_error_response(query_handler, response.open_query_id as i32)?;
            }
        }

        Ok(())
    }

    // /// Handles a gossip command from another node.
    // fn handle_gossip_command(
    //     &self,
    //     node: &Arc<Mutex<Node>>,
    //     message: &str,
    //     connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
    // ) -> Result<(), NodeError> {
    //     let mut guard_node = node.lock()?;

    //     // guard_node.gossiper;

    //     // TODO
    //     // acá tendríamos acceso a node.gossiper y node.partitioner
    //     // 1. deserializar el msj
    //     // 2. mandar el mensaje que corresponda
    //     // 3. actualizar el node.endpoints_state según corresponda
    //     // 4. informarle al partitioner según corresponda
    //     // listo

    //     // connect_and_send_message(peer_id, port, connections, message);

    //     Ok(())
    // }

    /// Procesa la respuesta cuando el estado es "OK"
    fn process_ok_response(
        &self,
        query_handler: &mut OpenQueryHandler,
        response: &InternodeResponse,
        open_query_id: i32,
        keyspace_name: String,
        from: Ipv4Addr,
    ) -> Result<(), NodeError> {
        // Obtener la consulta abierta

        let columns;
        {
            let open_query = if let Some(value) = query_handler.get_query_mut(&open_query_id) {
                value
            } else {
                // Si es `None`, retorna `Ok(())`.
                return Ok(());
            };

            // Copiar los valores necesarios para evitar el uso de `open_query` posteriormente
            columns = open_query
                .get_table()
                .map_or_else(Vec::new, |table| table.get_columns());
        }
        // Llamar a la función con los valores copiados, sin `open_query` en uso
        Self::add_ok_response_to_open_query_and_send_response_if_closed(
            query_handler,
            response,
            open_query_id,
            keyspace_name,
            columns,
            from,
        )?;

        Ok(())
    }

    /// Procesa la respuesta cuando el estado es "OK"
    fn process_error_response(
        &self,
        query_handler: &mut OpenQueryHandler,
        open_query_id: i32,
    ) -> Result<(), NodeError> {
        Self::add_error_response_to_open_query_and_send_response_if_closed(
            query_handler,
            open_query_id,
        )?;

        Ok(())
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
        timestamp: i64,
    ) -> Result<Option<((i32, i32), InternodeResponse)>, NodeError> {
        let query = Insert::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections)?.execute(
            Query::Insert(query),
            internode,
            replication,
            open_query_id,
            client_id,
            Some(timestamp),
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
    ) -> Result<Option<((i32, i32), InternodeResponse)>, NodeError> {
        let query = CreateTable::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections)?.execute(
            Query::CreateTable(query),
            internode,
            false,
            open_query_id,
            client_id,
            None,
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
    ) -> Result<Option<((i32, i32), InternodeResponse)>, NodeError> {
        let query = DropTable::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections)?.execute(
            Query::DropTable(query),
            internode,
            false,
            open_query_id,
            client_id,
            None,
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
    ) -> Result<Option<((i32, i32), InternodeResponse)>, NodeError> {
        let query = AlterTable::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections)?.execute(
            Query::AlterTable(query),
            internode,
            false,
            open_query_id,
            client_id,
            None,
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
    ) -> Result<Option<((i32, i32), InternodeResponse)>, NodeError> {
        let query = CreateKeyspace::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections)?.execute(
            Query::CreateKeyspace(query),
            internode,
            false,
            open_query_id,
            client_id,
            None,
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
    ) -> Result<Option<((i32, i32), InternodeResponse)>, NodeError> {
        let query = DropKeyspace::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections)?.execute(
            Query::DropKeyspace(query),
            internode,
            false,
            open_query_id,
            client_id,
            None,
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
    ) -> Result<Option<((i32, i32), InternodeResponse)>, NodeError> {
        let query = AlterKeyspace::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections)?.execute(
            Query::AlterKeyspace(query),
            internode,
            false,
            open_query_id,
            client_id,
            None,
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
        timestamp: i64,
    ) -> Result<Option<((i32, i32), InternodeResponse)>, NodeError> {
        let query = Update::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections)?.execute(
            Query::Update(query),
            internode,
            replication,
            open_query_id,
            client_id,
            Some(timestamp),
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
        timestamp: i64,
    ) -> Result<Option<((i32, i32), InternodeResponse)>, NodeError> {
        let query = Delete::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections)?.execute(
            Query::Delete(query),
            internode,
            replication,
            open_query_id,
            client_id,
            Some(timestamp),
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
    ) -> Result<Option<((i32, i32), InternodeResponse)>, NodeError> {
        let query = Select::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections)?.execute(
            Query::Select(query),
            internode,
            replication,
            open_query_id,
            client_id,
            None,
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
    ) -> Result<Option<((i32, i32), InternodeResponse)>, NodeError> {
        let query = Use::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections)?.execute(
            Query::Use(query),
            internode,
            false,
            open_query_id,
            client_id,
            None,
        )
    }
}
