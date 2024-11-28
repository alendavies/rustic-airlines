// Exportar todos los elementos del módulo query_execution

use crate::internode_protocol::message::{InternodeMessage, InternodeMessageContent};
use crate::internode_protocol::query::InternodeQuery;
use crate::internode_protocol::response::{InternodeResponse, InternodeResponseStatus};
use crate::open_query_handler::OpenQueryHandler;
use crate::table::Table;
use crate::utils::connect_and_send_message;
use crate::{storage_engine, Node, NodeError, Query, QueryExecution, INTERNODE_PORT};
use chrono::Utc;
use gossip::messages::GossipMessage;
use native_protocol::frame::Frame;
use native_protocol::messages::error;
use native_protocol::Serializable;
use partitioner::Partitioner;
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
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

/// Struct that represents the handler for internode communication protocol.
pub struct InternodeProtocolHandler;

impl InternodeProtocolHandler {
    /// Creates a new `InternodeProtocolHandler` for handling internode commands
    /// and responses between nodes in a distributed setting.
    pub fn new() -> Self {
        InternodeProtocolHandler
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
    /// - The incoming command formatInternodeResponseContent,  is invalid.
    /// - The command type is unrecognized.
    pub fn handle_command(
        &self,
        node: &Arc<Mutex<Node>>,
        message: InternodeMessage,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
    ) -> Result<(), NodeError> {
        match message.clone().content {
            InternodeMessageContent::Query(query) => {
                self.handle_query_command(node, query, connections, message.clone().from)?;
                Ok(())
            }
            InternodeMessageContent::Response(response) => {
                let _ = self.handle_response_command(node, &response, message.from, connections);

                Ok(())
            }
            InternodeMessageContent::Gossip(message) => {
                self.handle_gossip_command(node, &message, connections)?;
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
        table: Option<Table>,
        columns: Vec<Column>,
        self_ip: Ipv4Addr,
        from: Ipv4Addr,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        partitioner: Partitioner,
        storage_path: PathBuf,
    ) -> Result<(), NodeError> {
        if let Some(open_query) =
            query_handler.add_ok_response_and_get_if_closed(open_query_id, response.clone(), from)
        {
            let contents_of_different_nodes = open_query.get_acumulated_responses();
            //here we have to determinated the more new row
            // and do READ REPAIR

            let mut rows = vec![];
            if let Some(table) = table {
                rows = Self::read_repair(
                    contents_of_different_nodes,
                    columns.clone(),
                    self_ip,
                    keyspace_name.clone(),
                    table.clone(),
                    connections,
                    partitioner,
                    storage_path,
                )?;

                rows = if let Some(content) = &response.content {
                    Self::filter_and_join_columns(
                        rows,
                        content.select_columns.clone(),
                        content.columns.clone(),
                    )
                } else {
                    vec![]
                };
            };

            let mut connection = open_query.get_connection();
            let frame =
                open_query
                    .get_query()
                    .create_client_response(columns, keyspace_name, rows)?;
            println!(
                "Returning response to client de la query: {:?}",
                open_query_id
            );

            connection.write(&frame.to_bytes()?).unwrap();
            connection.flush()?;
            Ok(())
        } else {
            Ok(())
        }
    }
    fn read_repair(
        contents_of_different_nodes: Vec<(Ipv4Addr, InternodeResponse)>,
        columns: Vec<Column>,
        self_ip: Ipv4Addr,
        keyspace_name: String,
        table: Table,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        partitioner: Partitioner,
        storage_path: PathBuf,
    ) -> Result<Vec<String>, NodeError> {
        let primary_key_indices = Self::get_key_indices(&columns, true);
        let clustering_column_indices = Self::get_key_indices(&columns, false);

        let latest_versions = Self::find_latest_versions(
            &contents_of_different_nodes,
            &primary_key_indices,
            &clustering_column_indices,
        );

        let updated_rows = Self::repair_nodes(
            contents_of_different_nodes,
            &columns,
            &primary_key_indices,
            &clustering_column_indices,
            latest_versions,
            &self_ip,
            &keyspace_name,
            table,
            &connections,
            &partitioner,
            storage_path,
        )?;

        Ok(updated_rows)
    }

    fn get_key_indices(columns: &[Column], is_partition_key: bool) -> Vec<usize> {
        columns
            .iter()
            .enumerate()
            .filter_map(|(index, column)| {
                if is_partition_key && column.is_partition_key {
                    Some(index)
                } else if !is_partition_key && column.is_clustering_column {
                    Some(index)
                } else {
                    None
                }
            })
            .collect()
    }

    fn find_latest_versions(
        contents_of_different_nodes: &[(Ipv4Addr, InternodeResponse)],
        primary_key_indices: &[usize],
        clustering_column_indices: &[usize],
    ) -> HashMap<String, (Ipv4Addr, i64, Vec<String>)> {
        let mut latest_versions: HashMap<String, (Ipv4Addr, i64, Vec<String>)> = HashMap::new();

        for (node_ip, response) in contents_of_different_nodes {
            if let Some(content) = &response.content {
                for value in &content.values {
                    let key =
                        Self::build_key(value, primary_key_indices, clustering_column_indices);
                    let current_timestamp = Self::get_timestamp(value);

                    if let Some((_, latest_timestamp, _)) = latest_versions.get(&key) {
                        if *latest_timestamp < current_timestamp {
                            latest_versions
                                .insert(key, (*node_ip, current_timestamp, value.clone()));
                        }
                    } else {
                        latest_versions.insert(key, (*node_ip, current_timestamp, value.clone()));
                    }
                }
            }
        }

        latest_versions
    }

    fn build_key(
        value: &[String],
        primary_key_indices: &[usize],
        clustering_column_indices: &[usize],
    ) -> String {
        let mut key_components: Vec<String> = Vec::new();

        for &index in primary_key_indices {
            key_components.push(value[index].clone());
        }
        for &index in clustering_column_indices {
            key_components.push(value[index].clone());
        }

        key_components.join("|")
    }

    fn get_timestamp(value: &[String]) -> i64 {
        let timestamp_index = value.len() - 1;
        value[timestamp_index].parse::<i64>().unwrap_or(0)
    }

    fn repair_nodes(
        contents_of_different_nodes: Vec<(Ipv4Addr, InternodeResponse)>,
        columns: &[Column],
        primary_key_indices: &[usize],
        clustering_column_indices: &[usize],
        latest_versions: HashMap<String, (Ipv4Addr, i64, Vec<String>)>,
        self_ip: &Ipv4Addr,
        keyspace_name: &String,
        table: Table,
        connections: &Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        partitioner: &Partitioner,
        storage_path: PathBuf,
    ) -> Result<Vec<String>, NodeError> {
        let mut updated_rows: Vec<String> = Vec::new();
        let table_name = &table.get_name();
        for (node_ip, response) in &contents_of_different_nodes {
            if let Some(content) = &response.content {
                for value in &content.values {
                    let key =
                        Self::build_key(value, primary_key_indices, clustering_column_indices);

                    if let Some((latest_ip, latest_timestamp, latest_value)) =
                        latest_versions.get(&key)
                    {
                        let current_timestamp = Self::get_timestamp(value);

                        if node_ip != latest_ip && current_timestamp < *latest_timestamp {
                            let insert_query = Self::generate_insert_query(
                                keyspace_name,
                                table_name,
                                columns,
                                latest_value,
                            );

                            let replication = Self::get_is_replication(
                                latest_value,
                                primary_key_indices,
                                partitioner,
                                node_ip,
                            )?;

                            if node_ip != self_ip {
                                Self::send_update_to_node(
                                    *node_ip,
                                    connections,
                                    insert_query,
                                    self_ip,
                                    keyspace_name,
                                    replication,
                                )?;
                            } else {
                                let latest_values = latest_value
                                    .iter()
                                    .map(|v| v.as_str())
                                    .take(latest_value.len() - 1)
                                    .collect();

                                Self::update_this_node(
                                    self_ip,
                                    keyspace_name,
                                    replication,
                                    table_name,
                                    latest_values,
                                    table.get_clustering_column_in_order(),
                                    columns,
                                    storage_path.clone(),
                                )?;
                                // Opcional: manejar lógica para actualizar el propio nodo si es necesario
                            }
                        }
                    }
                }
            }
        }

        updated_rows.extend(
            latest_versions
                .into_iter()
                .map(|(_, (_, _, value))| value.join(",")),
        );

        Ok(updated_rows)
    }

    fn get_is_replication(
        latest_value: &[String],
        primary_key_indices: &[usize],
        partitioner: &Partitioner,
        node_ip: &Ipv4Addr,
    ) -> Result<bool, NodeError> {
        // Construir la clave particionada a partir de los valores de las claves primarias
        let value_partitioner_key: Vec<String> = primary_key_indices
            .iter()
            .map(|&index| latest_value[index].clone())
            .collect();

        let value_to_hash = value_partitioner_key.join("");

        // Determinar si el nodo necesita replicación
        let is_replication = partitioner.get_ip(value_to_hash)? != *node_ip;

        Ok(is_replication)
    }

    fn generate_insert_query(
        keyspace_name: &String,
        table_name: &String,
        columns: &[Column],
        latest_value: &[String],
    ) -> String {
        let mut insert_query = format!("INSERT INTO {}.{} (", keyspace_name, table_name);

        insert_query.push_str(
            &columns
                .iter()
                .map(|col| col.name.clone())
                .collect::<Vec<String>>()
                .join(","),
        );
        insert_query.push_str(") VALUES (");

        insert_query.push_str(
            &latest_value
                .iter()
                .take(latest_value.len().saturating_sub(1))
                .map(|val| format!("'{}'", val))
                .collect::<Vec<String>>()
                .join(","),
        );
        insert_query.push_str(");");

        insert_query
    }

    fn send_update_to_node(
        node_ip: Ipv4Addr,
        connections: &Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        query: String,
        self_ip: &Ipv4Addr,
        keyspace_name: &String,
        replication: bool,
    ) -> Result<(), NodeError> {
        let message = InternodeMessage::new(
            *self_ip,
            InternodeMessageContent::Query(InternodeQuery {
                query_string: query,
                open_query_id: 0,
                client_id: 0,
                replication: replication,
                keyspace_name: keyspace_name.clone(),
                timestamp: Utc::now().timestamp(),
            }),
        );

        connect_and_send_message(node_ip, INTERNODE_PORT, connections.clone(), message)?;
        Ok(())
    }

    fn update_this_node(
        self_ip: &Ipv4Addr,
        keyspace_name: &String,
        replication: bool,
        table_name: &String,
        values: Vec<&str>,
        clustering_columns_in_order: Vec<String>,
        columns: &[Column],
        path: PathBuf,
    ) -> Result<(), NodeError> {
        storage_engine::StorageEngine::new(path, self_ip.to_string()).insert(
            &keyspace_name,
            &table_name,
            values,
            columns.to_vec(),
            clustering_columns_in_order,
            replication,
            false,
            Utc::now().timestamp(),
        )?;
        Ok(())
    }

    fn filter_and_join_columns(
        rows: Vec<String>,
        select_columns: Vec<String>,
        columns: Vec<String>,
    ) -> Vec<String> {
        // Crear el encabezado con las columnas seleccionadas
        let mut result = vec![select_columns.join(",")];

        // Obtener los índices de las columnas seleccionadas
        let selected_indices: Vec<usize> = select_columns
            .iter()
            .filter_map(|col| columns.iter().position(|c| c == col))
            .collect();

        // Procesar cada fila de valores
        let filtered_rows: Vec<String> = rows
            .iter()
            .map(|row| {
                // Dividir la fila en sus componentes (se asume que están separadas por comas)
                let row_values: Vec<&str> = row.split(',').collect();

                // Seleccionar solo los valores correspondientes a los índices de las columnas seleccionadas
                selected_indices
                    .iter()
                    .map(|&i| row_values.get(i).unwrap_or(&"").to_string()) // Crear copias de los valores
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

            let error_frame = Frame::Error(error::Error::ServerError(".".to_string()));

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

        let self_ip;
        {
            let guard_node = node.lock()?;
            self_ip = guard_node.get_ip();
        };
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
            let (_, value): ((i32, i32), InternodeResponse) = responses.clone();

            if query.open_query_id != 0 {
                connect_and_send_message(
                    node_ip,
                    INTERNODE_PORT,
                    connections,
                    InternodeMessage {
                        from: self_ip,
                        content: InternodeMessageContent::Response(value),
                    },
                )?;
            }
        }

        Ok(())
    }

    /// Handles a response command from another node.
    fn handle_response_command(
        &self,
        node: &Arc<Mutex<Node>>,
        response: &InternodeResponse,
        from: Ipv4Addr,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
    ) -> Result<(), NodeError> {
        let self_ip;
        let partitioner;
        let storage_path;
        {
            let guard_node = node.lock()?;
            self_ip = guard_node.get_ip();
            partitioner = guard_node.get_partitioner();
            storage_path = guard_node.storage_path.clone();
        }
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
                    self_ip,
                    from,
                    connections,
                    partitioner,
                    storage_path.clone(),
                )?;
            }
            InternodeResponseStatus::Error => {
                self.process_error_response(query_handler, response.open_query_id as i32)?;
            }
        }

        Ok(())
    }

    /// Handles a gossip command from another node.
    /// This function is responsible for processing the gossip message and responding accordingly.
    fn handle_gossip_command(
        &self,
        node: &Arc<Mutex<Node>>,
        gossip_message: &GossipMessage,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
    ) -> Result<(), NodeError> {
        let mut guard_node = node.lock()?;

        match &gossip_message.payload {
            gossip::messages::Payload::Syn(syn) => {
                let ack = guard_node.gossiper.handle_syn(syn);

                let msg =
                    GossipMessage::new(guard_node.get_ip(), gossip::messages::Payload::Ack(ack));

                let result = connect_and_send_message(
                    gossip_message.from,
                    INTERNODE_PORT,
                    connections,
                    InternodeMessage::new(
                        guard_node.get_ip(),
                        InternodeMessageContent::Gossip(msg),
                    ),
                );

                if result.is_err() {
                    guard_node.gossiper.kill(gossip_message.from).ok();
                }
            }
            gossip::messages::Payload::Ack(ack) => {
                let ack2 = guard_node.gossiper.handle_ack(ack);

                let msg =
                    GossipMessage::new(guard_node.get_ip(), gossip::messages::Payload::Ack2(ack2));

                let result = connect_and_send_message(
                    gossip_message.from,
                    INTERNODE_PORT,
                    connections,
                    InternodeMessage::new(
                        guard_node.get_ip(),
                        InternodeMessageContent::Gossip(msg),
                    ),
                );

                if result.is_err() {
                    println!("Node is dead: {:?}", gossip_message.from);
                    guard_node.gossiper.kill(gossip_message.from).ok();
                }
            }
            gossip::messages::Payload::Ack2(ack2) => {
                guard_node.gossiper.handle_ack2(ack2);
            }
        };

        Ok(())
    }

    /// Procesa la respuesta cuando el estado es "OK"
    fn process_ok_response(
        &self,
        query_handler: &mut OpenQueryHandler,
        response: &InternodeResponse,
        open_query_id: i32,
        keyspace_name: String,
        self_ip: Ipv4Addr,
        from: Ipv4Addr,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        partitioner: Partitioner,
        storage_path: PathBuf,
    ) -> Result<(), NodeError> {
        // Obtener la consulta abierta

        let columns;
        let table;
        {
            let open_query = if let Some(value) = query_handler.get_query_mut(&open_query_id) {
                value
            } else {
                // Si es `None`, retorna `Ok(())`.
                return Ok(());
            };

            // if let Some(table) = open_query.get_table() {
            //     table_name = table.get_name()
            // } else {
            //     table_name = "".to_string();
            // }

            table = open_query.get_table();
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
            table,
            columns,
            self_ip,
            from,
            connections,
            partitioner,
            storage_path,
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
        let storage_path = { node.lock()?.storage_path.clone() };
        QueryExecution::new(node.clone(), connections, storage_path)?.execute(
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

        let storage_path = { node.lock()?.storage_path.clone() };
        QueryExecution::new(node.clone(), connections, storage_path)?.execute(
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
        let storage_path = { node.lock()?.storage_path.clone() };
        QueryExecution::new(node.clone(), connections, storage_path)?.execute(
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
        let storage_path = { node.lock()?.storage_path.clone() };
        QueryExecution::new(node.clone(), connections, storage_path)?.execute(
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
        let storage_path = { node.lock()?.storage_path.clone() };
        let query = CreateKeyspace::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections, storage_path)?.execute(
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
        let storage_path = { node.lock()?.storage_path.clone() };
        QueryExecution::new(node.clone(), connections, storage_path)?.execute(
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
        let storage_path = { node.lock()?.storage_path.clone() };
        QueryExecution::new(node.clone(), connections, storage_path)?.execute(
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
        let storage_path = { node.lock()?.storage_path.clone() };
        QueryExecution::new(node.clone(), connections, storage_path)?.execute(
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
        let storage_path = { node.lock()?.storage_path.clone() };
        QueryExecution::new(node.clone(), connections, storage_path)?.execute(
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
        let storage_path = { node.lock()?.storage_path.clone() };
        QueryExecution::new(node.clone(), connections, storage_path)?.execute(
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
        let storage_path = { node.lock()?.storage_path.clone() };
        QueryExecution::new(node.clone(), connections, storage_path)?.execute(
            Query::Use(query),
            internode,
            false,
            open_query_id,
            client_id,
            None,
        )
    }
}
