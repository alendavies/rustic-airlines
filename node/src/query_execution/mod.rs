use crate::internode_protocol_handler::InternodeProtocolHandler;
use crate::table::Table;
use crate::utils::{connect, send_message};
use crate::NodeError;
use crate::{Node, INTERNODE_PORT};
use query_creator::clauses::types::column::Column;

pub mod alter_keyspace;
pub mod alter_table;
pub mod create_keyspace;
pub mod create_table;
pub mod delete;
pub mod drop_keyspace;
pub mod drop_table;
pub mod insert;
pub mod select;
pub mod update;
pub mod use_cql;
use query_creator::errors::CQLError;
use query_creator::Query;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufRead;
use std::io::Write;
use std::net::{Ipv4Addr, TcpStream};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{SystemTime, UNIX_EPOCH};

/// Struct for executing various database queries across nodes with support
/// for distributed communication and replication.
pub struct QueryExecution {
    node_that_execute: Arc<Mutex<Node>>,
    connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
    execution_finished_itself: bool,
    execution_replicate_itself: bool,
}

impl QueryExecution {
    /// Constructs a new `QueryExecution` instance, initializing the node and
    /// connection attributes required for handling and distributing queries.
    ///
    /// # Arguments
    /// * `node_that_execute` - A thread-safe reference to the `Node` responsible for executing the query.
    /// * `connections` - A thread-safe map of active connections to other nodes.
    ///
    /// # Returns
    /// * `QueryExecution` - The new instance configured for query execution.
    pub fn new(
        node_that_execute: Arc<Mutex<Node>>,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
    ) -> QueryExecution {
        QueryExecution {
            node_that_execute,
            connections,
            execution_finished_itself: false,
            execution_replicate_itself: false,
        }
    }

    /// Executes a database query by determining the query type and applying the necessary operations
    /// within a distributed setting. Handles various query types such as `SELECT`, `INSERT`,
    /// `DELETE`, and others, ensuring node-specific execution as well as inter-node
    /// communication and replication when required.
    ///
    /// # Arguments
    /// * `query` - The `Query` object specifying the type and details of the query.
    /// * `internode` - Boolean indicating if the query originates from another node.
    /// * `replication` - Boolean indicating whether replication is required.
    /// * `open_query_id` - ID used to track open queries for inter-node communication.
    ///
    /// # Returns
    /// * `Result<Option<(i32, String)>, NodeError>` - Returns:
    ///   - `Ok(Some((how_many_internode_query_has_finish, response)))`: On successful execution, with `how_many_internode_query_has_finish` representing the number of internode queries that have completed, and `response` containing any resulting message.
    ///   - `Ok(None)`: If no additional response is required.
    ///   - `Err(NodeError)`: If an error occurs during execution.
    ///
    /// # Errors
    /// This function may return `NodeError` in cases such as:
    /// - Connection issues between nodes.
    /// - Invalid query structure or unsupported query types.

    // Método para ejecutar la query según su tipo
    pub fn execute(
        &mut self,
        query: Query,
        internode: bool,
        replication: bool,
        open_query_id: i32,
        client_id: i32,
    ) -> Result<Option<(i32, String)>, NodeError> {
        let mut content: Result<Option<String>, NodeError> = Ok(Some(String::from("_")));
        let query_result = {
            match query.clone() {
                Query::Select(select_query) => {
                    match self.execute_select(
                        select_query,
                        internode,
                        replication,
                        open_query_id,
                        client_id,
                    ) {
                        Ok(select_querys) => {
                            content = Ok(Some(select_querys.join("/")));
                            Ok(())
                        }
                        Err(e) => {
                            // Aquí podrías mapear a un error específico de `NodeError`
                            Err(e)
                        }
                    }
                }
                Query::Insert(insert_query) => {
                    let table;
                    {
                        let table_name = insert_query.into_clause.table_name.clone();
                        let mut guard_node = self.node_that_execute.lock()?;
                        let keyspace = guard_node
                            .get_open_handle_query()
                            .get_keyspace_of_query(open_query_id)?
                            .ok_or(NodeError::CQLError(CQLError::NoActualKeyspaceError))?;

                        table = guard_node.get_table(table_name, keyspace)?;
                    }
                    self.execute_insert(
                        insert_query,
                        table,
                        internode,
                        replication,
                        open_query_id,
                        client_id,
                    )
                }
                Query::Update(update_query) => self.execute_update(
                    update_query,
                    internode,
                    replication,
                    open_query_id,
                    client_id,
                ),
                Query::Delete(delete_query) => self.execute_delete(
                    delete_query,
                    internode,
                    replication,
                    open_query_id,
                    client_id,
                ),
                Query::CreateTable(create_table) => {
                    self.execute_create_table(create_table, internode, open_query_id, client_id)
                }
                Query::DropTable(drop_table) => {
                    self.execute_drop_table(drop_table, internode, open_query_id, client_id)
                }
                Query::AlterTable(alter_table) => {
                    self.execute_alter_table(alter_table, internode, open_query_id, client_id)
                }
                Query::CreateKeyspace(create_keyspace) => self.execute_create_keyspace(
                    create_keyspace,
                    internode,
                    open_query_id,
                    client_id,
                ),
                Query::DropKeyspace(drop_keyspace) => {
                    self.execute_drop_keyspace(drop_keyspace, internode, open_query_id, client_id)
                }
                Query::AlterKeyspace(alter_keyspace) => {
                    self.execute_alter_keyspace(alter_keyspace, internode, open_query_id, client_id)
                }
                Query::Use(use_cql) => {
                    self.execute_use(use_cql, internode, open_query_id, client_id)
                }
            }
        };

        if internode {
            let response = {
                match query_result {
                    Ok(_) => InternodeProtocolHandler::create_protocol_response(
                        "OK",
                        &content?.unwrap_or("_".to_string()),
                        open_query_id,
                    ),
                    Err(_) => {
                        println!(
                            "el error en este nodo es {:?} de la query {:?}",
                            query_result, query
                        );
                        InternodeProtocolHandler::create_protocol_response(
                            "ERROR",
                            &content?.unwrap_or("_".to_string()),
                            open_query_id,
                        )
                    }
                }
            };
            Ok(Some((0, response)))
        } else {
            match query_result {
                Ok(_) => {
                    let how_many_internode_query_has_finish = match (
                        self.execution_finished_itself,
                        self.execution_replicate_itself,
                    ) {
                        (true, true) => 2,
                        (true, false) | (false, true) => 1,
                        (false, false) => 0,
                    };

                    if how_many_internode_query_has_finish > 0 {
                        return Ok(Some((
                            how_many_internode_query_has_finish,
                            content?.unwrap_or("_".to_string()),
                        )));
                    } else {
                        Ok(None)
                    }
                }

                Err(e) => return Err(e),
            }
        }
    }

    // Función auxiliar para enviar un mensaje a todos los nodos en el partitioner
    fn send_to_other_nodes(
        &self,
        local_node: MutexGuard<'_, Node>,
        header: &str,
        serialized_message: &str,
        internode: bool,
        open_query_id: i32,
        client_id: i32,
        keyspace_name: &str,
    ) -> Result<(), NodeError> {
        // Serializa el objeto que se quiere enviar
        let message = InternodeProtocolHandler::create_protocol_message(
            &&local_node.get_ip_string(),
            open_query_id,
            header,
            &serialized_message,
            internode,
            false,
            client_id,
            keyspace_name,
        );

        // Bloquea el nodo para obtener el partitioner y la IP
        let current_ip = local_node.get_ip();

        // Recorre los nodos del partitioner y envía el mensaje a cada nodo excepto el actual
        for ip in local_node.get_partitioner().get_nodes() {
            if ip != current_ip {
                let stream = connect(ip, INTERNODE_PORT, self.connections.clone())?;
                send_message(&stream, &message)?;
            }
        }
        Ok(())
    }

    // Función auxiliar para enviar un mensaje a un nodo específico en el partitioner
    fn send_to_single_node(
        &self,
        self_ip: Ipv4Addr,
        target_ip: Ipv4Addr,
        header: &str,
        serialized_message: &str,
        internode: bool,
        open_query_id: i32,
        client_id: i32,
        keyspace_name: &str,
    ) -> Result<(), NodeError> {
        // Serializa el objeto que se quiere enviar
        let message = InternodeProtocolHandler::create_protocol_message(
            &self_ip.to_string(),
            open_query_id,
            header,
            serialized_message,
            internode,
            false,
            client_id,
            keyspace_name,
        );

        // Conecta y envía el mensaje al nodo específico
        let stream = connect(target_ip, INTERNODE_PORT, self.connections.clone())?;
        send_message(&stream, &message)?;
        Ok(())
    }

    // Función auxiliar para enviar un mensaje a todos los nodos en el partitioner
    fn send_to_replication_nodes(
        &self,
        mut local_node: MutexGuard<'_, Node>,
        node_to_get_succesor: Ipv4Addr,
        header: &str,
        serialized_message: &str,
        internode: bool,
        open_query_id: i32,
        client_id: i32,
        keyspace_name: &str,
    ) -> Result<bool, NodeError> {
        // Serializa el objeto que se quiere enviar
        let message = InternodeProtocolHandler::create_protocol_message(
            &&local_node.get_ip_string(),
            open_query_id,
            header,
            &serialized_message,
            internode,
            true,
            client_id,
            keyspace_name,
        );

        // Bloquea el nodo para obtener el partitioner y la IP
        let current_ip = local_node.get_ip();
        let replication_factor = local_node
            .get_open_handle_query()
            .get_keyspace_of_query(open_query_id)?
            .ok_or(NodeError::KeyspaceError)?
            .get_replication_factor();

        let n_succesors = local_node
            .get_partitioner()
            .get_n_successors(node_to_get_succesor, (replication_factor - 1) as usize)?;

        let mut the_node_has_to_replicate = false;
        // Recorre los nodos del partitioner y envía el mensaje a cada nodo excepto el actual
        for ip in n_succesors {
            if ip != current_ip {
                let stream = connect(ip, INTERNODE_PORT, self.connections.clone())?;
                send_message(&stream, &message)?;
            } else {
                the_node_has_to_replicate = true;
            }
        }
        Ok(the_node_has_to_replicate)
    }

    fn validate_values(&self, columns: Vec<Column>, values: &[String]) -> Result<(), CQLError> {
        if values.len() != columns.len() {
            return Err(CQLError::InvalidSyntax);
        }

        for (column, value) in columns.iter().zip(values) {
            if value == "" {
                continue;
            }
            if !column.data_type.is_valid_value(value) {
                return Err(CQLError::InvalidSyntax);
            }
        }
        Ok(())
    }

    /// Obtiene las rutas del archivo principal y del temporal.
    /// Si `replication` es `true`, coloca los archivos dentro de una carpeta "replication" en el keyspace.
    fn get_file_paths(
        &self,
        table_name: &str,
        replication: bool,
        keyspace_name: &str,
    ) -> Result<(String, String), NodeError> {
        let node = self
            .node_that_execute
            .lock()
            .map_err(|_| NodeError::LockError)?;

        let add_str = node.get_ip_string().replace(".", "_");
        let base_folder = format!("keyspaces_{}/{}", add_str, keyspace_name);

        // Agrega la carpeta "replication" si el parámetro es verdadero
        let folder_name = if replication {
            format!("{}/replication", base_folder)
        } else {
            base_folder
        };

        let file_path = format!("{}/{}.csv", folder_name, table_name);

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| NodeError::OtherError)?
            .as_nanos();
        let temp_file_path = format!("{}.{}.temp", file_path, timestamp);

        Ok((file_path, temp_file_path))
    }

    /// Crea un mapa de valores de columna para una fila dada.
    fn create_column_value_map(
        &self,
        table: &Table,
        columns: &[String],
        only_partitioner_key: bool,
    ) -> HashMap<String, String> {
        let mut column_value_map = HashMap::new();
        for (i, column) in table.get_columns().iter().enumerate() {
            if let Some(value) = columns.get(i) {
                if column.is_partition_key || column.is_clustering_column || !only_partitioner_key {
                    column_value_map.insert(column.name.clone(), value.clone());
                }
            }
        }

        column_value_map
    }

    fn write_header<R: BufRead>(
        &self,
        reader: &mut R,
        temp_file: &mut File,
    ) -> Result<(), NodeError> {
        if let Some(header_line) = reader.lines().next() {
            writeln!(temp_file, "{}", header_line?).map_err(|e| NodeError::from(e))?;
        }
        Ok(())
    }

    // Funciones auxiliares adicionales (debes agregarlas también en tu implementación)
    fn create_temp_file(&self, temp_file_path: &str) -> Result<File, NodeError> {
        File::create(temp_file_path).map_err(NodeError::IoError)
    }

    fn replace_original_file(
        &self,
        temp_file_path: &str,
        file_path: &str,
    ) -> Result<(), NodeError> {
        std::fs::rename(temp_file_path, file_path).map_err(NodeError::from)
    }
}
