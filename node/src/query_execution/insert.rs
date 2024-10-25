// Ordered imports
use crate::table::Table;
use crate::NodeError;
use query_coordinator::clauses::insert_sql::Insert;
use query_coordinator::clauses::types::column::Column;
use query_coordinator::errors::CQLError;
use std::fs::File;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::net::Ipv4Addr;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use super::QueryExecution;

impl QueryExecution {
    /// Executes an INSERT operation. This function is public only for internal use
    /// within the library (defined as `pub(crate)`).
    pub(crate) fn execute_insert(
        &mut self,
        insert_query: Insert,
        table_to_insert: Table,
        internode: bool,
        open_query_id: i32,
    ) -> Result<(), NodeError> {
        let node = self.node_that_execute.lock()?;
        // Check if the keyspace exists in the node
        if node.has_no_actual_keyspace() {
            return Err(NodeError::CQLError(CQLError::NoActualKeyspaceError));
        }

        if !node.table_already_exist(table_to_insert.get_name())? {
            return Err(NodeError::CQLError(CQLError::TableAlreadyExist));
        }

        // Retrieve columns and the partition keys
        let columns = table_to_insert.get_columns();

        let mut keys_index: Vec<usize> = columns
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

        let clustering_columns_index: Vec<usize> = columns
            .clone()
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

        // Verificar si hay al menos una partition key
        if keys_index.is_empty() {
            return Err(NodeError::CQLError(CQLError::Error));
        }

        // Clonar los valores del query insert
        let mut values = insert_query.values.clone();

        // Concatenar los valores de las columnas de la partition key para generar el hash
        let value_to_hash = keys_index
            .iter()
            .map(|&index| values[index].clone())
            .collect::<Vec<String>>()
            .join("");

        // Aquí puedes aplicar el algoritmo de hash al `value_to_hash` según lo necesites

        // Validate values before proceeding
        values = self.complete_row(
            columns.clone(),
            insert_query.clone().into_clause.columns,
            values,
        )?;
        self.validate_values(columns, &values)?;
        let ip = node.get_partitioner().get_ip(value_to_hash)?;

        // If not internode and the IP to insert is different, forward the insert
        if !internode && ip != node.get_ip() {
            let serialized_insert = insert_query.serialize();
            self.send_to_single_node(
                node.get_ip(),
                ip,
                "INSERT",
                &serialized_insert,
                true,
                open_query_id,
            )?;
            return Ok(());
        }

        if !internode {
            self.execution_finished_itself = true;
        }

        keys_index.extend(&clustering_columns_index);
        // Perform the insert in this node
        QueryExecution::insert_in_this_node(
            values,
            node.get_ip(),
            insert_query.into_clause.table_name,
            keys_index,
            node.actual_keyspace_name()
                .ok_or(NodeError::KeyspaceError)?,
        )
    }

    fn complete_row(
        &self,
        columns: Vec<Column>,
        specified_columns: Vec<String>,
        values: Vec<String>,
    ) -> Result<Vec<String>, NodeError> {
        let mut complete_row = vec!["".to_string(); columns.len()]; // Crear una fila completa vacía con el tamaño de las columnas
        let mut specified_keys = 0;

        for (i, column) in columns.iter().enumerate() {
            // Verificar si la columna es clave de partición o clave de clustering
            if column.is_partition_key || column.is_clustering_column {
                // Verificar si la columna está especificada en specified_columns
                if let Some(pos) = specified_columns.iter().position(|c| c == &column.name) {
                    // Si está, copiar el valor correspondiente en complete_row
                    complete_row[i] = values[pos].clone();
                    specified_keys += 1;
                }
            } else {
                // Para columnas no clave, si están en specified_columns, copiar el valor
                if let Some(pos) = specified_columns.iter().position(|c| c == &column.name) {
                    complete_row[i] = values[pos].clone();
                }
            }
        }

        // Verificar si se especificaron todas las claves de partición y clustering
        let total_keys = columns
            .iter()
            .filter(|c| c.is_partition_key || c.is_clustering_column)
            .count();
        if specified_keys != total_keys {
            return Err(NodeError::CQLError(
                CQLError::MissingPartitionOrClusteringColumns,
            ));
        }

        Ok(complete_row)
    }

    /// Performs the actual insert operation in the current node
    fn insert_in_this_node(
        values: Vec<String>,
        ip: Ipv4Addr,
        table_name: String,
        index_of_keys: Vec<usize>, // Ahora acepta un vector de índices para las partition keys
        actual_keyspace_name: String,
    ) -> Result<(), NodeError> {
        // Convert the IP to a string to use in the folder name
        let add_str = ip.to_string().replace(".", "_");

        // Generate the folder and file paths for storing the table data
        let folder_name = format!("keyspaces_{}/{}", add_str, actual_keyspace_name);
        let folder_path = Path::new(&folder_name);

        if !folder_path.exists() {
            fs::create_dir_all(&folder_path).map_err(|_| NodeError::OtherError)?;
        }

        // Table file name with ".csv" extension
        let file_path = folder_path.join(format!("{}.csv", table_name));

        // Generate a unique name for the temporary file
        let temp_file_path = folder_path.join(format!(
            "{}.tmp",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| NodeError::OtherError)?
                .as_nanos()
        ));

        // Open the temporary file in write mode
        let mut temp_file = File::create(&temp_file_path).map_err(NodeError::IoError)?;

        // If the table file exists, open it in read mode
        let file = OpenOptions::new().read(true).open(&file_path);
        let mut key_exists = false;

        if let Ok(file) = file {
            let reader = BufReader::new(file);

            // Iterate through the existing file to check for partition key conflicts
            for line in reader.lines() {
                let line = line.map_err(NodeError::IoError)?;
                let row_values: Vec<&str> = line.split(',').map(|s| s.trim()).collect();

                // Check if all partition keys match
                let all_keys_match = index_of_keys
                    .iter()
                    .all(|&index| row_values.get(index) == Some(&values[index].as_str()));

                // If all partition keys match, overwrite the old row
                if all_keys_match {
                    writeln!(temp_file, "{}", values.join(",")).map_err(NodeError::IoError)?;
                    key_exists = true;
                } else {
                    // Otherwise, copy the old row to the temp file
                    writeln!(temp_file, "{}", line).map_err(NodeError::IoError)?;
                }
            }
        }

        // If no matching primary key exists, append the new row at the end
        if !key_exists {
            writeln!(temp_file, "{}", values.join(",")).map_err(NodeError::IoError)?;
        }

        // Rename the temp file to replace the original table file
        fs::rename(&temp_file_path, &file_path).map_err(NodeError::IoError)?;
        Ok(())
    }
}
