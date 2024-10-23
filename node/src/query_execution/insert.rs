// Ordered imports
use crate::table::Table;
use crate::NodeError;
use query_coordinator::clauses::insert_sql::Insert;
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

        // Retrieve columns and the primary key
        let columns = table_to_insert.get_columns();
        let primary_key = columns
            .iter()
            .find(|column| column.is_primary_key)
            .ok_or(NodeError::CQLError(CQLError::Error))?;
        // Find the position of the primary key
        let pos = columns
            .iter()
            .position(|x| x == primary_key)
            .ok_or(NodeError::CQLError(CQLError::Error))?;

        let values = insert_query.values.clone();
        let value_to_hash = values[pos].clone();

        // Validate values before proceeding
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
            println!("no soy internodo y tengo que insertar en mi mismo (soy el coordinador)")
        }

        // Perform the insert in this node
        QueryExecution::insert_in_this_node(
            values,
            node.get_ip(),
            insert_query.into_clause.table_name,
            pos,
            node.actual_keyspace_name()
                .ok_or(NodeError::KeyspaceError)?,
        )
    }

    /// Performs the actual insert operation in the current node
    fn insert_in_this_node(
        values: Vec<String>,
        ip: Ipv4Addr,
        table_name: String,
        index_of_primary_key: usize,
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

            // Iterate through the existing file to check for primary key conflicts
            for line in reader.lines() {
                let line = line.map_err(NodeError::IoError)?;
                let row_values: Vec<&str> = line.split(',').map(|s| s.trim()).collect();

                // If the primary key matches, overwrite the old row
                if row_values.get(index_of_primary_key)
                    == Some(&values[index_of_primary_key].as_str())
                {
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
