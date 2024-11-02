// Ordered imports
use crate::NodeError;
use query_creator::clauses::table::create_table_cql::CreateTable;
use query_creator::errors::CQLError;
use std::fs::OpenOptions;
use std::io::Write;

use super::QueryExecution;

/// Executes the creation of a table. This function is public only for internal use
/// within the library (defined as `pub(crate)`).
impl QueryExecution {
    pub(crate) fn execute_create_table(
        &self,
        create_table: CreateTable,
        internode: bool,
        open_query_id: i32,
        client_id: i32,
    ) -> Result<(), NodeError> {
        // Add the table to the node
        let mut node = self
            .node_that_execute
            .lock()
            .map_err(|_| NodeError::LockError)?;

        if !node.has_actual_keyspace(client_id)? {
            return Err(NodeError::CQLError(CQLError::NoActualKeyspaceError));
        }
        node.add_table(create_table.clone(), client_id)?;

        // Get the table name and column structure
        let table_name = create_table.get_name().clone();
        let columns = create_table.get_columns().clone();

        let client_keyspace = node
            .get_client_keyspace(client_id)?
            .ok_or(NodeError::KeyspaceError)?;

        // Generate the primary and replication folder paths
        let ip_str = node.get_ip_string().replace(".", "_");
        let keyspace_name = client_keyspace.get_name();
        let primary_folder = format!("keyspaces_{}/{}", ip_str, keyspace_name);
        let replication_folder = format!("{}/replication", primary_folder);
        let primary_file_path = format!("{}/{}.csv", primary_folder, table_name);
        let replication_file_path = format!("{}/{}.csv", replication_folder, table_name);

        // Create the primary and replication folders if they don't exist
        std::fs::create_dir_all(&primary_folder).map_err(NodeError::IoError)?;
        std::fs::create_dir_all(&replication_folder).map_err(NodeError::IoError)?;

        // Create the file in the primary folder and write the columns as the header
        let mut primary_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&primary_file_path)
            .map_err(NodeError::IoError)?;

        let header: Vec<String> = columns.iter().map(|col| col.name.clone()).collect();
        writeln!(primary_file, "{}", header.join(",")).map_err(NodeError::IoError)?;

        // Create the same file in the replication folder and write the columns as the header
        let mut replication_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&replication_file_path)
            .map_err(NodeError::IoError)?;

        writeln!(replication_file, "{}", header.join(",")).map_err(NodeError::IoError)?;

        // If this is not an internode operation, communicate to other nodes
        if !internode {
            // Serialize the `CreateTable` structure
            let serialized_create_table = create_table.serialize();
            self.send_to_other_nodes(
                node,
                "CREATE_TABLE",
                &serialized_create_table,
                true,
                open_query_id,
                client_id,
            )?;
        }

        Ok(())
    }
}
