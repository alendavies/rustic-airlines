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
    ) -> Result<(), NodeError> {
        // Add the table to the node
        let mut node = self
            .node_that_execute
            .lock()
            .map_err(|_| NodeError::LockError)?;

        if node.has_no_actual_keyspace() {
            return Err(NodeError::CQLError(CQLError::NoActualKeyspaceError));
        }
        node.add_table(create_table.clone())?;

        // Get the table name and column structure
        let table_name = create_table.get_name().clone();
        let columns = create_table.get_columns().clone();

        // Generate the file name and the folder where the table will be stored
        let ip_str = node.get_ip_string().replace(".", "_");
        let folder_name = format!(
            "keyspaces_{}/{}",
            ip_str,
            node.actual_keyspace_name()
                .ok_or(NodeError::KeyspaceError)?
        );
        let file_path = format!("{}/{}.csv", folder_name, table_name);

        // Create the folder if it doesn't exist
        if let Err(e) = std::fs::create_dir_all(&folder_name) {
            return Err(NodeError::IoError(e));
        }

        // Create the file and write the columns as the header
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&file_path)
            .map_err(NodeError::IoError)?;

        let header: Vec<String> = columns.iter().map(|col| col.name.clone()).collect();
        writeln!(file, "{}", header.join(",")).map_err(NodeError::IoError)?;

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
            )?;
        }

        Ok(())
    }
}
