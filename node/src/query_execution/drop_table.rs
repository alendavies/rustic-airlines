// Ordered imports
use crate::NodeError;
use query_coordinator::clauses::table::drop_table_cql::DropTable;
use query_coordinator::errors::CQLError;

use super::QueryExecution;

/// Executes the deletion of a table. This function is public only for internal use
/// within the library (defined as `pub(crate)`).
impl QueryExecution {
    pub(crate) fn execute_drop_table(
        &self,
        drop_table: DropTable,
        internode: bool,
        open_query_id: i32,
    ) -> Result<(), NodeError> {
        let mut node = self
            .node_that_execute
            .lock()
            .map_err(|_| NodeError::LockError)?;

        if node.has_no_actual_keyspace() {
            return Err(NodeError::CQLError(CQLError::NoActualKeyspaceError));
        }

        // Get the name of the table to delete
        let table_name = drop_table.get_table_name();

        // Lock the node and remove the table from the internal list
        node.remove_table(table_name.clone())?;

        // Generate the file name and folder where the table is stored
        let ip_str = node.get_ip_string().replace(".", "_");
        let folder_name = format!(
            "keyspaces_{}/{}",
            ip_str,
            node.actual_keyspace_name()
                .ok_or(NodeError::KeyspaceError)?
        );
        let file_path = format!("{}/{}.csv", folder_name, table_name);

        // Delete the table file if it exists
        std::fs::remove_file(&file_path)?;

        // If this is not an internode operation, communicate to other nodes
        if !internode {
            // Serialize the `DropTable` into a simple message
            let serialized_drop_table = drop_table.serialize();
            self.send_to_other_nodes(
                node,
                "DROP_TABLE",
                &serialized_drop_table,
                true,
                open_query_id,
            )?;
        }

        Ok(())
    }
}
