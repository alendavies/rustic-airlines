// Ordered imports
use super::QueryExecution;
use crate::NodeError;
use query_creator::clauses::table::drop_table_cql::DropTable;
use query_creator::errors::CQLError;

/// Executes the deletion of a table. This function is public only for internal use
/// within the library (defined as `pub(crate)`).
impl QueryExecution {
    pub(crate) fn execute_drop_table(
        &self,
        drop_table: DropTable,
        open_query_id: i32,
    ) -> Result<(), NodeError> {
        let mut node = self
            .node_that_execute
            .lock()
            .map_err(|_| NodeError::LockError)?;

        let client_keyspace = node
            .get_open_handle_query()
            .get_keyspace_of_query(open_query_id)?
            .ok_or(NodeError::CQLError(CQLError::NoActualKeyspaceError))?;

        // Get the name of the table to delete
        let table_name = drop_table.get_table_name();

        // Lock the node and remove the table from the internal list
        node.remove_table(table_name.clone(), open_query_id)?;

        self.storage_engine
            .drop_table(&client_keyspace.get_name(), &table_name)?;

        Ok(())
    }
}
