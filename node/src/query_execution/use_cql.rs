// Ordered imports
use crate::NodeError;
use query_creator::clauses::use_cql::Use;

use super::QueryExecution;

/// Executes the selection of a keyspace. This function is public only for internal use
/// within the library (defined as `pub(crate)`).
impl QueryExecution {
    pub(crate) fn execute_use(
        &self,
        use_keyspace: Use,
        internode: bool,
        open_query_id: i32,
    ) -> Result<(), NodeError> {
        let mut node = self
            .node_that_execute
            .lock()
            .map_err(|_| NodeError::LockError)?;

        // Get the name of the keyspace to use
        let keyspace_name = use_keyspace.get_name();

        // Set the current keyspace in the node
        node.set_actual_keyspace(keyspace_name.clone())?;

        // If this is not an internode operation, communicate the change to other nodes
        if !internode {
            // Serialize the `UseKeyspace` into a simple message
            let serialized_use_keyspace = use_keyspace.serialize();
            self.send_to_other_nodes(node, "USE", &serialized_use_keyspace, true, open_query_id)?;
        }

        Ok(())
    }
}
