// Ordered imports
use crate::NodeError;
use query_creator::clauses::keyspace::drop_keyspace_cql::DropKeyspace;

use super::QueryExecution;

/// Executes the deletion of a keyspace. This function is public only for internal use
/// within the library (defined as `pub(crate)`).
impl QueryExecution {
    pub(crate) fn execute_drop_keyspace(
        &self,
        drop_keyspace: DropKeyspace,
        internode: bool,
        open_query_id: i32,
        client_id: i32,
    ) -> Result<(), NodeError> {
        // Get the name of the keyspace to delete
        let keyspace_name = drop_keyspace.get_name().clone();

        // Lock the node and remove the keyspace from the internal structure
        let mut node = self
            .node_that_execute
            .lock()
            .map_err(|_| NodeError::LockError)?;

        node.remove_keyspace(keyspace_name.clone())?;

        self.storage_engine
            .drop_keyspace(&keyspace_name, &node.get_ip_string())?;

        // // If this is not an internode operation, communicate to other nodes
        // if !internode {
        //     // Serialize the `DropKeyspace` structure
        //     let serialized_drop_keyspace = drop_keyspace.serialize();
        //     self.send_to_other_nodes(
        //         node,
        //         &serialized_drop_keyspace,
        //         open_query_id,
        //         client_id,
        //         "None",
        //         0,
        //     )?;
        // }

        Ok(())
    }
}
