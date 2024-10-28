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
    ) -> Result<(), NodeError> {
        // Get the name of the keyspace to delete
        let keyspace_name = drop_keyspace.get_name().clone();

        // Lock the node and remove the keyspace from the internal structure
        let mut node = self
            .node_that_execute
            .lock()
            .map_err(|_| NodeError::LockError)?;
        node.remove_keyspace(keyspace_name.clone())?;

        // Generate the folder name where the keyspace is stored
        let ip_str = node.get_ip_string().to_string().replace(".", "_");
        let folder_name = format!("keyspaces_{}", ip_str);

        // Define the keyspace path and delete the folder if it exists
        let keyspace_path = format!("{}/{}", folder_name, keyspace_name);
        if let Err(e) = std::fs::remove_dir_all(&keyspace_path) {
            return Err(NodeError::IoError(e));
        }

        // If this is not an internode operation, communicate to other nodes
        if !internode {
            // Serialize the `DropKeyspace` structure
            let serialized_drop_keyspace = drop_keyspace.serialize();
            self.send_to_other_nodes(
                node,
                "DROP_KEYSPACE",
                &serialized_drop_keyspace,
                true,
                open_query_id,
            )?;
        }

        Ok(())
    }
}
