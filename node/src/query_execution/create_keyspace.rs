// Ordered imports
use crate::NodeError;
use query_creator::clauses::keyspace::create_keyspace_cql::CreateKeyspace;
use storage::StorageEngine;

use super::QueryExecution;

/// Executes the creation of a keyspace. This function is public only for internal use
/// within the library (defined as `pub(crate)`).
impl<T: StorageEngine> QueryExecution<T> {
    pub(crate) fn execute_create_keyspace(
        &self,
        create_keyspace: CreateKeyspace,
        internode: bool,
        open_query_id: i32,
        client_id: i32,
    ) -> Result<(), NodeError> {
        // Locks the node to ensure safe concurrent access

        let mut node = self
            .node_that_execute
            .lock()
            .map_err(|_| NodeError::LockError)?;

        let mut has_to_create = true;
        // Adds the keyspace to the node
        if let Err(e) = node.add_keyspace(create_keyspace.clone()) {
            if create_keyspace.if_not_exists_clause {
                has_to_create = true;
            } else {
                return Err(e);
            }
        }

        if has_to_create {
            // Get the keyspace name
            let keyspace_name = create_keyspace.get_name().clone();

            // Generate the folder name where the keyspace will be stored
            let ip_str = node.get_ip_string().to_string().replace(".", "_");
            let folder_name = format!("keyspaces_{}", ip_str);

            // Create the keyspace folder if it doesn't exist
            let keyspace_path = format!("{}/{}", folder_name, keyspace_name);
            if let Err(e) = std::fs::create_dir_all(&keyspace_path) {
                return Err(NodeError::IoError(e));
            }
        }

        // If this is not an internode operation, communicate the creation to other nodes
        if !internode {
            // Serialize the `CreateKeyspace` structure
            let serialized_create_keyspace = create_keyspace.serialize();
            self.send_to_other_nodes(
                node,
                "CREATE_KEYSPACE",
                &serialized_create_keyspace,
                true,
                open_query_id,
                client_id,
                "None",
            )?;
        }

        Ok(())
    }
}
