// Ordered imports
use crate::NodeError;
use query_creator::clauses::keyspace::create_keyspace_cql::CreateKeyspace;

use super::QueryExecution;

/// Executes the creation of a keyspace. This function is public only for internal use
/// within the library (defined as `pub(crate)`).
impl QueryExecution {
    pub(crate) fn execute_create_keyspace(
        &self,
        create_keyspace: CreateKeyspace,
        internode: bool,
        open_query_id: i32,
        client_id: i32,
    ) -> Result<(), NodeError> {
        // Locks the node to ensure safe concurrent access

        println!("entro a crear keyspace");
        let mut node = self
            .node_that_execute
            .lock()
            .map_err(|_| NodeError::LockError)?;

        // Adds the keyspace to the node
        node.add_keyspace(create_keyspace.clone())?;

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
            )?;
        }

        Ok(())
    }
}
