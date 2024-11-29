// Ordered imports
use crate::NodeError;
use query_creator::clauses::keyspace::create_keyspace_cql::CreateKeyspace;

use super::QueryExecution;

/// Executes the creation of a keyspace. This function is public only for internal use
/// within the library (defined as `pub(crate)`).
impl QueryExecution {
    pub(crate) fn execute_create_keyspace(
        &mut self,
        create_keyspace: CreateKeyspace,
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
            self.storage_engine.create_keyspace(&keyspace_name)?;
        }

        self.execution_finished_itself = true;
        Ok(())
    }
}
