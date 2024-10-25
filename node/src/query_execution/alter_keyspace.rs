// Ordered imports
use crate::NodeError;
use query_creator::clauses::keyspace::alter_keyspace_cql::AlterKeyspace;

use super::QueryExecution;

/// Executes the alteration of a keyspace. This function is public only for internal use
/// within the library (defined as `pub(crate)`).
impl QueryExecution {
    /// Executes the alteration of a keyspace.
    ///
    /// This method updates the replication class and factor for an existing keyspace.
    /// If there are no changes in the replication settings, it will skip the operation.
    /// If it's not an internode operation, it will communicate the changes to other nodes.
    ///
    /// # Arguments
    ///
    /// * `alter_keyspace` - The `AlterKeyspace` object containing the updated replication settings.
    /// * `internode` - A boolean indicating if this is an internode operation.
    /// * `open_query_id` - The ID of the open query for tracking purposes.
    ///
    /// # Returns
    /// Returns `Ok(())` if the keyspace was successfully altered or skipped. If an error occurs,
    /// it returns a `NodeError`.
    pub(crate) fn execute_alter_keyspace(
        &self,
        alter_keyspace: AlterKeyspace,
        internode: bool,
        open_query_id: i32,
    ) -> Result<(), NodeError> {
        // Look for the keyspace in the list of keyspaces
        let mut node = self.node_that_execute.lock()?;
        let mut keyspace = node
            .actual_keyspace
            .clone()
            .ok_or(NodeError::OtherError)?
            .clone();

        // Validate if the replication class and factor are the same to avoid unnecessary operations
        if keyspace.get_replication_class() == alter_keyspace.get_replication_class()
            && keyspace.get_replication_factor() == alter_keyspace.get_replication_factor()
        {
            return Ok(()); // No changes, nothing to execute
        }

        // Update the replication class and factor in the keyspace
        keyspace.update_replication_class(alter_keyspace.get_replication_class());
        keyspace.update_replication_factor(alter_keyspace.get_replication_factor());
        node.update_keyspace(keyspace)?;

        // If not an internode operation, communicate changes to other nodes
        if !internode {
            let serialized_alter_keyspace = alter_keyspace.serialize();
            self.send_to_other_nodes(
                node,
                "ALTER_KEYSPACE",
                &serialized_alter_keyspace,
                true,
                open_query_id,
            )?;
        }

        Ok(())
    }
}
