// Ordered imports
use crate::NodeError;
use query_creator::clauses::use_cql::Use;

use super::QueryExecution;

impl QueryExecution {
    pub(crate) fn execute_use(
        &self,
        use_keyspace: Use,
        internode: bool,
        open_query_id: i32,
        client_id: i32,
    ) -> Result<(), NodeError> {
        let mut node = self
            .node_that_execute
            .lock()
            .map_err(|_| NodeError::LockError)?;

        // Get the name of the keyspace to use
        let keyspace_name = use_keyspace.get_name();

        // Set the current keyspace in the node
        node.set_actual_keyspace(keyspace_name.clone(), client_id)?;

        let keyspaces = node.keyspaces.clone();

        // Buscar el índice del keyspace con el nombre dado
        let index = keyspaces
            .iter()
            .position(|keyspace| keyspace.get_name() == keyspace_name)
            .ok_or(NodeError::KeyspaceError)?;

        node.get_open_handle_query()
            .set_keyspace_of_query(open_query_id, keyspaces[index].clone());

        // If this is not an internode operation, communicate the change to other nodes
        if !internode {
            // Serialize the `UseKeyspace` into a simple message
            let serialized_use_keyspace = use_keyspace.serialize();
            self.send_to_other_nodes(
                node,
                "USE",
                &serialized_use_keyspace,
                true,
                open_query_id,
                client_id,
                &keyspace_name,
            )?;
        }

        Ok(())
    }
}
