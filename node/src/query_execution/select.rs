// Ordered imports
use super::QueryExecution;
use crate::NodeError;
use query_creator::clauses::select_cql::Select;
use query_creator::errors::CQLError;

impl QueryExecution {
    pub(crate) fn execute_select(
        &mut self,
        mut select_query: Select,
        internode: bool,
        mut replication: bool,
        open_query_id: i32,
        client_id: i32,
    ) -> Result<Vec<String>, NodeError> {
        let table;
        let mut do_in_this_node = true;

        let mut failed_nodes = 0;
        let mut internode_failed_nodes = 0;
        let client_keyspace;
        {
            // Get the table name and reference the node
            let table_name = select_query.table_name.clone();
            let mut node = self
                .node_that_execute
                .lock()
                .map_err(|_| NodeError::LockError)?;

            client_keyspace = node
                .get_open_handle_query()
                .get_keyspace_of_query(open_query_id)?
                .ok_or(NodeError::CQLError(CQLError::NoActualKeyspaceError))?;

            // Get the table and replication factor
            table = node.get_table(table_name.clone(), client_keyspace.clone())?;

            // Validate the primary key and where clause
            let partition_keys = table.get_partition_keys().unwrap();
            let clustering_columns = table.get_clustering_columns().unwrap();
            let where_clause = select_query
                .clone()
                .where_clause
                .ok_or(NodeError::CQLError(CQLError::NoWhereCondition))?;

            where_clause.validate_cql_conditions(
                &partition_keys,
                &clustering_columns,
                true,
                false,
            )?;

            select_query.validate_order_by_cql_conditions(&clustering_columns)?;

            // Ensure that the columns specified in the query exist in the table
            let complet_columns: Vec<String> =
                table.get_columns().iter().map(|c| c.name.clone()).collect();

            if select_query.columns[0] == String::from("*") {
                select_query.columns = complet_columns;
            } else {
                for col in select_query.clone().columns {
                    if !complet_columns.contains(&col) {
                        return Err(NodeError::CQLError(CQLError::InvalidColumn));
                    }
                }
            }

            // Determine the target node based on partition key hashing
            let value_to_hash = where_clause
                .get_value_partitioner_key_condition(partition_keys)?
                .join("");
            let node_to_query = node.partitioner.get_ip(value_to_hash.clone())?;
            let self_ip = node.get_ip().clone();

            // Forward the SELECT if this is not an internode operation and the target node differs
            if !internode && node_to_query != self_ip {
                let serialized_query = select_query.serialize();
                failed_nodes = self.send_to_single_node(
                    node.get_ip(),
                    node_to_query,
                    &serialized_query,
                    open_query_id,
                    client_id,
                    &client_keyspace.get_name(),
                    0,
                )?;
                do_in_this_node = false;
            }

            // Send the SELECT to replication nodes if needed
            if !internode {
                let serialized_select = select_query.serialize();
                (internode_failed_nodes, replication) = self.send_to_replication_nodes(
                    node,
                    node_to_query,
                    &serialized_select,
                    open_query_id,
                    client_id,
                    &client_keyspace.get_name(),
                    0,
                )?;
            }

            // Set execution finished if the node itself is the target and no other replication is needed
            if !internode && node_to_query == self_ip {
                self.execution_finished_itself = true;
            }
        }

        failed_nodes += internode_failed_nodes;
        self.how_many_nodes_failed = failed_nodes;
        // Return if no local execution or replication is needed
        if !do_in_this_node && !replication {
            return Ok(vec![]);
        }

        // Set the replication flag if this node should replicate
        if replication {
            self.execution_replicate_itself = true;
        }

        let results = self.storage_engine.select(
            select_query,
            table,
            replication,
            &client_keyspace.get_name(),
        )?;
        Ok(results)
    }

    // /// Extracts the selected columns from a line according to the SELECT query
    // fn extract_selected_columns(&self, line: &str, table: &Table, select_query: &Select) -> String {
    //     let columns: Vec<String> = line.split(',').map(|s| s.trim().to_string()).collect();
    //     let column_value_map = self.create_column_value_map(table, &columns, false);

    //     // Filter only the columns specified in the SELECT query
    //     let selected_columns: Vec<String> = select_query
    //         .columns
    //         .iter()
    //         .filter_map(|col| column_value_map.get(col).cloned())
    //         .collect();

    //     // Join the selected columns into a single comma-separated string
    //     selected_columns.join(",")
    // }
}
