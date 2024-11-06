// Ordered imports
use crate::table::Table;
use crate::NodeError;
use query_creator::clauses::select_cql::Select;
use query_creator::errors::CQLError;
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader};

use super::QueryExecution;

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
            let partition_keys = table.get_partition_keys()?;
            let clustering_columns = table.get_clustering_columns()?;
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
                self.send_to_single_node(
                    node.get_ip(),
                    node_to_query,
                    "SELECT",
                    &serialized_query,
                    true,
                    open_query_id,
                    client_id,
                    &client_keyspace.get_name(),
                )?;
                do_in_this_node = false;
            }

            // Send the SELECT to replication nodes if needed
            if !internode {
                let serialized_select = select_query.serialize();
                replication = self.send_to_replication_nodes(
                    node,
                    node_to_query,
                    "SELECT",
                    &serialized_select,
                    true,
                    open_query_id,
                    client_id,
                    &client_keyspace.get_name(),
                )?;
            }

            // Set execution finished if the node itself is the target and no other replication is needed
            if !internode && node_to_query == self_ip {
                self.execution_finished_itself = true;
            }
        }

        // Return if no local execution or replication is needed
        if !do_in_this_node && !replication {
            return Ok(vec![]);
        }

        // Set the replication flag if this node should replicate
        if replication {
            self.execution_replicate_itself = true;
        }

        // Execute the SELECT query on this node if applicable
        let result = self.execute_select_in_this_node(
            select_query,
            table,
            replication,
            &client_keyspace.get_name(),
        )?;
        Ok(result)
    }

    /// Executes the SELECT operation locally on this node
    fn execute_select_in_this_node(
        &self,
        select_query: Select,
        table: Table,
        replication: bool,
        client_keyspace_name: &str,
    ) -> Result<Vec<String>, NodeError> {
        let (file_path, _) =
            self.get_file_paths(&select_query.table_name, replication, client_keyspace_name)?;
        let file = OpenOptions::new().read(true).open(&file_path)?;
        let reader = BufReader::new(file);
        let mut results = Vec::new();
        results.push(select_query.columns.join(","));
        // Iterate over each line in the file and apply the WHERE clause condition
        for (i, line) in reader.lines().enumerate() {
            if i == 0 {
                continue;
            }
            let line = line?;
            if self.line_matches_where_clause(&line, &table, &select_query)? {
                let selected_columns = self.extract_selected_columns(&line, &table, &select_query);
                results.push(selected_columns);
            }
        }
        if let Some(limit) = select_query.limit {
            results = results[..limit].to_vec();
        }

        if let Some(order_by) = select_query.orderby_clause {
            self.sort_results_single_column(&mut results, &order_by.columns[0], &order_by.order)?
        }
        Ok(results)
    }

    /// Sorts the results based on a single specified column and its ordering
    fn sort_results_single_column(
        &self,
        results: &mut Vec<String>,
        order_by_column: &str,
        order: &str, // Either "ASC" or "DESC"
    ) -> Result<(), NodeError> {
        if results.len() <= 1 {
            // No sorting needed if only header or empty results
            return Ok(());
        }

        // Split header from the rest of the rows
        let header = results[0].clone();
        let rows = &mut results[1..];

        // Get the index of the column specified in order_by_column
        let header_columns: Vec<&str> = header.split(',').collect();
        let col_index = header_columns
            .iter()
            .position(|&col| col == order_by_column);

        if let Some(col_index) = col_index {
            // Define sort closure based on order
            rows.sort_by(|a, b| {
                let a_val = a.split(',').nth(col_index).unwrap_or("");
                let b_val = b.split(',').nth(col_index).unwrap_or("");
                let cmp = a_val.cmp(b_val);

                match order {
                    "ASC" => cmp,
                    "DESC" => cmp.reverse(),
                    _ => std::cmp::Ordering::Equal, // Ignore invalid order specifiers
                }
            });
        }

        // Restore header
        results[0] = header;
        Ok(())
    }

    /// Checks if the line matches the WHERE clause condition
    fn line_matches_where_clause(
        &self,
        line: &str,
        table: &Table,
        select_query: &Select,
    ) -> Result<bool, NodeError> {
        // Convert the line into a map of column to value
        let columns: Vec<String> = line.split(',').map(|s| s.trim().to_string()).collect();
        let column_value_map = self.create_column_value_map(table, &columns, true);

        let columns_ = table.get_columns();
        // Check the WHERE clause condition in the SELECT query
        if let Some(where_clause) = &select_query.where_clause {
            Ok(where_clause
                .condition
                .execute(&column_value_map, columns_)?)
        } else {
            Ok(true) // If no WHERE clause, consider the line as matching
        }
    }

    /// Extracts the selected columns from a line according to the SELECT query
    fn extract_selected_columns(&self, line: &str, table: &Table, select_query: &Select) -> String {
        let columns: Vec<String> = line.split(',').map(|s| s.trim().to_string()).collect();
        let column_value_map = self.create_column_value_map(table, &columns, false);

        // Filter only the columns specified in the SELECT query
        let selected_columns: Vec<String> = select_query
            .columns
            .iter()
            .filter_map(|col| column_value_map.get(col).cloned())
            .collect();

        // Join the selected columns into a single comma-separated string
        selected_columns.join(",")
    }
}
