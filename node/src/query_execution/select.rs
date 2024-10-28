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
    ) -> Result<Vec<String>, NodeError> {
        let table;
        let rf;
        let mut do_in_this_node = true;

        {
            // Get the table name and reference the node
            let table_name = select_query.table_name.clone();
            let node = self
                .node_that_execute
                .lock()
                .map_err(|_| NodeError::LockError)?;

            if node.has_no_actual_keyspace() {
                return Err(NodeError::CQLError(CQLError::NoActualKeyspaceError));
            }
            // Get the table and primary key
            table = node.get_table(table_name.clone())?;

            rf = node
                .get_replication_factor()
                .ok_or(NodeError::KeyspaceError)?;

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

            // Get the value to hash and determine which node should handle the delete
            let value_to_hash = where_clause
                .get_value_partitioner_key_condition(partition_keys)?
                .join("");
            let node_to_query = node.partitioner.get_ip(value_to_hash.clone())?;

            let self_ip = node.get_ip().clone();

            // If this is not an internode operation, forward the query to the appropriate node
            if !internode && node_to_query != self_ip {
                let serialized_query = select_query.serialize();
                self.send_to_single_node(
                    node.get_ip(),
                    node_to_query,
                    "SELECT",
                    &serialized_query,
                    true,
                    open_query_id,
                )?;
                do_in_this_node = false;
            }

            if !internode {
                let serialized_delete = select_query.serialize();
                replication = self.send_to_replication_nodes(
                    node,
                    node_to_query,
                    "SELECT",
                    &serialized_delete,
                    true,
                    open_query_id,
                )?;
            }

            if !internode && rf == 1 && node_to_query == self_ip {
                self.execution_finished_itself = true;
            }
        }

        if !do_in_this_node && !replication {
            return Ok(vec![]);
        }

        if replication {
            self.execution_replicate_itself = true;
        }

        // Execute the SELECT locally if this is not an internode operation
        let result = self.execute_select_in_this_node(select_query, table, replication)?;
        Ok(result)
    }

    /// Executes the SELECT operation locally on this node
    fn execute_select_in_this_node(
        &self,
        select_query: Select,
        table: Table,
        replication: bool,
    ) -> Result<Vec<String>, NodeError> {
        let (file_path, _) = self.get_file_paths(&select_query.table_name, replication)?;
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
        Ok(results)
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
