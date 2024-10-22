// Ordered imports
use crate::table::Table;
use crate::NodeError;
use query_coordinator::clauses::select_sql::Select;
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader};

use super::QueryExecution;

impl QueryExecution {
    /// Executes a SELECT operation. This function is public only for internal use
    /// within the library (defined as `pub(crate)`).
    pub(crate) fn execute_select(
        &self,
        select_query: Select,
        internode: bool,
        open_query_id: i32,
    ) -> Result<Vec<String>, NodeError> {
        let table;
        {
            // Get the table name and reference the node
            let table_name = select_query.table_name.clone();
            let node = self
                .node_that_execute
                .lock()
                .map_err(|_| NodeError::LockError)?;

            // Get the table and primary key
            table = node.get_table(table_name.clone())?;
            let primary_key = table.get_primary_key()?;

            // Validate the WHERE clause to ensure it contains the primary key
            let where_clause = select_query
                .where_clause
                .clone()
                .ok_or(NodeError::OtherError)?;
            where_clause.validate_cql_conditions(&primary_key, "")?;

            // Get the value of the primary key condition to calculate the node location
            let value_to_hash = where_clause
                .get_value_primary_condition(&primary_key)?
                .ok_or(NodeError::OtherError)?;
            let node_to_query = node.partitioner.get_ip(value_to_hash.clone())?;

            // If this is not an internode operation, forward the query to the appropriate node
            if !internode && node_to_query != node.get_ip() {
                let serialized_query = select_query.serialize();
                self.send_to_single_node(
                    node.get_ip(),
                    node_to_query,
                    "SELECT",
                    &serialized_query,
                    true,
                    open_query_id,
                )?;
            }
        }
        // Execute the SELECT locally if this is not an internode operation
        let result = self.execute_select_in_this_node(select_query, table)?;
        Ok(result)
    }

    /// Executes the SELECT operation locally on this node
    fn execute_select_in_this_node(
        &self,
        select_query: Select,
        table: Table,
    ) -> Result<Vec<String>, NodeError> {
        let (file_path, _) = self.get_file_paths(&select_query.table_name)?;
        let file = OpenOptions::new().read(true).open(&file_path)?;
        let reader = BufReader::new(file);
        let mut results = Vec::new();
        results.push(select_query.columns.join(","));
        // Iterate over each line in the file and apply the WHERE clause condition
        for line in reader.lines() {
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
        // Check the WHERE clause condition in the SELECT query
        if let Some(where_clause) = &select_query.where_clause {
            Ok(where_clause
                .condition
                .execute(&column_value_map)
                .unwrap_or(false))
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
