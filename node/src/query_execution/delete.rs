// Ordered imports
use crate::table::Table;
use crate::CQLError;
use crate::NodeError;
use query_coordinator::clauses::delete_sql::Delete;
use std::fs::OpenOptions;
use std::io::Write;
use std::io::{BufRead, BufReader};

use super::QueryExecution;

impl QueryExecution {
    /// Executes a DELETE operation. This function is public only for internal use
    /// within the library (defined as `pub(crate)`).
    pub(crate) fn execute_delete(
        &mut self,
        delete_query: Delete,
        internode: bool,
        open_query_id: i32,
    ) -> Result<(), NodeError> {
        let table;
        {
            // Get the table name and generate the file path
            let table_name = delete_query.table_name.clone();
            let node = self
                .node_that_execute
                .lock()
                .map_err(|_| NodeError::LockError)?;
            table = node.get_table(table_name.clone())?;

            // Validate the primary key and where clause
            let primary_key = table.get_primary_key()?;
            let where_clause = delete_query
                .clone()
                .where_clause
                .ok_or(NodeError::CQLError(CQLError::NoWhereCondition))?;
            where_clause.validate_cql_conditions(&primary_key, "")?;

            // Get the value to hash and determine which node should handle the delete
            let value_to_hash = where_clause
                .get_value_primary_condition(&primary_key)?
                .ok_or(NodeError::OtherError)?;
            let node_to_delete = node.partitioner.get_ip(value_to_hash.clone())?;

            // If this is not an internode operation and the node to delete is different, forward the delete
            if !internode && node_to_delete != node.get_ip() {
                let serialized_delete = delete_query.serialize();
                self.send_to_single_node(
                    node.get_ip(),
                    node_to_delete,
                    "DELETE",
                    &serialized_delete,
                    true,
                    open_query_id,
                )?;
                return Ok(());
            }
        }

        if !internode {
            self.execution_finished_itself = true;
        }

        // Execute the delete on this node
        let (file_path, temp_file_path) = self.get_file_paths(&delete_query.table_name)?;
        if self
            .delete_in_this_node(delete_query, table, &file_path, &temp_file_path)
            .is_err()
        {
            let _ = std::fs::remove_file(temp_file_path);
            return Err(NodeError::OtherError);
        }
        Ok(())
    }

    /// Executes the delete operation on this node by filtering the table's CSV file
    fn delete_in_this_node(
        &self,
        delete_query: Delete,
        table: Table,
        file_path: &str,
        temp_file_path: &str,
    ) -> Result<(), NodeError> {
        // Open the original and temporary files
        let file = OpenOptions::new().read(true).open(&file_path)?;
        let mut reader = BufReader::new(file);
        let mut temp_file = self.create_temp_file(&temp_file_path)?;

        // Write the header to the temporary file
        self.write_header(&mut reader, &mut temp_file)?;

        // Iterate over each line in the original file and perform the deletion
        for line in reader.lines() {
            let line = line?;
            if !self.should_delete_line(&table, &delete_query, &line)? {
                writeln!(temp_file, "{}", line)?;
            }
        }

        // Replace the original file with the temporary file
        self.replace_original_file(&temp_file_path, &file_path)?;
        Ok(())
    }

    /// Checks if the line should be deleted based on the where_clause condition
    fn should_delete_line(
        &self,
        table: &Table,
        delete_query: &Delete,
        line: &str,
    ) -> Result<bool, NodeError> {
        let columns: Vec<String> = line.split(',').map(|s| s.trim().to_string()).collect();
        let column_value_map = self.create_column_value_map(table, &columns, true);

        if let Some(where_clause) = &delete_query.where_clause {
            return Ok(where_clause
                .condition
                .execute(&column_value_map)
                .unwrap_or(false));
        }
        Err(NodeError::OtherError)
    }
}
