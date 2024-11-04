// Ordered imports
use crate::table::Table;
use crate::CQLError;
use crate::NodeError;
use query_creator::clauses::delete_cql::Delete;
use std::fs::OpenOptions;
use std::io::Write;
use std::io::{BufRead, BufReader};

use super::QueryExecution;

impl QueryExecution {
    pub(crate) fn execute_delete(
        &mut self,
        delete_query: Delete,
        internode: bool,
        mut replication: bool,
        open_query_id: i32,
    ) -> Result<(), NodeError> {
        let table;
        let mut do_in_this_node = true;

        {
            // Get the table name and reference the node
            let table_name = delete_query.table_name.clone();
            let node = self
                .node_that_execute
                .lock()
                .map_err(|_| NodeError::LockError)?;

            // Retrieve the table and replication factor
            table = node.get_table(table_name.clone())?;

            if node.has_no_actual_keyspace() {
                return Err(NodeError::CQLError(CQLError::NoActualKeyspaceError));
            }

            // Validate the primary and clustering keys
            let partition_keys = table.get_partition_keys()?;
            let clustering_columns = table.get_clustering_columns()?;

            // Check if columns in DELETE conflict with primary or clustering keys
            if let Some(columns) = delete_query.columns.clone() {
                for column in columns {
                    if partition_keys.contains(&column) || clustering_columns.contains(&column) {
                        return Err(NodeError::CQLError(CQLError::InvalidColumn));
                    }
                }
            }

            // Validate WHERE clause
            let where_clause = delete_query
                .clone()
                .where_clause
                .ok_or(NodeError::CQLError(CQLError::NoWhereCondition))?;

            where_clause.validate_cql_conditions(
                &partition_keys,
                &clustering_columns,
                true,
                false,
            )?;

            // Determine the node responsible for deletion based on hashed partition key values
            let value_to_hash = where_clause
                .get_value_partitioner_key_condition(partition_keys)?
                .join("");
            let node_to_delete = node.partitioner.get_ip(value_to_hash.clone())?;
            let self_ip = node.get_ip().clone();

            // Forward the DELETE operation if the responsible node is different and not an internode operation
            if !internode && node_to_delete != self_ip {
                let serialized_delete = delete_query.serialize();
                self.send_to_single_node(
                    node.get_ip(),
                    node_to_delete,
                    "DELETE",
                    &serialized_delete,
                    true,
                    open_query_id,
                )?;
                do_in_this_node = false;
            }

            // Send DELETE to replication nodes if required
            if !internode {
                let serialized_delete = delete_query.serialize();
                replication = self.send_to_replication_nodes(
                    node,
                    node_to_delete,
                    "DELETE",
                    &serialized_delete,
                    true,
                    open_query_id,
                )?;
            }

            // Set execution_finished_itself if this node is the primary and replication is not needed
            if !internode && node_to_delete == self_ip {
                self.execution_finished_itself = true;
            }
        }

        // Early return if no local execution or replication is needed
        if !do_in_this_node && !replication {
            return Ok(());
        }

        // Set the replication flag if this node should replicate the operation
        if replication {
            self.execution_replicate_itself = true;
        }

        // Execute the delete on this node
        let (file_path, temp_file_path) =
            self.get_file_paths(&delete_query.table_name, replication)?;

        if let Err(e) = self.delete_in_this_node(delete_query, table, &file_path, &temp_file_path) {
            let _ = std::fs::remove_file(temp_file_path); // Cleanup temp file on error
            return Err(e);
        }
        Ok(())
    }

    fn delete_in_this_node(
        &self,
        delete_query: Delete,
        table: Table,
        file_path: &str,
        temp_file_path: &str,
    ) -> Result<(), NodeError> {
        let file = OpenOptions::new().read(true).open(&file_path)?;
        let mut reader = BufReader::new(file);
        let mut temp_file = self.create_temp_file(&temp_file_path)?;

        // Write header to the temporary file
        self.write_header(&mut reader, &mut temp_file)?;

        // Iterate over each line in the original file and apply the delete condition
        for line in reader.lines() {
            let line = line?;
            let mut columns: Vec<String> = line.split(',').map(|s| s.trim().to_string()).collect();

            if let Some(columns_to_delete) = &delete_query.columns {
                // If specific columns are to be deleted, clear those column values
                if self.should_delete_line(&table, &delete_query, &line)? {
                    for column_name in columns_to_delete {
                        if let Some(index) = table.get_column_index(column_name) {
                            columns[index] = "".to_string(); // Clear the value of the specified column
                        }
                    }
                }
                // Write the modified row to the temporary file
                writeln!(temp_file, "{}", columns.join(","))?;
            } else {
                // If no specific columns, delete the entire row if conditions are met
                if !self.should_delete_line(&table, &delete_query, &line)? {
                    writeln!(temp_file, "{}", line)?;
                }
            }
        }
        // Replace the original file with the updated temporary file
        self.replace_original_file(&temp_file_path, &file_path)?;
        Ok(())
    }

    fn should_delete_line(
        &self,
        table: &Table,
        delete_query: &Delete,
        line: &str,
    ) -> Result<bool, NodeError> {
        let columns: Vec<String> = line.split(',').map(|s| s.trim().to_string()).collect();
        let column_value_map = self.create_column_value_map(table, &columns, false);

        let columns = table.get_columns();

        // Verify the `WHERE` clause
        if let Some(where_clause) = &delete_query.where_clause {
            if where_clause
                .condition
                .execute(&column_value_map, columns.clone())
                .unwrap_or(false)
            {
                // Check `IF` clause if present
                if let Some(if_clause) = &delete_query.if_clause {
                    if !if_clause
                        .condition
                        .execute(&column_value_map, columns.clone())
                        .unwrap_or(false)
                    {
                        // `IF` clause exists but does not match; do not delete
                        return Ok(false);
                    }
                }
                // Delete if `WHERE` is met and, if it exists, `IF` is also met
                return Ok(true);
            } else {
                // `WHERE` condition not met; do not delete
                return Ok(false);
            }
        } else {
            // `WHERE` clause is missing, return an error
            return Err(NodeError::OtherError);
        }
    }
}
