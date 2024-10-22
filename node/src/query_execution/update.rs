// Ordered imports
use crate::table::Table;
use crate::NodeError;
use query_coordinator::clauses::set_sql::Set;
use query_coordinator::clauses::types::column::Column;
use query_coordinator::clauses::update_sql::Update;
use query_coordinator::errors::CQLError;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Write};

use super::QueryExecution;

impl QueryExecution {
    /// Validates the types of the `SET` clause against the columns of the table
    pub(crate) fn validate_update_types(
        set_clause: Set,
        columns: Vec<Column>,
    ) -> Result<(), NodeError> {
        for (column_name, value) in set_clause.get_pairs() {
            for column in &columns {
                if *column_name == column.name {
                    if !column.data_type.is_valid_value(value) {
                        return Err(NodeError::CQLError(CQLError::InvalidSyntax));
                    }
                }
            }
        }
        Ok(())
    }

    /// Executes an `UPDATE` operation. This function is public only for internal use
    /// within the library (defined as `pub(crate)`).
    pub(crate) fn execute_update(
        &self,
        update_query: Update,
        internode: bool,
        open_query_id: i32,
    ) -> Result<(), NodeError> {
        let table;
        {
            // Get the table name and the file path
            let table_name = update_query.table_name.clone();
            let node = self
                .node_that_execute
                .lock()
                .map_err(|_| NodeError::LockError)?;
            table = node.get_table(table_name.clone())?;

            // Validate the primary key and where clause
            let primary_key = table.get_primary_key()?;
            let where_clause = update_query
                .clone()
                .where_clause
                .ok_or(NodeError::CQLError(CQLError::NoWhereCondition))?;
            where_clause.validate_cql_conditions(&primary_key, "")?;

            // Get the value to hash for finding the node to update
            let value_to_hash = where_clause
                .get_value_primary_condition(&primary_key)?
                .ok_or(NodeError::OtherError)?;
            let node_to_update = node.partitioner.get_ip(value_to_hash.clone())?;

            // If this is not an internode operation and the target node is different, forward the update
            if !internode && node_to_update != node.get_ip() {
                let serialized_update = update_query.serialize();
                self.send_to_single_node(
                    node.get_ip(),
                    node_to_update,
                    "UPDATE",
                    &serialized_update,
                    true,
                    open_query_id,
                )?;
                return Ok(());
            }
        }

        // Execute the update on this node
        let (file_path, temp_file_path) = self.get_file_paths(&update_query.table_name)?;
        if let Err(e) = self.update_in_this_node(update_query, table, &file_path, &temp_file_path) {
            let _ = std::fs::remove_file(temp_file_path);
            return Err(e);
        }
        Ok(())
    }

    /// Performs the update operation locally on this node
    fn update_in_this_node(
        &self,
        update_query: Update,
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

        // Validate the update types
        Self::validate_update_types(update_query.clone().set_clause, table.get_columns())?;

        let mut found_match = false;

        // Iterate over each line in the original file and apply the update
        for line in reader.lines() {
            let line = line?;
            found_match |=
                self.update_or_write_line(&table, &update_query, &line, &mut temp_file)?;
        }

        // If no matching row was found, add a new row
        if !found_match {
            self.add_new_row(&table, &update_query, &mut temp_file)?;
        }

        // Replace the original file with the updated one
        self.replace_original_file(&temp_file_path, &file_path)?;

        Ok(())
    }

    /// Updates or writes the line to the temporary file, depending on whether it matches the `WHERE` clause
    fn update_or_write_line(
        &self,
        table: &Table,
        update_query: &Update,
        line: &str,
        temp_file: &mut File,
    ) -> Result<bool, NodeError> {
        let mut columns: Vec<String> = line.split(',').map(|s| s.trim().to_string()).collect();
        let column_value_map = self.create_column_value_map(table, &columns, true);

        let mut found_match = false;
        if let Some(where_clause) = &update_query.where_clause {
            if where_clause
                .condition
                .execute(&column_value_map)
                .unwrap_or(false)
            {
                found_match = true;
                for (column, new_value) in update_query.clone().set_clause.get_pairs() {
                    if table.is_primary_key(&column)? {
                        return Err(NodeError::OtherError);
                    }
                    let index = table
                        .get_column_index(&column)
                        .ok_or(NodeError::CQLError(CQLError::InvalidColumn))?;
                    columns[index] = new_value.clone();
                }
            }
        } else {
            return Err(NodeError::OtherError);
        }

        writeln!(temp_file, "{}", columns.join(",")).map_err(|e| NodeError::from(e))?;
        Ok(found_match)
    }

    /// Adds a new row to the table if no matching row was found during the update
    fn add_new_row(
        &self,
        table: &Table,
        update_query: &Update,
        temp_file: &mut File,
    ) -> Result<(), NodeError> {
        let mut new_row: Vec<String> = vec!["".to_string(); table.get_columns().len()];
        let primary_key = table.get_primary_key()?;
        let primary_key_index = table
            .get_column_index(&primary_key)
            .ok_or(NodeError::OtherError)?;

        // Extract the primary key value from the `WHERE` clause
        let primary_key_value = update_query
            .where_clause
            .as_ref()
            .and_then(|where_clause| where_clause.get_value_primary_condition(&primary_key).ok())
            .flatten()
            .ok_or(NodeError::OtherError)?;

        new_row[primary_key_index] = primary_key_value;

        // Set the new values based on the `SET` clause
        for (column, new_value) in update_query.set_clause.get_pairs() {
            if table.is_primary_key(&column)? {
                return Err(NodeError::OtherError);
            }
            let index = table
                .get_column_index(&column)
                .ok_or(NodeError::CQLError(CQLError::InvalidColumn))?;
            new_row[index] = new_value.clone();
        }

        writeln!(temp_file, "{}", new_row.join(",")).map_err(|e| NodeError::from(e))
    }
}
