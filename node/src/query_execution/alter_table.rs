// Ordered imports
use crate::NodeError;
use query_coordinator::clauses::table::alter_table_cql::AlterTable;
use query_coordinator::clauses::types::alter_table_op::AlterTableOperation;
use query_coordinator::errors::CQLError;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

use super::QueryExecution;

/// Executes the alteration of a table. This function is public only for internal use
/// within the library (defined as `pub(crate)`).
impl QueryExecution {
    pub(crate) fn execute_alter_table(
        &self,
        alter_table: AlterTable,
        internode: bool,
        open_query_id: i32,
    ) -> Result<(), NodeError> {
        let mut node = self
            .node_that_execute
            .lock()
            .map_err(|_| NodeError::LockError)?;

        if node.has_no_actual_keyspace() {
            return Err(NodeError::CQLError(CQLError::NoActualKeyspaceError));
        }

        // Get the table name and lock access to it
        let table_name = alter_table.get_table_name();
        let mut table = node.get_table(table_name.clone())?.inner;

        // Path to the table's file
        // Generate the file name and folder where the table will be stored
        let ip_str = node.get_ip_string().replace(".", "_");
        let folder_name = format!(
            "keyspaces_{}/{}",
            ip_str,
            node.actual_keyspace_name()
                .ok_or(NodeError::KeyspaceError)?
        );
        let file_path = format!("{}/{}.csv", folder_name, table_name);

        // Check if the file exists before proceeding
        if !Path::new(&file_path).exists() {
            return Err(NodeError::CQLError(CQLError::InvalidTable));
        }

        // Apply the alteration operations
        for operation in alter_table.get_operations() {
            match operation {
                AlterTableOperation::AddColumn(column) => {
                    // Add the column to the table's internal structure
                    table.add_column(column.clone())?;
                    // Add the column to the file (update header)
                    Self::add_column_to_file(&file_path, &column.name)?;
                }
                AlterTableOperation::DropColumn(column_name) => {
                    // Remove the column from the table's internal structure
                    table.remove_column(&column_name)?;
                    // Update the file to remove the column
                    Self::remove_column_from_file(&file_path, &column_name)?;
                }
                AlterTableOperation::ModifyColumn(_column_name, _new_data_type, _allows_null) => {
                    // Not supported yet, and may not be needed
                    return Err(NodeError::CQLError(CQLError::InvalidSyntax));
                }
                AlterTableOperation::RenameColumn(old_name, new_name) => {
                    // Rename the column in the table's internal structure
                    table.rename_column(&old_name, &new_name)?;
                    // Update the CSV file with the new name in the header
                    Self::rename_column_in_file(&file_path, &old_name, &new_name)?;
                }
            }
        }

        // Save the changes to the node
        node.update_table(table)?;

        // Communicate changes to other nodes if it's not an internode operation
        if !internode {
            let serialized_alter_table = alter_table.serialize();
            self.send_to_other_nodes(
                node,
                "ALTER_TABLE",
                &serialized_alter_table,
                true,
                open_query_id,
            )?;
        }

        Ok(())
    }

    // Helper function to add a column to the CSV file
    pub(crate) fn add_column_to_file(file_path: &str, column_name: &str) -> Result<(), NodeError> {
        let temp_path = format!("{}.temp", file_path);
        let mut temp_file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&temp_path)?;

        // Read the original file and add the new column to the header
        let file = OpenOptions::new().read(true).open(file_path)?;
        let reader = BufReader::new(file);
        let mut first_line = true;

        for line in reader.lines() {
            let mut line = line?;
            if first_line {
                line.push_str(&format!(",{}", column_name));
                first_line = false;
            } else {
                line.push_str(","); // Add an empty cell for the new column in each row
            }
            writeln!(temp_file, "{}", line)?;
        }

        fs::rename(temp_path, file_path).map_err(NodeError::IoError)
    }

    // Helper function to remove a column from the CSV file
    pub(crate) fn remove_column_from_file(
        file_path: &str,
        column_name: &str,
    ) -> Result<(), NodeError> {
        let temp_path = format!("{}.temp", file_path);
        let mut temp_file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&temp_path)?;

        let file = OpenOptions::new().read(true).open(file_path)?;
        let reader = BufReader::new(file);
        let mut col_index: Option<usize> = None;

        for line in reader.lines() {
            let line = line?;
            let cells: Vec<&str> = line.split(',').collect();

            if col_index.is_none() {
                // Find the index of the column to be removed
                col_index = cells.iter().position(|&col| col == column_name);
                if col_index.is_none() {
                    return Err(NodeError::CQLError(CQLError::InvalidColumn));
                }
            }

            let filtered_line: Vec<&str> = cells
                .iter()
                .enumerate()
                .filter(|&(i, _)| Some(i) != col_index)
                .map(|(_, &cell)| cell)
                .collect();

            writeln!(temp_file, "{}", filtered_line.join(","))?;
        }

        fs::rename(temp_path, file_path).map_err(NodeError::IoError)
    }

    // Helper function to rename a column in the CSV file
    pub(crate) fn rename_column_in_file(
        file_path: &str,
        old_name: &str,
        new_name: &str,
    ) -> Result<(), NodeError> {
        let temp_path = format!("{}.temp", file_path);
        let mut temp_file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&temp_path)?;

        let file = OpenOptions::new().read(true).open(file_path)?;
        let reader = BufReader::new(file);

        for (i, line) in reader.lines().enumerate() {
            let mut line = line?;
            if i == 0 {
                line = line.replace(old_name, new_name); // Rename in the header
            }
            writeln!(temp_file, "{}", line)?;
        }

        fs::rename(temp_path, file_path).map_err(NodeError::IoError)
    }
}
