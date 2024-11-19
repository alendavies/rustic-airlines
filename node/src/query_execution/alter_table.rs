// Ordered imports
use crate::NodeError;
use query_creator::clauses::table::alter_table_cql::AlterTable;
use query_creator::clauses::types::alter_table_op::AlterTableOperation;
use query_creator::errors::CQLError;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

use super::QueryExecution;

impl QueryExecution {
    pub(crate) fn execute_alter_table(
        &mut self,
        alter_table: AlterTable,
        internode: bool,
        open_query_id: i32,
        client_id: i32,
    ) -> Result<(), NodeError> {
        let mut node = self
            .node_that_execute
            .lock()
            .map_err(|_| NodeError::LockError)?;

        let client_keyspace = node
            .get_open_handle_query()
            .get_keyspace_of_query(open_query_id)?
            .ok_or(NodeError::CQLError(CQLError::NoActualKeyspaceError))?;

        // Get the table name and lock access to it
        let table_name = alter_table.get_table_name();

        let mut table = node
            .get_table(table_name.clone(), client_keyspace.clone())?
            .inner;
        // Generate the path to the table's file
        let ip_str = node.get_ip_string().replace(".", "_");
        let folder_name = format!("keyspaces_{}/{}", ip_str, client_keyspace.get_name());
        let file_path = format!("{}/{}.csv", folder_name, table_name);

        // Verify that the file exists before proceeding
        if !Path::new(&file_path).exists() {
            return Err(NodeError::CQLError(CQLError::InvalidTable));
        }

        // Apply each alteration operation
        for operation in alter_table.get_operations() {
            match operation {
                AlterTableOperation::AddColumn(column) => {
                    table.add_column(column.clone())?;
                    Self::add_column_to_file(&file_path, &column.name)?;
                }
                AlterTableOperation::DropColumn(column_name) => {
                    table.remove_column(&column_name)?;
                    Self::remove_column_from_file(&file_path, &column_name)?;
                }
                AlterTableOperation::ModifyColumn(_column_name, _new_data_type, _allows_null) => {
                    return Err(NodeError::CQLError(CQLError::InvalidSyntax));
                }
                AlterTableOperation::RenameColumn(old_name, new_name) => {
                    table.rename_column(&old_name, &new_name)?;
                    Self::rename_column_in_file(&file_path, &old_name, &new_name)?;
                }
            }
        }

        // Save the updated table structure to the node
        node.update_table(&client_keyspace.get_name(), table)?;

        // Broadcast the changes to other nodes if not an internode request
        if !internode {
            let serialized_alter_table = alter_table.serialize();
            self.how_many_nodes_failed = self.send_to_other_nodes(
                node,
                &serialized_alter_table,
                open_query_id,
                client_id,
                &client_keyspace.get_name(),
            )?;
        }

        Ok(())
    }

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
                line.push_str(","); // Append an empty cell for the new column in each row
            }
            writeln!(temp_file, "{}", line)?;
        }

        fs::rename(temp_path, file_path).map_err(NodeError::IoError)
    }

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
