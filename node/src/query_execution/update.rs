// Ordered imports
use crate::table::Table;
use crate::NodeError;
use query_creator::clauses::set_cql::Set;
use query_creator::clauses::types::column::Column;
use query_creator::clauses::update_cql::Update;
use query_creator::errors::CQLError;
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
                    if column.is_partition_key || column.is_clustering_column {
                        return Err(NodeError::CQLError(CQLError::InvalidCondition));
                    }
                    if !column.data_type.is_valid_value(value) {
                        return Err(NodeError::CQLError(CQLError::InvalidCondition));
                    }
                }
            }
        }
        Ok(())
    }

    /// Executes an `UPDATE` operation. This function is public only for internal use
    /// within the library (defined as `pub(crate)`).
    pub(crate) fn execute_update(
        &mut self,
        update_query: Update,
        internode: bool,
        mut replication: bool,
        open_query_id: i32,
    ) -> Result<(), NodeError> {
        let table;
        let rf;
        let mut do_in_this_node = true;
        {
            // Get the table name and the file path
            let table_name = update_query.table_name.clone();
            let node = self
                .node_that_execute
                .lock()
                .map_err(|_| NodeError::LockError)?;

            if node.has_no_actual_keyspace() {
                return Err(NodeError::CQLError(CQLError::NoActualKeyspaceError));
            }

            table = node.get_table(table_name.clone())?;

            rf = node
                .get_replication_factor()
                .ok_or(NodeError::KeyspaceError)?;

            // Validate the primary key and where clause
            let partition_keys = table.get_partition_keys()?;
            let clustering_columns = table.get_clustering_columns()?;
            let where_clause = update_query
                .clone()
                .where_clause
                .ok_or(NodeError::CQLError(CQLError::NoWhereCondition))?;
            where_clause.validate_cql_conditions(
                &partition_keys,
                &clustering_columns,
                false,
                true,
            )?;

            // Get the value to hash and determine which node should handle the delete
            let value_to_hash = where_clause
                .get_value_partitioner_key_condition(partition_keys)?
                .join("");

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
                do_in_this_node = false;
            }

            let self_ip = node.get_ip().clone();

            if !internode {
                let serialized_delete = update_query.serialize();
                replication = self.send_to_replication_nodes(
                    node,
                    node_to_update,
                    "UPDATE",
                    &serialized_delete,
                    true,
                    open_query_id,
                )?;
            }

            if !internode && rf == 1 && node_to_update != self_ip {
                self.execution_finished_itself = true;
            }
        }

        if !do_in_this_node && !replication {
            return Ok(());
        }

        if replication {
            self.execution_replicate_itself = true;
        }

        // Execute the update on this node
        let (file_path, temp_file_path) =
            self.get_file_paths(&update_query.table_name, replication)?;
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
        // Crea una fila nueva vacía con el tamaño de las columnas de la tabla
        let mut new_row: Vec<String> = vec!["".to_string(); table.get_columns().len()];

        // Obtener todas las claves primarias (pueden ser múltiples)
        let primary_keys = table.get_partition_keys()?; // Supongo que get_primary_keys devuelve Vec<String>

        // Obtener los valores de las claves primarias desde la cláusula `WHERE`
        let primary_key_values = update_query
            .where_clause
            .as_ref()
            .map(|where_clause| {
                where_clause.get_value_partitioner_key_condition(primary_keys.clone())
            })
            .ok_or(NodeError::OtherError)??;

        // Verifica que la cantidad de valores coincida con la cantidad de claves primarias
        if primary_key_values.len() != primary_keys.len() {
            return Err(NodeError::OtherError);
        }

        // Coloca cada valor de la clave primaria en la posición correcta en `new_row`
        for (i, primary_key) in primary_keys.iter().enumerate() {
            let primary_key_index = table
                .get_column_index(primary_key)
                .ok_or(NodeError::OtherError)?;

            new_row[primary_key_index] = primary_key_values[i].clone();
        }

        // Setea los nuevos valores basados en la cláusula `SET`
        for (column, new_value) in update_query.set_clause.get_pairs() {
            if table.is_primary_key(&column)? {
                return Err(NodeError::OtherError); // No se permite modificar una clave primaria
            }
            let index = table
                .get_column_index(&column)
                .ok_or(NodeError::CQLError(CQLError::InvalidColumn))?;
            new_row[index] = new_value.clone();
        }

        // Escribe la nueva fila en el archivo temporal
        writeln!(temp_file, "{}", new_row.join(",")).map_err(|e| NodeError::from(e))
    }
}
