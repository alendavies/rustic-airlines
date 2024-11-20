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
use uuid::timestamp;

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
        client_id: i32,
        timestamp: i64,
    ) -> Result<(), NodeError> {
        let table;
        let mut do_in_this_node = true;
        let client_keyspace;
        let mut failed_nodes = 0;
        let mut internode_failed_nodes = 0;
        {
            // Get the table name and reference the node
            let table_name = update_query.table_name.clone();
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

            // Validate primary key and where clause
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

            // Validate `IF` clause conditions, if any
            if let Some(if_clause) = update_query.clone().if_clause {
                if_clause.validate_cql_conditions(&partition_keys, &clustering_columns)?;
            }

            // Get the value to hash and determine the node responsible for handling the update
            let value_to_hash = where_clause
                .get_value_partitioner_key_condition(partition_keys)?
                .join("");

            let node_to_update = node.partitioner.get_ip(value_to_hash.clone())?;
            let self_ip = node.get_ip().clone();

            // If not an internode operation and the target node differs, forward the update
            if !internode && node_to_update != self_ip {
                let serialized_update = update_query.serialize();
                failed_nodes = self.send_to_single_node(
                    node.get_ip(),
                    node_to_update,
                    &serialized_update,
                    open_query_id,
                    client_id,
                    &client_keyspace.get_name(),
                    timestamp,
                )?;
                do_in_this_node = false;
            }

            // Send update to replication nodes if needed
            if !internode {
                let serialized_update = update_query.serialize();
                (internode_failed_nodes, replication) = self.send_to_replication_nodes(
                    node,
                    node_to_update,
                    &serialized_update,
                    open_query_id,
                    client_id,
                    &client_keyspace.get_name(),
                    timestamp,
                )?;
            }

            // Set execution finished if this node is the primary and no replication is needed
            if !internode && node_to_update == self_ip {
                self.execution_finished_itself = true;
            }
        }

        failed_nodes += internode_failed_nodes;
        self.how_many_nodes_failed = failed_nodes;

        // Early return if no local execution or replication is needed
        if !do_in_this_node && !replication {
            return Ok(());
        }

        // Set the replication flag if this node should replicate the operation
        if replication {
            self.execution_replicate_itself = true;
        }

        // Perform the update on this node
        let (file_path, temp_file_path) = self.get_file_paths(
            &update_query.table_name,
            replication,
            &client_keyspace.get_name(),
        )?;
        if let Err(e) = self.update_in_this_node(update_query, table, &file_path, &temp_file_path) {
            let _ = std::fs::remove_file(temp_file_path); // Cleanup temp file on error
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

    fn update_or_write_line(
        &self,
        table: &Table,
        update_query: &Update,
        line: &str,
        temp_file: &mut File,
    ) -> Result<bool, NodeError> {
        let mut columns: Vec<String> = line.split(',').map(|s| s.trim().to_string()).collect();
        let column_value_map = self.create_column_value_map(table, &columns, false);

        let columns_ = table.get_columns();

        // Verificar la cláusula `WHERE`
        if let Some(where_clause) = &update_query.where_clause {
            if where_clause
                .condition
                .execute(&column_value_map, columns_.clone())
                .unwrap_or(false)
            {
                // Verificar la cláusula `IF` si está presente
                if let Some(if_clause) = &update_query.if_clause {
                    if !if_clause
                        .condition
                        .execute(&column_value_map, columns_.clone())
                        .unwrap_or(false)
                    {
                        // Si la cláusula `IF` está presente pero no se cumple, no actualizar
                        writeln!(temp_file, "{}", line)?;
                        return Ok(true);
                    }
                }

                // Realizar la actualización si se cumple `WHERE` y, si existe, la `IF`
                for (column, new_value) in update_query.clone().set_clause.get_pairs() {
                    if table.is_primary_key(&column)? {
                        return Err(NodeError::OtherError); // No se permite actualizar claves primarias
                    }
                    let index = table
                        .get_column_index(&column)
                        .ok_or(NodeError::CQLError(CQLError::InvalidColumn))?;

                    columns[index] = new_value.clone();
                }
                writeln!(temp_file, "{}", columns.join(","))?;
                return Ok(true);
            } else {
                // Si `WHERE` no se cumple, verificar si hay `IF`
                writeln!(temp_file, "{}", line)?;
                if let Some(if_clause) = &update_query.if_clause {
                    if if_clause
                        .condition
                        .execute(&column_value_map, columns_.clone())
                        .unwrap_or(false)
                    {
                        return Ok(false);
                    } else {
                        // IF está y no se cumple, devolver true para no crear una nueva fila
                        return Ok(true);
                    }
                } else {
                    return Ok(false);
                }
            }
        } else {
            // Si falta la cláusula `WHERE`, retornar un error
            return Err(NodeError::OtherError);
        }
    }
    fn add_new_row(
        &self,
        table: &Table,
        update_query: &Update,
        temp_file: &mut File,
    ) -> Result<(), NodeError> {
        // Crea una fila nueva vacía con el tamaño de las columnas de la tabla
        let mut new_row: Vec<String> = vec!["".to_string(); table.get_columns().len()];

        // Obtener todas las claves primarias (pueden ser múltiples)
        let primary_keys = table.get_partition_keys()?;
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

        // Obtener todas las clustering columns
        let clustering_keys = table.get_clustering_columns()?;

        // Obtener los valores de las clustering columns desde la cláusula `WHERE`
        let clustering_key_values = update_query
            .where_clause
            .as_ref()
            .map(|where_clause| {
                where_clause.get_value_clustering_column_condition(clustering_keys.clone())
            })
            .ok_or(NodeError::OtherError)??;

        // Coloca cada valor de la clustering column en la posición correcta en `new_row`
        for (i, clustering_key) in clustering_keys.iter().enumerate() {
            let clustering_key_index = table
                .get_column_index(clustering_key)
                .ok_or(NodeError::OtherError)?;

            new_row[clustering_key_index] = clustering_key_values[i].clone();
        }

        // Setea los nuevos valores basados en la cláusula `SET`
        for (column, new_value) in update_query.set_clause.get_pairs() {
            if table.is_primary_key(&column)? {
                return Err(NodeError::OtherError); // No se permite modificar claves primarias
            }
            let index = table
                .get_column_index(&column)
                .ok_or(NodeError::CQLError(CQLError::InvalidColumn))?;

            new_row[index] = new_value.clone();
        }

        // Escribe la nueva fila en el archivo temporal
        writeln!(temp_file, "{}", new_row.join(",")).map_err(|e| {
            println!("Error al escribir en el archivo temporal: {:?}", e);
            NodeError::from(e)
        })
    }
}
