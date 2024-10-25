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
    // Función pública de ejecución de DELETE
    pub(crate) fn execute_delete(
        &mut self,
        delete_query: Delete,
        internode: bool,
        open_query_id: i32,
    ) -> Result<(), NodeError> {
        let table;
        {
            let table_name = delete_query.table_name.clone();
            let node = self
                .node_that_execute
                .lock()
                .map_err(|_| NodeError::LockError)?;
            table = node.get_table(table_name.clone())?;

            let partition_keys = table.get_partition_keys()?;
            let clustering_columns = table.get_clustering_columns()?;

            if let Some(columns) = delete_query.columns.clone() {
                for column in columns {
                    if partition_keys.contains(&column) || clustering_columns.contains(&column) {
                        return Err(NodeError::CQLError(CQLError::InvalidColumn));
                    }
                }
            }

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

            let value_to_hash = where_clause
                .get_value_partitioner_key_condition(partition_keys)?
                .join("");

            let node_to_delete = node.partitioner.get_ip(value_to_hash.clone())?;

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

    /// Ejecuta la eliminación en este nodo, reemplazando el archivo CSV de la tabla
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

        // Escribe el encabezado en el archivo temporal
        self.write_header(&mut reader, &mut temp_file)?;

        // Itera sobre cada línea en el archivo original y ejecuta la eliminación
        for line in reader.lines() {
            let line = line?;
            let mut columns: Vec<String> = line.split(',').map(|s| s.trim().to_string()).collect();

            if let Some(columns_to_delete) = &delete_query.columns {
                // Si hay columnas específicas para eliminar, actualiza esas columnas
                if self.should_delete_line(&table, &delete_query, &line)? {
                    for column_name in columns_to_delete {
                        if let Some(index) = table.get_column_index(column_name) {
                            columns[index] = "".to_string(); // Borra el valor de la columna específica
                        }
                    }
                }
                // Escribe la fila modificada en el archivo temporal
                writeln!(temp_file, "{}", columns.join(","))?;
            } else {
                // Si no hay columnas específicas, elimina la fila completa si debe eliminarse
                if !self.should_delete_line(&table, &delete_query, &line)? {
                    writeln!(temp_file, "{}", line)?;
                }
            }
        }
        // Reemplaza el archivo original con el archivo temporal
        self.replace_original_file(&temp_file_path, &file_path)?;
        Ok(())
    }

    /// Verifica si la línea debe ser eliminada según la condición where_clause
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
