use std::{
    fs::{self, OpenOptions},
    io::{BufRead, BufReader, Seek},
};

use query_creator::clauses::select_cql::Select;

use crate::table::Table;

use super::{errors::StorageEngineError, StorageEngine};

impl StorageEngine {
    pub fn select(
        &self,
        select_query: Select,
        table: Table,
        is_replication: bool,
        keyspace: &str,
    ) -> Result<Vec<String>, StorageEngineError> {
        let table_name = table.get_name();
        let base_folder_path = self.get_keyspace_path(keyspace);

        // Construcción de la ruta de la carpeta según si es replicación o no
        let folder_path = if is_replication {
            base_folder_path.join("replication")
        } else {
            base_folder_path
        };

        // Crear la carpeta si no existe
        if !folder_path.exists() {
            fs::create_dir_all(&folder_path)
                .map_err(|_| StorageEngineError::DirectoryCreationFailed)?;
        }

        // Rutas para los archivos de datos e índices
        let file_path = folder_path.join(format!("{}.csv", table_name));
        let index_file_path = folder_path.join(format!("{}_index.csv", table_name));

        let file = OpenOptions::new().read(true).open(&file_path)?;
        let index_file = OpenOptions::new().read(true).open(&index_file_path)?;
        let mut reader = BufReader::new(file);

        // Leer los índices
        let index_reader = BufReader::new(index_file);
        let mut start_byte = 0;
        let mut end_byte = u64::MAX;

        // Obtener la primera columna de clustering y sus valores
        if let Some(first_clustering_column) = table.get_clustering_column_in_order().get(0) {
            let clustering_value = select_query
                .clone()
                .where_clause
                .ok_or(StorageEngineError::MissingWhereClause)?
                .get_value_for_clustering_column(&first_clustering_column);

            if let Some(clustering_column_value) = clustering_value {
                for (i, line) in index_reader.lines().enumerate() {
                    if i == 0 {
                        // Saltar el header del archivo de índices
                        continue;
                    }
                    let line = line?;
                    let parts: Vec<&str> = line.split(',').collect();
                    if parts.len() == 3 && parts[0] == clustering_column_value {
                        start_byte = parts[1].parse::<u64>().unwrap_or(0);
                        end_byte = parts[2].parse::<u64>().unwrap_or(u64::MAX);
                        break;
                    }
                }
            }
        }

        // Posicionar el lector en el rango de bytes
        if start_byte > 0 {
            reader.seek(std::io::SeekFrom::Start(start_byte))?;
        } else {
            // Si no se encontró la clustering column, saltar el header manualmente
            let mut buffer = String::new();
            reader.read_line(&mut buffer)?; // Leer y descartar el header
        }

        let mut results = Vec::new();
        let complete_columns: Vec<String> =
            table.get_columns().iter().map(|c| c.name.clone()).collect();
        results.push(complete_columns.join(","));
        results.push(select_query.columns.join(","));

        // Leer las líneas del rango especificado
        let mut current_byte_offset = start_byte;

        while current_byte_offset < end_byte {
            let mut buffer = String::new();
            let bytes_read = reader.read_line(&mut buffer)?;
            if bytes_read == 0 {
                break; // Fin del archivo
            }
            current_byte_offset += bytes_read as u64;
            let (line, _) = buffer
                .trim_end()
                .split_once(";")
                .ok_or(StorageEngineError::IoError)?;

            if self.line_matches_where_clause(&line, &table, &select_query)? {
                results.push(buffer.trim_end().to_string());
            }
        }

        // Aplicar `LIMIT` si está presente
        if let Some(limit) = select_query.limit {
            if limit < results.len() {
                results = results[..limit + 1].to_vec();
            }
        }

        // Ordenar los resultados si hay cláusula `ORDER BY`
        if let Some(order_by) = select_query.orderby_clause {
            self.sort_results_single_column(&mut results, &order_by.columns[0], &order_by.order)?
        }

        Ok(results)
    }

    fn sort_results_single_column(
        &self,
        results: &mut Vec<String>,
        order_by_column: &str,
        order: &str, // Either "ASC" or "DESC"
    ) -> Result<(), StorageEngineError> {
        if results.len() <= 3 {
            // No sorting needed if only headers or very few rows
            return Ok(());
        }

        // Separate the two headers
        let header1 = results[0].clone();
        let header2 = results[1].clone();
        let rows = &mut results[2..];

        // Get the index of the column specified in order_by_column
        let header_columns: Vec<&str> = header1.split(',').collect();
        let col_index = header_columns
            .iter()
            .position(|&col| col == order_by_column);

        if let Some(col_index) = col_index {
            // Define sort closure based on order
            rows.sort_by(|a, b| {
                let a_val = a.split(',').nth(col_index).unwrap_or("");
                let b_val = b.split(',').nth(col_index).unwrap_or("");
                let cmp = a_val.cmp(b_val);

                match order {
                    "ASC" => cmp,
                    "DESC" => cmp.reverse(),
                    _ => std::cmp::Ordering::Equal, // Ignore invalid order specifiers
                }
            });
        }

        // Restore headers
        results[0] = header1;
        results[1] = header2;
        Ok(())
    }

    fn line_matches_where_clause(
        &self,
        line: &str,
        table: &Table,
        select_query: &Select,
    ) -> Result<bool, StorageEngineError> {
        // Convert the line into a map of column to value
        let columns: Vec<String> = line.split(',').map(|s| s.trim().to_string()).collect();
        let column_value_map = self.create_column_value_map(table, &columns, false);

        let columns = table.get_columns();
        // Check the WHERE clause condition in the SELECT query
        if let Some(where_clause) = &select_query.where_clause {
            Ok(where_clause
                .condition
                .execute(&column_value_map, columns)
                .map_err(|_| StorageEngineError::MissingWhereClause)?)
        } else {
            Ok(true) // If no WHERE clause, consider the line as matching
        }
    }
}
