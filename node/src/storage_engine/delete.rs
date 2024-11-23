use std::{
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Write},
    time::{SystemTime, UNIX_EPOCH},
};

use query_creator::clauses::delete_cql::Delete;

use crate::table::Table;

use super::{errors::StorageEngineError, StorageEngine};

impl StorageEngine {
    pub fn delete(
        &self,
        delete_query: Delete,
        table: Table,
        keyspace: &str,
        is_replication: bool,
    ) -> Result<(), StorageEngineError> {
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

        // Rutas para los archivos de datos y de índices
        let file_path = folder_path.join(format!("{}.csv", table_name));
        let temp_file_path = folder_path.join(format!(
            "{}.tmp",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| StorageEngineError::TempFileCreationFailed)?
                .as_nanos()
        ));
        let index_file_path = folder_path.join(format!("{}_index.csv", table_name));
        let temp_index_file_path = folder_path.join(format!(
            "{}_index.tmp",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| StorageEngineError::TempFileCreationFailed)?
                .as_nanos()
        ));

        // Abrir el archivo original, si no existe retornar error
        let file = OpenOptions::new()
            .read(true)
            .open(&file_path)
            .map_err(|_| StorageEngineError::FileNotFound)?;
        let reader = BufReader::new(file);

        // Crear los archivos temporales para datos y para índices
        let mut temp_file = File::create(&temp_file_path)
            .map_err(|_| StorageEngineError::TempFileCreationFailed)?;
        let mut temp_index_file = File::create(&temp_index_file_path)
            .map_err(|_| StorageEngineError::TempFileCreationFailed)?;

        // Escribir el encabezado en el archivo temporal de índices
        writeln!(temp_index_file, "clustering_column,start_byte,end_byte")
            .map_err(|_| StorageEngineError::FileWriteFailed)?;

        // Variables para manejar índices
        let mut current_byte_offset: u64 = 0;
        let mut index_map: Vec<(String, (u64, u64))> = Vec::new();

        // Obtener los nombres y órdenes de las columnas de clustering
        let clustering_key_order: Vec<(usize, String)> = table
            .get_clustering_column_in_order()
            .iter()
            .filter_map(|col_name| {
                table.get_column_index(col_name).map(|idx| {
                    let order = table
                        .get_columns()
                        .iter()
                        .find(|col| &col.name == col_name)
                        .map(|col| col.clustering_order.clone()) // Suponiendo que `order` es un String en la columna
                        .unwrap_or_else(|| "ASC".to_string()); // Predeterminado a ASC si no se encuentra
                    (idx, order)
                })
            })
            .collect();

        // Iterar sobre cada línea del archivo original
        for (i, line) in reader.lines().enumerate() {
            let line = line.map_err(|_| StorageEngineError::IoError)?;
            let line_length = line.len() as u64;
            if i == 0 {
                current_byte_offset += line_length + 1;
                writeln!(temp_file, "{}", line)?;
                continue;
            }

            let mut columns: Vec<String> = line.split(',').map(|s| s.trim().to_string()).collect();

            let mut write_line = true; // Flag para determinar si la línea debe ser escrita

            if let Some(columns_to_delete) = &delete_query.columns {
                // Si hay columnas específicas para eliminar, borra esos valores
                if self.should_delete_line(&table, &delete_query, &line)? {
                    for column_name in columns_to_delete {
                        if let Some(index) = table.get_column_index(column_name) {
                            columns[index] = "".to_string(); // Vaciar el valor de la columna específica
                        }
                    }
                } else {
                    // Si se debe borrar toda la fila, no la escribimos
                    write_line = true;
                }
            } else {
                // Si no hay columnas específicas, elimina la fila si se cumplen las condiciones
                if self.should_delete_line(&table, &delete_query, &line)? {
                    write_line = false;
                }
            }

            // Si la línea no debe ser eliminada, escribirla en el archivo temporal
            if write_line {
                writeln!(temp_file, "{}", columns.join(","))?;
                if let Some(&(idx, _)) = clustering_key_order.first() {
                    if let Some(key) = columns.get(idx) {
                        let entry = (
                            key.clone(),
                            (current_byte_offset, current_byte_offset + line_length),
                        );
                        index_map.push(entry);
                    }
                }
                current_byte_offset += line_length + 1;
            }
        }

        // Ordenar el archivo de índices según el orden de las clustering columns
        for (_, order) in &clustering_key_order {
            if order == "ASC" {
                index_map.sort_by(|a, b| a.0.cmp(&b.0));
            } else {
                index_map.sort_by(|a, b| b.0.cmp(&a.0));
            }
        }

        // Escribir el archivo de índices actualizado
        for (key, (start_byte, end_byte)) in index_map {
            writeln!(temp_index_file, "{},{},{}", key, start_byte, end_byte)
                .map_err(|_| StorageEngineError::FileWriteFailed)?;
        }

        // Reemplazar los archivos originales con los temporales
        fs::rename(&temp_file_path, &file_path)
            .map_err(|_| StorageEngineError::FileReplacementFailed)?;
        fs::rename(&temp_index_file_path, &index_file_path)
            .map_err(|_| StorageEngineError::FileReplacementFailed)?;

        Ok(())
    }

    /// Verifica si una línea cumple las condiciones para ser eliminada
    fn should_delete_line(
        &self,
        table: &Table,
        delete_query: &Delete,
        line: &str,
    ) -> Result<bool, StorageEngineError> {
        let columns: Vec<String> = line.split(',').map(|s| s.trim().to_string()).collect();
        let column_value_map = self.create_column_value_map(table, &columns, false);

        let columns = table.get_columns();

        // Verificar la cláusula `WHERE`
        if let Some(where_clause) = &delete_query.where_clause {
            if where_clause
                .condition
                .execute(&column_value_map, columns.clone())
                .unwrap_or(false)
            {
                // Si la cláusula `IF` está presente, comprobarla
                if let Some(if_clause) = &delete_query.if_clause {
                    if !if_clause
                        .condition
                        .execute(&column_value_map, columns.clone())
                        .unwrap_or(false)
                    {
                        // Si la cláusula `IF` no coincide, no eliminar
                        return Ok(false);
                    }
                }
                // Si `WHERE` se cumple y (si existe) `IF` también, eliminar
                return Ok(true);
            } else {
                // Si `WHERE` no se cumple, no eliminar
                return Ok(false);
            }
        } else {
            // Si falta la cláusula `WHERE`, devolver un error
            return Err(StorageEngineError::InvalidQuery);
        }
    }
}
