use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Write},
    time::{SystemTime, UNIX_EPOCH},
};

use query_creator::clauses::update_cql::Update;

use crate::table::Table;

use super::{errors::StorageEngineError, StorageEngine};

impl StorageEngine {
    /// Performs the update operation locally on this node
    pub fn update(
        &self,
        update_query: Update,
        table: Table,
        is_replication: bool,
        keyspace: &str,
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

        // Rutas para el archivo original y el archivo temporal
        let file_path = folder_path.join(format!("{}.csv", table_name));
        let temp_file_path = folder_path.join(format!(
            "{}.tmp",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| StorageEngineError::TempFileCreationFailed)?
                .as_nanos()
        ));

        // Abrir el archivo original, si existe, o crear un nuevo archivo vacío
        let file = if file_path.exists() {
            OpenOptions::new()
                .read(true)
                .open(&file_path)
                .map_err(|_| StorageEngineError::DirectoryCreationFailed)?
        } else {
            File::create(&file_path).map_err(|_| StorageEngineError::DirectoryCreationFailed)?
        };
        let mut reader = BufReader::new(file);

        // Crear el archivo temporal
        let mut temp_file = File::create(&temp_file_path)
            .map_err(|_| StorageEngineError::TempFileCreationFailed)?;

        // Escribir el encabezado en el archivo temporal
        self.write_header(&mut reader, &mut temp_file)?;

        let mut found_match = false;

        // Iterar sobre las líneas del archivo original y aplicar la actualización
        for (i, line) in reader.lines().enumerate() {
            if i == 0 {
                continue;
            }
            let line = line?;
            found_match |=
                self.update_or_write_line(&table, &update_query, &line, &mut temp_file)?;
        }

        // Reemplazar el archivo original con el actualizado
        fs::rename(&temp_file_path, &file_path)
            .map_err(|_| StorageEngineError::DirectoryCreationFailed)?;

        // Si no se encontró ninguna fila que coincida, agregar una nueva
        if !found_match {
            self.add_new_row_in_update(&table, &update_query, keyspace, is_replication)?;
        }
        Ok(())
    }

    /// Crea un mapa de valores de columna para una fila dada.
    pub fn create_column_value_map(
        &self,
        table: &Table,
        columns: &[String],
        only_partitioner_key: bool,
    ) -> HashMap<String, String> {
        let mut column_value_map: HashMap<String, String> = HashMap::new();
        for (i, column) in table.get_columns().iter().enumerate() {
            if let Some(value) = columns.get(i) {
                if column.is_partition_key || column.is_clustering_column || !only_partitioner_key {
                    column_value_map.insert(column.name.clone(), value.clone());
                }
            }
        }

        column_value_map
    }

    fn update_or_write_line(
        &self,
        table: &Table,
        update_query: &Update,
        line: &str,
        temp_file: &mut File,
    ) -> Result<bool, StorageEngineError> {
        let mut columns: Vec<String> = line.split(',').map(|s| s.trim().to_string()).collect();
        let column_value_map = self.create_column_value_map(table, &columns, false);

        let columns_ = table.get_columns();

        if let Some(where_clause) = &update_query.where_clause {
            if where_clause
                .condition
                .execute(&column_value_map, columns_.clone())
                .unwrap_or(false)
            {
                if let Some(if_clause) = &update_query.if_clause {
                    if !if_clause
                        .condition
                        .execute(&column_value_map, columns_.clone())
                        .unwrap_or(false)
                    {
                        writeln!(temp_file, "{}", line)?;
                        return Ok(true);
                    }
                }

                for (column, new_value) in update_query.clone().set_clause.get_pairs() {
                    if table
                        .is_primary_key(&column)
                        .map_err(|_| StorageEngineError::ColumnNotFound)?
                    {
                        return Err(StorageEngineError::PrimaryKeyModificationNotAllowed);
                    }
                    let index = table
                        .get_column_index(&column)
                        .ok_or(StorageEngineError::ColumnNotFound)?;

                    columns[index] = new_value.clone();
                }
                writeln!(temp_file, "{}", columns.join(","))?;
                return Ok(true);
            } else {
                writeln!(temp_file, "{}", line)?;
                if let Some(if_clause) = &update_query.if_clause {
                    if if_clause
                        .condition
                        .execute(&column_value_map, columns_.clone())
                        .unwrap_or(false)
                    {
                        return Ok(false);
                    } else {
                        return Ok(true);
                    }
                } else {
                    return Ok(false);
                }
            }
        } else {
            return Err(StorageEngineError::MissingWhereClause);
        }
    }

    fn add_new_row_in_update(
        &self,
        table: &Table,
        update_query: &Update,
        keyspace: &str,
        is_replication: bool,
    ) -> Result<(), StorageEngineError> {
        let mut new_row: Vec<String> = vec!["".to_string(); table.get_columns().len()];

        let primary_keys = table
            .get_partition_keys()
            .map_err(|_| StorageEngineError::PartitionKeyMismatch)?;
        let primary_key_values = update_query
            .where_clause
            .as_ref()
            .map(|where_clause| {
                where_clause.get_value_partitioner_key_condition(primary_keys.clone())
            })
            .ok_or(StorageEngineError::MissingWhereClause)?
            .map_err(|_| StorageEngineError::PartitionKeyMismatch)?;

        if primary_key_values.len() != primary_keys.len() {
            return Err(StorageEngineError::PartitionKeyMismatch);
        }

        for (i, primary_key) in primary_keys.iter().enumerate() {
            let primary_key_index = table
                .get_column_index(primary_key)
                .ok_or(StorageEngineError::ColumnNotFound)?;

            new_row[primary_key_index] = primary_key_values[i].clone();
        }

        let clustering_keys = table
            .get_clustering_columns()
            .map_err(|_| StorageEngineError::ClusteringKeyMismatch)?;

        let clustering_key_values = update_query
            .where_clause
            .as_ref()
            .map(|where_clause| {
                where_clause.get_value_clustering_column_condition(clustering_keys.clone())
            })
            .ok_or(StorageEngineError::MissingWhereClause)?
            .map_err(|_| StorageEngineError::ClusteringKeyMismatch)?;

        for (i, clustering_key) in clustering_keys.iter().enumerate() {
            let clustering_key_index = table
                .get_column_index(clustering_key)
                .ok_or(StorageEngineError::ColumnNotFound)?;

            new_row[clustering_key_index] = clustering_key_values[i].clone();
        }

        for (column, new_value) in update_query.set_clause.get_pairs() {
            if table
                .is_primary_key(&column)
                .map_err(|_| StorageEngineError::ColumnNotFound)?
            {
                return Err(StorageEngineError::PrimaryKeyModificationNotAllowed);
            }
            let index = table
                .get_column_index(&column)
                .ok_or(StorageEngineError::ColumnNotFound)?;

            new_row[index] = new_value.clone();
        }

        let values: Vec<&str> = new_row.iter().map(|v| v.as_str()).collect();

        self.insert(
            keyspace,
            &table.get_name(),
            values,
            table.get_columns(),
            table.get_clustering_column_in_order(),
            is_replication,
            true,
        )?;

        Ok(())
    }
}
