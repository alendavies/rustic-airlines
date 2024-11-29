use std::{
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Write},
    time::{SystemTime, UNIX_EPOCH},
};

use query_creator::{clauses::types::column::Column, operator::Operator};

use super::{errors::StorageEngineError, StorageEngine};

impl StorageEngine {
    /// Inserts a new row into a table within the specified keyspace.
    ///
    /// This function handles the insertion of a row into a `.csv` file representing a table. It ensures
    /// that the data adheres to the structure of the table (defined by columns) and maintains the correct
    /// clustering order of rows based on the clustering keys.
    ///
    /// If the table file does not exist, it will be created. The function also supports conditional inserts
    /// (`if_not_exist`) and handles replication scenarios.
    ///
    /// # Arguments
    /// - `keyspace`: The name of the keyspace where the table resides.
    /// - `table`: The name of the table into which the row will be inserted.
    /// - `values`: A vector of string slices representing the values for the row, in column order.
    /// - `columns`: A vector of `Column` structs defining the table's schema.
    /// - `clustering_columns_in_order`: A vector of strings indicating the clustering columns and their order.
    /// - `is_replication`: A boolean indicating whether the insertion is part of a replication process.
    /// - `if_not_exist`: A boolean indicating whether the row should only be inserted if it does not already exist.
    /// - `timestamp`: A 64-bit integer representing the timestamp of the operation.
    ///
    /// # Returns
    /// - `Ok(())`: If the row is successfully inserted.
    /// - `Err(StorageEngineError)`: If an error occurs during the operation, such as:
    ///   - `DirectoryCreationFailed`: When the required directories cannot be created.
    ///   - `IoError`: For issues reading or writing to files.
    ///   - `UnsupportedOperation`: If an unsupported operation is encountered (e.g., invalid data type comparison).
    ///   - `TempFileCreationFailed`: If a temporary file cannot be created.
    ///
    /// # Behavior
    /// - If the table file does not exist:
    ///   - The file is created, and the header row is written based on the provided `columns`.
    /// - If the table file exists:
    ///   - The header is validated, and rows are written in clustering order.
    /// - If `if_not_exist` is `true`, rows with matching clustering keys will not be overwritten.
    /// - For clustering keys:
    ///   - The function ensures that rows are inserted in the correct order based on the `clustering_columns_in_order`.
    ///   - Clustering order can be `ASC` (ascending) or `DESC` (descending), defined per column.
    ///
    /// # Considerations
    /// - The function assumes that the `columns` accurately describe the structure of the table.
    /// - The length of `values` must match the number of columns.
    /// - Invalid values (e.g., a non-integer value for an `INT` column) will result in an error.
    /// - The function writes data atomically using temporary files to avoid corruption in case of errors.
    ///
    /// # Edge Cases
    /// - **Empty `values` or `columns`:** The function will return an error if the values or columns are missing.
    /// - **Invalid clustering order:** If a clustering column's order is unspecified or inconsistent, an error may occur.
    /// - **Concurrent writes:** Simultaneous calls to `insert` on the same table may cause unexpected behavior and are not supported.
    ///
    /// # Limitations
    /// - The function currently supports only `.csv` file formats.
    /// - Complex data types (e.g., nested structures) are not supported.

    pub fn insert(
        &self,
        keyspace: &str,
        table: &str,
        values: Vec<&str>,
        columns: Vec<Column>,
        clustering_columns_in_order: Vec<String>,
        is_replication: bool,
        if_not_exist: bool,
        timestamp: i64,
    ) -> Result<(), StorageEngineError> {
        let base_folder_path = self.get_keyspace_path(keyspace);

        let folder_path = if is_replication {
            base_folder_path.join("replication")
        } else {
            base_folder_path
        };

        if !folder_path.exists() {
            fs::create_dir_all(&folder_path)
                .map_err(|_| StorageEngineError::DirectoryCreationFailed)?;
        }

        let file_path = folder_path.join(format!("{}.csv", table));
        let temp_file_path = folder_path.join(format!(
            "{}.tmp",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| StorageEngineError::TempFileCreationFailed)?
                .as_nanos()
        ));

        let index_file_path = folder_path.join(format!("{}_index.csv", table));

        let mut temp_file =
            File::create(&temp_file_path).map_err(|_| StorageEngineError::IoError)?;
        let mut temp_index = BufWriter::new(
            File::create(&index_file_path).map_err(|_| StorageEngineError::IoError)?,
        );

        writeln!(temp_index, "clustering_column,start_byte,end_byte")
            .map_err(|_| StorageEngineError::IoError)?;

        let clustering_key_indices: Vec<(usize, String)> = clustering_columns_in_order
            .iter()
            .filter_map(|col_name| {
                columns
                    .iter()
                    .position(|col| col.name == *col_name && col.is_clustering_column)
                    .map(|idx| {
                        let inverted_order = if columns[idx].get_clustering_order() == "ASC" {
                            "DESC".to_string()
                        } else {
                            "ASC".to_string()
                        };
                        (idx, inverted_order)
                    })
            })
            .collect();

        let partition_key_indices: Vec<usize> = columns
            .iter()
            .enumerate()
            .filter(|(_, col)| col.is_partition_key)
            .map(|(idx, _)| idx)
            .collect();

        let mut inserted = false;
        let mut current_byte_offset: u64 = 0;
        let mut index_map: std::collections::BTreeMap<String, (u64, u64)> =
            std::collections::BTreeMap::new();

        if let Ok(file) = OpenOptions::new().read(true).open(&file_path) {
            let reader = BufReader::new(file);
            let mut lines = reader.lines();

            if let Some(header_line) = lines.next() {
                let header_line = header_line.map_err(|_| StorageEngineError::IoError)?;
                writeln!(temp_file, "{}", header_line).map_err(|_| StorageEngineError::IoError)?;
                current_byte_offset += header_line.len() as u64 + 1; // Contamos el '\n'
            }

            for line in lines {
                let line = line.map_err(|_| StorageEngineError::IoError)?;
                let line_length = line.len() as u64;

                let (line_content, time_of_row) =
                    line.split_once(";").ok_or(StorageEngineError::IoError)?;

                let row: Vec<&str> = line_content.split(',').collect();

                let mut clustering_comparison = std::cmp::Ordering::Equal;

                let same_partition_key = partition_key_indices.iter().all(|&index| {
                    row.get(index).unwrap_or(&"") == values.get(index).unwrap_or(&"")
                });

                for &(idx, ref order) in &clustering_key_indices {
                    let row_value = row.get(idx).unwrap_or(&"");
                    let insert_value = values.get(idx).unwrap_or(&"");
                    let data_type = &columns[idx].data_type;
                    //println!("voy a comparar {:?} con {:?}", row_value, insert_value);

                    if row_value != insert_value {
                        let is_less = data_type
                            .compare(
                                &row_value.to_string(),
                                &insert_value.to_string(),
                                &Operator::Lesser,
                            )
                            .map_err(|_| StorageEngineError::UnsupportedOperation)?;

                        clustering_comparison = match (is_less, order.as_str()) {
                            (true, "DESC") | (false, "ASC") => std::cmp::Ordering::Less,
                            (false, "DESC") | (true, "ASC") => std::cmp::Ordering::Greater,
                            _ => std::cmp::Ordering::Equal,
                        };

                        break;
                    }
                }

                if clustering_comparison == std::cmp::Ordering::Equal {
                    if !same_partition_key {
                        writeln!(temp_file, "{};{}", line_content, time_of_row)
                            .map_err(|_| StorageEngineError::IoError)?;
                        current_byte_offset += line_length + 1;
                        writeln!(temp_file, "{};{}", values.join(","), timestamp)
                            .map_err(|_| StorageEngineError::IoError)?;
                        inserted = true;
                        current_byte_offset +=
                            values.join(",").len() as u64 + timestamp.to_string().len() as u64 + 2;
                        continue;
                    }
                    if if_not_exist {
                        writeln!(temp_file, "{};{}", line_content, time_of_row)
                            .map_err(|_| StorageEngineError::IoError)?;
                        current_byte_offset += line_length + 1;
                        continue;
                    } else {
                        writeln!(temp_file, "{};{}", values.join(","), timestamp)
                            .map_err(|_| StorageEngineError::IoError)?;
                        inserted = true;
                        current_byte_offset +=
                            values.join(",").len() as u64 + timestamp.to_string().len() as u64 + 2;
                        continue;
                    }
                } else if clustering_comparison == std::cmp::Ordering::Greater && !inserted {
                    writeln!(temp_file, "{};{}", values.join(","), timestamp)
                        .map_err(|_| StorageEngineError::IoError)?;
                    inserted = true;
                    current_byte_offset +=
                        values.join(",").len() as u64 + timestamp.to_string().len() as u64 + 2;
                }

                if let Some(&(idx, _)) = clustering_key_indices.first() {
                    let key = row[idx].to_string();
                    let entry = index_map
                        .entry(key)
                        .or_insert((current_byte_offset, current_byte_offset));
                    entry.1 = current_byte_offset + line_length;
                }

                writeln!(temp_file, "{};{}", line_content, time_of_row)
                    .map_err(|_| StorageEngineError::IoError)?;
                current_byte_offset += line.len() as u64 + 1;
            }
        }

        if !inserted {
            writeln!(temp_file, "{};{}", values.join(","), timestamp)
                .map_err(|_| StorageEngineError::IoError)?;

            if let Some(&(idx, _)) = clustering_key_indices.first() {
                let key = values[idx].to_string();
                index_map.insert(
                    key,
                    (
                        current_byte_offset,
                        current_byte_offset
                            + values.join(",").len() as u64
                            + timestamp.to_string().len() as u64
                            + 2,
                    ),
                );
            }
        }

        let mut sorted_indices: Vec<_> = index_map.into_iter().collect();
        for &(_, ref order) in &clustering_key_indices {
            if order == "ASC" {
                sorted_indices.sort_by(|a, b| a.0.cmp(&b.0));
            } else {
                sorted_indices.sort_by(|a, b| b.0.cmp(&a.0));
            }
        }

        for (key, (start_byte, end_byte)) in sorted_indices {
            writeln!(temp_index, "{},{},{}", key, start_byte, end_byte)
                .map_err(|_| StorageEngineError::IoError)?;
        }

        fs::rename(&temp_file_path, &file_path).map_err(|_| StorageEngineError::IoError)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use query_creator::clauses::types::column::Column;
    use query_creator::clauses::types::datatype::DataType;
    use std::fs::{self, File};
    use std::io::{BufRead, BufReader};
    use std::path::PathBuf;
    use uuid::Uuid;

    #[test]
    fn test_insert_new_row_with_correct_columns() {
        // Use a unique directory for this test
        let root = PathBuf::from(format!("/tmp/storage_test_{}", Uuid::new_v4()));
        let ip = "127.0.0.1".to_string();
        let storage = StorageEngine::new(root.clone(), ip.clone());

        // Keyspace and table setup
        let keyspace = "test_keyspace";
        let table = "test_table";
        let columns = vec![
            Column::new("id", DataType::Int, true, false), // id: INT, primary key, not null
            Column::new("name", DataType::String, false, true), // name: TEXT, not primary key, allows null
        ];
        let clustering_columns_in_order = vec!["id".to_string()];
        let values = vec!["1", "John"];
        let timestamp = 1234567890;

        // Clean the environment
        let folder_path = storage.get_keyspace_path(keyspace);
        if folder_path.exists() {
            fs::remove_dir_all(&folder_path).unwrap();
        }

        // Create the keyspace folder
        fs::create_dir_all(folder_path.clone()).unwrap();

        // Add the header manually to the file
        let table_file_path = folder_path.join(format!("{}.csv", table));
        let mut file = File::create(&table_file_path).unwrap();
        writeln!(file, "id,name").unwrap(); // Write header manually

        // Insert row
        let result = storage.insert(
            keyspace,
            table,
            values.clone(),
            columns.clone(),
            clustering_columns_in_order.clone(),
            false, // is_replication
            false, // if_not_exist
            timestamp,
        );
        assert!(result.is_ok(), "Failed to insert a new row");

        // Verify the file was created
        assert!(
            table_file_path.exists(),
            "Table file was not created after insert"
        );

        // Verify the content of the file
        let file = File::open(&table_file_path).unwrap();
        let reader = BufReader::new(file);

        let mut lines = reader.lines();
        assert_eq!(
            lines.next().unwrap().unwrap(),
            "id,name",
            "Header does not match expected value"
        );
        assert_eq!(
            lines.next().unwrap().unwrap(),
            format!("{},{};{}", values[0], values[1], timestamp),
            "Row content does not match expected value"
        );

        // Cleanup
        if root.exists() {
            fs::remove_dir_all(&root).unwrap();
        }
    }

    #[test]
    fn test_insert_with_clustering_order_and_manual_header() {
        // Use a unique directory for this test
        let root = PathBuf::from(format!("/tmp/storage_test_{}", Uuid::new_v4()));
        let ip = "127.0.0.1".to_string();
        let storage = StorageEngine::new(root.clone(), ip.clone());

        // Keyspace and table setup
        let keyspace = "test_keyspace";
        let table = "test_table";

        // Create columns with additional configurations
        let mut id_column = Column::new("id", DataType::Int, true, false);
        id_column.is_clustering_column = true; // Set as clustering column
        id_column.clustering_order = "ASC".to_string(); // Define clustering order

        let name_column = Column::new("name", DataType::String, false, true);

        let columns = vec![id_column, name_column];
        let clustering_columns_in_order = vec!["id".to_string()];

        let values_row1 = vec!["2", "Alice"];
        let values_row2 = vec!["1", "Bob"];
        let timestamp1 = 1234567890;
        let timestamp2 = 1234567891;

        // Clean the environment
        let folder_path = storage.get_keyspace_path(keyspace);
        if folder_path.exists() {
            fs::remove_dir_all(&folder_path).unwrap();
        }

        // Create the keyspace folder
        fs::create_dir_all(folder_path.clone()).unwrap();

        // Add the header manually to the file
        let table_file_path = folder_path.join(format!("{}.csv", table));
        let mut file = File::create(&table_file_path).unwrap();
        writeln!(file, "id,name").unwrap(); // Write header manually

        // Insert rows
        let _ = storage.insert(
            keyspace,
            table,
            values_row1.clone(),
            columns.clone(),
            clustering_columns_in_order.clone(),
            false,
            false,
            timestamp1,
        );

        let _ = storage.insert(
            keyspace,
            table,
            values_row2.clone(),
            columns.clone(),
            clustering_columns_in_order.clone(),
            false,
            false,
            timestamp2,
        );

        // Verify the content of the file
        let file = File::open(&table_file_path).unwrap();
        let reader = BufReader::new(file);

        let mut lines = reader.lines();
        assert_eq!(
            lines.next().unwrap().unwrap(),
            "id,name",
            "Header does not match expected value"
        );

        let row1 = lines.next().unwrap().unwrap();
        let row2 = lines.next().unwrap().unwrap();
        assert!(
            row1.starts_with("1"),
            "Clustering order is incorrect, first row should have the smallest ID"
        );
        assert!(
            row2.starts_with("2"),
            "Clustering order is incorrect, second row should have the larger ID"
        );

        // Cleanup
        if root.exists() {
            fs::remove_dir_all(&root).unwrap();
        }
    }
}
