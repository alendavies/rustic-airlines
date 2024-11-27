use std::{
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Write},
    time::{SystemTime, UNIX_EPOCH},
};

use query_creator::{clauses::types::column::Column, operator::Operator};

use super::{errors::StorageEngineError, StorageEngine};

impl StorageEngine {
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

                for &(idx, ref order) in &clustering_key_indices {
                    let row_value = row.get(idx).unwrap_or(&"");
                    let insert_value = values.get(idx).unwrap_or(&"");
                    let data_type = &columns[idx].data_type;

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
