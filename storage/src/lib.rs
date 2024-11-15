use query_creator::clauses::{delete_cql::Delete, select_cql::Select, update_cql::Update};
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;

pub mod errors;

use errors::StorageEngineError;

pub struct StorageEngine {
    root: PathBuf,
    ip: String,
}

impl StorageEngine {
    /// Creates a new instance of StorageEngine with the specified root path.
    pub fn new(root: PathBuf, ip: String) -> Self {
        Self { root, ip }
    }
    /// Creates a keyspace in the storage location.
    pub fn create_keyspace(&self, name: &str) -> Result<(), StorageEngineError> {
        // Generate the folder name where the keyspace will be stored
        let ip_str = self.ip.replace(".", "_");
        let keyspace_folder = format!("keyspaces_of_{}", ip_str);
        let keyspace_path = self.root.join(&keyspace_folder).join(name);

        // Create the keyspace folder if it doesn't exist
        if let Err(_) = std::fs::create_dir_all(&keyspace_path) {
            return Err(StorageEngineError::DirectoryCreationFailed);
        }

        // Create the replication folder inside the keyspace folder
        let replication_path = keyspace_path.join("replication");
        if let Err(_) = std::fs::create_dir_all(&replication_path) {
            return Err(StorageEngineError::DirectoryCreationFailed);
        }

        Ok(())
    }

    /// Drops a keyspace from the storage location.
    pub fn drop_keyspace(&self, name: &str, ip: &str) -> Result<(), StorageEngineError> {
        // Generate the folder name where the keyspace is stored
        let ip_str = ip.replace(".", "_");
        let keyspace_folder = format!("keyspaces_of_{}", ip_str);
        let keyspace_path = self.root.join(&keyspace_folder).join(name);

        // Remove the keyspace folder
        if let Err(_) = std::fs::remove_dir_all(&keyspace_path) {
            return Err(StorageEngineError::FileDeletionFailed);
        }

        Ok(())
    }

    /// Creates a table in `keyspace` with the name `table`.
    pub fn create_table(
        &self,
        keyspace: &str,
        table: &str,
        columns: Vec<&str>,
    ) -> Result<(), StorageEngineError> {
        // Generate the folder name where the keyspace will be stored
        let ip_str = self.ip.replace(".", "_");
        let keyspace_folder = format!("keyspaces_of_{}", ip_str);
        let keyspace_path = self.root.join(&keyspace_folder).join(keyspace);
        let replication_path = keyspace_path.join("replication");

        let primary_file_path = keyspace_path.join(format!("{}.csv", table));
        let replication_file_path = replication_path.join(format!("{}.csv", table));

        // Create the keyspace and replication folders if they don't exist
        std::fs::create_dir_all(&keyspace_path)
            .map_err(|_| StorageEngineError::DirectoryCreationFailed)?;
        std::fs::create_dir_all(&replication_path)
            .map_err(|_| StorageEngineError::DirectoryCreationFailed)?;

        // Create the file in the primary folder and write the columns as the header
        let mut primary_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&primary_file_path)
            .map_err(|_| StorageEngineError::FileWriteFailed)?;

        let header: Vec<String> = columns.iter().map(|col| col.to_string()).collect();
        writeln!(primary_file, "{}", header.join(","))
            .map_err(|_| StorageEngineError::FileWriteFailed)?;

        // Create the same file in the replication folder and write the columns as the header
        let mut replication_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&replication_file_path)
            .map_err(|_| StorageEngineError::FileWriteFailed)?;

        writeln!(replication_file, "{}", header.join(","))
            .map_err(|_| StorageEngineError::FileWriteFailed)?;

        // Create the index file in the primary folder
        let index_file_path = keyspace_path.join(format!("{}_index.csv", table));
        let mut index_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&index_file_path)
            .map_err(|_| StorageEngineError::FileWriteFailed)?;

        writeln!(index_file, "ClusteringColumns,StartByte,EndByte")
            .map_err(|_| StorageEngineError::FileWriteFailed)?;

        // Create the index file in the replication folder
        let replication_index_file_path = replication_path.join(format!("{}_index.csv", table));
        let mut replication_index_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&replication_index_file_path)
            .map_err(|_| StorageEngineError::FileWriteFailed)?;

        writeln!(
            replication_index_file,
            "ClusteringColumns,StartByte,EndByte"
        )
        .map_err(|_| StorageEngineError::FileWriteFailed)?;

        Ok(())
    }

    // Drops a table from the storage location.
    pub fn drop_table(&self, keyspace: &str, table: &str) -> Result<(), StorageEngineError> {
        let ip_str = self.ip.replace(".", "_");
        let keyspace_folder = format!("keyspaces_of_{}", ip_str);
        let keyspace_path = self.root.join(&keyspace_folder).join(keyspace);
        let replication_path = keyspace_path.join("replication");

        // Paths for primary and replication files and index files
        let primary_file_path = keyspace_path.join(format!("{}.csv", table));
        let replication_file_path = replication_path.join(format!("{}.csv", table));
        let primary_index_path = keyspace_path.join(format!("{}_index.csv", table));
        let replication_index_path = replication_path.join(format!("{}_index.csv", table));

        // Remove the primary and replication files
        if let Err(_) = std::fs::remove_file(&primary_file_path) {
            return Err(StorageEngineError::FileDeletionFailed);
        }

        if let Err(_) = std::fs::remove_file(&replication_file_path) {
            return Err(StorageEngineError::FileDeletionFailed);
        }

        // Remove the primary and replication index files
        if let Err(_) = std::fs::remove_file(&primary_index_path) {
            return Err(StorageEngineError::FileDeletionFailed);
        }

        if let Err(_) = std::fs::remove_file(&replication_index_path) {
            return Err(StorageEngineError::FileDeletionFailed);
        }

        Ok(())
    }

    fn get_keyspace_path(&self, keyspace: &str) -> PathBuf {
        let ip_str = self.ip.replace(".", "_");
        let keyspace_folder = format!("keyspaces_of_{}", ip_str);
        self.root.join(&keyspace_folder).join(keyspace)
    }

    pub fn add_column_to_table(
        &self,
        keyspace: &str,
        table: &str,
        column: &str,
    ) -> Result<(), StorageEngineError> {
        let keyspace_path = self.get_keyspace_path(keyspace);
        let file_path = keyspace_path.join(format!("{}.csv", table));
        let replica_path = keyspace_path
            .join("replication")
            .join(format!("{}.csv", table));

        Self::add_column_to_file(file_path.to_str().unwrap(), column)?;
        Self::add_column_to_file(replica_path.to_str().unwrap(), column)?;

        Ok(())
    }

    pub fn remove_column_from_table(
        &self,
        keyspace: &str,
        table: &str,
        column: &str,
    ) -> Result<(), StorageEngineError> {
        let keyspace_path = self.get_keyspace_path(keyspace);
        let file_path = keyspace_path.join(format!("{}.csv", table));
        let replica_path = keyspace_path
            .join("replication")
            .join(format!("{}.csv", table));

        Self::remove_column_from_file(file_path.to_str().unwrap(), column)?;
        Self::remove_column_from_file(replica_path.to_str().unwrap(), column)?;

        Ok(())
    }

    pub fn rename_column_from_table(
        &self,
        keyspace: &str,
        table: &str,
        column: &str,
        new_column: &str,
    ) -> Result<(), StorageEngineError> {
        let keyspace_path = self.get_keyspace_path(keyspace);
        let file_path = keyspace_path.join(format!("{}.csv", table));
        let replica_path = keyspace_path
            .join("replication")
            .join(format!("{}.csv", table));

        Self::rename_column_in_file(file_path.to_str().unwrap(), column, new_column)?;
        Self::rename_column_in_file(replica_path.to_str().unwrap(), column, new_column)?;

        Ok(())
    }

    pub(crate) fn add_column_to_file(
        file_path: &str,
        column_name: &str,
    ) -> Result<(), StorageEngineError> {
        let temp_path = format!("{}.temp", file_path);
        let mut temp_file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&temp_path)?;

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

        fs::rename(temp_path, file_path).map_err(|_| StorageEngineError::IoError)
    }

    pub(crate) fn remove_column_from_file(
        file_path: &str,
        column_name: &str,
    ) -> Result<(), StorageEngineError> {
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
                col_index = cells.iter().position(|&col| col == column_name);
                if col_index.is_none() {
                    return Err(StorageEngineError::UnsupportedOperation);
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

        fs::rename(temp_path, file_path).map_err(|_| StorageEngineError::IoError)
    }

    pub(crate) fn rename_column_in_file(
        file_path: &str,
        old_name: &str,
        new_name: &str,
    ) -> Result<(), StorageEngineError> {
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

        fs::rename(temp_path, file_path).map_err(|_| StorageEngineError::IoError)
    }

    /// Inserts the values `values` into `table` in `keyspace`.
    pub fn insert(
        &self,
        keyspace: &str,
        table: &str,
        values: Vec<&str>,
    ) -> Result<(), StorageEngineError> {
        Ok(())
    }

    /// Deletes records according to the query `query`.
    pub fn delete(&self, query: Delete, is_replication: bool) -> Result<(), StorageEngineError> {
        Ok(())
    }

    /// Updates records according to the query `query`.
    pub fn update(&self, query: Update, is_replication: bool) -> Result<(), StorageEngineError> {
        Ok(())
    }

    /// Executes a `SELECT` query. The first element of the returned vector is the header.
    pub fn select(
        &self,
        query: Select,
        is_replication: bool,
    ) -> Result<Vec<String>, StorageEngineError> {
        Ok(vec![])
    }
}
