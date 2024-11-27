use std::fs::{self, File};
use std::io::{BufRead, Write};
use std::path::PathBuf;

pub mod data_redistribution;
pub mod delete;
pub mod errors;
pub mod insert;
pub mod keyspace_operations;
pub mod select;
pub mod table_operations;
pub mod update;
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

    /// Resets the keyspace folders.
    ///
    /// If the folder for the keyspaces exists, it is deleted and recreated.
    /// If it doesn't exist, it is created.
    ///
    /// Returns `Ok(())` on success or a `StorageEngineError` on failure.
    pub fn reset_folders(&self) -> Result<(), StorageEngineError> {
        let ip_str = self.ip.replace(".", "_");
        let keyspace_folder = format!("keyspaces_of_{}", ip_str);
        let keyspace_path = self.root.join(&keyspace_folder);

        // Check if the folder exists and delete it if it does
        if keyspace_path.exists() {
            fs::remove_dir_all(&keyspace_path)
                .map_err(|_| StorageEngineError::DirectoryCreationFailed)?;
        }

        // Create the folder
        fs::create_dir_all(&keyspace_path)
            .map_err(|_| StorageEngineError::DirectoryCreationFailed)?;

        Ok(())
    }

    fn get_keyspace_path(&self, keyspace: &str) -> PathBuf {
        let ip_str = self.ip.replace(".", "_");
        let keyspace_folder = format!("keyspaces_of_{}", ip_str);
        self.root.join(&keyspace_folder).join(keyspace)
    }
}
