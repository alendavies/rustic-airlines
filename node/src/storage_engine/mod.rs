use std::fs::{self, File};
use std::io::{BufRead, Write};
use std::path::PathBuf;

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

    fn write_header<R: BufRead>(
        &self,
        reader: &mut R,
        temp_file: &mut File,
    ) -> Result<(), StorageEngineError> {
        if let Some(header_line) = reader.lines().next() {
            writeln!(temp_file, "{}", header_line?)
                .map_err(|_| StorageEngineError::FileWriteFailed)?;
        }
        Ok(())
    }
}
