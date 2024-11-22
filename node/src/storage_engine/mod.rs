use std::fs::File;
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
