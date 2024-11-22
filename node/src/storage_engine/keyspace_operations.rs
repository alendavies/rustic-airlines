use super::{errors::StorageEngineError, StorageEngine};

impl StorageEngine {
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
}
