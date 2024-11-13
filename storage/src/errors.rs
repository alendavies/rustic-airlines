/// Enumeration of possible errors that can be returned by a StorageEngine.
#[derive(Debug)]
pub enum StorageEngineError {
    /// Error related to input/output operations.
    IoError,

    /// Error when creating a temporary file fails.
    TempFileCreationFailed,

    /// Error when attempting to write to a file fails.
    FileWriteFailed,

    /// Error when attempting to read from a file fails.
    FileReadFailed,

    /// Error when attempting to delete a file fails.
    FileDeletionFailed,

    /// Error when a directory creation operation fails.
    DirectoryCreationFailed,

    /// General error for unsupported operations.
    UnsupportedOperation,
}

impl std::fmt::Display for StorageEngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageEngineError::IoError => write!(f, "I/O operation failed."),
            StorageEngineError::TempFileCreationFailed => {
                write!(f, "Failed to create a temporary file.")
            }
            StorageEngineError::FileWriteFailed => write!(f, "Failed to write to the file."),
            StorageEngineError::FileReadFailed => write!(f, "Failed to read from the file."),
            StorageEngineError::FileDeletionFailed => write!(f, "Failed to delete the file."),
            StorageEngineError::DirectoryCreationFailed => {
                write!(f, "Failed to create the directory.")
            }
            StorageEngineError::UnsupportedOperation => write!(f, "This operation is unsupported."),
        }
    }
}

impl std::error::Error for StorageEngineError {}

impl From<std::io::Error> for StorageEngineError {
    fn from(_: std::io::Error) -> Self {
        StorageEngineError::IoError
    }
}
