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

    /// Error when a file is not found.
    FileNotFound,

    /// Error when replacing a file fails.
    FileReplacementFailed,

    /// Error due to an invalid query.
    InvalidQuery,

    /// Error when attempting to update or modify a primary key.
    PrimaryKeyModificationNotAllowed,

    /// Error when a required column is missing.
    ColumnNotFound,

    /// Error when the WHERE clause is missing or invalid.
    MissingWhereClause,

    /// Error when partition key values are incomplete or mismatched.
    PartitionKeyMismatch,

    /// Error when clustering key values are incomplete or mismatched.
    ClusteringKeyMismatch,

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
            StorageEngineError::FileNotFound => write!(f, "File not found."),
            StorageEngineError::FileReplacementFailed => {
                write!(f, "Failed to replace the original file.")
            }
            StorageEngineError::InvalidQuery => write!(f, "The query is invalid."),
            StorageEngineError::PrimaryKeyModificationNotAllowed => {
                write!(f, "Modification of primary keys is not allowed.")
            }
            StorageEngineError::ColumnNotFound => write!(f, "Specified column not found."),
            StorageEngineError::MissingWhereClause => {
                write!(f, "The WHERE clause is missing or invalid.")
            }
            StorageEngineError::PartitionKeyMismatch => {
                write!(f, "Partition key values are incomplete or mismatched.")
            }
            StorageEngineError::ClusteringKeyMismatch => {
                write!(f, "Clustering key values are incomplete or mismatched.")
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
