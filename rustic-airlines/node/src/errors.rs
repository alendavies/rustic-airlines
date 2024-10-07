use std::fmt::{self, Display};
use std::io;
use partitioner::errors::PartitionerError;
use query_coordinator::errors::CQLError;

/// Enum que representa los posibles errores dentro del `Node` y `QueryExecution`.
#[derive(Debug)]
pub enum NodeError {
    PartitionerError(PartitionerError),
    CQLError(CQLError),
    IoError(io::Error),
    LockError,
    OtherError,
}

impl Display for NodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NodeError::PartitionerError(e) => write!(f, "Partitioner Error: {}", e),
            NodeError::CQLError(e) => write!(f, "Query Coordinator Error: {}", e),
            NodeError::IoError(e) => write!(f, "I/O Error: {}", e),
            NodeError::LockError => write!(f, "Failed to acquire lock"),
            NodeError::OtherError => write!(f, "Other error")
        }
    }
}

impl From<PartitionerError> for NodeError {
    fn from(error: PartitionerError) -> Self {
        NodeError::PartitionerError(error)
    }
}

impl From<CQLError> for NodeError {
    fn from(error: CQLError) -> Self {
        NodeError::CQLError(error)
    }
}

impl From<io::Error> for NodeError {
    fn from(error: io::Error) -> Self {
        NodeError::IoError(error)
    }
}

impl<T> From<std::sync::PoisonError<T>> for NodeError {
    fn from(_: std::sync::PoisonError<T>) -> Self {
        NodeError::LockError
    }
}