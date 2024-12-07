use std::fmt;

#[derive(Debug)]
/// Enum to represent the different errors that can occur during the gossip protocol.
pub enum GossipError {
    SynError,
    NoEndpointStateForIp,
    NoSuchKeyspace,
    KeyspaceAlreadyExists,
    TableAlreadyExists,
    WriteLockError,
    ReadLockError,
}

impl fmt::Display for GossipError {
    /// Implementation of the `fmt` method to convert the error into a readable string.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let description = match self {
            GossipError::SynError => "Syn error occurred",
            GossipError::NoEndpointStateForIp => "There is no endpoint state for the given ip",
            GossipError::NoSuchKeyspace => "The given keyspace does not exist",
            GossipError::KeyspaceAlreadyExists => "The given keyspace already exists",
            GossipError::TableAlreadyExists => "The given table already exists",
            GossipError::WriteLockError => "Error acquiring write lock",
            GossipError::ReadLockError => "Error acquiring read lock",
        };
        write!(f, "{}", description)
    }
}
