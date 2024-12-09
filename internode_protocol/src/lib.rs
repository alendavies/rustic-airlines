use messages::InternodeMessageError;

pub mod internode_link;
pub mod messages;

/// The InternodeSerializable trait is used to serialize and deserialize internode protocol messages.\
/// This trait is implemented by all internode protocol messages, queries, and responses.\
pub trait Serializable {
    /// Serializes the internode protocol message to a byte array.
    fn as_bytes(&self) -> Vec<u8>;

    /// Deserializes the internode protocol message from a byte array.
    fn from_bytes(bytes: &[u8]) -> Result<Self, InternodeMessageError>
    where
        Self: Sized;
}
