pub mod frame;
pub mod header;
pub mod messages;
mod types;

#[derive(Debug)]
pub struct SerializationError;

pub trait Serializable {
    fn to_bytes(&self) -> Vec<u8>;

    fn from_bytes(bytes: &[u8]) -> std::result::Result<Self, SerializationError>
    where
        Self: Sized;
}

pub trait ByteSerializable {
    fn to_byte(&self) -> u8;

    fn from_byte(byte: u8) -> std::result::Result<Self, SerializationError>
    where
        Self: Sized;
}
