use std::io::Read;

use crate::{Serializable, SerializationError};

use super::metadata::Metadata;

#[derive(Debug, PartialEq)]
/// The result to a PREPARE message.
pub struct Prepared {
    id: Vec<u8>,
    metadata: Metadata,
    result_metadata: Metadata,
}

impl Serializable for Prepared {
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        bytes.extend_from_slice(&(self.id.len() as u16).to_be_bytes());
        bytes.extend_from_slice(&self.id);

        bytes.extend_from_slice(&self.metadata.to_bytes());

        bytes.extend_from_slice(&self.result_metadata.to_bytes());

        bytes
    }

    fn from_bytes(bytes: &[u8]) -> std::result::Result<Self, SerializationError> {
        let mut cursor = std::io::Cursor::new(bytes);

        let mut id_len_bytes = [0u8; 2];
        cursor.read_exact(&mut id_len_bytes).unwrap();
        let id_len = u16::from_be_bytes(id_len_bytes) as usize;

        let mut id_bytes = vec![0u8; id_len];
        cursor.read_exact(&mut id_bytes).unwrap();
        let id = id_bytes;

        let metadata = Metadata::from_bytes(&mut cursor);

        let result_metadata = Metadata::from_bytes(&mut cursor);

        Ok(Prepared {
            id,
            metadata,
            result_metadata,
        })
    }
}
