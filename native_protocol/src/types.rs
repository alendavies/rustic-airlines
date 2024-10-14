use std::io::{Cursor, Read};

use crate::{Serializable, SerializationError};

/// A 2 bytes unsigned integer.
pub type Short = u16;
/// A 4 bytes signed integer.
pub type Int = i32;
/// A 8 bytes signed integer.
pub type Long = i64;
type LongString = String;

#[derive(Debug, PartialEq)]
pub enum Bytes {
    None,
    Vec(Vec<u8>),
}

impl Serializable for Bytes {
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        match self {
            Bytes::None => {
                bytes.extend_from_slice(&Int::from(-1).to_be_bytes());
            }
            Bytes::Vec(vec) => {
                bytes.extend_from_slice(&Int::from(vec.len() as i32).to_be_bytes());
                bytes.extend_from_slice(vec.as_slice());
            }
        }
        return bytes;
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, SerializationError>
    where
        Self: Sized,
    {
        let mut cursor = Cursor::new(bytes);

        // get length of bytes
        let mut bytes_len_bytes = [0u8; 4];
        cursor.read_exact(&mut bytes_len_bytes).unwrap();
        let bytes_len = Int::from_be_bytes(bytes_len_bytes);

        if bytes_len < 0 {
            return Ok(Self::None);
        }

        let mut bytes_bytes = [0u8; 4];
        cursor.read_exact(&mut bytes_bytes).unwrap();
        let bytes = bytes_bytes.to_vec();

        Ok(Self::Vec(bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_bytes_null() {
        let bytes = Bytes::None;

        let bytes = bytes.to_bytes();

        // largo -1 para representar null
        assert_eq!(bytes, [0xFF; 4])
    }

    #[test]
    fn test_to_bytes_vec() {
        let bytes = Bytes::Vec(vec![0x01, 0x02, 0x03, 0x00]);

        let bytes = bytes.to_bytes();

        // 4 bytes para el Int + los 4 bytes a transportar
        assert_eq!(bytes, vec![0x00, 0x00, 0x00, 0x04, 0x01, 0x02, 0x03, 0x00])
    }

    #[test]
    fn test_from_bytes_null() {
        let input = [0xFF, 0xFF, 0xFF, 0xFF];

        let result = Bytes::from_bytes(&input).unwrap();

        assert_eq!(result, Bytes::None)
    }

    #[test]
    fn test_from_bytes_vec() {
        let input = [0x00, 0x00, 0x00, 0x04, 0x01, 0x02, 0x03, 0x00];

        let result = Bytes::from_bytes(&input).unwrap();

        assert_eq!(result, Bytes::Vec(vec![0x01, 0x02, 0x03, 0x00]));
    }
}
