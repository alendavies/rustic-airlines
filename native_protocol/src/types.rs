use std::io::{Cursor, Read};

/// A 2 bytes unsigned integer.
pub type Short = u16;
/// A 4 bytes signed integer.
pub type Int = i32;
/// A 8 bytes signed integer.
pub type Long = i64;

pub trait FromCursorDeserializable {
    fn deserialize(cursor: &mut Cursor<&[u8]>) -> Self;
}

impl FromCursorDeserializable for Int {
    fn deserialize(cursor: &mut Cursor<&[u8]>) -> Self {
        let mut bytes = [0u8; 4];
        cursor.read_exact(&mut bytes).unwrap();

        Int::from_be_bytes(bytes)
    }
}

pub trait OptionSerializable {
    fn deserialize_option(
        option_id: u16,
        cursor: &mut Cursor<&[u8]>,
    ) -> std::result::Result<Self, String>
    where
        Self: Sized;

    fn serialize_option(&self) -> Vec<u8>;
    //fn get_option_code(&self) -> u16;
}

pub trait OptionBytes: Sized {
    fn from_option_bytes(cursor: &mut std::io::Cursor<&[u8]>) -> std::result::Result<Self, String>;
    fn to_option_bytes(&self) -> Vec<u8>;
}

impl<T: OptionSerializable> OptionBytes for T {
    fn to_option_bytes(&self) -> Vec<u8> {
        //let code = self.get_option_code();
        self.serialize_option()
    }

    fn from_option_bytes(cursor: &mut std::io::Cursor<&[u8]>) -> std::result::Result<Self, String> {
        let mut option_id_bytes = [0u8; 2];
        cursor.read_exact(&mut option_id_bytes).unwrap();
        let option_id = u16::from_be_bytes(option_id_bytes);

        T::deserialize_option(option_id, cursor)
    }
}

pub trait CassandraString {
    fn from_long_string_bytes(cursor: &mut std::io::Cursor<&[u8]>) -> Self;
    fn to_long_string_bytes(&self) -> Vec<u8>;
    fn from_string_bytes(cursor: &mut std::io::Cursor<&[u8]>) -> Self;
    fn to_string_bytes(&self) -> Vec<u8>;
}

impl CassandraString for String {
    fn from_long_string_bytes(cursor: &mut std::io::Cursor<&[u8]>) -> Self {
        let mut len_bytes = [0u8; 4];
        cursor.read_exact(&mut len_bytes).unwrap();
        let len = u32::from_be_bytes(len_bytes) as usize;

        let mut string_bytes = vec![0u8; len];
        cursor.read_exact(&mut string_bytes).unwrap();
        String::from_utf8(string_bytes).unwrap()
    }

    fn to_long_string_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(self.len() as u32).to_be_bytes());
        bytes.extend_from_slice(self.as_bytes());
        bytes
    }

    fn from_string_bytes(cursor: &mut std::io::Cursor<&[u8]>) -> Self {
        let mut len_bytes = [0u8; 2];
        cursor.read_exact(&mut len_bytes).unwrap();
        let len = u16::from_be_bytes(len_bytes) as usize;

        if len == 0 {
            return String::new();
        }

        let mut string_bytes = vec![0u8; len];
        cursor.read_exact(&mut string_bytes).unwrap();
        String::from_utf8(string_bytes).unwrap()
    }

    fn to_string_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(self.len() as u16).to_be_bytes());
        bytes.extend_from_slice(self.as_bytes());
        bytes
    }
}

#[derive(Debug, PartialEq)]
pub enum Bytes {
    None,
    Vec(Vec<u8>),
}

// value_bytes = [0x01, 0x02, 0x00, 0x07]
// bytes = Bytes::Vec(value_bytes).to_bytes() -> [0x00, 0x04, 0x01, 0x02, 0x00, 0x07]

impl Bytes {
    pub fn to_bytes(&self) -> Vec<u8> {
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

    pub fn from_bytes(cursor: &mut std::io::Cursor<&[u8]>) -> std::result::Result<Self, String> {
        let mut bytes_len_bytes = [0u8; 4];
        cursor.read_exact(&mut bytes_len_bytes).unwrap();
        let bytes_len = Int::from_be_bytes(bytes_len_bytes);

        if bytes_len < 0 {
            return Ok(Self::None);
        }

        let mut bytes_bytes = vec![0u8; bytes_len as usize];
        cursor.read_exact(&mut bytes_bytes).unwrap();

        Ok(Self::Vec(bytes_bytes))
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
        let input = [0xFF, 0xFF, 0xFF, 0xFF].as_slice();
        let mut cursor = std::io::Cursor::new(input);

        let result = Bytes::from_bytes(&mut cursor).unwrap();

        assert_eq!(result, Bytes::None)
    }

    #[test]
    fn test_from_bytes_vec() {
        let input = [0x00, 0x00, 0x00, 0x04, 0x01, 0x02, 0x03, 0x00].as_slice();

        let mut cursor = std::io::Cursor::new(input);

        let result = Bytes::from_bytes(&mut cursor).unwrap();

        assert_eq!(result, Bytes::Vec(vec![0x01, 0x02, 0x03, 0x00]));
    }

    #[test]
    fn string_from_string_bytes() {
        let input = [0x00, 0x03, 'a' as u8, 'b' as u8, 'c' as u8, 'd' as u8];

        let mut cursor = std::io::Cursor::new(input.as_slice());

        let string = String::from_string_bytes(&mut cursor);

        assert_eq!(string, "abc");
    }

    #[test]
    fn string_from_long_string_bytes() {
        let input = [
            0x00, 0x00, 0x00, 0x03, 'a' as u8, 'b' as u8, 'c' as u8, 'd' as u8,
        ];

        let mut cursor = std::io::Cursor::new(input.as_slice());

        let string = String::from_long_string_bytes(&mut cursor);

        assert_eq!(string, "abc");
    }

    #[test]
    fn option_from_option_bytes() {
        #[derive(PartialEq, Debug)]
        enum Options {
            Something,
            SomethinElse(String),
        }

        impl OptionSerializable for Options {
            fn deserialize_option(
                option_id: u16,
                cursor: &mut Cursor<&[u8]>,
            ) -> std::result::Result<Self, String>
            where
                Self: Sized,
            {
                match option_id {
                    0x0001 => Ok(Options::Something),
                    0x0002 => {
                        let string = String::from_string_bytes(cursor);
                        Ok(Options::SomethinElse(string))
                    }
                    _ => unimplemented!(),
                }
            }

            fn serialize_option(&self) -> Vec<u8> {
                todo!()
            }

            /* fn get_option_code(&self) -> u16 {
                todo!()
            } */
        }

        let input = [0x00, 0x02, 0x00, 0x03, 'a' as u8, 'b' as u8, 'c' as u8];
        let mut cursor = std::io::Cursor::new(input.as_slice());

        let option = Options::from_option_bytes(&mut cursor).unwrap();

        assert_eq!(option, Options::SomethinElse("abc".to_string()));
    }

    #[test]
    fn option_to_option_bytes() {
        #[derive(PartialEq, Debug)]
        enum Options {
            Something,
            SomethinElse(String),
        }

        impl OptionSerializable for Options {
            fn deserialize_option(
                option_id: u16,
                cursor: &mut Cursor<&[u8]>,
            ) -> std::result::Result<Self, String>
            where
                Self: Sized,
            {
                todo!()
            }

            fn serialize_option(&self) -> Vec<u8> {
                let mut bytes = Vec::new();

                match self {
                    Options::Something => {
                        bytes.extend_from_slice(&(0x0001 as u16).to_be_bytes());
                        bytes
                    }
                    Options::SomethinElse(txt) => {
                        bytes.extend_from_slice(&(0x0002 as u16).to_be_bytes());
                        bytes.extend_from_slice(&txt.to_string_bytes());
                        bytes
                    }
                }
            }

            /* fn get_option_code(&self) -> u16 {
                match self {
                    Options::Something => 0x0001,
                    Options::SomethinElse(_) => 0x0002,
                }
            } */
        }

        let option = Options::SomethinElse("abc".to_string());
        let bytes = option.to_option_bytes();

        let expected = vec![0x00, 0x02, 0x00, 0x03, 'a' as u8, 'b' as u8, 'c' as u8];

        assert_eq!(bytes, expected)
    }
}
