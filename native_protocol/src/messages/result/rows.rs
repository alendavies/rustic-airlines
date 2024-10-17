use std::{collections::HashMap, io::Read, net::IpAddr};

use crate::{
    types::{Bytes, CassandraString, OptionBytes, OptionSerializable},
    Serializable, SerializationError,
};

use super::metadata::Metadata;

enum ColumnTypeCode {
    Custom = 0x0000,
    Ascii = 0x0001,
    Bigint = 0x0002,
    Blob = 0x0003,
    Boolean = 0x0004,
    Counter = 0x0005,
    Decimal = 0x0006,
    Double = 0x0007,
    Float = 0x0008,
    Int = 0x0009,
    Timestamp = 0x000B,
    Uuid = 0x000C,
    Varchar = 0x000D,
    Varint = 0x000E,
    Timeuuid = 0x000F,
    Inet = 0x0010,
    List = 0x0020,
    Map = 0x0021,
    Set = 0x0022,
    UDT = 0x0030, // Keyspace, UDT name, fields
    Tuple = 0x0031,
}

#[derive(Debug, PartialEq)]
pub enum ColumnType {
    Custom(String),
    Ascii,
    Bigint,
    Blob,
    Boolean,
    Counter,
    Decimal,
    Double,
    Float,
    Int,
    Timestamp,
    Uuid,
    Varchar,
    Varint,
    Timeuuid,
    Inet,
    List(Box<ColumnType>),
    Map(Box<ColumnType>, Box<ColumnType>),
    Set(Box<ColumnType>),
    UDT {
        keyspace: String,
        name: String,
        fields: Vec<(String, ColumnType)>,
    },
    Tuple(Vec<ColumnType>),
}

impl OptionSerializable for ColumnType {
    fn get_option_code(&self) -> u16 {
        match self {
            ColumnType::Custom(_) => 0x0000,
            _ => todo!(),
        }
    }

    fn serialize_option(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        match self {
            ColumnType::Custom(custom) => {
                bytes.extend_from_slice(&(ColumnTypeCode::Custom as u16).to_be_bytes());
                bytes.extend_from_slice(custom.to_string_bytes().as_slice());

                bytes
            }
            ColumnType::Ascii => {
                bytes.extend_from_slice(&(ColumnTypeCode::Ascii as u16).to_be_bytes());
                bytes
            }
            ColumnType::Bigint => {
                bytes.extend_from_slice(&(ColumnTypeCode::Bigint as u16).to_be_bytes());
                bytes
            }
            ColumnType::Blob => {
                bytes.extend_from_slice(&(ColumnTypeCode::Blob as u16).to_be_bytes());
                bytes
            }
            ColumnType::Boolean => {
                bytes.extend_from_slice(&(ColumnTypeCode::Boolean as u16).to_be_bytes());
                bytes
            }
            ColumnType::Counter => {
                bytes.extend_from_slice(&(ColumnTypeCode::Counter as u16).to_be_bytes());
                bytes
            }
            ColumnType::Decimal => {
                bytes.extend_from_slice(&(ColumnTypeCode::Decimal as u16).to_be_bytes());
                bytes
            }
            ColumnType::Double => {
                bytes.extend_from_slice(&(ColumnTypeCode::Double as u16).to_be_bytes());
                bytes
            }
            ColumnType::Float => {
                bytes.extend_from_slice(&(ColumnTypeCode::Float as u16).to_be_bytes());
                bytes
            }
            ColumnType::Int => {
                bytes.extend_from_slice(&(ColumnTypeCode::Int as u16).to_be_bytes());
                bytes
            }
            ColumnType::Timestamp => {
                bytes.extend_from_slice(&(ColumnTypeCode::Timestamp as u16).to_be_bytes());
                bytes
            }
            ColumnType::Uuid => {
                bytes.extend_from_slice(&(ColumnTypeCode::Uuid as u16).to_be_bytes());
                bytes
            }
            ColumnType::Varchar => {
                bytes.extend_from_slice(&(ColumnTypeCode::Varchar as u16).to_be_bytes());
                bytes
            }
            ColumnType::Varint => {
                bytes.extend_from_slice(&(ColumnTypeCode::Varint as u16).to_be_bytes());
                bytes
            }
            ColumnType::Timeuuid => {
                bytes.extend_from_slice(&(ColumnTypeCode::Timeuuid as u16).to_be_bytes());
                bytes
            }
            ColumnType::Inet => {
                bytes.extend_from_slice(&(ColumnTypeCode::Inet as u16).to_be_bytes());
                bytes
            }
            ColumnType::List(inner_type) => {
                bytes.extend_from_slice(&(ColumnTypeCode::List as u16).to_be_bytes());
                let inner_type_bytes = inner_type.to_option_bytes();
                bytes.extend_from_slice(inner_type_bytes.as_slice());

                bytes
            }
            ColumnType::Map(key_type, value_type) => {
                bytes.extend_from_slice(&(ColumnTypeCode::Map as u16).to_be_bytes());
                let key_type_bytes = key_type.to_option_bytes();
                bytes.extend_from_slice(key_type_bytes.as_slice());
                let value_type_bytes = value_type.to_option_bytes();
                bytes.extend_from_slice(value_type_bytes.as_slice());

                bytes
            }
            ColumnType::Set(inner_type) => {
                bytes.extend_from_slice(&(ColumnTypeCode::Set as u16).to_be_bytes());
                let inner_type_bytes = inner_type.to_option_bytes();
                bytes.extend_from_slice(inner_type_bytes.as_slice());

                bytes
            }
            ColumnType::UDT {
                keyspace,
                name,
                fields,
            } => {
                bytes.extend_from_slice(&(ColumnTypeCode::UDT as u16).to_be_bytes());
                bytes.extend_from_slice(keyspace.to_string_bytes().as_slice());
                bytes.extend_from_slice(name.to_string_bytes().as_slice());
                let fields_len = fields.len() as u16;
                bytes.extend_from_slice(&fields_len.to_be_bytes());
                for (field_name, field_type) in fields {
                    bytes.extend_from_slice(field_name.to_string_bytes().as_slice());
                    let field_type_bytes = field_type.to_option_bytes();
                    bytes.extend_from_slice(field_type_bytes.as_slice());
                }

                bytes
            }
            ColumnType::Tuple(inner_types) => {
                bytes.extend_from_slice(&(ColumnTypeCode::Tuple as u16).to_be_bytes());
                let inner_types_len = inner_types.len() as u16;
                bytes.extend_from_slice(&inner_types_len.to_be_bytes());
                for inner_type in inner_types {
                    let inner_type_bytes = inner_type.to_option_bytes();
                    bytes.extend_from_slice(inner_type_bytes.as_slice());
                }

                bytes
            }
        }
    }

    fn deserialize_option(
        option_id: u16,
        cursor: &mut std::io::Cursor<&[u8]>,
    ) -> std::result::Result<Self, String> {
        match option_id {
            0x0000 => {
                let custom = String::from_string_bytes(cursor);
                Ok(ColumnType::Custom(custom))
            }
            0x0001 => Ok(ColumnType::Ascii),
            0x0002 => Ok(ColumnType::Bigint),
            0x0003 => Ok(ColumnType::Blob),
            0x0004 => Ok(ColumnType::Boolean),
            0x0005 => Ok(ColumnType::Counter),
            0x0006 => Ok(ColumnType::Decimal),
            0x0007 => Ok(ColumnType::Double),
            0x0008 => Ok(ColumnType::Float),
            0x0009 => Ok(ColumnType::Int),
            0x000B => Ok(ColumnType::Timestamp),
            0x000C => Ok(ColumnType::Uuid),
            0x000D => Ok(ColumnType::Varchar),
            0x000E => Ok(ColumnType::Varint),
            0x000F => Ok(ColumnType::Timeuuid),
            0x0010 => Ok(ColumnType::Inet),
            0x0020 => {
                let inner_type = ColumnType::from_option_bytes(cursor).unwrap();
                Ok(ColumnType::List(Box::new(inner_type)))
            }
            0x0021 => {
                let key_type = ColumnType::from_option_bytes(cursor).unwrap();
                let value_type = ColumnType::from_option_bytes(cursor).unwrap();
                Ok(ColumnType::Map(Box::new(key_type), Box::new(value_type)))
            }
            0x0022 => {
                let inner_type = ColumnType::from_option_bytes(cursor).unwrap();
                Ok(ColumnType::Set(Box::new(inner_type)))
            }
            0x0030 => {
                let keyspace = String::from_string_bytes(cursor);
                let name = String::from_string_bytes(cursor);

                let mut fields_len_bytes = [0u8; 2];
                cursor.read_exact(&mut fields_len_bytes).unwrap();
                let fields_count = u16::from_be_bytes(fields_len_bytes);
                let mut fields = Vec::new();
                for _ in 0..fields_count {
                    let field_name = String::from_string_bytes(cursor);
                    let field_type = ColumnType::from_option_bytes(cursor).unwrap();
                    fields.push((field_name, field_type));
                }
                Ok(ColumnType::UDT {
                    keyspace,
                    name,
                    fields,
                })
            }
            0x0031 => {
                let mut inner_type_len_bytes = [0u8; 2];
                cursor.read_exact(&mut inner_type_len_bytes).unwrap();
                let inner_types_count = u16::from_be_bytes(inner_type_len_bytes);
                let mut inner_types = Vec::new();
                for _ in 0..inner_types_count {
                    let inner_type = ColumnType::from_option_bytes(cursor).unwrap();
                    inner_types.push(inner_type);
                }
                Ok(ColumnType::Tuple(inner_types))
            }
            _ => Err(format!("Invalid ColumnType option id: {}", option_id)),
        }
    }
}

type Uuid = [u8; 16];

#[derive(Debug, PartialEq)]
enum ColumnValue {
    Custom(String),
    Ascii(String), // this is actually an ascii string
    Bigint(i64),
    Blob(Vec<u8>),
    Boolean(bool),
    Counter(i64),
    Decimal {
        scale: i32,
        unscaled: Vec<u8>, // Big-endian two's complement representation
    },
    Double(f64),
    Float(f32),
    Int(i32),
    Timestamp(i64), // Milliseconds since epoch
    Uuid(Uuid),
    Varchar(String),
    Varint(Vec<u8>),
    Timeuuid(Uuid),
    Inet(IpAddr),
    /* List {
        len: u16,
        values: Vec,
    }, */
    Map(Box<ColumnValue>, Box<ColumnValue>),
    Set(Box<ColumnValue>),
    UDT {
        keyspace: String,
        name: String,
        fields: Vec<(String, ColumnValue)>,
    },
    Tuple(Vec<ColumnValue>),
}

impl ColumnValue {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        match self {
            ColumnValue::Custom(custom) => {
                bytes.extend_from_slice(custom.to_string_bytes().as_slice());
            }
            ColumnValue::Ascii(ascii) => {
                bytes.extend_from_slice(ascii.to_string_bytes().as_slice());
            }
            ColumnValue::Bigint(bigint) => {
                bytes.extend_from_slice(&bigint.to_be_bytes());
            }
            ColumnValue::Blob(blob) => {
                bytes.extend_from_slice(blob.as_slice());
            }
            ColumnValue::Boolean(boolean) => {
                let byte = if *boolean { 1u8 } else { 0u8 };
                bytes.push(byte);
            }
            ColumnValue::Counter(counter) => {
                bytes.extend_from_slice(&counter.to_be_bytes());
            }
            ColumnValue::Decimal { scale, unscaled } => {
                bytes.extend_from_slice(&scale.to_be_bytes());
                bytes.extend_from_slice(unscaled.as_slice());
            }
            ColumnValue::Double(double) => {
                bytes.extend_from_slice(&double.to_be_bytes());
            }
            ColumnValue::Float(float) => {
                bytes.extend_from_slice(&float.to_be_bytes());
            }
            ColumnValue::Int(int) => {
                bytes.extend_from_slice(&int.to_be_bytes());
            }
            ColumnValue::Timestamp(timestamp) => {
                bytes.extend_from_slice(&timestamp.to_be_bytes());
            }
            ColumnValue::Uuid(uuid) => {
                bytes.extend_from_slice(uuid);
            }
            ColumnValue::Varchar(varchar) => {
                bytes.extend_from_slice(varchar.to_string_bytes().as_slice());
            }
            ColumnValue::Varint(varint) => {
                bytes.extend_from_slice(varint.as_slice());
            }
            ColumnValue::Timeuuid(timeuuid) => {
                bytes.extend_from_slice(timeuuid);
            }
            ColumnValue::Inet(inet) => match inet {
                IpAddr::V4(ipv4) => {
                    bytes.extend_from_slice(&ipv4.octets());
                }
                IpAddr::V6(ipv6) => {
                    bytes.extend_from_slice(&ipv6.octets());
                }
            },
            /* ColumnValue::List(inner_value) => {
                todo!()
            } */
            ColumnValue::Map(key_value, value_value) => {
                todo!()
            }
            ColumnValue::Set(inner_value) => {
                todo!()
            }
            ColumnValue::UDT {
                keyspace,
                name,
                fields,
            } => {
                todo!()
            }
            ColumnValue::Tuple(inner_values) => {
                todo!()
            }
        }
        bytes
    }

    pub fn from_bytes(bytes: &[u8], type_: &ColumnType) -> Self {
        let mut cursor = std::io::Cursor::new(bytes);
        match type_ {
            ColumnType::Custom(_) => {
                let custom = String::from_string_bytes(&mut cursor);
                ColumnValue::Custom(custom)
            }
            ColumnType::Ascii => {
                let ascii = String::from_string_bytes(&mut cursor);
                ColumnValue::Ascii(ascii)
            }
            ColumnType::Bigint => {
                let mut bigint_bytes = [0u8; 8];
                bigint_bytes.copy_from_slice(&bytes[0..8]);
                let bigint = i64::from_be_bytes(bigint_bytes);
                ColumnValue::Bigint(bigint)
            }
            ColumnType::Blob => ColumnValue::Blob(bytes.to_vec()),
            ColumnType::Boolean => {
                let boolean = bytes[0] != 0;
                ColumnValue::Boolean(boolean)
            }
            ColumnType::Counter => {
                let mut counter_bytes = [0u8; 8];
                counter_bytes.copy_from_slice(&bytes[0..8]);
                let counter = i64::from_be_bytes(counter_bytes);
                ColumnValue::Counter(counter)
            }
            ColumnType::Decimal => {
                let mut scale_bytes = [0u8; 4];
                scale_bytes.copy_from_slice(&bytes[0..4]);
                let scale = i32::from_be_bytes(scale_bytes);
                let unscaled = bytes[4..].to_vec();
                ColumnValue::Decimal { scale, unscaled }
            }
            ColumnType::Double => {
                let mut double_bytes = [0u8; 8];
                double_bytes.copy_from_slice(&bytes[0..8]);
                let double = f64::from_be_bytes(double_bytes);
                ColumnValue::Double(double)
            }
            ColumnType::Float => {
                let mut float_bytes = [0u8; 4];
                float_bytes.copy_from_slice(&bytes[0..4]);
                let float = f32::from_be_bytes(float_bytes);
                ColumnValue::Float(float)
            }
            ColumnType::Int => {
                let mut int_bytes = [0u8; 4];
                int_bytes.copy_from_slice(&bytes[0..4]);
                let int = i32::from_be_bytes(int_bytes);
                ColumnValue::Int(int)
            }
            ColumnType::Timestamp => {
                let mut timestamp_bytes = [0u8; 8];
                timestamp_bytes.copy_from_slice(&bytes[0..8]);
                let timestamp = i64::from_be_bytes(timestamp_bytes);
                ColumnValue::Timestamp(timestamp)
            }
            ColumnType::Uuid => {
                let mut uuid = [0u8; 16];
                uuid.copy_from_slice(&bytes[0..16]);
                ColumnValue::Uuid(uuid)
            }
            ColumnType::Varchar => {
                let varchar = String::from_string_bytes(&mut cursor);
                ColumnValue::Varchar(varchar)
            }
            ColumnType::Varint => ColumnValue::Varint(bytes.to_vec()),
            ColumnType::Timeuuid => {
                let mut timeuuid = [0u8; 16];
                timeuuid.copy_from_slice(&bytes[0..16]);
                ColumnValue::Timeuuid(timeuuid)
            }
            ColumnType::Inet => {
                let inet = match bytes.len() {
                    4 => IpAddr::V4(std::net::Ipv4Addr::new(
                        bytes[0], bytes[1], bytes[2], bytes[3],
                    )),
                    16 => IpAddr::V6(std::net::Ipv6Addr::new(
                        u16::from_be_bytes([bytes[0], bytes[1]]),
                        u16::from_be_bytes([bytes[2], bytes[3]]),
                        u16::from_be_bytes([bytes[4], bytes[5]]),
                        u16::from_be_bytes([bytes[6], bytes[7]]),
                        u16::from_be_bytes([bytes[8], bytes[9]]),
                        u16::from_be_bytes([bytes[10], bytes[11]]),
                        u16::from_be_bytes([bytes[12], bytes[13]]),
                        u16::from_be_bytes([bytes[14], bytes[15]]),
                    )),
                    _ => panic!("Invalid Inet address length"),
                };
                ColumnValue::Inet(inet)
            }
            ColumnType::List(_) => {
                todo!()
            }
            ColumnType::Map(_, _) => {
                todo!()
            }
            ColumnType::Set(_) => {
                todo!()
            }
            ColumnType::UDT { .. } => {
                todo!()
            }
            ColumnType::Tuple(_) => {
                todo!()
            }
        }
    }
}

// key: column name, value: column value
type Row = HashMap<String, ColumnValue>;

#[derive(Debug, PartialEq)]
/// Indicates a set of rows.
pub struct Rows {
    metadata: Metadata,
    rows_count: u32,
    rows_content: Vec<Row>,
}

impl Serializable for Rows {
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        bytes.extend_from_slice(&self.metadata.to_bytes());

        bytes.extend_from_slice(&self.rows_count.to_be_bytes());

        for row in &self.rows_content {
            for (column_name, column_value) in row {
                bytes.extend_from_slice(column_name.to_string_bytes().as_slice());
                let value_bytes = Bytes::Vec(column_value.to_bytes()).to_bytes();
                bytes.extend_from_slice(&value_bytes);
            }
        }

        bytes
    }

    fn from_bytes(bytes: &[u8]) -> std::result::Result<Self, SerializationError> {
        let mut cursor = std::io::Cursor::new(bytes);

        let metadata = Metadata::from_bytes(&mut cursor);

        let mut rows_count_bytes = [0u8; 4];
        cursor.read_exact(&mut rows_count_bytes).unwrap();
        let rows_count = u32::from_be_bytes(rows_count_bytes);

        let mut rows_content = Vec::new();
        for _ in 0..rows_count {
            let mut row = HashMap::new();
            for col_spec in &metadata.col_spec_i {
                // let col_value = ColumnValue::from_bytes(&mut cursor, &col_spec.type_);
                // row.insert(col_spec.name, col_value);
            }
            rows_content.push(row);
        }

        Ok(Rows {
            metadata,
            rows_count,
            rows_content,
        })
    }
}
