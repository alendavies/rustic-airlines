use std::{collections::HashMap, io::Read, net::IpAddr};

use crate::{
    types::{Bytes, CassandraString, OptionBytes, OptionSerializable},
    Serializable, SerializationError,
};

pub enum ResultCode {
    Void = 0x0001,
    Rows = 0x0002,
    SetKeyspace = 0x0003,
    Prepared = 0x0004,
    SchemaChange = 0x0005,
}

impl ResultCode {
    pub fn from_bytes(bytes: [u8; 4]) -> Self {
        match u32::from_be_bytes(bytes) {
            0x0001 => ResultCode::Void,
            0x0002 => ResultCode::Rows,
            0x0003 => ResultCode::SetKeyspace,
            0x0004 => ResultCode::Prepared,
            0x0005 => ResultCode::SchemaChange,
            _ => panic!("Invalid ResultCode"),
        }
    }
}

enum MetadataFlagsCode {
    GlobalTablesSpec = 0x0001,
    HasMorePages = 0x0002,
    NoMetadata = 0x0004,
}

#[derive(Debug, PartialEq)]
struct MetadataFlags {
    global_tables_spec: bool,
    has_more_pages: bool,
    no_metadata: bool,
}

impl MetadataFlags {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut flags = 0u32;
        if self.global_tables_spec {
            flags |= MetadataFlagsCode::GlobalTablesSpec as u32;
        }
        if self.has_more_pages {
            flags |= MetadataFlagsCode::HasMorePages as u32;
        }
        if self.no_metadata {
            flags |= MetadataFlagsCode::NoMetadata as u32;
        }
        flags.to_be_bytes().to_vec()
    }

    pub fn from_bytes(cursor: &mut std::io::Cursor<&[u8]>) -> Self {
        let mut flag_bytes = [0u8; 4];
        cursor.read_exact(&mut flag_bytes).unwrap();
        let flags = u32::from_be_bytes(flag_bytes);

        MetadataFlags {
            global_tables_spec: (flags & MetadataFlagsCode::GlobalTablesSpec as u32) != 0,
            has_more_pages: (flags & MetadataFlagsCode::HasMorePages as u32) != 0,
            no_metadata: (flags & MetadataFlagsCode::NoMetadata as u32) != 0,
        }
    }
}

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
enum ColumnType {
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
    List {
        len: u16,
        values: Vec,
    },
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
            ColumnValue::List(inner_value) => {
                bytes.extend_from_slice(&inner_value.to_bytes());
            }
            ColumnValue::Map(key_value, value_value) => {
                bytes.extend_from_slice(&key_value.to_bytes());
                bytes.extend_from_slice(&value_value.to_bytes());
            }
            ColumnValue::Set(inner_value) => {
                bytes.extend_from_slice(&inner_value.to_bytes());
            }
            ColumnValue::UDT {
                keyspace,
                name,
                fields,
            } => {
                bytes.extend_from_slice(keyspace.to_string_bytes().as_slice());
                bytes.extend_from_slice(name.to_string_bytes().as_slice());
                let fields_len = fields.len() as u16;
                bytes.extend_from_slice(&fields_len.to_be_bytes());
                for (field_name, field_value) in fields {
                    bytes.extend_from_slice(field_name.to_string_bytes().as_slice());
                    bytes.extend_from_slice(&field_value.to_bytes());
                }
            }
            ColumnValue::Tuple(inner_values) => {
                let inner_values_len = inner_values.len() as u16;
                bytes.extend_from_slice(&inner_values_len.to_be_bytes());
                for inner_value in inner_values {
                    bytes.extend_from_slice(&inner_value.to_bytes());
                }
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
                let r#type = ColumnType::from_option_bytes(&mut cursor).unwrap();
                ColumnValue::List(Box::new(r#type))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
struct ColumnSpec {
    keyspace: Option<String>,
    table_name: Option<String>,
    name: String,
    type_: ColumnType,
}

impl ColumnSpec {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // keyspace and table name only present if global_tables_spec flag is set
        if let Some(keyspace) = &self.keyspace {
            bytes.extend_from_slice(keyspace.to_string_bytes().as_slice());
        } else {
            bytes.extend_from_slice(&[0u8, 0u8]);
        }
        if let Some(table_name) = &self.table_name {
            bytes.extend_from_slice(table_name.to_string_bytes().as_slice());
        } else {
            bytes.extend_from_slice(&[0u8, 0u8]);
        }
        bytes.extend_from_slice(self.name.to_string_bytes().as_slice());
        bytes.extend_from_slice(&self.type_.to_option_bytes());

        bytes
    }

    pub fn from_bytes(cursor: &mut std::io::Cursor<&[u8]>) -> Self {
        let keyspace_string = String::from_string_bytes(cursor);
        let mut keyspace = None;
        if !keyspace_string.is_empty() {
            keyspace = Some(keyspace_string);
        }
        let table_name_string = String::from_string_bytes(cursor);
        let mut table_name = None;
        if !table_name_string.is_empty() {
            table_name = Some(table_name_string);
        }
        let name = String::from_string_bytes(cursor);
        let type_ = ColumnType::from_option_bytes(cursor).unwrap();

        ColumnSpec {
            keyspace,
            table_name,
            name,
            type_,
        }
    }
}

#[derive(Debug, PartialEq)]
struct TableSpec {
    keyspace: String,
    table_name: String,
}

#[derive(Debug, PartialEq)]
struct Metadata {
    flags: MetadataFlags,
    columns_count: u32,
    global_table_spec: Option<TableSpec>,
    col_spec_i: Vec<ColumnSpec>,
}

impl Metadata {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        bytes.extend_from_slice(&self.flags.to_bytes());

        bytes.extend_from_slice(&self.columns_count.to_be_bytes());

        if let Some(table_spec) = &self.global_table_spec {
            bytes.extend_from_slice(table_spec.keyspace.to_string_bytes().as_slice());
            bytes.extend_from_slice(table_spec.table_name.to_string_bytes().as_slice());
        }

        for col_spec in &self.col_spec_i {
            bytes.extend_from_slice(&col_spec.to_bytes());
        }

        bytes
    }

    pub fn from_bytes(cursor: &mut std::io::Cursor<&[u8]>) -> Self {
        let flags = MetadataFlags::from_bytes(cursor);

        let mut columns_count_bytes = [0u8; 4];
        cursor.read_exact(&mut columns_count_bytes).unwrap();
        let columns_count = u32::from_be_bytes(columns_count_bytes);

        let global_table_spec = if flags.global_tables_spec {
            let keyspace = String::from_string_bytes(cursor);
            let table_name = String::from_string_bytes(cursor);
            Some(TableSpec {
                keyspace,
                table_name,
            })
        } else {
            None
        };

        let mut col_spec_i = Vec::new();
        for _ in 0..columns_count {
            col_spec_i.push(ColumnSpec::from_bytes(cursor));
        }

        Metadata {
            flags,
            columns_count,
            global_table_spec,
            col_spec_i,
        }
    }
}

// key: column name, value: column value
type Row = HashMap<String, ColumnValue>;

#[derive(Debug, PartialEq)]
/// Indicates a set of rows.
struct Rows {
    metadata: Metadata,
    rows_count: u32,
    rows_content: Vec<Row>,
}

impl Serializable for Rows {
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        bytes.extend_from_slice(&self.metadata.to_bytes());

        bytes.extend_from_slice(&self.rows_count.to_be_bytes());

        /* for row in &self.rows_content {
            for (column_name, column_value) in row {
                bytes.extend_from_slice(&column_value.to_bytes());
            }
        } */

        bytes
    }

    fn from_bytes(bytes: &[u8]) -> std::result::Result<Self, SerializationError> {
        let mut cursor = std::io::Cursor::new(bytes);

        let metadata = Metadata::from_bytes(&mut cursor);

        let mut rows_count_bytes = [0u8; 4];
        cursor.read_exact(&mut rows_count_bytes).unwrap();
        let rows_count = u32::from_be_bytes(rows_count_bytes);

        let mut rows_content = Vec::new();
        /* for _ in 0..rows_count {
            let mut row = HashMap::new();
            for col_spec in &metadata.col_spec_i {
                let col_value = ColumnValue::from_bytes(&mut cursor, &col_spec.type_);
                row.insert(col_spec.name.clone(), col_value);
            }
            rows_content.push(row);
        } */

        Ok(Rows {
            metadata,
            rows_count,
            rows_content,
        })
    }
}

/// The result to a `use` query.
type SetKeyspace = String;

#[derive(Debug, PartialEq)]
/// The result to a PREPARE message.
struct Prepared {
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

// Represents the type of change in a schema altering query
#[derive(Debug, PartialEq)]
pub enum ChangeType {
    Created,
    Updated,
    Dropped,
}

// Represents the target of a schema altering query
#[derive(Debug, PartialEq)]
pub enum Target {
    Keyspace,
    Table,
    Type,
}

// If target is Keyspace, name is None and keyspace is the name of the keyspace changed
// If target is Table or Type, name is the name of the table or type changed and keyspace is the name of the keyspace
#[derive(Debug, PartialEq)]
struct Options {
    keyspace: String,
    name: Option<String>,
}

#[derive(Debug, PartialEq)]
///  The result to a schema altering query
/// (creation/update/drop of a keyspace/table/index).
struct SchemaChange {
    change_type: ChangeType,
    target: Target,
    options: Options,
}

impl Serializable for SchemaChange {
    /// Serializes the schema change to bytes.
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        let change_type = match self.change_type {
            ChangeType::Created => "CREATED",
            ChangeType::Updated => "UPDATED",
            ChangeType::Dropped => "DROPPED",
        };
        bytes.extend_from_slice(&(change_type.len() as u16).to_be_bytes());
        bytes.extend_from_slice(change_type.as_bytes());

        let target = match self.target {
            Target::Keyspace => "KEYSPACE",
            Target::Table => "TABLE",
            Target::Type => "TYPE",
        };
        bytes.extend_from_slice(&(target.len() as u16).to_be_bytes());
        bytes.extend_from_slice(target.as_bytes());

        bytes.extend_from_slice(&(self.options.keyspace.len() as u16).to_be_bytes());
        bytes.extend_from_slice(self.options.keyspace.as_bytes());
        if let Some(name) = &self.options.name {
            bytes.extend_from_slice(&(name.len() as u16).to_be_bytes());
            bytes.extend_from_slice(name.as_bytes());
        }

        bytes
    }

    /// Deserializes the schema change from bytes, returning a SchemaChange.
    fn from_bytes(bytes: &[u8]) -> std::result::Result<Self, SerializationError> {
        let mut cursor = std::io::Cursor::new(bytes);

        // Read change type
        let mut change_type_len_bytes = [0u8; 2];
        cursor.read_exact(&mut change_type_len_bytes).unwrap();
        let change_type_len = u16::from_be_bytes(change_type_len_bytes) as usize;

        let mut change_type_bytes = vec![0u8; change_type_len];
        cursor.read_exact(&mut change_type_bytes).unwrap();
        let change_type = String::from_utf8(change_type_bytes).unwrap();

        // Read target
        let mut target_len_bytes = [0u8; 2];
        cursor.read_exact(&mut target_len_bytes).unwrap();
        let target_len = u16::from_be_bytes(target_len_bytes) as usize;

        let mut target_bytes = vec![0u8; target_len];
        cursor.read_exact(&mut target_bytes).unwrap();
        let target = String::from_utf8(target_bytes).unwrap();

        // Read keyspace
        let mut keyspace_len_bytes = [0u8; 2];
        cursor.read_exact(&mut keyspace_len_bytes).unwrap();
        let keyspace_len = u16::from_be_bytes(keyspace_len_bytes) as usize;

        let mut keyspace_bytes = vec![0u8; keyspace_len];
        cursor.read_exact(&mut keyspace_bytes).unwrap();
        let keyspace = String::from_utf8(keyspace_bytes).unwrap();

        // Read name of the table or type if present
        let name = {
            let mut name_bytes_len = [0u8; 2];
            cursor.read_exact(&mut name_bytes_len).unwrap();
            let name_len = u16::from_be_bytes(name_bytes_len) as usize;

            if name_len > 0 {
                let mut name_bytes = vec![0u8; name_len];
                cursor.read_exact(&mut name_bytes).unwrap();
                Some(String::from_utf8(name_bytes).unwrap())
            } else {
                None
            }
        };

        let change_type = match change_type.as_str() {
            "CREATED" => ChangeType::Created,
            "UPDATED" => ChangeType::Updated,
            "DROPPED" => ChangeType::Dropped,
            _ => panic!("Invalid change type"),
        };

        let target = match target.as_str() {
            "KEYSPACE" => Target::Keyspace,
            "TABLE" => Target::Table,
            "TYPE" => Target::Type,
            _ => panic!("Invalid target"),
        };

        Ok(SchemaChange {
            change_type,
            target,
            options: Options { keyspace, name },
        })
    }
}

#[derive(Debug, PartialEq)]
pub enum Result {
    /// For results carrying no information.
    /// 0         8        16        24        32
    /// +---------+---------+---------+---------+
    /// |             kind (4 bytes)            |  // 0x0001
    /// +---------+---------+---------+---------+
    /// |             (empty body)              |
    /// +---------+---------+---------+---------+
    Void,
    /// For results to select queries, returning a set of rows.
    /// 0         8        16        24        32
    /// +---------+---------+---------+---------+
    /// |             kind (4 bytes)            |  // 0x0002
    /// +---------+---------+---------+---------+__
    /// |            flags (4 bytes)            |  |
    /// +---------+---------+---------+---------+  |
    /// |        columns_count (4 bytes)        |  |
    /// +---------+---------+---------+---------+  |
    /// |        (optional) paging_state        |  |
    /// +---------+---------+---------+---------+  |
    /// |    (optional) global_table_spec       |  | -> Metadata
    /// +---------+---------+---------+---------+  |
    /// |       (optional) col_spec_1           |  |
    /// +---------+---------+---------+---------+  |
    /// |                 ...                   |  |
    /// +---------+---------+---------+---------+  |
    /// |       (optional) col_spec_i           |__|
    /// +---------+---------+---------+---------+
    /// |         rows_count (4 bytes)          |
    /// +---------+---------+---------+---------+
    /// |          row_1 (value bytes)          |
    /// +---------+---------+---------+---------+
    /// |          row_2 (value bytes)          |
    /// +---------+---------+---------+---------+
    /// |                 ...                   |
    /// +---------+---------+---------+---------+
    /// |          row_m (value bytes)          |
    /// +---------+---------+---------+---------+
    Rows(Rows),
    /// The result to a `use` query.
    /// 0         8        16        24        32
    /// +---------+---------+---------+---------+
    /// |            kind (4 bytes)             |  // 0x0003
    /// +---------+---------+---------+---------+
    /// |    keyspace name (string + len (2))   |
    /// +---------+---------+---------+---------+
    SetKeyspace(SetKeyspace),
    /// Result to a PREPARE message.
    /// 0         8        16        24        32
    /// +---------+---------+---------+---------+
    /// |             kind (4 bytes)            |  // 0x0004
    /// +---------+---------+---------+---------+
    /// |           id (short bytes)            |
    /// +---------+---------+---------+---------+ __
    /// |            flags (4 bytes)            |   |
    /// +---------+---------+---------+---------+   |
    /// |         columns_count (4 bytes)       |   |
    /// +---------+---------+---------+---------+   |
    /// |        (optional) paging_state        |   |
    /// +---------+---------+---------+---------+   | -> Metadata
    /// |    (optional) global_table_spec       |   |
    /// +---------+---------+---------+---------+   |
    /// |      (optional)  col_spec_1           |   |
    /// +---------+---------+---------+---------+   |
    /// |                 ...                   |   |
    /// +---------+---------+---------+---------+   |
    /// |      (optional)  col_spec_i           |_ _|
    /// +---------+---------+---------+---------+
    /// |           result_metadata             | -> Metadata
    /// +---------+---------+---------+---------+
    Prepared(Prepared),
    /// The result to a schema altering query.
    /// 0         8        16        24        32
    /// +---------+---------+---------+---------+
    /// |            kind (4 bytes)             |  // 0x0005
    /// +---------+---------+---------+---------+
    /// |    change_type (string + len (2))     |
    /// +---------+---------+---------+---------+
    /// |       target (string + len (2))       |
    /// +---------+---------+---------+---------+
    /// |       options (string + len (2))      |
    /// +---------+---------+---------+---------+
    SchemaChange(SchemaChange),
}

impl Serializable for Result {
    /// 0        8        16       24       32
    /// +---------+---------+---------+---------+
    /// |            Kind (4 bytes)             |
    /// +---------+---------+---------+---------+
    /// |             Result Body               |
    /// +                                       +
    /// |                ...                    |
    /// +---------+---------+---------+---------+
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        let code = match self {
            Result::Void => ResultCode::Void,
            Result::Rows(_) => ResultCode::Rows,
            Result::SetKeyspace(_) => ResultCode::SetKeyspace,
            Result::Prepared(_) => ResultCode::Prepared,
            Result::SchemaChange(_) => ResultCode::SchemaChange,
        };

        bytes.extend_from_slice(&(code as u32).to_be_bytes());

        match self {
            Result::Void => {}
            Result::Rows(rows) => {
                bytes.extend_from_slice(&rows.to_bytes());
            }
            Result::SetKeyspace(keyspace) => {
                bytes.extend_from_slice(keyspace.as_bytes());
            }
            Result::Prepared(prepared) => {
                bytes.extend_from_slice(&prepared.to_bytes());
            }
            Result::SchemaChange(schema_change) => {
                bytes.extend_from_slice(&schema_change.to_bytes());
            }
        }

        bytes
    }

    fn from_bytes(bytes: &[u8]) -> std::result::Result<Result, SerializationError> {
        let mut cursor = std::io::Cursor::new(bytes);
        let mut code_bytes = [0u8; 4];
        cursor.read_exact(&mut code_bytes).unwrap();

        let code = ResultCode::from_bytes(code_bytes);

        match code {
            ResultCode::Void => Ok(Result::Void),
            ResultCode::Rows => {
                let rows = Rows::from_bytes(&bytes[4..])?;
                Ok(Result::Rows(rows))
            }
            ResultCode::SetKeyspace => {
                let mut keyspace = String::new();
                cursor.read_to_string(&mut keyspace).unwrap();
                Ok(Result::SetKeyspace(keyspace))
            }
            ResultCode::Prepared => {
                let prepared = Prepared::from_bytes(&bytes[4..])?;
                Ok(Result::Prepared(prepared))
            }
            ResultCode::SchemaChange => {
                let schema_change = SchemaChange::from_bytes(&bytes[4..])?;
                Ok(Result::SchemaChange(schema_change))
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use super::ResultCode;
    use crate::messages::result::{ChangeType, Options, Result, SchemaChange, Target};
    use crate::Serializable;

    #[test]
    fn test_void_result_to_bytes() {
        let result = Result::Void;

        let bytes = result.to_bytes();

        let expected_bytes = [0x00, 0x00, 0x00, 0x01];

        assert_eq!(bytes, expected_bytes);
    }

    #[test]
    fn test_void_result_from_bytes() {
        let bytes: [u8; 4] = (ResultCode::Void as u32).to_be_bytes();

        let result = Result::from_bytes(&bytes).unwrap();

        assert_eq!(result, Result::Void);
    }

    #[test]
    fn test_set_keyspace_to_bytes() {
        let set_keyspace = Result::SetKeyspace("test_keyspace".to_string());

        let bytes = set_keyspace.to_bytes();

        let expected_bytes = [
            0x00, 0x00, 0x00, 0x03, // kind = 0x0003
            0x74, 0x65, 0x73, 0x74, 0x5f, 0x6b, 0x65, 0x79, 0x73, 0x70, 0x61, 0x63, 0x65,
        ];

        assert_eq!(bytes, expected_bytes);
    }

    #[test]
    fn test_set_keyspace_from_bytes() {
        let keyspace = "test_keyspace";

        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(ResultCode::SetKeyspace as u32).to_be_bytes());
        bytes.extend_from_slice(keyspace.as_bytes());

        let result = Result::from_bytes(&bytes).unwrap();

        assert_eq!(result, Result::SetKeyspace(keyspace.to_string()));
    }

    #[test]
    fn test_schema_change_to_bytes() {
        let schema_change = Result::SchemaChange(SchemaChange {
            change_type: ChangeType::Created,
            target: Target::Table,
            options: Options {
                keyspace: "my_keyspace".to_string(),
                name: Some("my_table".to_string()),
            },
        });

        let bytes = schema_change.to_bytes();

        let mut expected_bytes = Vec::new();
        expected_bytes.extend_from_slice(&(ResultCode::SchemaChange as u32).to_be_bytes());
        expected_bytes.extend_from_slice(&("CREATED".len() as u16).to_be_bytes());
        expected_bytes.extend_from_slice("CREATED".as_bytes());
        expected_bytes.extend_from_slice(&("TABLE".len() as u16).to_be_bytes());
        expected_bytes.extend_from_slice("TABLE".as_bytes());
        expected_bytes.extend_from_slice(&("my_keyspace".len() as u16).to_be_bytes());
        expected_bytes.extend_from_slice("my_keyspace".as_bytes());
        expected_bytes.extend_from_slice(&("my_table".len() as u16).to_be_bytes());
        expected_bytes.extend_from_slice("my_table".as_bytes());

        assert_eq!(bytes, expected_bytes);
    }

    #[test]
    fn test_schema_change_from_bytes() {
        let expected_result = Result::SchemaChange(SchemaChange {
            change_type: ChangeType::Created,
            target: Target::Table,
            options: Options {
                keyspace: "my_keyspace".to_string(),
                name: Some("my_table".to_string()),
            },
        });

        let bytes = Result::to_bytes(&expected_result);

        let result = Result::from_bytes(&bytes).unwrap();

        assert_eq!(result, expected_result);
    }
}
