use std::{collections::HashMap, io::Read, net::IpAddr};

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

struct MetadataFlags {
    global_tables_spec: bool,
    has_more_pages: bool,
    no_metadata: bool,
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

#[derive(Debug)]
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

type Uuid = [u8; 16];

#[derive(Debug)]
enum ColumnValue {
    Custom(Vec<u8>),
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
    List(Box<ColumnValue>),
    Map(Box<ColumnValue>, Box<ColumnValue>),
    Set(Box<ColumnValue>),
    UDT {
        keyspace: String,
        name: String,
        fields: Vec<(String, ColumnValue)>,
    },
    Tuple(Vec<ColumnValue>),
}

struct ColumnSpec {
    keyspace: Option<String>,
    table_name: Option<String>,
    name: String,
    type_: ColumnType,
}

struct TableSpec {
    keyspace: String,
    table_name: String,
}

struct Metadata {
    flags: MetadataFlags,
    columns_count: u32,
    global_table_spec: Option<TableSpec>,
    col_spec_i: Vec<ColumnSpec>,
}

/// Indicates a set of rows.
struct Rows {
    metadata: Metadata,
    rows_count: u32,
    rows_content: Vec<Row>,
}

// key: column name, value: column value
type Row = HashMap<String, ColumnType>;

/// The result to a `use` query.
type SetKeyspace = String;

/// The result to a PREPARE message.
struct Prepared {
    id: u32,
    metadata: Metadata,
    result_metadata: Metadata,
}

///  The result to a schema altering query
/// (creation/update/drop of a keyspace/table/index).
struct SchemaChange {
    change_type: String,
    target: String,
    options: String,
}

pub enum Result {
    /// For results carrying no information.
    Void,
    /// For results to select queries, returning a set of rows.
    Rows(Rows),
    /// The result to a `use` query.
    SetKeyspace(SetKeyspace),
    /// Result to a PREPARE message.
    Prepared(Prepared),
    /// The result to a schema altering query.
    SchemaChange(SchemaChange),
}

impl Result {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        let code = match self {
            Result::Void => ResultCode::Void,
            Result::Rows(_) => ResultCode::Rows,
            Result::SetKeyspace(_) => ResultCode::SetKeyspace,
            Result::Prepared(_) => ResultCode::Prepared,
            Result::SchemaChange(_) => ResultCode::SchemaChange,
        };

        bytes.extend_from_slice(&(code as u16).to_be_bytes());

        match self {
            Result::Void => {}
            Result::Rows(_) => todo!(),
            Result::SetKeyspace(keyspace) => {
                bytes.extend_from_slice(keyspace.as_bytes());
            }
            Result::Prepared(_) => todo!(),
            Result::SchemaChange(schema_change) => {
                bytes.extend_from_slice(schema_change.change_type.as_bytes());
                bytes.extend_from_slice(schema_change.target.as_bytes());
                bytes.extend_from_slice(schema_change.options.as_bytes());
            }
        }

        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        let mut cursor = std::io::Cursor::new(bytes);
        let mut code_bytes = [0u8; 4];
        cursor.read_exact(&mut code_bytes).unwrap();

        let code = ResultCode::from_bytes(code_bytes);

        match code {
            ResultCode::Void => Result::Void,
            ResultCode::Rows => {
                todo!();
            }
            ResultCode::SetKeyspace => {
                let mut keyspace = String::new();
                cursor.read_to_string(&mut keyspace).unwrap();
                Result::SetKeyspace(keyspace)
            }
            ResultCode::Prepared => {
                todo!();
            }
            ResultCode::SchemaChange => {
                let mut change_type = String::new();
                cursor.read_to_string(&mut change_type).unwrap();
                let mut target = String::new();
                cursor.read_to_string(&mut target).unwrap();
                let mut options = String::new();
                cursor.read_to_string(&mut options).unwrap();

                Result::SchemaChange(SchemaChange {
                    change_type,
                    target,
                    options,
                })
            }
        }
    }
}
