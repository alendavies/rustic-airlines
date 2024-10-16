use std::{collections::HashMap, io::Read, net::IpAddr};

use crate::{Serializable, SerializationError};

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

type Uuid = [u8; 16];

#[derive(Debug, PartialEq)]
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

impl ColumnValue {
    fn to_bytes(&self) -> Vec<u8> {
        match self {
            ColumnValue::Ascii(val) => val.as_bytes().to_vec(),
            ColumnValue::Bigint(val) => val.to_be_bytes().to_vec(),
            _ => unimplemented!(),
        }
    }

    fn from_bytes(cursor: &mut std::io::Cursor<&[u8]>, col_type: &ColumnType) -> ColumnValue {
        match col_type {
            ColumnType::Ascii => {
                let mut string = String::new();
                cursor.read_to_string(&mut string).unwrap();
                ColumnValue::Ascii(string)
            }
            ColumnType::Bigint => {
                let mut bigint_bytes = [0u8; 8];
                cursor.read_exact(&mut bigint_bytes).unwrap();
                let bigint = i64::from_be_bytes(bigint_bytes);
                ColumnValue::Bigint(bigint)
            }
            _ => unimplemented!(),
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

        // Serializa los flags
        bytes.extend_from_slice(&self.flags.to_bytes());

        // Serializa el número de columnas
        bytes.extend_from_slice(&self.columns_count.to_be_bytes());

        // Serializa la tabla global si existe
        if let Some(table_spec) = &self.global_table_spec {
            bytes.extend_from_slice(table_spec.keyspace.as_bytes());
            bytes.extend_from_slice(table_spec.table_name.as_bytes());
        }

        // Serializa las especificaciones de columnas
        for col_spec in &self.col_spec_i {
            if let Some(keyspace) = &col_spec.keyspace {
                bytes.extend_from_slice(keyspace.as_bytes());
            }
            if let Some(table_name) = &col_spec.table_name {
                bytes.extend_from_slice(table_name.as_bytes());
            }
            bytes.extend_from_slice(col_spec.name.as_bytes());
            // Aquí puedes implementar la serialización para cada tipo de columna.
        }

        bytes
    }

    pub fn from_bytes(cursor: &mut std::io::Cursor<&[u8]>) -> Self {
        // Deserializa los flags
        let flags = MetadataFlags::from_bytes(cursor);

        // Deserializa el número de columnas
        let mut columns_count_bytes = [0u8; 4];
        cursor.read_exact(&mut columns_count_bytes).unwrap();
        let columns_count = u32::from_be_bytes(columns_count_bytes);

        // Deserializa la tabla global si existe
        let global_table_spec = if flags.global_tables_spec {
            let mut keyspace = String::new();
            let mut table_name = String::new();
            cursor.read_to_string(&mut keyspace).unwrap();
            cursor.read_to_string(&mut table_name).unwrap();
            Some(TableSpec {
                keyspace,
                table_name,
            })
        } else {
            None
        };

        // Deserializa las especificaciones de columnas
        let mut col_spec_i = Vec::new();
        for _ in 0..columns_count {
            let mut keyspace = None;
            let mut table_name = None;
            let mut name = String::new();
            cursor.read_to_string(&mut name).unwrap();
            // Aquí puedes implementar la deserialización para cada tipo de columna.

            col_spec_i.push(ColumnSpec {
                keyspace,
                table_name,
                name,
                type_: ColumnType::Ascii, // Ejemplo de tipo, deberías implementar el mapping adecuado
            });
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

        // Serializamos los metadatos
        bytes.extend_from_slice(&self.metadata.to_bytes());

        // Serializamos el número de filas
        bytes.extend_from_slice(&self.rows_count.to_be_bytes());

        // Serializamos el contenido de cada fila
        for row in &self.rows_content {
            for (column_name, column_value) in row {
                // Serializar cada valor de columna en la fila
                bytes.extend_from_slice(&column_value.to_bytes());
            }
        }

        bytes
    }

    fn from_bytes(bytes: &[u8]) -> std::result::Result<Self, SerializationError> {
        let mut cursor = std::io::Cursor::new(bytes);
        // Deserializamos los metadatos
        let metadata = Metadata::from_bytes(&mut cursor);

        // Deserializamos el número de filas
        let mut rows_count_bytes = [0u8; 4];
        cursor.read_exact(&mut rows_count_bytes).unwrap();
        let rows_count = u32::from_be_bytes(rows_count_bytes);

        // Deserializamos el contenido de las filas
        let mut rows_content = Vec::new();
        for _ in 0..rows_count {
            let mut row = HashMap::new();
            for col_spec in &metadata.col_spec_i {
                // Deserializamos el valor de la columna de acuerdo al tipo
                let col_value = ColumnValue::from_bytes(&mut cursor, &col_spec.type_);
                row.insert(col_spec.name.clone(), col_value);
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

/// The result to a `use` query.
type SetKeyspace = String;

#[derive(Debug, PartialEq)]
/// The result to a PREPARE message.
struct Prepared {
    id: u32,
    metadata: Metadata,
    result_metadata: Metadata,
}

impl Serializable for Prepared {
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // Serializamos el ID del Prepared statement (4 bytes)
        bytes.extend_from_slice(&self.id.to_be_bytes());

        // Serializamos el metadata (estructura de las columnas de la consulta)
        bytes.extend_from_slice(&self.metadata.to_bytes());

        // Serializamos el metadata del resultado (estructura de las columnas devueltas)
        bytes.extend_from_slice(&self.result_metadata.to_bytes());

        bytes
    }

    fn from_bytes(bytes: &[u8]) -> std::result::Result<Self, SerializationError> {
        let mut cursor = std::io::Cursor::new(bytes);

        // Deserializamos el ID del Prepared statement (4 bytes)
        let mut id_bytes = [0u8; 4];
        cursor.read_exact(&mut id_bytes).unwrap();
        let id = u32::from_be_bytes(id_bytes);

        // Deserializamos el metadata de la consulta
        let metadata = Metadata::from_bytes(&mut cursor);

        // Deserializamos el metadata del resultado
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
        bytes.extend_from_slice(change_type.as_bytes());

        let target = match self.target {
            Target::Keyspace => "KEYSPACE",
            Target::Table => "TABLE",
            Target::Type => "TYPE",
        };
        bytes.extend_from_slice(target.as_bytes());

        bytes.extend_from_slice(self.options.keyspace.as_bytes());
        if let Some(name) = &self.options.name {
            bytes.extend_from_slice(name.as_bytes());
        }

        bytes
    }

    /// Deserializes the schema change from bytes, returning a SchemaChange.
    fn from_bytes(bytes: &[u8]) -> std::result::Result<Self, SerializationError> {
        let mut cursor = std::io::Cursor::new(bytes);

        let mut change_type = String::new();
        cursor.read_to_string(&mut change_type).unwrap();

        let mut target = String::new();
        cursor.read_to_string(&mut target).unwrap();

        let mut keyspace = String::new();
        cursor.read_to_string(&mut keyspace).unwrap();

        let name = {
            let mut name_buf = Vec::new();
            cursor
                .read_to_end(&mut name_buf)
                .map_err(|_| SerializationError)?;
            if !name_buf.is_empty() {
                Some(String::from_utf8(name_buf).map_err(|_| SerializationError)?)
            } else {
                None
            }
        };

        dbg!(change_type.clone());

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
    /// |        keyspace name (string)         |
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
    /// |        change_type (string)           |
    /// +---------+---------+---------+---------+
    /// |            target (string)            |
    /// +---------+---------+---------+---------+
    /// |            options (string)           |
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
        expected_bytes.extend_from_slice("CREATED".as_bytes());
        expected_bytes.extend_from_slice("TABLE".as_bytes());
        expected_bytes.extend_from_slice("my_keyspace".as_bytes());
        expected_bytes.extend_from_slice("my_table".as_bytes());

        assert_eq!(bytes, expected_bytes);
    }

    #[test]
    fn test_schema_change_from_bytes() {
        let keyspace = "my_keyspace".to_string();
        let name = "my_table".to_string();

        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(ResultCode::SchemaChange as u32).to_be_bytes());
        bytes.extend_from_slice("CREATED".as_bytes());
        bytes.extend_from_slice("TABLE".as_bytes());
        bytes.extend_from_slice(keyspace.as_bytes());
        bytes.extend_from_slice(name.as_bytes());

        let result = Result::from_bytes(&bytes).unwrap();

        let expected_result = Result::SchemaChange(SchemaChange {
            change_type: ChangeType::Created,
            target: Target::Table,
            options: Options {
                keyspace: "my_keyspace".to_string(),
                name: Some("my_table".to_string()),
            },
        });

        assert_eq!(result, expected_result);
    }
}
