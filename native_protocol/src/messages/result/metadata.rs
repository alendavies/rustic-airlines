use std::io::Read;

use crate::types::{CassandraString, OptionBytes};

use super::rows::ColumnType;

#[derive(Debug, PartialEq)]
pub struct ColumnSpec {
    pub keyspace: Option<String>,
    pub table_name: Option<String>,
    pub name: String,
    pub type_: ColumnType,
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
pub struct TableSpec {
    pub keyspace: String,
    pub table_name: String,
}

enum MetadataFlagsCode {
    GlobalTablesSpec = 0x0001,
    HasMorePages = 0x0002,
    NoMetadata = 0x0004,
}

#[derive(Debug, PartialEq)]
pub struct MetadataFlags {
    pub global_tables_spec: bool,
    pub has_more_pages: bool,
    pub no_metadata: bool,
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

#[derive(Debug, PartialEq)]
pub struct Metadata {
    pub flags: MetadataFlags,
    pub columns_count: u32,
    pub global_table_spec: Option<TableSpec>,
    pub col_spec_i: Vec<ColumnSpec>,
}

impl Metadata {
    pub fn new(columns_count: u32, col_spec: Vec<(String, ColumnType)>) -> Self {
        let flags = MetadataFlags {
            global_tables_spec: false,
            has_more_pages: false,
            no_metadata: false,
        };

        let mut col_spec_i = Vec::new();

        for col in col_spec {
            col_spec_i.push(ColumnSpec {
                keyspace: None,
                table_name: None,
                name: col.0,
                type_: col.1,
            });
        }

        Self {
            flags,
            columns_count,
            global_table_spec: None,
            col_spec_i,
        }
    }
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

#[cfg(test)]
mod tests {
    use std::{io::Cursor, vec};

    use crate::{
        messages::result::{
            metadata::{ColumnSpec, Metadata, MetadataFlags, TableSpec},
            rows::ColumnType,
        },
        types::{CassandraString, OptionBytes},
    };

    #[test]
    fn test_column_spec_to_bytes() {
        let col_spec = ColumnSpec {
            keyspace: Some("test_keyspace".to_string()),
            table_name: Some("test_table".to_string()),
            name: "test_column".to_string(),
            type_: ColumnType::Int,
        };

        let bytes = col_spec.to_bytes();
        let keyspace_bytes = if let Some(keyspace) = &col_spec.keyspace {
            keyspace.to_string_bytes()
        } else {
            vec![]
        };
        let table_name_bytes = if let Some(table_name) = &col_spec.table_name {
            table_name.to_string_bytes()
        } else {
            vec![]
        };
        let expected_bytes = [
            keyspace_bytes.as_slice(),
            table_name_bytes.as_slice(),
            col_spec.name.to_string_bytes().as_slice(),
            col_spec.type_.to_option_bytes().as_slice(),
        ]
        .concat();

        assert_eq!(bytes, expected_bytes);
    }

    #[test]
    fn test_column_spec_from_bytes() {
        let expected_col_spec = ColumnSpec {
            keyspace: Some("test_keyspace".to_string()),
            table_name: Some("test_table".to_string()),
            name: "test_column".to_string(),
            type_: ColumnType::Int,
        };

        let bytes = expected_col_spec.to_bytes();
        let mut cursor = Cursor::new(bytes.as_slice());
        let col_spec = ColumnSpec::from_bytes(&mut cursor);

        assert_eq!(expected_col_spec, col_spec);
    }

    #[test]
    fn test_metadata_flags_to_bytes() {
        let flags = MetadataFlags {
            global_tables_spec: true,
            has_more_pages: false,
            no_metadata: false,
        };
        let bytes = flags.to_bytes();
        let expected_bytes = 0x0001u32.to_be_bytes().to_vec();

        assert_eq!(bytes, expected_bytes);
    }

    #[test]
    fn test_metadata_flags_to_from_bytes() {
        let expected_flags = MetadataFlags {
            global_tables_spec: true,
            has_more_pages: false,
            no_metadata: false,
        };
        let bytes = expected_flags.to_bytes();
        let mut cursor = Cursor::new(bytes.as_slice());
        let flags = MetadataFlags::from_bytes(&mut cursor);

        assert_eq!(expected_flags, flags);
    }

    #[test]
    fn test_metadata_to_bytes() {
        let metadata = Metadata {
            flags: MetadataFlags {
                global_tables_spec: true,
                has_more_pages: false,
                no_metadata: false,
            },
            columns_count: 1,
            global_table_spec: Some(TableSpec {
                keyspace: "test_keyspace".to_string(),
                table_name: "test_table".to_string(),
            }),
            col_spec_i: vec![ColumnSpec {
                keyspace: Some("test_keyspace".to_string()),
                table_name: Some("test_table".to_string()),
                name: "test_column".to_string(),
                type_: ColumnType::Int,
            }],
        };
        let bytes = metadata.to_bytes();

        let global_table_spec = metadata.global_table_spec.unwrap();

        let expected_bytes = [
            metadata.flags.to_bytes(),
            metadata.columns_count.to_be_bytes().to_vec(),
            global_table_spec.keyspace.to_string_bytes(),
            global_table_spec.table_name.to_string_bytes(),
            metadata.col_spec_i[0].to_bytes(),
        ]
        .concat();

        assert_eq!(bytes, expected_bytes);
    }

    #[test]
    fn test_metadata_from_bytes() {
        let expected_metadata = Metadata {
            flags: MetadataFlags {
                global_tables_spec: true,
                has_more_pages: false,
                no_metadata: false,
            },
            columns_count: 1,
            global_table_spec: Some(TableSpec {
                keyspace: "test_keyspace".to_string(),
                table_name: "test_table".to_string(),
            }),
            col_spec_i: vec![ColumnSpec {
                keyspace: Some("test_keyspace".to_string()),
                table_name: Some("test_table".to_string()),
                name: "test_column".to_string(),
                type_: ColumnType::Int,
            }],
        };

        let bytes = expected_metadata.to_bytes();
        let mut cursor = Cursor::new(bytes.as_slice());
        let metadata = Metadata::from_bytes(&mut cursor);

        assert_eq!(expected_metadata, metadata);
    }
}
