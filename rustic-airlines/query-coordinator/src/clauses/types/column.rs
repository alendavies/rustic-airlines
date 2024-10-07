use super::datatype::DataType;

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub is_primary_key: bool,
    pub allows_null: bool,
}

impl Column {
    pub fn new(name: &str, data_type: DataType, is_primary_key: bool, allows_null: bool) -> Column {
        Column {
            name: name.to_string(),
            data_type,
            is_primary_key,
            allows_null,
        }
    }
}