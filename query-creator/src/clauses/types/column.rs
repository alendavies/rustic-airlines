use super::datatype::DataType;

#[derive(Debug, Clone, Eq, Hash)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub is_primary_key: bool,
    pub allows_null: bool,
    pub is_clustering_column: bool,
    pub is_partition_key: bool,
}

impl Column {
    pub fn new(name: &str, data_type: DataType, is_primary_key: bool, allows_null: bool) -> Column {
        Column {
            name: name.to_string(),
            data_type,
            is_primary_key,
            allows_null,
            is_clustering_column: false,
            is_partition_key: false,
        }
    }
}

// Implementación del trait `PartialEq` para comparar solo por el nombre
impl PartialEq for Column {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}
