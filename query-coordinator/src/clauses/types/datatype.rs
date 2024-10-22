#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    Int,
    String,
    Boolean,
    Float,
    Double,
    Timestamp,
    Uuid,
    Blob,
}

impl DataType {
    /// Devuelve el nombre del tipo de datos como una cadena CQL
    pub fn to_string(&self) -> &str {
        match self {
            DataType::Int => "INT",
            DataType::String => "TEXT",
            DataType::Boolean => "BOOLEAN",
            DataType::Float => "FLOAT",
            DataType::Double => "DOUBLE",
            DataType::Timestamp => "TIMESTAMP",
            DataType::Uuid => "UUID",
            DataType::Blob => "BLOB",
        }
    }

    /// Verifica si el valor dado es válido para el tipo de datos especificado
    pub fn is_valid_value(&self, value: &str) -> bool {
        match self {
            DataType::Int => value.parse::<i32>().is_ok(), // Verifica si es un entero válido
            DataType::String => true,                      // Cualquier cadena es válida para TEXT
            DataType::Boolean => {
                value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("false")
            }
            DataType::Float => value.parse::<f32>().is_ok(), // Verifica si es un float válido
            DataType::Double => value.parse::<f64>().is_ok(), // Verifica si es un double válido
            DataType::Timestamp => self.is_valid_timestamp(value), // Verifica si es un timestamp válido
            DataType::Uuid => self.is_valid_uuid(value),           // Verifica si es un UUID válido
            DataType::Blob => self.is_valid_blob(value), // Verifica si es un BLOB válido (hexadecimal)
        }
    }

    /// Verifica si una cadena es un timestamp válido en formato CQL
    fn is_valid_timestamp(&self, value: &str) -> bool {
        chrono::DateTime::parse_from_rfc3339(value).is_ok() || value.parse::<i64>().is_ok()
        // Cassandra también permite timestamps en milisegundos
    }

    /// Verifica si una cadena es un UUID válido
    fn is_valid_uuid(&self, value: &str) -> bool {
        uuid::Uuid::parse_str(value).is_ok()
    }

    /// Verifica si una cadena es un BLOB válido (hexadecimal)
    fn is_valid_blob(&self, value: &str) -> bool {
        hex::decode(value).is_ok()
    }
}
