use crate::{errors::CQLError, operator::Operator};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub enum DataType {
    Int = 0x00,
    String = 0x01,
    Boolean = 0x02,
    Float = 0x03,
    Double = 0x04,
    Timestamp = 0x05,
    Uuid = 0x06,
}

impl std::str::FromStr for DataType {
    type Err = CQLError;

    // Crea un DataType a partir de una cadena
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "INT" => Ok(DataType::Int),
            "TEXT" => Ok(DataType::String),
            "STRING" => Ok(DataType::String),
            "BOOLEAN" => Ok(DataType::Boolean),
            "FLOAT" => Ok(DataType::Float),
            "DOUBLE" => Ok(DataType::Double),
            "TIMESTAMP" => Ok(DataType::Timestamp),
            "UUID" => Ok(DataType::Uuid),
            _ => Err(CQLError::InvalidSyntax),
        }
    }
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
            // DataType::Blob => "BLOB",
        }
    }

    pub fn compare(&self, x: &str, y: &str, operator: &Operator) -> Result<bool, CQLError> {
        match self {
            DataType::Int => {
                let x = x.parse::<i32>().map_err(|_| CQLError::InvalidCondition)?;
                let y = y.parse::<i32>().map_err(|_| CQLError::InvalidCondition)?;
                match operator {
                    Operator::Equal => Ok(x == y),
                    Operator::Greater => Ok(x > y),
                    Operator::Lesser => Ok(x < y),
                }
            }
            DataType::String => {
                let x = x
                    .parse::<String>()
                    .map_err(|_| CQLError::InvalidCondition)?;
                let y = y
                    .parse::<String>()
                    .map_err(|_| CQLError::InvalidCondition)?;
                match operator {
                    Operator::Equal => Ok(x == y),
                    Operator::Greater => Ok(x > y),
                    Operator::Lesser => Ok(x < y),
                }
            }
            DataType::Boolean => {
                let x = x.parse::<bool>().map_err(|_| CQLError::InvalidCondition)?;
                let y = y.parse::<bool>().map_err(|_| CQLError::InvalidCondition)?;
                match operator {
                    Operator::Equal => Ok(x == y),
                    Operator::Greater => Ok(x & !y),
                    Operator::Lesser => Ok(!x & y),
                }
            }
            DataType::Float => {
                let x = x.parse::<f32>().map_err(|_| CQLError::InvalidCondition)?;
                let y = y.parse::<f32>().map_err(|_| CQLError::InvalidCondition)?;
                match operator {
                    Operator::Equal => Ok(x == y),
                    Operator::Greater => Ok(x > y),
                    Operator::Lesser => Ok(x < y),
                }
            }
            DataType::Double => {
                let x = x.parse::<f64>().map_err(|_| CQLError::InvalidCondition)?;
                let y = y.parse::<f64>().map_err(|_| CQLError::InvalidCondition)?;
                match operator {
                    Operator::Equal => Ok(x == y),
                    Operator::Greater => Ok(x > y),
                    Operator::Lesser => Ok(x < y),
                }
            }
            DataType::Timestamp => {
                let x = x.parse::<i64>().map_err(|_| CQLError::InvalidCondition)?;
                let y = y.parse::<i64>().map_err(|_| CQLError::InvalidCondition)?;

                match operator {
                    Operator::Equal => Ok(x == y),
                    Operator::Greater => Ok(x > y),
                    Operator::Lesser => Ok(x < y),
                }
            }
            DataType::Uuid => {
                let x = x
                    .parse::<uuid::Uuid>()
                    .map_err(|_| CQLError::InvalidCondition)?;
                let y = y
                    .parse::<uuid::Uuid>()
                    .map_err(|_| CQLError::InvalidCondition)?;
                match operator {
                    Operator::Equal => Ok(x == y),
                    Operator::Greater => Ok(x > y),
                    Operator::Lesser => Ok(x < y),
                }
            }
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
            DataType::Uuid => value.parse::<Uuid>().is_ok(),       // Verifica si es un UUID válido

                                                                    // DataType::Blob => self.is_valid_blob(value), // Verifica si es un BLOB válido (hexadecimal)
        }
    }

    /// Verifica si una cadena es un timestamp válido en formato CQL
    fn is_valid_timestamp(&self, value: &str) -> bool {
        chrono::DateTime::parse_from_rfc3339(value).is_ok() || value.parse::<i64>().is_ok()
        // Cassandra también permite timestamps en milisegundos
    }

    // /// Verifica si una cadena es un BLOB válido (hexadecimal)
    // fn is_valid_blob(&self, value: &str) -> bool {
    //     hex::decode(value).is_ok()
    // }
}
