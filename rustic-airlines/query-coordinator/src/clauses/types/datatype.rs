#[derive(Debug, Clone)]
pub enum DataType {
    Int,
    String,
    Boolean,
}

impl DataType {
    // Implementación de `to_string` para devolver el nombre del tipo de datos
    pub fn to_string(&self) -> &str {
        match self {
            DataType::Int => "INT",
            DataType::String => "TEXT",
            DataType::Boolean => "BOOLEAN",
        }
    }

    // Función que verifica si un valor dado es válido para el tipo de datos
    pub fn is_valid_value(&self, value: &str) -> bool {
        match self {
            DataType::Int => value.parse::<i32>().is_ok(),      // Verifica si el valor es un entero
            DataType::String => true,                            // Cualquier cadena es válida para STRING
            DataType::Boolean => value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("false"),
        }
    }
}
