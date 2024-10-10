use crate::errors::CQLError;

#[derive(Debug, Clone)]
pub struct DropTable {
    table_name: String,
}

impl DropTable {
    pub fn new_from_tokens(query: Vec<String>) -> Result<Self, CQLError> {
        if query.len() != 3 || query[0].to_uppercase() != "DROP" || query[1].to_uppercase() != "TABLE" {
            return Err(CQLError::InvalidSyntax);
        }

        let name = &query[2];

        Ok(Self {
            table_name: name.to_string(),
        })
    }

    pub fn get_table_name(&self) -> String {
        self.table_name.clone()
    }

    // Método para serializar la estructura `DropTable` a una cadena de texto
    pub fn serialize(&self) -> String {
        format!("DROP TABLE {}", self.table_name)
    }

    // Método para deserializar una cadena de texto a una instancia de `DropTable`
    pub fn deserialize(serialized: &str) -> Result<Self, CQLError> {
        
        let tokens: Vec<String> = serialized.split_whitespace().map(|s| s.to_string()).collect();
        Self::new_from_tokens(tokens)
    
    }
}

// Implementación de `PartialEq` para permitir comparación de `DropTable`
impl PartialEq for DropTable {
    fn eq(&self, other: &Self) -> bool {
        self.table_name == other.table_name
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::CQLError;

    #[test]
    fn test_new_from_tokens_valid() {
        let query = vec!["DROP".to_string(), "TABLE".to_string(), "test_table".to_string()];
        let drop_table = DropTable::new_from_tokens(query);
        assert!(drop_table.is_ok());
        assert_eq!(drop_table.unwrap().get_table_name(), "test_table");
    }

    #[test]
    fn test_new_from_tokens_invalid_syntax() {
        // Caso donde faltan tokens
        let query = vec!["DROP".to_string(), "TABLE".to_string()];
        let drop_table = DropTable::new_from_tokens(query);
        assert_eq!(drop_table, Err(CQLError::InvalidSyntax));

        // Caso donde el primer token es incorrecto
        let query = vec!["DELETE".to_string(), "TABLE".to_string(), "test_table".to_string()];
        let drop_table = DropTable::new_from_tokens(query);
        assert_eq!(drop_table, Err(CQLError::InvalidSyntax));
    }

    #[test]
    fn test_serialize() {
        let drop_table = DropTable {
            table_name: "test_table".to_string(),
        };
        let serialized = drop_table.serialize();
        assert_eq!(serialized, "DROP TABLE test_table");
    }

    #[test]
    fn test_deserialize_valid() {
        let serialized = "DROP TABLE test_table";
        let drop_table = DropTable::deserialize(serialized);
        assert!(drop_table.is_ok());
        assert_eq!(drop_table.unwrap().get_table_name(), "test_table");
    }

    #[test]
    fn test_deserialize_invalid_syntax() {
        // Caso donde falta el nombre de la tabla
        let serialized = "DROP TABLE";
        let drop_table = DropTable::deserialize(serialized);
        assert_eq!(drop_table, Err(CQLError::InvalidSyntax));

        // Caso donde el comando no es "DROP TABLE"
        let serialized = "DELETE TABLE test_table";
        let drop_table = DropTable::deserialize(serialized);
        assert_eq!(drop_table, Err(CQLError::InvalidSyntax));
    }

    #[test]
    fn test_partial_eq() {
        let drop_table1 = DropTable {
            table_name: "test_table".to_string(),
        };
        let drop_table2 = DropTable {
            table_name: "test_table".to_string(),
        };
        let drop_table3 = DropTable {
            table_name: "another_table".to_string(),
        };

        assert_eq!(drop_table1, drop_table2);
        assert_ne!(drop_table1, drop_table3);
    }
}

