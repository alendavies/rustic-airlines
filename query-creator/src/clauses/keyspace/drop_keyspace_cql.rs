
use crate::errors::CQLError;

#[derive(Debug, Clone)]
pub struct DropKeyspace {
    name: String,
}

impl DropKeyspace {

    pub fn new_from_tokens(query: Vec<String>) -> Result<Self, CQLError> {
        if query.len() != 3 || query[0].to_uppercase() != "DROP" || query[1].to_uppercase() != "KEYSPACE" {
            return Err(CQLError::InvalidSyntax);
        }

        let name = &query[2];

        Ok(Self {
            name: name.to_string(),
        })
    }

    pub fn get_name(&self)->String{
        self.name.clone()
    }
    /// Serializa la estructura `DropKeyspace` a una consulta CQL
    pub fn serialize(&self) -> String {
        format!("DROP KEYSPACE {}", self.name)
    }
    
    /// Deserializa una consulta CQL en formato `String` y convierte a la estructura `DropKeyspace`
    pub fn deserialize(query: &str) -> Result<Self, CQLError> {
        // Divide la consulta en tokens y convierte a `Vec<String>`
        let tokens = query.split_whitespace().map(|s| s.to_string()).collect();
        Self::new_from_tokens(tokens)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::CQLError;

    #[test]
    fn test_new_from_tokens_valid() {
        let query = vec!["DROP".to_string(), "KEYSPACE".to_string(), "example_keyspace".to_string()];
        let drop_keyspace = DropKeyspace::new_from_tokens(query).unwrap();
        
        assert_eq!(drop_keyspace.get_name(), "example_keyspace".to_string());
    }

    #[test]
    fn test_new_from_tokens_invalid_syntax() {
        // Caso: Tokens insuficientes
        let query = vec!["DROP".to_string(), "KEYSPACE".to_string()];
        assert!(matches!(DropKeyspace::new_from_tokens(query), Err(CQLError::InvalidSyntax)));
        
        // Caso: Primer token incorrecto
        let query = vec!["DELETE".to_string(), "KEYSPACE".to_string(), "example_keyspace".to_string()];
        assert!(matches!(DropKeyspace::new_from_tokens(query), Err(CQLError::InvalidSyntax)));
        
        // Caso: Segundo token incorrecto
        let query = vec!["DROP".to_string(), "DATABASE".to_string(), "example_keyspace".to_string()];
        assert!(matches!(DropKeyspace::new_from_tokens(query), Err(CQLError::InvalidSyntax)));
    }

    #[test]
    fn test_serialize() {
        let drop_keyspace = DropKeyspace {
            name: "example_keyspace".to_string(),
        };
        let serialized = drop_keyspace.serialize();
        
        assert_eq!(serialized, "DROP KEYSPACE example_keyspace");
    }

    #[test]
    fn test_deserialize_valid() {
        let query = "DROP KEYSPACE example_keyspace";
        let drop_keyspace = DropKeyspace::deserialize(query).unwrap();
        
        assert_eq!(drop_keyspace.get_name(), "example_keyspace".to_string());
    }

    #[test]
    fn test_deserialize_invalid_syntax() {
        // Caso: Query incompleta
        let query = "DROP KEYSPACE";
        assert!(matches!(DropKeyspace::deserialize(query), Err(CQLError::InvalidSyntax)));
        
        // Caso: Query incorrecta
        let query = "REMOVE KEYSPACE example_keyspace;";
        assert!(matches!(DropKeyspace::deserialize(query), Err(CQLError::InvalidSyntax)));
    }
}


