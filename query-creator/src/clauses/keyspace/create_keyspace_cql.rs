use crate::{errors::CQLError, QueryCreator};

#[derive(Debug, Clone)]
pub struct CreateKeyspace {
    name: String,
    replication_class: String,
    replication_factor: u32,
}

impl CreateKeyspace {
    pub fn new_from_tokens(query: Vec<String>) -> Result<Self, CQLError> {
        if query.len() < 10
            || query[0].to_uppercase() != "CREATE"
            || query[1].to_uppercase() != "KEYSPACE"
        {
            return Err(CQLError::InvalidSyntax);
        }

        let keyspace_name = query[2].to_string();

        if query[3].to_uppercase() != "WITH"
            || query[4].to_uppercase() != "REPLICATION"
            || query[5] != "="
        {
            return Err(CQLError::InvalidSyntax);
        }

        let mut replication_class = String::new();
        let mut replication_factor = 0;

        let mut index = 6; // Comienza después de "WITH REPLICATION ="
        while index < query.len() {
            match query[index].as_str() {
                "{" => index += 1, // Saltar el inicio de bloque '{'
                "class" => {
                    replication_class = query[index + 1].to_string();
                    index += 2;
                }
                "replication_factor" => {
                    replication_factor = query[index + 1]
                        .parse::<u32>()
                        .map_err(|_| CQLError::InvalidSyntax)?;
                    index += 2;
                }
                "}" => break, // Finaliza al encontrar '}'
                _ => index += 1,
            }
        }

        if replication_class != "SimpleStrategy" {
            return Err(CQLError::InvalidSyntax);
        }

        Ok(Self {
            name: keyspace_name,
            replication_class,
            replication_factor,
        })
    }

    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    pub fn get_replication_class(&self) -> String {
        self.replication_class.clone()
    }

    pub fn get_replication_factor(&self) -> u32 {
        self.replication_factor.clone()
    }

    pub fn update_replication_class(&mut self, replication_class: String) {
        self.replication_class = replication_class;
    }

    pub fn update_replication_factor(&mut self, replication_factor: u32) {
        self.replication_factor = replication_factor;
    }

    /// Serializa la estructura `CreateKeyspace` a una consulta CQL
    pub fn serialize(&self) -> String {
        format!(
            "CREATE KEYSPACE {} WITH replication = {{'class': '{}', 'replication_factor': {}}};",
            self.name, self.replication_class, self.replication_factor
        )
    }

    /// Deserializa una consulta CQL en formato `String` y convierte a la estructura `CreateKeyspace`
    pub fn deserialize(query: &str) -> Result<Self, CQLError> {
        // Divide la consulta en tokens y convierte a `Vec<String>`
        let tokens = QueryCreator::tokens_from_query(query);
        Self::new_from_tokens(tokens)
    }
}

impl PartialEq for CreateKeyspace {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_keyspace_valid_simple_strategy() {
        let query = vec![
            "CREATE".to_string(),
            "KEYSPACE".to_string(),
            "example".to_string(),
            "WITH".to_string(),
            "replication".to_string(),
            "=".to_string(),
            "{".to_string(),
            "class".to_string(),
            "SimpleStrategy".to_string(),
            "replication_factor".to_string(),
            "3".to_string(),
            "}".to_string(),
        ];

        let result = CreateKeyspace::new_from_tokens(query);
        assert!(result.is_ok());

        let create_keyspace = result.unwrap();
        assert_eq!(create_keyspace.name, "example");
        assert_eq!(create_keyspace.replication_class, "SimpleStrategy");
        assert_eq!(create_keyspace.replication_factor, 3);
    }

    #[test]
    fn test_create_keyspace_invalid_replication_class() {
        let query = vec![
            "CREATE".to_string(),
            "KEYSPACE".to_string(),
            "example".to_string(),
            "WITH".to_string(),
            "replication".to_string(),
            "=".to_string(),
            "{".to_string(),
            "class".to_string(),
            "InvalidStrategy".to_string(),
            "replication_factor".to_string(),
            "3".to_string(),
            "}".to_string(),
        ];

        let result = CreateKeyspace::new_from_tokens(query);
        assert!(matches!(result, Err(CQLError::InvalidSyntax)));
    }

    #[test]
    fn test_create_keyspace_invalid_replication_factor() {
        let query = vec![
            "CREATE".to_string(),
            "KEYSPACE".to_string(),
            "example".to_string(),
            "WITH".to_string(),
            "replication".to_string(),
            "=".to_string(),
            "{".to_string(),
            "class".to_string(),
            "SimpleStrategy".to_string(),
            "replication_factor".to_string(),
            "three".to_string(),
            "}".to_string(),
        ];

        let result = CreateKeyspace::new_from_tokens(query);
        assert!(matches!(result, Err(CQLError::InvalidSyntax)));
    }
}
