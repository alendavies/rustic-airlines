
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

    /// Serializa la estructura `DropKeyspace` a una consulta CQL
    pub fn serialize(&self) -> String {
        format!("DROP KEYSPACE {};", self.name)
    }

    /// Deserializa una consulta CQL en formato `String` y convierte a la estructura `DropKeyspace`
    pub fn deserialize(query: &str) -> Result<Self, CQLError> {
        // Divide la consulta en tokens y convierte a `Vec<String>`
        let tokens = query.split_whitespace().map(|s| s.to_string()).collect();
        Self::new_from_tokens(tokens)
    }
}

