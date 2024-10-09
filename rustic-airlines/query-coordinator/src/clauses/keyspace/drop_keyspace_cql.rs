
use crate::errors::SqlError;

#[derive(Debug, Clone)]
pub struct DropKeyspace {
    name: String,
}

impl DropKeyspace {

    pub fn new_from_tokens(query: Vec<String>) -> Result<Self, SqlError> {
        if query.len() != 3 || query[0].to_uppercase() != "DROP" || query[1].to_uppercase() != "KEYSPACE" {
            return Err(SqlError::InvalidSyntax);
        }

        let name = &query[2];

        Ok(Self {
            name: name.to_string(),
        })
    }
}

