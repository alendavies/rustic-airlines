
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
}

