use std::collections::HashMap;

use crate::clauses::types::column::Column;
use crate::clauses::types::datatype::DataType;
use crate::errors::SqlError;

#[derive(Debug, Clone)]
pub struct AlterKeyspace {
    name: String,
    replication_class: String,
    replication_factor: u32,
}


/// Very similar logic to the creation of the keyspace, after all, it's a rewrite

impl AlterKeyspace {
   
    pub fn new_from_tokens(query: Vec<String>) -> Result<Self, SqlError> {
        if query.len() < 6 || query[0].to_uppercase() != "ALTER" || query[1].to_uppercase() != "KEYSPACE" {
            return Err(SqlError::InvalidSyntax);
        }

        let keyspace_name = query[2].to_string();

        if query[3].to_uppercase() != "WITH" || query[4].to_uppercase() != "REPLICATION" {
            return Err(SqlError::InvalidSyntax);
        }

        let replication_options = &query[5];
        if !replication_options.starts_with('{') || !replication_options.ends_with('}') {
            return Err(SqlError::InvalidSyntax);
        }

        let cleaned_options = &replication_options[1..replication_options.len() - 1];
        let options_parts: Vec<&str> = cleaned_options.split(',').collect();

        let mut replication_class = String::new();
        let mut replication_factor = 0;

        for option in options_parts {
            let kv: Vec<&str> = option.split(':').collect();
            if kv.len() != 2 {
                return Err(SqlError::InvalidSyntax);
            }

            let key = kv[0].trim().replace("'", ""); // Remove quotes, just in case
            let value = kv[1].trim().replace("'", "");

            match key.as_str() {
                "class" => replication_class = value,
                "replication_factor" => {
                    replication_factor = value.parse::<u32>().map_err(|_| SqlError::InvalidSyntax)?;
                }
                // Ignore other options like "durable_writes" for now
                _ => continue, 
            }
        }

        Ok(Self {
            name: keyspace_name, 
            replication_class: replication_class, 
            replication_factor: replication_factor
        })
    }

}