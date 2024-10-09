use std::collections::HashMap;

use crate::clauses::types::column::Column;
use crate::clauses::types::datatype::DataType;
use crate::errors::CQLError;

#[derive(Debug, Clone)]
pub struct CreateKeyspace {
    name: String,
    replication_class: String,
    replication_factor: u32,
}


/// Partamos de que solamente vamos a usar la clase simple, entonces durable writes no es necesario, siempre va a ser true.

impl CreateKeyspace {
   
    pub fn new_from_tokens(query: Vec<String>) -> Result<Self, CQLError> {
        if query.len() < 6 || query[0].to_uppercase() != "CREATE" || query[1].to_uppercase() != "KEYSPACE" {
            return Err(CQLError::InvalidSyntax);
        }

        let keyspace_name = query[2].to_string();

        if query[3].to_uppercase() != "WITH" || query[4].to_uppercase() != "REPLICATION" {
            return Err(CQLError::InvalidSyntax);
        }

        let replication_options = &query[5];
        if !replication_options.starts_with('{') || !replication_options.ends_with('}') {
            return Err(CQLError::InvalidSyntax);
        }

        let cleaned_options = &replication_options[1..replication_options.len() - 1];
        let options_parts: Vec<&str> = cleaned_options.split(',').collect();

        let mut replication_class = String::new();
        let mut replication_factor = 0;

        for option in options_parts {
            let kv: Vec<&str> = option.split(':').collect();
            if kv.len() != 2 {
                return Err(CQLError::InvalidSyntax);
            }

            let key = kv[0].trim().replace("'", ""); // Remove quotes, just in case
            let value = kv[1].trim().replace("'", "");

            match key.as_str() {
                "class" => replication_class = value,
                "replication_factor" => {
                    replication_factor = value.parse::<u32>().map_err(|_| CQLError::InvalidSyntax)?;
                }
                // Ignore other options like "durable_writes" fow now
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