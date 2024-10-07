use std::collections::HashMap;

use crate::clauses::types::column::Column;
use crate::clauses::types::datatype::DataType;
use crate::errors::SqlError;

#[derive(Debug, Clone)]
pub struct CreateTable {
    name: String,
    columns: Vec<Column>,
}

impl CreateTable {
   
    pub fn new_from_tokens(query: Vec<String>) -> Result<Self, SqlError> {
        if query.len() < 4 || query[0].to_uppercase() != "CREATE" || query[1].to_uppercase() != "TABLE" {
            return Err(SqlError::InvalidSyntax);
        }

        let table_name = query[2].to_string();
        let columns_str = &query[3];

        let mut columns: Vec<Column> = Vec::new();
        for col_def in columns_str.split(',') {
            let col_parts: Vec<&str> = col_def.trim().split_whitespace().collect();
            if col_parts.len() < 2 {
                return Err(SqlError::InvalidSyntax);
            }

            let col_name = col_parts[0].to_string();
            let col_type = match col_parts[1].to_uppercase().as_str() {
                "INT" => DataType::Int,
                "STRING" => DataType::String,
                "BOOLEAN" => DataType::Boolean,
                _ => return Err(SqlError::Error),
            };
        
            let mut is_primary_key = false;
            let mut allows_null = true;
            if col_parts.len() > 2 {
                for part in &col_parts[2..] {
                    match part.to_uppercase().as_str() {
                        "PRIMARY" => is_primary_key = true,
                        "KEY" => (), // Skip "KEY", part of "PRIMARY KEY"
                        "NOT" => allows_null = false, // Assuming NOT NULL is specified
                        _ => return Err(SqlError::InvalidSyntax),
                    }
                }
            }

            columns.push(Column::new(&col_name, col_type, is_primary_key, allows_null));
        }

        Ok(Self {
            name: table_name,
            columns: columns,
        })
    }

}