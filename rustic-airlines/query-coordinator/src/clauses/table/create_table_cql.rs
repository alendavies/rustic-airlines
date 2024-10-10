use crate::clauses::types::column::Column;
use crate::clauses::types::datatype::DataType;
use crate::errors::CQLError;

#[derive(Debug, Clone)]
pub struct CreateTable {
    name: String,
    columns: Vec<Column>,
    clustering_order: Vec<(String, String)>
}

impl CreateTable {
   
    pub fn new_from_tokens(query: Vec<String>) -> Result<Self, CQLError> {
        if query.len() < 4 || query[0].to_uppercase() != "CREATE" || query[1].to_uppercase() != "TABLE" {
            return Err(CQLError::InvalidSyntax);
        }

        let table_name = query[2].to_string();
        let columns_str = &query[3];

        let mut columns: Vec<Column> = Vec::new();
        let mut primary_keys: Vec<String> = Vec::new();
        let mut with_clause = None;

        if query.len() > 4 {
            for i in 4..query.len() {
                if query[i].to_uppercase().starts_with("WITH") {
                    with_clause = Some(query[i..].join(" "));
                    break;
                }
            }
        }

        for col_def in columns_str.split(',') {
            let col_def = col_def.trim();

            // Handle separate PRIMARY KEY clause if there's one
            if col_def.to_uppercase().starts_with("PRIMARY KEY") {
                primary_keys = col_def["PRIMARY KEY".len()..].trim().trim_start_matches('(').trim_end_matches(')').split(',').map(|s| s.trim().to_string()).collect();
                continue;
            }

            let col_parts: Vec<&str> = col_def.split_whitespace().collect();
            if col_parts.len() < 2 {
                return Err(CQLError::InvalidSyntax);
            }

            let col_name = col_parts[0].to_string();
            let col_type = match col_parts[1].to_uppercase().as_str() {
                "INT" => DataType::Int,
                "STRING" => DataType::String,
                "BOOLEAN" => DataType::Boolean,
                _ => return Err(CQLError::Error),
            };
        
            let mut is_primary_key = false;
            let mut allows_null = true;
            if col_parts.len() > 2 {
                for part in &col_parts[2..] {
                    match part.to_uppercase().as_str() {
                        "PRIMARY" => is_primary_key = true,
                        "KEY" => (), // Skip "KEY", part of "PRIMARY KEY"
                        "NOT" => allows_null = false, // Assuming NOT NULL is specified
                        _ => return Err(CQLError::InvalidSyntax),
                    }
                }
            }

            columns.push(Column::new(&col_name, col_type, is_primary_key, allows_null));
        }

        // Check if there was a separate PRIMARY KEY clause, and mark relevant columns
        if !primary_keys.is_empty() {
            for pk in primary_keys {
                if let Some(column) = columns.iter_mut().find(|col| col.name == pk) {
                    column.is_primary_key = true;
                } else {
                    return Err(CQLError::InvalidSyntax);
                }
            }
        }

        let mut clustering_order: Vec<(String, String)> = Vec::new();
        if let Some(with_str) = with_clause {
            if with_str.to_uppercase().starts_with("WITH CLUSTERING ORDER BY") {
                let order_str = with_str["WITH CLUSTERING ORDER BY".len()..].trim().trim_start_matches('(').trim_end_matches(')');
                for order_def in order_str.split(',') {
                    let order_parts: Vec<&str> = order_def.trim().split_whitespace().collect();
                    if order_parts.len() == 2 {
                        clustering_order.push((order_parts[0].to_string(), order_parts[1].to_uppercase()));
                    }
                }
            }
        }

        Ok(Self {
            name: table_name,
            columns: columns,
            clustering_order: clustering_order
        })
    }

}