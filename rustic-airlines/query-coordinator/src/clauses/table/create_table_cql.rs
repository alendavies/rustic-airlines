use crate::clauses::types::column::Column;
use crate::clauses::types::datatype::DataType;
use crate::errors::CQLError;
use std::cmp::PartialEq;

#[derive(Debug, Clone)]
pub struct CreateTable {
    name: String,
    columns: Vec<Column>,
}

impl CreateTable {

    pub fn get_name(&self)-> String{
        self.name.clone()
    }

    pub fn get_columns(&self)-> Vec<Column>{
        self.columns.clone()
    }

        // Constructor
    pub fn new_from_tokens(query: Vec<String>) -> Result<Self, CQLError> {
        
        if query.len() < 4 || query[0].to_uppercase() != "CREATE" || query[1].to_uppercase() != "TABLE" {
            return Err(CQLError::InvalidSyntax);
        }

        let table_name = query[2].to_string();
        
        // Eliminar paréntesis de apertura y cierre de columns_str
        let columns_str = query[3].trim_matches(|c| c == '(' || c == ')');

        let mut columns: Vec<Column> = Vec::new();

        for col_def in columns_str.split(',') {
            let col_parts: Vec<&str> = col_def.trim().split_whitespace().collect();
            if col_parts.len() < 2 {
                return Err(CQLError::InvalidSyntax);
            }

            let col_name = col_parts[0].to_string();
            let col_type = match col_parts[1].to_uppercase().as_str() {
                "INT" => DataType::Int,
                "TEXT" => DataType::String,
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

        Ok(Self {
            name: table_name,
            columns,
        })
    }

    // Método para serializar la estructura `CreateTable` a una cadena de texto
    pub fn serialize(&self) -> String {
        let columns_str: Vec<String> = self.columns.iter().map(|col| {
            let mut col_def = format!("{} {}", col.name, col.data_type.to_string());
            if col.is_primary_key {
                col_def.push_str(" PRIMARY KEY");
            }
            if !col.allows_null {
                col_def.push_str(" NOT NULL");
            }
            col_def
        }).collect();
        
        format!("CREATE TABLE {} ({})", self.name, columns_str.join(", "))
    }

    // Método para deserializar una cadena de texto a una instancia de `CreateTable`
    pub fn deserialize(serialized: &str) -> Result<Self, CQLError> {
        let mut tokens: Vec<String> = Vec::new();
        let mut current = String::new();
        let mut in_parens = false;

        for word in serialized.split_whitespace() {
            if word.contains('(') {
                in_parens = true;
                current.push_str(word);
            } else if word.contains(')') {
                current.push(' ');
                current.push_str(word);
                tokens.push(current.clone());
                current.clear();
                in_parens = false;
            } else if in_parens {
                current.push(' ');
                current.push_str(word);
            } else {
                tokens.push(word.to_string());
            }
        }

        Self::new_from_tokens(tokens)
    }

  
}

impl PartialEq for CreateTable {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}
