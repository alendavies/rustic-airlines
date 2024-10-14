use std::collections::HashMap;

use crate::clauses::types::column::Column;
use crate::clauses::types::datatype::DataType;
use crate::errors::CQLError;
use std::cmp::PartialEq;

#[derive(Debug, Clone)]
pub struct CreateTable {
    name: String,
    columns: Vec<Column>
}

impl CreateTable {

    // Métodos anteriores...

    // Método para agregar una columna a la tabla
    pub fn add_column(&mut self, column: Column) -> Result<(), CQLError> {
        if self.columns.iter().any(|col| col.name == column.name) {
            return Err(CQLError::InvalidColumn);
        }
        self.columns.push(column);
        Ok(())
    }

    // Método para eliminar una columna de la tabla
    pub fn remove_column(&mut self, column_name: &str) -> Result<(), CQLError> {
        let index = self.columns.iter().position(|col| col.name == column_name);
        if let Some(i) = index {
            self.columns.remove(i);
            Ok(())
        } else {
            Err(CQLError::InvalidColumn)
        }
    }

    // Método para modificar una columna existente
    pub fn modify_column(&mut self, column_name: &str, new_data_type: DataType, allows_null: bool) -> Result<(), CQLError> {
        for col in &mut self.columns {
            if col.name == column_name {
                col.data_type = new_data_type;
                col.allows_null = allows_null;
                return Ok(());
            }
        }
        Err(CQLError::InvalidColumn)
    }

    // Método para renombrar una columna existente
    pub fn rename_column(&mut self, old_name: &str, new_name: &str) -> Result<(), CQLError> {
        if self.columns.iter().any(|col| col.name == new_name) {
            return Err(CQLError::InvalidColumn);
        }
        for col in &mut self.columns {
            if col.name == old_name {
                col.name = new_name.to_string();
                return Ok(());
            }
        }
        Err(CQLError::InvalidColumn)
    }

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
        let mut primary_key_def: Option<String> = None;
    
        for col_def in columns_str.split(',') {
            let col_parts: Vec<&str> = col_def.trim().split_whitespace().collect();
            if col_parts.len() < 2 {
                return Err(CQLError::InvalidSyntax);
            }
        
            if col_parts[0].to_uppercase() == "PRIMARY" && col_parts[1].to_uppercase() == "KEY" {
                primary_key_def = Some(col_parts[2..].join(" "));
                continue;
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
    
        if let Some(pk_def) = primary_key_def {
            process_primary_key(&mut columns, &pk_def)?;
        } else {
            // IF NO EXPLICIT DEFINITION OF PRIMARY KEY, SEARCH COLUMN DEFINITION.
            if !columns.iter().any(|c| c.is_primary_key) {
                return Err(CQLError::InvalidSyntax);
            }
        }
    
        Ok(Self {
            name: table_name,
            columns,
        })
    }

    // MÃ©todo para serializar la estructura `CreateTable` a una cadena de texto
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

    // MÃ©todo para deserializar una cadena de texto a una instancia de `CreateTable`
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

fn process_primary_key(columns: &mut Vec<Column>, pk_def: &str) -> Result<(), CQLError> {
        let pk_parts: Vec<&str> = pk_def.trim_matches(|c| c == '(' || c == ')').split(',').map(|s| s.trim()).collect();
    
        let partition_key_end = pk_parts.iter().position(|&s| !s.starts_with('(')).unwrap_or(pk_parts.len());
    
        // Mark partition key columns
        for pk_col in &pk_parts[0..partition_key_end] {
            let col_name = pk_col.trim_matches(|c| c == '(' || c == ')');
            if let Some(col) = columns.iter_mut().find(|c| c.name == col_name) {
                col.is_primary_key = true;
            } else {
                return Err(CQLError::InvalidSyntax);
            }
        }
    
        // Mark clustering columns
        for pk_col in &pk_parts[partition_key_end..] {
            if let Some(col) = columns.iter_mut().find(|c| c.name == *pk_col) {
                col.is_primary_key = true;
                col.is_clustering_column = true;
            } else {
                return Err(CQLError::InvalidSyntax);
            }
        }
    
        Ok(())
    }

impl PartialEq for CreateTable {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

