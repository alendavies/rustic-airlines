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
            let column = &self.columns[i];
            if column.is_partition_key || column.is_clustering_column {
                return Err(CQLError::InvalidColumn);
            }
            self.columns.remove(i);
            Ok(())
        } else {
            Err(CQLError::InvalidColumn)
        }
    }

    // Método para modificar una columna existente
    pub fn modify_column(
        &mut self,
        column_name: &str,
        new_data_type: DataType,
        allows_null: bool,
    ) -> Result<(), CQLError> {
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

    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    pub fn get_columns(&self) -> Vec<Column> {
        self.columns.clone()
    }

    // Constructor
    pub fn new_from_tokens(tokens: Vec<String>) -> Result<Self, CQLError> {
        if tokens.len() < 4 {
            return Err(CQLError::InvalidSyntax);
        }

        if tokens[0] != "CREATE" || tokens[1] != "TABLE" {
            return Err(CQLError::InvalidSyntax);
        }

        let table_name = tokens[2].clone();

        let mut column_def = &tokens[3][..];
        if column_def.starts_with('(') {
            column_def = &column_def[1..];
        }
        if column_def.ends_with(')') {
            column_def = &column_def[..column_def.len() - 1];
        }

        let column_parts = split_preserving_parentheses(column_def);

        let mut columns = Vec::new();
        let mut partition_key_cols = Vec::new();
        let mut clustering_key_cols = Vec::new();

        let mut primary_key_def: Option<String> = None;

        for part in &column_parts {
            if part.to_uppercase().starts_with("PRIMARY KEY") {
                if primary_key_def.is_some() {
                    return Err(CQLError::InvalidSyntax);
                }
                primary_key_def = Some(part.to_string());
                continue;
            }

            let col_parts: Vec<&str> = part.split_whitespace().collect();
            if col_parts.len() < 2 {
                return Err(CQLError::InvalidSyntax);
            }

            let col_name = col_parts[0];
            let data_type = DataType::from_str(col_parts[1])?;

            if col_parts
                .get(2)
                .map_or(false, |&s| s.to_uppercase() == "PRIMARY")
            {
                if primary_key_def.is_some() {
                    return Err(CQLError::InvalidSyntax);
                }
                primary_key_def = Some(format!("PRIMARY KEY ({})", col_name));
            }

            columns.push(Column::new(col_name, data_type, false, true));
        }

        if let Some(pk_def) = primary_key_def {
            let pk_content = pk_def
                .find("PRIMARY KEY")
                .and_then(|index| {
                    let substring = &pk_def[index + "PRIMARY KEY".len()..].trim();
                    substring
                        .strip_prefix("(")
                        .and_then(|s| s.strip_suffix(")").or(Some(s)))
                })
                .ok_or(CQLError::InvalidSyntax)?;

            let pk_parts = split_preserving_parentheses(pk_content);

            if let Some(first_part) = pk_parts.first() {
                if first_part.starts_with('(') {
                    // Clave de partición compuesta
                    let partition_content = first_part
                        .trim_start_matches('(')
                        .trim_end_matches(')')
                        .split(',')
                        .map(|s| s.trim().to_string())
                        .collect::<Vec<String>>();

                    partition_key_cols.extend(partition_content);
                } else {
                    // Clave de partición simple
                    partition_key_cols.push(first_part.to_string());
                }

                // El resto son clustering keys
                clustering_key_cols.extend(pk_parts.iter().skip(1).map(|s| s.trim().to_string()));
            }

            for column in &mut columns {
                if partition_key_cols.contains(&column.name) {
                    column.is_partition_key = true;
                } else if clustering_key_cols.contains(&column.name) {
                    column.is_clustering_column = true;
                }
            }
        }

        Ok(CreateTable {
            name: table_name,
            columns,
        })
    }

    // MÃ©todo para serializar la estructura `CreateTable` a una cadena de texto
    // Método para serializar la estructura `CreateTable` a una cadena de texto
    pub fn serialize(&self) -> String {
        let mut columns_str: Vec<String> = Vec::new();
        let mut partition_key_cols: Vec<String> = Vec::new();
        let mut clustering_key_cols: Vec<String> = Vec::new();

        // Recorre las columnas y arma la cadena de definición de cada una
        for col in &self.columns {
            let mut col_def = format!("{} {}", col.name, col.data_type.to_string());
            if !col.allows_null {
                col_def.push_str(" NOT NULL");
            }
            columns_str.push(col_def);

            // Identifica las columnas de la clave primaria
            if col.is_partition_key {
                partition_key_cols.push(col.name.clone());
            } else if col.is_clustering_column {
                clustering_key_cols.push(col.name.clone());
            }
        }

        // Construye la definición de la clave primaria
        let primary_key = if !partition_key_cols.is_empty() {
            if clustering_key_cols.is_empty() {
                format!("PRIMARY KEY ({})", partition_key_cols.join(", "))
            } else {
                format!(
                    "PRIMARY KEY (({}), {})",
                    partition_key_cols.join(", "),
                    clustering_key_cols.join(", ")
                )
            }
        } else {
            String::new()
        };

        // Añade la definición de la Primary Key al final de la tabla
        if !primary_key.is_empty() {
            columns_str.push(primary_key);
        }

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

fn split_preserving_parentheses(input: &str) -> Vec<String> {
    let mut result = Vec::new();
    let mut current = String::new();
    let mut paren_count = 0;

    for c in input.chars() {
        match c {
            '(' => {
                paren_count += 1;
                current.push(c);
            }
            ')' => {
                paren_count -= 1;
                current.push(c);
                if paren_count == 0 && !current.trim().is_empty() {
                    result.push(current.trim().to_string());
                    current = String::new();
                }
            }
            ',' if paren_count == 0 => {
                if !current.trim().is_empty() {
                    result.push(current.trim().to_string());
                }
                current = String::new();
            }
            _ => current.push(c),
        }
    }

    if !current.trim().is_empty() {
        result.push(current.trim().to_string());
    }

    result
}

impl PartialEq for CreateTable {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}
