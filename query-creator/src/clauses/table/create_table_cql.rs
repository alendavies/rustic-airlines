use crate::clauses::types::column::Column;
use crate::clauses::types::datatype::DataType;
use crate::errors::CQLError;
use crate::QueryCreator;
use std::cmp::PartialEq;
use std::collections::HashMap;
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct CreateTable {
    name: String,
    keyspace_used_name: String,
    if_not_exists_clause: bool,
    columns: Vec<Column>,
    clustering_columns_in_order: Vec<String>,
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

    pub fn get_if_not_exists_clause(&self) -> bool {
        self.if_not_exists_clause
    }

    pub fn get_used_keyspace(&self) -> String {
        self.keyspace_used_name.clone()
    }

    pub fn get_clustering_column_in_order(&self) -> Vec<String> {
        self.clustering_columns_in_order.clone()
    }
    // Constructor
    pub fn new_from_tokens(tokens: Vec<String>) -> Result<Self, CQLError> {
        if tokens.len() < 4 {
            return Err(CQLError::InvalidSyntax);
        }

        let mut index = 0;

        // Asegurarse de que comenzamos con "CREATE" y "TABLE"
        if tokens[index] != "CREATE" || tokens[index + 1] != "TABLE" {
            return Err(CQLError::InvalidSyntax);
        }
        index += 2;

        // Verificar si IF NOT EXISTS está presente
        let mut if_not_exists_clause = false;
        if tokens[index] == "IF" && tokens[index + 1] == "NOT" && tokens[index + 2] == "EXISTS" {
            if_not_exists_clause = true;
            index += 3;
        }

        // Obtener el nombre de la tabla, incluyendo el keyspace si está presente
        let full_table_name = &tokens[index];
        let (keyspace_used_name, table_name) = if full_table_name.contains('.') {
            let parts: Vec<&str> = full_table_name.split('.').collect();
            (parts[0].to_string(), parts[1].to_string())
        } else {
            (String::new(), full_table_name.clone())
        };
        index += 1;

        // Procesar los siguientes tokens para definir columnas y claves primarias
        let mut column_def = &tokens[index][..];
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
        let mut clustering_orders = HashMap::new();

        let mut primary_key_def: Option<String> = None;

        // Procesar columnas y primary key
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

            // Si es una columna con PRIMARY KEY explícito
            if col_parts
                .get(2)
                .map_or(false, |&s| s.to_uppercase() == "PRIMARY")
            {
                partition_key_cols.push(col_name.to_string());
            }

            columns.push(Column::new(col_name, data_type, false, true));
        }

        // Procesar primary key
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
        }

        // Procesar WITH CLUSTERING ORDER BY si existe
        index += 1;
        if index + 4 < tokens.len()
            && tokens[index] == "WITH"
            && tokens[index + 1] == "CLUSTERING"
            && tokens[index + 2] == "ORDER"
            && tokens[index + 3] == "BY"
        {
            let clustering_order_def = &tokens[index + 4];
            let order_parts: Vec<&str> = clustering_order_def.split(',').collect();

            for order_part in order_parts {
                let parts: Vec<&str> = order_part.split_whitespace().collect();
                if parts.len() == 2 {
                    let col_name = parts[0].trim().to_string();
                    let order = parts[1].trim().to_uppercase();

                    if order == "ASC" || order == "DESC" {
                        clustering_orders.insert(col_name, order);
                    }
                }
            }
        }

        // Actualizar las columnas con la información de clustering
        for column in &mut columns {
            if partition_key_cols.contains(&column.name) {
                column.is_partition_key = true;
            } else if clustering_key_cols.contains(&column.name) {
                column.is_clustering_column = true;
                column.clustering_order = clustering_orders
                    .get(&column.name)
                    .map_or(String::from("ASC"), |order| order.to_string());
            }
        }

        Ok(CreateTable {
            name: table_name,
            keyspace_used_name,
            if_not_exists_clause,
            columns,
            clustering_columns_in_order: clustering_key_cols,
        })
    }

    pub fn serialize(&self) -> String {
        let mut columns_str: Vec<String> = Vec::new();
        let mut partition_key_cols: Vec<String> = Vec::new();
        let mut clustering_key_cols: Vec<String> = Vec::new();
        let mut clustering_orders: Vec<String> = Vec::new();

        // Recorrer columnas y armar la definición de cada una
        for col in &self.columns {
            let mut col_def = format!("{} {}", col.name, col.data_type.to_string());
            if !col.allows_null {
                col_def.push_str(" NOT NULL");
            }

            // Identificar las columnas de clave primaria y órdenes de clustering
            if col.is_partition_key {
                partition_key_cols.push(col.name.clone());
                // Si hay una sola partition key sin clustering columns, agregar PRIMARY KEY aquí
                if partition_key_cols.len() == 1 && clustering_key_cols.is_empty() {
                    col_def.push_str(" PRIMARY KEY");
                }
            } else if col.is_clustering_column {
                clustering_key_cols.push(col.name.clone());
                if !col.clustering_order.is_empty() {
                    clustering_orders.push(format!("{} {}", col.name, col.clustering_order));
                }
            }

            columns_str.push(col_def);
        }

        // Ordenar clustering_key_cols y clustering_orders según self.clustering_columns_in_order
        let mut ordered_clustering_key_cols = Vec::new();
        let mut ordered_clustering_orders = Vec::new();
        for col_name in &self.clustering_columns_in_order {
            if let Some(pos) = clustering_key_cols.iter().position(|c| c == col_name) {
                ordered_clustering_key_cols.push(clustering_key_cols[pos].clone());
                if let Some(order) = clustering_orders.iter().find(|o| o.starts_with(col_name)) {
                    ordered_clustering_orders.push(order.clone());
                }
            }
        }

        // Construir la definición de la clave primaria si hay clustering columns
        let primary_key =
            if !partition_key_cols.is_empty() && !ordered_clustering_key_cols.is_empty() {
                format!(
                    "PRIMARY KEY (({}), {})",
                    partition_key_cols.join(", "),
                    ordered_clustering_key_cols.join(", ")
                )
            } else {
                String::new()
            };

        // Añadir la definición de la Primary Key al final de la tabla si aplica
        if !primary_key.is_empty() {
            columns_str.push(primary_key);
        }

        // Construir la declaración base
        let if_not_exists_str = if self.if_not_exists_clause {
            "IF NOT EXISTS "
        } else {
            ""
        };
        let table_name_str = if !self.keyspace_used_name.is_empty() {
            format!("{}.{}", self.keyspace_used_name, self.name)
        } else {
            self.name.clone()
        };

        let mut query = format!(
            "CREATE TABLE {}{} ({})",
            if_not_exists_str,
            table_name_str,
            columns_str.join(", ")
        );

        // Añadir la cláusula WITH CLUSTERING ORDER BY si hay órdenes de clustering
        if !ordered_clustering_orders.is_empty() {
            query.push_str(" WITH CLUSTERING ORDER BY (");
            query.push_str(&ordered_clustering_orders.join(", "));
            query.push(')');
        }

        query
    }

    // MÃ©todo para deserializar una cadena de texto a una instancia de `CreateTable`
    pub fn deserialize(serialized: &str) -> Result<Self, CQLError> {
        let tokens = QueryCreator::tokens_from_query(serialized);
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
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_table_without_if_not_exists_and_keyspace() {
        // Ejemplo: CREATE TABLE airports (iata TEXT, country TEXT, PRIMARY KEY (country, iata))
        let tokens = vec![
            "CREATE".to_string(),
            "TABLE".to_string(),
            "airports".to_string(),
            "iata TEXT, country TEXT, PRIMARY KEY (country, iata)".to_string(),
        ];

        let result = CreateTable::new_from_tokens(tokens);

        assert!(result.is_ok());

        let expected_table = CreateTable {
            name: "airports".to_string(),
            keyspace_used_name: "".to_string(),
            if_not_exists_clause: false,
            columns: vec![
                Column {
                    name: "iata".to_string(),
                    data_type: DataType::String,
                    is_primary_key: false,
                    allows_null: true,
                    is_clustering_column: true,
                    is_partition_key: false,
                    clustering_order: String::from("ASC"),
                },
                Column {
                    name: "country".to_string(),
                    data_type: DataType::String,
                    is_primary_key: false,
                    allows_null: true,
                    is_clustering_column: false,
                    is_partition_key: true,
                    clustering_order: String::new(),
                },
            ],
            clustering_columns_in_order: vec!["iata".to_string()],
        };

        assert_eq!(result.unwrap(), expected_table);
    }

    #[test]
    fn test_create_table_with_if_not_exists_and_keyspace() {
        // Ejemplo: CREATE TABLE IF NOT EXISTS sky.airports (iata TEXT, country TEXT, PRIMARY KEY (country, iata))
        let tokens = vec![
            "CREATE".to_string(),
            "TABLE".to_string(),
            "IF".to_string(),
            "NOT".to_string(),
            "EXISTS".to_string(),
            "sky.airports".to_string(),
            "iata TEXT, country TEXT, PRIMARY KEY (country, iata)".to_string(),
        ];

        let result = CreateTable::new_from_tokens(tokens);

        assert!(result.is_ok());

        let expected_table = CreateTable {
            name: "airports".to_string(),
            keyspace_used_name: "sky".to_string(),
            if_not_exists_clause: true,
            columns: vec![
                Column {
                    name: "iata".to_string(),
                    data_type: DataType::String,
                    is_primary_key: false,
                    allows_null: true,
                    is_clustering_column: true,
                    is_partition_key: false,
                    clustering_order: String::from("ASC"),
                },
                Column {
                    name: "country".to_string(),
                    data_type: DataType::String,
                    is_primary_key: false,
                    allows_null: true,
                    is_clustering_column: false,
                    is_partition_key: true,
                    clustering_order: String::new(),
                },
            ],
            clustering_columns_in_order: vec!["iata".to_string()],
        };

        assert_eq!(result.unwrap(), expected_table);
    }

    #[test]
    fn test_create_table_with_clustering_order() {
        // Example: CREATE TABLE IF NOT EXISTS sky.airports (
        //     iata TEXT,
        //     country TEXT,
        //     name TEXT,
        //     PRIMARY KEY (country, iata, name)
        // ) WITH CLUSTERING ORDER BY (iata ASC, name DESC)
        let tokens = vec![
            "CREATE".to_string(),
            "TABLE".to_string(),
            "IF".to_string(),
            "NOT".to_string(),
            "EXISTS".to_string(),
            "sky.airports".to_string(),
            "iata TEXT, country TEXT, name TEXT, PRIMARY KEY (country, iata, name)".to_string(),
            "WITH".to_string(),
            "CLUSTERING".to_string(),
            "ORDER".to_string(),
            "BY".to_string(),
            "iata ASC, name DESC".to_string(),
        ];

        let result = CreateTable::new_from_tokens(tokens);

        assert!(result.is_ok());

        let expected_table = CreateTable {
            name: "airports".to_string(),
            keyspace_used_name: "sky".to_string(),
            if_not_exists_clause: true,
            columns: vec![
                Column {
                    name: "country".to_string(),
                    data_type: DataType::String,
                    is_primary_key: false,
                    allows_null: true,
                    is_clustering_column: false,
                    is_partition_key: true,
                    clustering_order: String::new(),
                },
                Column {
                    name: "iata".to_string(),
                    data_type: DataType::String,
                    is_primary_key: false,
                    allows_null: true,
                    is_clustering_column: true,
                    is_partition_key: false,
                    clustering_order: "ASC".to_string(),
                },
                Column {
                    name: "name".to_string(),
                    data_type: DataType::String,
                    is_primary_key: false,
                    allows_null: true,
                    is_clustering_column: true,
                    is_partition_key: false,
                    clustering_order: "DESC".to_string(),
                },
            ],
            clustering_columns_in_order: vec!["iata".to_string(), "name".to_string()],
        };

        assert_eq!(result.unwrap(), expected_table);
    }

    #[test]
    fn test_clustering_columns_in_order() {
        // Verificar que clustering_columns_in_order se inicializa correctamente
        let tokens = vec![
            "CREATE".to_string(),
            "TABLE".to_string(),
            "airports".to_string(),
            "iata TEXT, country TEXT, name TEXT, PRIMARY KEY (country, iata, name)".to_string(),
        ];

        let result = CreateTable::new_from_tokens(tokens);

        assert!(result.is_ok());
        let table = result.unwrap();

        assert_eq!(
            table.clustering_columns_in_order,
            vec!["iata".to_string(), "name".to_string()]
        );
    }
}
