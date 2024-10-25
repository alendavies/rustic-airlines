use super::where_sql::Where;
use crate::errors::CQLError;
use crate::utils::{is_delete, is_from, is_where};
use crate::QueryCoordinator;

/// Struct that represents the `DELETE` SQL clause.
/// The `DELETE` clause is used to delete records from a table.
///
/// # Fields
///
/// - `table_name`: a `String` that holds the name of the table from which the records will be deleted.
/// - `where_clause`: an `Option<Where>` that holds the condition that the records must meet to be deleted. If it is `None`, all records will be deleted.
///
#[derive(PartialEq, Debug, Clone)]
pub struct Delete {
    pub table_name: String,
    pub columns: Option<Vec<String>>,  // Agregamos un vector opcional para las columnas
    pub where_clause: Option<Where>,
}

impl Delete {
    /// Creates and returns a new `Delete` instance from tokens.
    ///
    /// # Arguments
    ///
    /// - `tokens`: a `Vec<String>` that holds the tokens that form the `DELETE` clause.
    ///
    /// The tokens must be in the following order: `DELETE`, `column(s)_optional`, `FROM`, `table_name`, `WHERE`, `condition`.
    ///
    /// If the `WHERE` clause is not present, the `where_clause` field will be `None`.
    ///
    pub fn new_from_tokens(tokens: Vec<String>) -> Result<Self, CQLError> {
        if tokens.len() < 3 {
            return Err(CQLError::InvalidSyntax);
        }

        let mut i = 0;
        let mut columns = None;
        let table_name: String;
        let mut where_tokens: Vec<&str> = Vec::new();

        // Verificamos que la primera palabra sea DELETE
        if !is_delete(&tokens[i]) {
            return Err(CQLError::InvalidSyntax);
        }
        i += 1;

        // Procesamos las columnas opcionales antes de la palabra clave FROM
        if i < tokens.len() && !is_from(&tokens[i]) {
            let mut column_names = Vec::new();
            while i < tokens.len() && !is_from(&tokens[i]) {
                column_names.push(tokens[i].clone());
                i += 1;
            }
            columns = Some(column_names);
        }

        // Verificamos que la palabra clave FROM esté presente y que haya un nombre de tabla después
        if i < tokens.len() && is_from(&tokens[i]) && i + 1 < tokens.len() {
            table_name = tokens[i + 1].clone();
            i += 2;
        } else {
            return Err(CQLError::InvalidSyntax);
        }

        // Procesamos la cláusula WHERE, si está presente
        if i < tokens.len() && is_where(&tokens[i]) {
            while i < tokens.len() {
                where_tokens.push(tokens[i].as_str());
                i += 1;
            }
        }

        let where_clause = if !where_tokens.is_empty() {
            Some(Where::new_from_tokens(where_tokens)?)
        } else {
            None
        };

        Ok(Self {
            table_name,
            columns,
            where_clause,
        })
    }

    /// Serializa la instancia de `Delete` en una cadena de texto.
    pub fn serialize(&self) -> String {
        let mut serialized = String::from("DELETE");

        // Añadimos las columnas si existen
        if let Some(columns) = &self.columns {
            serialized.push_str(&format!(" {}", columns.join(", ")));
        }

        serialized.push_str(&format!(" FROM {}", self.table_name));

        if let Some(where_clause) = &self.where_clause {
            serialized.push_str(&format!(" WHERE {}", where_clause.serialize()));
        }

        serialized
    }

    /// Deserializa una cadena de texto en una instancia de `Delete`.
    pub fn deserialize(serialized: &str) -> Result<Self, CQLError> {
        let tokens: Vec<String> = QueryCoordinator::tokens_from_query(serialized);
        Self::new_from_tokens(tokens)
    }
}

#[cfg(test)]
mod tests {

    use super::Delete;
    use crate::{
        clauses::{condition::Condition, where_sql::Where},
        errors::CQLError,
        operator::Operator,
    };

    #[test]
    fn new_1_token() {
        let tokens = vec![String::from("DELETE")];
        let delete = Delete::new_from_tokens(tokens);
        assert_eq!(delete, Err(CQLError::InvalidSyntax));
    }

    #[test]
    fn new_2_token() {
        let tokens = vec![String::from("DELETE"), String::from("FROM")];
        let delete = Delete::new_from_tokens(tokens);
        assert_eq!(delete, Err(CQLError::InvalidSyntax));
    }

    #[test]
    fn new_without_where() {
        let tokens = vec![
            String::from("DELETE"),
            String::from("FROM"),
            String::from("table"),
        ];
        let delete = Delete::new_from_tokens(tokens).unwrap();
        assert_eq!(
            delete,
            Delete {
                table_name: String::from("table"),
                where_clause: None,
                columns: None
            }
        );
    }

    #[test]
    fn new_4_tokens() {
        let tokens = vec![
            String::from("DELETE"),
            String::from("FROM"),
            String::from("table"),
            String::from("WHERE"),
        ];
        let delete = Delete::new_from_tokens(tokens);
        assert_eq!(delete, Err(CQLError::InvalidSyntax));
    }

    #[test]
    fn new_with_where() {
        let tokens = vec![
            String::from("DELETE"),
            String::from("FROM"),
            String::from("table"),
            String::from("WHERE"),
            String::from("cantidad"),
            String::from(">"),
            String::from("1"),
        ];
        let delete = Delete::new_from_tokens(tokens).unwrap();
        assert_eq!(
            delete,
            Delete {
                table_name: String::from("table"),
                where_clause: Some(Where {
                    condition: Condition::Simple {
                        field: String::from("cantidad"),
                        operator: Operator::Greater,
                        value: String::from("1")
                    }
                }),
                columns: None
            }
        );
    }

    #[test]
    fn new_with_columns() {
        let tokens = vec![
            String::from("DELETE"),
            String::from("columna_a"),
            String::from("columna_b"),
            String::from("FROM"),
            String::from("table"),
            String::from("WHERE"),
            String::from("cantidad"),
            String::from(">"),
            String::from("1"),
        ];
        let delete = Delete::new_from_tokens(tokens).unwrap();
        assert_eq!(
            delete,
            Delete {
                table_name: String::from("table"),
                columns: Some(vec![String::from("columna_a"), String::from("columna_b")]),
                where_clause: Some(Where {
                    condition: Condition::Simple {
                        field: String::from("cantidad"),
                        operator: Operator::Greater,
                        value: String::from("1")
                    }
                }),
            }
        );
    }
}
