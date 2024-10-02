use super::where_sql::Where;
use crate::errors::SqlError;
use crate::utils::{is_delete, is_from, is_where};

/// Struct that represents the `DELETE` SQL clause.
/// The `DELETE` clause is used to delete records from a table.
///
/// # Fields
///
/// - `table_name`: a `String` that holds the name of the table from which the records will be deleted.
/// - `where_clause`: an `Option<Where>` that holds the condition that the records must meet to be deleted. If it is `None`, all records will be deleted.
///
#[derive(PartialEq, Debug)]
pub struct Delete {
    pub table_name: String,
    pub where_clause: Option<Where>,
}

impl Delete {
    /// Creates and returns a new `Delete` instance from tokens.
    ///
    /// # Arguments
    ///
    /// - `tokens`: a `Vec<String>` that holds the tokens that form the `DELETE` clause.
    ///
    /// The tokens must be in the following order: `DELETE`, `FROM`, `table_name`, `WHERE`, `condition`.
    ///
    /// If the `WHERE` clause is not present, the `where_clause` field will be `None`.
    ///
    /// # Examples
    ///
    /// ```
    /// let tokens = vec![
    ///     String::from("DELETE"),
    ///     String::from("FROM"),
    ///     String::from("table"),
    /// ];
    /// let delete = Delete::new_from_tokens(tokens).unwrap();
    ///
    /// assert_eq!(
    ///    delete,
    ///     Delete {
    ///         table_name: String::from("table"),
    ///         where_clause: None
    ///     }
    /// );
    /// ```
    ///
    pub fn new_from_tokens(tokens: Vec<String>) -> Result<Self, SqlError> {
        if tokens.len() < 3 {
            return Err(SqlError::InvalidSyntax);
        }
        let mut where_tokens: Vec<&str> = Vec::new();

        let mut i = 0;
        let mut table_name = String::new();

        while i < tokens.len() {
            if i == 0 && !is_delete(&tokens[i]) || i == 1 && !is_from(&tokens[i]) {
                return Err(SqlError::InvalidSyntax);
            }
            if i == 1 && is_from(&tokens[i]) && i + 1 < tokens.len() {
                table_name = tokens[i + 1].to_string();
            }

            if i == 3 && is_where(&tokens[i]) {
                while i < tokens.len() {
                    where_tokens.push(tokens[i].as_str());
                    i += 1;
                }
            }
            i += 1;
        }

        if table_name.is_empty() {
            return Err(SqlError::InvalidSyntax);
        }

        let mut where_clause = None;

        if !where_tokens.is_empty() {
            where_clause = Some(Where::new_from_tokens(where_tokens)?);
        }

        Ok(Self {
            table_name,
            where_clause,
        })
    }
}

#[cfg(test)]
mod tests {

    use super::Delete;
    use crate::{
        clauses::{condition::Condition, where_sql::Where},
        errors::SqlError,
        operator::Operator,
    };

    #[test]
    fn new_1_token() {
        let tokens = vec![String::from("DELETE")];
        let delete = Delete::new_from_tokens(tokens);
        assert_eq!(delete, Err(SqlError::InvalidSyntax));
    }

    #[test]
    fn new_2_token() {
        let tokens = vec![String::from("DELETE"), String::from("FROM")];
        let delete = Delete::new_from_tokens(tokens);
        assert_eq!(delete, Err(SqlError::InvalidSyntax));
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
                where_clause: None
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
        assert_eq!(delete, Err(SqlError::InvalidSyntax));
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
            }
        );
    }
}
