use super::into_sql::Into;
use crate::errors::SqlError;
use crate::utils::{is_insert, is_values};

/// Struct that represents the `INSERT` SQL clause.
/// The `INSERT` clause is used to insert new records into a table.
///
/// # Fields
///
/// * `values` - A vector of strings that contains the values to be inserted.
/// * `into_clause` - An `Into` struct that contains the table name and columns.
///
#[derive(Debug, PartialEq)]
pub struct Insert {
    pub values: Vec<String>,
    pub into_clause: Into,
}

impl Insert {
    /// Creates and returns a new `Insert` instance from a vector of tokens.
    ///
    /// # Arguments
    ///
    /// * `tokens` - A vector of strings that contains the tokens to be parsed.
    ///
    /// The tokens should be in the following order: `INSERT`, `INTO`, `table_name`, `column_names`, `VALUES`, `values`.
    ///
    /// The `column_names` and `values` should be comma-separated and between parentheses.
    ///
    /// If a pair of col, value is missing for a column in the table, the value will be an empty string for that column.
    ///
    /// # Examples
    ///
    /// ```
    /// let tokens = vec![
    ///     String::from("INSERT"),
    ///     String::from("INTO"),
    ///     String::from("table"),
    ///     String::from("name, age"),
    ///     String::from("VALUES"),
    ///     String::from("Alen, 25"),
    /// ];
    ///
    /// let insert = Insert::new_from_tokens(tokens).unwrap();
    ///
    /// assert_eq!(
    ///     insert,
    ///     Insert {
    ///         values: vec![String::from("Alen"), String::from("25")],
    ///         into_clause: Into {
    ///             table_name: String::from("table"),
    ///             columns: vec![String::from("name"), String::from("age")]
    ///         }
    ///     }
    /// );
    /// ```
    ///
    pub fn new_from_tokens(tokens: Vec<String>) -> Result<Self, SqlError> {
        if tokens.len() < 6 {
            return Err(SqlError::InvalidSyntax);
        }
        let mut into_tokens: Vec<&str> = Vec::new();
        let mut values: Vec<String> = Vec::new();

        let mut i = 0;

        if is_insert(&tokens[i]) {
            i += 1;
            while !is_values(&tokens[i]) && i < tokens.len() {
                into_tokens.push(tokens[i].as_str());
                i += 1;
            }
        }
        if is_values(&tokens[i]) {
            i += 1;

            let vals: Vec<String> = tokens[i]
                .replace("\'", "")
                .split(",")
                .map(|c| c.trim().to_string())
                .collect();

            for val in vals {
                values.push(val);
            }
        }

        if into_tokens.is_empty() || values.is_empty() {
            return Err(SqlError::InvalidSyntax);
        }

        let into_clause = Into::new_from_tokens(into_tokens)?;

        Ok(Self {
            values,
            into_clause,
        })
    }
}

#[cfg(test)]
mod test {
    use crate::errors::SqlError;

    #[test]
    fn new_1_token() {
        let tokens = vec![String::from("INSERT")];
        let result = super::Insert::new_from_tokens(tokens);
        assert_eq!(result, Err(SqlError::InvalidSyntax));
    }

    #[test]
    fn new_3_tokens() {
        let tokens = vec![
            String::from("INSERT"),
            String::from("INTO"),
            String::from("table"),
        ];

        let result = super::Insert::new_from_tokens(tokens);
        assert_eq!(result, Err(SqlError::InvalidSyntax));
    }

    #[test]
    fn new_6_tokens() {
        let tokens = vec![
            String::from("INSERT"),
            String::from("INTO"),
            String::from("table"),
            String::from("name"),
            String::from("VALUES"),
            String::from("Alen"),
        ];
        let result = super::Insert::new_from_tokens(tokens).unwrap();
        assert_eq!(
            result,
            super::Insert {
                values: vec![String::from("Alen")],
                into_clause: super::Into {
                    table_name: String::from("table"),
                    columns: vec![String::from("name")]
                }
            }
        );
    }

    #[test]
    fn new_more_values() {
        let tokens = vec![
            String::from("INSERT"),
            String::from("INTO"),
            String::from("table"),
            String::from("name, age"),
            String::from("VALUES"),
            String::from("Alen, 25"),
        ];
        let result = super::Insert::new_from_tokens(tokens).unwrap();
        assert_eq!(
            result,
            super::Insert {
                values: vec![String::from("Alen"), String::from("25")],
                into_clause: super::Into {
                    table_name: String::from("table"),
                    columns: vec![String::from("name"), String::from("age")]
                }
            }
        );
    }
}
