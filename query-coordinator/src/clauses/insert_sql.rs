use super::into_sql::Into;
use crate::errors::CQLError;
use crate::utils::{is_insert, is_values};

/// Struct that represents the `INSERT` SQL clause.
/// The `INSERT` clause is used to insert new records into a table.
///
/// # Fields
///
/// * `values` - A vector of strings that contains the values to be inserted.
/// * `into_clause` - An `Into` struct that contains the table name and columns.
///
#[derive(Debug, PartialEq, Clone)]
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
    pub fn new_from_tokens(tokens: Vec<String>) -> Result<Self, CQLError> {
        if tokens.len() < 6 {
            return Err(CQLError::InvalidSyntax);
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
            return Err(CQLError::InvalidSyntax);
        }

        let into_clause = Into::new_from_tokens(into_tokens)?;

        Ok(Self {
            values,
            into_clause,
        })
    }

     /// Serializes the `Insert` struct into a JSON-like string representation.
     pub fn serialize(&self) -> String {
        let values = self
            .values
            .iter()
            .map(|v| format!("\"{}\"", v))
            .collect::<Vec<String>>()
            .join(", ");

        let columns = self
            .into_clause
            .columns
            .iter()
            .map(|c| format!("\"{}\"", c))
            .collect::<Vec<String>>()
            .join(", ");

        format!(
            "{{ \"into_clause\": {{ \"table_name\": \"{}\", \"columns\": [{}] }}, \"values\": [{}] }}",
            self.into_clause.table_name, columns, values
        )
    }

    /// Deserializes a JSON-like string into an `Insert` struct.
    ///
    /// The string should have the format:
    ///
    /// `{ "into_clause": { "table_name": "table", "columns": ["name", "age"] }, "values": ["Alen", "25"] }`
    pub fn deserialize(s: &str) -> Result<Self, CQLError> {
        // Remove outer curly braces and split into parts
        let trimmed = s.trim().trim_start_matches('{').trim_end_matches('}');
        let parts: Vec<&str> = trimmed.split(", \"values\": ").collect();

        if parts.len() != 2 {
            return Err(CQLError::InvalidSyntax);
        }

        // Deserialize the `into_clause`
        let into_part = parts[0]
            .trim()
            .trim_start_matches("\"into_clause\": {")
            .trim_end_matches('}');
        let into_parts: Vec<&str> = into_part.split(", \"columns\": ").collect();

        if into_parts.len() != 2 {
            return Err(CQLError::InvalidSyntax);
        }

        let table_name = into_parts[0]
            .trim()
            .trim_start_matches("\"table_name\": \"")
            .trim_end_matches('\"')
            .to_string();

        let columns_str = into_parts[1]
            .trim()
            .trim_start_matches('[')
            .trim_end_matches(']');
        let columns: Vec<String> = columns_str
            .split(',')
            .map(|c| c.trim().trim_matches('\"').to_string())
            .collect();

        // Deserialize the `values`
        let values_str = parts[1].trim().trim_start_matches('[').trim_end_matches(']');
        let values: Vec<String> = values_str
            .split(',')
            .map(|v| v.trim().trim_matches('\"').to_string())
            .collect();

        Ok(Insert {
            values,
            into_clause: Into {
                table_name,
                columns,
            },
        })
    }
}

#[cfg(test)]
mod test {
    use crate::errors::CQLError;

    #[test]
    fn new_1_token() {
        let tokens = vec![String::from("INSERT")];
        let result = super::Insert::new_from_tokens(tokens);
        assert_eq!(result, Err(CQLError::InvalidSyntax));
    }

    #[test]
    fn new_3_tokens() {
        let tokens = vec![
            String::from("INSERT"),
            String::from("INTO"),
            String::from("table"),
        ];

        let result = super::Insert::new_from_tokens(tokens);
        assert_eq!(result, Err(CQLError::InvalidSyntax));
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
