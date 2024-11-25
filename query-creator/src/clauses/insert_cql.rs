use super::into_cql::Into;
use crate::errors::CQLError;
use crate::utils::{is_insert, is_values};
use crate::QueryCreator;

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
    pub if_not_exists: bool,
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
            i += 1;
        }

        let mut if_not_exists = false;

        if i < tokens.len()
            && tokens[i] == "IF"
            && tokens[i + 1] == "NOT"
            && tokens[i + 2] == "EXISTS"
        {
            if_not_exists = true;
        }

        if into_tokens.is_empty() || values.is_empty() {
            return Err(CQLError::InvalidSyntax);
        }

        let into_clause = Into::new_from_tokens(into_tokens)?;

        Ok(Self {
            values,
            into_clause,
            if_not_exists,
        })
    }

    /// Serializes the `Insert` struct into a plain string representation.
    pub fn serialize(&self) -> String {
        let columns = self.into_clause.columns.join(", ");
        let values = self.values.join(", ");

        let if_not_exists = if self.if_not_exists {
            " IF NOT EXISTS"
        } else {
            ""
        };

        let table_name_str = if !self.into_clause.keyspace_used_name.is_empty() {
            format!(
                "{}.{}",
                self.into_clause.keyspace_used_name, self.into_clause.table_name
            )
        } else {
            self.into_clause.table_name.clone()
        };

        format!(
            "INSERT INTO {} ({}) VALUES ({}){}",
            table_name_str, columns, values, if_not_exists
        )
    }

    /// Deserializes a plain string representation into an `Insert` struct.
    ///
    /// The expected format for the string is:
    ///
    /// `"INSERT INTO table_name (column1, column2) VALUES (value1, value2) [IF NOT EXISTS]"`
    pub fn deserialize(s: &str) -> Result<Self, CQLError> {
        let tokens: Vec<String> = QueryCreator::tokens_from_query(s);
        Self::new_from_tokens(tokens)
    }
}

#[cfg(test)]
mod test {
    use crate::{clauses::into_cql, errors::CQLError, Insert};

    #[test]
    fn serialize_basic_insert() {
        let insert = Insert {
            values: vec![String::from("Alen"), String::from("25")],
            into_clause: into_cql::Into {
                table_name: String::from("keyspace.table"),
                keyspace_used_name: String::new(),
                columns: vec![String::from("name"), String::from("age")],
            },
            if_not_exists: false,
        };

        let serialized = insert.serialize();
        assert_eq!(
            serialized,
            "INSERT INTO keyspace.table (name, age) VALUES (Alen, 25)"
        );
    }

    #[test]
    fn serialize_insert_if_not_exists() {
        let insert = Insert {
            values: vec![String::from("Alen"), String::from("25")],
            into_clause: into_cql::Into {
                table_name: String::from("table"),
                keyspace_used_name: String::new(),
                columns: vec![String::from("name"), String::from("age")],
            },
            if_not_exists: true,
        };

        let serialized = insert.serialize();
        assert_eq!(
            serialized,
            "INSERT INTO table (name, age) VALUES (Alen, 25) IF NOT EXISTS"
        );
    }

    #[test]
    fn deserialize_basic_insert() {
        let s = "INSERT INTO table (name, age) VALUES (Alen, 25)";
        let deserialized = Insert::deserialize(s).unwrap();

        assert_eq!(
            deserialized,
            Insert {
                values: vec![String::from("Alen"), String::from("25")],
                into_clause: into_cql::Into {
                    table_name: String::from("table"),
                    keyspace_used_name: String::new(),
                    columns: vec![String::from("name"), String::from("age")],
                },
                if_not_exists: false,
            }
        );
    }

    #[test]
    fn deserialize_insert_if_not_exists() {
        let s = "INSERT INTO table (name, age) VALUES (Alen, 25) IF NOT EXISTS";
        let deserialized = Insert::deserialize(s).unwrap();

        assert_eq!(
            deserialized,
            Insert {
                values: vec![String::from("Alen"), String::from("25")],
                into_clause: into_cql::Into {
                    table_name: String::from("table"),
                    keyspace_used_name: String::new(),
                    columns: vec![String::from("name"), String::from("age")],
                },
                if_not_exists: true,
            }
        );
    }

    #[test]
    fn deserialize_invalid_syntax_missing_values() {
        let s = "INSERT INTO table (name, age)";
        let deserialized = Insert::deserialize(s);
        assert_eq!(deserialized, Err(CQLError::InvalidSyntax));
    }

    #[test]
    fn deserialize_invalid_syntax_incorrect_format() {
        let s = "INSERT INTO table VALUES (Alen, 25)";
        let deserialized = Insert::deserialize(s);
        assert_eq!(deserialized, Err(CQLError::InvalidSyntax));
    }
}
