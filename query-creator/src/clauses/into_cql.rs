use crate::{errors::CQLError, utils::is_into};

/// Struct that represents the `INTO` SQL clause.
/// The `INTO` clause is used to specify the table name and columns in the `INSERT` clause.
///
/// # Fields
///
/// * `table_name` - The name of the table to insert data into.
/// * `columns` - The columns of the table to insert data into.
///
#[derive(Debug, PartialEq, Clone)]
pub struct Into {
    pub table_name: String,
    pub keyspace_used_name: String,
    pub columns: Vec<String>,
}

impl Into {
    /// Creates and returns a new `Into` instance from a vector of `&str` tokens.
    ///
    /// # Arguments
    ///
    /// * `tokens` - A vector of `&str` tokens that represent the `INTO` clause.
    ///
    /// The tokens should be in the following order: `INTO`, `table_name`, `columns`.
    /// The `columns` should be comma-separated and between parentheses.
    ///
    pub fn new_from_tokens(tokens: Vec<&str>) -> Result<Self, CQLError> {
        if tokens.len() < 3 {
            return Err(CQLError::InvalidSyntax);
        }
        let mut i = 0;
        let table_name;
        let keyspace_used_name: String;
        let mut columns: Vec<String> = Vec::new();

        if is_into(tokens[i]) {
            i += 1;
            let full_table_name = tokens[i].to_string();
            (keyspace_used_name, table_name) = if full_table_name.contains('.') {
                let parts: Vec<&str> = full_table_name.split('.').collect();
                (parts[0].to_string(), parts[1].to_string())
            } else {
                (String::new(), full_table_name.clone())
            };
            i += 1;

            let cols: Vec<String> = tokens[i].split(",").map(|c| c.trim().to_string()).collect();

            for col in cols {
                columns.push(col);
            }

            if columns.is_empty() {
                return Err(CQLError::InvalidSyntax);
            }
        } else {
            return Err(CQLError::InvalidSyntax);
        }

        Ok(Self {
            table_name,
            keyspace_used_name,
            columns,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::CQLError;

    #[test]
    fn test_new_from_tokens_valid_simple() {
        let tokens = vec!["INTO", "users", "id,name,age"];
        let into_clause = Into::new_from_tokens(tokens).unwrap();

        assert_eq!(into_clause.table_name, "users".to_string());
        assert_eq!(
            into_clause.columns,
            vec!["id".to_string(), "name".to_string(), "age".to_string()]
        );
    }

    #[test]
    fn test_new_from_tokens_valid_with_whitespace() {
        let tokens = vec!["INTO", "employees", "id, name , salary"];
        let into_clause = Into::new_from_tokens(tokens).unwrap();

        assert_eq!(into_clause.table_name, "employees".to_string());
        assert_eq!(
            into_clause.columns,
            vec!["id".to_string(), "name".to_string(), "salary".to_string()]
        );
    }

    #[test]
    fn test_new_from_tokens_missing_into_keyword() {
        let tokens = vec!["users", "id,name,age"];
        let result = Into::new_from_tokens(tokens);

        assert!(matches!(result, Err(CQLError::InvalidSyntax)));
    }

    #[test]
    fn test_new_from_tokens_missing_table_name() {
        let tokens = vec!["INTO", "id,name,age"];
        let result = Into::new_from_tokens(tokens);

        assert!(matches!(result, Err(CQLError::InvalidSyntax)));
    }
}
