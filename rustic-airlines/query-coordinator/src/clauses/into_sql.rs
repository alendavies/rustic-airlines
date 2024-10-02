use crate::{errors::SqlError, utils::is_into};

/// Struct that represents the `INTO` SQL clause.
/// The `INTO` clause is used to specify the table name and columns in the `INSERT` clause.
///
/// # Fields
///
/// * `table_name` - The name of the table to insert data into.
/// * `columns` - The columns of the table to insert data into.
///
#[derive(Debug, PartialEq)]
pub struct Into {
    pub table_name: String,
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
    pub fn new_from_tokens(tokens: Vec<&str>) -> Result<Self, SqlError> {
        if tokens.len() < 3 {
            return Err(SqlError::InvalidSyntax);
        }
        let mut i = 0;
        let table_name;
        let mut columns: Vec<String> = Vec::new();

        if is_into(tokens[i]) {
            i += 1;
            table_name = tokens[i].to_string();
            i += 1;

            let cols: Vec<String> = tokens[i].split(",").map(|c| c.trim().to_string()).collect();

            for col in cols {
                columns.push(col);
            }
        } else {
            return Err(SqlError::InvalidSyntax);
        }

        Ok(Self {
            table_name,
            columns,
        })
    }
}
