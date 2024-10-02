use crate::{errors::SqlError, utils::is_set};

/// Struct representing the `SET` SQL clause.
///
/// The `SET` clause is used in an `UPDATE` statement to set new values to columns.
///
/// # Fields
///
/// * A vector of tuples containing the column name and the new value.
///
#[derive(PartialEq, Debug)]
pub struct Set(pub Vec<(String, String)>);

impl Set {
    /// Creates and returns a new `Set` instance from a vector of tokens.
    ///
    /// # Arguments
    ///
    /// * `tokens` - A vector of tokens that can be used to build a `Set` instance.
    ///
    /// The tokens should be in the following order: `SET`, `column`, `=`, `value`.
    ///
    /// # Examples
    ///
    /// ```
    /// let tokens = vec!["SET", "age", "=", "18"];
    /// let set_from_tokens = Set::new_from_tokens(tokens).unwrap();
    /// let set_clause = Set(vec![("age".to_string(), "18".to_string())]);
    ///
    /// assert_eq!(set_from_tokens, set_clause);
    /// ```
    ///
    pub fn new_from_tokens(tokens: Vec<&str>) -> Result<Self, SqlError> {
        let mut set = Vec::new();
        let mut i = 0;

        if !is_set(tokens[i]) || !tokens.contains(&"=") {
            return Err(SqlError::InvalidSyntax);
        }
        i += 1;

        while i < tokens.len() {
            if tokens[i] == "=" && i + 1 < tokens.len() {
                set.push((tokens[i - 1].to_string(), tokens[i + 1].to_string()));
            }
            i += 1;
        }

        Ok(Self(set))
    }
}
