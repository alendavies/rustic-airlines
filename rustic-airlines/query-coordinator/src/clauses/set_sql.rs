use crate::{errors::CQLError, utils::is_set};

/// Struct representing the `SET` SQL clause.
///
/// The `SET` clause is used in an `UPDATE` statement to set new values to columns.
///
/// # Fields
///
/// * A vector of tuples containing the column name and the new value.
///
#[derive(PartialEq, Debug, Clone)]
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
    /// 
    // Método para obtener una referencia al vector interno
    pub fn get_pairs(&self) -> &Vec<(String, String)> {
        &self.0
    }
    pub fn new_from_tokens(tokens: Vec<&str>) -> Result<Self, CQLError> {
        let mut set = Vec::new();
        let mut i = 0;

        if !is_set(tokens[i]) || !tokens.contains(&"=") {
            return Err(CQLError::InvalidSyntax);
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
    pub fn serialize(&self) -> String {
        self.0.iter()
            .map(|(col, val)| {
                let formatted_value = if val.parse::<f64>().is_ok() {
                    val.clone() // Es un número, se deja sin comillas
                } else {
                    format!("'{}'", val) // No es un número, se envuelve entre comillas
                };
                format!("{} = {}", col, formatted_value)
            })
            .collect::<Vec<String>>()
            .join(", ")
    }
    

}
