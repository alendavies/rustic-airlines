use super::{condition::Condition, recursive_parser::parse_condition};
use crate::errors::CQLError;

/// Struct representing the `WHERE` SQL clause.
///
/// The `WHERE` clause is used to filter records that match a certain condition.
///
/// # Fields
///
/// * `condition` - The condition to be evaluated.
///
#[derive(Debug, PartialEq, Clone)]
pub struct Where {
    pub condition: Condition,
}

impl Where {
    /// Creates and returns a new `Where` instance from a vector of tokens.
    ///
    /// # Arguments
    ///
    /// * `tokens` - A vector of tokens that can be used to build a `Where` instance.
    ///
    /// The tokens should be in the following order: `WHERE`, `column`, `operator`, `value` in the case of a simple condition, and `WHERE`, `condition`, `AND` or `OR`, `condition` for a complex condition.
    ///
    /// # Examples
    ///
    /// ```
    /// let tokens = vec!["WHERE", "age", ">", "18"];
    /// let where_from_tokens = Where::new_from_tokens(tokens).unwrap();
    /// let where_clause = Where {
    ///    condition: Condition::Simple {
    ///         column: "age".to_string(),
    ///         operator: Operator::Greater,
    ///         value: "18".to_string(),
    ///     },
    /// };
    ///
    /// assert_eq!(where_from_tokens, where_clause);
    /// ```
    ///
    pub fn new_from_tokens(tokens: Vec<&str>) -> Result<Self, CQLError> {
        if tokens.len() < 4 {
            return Err(CQLError::InvalidSyntax);
        }
        let mut pos = 1;
        let condition = parse_condition(&tokens, &mut pos)?;

        Ok(Self { condition })
    }
    pub fn serialize(&self) -> String {
        self.condition.serialize()
    }
}
