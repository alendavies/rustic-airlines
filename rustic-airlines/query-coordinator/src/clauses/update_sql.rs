use super::set_sql::Set;
use super::where_sql::Where;
use crate::errors::CQLError;
use crate::utils::{is_set, is_update, is_where};

/// Struct representing the `UPDATE` SQL clause.
/// The `UPDATE` clause is used to modify records in a table.
///
/// # Fields
///
/// * `table_name` - The name of the table to be updated.
/// * `set_clause` - The set clause to be applied.
/// * `where_clause` - The where clause to be applied.
///
#[derive(PartialEq, Debug)]
pub struct Update {
    pub table_name: String,
    pub set_clause: Set,
    pub where_clause: Option<Where>,
}

impl Update {
    /// Creates and returns a new `Update` instance from a vector of tokens.
    ///
    /// # Arguments
    ///
    /// * `tokens` - A vector of tokens that can be used to build a `Update` instance.
    ///
    /// The tokens should be in the following order: `UPDATE`, `table`, `SET`, `column`, `=`, `value`.
    ///
    /// # Examples
    ///
    /// ```
    /// let tokens = vec!["UPDATE", "table", "SET", "nombre", "=", "Alen"];
    /// let update_from_tokens = Update::new_from_tokens(tokens).unwrap();
    /// let update = Update {
    ///     table_name: "table".to_string(),
    ///     set_clause: Set(vec![("nombre".to_string(), "Alen".to_string())]),
    ///     where_clause: None,
    /// };
    ///
    /// assert_eq!(update_from_tokens, update);
    /// ```
    pub fn new_from_tokens(tokens: Vec<String>) -> Result<Self, CQLError> {
        if tokens.len() < 6 {
            return Err(CQLError::InvalidSyntax);
        }
        let mut where_tokens = Vec::new();
        let mut set_tokens = Vec::new();
        let mut table_name = String::new();

        let mut i = 0;

        while i < tokens.len() {
            if i == 0 && !is_update(&tokens[i]) || i == 2 && !is_set(&tokens[i]) {
                return Err(CQLError::InvalidSyntax);
            }

            if i == 0 && is_update(&tokens[i]) && i + 1 < tokens.len() {
                table_name = tokens[i + 1].to_string();
            }

            if i == 2 && is_set(&tokens[i]) {
                while i < tokens.len() && !is_where(&tokens[i]) {
                    set_tokens.push(tokens[i].as_str());
                    i += 1;
                }
                if i < tokens.len() && is_where(&tokens[i]) {
                    while i < tokens.len() {
                        where_tokens.push(tokens[i].as_str());
                        i += 1;
                    }
                }
            }
            i += 1;
        }

        if table_name.is_empty() || set_tokens.is_empty() {
            return Err(CQLError::InvalidSyntax);
        }

        let mut where_clause = None;

        if !where_tokens.is_empty() {
            where_clause = Some(Where::new_from_tokens(where_tokens)?);
        }

        let set_clause = Set::new_from_tokens(set_tokens)?;

        Ok(Self {
            table_name,
            where_clause,
            set_clause,
        })
    }
}

#[cfg(test)]
mod tests {

    use crate::{
        clauses::{condition::Condition, set_sql::Set, update_sql::Update, where_sql::Where},
        errors::CQLError,
        operator::Operator,
    };

    #[test]
    fn new_1_token() {
        let tokens = vec![String::from("UPDATE")];
        let update = Update::new_from_tokens(tokens);
        assert_eq!(update, Err(CQLError::InvalidSyntax));
    }

    #[test]
    fn new_3_tokens() {
        let tokens = vec![
            String::from("UPDATE"),
            String::from("table"),
            String::from("SET"),
        ];
        let update = Update::new_from_tokens(tokens);
        assert_eq!(update, Err(CQLError::InvalidSyntax));
    }

    #[test]
    fn new_without_where() {
        let tokens = vec![
            String::from("UPDATE"),
            String::from("table"),
            String::from("SET"),
            String::from("nombre"),
            String::from("="),
            String::from("Alen"),
        ];
        let update = Update::new_from_tokens(tokens).unwrap();
        assert_eq!(
            update,
            Update {
                table_name: String::from("table"),
                set_clause: Set(vec![(String::from("nombre"), String::from("Alen"))]),
                where_clause: None
            }
        );
    }

    #[test]
    fn new_with_where() {
        let tokens = vec![
            String::from("UPDATE"),
            String::from("table"),
            String::from("SET"),
            String::from("nombre"),
            String::from("="),
            String::from("Alen"),
            String::from("WHERE"),
            String::from("edad"),
            String::from("<"),
            String::from("30"),
        ];
        let update = Update::new_from_tokens(tokens).unwrap();
        assert_eq!(
            update,
            Update {
                table_name: String::from("table"),
                set_clause: Set(vec![(String::from("nombre"), String::from("Alen"))]),
                where_clause: Some(Where {
                    condition: Condition::Simple {
                        field: String::from("edad"),
                        operator: Operator::Lesser,
                        value: String::from("30"),
                    },
                }),
            }
        );
    }
}
