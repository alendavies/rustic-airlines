use super::{orderby_sql::OrderBy, where_sql::Where};
use crate::{
    errors::SqlError,
    utils::{is_by, is_from, is_order, is_select, is_where},
};

/// Struct that represents the `SELECT` SQL clause.
/// The `SELECT` clause is used to select data from a table.
///
/// # Fields
///
/// * `table_name` - The name of the table to select data from.
/// * `columns` - The columns to select from the table.
/// * `where_clause` - The `WHERE` clause to filter the result set.
/// * `orderby_clause` - The `ORDER BY` clause to sort the result set.
///
#[derive(Debug, PartialEq)]
pub struct Select {
    pub table_name: String,
    pub columns: Vec<String>,
    pub where_clause: Option<Where>,
    pub orderby_clause: Option<OrderBy>,
}

fn parse_columns<'a>(tokens: &'a [String], i: &mut usize) -> Result<Vec<&'a String>, SqlError> {
    let mut columns = Vec::new();
    if is_select(&tokens[*i]) {
        if *i < tokens.len() {
            *i += 1;
            while !is_from(&tokens[*i]) && *i < tokens.len() {
                columns.push(&tokens[*i]);
                *i += 1;
            }
        }
    } else {
        return Err(SqlError::InvalidSyntax);
    }
    Ok(columns)
}

fn parse_table_name(tokens: &[String], i: &mut usize) -> Result<String, SqlError> {
    if *i < tokens.len() && is_from(&tokens[*i]) {
        *i += 1;
        let table_name = tokens[*i].to_string();
        *i += 1;
        Ok(table_name)
    } else {
        Err(SqlError::InvalidSyntax)
    }
}

fn parse_where_and_orderby<'a>(
    tokens: &'a [String],
    i: &mut usize,
) -> Result<(Vec<&'a str>, Vec<&'a str>), SqlError> {
    let mut where_tokens = Vec::new();
    let mut orderby_tokens = Vec::new();

    if *i < tokens.len() {
        if is_where(&tokens[*i]) {
            while *i < tokens.len() && !is_order(&tokens[*i]) {
                where_tokens.push(tokens[*i].as_str());
                *i += 1;
            }
        }
        if *i < tokens.len() && is_order(&tokens[*i]) {
            orderby_tokens.push(tokens[*i].as_str());
            *i += 1;
            if *i < tokens.len() && is_by(&tokens[*i]) {
                while *i < tokens.len() {
                    orderby_tokens.push(tokens[*i].as_str());
                    *i += 1;
                }
            }
        }
    }
    Ok((where_tokens, orderby_tokens))
}

impl Select {
    /// Creates and returns a new `Select` instance from a vector of `String` tokens.
    ///
    /// # Arguments
    ///
    /// * `tokens` - A vector of `String` tokens that represent the `SELECT` clause.
    ///
    /// The tokens should be in the following order: `SELECT`, `columns`, `FROM`, `table_name`, `WHERE`, `condition`, `ORDER`, `BY`, `columns`, `order`.
    ///
    /// The `columns` should be comma-separated.
    ///
    pub fn new_from_tokens(tokens: Vec<String>) -> Result<Self, SqlError> {
        if tokens.len() < 4 {
            return Err(SqlError::InvalidSyntax);
        }

        let mut i = 0;

        let columns = parse_columns(&tokens, &mut i)?;
        let table_name = parse_table_name(&tokens, &mut i)?;

        if columns.is_empty() || table_name.is_empty() {
            return Err(SqlError::InvalidSyntax);
        }

        let (where_tokens, orderby_tokens) = parse_where_and_orderby(&tokens, &mut i)?;

        let where_clause = if !where_tokens.is_empty() {
            Some(Where::new_from_tokens(where_tokens)?)
        } else {
            None
        };

        let orderby_clause = if !orderby_tokens.is_empty() {
            Some(OrderBy::new_from_tokens(orderby_tokens)?)
        } else {
            None
        };

        Ok(Self {
            table_name,
            columns: columns.iter().map(|c| c.to_string()).collect(),
            where_clause,
            orderby_clause,
        })
    }
}

#[cfg(test)]
mod tests {

    use super::Select;
    use crate::{
        clauses::{condition::Condition, orderby_sql::OrderBy},
        errors::SqlError,
        operator::Operator,
    };

    #[test]
    fn new_1_tokens() {
        let tokens = vec![String::from("SELECT")];
        let select = Select::new_from_tokens(tokens);
        assert_eq!(select, Err(SqlError::InvalidSyntax));
    }

    #[test]
    fn new_2_tokens() {
        let tokens = vec![String::from("SELECT"), String::from("col")];
        let select = Select::new_from_tokens(tokens);
        assert_eq!(select, Err(SqlError::InvalidSyntax));
    }
    #[test]
    fn new_3_tokens() {
        let tokens = vec![
            String::from("SELECT"),
            String::from("col"),
            String::from("FROM"),
        ];
        let select = Select::new_from_tokens(tokens);
        assert_eq!(select, Err(SqlError::InvalidSyntax));
    }

    #[test]
    fn new_4_tokens() {
        let tokens = vec![
            String::from("SELECT"),
            String::from("col"),
            String::from("FROM"),
            String::from("table"),
        ];
        let select = Select::new_from_tokens(tokens).unwrap();
        assert_eq!(select.columns, ["col"]);
        assert_eq!(select.table_name, "table");
        assert_eq!(select.where_clause, None);
        assert_eq!(select.orderby_clause, None);
    }

    #[test]
    fn new_with_where() {
        let tokens = vec![
            String::from("SELECT"),
            String::from("col"),
            String::from("FROM"),
            String::from("table"),
            String::from("WHERE"),
            String::from("cantidad"),
            String::from(">"),
            String::from("1"),
        ];
        let select = Select::new_from_tokens(tokens).unwrap();
        assert_eq!(select.columns, ["col"]);
        assert_eq!(select.table_name, "table");
        let where_clause = select.where_clause.unwrap();
        assert_eq!(
            where_clause.condition,
            Condition::Simple {
                field: String::from("cantidad"),
                operator: Operator::Greater,
                value: String::from("1"),
            }
        );
        assert_eq!(select.orderby_clause, None);
    }

    #[test]
    fn new_with_orderby() {
        let tokens = vec![
            String::from("SELECT"),
            String::from("col"),
            String::from("FROM"),
            String::from("table"),
            String::from("ORDER"),
            String::from("BY"),
            String::from("cantidad"),
            String::from("DESC"),
        ];
        let select = Select::new_from_tokens(tokens).unwrap();
        assert_eq!(select.columns, ["col"]);
        assert_eq!(select.table_name, "table");
        let orderby_clause = select.orderby_clause.unwrap();
        assert_eq!(
            orderby_clause,
            OrderBy {
                columns: vec![String::from("cantidad")],
                order: String::from("DESC")
            }
        );
        assert_eq!(select.where_clause, None);
    }

    #[test]
    fn new_with_where_orderby() {
        let tokens = vec![
            String::from("SELECT"),
            String::from("col"),
            String::from("FROM"),
            String::from("table"),
            String::from("WHERE"),
            String::from("cantidad"),
            String::from(">"),
            String::from("1"),
            String::from("ORDER"),
            String::from("BY"),
            String::from("email"),
        ];
        let select = Select::new_from_tokens(tokens).unwrap();
        assert_eq!(select.columns, ["col"]);
        assert_eq!(select.table_name, "table");
        let where_clause = select.where_clause.unwrap();
        assert_eq!(
            where_clause.condition,
            Condition::Simple {
                field: String::from("cantidad"),
                operator: Operator::Greater,
                value: String::from("1"),
            }
        );
        let orderby_clause = select.orderby_clause.unwrap();
        let mut columns = Vec::new();
        columns.push(String::from("email"));
        assert_eq!(
            orderby_clause,
            OrderBy {
                columns,
                order: String::new()
            }
        );
    }
}
