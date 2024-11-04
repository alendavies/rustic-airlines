use super::{order_by_cql::OrderBy, where_cql::Where};
use crate::QueryCreator;
use crate::{
    errors::CQLError,
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
#[derive(Debug, PartialEq, Clone)]
pub struct Select {
    pub table_name: String,
    pub keyspace_used_name: String,
    pub columns: Vec<String>,
    pub where_clause: Option<Where>,
    pub orderby_clause: Option<OrderBy>,
}

fn parse_columns<'a>(tokens: &'a [String], i: &mut usize) -> Result<Vec<&'a String>, CQLError> {
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
        return Err(CQLError::InvalidSyntax);
    }
    Ok(columns)
}

fn parse_table_name(tokens: &[String], i: &mut usize) -> Result<String, CQLError> {
    if *i < tokens.len() && is_from(&tokens[*i]) {
        *i += 1;
        let table_name = tokens[*i].to_string();
        *i += 1;
        Ok(table_name)
    } else {
        Err(CQLError::InvalidSyntax)
    }
}

fn parse_where_and_orderby<'a>(
    tokens: &'a [String],
    i: &mut usize,
) -> Result<(Vec<&'a str>, Vec<&'a str>), CQLError> {
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
    pub fn new_from_tokens(tokens: Vec<String>) -> Result<Self, CQLError> {
        if tokens.len() < 4 {
            return Err(CQLError::InvalidSyntax);
        }

        let mut i = 0;

        let columns = parse_columns(&tokens, &mut i)?;
        let full_table_name = parse_table_name(&tokens, &mut i)?;
        
        let (keyspace_used_name, table_name) = if full_table_name.contains('.') {
            let parts: Vec<&str> = full_table_name.split('.').collect();
            (parts[0].to_string(), parts[1].to_string())
        } else {
            (String::new(), full_table_name.clone())
        };

        if columns.is_empty() || table_name.is_empty() {
            return Err(CQLError::InvalidSyntax);
        }

        let (where_tokens, orderby_tokens) = parse_where_and_orderby(&tokens, &mut i)?;

        let where_clause = if !where_tokens.is_empty() {
            Some(Where::new_from_tokens(where_tokens)?)
        } else {
            None
        };

        let order_by_tokens = orderby_tokens.iter().map(|s| s.to_string()).collect();

        let orderby_clause = if !orderby_tokens.is_empty() {
            Some(OrderBy::new_from_tokens(order_by_tokens)?)
        } else {
            None
        };

        Ok(Self {
            table_name,
            keyspace_used_name,
            columns: columns.iter().map(|c| c.to_string()).collect(),
            where_clause,
            orderby_clause,
        })
    }

    /// Serializa la consulta `Select` a un `String`.
    pub fn serialize(&self) -> String {

        let table_name_str = if !self.keyspace_used_name.is_empty() {
            format!("{}.{}", self.keyspace_used_name, self.table_name)
        } else {
            self.table_name.clone()
        };
        let mut result = format!("SELECT {} FROM {}", self.columns.join(","), table_name_str);

        // Agrega el `WHERE` si existe
        if let Some(where_clause) = &self.where_clause {
            result.push_str(&format!(" WHERE {}", where_clause.serialize()));
        }

        // Agrega el `ORDER BY` si existe
        if let Some(orderby_clause) = &self.orderby_clause {
            result.push_str(&format!(" ORDER BY {}", orderby_clause.serialize()));
        }

        result
    }

    pub fn deserialize(query: &str) -> Result<Self, CQLError> {
        let tokens = QueryCreator::tokens_from_query(query);
        Self::new_from_tokens(tokens)
    }
}

#[cfg(test)]
mod tests {

    use super::Select;
    use crate::{
        clauses::{condition::Condition, order_by_cql::OrderBy},
        errors::CQLError,
        operator::Operator,
    };

    #[test]
    fn new_1_tokens() {
        let tokens = vec![String::from("SELECT")];
        let select = Select::new_from_tokens(tokens);
        assert_eq!(select, Err(CQLError::InvalidSyntax));
    }

    #[test]
    fn new_2_tokens() {
        let tokens = vec![String::from("SELECT"), String::from("col")];
        let select = Select::new_from_tokens(tokens);
        assert_eq!(select, Err(CQLError::InvalidSyntax));
    }
    #[test]
    fn new_3_tokens() {
        let tokens = vec![
            String::from("SELECT"),
            String::from("col"),
            String::from("FROM"),
        ];
        let select = Select::new_from_tokens(tokens);
        assert_eq!(select, Err(CQLError::InvalidSyntax));
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
    fn new_with_keyspace() {
        let tokens = vec![
            String::from("SELECT"),
            String::from("col"),
            String::from("FROM"),
            String::from("keyspace.table"),
        ];
        let select = Select::new_from_tokens(tokens).unwrap();
        assert_eq!(select.columns, ["col"]);
        assert_eq!(select.table_name, "table");
        assert_eq!(select.keyspace_used_name, "keyspace");
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
