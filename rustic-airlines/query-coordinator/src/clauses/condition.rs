use crate::{
    errors::SqlError, logical_operator::LogicalOperator, operator::Operator, utils::is_number,
};
use std::collections::HashMap;

/// Enum for the conditions used in the `WHERE` clause.
///
/// - `Simple`: Simple condition with a field, operator and value.
/// - `Complex`: Complex condition with a left condition, logical operator and right condition.
///
#[derive(Debug, PartialEq)]
pub enum Condition {
    Simple {
        field: String,
        operator: Operator,
        value: String,
    },
    Complex {
        left: Option<Box<Condition>>, // Opcional para el caso de 'Not'
        operator: LogicalOperator,
        right: Box<Condition>,
    },
}

impl Condition {
    /// Creates a new `Condition` with a simple condition from tokens.
    ///
    /// # Arguments
    ///
    /// * `tokens` - A slice of `&str` with the tokens of the condition.
    /// * `pos` - A mutable reference to `usize` with the position of the tokens.
    ///
    /// The tokens must be in the following order: `field`, `operator`, `value`.
    ///
    /// # Examples
    ///
    /// ```
    /// let tokens = vec!["age", ">", "18"];
    /// let pos = 0;
    /// let condition = Condition::new_simple_from_tokens(&tokens, &mut pos).unwrap();
    /// assert_eq!(condition,
    ///     Condition::Simple {
    ///         field: String::from("age"),
    ///         operator: Operator::Greater,
    ///         value: String::from("18")
    ///     })
    ///
    /// ```
    ///
    pub fn new_simple_from_tokens(tokens: &[&str], pos: &mut usize) -> Result<Self, SqlError> {
        if let Some(field) = tokens.get(*pos) {
            *pos += 1;

            if let Some(operator) = tokens.get(*pos) {
                *pos += 1;

                if let Some(value) = tokens.get(*pos) {
                    *pos += 1;
                    Ok(Condition::new_simple(field, operator, value)?)
                } else {
                    Err(SqlError::InvalidSyntax)
                }
            } else {
                Err(SqlError::InvalidSyntax)
            }
        } else {
            Err(SqlError::InvalidSyntax)
        }
    }

    fn new_simple(field: &str, operator: &str, value: &str) -> Result<Self, SqlError> {
        let op = match operator {
            "=" => Operator::Equal,
            ">" => Operator::Greater,
            "<" => Operator::Lesser,
            _ => return Err(SqlError::InvalidSyntax),
        };

        Ok(Condition::Simple {
            field: field.to_string(),
            operator: op,
            value: value.to_string(),
        })
    }

    /// Creates a new `Condition` with a complex condition.
    ///
    /// # Arguments
    ///
    /// * `left` - An optional `Condition` with the left condition.
    /// * `operator` - A `LogicalOperator` with the logical operator.
    /// * `right` - A `Condition` with the right condition.
    ///
    /// # Examples
    ///
    /// ```
    /// let left = Condition::Simple {
    ///     field: String::from("age"),
    ///     operator: Operator::Greater,
    ///     value: String::from("18"),
    /// };
    /// let right = Condition::Simple {
    ///     field: String::from("city"),
    ///     operator: Operator::Equal,
    ///     value: String::from("Gaiman"),
    /// };
    /// let complex = Condition::new_complex(Some(left), LogicalOperator::And, right);
    ///
    /// assert_eq!(complex,
    ///    Condition::Complex {
    ///         left: Some(Box::new(Condition::Simple {
    ///                     field: String::from("age"),
    ///                     operator: Operator::Greater,
    ///                     value: String::from("18"),
    ///          })),
    ///         operator: LogicalOperator::And,
    ///         right: Box::new(Condition::Simple {
    ///                     field: String::from("city"),
    ///                     operator: Operator::Equal,
    ///                     value: String::from("Gaiman"),
    ///          })
    /// })
    /// ```
    ///
    pub fn new_complex(
        left: Option<Condition>,
        operator: LogicalOperator,
        right: Condition,
    ) -> Self {
        Condition::Complex {
            left: left.map(Box::new),
            operator,
            right: Box::new(right),
        }
    }

    /// Executes the condition on the given register.
    /// Returns a bool with the result of the condition.
    ///
    /// # Arguments
    ///
    /// * `register` - A reference to a `HashMap<String, String>` with the register to evaluate.
    ///
    pub fn execute(&self, register: &HashMap<String, String>) -> Result<bool, SqlError> {
        let op_result: Result<bool, SqlError> = match &self {
            Condition::Simple {
                field,
                operator,
                value,
            } => {
                let y = value;
                if let Some(x) = register.get(field) {
                    if is_number(y) && !is_number(x) || !is_number(y) && is_number(x) {
                        return Err(SqlError::InvalidSyntax);
                    }
                    match operator {
                        Operator::Lesser => Ok(x < y),
                        Operator::Greater => Ok(x > y),
                        Operator::Equal => Ok(x == y),
                    }
                } else {
                    Err(SqlError::Error)
                }
            }
            Condition::Complex {
                left,
                operator,
                right,
            } => match operator {
                LogicalOperator::Not => {
                    let result = right.execute(register)?;
                    Ok(!result)
                }
                LogicalOperator::Or => {
                    if let Some(left) = left {
                        let left_result = left.execute(register)?;
                        let right_result = right.execute(register)?;
                        Ok(left_result || right_result)
                    } else {
                        Err(SqlError::Error)
                    }
                }
                LogicalOperator::And => {
                    if let Some(left) = left {
                        let left_result = left.execute(register)?;
                        let right_result = right.execute(register)?;
                        Ok(left_result && right_result)
                    } else {
                        Err(SqlError::Error)
                    }
                }
            },
        };
        op_result
    }
}

#[cfg(test)]
mod tests {
    use super::Condition;
    use crate::clauses::condition::{LogicalOperator, Operator};
    use std::collections::HashMap;

    #[test]
    fn create_simple() {
        let condition = Condition::new_simple("age", ">", "18").unwrap();
        assert_eq!(
            condition,
            Condition::Simple {
                field: String::from("age"),
                operator: Operator::Greater,
                value: String::from("18")
            }
        )
    }

    #[test]
    fn create_simple_from_tokens() {
        let tokens = vec!["age", ">", "18"];
        let mut pos = 0;
        let condition = Condition::new_simple_from_tokens(&tokens, &mut pos).unwrap();

        assert_eq!(
            condition,
            Condition::Simple {
                field: String::from("age"),
                operator: Operator::Greater,
                value: String::from("18")
            }
        )
    }

    #[test]
    fn create_complex_with_left() {
        let left = Condition::Simple {
            field: String::from("age"),
            operator: Operator::Greater,
            value: String::from("18"),
        };

        let right = Condition::Simple {
            field: String::from("city"),
            operator: Operator::Equal,
            value: String::from("Gaiman"),
        };

        let complex = Condition::new_complex(Some(left), LogicalOperator::And, right);

        assert_eq!(
            complex,
            Condition::Complex {
                left: Some(Box::new(Condition::Simple {
                    field: String::from("age"),
                    operator: Operator::Greater,
                    value: String::from("18"),
                })),
                operator: LogicalOperator::And,
                right: Box::new(Condition::Simple {
                    field: String::from("city"),
                    operator: Operator::Equal,
                    value: String::from("Gaiman"),
                })
            }
        )
    }

    #[test]
    fn create_complex_without_left() {
        let right = Condition::Simple {
            field: String::from("name"),
            operator: Operator::Equal,
            value: String::from("Alen"),
        };

        let complex = Condition::new_complex(None, LogicalOperator::Not, right);

        assert_eq!(
            complex,
            Condition::Complex {
                left: None,
                operator: LogicalOperator::Not,
                right: Box::new(Condition::Simple {
                    field: String::from("name"),
                    operator: Operator::Equal,
                    value: String::from("Alen"),
                })
            }
        )
    }

    #[test]
    fn execute_simple() {
        let mut register = HashMap::new();
        register.insert(String::from("name"), String::from("Alen"));
        register.insert(String::from("lastname"), String::from("Davies"));
        register.insert(String::from("age"), String::from("24"));

        let condition_true = Condition::Simple {
            field: String::from("age"),
            operator: Operator::Greater,
            value: String::from("18"),
        };

        let condition_false = Condition::Simple {
            field: String::from("age"),
            operator: Operator::Greater,
            value: String::from("40"),
        };

        let result_true = condition_true.execute(&register).unwrap();
        let result_false = condition_false.execute(&register).unwrap();

        assert_eq!(result_true, true);

        assert_eq!(result_false, false);
    }

    #[test]
    fn execute_and() {
        let mut register = HashMap::new();
        register.insert(String::from("name"), String::from("Alen"));
        register.insert(String::from("lastname"), String::from("Davies"));
        register.insert(String::from("age"), String::from("24"));

        let left = Condition::Simple {
            field: String::from("age"),
            operator: Operator::Greater,
            value: String::from("18"),
        };
        let right = Condition::Simple {
            field: String::from("name"),
            operator: Operator::Equal,
            value: String::from("Alen"),
        };

        let condition = Condition::Complex {
            left: Some(Box::new(left)),
            operator: LogicalOperator::And,
            right: Box::new(right),
        };

        let result = condition.execute(&register).unwrap();

        assert_eq!(result, true)
    }

    #[test]
    fn execute_or() {
        let mut register = HashMap::new();
        register.insert(String::from("name"), String::from("Alen"));
        register.insert(String::from("lastname"), String::from("Davies"));
        register.insert(String::from("age"), String::from("24"));

        let left = Condition::Simple {
            field: String::from("age"),
            operator: Operator::Greater,
            value: String::from("40"),
        };
        let right = Condition::Simple {
            field: String::from("name"),
            operator: Operator::Equal,
            value: String::from("Emily"),
        };

        let condition = Condition::Complex {
            left: Some(Box::new(left)),
            operator: LogicalOperator::Or,
            right: Box::new(right),
        };

        let result = condition.execute(&register).unwrap();

        assert_eq!(result, false)
    }

    #[test]
    fn execute_not() {
        let mut register = HashMap::new();
        register.insert(String::from("name"), String::from("Alen"));
        register.insert(String::from("lastname"), String::from("Davies"));
        register.insert(String::from("age"), String::from("24"));

        let right = Condition::Simple {
            field: String::from("name"),
            operator: Operator::Equal,
            value: String::from("Emily"),
        };

        let condition = Condition::Complex {
            left: None,
            operator: LogicalOperator::Not,
            right: Box::new(right),
        };

        let result = condition.execute(&register).unwrap();

        assert_eq!(result, true)
    }

    #[test]
    fn execute_and_or() {
        let mut register = HashMap::new();
        register.insert(String::from("name"), String::from("Alen"));
        register.insert(String::from("lastname"), String::from("Davies"));
        register.insert(String::from("age"), String::from("24"));
        register.insert(String::from("city"), String::from("Gaiman"));

        let left = Condition::Simple {
            field: String::from("age"),
            operator: Operator::Greater,
            value: String::from("40"),
        };
        let right1 = Condition::Simple {
            field: String::from("name"),
            operator: Operator::Equal,
            value: String::from("Alen"),
        };

        let or = Condition::Complex {
            left: Some(Box::new(left)),
            operator: LogicalOperator::Or,
            right: Box::new(right1),
        };

        let right2 = Condition::Simple {
            field: String::from("city"),
            operator: Operator::Equal,
            value: String::from("Trelew"),
        };

        let and = Condition::Complex {
            left: Some(Box::new(or)),
            operator: LogicalOperator::And,
            right: Box::new(right2),
        };

        let result = and.execute(&register).unwrap();

        assert_eq!(result, false)
    }

    #[test]
    fn execute_not_and() {
        let mut register = HashMap::new();
        register.insert(String::from("name"), String::from("Alen"));
        register.insert(String::from("lastname"), String::from("Davies"));
        register.insert(String::from("age"), String::from("24"));
        register.insert(String::from("city"), String::from("Gaiman"));

        let right1 = Condition::Simple {
            field: String::from("age"),
            operator: Operator::Greater,
            value: String::from("40"),
        };

        let not = Condition::Complex {
            left: None,
            operator: LogicalOperator::Not,
            right: Box::new(right1),
        };

        let right2 = Condition::Simple {
            field: String::from("city"),
            operator: Operator::Equal,
            value: String::from("Gaiman"),
        };

        let and = Condition::Complex {
            left: Some(Box::new(not)),
            operator: LogicalOperator::And,
            right: Box::new(right2),
        };

        let result = and.execute(&register).unwrap();

        assert_eq!(result, true)
    }

    #[test]
    fn execute_not_and_or_with_paren() {
        let mut register = HashMap::new();
        register.insert(String::from("name"), String::from("Alen"));
        register.insert(String::from("lastname"), String::from("Davies"));
        register.insert(String::from("age"), String::from("24"));
        register.insert(String::from("city"), String::from("Gaiman"));

        // NOT (city = Gaiman AND (age > 18 OR lastname = Davies))

        let condition = Condition::Complex {
            left: None,
            operator: LogicalOperator::Not,
            right: Box::new(Condition::Complex {
                left: Some(Box::new(Condition::Simple {
                    field: String::from("city"),
                    operator: Operator::Equal,
                    value: String::from("Gaiman"),
                })),
                operator: LogicalOperator::And,
                right: Box::new(Condition::Complex {
                    left: Some(Box::new(Condition::Simple {
                        field: String::from("age"),
                        operator: Operator::Greater,
                        value: String::from("18"),
                    })),
                    operator: LogicalOperator::Or,
                    right: Box::new(Condition::Simple {
                        field: String::from("lastname"),
                        operator: Operator::Equal,
                        value: String::from("Davies"),
                    }),
                }),
            }),
        };

        let result = condition.execute(&register).unwrap();

        assert_eq!(result, false)
    }

    #[test]

    fn execute_and_or_with_paren2() {
        let mut register = HashMap::new();
        register.insert(String::from("name"), String::from("Alen"));
        register.insert(String::from("lastname"), String::from("Davies"));
        register.insert(String::from("age"), String::from("24"));
        register.insert(String::from("city"), String::from("Gaiman"));

        // city = Gaiman AND (age > 30 OR lastname = Davies)

        let condition = Condition::Complex {
            left: Some(Box::new(Condition::Simple {
                field: String::from("city"),
                operator: Operator::Equal,
                value: String::from("Gaiman"),
            })),
            operator: LogicalOperator::And,
            right: Box::new(Condition::Complex {
                left: Some(Box::new(Condition::Simple {
                    field: String::from("age"),
                    operator: Operator::Greater,
                    value: String::from("30"),
                })),
                operator: LogicalOperator::Or,
                right: Box::new(Condition::Simple {
                    field: String::from("lastname"),
                    operator: Operator::Equal,
                    value: String::from("Davies"),
                }),
            }),
        };

        let result = condition.execute(&register).unwrap();

        assert_eq!(result, true);
    }
}
