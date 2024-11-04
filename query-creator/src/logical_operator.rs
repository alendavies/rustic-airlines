/// Logical operators used in the `WHERE` clause.
/// - `And`: Logical AND operator
/// - `Or`: Logical OR operator
/// - `Not`: Logical NOT operator
///
/// 
use crate::CQLError;
#[derive(Debug, PartialEq, Clone)]

pub enum LogicalOperator {
    And,
    Or,
    Not,
}

impl LogicalOperator {
    /// Serializes the `LogicalOperator` to its SQL string representation
    pub fn serialize(&self) -> &str {
        match self {
            LogicalOperator::And => "AND",
            LogicalOperator::Or => "OR",
            LogicalOperator::Not => "NOT",
        }
    }

    /// Deserializes a string to a `LogicalOperator`
    pub fn deserialize(op_str: &str) -> Result<Self, CQLError> {
        match op_str {
            "AND" => Ok(LogicalOperator::And),
            "OR" => Ok(LogicalOperator::Or),
            "NOT" => Ok(LogicalOperator::Not),
            _ => Err(CQLError::InvalidSyntax),
        }
    }
}