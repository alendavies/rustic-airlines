/// Enum for the operators used in the queries.
/// - `Equal`: Equal operator
/// - `Greater`: Greater than operator
/// - `Lesser`: Lesser than operator
///
/// 
///
use crate::CQLError;
#[derive(Debug, PartialEq, Clone)]
pub enum Operator {
    Equal,
    Greater,
    Lesser,
}

impl Operator {
    /// Serializes the `Operator` to its SQL string representation
    pub fn serialize(&self) -> &str {
        match self {
            Operator::Equal => "=",
            Operator::Greater => ">",
            Operator::Lesser => "<",
        }
    }

    /// Deserializes a string to an `Operator`
    pub fn deserialize(op_str: &str) -> Result<Self, CQLError> {
        match op_str {
            "=" => Ok(Operator::Equal),
            ">" => Ok(Operator::Greater),
            "<" => Ok(Operator::Lesser),
            _ => Err(CQLError::InvalidSyntax),
        }
    }
}