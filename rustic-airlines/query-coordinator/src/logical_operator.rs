/// Logical operators used in the `WHERE` clause.
/// - `And`: Logical AND operator
/// - `Or`: Logical OR operator
/// - `Not`: Logical NOT operator
///
#[derive(Debug, PartialEq)]
pub enum LogicalOperator {
    And,
    Or,
    Not,
}
