use std::fmt::Display;

/// Enum representing the possible errors that can occur when processing SQL queries.
///
/// The possible errors are:
///
/// - `InvalidTable`: related to problems with the processing of tables.
/// - `InvalidColumn`: related to problems with the processing of columns.
/// - `InvalidSyntax`: related to problems with the processing of queries.
/// - `Error`: generic type for other possible errors detected.
///
#[derive(Debug, PartialEq)]
pub enum SqlError {
    InvalidTable,
    InvalidColumn,
    InvalidSyntax,
    Error,
}

impl Display for SqlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SqlError::InvalidTable => write!(f, "[InvalidTable]: [Error to process table]"),
            SqlError::InvalidColumn => write!(f, "[InvalidColumn]: [Error to process column]"),
            SqlError::InvalidSyntax => write!(f, "[InvalidSyntax]: [Error to process query]"),
            SqlError::Error => write!(f, "[Error]: [An error occurred]"),
        }
    }
}
