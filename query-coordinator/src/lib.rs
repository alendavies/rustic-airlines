pub mod clauses;
pub mod errors;
mod logical_operator;
mod operator;
mod utils;

use clauses::keyspace::{
    alter_keyspace_cql::AlterKeyspace, create_keyspace_cql::CreateKeyspace,
    drop_keyspace_cql::DropKeyspace,
};
use clauses::table::{
    alter_table_cql::AlterTable, create_table_cql::CreateTable, drop_table_cql::DropTable,
};
use clauses::{delete_sql::Delete, insert_sql::Insert, select_sql::Select, update_sql::Update};
use errors::CQLError;
use native_protocol::frame::Frame;
use native_protocol::messages::result::result;
use native_protocol::messages::result::rows::Rows;
use native_protocol::messages::result::schema_change;
use native_protocol::messages::result::schema_change::SchemaChange;
use std::fmt;
use std::option;
/// The `NeededResponses` trait defines how many responses are required for a given query.
/// Queries like `CREATE` and `DROP` often require responses from all nodes in a distributed system,
/// while `SELECT`, `INSERT`, etc., may only need specific responses from certain nodes.
pub trait NeededResponses {
    fn needed_responses(&self) -> NeededResponseCount;
}

pub trait CreateClientResponse {
    fn create_client_response(
        &self,
        table: Option<String>,
        keyspace: Option<String>,
        rows: Option<Rows>,
    ) -> Result<Frame, CQLError>;
}

/// Represents the count of responses needed for a query. It can either be all nodes
/// or a specific number of nodes based on the query type.
#[derive(Debug, Clone)]
pub enum NeededResponseCount {
    AllNodes,
    Specific(u32),
}

/// `Query` is an enumeration representing different query types supported by the system,
/// such as `SELECT`, `INSERT`, `CREATE`, `DROP`, etc. Each variant wraps the respective
/// query structure used to execute the query.
#[derive(Debug, Clone)]
pub enum Query {
    Select(Select),
    Insert(Insert),
    Update(Update),
    Delete(Delete),
    CreateTable(CreateTable),
    DropTable(DropTable),
    AlterTable(AlterTable),
    CreateKeyspace(CreateKeyspace),
    DropKeyspace(DropKeyspace),
    AlterKeyspace(AlterKeyspace),
}

/// Implements the `fmt::Display` trait for `Query`. This allows the enum to be printed in a human-readable format.
impl fmt::Display for Query {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let query_type = match self {
            Query::Select(_) => "Select",
            Query::Insert(_) => "Insert",
            Query::Update(_) => "Update",
            Query::Delete(_) => "Delete",
            Query::CreateTable(_) => "CreateTable",
            Query::DropTable(_) => "DropTable",
            Query::AlterTable(_) => "AlterTable",
            Query::CreateKeyspace(_) => "CreateKeyspace",
            Query::DropKeyspace(_) => "DropKeyspace",
            Query::AlterKeyspace(_) => "AlterKeyspace",
        };
        write!(f, "{}", query_type)
    }
}

/// Implements the `fmt::Display` trait for `Query`. This allows the enum to be printed in a human-readable format.
impl CreateClientResponse for Query {
    fn create_client_response(
        &self,
        table: Option<String>,
        keyspace: Option<String>,
        rows: Option<Vec<String, String>>,
    ) -> Result<Frame, CQLError> {
        let query_type = match self {
            Query::Select(_) => Frame::Ready,
            Query::Insert(_) => Frame::Result(result::Result::Void),
            Query::Update(_) => Frame::Result(result::Result::Void),
            Query::Delete(_) => Frame::Result(result::Result::Void),
            Query::CreateTable(_) => {
                if let Some(keyspace_string) = keyspace {
                    Frame::Result(result::Result::SchemaChange(SchemaChange::new(
                        schema_change::ChangeType::Created,
                        schema_change::Target::Table,
                        schema_change::Options::new(keyspace_string, table),
                    )))
                } else {
                    return Err(CQLError::Error);
                }
            }
            Query::DropTable(_) => {
                if let Some(keyspace_string) = keyspace {
                    Frame::Result(result::Result::SchemaChange(SchemaChange::new(
                        schema_change::ChangeType::Dropped,
                        schema_change::Target::Table,
                        schema_change::Options::new(keyspace_string, table),
                    )))
                } else {
                    return Err(CQLError::Error);
                }
            }
            Query::AlterTable(_) => {
                if let Some(keyspace_string) = keyspace {
                    Frame::Result(result::Result::SchemaChange(SchemaChange::new(
                        schema_change::ChangeType::Updated,
                        schema_change::Target::Table,
                        schema_change::Options::new(keyspace_string, table),
                    )))
                } else {
                    return Err(CQLError::Error);
                }
            }
            Query::CreateKeyspace(_) => {
                if let Some(keyspace_string) = keyspace {
                    Frame::Result(result::Result::SchemaChange(SchemaChange::new(
                        schema_change::ChangeType::Created,
                        schema_change::Target::Keyspace,
                        schema_change::Options::new(keyspace_string, None),
                    )))
                } else {
                    return Err(CQLError::Error);
                }
            }
            Query::DropKeyspace(_) => {
                if let Some(keyspace_string) = keyspace {
                    Frame::Result(result::Result::SchemaChange(SchemaChange::new(
                        schema_change::ChangeType::Dropped,
                        schema_change::Target::Keyspace,
                        schema_change::Options::new(keyspace_string, None),
                    )))
                } else {
                    return Err(CQLError::Error);
                }
            }
            Query::AlterKeyspace(_) => {
                if let Some(keyspace_string) = keyspace {
                    Frame::Result(result::Result::SchemaChange(SchemaChange::new(
                        schema_change::ChangeType::Updated,
                        schema_change::Target::Keyspace,
                        schema_change::Options::new(keyspace_string, None),
                    )))
                } else {
                    return Err(CQLError::Error);
                }
            }
        };

        Ok(query_type)
    }
}

/// Implements the `NeededResponses` trait for each type of query. Queries like `SELECT` and `INSERT`
/// require a specific number of responses, while `CREATE` and `DROP` require responses from all nodes.
impl NeededResponses for Query {
    fn needed_responses(&self) -> NeededResponseCount {
        match self {
            Query::Select(_) => NeededResponseCount::Specific(1),
            Query::Insert(_) => NeededResponseCount::Specific(1),
            Query::Update(_) => NeededResponseCount::Specific(1),
            Query::Delete(_) => NeededResponseCount::Specific(1),
            Query::CreateTable(_) => NeededResponseCount::AllNodes,
            Query::DropTable(_) => NeededResponseCount::AllNodes,
            Query::AlterTable(_) => NeededResponseCount::AllNodes,
            Query::CreateKeyspace(_) => NeededResponseCount::AllNodes,
            Query::DropKeyspace(_) => NeededResponseCount::AllNodes,
            Query::AlterKeyspace(_) => NeededResponseCount::AllNodes,
        }
    }
}

/// The `QueryCoordinator` struct is responsible for coordinating the execution of queries.
/// It parses a query string into tokens, determines the type of query, and returns a corresponding
/// `Query` enum variant.
#[derive(Debug)]
pub struct QueryCoordinator;

impl QueryCoordinator {
    /// Creates a new instance of `QueryCoordinator`.
    pub fn new() -> QueryCoordinator {
        QueryCoordinator {}
    }

    /// Parses a query string and determines the type of query (e.g., `SELECT`, `INSERT`, `CREATE TABLE`).
    /// Returns a `Query` enum or an error if the query is invalid.
    ///
    /// # Parameters
    /// - `query`: A `String` representing the query to be handled.
    ///
    /// # Returns
    /// A `Result` containing either a `Query` enum or a `CQLError`.
    pub fn handle_query(self, query: String) -> Result<Query, CQLError> {
        let tokens = Self::tokens_from_query(&query);

        match tokens[0].as_str() {
            "SELECT" => {
                let select = Select::new_from_tokens(tokens)?;
                Ok(Query::Select(select))
            }
            "INSERT" => {
                let insert = Insert::new_from_tokens(tokens)?;
                Ok(Query::Insert(insert))
            }
            "DELETE" => {
                let delete = Delete::new_from_tokens(tokens)?;
                Ok(Query::Delete(delete))
            }
            "UPDATE" => {
                let update = Update::new_from_tokens(tokens)?;
                Ok(Query::Update(update))
            }
            "CREATE" => match tokens[1].as_str() {
                "TABLE" => {
                    let create_table = CreateTable::new_from_tokens(tokens)?;
                    Ok(Query::CreateTable(create_table))
                }
                "KEYSPACE" => {
                    let create_keyspace = CreateKeyspace::new_from_tokens(tokens)?;
                    Ok(Query::CreateKeyspace(create_keyspace))
                }
                _ => Err(CQLError::InvalidSyntax),
            },
            "DROP" => match tokens[1].as_str() {
                "TABLE" => {
                    let drop_table = DropTable::new_from_tokens(tokens)?;
                    Ok(Query::DropTable(drop_table))
                }
                "KEYSPACE" => {
                    let drop_keyspace = DropKeyspace::new_from_tokens(tokens)?;
                    Ok(Query::DropKeyspace(drop_keyspace))
                }
                _ => Err(CQLError::InvalidSyntax),
            },
            "ALTER" => match tokens[1].as_str() {
                "TABLE" => {
                    let alter_table = AlterTable::new_from_tokens(tokens)?;
                    Ok(Query::AlterTable(alter_table))
                }
                "KEYSPACE" => {
                    let alter_keyspace = AlterKeyspace::new_from_tokens(tokens)?;
                    Ok(Query::AlterKeyspace(alter_keyspace))
                }
                _ => Err(CQLError::InvalidSyntax),
            },
            _ => Err(CQLError::InvalidSyntax),
        }
    }

    /// Tokenizes a query string by breaking it into its constituent parts.
    /// This function handles various elements such as braces, parentheses, and quotes.
    ///
    /// # Parameters
    /// - `string`: The query string to be tokenized.
    ///
    /// # Returns
    /// A `Vec<String>` containing the tokens.
    pub fn tokens_from_query(string: &str) -> Vec<String> {
        let mut index = 0;
        let mut tokens = Vec::new();
        let mut current = String::new();
        let mut in_braces = false;

        let string = string.replace(";", "");
        let length = string.len();

        while index < length {
            let char = string.chars().nth(index).unwrap_or('0');

            if char == '{' {
                tokens.push("{".to_string());
                in_braces = true;
                index += 1;
            } else if char == '}' {
                if !current.is_empty() {
                    tokens.push(current.clone());
                    current.clear();
                }
                tokens.push("}".to_string());
                in_braces = false;
                index += 1;
            } else if in_braces {
                if char == '\'' {
                    index = Self::process_quotes(&string, index, &mut current, &mut tokens);
                } else if char.is_alphanumeric() || char == '_' {
                    current.push(char);
                    index += 1;
                } else if char == ':' || char == ',' {
                    if !current.is_empty() {
                        tokens.push(current.clone());
                        current.clear();
                    }
                    index += 1; // Skip separators ':' and ','
                } else {
                    index += 1;
                }
            } else if char.is_alphabetic() || char == '_' {
                index = Self::process_alphabetic(&string, index, &mut current, &mut tokens);
            } else if char.is_numeric() {
                index = Self::process_numeric(&string, index, &mut current, &mut tokens);
            } else if char == '\'' {
                index = Self::process_quotes(&string, index, &mut current, &mut tokens);
            } else if char == '(' {
                index = Self::process_paren(&string, index, &mut current, &mut tokens);
            } else if char.is_whitespace() || char == ',' {
                index += 1;
            } else {
                index = Self::process_other(&string, index, &mut current, &mut tokens);
            }
        }

        tokens.retain(|s| !s.is_empty());
        tokens
    }

    fn process_alphabetic(
        string: &str,
        mut index: usize,
        current: &mut String,
        tokens: &mut Vec<String>,
    ) -> usize {
        while index < string.len() {
            let char = string.chars().nth(index).unwrap_or('0');
            if char.is_alphabetic() || char == '_' {
                current.push(char);
                index += 1;
            } else {
                break;
            }
        }
        tokens.push(current.clone());
        current.clear();
        index
    }

    fn process_numeric(
        string: &str,
        mut index: usize,
        current: &mut String,
        tokens: &mut Vec<String>,
    ) -> usize {
        while index < string.len() {
            let char = string.chars().nth(index).unwrap_or('0');
            if char.is_numeric() {
                current.push(char);
                index += 1;
            } else {
                break;
            }
        }
        tokens.push(current.clone());
        current.clear();
        index
    }

    fn process_quotes(
        string: &str,
        mut index: usize,
        current: &mut String,
        tokens: &mut Vec<String>,
    ) -> usize {
        index += 1;
        while index < string.len() {
            let char = string.chars().nth(index).unwrap_or('0');
            if char == '\'' {
                break;
            }
            current.push(char);
            index += 1;
        }
        index += 1;
        tokens.push(current.clone());
        current.clear();
        index
    }

    fn process_paren(
        string: &str,
        mut index: usize,
        current: &mut String,
        tokens: &mut Vec<String>,
    ) -> usize {
        index += 1;
        while index < string.len() {
            let char = string.chars().nth(index).unwrap_or('0');
            if char == ')' {
                break;
            }
            current.push(char);
            index += 1;
        }
        index += 1;
        tokens.push(current.clone());
        current.clear();
        index
    }

    fn process_other(
        string: &str,
        mut index: usize,
        current: &mut String,
        tokens: &mut Vec<String>,
    ) -> usize {
        while index < string.len() {
            let char = string.chars().nth(index).unwrap_or('0');
            if char.is_alphanumeric() || char.is_whitespace() {
                break;
            }
            current.push(char);
            index += 1;
        }
        tokens.push(current.clone());
        current.clear();
        index
    }
}
