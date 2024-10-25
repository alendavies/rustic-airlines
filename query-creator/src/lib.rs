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
use clauses::types::column::Column;
use clauses::types::datatype::DataType;
use clauses::{
    delete_cql::Delete, insert_cql::Insert, select_cql::Select, update_cql::Update, use_cql::Use,
};
use errors::CQLError;
use native_protocol::frame::Frame;
use native_protocol::messages::result::result;
use native_protocol::messages::result::rows::{ColumnType, ColumnValue, Rows};
use native_protocol::messages::result::schema_change;
use native_protocol::messages::result::schema_change::SchemaChange;
use std::collections::BTreeMap;
use std::fmt;

/// The `NeededResponses` trait defines how many responses are required for a given query.
/// Queries like `CREATE` and `DROP` often require responses from all nodes in a distributed system,
/// while `SELECT`, `INSERT`, etc., may only need specific responses from certain nodes.
pub trait NeededResponses {
    fn needed_responses(&self) -> NeededResponseCount;
}

pub trait GetTableName {
    fn get_table_name(&self) -> Option<String>;
}
pub trait CreateClientResponse {
    fn create_client_response(
        &self,
        columns: Vec<Column>,
        keyspace: String,
        rows: Vec<String>,
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
    Use(Use),
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
            Query::Use(_) => "Use",
        };
        write!(f, "{}", query_type)
    }
}

impl From<DataType> for ColumnType {
    fn from(value: DataType) -> Self {
        match value {
            DataType::Int => ColumnType::Int,
            DataType::String => ColumnType::Ascii,
            DataType::Boolean => ColumnType::Boolean,
            // DataType::Blob => ColumnType::Blob,
            DataType::Double => ColumnType::Double,
            DataType::Float => ColumnType::Float,
            DataType::Timestamp => ColumnType::Timestamp,
            DataType::Uuid => ColumnType::Uuid,
        }
    }
}

fn create_column_value_from_type(
    col_type: &ColumnType,
    value: &str,
) -> Result<ColumnValue, CQLError> {
    match col_type {
        ColumnType::Ascii => Ok(ColumnValue::Ascii(value.to_string())),
        ColumnType::Bigint => Ok(ColumnValue::Bigint(
            value.parse::<i64>().map_err(|_| CQLError::Error)?,
        )),
        // ColumnType::Blob => Ok(ColumnValue::Blob(
        //     hex::decode(value).map_err(|_| CQLError::Error)?,
        // )),
        ColumnType::Boolean => Ok(ColumnValue::Boolean(
            value.parse::<bool>().map_err(|_| CQLError::Error)?,
        )),
        ColumnType::Counter => Ok(ColumnValue::Counter(
            value.parse::<i64>().map_err(|_| CQLError::Error)?,
        )),
        ColumnType::Decimal => Ok(ColumnValue::Decimal {
            scale: value.parse::<i32>().map_err(|_| CQLError::Error)?,
            unscaled: value.as_bytes().to_vec(),
        }),
        ColumnType::Double => Ok(ColumnValue::Double(
            value.parse::<f64>().map_err(|_| CQLError::Error)?,
        )),
        ColumnType::Float => Ok(ColumnValue::Float(
            value.parse::<f32>().map_err(|_| CQLError::Error)?,
        )),
        ColumnType::Int => Ok(ColumnValue::Int(
            value.parse::<i32>().map_err(|_| CQLError::Error)?,
        )),
        ColumnType::Timestamp => Ok(ColumnValue::Timestamp(
            value.parse::<i64>().map_err(|_| CQLError::Error)?,
        )),
        ColumnType::Uuid => {
            let bytes = value.as_bytes();
            let uuid: [u8; 16] = bytes.try_into().map_err(|_| CQLError::Error)?;
            Ok(ColumnValue::Uuid(uuid))
        }
        ColumnType::Varchar => Ok(ColumnValue::Varchar(value.to_string())),
        ColumnType::Varint => Ok(ColumnValue::Varint(value.as_bytes().to_vec())),
        _ => Err(CQLError::Error),
    }
}

/// Implements the CreateClientResponse that return the Frame to respond to the client depending of what Query is.
impl CreateClientResponse for Query {
    fn create_client_response(
        &self,
        columns: Vec<Column>,
        keyspace: String,
        rows: Vec<String>,
    ) -> Result<Frame, CQLError> {
        let query_type = match self {
            Query::Select(_) => {
                let necessary_columns: Vec<_> = rows
                    .get(0)
                    .ok_or(CQLError::InvalidSyntax)?
                    .split(",")
                    .collect();

                let col_types: Result<Vec<_>, CQLError> = necessary_columns
                    .iter()
                    .map(|&name| {
                        let a = columns
                            .iter()
                            .find(|col| col.name == *name)
                            .ok_or(CQLError::Error)?;

                        let b = ColumnType::from(a.data_type.clone());
                        Ok((name.to_string(), b))
                    })
                    .collect();

                let col_types = col_types?;

                let mut records = Vec::new();

                for row in rows[1..].to_vec() {
                    let mut record = BTreeMap::new();

                    for (idx, value) in row.split(",").enumerate() {
                        let (name, r#type) = col_types.get(idx).ok_or(CQLError::Error)?;
                        let col_value = create_column_value_from_type(r#type, value)
                            .map_err(|_| CQLError::Error)?;

                        record.insert(name.to_string(), col_value);
                    }

                    records.push(record);
                }

                let rows = Rows::new(col_types, records);
                Frame::Result(result::Result::Rows(rows))
            }
            Query::Insert(_) => Frame::Result(result::Result::Void),
            Query::Update(_) => Frame::Result(result::Result::Void),
            Query::Delete(_) => Frame::Result(result::Result::Void),
            Query::CreateTable(create_table) => {
                Frame::Result(result::Result::SchemaChange(SchemaChange::new(
                    schema_change::ChangeType::Created,
                    schema_change::Target::Table,
                    schema_change::Options::new(keyspace, Some(create_table.get_name())),
                )))
            }
            Query::DropTable(create_table) => {
                Frame::Result(result::Result::SchemaChange(SchemaChange::new(
                    schema_change::ChangeType::Dropped,
                    schema_change::Target::Table,
                    schema_change::Options::new(keyspace, Some(create_table.get_table_name())),
                )))
            }
            Query::AlterTable(create_table) => {
                Frame::Result(result::Result::SchemaChange(SchemaChange::new(
                    schema_change::ChangeType::Updated,
                    schema_change::Target::Table,
                    schema_change::Options::new(keyspace, Some(create_table.get_table_name())),
                )))
            }
            Query::CreateKeyspace(_) => {
                let schema_change = SchemaChange::new(
                    schema_change::ChangeType::Created,
                    schema_change::Target::Keyspace,
                    schema_change::Options::new(keyspace, None),
                );
                Frame::Result(result::Result::SchemaChange(schema_change))
            }
            Query::DropKeyspace(_) => {
                Frame::Result(result::Result::SchemaChange(SchemaChange::new(
                    schema_change::ChangeType::Dropped,
                    schema_change::Target::Keyspace,
                    schema_change::Options::new(keyspace, None),
                )))
            }
            Query::AlterKeyspace(_) => {
                Frame::Result(result::Result::SchemaChange(SchemaChange::new(
                    schema_change::ChangeType::Updated,
                    schema_change::Target::Keyspace,
                    schema_change::Options::new(keyspace, None),
                )))
            }
            Query::Use(_) => Frame::Result(result::Result::SetKeyspace(keyspace)),
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
            Query::Use(_) => NeededResponseCount::AllNodes,
        }
    }
}

/// Implements the `NeededResponses` trait for each type of query. Queries like `SELECT` and `INSERT`
/// require a specific number of responses, while `CREATE` and `DROP` require responses from all nodes.
impl GetTableName for Query {
    fn get_table_name(&self) -> Option<String> {
        {
            match self {
                Query::Select(select) => Some(select.table_name.clone()),
                Query::Insert(insert) => Some(insert.into_clause.table_name.clone()),
                Query::Update(update) => Some(update.table_name.clone()),
                Query::Delete(delete) => Some(delete.table_name.clone()),
                Query::CreateTable(create_table) => Some(create_table.get_name().clone()),
                Query::DropTable(drop_table) => Some(drop_table.get_table_name().clone()),
                Query::AlterTable(alter_table) => Some(alter_table.get_table_name().clone()),
                Query::CreateKeyspace(_) => None,
                Query::DropKeyspace(_) => None,
                Query::AlterKeyspace(_) => None,
                Query::Use(_) => None,
            }
        }
    }
}

/// The `QueryCreator` struct is responsible for coordinating the execution of queries.
/// It parses a query string into tokens, determines the type of query, and returns a corresponding
/// `Query` enum variant.
#[derive(Debug)]
pub struct QueryCreator;

impl QueryCreator {
    /// Creates a new instance of `QueryCreator`.
    pub fn new() -> QueryCreator {
        QueryCreator {}
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
            "USE" => {
                let use_cql = Use::new_from_tokens(tokens)?;
                Ok(Query::Use(use_cql))
            }
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
        let mut paren_count = 1;
        index += 1; // Skip the opening parenthesis

        // No agregamos el paréntesis de apertura al current

        while index < string.len() {
            let char = string.chars().nth(index).unwrap_or('0');
            if char == '(' {
                paren_count += 1;
                current.push(char);
            } else if char == ')' {
                paren_count -= 1;
                if paren_count == 0 {
                    // No agregamos el paréntesis de cierre al current
                    index += 1;
                    break;
                }
                current.push(char);
            } else {
                current.push(char);
            }
            index += 1;
        }

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
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_select_query() {
        let coordinator = QueryCreator::new();
        let query = "SELECT name, age FROM users WHERE age > 30;".to_string();
        let result = coordinator.handle_query(query);
        assert!(matches!(result, Ok(Query::Select(_))));

        if let Ok(query) = result {
            assert!(matches!(
                query.needed_responses(),
                NeededResponseCount::AllNodes
            ));
        }
    }

    #[test]
    fn test_create_insert_query() {
        let coordinator = QueryCreator::new();
        let query = "INSERT INTO users (name, age) VALUES ('John', 28);".to_string();
        let result = coordinator.handle_query(query);
        assert!(matches!(result, Ok(Query::Insert(_))));

        if let Ok(query) = result {
            assert!(matches!(
                query.needed_responses(),
                NeededResponseCount::Specific(1)
            ));
        }
    }

    #[test]
    fn test_create_update_query() {
        let coordinator = QueryCreator::new();
        let query = "UPDATE users SET age = 29 WHERE name = 'John';".to_string();
        let result = coordinator.handle_query(query);
        assert!(matches!(result, Ok(Query::Update(_))));

        if let Ok(query) = result {
            assert!(matches!(
                query.needed_responses(),
                NeededResponseCount::Specific(1)
            ));
        }
    }

    #[test]
    fn test_create_delete_query() {
        let coordinator = QueryCreator::new();
        let query = "DELETE FROM users WHERE age < 20;".to_string();
        let result = coordinator.handle_query(query);
        assert!(matches!(result, Ok(Query::Delete(_))));

        if let Ok(query) = result {
            assert!(matches!(
                query.needed_responses(),
                NeededResponseCount::Specific(1)
            ));
        }
    }

    #[test]
    fn test_create_table_query_success() {
        let coordinator = QueryCreator::new();
        let query =
            "CREATE TABLE t (a int, b int, c int, d int, PRIMARY KEY ((a, b), c, d));".to_string();
        let result = coordinator.handle_query(query);
        assert!(matches!(result, Ok(Query::CreateTable(_))));

        if let Ok(query) = result {
            assert!(matches!(
                query.needed_responses(),
                NeededResponseCount::AllNodes
            ));
        }
    }

    #[test]
    fn test_create_keyspace_query_success() {
        let coordinator = QueryCreator::new();
        let query = "CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};".to_string();
        let result = coordinator.handle_query(query);
        assert!(matches!(result, Ok(Query::CreateKeyspace(_))));

        if let Ok(query) = result {
            assert!(matches!(
                query.needed_responses(),
                NeededResponseCount::AllNodes
            ));
        }
    }

    #[test]
    fn test_drop_keyspace_query_success() {
        let coordinator = QueryCreator::new();
        let query = "DROP KEYSPACE test;".to_string();
        let result = coordinator.handle_query(query);
        assert!(matches!(result, Ok(Query::DropKeyspace(_))));

        if let Ok(query) = result {
            assert!(matches!(
                query.needed_responses(),
                NeededResponseCount::AllNodes
            ));
        }
    }

    #[test]
    fn test_alter_keyspace_query_success() {
        let coordinator = QueryCreator::new();
        let query = "ALTER KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};".to_string();
        let result = coordinator.handle_query(query);
        assert!(matches!(result, Ok(Query::AlterKeyspace(_))));

        if let Ok(query) = result {
            assert!(matches!(
                query.needed_responses(),
                NeededResponseCount::AllNodes
            ));
        }
    }
}
