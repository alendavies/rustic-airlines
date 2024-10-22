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
use std::{clone, fmt};

/// Define the NeededResponses trait to specify the response requirements for each query.
pub trait NeededResponses {
    fn needed_responses(&self) -> NeededResponseCount;
}

#[derive(Debug, Clone)]
pub enum NeededResponseCount {
    AllNodes,
    Specific(u32),
}

#[derive(Debug, Clone)] // Derivar Debug para Query
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

// Implementación del trait fmt::Display para Query
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

// Implementación de NeededResponses para cada tipo de consulta en Query
impl NeededResponses for Query {
    fn needed_responses(&self) -> NeededResponseCount {
        match self {
            Query::Select(_) => NeededResponseCount::AllNodes,
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

#[derive(Debug)]
pub struct QueryCoordinator;

impl QueryCoordinator {
    pub fn new() -> QueryCoordinator {
        QueryCoordinator {}
    }

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
                    index += 1; // Saltar separadores ':' y ','
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_select_query() {
        let coordinator = QueryCoordinator::new();
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
        let coordinator = QueryCoordinator::new();
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
        let coordinator = QueryCoordinator::new();
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
        let coordinator = QueryCoordinator::new();
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
        let coordinator = QueryCoordinator::new();
        let query = "CREATE TABLE users (id INT PRIMARY KEY, name TEXT);".to_string();
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
        let coordinator = QueryCoordinator::new();
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
        let coordinator = QueryCoordinator::new();
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
        let coordinator = QueryCoordinator::new();
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
