pub mod clauses;
pub mod errors;
mod logical_operator;
mod operator;
mod utils;

use clauses::{delete_sql::Delete, insert_sql::Insert, select_sql::Select, update_sql::Update};
use clauses::table::{create_table_cql::CreateTable, drop_table_cql::DropTable, alter_table_cql::AlterTable};
use errors::CQLError;


#[derive(Debug)]  // Derivar Debug para Query
pub enum Query {
    Select(Select),
    Insert(Insert),
    Update(Update),
    Delete(Delete),
    CreateTable(CreateTable),
    DropTable(DropTable),
    AlterTable(AlterTable)
}

#[derive(Debug)]  // Agrega Debug también al QueryCoordinator si lo necesitas
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
            "CREATE" => {
                match tokens[1].as_str() {
                    "TABLE" => {
                        let create_table = CreateTable::new_from_tokens(tokens)?;
                        Ok(Query::CreateTable(create_table))
                    }
                    //"KEYSPACE" => {
                    //    
                    //}
                    _ => Err(CQLError::InvalidSyntax),
                }                                        
            }
            "DROP" => {
                match tokens[1].as_str() {
                    "TABLE" => {
                        let drop_table = DropTable::new_from_tokens(tokens)?;
                        Ok(Query::DropTable(drop_table))
                    }                   
                    _ => Err(CQLError::InvalidSyntax),
                }                                        
            }
            "ALTER" => {
                match tokens[1].as_str() {
                    "TABLE" => {
                        let alter_table = AlterTable::new_from_tokens(tokens)?;
                        Ok(Query::AlterTable(alter_table))
                    }                   
                    _ => Err(CQLError::InvalidSyntax),
                }                                        
            }
            _ => Err(CQLError::InvalidSyntax),
        }
    }

    pub fn tokens_from_query(string: &str) -> Vec<String> {
        let mut index = 0;
        let mut tokens = Vec::new();
        let mut current = String::new();
    
        let string = string.replace(";", "");
        let length = string.len();
    
        while index < length {
            let char = string.chars().nth(index).unwrap_or('0');
    
            if char.is_alphabetic() || char == '_' {
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