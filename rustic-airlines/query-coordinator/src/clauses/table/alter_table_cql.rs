use crate::errors::CQLError;
use crate::clauses::types::column::Column;
use crate::clauses::types::datatype::DataType;
use crate::clauses::types::alter_table_op::AlterTableOperation;


#[derive(Debug)]
pub struct AlterTable {
    table_name: String,
    operations: Vec<AlterTableOperation>,
}

impl AlterTable {
    pub fn new(table_name: String, operations: Vec<AlterTableOperation>) -> AlterTable {
        AlterTable {
            table_name: table_name.to_string(),
            operations,
        }
    }

    pub fn new_from_tokens(query: Vec<String>) -> Result<AlterTable, CQLError> {
        if query.len() < 4 || query[0].to_uppercase() != "ALTER" || query[1].to_uppercase() != "TABLE" {
            return Err(CQLError::InvalidSyntax);
        }

        let table_name = query[2].to_string(); 
        let operations = &query[3..];

        let mut ops: Vec<AlterTableOperation> = Vec::new();

        let mut i = 0;
        while i < operations.len() {
            match operations[i].to_uppercase().as_str() {
                "ADD" => {
                    if i + 2 >= operations.len() || operations[i + 1].to_uppercase() != "COLUMN" {
                        return Err(CQLError::InvalidSyntax);
                    }

                    let column_def = &operations[i + 2];
                    let col_parts: Vec<&str> = column_def.trim().split_whitespace().collect();
                    if col_parts.len() < 2 {
                        return Err(CQLError::InvalidSyntax);
                    }

                    let col_name = col_parts[0].to_string();
                    let col_type = match col_parts[1].to_uppercase().as_str() {
                        "INT" => DataType::Int,
                        "STRING" => DataType::String,
                        "BOOLEAN" => DataType::Boolean,
                        _ => return Err(CQLError::InvalidSyntax),
                    };

                    let mut allows_null = true;
                    if col_parts.len() > 2 && col_parts[2].to_uppercase() == "NOT" {
                        if col_parts.len() < 4 || col_parts[3].to_uppercase() != "NULL" {
                            return Err(CQLError::InvalidSyntax);
                        }
                        allows_null = false;
                    }

                    ops.push(AlterTableOperation::AddColumn(Column::new(&col_name, col_type, false, allows_null)));
                    i += 3; 
                }
                "DROP" => {
                    if i + 1 >= operations.len() || operations[i + 1].to_uppercase() != "COLUMN" {
                        return Err(CQLError::InvalidSyntax);
                    }
                
                    let col_name = operations[i + 2].to_string();
                    ops.push(AlterTableOperation::DropColumn(col_name));
                    i += 3; 
                }
                "MODIFY" => {
                    if i + 2 >= operations.len() {
                        return Err(CQLError::InvalidSyntax);
                    }

                    let col_name = operations[i + 1].to_string();
                    let col_type = match operations[i + 2].to_uppercase().as_str() {
                        "INT" => DataType::Int,
                        "STRING" => DataType::String,
                        "BOOLEAN" => DataType::Boolean,
                        _ => return Err(CQLError::InvalidSyntax),
                    };

                    let mut allows_null = true;
                    if operations.len() > i + 3 && operations[i + 3].to_uppercase() == "NOT" {
                        if operations.len() < i + 5 || operations[i + 4].to_uppercase() != "NULL" {
                            return Err(CQLError::InvalidSyntax);
                        }
                        allows_null = false;
                        i += 2; 
                    }

                    ops.push(AlterTableOperation::ModifyColumn(col_name, col_type, allows_null));
                    i += 3; 
                }
                "RENAME" => {
                    if i + 2 >= operations.len() || operations[i + 1].to_uppercase() != "COLUMN" {
                        return Err(CQLError::InvalidSyntax);
                    }

                    let old_col_name = operations[i + 2].to_string();
                    let new_col_name = operations[i + 3].to_string();
                    ops.push(AlterTableOperation::RenameColumn(old_col_name, new_col_name));
                    i += 4; 
                }
                _ => return Err(CQLError::InvalidSyntax),
            }
            i += 1; 
        }
        Ok(AlterTable::new(table_name, ops))
    }

}