use crate::errors::CQLError;
use crate::clauses::types::column::Column;
use crate::clauses::types::datatype::DataType;
use crate::clauses::types::alter_table_op::AlterTableOperation;
use std::cmp::PartialEq;

#[derive(Debug, Clone)]
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

    // Método para deserializar una cadena de texto en una instancia de `AlterTable` utilizando `new_from_tokens`
    pub fn deserialize(serialized: &str) -> Result<Self, CQLError> {
        let tokens: Vec<String> = serialized.split_whitespace().map(|s| s.to_string()).collect();
        Self::new_from_tokens(tokens)
    }

    // Constructor alternativo que recibe tokens y construye una instancia `AlterTable`
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
                    // Soporte para omitir "COLUMN"
                    let mut offset = 1;
                    if i + 2 < operations.len() && operations[i + 1].to_uppercase() == "COLUMN" {
                        offset = 2;
                    }
                    
                    if i + offset + 1 >= operations.len() {
                        return Err(CQLError::InvalidSyntax);
                    }
    
                    let col_name = operations[i + offset].to_string();
                    let col_type = match operations[i + offset + 1].to_uppercase().as_str() {
                        "INT" => DataType::Int,
                        "STRING" => DataType::String,
                        "BOOLEAN" => DataType::Boolean,
                        "TEXT" => DataType::String, // Soporte adicional para TEXT como STRING
                        _ => return Err(CQLError::InvalidSyntax),
                    };
    
                    let allows_null = if operations.len() > i + offset + 2 && operations[i + offset + 2].to_uppercase() == "NOT" {
                        if operations.len() < i + offset + 4 || operations[i + offset + 3].to_uppercase() != "NULL" {
                            return Err(CQLError::InvalidSyntax);
                        }
                        false
                    } else {
                        true
                    };
    
                    ops.push(AlterTableOperation::AddColumn(Column::new(&col_name, col_type, false, allows_null)));
                    i += offset + 2;
                }
                "DROP" => {
                    let col_name = operations[i + 1].to_string();
                    ops.push(AlterTableOperation::DropColumn(col_name));
                    i += 2;
                }
                "MODIFY" => {
                    let col_name = operations[i + 1].to_string();
                    let col_type = match operations[i + 2].to_uppercase().as_str() {
                        "INT" => DataType::Int,
                        "STRING" => DataType::String,
                        "BOOLEAN" => DataType::Boolean,
                        _ => return Err(CQLError::InvalidSyntax),
                    };
    
                    let allows_null = if operations.len() > i + 3 && operations[i + 3].to_uppercase() == "NOT" {
                        if operations.len() < i + 5 || operations[i + 4].to_uppercase() != "NULL" {
                            return Err(CQLError::InvalidSyntax);
                        }
                        false
                    } else {
                        true
                    };
    
                    ops.push(AlterTableOperation::ModifyColumn(col_name, col_type, allows_null));
                    i += 3;
                }
                "RENAME" => {
                    let old_col_name = operations[i + 1].to_string();
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
    
    // Método para serializar una instancia de `AlterTable` a una cadena de texto
    pub fn serialize(&self) -> String {
        let operations_str: Vec<String> = self.operations.iter().map(|op| match op {
            AlterTableOperation::AddColumn(column) => {
                let mut op_str = format!("ADD {} {}", column.name, column.data_type.to_string());
                if !column.allows_null {
                    op_str.push_str(" NOT NULL");
                }
                op_str
            }
            AlterTableOperation::DropColumn(column_name) => format!("DROP {}", column_name),
            AlterTableOperation::ModifyColumn(column_name, data_type, allows_null) => {
                let mut op_str = format!("MODIFY {} {}", column_name, data_type.to_string());
                if !*allows_null {
                    op_str.push_str(" NOT NULL");
                }
                op_str
            }
            AlterTableOperation::RenameColumn(old_name, new_name) => format!("RENAME {} TO {}", old_name, new_name),
        }).collect();

        format!("ALTER TABLE {} {}", self.table_name, operations_str.join(" "))
    }

    pub fn get_table_name(&self) -> String {
        self.table_name.clone()
    }

    pub fn get_operations(&self) -> Vec<AlterTableOperation> {
        self.operations.clone()
    }
}


// Implementación de PartialEq para comparar por `table_name` y `operations`
impl PartialEq for AlterTable {
    fn eq(&self, other: &Self) -> bool {
        self.table_name == other.table_name 
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clauses::types::datatype::DataType;

    #[test]
    fn test_alter_table_add_column() {
        let query = vec![
            "ALTER".to_string(),
            "TABLE".to_string(),
            "airports".to_string(),
            "ADD".to_string(),
            "new_col".to_string(),
            "INT".to_string(),
        ];
        let alter_table = AlterTable::new_from_tokens(query).unwrap();
        assert_eq!(alter_table.get_table_name(), "airports");
        assert_eq!(
            alter_table.get_operations(),
            vec![AlterTableOperation::AddColumn(Column::new("new_col", DataType::Int, false, true))]
        );
    }

    #[test]
    fn test_alter_table_serialize() {
        let operations = vec![
            AlterTableOperation::AddColumn(Column::new("new_col", DataType::Int, false, true)),
        ];
        let alter_table = AlterTable::new("airports".to_string(), operations.clone());
        let serialized = alter_table.serialize();
        assert_eq!(serialized, "ALTER TABLE airports ADD new_col INT");
    }

    #[test]
    fn test_alter_table_deserialize() {
        let serialized = "ALTER TABLE airports ADD new_col INT";
        let alter_table = AlterTable::deserialize(serialized).unwrap();
        assert_eq!(alter_table.get_table_name(), "airports");
        assert_eq!(
            alter_table.get_operations(),
            vec![AlterTableOperation::AddColumn(Column::new("new_col", DataType::Int, false, true))]
        );
    }

    #[test]
    fn test_alter_table_equality() {
        let alter_table1 = AlterTable::new("airports".to_string(), vec![]);
        let alter_table2 = AlterTable::new("airports".to_string(), vec![]);
        assert_eq!(alter_table1, alter_table2);
    }

    #[test]
    fn test_alter_table_drop_column() {
        let query = vec![
            "ALTER".to_string(),
            "TABLE".to_string(),
            "airports".to_string(),
            "DROP".to_string(),
            "old_col".to_string(),
        ];
        let alter_table = AlterTable::new_from_tokens(query).unwrap();
        assert_eq!(alter_table.get_table_name(), "airports");
        assert_eq!(
            alter_table.get_operations(),
            vec![AlterTableOperation::DropColumn("old_col".to_string())]
        );
    }

    #[test]
    fn test_alter_table_modify_column() {
        let query = vec![
            "ALTER".to_string(),
            "TABLE".to_string(),
            "airports".to_string(),
            "MODIFY".to_string(),
            "new_col".to_string(),
            "STRING".to_string(),
        ];
        let alter_table = AlterTable::new_from_tokens(query).unwrap();
        assert_eq!(alter_table.get_table_name(), "airports");
        assert_eq!(
            alter_table.get_operations(),
            vec![AlterTableOperation::ModifyColumn("new_col".to_string(), DataType::String, true)]
        );
    }

    #[test]
    fn test_alter_table_rename_column() {
        let query = vec![
            "ALTER".to_string(),
            "TABLE".to_string(),
            "airports".to_string(),
            "RENAME".to_string(),
            "old_col".to_string(),
            "TO".to_string(),
            "new_col".to_string(),
        ];
        let alter_table = AlterTable::new_from_tokens(query).unwrap();
        assert_eq!(alter_table.get_table_name(), "airports");
        assert_eq!(
            alter_table.get_operations(),
            vec![AlterTableOperation::RenameColumn("old_col".to_string(), "new_col".to_string())]
        );
    }
}
