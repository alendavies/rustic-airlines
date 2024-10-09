use super::column::Column;
use super::datatype::DataType;

#[derive(Debug, Clone)]
pub enum AlterTableOperation {
    AddColumn(Column),
    DropColumn(String),
    ModifyColumn(String, DataType, bool), // column name, new data type, allows null
    RenameColumn(String, String), // old column name, new column name
}