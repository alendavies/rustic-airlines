// table.rs
use query_coordinator::clauses::{table::create_table_cql::CreateTable, types::column::Column};
use std::fmt;

use crate::errors::NodeError;

#[derive(Clone, PartialEq)]
pub struct Table {
    pub inner: CreateTable,
}

impl Table {
    // Constructor para crear una nueva instancia de Table a partir de CreateTable
    pub fn new(create_table: CreateTable) -> Self {
        Self { inner: create_table}
    }

    // Método para obtener el nombre de la tabla
    pub fn get_name(&self) -> String {
        self.inner.get_name()
    }

    // Método para obtener las columnas de la tabla
    pub fn get_columns(&self) -> Vec<Column> {
        self.inner.get_columns()
    }

    pub fn get_column_index(&self, column_name: &str) -> Option<usize> {
        self.get_columns()
            .iter()
            .position(|col| col.name == column_name)
    }

    pub fn is_primary_key(&self, column_name: &str) -> Result<bool, NodeError> {
        let column_index = self.get_column_index(column_name).ok_or(NodeError::OtherError)?;
        let columns = self.inner.get_columns(); // Guarda una referencia a los columns
        let column = columns.get(column_index).ok_or(NodeError::OtherError)?;
        Ok(column.is_primary_key)
    }

    pub fn get_primary_key(&self) -> Result<String, NodeError> {
        let columns = self.get_columns();
        for column in columns {
            if column.is_primary_key {
                return Ok(column.name.clone());
            }
        }
        Err(NodeError::OtherError)
    }


}
impl fmt::Debug for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Table: {}", self.get_name())
    }
}

