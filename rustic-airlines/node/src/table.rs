// table.rs
use query_coordinator::clauses::{table::create_table_cql::CreateTable, types::column::Column};
use std::fmt;

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
}

impl fmt::Debug for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Table: {}", self.get_name())
    }
}

