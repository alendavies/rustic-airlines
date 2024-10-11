// keyspace.rs
use query_coordinator::clauses::keyspace::create_keyspace_cql::CreateKeyspace;
use query_coordinator::clauses::table;
use crate::table::Table;
use crate::errors::NodeError;
use query_coordinator::errors::CQLError;

#[derive(Debug, Clone)]
pub struct Keyspace {
    pub inner: CreateKeyspace,
    pub tables: Vec<Table>,
}

impl Keyspace {
    // Constructor para crear una nueva instancia de Keyspace a partir de CreateKeyspace
    pub fn new(create_keyspace: CreateKeyspace) -> Self {
        Self {
            inner: create_keyspace,
            tables: vec![],
        }
    }

    // Método para obtener el nombre del Keyspace
    pub fn get_name(&self) -> String {
        self.inner.get_name()
    }

    // Método para obtener todas las tablas en el Keyspace
    pub fn get_tables(&self) -> Vec<Table> {
        self.tables.clone()
    }

    // Método para agregar una nueva tabla al Keyspace
    pub fn add_table(&mut self, new_table: Table) -> Result<(), NodeError> {
        if self.tables.contains(&new_table) {
            return Err(NodeError::CQLError(CQLError::InvalidTable));
        }
        self.tables.push(new_table);
        Ok(())
    }

    // Método para obtener una tabla por su nombre
    pub fn get_table(&self, table_name: &str) -> Result<Table, NodeError> {
        self.tables
            .iter()
            .find(|table| table.get_name() == table_name)
            .cloned()
            .ok_or(NodeError::CQLError(CQLError::InvalidTable))
    }

    // Método para eliminar una tabla por su nombre
    pub fn remove_table(&mut self, table_name: &str) -> Result<(), NodeError> {
        let index = self
            .tables
            .iter()
            .position(|table| table.get_name() == table_name)
            .ok_or(NodeError::CQLError(CQLError::InvalidTable))?;
        
        self.tables.remove(index);
        Ok(())
    }
}

// Implementación de PartialEq y Eq para Keyspace, comparando el campo `inner` (CreateKeyspace)
impl PartialEq for Keyspace {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}
