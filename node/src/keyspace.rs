// keyspace.rs

// Ordered imports
use std::fmt;

use query_creator::clauses::keyspace::create_keyspace_cql::CreateKeyspace;
use query_creator::errors::CQLError;

use crate::errors::NodeError;
use crate::table::Table;

/// Represents a keyspace in the system.
/// The keyspace can contain multiple tables and handles replication.
#[derive(Clone)]
pub struct Keyspace {
    pub inner: CreateKeyspace,
    pub tables: Vec<Table>,
}

impl Keyspace {
    /// Creates a new instance of `Keyspace` from a `CreateKeyspace`.
    ///
    /// # Arguments
    ///
    /// * `create_keyspace` - The keyspace definition to create the instance.
    ///
    /// # Returns
    /// Returns a new instance of `Keyspace`.
    pub fn new(create_keyspace: CreateKeyspace) -> Self {
        Self {
            inner: create_keyspace,
            tables: vec![],
        }
    }

    /// Gets the name of the keyspace.
    ///
    /// # Returns
    /// Returns the keyspace name as a `String`.
    pub fn get_name(&self) -> String {
        self.inner.get_name()
    }

    /// Retrieves all tables associated with this keyspace.
    ///
    /// # Returns
    /// Returns a vector of tables (`Vec<Table>`).
    pub fn get_tables(&self) -> Vec<Table> {
        self.tables.clone()
    }

    /// Gets the replication class of the keyspace.
    ///
    /// # Returns
    /// Returns the replication class as a `String`.
    pub fn get_replication_class(&self) -> String {
        self.inner.get_replication_class()
    }

    /// Gets the replication factor of the keyspace.
    ///
    /// # Returns
    /// Returns the replication factor as `u32`.
    pub fn get_replication_factor(&self) -> u32 {
        self.inner.get_replication_factor()
    }

    /// Updates the replication class of the keyspace.
    ///
    /// # Arguments
    ///
    /// * `replication_class` - The new replication class.
    pub fn update_replication_class(&mut self, replication_class: String) {
        self.inner.update_replication_class(replication_class);
    }

    /// Updates the replication factor of the keyspace.
    ///
    /// # Arguments
    ///
    /// * `replication_factor` - The new replication factor.
    pub fn update_replication_factor(&mut self, replication_factor: u32) {
        self.inner.update_replication_factor(replication_factor)
    }

    /// Adds a new table to the keyspace.
    ///
    /// # Arguments
    ///
    /// * `new_table` - The table to add.
    ///
    /// # Returns
    /// Returns `Ok(())` if the table was successfully added, or a `NodeError` if the table already exists.
    pub fn add_table(&mut self, new_table: Table) -> Result<(), NodeError> {
        if self.tables.contains(&new_table) {
            return Err(NodeError::CQLError(CQLError::InvalidTable));
        }
        self.tables.push(new_table);
        Ok(())
    }

    /// Retrieves a table by its name.
    ///
    /// # Arguments
    ///
    /// * `table_name` - The name of the table to search for.
    ///
    /// # Returns
    /// Returns the found table or a `NodeError` if not found.
    pub fn get_table(&self, table_name: &str) -> Result<Table, NodeError> {
        self.tables
            .iter()
            .find(|table| table.get_name() == table_name)
            .cloned()
            .ok_or(NodeError::CQLError(CQLError::InvalidTable))
    }

    /// Removes a table by its name.
    ///
    /// # Arguments
    ///
    /// * `table_name` - The name of the table to remove.
    ///
    /// # Returns
    /// Returns `Ok(())` if the table was successfully removed or a `NodeError` if not found.
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

// Implementation of PartialEq and Eq for Keyspace, comparing the `inner` field (CreateKeyspace)
impl PartialEq for Keyspace {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl fmt::Debug for Keyspace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Keyspace: {} (RF: {}, RC: {})",
            self.get_name(),
            self.get_replication_factor(),
            self.get_replication_class()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::Table;
    use query_creator::clauses::keyspace::create_keyspace_cql::CreateKeyspace;
    use query_creator::clauses::table::create_table_cql::CreateTable;

    #[test]
    fn test_create_keyspace() {
        let query = "CREATE KEYSPACE example WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};";

        let create_keyspace = CreateKeyspace::deserialize(query).unwrap();
        let keyspace = Keyspace::new(create_keyspace);

        assert_eq!(keyspace.get_name(), "example");
        assert_eq!(keyspace.get_replication_class(), "SimpleStrategy");
        assert_eq!(keyspace.get_replication_factor(), 3);
    }

    #[test]
    fn test_add_table() {
        let query = "CREATE KEYSPACE example WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}";
        let create_keyspace = CreateKeyspace::deserialize(query).unwrap();
        let mut keyspace = Keyspace::new(create_keyspace);

        let create_table_query = "CREATE TABLE test_table (id INT PRIMARY KEY, name TEXT)";
        let create_table = CreateTable::deserialize(create_table_query).unwrap();
        let table = Table::new(create_table);

        let result = keyspace.add_table(table.clone());
        assert!(result.is_ok());
        assert_eq!(keyspace.get_tables().len(), 1);
        assert_eq!(keyspace.get_tables()[0], table);
    }

    #[test]
    fn test_remove_table() {
        let query = "CREATE KEYSPACE example WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};";
        let create_keyspace = CreateKeyspace::deserialize(query).unwrap();
        let mut keyspace = Keyspace::new(create_keyspace);

        let create_table_query = "CREATE TABLE test_table (id INT PRIMARY KEY, name TEXT)";
        let create_table = CreateTable::deserialize(create_table_query).unwrap();
        let table = Table::new(create_table);

        keyspace.add_table(table.clone()).unwrap();
        let result = keyspace.remove_table("test_table");
        assert!(result.is_ok());
        assert!(keyspace.get_tables().is_empty());
    }

    #[test]
    fn test_get_table() {
        let query = "CREATE KEYSPACE example WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}";
        let create_keyspace = CreateKeyspace::deserialize(query).unwrap();
        let mut keyspace = Keyspace::new(create_keyspace);

        let create_table_query = "CREATE TABLE test_table (id INT PRIMARY KEY, name TEXT)";
        let create_table = CreateTable::deserialize(create_table_query).unwrap();
        let table = Table::new(create_table);

        keyspace.add_table(table.clone()).unwrap();
        let result = keyspace.get_table("test_table");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), table);
    }

    #[test]
    fn test_get_table_not_found() {
        let query = "CREATE KEYSPACE example WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}";
        let create_keyspace = CreateKeyspace::deserialize(query).unwrap();
        let keyspace = Keyspace::new(create_keyspace);

        let result = keyspace.get_table("nonexistent_table");
        assert!(result.is_err());
    }
}
