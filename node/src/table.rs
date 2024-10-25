use query_creator::clauses::{table::create_table_cql::CreateTable, types::column::Column};
use std::{fmt, vec};

use crate::errors::NodeError;

/// Represents a table in a database, containing metadata such as columns and primary keys.
#[derive(Clone, PartialEq)]
pub struct Table {
    pub inner: CreateTable,
}

impl Table {
    /// Creates a new `Table` instance from a `CreateTable` CQL object.
    ///
    /// # Parameters
    /// - `create_table`: The CQL structure to initialize the table.
    ///
    /// # Returns
    /// A new `Table` instance.
    pub fn new(create_table: CreateTable) -> Self {
        Self {
            inner: create_table,
        }
    }

    /// Gets the name of the table.
    ///
    /// # Returns
    /// The table name as a `String`.
    pub fn get_name(&self) -> String {
        self.inner.get_name()
    }

    /// Retrieves all columns in the table.
    ///
    /// # Returns
    /// A `Vec<Column>` containing the columns.
    pub fn get_columns(&self) -> Vec<Column> {
        self.inner.get_columns()
    }

    /// Gets the index of a column by its name.
    ///
    /// # Parameters
    /// - `column_name`: The name of the column.
    ///
    /// # Returns
    /// The index of the column, or `None` if the column is not found.
    pub fn get_column_index(&self, column_name: &str) -> Option<usize> {
        self.get_columns()
            .iter()
            .position(|col| col.name == column_name)
    }

    /// Checks if a specific column is the primary key.
    ///
    /// # Parameters
    /// - `column_name`: The name of the column to check.
    ///
    /// # Returns
    /// `Ok(true)` if the column is the primary key, `Ok(false)` otherwise, or an error if the column is not found.
    pub fn is_primary_key(&self, column_name: &str) -> Result<bool, NodeError> {
        let column_index = self
            .get_column_index(column_name)
            .ok_or(NodeError::OtherError)?;
        let columns = self.inner.get_columns();
        let column = columns.get(column_index).ok_or(NodeError::OtherError)?;
        Ok(column.is_primary_key)
    }

    /// Gets the name of the primary key column.
    ///
    /// # Returns
    /// The name of the primary key column as a `String`, or an error if no primary key is found.
    pub fn get_partition_keys(&self) -> Result<Vec<String>, NodeError> {
        let mut partitioner_keys: Vec<String> = vec![];
        let columns = self.get_columns();
        for column in columns {
            if column.is_partition_key {
                partitioner_keys.push(column.name.clone());
            }
        }
        if partitioner_keys.is_empty() {
            Err(NodeError::OtherError)
        } else {
            Ok(partitioner_keys)
        }
    }

    /// Gets the name of the primary key column.
    ///
    /// # Returns
    /// The name of the primary key column as a `String`, or an error if no primary key is found.
    pub fn get_clustering_columns(&self) -> Result<Vec<String>, NodeError> {
        let mut clustering_columns: Vec<String> = vec![];
        let columns = self.get_columns();
        for column in columns {
            if column.is_clustering_column {
                clustering_columns.push(column.name.clone());
            }
        }
        if clustering_columns.is_empty() {
            Err(NodeError::OtherError)
        } else {
            Ok(clustering_columns)
        }
    }
}

/// Implements `fmt::Debug` for `Table` to provide human-readable information for debugging.
impl fmt::Debug for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Table: {}", self.get_name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper function to create a `CreateTable` using valid CQL tokens.
    fn create_sample_table() -> Table {
        let query_tokens = vec![
            "CREATE".to_string(),
            "TABLE".to_string(),
            "users".to_string(),
            "(id INT PRIMARY KEY, name TEXT, age INT)".to_string(),
        ];

        let create_table = CreateTable::new_from_tokens(query_tokens).unwrap();
        Table::new(create_table)
    }

    #[test]
    fn test_table_creation() {
        let table = create_sample_table();
        assert_eq!(table.get_name(), "users");
    }

    #[test]
    fn test_get_columns() {
        let table = create_sample_table();
        let columns = table.get_columns();
        assert_eq!(columns.len(), 3);
    }

    #[test]
    fn test_get_column_index() {
        let table = create_sample_table();
        assert_eq!(table.get_column_index("name"), Some(1));
        assert_eq!(table.get_column_index("nonexistent"), None);
    }

    #[test]
    fn test_is_primary_key() {
        let table = create_sample_table();
        assert_eq!(table.is_primary_key("id").unwrap(), true);
        assert_eq!(table.is_primary_key("name").unwrap(), false);
    }

    #[test]
    fn test_get_primary_key() {
        let table = create_sample_table();
        assert_eq!(table.get_partition_keys().unwrap(), vec!["id"]);
    }

    #[test]
    fn test_table_debug() {
        let table = create_sample_table();
        let debug_output = format!("{:?}", table);
        assert!(debug_output.contains("Table: users"));
    }
}
