use std::path::{Path, PathBuf};

use query_creator::clauses::{delete_cql::Delete, select_cql::Select, update_cql::Update};

struct FileStorageEngine {
    root: PathBuf,
}

impl FileStorageEngine {
    pub fn new(root: PathBuf) -> Self {
        // crea la carpeta en root (agrega Err)
        Self { root }
    }
}

impl StorageEngine for FileStorageEngine {
    type Error = std::io::Error;

    fn create_keyspace(name: &str) -> Result<(), Self::Error> {
        todo!()
    }

    fn drop_keyspace(name: &str) -> Result<(), Self::Error> {
        todo!()
    }

    fn create_table(keyspace: &str, table: &str, columns: Vec<&str>) -> Result<(), Self::Error> {
        todo!()
    }

    fn drop_table(keyspace: &str, table: &str) -> Result<(), Self::Error> {
        todo!()
    }

    fn add_column_to_table(keyspace: &str, table: &str) -> Result<(), Self::Error> {
        todo!()
    }

    fn remove_column_from_table(
        keyspace: &str,
        table: &str,
        column: &str,
    ) -> Result<(), Self::Error> {
        todo!()
    }

    fn rename_column_from_table(
        keyspace: &str,
        table: &str,
        column: &str,
        new_column: &str,
    ) -> Result<(), Self::Error> {
        todo!()
    }

    fn insert(
        keyspace: &str,
        table: &str,
        values: Vec<&str>,
        index_of_keys: Vec<usize>,
        is_replication: bool,
        if_not_exist: bool,
    ) -> Result<(), Self::Error> {
        todo!()
    }

    fn delete(query: Delete, is_replication: bool) -> Result<(), Self::Error> {
        todo!()
    }

    fn update(query: Update, is_replication: bool) -> Result<(), Self::Error> {
        todo!()
    }

    fn select(query: Select, is_replication: bool) -> Result<Vec<String>, Self::Error> {
        todo!()
    }
}

struct MockStorageEngine;

impl StorageEngine for MockStorageEngine {
    type Error = String;

    fn create_keyspace(name: &str) -> Result<(), Self::Error> {
        todo!()
    }

    fn drop_keyspace(name: &str) -> Result<(), Self::Error> {
        todo!()
    }

    fn create_table(keyspace: &str, table: &str, columns: Vec<&str>) -> Result<(), Self::Error> {
        todo!()
    }

    fn drop_table(keyspace: &str, table: &str) -> Result<(), Self::Error> {
        todo!()
    }

    fn add_column_to_table(keyspace: &str, table: &str) -> Result<(), Self::Error> {
        todo!()
    }

    fn remove_column_from_table(
        keyspace: &str,
        table: &str,
        column: &str,
    ) -> Result<(), Self::Error> {
        todo!()
    }

    fn rename_column_from_table(
        keyspace: &str,
        table: &str,
        column: &str,
        new_column: &str,
    ) -> Result<(), Self::Error> {
        todo!()
    }

    fn insert(
        keyspace: &str,
        table: &str,
        values: Vec<&str>,
        index_of_keys: Vec<usize>,
        is_replication: bool,
        if_not_exist: bool,
    ) -> Result<(), Self::Error> {
        todo!()
    }

    fn delete(query: Delete, is_replication: bool) -> Result<(), Self::Error> {
        todo!()
    }

    fn update(query: Update, is_replication: bool) -> Result<(), Self::Error> {
        todo!()
    }

    fn select(query: Select, is_replication: bool) -> Result<Vec<String>, Self::Error> {
        todo!()
    }
}

pub trait StorageEngine {
    type Error;

    /// Creates a keyspace in the storage location.
    fn create_keyspace(name: &str) -> Result<(), Self::Error>;

    /// Drops a keyspace from the storage location.
    fn drop_keyspace(name: &str) -> Result<(), Self::Error>;

    /// Creates a table in `keyspace` with name `table`.
    fn create_table(keyspace: &str, table: &str, columns: Vec<&str>) -> Result<(), Self::Error>;

    /// Drops a table the storage location.
    fn drop_table(keyspace: &str, table: &str) -> Result<(), Self::Error>;

    /// Adds a columns to `table` in `keyspace`, filling with NULL in existing records.
    fn add_column_to_table(keyspace: &str, table: &str) -> Result<(), Self::Error>;

    /// Removes the `column` column from `table` in `keyspace`.
    fn remove_column_from_table(
        keyspace: &str,
        table: &str,
        column: &str,
    ) -> Result<(), Self::Error>;

    /// Renames the `column` column to `new_column` from `table` in `keyspace`.
    fn rename_column_from_table(
        keyspace: &str,
        table: &str,
        column: &str,
        new_column: &str,
    ) -> Result<(), Self::Error>;

    /// Inserts the `values`, assuming they are in the correct order, in
    /// `table` from `keyspace`.
    ///
    /// TODO: format this
    /// `index_of_keys`: index of the PK from `values` which are partition keys
    /// and clustering columns, use to compare rows.
    /// `is_replication`: boolean that indicates that this insertion belongs to
    /// the replication table
    /// `if_not_exist`: boolean that indicates that if the row exists, it
    /// shouldn't be updated. (no upsertion)
    fn insert(
        keyspace: &str,
        table: &str,
        values: Vec<&str>,
        index_of_keys: Vec<usize>,
        is_replication: bool,
        if_not_exist: bool,
    ) -> Result<(), Self::Error>;

    fn delete(query: Delete, is_replication: bool) -> Result<(), Self::Error>;

    fn update(query: Update, is_replication: bool) -> Result<(), Self::Error>;

    /// In the returned vector, the first row is the header with the columns.
    fn select(query: Select, is_replication: bool) -> Result<Vec<String>, Self::Error>;
}
