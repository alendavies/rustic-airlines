pub struct Record;

pub struct Selection {
    pub table: String,
    pub records: Vec<Record>,
}

pub trait StorageEngine {
    fn select(&self) -> Result<Vec<Record>, ()>;
    fn insert(&self) -> Result<(), ()>;
    fn delete(&self) -> Result<(), ()>;
    fn create_table(&self) -> Result<(), ()>;
    fn create_keyspace(&self) -> Result<(), ()>;
}

pub struct CsvStorageEngine;

impl StorageEngine for CsvStorageEngine {
    fn select(&self) -> Result<Vec<Record>, ()> {
        todo!()
    }

    fn insert(&self) -> Result<(), ()> {
        todo!()
    }

    fn delete(&self) -> Result<(), ()> {
        todo!()
    }

    fn create_table(&self) -> Result<(), ()> {
        todo!()
    }

    fn create_keyspace(&self) -> Result<(), ()> {
        todo!()
    }
}
