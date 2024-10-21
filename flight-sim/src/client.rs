use std::net::Ipv4Addr;
use crate::types::flight::Flight;
use crate::types::airport::Airport;
use crate::native_protocol::{messages::query::QueryParams, CassandraClient, ClientError, QueryResult};

pub struct Client {
    cassandra_client: CassandraClient,
}

impl Client {
    /// Initializes the flight simulation by connecting to Cassandra and setting up the keyspace and tables.
    pub fn new(ip: Ipv4Addr) -> Result<Self, ClientError> {
        
        let mut cassandra_client = CassandraClient::connect(ip)?;

        cassandra_client.startup()?;

        let client = Self { cassandra_client };
        client.setup_keyspace_and_tables()?;

        Ok(client)
    }

    /// Sets up the keyspace and required tables in Cassandra
    fn setup_keyspace_and_tables(&self) -> Result<(), ClientError> {
        
        let create_keyspace_query = r#"
            CREATE KEYSPACE simulation
            WITH REPLICATION = {
                'class': 'SimpleStrategy',
                'replication_factor': 3
            };
        "#;
        self.cassandra_client.execute(create_keyspace_query)?;

        
        let create_flights_table = r#"
            CREATE TABLE simulation.flights (
                flight_number TEXT PRIMARY KEY,
                origin TEXT,
                destination TEXT,
                average_speed DOUBLE,
                status TEXT,
                latitude DOUBLE,
                longitude DOUBLE,
                altitude DOUBLE,
                fuel_level DOUBLE
            ) WITH CLUSTERING ORDER BY (flight_number DESC);
        "#;
        self.cassandra_client.execute(create_flights_table)?;

        let create_airports_table = r#"
            CREATE TABLE simulation.airports (
                iata_code TEXT PRIMARY KEY,
                name TEXT,
                latitude DOUBLE,
                longitude DOUBLE
            ) WITH CLUSTERING ORDER BY (iata_code DESC);
        "#;
        self.cassandra_client.execute(create_airports_table)?;

        println!("Keyspace and tables created successfully.");
        Ok(())
    }

    pub fn insert_airport(
        &mut self, 
        airport: &Airport
    ) -> Result<(), ClientError> {
        let insert_airport_query = format!(
            "INSERT INTO simulation.airports (iata_code, name, latitude, longitude) VALUES ('{}', '{}', {}, {});",
            airport.iata_code, airport.name, airport.latitude, airport.longitude
        );
        self.cassandra_client.execute(insert_airport_query)?;
        println!("Airport '{}' added successfully.", airport.iata_code);
        Ok(())
    }

    pub fn insert_flight(
        &mut self,
        flight: &Flight
    ) -> Result<(), ClientError> {
        let insert_flight_query = format!(
            "INSERT INTO simulation.flights (flight_number, origin, destination, average_speed, status, latitude, longitude, altitude, fuel_level) \
             VALUES ('{}', '{}', '{}', {}, '{}', {}, {}, {}, {});",
            flight.flight_number,
            flight.origin.iata_code,
            flight.destination.iata_code,
            flight.average_speed,
            flight.status.as_str(),
            flight.latitude,
            flight.longitude,
            flight.altitude,
            flight.fuel_level
        );
        self.cassandra_client.execute(insert_flight_query)?;
        Ok(())
    }

    pub fn update_flight(
        &mut self,
        flight: &Flight
    ) -> Result<(), ClientError> {
        let update_query = format!(
                "UPDATE simulation.flights SET latitude = {}, longitude = {}, altitude = {}, fuel_level = {}, status = '{}' WHERE flight_number = '{}';",
                flight.latitude,
                flight.longitude,
                flight.altitude,
                flight.fuel_level,
                flight.status.as_str(),
                flight.flight_number
            );
        self.cassandra_client.execute(update_query)?;
        Ok(())
    }

    /// Executes a custom CQL query
    pub fn execute_query(&mut self, query: &str) -> Result<QueryResult, ClientError> {
        self.cassandra_client.execute(query)
    }
}
