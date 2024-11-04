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
            CREATE KEYSPACE sky
            WITH REPLICATION = {
                'class': 'SimpleStrategy',
                'replication_factor': 3
            };
        "#;
        self.cassandra_client.execute(create_keyspace_query)?;

        
        let create_flights_table = r#"
            CREATE TABLE sky.flights (
                number TEXT,
                status TEXT,
                departure_time TIMESTAMP,
                arrival_time TIMESTAMP,
                airport TEXT,
                direction TEXT,
                PRIMARY KEY (airport, direction, departure_time, arrival_time)
            )
        "#;
        self.cassandra_client.execute(create_flights_table)?;

        let create_flight_info_table = r#"
            CREATE TABLE sky.flight_info (
                number TEXT,
                lat DOUBLE,
                lon DOUBLE,
                fuel DOUBLE,
                height INT,
                speed INT,
                PRIMARY KEY (number, lat)
            )
        "#;
        self.cassandra_client.execute(create_flight_info_table)?;

        let create_airports_table = r#"
            CREATE TABLE sky.airports (
                iata TEXT,
                country TEXT,
                name TEXT,
                lat DOUBLE,
                lon DOUBLE,
                PRIMARY KEY (country, iata)
            )
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
            "INSERT INTO simulation.airports (iata, country, name, lat, lon) VALUES ('{}', '{}', '{}', {}, {});",
            airport.iata_code, airport.country, airport.name, airport.latitude, airport.longitude
        );
        self.cassandra_client.execute(insert_airport_query)?;
        println!("Airport '{}' added successfully.", airport.iata_code);
        Ok(())
    }

    pub fn insert_flight(
        &mut self,
        flight: &Flight
    ) -> Result<(), ClientError> {
        // Inserción en la tabla flights para el origen (DEPARTURE)
        let insert_departure_query = format!(
            "INSERT INTO simulation.flights (number, status, departure_time, arrival_time, airport, direction) \
             VALUES ('{}', '{}', '{}', '{}', '{}', 'DEPARTURE');",
            flight.flight_number,
            flight.status.as_str(),
            flight.departure_time,
            flight.arrival_time,
            flight.origin.iata_code,
        );

        // Inserción en la tabla flights para el destino (ARRIVAL)
        let insert_arrival_query = format!(
            "INSERT INTO simulation.flights (number, status, departure_time, arrival_time, airport, direction) \
             VALUES ('{}', '{}', null, toTimestamp(now()), '{}', 'ARRIVAL');",
            flight.flight_number,
            flight.status.as_str(),
            flight.destination.iata_code,
        );

        // Inserción en la tabla flight_info con la información del vuelo
        let insert_flight_info_query = format!(
            "INSERT INTO simulation.flight_info (number, lat, lon, fuel, alt, speed) \
             VALUES ('{}', {}, {}, {}, {}, {});",
            flight.flight_number,
            flight.latitude,
            flight.longitude,
            flight.fuel_level,
            flight.altitude,
            flight.average_speed,
        );

        // Ejecución de las consultas en Cassandra
        self.cassandra_client.execute(insert_departure_query)?;
        self.cassandra_client.execute(insert_arrival_query)?;
        self.cassandra_client.execute(insert_flight_info_query)?;

        Ok(())
    }

    pub fn update_flight(
        &mut self,
        flight: &Flight
    ) -> Result<(), ClientError> {
        let update_query_status = format!(
                "UPDATE simulation.flights SET latitude = {}, longitude = {}, altitude = {}, fuel_level = {}, status = '{}' WHERE flight_number = '{}';",
                flight.latitude,
                flight.longitude,
                flight.altitude,
                flight.fuel_level,
                flight.status.as_str(),
                flight.flight_number
            );
        self.cassandra_client.execute(update_query_status)?;
        Ok(())
    }

    /// Executes a custom CQL query
    pub fn execute_query(&mut self, query: &str) -> Result<QueryResult, ClientError> {
        self.cassandra_client.execute(query)
    }
}
