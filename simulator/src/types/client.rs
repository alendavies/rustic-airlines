use chrono::{DateTime, NaiveDateTime, NaiveTime};
use driver::{CassandraClient, ClientError, QueryResult};
use native_protocol::messages::result::rows::ColumnValue;
use native_protocol::messages::result::{result_, rows};
use std::collections::{BTreeMap, HashMap};
use std::net::Ipv4Addr;

use crate::types::airport::Airport;
use crate::types::flight::Flight;
use crate::types::flight_status::FlightStatus;

pub struct Client {
    cassandra_client: CassandraClient,
    ip: Ipv4Addr,
}

impl Client {
    /// Initializes the flight simulation by connecting to Cassandra and setting up the keyspace and tables.
    pub fn new(ip: Ipv4Addr) -> Result<Self, ClientError> {
        let mut cassandra_client = CassandraClient::connect(ip, None)?;

        cassandra_client.startup()?;

        let mut client = Self {
            cassandra_client,
            ip,
        };
        client.setup_keyspace_and_tables()?;

        Ok(client)
    }

    pub fn recreate_client(&mut self) -> Result<Client, ClientError> {
        let ip = self.ip.clone();
        let mut cassandra_client = CassandraClient::connect(ip, Some(self.cassandra_client.config()))?;

        cassandra_client.startup()?;

        let client = Client {
            cassandra_client,
            ip,
        };

        Ok(client)
    }

    /// Sets up the keyspace and required tables in Cassandra
    fn setup_keyspace_and_tables(&mut self) -> Result<(), ClientError> {
        let create_keyspace_query = r#"
            CREATE KEYSPACE sky
            WITH REPLICATION = {
                'class': 'SimpleStrategy',
                'replication_factor': 3
            };
        "#;
        self.cassandra_client
            .execute(create_keyspace_query, "quorum")?;

        let create_flights_table = r#"
            CREATE TABLE sky.flights (
                number TEXT,
                status TEXT,
                lat DOUBLE,
                lon DOUBLE,
                angle FLOAT,
                departure_time TIMESTAMP,
                arrival_time TIMESTAMP,
                airport TEXT,
                direction TEXT,
                PRIMARY KEY (airport, direction, departure_time, arrival_time, number)
            )
            "#;
        self.cassandra_client
            .execute(create_flights_table, "quorum")?;

        let create_flight_info_table = r#"
            CREATE TABLE sky.flight_info (
                number TEXT,
                fuel DOUBLE,
                height INT,
                speed INT,
                origin TEXT,
                destination TEXT,
                PRIMARY KEY (number)
            )
        "#;
        self.cassandra_client
            .execute(create_flight_info_table, "quorum")?;

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
        self.cassandra_client
            .execute(create_airports_table, "quorum")?;

        println!("Keyspace and tables created successfully.");
        Ok(())
    }

    pub fn insert_airport(&mut self, airport: &Airport) -> Result<(), ClientError> {
        let insert_airport_query = format!(
            "INSERT INTO sky.airports (iata, country, name, lat, lon) VALUES ('{}', '{}', '{}', {}, {});",
            airport.iata_code, airport.country, airport.name, airport.latitude, airport.longitude
        );

        if let Err(e) = self
            .cassandra_client
            .execute(&insert_airport_query, "quorum")
        {
            println!("No se pudo agregar el aeropuerto, el error es {:?}", e);
            return Ok(());
        }

        println!("Airport '{}' added successfully.", airport.iata_code);
        Ok(())
    }

    pub fn insert_flight(&mut self, flight: &Flight) -> Result<(), ClientError> {
        // Inserción en la tabla flights para el origen (DEPARTURE)
        let insert_departure_query = format!(
            "INSERT INTO sky.flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('{}', '{}', {}, {}, {}, {}, {}, '{}', 'departure');",
            flight.flight_number,
            flight.status.as_str(),
            flight.latitude,
            flight.longitude,
            flight.angle,
            flight.departure_time.and_utc().timestamp(),
            flight.arrival_time.and_utc().timestamp(),
            flight.origin.iata_code,
        );

        // Inserción en la tabla flights para el destino (ARRIVAL)
        let insert_arrival_query = format!(
            "INSERT INTO sky.flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('{}', '{}', {}, {}, {}, {}, {}, '{}', 'arrival');",
            flight.flight_number,
            flight.status.as_str(),
            flight.latitude,
            flight.longitude,
            flight.angle,
            flight.departure_time.and_utc().timestamp(),
            flight.arrival_time.and_utc().timestamp(),
            flight.destination.iata_code
        );

        // Inserción en la tabla flight_info con la información del vuelo
        let insert_flight_info_query = format!(
            "INSERT INTO sky.flight_info (number, fuel, height, speed, origin, destination) VALUES ('{}', {}, {}, {}, '{}', '{}');",
            flight.flight_number,
            flight.fuel_level,
            flight.altitude,
            flight.average_speed,
            flight.origin.iata_code,
            flight.destination.iata_code
        );

        if let Err(e) = self
            .cassandra_client
            .execute(&insert_departure_query, "quorum")
        {
            println!("No se pudo agregar el vuelo, el error es {:?}", e);
            return Ok(());
        }

        if let Err(e) = self
            .cassandra_client
            .execute(&insert_arrival_query, "quorum")
        {
            println!("No se pudo agregar el aeropuerto, el error es {:?}", e);
            return Ok(());
        }

        if let Err(e) = self
            .cassandra_client
            .execute(&insert_flight_info_query, "one")
        {
            println!("No se pudo agregar el aeropuerto, el error es {:?}", e);
            return Ok(());
        }

        println!("Flight '{}' added successfully.", flight.flight_number);

        Ok(())
    }

    pub fn update_flight(&mut self, flight: &Flight) -> Result<(), ClientError> {
        let update_query_status_departure = format!(
                "UPDATE sky.flights SET lat = {}, lon = {}, angle = {} WHERE airport = '{}' AND direction = '{}' AND departure_time = {} AND arrival_time = {} AND number = {};",
                flight.latitude,
                flight.longitude,
                flight.angle,
                flight.origin.iata_code,
                "departure",
                flight.departure_time.and_utc().timestamp(),
                flight.arrival_time.and_utc().timestamp(),
                flight.flight_number
            );

        if let Err(e) = self
            .cassandra_client
            .execute(&update_query_status_departure, "one")
        {
            println!("No se pudo actualizar el vuelo, el error es {:?}", e);

            return Ok(());
        }

        let update_query_status_arrival = format!(
                "UPDATE sky.flights SET lat = {}, lon = {}, angle = {} WHERE airport = '{}' AND direction = '{}' AND departure_time = {} AND arrival_time = {} AND number = {};",
                flight.latitude,
                flight.longitude,
                flight.angle,
                flight.destination.iata_code,
                "arrival",
                flight.departure_time.and_utc().timestamp(),
                flight.arrival_time.and_utc().timestamp(),
                flight.flight_number
            );

        if let Err(e) = self
            .cassandra_client
            .execute(&update_query_status_arrival, "one")
        {
            println!("No se pudo actualizar el vuelo, el error es {:?} 1", e);
            return Ok(());
        }

        let update_query_flight_info = format!(
            "UPDATE sky.flight_info SET fuel = {}, speed = {}, height = {} WHERE number = '{}';",
            flight.fuel_level, flight.average_speed, flight.altitude, flight.flight_number
        );

        if let Err(e) = self
            .cassandra_client
            .execute(&update_query_flight_info, "one")
        {
            println!("No se pudo actualizar el vuelo, el error es {:?} 2", e);
            return Ok(());
        }

        Ok(())
    }

    pub fn update_flight_status(&mut self, flight: &Flight) -> Result<(), ClientError> {
        let update_query_status_departure = format!(
                "UPDATE sky.flights SET status = '{}', lat = {}, lon = {}, WHERE airport = '{}' AND direction = '{}' AND departure_time = {} AND arrival_time = {} AND number = {};",
                flight.status.as_str(),
                flight.latitude,
                flight.longitude,
                flight.origin.iata_code,
                "departure",
                flight.departure_time.and_utc().timestamp(),
                flight.arrival_time.and_utc().timestamp(),
                flight.flight_number
            );

        if let Err(e) = self
            .cassandra_client
            .execute(&update_query_status_departure, "quorum")
        {
            println!(
                "No se pudo actualizar el estado del vuelo, el error es {:?} 1",
                e
            );

            return Ok(());
        }

        let update_query_status_arrival = format!(
                "UPDATE sky.flights SET status = '{}', lat = {}, lon = {}, WHERE airport = '{}' AND direction = '{}' AND departure_time = {} AND arrival_time = {} AND number = {};",
                flight.status.as_str(),
                flight.latitude,
                flight.longitude,
                flight.destination.iata_code,
                "arrival",
                flight.departure_time.and_utc().timestamp(),
                flight.arrival_time.and_utc().timestamp(),
                flight.flight_number
            );
        if let Err(e) = self
            .cassandra_client
            .execute(&update_query_status_arrival, "quorum")
        {
            println!(
                "No se pudo actualizar el estado del vuelo, el error es {:?}",
                e
            );
            return Ok(());
        }

        Ok(())
    }

    pub fn fetch_flights(
        &mut self,
        date: NaiveDateTime,
        airports: &HashMap<String, Airport>,
    ) -> Result<Vec<Flight>, ClientError> {
        let from = NaiveDateTime::new(date.date(), NaiveTime::from_hms_opt(0, 0, 0).unwrap());
        let from = from.and_utc().timestamp();

        let mut flights: Vec<Flight> = Vec::new();

        // Iterate through each airport in the HashMap
        for (airport_code, airport) in airports {
            let query = format!(
                "SELECT number, status, lat, lon, angle, departure_time, arrival_time, direction \
                FROM sky.flights \
                WHERE airport = '{airport_code}' AND direction = 'departure' AND arrival_time > {from}"
            );
        
            match self.cassandra_client.execute(&query, "quorum") {
                Ok(result) => {
                    if let QueryResult::Result(result_::Result::Rows(res)) = result {
                        for row in res.rows_content {
                            let flight = Client::build_flight_from_row(self, &row, airport)?;
                            flights.push(flight);
                        }
                    }
                }
                Err(e) => {
                    println!("Query error for airport {}: {:?}", airport_code, e);
                }
            }
        }

        Ok(flights)
    }

    fn build_flight_from_row(
        &mut self,
        row: &BTreeMap<String, ColumnValue>,
        selected_airport: &Airport,
    ) -> Result<Flight, ClientError> {
        let mut flight = Flight {
            flight_number: "XXXX".to_string(),
            status: FlightStatus::Scheduled,
            departure_time: NaiveDateTime::default(),
            arrival_time: NaiveDateTime::default(),
            origin: selected_airport.clone(),
            destination: Airport::default(),
            latitude: 0.0,
            longitude: 0.0,
            angle: 0.0,
            altitude: 0,
            fuel_level: 100.0,
            total_distance: 0.0,
            distance_traveled: 0.0,
            average_speed: 0,
        };

        if let Some(number) = row.get("number") {
            if let rows::ColumnValue::Ascii(number) = number {
                flight.flight_number = number.to_string();
            }
        } else {
            println!("1: {:?}", row);
            return Err(ClientError);
        }

        if let Some(status) = row.get("status") {
            if let rows::ColumnValue::Ascii(status) = status {
                match FlightStatus::from_str(status) {
                    Ok(status) => flight.status = status,
                    Err(_) => return Err(ClientError),
                }
            }
        } else {
            println!("2: {:?}", row);
            return Err(ClientError);
        }

        if let Some(departure_time) = row.get("departure_time") {
            if let rows::ColumnValue::Timestamp(departure_time) = departure_time {
                if let Some(datetime) = DateTime::from_timestamp(*departure_time, 0) {
                    flight.departure_time = datetime.naive_utc()
                } else {
                    return Err(ClientError);
                }
            }
        } else {
            println!("3: {:?}", row);
            return Err(ClientError);
        }

        if let Some(arrival_time) = row.get("arrival_time") {
            if let rows::ColumnValue::Timestamp(arrival_time) = arrival_time {
                if let Some(datetime) = DateTime::from_timestamp(*arrival_time, 0) {
                    flight.arrival_time = datetime.naive_utc()
                } else {
                    return Err(ClientError);
                }
            }
        } else {
            println!("4: {:?}", row);
            return Err(ClientError);
        }

        if let Some(lat) = row.get("lat") {
            if let rows::ColumnValue::Double(lat) = lat {
                flight.latitude = *lat;
            }
        } else {
            println!("5: {:?}", row);
            return Err(ClientError);
        }

        if let Some(lon) = row.get("lon") {
            if let rows::ColumnValue::Double(lon) = lon {
                flight.longitude = *lon;
            }
        } else {
            println!("6: {:?}", row);
            return Err(ClientError);
        }

        if let Some(angle) = row.get("angle") {
            if let rows::ColumnValue::Float(angle) = angle {
                flight.angle = *angle;
            }
        } else {
            println!("7: {:?}", row);
            return Err(ClientError);
        }

        Ok(flight)
    }

    pub fn fetch_flight_info(
        &mut self,
        flight: &mut Flight,
        airports: &HashMap<String, Airport>,
    ) -> Result<(), ClientError> {
        let number = &flight.flight_number;

        let query = format!(
            "SELECT fuel, height, speed, destination FROM sky.flight_info WHERE number = '{number}'"
        );

        let result = self.cassandra_client.execute(&query, "one")?;

        if let QueryResult::Result(result_::Result::Rows(res)) = result {
            for row in res.rows_content {
                if let Some(fuel) = row.get("fuel") {
                    if let rows::ColumnValue::Double(fuel) = fuel {
                        flight.fuel_level = *fuel;
                    }
                } else {
                    return Err(ClientError);
                }

                if let Some(height) = row.get("height") {
                    if let rows::ColumnValue::Int(height) = height {
                        flight.altitude = *height;
                    }
                } else {
                    return Err(ClientError);
                }

                if let Some(speed) = row.get("speed") {
                    if let rows::ColumnValue::Int(speed) = speed {
                        flight.average_speed = *speed;
                    }
                } else {
                    return Err(ClientError);
                }

                if let Some(destination) = row.get("destination") {
                    if let rows::ColumnValue::Ascii(destination) = destination {
                        if let Some(airport) = airports.get(destination) {
                            flight.destination = airport.clone();
                        } else {
                            return Err(ClientError);
                        }
                    }
                } else {
                    return Err(ClientError);
                }
            }
        }

        Ok(())
    }
}
