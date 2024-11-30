use std::collections::{BTreeMap, HashMap};
use std::net::Ipv4Addr;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime};
use driver::{CassandraClient, ClientError, QueryResult};
use native_protocol::messages::result::rows::ColumnValue;
use native_protocol::messages::result::{result_, rows};

use crate::types::flight::Flight;
use crate::types::airport::Airport;
use crate::types::flight_status::FlightStatus;
use crate::types::sim_state::{self, SimState};

pub struct Client {
    cassandra_client: CassandraClient,
}

impl Client {
    /// Initializes the flight simulation by connecting to Cassandra and setting up the keyspace and tables.
    pub fn new(ip: Ipv4Addr) -> Result<Self, ClientError> {
        
        let mut cassandra_client = CassandraClient::connect(ip)?;

        cassandra_client.startup()?;

        let mut client = Self { cassandra_client };
        client.setup_keyspace_and_tables()?;

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
        self.cassandra_client.execute(&create_keyspace_query, "all")?;

        
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
                PRIMARY KEY (direction, airport, departure_time, arrival_time, number)
            )
            "#;
        self.cassandra_client.execute(&create_flights_table, "all")?;

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
        self.cassandra_client.execute(&create_flight_info_table, "all")?;

        let create_airports_table = r#"
            CREATE TABLE airports (
                iata TEXT,
                country TEXT,
                name TEXT,
                lat DOUBLE,
                lon DOUBLE,
                PRIMARY KEY (country, iata)
            )
        "#;
        self.cassandra_client.execute(&create_airports_table, "all")?;

        println!("Keyspace and tables created successfully.");
        Ok(())
    }

    pub fn insert_airport(
        &mut self, 
        airport: &Airport
    ) -> Result<(), ClientError> {
        let insert_airport_query = format!(
            "INSERT INTO sky.airports (iata, country, name, lat, lon) VALUES ('{}', '{}', '{}', {}, {});",
            airport.iata_code, airport.country, airport.name, airport.latitude, airport.longitude
        );
        self.cassandra_client.execute(&insert_airport_query, "all")?;
        println!("Airport '{}' added successfully.", airport.iata_code);
        Ok(())
    }

    pub fn insert_flight(
        &mut self,
        flight: &Flight
    ) -> Result<(), ClientError> {
        // Inserción en la tabla flights para el origen (DEPARTURE)
        let insert_departure_query = format!(
            "INSERT INTO sky.flights (number, status, departure_time, arrival_time, airport, direction, lat, lon, angle) VALUES ('{}', '{}', {}, {}, '{}', 'DEPARTURE', {}, {}, {});",
            flight.flight_number,
            flight.status.as_str(),
            flight.departure_time.and_utc().timestamp(),
            flight.arrival_time.and_utc().timestamp(),
            flight.origin.iata_code,
            flight.latitude,
            flight.longitude,
            flight.angle
        );

        // Inserción en la tabla flights para el destino (ARRIVAL)
        let insert_arrival_query = format!(
            "INSERT INTO sky.flights (number, status, departure_time, arrival_time, airport, direction, lat, lon, angle) VALUES ('{}', '{}', {}, {}, '{}', 'ARRIVAL', {}, {}, {});",
            flight.flight_number,
            flight.status.as_str(),
            flight.departure_time.and_utc().timestamp(),
            flight.arrival_time.and_utc().timestamp(),
            flight.destination.iata_code,
            flight.latitude,
            flight.longitude,
            flight.angle
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

        // Ejecución de las consultas en Cassandra
        self.cassandra_client.execute(&insert_departure_query, "all")?;
        self.cassandra_client.execute(&insert_arrival_query, "all")?;
        self.cassandra_client.execute(&insert_flight_info_query, "all")?;

        println!("Flight '{}' added successfully.", flight.flight_number);

        Ok(())
    }

    pub fn update_flight(
        &mut self,
        flight: &Flight
    ) -> Result<(), ClientError> {
        let update_query_status_departure = format!(
                "UPDATE sky.flights SET status = '{}', lat = {}, lon = {}, angle = {} WHERE airport = '{}' AND direction = '{}' AND departure_time = {} AND arrival_time = {} AND number = {};",
                flight.status.as_str(),
                flight.latitude,
                flight.longitude,
                flight.angle,
                flight.origin.iata_code,
                "DEPARTURE",
                flight.departure_time.and_utc().timestamp(),
                flight.arrival_time.and_utc().timestamp(),
                flight.flight_number
            );
        self.cassandra_client.execute(&update_query_status_departure, "all")?;

        let update_query_status_arrival = format!(
                "UPDATE sky.flights SET status = '{}', lat = {}, lon = {}, angle = {} WHERE airport = '{}' AND direction = '{}' AND departure_time = {} AND arrival_time = {} AND number = {};",
                flight.status.as_str(),
                flight.latitude,
                flight.longitude,
                flight.angle,
                flight.destination.iata_code,
                "ARRIVAL",
                flight.departure_time.and_utc().timestamp(),
                flight.arrival_time.and_utc().timestamp(),
                flight.flight_number
            );
        self.cassandra_client.execute(&update_query_status_arrival, "all")?;
        let update_query_flight_info = format!(
                "UPDATE sky.flight_info SET fuel = {}, speed = {}, height = {} WHERE number = '{}';",
                flight.fuel_level,
                flight.average_speed,
                flight.altitude,
                flight.flight_number
            );
        self.cassandra_client.execute(&update_query_flight_info, "quorum")?;

        Ok(())
    }

    pub fn get_all_new_flights(&mut self, date: NaiveDate, sim_state: &mut SimState) -> Result<Vec<Flight>, ClientError> {
        let from = NaiveDateTime::new(date, NaiveTime::from_hms_opt(0, 0, 0).unwrap());
        let from = from.and_utc().timestamp();
    
        let to = NaiveDateTime::new(date, NaiveTime::from_hms_opt(23, 59, 59).unwrap());
        let to = to.and_utc().timestamp();
    
        let mut new_flights: Vec<Flight> = Vec::new();
    
        // Obtener los estados actuales de vuelos
        let current_flight_states = sim_state.flight_states.read().map_err(|_| ClientError)?;
    
        // Iterate through each airport in the HashMap
        for (airport_code, airport) in &sim_state.airports {
            let query = format!(
                "SELECT number, status, lat, lon, angle, departure_time, arrival_time, direction FROM flights WHERE airport = '{airport_code}' AND direction = 'departure' AND arrival_time > {from} AND arrival_time < {to}"
            );
    
            let result = self.cassandra_client.execute(&query, "all")?;
    
            match result {
                QueryResult::Result(result_::Result::Rows(res)) => {
                    for row in res.rows_content {
                        let flight_number = match row.get("number") {
                            Some(rows::ColumnValue::Ascii(number)) => number.to_string(),
                            _ => continue 
                        };
    
                        // Verificar si el vuelo ya existe, si es asi ver si hay que actualizar su estado.
                        match current_flight_states.get(&flight_number) {
                            Some(existing_state) => {
                                if let Some(status) = row.get("status") {
                                    match status {
                                        rows::ColumnValue::Ascii(status) => {
                                            match FlightStatus::from_str(&status) {
                                                Ok(status) => {
                                                    if *existing_state != status {
                                                        // Si tenemos un estado distinto, lo actualizamos.
                                                        if sim_state.update_flight_in_simulation(&flight_number, status).is_ok(){
                                                            continue;
                                                        }
                                                        else{
                                                            return Err(ClientError)
                                                        }
                                                    }
                                                    else {
                                                        continue;
                                                    }
                                                },
                                                Err(_) => return Err(ClientError),
                                            }
                                        }
                                        _ => {}
                                    }
                                } else {
                                    return Err(ClientError);
                                }
                            },
                            None => {
                                let flight = Client::build_flight_from_row(self, &row, airport, &sim_state.airports)?;
                                new_flights.push(flight);
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    
        Ok(new_flights)
    }

    fn build_flight_from_row(&mut self, row: &BTreeMap<String, ColumnValue>, selected_airport: &Airport, airports: &HashMap<String, Airport>) -> Result<Flight, ClientError> {
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
            match number {
                rows::ColumnValue::Ascii(number) => {
                    flight.flight_number = number.to_string();
                }
                _ => {}
            }
        } else {
            return Err(ClientError);
        }

        if let Some(status) = row.get("status") {
            match status {
                rows::ColumnValue::Ascii(status) => {
                    match FlightStatus::from_str(&status) {
                        Ok(status) => flight.status = status,
                        Err(_) => return Err(ClientError),
                    }
                }
                _ => {}
            }
        } else {
            return Err(ClientError);
        }

        if let Some(departure_time) = row.get("departure_time") {
            match departure_time {
                rows::ColumnValue::Timestamp(departure_time) => {
                    if let Some(datetime) = DateTime::from_timestamp(*departure_time, 0) {
                        flight.departure_time = datetime.naive_utc()
                    } else {
                        return Err(ClientError);
                    }
                }
                _ => {}
            }
        } else {
            return Err(ClientError);
        }

        if let Some(arrival_time) = row.get("arrival_time") {
            match arrival_time {
                rows::ColumnValue::Timestamp(arrival_time) => {
                    if let Some(datetime) = DateTime::from_timestamp(*arrival_time, 0) {
                        flight.arrival_time = datetime.naive_utc()
                    } else {
                        return Err(ClientError);
                    }
                }
                _ => {}
            }
        } else {
            return Err(ClientError);
        }

        if let Some(lat) = row.get("lat") {
            match lat {
                rows::ColumnValue::Double(lat) => {
                    flight.latitude = *lat;
                }
                _ => {}
            }
        } else {
            return Err(ClientError);
        }

        if let Some(lon) = row.get("lon") {
            match lon {
                rows::ColumnValue::Double(lon) => {
                    flight.longitude = *lon;
                }
                _ => {}
            }
        } else {
            return Err(ClientError);
        }

        if let Some(angle) = row.get("angle") {
            match angle {
                rows::ColumnValue::Float(angle) => {
                    flight.angle = *angle;
                }
                _ => {}
            }
        } else {
            return Err(ClientError);
        }
        
        self.get_additional_flight_info(&mut flight, airports)?;

        Ok(flight)
    }

    pub fn get_additional_flight_info(&mut self, flight: &mut Flight, airports: &HashMap<String, Airport>)-> Result<(), ClientError>{

        let number = flight.flight_number;

        let query = format!(
            "SELECT fuel, height, speed, destination FROM sky.flight_info WHERE number = '{number}'"
        );

        let result = self.cassandra_client.execute(&query, "all")?;

        match result {
            QueryResult::Result(result_::Result::Rows(res)) => {
                for row in res.rows_content {

                    if let Some(fuel) = row.get("fuel") {
                        match fuel {
                            rows::ColumnValue::Double(fuel) => {
                                flight.fuel_level = *fuel;
                            }
                            _ => {}
                        }
                    } else {
                        return Err(ClientError);
                    }

                    if let Some(height) = row.get("height") {
                        match height {
                            rows::ColumnValue::Int(height) => {
                                flight.altitude = *height;
                            }
                            _ => {}
                        }
                    } else {
                        return Err(ClientError);
                    }

                    if let Some(speed) = row.get("speed") {
                        match speed {
                            rows::ColumnValue::Int(speed) => {
                                flight.average_speed = *speed;
                            }
                            _ => {}
                        }
                    } else {
                        return Err(ClientError);
                    }

                    if let Some(destination) = row.get("destination") {
                        match destination {
                            rows::ColumnValue::Ascii(destination) => {
                                if let Some(airport) = airports.get(destination){
                                    flight.destination = airport.clone();
                                }
                                else {
                                    return Err(ClientError);
                                }
                            }
                            _ => {}
                        }
                    } else {
                        return Err(ClientError);
                    }
                }
            }
            _ => {}
        }
        Ok()
    }

    /* pub fn get_all_new_flights(&mut self, date: NaiveDate, sim_state: SimState) -> Result<Vec<Flight>, ClientError> {
        let from = NaiveDateTime::new(date, NaiveTime::from_hms_opt(0, 0, 0).unwrap());
        let from = from.and_utc().timestamp();
    
        let to = NaiveDateTime::new(date, NaiveTime::from_hms_opt(23, 59, 59).unwrap());
        let to = to.and_utc().timestamp();
    
        let mut flights: Vec<Flight> = Vec::new();
    
        // Iterate through each airport in the HashMap
        for (airport_code, airport) in sim_state.airports {
            let query = format!(
                "SELECT number, status, lat, lon, angle, departure_time, arrival_time, airport, direction FROM flights WHERE airport = '{airport_code}' AND direction = 'departure' AND arrival_time > {from} AND arrival_time < {to}"
            );
    
            let result = self.cassandra_client.execute(&query, "all")?;
    
            match result {
                QueryResult::Result(result_::Result::Rows(res)) => {

                    
                    for row in res.rows_content {
                        let mut flight = Flight {
                            flight_number: "XXXX".to_string(), 
                            status: FlightStatus::Scheduled, 
                            departure_time: NaiveDateTime::default(),
                            arrival_time: NaiveDateTime::default(),   
                            origin: airport.clone(),      
                            destination: Airport::default(), 
                            latitude: 0.0,                  
                            longitude: 0.0,                 
                            angle: 0.0,                     
                            altitude: 0.0,                  
                            fuel_level: 100.0,              
                            total_distance: 0.0,            
                            distance_traveled: 0.0,         
                            average_speed: 0.0,             
                        };
    
                        if let Some(number) = row.get("number") {
                            match number {
                                rows::ColumnValue::Ascii(number) => {
                                    flight.flight_number = number.to_string();
                                }
                                _ => {}
                            }
                        } else {
                            return Err(ClientError);
                        }
    
                        if let Some(status) = row.get("status") {
                            match status {
                                rows::ColumnValue::Ascii(status) => {
                                    match FlightStatus::from_str(status) {
                                        Ok(status) => flight.status = status,
                                        Err(_) => return Err(ClientError),
                                    }
                                }
                                _ => {}
                            }
                        } else {
                            return Err(ClientError);
                        }
    
                        if let Some(departure_time) = row.get("departure_time") {
                            match departure_time {
                                rows::ColumnValue::Timestamp(departure_time) => {
                                    if let Some(datetime) = DateTime::from_timestamp(*departure_time, 0) {
                                        flight.departure_time = datetime.naive_utc()
                                    } else {
                                        return Err(ClientError);
                                    }
                                }
                                _ => {}
                            }
                        } else {
                            return Err(ClientError);
                        }
    
                        if let Some(arrival_time) = row.get("arrival_time") {
                            match arrival_time {
                                rows::ColumnValue::Timestamp(arrival_time) => {
                                    if let Some(datetime) = DateTime::from_timestamp(*arrival_time, 0) {
                                        flight.arrival_time = datetime.naive_utc()
                                    } else {
                                        return Err(ClientError);
                                    }
                                }
                                _ => {}
                            }
                        } else {
                            return Err(ClientError);
                        }
    
                        if let Some(lat) = row.get("lat") {
                            match lat {
                                rows::ColumnValue::Double(lat) => {
                                    flight.latitude = *lat;
                                }
                                _ => {}
                            }
                        } else {
                            return Err(ClientError);
                        }
    
                        if let Some(lon) = row.get("lon") {
                            match lon {
                                rows::ColumnValue::Double(lon) => {
                                    flight.longitude = *lon;
                                }
                                _ => {}
                            }
                        } else {
                            return Err(ClientError);
                        }
    
                        if let Some(angle) = row.get("angle") {
                            match angle {
                                rows::ColumnValue::Float(angle) => {
                                    flight.angle = *angle;
                                }
                                _ => {}
                            }
                        } else {
                            return Err(ClientError);
                        }
    
                        flights.push(flight);
                    }
                }
                _ => {}
            }
        }
    
        Ok(flights)
    } */

}
