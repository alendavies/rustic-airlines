use std::{net::Ipv4Addr, str::FromStr};

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use driver::{self, CassandraClient, QueryResult};
use native_protocol::messages::result::{result_, rows};
use walkers::Position;

use crate::types::{Airport, Flight, FlightInfo};

#[derive(Debug, Clone)]
pub struct DBError;

const IP: &str = "127.0.0.1";

pub trait Provider {
    fn get_airports_by_country(country: &str) -> Result<Vec<Airport>, DBError>;

    fn get_departure_flights(airport: &str, date: NaiveDate) -> Result<Vec<Flight>, DBError>;

    fn get_arrival_flights(airport: &str, date: NaiveDate) -> Result<Vec<Flight>, DBError>;

    fn get_flight_info(number: &str) -> Result<FlightInfo, DBError>;

    fn get_flights_by_airport(airport: &str) -> Result<Vec<Flight>, DBError>;

    fn get_airports() -> Result<Vec<Airport>, DBError>;

    fn add_flight(flight: Flight) -> Result<(), DBError>;

    fn update_state(flight: Flight, direction: &str) -> Result<(), DBError>;
}

/* #[derive(Debug, Deserialize)]
// TODO: airport types
// TODO: airport countries
struct CsvAirport {
    name: String,
    iata_code: String,
    latitude_deg: f64,
    longitude_deg: f64,
    iso_country: String,
}

pub struct MockProvider;

impl Provider for MockProvider {
    fn get_airports_by_country(country: &str) -> Result<Vec<Airport>, DBError> {
        todo!()
    }

    fn get_departure_flights(airport: &str, date: NaiveDate) -> Result<Vec<Flight>, DBError> {
        let flights = vec![
            Flight::new(Position::from_lat_lon(-30., -60.), 0.),
            Flight::new(Position::from_lat_lon(-45., -65.), 90.),
            Flight::new(Position::from_lat_lon(-40., -70.), 270.),
            Flight::new(Position::from_lat_lon(-35., -65.), 45.),
            Flight::new(Position::from_lat_lon(-25., -55.), 290.),
            Flight::new(Position::from_lat_lon(-30., -75.), 340.),
        ];

        Ok(flights)
    }

    fn get_arrival_flights(airport: &str, date: NaiveDate) -> Result<Vec<Flight>, DBError> {
        let flights = vec![
            Flight::new(Position::from_lat_lon(-30., -60.), 0.),
            Flight::new(Position::from_lat_lon(-45., -65.), 90.),
            Flight::new(Position::from_lat_lon(-40., -70.), 270.),
            Flight::new(Position::from_lat_lon(-35., -65.), 45.),
            Flight::new(Position::from_lat_lon(-25., -55.), 290.),
            Flight::new(Position::from_lat_lon(-30., -75.), 340.),
        ];

        Ok(flights)
    }

    fn get_flight_info(number: &str) -> Result<FlightInfo, DBError> {
        todo!()
    }

    fn get_flights() -> Result<Vec<Flight>, DBError> {
        let flights = vec![
            Flight::new(Position::from_lat_lon(-30., -60.), 0.),
            Flight::new(Position::from_lat_lon(-45., -65.), 90.),
            Flight::new(Position::from_lat_lon(-40., -70.), 270.),
            Flight::new(Position::from_lat_lon(-35., -65.), 45.),
            Flight::new(Position::from_lat_lon(-25., -55.), 290.),
            Flight::new(Position::from_lat_lon(-30., -75.), 340.),
        ];

        Ok(flights)
    }

    fn get_airports() -> Result<Vec<Airport>, DBError> {
        let project_dir = env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
        let path = Path::new(&project_dir).join("airports_ar.csv");
        println!("{:?}", path);
        let file = File::open(path).unwrap();

        let mut rdr = ReaderBuilder::new().has_headers(true).from_reader(file);

        let mut raw_airports = Vec::new();

        for result in rdr.deserialize() {
            let airport: CsvAirport = result.unwrap();
            raw_airports.push(airport);
        }

        let airports: Vec<_> = raw_airports
            .iter()
            .map(|raw| {
                let pos = Position::from_lat_lon(raw.latitude_deg, raw.longitude_deg);

                Airport::new(
                    raw.name.clone(),
                    raw.iata_code.clone(),
                    pos,
                    raw.iso_country.clone(),
                )
            })
            .collect();

        Ok(airports)
    }
} */

pub struct Db;

impl Db {
    pub fn new() -> Self {
        let mut driver = CassandraClient::connect(Ipv4Addr::from_str(IP).unwrap()).unwrap();

        driver.startup().unwrap();

        Self
    }
}

impl Provider for Db {
    /// Get the airports from a country from the database to show them in the graphical interface.
    fn get_airports_by_country(country: &str) -> std::result::Result<Vec<Airport>, DBError> {

        let mut driver = CassandraClient::connect(Ipv4Addr::from_str(IP).unwrap()).unwrap();

        let query = format!("SELECT * FROM sky.airports WHERE country = 'ARG'");

        let result = driver.execute(query.as_str(), "all").map_err(|_| DBError)?;

        let mut airports: Vec<Airport> = Vec::new();
        match result {
            QueryResult::Result(result_::Result::Rows(res)) => {
                for row in res.rows_content {
                    let mut airport = Airport {
                        name: String::new(),
                        iata: String::new(),
                        position: Position::from_lat_lon(0.0, 0.0),
                        country: String::from(country)
                    };

                    if let Some(iata) = row.get("iata") {
                        match iata {
                            rows::ColumnValue::Ascii(iata) => {
                                airport.iata = iata.to_string();
                            }
                            _ => {}
                        }
                    } else {
                        return Err(DBError);
                    }

                    if let Some(name) = row.get("name") {
                        match name {
                            rows::ColumnValue::Ascii(name) => {
                                airport.name = name.to_string();
                            }
                            _ => {}
                        }
                    } else {
                        return Err(DBError);
                    }

                    if let (Some(lat), Some(lon)) = (row.get("lat"), row.get("lon")) {
                        match (lat, lon) {
                            (
                                rows::ColumnValue::Double(latitud),
                                rows::ColumnValue::Double(longitud),
                            ) => {
                                airport.position = Position::from_lat_lon(*latitud, *longitud);
                            }
                            _ => {}
                        }
                    } else {
                        return Err(DBError);
                    }

                    airports.push(airport);
                }
            }
            _ => {}
        }

        Ok(airports)
    }

    fn get_departure_flights(
        airport: &str,
        date: NaiveDate,
    ) -> std::result::Result<Vec<Flight>, DBError> {
        let from = NaiveDateTime::new(date, NaiveTime::from_hms_opt(0, 0, 0).unwrap());
        let from = from.and_utc().timestamp();

        let query = format!(
            "SELECT number, status, lat, lon, angle, departure_time, arrival_time, airport, direction FROM sky.flights WHERE airport = '{airport}' AND direction = 'departure' AND departure_time > {from}"
        );

        let mut driver = CassandraClient::connect(Ipv4Addr::from_str(IP).unwrap()).unwrap();

        let result = driver.execute(query.as_str(), "all").map_err(|_| DBError)?;

        let mut flights: Vec<Flight> = Vec::new();

        match result {
            QueryResult::Result(result_::Result::Rows(res)) => {
                for row in res.rows_content {
                    let mut flight = Flight {
                        number: String::new(),
                        status: String::new(),
                        position: Position::from_lat_lon(0.0, 0.0),
                        heading: 0.0,
                        departure_time: 0,
                        arrival_time: 0,
                        airport: String::new(),
                        direction: String::new(),
                        info: None
                    };

                    if let Some(number) = row.get("number") {
                        match number {
                            rows::ColumnValue::Ascii(number) => {
                                flight.number = number.to_string();
                            }
                            _ => {}
                        }
                    } else {
                        return Err(DBError);
                    }

                    if let Some(status) = row.get("status") {
                        match status {
                            rows::ColumnValue::Ascii(status) => {
                                flight.status = status.to_string();
                            }
                            _ => {}
                        }
                    } else {
                        return Err(DBError);
                    }

                    if let Some(departure_time) = row.get("departure_time") {
                        match departure_time {
                            rows::ColumnValue::Timestamp(departure_time) => {
                                flight.departure_time = *departure_time;
                            }
                            _ => {}
                        }
                    } else {
                        return Err(DBError);
                    }

                    if let Some(arrival_time) = row.get("arrival_time") {
                        match arrival_time {
                            rows::ColumnValue::Timestamp(arrival_time) => {
                                flight.arrival_time = *arrival_time;
                            }
                            _ => {}
                        }
                    } else {
                        return Err(DBError);
                    }

                    if let Some(airport) = row.get("airport") {
                        match airport {
                            rows::ColumnValue::Ascii(airport) => {
                                flight.airport = airport.to_string();
                            }
                            _ => {}
                        }
                    } else {
                        return Err(DBError);
                    }

                    if let Some(direction) = row.get("direction") {
                        match direction {
                            rows::ColumnValue::Ascii(direction) => {
                                flight.direction = direction.to_string();
                            }
                            _ => {}
                        }
                    } else {
                        return Err(DBError);
                    }

                    flights.push(flight);
                }
            }
            _ => {}
        }

        Ok(flights)
    }

    fn get_arrival_flights(
        airport: &str,
        date: NaiveDate,
    ) -> std::result::Result<Vec<Flight>, DBError> {
        let from = NaiveDateTime::new(date, NaiveTime::from_hms_opt(0, 0, 0).unwrap());
        let from = from.and_utc().timestamp();

        let to = NaiveDateTime::new(date, NaiveTime::from_hms_opt(23, 59, 59).unwrap());
        let to = to.and_utc().timestamp();

        let query = format!(
            "SELECT number, status, lat, lon, angle, departure_time, arrival_time, airport, direction FROM sky.flights WHERE airport = '{airport}' AND direction = 'arrival' AND arrival_time > {from} AND arrival_time < {to}"
        );

        let mut driver = CassandraClient::connect(Ipv4Addr::from_str(IP).unwrap()).unwrap();

        let result = driver.execute(query.as_str(), "all").map_err(|_| DBError)?;

        let mut flights: Vec<Flight> = Vec::new();

        match result {
            QueryResult::Result(result_::Result::Rows(res)) => {
                for row in res.rows_content {
                    let mut flight = Flight {
                        number: String::new(),
                        status: String::new(),
                        position: Position::from_lat_lon(0.0, 0.0),
                        heading: 0.0,
                        departure_time: 0,
                        arrival_time: 0,
                        airport: String::new(),
                        direction: String::new(),
                        info: None
                    };

                    if let Some(number) = row.get("number") {
                        match number {
                            rows::ColumnValue::Ascii(number) => {
                                flight.number = number.to_string();
                            }
                            _ => {}
                        }
                    } else {
                        return Err(DBError);
                    }

                    if let Some(status) = row.get("status") {
                        match status {
                            rows::ColumnValue::Ascii(status) => {
                                flight.status = status.to_string();
                            }
                            _ => {}
                        }
                    } else {
                        return Err(DBError);
                    }

                    if let Some(departure_time) = row.get("departure_time") {
                        match departure_time {
                            rows::ColumnValue::Timestamp(departure_time) => {
                                flight.departure_time = *departure_time;
                            }
                            _ => {}
                        }
                    } else {
                        return Err(DBError);
                    }

                    if let Some(arrival_time) = row.get("arrival_time") {
                        match arrival_time {
                            rows::ColumnValue::Timestamp(arrival_time) => {
                                flight.arrival_time = *arrival_time;
                            }
                            _ => {}
                        }
                    } else {
                        return Err(DBError);
                    }

                    if let Some(airport) = row.get("airport") {
                        match airport {
                            rows::ColumnValue::Ascii(airport) => {
                                flight.airport = airport.to_string();
                            }
                            _ => {}
                        }
                    } else {
                        return Err(DBError);
                    }

                    if let Some(direction) = row.get("direction") {
                        match direction {
                            rows::ColumnValue::Ascii(direction) => {
                                flight.direction = direction.to_string();
                            }
                            _ => {}
                        }
                    } else {
                        return Err(DBError);
                    }

                    flights.push(flight);
                }
            }
            _ => {}
        }

        Ok(flights)
    }

    fn get_flight_info(number: &str) -> std::result::Result<FlightInfo, DBError> {

        let query = format!(
            "SELECT number, fuel, height, speed, origin, destination FROM sky.flight_info WHERE number = '{number}'"
        );

        let mut driver = CassandraClient::connect(Ipv4Addr::from_str(IP).unwrap()).unwrap();

        let result = driver.execute(query.as_str(), "quorum").map_err(|_| DBError)?;

        let mut flight_info = FlightInfo {
            number: String::new(),
            fuel: 0.0,
            height: 0,
            speed: 0,
            origin: Default::default(),
            destination: Default::default()
        };

        match result {
            QueryResult::Result(result_::Result::Rows(res)) => {
                for row in res.rows_content {
                    if let Some(number) = row.get("number") {
                        match number {
                            rows::ColumnValue::Ascii(number) => {
                                flight_info.number = number.to_string();
                            }
                            _ => {}
                        }
                    } else {
                        return Err(DBError);
                    }

                    if let Some(fuel) = row.get("fuel") {
                        match fuel {
                            rows::ColumnValue::Double(fuel) => {
                                flight_info.fuel = *fuel;
                            }
                            _ => {}
                        }
                    } else {
                        return Err(DBError);
                    }

                    if let Some(height) = row.get("height") {
                        match height {
                            rows::ColumnValue::Int(height) => {
                                flight_info.height = *height;
                            }
                            _ => {}
                        }
                    } else {
                        return Err(DBError);
                    }

                    if let Some(speed) = row.get("speed") {
                        match speed {
                            rows::ColumnValue::Int(speed) => {
                                flight_info.speed = *speed;
                            }
                            _ => {}
                        }
                    } else {
                        return Err(DBError);
                    }

                    if let Some(origin) = row.get("origin") {
                        match origin {
                            rows::ColumnValue::Ascii(origin) => {
                                flight_info.origin = origin.to_string();
                            }
                            _ => {}
                        }
                    } else {
                        return Err(DBError);
                    }

                    if let Some(destination) = row.get("destination") {
                        match destination {
                            rows::ColumnValue::Ascii(destination) => {
                                flight_info.destination = destination.to_string();
                            }
                            _ => {}
                        }
                    } else {
                        return Err(DBError);
                    }
                }
            }
            _ => {}
        }

        Ok(flight_info)
    }

    fn get_flights_by_airport(airport: &str) -> Result<Vec<Flight>, DBError> {

        /* Nos gustaria trabajar con los vuelos de hoy para mostrar, pero por conveniencia vamos por la fecha 0 ahora.
        let today = Utc::now().date_naive(); 
        let from = NaiveDateTime::new(today, NaiveTime::from_hms_opt(0, 0, 0).unwrap());
        let from = from.and_utc().timestamp();
        */

        let from: i64 = 0;

        let query = format!(
            "SELECT number, status, lat, lon, angle, departure_time, arrival_time, airport, direction FROM sky.flights WHERE airport = '{airport}' AND departure_time > {from}"
        );

        let mut driver = CassandraClient::connect(Ipv4Addr::from_str(IP).unwrap()).unwrap();

        let result = driver.execute(query.as_str(), "quorum").map_err(|_| DBError)?;

        let mut flights: Vec<Flight> = Vec::new();

        match result {
            QueryResult::Result(result_::Result::Rows(res)) => {
                for row in res.rows_content {
                    let mut flight = Flight {
                        number: String::new(),
                        status: String::new(),
                        position: Position::from_lat_lon(0.0, 0.0),
                        heading: 0.0,
                        departure_time: 0,
                        arrival_time: 0,
                        airport: String::new(),
                        direction: String::new(),
                        info: None
                    };

                    if let Some(number) = row.get("number") {
                        match number {
                            rows::ColumnValue::Ascii(number) => {
                                flight.number = number.to_string();
                            }
                            _ => {}
                        }
                    } else {
                        return Err(DBError);
                    }

                    if let Some(status) = row.get("status") {
                        match status {
                            rows::ColumnValue::Ascii(status) => {
                                flight.status = status.to_string();
                            }
                            _ => {}
                        }
                    } else {
                        return Err(DBError);
                    }

                    if let Some(departure_time) = row.get("departure_time") {
                        match departure_time {
                            rows::ColumnValue::Timestamp(departure_time) => {
                                flight.departure_time = *departure_time;
                            }
                            _ => {}
                        }
                    } else {
                        return Err(DBError);
                    }

                    if let Some(arrival_time) = row.get("arrival_time") {
                        match arrival_time {
                            rows::ColumnValue::Timestamp(arrival_time) => {
                                flight.arrival_time = *arrival_time;
                            }
                            _ => {}
                        }
                    } else {
                        return Err(DBError);
                    }

                    if let Some(airport) = row.get("airport") {
                        match airport {
                            rows::ColumnValue::Ascii(airport) => {
                                flight.airport = airport.to_string();
                            }
                            _ => {}
                        }
                    } else {
                        return Err(DBError);
                    }

                    if let Some(direction) = row.get("direction") {
                        match direction {
                            rows::ColumnValue::Ascii(direction) => {
                                flight.direction = direction.to_string();
                            }
                            _ => {}
                        }
                    } else {
                        return Err(DBError);
                    }

                    if let (Some(lat), Some(lon)) = (row.get("lat"), row.get("lon")) {
                        match (lat, lon) {
                            (
                                rows::ColumnValue::Double(latitud),
                                rows::ColumnValue::Double(longitud),
                            ) => {

                                flight.position = Position::from_lat_lon(*latitud,*longitud);
                            }
                            _ => {}
                        }
                    } else {
                        return Err(DBError);
                    }

                    if let Some(angle) = row.get("angle") {
                        match angle {
                            rows::ColumnValue::Float(angle) => {
                                flight.heading = *angle;
                            }
                            _ => {}
                        }
                    } else {
                        return Err(DBError);
                    }

                    flights.push(flight);
                }
            }
            _ => {}
        }

        Ok(flights)
    }

    fn add_flight(flight: Flight) -> Result<(), DBError>{

        let query_check = format!(
            "SELECT number FROM sky.flight_info WHERE number = '{}';",
            flight.number
        );

        let mut driver = CassandraClient::connect(Ipv4Addr::from_str(IP).unwrap()).map_err(|_| DBError)?;

        let result_check = driver.execute(query_check.as_str(), "all").map_err(|_| DBError)?;
        
        match result_check {
            QueryResult::Result(result_::Result::Rows(res)) => {
                if !res.rows_content.is_empty() {
                    return Err(DBError);
                }
            }
            _ => {}
        }

        let flight_info = match flight.info {
            Some(data) => data,
            None => return Err(DBError)
        };

        let insert_departure_query = format!(
            "INSERT INTO sky.flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('{}', '{}', {}, {}, {}, {}, {}, '{}', 'departure');",
            flight.number,
            flight.status.as_str(),
            flight.position.lat(),
            flight.position.lon(),
            flight.heading,
            flight.departure_time,
            flight.arrival_time,
            flight_info.origin
        );
        
        let insert_arrival_query = format!(
            "INSERT INTO sky.flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('{}', '{}', {}, {}, {}, {}, {}, '{}', 'arrival');",
            flight.number,
            flight.status.as_str(),
            flight.position.lat(),
            flight.position.lon(),
            flight.heading,
            flight.departure_time,
            flight.arrival_time,
            flight_info.destination
        );

        // Inserción en la tabla flight_info con la información del vuelo
        let insert_flight_info_query = format!(
            "INSERT INTO sky.flight_info (number, fuel, height, speed, origin, destination) VALUES ('{}', {}, {}, {}, '{}', '{}');",
            flight_info.number,
            flight_info.fuel,
            flight_info.height,
            flight_info.speed,
            flight_info.origin,
            flight_info.destination
        );

        // Ejecución de las consultas en Cassandra
        driver.execute(insert_departure_query.as_str(), "all").map_err(|_| DBError)?;
        driver.execute(insert_arrival_query.as_str(), "all").map_err(|_| DBError)?;
        driver.execute(insert_flight_info_query.as_str(), "all").map_err(|_| DBError)?;

        Ok(())
    }

    fn update_state(flight: Flight, direction: &str) -> Result<(), DBError> {

        let info = Self::get_flight_info(&flight.number)?;

        let (other_airport, other_direction) = match direction {
            "ARRIVAL" => (&info.origin, "DEPARTURE"),
            "DEPARTURE" => (&info.destination, "ARRIVAL"),
            _ => return Err(DBError)
        };
        
        let mut driver = CassandraClient::connect(Ipv4Addr::from_str(IP).unwrap()).unwrap();

        let update_query_status_departure = format!(
            "UPDATE sky.flights SET status = '{}' WHERE airport = '{}' AND direction = '{}' AND departure_time = {} AND arrival_time = {} AND number = {};",
            flight.status.as_str(),
            flight.airport,
            &direction.to_lowercase(),
            flight.departure_time,
            flight.arrival_time,
            flight.number
        );
        driver.execute(&update_query_status_departure, "all").map_err(|_| DBError)?;

        let update_query_status_arrival = format!(
                "UPDATE sky.flights SET status = '{}' WHERE airport = '{}' AND direction = '{}' AND departure_time = {} AND arrival_time = {} AND number = {};",
                flight.status.as_str(),
                other_airport,
                other_direction.to_lowercase(),
                flight.departure_time,
                flight.arrival_time,
                flight.number
            );

        driver.execute(&update_query_status_arrival, "all").map_err(|_| DBError)?;

        Ok(())
    }

    fn get_airports() -> Result<Vec<Airport>, DBError> {
        Self::get_airports_by_country("ARG")
    }
}


/* #[derive(Debug, Clone, PartialEq)]
pub struct Flight {
    pub number: String,
    pub status: String,
    pub departure_time: i64,
    pub arrival_time: i64,
    pub origin_airport: String,
    pub destination_airport: String,
    pub position: Position,
    /// Angle in degrees, where 0° is East, 90° is North, etc.
    pub heading: f32,
    pub fuel: f64,
    pub height: f64,
    pub speed: i32,
    // TODO: add heading vector
}

impl Flight {
    pub fn new(position: Position, heading: f32) -> Self {
        Self {
            arrival_time: 1731486006,
            departure_time: 1731473166,
            destination_airport: String::from("EZE"),
            fuel: Default::default(),
            height: 9550.0,
            number: String::from("AR1234"),
            origin_airport: String::from("AEZ"),
            position,
            heading,
            status: String::from("Departing"),
            speed: 880,
        }
    }
} */


