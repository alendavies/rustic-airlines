use std::{net::Ipv4Addr, str::FromStr};

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use driver::{self, CassandraClient, QueryResult};
use native_protocol::messages::result::{result_, rows};
use walkers::Position;

use crate::types::{Airport, Flight, FlightInfo, FlightStatus};

#[derive(Debug, Clone)]
pub struct DBError;

const IP: &str = "127.0.0.1";

pub trait Provider {
    fn get_airports_by_country(&mut self, country: &str) -> Result<Vec<Airport>, DBError>;

    fn get_departure_flights(
        &mut self,
        airport: &str,
        date: NaiveDate,
    ) -> Result<Vec<Flight>, DBError>;

    fn get_arrival_flights(
        &mut self,
        airport: &str,
        date: NaiveDate,
    ) -> Result<Vec<Flight>, DBError>;

    fn get_flight_info(&mut self, number: &str) -> Result<FlightInfo, DBError>;

    fn get_flights_by_airport(&mut self, airport: &str) -> Result<Vec<Flight>, DBError>;

    fn get_airports(&mut self) -> Result<Vec<Airport>, DBError>;

    fn add_flight(&mut self, flight: Flight) -> Result<(), DBError>;

    fn update_state(&mut self, flight: Flight, direction: &str) -> Result<(), DBError>;
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

pub struct Db {
    driver: CassandraClient,
}

impl Default for Db {
    fn default() -> Self {
        Self::new()
    }
}

impl Db {
    pub fn new() -> Self {
        let mut driver = CassandraClient::connect(Ipv4Addr::from_str(IP).unwrap()).unwrap();
        driver.startup().unwrap();
        Self { driver: driver }
    }

    fn execute_query(&mut self, query: &str, consistency: &str) -> Result<QueryResult, DBError> {
        self.driver.execute(query, consistency).map_err(|_| DBError)
    }
}

impl Provider for Db {
    /// Get the airports from a country from the database to show them in the graphical interface.
    fn get_airports_by_country(
        &mut self,
        country: &str,
    ) -> std::result::Result<Vec<Airport>, DBError> {
        let query = "SELECT * FROM sky.airports WHERE country = 'ARG'".to_string();

        let result = self
            .execute_query(query.as_str(), "all")
            .map_err(|_| DBError)?;

        let mut airports: Vec<Airport> = Vec::new();
        if let QueryResult::Result(result_::Result::Rows(res)) = result {
            for row in res.rows_content {
                let mut airport = Airport {
                    name: String::new(),
                    iata: String::new(),
                    position: Position::from_lat_lon(0.0, 0.0),
                    country: String::from(country),
                };

                if let Some(iata) = row.get("iata") {
                    if let rows::ColumnValue::Ascii(iata) = iata {
                        airport.iata = iata.to_string();
                    }
                } else {
                    return Err(DBError);
                }

                if let Some(name) = row.get("name") {
                    if let rows::ColumnValue::Ascii(name) = name {
                        airport.name = name.to_string();
                    }
                } else {
                    return Err(DBError);
                }

                if let (Some(lat), Some(lon)) = (row.get("lat"), row.get("lon")) {
                    if let (
                        rows::ColumnValue::Double(latitud),
                        rows::ColumnValue::Double(longitud),
                    ) = (lat, lon)
                    {
                        airport.position = Position::from_lat_lon(*latitud, *longitud);
                    }
                } else {
                    return Err(DBError);
                }

                airports.push(airport);
            }
        }

        Ok(airports)
    }

    fn get_departure_flights(
        &mut self,
        airport: &str,
        date: NaiveDate,
    ) -> std::result::Result<Vec<Flight>, DBError> {
        let from = NaiveDateTime::new(date, NaiveTime::from_hms_opt(0, 0, 0).unwrap());
        let from = from.and_utc().timestamp();

        let query = format!(
            "SELECT number, status, lat, lon, angle, departure_time, arrival_time, airport, direction FROM sky.flights WHERE airport = '{airport}' AND direction = 'departure' AND departure_time > {from}"
        );

        let result = self
            .execute_query(query.as_str(), "all")
            .map_err(|_| DBError)?;

        let mut flights: Vec<Flight> = Vec::new();

        if let QueryResult::Result(result_::Result::Rows(res)) = result {
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
                    info: None,
                };

                if let Some(number) = row.get("number") {
                    if let rows::ColumnValue::Ascii(number) = number {
                        flight.number = number.to_string();
                    }
                } else {
                    return Err(DBError);
                }

                if let Some(status) = row.get("status") {
                    if let rows::ColumnValue::Ascii(status) = status {
                        flight.status = status.to_string();
                    }
                } else {
                    return Err(DBError);
                }

                if let Some(departure_time) = row.get("departure_time") {
                    if let rows::ColumnValue::Timestamp(departure_time) = departure_time {
                        flight.departure_time = *departure_time;
                    }
                } else {
                    return Err(DBError);
                }

                if let Some(arrival_time) = row.get("arrival_time") {
                    if let rows::ColumnValue::Timestamp(arrival_time) = arrival_time {
                        flight.arrival_time = *arrival_time;
                    }
                } else {
                    return Err(DBError);
                }

                if let Some(airport) = row.get("airport") {
                    if let rows::ColumnValue::Ascii(airport) = airport {
                        flight.airport = airport.to_string();
                    }
                } else {
                    return Err(DBError);
                }

                if let Some(direction) = row.get("direction") {
                    if let rows::ColumnValue::Ascii(direction) = direction {
                        flight.direction = direction.to_string();
                    }
                } else {
                    return Err(DBError);
                }

                flights.push(flight);
            }
        }

        Ok(flights)
    }

    fn get_arrival_flights(
        &mut self,
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

        let result = self
            .execute_query(query.as_str(), "all")
            .map_err(|_| DBError)?;

        let mut flights: Vec<Flight> = Vec::new();

        if let QueryResult::Result(result_::Result::Rows(res)) = result {
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
                    info: None,
                };

                if let Some(number) = row.get("number") {
                    if let rows::ColumnValue::Ascii(number) = number {
                        flight.number = number.to_string();
                    }
                } else {
                    return Err(DBError);
                }

                if let Some(status) = row.get("status") {
                    if let rows::ColumnValue::Ascii(status) = status {
                        flight.status = status.to_string();
                    }
                } else {
                    return Err(DBError);
                }

                if let Some(departure_time) = row.get("departure_time") {
                    if let rows::ColumnValue::Timestamp(departure_time) = departure_time {
                        flight.departure_time = *departure_time;
                    }
                } else {
                    return Err(DBError);
                }

                if let Some(arrival_time) = row.get("arrival_time") {
                    if let rows::ColumnValue::Timestamp(arrival_time) = arrival_time {
                        flight.arrival_time = *arrival_time;
                    }
                } else {
                    return Err(DBError);
                }

                if let Some(airport) = row.get("airport") {
                    if let rows::ColumnValue::Ascii(airport) = airport {
                        flight.airport = airport.to_string();
                    }
                } else {
                    return Err(DBError);
                }

                if let Some(direction) = row.get("direction") {
                    if let rows::ColumnValue::Ascii(direction) = direction {
                        flight.direction = direction.to_string();
                    }
                } else {
                    return Err(DBError);
                }

                flights.push(flight);
            }
        }

        Ok(flights)
    }

    fn get_flight_info(&mut self, number: &str) -> std::result::Result<FlightInfo, DBError> {
        let query = format!(
            "SELECT number, fuel, height, speed, origin, destination FROM sky.flight_info WHERE number = '{number}'"
        );

        let result = self
            .execute_query(query.as_str(), "quorum")
            .map_err(|_| DBError)?;

        let mut flight_info = FlightInfo {
            number: String::new(),
            fuel: 0.0,
            height: 0,
            speed: 0,
            origin: Default::default(),
            destination: Default::default(),
        };

        if let QueryResult::Result(result_::Result::Rows(res)) = result {
            for row in res.rows_content {
                if let Some(number) = row.get("number") {
                    if let rows::ColumnValue::Ascii(number) = number {
                        flight_info.number = number.to_string();
                    }
                } else {
                    return Err(DBError);
                }

                if let Some(fuel) = row.get("fuel") {
                    if let rows::ColumnValue::Double(fuel) = fuel {
                        flight_info.fuel = *fuel;
                    }
                } else {
                    return Err(DBError);
                }

                if let Some(height) = row.get("height") {
                    if let rows::ColumnValue::Int(height) = height {
                        flight_info.height = *height;
                    }
                } else {
                    return Err(DBError);
                }

                if let Some(speed) = row.get("speed") {
                    if let rows::ColumnValue::Int(speed) = speed {
                        flight_info.speed = *speed;
                    }
                } else {
                    return Err(DBError);
                }

                if let Some(origin) = row.get("origin") {
                    if let rows::ColumnValue::Ascii(origin) = origin {
                        flight_info.origin = origin.to_string();
                    }
                } else {
                    return Err(DBError);
                }

                if let Some(destination) = row.get("destination") {
                    if let rows::ColumnValue::Ascii(destination) = destination {
                        flight_info.destination = destination.to_string();
                    }
                } else {
                    return Err(DBError);
                }
            }
        }

        Ok(flight_info)
    }

    fn get_flights_by_airport(&mut self, airport: &str) -> Result<Vec<Flight>, DBError> {
        /* Nos gustaria trabajar con los vuelos de hoy para mostrar, pero por conveniencia vamos por la fecha 0 ahora.
        let today = Utc::now().date_naive();
        let from = NaiveDateTime::new(today, NaiveTime::from_hms_opt(0, 0, 0).unwrap());
        let from = from.and_utc().timestamp();
        */

        let from: i64 = 0;

        let query = format!(
            "SELECT number, status, lat, lon, angle, departure_time, arrival_time, airport, direction FROM sky.flights WHERE airport = '{airport}' AND departure_time > {from}"
        );

        let result = self
            .execute_query(query.as_str(), "quorum")
            .map_err(|_| DBError)?;

        let mut flights: Vec<Flight> = Vec::new();

        if let QueryResult::Result(result_::Result::Rows(res)) = result {
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
                    info: None,
                };

                if let Some(number) = row.get("number") {
                    if let rows::ColumnValue::Ascii(number) = number {
                        flight.number = number.to_string();
                    }
                } else {
                    return Err(DBError);
                }

                if let Some(status) = row.get("status") {
                    if let rows::ColumnValue::Ascii(status) = status {
                        flight.status = status.to_string();
                    }
                } else {
                    return Err(DBError);
                }

                if let Some(departure_time) = row.get("departure_time") {
                    if let rows::ColumnValue::Timestamp(departure_time) = departure_time {
                        flight.departure_time = *departure_time;
                    }
                } else {
                    return Err(DBError);
                }

                if let Some(arrival_time) = row.get("arrival_time") {
                    if let rows::ColumnValue::Timestamp(arrival_time) = arrival_time {
                        flight.arrival_time = *arrival_time;
                    }
                } else {
                    return Err(DBError);
                }

                if let Some(airport) = row.get("airport") {
                    if let rows::ColumnValue::Ascii(airport) = airport {
                        flight.airport = airport.to_string();
                    }
                } else {
                    return Err(DBError);
                }

                if let Some(direction) = row.get("direction") {
                    if let rows::ColumnValue::Ascii(direction) = direction {
                        flight.direction = direction.to_string();
                    }
                } else {
                    return Err(DBError);
                }

                if let (Some(lat), Some(lon)) = (row.get("lat"), row.get("lon")) {
                    if let (
                        rows::ColumnValue::Double(latitud),
                        rows::ColumnValue::Double(longitud),
                    ) = (lat, lon)
                    {
                        flight.position = Position::from_lat_lon(*latitud, *longitud);
                    }
                } else {
                    return Err(DBError);
                }

                if let Some(angle) = row.get("angle") {
                    if let rows::ColumnValue::Float(angle) = angle {
                        flight.heading = *angle;
                    }
                } else {
                    return Err(DBError);
                }

                if flight.status == FlightStatus::OnTime.as_str()
                    || flight.status == FlightStatus::Delayed.as_str()
                {
                    flights.push(flight);
                }
            }
        }

        Ok(flights)
    }

    fn add_flight(&mut self, flight: Flight) -> Result<(), DBError> {
        let query_check = format!(
            "SELECT number FROM sky.flight_info WHERE number = '{}';",
            flight.number
        );

        let result_check = self
            .execute_query(query_check.as_str(), "all")
            .map_err(|_| DBError)?;

        if let QueryResult::Result(result_::Result::Rows(res)) = result_check {
            if !res.rows_content.is_empty() {
                return Err(DBError);
            }
        }

        let flight_info = match flight.info {
            Some(data) => data,
            None => return Err(DBError),
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
        self.execute_query(insert_departure_query.as_str(), "all")
            .map_err(|_| DBError)?;
        self.execute_query(insert_arrival_query.as_str(), "all")
            .map_err(|_| DBError)?;
        self.execute_query(insert_flight_info_query.as_str(), "all")
            .map_err(|_| DBError)?;

        Ok(())
    }

    fn update_state(&mut self, flight: Flight, direction: &str) -> Result<(), DBError> {
        let info = self.get_flight_info(&flight.number)?;

        let (other_airport, other_direction) = match direction {
            "ARRIVAL" => (&info.origin, "DEPARTURE"),
            "DEPARTURE" => (&info.destination, "ARRIVAL"),
            _ => return Err(DBError),
        };

        let update_query_status_departure = format!(
            "UPDATE sky.flights SET status = '{}' WHERE airport = '{}' AND direction = '{}' AND departure_time = {} AND arrival_time = {} AND number = {};",
            flight.status.as_str(),
            flight.airport,
            &direction.to_lowercase(),
            flight.departure_time,
            flight.arrival_time,
            flight.number
        );

        self.execute_query(&update_query_status_departure, "all")
            .map_err(|_| DBError)?;

        let update_query_status_arrival = format!(
                "UPDATE sky.flights SET status = '{}' WHERE airport = '{}' AND direction = '{}' AND departure_time = {} AND arrival_time = {} AND number = {};",
                flight.status.as_str(),
                other_airport,
                other_direction.to_lowercase(),
                flight.departure_time,
                flight.arrival_time,
                flight.number
            );

        self.execute_query(&update_query_status_arrival, "all")
            .map_err(|_| DBError)?;

        Ok(())
    }

    fn get_airports(&mut self) -> Result<Vec<Airport>, DBError> {
        self.get_airports_by_country("ARG")
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
