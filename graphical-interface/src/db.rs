use std::{fs::File, net::Ipv4Addr, path::Path, str::FromStr};

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use csv::ReaderBuilder;
use driver::{self, CassandraClient, QueryResult};
use native_protocol::messages::result::{result, rows};
use serde::Deserialize;
use walkers::Position;

#[derive(Debug, Clone)]
pub struct DBError;

const IP: &str = "127.0.0.1";

pub trait Provider {
    fn get_airports(country: &str) -> Result<Vec<Airport>, DBError>;

    fn get_departure_flights(airport: &str, date: NaiveDate) -> Result<Vec<Flight>, DBError>;

    fn get_arrival_flights(airport: &str, date: NaiveDate) -> Result<Vec<Flight>, DBError>;

    fn get_flight_info(number: &str) -> Result<FlightInfo, DBError>;
}

#[derive(Debug, Deserialize)]
// TODO: airport types
// TODO: airport countries
struct CsvAirport {
    name: String,
    iata_code: String,
    latitude_deg: f64,
    longitude_deg: f64,
}

pub struct MockProvider;

impl Provider for MockProvider {
    fn get_airports(country: &str) -> Result<Vec<Airport>, DBError> {
        dbg!("aca");
        let path = Path::new("graphical-interface/airports.csv");
        let file = File::open(path).unwrap();

        let mut rdr = ReaderBuilder::new().has_headers(true).from_reader(file);

        let mut raw_airports = Vec::new();

        for result in rdr.deserialize().take(100) {
            let airport: CsvAirport = result.unwrap();
            raw_airports.push(airport);
        }

        let airports: Vec<_> = raw_airports
            .iter()
            .map(|raw| {
                let pos = Position::from_lat_lon(raw.latitude_deg, raw.longitude_deg);

                Airport::new(raw.name.clone(), raw.iata_code.clone(), pos)
            })
            .collect();

        Ok(airports)
    }

    fn get_departure_flights(airport: &str, date: NaiveDate) -> Result<Vec<Flight>, DBError> {
        todo!()
    }

    fn get_arrival_flights(airport: &str, date: NaiveDate) -> Result<Vec<Flight>, DBError> {
        todo!()
    }

    fn get_flight_info(number: &str) -> Result<FlightInfo, DBError> {
        todo!()
    }
}

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
    fn get_airports(country: &str) -> std::result::Result<Vec<Airport>, DBError> {
        let query = format!("SELECT iata, name, lat, lon FROM airports WHERE country = {country}");

        let mut driver = CassandraClient::connect(Ipv4Addr::from_str(IP).unwrap()).unwrap();

        let result = driver.execute(query.as_str(), "all").map_err(|_| DBError)?;

        let mut airports: Vec<Airport> = Vec::new();
        match result {
            QueryResult::Result(result::Result::Rows(res)) => {
                for row in res.rows_content {
                    let mut airport = Airport {
                        name: String::new(),
                        iata: String::new(),
                        position: Position::from_lat_lon(0.0, 0.0),
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

        let to = NaiveDateTime::new(date, NaiveTime::from_hms_opt(23, 59, 59).unwrap());
        let to = to.and_utc().timestamp();

        let query = format!(
            "SELECT number, status, departure_time, arrival_time, airport, direction FROM flights WHERE airport = '{airport}' AND direction = 'departure' AND departure_time > {from}"
        );

        let mut driver = CassandraClient::connect(Ipv4Addr::from_str(IP).unwrap()).unwrap();

        let result = driver.execute(query.as_str(), "all").map_err(|_| DBError)?;

        let mut flights: Vec<Flight> = Vec::new();

        match result {
            QueryResult::Result(result::Result::Rows(res)) => {
                for row in res.rows_content {
                    let mut flight = Flight {
                        number: String::new(),
                        status: String::new(),
                        departure_time: 0,
                        arrival_time: 0,
                        airport: String::new(),
                        direction: String::new(),
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
            "SELECT number, status, departure_time, arrival_time, airport, direction FROM flights WHERE airport = {airport} AND direction = 'arrival' AND arrival_time > {from} AND arrival_time < {to}"
        );

        let mut driver = CassandraClient::connect(Ipv4Addr::from_str(IP).unwrap()).unwrap();

        let result = driver.execute(query.as_str(), "all").map_err(|_| DBError)?;

        let mut flights: Vec<Flight> = Vec::new();

        match result {
            QueryResult::Result(result::Result::Rows(res)) => {
                for row in res.rows_content {
                    let mut flight = Flight {
                        number: String::new(),
                        status: String::new(),
                        departure_time: 0,
                        arrival_time: 0,
                        airport: String::new(),
                        direction: String::new(),
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
        "SELECT number, lat, lon, fuel, height, speed FROM flight_info WHERE number = '{number}'"
    );

        let mut driver = CassandraClient::connect(Ipv4Addr::from_str(IP).unwrap()).unwrap();

        let result = driver.execute(query.as_str(), "all").map_err(|_| DBError)?;

        let mut flight_info = FlightInfo {
            number: String::new(),
            lat: 0.0,
            lon: 0.0,
            fuel: 0.0,
            height: 0,
            speed: 0,
        };

        match result {
            QueryResult::Result(result::Result::Rows(res)) => {
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
                    if let (Some(lat), Some(lon)) = (row.get("lat"), row.get("lon")) {
                        match (lat, lon) {
                            (
                                rows::ColumnValue::Double(latitud),
                                rows::ColumnValue::Double(longitud),
                            ) => {
                                flight_info.lat = *latitud;
                                flight_info.lon = *longitud;
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
                }
            }
            _ => {}
        }

        Ok(flight_info)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Airport {
    pub name: String,
    pub iata: String,
    pub position: Position,
}

impl Airport {
    pub fn new(name: String, iata: String, position: Position) -> Self {
        Self {
            name,
            iata,
            position,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Flight {
    pub number: String,
    pub status: String,
    pub departure_time: i64,
    pub arrival_time: i64,
    pub airport: String,
    pub direction: String,
}

#[derive(Debug, Clone)]
pub struct FlightInfo {
    pub number: String,
    pub lat: f64,
    pub lon: f64,
    pub fuel: f64,
    pub height: i32,
    pub speed: i32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mock_reader() {
        let airports = MockProvider::get_airports("").unwrap();
    }
}
