use std::{thread, time::Duration};

use driver::{self, CassandraClient, QueryResult};
use native_protocol::messages::result::{result::Result, rows};
use walkers::Position;

#[derive(Debug, Clone)]
pub struct Airport {
    // name: String,
    pub iata: String,
    pub position: Position,
}

/// Get the airports to display on the map.
pub fn get_airports(driver: &mut CassandraClient, country: &str) -> Vec<Airport> {
    let query = format!("SELECT iata, name, lat, lon FROM airports WHERE country = {country}");

    let result = driver.execute(query.as_str()).unwrap();

    let mut airports: Vec<Airport> = Vec::new();
    match result {
        QueryResult::Result(Result::Rows(res)) => {
            for row in res.rows_content {
                let mut airport = Airport {
                    iata: String::new(),
                    position: Position::from_lat_lon(0.0, 0.0),
                };

                let iata = row.get("iata").unwrap();
                match iata {
                    rows::ColumnValue::Ascii(iata) => {
                        airport.iata = iata.to_string();
                    }
                    _ => {}
                }

                let lat = row.get("lat").unwrap();
                let lon = row.get("lon").unwrap();

                match (lat, lon) {
                    (rows::ColumnValue::Double(latitud), rows::ColumnValue::Double(longitud)) => {
                        airport.position = Position::from_lat_lon(*latitud, *longitud);
                    }
                    _ => {}
                }

                airports.push(airport);
            }
        }
        _ => {}
    }

    airports
}
