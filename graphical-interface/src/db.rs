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
pub(crate) fn get_airports(driver: &mut CassandraClient) -> Vec<Airport> {
    let query = "SELECT iata, lat, lon FROM airports WHERE iata = 'JFK'";

    /* if let QueryResult::Result(Result::Rows(res)) = driver.execute(query).unwrap() {
        // obtener airports de las rows
        let airports: Vec<_> = res
            .rows_content
            .iter()
            .map(|record| Airport {
                iata: record.get("iata").unwrap(),
                position: Position::from_lat_lon(
                    record.get("lat").unwrap(),
                    record.get("lon").unwrap(),
                ),
            })
            .collect();
    } */

    let result = driver.execute(query).unwrap();

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

                dbg!(iata);

                let lat = row.get("lat").unwrap();
                let lon = row.get("lon").unwrap();

                match (lat, lon) {
                    (rows::ColumnValue::Double(latitud), rows::ColumnValue::Double(longitud)) => {
                        airport.position = Position::from_lat_lon(*latitud, *longitud);
                    }
                    _ => {}
                }

                dbg!(lat, lon);
                dbg!(&airport);

                airports.push(airport);
            }
        }
        _ => {}
    }

    airports

    /*  vec![
        Airport {
            iata: "AEP".to_string(),
            position: Position::from_lat_lon(-34.557571, -58.418577),
        },
        Airport {
            iata: "PMY".to_string(),
            position: Position::from_lat_lon(-42.759000, -65.103000),
        },
    ] */
}
