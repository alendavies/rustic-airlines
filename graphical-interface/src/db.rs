use std::{thread, time::Duration};

use driver::{self, CassandraClient, QueryResult};
use walkers::Position;

#[derive(Debug, Clone)]
pub struct Airport {
    // name: String,
    pub iata: String,
    pub position: Position,
}

/// Get the airports to display on the map.
pub(crate) fn get_airports(/*driver: &CassandraClient*/) -> Vec<Airport> {
    let query = "query to select all airports";

    dbg!("call");
    thread::sleep(Duration::from_millis(200));

    vec![
        Airport {
            iata: "AEP".to_string(),
            position: Position::from_lat_lon(-34.557571, -58.418577),
        },
        Airport {
            iata: "PMY".to_string(),
            position: Position::from_lat_lon(-42.759000, -65.103000),
        },
    ]
}
