use std::{net::Ipv4Addr, str::FromStr};

use driver::CassandraClient;

mod db;
mod map;
mod plugins;
mod windows;
// use map::MyApp;

/* fn main() -> Result<(), eframe::Error> {
    eframe::run_native(
        "Flight Tracker",
        Default::default(),
        Box::new(|cc| Ok(Box::new(MyApp::new(cc.egui_ctx.clone())))),
    )
}
 */

fn main() {
    let server_ip = "127.0.0.4";
    let ip = Ipv4Addr::from_str(&server_ip).unwrap();

    let mut client = CassandraClient::connect(ip).unwrap();
    client.startup().unwrap();

    let queries = vec![
        "CREATE KEYSPACE airports WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}",
        "USE airports",
        "CREATE TABLE airports (
            iata TEXT PRIMARY KEY,
            lat DOUBLE,
            lon DOUBLE
            PRIMARY KEY (iata)
            )",
        "INSERT INTO airports (iata, lat, lon) VALUES ('MAD', 40.5, -3.5)",
        "INSERT INTO airports (iata, lat, lon) VALUES ('BCN', 41.3, 2.1)",
        "INSERT INTO airports (iata, lat, lon) VALUES ('LAX', 33.9, -118.4)",
        "INSERT INTO airports (iata, lat, lon) VALUES ('JFK', 40.6, -73.8)"
        ];

    let mut contador = 0;
    let len = queries.len();
    for query in queries {
        match client.execute(&query) {
            Ok(query_result) => {
                contador += 1;
                println!(
                    "Consulta ejecutada exitosamente: {} y el resultado fue {:?}",
                    query, query_result
                );
                println!("exitosas {:?}/{:?}", contador, len)
            }
            Err(e) => eprintln!("Error al ejecutar la consulta: {}\nError: {:?}", query, e),
        }
    }

    let airports = db::get_airports(&mut client);

    println!("Airports: {:?}", airports);
}
