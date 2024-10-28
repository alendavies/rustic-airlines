use driver::CassandraClient;
use graphical_interface::db;
use std::{net::Ipv4Addr, str::FromStr};

fn main() {
    let server_ip = "127.0.0.4";
    let ip = Ipv4Addr::from_str(&server_ip).unwrap();

    let mut client = CassandraClient::connect(ip).unwrap();
    client.startup().unwrap();

    let queries = vec![
        "CREATE KEYSPACE sky WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
        "USE sky",
        /* "CREATE TABLE airports (
            iata TEXT,
            country TEXT,
            name TEXT,
            lat DOUBLE,
            lon DOUBLE,
            PRIMARY KEY (country, iata)
            )",
        "INSERT INTO airports (iata, country, name, lat, lon) VALUES ('JFK', 'USA', 'John F. Kennedy', 40.6413, -73.7781)",
        "INSERT INTO airports (iata, country, name, lat, lon) VALUES ('EZE', 'ARG', 'Ministro Pistarini', -34.8222, -58.5358)",
        "INSERT INTO airports (iata, country, name, lat, lon) VALUES ('MVD', 'URY', 'Carrasco', -34.8381, -56.0308)",
        "INSERT INTO airports (iata, country, name, lat, lon) VALUES ('MIA', 'USA', 'Miami', 25.7959, -80.2870)",
        "INSERT INTO airports (iata, country, name, lat, lon) VALUES ('LAX', 'USA', 'Los Angeles', 33.9416, -118.4085)", */
        "CREATE TABLE flights (
            number TEXT,
            status TEXT,
            departure_time TIMESTAMP,
            arrival_time TIMESTAMP,
            airport TEXT,
            direction TEXT,
            PRIMARY KEY (airport, direction, departure_time, arrival_time)
            )",
        "INSERT INTO flights (number, status, departure_time, arrival_time, airport, direction) VALUES ('AA123', 'on time', '1730073688', '1730131200', 'LAX', 'arrival')",
        "INSERT INTO flights (number, status, departure_time, arrival_time, airport, direction) VALUES ('AA124', 'on time', '1730131200', '1730131200', 'JFK', 'departure')",
        "INSERT INTO flights (number, status, departure_time, arrival_time, airport, direction) VALUES ('AA125', 'on time', '1730073698', '46741883131', 'LAX', 'arrival')",
        "INSERT INTO flights (number, status, departure_time, arrival_time, airport, direction) VALUES ('AA126', 'on time', '1730073698', '46741889943', 'JFK', 'departure')",
        "INSERT INTO flights (number, status, departure_time, arrival_time, airport, direction) VALUES ('AA127', 'on time', '1730073698', '46741889943', 'LAX', 'arrival')",
        "INSERT INTO flights (number, status, departure_time, arrival_time, airport, direction) VALUES ('AA128', 'on time', '1730073698', '46741889943', 'JFK', 'departure')",

        /* "CREATE TABLE flight_info (
            number TEXT,
            lat DOUBLE,
            lon DOUBLE,
            fuel DOUBLE,
            height INT,
            speed INT,
            PRIMARY KEY (number, lat)
            )",
        "INSERT INTO flight_info (number, lat, lon, fuel, height, speed) VALUES ('AA123', 40.6413, -73.7781, 100.0, 10000, 500)",
        "INSERT INTO flight_info (number, lat, lon, fuel, height, speed) VALUES ('AA124', -34.8222, -58.5358, 100.0, 10000, 500)",
        "INSERT INTO flight_info (number, lat, lon, fuel, height, speed) VALUES ('AA125', -34.8381, -56.0308, 100.0, 10000, 500)",
        "INSERT INTO flight_info (number, lat, lon, fuel, height, speed) VALUES ('AA126', 25.7959, -80.2870, 100.0, 10000, 500)",
        "INSERT INTO flight_info (number, lat, lon, fuel, height, speed) VALUES ('AA127', 33.9416, -118.4085, 100.0, 10000, 500)", */
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

    /* let airports = db::get_airports(&mut client, "USA").unwrap();
    println!("Airports: {:?}", airports); */

    let departure_flights =
        db::get_departure_flights(&mut client, "JFK", chrono::offset::Utc::now().date_naive())
            .unwrap();
    println!("Departure flights: {:?}", departure_flights);

    /* let arrival_flights =
        db::get_arrival_flights(&mut client, "LAX", chrono::offset::Utc::now().date_naive())
            .unwrap();
    println!("Arrival flights: {:?}", arrival_flights); */

    /*   let flight_info = db::get_flight_info(&mut client, "AA123").unwrap();
    println!("Flight info: {:?}", flight_info); */
}
