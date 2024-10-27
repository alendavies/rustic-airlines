use driver::CassandraClient;
use graphical_interface::db;
use std::{net::Ipv4Addr, str::FromStr};

fn main() {
    let server_ip = "127.0.0.4";
    let ip = Ipv4Addr::from_str(&server_ip).unwrap();

    let mut client = CassandraClient::connect(ip).unwrap();
    client.startup().unwrap();

    let queries = vec![
        "CREATE KEYSPACE sky WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}",
        "USE sky",
        "CREATE TABLE airports (
            iata TEXT,
            country TEXT,
            name TEXT,
            lat DOUBLE,
            lon DOUBLE,
            PRIMARY KEY (country, iata)
            )",

        "INSERT INTO airports (iata, country, name, lat, lon) VALUES ('JFK', 'USA', 'John F. Kennedy International Airport', 40.6413, -73.7781)",
        "INSERT INTO airports (iata, country, name, lat, lon) VALUES ('EZE', 'ARG', 'Ministro Pistarini International Airport', -34.8222, -58.5358)",
        "INSERT INTO airports (iata, country, name, lat, lon) VALUES ('MVD', 'URY', 'Carrasco International Airport', -34.8381, -56.0308)",
        "INSERT INTO airports (iata, country, name, lat, lon) VALUES ('MIA', 'USA', 'Miami International Airport', 25.7959, -80.2870)",
        "INSERT INTO airports (iata, country, name, lat, lon) VALUES ('LAX', 'USA', 'Los Angeles International Airport', 33.9416, -118.4085)",

        "CREATE TABLE flights (
            number TEXT,
            status TEXT,
            departure_time TIMESTAMP,
            arrival_time TIMESTAMP,
            airport TEXT,
            direction TEXT,
            PRIMARY KEY (direction, departure_time)
            )",

        "INSERT INTO flights (number, status, departure_time, arrival_time, airport, direction) VALUES ('AA123', 'on time', '2021-09-01 10:00:00', '2021-09-01 12:00:00', 'JFK', 'arrival')",
        "INSERT INTO flights (number, status, departure_time, arrival_time, airport, direction) VALUES ('AA124', 'on time', '2021-09-01 10:00:00', '2021-09-01 12:00:00', 'EZE', 'departure')",
        "INSERT INTO flights (number, status, departure_time, arrival_time, airport, direction) VALUES ('AA125', 'on time', '2021-09-01 10:00:00', '2021-09-01 12:00:00', 'MVD', 'arrival')",
        "INSERT INTO flights (number, status, departure_time, arrival_time, airport, direction) VALUES ('AA126', 'on time', '2021-09-01 10:00:00', '2021-09-01 12:00:00', 'MIA', 'departure')",
        "INSERT INTO flights (number, status, departure_time, arrival_time, airport, direction) VALUES ('AA127', 'on time', '2021-09-01 10:00:00', '2021-09-01 12:00:00', 'LAX', 'arrival')",
        "INSERT INTO flights (number, status, departure_time, arrival_time, airport, direction) VALUES ('AA128', 'on time', '2021-09-01 10:00:00', '2021-09-01 12:00:00', 'JFK', 'departure')",

        "CREATE TABLE flight_info (
            number TEXT,
            lat DOUBLE,
            lon DOUBLE,
            fuel DOUBLE,
            height INT,
            speed INT,
            PRIMARY KEY (direction, departure_time)
            )",

        "INSERT INTO flight_info (number, lat, lon, fuel, height, speed) VALUES ('AA123', 40.6413, -73.7781, 100.0, 10000, 500)",
        "INSERT INTO flight_info (number, lat, lon, fuel, height, speed) VALUES ('AA124', -34.8222, -58.5358, 100.0, 10000, 500)",
        "INSERT INTO flight_info (number, lat, lon, fuel, height, speed) VALUES ('AA125', -34.8381, -56.0308, 100.0, 10000, 500)",
        "INSERT INTO flight_info (number, lat, lon, fuel, height, speed) VALUES ('AA126', 25.7959, -80.2870, 100.0, 10000, 500)",
        "INSERT INTO flight_info (number, lat, lon, fuel, height, speed) VALUES ('AA127', 33.9416, -118.4085, 100.0, 10000, 500)",
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

    let airports = db::get_airports(&mut client, "USA");

    println!("Airports: {:?}", airports);
}
