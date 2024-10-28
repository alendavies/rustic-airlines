use driver::CassandraClient;
use graphical_interface::db;
use std::{net::Ipv4Addr, str::FromStr};

fn main() {
    let server_ip = "127.0.0.2";
    let ip = Ipv4Addr::from_str(&server_ip).unwrap();

    let mut client = CassandraClient::connect(ip).unwrap();
    client.startup().unwrap();

    let queries = vec![
    "CREATE KEYSPACE sky WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
    "USE sky",
    "CREATE TABLE airports (
            iata TEXT,
            country TEXT,
            name TEXT,
            lat DOUBLE,
            lon DOUBLE,
            PRIMARY KEY (country, iata)
            )",
    "INSERT INTO airports (iata, country, name, lat, lon) VALUES ('EZE', 'ARG', 'Ministro Pistarini', -34.8222, -58.5358)",
    "INSERT INTO airports (iata, country, name, lat, lon) VALUES ('AEP', 'ARG', 'Aeroparque Jorge Newbery', -34.5592, -58.4156)",
    "INSERT INTO airports (iata, country, name, lat, lon) VALUES ('COR', 'ARG', 'Ingeniero Ambrosio Taravella', -31.3236, -64.2080)",
    "INSERT INTO airports (iata, country, name, lat, lon) VALUES ('MDZ', 'ARG', 'El Plumerillo', -32.8328, -68.7928)",
    "INSERT INTO airports (iata, country, name, lat, lon) VALUES ('ROS', 'ARG', 'Islas Malvinas', -32.9036, -60.7850)",
    "INSERT INTO airports (iata, country, name, lat, lon) VALUES ('SLA', 'ARG', 'Martín Miguel de Güemes', -24.8425, -65.4861)",
    "INSERT INTO airports (iata, country, name, lat, lon) VALUES ('IGR', 'ARG', 'Cataratas del Iguazú', -25.7373, -54.4734)",
    "INSERT INTO airports (iata, country, name, lat, lon) VALUES ('BRC', 'ARG', 'Teniente Luis Candelaria', -41.9629, -71.5332)",
    "INSERT INTO airports (iata, country, name, lat, lon) VALUES ('USH', 'ARG', 'Malvinas Argentinas', -54.8433, -68.2958)",
    "INSERT INTO airports (iata, country, name, lat, lon) VALUES ('TUC', 'ARG', 'Teniente General Benjamín Matienzo', -26.8409, -65.1048)",

    "CREATE TABLE flights (
            number TEXT,
            status TEXT,
            departure_time TIMESTAMP,
            arrival_time TIMESTAMP,
            airport TEXT,
            direction TEXT,
            PRIMARY KEY (airport, direction, departure_time, arrival_time)
            )",
    "INSERT INTO flights (number, status, departure_time, arrival_time, airport, direction) VALUES ('AR001', 'on time', '1730073688', '1730131200', 'EZE', 'arrival')",
    "INSERT INTO flights (number, status, departure_time, arrival_time, airport, direction) VALUES ('AR002', 'delayed', '1730131200', '1730131200', 'AEP', 'departure')",
    "INSERT INTO flights (number, status, departure_time, arrival_time, airport, direction) VALUES ('AR003', 'on time', '1730073698', '46741883131', 'COR', 'arrival')",
    "INSERT INTO flights (number, status, departure_time, arrival_time, airport, direction) VALUES ('AR004', 'on time', '1730073698', '46741889943', 'MDZ', 'departure')",
    "INSERT INTO flights (number, status, departure_time, arrival_time, airport, direction) VALUES ('AR005', 'on time', '1730073698', '46741889943', 'ROS', 'arrival')",
    "INSERT INTO flights (number, status, departure_time, arrival_time, airport, direction) VALUES ('AR006', 'cancelled', '1730073698', '46741889943', 'SLA', 'departure')",
    "INSERT INTO flights (number, status, departure_time, arrival_time, airport, direction) VALUES ('AR007', 'on time', '1730073698', '46741889943', 'IGR', 'arrival')",
    "INSERT INTO flights (number, status, departure_time, arrival_time, airport, direction) VALUES ('AR008', 'on time', '1730073698', '46741889943', 'BRC', 'departure')",
    "INSERT INTO flights (number, status, departure_time, arrival_time, airport, direction) VALUES ('AR009', 'delayed', '1730073698', '46741889943', 'USH', 'arrival')",
    "INSERT INTO flights (number, status, departure_time, arrival_time, airport, direction) VALUES ('AR010', 'on time', '1730073698', '46741889943', 'TUC', 'departure')",


    "CREATE TABLE flight_info (
            number TEXT,
            lat DOUBLE,
            lon DOUBLE,
            fuel DOUBLE,
            height INT,
            speed INT,
            PRIMARY KEY (number, lat)
            )",
    "INSERT INTO flight_info (number, lat, lon, fuel, height, speed) VALUES ('AR001', -34.8222, -58.5358, 95.0, 10000, 550)",
    "INSERT INTO flight_info (number, lat, lon, fuel, height, speed) VALUES ('AR002', -34.5592, -58.4156, 90.0, 12000, 540)",
    "INSERT INTO flight_info (number, lat, lon, fuel, height, speed) VALUES ('AR003', -31.3236, -64.2080, 85.0, 11000, 530)",
    "INSERT INTO flight_info (number, lat, lon, fuel, height, speed) VALUES ('AR004', -32.8328, -68.7928, 80.0, 10000, 520)",
    "INSERT INTO flight_info (number, lat, lon, fuel, height, speed) VALUES ('AR005', -32.9036, -60.7850, 75.0, 9500, 510)",
    "INSERT INTO flight_info (number, lat, lon, fuel, height, speed) VALUES ('AR006', -24.8425, -65.4861, 70.0, 12000, 550)",
    "INSERT INTO flight_info (number, lat, lon, fuel, height, speed) VALUES ('AR007', -25.7373, -54.4734, 65.0, 11500, 540)",
    "INSERT INTO flight_info (number, lat, lon, fuel, height, speed) VALUES ('AR008', -41.9629, -71.5332, 60.0, 10500, 530)",
    "INSERT INTO flight_info (number, lat, lon, fuel, height, speed) VALUES ('AR009', -54.8433, -68.2958, 55.0, 10000, 520)",
    "INSERT INTO flight_info (number, lat, lon, fuel, height, speed) VALUES ('AR010', -26.8409, -65.1048, 50.0, 9000, 510)",

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

    /* let departure_flights =
        db::get_departure_flights(&mut client, "JFK", chrono::offset::Utc::now().date_naive())
            .unwrap();
    println!("Departure flights: {:?}", departure_flights); */

    /* let arrival_flights =
        db::get_arrival_flights(&mut client, "LAX", chrono::offset::Utc::now().date_naive())
            .unwrap();
    println!("Arrival flights: {:?}", arrival_flights); */

    /*   let flight_info = db::get_flight_info(&mut client, "AA123").unwrap();
    println!("Flight info: {:?}", flight_info); */
}
