use driver::CassandraClient;
use std::{net::Ipv4Addr, str::FromStr};

fn main() {
    let server_ip = "127.0.0.2";
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
    "INSERT INTO airports (iata, country, name, lat, lon) VALUES ('AFA', 'ARG', 'Suboficial Ayudante Santiago Germano', -34.5883, -68.4039)",
    "INSERT INTO airports (iata, country, name, lat, lon) VALUES ('CRD', 'ARG', 'General Enrique Mosconi', -45.7853, -67.4655)",
    "INSERT INTO airports (iata, country, name, lat, lon) VALUES ('CNQ', 'ARG', 'Doctor Fernando Piragine Niveyro', -27.4455, -58.7619)",
    "INSERT INTO airports (iata, country, name, lat, lon) VALUES ('EHL', 'ARG', 'Aeropuerto El Bolsón', -41.9432, -71.5327)",
    "INSERT INTO airports (iata, country, name, lat, lon) VALUES ('EPA', 'ARG', 'El Palomar', -34.6099, -58.6126)",
    "INSERT INTO airports (iata, country, name, lat, lon) VALUES ('EQS', 'ARG', 'Brigadier General Antonio Parodi', -42.9080, -71.1395)",
    "INSERT INTO airports (iata, country, name, lat, lon) VALUES ('FMA', 'ARG', 'Formosa', -26.2127, -58.2281)",
    "INSERT INTO airports (iata, country, name, lat, lon) VALUES ('GGS', 'ARG', 'Gobernador Gregores', -48.7831, -70.1500)",
    "INSERT INTO airports (iata, country, name, lat, lon) VALUES ('GPO', 'ARG', 'General Pico', -35.6962, -63.7580)",
    "INSERT INTO airports (iata, country, name, lat, lon) VALUES ('JUJ', 'ARG', 'Horacio Guzmán', -24.3928, -65.0978)",
    "INSERT INTO airports (iata, country, name, lat, lon) VALUES ('LGS', 'ARG', 'Comodoro D. Ricardo Salomón', -35.4936, -69.5747)",
    "INSERT INTO airports (iata, country, name, lat, lon) VALUES ('LAP', 'ARG', 'Comodoro Arturo Merino Benítez', -32.85, -68.86)",
    "INSERT INTO airports (iata, country, name, lat, lon) VALUES ('PMQ', 'ARG', 'Perito Moreno', -46.5361, -70.9787)",
    "INSERT INTO airports (iata, country, name, lat, lon) VALUES ('PRQ', 'ARG', 'Presidente Roque Sáenz Peña', -26.7564, -60.4922)",
    "INSERT INTO airports (iata, country, name, lat, lon) VALUES ('REL', 'ARG', 'Almirante Zar', -43.2105, -65.2703)",
    "INSERT INTO airports (iata, country, name, lat, lon) VALUES ('RCQ', 'ARG', 'General Justo José de Urquiza', -31.7948, -60.4804)",
    "INSERT INTO airports (iata, country, name, lat, lon) VALUES ('RGL', 'ARG', 'Piloto Civil Norberto Fernández', -51.6089, -69.3126)",
    "INSERT INTO airports (iata, country, name, lat, lon) VALUES ('RSA', 'ARG', 'Santa Rosa', -36.5883, -64.2757)",
    "INSERT INTO airports (iata, country, name, lat, lon) VALUES ('VDM', 'ARG', 'Gobernador Castello', -40.8692, -63.0004)",
    "INSERT INTO airports (iata, country, name, lat, lon) VALUES ('BHI', 'ARG', 'Comandante Espora', -38.7242, -62.1693)",

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


    "INSERT INTO flights (number, status, departure_time, arrival_time, airport, direction) VALUES ('AR011', 'on time', '1730073800', '1730131300', 'EZE', 'departure')",
    "INSERT INTO flights (number, status, departure_time, arrival_time, airport, direction) VALUES ('AR011', 'on time', '1730073800', '1730131300', 'AEP', 'arrival')",
    "INSERT INTO flight_info (number, lat, lon, fuel, height, speed) VALUES ('AR011', -34.5592, -58.4156, 93.0, 11000, 550)",

    "INSERT INTO flights (number, status, departure_time, arrival_time, airport, direction) VALUES ('AR012', 'delayed', '1730131500', '1730190000', 'COR', 'departure')",
    "INSERT INTO flights (number, status, departure_time, arrival_time, airport, direction) VALUES ('AR012', 'delayed', '1730131500', '1730190000', 'MDZ', 'arrival')",
    "INSERT INTO flight_info (number, lat, lon, fuel, height, speed) VALUES ('AR012', -32.8328, -68.7928, 88.0, 10500, 530)",

    "INSERT INTO flights (number, status, departure_time, arrival_time, airport, direction) VALUES ('AR013', 'on time', '1730073600', '1730132000', 'AEP', 'departure')",
    "INSERT INTO flights (number, status, departure_time, arrival_time, airport, direction) VALUES ('AR013', 'on time', '1730073600', '1730132000', 'SLA', 'arrival')",
    "INSERT INTO flight_info (number, lat, lon, fuel, height, speed) VALUES ('AR013', -24.8425, -65.4861, 86.0, 11500, 540)",

    "INSERT INTO flights (number, status, departure_time, arrival_time, airport, direction) VALUES ('AR014', 'cancelled', '1730074100', '1730132700', 'EZE', 'departure')",
    "INSERT INTO flights (number, status, departure_time, arrival_time, airport, direction) VALUES ('AR014', 'cancelled', '1730074100', '1730132700', 'IGR', 'arrival')",
    "INSERT INTO flight_info (number, lat, lon, fuel, height, speed) VALUES ('AR014', -25.7373, -54.4734, 82.0, 11000, 530)",

    "INSERT INTO flights (number, status, departure_time, arrival_time, airport, direction) VALUES ('AR015', 'on time', '1730073800', '1730133200', 'MDZ', 'departure')",
    "INSERT INTO flights (number, status, departure_time, arrival_time, airport, direction) VALUES ('AR015', 'on time', '1730073800', '1730133200', 'BRC', 'arrival')",
    "INSERT INTO flight_info (number, lat, lon, fuel, height, speed) VALUES ('AR015', -41.9629, -71.5332, 78.0, 10000, 520)",

    "INSERT INTO flights (number, status, departure_time, arrival_time, airport, direction) VALUES ('AR016', 'delayed', '1730133500', '1730191200', 'USH', 'departure')",
    "INSERT INTO flights (number, status, departure_time, arrival_time, airport, direction) VALUES ('AR016', 'delayed', '1730133500', '1730191200', 'TUC', 'arrival')",
    "INSERT INTO flight_info (number, lat, lon, fuel, height, speed) VALUES ('AR016', -26.8409, -65.1048, 76.0, 9500, 510)",

    "INSERT INTO flights (number, status, departure_time, arrival_time, airport, direction) VALUES ('AR017', 'on time', '1730073900', '1730133700', 'ROS', 'departure')",
    "INSERT INTO flights (number, status, departure_time, arrival_time, airport, direction) VALUES ('AR017', 'on time', '1730073900', '1730133700', 'AEP', 'arrival')",
    "INSERT INTO flight_info (number, lat, lon, fuel, height, speed) VALUES ('AR017', -34.5592, -58.4156, 90.0, 12000, 540)",

    "INSERT INTO flights (number, status, departure_time, arrival_time, airport, direction) VALUES ('AR018', 'on time', '1730074000', '1730133800', 'SLA', 'departure')",
    "INSERT INTO flights (number, status, departure_time, arrival_time, airport, direction) VALUES ('AR018', 'on time', '1730074000', '1730133800', 'EZE', 'arrival')",
    "INSERT INTO flight_info (number, lat, lon, fuel, height, speed) VALUES ('AR018', -34.8222, -58.5358, 92.0, 11000, 540)",

    "INSERT INTO flights (number, status, departure_time, arrival_time, airport, direction) VALUES ('AR019', 'delayed', '1730133900', '1730191400', 'COR', 'departure')",
    "INSERT INTO flights (number, status, departure_time, arrival_time, airport, direction) VALUES ('AR019', 'delayed', '1730133900', '1730191400', 'USH', 'arrival')",
    "INSERT INTO flight_info (number, lat, lon, fuel, height, speed) VALUES ('AR019', -54.8433, -68.2958, 74.0, 10000, 520)",

    "INSERT INTO flights (number, status, departure_time, arrival_time, airport, direction) VALUES ('AR020', 'on time', '1730074200', '1730134000', 'MDZ', 'departure')",
    "INSERT INTO flights (number, status, departure_time, arrival_time, airport, direction) VALUES ('AR020', 'on time', '1730074200', '1730134000', 'AEP', 'arrival')",
    "INSERT INTO flight_info (number, lat, lon, fuel, height, speed) VALUES ('AR020', -34.5592, -58.4156, 77.0, 10500, 530)",

    "INSERT INTO flights (number, status, departure_time, arrival_time, airport, direction) VALUES ('AR021', 'cancelled', '1730074300', '1730134100', 'EZE', 'departure')",
    "INSERT INTO flights (number, status, departure_time, arrival_time, airport, direction) VALUES ('AR021', 'cancelled', '1730074300', '1730134100', 'ROS', 'arrival')",
    "INSERT INTO flight_info (number, lat, lon, fuel, height, speed) VALUES ('AR021', -32.9036, -60.7850, 80.0, 12000, 550)",

    "INSERT INTO flights (number, status, departure_time, arrival_time, airport, direction) VALUES ('AR022', 'on time', '1730134200', '1730191800', 'BRC', 'departure')",
    "INSERT INTO flights (number, status, departure_time, arrival_time, airport, direction) VALUES ('AR022', 'on time', '1730134200', '1730191800', 'COR', 'arrival')",
    "INSERT INTO flight_info (number, lat, lon, fuel, height, speed) VALUES ('AR022', -31.3236, -64.2080, 65.0, 11000, 530)",

    ];

    let mut contador = 0;
    let len = queries.len();
    for query in queries {
        match client.execute(&query, "all") {
            Ok(query_result) => {
                match query_result {
                    driver::QueryResult::Result(_) => {
                        contador += 1;
                        println!(
                            "Consulta ejecutada exitosamente: {} y el resultado fue {:?}",
                            query, query_result
                        );
                    }
                    driver::QueryResult::Error(error) => {
                        println!("Error en la consulta: {:?}", error);
                    }
                }
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
