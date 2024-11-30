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
            lat DOUBLE,
            lon DOUBLE,
            angle FLOAT,
            departure_time TIMESTAMP,
            arrival_time TIMESTAMP,
            airport TEXT,
            direction TEXT,
            PRIMARY KEY (airport, direction, departure_time, arrival_time, number)
        )",

    "CREATE TABLE flight_info (
            number TEXT,
            fuel DOUBLE,
            height INT,
            speed INT,
            origin TEXT,
            destination TEXT,
            PRIMARY KEY (number)
        )",

    "INSERT INTO flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('AR101', 'on time', -34.5592, -58.4156, 125.3, '1730073800', '1730131300', 'AEP', 'departure')",
    "INSERT INTO flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('AR101', 'on time', -34.5592, -58.4156, 125.3, '1730073800', '1730131300', 'BRC', 'arrival')",
    "INSERT INTO flight_info (number, fuel, height, speed, origin, destination) VALUES ('AR101', 92.0, 11000, 540, 'AEP', 'BRC')",

    "INSERT INTO flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('AR001', 'on time', -34.8222, -58.5358, 239.5, '1730073688', '1730131200', 'AEP', 'departure')",
    "INSERT INTO flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('AR001', 'on time', -34.8222, -58.5358, 239.5, '1730073688', '1730131200', 'EZE', 'arrival')",
    "INSERT INTO flight_info (number, fuel, height, speed, origin, destination) VALUES ('AR001',  95.0, 10000, 550, 'AEP', 'EZE')",

    "INSERT INTO flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('AR102', 'delayed', -31.3236, -64.2080, 178.6, '1730074100', '1730132700', 'COR', 'departure')",
    "INSERT INTO flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('AR102', 'delayed', -31.3236, -64.2080, 178.6, '1730074100', '1730132700', 'USH', 'arrival')",
    "INSERT INTO flight_info (number, fuel, height, speed, origin, destination) VALUES ('AR102', 88.5, 12000, 530, 'COR', 'USH')",

    "INSERT INTO flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('AR103', 'on time', -32.8328, -68.7928, 245.7, '1730074300', '1730131900', 'MDZ', 'departure')",
    "INSERT INTO flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('AR103', 'on time', -32.8328, -68.7928, 245.7, '1730074300', '1730131900', 'SLA', 'arrival')",
    "INSERT INTO flight_info (number, fuel, height, speed, origin, destination) VALUES ('AR103', 85.0, 11500, 545, 'MDZ', 'SLA')",

    "INSERT INTO flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('AR104', 'cancelled', -25.7373, -54.4734, 90.2, '1730074500', '1730132100', 'IGR', 'departure')",
    "INSERT INTO flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('AR104', 'cancelled', -25.7373, -54.4734, 90.2, '1730074500', '1730132100', 'EZE', 'arrival')",
    "INSERT INTO flight_info (number, fuel, height, speed, origin, destination) VALUES ('AR104', 94.0, 10500, 535, 'IGR', 'EZE')",

    "INSERT INTO flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('AR105', 'on time', -32.9036, -60.7850, 156.8, '1730074700', '1730132300', 'ROS', 'departure')",
    "INSERT INTO flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('AR105', 'on time', -32.9036, -60.7850, 156.8, '1730074700', '1730132300', 'TUC', 'arrival')",
    "INSERT INTO flight_info (number, fuel, height, speed, origin, destination) VALUES ('AR105', 87.5, 11000, 525, 'ROS', 'TUC')",

    "INSERT INTO flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('AR106', 'delayed', -38.7242, -62.1693, 212.4, '1730074900', '1730132500', 'BHI', 'departure')",
    "INSERT INTO flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('AR106', 'delayed', -38.7242, -62.1693, 212.4, '1730074900', '1730132500', 'MDZ', 'arrival')",
    "INSERT INTO flight_info (number, fuel, height, speed, origin, destination) VALUES ('AR106', 89.0, 10000, 520, 'BHI', 'MDZ')",

    "INSERT INTO flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('AR107', 'on time', -24.3928, -65.0978, 278.9, '1730075100', '1730132700', 'JUJ', 'departure')",
    "INSERT INTO flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('AR107', 'on time', -24.3928, -65.0978, 278.9, '1730075100', '1730132700', 'ROS', 'arrival')",
    "INSERT INTO flight_info (number, fuel, height, speed, origin, destination) VALUES ('AR107', 91.5, 11500, 540, 'JUJ', 'ROS')",

    "INSERT INTO flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('AR108', 'on time', -51.6089, -69.3126, 34.6, '1730075300', '1730132900', 'RGL', 'departure')",
    "INSERT INTO flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('AR108', 'on time', -51.6089, -69.3126, 34.6, '1730075300', '1730132900', 'AEP', 'arrival')",
    "INSERT INTO flight_info (number, fuel, height, speed, origin, destination) VALUES ('AR108', 93.0, 12000, 550, 'RGL', 'AEP')",

    "INSERT INTO flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('AR109', 'delayed', -27.4455, -58.7619, 145.7, '1730075500', '1730133100', 'CNQ', 'departure')",
    "INSERT INTO flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('AR109', 'delayed', -27.4455, -58.7619, 145.7, '1730075500', '1730133100', 'COR', 'arrival')",
    "INSERT INTO flight_info (number, fuel, height, speed, origin, destination) VALUES ('AR109', 86.0, 10500, 530, 'CNQ', 'COR')",

    "INSERT INTO flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('AR110', 'on time', -36.5883, -64.2757, 198.3, '1730075700', '1730133300', 'RSA', 'departure')",
    "INSERT INTO flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('AR110', 'on time', -36.5883, -64.2757, 198.3, '1730075700', '1730133300', 'BRC', 'arrival')",
    "INSERT INTO flight_info (number, fuel, height, speed, origin, destination) VALUES ('AR110', 90.5, 11000, 535, 'RSA', 'BRC')",

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
