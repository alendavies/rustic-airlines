use driver::CassandraClient;
use std::{net::Ipv4Addr, str::FromStr};

fn main() {
    // Reemplaza con la dirección IP y puerto correctos del servidor
    let server_ip = "127.0.0.1";
    let ip = Ipv4Addr::from_str(&server_ip).unwrap();

    // Conectarse al servidor Cassandra
    let mut client = CassandraClient::connect(ip).unwrap();
    client.startup().unwrap();

    // Lista de consultas CQL para ejecutar
    let queries = vec![
        // Crear el keyspace
        "CREATE KEYSPACE world WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}",
        "CREATE TABLE users_activity (
            id INT,
            created_at TEXT,
            status TEXT,
            activity_details TEXT,
            PRIMARY KEY (id, created_at)
        )",
        // // Crear la tabla
        // "CREATE TABLE users (
        //     user_id int PRIMARY KEY,
        //     first_name text,
        //     last_name text,
        //     email text,
        //     age int
        // )",
        // // Insertar algunos elementos
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (1, 'John', 'Doe', 'john.doe@example.com', 30)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (7, 'Jane', 'Smith', 'jane.smith@example.com', 25)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (16, 'Alice', 'Johnson', 'alice.johnson@example.com', 35)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (16, 'Alice2', 'Johnson', 'alice.johnson@example.com', 35)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (16, 'Alice3', 'Johnson', 'alice.johnson@example.com', 35)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (16, 'Alice4', 'Johnson', 'alice.johnson@example.com', 35)",

        // // // Borrar algunos elementos
        // "DELETE FROM users WHERE user_id = 7",
        // "DELETE FROM users WHERE user_id = 7",
        // "SELECT email, user_id, age FROM users WHERE user_id = 16"
    ];

    // Ejecutar cada consulta en un loop
    for query in queries {
        match client.execute(&query) {
            Ok(query_result) => println!(
                "Consulta ejecutada exitosamente: {} y el resultado fue {:?}",
                query, query_result
            ),
            Err(e) => eprintln!("Error al ejecutar la consulta: {}\nError: {:?}", query, e),
        }
    }
}
