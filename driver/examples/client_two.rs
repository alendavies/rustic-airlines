use driver::CassandraClient;
use std::{net::Ipv4Addr, str::FromStr, thread, time::Duration};

fn main() {
    // Dirección IP del servidor Cassandra
    let server_ip = "127.0.0.2";
    let ip = Ipv4Addr::from_str(&server_ip).unwrap();

    // Conectarse al servidor Cassandra
    let mut client = CassandraClient::connect(ip).unwrap();
    client.startup().unwrap();

    let setup_queries = vec![
        "CREATE KEYSPACE test_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}",
        "USE test_keyspace",
        "CREATE TABLE simple_table (
            id INT,
            name TEXT,
            PRIMARY KEY (id, name)
        )",
    ];

    for query in setup_queries {
        match client.execute(query, "any") {
            Ok(_) => println!("Setup query executed: {}", query),
            Err(e) => eprintln!("Error executing setup query: {}\nError: {:?}", query, e),
        }
    }

    // Insertar 100,000 registros en la tabla simple
    let total_inserts = 100_000;
    for i in 1..=total_inserts {
        let name = format!("name_{}", i); // Generar un nombre único para cada registro
        let insert_query = format!(
            "INSERT INTO simple_table (id, name) VALUES ({}, '{}')",
            i, name
        );

        match client.execute(&insert_query, "quorum") {
            Ok(frame) => {
                println!(
                    "la query {:?} dio OK y el frame es {:?}",
                    insert_query, frame
                );
                if i % 1000 == 0 {
                    println!("Inserted records successfully con frame");
                    // Pausar por 4 segundos cada 1,000 registros insertados
                    thread::sleep(Duration::from_secs(4));
                }
            }
            Err(e) => {
                eprintln!("Error executing insert query for id {}: {:?}", i, e);
                // Detener el bucle si hay un error
                break;
            }
        }
    }

    println!("Finished inserting {} records", total_inserts);
}
