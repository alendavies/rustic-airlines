use driver::CassandraClient;
use std::{net::Ipv4Addr, str::FromStr};

fn main() {
    // Dirección IP del servidor Cassandra
    let server_ip = "127.0.0.4";
    let ip = Ipv4Addr::from_str(&server_ip).unwrap();

    // Conectarse al servidor Cassandra
    let mut client = CassandraClient::connect(ip).unwrap();
    client.startup().unwrap();

    let setup_queries = vec![
        //"CREATE KEYSPACE test_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}",
        //"USE test_keyspace",
        "CREATE TABLE simple_table (
        id INT,
        name TEXT,
        PRIMARY KEY (id, name)
    )",
    ];

    for query in setup_queries {
        match client.execute(query, "all") {
            Ok(_) => println!("Setup query executed: {}", query),
            Err(e) => eprintln!("Error executing setup query: {}\nError: {:?}", query, e),
        }
    }

    // Insertar 100,000 registros en la tabla simple
    let total_inserts = 100_000;
    for i in 1..=total_inserts {
        //thread::sleep(Duration::from_millis(500));
        let name = format!("name_{}", i); // Generar un nombre único para cada registro
        let insert_query = format!(
            "INSERT INTO simple_table (id, name) VALUES ({}, '{}')",
            i, name
        );

        println!("{:?}", insert_query);
        match client.execute(&insert_query, "all") {
            Ok(_) => {
                if i % 1000 == 0 {
                    println!("Inserted {} records successfully", i);
                }
            }
            Err(e) => {
                eprintln!("Error executing insert query for id {}: {:?}", i, e);
                break; // Detener el bucle si hay un error
            }
        }
    }

    println!("Finished inserting {} records", total_inserts);
}
