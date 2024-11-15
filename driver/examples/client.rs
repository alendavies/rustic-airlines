use driver::CassandraClient;
use std::{net::Ipv4Addr, str::FromStr};

fn main() {
    // Reemplaza con la dirección IP y puerto correctos del servidor
    let server_ip = "127.0.0.4";
    let ip = Ipv4Addr::from_str(&server_ip).unwrap();

    // Conectarse al servidor Cassandra
    let mut client = CassandraClient::connect(ip).unwrap();
    client.startup().unwrap();
    let queries = vec![
        "CREATE KEYSPACE test_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}".to_string(),
        "CREATE TABLE test_keyspace.test_table (id INT, name TEXT, last_name TEXT, age INT, PRIMARY KEY (id, name))".to_string(),
        // "INSERT INTO test_keyspace.test_table (id, name, last_name, age) VALUES (1, 'Loren', 'Smith', 24)".to_string(),
        // "INSERT INTO test_keyspace.test_table (id, name, last_name, age) VALUES (2, 'Alice', 'Johnson', 30)".to_string(),
        // "INSERT INTO test_keyspace.test_table (id, name, last_name, age) VALUES (3, 'Nau', 'Brown', 45)".to_string(),
        // "INSERT INTO test_keyspace.test_table (id, name, last_name, age) VALUES (4, 'Lib', 'Brown', 45)".to_string(),
        // "INSERT INTO test_keyspace.test_table (id, name, last_name, age) VALUES (5, 'AA', 'Brown', 45)".to_string(),
        // "INSERT INTO test_keyspace.test_table (id, name, last_name, age) VALUES (6, 'CCC', 'Brown', 45)".to_string(),
        // "SELECT last_name, name FROM test_keyspace.test_table WHERE id = 1 ORDER BY name LIMIT 5".to_string(),
        // Queries for column modifications
        "ALTER TABLE test_keyspace.test_table ADD middle_name TEXT".to_string(),
        "ALTER TABLE test_keyspace.test_table DROP age".to_string(),
        "ALTER TABLE test_keyspace.test_table RENAME last_name TO surname".to_string(),
    ];

    // Ejecutar cada consulta en un loopc
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
                        println!("La query: {:?} fallo con el error {:?}", query, error);
                    }
                }
                println!("exitosas {:?}/{:?}", contador, len)
            }
            Err(e) => eprintln!("Error al ejecutar la consulta: {}\nError: {:?}", query, e),
        }
    }
}
