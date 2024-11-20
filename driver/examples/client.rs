use driver::CassandraClient;
use native_protocol::messages::result;
use std::{net::Ipv4Addr, str::FromStr};

fn main() {
    // Reemplaza con la dirección IP y puerto correctos del servidor
    let server_ip = "127.0.0.4";
    let ip = Ipv4Addr::from_str(&server_ip).unwrap();

    // Conectarse al servidor Cassandra
    let mut client = CassandraClient::connect(ip).unwrap();
    client.startup().unwrap();
    let queries = vec![
    "CREATE KEYSPACE test_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}".to_string(),
    "CREATE TABLE test_keyspace.test_table (id INT, name TEXT, last_name INT, age INT, PRIMARY KEY (id, name, last_name)) WITH CLUSTERING ORDER BY (name DESC, last_name ASC)".to_string(),
    "INSERT INTO test_keyspace.test_table (id, name, last_name, age) VALUES (1, 'Alpha', 500, 40)".to_string(),
    "INSERT INTO test_keyspace.test_table (id, name, last_name, age) VALUES (1, 'Alpha', 300, 30)".to_string(),
    "INSERT INTO test_keyspace.test_table (id, name, last_name, age) VALUES (1, 'Alpha', 300, 35)".to_string(), // Caso idéntico
    "INSERT INTO test_keyspace.test_table (id, name, last_name, age) VALUES (1, 'Beta', 700, 50)".to_string(),
    "INSERT INTO test_keyspace.test_table (id, name, last_name, age) VALUES (1, 'Beta', 600, 45)".to_string(),
    "INSERT INTO test_keyspace.test_table (id, name, last_name, age) VALUES (1, 'Beta', 600, 60)".to_string(),
    "INSERT INTO test_keyspace.test_table (id, name, last_name, age) VALUES (1, 'Gamma', 800, 55)".to_string(),
    "INSERT INTO test_keyspace.test_table (id, name, last_name, age) VALUES (1, 'Delta', 400, 25)".to_string(),
    "INSERT INTO test_keyspace.test_table (id, name, last_name, age) VALUES (1, 'Delta', 450, 35)".to_string(),
];


    // Ejecutar cada consulta en un loop
    let mut contador = 0;
    let len = queries.len();
    for query in queries {
        match client.execute(&query, "quorum") {
            Ok(query_result) => {
                match query_result {
                    driver::QueryResult::Result(result) => {
                        contador += 1;
                        println!(
                            "Consulta ejecutada exitosamente: {} y el resultado fue {:?}",
                            query, result
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
