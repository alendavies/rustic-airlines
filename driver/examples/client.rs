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
        "CREATE KEYSPACE test_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}".to_string(),
        "ALTER KEYSPACE test_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}".to_string(),
        "USE test_keyspace".to_string(),
        "CREATE KEYSPACE test_keyspace_dos WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}".to_string(),
        "CREATE TABLE test_table (id INT ,name TEXT, PRIMARY KEY (id, name))".to_string(),
        "INSERT INTO test_table (id, name) VALUES (1, 'Loren')".to_string(),
        "ALTER TABLE test_table ADD last_name TEXT".to_string(),
        "USE test_keyspace_dos".to_string(),
        "USE test_keyspace".to_string(),
        "INSERT INTO test_table (id, name) VALUES (2, 'Marcos')".to_string(),
        "SELECT name FROM test_table WHERE id = 2".to_string()
     ];

    // Ejecutar cada consulta en un loop
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
                        println!("La query fallo con el error {:?}", error);
                    }
                }
                println!("exitosas {:?}/{:?}", contador, len)
            }
            Err(e) => eprintln!("Error al ejecutar la consulta: {}\nError: {:?}", query, e),
        }
    }
}
