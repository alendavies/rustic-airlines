use driver::CassandraClient;
use std::{net::Ipv4Addr, str::FromStr};

fn main() {
    // Reemplaza con la dirección IP y puerto correctos del servidor
    let server_ip = "127.0.0.2";
    let ip = Ipv4Addr::from_str(&server_ip).unwrap();

    // Conectarse al servidor Cassandra
    let mut client = CassandraClient::connect(ip).unwrap();
    client.startup().unwrap();
    let queries = vec![
        // // Create a keyspace with SimpleStrategy
        // "CREATE KEYSPACE test_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}".to_string(),

        // // Create a table within the keyspace
        // "CREATE TABLE test_keyspace.test_table (
        //     id UUID PRIMARY KEY,
        //     name TEXT,
        //     age INT
        // )".to_string(),

        // Drop the table
        "DROP TABLE test_keyspace.test_table".to_string(),
        //     // Drop the keyspace
        //     "DROP KEYSPACE test_keyspace".to_string(),
    ];

    // Ejecutar cada consulta en un loop
    let mut contador = 0;
    let len = queries.len();
    for query in queries {
        match client.execute(&query, "all") {
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
