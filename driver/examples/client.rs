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
        //"USE test_keyspace".to_string(),
        "CREATE TABLE test_keyspace.test_table (id INT, name TEXT, last_name TEXT ,personal_id UUID , PRIMARY KEY (id, name))".to_string(),
        "INSERT INTO test_keyspace.test_table (id, name ,personal_id) VALUES (1, 'Loren', uuid())".to_string(),
        //"INSERT INTO test_keyspace.test_table (id, name ,personal_id) VALUES (2, 'Loren', uuid())".to_string(),
        //"SELECT name FROM test_keyspace.test_table WHERE id = 2".to_string(),
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
