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
        // Creación del keyspace
        "CREATE KEYSPACE test_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}".to_string(),

        // Creación de la tabla con una sola clave primaria
        "CREATE TABLE test_keyspace.test_table (id TEXT PRIMARY KEY, value1 INT, value2 INT, value3 INT)".to_string(),

        // INSERTs iniciales con diferentes valores de clave primaria
        "INSERT INTO test_keyspace.test_table (id, value1, value2, value3) VALUES ('A1', 100, 500, 40)".to_string(),
        "INSERT INTO test_keyspace.test_table (id, value1, value2, value3) VALUES ('A2', 200, 400, 35)".to_string(),
        "INSERT INTO test_keyspace.test_table (id, value1, value2, value3) VALUES ('A3', 300, 700, 50)".to_string(),
        "INSERT INTO test_keyspace.test_table (id, value1, value2, value3) VALUES ('A4', 150, 300, 25)".to_string(),
        "INSERT INTO test_keyspace.test_table (id, value1, value2, value3) VALUES ('A5', 250, 600, 55)".to_string(),
        "INSERT INTO test_keyspace.test_table (id, value1, value2, value3) VALUES ('A6', 350, 800, 60)".to_string(),

        // UPDATE a un registro existente
        "UPDATE test_keyspace.test_table SET value3 = 42 WHERE id = 'A1'".to_string(),

        // UPDATE a un registro inexistente
        "UPDATE test_keyspace.test_table SET value3 = 60 WHERE id = 'A7'".to_string(),

        // DELETE de un registro existente
        "DELETE FROM test_keyspace.test_table WHERE id = 'A1'".to_string(),

        // DELETE de un registro inexistente
        "DELETE FROM test_keyspace.test_table WHERE id = 'A9'".to_string(),

        // DELETE de una columna específica en un registro existente
        "DELETE value3 FROM test_keyspace.test_table WHERE id = 'A2'".to_string(),

        // SELECT registros específicos con WHERE
        "SELECT value1, value2 FROM test_keyspace.test_table WHERE id = 'A4'".to_string(),

        // SELECT con condiciones sobre la clave primaria (igualdad)
        "SELECT value1, value2 FROM test_keyspace.test_table WHERE id = 'A5'".to_string(),

        // SELECT sin resultados esperados
        "SELECT value1, value2 FROM test_keyspace.test_table WHERE id = 'A10'".to_string(),

        // Eliminar la tabla al final
        "DROP TABLE test_keyspace.test_table".to_string(),
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
