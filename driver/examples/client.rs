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
    "CREATE KEYSPACE test_keyspace_simple WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}".to_string(),

    // Creación de la tabla con solo una clave primaria
    "CREATE TABLE test_keyspace_simple.test_table_simple (
        id TEXT PRIMARY KEY,
        value1 INT,
        value2 TEXT,
        value3 FLOAT
    )".to_string(),

    // INSERTs iniciales con valores para la clave primaria
    "INSERT INTO test_keyspace_simple.test_table_simple (id, value1, value2, value3) VALUES ('A1', 100, 'test1', 1.5)".to_string(),
    "INSERT INTO test_keyspace_simple.test_table_simple (id, value1, value2, value3) VALUES ('A2', 200, 'test2', 2.5)".to_string(),
    "INSERT INTO test_keyspace_simple.test_table_simple (id, value1, value2, value3) VALUES ('A3', 300, 'test3', 3.5)".to_string(),

    // SELECT para verificar los valores iniciales
    "SELECT id, value1, value2, value3 FROM test_keyspace_simple.test_table_simple WHERE id = 'A1'".to_string(),

    // UPDATE un registro existente, modificando múltiples columnas
    "UPDATE test_keyspace_simple.test_table_simple SET value1 = 150, value2 = 'updated' WHERE id = 'A1'".to_string(),

    // SELECT después del UPDATE para verificar los cambios
    "SELECT id, value1, value2, value3 FROM test_keyspace_simple.test_table_simple WHERE id = 'A1'".to_string(),

    // UPDATE un registro inexistente
    "UPDATE test_keyspace_simple.test_table_simple SET value1 = 500 WHERE id = 'A10'".to_string(),

    // DELETE un registro existente
    "DELETE FROM test_keyspace_simple.test_table_simple WHERE id = 'A2'".to_string(),

    // SELECT después de DELETE para verificar que el registro fue eliminado
    "SELECT id, value1, value2, value3 FROM test_keyspace_simple.test_table_simple WHERE id = 'A2'".to_string(),

    // DELETE un registro inexistente
    "DELETE FROM test_keyspace_simple.test_table_simple WHERE id = 'A11'".to_string(),

    // DELETE de una columna específica de un registro existente
    "DELETE value3 FROM test_keyspace_simple.test_table_simple WHERE id = 'A3'".to_string(),

    // SELECT después del DELETE de columna para verificar la ausencia de la columna
    "SELECT id, value1, value2, value3 FROM test_keyspace_simple.test_table_simple WHERE id = 'A3'".to_string(),

    // SELECT con una clave primaria existente para verificar consistencia
    "SELECT value1, value2 FROM test_keyspace_simple.test_table_simple WHERE id = 'A1'".to_string(),

    // SELECT con una clave primaria inexistente para verificar que no arroje resultados
    "SELECT value1, value2 FROM test_keyspace_simple.test_table_simple WHERE id = 'A12'".to_string(),
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
