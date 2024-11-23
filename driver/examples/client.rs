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

    // Creación de la tabla con clave primaria y dos columnas de clustering
    "CREATE TABLE test_keyspace.test_table (
        id TEXT, 
        cluster1 TEXT, 
        cluster2 TEXT, 
        value1 INT, 
        value2 INT, 
        value3 INT, 
        PRIMARY KEY (id, cluster1, cluster2)
    )".to_string(),

    // INSERTs iniciales con valores para las claves de clustering
    "INSERT INTO test_keyspace.test_table (id, cluster1, cluster2, value1, value2, value3) VALUES ('A2', 'B1', 'C1', 100, 500, 40)".to_string(),
    "INSERT INTO test_keyspace.test_table (id, cluster1, cluster2, value1, value2, value3) VALUES ('A2', 'B2', 'C2', 200, 400, 35)".to_string(),
    "INSERT INTO test_keyspace.test_table (id, cluster1, cluster2, value1, value2, value3) VALUES ('A2', 'B3', 'C3', 300, 700, 50)".to_string(),
    "INSERT INTO test_keyspace.test_table (id, cluster1, cluster2, value1, value2, value3) VALUES ('A2', 'B4', 'C4', 150, 300, 25)".to_string(),

    // UPDATE a un registro existente con un cambio significativo, especificando todas las claves
    "UPDATE test_keyspace.test_table SET value1 = 900000, value3 = 42 WHERE id = 'A2' AND cluster1 = 'B1' AND cluster2 = 'C1'".to_string(),

    // SELECT después del UPDATE para verificar la integridad
    "SELECT id, cluster1, cluster2, value1, value3 FROM test_keyspace.test_table WHERE id = 'A2' AND cluster1 = 'B1' AND cluster2 = 'C1'".to_string(),

//     // UPDATE que alarga un valor para provocar un cambio en los índices, especificando todas las claves
//     "UPDATE test_keyspace.test_table SET value2 = 99999 WHERE id = 'A2' AND cluster1 = 'B2' AND cluster2 = 'C2'".to_string(),

//     // SELECT después de un UPDATE que modifica el tamaño del valor
//     "SELECT id, cluster1, cluster2, value1, value2, value3 FROM test_keyspace.test_table WHERE id = 'A2' AND cluster1 = 'B2' AND cluster2 = 'C2'".to_string(),

//     // UPDATE con una clave primaria inexistente
//     "UPDATE test_keyspace.test_table SET value3 = 60 WHERE id = 'A7' AND cluster1 = 'B5' AND cluster2 = 'C5'".to_string(),

//     // DELETE un registro existente
//     "DELETE FROM test_keyspace.test_table WHERE id = 'A2' AND cluster1 = 'B1' AND cluster2 = 'C1'".to_string(),

//     // SELECT después de DELETE para verificar que el registro fue eliminado
//     "SELECT id, cluster1, cluster2, value1, value3 FROM test_keyspace.test_table WHERE id = 'A2' AND cluster1 = 'B1' AND cluster2 = 'C1'".to_string(),

//     // DELETE con una clave inexistente para verificar que no afecta índices
//     "DELETE FROM test_keyspace.test_table WHERE id = 'A8' AND cluster1 = 'B6' AND cluster2 = 'C6'".to_string(),

//     // DELETE de una columna específica de un registro existente
//     "DELETE value3 FROM test_keyspace.test_table WHERE id = 'A2' AND cluster1 = 'B2' AND cluster2 = 'C2'".to_string(),

//     // SELECT después del DELETE de columna para verificar la ausencia de la columna
//     "SELECT id, cluster1, cluster2, value1, value2, value3 FROM test_keyspace.test_table WHERE id = 'A2' AND cluster1 = 'B2' AND cluster2 = 'C2'".to_string(),

//     // SELECT con una clave primaria existente para verificar que los índices aún son consistentes
//     "SELECT value1, value2 FROM test_keyspace.test_table WHERE id = 'A2' AND cluster1 = 'B4' AND cluster2 = 'C4'".to_string(),

//     // SELECT sin resultados esperados
//     "SELECT value1, value2 FROM test_keyspace.test_table WHERE id = 'A10' AND cluster1 = 'B7' AND cluster2 = 'C7'".to_string(),
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
