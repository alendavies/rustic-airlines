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
        
        // Creación de la tabla
        "CREATE TABLE test_keyspace.test_table (id TEXT, value1 INT, value2 INT, value3 INT, PRIMARY KEY (id, value1, value2)) WITH CLUSTERING ORDER BY (value1 ASC, value2 DESC)".to_string(),
    
        // INSERTs iniciales con la misma clave de partición
        "INSERT INTO test_keyspace.test_table (id, value1, value2, value3) VALUES ('A1', 100, 500, 40)".to_string(),
        "INSERT INTO test_keyspace.test_table (id, value1, value2, value3) VALUES ('A1', 200, 400, 35)".to_string(),
        "INSERT INTO test_keyspace.test_table (id, value1, value2, value3) VALUES ('A1', 300, 700, 50)".to_string(),
        "INSERT INTO test_keyspace.test_table (id, value1, value2, value3) VALUES ('A1', 150, 300, 25)".to_string(),
        "INSERT INTO test_keyspace.test_table (id, value1, value2, value3) VALUES ('A1', 250, 600, 55)".to_string(),
        "INSERT INTO test_keyspace.test_table (id, value1, value2, value3) VALUES ('A1', 350, 800, 60)".to_string(),
    
        // INSERTs adicionales para probar clustering y orden
        "INSERT INTO test_keyspace.test_table (id, value1, value2, value3) VALUES ('A1', 120, 450, 30)".to_string(),
        "INSERT INTO test_keyspace.test_table (id, value1, value2, value3) VALUES ('A1', 220, 550, 45)".to_string(),
        "INSERT INTO test_keyspace.test_table (id, value1, value2, value3) VALUES ('A1', 320, 750, 70)".to_string(),
        "INSERT INTO test_keyspace.test_table (id, value1, value2, value3) VALUES ('A1', 180, 350, 20)".to_string(),
        "INSERT INTO test_keyspace.test_table (id, value1, value2, value3) VALUES ('A1', 280, 650, 65)".to_string(),
    
        // UPDATE a un registro existente
        "UPDATE test_keyspace.test_table SET value3 = 42 WHERE id = 'A1' AND value1 = 100 AND value2 = 500".to_string(),
    
        // UPDATE a un registro inexistente
        "UPDATE test_keyspace.test_table SET value3 = 60 WHERE id = 'A1' AND value1 = 400 AND value2 = 900".to_string(),
    
        // DELETE de un registro existente
        "DELETE FROM test_keyspace.test_table WHERE id = 'A1' AND value1 = 100 AND value2 = 500".to_string(),
    
        // DELETE de un registro inexistente
        "DELETE FROM test_keyspace.test_table WHERE id = 'A1' AND value1 = 999 AND value2 = 888".to_string(),
    
        // DELETE de una columna específica en un registro existente
        "DELETE value3 FROM test_keyspace.test_table WHERE id = 'A1' AND value1 = 200 AND value2 = 400".to_string(),
    
        // SELECT registros específicos con WHERE
        "SELECT value1, value2 FROM test_keyspace.test_table WHERE id = 'A1' AND value1 = 150".to_string(),
    
        // SELECT con condiciones en las clustering columns
        "SELECT value1, value2 FROM test_keyspace.test_table WHERE id = 'A1' AND value1 > 200 AND value2 < 700".to_string(),
    
        // SELECT con ORDER BY y múltiples clustering columns
        "SELECT value1, value2, value3 FROM test_keyspace.test_table WHERE id = 'A1' AND value1 = 120 AND value2 = 800".to_string(),
    
        // SELECT con LIMIT y condiciones en clustering columns
        "SELECT value1, value3 FROM test_keyspace.test_table WHERE id = 'A1' LIMIT 3".to_string(),
    
        // SELECT sin resultados esperados
        "SELECT value1, value2 FROM test_keyspace.test_table WHERE id = 'A1' AND value1 = 999 AND value2 = 888".to_string(),
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
