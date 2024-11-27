use driver::CassandraClient;
use std::{net::Ipv4Addr, str::FromStr};

fn main() {
    // Reemplaza con la dirección IP y puerto correctos del servidor
    let server_ip = "127.0.0.3";
    let ip = Ipv4Addr::from_str(&server_ip).unwrap();

    // Conectarse al servidor Cassandra
    let mut client = CassandraClient::connect(ip).unwrap();
    client.startup().unwrap();
    let queries = vec![
        // Crear un keyspace
        "CREATE KEYSPACE IF NOT EXISTS my_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};".to_string(),
    
        // Crear una tabla
        "CREATE TABLE IF NOT EXISTS my_keyspace.my_table (
            id UUID,
            partition_key TEXT,
            clustering_key INT,
            data TEXT,
            PRIMARY KEY (partition_key, clustering_key)
        );".to_string(),
    
        // Insertar 30 filas con la misma partition_key
        "INSERT INTO my_keyspace.my_table (id, partition_key, clustering_key, data) VALUES (uuid(), 'my_partition', 0, 'data_0');".to_string(),
        "INSERT INTO my_keyspace.my_table (id, partition_key, clustering_key, data) VALUES (uuid(), 'my_partition', 1, 'data_1');".to_string(),
        "INSERT INTO my_keyspace.my_table (id, partition_key, clustering_key, data) VALUES (uuid(), 'my_partition', 2, 'data_2');".to_string(),
        "INSERT INTO my_keyspace.my_table (id, partition_key, clustering_key, data) VALUES (uuid(), 'my_partition', 3, 'data_3');".to_string(),
        "INSERT INTO my_keyspace.my_table (id, partition_key, clustering_key, data) VALUES (uuid(), 'my_partition', 4, 'data_4');".to_string(),
        "INSERT INTO my_keyspace.my_table (id, partition_key, clustering_key, data) VALUES (uuid(), 'my_partition', 5, 'data_5');".to_string(),
        "INSERT INTO my_keyspace.my_table (id, partition_key, clustering_key, data) VALUES (uuid(), 'my_partition', 6, 'data_6');".to_string(),
        "INSERT INTO my_keyspace.my_table (id, partition_key, clustering_key, data) VALUES (uuid(), 'my_partition', 7, 'data_7');".to_string(),
        "INSERT INTO my_keyspace.my_table (id, partition_key, clustering_key, data) VALUES (uuid(), 'my_partition', 8, 'data_8');".to_string(),
        "INSERT INTO my_keyspace.my_table (id, partition_key, clustering_key, data) VALUES (uuid(), 'my_partition', 9, 'data_9');".to_string(),
        "INSERT INTO my_keyspace.my_table (id, partition_key, clustering_key, data) VALUES (uuid(), 'my_partition', 10, 'data_10');".to_string(),
        "INSERT INTO my_keyspace.my_table (id, partition_key, clustering_key, data) VALUES (uuid(), 'my_partition', 11, 'data_11');".to_string(),
        "INSERT INTO my_keyspace.my_table (id, partition_key, clustering_key, data) VALUES (uuid(), 'my_partition', 12, 'data_12');".to_string(),
        "INSERT INTO my_keyspace.my_table (id, partition_key, clustering_key, data) VALUES (uuid(), 'my_partition', 13, 'data_13');".to_string(),
        "INSERT INTO my_keyspace.my_table (id, partition_key, clustering_key, data) VALUES (uuid(), 'my_partition', 14, 'data_14');".to_string(),
        "INSERT INTO my_keyspace.my_table (id, partition_key, clustering_key, data) VALUES (uuid(), 'my_partition', 15, 'data_15');".to_string(),
        "INSERT INTO my_keyspace.my_table (id, partition_key, clustering_key, data) VALUES (uuid(), 'my_partition', 16, 'data_16');".to_string(),
        "INSERT INTO my_keyspace.my_table (id, partition_key, clustering_key, data) VALUES (uuid(), 'my_partition', 17, 'data_17');".to_string(),
        "INSERT INTO my_keyspace.my_table (id, partition_key, clustering_key, data) VALUES (uuid(), 'my_partition', 18, 'data_18');".to_string(),
        "INSERT INTO my_keyspace.my_table (id, partition_key, clustering_key, data) VALUES (uuid(), 'my_partition', 19, 'data_19');".to_string(),
        "INSERT INTO my_keyspace.my_table (id, partition_key, clustering_key, data) VALUES (uuid(), 'my_partition', 20, 'data_20');".to_string(),
        //"INSERT INTO my_keyspace.my_table (id, partition_key, clustering_key, data) VALUES (uuid(), 'my_partition', 21, 'data_21');".to_string(),
        //"INSERT INTO my_keyspace.my_table (id, partition_key, clustering_key, data) VALUES (uuid(), 'my_partition', 22, 'data_22');".to_string(),
        //"INSERT INTO my_keyspace.my_table (id, partition_key, clustering_key, data) VALUES (uuid(), 'my_partition', 23, 'data_23');".to_string(),
         //"INSERT INTO my_keyspace.my_table (id, partition_key, clustering_key, data) VALUES (uuid(), 'my_partition', 24, 'data_24');".to_string(),
         //"INSERT INTO my_keyspace.my_table (id, partition_key, clustering_key, data) VALUES (uuid(), 'my_partition', 25, 'data_25');".to_string(),
        // "INSERT INTO my_keyspace.my_table (id, partition_key, clustering_key, data) VALUES (uuid(), 'my_partition', 26, 'data_26');".to_string(),
        // "INSERT INTO my_keyspace.my_table (id, partition_key, clustering_key, data) VALUES (uuid(), 'my_partition', 27, 'data_27');".to_string(),
        // "INSERT INTO my_keyspace.my_table (id, partition_key, clustering_key, data) VALUES (uuid(), 'my_partition', 28, 'data_28');".to_string(),
        // "INSERT INTO my_keyspace.my_table (id, partition_key, clustering_key, data) VALUES (uuid(), 'my_partition', 29, 'data_29');".to_string(),
    
        // Realizar un SELECT para consultar los datos
        "SELECT * FROM my_keyspace.my_table WHERE partition_key = 'my_partition';".to_string(),
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
