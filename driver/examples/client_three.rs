use driver::CassandraClient;
use std::{net::Ipv4Addr, str::FromStr};

fn main() {
    // Dirección IP del servidor Cassandra
    let server_ip = "127.0.0.4";
    let ip = Ipv4Addr::from_str(&server_ip).unwrap();

    // Conectarse al servidor Cassandra
    let mut client = CassandraClient::connect(ip).unwrap();
    client.startup().unwrap();

    // Definir el total de registros a seleccionar
    let total_selects = 100_000;
    for i in 1..=total_selects {
        let select_query = format!("SELECT * FROM simple_table WHERE id = {}", i);

        match client.execute(&select_query, "all") {
            Ok(result) => {
                println!("Query result for id {}: {:?}", i, result);
                if i % 1000 == 0 {
                    println!("Retrieved {} records successfully", i);
                }
            }
            Err(e) => {
                eprintln!("Error executing select query for id {}: {:?}", i, e);
                // Detener el bucle si hay un error
                break;
            }
        }
    }

    println!("Finished retrieving {} records", total_selects);
}
