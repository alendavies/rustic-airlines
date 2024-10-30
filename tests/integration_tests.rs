use driver::{CassandraClient, QueryResult};
use native_protocol::messages::result::result::Result;
use native_protocol::messages::result::schema_change;
use native_protocol::messages::result::schema_change::SchemaChange;
use std::process::{Child, Command};
use std::thread::sleep;
use std::time::Duration;
use std::{net::Ipv4Addr, str::FromStr};

// Función para lanzar un nodo dado una IP
fn launch_node(ip: &str) -> Child {
    Command::new("cargo")
        .arg("run")
        .current_dir("node_launcher") // Cambia a la carpeta correcta de node_launcher
        .arg("--")
        .arg(ip)
        .spawn()
        .expect("Failed to launch node")
}

// Función para ejecutar una consulta y verificar el tipo de resultado
fn execute_and_verify(
    client: &mut CassandraClient,
    query: &str,
    expected_result: QueryResult,
) -> bool {
    match client.execute(query) {
        Ok(query_result) => match (&expected_result, &query_result) {
            (
                QueryResult::Result(Result::SchemaChange(_)),
                QueryResult::Result(Result::SchemaChange(_)),
            ) => true,
            (QueryResult::Result(Result::Void), QueryResult::Result(Result::Void)) => true,
            (
                QueryResult::Result(Result::SetKeyspace(_)),
                QueryResult::Result(Result::SetKeyspace(_)),
            ) => true,
            (QueryResult::Error(_), QueryResult::Error(_)) => true,
            _ => false,
        },
        Err(e) => {
            eprintln!("Error executing query: {}\nError: {:?}", query, e);
            false
        }
    }
}

#[test]
fn test_integration_with_multiple_nodes() {
    // Lista de IPs para los nodos
    let ips = vec![
        "127.0.0.1",
        "127.0.0.2",
        "127.0.0.3",
        "127.0.0.4",
        "127.0.0.5",
    ];

    // Vector para almacenar los procesos de los nodos
    let mut children = vec![];

    // Lanzar cada nodo en un proceso separado
    for ip in &ips {
        sleep(Duration::from_secs(2)); // Pausa para asegurar que los nodos se inicien secuencialmente
        let child = launch_node(ip);
        children.push(child);
        println!("Node with IP {} started", ip);
    }

    // Dar tiempo para que los nodos inicialicen completamente
    sleep(Duration::from_secs(5));

    // Conectarse a uno de los nodos para enviar consultas
    let server_ip = "127.0.0.1";
    let ip = Ipv4Addr::from_str(&server_ip).unwrap();
    let mut client = CassandraClient::connect(ip).expect("Failed to connect to Cassandra client");
    client.startup().expect("Failed to start Cassandra client");

    // Ejecutar y verificar cada consulta individualmente

    // Crear un keyspace con replication_factor = 3
    let query = "CREATE KEYSPACE test_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}";
    let expected_result = QueryResult::Result(Result::SchemaChange(SchemaChange::new(
        schema_change::ChangeType::Created,
        schema_change::Target::Keyspace,
        schema_change::Options::new("test_keyspace".to_string(), None),
    )));
    assert!(
        execute_and_verify(&mut client, query, expected_result),
        "Query failed or did not match expected result: {}",
        query
    );
    println!("Query executed and matched expected result type: {}", query);

    // Alterar el keyspace para cambiar el replication_factor a 2
    let query = "ALTER KEYSPACE test_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}";
    let expected_result = QueryResult::Result(Result::SchemaChange(SchemaChange::new(
        schema_change::ChangeType::Updated,
        schema_change::Target::Keyspace,
        schema_change::Options::new("test_keyspace".to_string(), None),
    )));
    assert!(
        execute_and_verify(&mut client, query, expected_result),
        "Query failed or did not match expected result: {}",
        query
    );
    println!("Query executed and matched expected result type: {}", query);

    // Cambiar al keyspace "test_keyspace"
    let query = "USE test_keyspace";
    let expected_result = QueryResult::Result(Result::SetKeyspace("test_keyspace".to_string()));
    assert!(
        execute_and_verify(&mut client, query, expected_result),
        "Query failed or did not match expected result: {}",
        query
    );
    println!("Query executed and matched expected result type: {}", query);

    // // Eliminar el keyspace
    // let query = "DROP KEYSPACE test_keyspace";
    // let expected_result = QueryResult::Result(Result::SchemaChange(SchemaChange::new(
    //     schema_change::ChangeType::Dropped,
    //     schema_change::Target::Keyspace,
    //     schema_change::Options::new("test_keyspace".to_string(), None),
    // )));
    // assert!(
    //     execute_and_verify(&mut client, query, expected_result),
    //     "Query failed or did not match expected result: {}",
    //     query
    // );
    // println!("Query executed and matched expected result type: {}", query);

    // Esperar y finalizar los procesos de los nodos al terminar
    for mut child in children {
        let _ = child.kill(); // Termina el proceso del nodo
        let _ = child.wait(); // Espera a que el proceso termine
    }

    println!("Integration test completed successfully.");
}
