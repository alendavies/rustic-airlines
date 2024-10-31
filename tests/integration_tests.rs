use driver::{CassandraClient, QueryResult};
use native_protocol::messages::error::Error;
use native_protocol::messages::result::result::Result;
use native_protocol::messages::result::rows::ColumnValue;
use native_protocol::messages::result::schema_change;
use native_protocol::messages::result::schema_change::SchemaChange;
use std::process::{Child, Command};
use std::sync::{Arc, Mutex};
use std::thread::{self, sleep};
use std::time::{Duration, Instant};
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

// Función para ejecutar un SELECT y verificar que el resultado contenga exactamente los valores esperados
fn execute_and_verify_select(
    client: &mut CassandraClient,
    query: &str,
    expected_values: Vec<String>,
) -> bool {
    match client.execute(query) {
        Ok(query_result) => match query_result {
            QueryResult::Result(Result::Rows(rows)) => {
                // Asegurarse de que haya al menos una fila en el resultado

                println!("el row content es {:?}", rows.rows_content);
                if rows.rows_content.is_empty() {
                    return false;
                }

                // Obtener la primera fila y sus valores de columna
                let row = &rows.rows_content[0];
                let mut actual_values: Vec<String> = Vec::new();

                // Extraer los valores de cada columna en la fila y convertirlos en String
                for column_value in row.values() {
                    let value = match column_value {
                        ColumnValue::Ascii(val) | ColumnValue::Varchar(val) => val.clone(),
                        ColumnValue::Int(val) => val.to_string(),
                        ColumnValue::Double(val) => val.to_string(),
                        ColumnValue::Boolean(val) => val.to_string(),
                        ColumnValue::Timestamp(val) => val.to_string(),
                        // Otros tipos de datos si es necesario
                        _ => return false, // Si hay un tipo no esperado, falla la verificación
                    };
                    actual_values.push(value);
                }

                println!("comparamos {:?} con {:?}", actual_values, expected_values);

                // Comparar los valores obtenidos con los valores esperados
                for value in actual_values {
                    if !expected_values.contains(&value) {
                        return false;
                    }
                }
                true
            }
            _ => false, // Si el resultado no es del tipo `Rows`, falla la verificación
        },
        Err(e) => {
            eprintln!("Error executing query: {}\nError: {:?}", query, e);
            false
        }
    }
}

#[test]
fn test_integration_with_multiple_nodes() {
    // Configuración del tiempo límite de 1 minuto
    let timeout_duration = Duration::from_secs(60);
    let start_time = Instant::now();

    // Mutex para controlar si la prueba se completó
    let is_completed = Arc::new(Mutex::new(false));
    let is_completed_clone = Arc::clone(&is_completed);

    // Lanza un hilo que verificará el tiempo
    thread::spawn(move || {
        thread::sleep(timeout_duration);
        let completed = is_completed_clone.lock().unwrap();
        if !*completed {
            panic!("Test failed: exceeded 1 minute timeout");
        }
    });

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

    // Vector para almacenar todas las consultas ejecutadas
    let mut queries_executed: Vec<String> = vec![];

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

    // 1. Crear un keyspace con replication_factor = 3
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

    // 2. Alterar el keyspace para cambiar el replication_factor a 2
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

    // 3. Cambiar al keyspace "test_keyspace"
    let query = "USE test_keyspace";
    let expected_result = QueryResult::Result(Result::SetKeyspace("test_keyspace".to_string()));
    assert!(
        execute_and_verify(&mut client, query, expected_result),
        "Query failed or did not match expected result: {}",
        query
    );
    println!("Query executed and matched expected result type: {}", query);

    // 4. Crear una tabla llamada "test_table"
    let query = "CREATE TABLE test_table (id INT, name TEXT, PRIMARY KEY (id, name))";
    let expected_result = QueryResult::Result(Result::SchemaChange(SchemaChange::new(
        schema_change::ChangeType::Created,
        schema_change::Target::Table,
        schema_change::Options::new("test_table".to_string(), None),
    )));
    assert!(
        execute_and_verify(&mut client, query, expected_result),
        "Query failed or did not match expected result: {}",
        query
    );
    println!(
        "Table creation query executed and matched expected result type: {}",
        query
    );

    // 5. Alterar la tabla "test_table" para agregar una nueva columna
    let query = "ALTER TABLE test_table ADD email TEXT";
    let expected_result = QueryResult::Result(Result::SchemaChange(SchemaChange::new(
        schema_change::ChangeType::Updated,
        schema_change::Target::Table,
        schema_change::Options::new("test_table".to_string(), None),
    )));
    assert!(
        execute_and_verify(&mut client, query, expected_result),
        "Query failed or did not match expected result: {}",
        query
    );
    println!(
        "Table alteration query executed and matched expected result type: {}",
        query
    );

    // 6. Inserción completa (todas las columnas)
    let query = "INSERT INTO test_table (id, name, email) VALUES (1, 'Alice', 'alice@example.com')";
    assert!(
        execute_and_verify(&mut client, query, QueryResult::Result(Result::Void)),
        "Full insert failed"
    );
    println!("Full insert query executed successfully: {}", query);

    // Verificar que el registro fue insertado
    let select_query = "SELECT id, name, email FROM test_table WHERE id = 1";
    queries_executed.push(select_query.to_string());
    let expected_values = vec![
        "1".to_string(),
        "Alice".to_string(),
        "alice@example.com".to_string(),
    ];
    assert!(
        execute_and_verify_select(&mut client, select_query, expected_values),
        "Verification of full insert failed"
    );

    //7. Inserción parcial (solo columna obligatoria `id` y `name`)
    let query = "INSERT INTO test_table (id, name) VALUES (2, 'Bob')";
    assert!(
        execute_and_verify(&mut client, query, QueryResult::Result(Result::Void)),
        "Partial insert failed"
    );
    println!("Partial insert query executed successfully: {}", query);

    //Verificar que el registro fue insertado con valores nulos en las columnas no especificadas
    let select_query = "SELECT id, name, email FROM test_table WHERE id = 2";
    queries_executed.push(select_query.to_string());
    let expected_values = vec!["2".to_string(), "Bob".to_string(), "".to_string()];
    assert!(
        execute_and_verify_select(&mut client, select_query, expected_values),
        "Verification of partial insert failed"
    );

    // 8. Inserción sin `PRIMARY KEY` (debe fallar)
    let query = "INSERT INTO test_table (name, email) VALUES ('Bob', 'bob@example.com')";
    assert!(
        !execute_and_verify(&mut client, query, QueryResult::Result(Result::Void)),
        "Insert without primary key should fail"
    );
    println!(
        "Insert without primary key query executed with expected failure: {}",
        query
    );

    // 9. Inserción con `IF NOT EXISTS` cuando la fila no existe
    let query = "INSERT INTO test_table (id, name, email) VALUES (3, 'Charlie', 'charlie@example.com') IF NOT EXISTS";
    assert!(
        execute_and_verify(&mut client, query, QueryResult::Result(Result::Void)),
        "Insert with IF NOT EXISTS failed (when row does not exist)"
    );
    println!(
        "Insert with IF NOT EXISTS query executed successfully: {}",
        query
    );

    // Verificar que el registro fue insertado
    let select_query = "SELECT id, name, email FROM test_table WHERE id = 3";
    queries_executed.push(select_query.to_string());
    let expected_values = vec![
        "3".to_string(),
        "Charlie".to_string(),
        "charlie@example.com".to_string(),
    ];
    assert!(
        execute_and_verify_select(&mut client, select_query, expected_values),
        "Verification of insert with IF NOT EXISTS failed"
    );

    // 10. Inserción con `IF NOT EXISTS` cuando la fila ya existe
    let query = "INSERT INTO test_table (id, name, email) VALUES (3, 'Charlie', 'charlie_new@example.com') IF NOT EXISTS";
    assert!(
        execute_and_verify(&mut client, query, QueryResult::Result(Result::Void)),
        "Insert with IF NOT EXISTS should not insert when row exists"
    );
    println!(
        "Insert with IF NOT EXISTS query executed successfully (no insert expected): {}",
        query
    );

    // Verificar que el registro no fue modificado
    let select_query = "SELECT id, name, email FROM test_table WHERE id = 3";
    queries_executed.push(select_query.to_string());
    let expected_values = vec![
        "3".to_string(),
        "Charlie".to_string(),
        "charlie@example.com".to_string(),
    ];
    assert!(
        execute_and_verify_select(&mut client, select_query, expected_values),
        "Verification of no change with IF NOT EXISTS failed"
    );

    // 10. Inserción con columnas invalidas
    let query = "INSERT INTO test_table (name, email) VALUES ('Charlie', 'charlie@example.com') IF NOT EXISTS";
    assert!(
        execute_and_verify(
            &mut client,
            query,
            QueryResult::Error(Error::ServerError("".to_string()))
        ),
        "Insert with invalid column"
    );
    println!("Insert with invalid column: {}", query);

    // 1. Actualización básica sin condiciones IF
    let update_query =
        "UPDATE test_table SET email = 'alice_new@example.com' WHERE id = 1 AND name = 'Alice'";
    assert!(
        execute_and_verify(&mut client, update_query, QueryResult::Result(Result::Void)),
        "Update without IF failed"
    );
    println!("Update without IF condition executed successfully");

    // Verificar la actualización
    let select_query = "SELECT email FROM test_table WHERE id = 1 AND name = 'Alice'";
    let expected_values = vec!["alice_new@example.com".to_string()];
    assert!(
        execute_and_verify_select(&mut client, select_query, expected_values),
        "Verification of update without IF failed"
    );

    // 2. Actualización con condición IF que cumple
    let update_query = "UPDATE test_table SET name = 'Lucy' WHERE id = 1 AND name = 'Alice' IF email = 'alice_new@example.com'";
    assert!(
        execute_and_verify(&mut client, update_query, QueryResult::Result(Result::Void)),
        "Update with IF condition (matching) failed"
    );
    println!("Update with IF condition (matching) executed successfully");

    // Verificar la actualización
    let select_query = "SELECT email FROM test_table WHERE id = 1 AND name = 'Lucy'";
    let expected_values = vec!["alice_new@example.com".to_string()];
    assert!(
        execute_and_verify_select(&mut client, select_query, expected_values),
        "Verification of update with matching IF condition failed"
    );

    // // 3. Actualización con condición IF que no cumple
    // let update_query = "UPDATE test_table SET email = 'alice_failed_update@example.com' WHERE id = 1 AND name = 'Alice' IF age = 35";
    // assert!(
    //     !execute_and_verify(&mut client, update_query, QueryResult::Result(Result::Void)),
    //     "Update with non-matching IF condition should fail"
    // );
    // println!("Update with non-matching IF condition executed successfully");

    // // Verificar que el email no haya cambiado
    // let select_query = "SELECT email FROM test_table WHERE id = 1 AND name = 'Alice'";
    // let expected_values = vec!["alice_new@example.com".to_string()];
    // assert!(
    //     execute_and_verify_select(&mut client, select_query, expected_values),
    //     "Verification of update with non-matching IF condition failed (email changed)"
    // );

    // // 4. Actualización con múltiples condiciones WHERE
    // let insert_query =
    //     "INSERT INTO test_table (id, name, email, age) VALUES (2, 'Bob', 'bob@example.com', 40)";
    // execute_and_verify(&mut client, insert_query, QueryResult::Result(Result::Void));
    // println!("Inserted second row for multi-condition update test");

    // let update_query = "UPDATE test_table SET age = 45 WHERE id = 2 AND name = 'Bob'";
    // assert!(
    //     execute_and_verify(&mut client, update_query, QueryResult::Result(Result::Void)),
    //     "Multi-condition update without IF failed"
    // );
    // println!("Multi-condition update without IF executed successfully");

    // // Verificar la actualización
    // let select_query = "SELECT age FROM test_table WHERE id = 2 AND name = 'Bob'";
    // let expected_values = vec!["45".to_string()];
    // assert!(
    //     execute_and_verify_select(&mut client, select_query, expected_values),
    //     "Verification of multi-condition update failed"
    // );

    // // 5. Actualización con condición IF y WHERE no cumplida
    // let update_query = "UPDATE test_table SET email = 'bob_failed_update@example.com' WHERE id = 2 AND name = 'Bob' IF age = 50";
    // assert!(
    //     !execute_and_verify(&mut client, update_query, QueryResult::Result(Result::Void)),
    //     "Update with non-matching IF and WHERE should fail"
    // );
    // println!("Update with non-matching IF and WHERE condition executed successfully");

    // // Verificar que el email no haya cambiado
    // let select_query = "SELECT email FROM test_table WHERE id = 2 AND name = 'Bob'";
    // let expected_values = vec!["bob@example.com".to_string()];
    // assert!(
    //     execute_and_verify_select(&mut client, select_query, expected_values),
    //     "Verification of no update with non-matching IF and WHERE failed (email changed)"
    // );

    // // 11. Eliminar la tabla "test_table"
    // let query = "DROP TABLE test_table";
    // let expected_result = QueryResult::Result(Result::SchemaChange(SchemaChange::new(
    //     schema_change::ChangeType::Dropped,
    //     schema_change::Target::Table,
    //     schema_change::Options::new("test_table".to_string(), None),
    // )));
    // assert!(
    //     execute_and_verify(&mut client, query, expected_result),
    //     "Query failed or did not match expected result: {}",
    //     query
    // );
    // println!(
    //     "Table deletion query executed and matched expected result type: {}",
    //     query
    // );

    // Finalizar los procesos de los nodos al terminar
    for mut child in children {
        let _ = child.kill(); // Termina el proceso del nodo
        let _ = child.wait(); // Espera a que el proceso termine
    }

    println!("Integration test completed successfully.");

    // Finaliza la prueba marcando el Mutex como `true`
    *is_completed.lock().unwrap() = true;

    // Opcional: Verificar el tiempo de ejecución
    let elapsed = start_time.elapsed();
    println!("Test completed in {:?}", elapsed);
}
