use driver::CassandraClient;
use std::{net::Ipv4Addr, str::FromStr};

fn main() {
    // Reemplaza con la dirección IP y puerto correctos del servidor
    let server_ip = "127.0.0.1";
    let ip = Ipv4Addr::from_str(&server_ip).unwrap();

    // Conectarse al servidor Cassandra
    let mut client = CassandraClient::connect(ip).unwrap();
    client.startup().unwrap();

    // Lista de consultas CQL para ejecutar
    let queries = vec![
        // Crear el keyspace
        "CREATE KEYSPACE world WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}",
        // // Crear la tabla
       
    "CREATE TABLE users (
        user_id INT,
        first_name TEXT,
        last_name TEXT,
        email TEXT,
        age INT,
        PRIMARY KEY (user_id, age)
        )",

        // // // Insertar algunos elementos
        "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (1, 'Michael', 'Brown', 'michael.brown@example.com', 40)",
        "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (1, 'Emily', 'Davis', 'emily.davis@example.com', 28)",
        "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (1, 'Negrazo', 'Davis', 'emily.davis@example.com', 28)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (789, 'Daniel', 'Garcia', 'daniel.garcia@example.com', 22)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (190, 'Sophia', 'Martinez', 'sophia.martinez@example.com', 31)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (345, 'James', 'Rodriguez', 'james.rodriguez@example.com', 45)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (876, 'Oliver', 'Martinez', 'oliver.martinez@example.com', 29)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (235, 'Lucas', 'Lopez', 'lucas.lopez@example.com', 27)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (567, 'Charlotte', 'Perez', 'charlotte.perez@example.com', 33)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (901, 'Amelia', 'Wilson', 'amelia.wilson@example.com', 26)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (128, 'Ethan', 'Anderson', 'ethan.anderson@example.com', 34)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (432, 'Mia', 'Taylor', 'mia.taylor@example.com', 24)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (654, 'Isabella', 'Thomas', 'isabella.thomas@example.com', 37)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (987, 'Liam', 'Hernandez', 'liam.hernandez@example.com', 41)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (3456, 'Benjamin', 'Moore', 'benjamin.moore@example.com', 30)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (6789, 'Elijah', 'Jackson', 'elijah.jackson@example.com', 36)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (1010, 'Ava', 'Martin', 'ava.martin@example.com', 39)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (1023, 'Noah', 'Lee', 'noah.lee@example.com', 27)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (1224, 'William', 'Kim', 'william.kim@example.com', 42)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (1456, 'Henry', 'Clark', 'henry.clark@example.com', 50)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (1789, 'Evelyn', 'Lewis', 'evelyn.lewis@example.com', 31)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (2001, 'Alexander', 'Walker', 'alexander.walker@example.com', 29)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (2323, 'Sebastian', 'Hall', 'sebastian.hall@example.com', 23)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (2789, 'Victoria', 'Allen', 'victoria.allen@example.com', 35)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (3010, 'Harper', 'Young', 'harper.young@example.com', 28)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (3457, 'Jack', 'King', 'jack.king@example.com', 32)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (4000, 'Samuel', 'Scott', 'samuel.scott@example.com', 33)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (4321, 'Layla', 'Green', 'layla.green@example.com', 30)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (4789, 'David', 'Baker', 'david.baker@example.com', 41)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (5002, 'Ella', 'Hill', 'ella.hill@example.com', 29)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (5320, 'Aiden', 'Rivera', 'aiden.rivera@example.com', 38)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (5789, 'Matthew', 'Carter', 'matthew.carter@example.com', 24)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (6011, 'Sophia', 'Mitchell', 'sophia.mitchell@example.com', 40)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (6325, 'Lucas', 'Perez', 'lucas.perez@example.com', 27)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (6789, 'Chloe', 'Adams', 'chloe.adams@example.com', 31)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (7010, 'Jacob', 'Nelson', 'jacob.nelson@example.com', 23)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (7321, 'Emily', 'Cox', 'emily.cox@example.com', 34)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (7890, 'Mason', 'Diaz', 'mason.diaz@example.com', 30)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (8001, 'Ella', 'Ward', 'ella.ward@example.com', 35)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (8432, 'Avery', 'Flores', 'avery.flores@example.com', 29)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (8900, 'Lily', 'Bennett', 'lily.bennett@example.com', 22)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (9123, 'Zoe', 'Brooks', 'zoe.brooks@example.com', 33)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (9320, 'Owen', 'Murphy', 'owen.murphy@example.com', 36)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (9678, 'Isabella', 'Rivera', 'isabella.rivera@example.com', 40)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (10011, 'Logan', 'Torres', 'logan.torres@example.com', 27)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (10543, 'Grace', 'Peterson', 'grace.peterson@example.com', 31)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (10989, 'Evelyn', 'Gray', 'evelyn.gray@example.com', 30)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (11567, 'Wyatt', 'Ramirez', 'wyatt.ramirez@example.com', 29)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (12001, 'Victoria', 'Hughes', 'victoria.hughes@example.com', 42)",
        // "INSERT INTO users (user_id, first_name, last_name, email, age) VALUES (12345, 'Levi', 'Nguyen', 'levi.nguyen@example.com', 34)",
        //  // // Borrar algunos elementos
        //  "DELETE FROM users WHERE user_id = 7",
        //  "DELETE FROM users WHERE user_id = 7",
        //  "SELECT email, user_id, age FROM users WHERE user_id = 16"
    ];

    // Ejecutar cada consulta en un loop
    let mut contador = 0;
    let len = queries.len();
    for query in queries {
        match client.execute(&query) {
            Ok(query_result) => {
                contador += 1;
                println!(
                    "Consulta ejecutada exitosamente: {} y el resultado fue {:?}",
                    query, query_result
                );
                println!("exitosas {:?}/{:?}", contador, len)
            }
            Err(e) => eprintln!("Error al ejecutar la consulta: {}\nError: {:?}", query, e),
        }
    }
}
