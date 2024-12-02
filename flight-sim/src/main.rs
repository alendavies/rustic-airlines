mod client;
mod types;

use crate::types::airport::Airport;
use crate::types::flight::Flight;
use crate::types::sim_state::SimState;
use chrono::Utc;
use client::Client;
use std::io::{self, Write};
use types::sim_error::SimError;

fn clean_scr() {
    print!("\x1B[2J\x1B[1;1H");
    io::stdout().flush().unwrap();
}

fn add_flight(sim_state: &mut SimState) -> Result<(), SimError> {
    clean_scr();
    let flight_number = prompt_input("Enter the flight number: ");
    let origin = prompt_input("Enter the origin IATA code: ");
    let destination = prompt_input("Enter the destination IATA code: ");
    let departure_time = prompt_input("Enter the departure time (DD-MM-YYYY HH:MM:SS): ");
    let arrival_time = prompt_input("Enter the arrival time (DD-MM-YYYY HH:MM:SS): ");

    let avg_speed_input = prompt_input("Enter the average speed (in km/h): ");
    let avg_speed: i32 = match avg_speed_input.parse() {
        Ok(speed) => speed,
        Err(_) => return Err(SimError::InvalidInput),
    };

    let flight = Flight::new_from_console(
        &sim_state.airports()?,
        &flight_number,
        &origin,
        &destination,
        &departure_time,
        &arrival_time,
        avg_speed,
    )
    .map_err(|_| SimError::InvalidFlight("Flight details are incorrect.".to_string()))?;

    sim_state.add_flight(flight)?;
    Ok(())
}

fn add_airport(sim_state: &mut SimState) -> Result<(), SimError> {
    clean_scr();
    let iata_code = prompt_input("Enter the IATA code: ");
    let country = prompt_input("Enter the country: ");
    let name = prompt_input("Enter the airport name: ");
    let latitude_input = prompt_input("Enter the latitude: ");
    let latitude: f64 = match latitude_input.parse() {
        Ok(lat) => lat,
        Err(_) => return Err(SimError::InvalidInput),
    };

    let longitude_input = prompt_input("Enter the longitude: ");
    let longitude: f64 = match longitude_input.parse() {
        Ok(lon) => lon,
        Err(_) => return Err(SimError::InvalidInput),
    };

    let airport = Airport::new(iata_code, country, name, latitude, longitude);
    sim_state.add_airport(airport)?;
    Ok(())
}

fn set_time_rate(sim_state: &mut SimState) -> Result<(), SimError> {
    let minutes_input = prompt_input("Enter the time rate (in minutes): ");
    let minutes: u64 = match minutes_input.parse() {
        Ok(m) => m,
        Err(_) => return Err(SimError::InvalidInput),
    };
    sim_state.set_time_rate(minutes);
    Ok(())
}
fn main() -> Result<(), SimError> {
    let ip = "127.0.0.1".parse().expect("Invalid IP format");
    let client = Client::new(ip).map_err(|_| SimError::ClientError)?;
    let mut sim_state = SimState::new(client)?;

    loop {
        println!("Enter command (type '-h' or '--help' for options): ");
        let mut command = String::new();
        io::stdin()
            .read_line(&mut command)
            .expect("Failed to read input");

        let args: Vec<&str> = command.trim().split_whitespace().collect();
        if args.is_empty() {
            continue;
        }

        match args[0] {
            "add-flight" => {
                if let Err(_) = add_flight(&mut sim_state) {
                    println!("{}", SimError::InvalidInput);
                }
            }

            "add-airport" => {
                if let Err(_) = add_airport(&mut sim_state) {
                    println!("{}", SimError::InvalidInput);
                }
            }

            "list-flights" => sim_state.list_flights(),

            "list-airports" => sim_state.list_airports(),

            "time-rate" => {
                clean_scr();
                if let Err(_) = set_time_rate(&mut sim_state) {
                    println!("{}", SimError::InvalidInput);
                }
            }

            "test-data" => {
                clean_scr();
                if let Err(_) = add_test_data(&mut sim_state) {
                    println!("{}", SimError::InvalidInput);
                }
            }

            "-h" | "help" => print_help(),

            "exit" => break,

            _ => eprintln!("Invalid command. Use -h for help."),
        }
    }

    sim_state.close_pool();
    Ok(())
}

fn prompt_input(prompt: &str) -> String {
    print!("{}", prompt);
    io::stdout().flush().unwrap();
    let mut input = String::new();
    io::stdin()
        .read_line(&mut input)
        .expect("Failed to read input");
    input.trim().to_string() // Remove any trailing newline or extra space
}

fn print_help() {
    clean_scr();
    println!("Available commands:");
    println!("  add-flight");
    println!("    Adds a new flight to the simulation. You'll be prompted for each detail.");
    println!("  add-airport");
    println!("    Adds a new airport. You'll be prompted for each detail.");
    println!("  list-flights");
    println!("    Show the current flights.");
    println!("  list-airports");
    println!("    Show the current airports.");
    println!("  time-rate");
    println!("    Changes the simulation's elapsed time per tick.");
    println!("  test-data");
    println!("    Adds four airports and four flights to the simulation.");
    println!("  exit");
    println!("    Closes this application.");
}

fn add_test_data(sim_state: &mut SimState) -> Result<(), SimError> {
    // List of airports in Argentina
    let airports = vec![
        ("AEP", "ARG", "Aeroparque Jorge Newbery", -34.553, -58.413),
        (
            "EZE",
            "ARG",
            "Aeropuerto Internacional Ministro Pistarini",
            -34.822,
            -58.535,
        ),
        ("MDZ", "ARG", "Aeropuerto El Plumerillo", -32.883, -68.845),
        (
            "COR",
            "ARG",
            "Aeropuerto Internacional Ingeniero Aeronáutico Ambrosio Taravella",
            -31.321,
            -64.213,
        ),
        (
            "ROS",
            "ARG",
            "Aeropuerto Internacional Rosario",
            -32.948,
            -60.787,
        ),
        (
            "BRC",
            "ARG",
            "Aeropuerto Internacional Teniente Luis Candelaria",
            -41.151,
            -71.158,
        ),
        (
            "USH",
            "ARG",
            "Aeropuerto Internacional Malvinas Argentinas",
            -54.843,
            -68.295,
        ),
        (
            "FTE",
            "ARG",
            "Aeropuerto Internacional Comandante Armando Tola",
            -50.280,
            -72.053,
        ),
        (
            "REL",
            "ARG",
            "Aeropuerto Internacional Almirante Marcos A. Zar",
            -43.211,
            -65.270,
        ),
        (
            "CRD",
            "ARG",
            "Aeropuerto Internacional General Enrique Mosconi",
            -45.785,
            -67.465,
        ),
        (
            "NQN",
            "ARG",
            "Aeropuerto Presidente Perón",
            -38.949,
            -68.156,
        ),
        (
            "SLA",
            "ARG",
            "Aeropuerto Internacional Martín Miguel de Güemes",
            -24.854,
            -65.486,
        ),
        (
            "JUJ",
            "ARG",
            "Aeropuerto Internacional Gobernador Horacio Guzmán",
            -24.392,
            -65.097,
        ),
        (
            "TUC",
            "ARG",
            "Aeropuerto Internacional Teniente Benjamín Matienzo",
            -26.842,
            -65.104,
        ),
        (
            "CNQ",
            "ARG",
            "Aeropuerto Internacional Doctor Fernando Piragine Niveyro",
            -27.445,
            -58.762,
        ),
        (
            "RES",
            "ARG",
            "Aeropuerto Internacional Resistencia",
            -27.450,
            -59.056,
        ),
        (
            "PSS",
            "ARG",
            "Aeropuerto Internacional Libertador General José de San Martín",
            -27.385,
            -55.970,
        ),
        (
            "RGL",
            "ARG",
            "Aeropuerto Internacional Piloto Civil Norberto Fernández",
            -51.609,
            -69.312,
        ),
        (
            "CTC",
            "ARG",
            "Aeropuerto Coronel Felipe Varela",
            -28.448,
            -65.780,
        ),
        (
            "RIA",
            "ARG",
            "Aeropuerto Internacional Termas de Río Hondo",
            -27.486,
            -64.935,
        ),
    ];

    // Add airports
    for airport in airports {
        let (iata_code, country, name, latitude, longitude) = airport;
        let airport = Airport::new(
            iata_code.to_string(),
            country.to_string(),
            name.to_string(),
            latitude,
            longitude,
        );
        sim_state.add_airport(airport)?;
    }

    // Add flights
    let today = Utc::now().naive_utc();
    let yesterday = today - chrono::Duration::days(1);
    let tomorrow = today + chrono::Duration::days(1);

    let flight_data = vec![
        (
            "AR1234",
            "AEP",
            "MDZ",
            yesterday,
            yesterday + chrono::Duration::hours(2),
            550,
        ),
        (
            "AR5678",
            "MDZ",
            "AEP",
            today,
            today + chrono::Duration::hours(2),
            550,
        ),
        (
            "AR9101",
            "EZE",
            "BRC",
            today,
            today + chrono::Duration::hours(3),
            600,
        ),
        (
            "AR1122",
            "BRC",
            "EZE",
            tomorrow,
            tomorrow + chrono::Duration::hours(3),
            600,
        ),
        (
            "AR2233",
            "COR",
            "USH",
            yesterday,
            yesterday + chrono::Duration::hours(4),
            700,
        ),
        (
            "AR3344",
            "USH",
            "COR",
            today,
            today + chrono::Duration::hours(4),
            700,
        ),
        (
            "AR4455",
            "FTE",
            "REL",
            today,
            today + chrono::Duration::hours(2),
            400,
        ),
        (
            "AR5566",
            "REL",
            "FTE",
            tomorrow,
            tomorrow + chrono::Duration::hours(2),
            400,
        ),
        (
            "AR6677",
            "CRD",
            "NQN",
            yesterday,
            yesterday + chrono::Duration::hours(2),
            500,
        ),
        (
            "AR7788",
            "NQN",
            "CRD",
            today,
            today + chrono::Duration::hours(2),
            500,
        ),
        (
            "AR8899",
            "SLA",
            "JUJ",
            today,
            today + chrono::Duration::minutes(45),
            300,
        ),
        (
            "AR9900",
            "JUJ",
            "SLA",
            tomorrow,
            tomorrow + chrono::Duration::minutes(45),
            300,
        ),
        (
            "AR1011",
            "TUC",
            "CNQ",
            yesterday,
            yesterday + chrono::Duration::hours(3),
            650,
        ),
        (
            "AR1212",
            "CNQ",
            "TUC",
            today,
            today + chrono::Duration::hours(3),
            650,
        ),
        (
            "AR1313",
            "RES",
            "PSS",
            today,
            today + chrono::Duration::hours(2),
            450,
        ),
        (
            "AR1414",
            "PSS",
            "RES",
            tomorrow,
            tomorrow + chrono::Duration::hours(2),
            450,
        ),
        (
            "AR1515",
            "RGL",
            "CTC",
            yesterday,
            yesterday + chrono::Duration::hours(4),
            700,
        ),
        (
            "AR1616",
            "CTC",
            "RGL",
            today,
            today + chrono::Duration::hours(4),
            700,
        ),
        (
            "AR1717",
            "RIA",
            "AEP",
            today,
            today + chrono::Duration::hours(3),
            500,
        ),
        (
            "AR1818",
            "AEP",
            "RIA",
            tomorrow,
            tomorrow + chrono::Duration::hours(2),
            500,
        ),
        (
            "AR1920",
            "EZE",
            "ROS",
            today,
            today + chrono::Duration::hours(2),
            550,
        ),
        (
            "AR2021",
            "ROS",
            "EZE",
            tomorrow,
            tomorrow + chrono::Duration::hours(2),
            550,
        ),
        (
            "AR2122",
            "NQN",
            "AEP",
            yesterday,
            yesterday + chrono::Duration::hours(3),
            450,
        ),
        (
            "AR2223",
            "AEP",
            "NQN",
            today,
            today + chrono::Duration::hours(3),
            450,
        ),
        (
            "AR2324",
            "COR",
            "MDZ",
            tomorrow,
            tomorrow + chrono::Duration::hours(2),
            500,
        ),
        (
            "AR2425",
            "MDZ",
            "COR",
            today,
            today + chrono::Duration::hours(2),
            500,
        ),
    ];

    for (flight_number, origin, destination, departure_time, arrival_time, avg_speed) in flight_data
    {
        let departure_str = departure_time.format("%d-%m-%Y %H:%M:%S").to_string();
        let arrival_str = arrival_time.format("%d-%m-%Y %H:%M:%S").to_string();
        let flight = Flight::new_from_console(
            &sim_state.airports()?,
            flight_number,
            origin,
            destination,
            &departure_str,
            &arrival_str,
            avg_speed,
        )
        .map_err(|_| SimError::Other("Error".to_string()))?;

        sim_state.add_flight(flight)?;
    }

    println!("Test data added successfully!");
    Ok(())
}

// fn add_test_data(sim_state: &mut SimState) -> Result<(), SimError> {
//     use rand::Rng; // Asegúrate de usar `rand::Rng` para generación de números aleatorios.

//     // List of airports in Latin America with 20 airports per country
//     let airports = vec![
//         // Argentina
//         ("AEP", "ARG", "Aeroparque Jorge Newbery", -34.553, -58.413),
//         (
//             "EZE",
//             "ARG",
//             "Aeropuerto Internacional Ministro Pistarini",
//             -34.822,
//             -58.535,
//         ),
//         (
//             "COR",
//             "ARG",
//             "Aeropuerto Internacional Ingeniero Aeronáutico Ambrosio Taravella",
//             -31.321,
//             -64.213,
//         ),
//         (
//             "ROS",
//             "ARG",
//             "Aeropuerto Internacional Rosario",
//             -32.948,
//             -60.787,
//         ),
//         (
//             "MDZ",
//             "ARG",
//             "Aeropuerto Internacional El Plumerillo",
//             -32.883,
//             -68.845,
//         ),
//         (
//             "BRC",
//             "ARG",
//             "Aeropuerto Internacional Teniente Luis Candelaria",
//             -41.151,
//             -71.158,
//         ),
//         (
//             "USH",
//             "ARG",
//             "Aeropuerto Internacional Malvinas Argentinas",
//             -54.843,
//             -68.295,
//         ),
//         (
//             "FTE",
//             "ARG",
//             "Aeropuerto Internacional Comandante Armando Tola",
//             -50.280,
//             -72.053,
//         ),
//         (
//             "REL",
//             "ARG",
//             "Aeropuerto Internacional Almirante Marcos A. Zar",
//             -43.211,
//             -65.270,
//         ),
//         (
//             "CRD",
//             "ARG",
//             "Aeropuerto Internacional General Enrique Mosconi",
//             -45.785,
//             -67.465,
//         ),
//         (
//             "NQN",
//             "ARG",
//             "Aeropuerto Presidente Perón",
//             -38.949,
//             -68.156,
//         ),
//         (
//             "SLA",
//             "ARG",
//             "Aeropuerto Internacional Martín Miguel de Güemes",
//             -24.854,
//             -65.486,
//         ),
//         (
//             "JUJ",
//             "ARG",
//             "Aeropuerto Internacional Gobernador Horacio Guzmán",
//             -24.392,
//             -65.097,
//         ),
//         (
//             "TUC",
//             "ARG",
//             "Aeropuerto Internacional Teniente Benjamín Matienzo",
//             -26.842,
//             -65.104,
//         ),
//         (
//             "CNQ",
//             "ARG",
//             "Aeropuerto Internacional Doctor Fernando Piragine Niveyro",
//             -27.445,
//             -58.762,
//         ),
//         (
//             "RES",
//             "ARG",
//             "Aeropuerto Internacional Resistencia",
//             -27.450,
//             -59.056,
//         ),
//         (
//             "PSS",
//             "ARG",
//             "Aeropuerto Internacional Libertador General José de San Martín",
//             -27.385,
//             -55.970,
//         ),
//         (
//             "RGL",
//             "ARG",
//             "Aeropuerto Internacional Piloto Civil Norberto Fernández",
//             -51.609,
//             -69.312,
//         ),
//         (
//             "CTC",
//             "ARG",
//             "Aeropuerto Coronel Felipe Varela",
//             -28.448,
//             -65.780,
//         ),
//         (
//             "VDM",
//             "ARG",
//             "Aeropuerto Gobernador Castello",
//             -40.868,
//             -63.000,
//         ),
//         // Brazil
//         (
//             "GRU",
//             "BRA",
//             "Aeroporto Internacional de São Paulo/Guarulhos",
//             -23.435,
//             -46.473,
//         ),
//         (
//             "GIG",
//             "BRA",
//             "Aeroporto Internacional do Rio de Janeiro/Galeão",
//             -22.811,
//             -43.250,
//         ),
//         (
//             "BSB",
//             "BRA",
//             "Aeroporto Internacional de Brasília",
//             -15.871,
//             -47.918,
//         ),
//         ("CGH", "BRA", "Aeroporto de Congonhas", -23.626, -46.656),
//         (
//             "SSA",
//             "BRA",
//             "Aeroporto Internacional de Salvador",
//             -12.908,
//             -38.322,
//         ),
//         (
//             "POA",
//             "BRA",
//             "Aeroporto Internacional Salgado Filho",
//             -29.994,
//             -51.171,
//         ),
//         (
//             "CWB",
//             "BRA",
//             "Aeroporto Internacional Afonso Pena",
//             -25.532,
//             -49.175,
//         ),
//         (
//             "FLN",
//             "BRA",
//             "Aeroporto Internacional Hercílio Luz",
//             -27.670,
//             -48.547,
//         ),
//         (
//             "REC",
//             "BRA",
//             "Aeroporto Internacional dos Guararapes",
//             -8.126,
//             -34.924,
//         ),
//         (
//             "BEL",
//             "BRA",
//             "Aeroporto Internacional de Belém",
//             -1.383,
//             -48.481,
//         ),
//         (
//             "FOR",
//             "BRA",
//             "Aeroporto Internacional Pinto Martins",
//             -3.777,
//             -38.532,
//         ),
//         (
//             "NAT",
//             "BRA",
//             "Aeroporto Internacional Aluízio Alves",
//             -5.911,
//             -35.247,
//         ),
//         (
//             "MAO",
//             "BRA",
//             "Aeroporto Internacional Eduardo Gomes",
//             -3.036,
//             -60.049,
//         ),
//         (
//             "VCP",
//             "BRA",
//             "Aeroporto Internacional de Viracopos",
//             -23.008,
//             -47.137,
//         ),
//         (
//             "MCZ",
//             "BRA",
//             "Aeroporto Internacional Zumbi dos Palmares",
//             -9.510,
//             -35.793,
//         ),
//         (
//             "JPA",
//             "BRA",
//             "Aeroporto Internacional Presidente Castro Pinto",
//             -7.145,
//             -34.949,
//         ),
//         (
//             "GRH",
//             "BRA",
//             "Aeroporto Internacional de Guararapes",
//             -8.126,
//             -34.923,
//         ),
//         (
//             "LDB",
//             "BRA",
//             "Aeroporto Governador José Richa",
//             -23.333,
//             -51.135,
//         ),
//         (
//             "SLZ",
//             "BRA",
//             "Aeroporto Internacional Marechal Cunha Machado",
//             -2.584,
//             -44.236,
//         ),
//         (
//             "MCZ",
//             "BRA",
//             "Aeroporto Internacional Zumbi dos Palmares",
//             -9.509,
//             -35.793,
//         ),
//         // Chile
//         (
//             "SCL",
//             "CHL",
//             "Aeropuerto Internacional Comodoro Arturo Merino Benítez",
//             -33.391,
//             -70.785,
//         ),
//         (
//             "ANF",
//             "CHL",
//             "Aeropuerto Andrés Sabella Gálvez",
//             -23.445,
//             -70.445,
//         ),
//         (
//             "PUQ",
//             "CHL",
//             "Aeropuerto Presidente Carlos Ibáñez del Campo",
//             -53.002,
//             -70.845,
//         ),
//         ("PMC", "CHL", "Aeropuerto El Tepual", -41.438, -73.094),
//         ("CJC", "CHL", "Aeropuerto El Loa", -22.497, -68.903),
//         ("LSC", "CHL", "Aeropuerto La Florida", -29.916, -71.199),
//         (
//             "ARI",
//             "CHL",
//             "Aeropuerto Internacional Chacalluta",
//             -18.348,
//             -70.338,
//         ),
//         (
//             "IQQ",
//             "CHL",
//             "Aeropuerto Internacional Diego Aracena",
//             -20.535,
//             -70.181,
//         ),
//         ("ZAL", "CHL", "Aeropuerto Pichoy", -39.650, -73.086),
//         (
//             "CCP",
//             "CHL",
//             "Aeropuerto Internacional Carriel Sur",
//             -36.772,
//             -73.063,
//         ),
//         (
//             "WPR",
//             "CHL",
//             "Aeropuerto Guardia Marina Zañartu",
//             -54.931,
//             -67.626,
//         ),
//         ("LSQ", "CHL", "Aeropuerto Mocopulli", -42.338, -73.719),
//         ("GXQ", "CHL", "Aeropuerto Teniente Vidal", -45.594, -72.106),
//         ("ZCO", "CHL", "Aeropuerto La Araucanía", -38.766, -72.637),
//         (
//             "PNT",
//             "CHL",
//             "Aeropuerto Teniente Gallardo",
//             -51.671,
//             -72.528,
//         ),
//         ("FFU", "CHL", "Aeropuerto Futaleufú", -43.184, -71.851),
//         (
//             "SOD",
//             "CHL",
//             "Aeropuerto Domingo Faustino Sarmiento",
//             -39.626,
//             -71.915,
//         ),
//         ("SWB", "CHL", "Aeropuerto Rodelillo", -32.951, -71.488),
//         ("TOQ", "CHL", "Aeropuerto Barriles", -22.136, -70.063),
//     ];

//     // Agregar aeropuertos
//     for (iata_code, country, name, latitude, longitude) in &airports {
//         let airport = Airport::new(
//             iata_code.to_string(),
//             country.to_string(),
//             name.to_string(),
//             *latitude,
//             *longitude,
//         );
//         sim_state.add_airport(airport)?;
//     }

//     // Generar datos de vuelos
//     let today = Utc::now().naive_utc();
//     // let yesterday = today - chrono::Duration::days(1);
//     // let tomorrow = today + chrono::Duration::days(1);

//     let mut rng = rand::thread_rng(); // Crear un generador de números aleatorios
//     let mut flight_data = Vec::new();

//     for (origin, _, _, _, _) in &airports {
//         let flight_count = rng.gen_range(5..=10); // Generar entre 5 y 10 vuelos por aeropuerto
//         for _ in 0..flight_count {
//             let destination_index = rng.gen_range(0..airports.len());
//             let destination = airports[destination_index].0;

//             // Evitar vuelos con el mismo origen y destino
//             if origin != &destination {
//                 let departure_time = today;

//                 let duration_hours = rng.gen_range(1..=6); // Duración de vuelo entre 1 y 6 horas
//                 let arrival_time = departure_time + chrono::Duration::hours(duration_hours as i64);

//                 let flight_number = format!("{}{:04}", origin, rng.gen_range(1000..9999));
//                 let avg_speed = rng.gen_range(400..=600); // Velocidad promedio entre 400 y 600 km/h

//                 flight_data.push((
//                     flight_number,
//                     origin.to_string(),
//                     destination.to_string(),
//                     departure_time,
//                     arrival_time,
//                     avg_speed,
//                 ));
//             }
//         }
//     }

//     // Agregar vuelos al estado de simulación
//     for (flight_number, origin, destination, departure_time, arrival_time, avg_speed) in flight_data
//     {
//         let departure_str = departure_time.format("%d-%m-%Y %H:%M:%S").to_string();
//         let arrival_str = arrival_time.format("%d-%m-%Y %H:%M:%S").to_string();

//         let flight = Flight::new_from_console(
//             &sim_state.airports()?,
//             &flight_number,
//             &origin,
//             &destination,
//             &departure_str,
//             &arrival_str,
//             avg_speed,
//         )
//         .map_err(|_| SimError::Other("Error al crear el vuelo".to_string()))?;

//         sim_state.add_flight(flight)?;
//     }

//     println!("Test data added successfully!");
//     Ok(())
// }
