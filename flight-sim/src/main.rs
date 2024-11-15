mod types;
mod client;

use crate::types::sim_state::SimState;
use crate::types::flight::Flight;
use crate::types::airport::Airport;
use chrono::Utc;
use client::Client;
use types::sim_error::SimError;
use std::io::{self, Write};

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
    let avg_speed: f64 = match avg_speed_input.parse() {
        Ok(speed) => speed,
        Err(_) => return Err(SimError::InvalidInput),
    };

    let flight = Flight::new_from_console(
        &sim_state.airports(), &flight_number, &origin, &destination, &departure_time, &arrival_time, avg_speed
    ).map_err(|_| SimError::InvalidFlight("Flight details are incorrect.".to_string()))?;

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
        io::stdin().read_line(&mut command).expect("Failed to read input");

        let args: Vec<&str> = command.trim().split_whitespace().collect();
        if args.is_empty() { continue; }

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
    io::stdin().read_line(&mut input).expect("Failed to read input");
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
    println!("  time-rate");
    println!("    Changes the simulation's elapsed time per tick.");
    println!("  exit");
    println!("    Closes this application.");
}


fn add_test_data(sim_state: &mut SimState) -> Result<(), SimError> {
    // List of airports in Argentina
    let airports = vec![
        ("AEP", "Argentina", "Aeroparque Jorge Newbery", -34.553, -58.413),
        ("EZE", "Argentina", "Aeropuerto Internacional Ministro Pistarini", -34.822, -58.535),
        ("MDZ", "Argentina", "Aeropuerto El Plumerillo", -32.883, -68.845),
        ("COR", "Argentina", "Aeropuerto Internacional Ingeniero Aeronáutico Ambrosio Taravella", -31.321, -64.213),
        ("ROS", "Argentina", "Aeropuerto Internacional Rosario", -32.948, -60.787),
    ];

    // Add airports
    for airport in airports {
        let (iata_code, country, name, latitude, longitude) = airport;
        let airport = Airport::new(iata_code.to_string(), country.to_string(), name.to_string(), latitude, longitude);
        sim_state.add_airport(airport)?;
    }

    // Add flights (for today)
    let today = Utc::now().naive_utc();
    let flight_data = vec![
        ("AR1234", "AEP", "MDZ", today, today + chrono::Duration::hours(2), 550.0),
        ("AR5678", "EZE", "ROS", today, today + chrono::Duration::hours(1), 600.0),
        ("AR9101", "COR", "EZE", today, today + chrono::Duration::hours(3), 500.0),
        ("AR1122", "ROS", "AEP", today, today + chrono::Duration::hours(1), 650.0),
    ];

    // Add flights
    for (flight_number, origin, destination, departure_time, arrival_time, avg_speed) in flight_data {
        let departure_str = departure_time.format("%d-%m-%Y %H:%M:%S").to_string();
        let arrival_str = arrival_time.format("%d-%m-%Y %H:%M:%S").to_string();
        let flight = Flight::new_from_console(
            sim_state.airports(), flight_number, origin, destination, &departure_str, &arrival_str, avg_speed
        ).map_err(|_| SimError::Other("Error".to_string()))?;
        
        sim_state.add_flight(flight)?;
    }

    println!("Test data added successfully!");
    Ok(())
}