mod types;
mod client;

use crate::types::flight_status::FlightStatus;
use crate::types::flight::Flight;
use crate::types::airport::Airport;

use std::env;
use std::sync::{Arc, Mutex};
use std::thread;
use client::Client;
use threadpool::ThreadPool;
use std::collections::HashMap;
use std::time::Duration;
use std::error::Error;


fn list_flights(flights: &Vec<Arc<Mutex<Flight>>>) {
    if flights.is_empty() {
        println!("No flights available.");
        return;
    }

    println!("{:<15} {:<10} {:<10} {:<15} {:<10} {:<10}", 
        "Flight Number", "Status", "Origin", "Destination", "Latitude", "Longitude");

    for flight in flights {
        let flight_data = flight.lock().unwrap();

        let status = flight_data.status.as_str();

        println!(
            "{:<15} {:<10} {:<10} {:<10} {:<15.5} {:<15.5}", 
            flight_data.flight_number, 
            status, 
            flight_data.origin.iata_code, 
            flight_data.destination.iata_code, 
            flight_data.latitude, 
            flight_data.longitude
        );
    }
}

fn simulate_flight(flight: Arc<Mutex<Flight>>) {
    loop {
        let mut flight_data = flight.lock().unwrap();
       
        match flight_data.status {
            FlightStatus::Pending => {
                println!("Flight {} pending, waiting for departure...", flight_data.flight_number);
            }
            FlightStatus::InFlight => {
                let direction = flight_data.calculate_current_direction();
                flight_data.update_position(direction);

                println!(
                    "Flight {} in progress: latitude={}, longitude={}, distance_traveled={}", 
                    flight_data.flight_number, flight_data.latitude, flight_data.longitude,
                    flight_data.distance_traveled
                );

            }
            FlightStatus::Finished => {
                println!("Flight {} has landed.", flight_data.flight_number);
                break;
            }
        }

        flight_data.check_status();

        thread::sleep(Duration::from_secs(1));
    }
}


fn add_flight(
    airports: &HashMap<String, Airport>, 
    pool: &ThreadPool, 
    flights: &mut Vec<Arc<Mutex<Flight>>>, 
    flight_number: &str, 
    origin_code: &str, 
    destination_code: &str, 
    average_speed: f64
) -> Result<(), Box<dyn Error>> {
    let origin = airports.get(origin_code)
        .ok_or(format!("Origin airport with IATA code '{}' not found.", origin_code))?
        .clone();

    let destination = airports.get(destination_code)
        .ok_or(format!("Destination airport with IATA code '{}' not found.", destination_code))?
        .clone();

    let flight = Flight::new(flight_number.to_string(), origin, destination, average_speed);
    
    let flight_arc = Arc::new(Mutex::new(flight));

    flights.push(Arc::clone(&flight_arc));

    pool.execute(move || {
        simulate_flight(flight_arc);
    });

    println!("Flight added and started simulation.");
    Ok(())
}

fn print_help() {
    println!("Available commands:");
    println!("  add-flight <flight_number> <origin> <destination> <average_speed>");
    println!("    Add a new flight with the specified parameters.");
    println!("  add-airport <IATA_code> <name> <latitude> <longitude>");
    println!("    Add a new airport with the specified parameters.");
    println!("  list-flights");
    println!("    Prints all recorded flights.");
    println!("  -h or --help");
    println!("    Show this help message.");
}


fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    let pool = ThreadPool::new(4); // Create a thread pool with 4 threads (arbitrary number)
    let mut flights = vec![];
    let mut airports: HashMap<String, Airport> = HashMap::new();

    let ip = "127.0.0.1".parse().unwrap();  // Replace with actual IP
    let mut flight_sim_client = Client::new(ip)?;

    if args.len() < 2 {
        print_help();
        return Ok(());
    }

    match args[1].as_str() {
        "add-flight" => {
            if args.len() < 6 {
                eprintln!("Usage: add-flight <flight_number> <origin> <destination> <average_speed>");
                return Ok(());
            }
            let flight_number = &args[2];
            let origin_code = &args[3];
            let destination_code = &args[4];
            let average_speed: f64 = args[5].parse()?;

            add_flight(&airports, &pool, &mut flights, flight_number, origin_code, destination_code, average_speed)?;

        }
        "add-airport" => {
            if args.len() < 5 {
                eprintln!("Usage: add-airport <IATA_code> <name> <latitude> <longitude>");
                return Ok(());
            }
            let iata_code = &args[2];
            let name = &args[3];
            let latitude: f64 = args[4].parse()?;
            let longitude: f64 = args[5].parse()?;

            let airport = Airport::new (
                iata_code.to_string(),
                name.to_string(),
                latitude,
                longitude,
            );

            airports.insert(iata_code.to_string(), airport);
            flight_sim_client.insert_airport(airport)?;
            
        }
        "list-flights" => {
            list_flights(&flights);
        }
        "-h" | "--help" => {
            print_help();
        }
        _ => {
            eprintln!("Invalid command. Use -h for help.");
        }
    }

    pool.join(); 
    Ok(())
}

