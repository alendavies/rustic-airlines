mod types;

use crate::types::flight_status::FlightStatus;
use crate::types::flight::Flight;
use crate::types::tracking::Tracking;
use crate::types::airport::Airport;

use std::env;
use std::sync::{Arc, Mutex};
use std::thread;
use threadpool::ThreadPool;
use std::collections::HashMap;
use std::time::{SystemTime, Duration};
use std::error::Error;


// De momento esto es para testear, deberiamos tener una DB completa.
fn load_airports() -> HashMap<String, Airport> {
    let mut airports = HashMap::new();
    airports.insert(
        "EZE".to_string(),
        Airport::new(
            "EZE".to_string(),
            "Aeropuerto internacional de Ezeiza".to_string(),
            40.6413,
            -73.7781,
        ),
    );
    airports.insert(
        "BRC".to_string(),
        Airport::new (
            "BRC".to_string(),
            "Aeropuerto de San Carlos de Bariloche".to_string(),
            33.9416,
            -118.4085,
        ),
    );
    airports.insert(
        "AEP".to_string(),
        Airport::new (
            "AEP".to_string(),
            "Aeroparque Jorge Newbery".to_string(),
            33.9416,
            -118.4085,
        ),
    );
    
    airports
}

fn simulate_flight(mut flight: Flight, tracking: Arc<Mutex<Tracking>>) {
    while flight.status != FlightStatus::Finished {
        let mut tracking_data = tracking.lock().unwrap();
        flight.check_status(&tracking_data);

        match flight.status {
            FlightStatus::Pending => {
                println!("Flight {} pending, waiting for departure...", flight.flight_number);
            }
            FlightStatus::InFlight => {
                let direction = tracking_data.calculate_current_direction(&flight.destination);
                tracking_data.update_position(flight.average_speed, direction);

                println!(
                    "Flight {} in progress: latitude={}, longitude={}, distance_traveled={}",
                    flight.flight_number, tracking_data.latitude(), tracking_data.longitude(),
                    tracking_data.distance_traveled()
                );

                if flight.check_arrival(&tracking_data) {
                    flight.status = FlightStatus::Finished;
                }
            }
            FlightStatus::Finished => {
                tracking_data.land();
                println!("Flight {} has landed.", flight.flight_number);
                break;
            }
        }

        thread::sleep(Duration::from_secs(1));
    }
}

fn add_airport(airports: &mut HashMap<String, Airport>, iata_code: &str, name: &str, latitude: f64, longitude: f64) -> Result<(), Box<dyn Error>> {
    let airport = Airport::new (
        iata_code.to_string(),
        name.to_string(),
        latitude,
        longitude,
    );

    airports.insert(iata_code.to_string(), airport);
    println!("Airport added successfully.");
    Ok(())
}

fn add_flight(airports: &HashMap<String, Airport>, pool: &ThreadPool, flights: &mut Vec<String>, flight_number: &str, origin_code: &str, destination_code: &str, average_speed: f64) -> Result<(), Box<dyn Error>> {
    let origin = airports.get(origin_code)
        .ok_or(format!("Origin airport with IATA code '{}' not found.", origin_code))?
        .clone();

    let destination = airports.get(destination_code)
        .ok_or(format!("Destination airport with IATA code '{}' not found.", destination_code))?
        .clone();

    let flight = Flight {
        flight_number: flight_number.to_string(),
        origin,
        destination,
        average_speed,
        status: FlightStatus::Pending,
        departure_time: SystemTime::now(),
    };

    let tracking = Arc::new(Mutex::new(flight.start_tracking()));

    flights.push(flight_number.to_string());

    let flight_clone = flight.clone();
    let tracking_clone = Arc::clone(&tracking);

    pool.execute(move || {
        simulate_flight(flight_clone, tracking_clone);
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
    println!("  -h or --help");
    println!("    Show this help message.");
}


fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    let mut airports = load_airports();
    let pool = ThreadPool::new(4); // Create a thread pool with 4 threads
    let mut flights = vec![];

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

            add_airport(&mut airports, iata_code, name, latitude, longitude)?;
        }
        "-h" | "--help" => {
            print_help();
        }
        _ => {
            eprintln!("Invalid command. Use -h for help.");
        }
    }

    pool.join(); // Wait for all threads to finish
    Ok(())
}

