mod types;

use crate::types::flight_status::FlightStatus;
use crate::types::flight::Flight;
use crate::types::tracking::Tracking;
use crate::types::airport::Airport;

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

fn main() -> Result<(), Box<dyn Error>> {

    let airports = load_airports();

    // We use 4 threads in this example.
    let pool = ThreadPool::new(4); 

    let mut flights = vec![];

    loop {
        println!("Enter flight number (or type 'exit' to finish):");
        let mut flight_number = String::new();
        std::io::stdin().read_line(&mut flight_number).unwrap();
        let flight_number = flight_number.trim().to_string();

        if flight_number == "exit" {
            break;
        }

        println!("Enter origin airport IATA code:");
        let mut origin_code = String::new();
        std::io::stdin().read_line(&mut origin_code).unwrap();
        let origin_code = origin_code.trim();

        println!("Enter destination airport IATA code:");
        let mut destination_code = String::new();
        std::io::stdin().read_line(&mut destination_code).unwrap();
        let destination_code = destination_code.trim();

        println!("Enter estimated average speed (km/h):");
        let mut average_speed_input = String::new();
        std::io::stdin().read_line(&mut average_speed_input).unwrap();
        let average_speed: f64 = average_speed_input.trim().parse()?;

        let origin = airports.get(origin_code)
            .ok_or(format!("Origin airport with IATA code '{}' not found.", origin_code))?
            .clone();

        let destination = airports.get(destination_code)
            .ok_or(format!("Destination airport with IATA code '{}' not found.", destination_code))?
            .clone();

        let flight = Flight {
            flight_number: flight_number.clone(),
            origin,
            destination,
            average_speed,
            status: FlightStatus::Pending,
            departure_time: SystemTime::now(),
        };

        let tracking = Arc::new(Mutex::new(flight.start_tracking()));

        flights.push(flight_number.clone());

        let flight_clone = flight.clone();
        let tracking_clone = Arc::clone(&tracking);

        pool.execute(move || {
            simulate_flight(flight_clone, tracking_clone);
        });
    }

    pool.join(); 
    Ok(())
}

