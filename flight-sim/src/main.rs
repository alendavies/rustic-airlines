mod types;
mod client;

use crate::types::flight_status::FlightStatus;
use crate::types::flight::Flight;
use crate::types::airport::Airport;

use chrono::{NaiveDateTime, Utc};
use driver::ClientError;
use std::io::{self, Write};
use std::sync::{Arc, Mutex, atomic::{AtomicBool, Ordering}};
use std::thread;
use client::Client;
use threadpool::ThreadPool;
use std::collections::HashMap;
use std::time::Duration;
use std::error::Error;

// Limpiar pantalla (compatible con la mayoría de terminales)
fn clean_scr(){
    print!("\x1B[2J\x1B[1;1H");
    io::stdout().flush().unwrap();
}

fn list_flights(
    flights: &Vec<Arc<Mutex<Flight>>>,
    is_listing: &Arc<AtomicBool>,
    time_rate: &Arc<Mutex<Duration>>,
    current_time: &Arc<Mutex<NaiveDateTime>>
) {
    is_listing.store(true, Ordering::SeqCst);

    let mut input = String::new();
    let stdin = io::stdin();

    loop {
        clean_scr();
        if flights.is_empty() {
            println!("No flights available.");
        } else {
            println!("Current Time: {}", current_time.lock().unwrap().format("%Y-%m-%d %H:%M:%S"));
            println!("\n{:<15} {:<10} {:<10} {:<15} {:<10} {:<10}", 
                "Flight Number", "Status", "Origin", "Destination", "Latitude", "Longitude");

            for flight in flights {
                if let Ok(flight_data) = flight.lock() {
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
        }

        println!("\nPress 'q' and Enter to exit list-flights mode");
        
        // Intentar leer input sin bloquear
        if let Ok(n) = stdin.read_line(&mut input) {
            if n > 0 && input.trim().to_lowercase() == "q" {
                break;
            }
            input.clear();
        }

        // Actualizar current_time basado en time_rate
        if let (Ok(mut time), Ok(rate)) = (current_time.lock(), time_rate.lock()) {
            *time = *time + chrono::Duration::from_std(*rate).unwrap_or(chrono::Duration::seconds(1));
        }

        thread::sleep(Duration::from_secs(1));
    }

    is_listing.store(false, Ordering::SeqCst);
    println!("\nExited list-flights mode");
}

fn simulate_flight(
    flight: Arc<Mutex<Flight>>, 
    client: Arc<Mutex<Client>>, 
    current_time: Arc<Mutex<NaiveDateTime>>,
    is_listing: Arc<AtomicBool>
) {
    loop {
        if !is_listing.load(Ordering::SeqCst) {
            thread::sleep(Duration::from_secs(1));
            continue;
        }

        let mut flight_data = match flight.lock() {
            Ok(data) => data,
            Err(e) => {
                eprintln!("Failed to lock flight data for simulation: {:?}", e);
                return;
            }
        };

        let current = current_time.lock().unwrap().clone();

        match flight_data.status {
            FlightStatus::Pending => {
                if current >= flight_data.departure_time {
                    flight_data.status = FlightStatus::InFlight;
                }
            }
            FlightStatus::InFlight | FlightStatus::Delayed => {
                flight_data.update_position(current);

                if let Ok(mut client_locked) = client.lock() {
                    if let Err(e) = client_locked.update_flight(&*flight_data) {
                        eprintln!("Failed to update flight {}: {:?}", flight_data.flight_number, e);
                    }
                }
            }
            FlightStatus::Finished => {
                if let Ok(mut client_locked) = client.lock() {
                    if let Err(e) = client_locked.update_flight(&*flight_data) {
                        eprintln!("Failed to update flight {}: {:?}", flight_data.flight_number, e);
                    }
                }
                break;
            }
        }

        thread::sleep(Duration::from_secs(1));
    }
}

fn add_flight(
    pool: &ThreadPool,
    flights: &mut Vec<Arc<Mutex<Flight>>>,
    flight: Flight,
    client: Arc<Mutex<Client>>,
    current_time: Arc<Mutex<NaiveDateTime>>,
    is_listing: Arc<AtomicBool>,
) -> Result<(), ClientError> {
    let flight_arc = Arc::new(Mutex::new(flight));
    
    if let Ok(mut client_locked) = client.lock() {
        client_locked.insert_flight(&(*flight_arc.lock().unwrap()))?;
    } else {
        return Err(ClientError);
    }

    flights.push(Arc::clone(&flight_arc));

    pool.execute({
        let client = Arc::clone(&client);
        let flight_arc = Arc::clone(&flight_arc);
        let current_time = Arc::clone(&current_time);
        let is_listing = Arc::clone(&is_listing);
        move || {
            simulate_flight(flight_arc, client, current_time, is_listing);
        }
    });

    println!("Flight added and started simulation.");
    Ok(())
}

fn print_help() {
    clean_scr();
    println!("Available commands:");
    println!("  add-flight <flight_number> <origin> <destination> <departure_time[DD/MM/YY-HH:MM:SS]> <arrival_time[DD/MM/YY-HH:MM:SS]> <average_speed>");
    println!("    Add a new flight with the specified parameters.");
    println!("  add-airport <IATA_code> <country> <name> <latitude> <longitude>");
    println!("    Add a new airport with the specified parameters.");
    println!("  list-flights <minutes>");
    println!("    Shows the current flights.");
    println!("  time-rate <minutes>");
    println!("    Changes the simulation's elapsed time per tick.");
    println!("  exit");
    println!("    Closes this application.");
    println!("  -h or help");
    println!("    Show this help message.");
}

fn main() -> Result<(), ClientError> {
    let pool = ThreadPool::new(4);
    let mut flights = vec![];
    let mut airports: HashMap<String, Airport> = HashMap::new();

    let ip = "127.0.0.1".parse().expect("Invalid IP format");
    let flight_sim_client = Arc::new(Mutex::new(Client::new(ip)?));

    let current_time = Arc::new(Mutex::new(Utc::now().naive_utc()));
    let time_rate = Arc::new(Mutex::new(Duration::from_secs(1)));
    let is_listing = Arc::new(AtomicBool::new(false));

    loop {
        if !is_listing.load(Ordering::SeqCst) {
            println!("\nEnter command (type '-h' or '--help' for options): ");
            let mut command = String::new();
            io::stdin().read_line(&mut command).map_err(|_| ClientError)?;
            let args: Vec<&str> = command.trim().split_whitespace().collect();

            if args.is_empty() {
                continue;
            }

            match args[0] {
                "add-flight" => {
                    if args.len() < 7 {
                        eprintln!("Usage: add-flight <flight_number> <origin> <destination> <departure_time[DD/MM/YY-HH:MM:SS]> <arrival_time[DD/MM/YY-HH:MM:SS]> <average_speed>");
                        continue;
                    }
                    
                    let flight = Flight::new_from_console(
                        &airports, 
                        &args[1], 
                        &args[2], 
                        &args[3], 
                        &args[4], 
                        &args[5], 
                        args[6].parse().map_err(|_| ClientError)?
                    ).map_err(|_| ClientError)?;

                    add_flight(
                        &pool,
                        &mut flights,
                        flight,
                        Arc::clone(&flight_sim_client),
                        Arc::clone(&current_time),
                        Arc::clone(&is_listing)
                    )?;
                }

                "add-airport" => {
                    if args.len() < 6 {
                        eprintln!("Usage: add-airport <IATA_code> <country> <name> <latitude> <longitude>");
                        continue;
                    }
                    
                    let airport = Airport::new(
                        args[1].to_string(),
                        args[2].to_string(),
                        args[3].to_string(),
                        args[4].parse().map_err(|_| ClientError)?,
                        args[5].parse().map_err(|_| ClientError)?
                    );

                    airports.insert(args[1].to_string(), airport.clone());
                    
                    if let Ok(mut client_locked) = flight_sim_client.lock() {
                        client_locked.insert_airport(&airport)?;
                    }
                }

                "list-flights" => {
                    list_flights(&flights, &is_listing, &time_rate, &current_time);
                }

                "time-rate" => {
                    if args.len() != 2 {
                        eprintln!("Usage: time-rate <seconds>");
                        continue;
                    }

                    let seconds: u64 = args[1].parse().map_err(|_| ClientError)?;
                    if let Ok(mut rate) = time_rate.lock() {
                        *rate = Duration::from_secs(seconds);
                        println!("Time rate updated to {} seconds per tick", seconds);
                    }
                }

                "-h" | "help" => {
                    print_help();
                }

                "exit" => {
                    break;
                }

                _ => {
                    eprintln!("Invalid command. Use -h for help.");
                }
            }
        }
    }

    pool.join();
    Ok(())
}

