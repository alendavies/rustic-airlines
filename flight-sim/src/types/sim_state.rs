use crate::client::Client;
use crate::types::{flight::Flight, airport::Airport, flight_status::FlightStatus};
use chrono::{NaiveDateTime, Utc};
use std::collections::HashMap;
use std::io::Read;
use std::sync::{Arc, Mutex, atomic::{AtomicBool, Ordering}};
use std::{io, thread};
use std::time::Duration;
use threadpool::ThreadPool;

use super::sim_error::SimError;

pub struct SimState {
    flights: Vec<Arc<Mutex<Flight>>>,
    airports: HashMap<String, Airport>,
    current_time: Arc<Mutex<NaiveDateTime>>,
    time_rate: Arc<Mutex<Duration>>,
    is_listing: Arc<AtomicBool>,
    pool: ThreadPool,
    client: Arc<Mutex<Client>>,
}

impl SimState {
    pub fn new(client: Client) -> Result<Self, SimError> {
        Ok(SimState {
            flights: vec![],
            airports: HashMap::new(),
            current_time: Arc::new(Mutex::new(Utc::now().naive_utc())),
            time_rate: Arc::new(Mutex::new(Duration::from_secs(60))),
            is_listing: Arc::new(AtomicBool::new(false)),
            pool: ThreadPool::new(4),
            client: Arc::new(Mutex::new(client)),
        })
    }

    pub fn add_flight(&mut self, flight: Flight) -> Result<(), SimError> {
        let flight_arc = Arc::new(Mutex::new(flight));
        {
            let mut client = self.client.lock().map_err(|_| SimError::ClientError)?;
            client.insert_flight(&flight_arc.lock().unwrap()).map_err(|_| SimError::new("Client Error: Error inserting flight into DB"))?;
        }
        self.flights.push(Arc::clone(&flight_arc));
        self.start_flight_simulation(flight_arc);
        Ok(())
    }

    pub fn add_airport(&mut self, airport: Airport) -> Result<(), SimError> {
        let mut client = self.client.lock().map_err(|_| SimError::ClientError)?;
        client.insert_airport(&airport).map_err(|_| SimError::new("Client Error: Error inserting flight into DB"))?;
        self.airports.insert(airport.iata_code.to_string(), airport);
        Ok(())
    }

    pub fn list_flights(&self) {
        self.is_listing.store(true, Ordering::SeqCst);
        println!("Press 'q' and Enter to exit list-flights mode");
    
        loop {
            // Mostrar los vuelos
            self.display_flights();
    
            // Verificar si se ha presionado 'q' para salir
            let mut buffer = [0; 1];
            if io::stdin().read(&mut buffer).is_ok() {
                if buffer[0] == b'q' {
                    self.is_listing.store(false, Ordering::SeqCst);
                    break;
                }
            }
    
            // Actualizar el tiempo actual en base a la tasa de tiempo
            if let (Ok(mut time), Ok(rate)) = (self.current_time.lock(), self.time_rate.lock()) {
                *time += chrono::Duration::from_std(*rate).unwrap_or(chrono::Duration::seconds(60));
            }
    
            // Esperar un segundo antes de la siguiente actualización
            thread::sleep(Duration::from_secs(1));
        }
    }

    pub fn set_time_rate(&self, minutes: u64) {
        if let Ok(mut rate) = self.time_rate.lock() {
            *rate = Duration::from_secs(minutes * 60);
            println!("Time rate updated to {} seconds per tick", minutes * 60);
        }
    }

    pub fn close_pool(&self) {
        self.pool.join();
    }

    pub fn airports(&self) -> &HashMap<String, Airport> {
        &self.airports
    }

    fn display_flights(&self) {
        print!("\x1B[2J\x1B[1;1H"); // Clear screen
        if self.flights.is_empty() {
            println!("No flights available.");
        } else {
            println!("Current Time: {}", self.current_time.lock().unwrap().format("%d-%m-%Y %H:%M:%S"));
            println!("\n{:<15} {:<10} {:<10} {:<15} {:<10} {:<10}", 
                "Flight Number", "Status", "Origin", "Destination", "Latitude", "Longitude");

            for flight in &self.flights {
                if let Ok(flight_data) = flight.lock() {
                    println!(
                        "{:<15} {:<10} {:<10} {:<10} {:<15.5} {:<15.5}", 
                        flight_data.flight_number, 
                        flight_data.status.as_str(), 
                        flight_data.origin.iata_code, 
                        flight_data.destination.iata_code, 
                        flight_data.latitude, 
                        flight_data.longitude
                    );
                }
            }
        }
        println!("\nPress 'q' and Enter to exit list-flights mode");
    }

    fn start_flight_simulation(&self, flight: Arc<Mutex<Flight>>) {
        let client = Arc::clone(&self.client);
        let current_time = Arc::clone(&self.current_time);
        let is_listing = Arc::clone(&self.is_listing);
        self.pool.execute(move || {
            loop {
                if !is_listing.load(Ordering::SeqCst) {
                    thread::sleep(Duration::from_secs(1));
                    continue;
                }

                let mut flight_data = match flight.lock() {
                    Ok(data) => data,
                    Err(e) => {
                        eprintln!("Failed to lock flight data: {:?}", e);
                        return;
                    }
                };

                let current = *current_time.lock().unwrap();

                match flight_data.status {
                    FlightStatus::Pending if current >= flight_data.departure_time => {
                        flight_data.status = FlightStatus::InFlight;
                    }
                    FlightStatus::InFlight | FlightStatus::Delayed => {
                        flight_data.update_position(current);
                        if let Ok(mut client) = client.lock() {
                            if let Err(e) = client.update_flight(&*flight_data) {
                                eprintln!("Failed to update flight {}: {:?}", flight_data.flight_number, e);
                            }
                        }
                    }
                    FlightStatus::Finished => break,
                    _ => {}
                }

                thread::sleep(Duration::from_secs(1));
            }
        });
    }
}
