use crate::client::Client;
use crate::types::{flight::Flight, airport::Airport, flight_status::FlightStatus};
use chrono::{NaiveDateTime, Utc};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock, mpsc};
use std::time::Duration;
use threadpool::ThreadPool;
use std::io::{stdin, stdout, Write};
use std::thread;

use super::sim_error::SimError;

const TICK_DURATION_SEC : u64 = 1;

/// Represents the simulation state, including flights, airports, and time management.
pub struct SimState {
    flights: HashMap<String, Arc<RwLock<Flight>>>,
    pub flight_states: Arc<RwLock<HashMap<String, FlightStatus>>>,  // Vamos a usar este para ver que vuelos ya tenemos
    pub airports: HashMap<String, Airport>,
    pub current_time: Arc<Mutex<NaiveDateTime>>,
    time_rate: Arc<Mutex<Duration>>,
    pool: ThreadPool,
    client: Arc<Mutex<Client>>,
}

impl SimState {

    /// Creates a new simulation state with the given client.
    pub fn new(client: Client) -> Result<Self, SimError> {
        let state = SimState {
            flights: HashMap::new(),
            flight_states: Arc::new(RwLock::new(HashMap::new())),
            airports: HashMap::new(),
            current_time: Arc::new(Mutex::new(Utc::now().naive_utc())),
            time_rate: Arc::new(Mutex::new(Duration::from_secs(60))),
            pool: ThreadPool::new(4),
            client: Arc::new(Mutex::new(client)),
        };

        state.start_time_update();
        Ok(state)
    }

    /// Adds a flight to the simulation and inserts it into the database.
    pub fn add_flight(&mut self, flight: Flight) -> Result<(), SimError> {
        {
            let mut flight_states = self.flight_states.write().map_err(|_| SimError::Other("LockError".to_string()))?;
            flight_states.insert(flight.flight_number.clone(), flight.status.clone());
        }
        let flight_arc = Arc::new(RwLock::new(flight));
        {
            let mut client = self.client.lock().map_err(|_| SimError::ClientError)?;
            client.insert_flight(&flight_arc.read().unwrap()).map_err(|_| SimError::new("Client Error: Error inserting flight into DB"))?;
        }
        self.flights.insert(flight_arc.read().unwrap().flight_number.clone(), Arc::clone(&flight_arc)); // Inserción en el HashMap
        self.start_flight_simulation(flight_arc);
        Ok(())
    }

    /// Adds an airport to the simulation and inserts it into the database.
    pub fn add_airport(&mut self, airport: Airport) -> Result<(), SimError> {
        let mut client = self.client.lock().map_err(|_| SimError::ClientError)?;
        client.insert_airport(&airport).map_err(|_| SimError::new("Client Error: Error inserting flight into DB"))?;
        self.airports.insert(airport.iata_code.to_string(), airport);
        Ok(())
    }
    
    /// Displays the list of flights in the simulation and allows the user to exit the list view.
    ///
    /// This function spawns a thread to listen for user input and continuously updates the flight information on the screen.
    pub fn list_flights(&self) {
        println!("Press 'q' and Enter to exit list-flights mode");
    
        let (tx, rx) = mpsc::channel();
    
        // Hilo para manejar input
        thread::spawn(move || {
            let mut buffer = String::new();
            loop {
                buffer.clear();
                if stdin().read_line(&mut buffer).is_ok() {
                    if buffer.trim() == "q" {
                        tx.send(true).unwrap();
                        break;
                    }
                }
                thread::sleep(Duration::from_millis(100));
            }
        });

        loop {
            self.display_flights();
            stdout().flush().unwrap();
    
            // Verificar si se ha presionado 'q'
            if let Ok(true) = rx.try_recv() {
                break;
            }
    
            thread::sleep(Duration::from_secs(TICK_DURATION_SEC));
        }
    }

    /// Sets the time rate for the simulation, adjusting the duration of each tick.
    pub fn set_time_rate(&self, minutes: u64) {
        if let Ok(mut rate) = self.time_rate.lock() {
            *rate = Duration::from_secs(minutes * 60);
            println!("Time rate updated to {} seconds per tick", minutes * 60);
        }
    }
    /// Closes the thread pool and waits for all threads to finish.
    pub fn close_pool(&self) {
        self.pool.join();
    }

    pub fn airports(&self) -> &HashMap<String, Airport> {
        &self.airports
    }

    /// Updates the status of a flight in the simulation.
    pub fn update_flight_in_simulation(
        &self,
        flight_number: &str,
        new_status: FlightStatus,
    ) -> Result<(), SimError> {
        // Update in flight_states map
        {
            let mut flight_states = self
                .flight_states
                .write()
                .map_err(|_| SimError::Other("LockError".to_string()))?;
            
            if let Some(status) = flight_states.get_mut(flight_number) {
                *status = new_status.clone();
            } else {
                return Err(SimError::Other(format!("Flight {} not found", flight_number)));
            }
        }

        // Update in the flights map
        if let Some(flight_arc) = self.flights.get(flight_number) {
            let mut flight = flight_arc
                .write()
                .map_err(|_| SimError::Other("LockError".to_string()))?;
            flight.status = new_status;
        } else {
            return Err(SimError::Other(format!("Flight {} not found in simulation", flight_number)));
        }

        Ok(())
    }

    fn display_flights(&self) {
        print!("\x1B[2J\x1B[1;1H"); // Clear screen
        if self.flights.is_empty() {
            println!("No flights available.");
            return;
        }

        let current_time = self.current_time.lock()
            .map(|time| time.format("%d-%m-%Y %H:%M:%S").to_string())
            .unwrap_or_else(|_| "Unknown Time".to_string());

        println!("Current Time: {}", current_time);
        println!("\n{:<15} {:<10} {:<10} {:<15} {:<10} {:<10}", 
            "Flight Number", "Status", "Origin", "Destination", "Latitude", "Longitude");

        let flight_info: Vec<String> = self.flights.values() // Iterar sobre los valores del HashMap
            .filter_map(|flight| {
                match flight.try_read() {
                    Ok(flight_data) => Some(format!(
                        "{:<15} {:<10} {:<10} {:<10} {:<15.5} {:<15.5}",
                        flight_data.flight_number, 
                        flight_data.status.as_str(), 
                        flight_data.origin.iata_code, 
                        flight_data.destination.iata_code, 
                        flight_data.latitude, 
                        flight_data.longitude
                    )),
                    Err(_) => None,
                }
            })
            .collect();

        for info in flight_info {
            println!("{}", info);
        }

        println!("\nPress 'q' and Enter to exit list-flights mode");
    }

    fn start_time_update(&self) {
        let current_time = Arc::clone(&self.current_time);
        let time_rate = Arc::clone(&self.time_rate);

        std::thread::spawn(move || {
            loop {
                if let (Ok(mut time), Ok(rate)) = (current_time.lock(), time_rate.lock()) {
                    *time += chrono::Duration::from_std(*rate).unwrap_or(chrono::Duration::seconds(60));
                }
                thread::sleep(Duration::from_secs(TICK_DURATION_SEC));
            }
        });
    }

    fn start_flight_simulation(&self, flight: Arc<RwLock<Flight>>) {
        let flight_states = Arc::clone(&self.flight_states);
        let client = Arc::clone(&self.client);
        let current_time = Arc::clone(&self.current_time);
        
        self.pool.execute(move || {
            loop {
                let current = *current_time.lock().unwrap();

                if let Ok(mut flight_data) = flight.write() {
                    match flight_data.status {
                        FlightStatus::Scheduled if current >= flight_data.departure_time => {
                            flight_data.update_position(current);
                            if let Err(e) = update_flight_state(&flight_states, &flight_data.flight_number, FlightStatus::OnTime) {
                                eprintln!("Failed to update flight state: {:?}", e);
                            }
                        }
                        FlightStatus::OnTime | FlightStatus::Delayed => {
                            flight_data.update_position(current);

                            if current >= flight_data.arrival_time {

                                if let Err(e) = update_flight_state(&flight_states, &flight_data.flight_number, FlightStatus::Delayed) {
                                    eprintln!("Failed to update flight state: {:?}", e);
                                }
                            } else if flight_data.distance_traveled >= flight_data.total_distance {

                                if let Err(e) = update_flight_state(&flight_states, &flight_data.flight_number, FlightStatus::Finished) {
                                    eprintln!("Failed to update flight state: {:?}", e);
                                }
                            } 

                            if let Ok(mut db_client) = client.lock() {
                                if let Err(e) = db_client.update_flight(&*flight_data) {
                                    eprintln!("Failed to update flight {}: {:?}", 
                                        flight_data.flight_number, e);
                                }
                            }
                        }
                        FlightStatus::Finished | FlightStatus::Canceled => break,
                        _ => {}
                    }
                }

                thread::sleep(Duration::from_secs(TICK_DURATION_SEC));
            }
        });
    }
}

fn update_flight_state(
    flight_states: &Arc<RwLock<HashMap<String, FlightStatus>>>,
    flight_number: &str,
    new_status: FlightStatus,
) -> Result<(), SimError> {
    let mut states = flight_states.write().map_err(|_| SimError::Other("LockError".to_string()))?;
    states.insert(flight_number.to_string(), new_status);
    Ok(())
}
