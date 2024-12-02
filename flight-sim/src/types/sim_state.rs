use crate::client::Client;
use crate::types::{airport::Airport, flight::Flight, flight_status::FlightStatus};
use chrono::{NaiveDateTime, Utc};
use std::collections::HashMap;
use std::io::{stdin, stdout, Write};
use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::thread;
use std::time::Duration;
use threadpool::ThreadPool;

use super::sim_error::SimError;

// The period it takes for the flights to be updated and time to pass.
const TICK_DURATION_MILLIS: u64 = 1000;

/// Represents the simulation state, including flights, airports, and time management.
#[derive(Clone)]
pub struct SimState {
    flights: Arc<RwLock<HashMap<String, Arc<RwLock<Flight>>>>>,
    pub flight_states: Arc<RwLock<HashMap<String, FlightStatus>>>,
    pub airports: Arc<RwLock<HashMap<String, Airport>>>,
    pub current_time: Arc<Mutex<NaiveDateTime>>,
    time_rate: Arc<Mutex<Duration>>,
    pool: ThreadPool,
    client: Arc<Mutex<Client>>,
}

impl SimState {
    /// Creates a new simulation state with the given client.
    pub fn new(client: Client) -> Result<Self, SimError> {
        let state = SimState {
            flights: Arc::new(RwLock::new(HashMap::new())),
            flight_states: Arc::new(RwLock::new(HashMap::new())),
            airports: Arc::new(RwLock::new(HashMap::new())),
            current_time: Arc::new(Mutex::new(Utc::now().naive_utc())),
            time_rate: Arc::new(Mutex::new(Duration::from_secs(30))),
            pool: ThreadPool::new(4),
            client: Arc::new(Mutex::new(client)),
        };

        state.start_time_update();
        state.start_periodic_flight_check(TICK_DURATION_MILLIS * 1000);
        Ok(state)
    }

    /// Adds a flight to the simulation and inserts it into the database.
    pub fn add_flight(&self, flight: Flight) -> Result<(), SimError> {
        {
            let mut flight_states = self
                .flight_states
                .write()
                .map_err(|_| SimError::Other("LockError".to_string()))?;
            flight_states.insert(flight.flight_number.clone(), flight.status.clone());
        }

        let flight_arc = Arc::new(RwLock::new(flight));
        let flight_number = {
            let read_guard = flight_arc
                .read()
                .map_err(|_| SimError::Other("LockError".to_string()))?;
            read_guard.flight_number.clone()
        };

        {
            let mut flights_lock = self
                .flights
                .write()
                .map_err(|_| SimError::Other("LockError".to_string()))?;
            flights_lock.insert(flight_number, Arc::clone(&flight_arc));
        }

        let mut client = self.client.lock().map_err(|_| SimError::ClientError)?;
        client
            .insert_flight(&flight_arc.read().unwrap())
            .map_err(|_| SimError::new("Client Error: Error inserting flight into DB"))?;

        self.start_flight_simulation(flight_arc);
        Ok(())
    }

    /// Adds an airport to the simulation and inserts it into the database.
    pub fn add_airport(&self, airport: Airport) -> Result<(), SimError> {
        {
            let mut client = self.client.lock().map_err(|_| SimError::ClientError)?;
            client
                .insert_airport(&airport)
                .map_err(|_| SimError::new("Client Error: Error inserting airport into DB"))?;
        }
        {
            let mut airports_lock = self
                .airports
                .write()
                .map_err(|_| SimError::Other("LockError".to_string()))?;
            airports_lock.insert(airport.iata_code.to_string(), airport);
        }

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

            thread::sleep(Duration::from_millis(TICK_DURATION_MILLIS));
        }
    }

    /// Simply displays the list of airports.
    pub fn list_airports(&self) {
        let airports_lock = self
            .airports
            .read()
            .map_err(|_| SimError::Other("LockError".to_string()));

        match airports_lock {
            Ok(airports) => {
                if airports.is_empty() {
                    println!("No airports available.");
                    return;
                }

                println!(
                    "\n{:<10} {:<50} {:<10} {:<15} {:<15}",
                    "IATA Code", "Airport Name", "Country", "Latitude", "Longitude"
                );
                println!("{}", "-".repeat(100));

                for airport in airports.values() {
                    let truncated_name = if airport.name.len() > 49 {
                        format!("{}...", &airport.name[..49])
                    } else {
                        airport.name.clone()
                    };

                    println!(
                        "{:<10} {:<50} {:<10} {:<15} {:<15}",
                        airport.iata_code,
                        truncated_name,
                        airport.country,
                        airport.latitude,
                        airport.longitude
                    );
                }
            }
            Err(_) => println!("Failed to access airports."),
        }

        thread::sleep(Duration::from_millis(2));
    }

    /// Sets the time rate for the simulation, adjusting the duration of each tick.
    pub fn set_time_rate(&self, minutes: u64) {
        if let Ok(mut rate) = self.time_rate.lock() {
            *rate = Duration::from_millis(minutes * 60);
            println!("Time rate updated to {} seconds per tick", minutes * 60);
        }
    }
    /// Closes the thread pool and waits for all threads to finish.
    pub fn close_pool(&self) {
        self.pool.join();
    }

    pub fn airports(&self) -> Result<HashMap<String, Airport>, SimError> {
        let airports_lock = self
            .airports
            .read()
            .map_err(|_| SimError::Other("LockError".to_string()))?;
        Ok(airports_lock.clone())
    }

    fn start_periodic_flight_check(&self, interval_sec: u64) {
        let flight_states = Arc::clone(&self.flight_states);
        let flights = Arc::clone(&self.flights);
        let client = Arc::clone(&self.client);
        let current_time = Arc::clone(&self.current_time);
        let airports = Arc::clone(&self.airports);
        let pool = self.pool.clone();

        thread::spawn(move || loop {
            if let Err(e) = check_for_new_flights(
                &client,
                &flight_states,
                &flights,
                &current_time,
                &airports,
                &pool,
            ) {
                eprintln!("Error checking for new flights: {:?}", e);
            }
            thread::sleep(Duration::from_millis(interval_sec));
        });
    }

    fn display_flights(&self) {
        print!("\x1B[2J\x1B[1;1H"); // Limpiar pantalla

        let current_time = self
            .current_time
            .lock()
            .map(|time| time.format("%d-%m-%Y %H:%M:%S").to_string())
            .unwrap_or_else(|_| "Unknown Time".to_string());

        println!("Current Time: {}", current_time);

        let flights = self
            .flights
            .read()
            .map_err(|_| SimError::Other("LockError".to_string()));

        match flights {
            Ok(flights_map) => {
                if flights_map.is_empty() {
                    println!("No flights available.");
                    return;
                }

                println!(
                    "\n{:<15} {:<10} {:<10} {:<15} {:<10} {:<10}",
                    "Flight Number", "Status", "Origin", "Destination", "Latitude", "Longitude"
                );

                for flight_arc in flights_map.values() {
                    if let Ok(flight) = flight_arc.read() {
                        println!(
                            "{:<15} {:<10} {:<10} {:<10} {:<15.5} {:<15.5}",
                            flight.flight_number,
                            flight.status.as_str(),
                            flight.origin.iata_code,
                            flight.destination.iata_code,
                            flight.latitude,
                            flight.longitude
                        );
                    }
                }
            }
            Err(_) => println!("Failed to access flights."),
        }

        println!("\nPress 'q' and Enter to exit list-flights mode");
    }

    fn start_time_update(&self) {
        let current_time = Arc::clone(&self.current_time);
        let time_rate = Arc::clone(&self.time_rate);

        std::thread::spawn(move || loop {
            if let (Ok(mut time), Ok(rate)) = (current_time.lock(), time_rate.lock()) {
                *time += chrono::Duration::from_std(*rate).unwrap_or(chrono::Duration::seconds(60));
            }
            thread::sleep(Duration::from_millis(TICK_DURATION_MILLIS));
        });
    }

    fn start_flight_simulation(&self, flight: Arc<RwLock<Flight>>) {
        let flight_states = Arc::clone(&self.flight_states);
        let client = Arc::clone(&self.client);
        let current_time = Arc::clone(&self.current_time);

        self.pool.execute(move || loop {
            let current = *current_time.lock().unwrap();

            if let Ok(mut flight_data) = flight.write() {
                match flight_data.status {
                    FlightStatus::Scheduled if current >= flight_data.departure_time => {
                        flight_data.update_position(current);
                        if let Err(e) = update_flight_state(
                            &client,
                            &flight_states,
                            &flight_data,
                            FlightStatus::OnTime,
                        ) {
                            eprintln!("Failed to update flight state: {:?}", e);
                        }
                    }
                    FlightStatus::OnTime | FlightStatus::Delayed => {
                        flight_data.update_position(current);

                        if current >= flight_data.arrival_time {
                            if let Err(e) = update_flight_state(
                                &client,
                                &flight_states,
                                &flight_data,
                                FlightStatus::Delayed,
                            ) {
                                eprintln!("Failed to update flight state: {:?}", e);
                            }
                        } else if flight_data.distance_traveled >= flight_data.total_distance {
                            if let Err(e) = update_flight_state(
                                &client,
                                &flight_states,
                                &flight_data,
                                FlightStatus::Finished,
                            ) {
                                eprintln!("Failed to update flight state: {:?}", e);
                            }
                        }

                        if let Ok(mut db_client) = client.lock() {
                            if let Err(e) = db_client.update_flight(&*flight_data) {
                                eprintln!(
                                    "Failed to update flight {}: {:?}",
                                    flight_data.flight_number, e
                                );
                            }
                        }
                    }
                    FlightStatus::Finished | FlightStatus::Canceled => break,
                    _ => {}
                }
            }

            thread::sleep(Duration::from_millis(TICK_DURATION_MILLIS));
        });
    }
}

fn update_flight_state(
    client: &Arc<Mutex<Client>>,
    flight_states: &Arc<RwLock<HashMap<String, FlightStatus>>>,
    flight: &Flight,
    new_status: FlightStatus,
) -> Result<(), SimError> {
    let mut states = flight_states
        .write()
        .map_err(|_| SimError::Other("LockError".to_string()))?;
    states.insert(flight.flight_number.to_string(), new_status);

    if let Ok(mut db_client) = client.lock() {
        if let Err(e) = db_client.update_flight_status(flight) {
            eprintln!(
                "Failed to update flight state in database {}: {:?}",
                flight.flight_number, e
            );
        }
    }

    Ok(())
}

fn add_flight_to_simulation(
    flights: &Arc<RwLock<HashMap<String, Arc<RwLock<Flight>>>>>,
    flight_states: &Arc<RwLock<HashMap<String, FlightStatus>>>,
    client: &Arc<Mutex<Client>>,
    flight: Flight,
    pool: &ThreadPool,
) -> Result<(), SimError> {
    {
        let mut flight_states_lock = flight_states
            .write()
            .map_err(|_| SimError::Other("LockError".to_string()))?;
        flight_states_lock.insert(flight.flight_number.clone(), flight.status.clone());
    }

    let flight_arc = Arc::new(RwLock::new(flight));

    let flight_number = {
        let read_guard = flight_arc
            .read()
            .map_err(|_| SimError::Other("LockError".to_string()))?;
        read_guard.flight_number.clone()
    };
    {
        let mut flights_lock = flights
            .write()
            .map_err(|_| SimError::Other("LockError".to_string()))?;
        flights_lock.insert(flight_number.clone(), Arc::clone(&flight_arc));
    }

    {
        let mut client_lock = client.lock().map_err(|_| SimError::ClientError)?;
        client_lock
            .insert_flight(&flight_arc.read().unwrap())
            .map_err(|_| SimError::new("Client Error: Error inserting flight into DB"))?;
    }

    start_flight_simulation(pool, flight_states, client, flight_arc);

    Ok(())
}

fn update_flight_in_simulation(
    flights: &Arc<RwLock<HashMap<String, Arc<RwLock<Flight>>>>>,
    flight_states: &Arc<RwLock<HashMap<String, FlightStatus>>>,
    flight_number: &str,
    new_status: FlightStatus,
) -> Result<(), SimError> {
    {
        let mut flight_states = flight_states
            .write()
            .map_err(|_| SimError::Other("LockError".to_string()))?;
        if let Some(status) = flight_states.get_mut(flight_number) {
            *status = new_status.clone();
        } else {
            return Err(SimError::Other(format!(
                "Flight {} not found",
                flight_number
            )));
        }
    }

    let flights_lock = flights
        .write()
        .map_err(|_| SimError::Other("LockError".to_string()))?;
    if let Some(flight_arc) = flights_lock.get(flight_number) {
        let mut flight = flight_arc
            .write()
            .map_err(|_| SimError::Other("LockError".to_string()))?;
        flight.status = new_status;
    } else {
        return Err(SimError::Other(format!(
            "Flight {} not found in simulation",
            flight_number
        )));
    }

    Ok(())
}

fn check_for_new_flights(
    client: &Arc<Mutex<Client>>,
    flight_states: &Arc<RwLock<HashMap<String, FlightStatus>>>,
    flights: &Arc<RwLock<HashMap<String, Arc<RwLock<Flight>>>>>,
    current_time: &Arc<Mutex<NaiveDateTime>>,
    airports: &Arc<RwLock<HashMap<String, Airport>>>,
    pool: &ThreadPool,
) -> Result<(), SimError> {
    let airports_lock = airports
        .read()
        .map_err(|_| SimError::Other("LockError".to_string()))?;

    // Obtener vuelos nuevos y actualizaciones desde el cliente
    let (flights_to_add, flights_to_update) = {
        let mut client_lock = client.lock().map_err(|_| SimError::ClientError)?;
        let current_time = *current_time
            .lock()
            .map_err(|_| SimError::Other("LockError".to_string()))?;
        let flight_states_lock = flight_states
            .read()
            .map_err(|_| SimError::Other("LockError".to_string()))?;

        client_lock
            .get_all_new_flights(current_time, &flight_states_lock, &airports_lock)
            .map_err(|_| SimError::ClientError)?
    };

    // Agregar vuelos nuevos a la simulación
    for flight in flights_to_add {
        add_flight_to_simulation(flights, flight_states, client, flight, pool)?;
    }

    // Actualizar vuelos existentes en la simulación
    for (flight_number, new_status) in flights_to_update {
        update_flight_in_simulation(flights, flight_states, &flight_number, new_status)?;
    }

    Ok(())
}

fn start_flight_simulation(
    pool: &ThreadPool,
    flight_states: &Arc<RwLock<HashMap<String, FlightStatus>>>,
    client: &Arc<Mutex<Client>>,
    flight: Arc<RwLock<Flight>>,
) {
    let flight_states_clone = Arc::clone(flight_states);
    let client_clone = Arc::clone(client);

    pool.execute(move || loop {
        let current_time = Utc::now().naive_utc(); // Simulación del tiempo

        {
            // Actualizar el estado del vuelo en `flight_states` y la posición
            if let Ok(mut flight_lock) = flight.write() {
                match flight_lock.status {
                    FlightStatus::Scheduled if current_time >= flight_lock.departure_time => {
                        flight_lock.update_position(current_time);
                        flight_lock.status = FlightStatus::OnTime;
                    }
                    FlightStatus::OnTime | FlightStatus::Delayed => {
                        flight_lock.update_position(current_time);

                        if current_time >= flight_lock.arrival_time {
                            flight_lock.status = FlightStatus::Finished;
                        }
                    }
                    FlightStatus::Finished | FlightStatus::Canceled => break,
                    _ => {}
                }

                // Actualizar el estado del vuelo en `flight_states`
                let flight_states_lock = flight_states_clone
                    .write()
                    .map_err(|_| SimError::Other("LockError".to_string()))
                    .ok();
                if let Some(mut flight_states) = flight_states_lock {
                    flight_states.insert(
                        flight_lock.flight_number.clone(),
                        flight_lock.status.clone(),
                    );
                }
            }
        }

        // Actualizar la base de datos
        {
            let client_lock = client_clone.lock().map_err(|_| SimError::ClientError).ok();
            if let Some(mut client) = client_lock {
                if let Err(e) = client.update_flight(&*flight.read().unwrap()) {
                    eprintln!("Failed to update flight in DB: {:?}", e);
                }
            }
        }

        thread::sleep(Duration::from_millis(1000));
    });
}
