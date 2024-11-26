use crate::db::{Airport, Db, Flight, Provider};

/// Tracks the state for the selection of flights and airports.
pub struct SelectionState {
    pub flight: Option<Flight>,
    pub airport: Option<Airport>,
}

impl SelectionState {
    pub fn new() -> SelectionState {
        Self {
            flight: None,
            airport: None,
        }
    }

    /// If the provided airport is already selected, it will be deselected.
    /// Otherwise, it will be selected.
    pub fn toggle_airport_selection<P: Provider>(
        &mut self,
        airport: &Airport,
        view_state: &mut ViewState,
        db: &P,
    ) {
        if let Some(selected_airport) = &self.airport {
            if *selected_airport == *airport {
                self.airport = None;
                view_state.clear_flights(); // Clear flights if deselecting the airport
            } else {
                self.airport = Some(airport.clone());
                view_state.update_flights_by_airport(airport, db); // Load flights for the new airport
            }
        } else {
            self.airport = Some(airport.clone());
            view_state.update_flights_by_airport(airport, db);
        }
    }

    /* /// If the provided flight is already selected, it will be deselected.
    /// Otherwise, it will be selected.
    pub fn toggle_flight_selection(&mut self, flight: &Flight) {
        if let Some(selected_flight) = &self.flight {
            if *selected_flight == *flight {
                self.flight = None;
            } else {
                self.flight = Some(flight.clone());
            }
        } else {
            self.flight = Some(flight.clone());
        }
    } */
}

/// Tracks the flights and airports to display.
pub struct ViewState {
    pub flights: Vec<Flight>,
    pub airports: Vec<Airport>,
}

impl ViewState {
    pub fn new(flights: Vec<Flight>, airports: Vec<Airport>) -> Self {
        Self { flights, airports }
    }

    pub fn update_flights_by_airport<P: Provider>(&mut self, airport: &Airport, _db: &P) {
        if let Ok(flights) = P::get_flights_by_airport(&airport.iata) {
            self.flights = flights; // Replace the flights vector
        }
    }

    pub fn clear_flights(&mut self) {
        self.flights.clear(); // Clear flights when no airport is selected
    }

    pub fn update_airports<P: Provider>(&mut self, _db: &P) {
        if let Ok(airports) = P::get_airports() {
            self.airports = airports; 
        }
    }
}