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
    pub fn toggle_airport_selection(&mut self, airport: &Airport) {
        if let Some(selected_airport) = &self.airport {
            if *selected_airport == *airport {
                self.airport = None;
            } else {
                self.airport = Some(airport.clone());
            }
        } else {
            self.airport = Some(airport.clone());
        }
    }

    /// If the provided flight is already selected, it will be deselected.
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
    }
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

    pub fn update<P: Provider>(&mut self, _db: &P) {
        if let Ok(new_flights) = P::get_flights() {
            self.flights = new_flights;
        }
        if let Ok(new_airports) = P::get_airports() {
            self.airports = new_airports;
        }
    }
}