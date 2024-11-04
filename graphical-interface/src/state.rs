use crate::{
    db::{Airport, Db, Flight, MockProvider, Provider},
    widgets::WidgetAirport,
};

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
    pub fn toggle_selected_flight(&self, flight: &Flight) {
        todo!()
    }
}

/// Tracks the flights and airports to display.
pub struct ViewState {
    // pub flights: Vec<Flight>,
    pub airports: Vec<Airport>,
}

impl ViewState {
    pub fn new() -> Self {
        Self {
            // TODO: pass a parameter?
            // flights: MockProvider::get_flights().unwrap(),
            airports: MockProvider::get_airports().unwrap(),
        }
    }

    // pub fn update_flights(&mut self) {
    //     // TODO: should spawn a thread to not block main thread?
    //     self.flights = MockProvider::get_flights().unwrap();
    // }
}
