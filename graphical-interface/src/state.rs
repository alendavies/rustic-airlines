use crate::{
    db::{Airport, Db, MockProvider, Provider},
    widgets::WidgetAirport,
};

pub struct AppState {
    pub displayed_airports: Vec<Airport>,
    pub airport_widget: Option<WidgetAirport>, // Add this
}

impl AppState {
    pub fn new() -> Self {
        Self {
            displayed_airports: vec![],
            airport_widget: None,
        }
    }

    pub fn init(&mut self) {
        // let initial_airports = Db::get_airports("ARG").unwrap();
        let initial_airports = MockProvider::get_airports("ARG").unwrap();

        self.displayed_airports = initial_airports;
    }

    pub fn toggle_airport_selection(&mut self, airport: &Airport) {
        if let Some(widget) = &self.airport_widget {
            if widget.selected_airport == *airport {
                self.airport_widget = None;
            } else {
                self.select_airport(airport.clone());
            }
        } else {
            self.select_airport(airport.clone());
        }
    }

    fn select_airport(&mut self, airport: Airport) {
        self.airport_widget = Some(WidgetAirport::new(airport));
    }
}
