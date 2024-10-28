use crate::{
    db::{get_airports_mock, Airport},
    widgets::WidgetAirport,
};

pub struct AppState {
    pub displayed_airports: Vec<Airport>,
    pub airport_widget: Option<WidgetAirport>, // Add this
}

impl AppState {
    pub fn new() -> Self {
        // let mut client =
        // CassandraClient::connect(Ipv4Addr::from_str("127.0.0.1").unwrap()).unwrap();

        // client.startup().unwrap();

        Self {
            displayed_airports: vec![],
            airport_widget: None,
        }
    }

    pub fn init(&mut self) {
        // let initial_aiports = get_airports(&mut self.client);
        let initial_aiports = get_airports_mock();

        self.displayed_airports = initial_aiports;
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
