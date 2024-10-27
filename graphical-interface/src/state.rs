use crate::db::{get_airports_mock, Airport};

pub struct AppState {
    pub displayed_airports: Vec<Airport>,
    pub selected_airport: Option<Airport>, // client: CassandraClient,
}

impl AppState {
    pub fn new() -> Self {
        // let mut client =
        // CassandraClient::connect(Ipv4Addr::from_str("127.0.0.1").unwrap()).unwrap();

        // client.startup().unwrap();

        Self {
            displayed_airports: vec![],
            selected_airport: None, // client,
        }
    }

    pub fn init(&mut self) {
        // let initial_aiports = get_airports(&mut self.client);
        let initial_aiports = get_airports_mock();

        self.displayed_airports = initial_aiports;
    }

    pub fn toggle_airport_selection(&mut self, airport: &Airport) {
        self.selected_airport = if self.selected_airport.as_ref().is_some_and(|a| a == airport) {
            None
        } else {
            Some(airport.clone())
        }
    }
}
