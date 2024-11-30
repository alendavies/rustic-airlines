mod airport;
mod add_flight;
mod flight;
mod flights_table;
pub use airport::WidgetAirport;
pub use add_flight::WidgetAddFlight;
pub use flight::WidgetFlight;
pub use flights_table::WidgetFlightsTable;

pub trait View {
    fn ui(&mut self, ui: &mut egui::Ui);
}
