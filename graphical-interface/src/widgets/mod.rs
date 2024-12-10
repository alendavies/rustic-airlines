mod add_flight;
mod airport;
mod flight;
mod flights_table;
pub use add_flight::WidgetAddFlight;
pub use airport::WidgetAirport;
pub use flight::WidgetFlight;
pub use flights_table::WidgetFlightsTable;

pub trait View {
    fn ui(&mut self, ui: &mut egui::Ui);
}
