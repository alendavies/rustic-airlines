mod airport;
mod airports;
mod flight;
pub use airport::WidgetAirport;
pub use airports::WidgetAirports;
pub use flight::WidgetFlight;

pub trait View {
    fn ui(&mut self, ui: &mut egui::Ui);
}
