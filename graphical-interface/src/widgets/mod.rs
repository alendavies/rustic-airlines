mod airport;
mod airports;
pub use airport::WidgetAirport;
pub use airports::WidgetAirports;

pub trait View {
    fn ui(&mut self, ui: &mut egui::Ui);
}
