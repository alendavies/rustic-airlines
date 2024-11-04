use crate::db::Flight;

pub struct WidgetFlight {
    pub selected_flight: Flight,
}

impl WidgetFlight {
    pub fn new(selected_flight: Flight) -> Self {
        Self { selected_flight }
    }

    pub fn show(&mut self, ctx: &egui::Context) {
        egui::Window::new(format!("Vuelo {}", self.selected_flight.number)).show(ctx, |ui| {});
    }
}
