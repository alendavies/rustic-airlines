use chrono::{NaiveDateTime, TimeZone, Utc};
use egui::{Color32, RichText};

use crate::db::Flight;

pub struct WidgetFlight {
    pub selected_flight: Flight,
}

impl WidgetFlight {
    pub fn new(selected_flight: Flight) -> Self {
        Self { selected_flight }
    }

    pub fn show(&mut self, ctx: &egui::Context) {
        egui::Window::new(format!("Flight: {}", self.selected_flight.number))
            .resizable(false)
            .movable(false)
            .collapsible(false)
            .fixed_pos([20., 20.])
            .show(ctx, |ui| {
                egui::ScrollArea::vertical().show(ui, |ui| {
                    ui.label(
                        RichText::new(format!("Flight: {}", self.selected_flight.number))
                            .strong()
                            .size(24.0)
                            .color(Color32::from_rgb(0, 150, 255)),
                    );
                    ui.label(
                        RichText::new(format!("Status: {}", self.selected_flight.status))
                            .size(18.0)
                            .color(Color32::from_rgb(0, 255, 0)),
                    );
                    ui.separator();
    
                    ui.label(RichText::new("Flight Information").strong().size(20.0));
                    ui.horizontal(|ui| {
                        ui.label(RichText::new("Departure:").size(16.0).strong());
                        if let Some(departure_time) = Utc.timestamp_opt(self.selected_flight.departure_time, 0).single() {
                            ui.label(
                                RichText::new(format!(
                                    "{} - {}",
                                    self.selected_flight.origin_airport,
                                    departure_time.format("%Y-%m-%d %H:%M:%S")
                                ))
                                .size(16.0),
                            );
                        } else {
                            ui.label(format!("{} - Invalid timestamp", self.selected_flight.origin_airport));
                        }
                    });
                    ui.horizontal(|ui| {
                        ui.label(RichText::new("Arrival:").size(16.0).strong());
                        if let Some(arrival_time) = Utc.timestamp_opt(self.selected_flight.arrival_time, 0).single() {
                            ui.label(
                                RichText::new(format!(
                                    "{} - {}",
                                    self.selected_flight.destination_airport,
                                    arrival_time.format("%Y-%m-%d %H:%M:%S")
                                ))
                                .size(16.0),
                            );
                        } else {
                            ui.label(format!("{} - Invalid timestamp", self.selected_flight.destination_airport));
                        }
                    });
                    ui.add_space(10.0);
                    ui.separator();
    
                    ui.label(RichText::new("Position Information").strong().size(20.0));
                    ui.horizontal(|ui| {
                        ui.label(RichText::new("Altitude:").size(16.0).strong());
                        ui.label(RichText::new(format!("{} m", self.selected_flight.height)).size(16.0));
                    });
                    ui.horizontal(|ui| {
                        ui.label(RichText::new("Speed:").size(16.0).strong());
                        ui.label(RichText::new(format!("{} km/h", self.selected_flight.speed)).size(16.0));
                    });
                    ui.horizontal(|ui| {
                        ui.label(RichText::new("Position:").size(16.0).strong());
                        ui.label(
                            RichText::new(format!(
                                "Latitude: {:.4}, Longitude: {:.4}",
                                self.selected_flight.position.lat(),
                                self.selected_flight.position.lon()
                            ))
                            .size(16.0),
                        );
                    });
                    ui.horizontal(|ui| {
                        ui.label(RichText::new("Heading:").size(16.0).strong());
                        ui.label(RichText::new(format!("{}°", self.selected_flight.heading)).size(16.0));
                    });
                    ui.add_space(10.0);
                    ui.separator();
    
                    ui.label(RichText::new("Fuel Information").strong().size(20.0));
                    ui.horizontal(|ui| {
                        ui.label(RichText::new("Fuel Level:").size(16.0).strong());
                        ui.label(
                            RichText::new(format!("{:.2}%", self.selected_flight.fuel * 100.0))
                                .size(16.0)
                                .color(Color32::from_rgb(255, 100, 100)), // Red color for emphasis
                        );
                    });
                    ui.add_space(10.0);
                });
            });
    }
}
