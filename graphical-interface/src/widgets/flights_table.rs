use egui_extras::{Column, TableBuilder};
use crate::db::{Db, Flight, Provider};
use super::View;

pub enum FlightType {
    Arrival,
    Departure,
}

pub struct WidgetFlightsTable {
    airport: String,
    selected_date: chrono::NaiveDate,
    flights: Option<Vec<Flight>>,
    flight_type: FlightType,
}

impl WidgetFlightsTable {
    pub fn new(airport: String, flight_type: FlightType) -> Self {
        Self {
            airport,
            selected_date: chrono::offset::Utc::now().date_naive(),
            flights: None,
            flight_type,
        }
    }

    fn fetch_flights(&mut self) {
        self.flights = Some(match self.flight_type {
            FlightType::Arrival => Db::get_arrival_flights(&self.airport, self.selected_date).unwrap(),
            FlightType::Departure => Db::get_departure_flights(&self.airport, self.selected_date).unwrap(),
        });
    }
}

impl View for WidgetFlightsTable {
    fn ui(&mut self, ui: &mut egui::Ui) {
        if self.flights.is_none() {
            self.fetch_flights();
        }

        ui.vertical_centered(|ui| {
            // Etiqueta de Fecha con espacio adicional
            ui.horizontal(|ui| {
                ui.label(
                    egui::RichText::new("Fecha:")
                        .size(16.0)
                        .strong()
                        .color(egui::Color32::WHITE),
                );
                let date_response = ui.add(egui_extras::DatePickerButton::new(&mut self.selected_date));
                
                if date_response.changed() {
                    self.fetch_flights();
                }
            });

            ui.add_space(10.0); // Espacio entre la fecha y la tabla

            // Tabla de vuelos con estilo mejorado
            if let Some(flights) = &self.flights {
                ui.group(|ui| {

                    TableBuilder::new(ui)
                        .striped(true) // Alterna colores en filas
                        .cell_layout(egui::Layout::left_to_right(egui::Align::Center))
                        .column(Column::remainder().at_least(100.0))
                        .column(Column::remainder().at_least(100.0))
                        .header(25.0, |mut header| {
                            header.col(|ui| {
                                ui.strong(
                                    egui::RichText::new("Vuelo")
                                        .color(egui::Color32::YELLOW)
                                        .size(16.0),
                                );
                            });
                            header.col(|ui| {
                                ui.strong(
                                    egui::RichText::new("Estado")
                                        .color(egui::Color32::YELLOW)
                                        .size(16.0),
                                );
                            });
                        })
                        .body(|mut body| {
                            for flight in flights {
                                body.row(20.0, |mut row| {
                                    row.col(|ui| {
                                        ui.label(
                                            egui::RichText::new(&flight.number)
                                                .color(egui::Color32::WHITE)
                                                .size(14.0),
                                        );
                                    });
                                    row.col(|ui| {
                                        ui.horizontal(|ui| {
                                            ui.label(
                                                egui::RichText::new(&flight.status)
                                                    .color(egui::Color32::WHITE)
                                                    .size(14.0),
                                            );
                                        });
                                    });
                                });
                            }
                        });
                });
            } else {
                ui.label("No hay vuelos.");
            }
        });
    }
}

