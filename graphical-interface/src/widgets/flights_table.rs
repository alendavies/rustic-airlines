use egui_extras::{Column, TableBuilder};
use crate::db::{Flight, MockProvider, Provider};
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
            FlightType::Arrival => MockProvider::get_arrival_flights(&self.airport, self.selected_date).unwrap(),
            FlightType::Departure => MockProvider::get_departure_flights(&self.airport, self.selected_date).unwrap(),
        });
    }
}

impl View for WidgetFlightsTable {
    fn ui(&mut self, ui: &mut egui::Ui) {
        if self.flights.is_none() {
            self.fetch_flights();
        }

        ui.vertical(|ui| {
            let date_response = ui.add(egui_extras::DatePickerButton::new(&mut self.selected_date));

            if date_response.changed() {
                self.fetch_flights();
            }

            if let Some(flights) = &self.flights {
                TableBuilder::new(ui)
                    .column(Column::auto())
                    .column(Column::remainder())
                    .sense(egui::Sense::click())
                    .header(20., |mut header| {
                        header.col(|ui| {
                            ui.strong("Vuelo");
                        });
                        header.col(|ui| {
                            ui.strong("Estado");
                        });
                    })
                    .body(|mut body| {
                        for flight in flights {
                            body.row(18., |mut row| {
                                row.col(|ui| {
                                    ui.label(&flight.number);
                                });
                                row.col(|ui| {
                                    ui.label(&flight.status);
                                });
                            });
                        }
                    });
            } else {
                ui.label("No hay vuelos.");
            }
        });
    }
}