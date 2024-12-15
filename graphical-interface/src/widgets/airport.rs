use crate::{db::Db, types::Airport};

use super::{flights_table::FlightType, View, WidgetFlightsTable};

/// Represents the selectable tabs for displaying flight information in the airport widget.
///
#[derive(PartialEq)]
enum Tabs {
    Departures,
    Arrivals,
}

/// A widget for displaying information about a specific airport.
///
/// This widget includes airport details (such as its IATA code and country) and
/// provides functionality to view flights categorized into departures and arrivals.
pub struct WidgetAirport {
    pub selected_airport: Airport,
    widget_departures: WidgetFlightsTable,
    widget_arrivals: WidgetFlightsTable,
    open_tab: Tabs,
}

impl WidgetAirport {
    /// Creates a new `WidgetAirport` for a given airport.
    pub fn new(selected_airport: Airport) -> Self {
        let iata_code = selected_airport.iata.clone();
        Self {
            selected_airport,
            open_tab: Tabs::Departures,
            widget_arrivals: WidgetFlightsTable::new(iata_code.clone(), FlightType::Arrival),
            widget_departures: WidgetFlightsTable::new(iata_code, FlightType::Departure),
        }
    }
}

impl WidgetAirport {
    /// This method shows a window with details about the selected airport,
    /// including a selector for viewing either departure or arrival flights.
    pub fn show(&mut self, ctx: &egui::Context, db: &mut Db) -> bool {
        let mut open = true;

        egui::Window::new(format!("Aeropuerto {}", self.selected_airport.name))
            .resizable(false)
            .collapsible(true)
            .open(&mut open)
            .fixed_pos([20.0, 20.0])
            .show(ctx, |ui| {
                ui.add_space(10.0);

                ui.visuals_mut().override_text_color = Some(egui::Color32::WHITE);
                ui.visuals_mut().widgets.noninteractive.bg_fill = egui::Color32::from_gray(30);
                ui.vertical(|ui| {
                    ui.label(
                        egui::RichText::new(format!("Código IATA: {}", self.selected_airport.iata))
                            .size(16.0),
                    );
                    ui.label(
                        egui::RichText::new(format!("País: {}", self.selected_airport.country))
                            .size(16.0),
                    );
                });

                ui.add_space(15.0);

                ui.horizontal(|ui| {
                    ui.label(
                        egui::RichText::new("Información de vuelos en:")
                            .size(18.0)
                            .strong(),
                    );
                    egui::ComboBox::from_label("")
                        .selected_text(match self.open_tab {
                            Tabs::Departures => "Salida",
                            Tabs::Arrivals => "Llegada",
                        })
                        .show_ui(ui, |ui| {
                            ui.selectable_value(&mut self.open_tab, Tabs::Departures, "Salidas");
                            ui.selectable_value(&mut self.open_tab, Tabs::Arrivals, "Llegadas");
                        });
                });

                ui.add_space(10.0);

                match self.open_tab {
                    Tabs::Departures => ui.vertical_centered(|ui| {
                        self.widget_departures.ui(ui, db);
                    }),
                    Tabs::Arrivals => ui.vertical_centered(|ui| {
                        self.widget_arrivals.ui(ui, db);
                    }),
                }
            });

        open
    }

    // fn ui(self, ui: &mut egui::Ui) -> egui::Response {
    //     let response = ui.allocate_response(egui::vec2(0., 0.), egui::Sense::hover());

    //     egui::Window::new(format!(
    //         "Aeropuerto {}",
    //         self.selected_airport
    //             .as_ref()
    //             .and_then(|x| Some(x.name.clone()))
    //             .unwrap_or_default()
    //     ))
    //     .resizable(false)
    //     .collapsible(false)
    //     .movable(false)
    //     .fixed_pos([20., 150.])
    //     // .open(&mut self.selected_airport.is_some())
    //     .open(&mut self.window_open)
    //     .show(ui.ctx(), |ui| {
    //         // egui::ScrollArea::vertical().show(ui, |ui| {
    //         ui.horizontal(|ui| {
    //             ui.selectable_value(&mut self.open_tab, Tabs::Info, "Info");
    //             ui.selectable_value(&mut self.open_tab, Tabs::Departures, "Departures");
    //             ui.selectable_value(&mut self.open_tab, Tabs::Arrivals, "Arrivals");
    //         });
    //         // });
    //     });

    //     response
    // }
}

/* struct WidgetDepartures {
    airport: String,
    selected_date: chrono::NaiveDate,
    departures: Option<Vec<Flight>>,
}

impl WidgetDepartures {
    pub fn new(from: String) -> Self {
        Self {
            airport: from,
            departures: None,
            selected_date: chrono::offset::Utc::now().date_naive(),
        }
    }
}

impl View for WidgetDepartures {
    fn ui(&mut self, ui: &mut egui::Ui) {
        if self.departures.is_none() {
            self.departures = Some(
                MockProvider::get_departure_flights(&self.airport, self.selected_date).unwrap(),
            );
        }

        ui.vertical(|ui| {
            let date_response = ui.add(egui_extras::DatePickerButton::new(&mut self.selected_date));

            if date_response.changed() {
                // TODO: find a way to do it async, with promises or something:
                // https://github.com/emilk/egui/blob/5b846b4554fe47269affb43efef2cad8710a8a47/crates/egui_demo_app/src/apps/http_app.rs
                self.departures = Some(
                    MockProvider::get_departure_flights(&self.airport, self.selected_date).unwrap(),
                );
            }

            if let Some(flights) = &self.departures {
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
                                    // ui.label(match &flight.status {
                                    //     db::FlightStatus::Scheduled => "Programado",
                                    //     db::FlightStatus::OnTime => "En Horario",
                                    //     db::FlightStatus::Boarding => "Embarcando",
                                    //     db::FlightStatus::Canceled => "Cancelado",
                                    //     db::FlightStatus::Delayed => "Demorado",
                                    //     db::FlightStatus::Landing => "Aterrizando",
                                    // });
                                });
                            });
                        }
                    });
            } else {
                ui.label("No hay vuelos.");
            }

            // egui::ScrollArea::vertical().show(ui, |ui| {
            // TableBuilder::new(ui)
            //     .column(Column::auto())
            //     .column(Column::remainder())
            //     .sense(egui::Sense::click())
            //     .header(20., |mut header| {
            //         // header.col(|ui| ui.strong("Vuelo"));
            //         // header.col(|ui| ui.strong("Estado"));
            //     })
            // });
        });
    }
}

struct WidgetArrivals {
    airport: String,
    selected_date: chrono::NaiveDate,
    arrivals: Option<Vec<Flight>>,
}

impl WidgetArrivals {
    pub fn new(from: String) -> Self {
        Self {
            airport: from,
            arrivals: None,
            selected_date: chrono::offset::Utc::now().date_naive(),
        }
    }
}

impl View for WidgetArrivals {
    fn ui(&mut self, ui: &mut egui::Ui) {
        if self.arrivals.is_none() {
            self.arrivals =
                Some(MockProvider::get_arrival_flights(&self.airport, self.selected_date).unwrap());
        }

        ui.vertical(|ui| {
            let date_response = ui.add(egui_extras::DatePickerButton::new(&mut self.selected_date));

            if date_response.changed() {
                self.arrivals = Some(
                    MockProvider::get_arrival_flights(&self.airport, self.selected_date).unwrap(),
                );
            }

            if let Some(flights) = &self.arrivals {
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
} */
