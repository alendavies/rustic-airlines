use egui_extras::{Column, TableBuilder};

use crate::db::{Airport, Db, Flight, Provider};

use super::View;

#[derive(PartialEq)]
enum Tabs {
    Info,
    Departures,
    Arrivals,
}

struct WidgetDepartures {
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
            self.departures =
                Some(Db::get_departure_flights(&self.airport, self.selected_date).unwrap());
        }

        ui.vertical(|ui| {
            let date_response = ui.add(egui_extras::DatePickerButton::new(&mut self.selected_date));

            if date_response.changed() {
                // TODO: find a way to do it async, with promises or something:
                // https://github.com/emilk/egui/blob/5b846b4554fe47269affb43efef2cad8710a8a47/crates/egui_demo_app/src/apps/http_app.rs
                self.departures =
                    Some(Db::get_departure_flights(&self.airport, self.selected_date).unwrap());
                // self.departures = Some(db::get_departures_mock(
                //     self.airport.clone(),
                //     self.selected_date,
                // ));
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
                Some(Db::get_departure_flights(&self.airport, self.selected_date).unwrap());
        }

        ui.vertical(|ui| {
            let date_response = ui.add(egui_extras::DatePickerButton::new(&mut self.selected_date));

            if date_response.changed() {
                self.arrivals =
                    Some(Db::get_arrival_flights(&self.airport, self.selected_date).unwrap());
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
}

pub struct WidgetAirport {
    pub selected_airport: Airport,
    widget_departures: WidgetDepartures,
    widget_arrivals: WidgetArrivals,
    open_tab: Tabs,
}

impl WidgetAirport {
    pub fn new(selected_airport: Airport) -> Self {
        Self {
            // TODO: should actually receive a reference to the airport
            selected_airport: selected_airport.clone(),
            open_tab: Tabs::Info,
            widget_arrivals: WidgetArrivals::new(selected_airport.iata.clone()),
            widget_departures: WidgetDepartures::new(selected_airport.iata.clone()),
        }
    }
}

impl WidgetAirport {
    pub fn show(&mut self, ctx: &egui::Context) {
        egui::Window::new(format!("Aeropuerto {}", self.selected_airport.name))
            .resizable(false)
            .collapsible(false)
            .movable(false)
            // TODO: find the way to make the widgets fill the space one after the other
            .fixed_pos([20., 600.])
            .show(ctx, |ui| {
                ui.horizontal(|ui| {
                    ui.selectable_value(&mut self.open_tab, Tabs::Info, "Info");
                    ui.selectable_value(&mut self.open_tab, Tabs::Departures, "Departures");
                    ui.selectable_value(&mut self.open_tab, Tabs::Arrivals, "Arrivals");
                });

                match self.open_tab {
                    Tabs::Info => ui.vertical(|ui| {
                        ui.label(format!("Código IATA: {}", self.selected_airport.iata));
                        ui.label(format!("Nombre: {}", self.selected_airport.name));
                    }),
                    Tabs::Departures => ui.vertical(|ui| {
                        self.widget_departures.ui(ui);
                    }),
                    Tabs::Arrivals => ui.vertical(|ui| {
                        self.widget_arrivals.ui(ui);
                    }),
                }
            });
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
