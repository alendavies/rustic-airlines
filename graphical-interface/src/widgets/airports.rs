use egui::Widget;
use egui_extras::{Column, TableBuilder};

use crate::state::AppState;

pub struct WidgetAirports<'a> {
    // airports: Vec<Airport>,
    pub app_state: &'a mut AppState,
}

impl Widget for WidgetAirports<'_> {
    fn ui(self, ui: &mut egui::Ui) -> egui::Response {
        let response = ui.allocate_response(egui::vec2(0., 0.), egui::Sense::hover());

        egui::Window::new("Airports")
            .resizable(false)
            .movable(false)
            .collapsible(false)
            .show(ui.ctx(), |ui| {
                egui::ScrollArea::vertical().show(ui, |ui| {
                    TableBuilder::new(ui)
                        .column(Column::auto())
                        .column(Column::remainder())
                        .sense(egui::Sense::click())
                        .header(20.0, |mut header| {
                            header.col(|ui| {
                                ui.strong("Code");
                            });
                            header.col(|ui| {
                                ui.strong("Name");
                            });
                        })
                        .body(|mut body| {
                            for airport in &self.app_state.displayed_airports.clone() {
                                body.row(18.0, |mut row| {
                                    row.set_selected(
                                        self.app_state
                                            .selected_airport
                                            .as_ref()
                                            .is_some_and(|a| a == airport),
                                    );

                                    row.col(|ui| {
                                        ui.label(&airport.iata);
                                    });

                                    row.col(|ui| {
                                        ui.label(&airport.name);
                                    });

                                    if row.response().clicked() {
                                        dbg!("clicked!");
                                        self.app_state.toggle_airport_selection(airport);
                                    }
                                });
                            }
                        });
                });
            });

        response
    }
}
