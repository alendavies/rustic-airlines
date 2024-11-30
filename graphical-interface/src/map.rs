use std::{cell::RefCell, rc::Rc, time::{Duration, Instant}};

use egui::Context;
use egui_extras::install_image_loaders;
use walkers::{HttpOptions, HttpTiles, Map, MapMemory, Position, Tiles};

use crate::{
    db::Provider,
    plugins,
    state::{SelectionState, ViewState},
    widgets::{WidgetAddFlight, WidgetAirport, WidgetFlight},
    windows,
};

const INITIAL_LAT: f64 = -34.608406;
const INITIAL_LON: f64 = -58.372159;

pub struct MyApp<P: Provider> {
    tiles: Box<dyn Tiles>,
    map_memory: MapMemory,
    selection_state: Rc<RefCell<SelectionState>>,
    view_state: ViewState,
    airport_widget: Option<WidgetAirport>,
    flight_widget: Option<WidgetFlight>,
    add_flight_widget: Option<WidgetAddFlight>,
    db: P,
    last_update: Instant,
    update_interval: Duration, 
}

impl<P: Provider> MyApp<P> {
    pub fn new(egui_ctx: Context, db: P) -> Self {
        install_image_loaders(&egui_ctx);
        let mut initial_map_memory = MapMemory::default();
        initial_map_memory.set_zoom(5.).unwrap();

        Self {
            tiles: Box::new(HttpTiles::with_options(
                walkers::sources::OpenStreetMap,
                HttpOptions::default(),
                egui_ctx.to_owned(),
            )),
            map_memory: initial_map_memory,
            selection_state: Rc::new(RefCell::new(SelectionState::new())),
            view_state: ViewState::new(
                vec![],
                P::get_airports().unwrap_or_default(),
            ),
            airport_widget: None,
            flight_widget: None,
            add_flight_widget: None,
            db,
            last_update: Instant::now(),
            update_interval: Duration::from_secs(20), // Actualiza cada 20 segundos
        }
    }

    fn maybe_update_view_state(&mut self) {
        if self.last_update.elapsed() >= self.update_interval {
            self.last_update = Instant::now();
            self.view_state.update_airports(&self.db);
            if let Some(selected_airport) = &self.selection_state.borrow().airport {
                if let Ok(new_flights) = P::get_flights_by_airport(&selected_airport.iata) {
                    self.view_state.flights = new_flights;
                }
            }
        }
    }
}

impl<P: Provider> eframe::App for MyApp<P> {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.maybe_update_view_state(); // Periodic update for flights based on the selected airport.

        let rimless = egui::Frame {
            fill: ctx.style().visuals.panel_fill,
            ..Default::default()
        };

        egui::CentralPanel::default()
            .frame(rimless)
            .show(ctx, |ui| {
                let my_position = Position::from_lat_lon(INITIAL_LAT, INITIAL_LON);

                let tiles = self.tiles.as_mut();

                let airport_plugin =
                    plugins::Airports::new(&self.view_state.airports, self.selection_state.clone());

                let flight_plugin = 
                    plugins::Flights::new(&self.view_state.flights, self.selection_state.clone());

                let map = Map::new(Some(tiles), &mut self.map_memory, my_position)
                    .with_plugin(airport_plugin)
                    .with_plugin(flight_plugin);

                ui.add(map);

                let selected_airport = self.selection_state.borrow().airport.clone();
                if let Some(airport) = selected_airport {
                    if let Some(widget) = &mut self.airport_widget {
                        if widget.selected_airport == airport {
                            if !widget.show(ctx) {
                                self.selection_state.borrow_mut().airport = None;
                                self.airport_widget = None;
                                self.view_state.flights.clear(); 
                            }
                        } else {
                            self.airport_widget = Some(WidgetAirport::new(airport.clone()));
                            if let Ok(new_flights) = P::get_flights_by_airport(&airport.iata) {
                                self.view_state.flights = new_flights;
                            }
                            self.selection_state.borrow_mut().flight = None;
                            self.flight_widget = None;
                        }
                    } else {
                        self.airport_widget = Some(WidgetAirport::new(airport.clone()));
                        if let Ok(new_flights) = P::get_flights_by_airport(&airport.iata) {
                            self.view_state.flights = new_flights;
                        }
                        self.selection_state.borrow_mut().flight = None;
                        self.flight_widget = None;
                    }
                } else {
                    self.airport_widget = None;
                }

                let selected_flight = self.selection_state.borrow().flight.clone();
                if let Some(flight) = selected_flight {
                    if let Some(widget) = &mut self.flight_widget {
                        if widget.selected_flight == flight {
                            if !widget.show(ctx) {
                                self.selection_state.borrow_mut().flight = None;
                                self.flight_widget = None;
                            }
                        } else {
                            self.flight_widget = None;
                        }
                    } else {
                        self.flight_widget = Some(WidgetFlight::new(flight));
                    }
                } else {
                    self.flight_widget = None;
                }

                let _button_response = egui::Area::new("add_flight_button".into())
                    .anchor(egui::Align2::RIGHT_BOTTOM, [-10.0, -10.0])
                    .show(ctx, |ui| {
                        // Tamaño personalizado para el botón
                        let button_size = [150.0, 60.0]; // Ajusta el tamaño del botón aquí

                        if ui.add_sized(button_size, egui::Button::new("Add Flight").rounding(10.0)).clicked() {
                            self.add_flight_widget = Some(WidgetAddFlight::new());
                        }
                    });

                // Mostrar el widget "Add Flight" si está activo
                if let Some(widget) = &mut self.add_flight_widget {
                    if !widget.show(ctx, &self.db, &self.view_state.airports) {
                        self.add_flight_widget = None; 
                    }
                }

                {
                    use windows::*;
                    zoom(ui, &mut self.map_memory);
                }
            });
    }
}




