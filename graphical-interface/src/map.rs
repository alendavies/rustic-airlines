use std::{cell::RefCell, rc::Rc};

use egui::Context;
use egui_extras::install_image_loaders;
use walkers::{HttpOptions, HttpTiles, Map, MapMemory, Position, Tiles};

use crate::{
    db::Provider,
    plugins,
    state::{SelectionState, ViewState},
    types::{CountryTracker, MapBounds},
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
    last_update: f64,
    country_tracker: CountryTracker,
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
            view_state: ViewState::new(vec![], P::get_airports().unwrap_or_default()),
            airport_widget: None,
            flight_widget: None,
            add_flight_widget: None,
            db,
            last_update: 0.0,
            country_tracker: CountryTracker::new(),
        }
    }
}

impl<P: Provider> eframe::App for MyApp<P> {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        let map_bounds = calculate_map_bounds(&self.map_memory);
        self.country_tracker.update_visible_countries(&map_bounds);

        if ctx.input(|i| i.time - self.last_update) >= 1.0 {
            self.view_state.update_airports(&self.db);

            if let Some(selected_airport) = &self.selection_state.borrow().airport {
                self.view_state.update_flights(&self.db, selected_airport);
            }

            // Update last update time
            self.last_update = ctx.input(|i| i.time);
        }

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
                            self.view_state.update_flights(&self.db, &airport);
                            self.selection_state.borrow_mut().flight = None;
                            self.flight_widget = None;
                        }
                    } else {
                        self.airport_widget = Some(WidgetAirport::new(airport.clone()));
                        self.view_state.update_flights(&self.db, &airport);
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
                        let button_size = [150.0, 60.0];

                        if ui
                            .add_sized(button_size, egui::Button::new("Add Flight").rounding(10.0))
                            .clicked()
                        {
                            self.add_flight_widget = Some(WidgetAddFlight::new());
                        }
                    });

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

fn calculate_map_bounds(map_memory: &MapMemory) -> MapBounds {
    let center_pos = match map_memory.detached() {
        Some(pos) => pos,
        None => Position::from_lat_lon(INITIAL_LAT, INITIAL_LON), // Fallback to initial position
    };

    let zoom = map_memory.zoom();

    // Rough calculation of visible area based on zoom
    // These multipliers are approximate and may need tuning
    let lat_span = 180.0 * (0.4 / zoom);
    let lon_span = 300.0 * (0.4 / zoom);

    MapBounds {
        min_lat: center_pos.lat() - (lat_span / 2.0),
        max_lat: center_pos.lat() + (lat_span / 2.0),
        min_lon: center_pos.lon() - (lon_span / 2.0),
        max_lon: center_pos.lon() + (lon_span / 2.0),
    }
}
