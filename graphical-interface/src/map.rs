use std::{cell::RefCell, rc::Rc};

use egui::Context;
use egui_extras::install_image_loaders;
use walkers::{HttpOptions, HttpTiles, Map, MapMemory, Position, Tiles};

use crate::{
    db::Provider,
    plugins,
    state::{SelectionState, ViewState},
    widgets::{WidgetAirport, WidgetFlight},
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
    db: P,
}

impl<P: Provider> MyApp<P> {
    pub fn new(egui_ctx: Context, db: P) -> Self {
        install_image_loaders(&egui_ctx);
        let mut initial_map_memory = MapMemory::default();
        // zoom inicial para mostrar argentina y uruguay
        initial_map_memory.set_zoom(5.).unwrap();

        Self {
            tiles: Box::new(HttpTiles::with_options(
                walkers::sources::OpenStreetMap,
                HttpOptions::default(),
                egui_ctx.to_owned(),
            )),
            map_memory: initial_map_memory,
            selection_state: Rc::new(RefCell::new(SelectionState::new())),
            view_state: ViewState::new(),
            airport_widget: None,
            flight_widget: None,
            db,
        }
    }
}

impl<P: Provider> eframe::App for MyApp<P> {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
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

                // In egui, widgets are constructed and consumed in each frame.
                let map = Map::new(Some(tiles), &mut self.map_memory, my_position)
                    .with_plugin(airport_plugin)
                    .with_plugin(flight_plugin);

                // Add the map widget.
                ui.add(map);

                let selected_airport = self.selection_state.borrow().airport.clone();
                let selected_flight = self.selection_state.borrow().flight.clone();

                // Airport window.
                if let Some(airport) = selected_airport {
                    if let Some(widget) = &mut self.airport_widget {
                        if widget.selected_airport == airport {
                            if !widget.show(ctx) {
                                self.selection_state.borrow_mut().airport = None;
                                self.airport_widget = None;
                            }
                        } else {
                            self.airport_widget = Some(WidgetAirport::new(airport));
                        }
                    } else {
                        self.airport_widget = Some(WidgetAirport::new(airport));
                    }
                } else {
                    self.airport_widget = None;
                }

                // Flight window.
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

                // Draw utility windows.
                {
                    use windows::*;
                    zoom(ui, &mut self.map_memory);
                }
            });
    }
}




