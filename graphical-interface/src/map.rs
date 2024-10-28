use egui::Context;
use walkers::{HttpOptions, HttpTiles, Map, MapMemory, Position, Tiles};

use crate::{
    db::{Airport, Db},
    plugins,
    state::AppState,
    widgets::WidgetAirports,
    windows,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Provider {
    OpenStreetMap,
    Geoportal,
    MapboxStreets,
    MapboxSatellite,
    LocalTiles,
}

pub struct MyApp {
    tiles: Box<dyn Tiles>,
    selected_provider: Provider,
    map_memory: MapMemory,
    app_state: AppState,
}

impl MyApp {
    pub fn new(egui_ctx: Context) -> Self {
        // create and initialize the AppState (to show some airports)
        let mut initial_state = AppState::new();
        initial_state.init();

        let mut initial_map_memory = MapMemory::default();
        // zoom inicial para mostrar argentina y uruguay
        initial_map_memory.set_zoom(5.).unwrap();

        Self {
            tiles: Box::new(HttpTiles::with_options(
                walkers::sources::OpenStreetMap,
                HttpOptions::default(),
                egui_ctx.to_owned(),
            )),
            selected_provider: Provider::OpenStreetMap,
            map_memory: initial_map_memory,
            app_state: initial_state,
        }
    }

    fn airports(&self) -> Vec<Airport> {
        self.app_state.displayed_airports.clone()
    }
}

impl eframe::App for MyApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        let rimless = egui::Frame {
            fill: ctx.style().visuals.panel_fill,
            ..Default::default()
        };

        egui::CentralPanel::default()
            .frame(rimless)
            .show(ctx, |ui| {
                // centrar en pza de mayo
                let my_position = Position::from_lat_lon(-34.608406, -58.372159);
                let airports = self.airports();

                let tiles = self.tiles.as_mut();

                // In egui, widgets are constructed and consumed in each frame.
                let map = Map::new(Some(tiles), &mut self.map_memory, my_position)
                    .with_plugin(plugins::Airports::new(airports.clone()));

                // Draw the map widget.
                ui.add(map);
                ui.add(WidgetAirports {
                    app_state: &mut self.app_state,
                });

                if let Some(widget) = &mut self.app_state.airport_widget {
                    widget.show(ctx);
                }

                // Draw utility windows.
                {
                    use windows::*;

                    zoom(ui, &mut self.map_memory);
                }
            });
    }
}
