use egui::Context;
use walkers::{HttpOptions, HttpTiles, Map, MapMemory, Position, Tiles};

use crate::{
    plugins,
    state::{SelectionState, ViewState},
    widgets::{WidgetAirport, WidgetAirports},
    windows,
};

const INITIAL_LAT: f64 = -34.608406;
const INITIAL_LON: f64 = -58.372159;

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
    selection_state: SelectionState,
    view_state: ViewState,
    airport_widget: Option<WidgetAirport>,
}

impl MyApp {
    pub fn new(egui_ctx: Context) -> Self {
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
            selection_state: SelectionState::new(),
            view_state: ViewState::new(),
            airport_widget: None,
        }
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
                let my_position = Position::from_lat_lon(INITIAL_LAT, INITIAL_LON);

                let tiles = self.tiles.as_mut();

                // In egui, widgets are constructed and consumed in each frame.
                let map = Map::new(Some(tiles), &mut self.map_memory, my_position)
                    .with_plugin(plugins::Airports::new(&self.view_state.airports));

                // Draw the map widget.
                ui.add(map);
                // ui.add(WidgetAirports {
                //     app_state: &mut self.app_state,
                // });

                ui.add(WidgetAirports::new(
                    &self.view_state,
                    &mut self.selection_state,
                ));

                if let Some(widget) = &mut self.airport_widget {
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
