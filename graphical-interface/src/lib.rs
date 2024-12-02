use db::Db;

pub mod db;
mod map;
mod plugins;
mod state;
mod widgets;
mod windows;
mod types;
use map::MyApp;

pub fn run() -> Result<(), eframe::Error> {
    eframe::run_native(
        "Flight Tracker",
        Default::default(),
        Box::new(|cc| Ok(Box::new(MyApp::new(cc.egui_ctx.clone(), Db)))),
    )
}
