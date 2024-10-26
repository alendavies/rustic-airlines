mod db;
mod map;
mod plugins;
mod windows;
use map::MyApp;

fn main() -> Result<(), eframe::Error> {
    eframe::run_native(
        "Flight Tracker",
        Default::default(),
        Box::new(|cc| Ok(Box::new(MyApp::new(cc.egui_ctx.clone())))),
    )
}
