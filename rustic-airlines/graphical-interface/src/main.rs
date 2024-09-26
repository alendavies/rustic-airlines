use std::time::{SystemTime, UNIX_EPOCH};
use eframe;
use egui::{Color32, Pos2, Vec2, Rect};

struct Airport {
    name: String,
    code: String,  // IATA code
    position: Pos2,
}

struct Flight {
    from: String,  // Airport IATA code
    to: String,
    date: u64,     // Unix timestamp (seconds since UNIX_EPOCH)
}

struct AppState {
    airports: Vec<Airport>,
    flights: Vec<Flight>,
    selected_airport: Option<String>,
    selected_date: Option<u64>,
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            airports: vec![
                Airport { name: "Ministro Pistarini International Airport".to_string(), code: "EZE".to_string(), position: Pos2::new(100.0, 100.0) },
                Airport { name: "Aeroparque Jorge Newbery".to_string(), code: "AEP".to_string(), position: Pos2::new(150.0, 120.0) },
                Airport { name: "Ing. Ambrosio Taravella Airport".to_string(), code: "COR".to_string(), position: Pos2::new(200.0, 180.0) },
                Airport { name: "Cataratas del Iguazú International Airport".to_string(), code: "IGR".to_string(), position: Pos2::new(250.0, 80.0) },
                Airport { name: "Martín Miguel de Güemes International Airport".to_string(), code: "SLA".to_string(), position: Pos2::new(180.0, 220.0) },
            ],
            flights: vec![
                Flight { from: "EZE".to_string(), to: "COR".to_string(), date: 1727395200 }, // 2024-09-26
                Flight { from: "AEP".to_string(), to: "IGR".to_string(), date: 1727395200 }, // 2024-09-26
                Flight { from: "COR".to_string(), to: "SLA".to_string(), date: 1727481600 }, // 2024-09-27
            ],
            selected_airport: None,
            selected_date: None
        }
    }
}

struct AirportMapApp {
    state: AppState,
}

impl AirportMapApp {
    fn new(_cc: &eframe::CreationContext<'_>) -> Self {
        Self {
            state: AppState::default(),
        }
    }

    fn format_date(timestamp: u64) -> String {
        let date = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(timestamp);
        let datetime = date.duration_since(UNIX_EPOCH).unwrap().as_secs();
        let days_since_epoch = datetime / 86400;
        let year = 1970 + (days_since_epoch / 365);
        let month = (days_since_epoch % 365) / 30 + 1;
        let day = (days_since_epoch % 365) % 30 + 1;
        format!("{}-{:02}-{:02}", year, month, day)
    }
}

impl eframe::App for AirportMapApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        egui::CentralPanel::default().show(ctx, |ui| {

            // Nuestro "mapa" de fondo
            let map_rect = ui.max_rect();
            let painter = ui.painter();
            painter.rect_filled(map_rect, 0.0, Color32::from_rgb(200, 200, 255));

            // Aeropuertos
            for airport in &self.state.airports {
                let airport_rect = Rect::from_center_size(airport.position, Vec2::new(20.0, 20.0));
                let airport_response = ui.interact(airport_rect, ui.id().with(airport.code.clone()), egui::Sense::click());
                painter.rect_filled(airport_rect, 5.0, Color32::from_rgb(100, 150, 100));

                painter.text(
                    airport.position + Vec2::new(0.0, -15.0),
                    egui::Align2::CENTER_CENTER,
                    &airport.code,
                    egui::FontId::proportional(20.0),
                    Color32::WHITE,
                );

                if airport_response.clicked() {
                    self.state.selected_airport = Some(airport.code.clone());
                    self.state.selected_date = None;
                }
            }

            // Si se selecciona un aeropuerto, muestra selector de flechas
            if let Some(ref airport_code) = self.state.selected_airport {
                ui.label(format!("Selected Airport: {}", airport_code));

                egui::ComboBox::from_label("Select Date")
                    .selected_text(self.state.selected_date.map_or("Select a date".to_string(), |date| format!("{}", AirportMapApp::format_date(date))))
                    .show_ui(ui, |ui| {
                        ui.selectable_value(&mut self.state.selected_date, Some(1727395200), "2024-09-26");
                        ui.selectable_value(&mut self.state.selected_date, Some(1727481600), "2024-09-27");
                    });

                if let Some(date) = self.state.selected_date {
                    ui.label(format!("Flights on {}:", AirportMapApp::format_date(date)));

                    for flight in &self.state.flights {
                        if flight.from == *airport_code && flight.date == date {
                            ui.label(format!("Flight to {} on {}", flight.to, AirportMapApp::format_date(flight.date)));
                        }
                    }
                }
            }
        });
    }
}

fn main() -> eframe::Result<()> {
    let native_options = eframe::NativeOptions::default();
    eframe::run_native(
        "Airport Map",
        native_options,
        Box::new(|cc| Ok(Box::new(AirportMapApp::new(cc)))),
    )
}
