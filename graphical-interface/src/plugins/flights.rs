use egui::{Align2, Color32, Painter, Response, Stroke};
use walkers::{extras::Style, Plugin, Projector};

use crate::db::Flight;

pub struct Flights<'a> {
    flights: &'a Vec<Flight>,
}

impl<'a> Flights<'a> {
    pub fn new(flights: &'a Vec<Flight>) -> Self {
        Self { flights }
    }
}

impl Plugin for Flights<'_> {
    fn run(
        &mut self,
        response: &egui::Response,
        painter: egui::Painter,
        projector: &walkers::Projector,
    ) {
        for flight in self.flights {
            let mut style = Style::default();
            style.symbol_font.size = 24.;
            flight.draw(response, painter.clone(), projector, style);
        }
    }
}

impl Flight {
    fn draw(&self, _response: &Response, painter: Painter, projector: &Projector, style: Style) {
        let screen_position = projector.project(self.position);

        painter.text(
            screen_position.to_pos2(),
            Align2::CENTER_CENTER,
            '✈',
            style.symbol_font.clone(),
            Color32::BLUE,
        );
    }
}
