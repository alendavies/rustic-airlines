use egui::{Align2, Color32, FontId, Painter, Response, Stroke, Vec2};
use walkers::{extras::Style, Plugin, Projector};

use crate::db::Airport;

pub struct Airports<'a> {
    airports: &'a Vec<Airport>,
}

impl<'a> Airports<'a> {
    pub fn new(airports: &'a Vec<Airport>) -> Self {
        Self { airports }
    }
}

impl Plugin for Airports<'_> {
    fn run(
        &mut self,
        response: &egui::Response,
        painter: egui::Painter,
        projector: &walkers::Projector,
    ) {
        for airport in self.airports {
            let mut style = Style::default();
            style.symbol_font.size = 24.;
            airport.draw(response, painter.clone(), projector, style);
        }
    }
}

impl Airport {
    fn draw(&self, _response: &Response, painter: Painter, projector: &Projector, style: Style) {
        let screen_position = projector.project(self.position);
        let offset = Vec2::new(8., 8.);

        let label =
            painter.layout_no_wrap(self.iata.to_string(), FontId::default(), Color32::BLACK);

        painter.rect_filled(
            label
                .rect
                .translate(screen_position)
                .translate(offset)
                .expand(5.),
            10.,
            Color32::TRANSPARENT,
        );

        painter.galley(
            (screen_position + offset).to_pos2(),
            label,
            egui::Color32::BLACK,
        );

        painter.circle(
            screen_position.to_pos2(),
            10.,
            Color32::default(),
            Stroke::default(),
        );

        painter.text(
            screen_position.to_pos2(),
            Align2::LEFT_BOTTOM,
            '📌',
            style.symbol_font.clone(),
            Color32::RED,
        );
    }
}
