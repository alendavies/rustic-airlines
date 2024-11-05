use std::{cell::RefCell, rc::Rc};

use egui::{Align2, Color32, FontId, Rect, Response, Stroke, Vec2};
use walkers::{extras::Style, Plugin, Projector};

use crate::{db::Airport, state::SelectionState};

pub struct Airports<'a> {
    airports: &'a Vec<Airport>,
    selection_state: Rc<RefCell<SelectionState>>,
}

impl<'a> Airports<'a> {
    pub fn new(airports: &'a Vec<Airport>, selection_state: Rc<RefCell<SelectionState>>) -> Self {
        Self {
            airports,
            selection_state,
        }
    }
}

impl Plugin for Airports<'_> {
    fn run(self: Box<Self>, ui: &mut egui::Ui, response: &Response, projector: &Projector) {
        for airport in self.airports {
            let mut style = Style::default();
            style.symbol_font.size = 24.;
            airport.draw(ui, projector, style, &mut self.selection_state.borrow_mut());
        }
    }
}

impl Airport {
    fn draw(
        &self,
        ui: &mut egui::Ui,
        projector: &Projector,
        style: Style,
        selection_state: &mut SelectionState,
    ) {
        let screen_position = projector.project(self.position);
        let offset = Vec2::new(8., 8.);

        let label =
            ui.painter()
                .layout_no_wrap(self.iata.to_string(), FontId::default(), Color32::BLACK);

        ui.painter().rect_filled(
            label
                .rect
                .translate(screen_position)
                .translate(offset)
                .expand(5.),
            10.,
            Color32::TRANSPARENT,
        );

        ui.painter().galley(
            (screen_position + offset).to_pos2(),
            label,
            egui::Color32::BLACK,
        );

        ui.painter().circle(
            screen_position.to_pos2(),
            10.,
            Color32::default(),
            Stroke::default(),
        );

        ui.painter().text(
            screen_position.to_pos2(),
            Align2::LEFT_BOTTOM,
            '📌',
            style.symbol_font.clone(),
            Color32::RED,
        );

        let symbol_size = Vec2::new(30.0, 30.0);
        let clickable_area = Rect::from_center_size(screen_position.to_pos2(), symbol_size);

        let response = ui.allocate_rect(clickable_area, egui::Sense::click());

        if response.clicked() {
            selection_state.toggle_airport_selection(&self);
        }
    }
}
