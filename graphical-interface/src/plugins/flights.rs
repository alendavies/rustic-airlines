use std::{cell::RefCell, rc::Rc};

use egui::{Align2, Color32, Rect, Response, Vec2};
use walkers::{extras::Style, Plugin, Projector};

use crate::{db::Flight, state::SelectionState};

pub struct Flights<'a> {
    flights: &'a Vec<Flight>,
    selection_state: Rc<RefCell<SelectionState>>,
}

impl<'a> Flights<'a> {
    pub fn new(flights: &'a Vec<Flight>, selection_state: Rc<RefCell<SelectionState>>) -> Self {
        Self {
            flights,
            selection_state,
        }
    }
}

impl Plugin for Flights<'_> {
    fn run(self: Box<Self>, ui: &mut egui::Ui, response: &Response, projector: &Projector) {
        for flight in self.flights {
            let mut style = Style::default();
            style.symbol_font.size = 24.;
            flight.draw(ui, projector, style, &mut self.selection_state.borrow_mut());
        }
    }
}

impl Flight {
    fn draw(
        &self,
        ui: &mut egui::Ui,
        projector: &Projector,
        style: Style,
        selection_state: &mut SelectionState,
    ) {
        let screen_position = projector.project(self.position);

        ui.painter().text(
            screen_position.to_pos2(),
            Align2::CENTER_CENTER,
            '✈',
            style.symbol_font.clone(),
            Color32::BLUE,
        );

        let symbol_size = Vec2::new(30.0, 30.0);
        let clickable_area = Rect::from_center_size(screen_position.to_pos2(), symbol_size);

        let response = ui.allocate_rect(clickable_area, egui::Sense::click());

        if response.clicked() {
            selection_state.toggle_flight_selection(&self);
        }
    }
}
