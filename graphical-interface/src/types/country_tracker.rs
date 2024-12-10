use std::collections::HashMap;

use walkers::Position;

use super::MapBounds;

pub struct CountryTracker {
    country_centers: HashMap<String, Position>,
    visible_countries: Vec<String>,
}

impl CountryTracker {
    pub fn new() -> Self {
        let country_centers = get_south_american_centers();

        Self {
            country_centers,
            visible_countries: Vec::new(),
        }
    }

    // Update visible countries based on current map view
    pub fn update_visible_countries(&mut self, map_bounds: &MapBounds) {
        self.visible_countries.clear();

        for (country, pos) in &self.country_centers {
            if map_bounds.is_within_bounds(pos) {
                self.visible_countries.push(country.clone());
            }
        }
    }

    // Getter for visible countries
    pub fn _get_visible_countries(&self) -> &Vec<String> {
        &self.visible_countries
    }
}

fn get_south_american_centers() -> HashMap<String, Position> {
    let mut centers = HashMap::new();

    centers.insert(
        "Argentina".to_string(),
        Position::from_lat_lon(-38.416097, -63.616672),
    ); // Centro aproximado de Argentina
    centers.insert(
        "Brazil".to_string(),
        Position::from_lat_lon(-14.235004, -51.925280),
    ); // Centro aproximado de Brasil
    centers.insert(
        "Chile".to_string(),
        Position::from_lat_lon(-35.675147, -71.542969),
    ); // Centro aproximado de Chile
    centers.insert(
        "Colombia".to_string(),
        Position::from_lat_lon(4.570868, -74.297333),
    ); // Centro aproximado de Colombia
    centers.insert(
        "Peru".to_string(),
        Position::from_lat_lon(-9.189967, -75.015152),
    ); // Centro aproximado de Perú
    centers.insert(
        "Venezuela".to_string(),
        Position::from_lat_lon(6.423750, -66.589730),
    ); // Centro aproximado de Venezuela
    centers.insert(
        "Ecuador".to_string(),
        Position::from_lat_lon(-1.831239, -78.183406),
    ); // Centro aproximado de Ecuador
    centers.insert(
        "Bolivia".to_string(),
        Position::from_lat_lon(-16.290154, -63.588653),
    ); // Centro aproximado de Bolivia
    centers.insert(
        "Paraguay".to_string(),
        Position::from_lat_lon(-23.442503, -58.443832),
    ); // Centro aproximado de Paraguay
    centers.insert(
        "Uruguay".to_string(),
        Position::from_lat_lon(-32.522779, -55.765835),
    ); // Centro aproximado de Uruguay
    centers.insert(
        "Suriname".to_string(),
        Position::from_lat_lon(4.130554, -55.657883),
    ); // Centro aproximado de Surinam
    centers.insert(
        "Guyana".to_string(),
        Position::from_lat_lon(4.860416, -58.930180),
    ); // Centro aproximado de Guyana

    centers
}
