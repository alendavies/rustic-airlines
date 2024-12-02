use walkers::Position;

pub struct MapBounds {
    pub min_lat: f64,
    pub max_lat: f64,
    pub min_lon: f64,
    pub max_lon: f64,
}

impl MapBounds{
    pub fn is_within_bounds(&self, pos: &Position) -> bool {
        pos.lat() >= self.min_lat && 
        pos.lat() <= self.max_lat && 
        pos.lon() >= self.min_lon && 
        pos.lon() <= self.max_lon
    }
}