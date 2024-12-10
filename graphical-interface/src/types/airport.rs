use walkers::Position;

#[derive(Debug, Clone, PartialEq)]
pub struct Airport {
    pub name: String,
    pub iata: String,
    pub position: Position,
    pub country: String,
}

impl Airport {
    pub fn new(name: String, iata: String, position: Position, country: String) -> Self {
        Self {
            name,
            iata,
            position,
            country,
        }
    }
}
