use walkers::Position;

use super::FlightInfo;

#[derive(Debug, Clone, PartialEq)]
pub struct Flight {
    pub number: String,
    pub status: String,
    pub position: Position,
    pub heading: f32,
    pub departure_time: i64,
    pub arrival_time: i64,
    pub airport: String,
    pub direction: String,
    pub info: Option<FlightInfo>,
}
