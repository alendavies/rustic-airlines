#[derive(Debug, Clone, PartialEq)]
pub struct FlightInfo {
    pub number: String,
    pub fuel: f64,
    pub height: i32,
    pub speed: i32,
    pub origin: String,
    pub destination: String,
}
