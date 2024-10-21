#[derive(Debug, PartialEq, Display, Clone)]
pub enum FlightStatus {
    Pending,
    InFlight,
    Finished,
}

impl FlightStatus {
    pub fn as_str(&self) -> &str {
        match self {
            FlightStatus::Pending => "Pending",
            FlightStatus::InFlight => "In Flight",
            FlightStatus::Finished => "Finished",
        }
    }
}