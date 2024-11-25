#[derive(Debug, PartialEq, Clone)]
pub enum FlightStatus {
    Pending,
    InFlight,
    Delayed,
    Finished,
}

impl FlightStatus {
    pub fn as_str(&self) -> &str {
        match self {
            FlightStatus::Pending => "Pending",
            FlightStatus::InFlight => "In Flight",
            FlightStatus::Delayed => "Delayed",
            FlightStatus::Finished => "Finished",
        }
    }
}