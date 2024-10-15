#[derive(Debug, PartialEq, Clone)]
pub enum FlightStatus {
    Pending,
    InFlight,
    Finished,
}