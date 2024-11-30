#[derive(Debug, PartialEq, Clone)]
pub enum FlightStatus {
    Scheduled,
    OnTime,
    Delayed,
    Finished,
    Canceled
}

impl FlightStatus {
    pub fn as_str(&self) -> &str {
        match self {
            FlightStatus::Scheduled => "scheduled",
            FlightStatus::OnTime => "on time",
            FlightStatus::Delayed => "delayed",
            FlightStatus::Finished => "finished",
            FlightStatus::Canceled => "canceled"
        }
    }

    pub fn from_str(status: &str) -> Result<FlightStatus, Box<dyn std::error::Error>> {
        match status.to_lowercase().as_str() {
            "scheduled" => Ok(FlightStatus::Scheduled),
            "on time" => Ok(FlightStatus::OnTime),
            "delayed" => Ok(FlightStatus::Delayed),
            "finished" => Ok(FlightStatus::Finished),
            "canceled" => Ok(FlightStatus::Canceled),
            _ => Err(format!("Invalid flight status").into()),
        }
    }
}
