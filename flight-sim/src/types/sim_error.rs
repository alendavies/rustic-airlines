use std::fmt;

#[derive(Debug)]
pub enum SimError {
    InvalidInput,
    InvalidAirport(String),  // For invalid airport code or details
    InvalidFlight(String),   // For invalid flight details (e.g., wrong date format)
    FlightNotFound(String),  // If flight can't be found
    AirportNotFound(String), // If airport can't be found
    InvalidDateFormat(String), // When the date format is incorrect
    Other(String),           // Generic error case with a custom message
    ClientError,            // If something went wrong with the client
}

// Implement the Display trait for user-friendly error messages
impl fmt::Display for SimError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            SimError::InvalidInput => write!(f, "Invalid input. Please check your input and try again."),
            SimError::InvalidAirport(ref code) => write!(f, "Invalid airport code: {}", code),
            SimError::InvalidFlight(ref flight) => write!(f, "Invalid flight details: {}", flight),
            SimError::FlightNotFound(ref flight_number) => write!(f, "Flight not found: {}", flight_number),
            SimError::AirportNotFound(ref iata_code) => write!(f, "Airport not found: {}", iata_code),
            SimError::InvalidDateFormat(ref date_str) => write!(f, "Invalid date format: {}", date_str),
            SimError::Other(ref message) => write!(f, "Error: {}", message),
            SimError::ClientError => write!(f, "Something went wrong with the client")
        }
    }
}

impl SimError {
    // Helper method to create an error with a custom message
    pub fn new(message: &str) -> Self {
        SimError::Other(message.to_string())
    }
}
