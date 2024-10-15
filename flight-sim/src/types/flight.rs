use crate::types::airport::Airport;
use crate::types::flight_status::FlightStatus;
use crate::types::tracking::Tracking;
use std::time::SystemTime;


pub struct Flight {
    flight_number: String,
    origin: Airport,
    destination: Airport,
    average_speed: f64, // Average speed in km/h
    status: FlightStatus,
    departure_time: SystemTime,
}

impl Flight {
    // Start tracking for the flight by initializing tracking information
    // For now lets assume the altitude and fuel level.
    pub fn start_tracking(&self) -> Tracking {
        let total_distance = ((self.destination.latitude() - self.origin.latitude()).powi(2)
            + (self.destination.longitude() - self.origin.longitude()).powi(2)).sqrt();

        Tracking::new(
            self.origin.latitude(),
            self.origin.longitude(),
            35000.0, 
            100.0, 
            total_distance,
            0.0)
    }

    // Check if the flight has reached its destination
    pub fn check_arrival(&self, tracking: &Tracking) -> bool {
        tracking.distance_traveled() >= tracking.total_distance()
    }

    // Check and update the flight status based on current tracking data
    pub fn check_status(&mut self, tracking: &Tracking) {
        if self.status == FlightStatus::Pending && self.departure_time <= SystemTime::now() {
            self.status = FlightStatus::InFlight;
        }
        if self.status == FlightStatus::InFlight && self.check_arrival(tracking) {
            self.status = FlightStatus::Finished;
        }
    }
}