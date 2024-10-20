use crate::types::airport::Airport;
use crate::types::flight_status::FlightStatus;
use std::time::SystemTime;

pub struct Flight {
    pub flight_number: String,
    pub origin: Airport,
    pub destination: Airport,
    pub latitude: f64,
    pub longitude: f64,
    pub altitude: f64,
    pub fuel_level: f64,
    pub total_distance: f64,  // Total distance between origin and destination
    pub distance_traveled: f64, // Distance traveled from origin
    pub average_speed: f64, // Average speed in km/h
    pub departure_time: SystemTime,
    pub status: FlightStatus,
}

impl Flight {
    pub fn new(flight_number: String, origin: Airport, destination: Airport, average_speed: f64) -> Self {
        let total_distance = ((destination.latitude - origin.latitude).powi(2)
            + (destination.longitude - origin.longitude).powi(2))
            .sqrt();

        let starting_latitude = origin.latitude;
        let starting_longitude = origin.longitude;

        Flight {
            flight_number,
            origin,
            destination,
            latitude: starting_latitude,
            longitude: starting_longitude,
            altitude: 35000.0, // default starting altitude
            fuel_level: 100.0, // default full fuel
            total_distance,
            distance_traveled: 0.0,
            average_speed,
            departure_time: SystemTime::now(),
            status: FlightStatus::Pending,
        }
    }

    // Calculate the direction to the destination
    pub fn calculate_current_direction(&self) -> (f64, f64) {
        let delta_latitude = self.destination.latitude - self.latitude;
        let delta_longitude = self.destination.longitude - self.longitude;
        let distance = ((delta_latitude.powi(2) + delta_longitude.powi(2)).sqrt()).max(1e-5); // Avoid division by zero
        (delta_latitude / distance, delta_longitude / distance)
    }

    // Update position based on speed and direction
    pub fn update_position(&mut self, direction: (f64, f64)) {
        self.latitude += direction.0 * self.average_speed * 0.01;
        self.longitude += direction.1 * self.average_speed * 0.01;
        self.distance_traveled += self.average_speed * 0.01;
        self.fuel_level -= 0.05;
    }

    // Land the flight
    pub fn land(&mut self) {
        self.fuel_level = 0.0;
        self.altitude = 0.0;
        self.status = FlightStatus::Finished;
    }

    // Check if the flight has reached its destination
    pub fn check_arrival(&self) -> bool {
        self.distance_traveled >= self.total_distance
    }

    // Update the flight status
    pub fn check_status(&mut self) {
        if self.status == FlightStatus::Pending && self.departure_time <= SystemTime::now() {
            self.status = FlightStatus::InFlight;
        }

        if self.status == FlightStatus::InFlight && self.check_arrival() {
            self.land();
        }
    }
}
