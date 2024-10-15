use crate::types::airport::Airport;

pub struct Tracking {
    latitude: f64,
    longitude: f64,
    altitude: f64,
    fuel_level: f64,
    total_distance: f64,  // Total distance between origin and destination
    distance_traveled: f64, // Distance traveled from origin
}

impl Tracking {

    pub fn new(latitude: f64, longitude: f64, altitude: f64, fuel_level: f64, total_distance: f64, distance_traveled: f64) -> Self {
        Tracking {
            latitude,
            longitude, 
            altitude, 
            fuel_level, 
            total_distance, 
            distance_traveled
        }
    }

    // Method to calculate the new direction based on the current tracking position
    pub fn calculate_current_direction(&self, destination: &Airport) -> (f64, f64) {
        let delta_latitude = destination.latitude() - self.latitude;
        let delta_longitude = destination.longitude() - self.longitude;
        let distance = ((delta_latitude.powi(2) + delta_longitude.powi(2)).sqrt()).max(1e-5); // Avoid division by zero
        (delta_latitude / distance, delta_longitude / distance)
    }

    // Method to update the position based on direction and speed
    pub fn update_position(&mut self, speed: f64, direction: (f64, f64)) {
        self.latitude += direction.0 * speed * 0.01;
        self.longitude += direction.1 * speed * 0.01;
        self.distance_traveled += speed * 0.01;
        self.fuel_level -= 0.05;
    }

    // Method to simulate the landing
    pub fn land(&mut self) {
        self.fuel_level = 0.0;
        self.altitude = 0.0;
    }

    pub fn total_distance(&self) -> &f64 {
        &self.total_distance
    }

    pub fn distance_traveled(&self) -> &f64 {
        &self.distance_traveled
    }

    pub fn latitude(&self) -> &f64 {
        &self.latitude
    }

    pub fn longitude(&self) -> &f64 {
        &self.longitude
    }
}