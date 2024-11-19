use crate::types::airport::Airport;
use crate::types::flight_status::FlightStatus;
use std::collections::HashMap;
use chrono::NaiveDateTime;
use std::f64::consts::PI;

use super::sim_error::SimError;

const EARTH_RADIUS_KM: f64 = 6371.0;

#[derive(Debug, Clone)]
pub struct Flight {
    pub flight_number: String,
    pub status: FlightStatus,
    pub departure_time: NaiveDateTime,
    pub arrival_time: NaiveDateTime,
    pub origin: Airport,
    pub destination: Airport,
    pub latitude: f64,
    pub longitude: f64,
    pub altitude: f64,
    pub fuel_level: f64,
    pub total_distance: f64,
    pub distance_traveled: f64,
    pub average_speed: f64,
}


impl Flight {
    
    pub fn new_from_console(
        airports: &HashMap<String, Airport>,
        flight_number: &str,
        origin_code: &str,
        destination_code: &str,
        departure_time_str: &str,
        arrival_time_str: &str,
        average_speed: f64,
    ) -> Result<Self, SimError> {
        // Look up airports and return error if not found
        let origin = airports.get(origin_code)
            .ok_or_else(|| SimError::AirportNotFound(origin_code.to_string()))?
            .clone();
    
        let destination = airports.get(destination_code)
            .ok_or_else(|| SimError::AirportNotFound(destination_code.to_string()))?
            .clone();
    
        // Parse datetime and return error if invalid
        let departure_time = parse_datetime(departure_time_str)?;
        let arrival_time = parse_datetime(arrival_time_str)?;
    
        let starting_latitude = origin.latitude;
        let starting_longitude = origin.longitude;
    
        let total_distance = haversine_distance(origin.latitude, origin.longitude, destination.latitude, destination.longitude);
        
        Ok(Flight {
            flight_number: flight_number.to_string(),
            status: FlightStatus::Pending,
            departure_time,
            arrival_time,
            origin,
            destination,
            latitude: starting_latitude,
            longitude: starting_longitude,
            altitude: 35000.0,
            fuel_level: 100.0,
            total_distance,
            distance_traveled: 0.0,
            average_speed,
        })
    }

    // Calculate current latitude and longitude according to distance traveled (using radians)
    fn update_position_with_direction(&mut self, distance_traveled_km: f64) {
        let delta_lat = (self.destination.latitude - self.latitude).to_radians();
        let delta_lon = (self.destination.longitude - self.longitude).to_radians();

        let mean_latitude = ((self.latitude + self.destination.latitude) / 2.0).to_radians();

        let distance_ratio = distance_traveled_km / self.total_distance;
        let lat_increment = (delta_lat * distance_ratio).atan2(EARTH_RADIUS_KM);
        let lon_increment = (delta_lon * distance_ratio * mean_latitude.cos()).atan2(EARTH_RADIUS_KM);

        self.latitude += lat_increment.to_degrees();
        self.longitude += lon_increment.to_degrees();
    }



    /// Update the position of the flight and its fuel level based on the current time
    pub fn update_position(&mut self, current_time: NaiveDateTime) {
        if self.status == FlightStatus::Pending && current_time >= self.departure_time {
            self.status = FlightStatus::InFlight;
        }
        
        if self.status == FlightStatus::InFlight {
            let elapsed_hours = current_time
                .signed_duration_since(self.departure_time)
                .num_seconds() as f64 / 3600.0;

            // Calculate traveled distance and update position
            let distance_traveled = self.average_speed * elapsed_hours;
            self.update_position_with_direction(distance_traveled);
            self.distance_traveled = distance_traveled.min(self.total_distance);
            self.fuel_level = (100.0 - elapsed_hours * 5.0).max(0.0); // Burn fuel over time

            // Update altitude when approaching the destination
            self.altitude = if self.distance_traveled >= self.total_distance * 0.95 {
                self.altitude - 500.0
            } else {
                self.altitude
            };

            // Check for arrival or delay
            if self.distance_traveled >= self.total_distance {
                self.land();
            } else if current_time >= self.arrival_time {
                self.status = FlightStatus::Delayed;
            }
        }
    }

    // Land the flight
    fn land(&mut self) {
        self.fuel_level = 0.0;
        self.altitude = 0.0;
        self.status = FlightStatus::Finished;
    }

}

fn parse_datetime(datetime_str: &str) -> Result<NaiveDateTime, SimError> {
    let format = "%d-%m-%Y %H:%M:%S"; // The expected format for the date input
    NaiveDateTime::parse_from_str(datetime_str, format)
        .map_err(|_| SimError::InvalidDateFormat(datetime_str.to_string()))
}


fn haversine_distance(origin_lat: f64, origin_lon: f64, dest_lat: f64, dest_lon: f64) -> f64 {

    let origin_lat_rad = origin_lat * PI / 180.0;
    let origin_lon_rad = origin_lon * PI / 180.0;
    let dest_lat_rad = dest_lat * PI / 180.0;
    let dest_lon_rad = dest_lon * PI / 180.0;

    let delta_lat = dest_lat_rad - origin_lat_rad;
    let delta_lon = dest_lon_rad - origin_lon_rad;

    // Haversine formula
    let a = (delta_lat / 2.0).sin().powi(2)
          + origin_lat_rad.cos() * dest_lat_rad.cos() * (delta_lon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());

    EARTH_RADIUS_KM * c
}
