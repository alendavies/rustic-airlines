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
    pub angle: f32,
    pub altitude: i32,
    pub fuel_level: f64,
    pub total_distance: f64,
    pub distance_traveled: f64,
    pub average_speed: i32,
}


impl Flight {
    
    pub fn new_from_console(
        airports: &HashMap<String, Airport>,
        flight_number: &str,
        origin_code: &str,
        destination_code: &str,
        departure_time_str: &str,
        arrival_time_str: &str,
        average_speed: i32,
    ) -> Result<Self, SimError> {
        let origin = airports.get(origin_code)
            .ok_or_else(|| SimError::AirportNotFound(origin_code.to_string()))?
            .clone();
    
        let destination = airports.get(destination_code)
            .ok_or_else(|| SimError::AirportNotFound(destination_code.to_string()))?
            .clone();
    

        let departure_time = parse_datetime(departure_time_str)?;
        let arrival_time = parse_datetime(arrival_time_str)?;

        if arrival_time <= departure_time || average_speed <= 0 {
            return Err(SimError::InvalidInput);
        }
    
        let starting_latitude = origin.latitude;
        let starting_longitude = origin.longitude;
    
        let total_distance = haversine_distance(origin.latitude, origin.longitude, destination.latitude, destination.longitude);
        
        let mut flight = Flight {
            flight_number: flight_number.to_string(),
            status: FlightStatus::Scheduled,
            departure_time,
            arrival_time,
            origin,
            destination,
            latitude: starting_latitude,
            longitude: starting_longitude,
            angle: 0.0,
            altitude: 35000,
            fuel_level: 100.0,
            total_distance,
            distance_traveled: 0.0,
            average_speed,
        };

        flight.angle = flight.calculate_bearing() as f32;

        Ok(flight)
    }

    fn calculate_bearing(&self) -> f64 {
        let lat1 = self.origin.latitude;
        let lon1 = self.origin.longitude;
        let lat2 = self.destination.latitude;
        let lon2 = self.destination.longitude;

        let delta_lon = lon2 - lon1;
        let delta_lat = lat2 - lat1;

        let bearing = delta_lat.atan2(delta_lon);

        bearing.to_degrees() + 90.0
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
        if self.status == FlightStatus::Scheduled && current_time >= self.departure_time {
            if self.altitude == 0 {self.altitude = 10000}; // Default plane altitude in case it wasn't specified.
            self.status = FlightStatus::OnTime;
        }
        
        if self.status == FlightStatus::OnTime {
            let elapsed_hours = current_time
                .signed_duration_since(self.departure_time)
                .num_seconds() as f64 / 3600.0;

            // Calculate traveled distance and update position
            let distance_traveled = self.average_speed as f64 * elapsed_hours;
            self.distance_traveled = distance_traveled.min(self.total_distance);
            self.update_position_with_direction(distance_traveled);
            self.fuel_level = (100.0 - elapsed_hours * 5.0).max(0.0); // Burn fuel over time

            // Update altitude when approaching the destination
            self.altitude = if self.distance_traveled >= self.total_distance * 0.95 {
                let altitude = self.altitude - 500;
                if altitude < 0 {0} else {altitude}
            } else {
                self.altitude
            };

            // Check for arrival or delay
            if self.distance_traveled >= self.total_distance || self.status == FlightStatus::Finished {
                self.land();
            } else if current_time >= self.arrival_time {
                self.status = FlightStatus::Delayed;
            }
        }
    }

    // Land the flight
    fn land(&mut self) {
        self.fuel_level = 0.0;
        self.altitude = 0;
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
