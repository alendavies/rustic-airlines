use crate::types::airport::Airport;
use crate::types::flight_status::FlightStatus;
use chrono::{NaiveDateTime};
use std::collections::HashMap;
use std::error::Error;

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
    ) -> Result<Self, Box<dyn Error>> {
        let origin = airports.get(origin_code)
            .ok_or_else(|| format!("Origin airport with IATA code '{}' not found.", origin_code))?
            .clone();

        let destination = airports.get(destination_code)
            .ok_or_else(|| format!("Destination airport with IATA code '{}' not found.", destination_code))?
            .clone();

        let departure_time = parse_datetime(departure_time_str)?;
        let arrival_time = parse_datetime(arrival_time_str)?;

        let starting_latitude = origin.latitude;
        let starting_longitude = origin.longitude;

        let total_distance = ((destination.latitude - origin.latitude).powi(2)
            + (destination.longitude - origin.longitude).powi(2))
            .sqrt();
        
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

    // Calculate direction to destination
    pub fn calculate_current_direction(&self) -> (f64, f64) {
        let delta_latitude = self.destination.latitude - self.latitude;
        let delta_longitude = self.destination.longitude - self.longitude;
        let distance = ((delta_latitude.powi(2) + delta_longitude.powi(2)).sqrt()).max(1e-5);
        (delta_latitude / distance, delta_longitude / distance)
    }

    // Update the position and fuel level based on the current time
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
            let direction = self.calculate_current_direction();
            self.latitude = self.origin.latitude + direction.0 * distance_traveled;
            self.longitude = self.origin.longitude + direction.1 * distance_traveled;
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

// Sample input format for dates: "DD/MM/YY HH:MM:SS"
fn parse_datetime(datetime_str: &str) -> Result<NaiveDateTime, Box<dyn Error>> {
    let format = "%d/%m/%y %H:%M:%S"; 
    let datetime = NaiveDateTime::parse_from_str(datetime_str, format)?;
    Ok(datetime)
}