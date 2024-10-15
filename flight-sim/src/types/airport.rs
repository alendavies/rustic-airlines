#[derive(Clone, Debug)]
pub struct Airport {
    iata_code: String,
    name: String,
    latitude: f64,
    longitude: f64,
}

impl Airport {

    pub fn new(iata_code: String, name: String, latitude: f64, longitude: f64) -> Self {
        Airport {
            iata_code,
            name,
            latitude,
            longitude
        }
    }

    pub fn latitude(&self) -> f64 {
        self.latitude
    }

    pub fn longitude(&self) -> f64 {
        self.longitude
    }

}