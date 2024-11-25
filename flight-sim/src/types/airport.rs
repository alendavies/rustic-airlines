#[derive(Clone, Debug)]
pub struct Airport {
    pub iata_code: String,
    pub country: String,
    pub name: String,
    pub latitude: f64,
    pub longitude: f64,
}

impl Airport {

    pub fn new(iata_code: String, country: String, name: String, latitude: f64, longitude: f64) -> Self {
        Airport {
            iata_code,
            country,
            name,
            latitude,
            longitude
        }
    }

}