#[derive(Debug)]
pub enum Version {
    RequestV3 = 0x03,    // Request frame for this protocol version
    ResponseV3 = 0x83,   // Response frame for this protocol version
}

impl Version {
    pub fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0x03 => Some(Version::RequestV3),
            0x83 => Some(Version::ResponseV3),
            _ => None,
        }
    }
}
