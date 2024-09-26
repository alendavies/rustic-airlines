#[derive(Debug)]
pub struct FrameHeader {
    version: Version,     // Usamos el enum Version
    flags: u8,            // 1 byte
    stream: i16,          // 2 bytes
    opcode: Opcode,       // Usamos el enum Opcode
    body_length: u32,     // 4 bytes
}

impl FrameHeader {
    pub fn new(version: Version, flags: u8, stream: i16, opcode: Opcode, body_length: u32) -> Self {
        FrameHeader {
            version,
            flags,
            stream,
            opcode,
            body_length,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buffer = Vec::new();

        buffer.push(self.version.clone() as u8);
        buffer.push(self.flags);
        buffer.extend_from_slice(&self.stream.to_be_bytes());
        buffer.push(self.opcode.clone() as u8);
        buffer.extend_from_slice(&self.body_length.to_be_bytes());

        buffer
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < 8 {
            return Err("El buffer es demasiado pequeño para un FrameHeader");
        }

        let version = Version::from_byte(bytes[0])
            .ok_or("Versión no válida en el FrameHeader")?;

        let flags = bytes[1];

        let stream = i16::from_be_bytes([bytes[2], bytes[3]]);

        let opcode = Opcode::from_byte(bytes[4])
            .ok_or("Opcode no válido en el FrameHeader")?;

        // Deserializar la longitud del cuerpo (4 bytes, big-endian)
        let body_length = u32::from_be_bytes([bytes[5], bytes[6], bytes[7], bytes[8]]);

        Ok(FrameHeader {
            version,
            flags,
            stream,
            opcode,
            body_length,
        })
    }
}
