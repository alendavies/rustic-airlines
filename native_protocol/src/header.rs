use crate::opcodes::Opcode;

#[derive(Debug, Copy, Clone)]
pub enum Version {
    RequestV3 = 0x03,  // Request frame for this protocol version
    ResponseV3 = 0x83, // Response frame for this protocol version
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

enum HeaderFlagsCodes {
    Compression = 0x01,
    Tracing = 0x02,
}

#[derive(Debug)]
pub struct HeaderFlags {
    pub compression: bool,
    pub tracing: bool,
}

impl HeaderFlags {
    pub fn to_byte(&self) -> u8 {
        let mut flags = 0u8;

        if self.compression {
            flags |= HeaderFlagsCodes::Compression as u8;
        };

        if self.tracing {
            flags |= HeaderFlagsCodes::Tracing as u8;
        };

        flags
    }

    pub fn from_byte(flags: u8) -> Self {
        let compression = flags & HeaderFlagsCodes::Compression as u8 != 0;
        let tracing = flags & HeaderFlagsCodes::Tracing as u8 != 0;

        Self {
            compression,
            tracing,
        }
    }
}

#[derive(Debug)]
pub struct FrameHeader {
    version: Version,   // Usamos el enum Version
    flags: HeaderFlags, // 1 byte
    stream: i16,        // 2 bytes
    opcode: Opcode,     // Usamos el enum Opcode
    body_length: u32,   // 4 bytes
}

impl FrameHeader {
    pub fn new(
        version: Version,
        flags: HeaderFlags,
        stream: i16,
        opcode: Opcode,
        body_length: u32,
    ) -> Self {
        Self {
            version,
            flags,
            stream,
            opcode,
            body_length,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buffer = Vec::new();

        buffer.push(self.version as u8);
        buffer.push(self.flags.to_byte());
        buffer.extend_from_slice(&self.stream.to_be_bytes());
        buffer.push(self.opcode as u8);
        buffer.extend_from_slice(&self.body_length.to_be_bytes());

        buffer
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, &str> {
        if bytes.len() < 8 {
            return Err("El buffer es demasiado pequeño para un FrameHeader");
        }

        let version = Version::from_byte(bytes[0]).ok_or("Versión no válida en el FrameHeader")?;

        let flags = HeaderFlags::from_byte(bytes[1]);

        let stream = i16::from_be_bytes([bytes[2], bytes[3]]);

        // let opcode = Opcode::from_byte(bytes[4]).ok_or("Opcode no válido en el FrameHeader")?;
        let opcode = Opcode::from_byte(bytes[4]).unwrap();

        // Deserializar la longitud del cuerpo (4 bytes, big-endian)
        let body_length = u32::from_be_bytes([bytes[5], bytes[6], bytes[7], bytes[8]]);

        Ok(Self {
            version,
            flags,
            stream,
            opcode,
            body_length,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flags_to_byte_all_false() {
        let flags = HeaderFlags {
            compression: false,
            tracing: false,
        };

        let flags = flags.to_byte();

        assert_eq!(flags, 0x00)
    }

    #[test]
    fn flags_to_byte_all_true() {
        let flags = HeaderFlags {
            compression: true,
            tracing: true,
        };

        let flags = flags.to_byte();

        assert_eq!(flags, 0x03)
    }

    #[test]
    fn byte_to_flags_all_true() {
        let flags = 0x03;

        let HeaderFlags {
            compression,
            tracing,
        } = HeaderFlags::from_byte(flags);

        assert!(compression);
        assert!(tracing);
    }
}
