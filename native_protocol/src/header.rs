use crate::{ByteSerializable, Serializable, SerializationError};

/// Each frame contains a fixed size header (9 bytes) followed by a variable size body.
#[derive(Debug)]
pub struct FrameHeader {
    version: Version, // Usamos el enum Version
    flags: Flags,     // 1 byte
    stream: i16,      // 2 bytes
    opcode: Opcode,   // Usamos el enum Opcode
    body_length: u32, // 4 bytes
}

impl FrameHeader {
    pub fn new(
        version: Version,
        flags: Flags,
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
}

impl Serializable for FrameHeader {
    fn to_bytes(&self) -> Vec<u8> {
        let mut buffer = Vec::new();

        buffer.push(self.version as u8);
        buffer.push(self.flags.to_byte());
        buffer.extend_from_slice(&self.stream.to_be_bytes());
        buffer.push(self.opcode as u8);
        buffer.extend_from_slice(&self.body_length.to_be_bytes());

        buffer
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, SerializationError> {
        if bytes.len() < 8 {
            return Err(SerializationError);
        }

        let version = Version::from_byte(bytes[0]).unwrap();

        let flags = Flags::from_byte(bytes[1]).unwrap();

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

#[derive(Debug, Copy, Clone)]
pub enum Opcode {
    Error = 0x00,
    Startup = 0x01,
    Ready = 0x02,
    Authenticate = 0x03,
    Options = 0x05,
    Supported = 0x06,
    Query = 0x07,
    Result = 0x08,
    Prepare = 0x09,
    Execute = 0x0A,
    Register = 0x0B,
    Event = 0x0C,
    Batch = 0x0D,
    AuthChallenge = 0x0E,
    AuthResponse = 0x0F,
    AuthSuccess = 0x10,
}

impl ByteSerializable for Opcode {
    fn from_byte(byte: u8) -> Result<Self, SerializationError> {
        match byte {
            0x00 => Ok(Opcode::Error),
            0x01 => Ok(Opcode::Startup),
            0x02 => Ok(Opcode::Ready),
            0x03 => Ok(Opcode::Authenticate),
            0x05 => Ok(Opcode::Options),
            0x06 => Ok(Opcode::Supported),
            0x07 => Ok(Opcode::Query),
            0x08 => Ok(Opcode::Result),
            0x09 => Ok(Opcode::Prepare),
            0x0A => Ok(Opcode::Execute),
            0x0B => Ok(Opcode::Register),
            0x0C => Ok(Opcode::Event),
            0x0D => Ok(Opcode::Batch),
            0x0E => Ok(Opcode::AuthChallenge),
            0x0F => Ok(Opcode::AuthResponse),
            0x10 => Ok(Opcode::AuthSuccess),
            _ => Err(SerializationError),
        }
    }

    fn to_byte(&self) -> u8 {
        *self as u8
    }
}

/// The version is a single byte that indicate both the direction of the message
/// (request or response) and the version of the protocol in use.
#[derive(Debug, Copy, Clone)]
pub enum Version {
    RequestV3 = 0x03,  // Request frame for this protocol version
    ResponseV3 = 0x83, // Response frame for this protocol version
}

impl ByteSerializable for Version {
    fn from_byte(byte: u8) -> Result<Self, SerializationError> {
        match byte {
            0x03 => Ok(Version::RequestV3),
            0x83 => Ok(Version::ResponseV3),
            _ => Err(SerializationError),
        }
    }

    fn to_byte(&self) -> u8 {
        todo!()
    }
}

enum FlagCodes {
    Compression = 0x01,
    Tracing = 0x02,
}

#[derive(Debug)]
pub struct Flags {
    /// Compression flag.
    pub compression: bool,
    /// Tracing flag.
    pub tracing: bool,
}

impl ByteSerializable for Flags {
    fn to_byte(&self) -> u8 {
        let mut flags = 0u8;

        if self.compression {
            flags |= FlagCodes::Compression as u8;
        };

        if self.tracing {
            flags |= FlagCodes::Tracing as u8;
        };

        flags
    }

    fn from_byte(flags: u8) -> Result<Self, SerializationError> {
        let compression = flags & FlagCodes::Compression as u8 != 0;
        let tracing = flags & FlagCodes::Tracing as u8 != 0;

        Ok(Self {
            compression,
            tracing,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flags_to_byte_all_false() {
        let flags = Flags {
            compression: false,
            tracing: false,
        };

        let flags = flags.to_byte();

        assert_eq!(flags, 0x00)
    }

    #[test]
    fn flags_to_byte_all_true() {
        let flags = Flags {
            compression: true,
            tracing: true,
        };

        let flags = flags.to_byte();

        assert_eq!(flags, 0x03)
    }

    #[test]
    fn byte_to_flags_all_true() {
        let flags = 0x03;

        let Flags {
            compression,
            tracing,
        } = Flags::from_byte(flags).unwrap();

        assert!(compression);
        assert!(tracing);
    }
}
