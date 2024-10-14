use crate::frame::SerializationError;

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

impl Opcode {
    pub fn from_byte(byte: u8) -> Result<Self, SerializationError> {
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

    pub fn to_byte(&self) -> u8 {
        *self as u8
    }
}
