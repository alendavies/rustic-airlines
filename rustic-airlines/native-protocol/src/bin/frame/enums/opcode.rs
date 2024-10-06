
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
    pub fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0x00 => Some(Opcode::Error),
            0x01 => Some(Opcode::Startup),
            0x02 => Some(Opcode::Ready),
            0x03 => Some(Opcode::Authenticate),
            0x05 => Some(Opcode::Options),
            0x06 => Some(Opcode::Supported),
            0x07 => Some(Opcode::Query),
            0x08 => Some(Opcode::Result),
            0x09 => Some(Opcode::Prepare),
            0x0A => Some(Opcode::Execute),
            0x0B => Some(Opcode::Register),
            0x0C => Some(Opcode::Event),
            0x0D => Some(Opcode::Batch),
            0x0E => Some(Opcode::AuthChallenge),
            0x0F => Some(Opcode::AuthResponse),
            0x10 => Some(Opcode::AuthSuccess),
            _ => None,  // Retorna None si no coincide con ningún opcode válido
        }
    }

    pub fn to_byte(&self) -> u8 {
        *self as u8
    }
}
