#[derive(Debug, Copy, Clone)]
pub enum ErrorCode {
    ServerError = 0x0000,
    ProtocolError = 0x000A,
    BadCredentials = 0x0100,
    UnavailableException = 0x1000,
    Overloaded = 0x1001,
    IsBootstrapping = 0x1002,
    TruncateError = 0x1003,
    WriteTimeout = 0x1100,
    ReadTimeout = 0x1200,
    SyntaxError = 0x2000,
    Unauthorized = 0x2100,
    Invalid = 0x2200,
    ConfigError = 0x2300,
    AlreadyExists = 0x2400,
    Unprepared = 0x2500,
}

struct WriteTimeout;
struct UnavailableException;

pub enum Error {
    /// Something unexpected happened. This indicates a server-side bug.
    ServerError(String),
    /// Timeout exception during a write request.
    WriteTimeout(String, WriteTimeout),
    /// Some client message triggered a protocol violation (for instance
    /// a QUERY message is sent before a STARTUP one has been sent).
    ProtocolError(String),
    /// The request cannot be processed because the coordinator node is
    /// overloaded.
    Overloaded(String),
    ///
    UnavailableException(String, UnavailableException),
    /// The request was a read request but the coordinator node is
    /// bootstrapping.
    IsBootstrapping(String),
}

impl ErrorCode {
    pub fn from_u32(value: u32) -> Option<ErrorCode> {
        match value {
            0x0000 => Some(ErrorCode::ServerError),
            0x000A => Some(ErrorCode::ProtocolError),
            0x0100 => Some(ErrorCode::BadCredentials),
            0x1000 => Some(ErrorCode::UnavailableException),
            0x1001 => Some(ErrorCode::Overloaded),
            0x1002 => Some(ErrorCode::IsBootstrapping),
            0x1003 => Some(ErrorCode::TruncateError),
            0x1100 => Some(ErrorCode::WriteTimeout),
            0x1200 => Some(ErrorCode::ReadTimeout),
            0x2000 => Some(ErrorCode::SyntaxError),
            0x2100 => Some(ErrorCode::Unauthorized),
            0x2200 => Some(ErrorCode::Invalid),
            0x2300 => Some(ErrorCode::ConfigError),
            0x2400 => Some(ErrorCode::AlreadyExists),
            0x2500 => Some(ErrorCode::Unprepared),
            _ => None,
        }
    }

    pub fn to_u32(&self) -> u32 {
        *self as u32
    }
}
