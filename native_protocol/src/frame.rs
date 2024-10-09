use std::vec::Vec;

use crate::error::Error;

pub enum Frame {
    /// Initialize the connection.
    Startup,
    /// Indicates that the server is ready to process queries.
    Ready,
    /// Performs a CQL query.
    Query,
    /// The result to a query.
    Result(),
    /// Indicates an error processing a request.
    Error(Error),
}

struct FrameError;

struct SerializationError;

trait Serializable {
    type Error;

    fn to_bytes(&self) -> Vec<u8>;

    fn from_bytes(bytes: &[u8]) -> Result<Self, Self::Error>
    where
        Self: Sized;
}

impl Frame {
    pub fn to_bytes(&self) -> Vec<u8> {
        todo!()
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Frame, FrameError> {
        if bytes.len() < 9 {
            return Err(FrameError);
        }

        // get opcode and build packet from there

        todo!();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialize_startup_frame() {
        let frame = Frame::Startup;

        assert!(matches!(frame, Frame::Startup));

        let _ = frame.to_bytes();
    }
}
