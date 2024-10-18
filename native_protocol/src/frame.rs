use std::{
    io::{Cursor, Read},
    vec::Vec,
};

use crate::{
    header::{Flags, FrameHeader, Opcode, Version},
    messages::{error::Error, query::Query, result::result::Result},
    types::{Int, Short},
    ByteSerializable, Serializable, SerializationError,
};

pub enum Frame {
    /// Initialize the connection.
    Startup,
    /// Indicates that the server is ready to process queries.
    Ready,
    /// Performs a CQL query.
    Query(Query),
    /// The result to a query.
    Result(Result),
    /// Indicates an error processing a request.
    Error(Error),
}

struct FrameError;

impl Serializable for Frame {
    /// 0         8        16        24        32         40
    /// +---------+---------+---------+---------+---------+
    /// | version |  flags  |      stream       | opcode  |
    /// +---------+---------+---------+---------+---------+
    /// |                length                 |         |
    /// +---------+---------+---------+---------+---------+
    /// |                                                 |
    /// .                ...  body ...                    .
    /// .                                                 .
    /// .                                                 .
    /// +-------------------------------------------------+
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        let version = match self {
            Frame::Startup | Frame::Query(_) => Version::RequestV3,
            Frame::Ready | Frame::Result(_) | Frame::Error(_) => Version::ResponseV3,
        };

        let opcode = match self {
            Frame::Startup => Opcode::Startup,
            Frame::Ready => Opcode::Ready,
            Frame::Query(_) => Opcode::Query,
            Frame::Result(_) => Opcode::Result,
            Frame::Error(_) => Opcode::Error,
        };

        let flags = Flags {
            compression: false,
            tracing: false,
        };

        let body_bytes = match self {
            Frame::Startup => vec![0x00, 0x00], // View 4.1.1., the startup body is a [string map] of options, but we do not use them. The [string map] requires 2 bytes for the length nonetheless, therefore, the 0x0000.
            Frame::Ready => Vec::new(),
            Frame::Query(query) => query.to_bytes(),
            Frame::Result(result) => result.to_bytes(),
            Frame::Error(error) => error.to_bytes(),
        };

        let length = u32::try_from(body_bytes.len()).unwrap();

        let header = FrameHeader::new(version, flags, 0, opcode, length);

        let header_bytes = header.to_bytes();

        bytes.extend_from_slice(&header_bytes);
        bytes.extend_from_slice(&body_bytes);

        bytes
    }

    fn from_bytes(bytes: &[u8]) -> std::result::Result<Self, SerializationError> {
        let mut cursor = Cursor::new(bytes);

        // Read version (1 byte)
        let mut version_bytes = [0u8];
        cursor.read_exact(&mut version_bytes).unwrap();
        let _ = u8::from_be_bytes(version_bytes);

        // Read flags (1 byte)
        let mut flags_bytes = [0u8];
        cursor.read_exact(&mut flags_bytes).unwrap();
        let _ = Flags::from_byte(flags_bytes[0]).unwrap();

        // Read stream (2 bytes)
        let mut stream_bytes = [0u8; 2];
        cursor.read_exact(&mut stream_bytes).unwrap();
        let _ = Short::from_be_bytes(stream_bytes);

        // Read opcode (2 bytes)
        let mut opcode_bytes = [0u8];
        cursor.read_exact(&mut opcode_bytes).unwrap();
        let opcode = Opcode::from_byte(opcode_bytes[0]).unwrap();

        // Read body length (4 bytes)
        let mut length_bytes = [0u8; 4];
        cursor.read_exact(&mut length_bytes).unwrap();
        let length = Int::from_be_bytes(length_bytes);

        // Read body
        let mut body = vec![0u8; length.try_into().unwrap()];
        cursor.read_exact(&mut body).unwrap();

        let frame = match opcode {
            Opcode::Startup => Self::Startup,
            Opcode::Ready => Self::Ready,
            Opcode::Query => Self::Query(Query::from_bytes(&body)),
            Opcode::Error => Self::Error(Error::from_bytes(&body).unwrap()),
            Opcode::Result => Self::Result(Result::from_bytes(&body)?),
            _ => unimplemented!(),
        };

        Ok(frame)
    }
}

#[cfg(test)]
mod tests {
    use crate::messages::query::{Consistency, QueryParams};

    use super::*;

    #[test]
    fn test_frame_to_bytes_startup() {
        let frame = Frame::Startup;
        let bytes = frame.to_bytes();

        let expected_bytes = vec![
            0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00,
        ];

        assert_eq!(bytes, expected_bytes);
    }

    #[test]
    fn test_frame_to_bytes_ready() {
        let frame = Frame::Ready;
        let bytes = frame.to_bytes();

        let expected_bytes = vec![0x83, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00];

        assert_eq!(bytes, expected_bytes);
    }

    #[test]
    fn test_frame_to_bytes_query() {
        let query_string = "SELECT * FROM table WHERE id = 1".to_string();
        let query_params = QueryParams::new(Consistency::One, vec![]);
        let query = Query::new(query_string, query_params);

        let body_bytes = query.to_bytes();
        let frame = Frame::Query(query);

        let body_len = body_bytes.len() as u8;

        let bytes = frame.to_bytes();

        let mut expected_bytes: Vec<u8> =
            vec![0x03, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, body_len];

        expected_bytes.extend_from_slice(body_bytes.as_slice());

        assert_eq!(bytes, expected_bytes);
    }

    #[test]
    fn test_frame_to_bytes_error() {
        let error_message = "Error".to_string();
        let error = Error::ServerError(error_message);

        let body_bytes = error.to_bytes();
        let frame = Frame::Error(error);

        let body_len = body_bytes.len() as u8;

        let bytes = frame.to_bytes();

        let mut expected_bytes: Vec<u8> =
            vec![0x83, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, body_len];

        expected_bytes.extend_from_slice(body_bytes.as_slice());

        assert_eq!(bytes, expected_bytes);
    }

    #[test]
    fn bytes_to_frame_startup() {
        let bytes = Frame::Startup.to_bytes();
        let frame = Frame::from_bytes(&bytes).unwrap();

        assert!(matches!(frame, Frame::Startup))
    }

    #[test]
    fn bytes_to_frame_ready() {
        let bytes = Frame::Ready.to_bytes();
        let frame = Frame::from_bytes(&bytes).unwrap();

        assert!(matches!(frame, Frame::Ready))
    }

    #[test]
    fn bytes_to_frame_query() {
        let query_string = "SELECT * FROM table WHERE id = 1".to_string();
        let query_params = QueryParams::new(Consistency::One, vec![]);
        let query = Query::new(query_string.clone(), query_params.clone());
        let bytes = Frame::Query(query).to_bytes();

        let frame = Frame::from_bytes(&bytes).unwrap();

        assert!(matches!(frame, Frame::Query(_)));

        let query = match frame {
            Frame::Query(query) => query,
            _ => panic!(),
        };

        assert_eq!(query.query, query_string);
        assert_eq!(query.params, query_params);
    }
}
