use std::io::{Cursor, Read};

// use gossip::messages::GossipMessage;

#[derive(Debug, PartialEq)]
pub enum InternodeMessage {
    Query(String),
    Response(String),
    // Gossip(GossipMessage),
}

enum Opcode {
    Query = 0x01,
    Response = 0x02,
    // Gossip = 0x03,
}

#[derive(Debug)]
pub struct InternodeMessageError;

impl InternodeMessage {
    // 0    8    16   24   32
    // +----+----+----+----+
    // |type|    content   |
    // +----+----+----+----+
    // |      content      |
    // |        ...        |
    // |      content      |
    // +----+----+----+----+
    pub fn as_bytes(&self) -> Result<Vec<u8>, InternodeMessageError> {
        let mut bytes = Vec::new();

        let opcode = match self {
            InternodeMessage::Query(_) => Opcode::Query as u8,
            InternodeMessage::Response(_) => Opcode::Response as u8,
            // InternodeMessage::Gossip(_) => Opcode::Gossip as u8,
        };

        let body_bytes = match self {
            InternodeMessage::Query(query) => query.as_bytes(),
            InternodeMessage::Response(response) => response.as_bytes(),
            // InternodeMessage::Gossip(gossip_message) => &gossip_message.as_bytes(),
        };

        bytes.push(opcode);
        bytes.extend_from_slice(body_bytes);

        Ok(bytes)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, InternodeMessageError> {
        let mut cursor = Cursor::new(bytes);

        let mut opcode_bytes = [0u8];
        cursor.read_exact(&mut opcode_bytes).unwrap();
        let opcode = opcode_bytes[0];

        let mut body_bytes = Vec::new();
        cursor.read_to_end(&mut body_bytes).unwrap();

        let content = match opcode {
            0x01 => Self::Query(String::from_utf8(body_bytes).unwrap()),
            0x02 => Self::Response(String::from_utf8(body_bytes).unwrap()),
            // 0x03 => Self::Gossip(GossipMessage::from_bytes(&body_bytes).unwrap()),
            _ => panic!("Invalid opcode for internode frame."),
        };

        Ok(content)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_to_bytes() {
        let query = "SELECT * FROM something";
        let msg = InternodeMessage::Query(query.to_string());
        let bytes = msg.as_bytes().unwrap();

        assert_eq!(bytes, [vec![0x01], query.as_bytes().to_vec()].concat());
    }

    #[test]
    fn test_query_from_bytes() {
        let query = "SELECT * FROM something";
        let bytes = [vec![0x01], query.as_bytes().to_vec()].concat();
        let msg = InternodeMessage::from_bytes(&bytes).unwrap();

        assert_eq!(msg, InternodeMessage::Query(query.to_string()));
    }

    #[test]
    fn test_response_to_bytes() {
        let response = "DATA DATA DATA";
        let msg = InternodeMessage::Response(response.to_string());
        let bytes = msg.as_bytes().unwrap();

        assert_eq!(bytes, [vec![0x02], response.as_bytes().to_vec()].concat());
    }

    #[test]
    fn test_response_from_bytes() {
        let response = "DATA DATA DATA";
        let bytes = [vec![0x02], response.as_bytes().to_vec()].concat();
        let msg = InternodeMessage::from_bytes(&bytes).unwrap();

        assert_eq!(msg, InternodeMessage::Response(response.to_string()));
    }
}
