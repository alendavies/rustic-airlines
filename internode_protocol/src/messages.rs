use core::panic;
use std::{
    io::{Cursor, Read},
    net::IpAddr,
};

use gossip::messages::GossipMessage;

use crate::Serializable;

/// An error that occurs when serializing or deserializing an internode message.
#[derive(Debug)]
pub struct InternodeMessageError;

#[derive(Clone, Copy, Debug, PartialEq)]
enum Opcode {
    Gossip = 0x03,
}

#[derive(Debug)]
pub struct InternodeMessage {
    /// The IP address of the destination node.
    //pub to: IpAddr, // TODO: remove
    /// The content of the message.
    pub content: InternodeMessageContent,
}

impl InternodeMessage {
    /// Creates a new internode message.
    pub fn new(content: InternodeMessageContent) -> Self {
        Self { content }
    }
}

impl Serializable for InternodeMessage {
    /// ```md
    /// 0    8    16   24   32
    /// +----+----+----+----+
    /// |       header      |
    /// +----+----+----+----+
    /// |head|  content...
    /// +----+----+----+----+
    /// ```
    /// Serializes the message into a byte vector.
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        let opcode = match self.content {
            InternodeMessageContent::Gossip(_) => Opcode::Gossip,
        };

        let content_bytes = match &self.content {
            InternodeMessageContent::Gossip(gossip_message) => gossip_message.as_bytes(),
        };

        let header = InternodeHeader {
            opcode,
            length: content_bytes.len() as u32,
        };

        bytes.extend_from_slice(&header.as_bytes());
        bytes.extend_from_slice(&content_bytes);

        bytes
    }

    /// Deserializes the message from a byte slice.
    fn from_bytes(bytes: &[u8]) -> Result<Self, InternodeMessageError> {
        let mut cursor = Cursor::new(bytes);

        let mut header_bytes = [0u8; HEADER_SIZE];
        cursor
            .read_exact(&mut header_bytes)
            .map_err(|_| InternodeMessageError)?;

        let header = InternodeHeader::from_bytes(&header_bytes).unwrap();

        let mut content_bytes = vec![0u8; header.length as usize];
        cursor.read_exact(&mut content_bytes).unwrap();

        let content = match header.opcode {
            Opcode::Gossip => {
                InternodeMessageContent::Gossip(GossipMessage::from_bytes(&content_bytes).unwrap())
            }
        };
        let message = InternodeMessage { content };

        Ok(message)
    }
}

#[derive(Debug, PartialEq)]
struct InternodeHeader {
    opcode: Opcode,
    //ip: Ipv4Addr,
    length: u32,
}

const HEADER_SIZE: usize = 5;

impl Serializable for InternodeHeader {
    /// ```md
    /// 0    8    16   24   32
    /// +----+----+----+----+
    /// |  content_length   |
    /// +----+----+----+----+
    /// | op |              |
    /// +----+----+----+----+
    /// ```
    /// Serializes the header into a byte vector.
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        bytes.extend_from_slice(&self.length.to_be_bytes());
        bytes.push(self.opcode as u8);

        bytes
    }

    /// Deserializes the header from a byte slice.
    fn from_bytes(bytes: &[u8]) -> Result<Self, InternodeMessageError>
    where
        Self: Sized,
    {
        let mut cursor = Cursor::new(bytes);

        let mut len_bytes = [0u8; 4];
        cursor.read_exact(&mut len_bytes).unwrap();

        let length = u32::from_be_bytes(len_bytes);

        let mut opcode_byte = [0u8; 1];
        cursor.read_exact(&mut opcode_byte).unwrap();

        let opcode = match opcode_byte[0] {
            0x03 => Opcode::Gossip,
            _ => panic!(),
        };

        Ok(InternodeHeader { opcode, length })
    }
}

#[derive(Debug)]
pub enum InternodeMessageContent {
    Gossip(GossipMessage),
}
