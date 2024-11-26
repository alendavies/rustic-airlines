use crate::structures::{ApplicationState, HeartbeatState, NodeStatus, Schema};
use std::{
    collections::BTreeMap,
    io::{Cursor, Read},
    net::Ipv4Addr,
};

#[derive(Debug)]
/// Errors that can occur when creating a message.
/// - `InvalidLength`: The message has an invalid length.
/// - `InvalidValue`: The message has an invalid value.
/// - `ConversionError`: Failed to convert bytes to a value.
pub enum MessageError {
    InvalidLength(String),
    InvalidValue(String),
    ConversionError(String),
    CursorError,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug, PartialOrd, Ord, Copy)]
/// A `Digest` used to identify a node in the cluster.
///
/// ### Fields
/// - `address`: The IP address of the node.
/// - `generation`: The generation of the node.
/// - `version`: The version of the node.
pub struct Digest {
    pub address: Ipv4Addr,
    pub generation: u128,
    pub version: u32,
}

impl Digest {
    /// Create a new `Digest` message.
    pub fn new(address: Ipv4Addr, generation: u128, version: u32) -> Self {
        Digest {
            address,
            generation,
            version,
        }
    }

    /// Create a `Digest` message from a `HeartbeatState`.
    pub fn from_heartbeat_state(address: Ipv4Addr, heartbeat_state: &HeartbeatState) -> Self {
        Digest {
            address,
            generation: heartbeat_state.generation,
            version: heartbeat_state.version,
        }
    }

    /// ```md
    /// 0    8    16   24   32
    /// +----+----+----+----+
    /// |    ip address     |
    /// +----+----+----+----+
    /// |     generation    |
    /// +----+----+----+----+
    /// |     generation    |
    /// +----+----+----+----+
    /// |     generation    |
    /// +----+----+----+----+
    /// |     generation    |
    /// +----+----+----+----+
    /// |      version      |
    /// +----+----+----+----+
    /// ```
    /// Convert the `Digest` message to a byte slice.
    pub fn as_bytes(&self) -> Vec<u8> {
        let ip_bytes = self.address.octets();
        let gen_bytes = self.generation.to_be_bytes();
        let ver_bytes = self.version.to_be_bytes();

        let mut bytes = Vec::new();
        bytes.extend_from_slice(&ip_bytes);
        bytes.extend_from_slice(&gen_bytes);
        bytes.extend_from_slice(&ver_bytes);

        bytes
    }

    /// Create a `Digest` messsage from a byte slice.
    pub fn from_bytes(cursor: &mut Cursor<&[u8]>) -> Result<Self, MessageError> {
        let mut address_bytes = [0u8; 4];

        cursor
            .read_exact(&mut address_bytes)
            .map_err(|_| MessageError::CursorError)?;

        let address = Ipv4Addr::from(address_bytes);

        let mut generation_bytes = [0u8; 16];

        cursor
            .read_exact(&mut generation_bytes)
            .map_err(|_| MessageError::CursorError)?;

        let generation = u128::from_be_bytes(generation_bytes);

        let mut version_bytes = [0u8; 4];

        cursor
            .read_exact(&mut version_bytes)
            .map_err(|_| MessageError::CursorError)?;

        let version = u32::from_be_bytes(version_bytes);

        Ok(Digest {
            address,
            generation,
            version,
        })
    }
}

#[derive(Debug, PartialEq, Clone)]
/// A `GossipMessage` used to communicate between nodes in the cluster.
///
/// ### Fields
/// - `from`: The IP address of the sender.
/// - `payload`: The payload of the message.
pub struct GossipMessage {
    pub from: Ipv4Addr,
    pub payload: Payload,
}

impl GossipMessage {
    /// Create a new `GossipMessage`.
    pub fn new(from: Ipv4Addr, payload: Payload) -> Self {
        GossipMessage { from, payload }
    }
}

#[derive(Debug)]
/// The type of payload in a `GossipMessage`.
///
/// - `Syn`: A `Syn` message.
/// - `Ack`: An `Ack` message.
/// - `Ack2`: An `Ack2` message.
pub enum PayloadType {
    Syn = 0x00,
    Ack = 0x01,
    Ack2 = 0x02,
}

#[derive(Debug, PartialEq, Clone)]
/// The payload of a `GossipMessage`.
/// - `Syn`: A `Syn` message.
/// - `Ack`: An `Ack` message.
/// - `Ack2`: An `Ack2` message.
pub enum Payload {
    Syn(Syn),
    Ack(Ack),
    Ack2(Ack2),
}

impl GossipMessage {
    /// ```md
    /// 0    8    16   24   32
    /// +----+----+----+----+
    /// |         ip        |
    /// +----+----+----+----+
    /// |type|   payload    |
    /// +----+----+----+----+
    /// |      payload      |
    /// |        ...        |
    /// +----+----+----+----+
    /// ```
    /// Convert the `GossipMessage` to a byte array.
    pub fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        bytes.extend_from_slice(&self.from.to_bits().to_be_bytes());

        let payload_type = match &self.payload {
            Payload::Syn(_) => PayloadType::Syn as u8,
            Payload::Ack(_) => PayloadType::Ack as u8,
            Payload::Ack2(_) => PayloadType::Ack2 as u8,
        };

        bytes.extend_from_slice(&payload_type.to_be_bytes());

        let payload_bytes = match &self.payload {
            Payload::Syn(syn) => syn.as_bytes(),
            Payload::Ack(ack) => ack.as_bytes(),
            Payload::Ack2(ack2) => ack2.as_bytes(),
        };

        bytes.extend_from_slice(&payload_bytes);

        bytes
    }

    /// Create a `GossipMessage` from a byte slice.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, MessageError> {
        let mut cursor = Cursor::new(bytes);

        let mut bytes_ip = [0u8; 4];
        cursor
            .read_exact(&mut bytes_ip)
            .map_err(|_| MessageError::CursorError)?;
        let mut bytes_type = [0u8; 1];
        cursor
            .read_exact(&mut bytes_type)
            .map_err(|_| MessageError::CursorError)?;

        let mut bytes_payload = Vec::new();
        cursor
            .read_to_end(&mut bytes_payload)
            .map_err(|_| MessageError::CursorError)?;

        let ip = Ipv4Addr::from_bits(u32::from_be_bytes(bytes_ip));

        let payload_type = match u8::from_be_bytes(bytes_type) {
            0x00 => PayloadType::Syn,
            0x01 => PayloadType::Ack,
            0x02 => PayloadType::Ack2,
            _ => panic!(),
        };

        let payload = match payload_type {
            PayloadType::Syn => Payload::Syn(Syn::from_bytes(&bytes_payload)?),
            PayloadType::Ack => Payload::Ack(Ack::from_bytes(&bytes_payload)?),
            PayloadType::Ack2 => Payload::Ack2(Ack2::from_bytes(&bytes_payload)?),
        };

        Ok(Self { from: ip, payload })
    }
}

#[derive(PartialEq, Debug, Clone)]
/// A `Syn` message used to synchronize the state of the cluster.
///
/// ### Fields
/// - `digests`: A list of `Digest` messages.
pub struct Syn {
    pub digests: Vec<Digest>,
}

impl Syn {
    /// Create a new `Syn` message.
    pub fn new(digests: Vec<Digest>) -> Self {
        Syn { digests }
    }

    /// ```md
    /// 0    8    16   24
    /// +----+----+----+
    /// |    digest    |
    /// |      ...     |
    /// +----+----+----+
    /// ```
    /// Convert the `Syn` message to a byte array.
    pub fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        let digest_len = self.digests.len() as u32;

        bytes.extend_from_slice(&digest_len.to_be_bytes());

        for digest in &self.digests {
            bytes.extend_from_slice(&digest.as_bytes());
        }

        bytes
    }

    /// Create a `Syn` message from a byte slice.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, MessageError> {
        let mut cursor = Cursor::new(bytes);

        let mut digest_len_bytes = [0u8; 4];

        cursor
            .read_exact(&mut digest_len_bytes)
            .map_err(|_| MessageError::CursorError)?;

        let digest_len = u32::from_be_bytes(digest_len_bytes);

        let mut digests = Vec::new();

        for _ in 0..digest_len {
            let digest = Digest::from_bytes(&mut cursor).map_err(|_| MessageError::CursorError)?;

            digests.push(digest);
        }

        Ok(Syn { digests })
    }
}

impl Schema {
    /// ```md
    /// 0    8    16   24   32
    /// +----+----+----+----+
    /// |    keyspace       |
    /// |        ...        |
    /// +----+----+----+----+
    /// |     tables        |
    /// |        ...        |
    /// +----+----+----+----+
    /// ```
    /// Convert the `Schema` message to a byte slice.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        let keyspace_len_bytes = (self.keyspace.len() as u32).to_be_bytes();
        let keyspace_bytes = self.keyspace.as_bytes();
        let tables_len = self.tables.len() as u32;
        let mut tables_bytes = vec![];

        for table in &self.tables {
            let table_len = table.len() as u32;
            tables_bytes.extend_from_slice(&table_len.to_be_bytes());
            tables_bytes.extend_from_slice(table.as_bytes());
        }

        bytes.extend_from_slice(&keyspace_len_bytes);
        bytes.extend_from_slice(keyspace_bytes);
        bytes.extend_from_slice(&tables_len.to_be_bytes());
        bytes.extend_from_slice(&tables_bytes);

        bytes
    }

    /// Create a `Schema` message from a byte slice.
    pub fn from_bytes(cursor: &mut Cursor<&[u8]>) -> Result<Self, MessageError> {
        let mut keyspace_len_bytes = [0u8; 4];
        cursor
            .read_exact(&mut keyspace_len_bytes)
            .map_err(|_| MessageError::CursorError)?;
        let keyspace_len = u32::from_be_bytes(keyspace_len_bytes);

        let mut keyspace_bytes = vec![0u8; keyspace_len as usize];
        cursor
            .read_exact(&mut keyspace_bytes)
            .map_err(|_| MessageError::CursorError)?;
        let keyspace = String::from_utf8(keyspace_bytes).map_err(|_| MessageError::CursorError)?;

        let mut tables_len_bytes = [0u8; 4];
        cursor
            .read_exact(&mut tables_len_bytes)
            .map_err(|_| MessageError::CursorError)?;
        let tables_len = u32::from_be_bytes(tables_len_bytes);

        let mut tables = Vec::new();

        for _ in 0..tables_len {
            let mut table_len_bytes = [0u8; 4];
            cursor
                .read_exact(&mut table_len_bytes)
                .map_err(|_| MessageError::CursorError)?;
            let table_len = u32::from_be_bytes(table_len_bytes);

            let mut table_bytes = vec![0u8; table_len as usize];
            cursor
                .read_exact(&mut table_bytes)
                .map_err(|_| MessageError::CursorError)?;
            let table = String::from_utf8(table_bytes).map_err(|_| MessageError::CursorError)?;

            tables.push(table);
        }

        Ok(Schema { keyspace, tables })
    }
}

impl ApplicationState {
    /// Create a new `ApplicationState` message.
    pub fn new(status: NodeStatus, version: u32, schemas: Vec<Schema>) -> Self {
        ApplicationState {
            status,
            version,
            schemas,
        }
    }

    /// ```md
    /// 0    8    16   24   32
    /// +----+----+----+----+
    /// |  status |   0x00  |
    /// +----+----+----+----+
    /// |       version     |
    /// +----+----+----+----+
    /// |       schemas     |
    /// |        ...        |
    /// +----+----+----+----+
    /// ```
    /// Convert the `ApplicationState` message to a byte slice.
    pub fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        let status_bytes = (self.status as u16).to_be_bytes();
        let version_bytes = self.version.to_be_bytes();

        let schemas_len = self.schemas.len() as u32;

        let mut schemas_bytes = vec![];

        for schema in &self.schemas {
            schemas_bytes.extend_from_slice(&schema.to_bytes());
        }

        bytes.extend_from_slice(&status_bytes);
        bytes.extend_from_slice(&version_bytes);
        bytes.extend_from_slice(&schemas_len.to_be_bytes());
        bytes.extend_from_slice(&schemas_bytes);

        bytes
    }

    /// Create an `ApplicationState` message from a byte slice.
    pub fn from_bytes(cursor: &mut Cursor<&[u8]>) -> Result<Self, MessageError> {
        let mut status_bytes = [0u8; 2];
        cursor
            .read_exact(&mut status_bytes)
            .map_err(|_| MessageError::CursorError)?;
        let status_value = u16::from_be_bytes(status_bytes);

        let mut version_bytes = [0u8; 4];
        cursor
            .read_exact(&mut version_bytes)
            .map_err(|_| MessageError::CursorError)?;
        let version = u32::from_be_bytes(version_bytes);

        let status = match status_value {
            0 => NodeStatus::Bootstrap,
            1 => NodeStatus::Normal,
            2 => NodeStatus::Leaving,
            3 => NodeStatus::Removing,
            4 => NodeStatus::Dead,
            _ => {
                return Err(MessageError::InvalidValue(format!(
                    "Invalid NodeStatus value: {}",
                    status_value
                )))
            }
        };

        let mut schemas_len_bytes = [0u8; 4];
        cursor
            .read_exact(&mut schemas_len_bytes)
            .map_err(|_| MessageError::CursorError)?;
        let schemas_len = u32::from_be_bytes(schemas_len_bytes);

        let mut schemas = Vec::new();

        for _ in 0..schemas_len {
            let schema = Schema::from_bytes(cursor).map_err(|_| MessageError::CursorError)?;
            schemas.push(schema);
        }

        Ok(ApplicationState {
            status,
            version,
            schemas,
        })
    }
}

/// The type of information in an `Ack` message.
/// - `Digest`: Only a digest.
/// - `DigestAndInfo`: Digest with info.
enum InfoType {
    /// Only a digest, e.g.
    /// `127.0.0.1:100:15`
    Digest = 0x00,
    /// Digest with info: e.g.
    /// `127.0.0.2:100:15 LOAD:55`
    DigestAndInfo = 0x01,
}

#[derive(Debug, PartialEq, Clone)]
/// An `Ack` message used to acknowledge a `Syn` message.
///
/// ### Fields
/// - `stale_digests`: Local outdated digests which application state need to be updated in the `Ack2`.
/// - `updated_info`: Local updated digests with application state which where outdated in the `Syn`.
pub struct Ack {
    /// Local outdated digests which application state need to be updated in the ACK2.
    pub stale_digests: Vec<Digest>,
    /// Local updated digests with application state which where outdated in the SYN.
    pub updated_info: BTreeMap<Digest, ApplicationState>,
}

impl Ack {
    /// Create a new `Ack` message.
    pub fn new(
        stale_digests: Vec<Digest>,
        updated_info: BTreeMap<Digest, ApplicationState>,
    ) -> Self {
        Ack {
            stale_digests,
            updated_info,
        }
    }

    /// ```md
    /// 0    8    16   24   32
    /// +----+----+----+----+
    /// |        0x00       |
    /// +----+----+----+----+
    /// |                   |
    /// +                   +
    /// |                   |
    /// +                   +
    /// |                   |
    /// +       digest      +
    /// |                   |
    /// +                   +
    /// |                   |
    /// +                   +
    /// |                   |
    /// +----+----+----+----+
    /// |        0x01       |
    /// +----+----+----+----+
    /// |                   |
    /// +                   +
    /// |                   |
    /// +                   +
    /// |                   |
    /// +       digest      +
    /// |                   |
    /// +                   +
    /// |                   |
    /// +                   +
    /// |                   |
    /// +----+----+----+----+
    /// | application state |
    /// +----+----+----+----+
    /// ```
    /// Convert the `Ack` message to a byte array.
    pub fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        let stale_len = self.stale_digests.len() as u32;

        bytes.extend_from_slice(&stale_len.to_be_bytes());

        let info_len = self.updated_info.len() as u32;

        bytes.extend_from_slice(&info_len.to_be_bytes());

        for digest in &self.stale_digests {
            bytes.extend_from_slice(&(InfoType::Digest as u32).to_be_bytes());
            bytes.extend_from_slice(&digest.as_bytes());
        }

        for (digest, info) in &self.updated_info {
            bytes.extend_from_slice(&(InfoType::DigestAndInfo as u32).to_be_bytes());
            bytes.extend_from_slice(&digest.as_bytes());
            bytes.extend_from_slice(&info.as_bytes());
        }

        bytes
    }

    /// Create an `Ack` message from a byte slice.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, MessageError> {
        let mut stale_digests = Vec::new();

        let mut updated_info = BTreeMap::new();

        let mut cursor = Cursor::new(bytes);

        let mut stale_len_bytes = [0u8; 4];

        cursor
            .read_exact(&mut stale_len_bytes)
            .map_err(|_| MessageError::CursorError)?;

        let stale_len = u32::from_be_bytes(stale_len_bytes);

        let mut info_len_bytes = [0u8; 4];

        cursor
            .read_exact(&mut info_len_bytes)
            .map_err(|_| MessageError::CursorError)?;

        let info_len = u32::from_be_bytes(info_len_bytes);

        for _ in 0..stale_len {
            let mut info_type_bytes = [0u8; 4];
            cursor
                .read_exact(&mut info_type_bytes)
                .map_err(|_| MessageError::CursorError)?;

            let info_type = u32::from_be_bytes(info_type_bytes);

            if info_type != InfoType::Digest as u32 {
                return Err(MessageError::InvalidValue(format!(
                    "Invalid InfoType value: {}",
                    info_type
                )));
            }

            let digest = Digest::from_bytes(&mut cursor).map_err(|_| MessageError::CursorError)?;

            stale_digests.push(digest);
        }

        for _ in 0..info_len {
            let mut info_type_bytes = [0u8; 4];
            cursor
                .read_exact(&mut info_type_bytes)
                .map_err(|_| MessageError::CursorError)?;

            let info_type = u32::from_be_bytes(info_type_bytes);

            if info_type != InfoType::DigestAndInfo as u32 {
                return Err(MessageError::InvalidValue(format!(
                    "Invalid InfoType value: {}",
                    info_type
                )));
            }

            let digest = Digest::from_bytes(&mut cursor).map_err(|_| MessageError::CursorError)?;
            let info =
                ApplicationState::from_bytes(&mut cursor).map_err(|_| MessageError::CursorError)?;

            updated_info.insert(digest, info);
        }

        Ok(Ack {
            stale_digests,
            updated_info,
        })
    }
}

#[derive(Debug, PartialEq, Clone)]
/// An `Ack2` message used to acknowledge an `Ack` message.
/// ### Fields
/// - `updated_info`: Local updated digests with application state which were outdated in the `Syn`.
pub struct Ack2 {
    pub updated_info: BTreeMap<Digest, ApplicationState>,
}

impl Ack2 {
    /// Create a new `Ack2` message.
    pub fn new(updated_info: BTreeMap<Digest, ApplicationState>) -> Self {
        Ack2 { updated_info }
    }

    /// ```md
    /// 0    8    16   24   32
    /// +----+----+----+----+
    /// |                   |
    /// +                   +
    /// |                   |
    /// +                   +
    /// |                   |
    /// +       digest      +
    /// |                   |
    /// +                   +
    /// |                   |
    /// +                   +
    /// |                   |
    /// +----+----+----+----+
    /// |                   |
    /// + application state +
    /// |                   |
    /// +----+----+----+----+
    /// ```
    /// Convert the `Ack2` message to a byte array.
    pub fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        let info_len = self.updated_info.len() as u32;

        bytes.extend_from_slice(&info_len.to_be_bytes());

        for (digest, info) in &self.updated_info {
            bytes.extend_from_slice(&digest.as_bytes());
            bytes.extend_from_slice(&info.as_bytes());
        }

        bytes
    }

    /// Create an `Ack2` message from a byte slice.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, MessageError> {
        let mut cursor = Cursor::new(bytes);

        let mut info_len_bytes = [0u8; 4];
        cursor
            .read_exact(&mut info_len_bytes)
            .map_err(|_| MessageError::CursorError)?;

        let digest_len = u32::from_be_bytes(info_len_bytes);

        let mut updated_info = BTreeMap::new();

        for _ in 0..digest_len {
            let digest = Digest::from_bytes(&mut cursor).map_err(|_| MessageError::CursorError)?;
            let app_state =
                ApplicationState::from_bytes(&mut cursor).map_err(|_| MessageError::CursorError)?;

            updated_info.insert(digest, app_state);
        }

        Ok(Ack2 { updated_info })
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, str::FromStr};

    use super::*;

    #[test]
    fn digest_as_bytes_ok() {
        let digest = Digest {
            address: Ipv4Addr::from_str("255.0.0.1").unwrap(),
            generation: 0x0123456789abcdef0123456789abcdef as u128,
            version: 0xfedcba98 as u32,
        };

        let digest_bytes = digest.as_bytes();

        let mut bytes = Vec::new();

        bytes.extend_from_slice(digest.address.octets().as_ref());
        bytes.extend_from_slice(&digest.generation.to_be_bytes());
        bytes.extend_from_slice(&digest.version.to_be_bytes());

        assert_eq!(digest_bytes, bytes)
    }

    #[test]
    fn application_state_as_bytes_ok() {
        let state = ApplicationState {
            status: NodeStatus::Normal,
            version: 0xffffffff,
            schemas: vec![Schema {
                keyspace: "keyspace".to_string(),
                tables: vec!["table1".to_string(), "table2".to_string()],
            }],
        };

        let state_bytes = state.as_bytes();

        let mut bytes = Vec::new();

        let status_bytes = 0x01u16.to_be_bytes();
        bytes.extend_from_slice(&status_bytes);
        let version_bytes = state.version.to_be_bytes();
        bytes.extend_from_slice(&version_bytes);
        let schemas_len_bytes = 1u32.to_be_bytes();
        bytes.extend_from_slice(&schemas_len_bytes);

        let mut schema_bytes = Vec::new();

        for schema in &state.schemas {
            schema_bytes.extend_from_slice(&schema.to_bytes());
        }

        bytes.extend_from_slice(&schema_bytes);

        assert_eq!(state_bytes.to_vec(), bytes);
    }

    #[test]
    fn syn_as_bytes_ok() {
        let node1 = Digest {
            address: Ipv4Addr::from_str("255.0.0.1").unwrap(),
            generation: 0x0123456789abcdef0123456789abcdef as u128,
            version: 0x12345678 as u32,
        };

        let node2 = Digest {
            address: Ipv4Addr::from_str("255.0.0.2").unwrap(),
            generation: 0x0123456789abcdef0123456789abcdef as u128,
            version: 0xfedcba98 as u32,
        };

        let node3 = Digest {
            address: Ipv4Addr::from_str("255.0.0.3").unwrap(),
            generation: 0x0123456789abcdef0123456789abcdef as u128,
            version: 0x98765432 as u32,
        };

        let syn = Syn {
            digests: vec![node1.clone(), node2.clone(), node3.clone()],
        };

        let syn_bytes = syn.as_bytes();

        let mut bytes = Vec::new();

        let digest_len = 3u32.to_be_bytes();

        bytes.extend_from_slice(&digest_len);

        for digest in vec![node1, node2, node3] {
            bytes.extend_from_slice(&digest.as_bytes());
        }

        assert_eq!(syn_bytes, bytes)
    }

    #[test]
    fn ack_as_bytes_ok() {
        let node1 = Digest {
            address: Ipv4Addr::from_str("255.0.0.1").unwrap(),
            generation: 0x0123456789abcdef0123456789abcdef as u128,
            version: 0x12345678 as u32,
        };

        let node2 = Digest {
            address: Ipv4Addr::from_str("255.0.0.2").unwrap(),
            generation: 0x0123456789abcdef0123456789abcdef as u128,
            version: 0xfedcba98 as u32,
        };

        let node3 = Digest {
            address: Ipv4Addr::from_str("255.0.0.3").unwrap(),
            generation: 0x0123456789abcdef0123456789abcdef as u128,
            version: 0x98765432 as u32,
        };

        let node3_state = ApplicationState {
            status: NodeStatus::Normal,
            version: 0xffffffff,
            schemas: vec![Schema {
                keyspace: "keyspace".to_string(),
                tables: vec!["table1".to_string(), "table2".to_string()],
            }],
        };

        let mut updated_info = BTreeMap::new();
        updated_info.insert(node3.clone(), node3_state.clone());

        let ack = Ack {
            stale_digests: vec![node1.clone(), node2.clone()],
            updated_info,
        };

        let ack_bytes = ack.as_bytes();

        let mut bytes = Vec::new();

        let stale_len = 2u32.to_be_bytes();
        bytes.extend_from_slice(&stale_len);

        let info_len = 1u32.to_be_bytes();
        bytes.extend_from_slice(&info_len);

        for digest in ack.stale_digests {
            bytes.extend_from_slice(&(InfoType::Digest as u32).to_be_bytes());
            bytes.extend_from_slice(&digest.as_bytes());
        }

        for (digest, info) in ack.updated_info {
            bytes.extend_from_slice(&(InfoType::DigestAndInfo as u32).to_be_bytes());
            bytes.extend_from_slice(&digest.as_bytes());
            bytes.extend_from_slice(&info.as_bytes());
        }

        assert_eq!(ack_bytes, bytes)
    }

    #[test]
    fn ack2_as_bytes_ok() {
        let node1 = Digest {
            address: Ipv4Addr::from_str("255.0.0.1").unwrap(),
            generation: 0x01 as u128,
            version: 0x8 as u32,
        };

        let node1_state = ApplicationState {
            status: NodeStatus::Normal,
            version: 0x1,
            schemas: vec![Schema {
                keyspace: "keyspace".to_string(),
                tables: vec!["table1".to_string(), "table2".to_string()],
            }],
        };

        let node2 = Digest {
            address: Ipv4Addr::from_str("255.0.0.2").unwrap(),
            generation: 0x9 as u128,
            version: 0x9 as u32,
        };

        let node2_state = ApplicationState {
            status: NodeStatus::Normal,
            version: 0x9,
            schemas: vec![Schema {
                keyspace: "keyspace".to_string(),
                tables: vec!["table1".to_string(), "table2".to_string()],
            }],
        };

        let mut updated_info = BTreeMap::new();
        updated_info.insert(node1.clone(), node1_state.clone());
        updated_info.insert(node2.clone(), node2_state.clone());

        let ack2 = Ack2 { updated_info };

        let ack2_bytes = ack2.as_bytes();

        assert_eq!(ack2_bytes.to_vec(), ack2_bytes);
    }

    #[test]
    fn digest_from_bytes_ok() {
        let expected_digest = Digest {
            address: Ipv4Addr::from_str("255.0.0.1").unwrap(),
            generation: 0x0123456789abcdef0123456789abcdef as u128,
            version: 0xfedcba98 as u32,
        };

        let bytes = expected_digest.as_bytes();

        let mut cursor = Cursor::new(bytes.as_slice());

        let digest = Digest::from_bytes(&mut cursor).unwrap();

        assert_eq!(digest, expected_digest);
    }

    #[test]
    fn syn_from_bytes_ok() {
        let node1 = Digest {
            address: Ipv4Addr::from_str("255.0.0.1").unwrap(),
            generation: 0x0123456789abcdef0123456789abcdef as u128,
            version: 0x12345678 as u32,
        };

        let node2 = Digest {
            address: Ipv4Addr::from_str("255.0.0.2").unwrap(),
            generation: 0x0123456789abcdef0123456789abcdef as u128,
            version: 0xfedcba98 as u32,
        };

        let node3 = Digest {
            address: Ipv4Addr::from_str("255.0.0.3").unwrap(),
            generation: 0x0123456789abcdef0123456789abcdef as u128,
            version: 0x98765432 as u32,
        };

        let expected_syn = Syn {
            digests: Vec::from([node1.clone(), node2.clone(), node3.clone()]),
        };

        let syn_bytes = expected_syn.as_bytes();

        let syn = Syn::from_bytes(&syn_bytes).unwrap();

        assert_eq!(expected_syn, syn);
    }

    #[test]
    fn application_state_from_bytes_ok() {
        let expected_app_state = ApplicationState {
            status: NodeStatus::Removing,
            version: 0xffffffff,
            schemas: vec![Schema {
                keyspace: "keyspace".to_string(),
                tables: vec!["table1".to_string(), "table2".to_string()],
            }],
        };

        let bytes = expected_app_state.as_bytes();

        let mut cursor = Cursor::new(bytes.as_slice());

        let state = ApplicationState::from_bytes(&mut cursor).unwrap();

        assert_eq!(state, expected_app_state);
    }

    #[test]
    fn ack_from_bytes_ok() {
        let node1 = Digest {
            address: Ipv4Addr::from_str("255.0.0.1").unwrap(),
            generation: 0x0123456789abcdef0123456789abcdef as u128,
            version: 0x12345678 as u32,
        };

        let node2 = Digest {
            address: Ipv4Addr::from_str("255.0.0.2").unwrap(),
            generation: 0x0123456789abcdef0123456789abcdef as u128,
            version: 0xfedcba98 as u32,
        };

        let node3 = Digest {
            address: Ipv4Addr::from_str("255.0.0.3").unwrap(),
            generation: 0x0123456789abcdef0123456789abcdef as u128,
            version: 0x98765432 as u32,
        };

        let node3_state = ApplicationState {
            status: NodeStatus::Normal,
            version: 0xffffffff,
            schemas: vec![Schema {
                keyspace: "keyspace".to_string(),
                tables: vec!["table1".to_string(), "table2".to_string()],
            }],
        };

        let mut updated_info = BTreeMap::new();
        updated_info.insert(node3.clone(), node3_state.clone());

        let expected_ack = Ack {
            stale_digests: vec![node1.clone(), node2.clone()],
            updated_info,
        };

        let ack_bytes = expected_ack.as_bytes();

        let ack = Ack::from_bytes(ack_bytes.as_slice()).unwrap();

        assert_eq!(ack, expected_ack);
    }

    #[test]
    fn ack2_from_bytes_ok() {
        let node1 = Digest {
            address: Ipv4Addr::from_str("255.0.0.1").unwrap(),
            generation: 0x0123456789abcdef0123456789abcdef as u128,
            version: 0xfedcba98 as u32,
        };

        let node1_state = ApplicationState {
            status: NodeStatus::Normal,
            version: 0xffffffff,
            schemas: vec![Schema {
                keyspace: "keyspace".to_string(),
                tables: vec!["table1".to_string(), "table2".to_string()],
            }],
        };

        let node2 = Digest {
            address: Ipv4Addr::from_str("255.0.0.2").unwrap(),
            generation: 0x0123456789abcdef0123456789abcdef as u128,
            version: 0x98765432 as u32,
        };

        let node2_state = ApplicationState {
            status: NodeStatus::Normal,
            version: 0xffffffff,
            schemas: vec![Schema {
                keyspace: "keyspace".to_string(),
                tables: vec!["table1".to_string(), "table2".to_string()],
            }],
        };

        let mut updated_info = BTreeMap::new();
        updated_info.insert(node1.clone(), node1_state.clone());
        updated_info.insert(node2.clone(), node2_state.clone());

        let expected_ack2 = Ack2 { updated_info };

        let ack_bytes = expected_ack2.as_bytes();

        let ack2 = Ack2::from_bytes(ack_bytes.as_slice()).unwrap();

        assert_eq!(ack2, expected_ack2);
    }
}
