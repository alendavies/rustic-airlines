use std::{collections::HashMap, net::Ipv4Addr};

#[derive(Debug)]
enum MessageError {
    InvalidLength(String),
    InvalidValue(String),
    ConversionError(String),
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct Digest {
    address: Ipv4Addr,
    generation: u128,
    version: u32,
}

impl Digest {
    // 0    8    16   24   32
    // +----+----+----+----+
    // |    ip address     |
    // +----+----+----+----+
    // |     generation    |
    // +----+----+----+----+
    // |     generation    |
    // +----+----+----+----+
    // |     generation    |
    // +----+----+----+----+
    // |     generation    |
    // +----+----+----+----+
    // |      version      |
    // +----+----+----+----+
    pub fn as_bytes(&self) -> [u8; 24] {
        let ip_bytes = self.address.octets();
        let gen_bytes: [u8; 16] = self.generation.to_be_bytes();
        let ver_bytes: [u8; 4] = self.version.to_be_bytes();

        let mut bytes = [0xff; 24];
        bytes[..4].copy_from_slice(&ip_bytes);
        bytes[4..20].copy_from_slice(&gen_bytes);
        bytes[20..].copy_from_slice(&ver_bytes);

        bytes
    }

    /// Create a `Digest` messsage from a byte array.
    /// - The byte array must be 24 bytes long.
    /// - The first 4 bytes are the IP address.
    /// - The next 16 bytes are the generation.
    /// - The last 4 bytes are the version.
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, MessageError> {
        if bytes.len() != 24 {
            return Err(MessageError::InvalidLength(format!(
                "Digest must be 24 bytes, got {}",
                bytes.len()
            )));
        }

        let address = Ipv4Addr::new(bytes[0], bytes[1], bytes[2], bytes[3]);

        let generation = u128::from_be_bytes(bytes[4..20].try_into().map_err(|_| {
            MessageError::ConversionError("Failed to convert generation bytes".to_string())
        })?);

        let version = u32::from_be_bytes(bytes[20..24].try_into().map_err(|_| {
            MessageError::ConversionError("Failed to convert version bytes".to_string())
        })?);

        Ok(Digest {
            address,
            generation,
            version,
        })
    }
}

#[derive(PartialEq, Debug)]
struct Syn {
    digests: Vec<Digest>,
}

impl Syn {
    pub fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.digests.len() * 24);

        for digest in &self.digests {
            bytes.extend_from_slice(&digest.as_bytes());
        }

        bytes
    }

    /// Create a `Syn` message from a byte array.
    /// - The byte array must be a multiple of 24 bytes.
    /// - Each 24 bytes chunk is a `Digest`.
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, MessageError> {
        if bytes.len() % 24 != 0 {
            return Err(MessageError::InvalidLength(format!(
                "Syn must be a multiple of 24 bytes, got {}",
                bytes.len()
            )));
        }

        let mut digests = Vec::new();

        for chunk in bytes.chunks(24) {
            digests.push(Digest::from_bytes(chunk.to_vec())?);
        }

        Ok(Syn { digests })
    }
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum NodeStatus {
    Bootstrap = 0x0,
    Normal = 0x1,
    Leaving = 0x2,
    Removing = 0x3,
}

#[derive(Clone, PartialEq, Debug)]
struct ApplicationState {
    status: NodeStatus,
}

impl ApplicationState {
    // 0    4    8   12   16
    // +----+----+----+----+
    // | status  | ???
    // por ahora pongo el status nomás
    pub fn as_bytes(&self) -> [u8; 4] {
        let status_bytes: [u8; 4] = (self.status as u32).to_be_bytes();

        status_bytes
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, MessageError> {
        if bytes.len() != 4 {
            return Err(MessageError::InvalidLength(format!(
                "ApplicationState must be 4 bytes, got {}",
                bytes.len()
            )));
        }

        let status_value = u32::from_be_bytes(bytes.try_into().map_err(|_| {
            MessageError::ConversionError("Failed to convert ApplicationState bytes".to_string())
        })?);

        let status = match status_value {
            0 => NodeStatus::Bootstrap,
            1 => NodeStatus::Normal,
            2 => NodeStatus::Leaving,
            3 => NodeStatus::Removing,
            _ => {
                return Err(MessageError::InvalidValue(format!(
                    "Invalid NodeStatus value: {}",
                    status_value
                )))
            }
        };

        Ok(ApplicationState { status })
    }
}

enum InfoType {
    /// Only a digest, e.g.
    /// `127.0.0.1:100:15`
    Digest = 0x00,
    /// Digest with info: e.g.
    /// `127.0.0.2:100:15 LOAD:55`
    DigestAndInfo = 0x01,
}

#[derive(Debug, PartialEq)]
struct Ack {
    stale_digests: Vec<Digest>,
    updated_info: HashMap<Digest, ApplicationState>,
}

impl Ack {
    // 0    8    16   24   32
    // +----+----+----+----+
    // |        0x00       |
    // +----+----+----+----+
    // |                   |
    // +                   +
    // |                   |
    // +                   +
    // |                   |
    // +       digest      +
    // |                   |
    // +                   +
    // |                   |
    // +                   +
    // |                   |
    // +----+----+----+----+
    // |        0x01       |
    // +----+----+----+----+
    // |                   |
    // +                   +
    // |                   |
    // +                   +
    // |                   |
    // +       digest      +
    // |                   |
    // +                   +
    // |                   |
    // +                   +
    // |                   |
    // +----+----+----+----+
    // | application state |
    // +----+----+----+----+
    pub fn as_bytes(&self) -> Vec<u8> {
        let length = self.stale_digests.len() * 28 + self.updated_info.len() * 32;
        let mut bytes = Vec::with_capacity(length);

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

    /// Create an `Ack` message from a byte array.
    /// - The byte array must be a multiple of 28 or 32 bytes.
    /// - Each 28 bytes chunk is a `Digest`.
    /// - Each 32 bytes chunk is a `Digest` followed by an `ApplicationState`.
    /// - The first 4 bytes of each chunk is the `InfoType`.
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, MessageError> {
        let mut stale_digests = Vec::new();
        let mut updated_info = HashMap::new();
        let mut i = 0;

        while i < bytes.len() {
            // Check if there are enough bytes to read the InfoType, which is 4 bytes
            if i + 4 > bytes.len() {
                return Err(MessageError::InvalidLength(
                    "Incomplete InfoType in Ack".to_string(),
                ));
            }

            // Read the 4 bytes of the InfoType
            let info_type = u32::from_be_bytes(bytes[i..i + 4].try_into().map_err(|_| {
                MessageError::ConversionError("Failed to convert InfoType bytes".to_string())
            })?);

            // If the InfoType was successfully read, move the index 4 bytes
            i += 4;

            match info_type {
                0 => {
                    // Digest
                    // Check if there are enough bytes to read the Digest, which is 24 bytes
                    if i + 24 > bytes.len() {
                        return Err(MessageError::InvalidLength(
                            "Incomplete Digest in Ack".to_string(),
                        ));
                    }

                    let digest = Digest::from_bytes(bytes[i..i + 24].to_vec())?;
                    stale_digests.push(digest);

                    // If the Digest was successfully read, move the index 24 bytes
                    i += 24;
                }
                1 => {
                    // DigestAndInfo
                    // Check if there are enough bytes to read the DigestAndInfo, which is 28 bytes
                    // (24 bytes for the Digest and 4 bytes for the ApplicationState)
                    if i + 28 > bytes.len() {
                        return Err(MessageError::InvalidLength(
                            "Incomplete DigestAndInfo in Ack".to_string(),
                        ));
                    }

                    let digest = Digest::from_bytes(bytes[i..i + 24].to_vec())?;
                    let app_state = ApplicationState::from_bytes(bytes[i + 24..i + 28].to_vec())?;
                    updated_info.insert(digest, app_state);

                    // If the DigestAndInfo was successfully read, move the index 28 bytes
                    i += 28;
                }
                _ => {
                    return Err(MessageError::InvalidValue(format!(
                        "Invalid InfoType in Ack: {}",
                        info_type
                    )))
                }
            }
        }

        Ok(Ack {
            stale_digests,
            updated_info,
        })
    }
}

struct Ack2 {
    updated_info: HashMap<Digest, ApplicationState>,
}

impl Ack2 {
    // 0    8    16   24   32
    // +----+----+----+----+
    // |                   |
    // +                   +
    // |                   |
    // +                   +
    // |                   |
    // +       digest      +
    // |                   |
    // +                   +
    // |                   |
    // +                   +
    // |                   |
    // +----+----+----+----+
    // | application state |
    // +----+----+----+----+
    pub fn as_bytes(&self) -> Vec<u8> {
        let length = self.updated_info.len() * 32;
        let mut bytes = Vec::with_capacity(length);

        for (digest, info) in &self.updated_info {
            bytes.extend_from_slice(&(InfoType::DigestAndInfo as u32).to_be_bytes());
            bytes.extend_from_slice(&digest.as_bytes());
            bytes.extend_from_slice(&info.as_bytes());
        }

        bytes
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn digest_as_bytes_ok() {
        let digest = Digest {
            address: Ipv4Addr::from_str("255.0.0.1").unwrap(),
            generation: 0x0123456789abcdef0123456789abcdef as u128,
            version: 0xfedcba98 as u32,
        };

        let digest_bytes = digest.as_bytes();

        assert_eq!(
            digest_bytes,
            [
                0xff, 0x00, 0x00, 0x01, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23,
                0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98,
            ]
        )
    }

    #[test]
    fn application_state_as_bytes_ok() {
        let state = ApplicationState {
            status: NodeStatus::Normal,
        };

        let state_bytes = state.as_bytes();

        assert_eq!(state_bytes, [0x00, 0x00, 0x00, 0x01]);
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

        assert_eq!(
            syn_bytes,
            [node1.as_bytes(), node2.as_bytes(), node3.as_bytes()].concat()
        )
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
        };

        let mut updated_info = HashMap::new();
        updated_info.insert(node3.clone(), node3_state.clone());

        let ack = Ack {
            stale_digests: vec![node1.clone(), node2.clone()],
            updated_info,
        };

        let ack_bytes = ack.as_bytes();

        assert_eq!(
            ack_bytes,
            [
                [0x00; 4].to_vec(),
                node1.as_bytes().to_vec(),
                [0x00; 4].to_vec(),
                node2.as_bytes().to_vec(),
                [0x00, 0x00, 0x00, 0x01].to_vec(),
                node3.as_bytes().to_vec(),
                node3_state.as_bytes().to_vec()
            ]
            .concat()
        )
    }

    #[test]
    fn ack2_as_bytes_ok() {
        let node1 = Digest {
            address: Ipv4Addr::from_str("255.0.0.1").unwrap(),
            generation: 0x0123456789abcdef0123456789abcdef as u128,
            version: 0x12345678 as u32,
        };

        let node1_state = ApplicationState {
            status: NodeStatus::Normal,
        };

        let node2 = Digest {
            address: Ipv4Addr::from_str("255.0.0.2").unwrap(),
            generation: 0x0123456789abcdef0123456789abcdef as u128,
            version: 0x98765432 as u32,
        };

        let node2_state = ApplicationState {
            status: NodeStatus::Normal,
        };

        let mut updated_info = HashMap::new();
        updated_info.insert(node1.clone(), node1_state.clone());
        updated_info.insert(node2.clone(), node2_state.clone());

        let ack2 = Ack2 { updated_info };

        let ack2_bytes = ack2.as_bytes();

        assert_eq!(
            ack2_bytes,
            [
                [0x00, 0x00, 0x00, 0x01].to_vec(),
                node1.as_bytes().to_vec(),
                node1_state.as_bytes().to_vec(),
                [0x00, 0x00, 0x00, 0x01].to_vec(),
                node2.as_bytes().to_vec(),
                node2_state.as_bytes().to_vec(),
            ]
            .concat()
        )
    }

    #[test]
    fn digest_from_bytes_ok() {
        let bytes = [
            0xff, 0x00, 0x00, 0x01, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23,
            0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98,
        ]
        .to_vec();

        let expected_digest = Digest {
            address: Ipv4Addr::from_str("255.0.0.1").unwrap(),
            generation: 0x0123456789abcdef0123456789abcdef as u128,
            version: 0xfedcba98 as u32,
        };

        let digest = Digest::from_bytes(bytes).unwrap();

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
            digests: vec![node1.clone(), node2.clone(), node3.clone()],
        };

        let node1_bytes = [
            0xff, 0x00, 0x00, 0x01, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23,
            0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x12, 0x34, 0x56, 0x78,
        ]
        .to_vec();

        let node2_bytes = [
            0xff, 0x00, 0x00, 0x02, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23,
            0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98,
        ]
        .to_vec();

        let node3_bytes = [
            0xff, 0x00, 0x00, 0x03, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23,
            0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x98, 0x76, 0x54, 0x32,
        ]
        .to_vec();

        let syn_bytes = [node1_bytes, node2_bytes, node3_bytes];

        let syn = Syn::from_bytes(syn_bytes.concat()).unwrap();

        assert_eq!(syn, expected_syn);
    }

    #[test]
    fn application_state_from_bytes_ok() {
        let bytes = [0x00, 0x00, 0x00, 0x03].to_vec();

        let expected_app_state = ApplicationState {
            status: NodeStatus::Removing,
        };

        let state = ApplicationState::from_bytes(bytes).unwrap();

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
        };

        let mut updated_info = HashMap::new();
        updated_info.insert(node3.clone(), node3_state.clone());

        let expected_ack = Ack {
            stale_digests: vec![node1.clone(), node2.clone()],
            updated_info,
        };

        let node1_bytes = [
            0xff, 0x00, 0x00, 0x01, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23,
            0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x12, 0x34, 0x56, 0x78,
        ]
        .to_vec();

        let node2_bytes = [
            0xff, 0x00, 0x00, 0x02, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23,
            0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98,
        ]
        .to_vec();

        let node3_bytes = [
            0xff, 0x00, 0x00, 0x03, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23,
            0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x98, 0x76, 0x54, 0x32,
        ]
        .to_vec();

        let node3_state_bytes = [0x00, 0x00, 0x00, 0x01].to_vec();

        let ack_bytes = [
            [0x00; 4].to_vec(),
            node1_bytes,
            [0x00; 4].to_vec(),
            node2_bytes,
            [0x00, 0x00, 0x00, 0x01].to_vec(),
            node3_bytes,
            node3_state_bytes,
        ];

        let ack = Ack::from_bytes(ack_bytes.concat()).unwrap();

        assert_eq!(ack, expected_ack);
    }
}
