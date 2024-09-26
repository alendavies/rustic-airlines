use std::net::Ipv4Addr;

#[derive(Clone)]
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
}

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
}
