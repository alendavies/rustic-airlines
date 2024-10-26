use errors::PartitionerError;
use murmur3::murmur3_32;
use std::collections::BTreeMap;
use std::fmt;
use std::io::Cursor;
use std::net::Ipv4Addr;
pub mod errors;

#[derive(Clone)]
pub struct Partitioner {
    nodes: BTreeMap<u64, Ipv4Addr>,
}

impl Partitioner {
    pub fn new() -> Self {
        Partitioner {
            nodes: BTreeMap::new(),
        }
    }

    fn hash_value<T: AsRef<[u8]>>(value: T) -> Result<u64, PartitionerError> {
        let mut hasher = Cursor::new(value);
        murmur3_32(&mut hasher, 0)
            .map(|hash| hash as u64)
            .map_err(|_| PartitionerError::HashError)
    }

    pub fn add_node(&mut self, ip: Ipv4Addr) -> Result<(), PartitionerError> {
        let hash = Self::hash_value(ip.to_string())?;
        if self.nodes.contains_key(&hash) {
            return Err(PartitionerError::NodeAlreadyExists);
        }
        self.nodes.insert(hash, ip);
        println!("{:?}", self);
        Ok(())
    }

    pub fn remove_node(&mut self, ip: Ipv4Addr) -> Result<Ipv4Addr, PartitionerError> {
        let hash = Self::hash_value(ip.to_string())?;
        self.nodes
            .remove(&hash)
            .ok_or(PartitionerError::NodeNotFound)
    }

    pub fn get_ip<T: AsRef<[u8]>>(&self, value: T) -> Result<Ipv4Addr, PartitionerError> {
        let hash = Self::hash_value(value)?;
        if self.nodes.is_empty() {
            return Err(PartitionerError::EmptyPartitioner);
        }

        match self.nodes.range(hash..).next() {
            Some((_key, addr)) => Ok(*addr),
            None => self
                .nodes
                .values()
                .next()
                .cloned()
                .ok_or(PartitionerError::EmptyPartitioner),
        }
    }

    pub fn get_nodes(&self) -> Vec<Ipv4Addr> {
        self.nodes.values().cloned().collect()
    }

    pub fn contains_node(&self, ip: &Ipv4Addr) -> bool {
        let hash = Self::hash_value(ip.to_string()).unwrap_or_default();
        self.nodes.contains_key(&hash)
    }

    pub fn get_n_successors<T: AsRef<[u8]>>(
        &self,
        value: T,
        n: usize,
    ) -> Result<Vec<Ipv4Addr>, PartitionerError> {
        if self.nodes.is_empty() {
            return Err(PartitionerError::EmptyPartitioner);
        }

        // Usar get_ip para obtener el nodo donde cae el hash
        let starting_ip = self.get_ip(&value)?;

        // Crear un iterador que comience desde el siguiente nodo al starting_ip
        let mut successors = Vec::new();

        for (_key, addr) in self.nodes.iter() {
            if !successors.contains(addr) && *addr != starting_ip {
                successors.push(*addr);
            }
            if successors.len() == n {
                break;
            }
        }

        Ok(successors)
    }
}

impl fmt::Debug for Partitioner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let addresses: Vec<String> = self.nodes.values().map(|addr| addr.to_string()).collect();
        if !addresses.is_empty() {
            write!(f, "{}", addresses.join(" -> "))
        } else {
            write!(f, "No nodes available")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use errors::PartitionerError;
    use std::net::Ipv4Addr;

    #[test]
    fn test_add_and_get_ip_with_string() {
        let mut partitioner = Partitioner::new();
        partitioner.add_node(Ipv4Addr::new(192, 168, 0, 1)).unwrap();
        partitioner.add_node(Ipv4Addr::new(192, 168, 0, 2)).unwrap();
        partitioner.add_node(Ipv4Addr::new(192, 168, 0, 3)).unwrap();

        let string_value = "test_string";
        let result = partitioner.get_ip(string_value.as_bytes());
        assert!(result.is_ok(), "Expected Ok result, got {:?}", result);
    }

    #[test]
    fn test_add_duplicate_ip() {
        let mut partitioner = Partitioner::new();
        partitioner.add_node(Ipv4Addr::new(192, 168, 0, 1)).unwrap();
        let result = partitioner.add_node(Ipv4Addr::new(192, 168, 0, 1));
        assert_eq!(result, Err(PartitionerError::NodeAlreadyExists));
    }

    #[test]
    fn test_remove_existing_node() {
        let mut partitioner = Partitioner::new();
        let addr = Ipv4Addr::new(192, 168, 0, 1);
        partitioner.add_node(addr).unwrap();
        let result = partitioner.remove_node(addr);
        assert_eq!(
            result,
            Ok(addr),
            "Expected Ok result with Addr, got {:?}",
            result
        );
    }

    #[test]
    fn test_remove_nonexistent_node() {
        let mut partitioner = Partitioner::new();
        let result = partitioner.remove_node(Ipv4Addr::new(192, 168, 0, 1));
        assert_eq!(result, Err(PartitionerError::NodeNotFound));
    }

    #[test]
    fn test_get_ip_empty() {
        let partitioner = Partitioner::new();
        assert_eq!(
            partitioner.get_ip(100u64.to_be_bytes()),
            Err(PartitionerError::EmptyPartitioner)
        );
    }

    #[test]
    fn test_get_ip_within_range() {
        let mut partitioner = Partitioner::new();
        let addr1 = Ipv4Addr::new(192, 168, 0, 1);
        let addr2 = Ipv4Addr::new(192, 168, 0, 2);
        partitioner.add_node(addr1).unwrap();
        partitioner.add_node(addr2).unwrap();

        let hash_to_search = Partitioner::hash_value(addr1.octets()).unwrap();
        let result_addr = partitioner.get_ip(hash_to_search.to_be_bytes());

        assert!(
            result_addr == Ok(addr1) || result_addr == Ok(addr2),
            "Expected node Addr {} or {}, got {:?}",
            addr1,
            addr2,
            result_addr
        );
    }

    #[test]
    fn test_contains_node() {
        let mut partitioner = Partitioner::new();
        partitioner.add_node(Ipv4Addr::new(192, 168, 0, 1)).unwrap();

        assert!(partitioner.contains_node(&Ipv4Addr::new(192, 168, 0, 1)));
        assert!(!partitioner.contains_node(&Ipv4Addr::new(192, 168, 0, 2)));
    }

    #[test]
    fn test_get_nodes() {
        let mut partitioner = Partitioner::new();
        partitioner.add_node(Ipv4Addr::new(192, 168, 0, 1)).unwrap();
        partitioner.add_node(Ipv4Addr::new(192, 168, 0, 2)).unwrap();

        let nodes = partitioner.get_nodes();
        assert_eq!(nodes.len(), 2);
        assert!(nodes.contains(&Ipv4Addr::new(192, 168, 0, 1)));
        assert!(nodes.contains(&Ipv4Addr::new(192, 168, 0, 2)));
    }

    #[test]
    fn test_hash_string_error_handling() {
        let string_value = "test_string";
        let hash = Partitioner::hash_value(string_value.as_bytes());
        assert!(
            hash.is_ok(),
            "Expected hash calculation to succeed, got {:?}",
            hash
        );
    }

    #[test]
    fn test_debug_trait() {
        let mut partitioner = Partitioner::new();
        partitioner.add_node(Ipv4Addr::new(192, 168, 0, 1)).unwrap();
        partitioner.add_node(Ipv4Addr::new(192, 168, 0, 2)).unwrap();

        let debug_string = format!("{:?}", partitioner);
        assert!(
            debug_string.contains("192.168.0.1 -> 192.168.0.2")
                || debug_string.contains("192.168.0.2 -> 192.168.0.1"),
            "Debug output mismatch: got {}",
            debug_string
        );
    }

    #[test]
    fn test_get_n_successors_no_duplicates_skip_current() {
        let mut partitioner = Partitioner::new();
        partitioner.add_node(Ipv4Addr::new(192, 168, 0, 1)).unwrap();
        partitioner.add_node(Ipv4Addr::new(192, 168, 0, 2)).unwrap();
        partitioner.add_node(Ipv4Addr::new(192, 168, 0, 3)).unwrap();
        partitioner.add_node(Ipv4Addr::new(192, 168, 0, 4)).unwrap();

        let string_value = "test_value";
        let hash = Partitioner::hash_value(string_value).unwrap();

        // Llamar a la función y verificar los resultados
        let successors = partitioner.get_n_successors(string_value, 2).unwrap();
        let unique_successors: std::collections::HashSet<_> = successors.iter().collect();

        // Asegurar que los sucesores no incluyan el nodo correspondiente al hash calculado
        let starting_node = partitioner.get_ip(hash.to_be_bytes()).unwrap();

        // Verificar que los sucesores son únicos y no contienen el nodo de inicio
        assert_eq!(
            unique_successors.len(),
            successors.len(),
            "Expected unique successors without duplicates, got {:?}",
            successors
        );
        assert!(
            !successors.contains(&starting_node),
            "Expected successors to skip the starting node, but it was included"
        );

        // Verificar que el número de sucesores es el solicitado o el máximo disponible
        assert!(
            successors.len() <= 2,
            "Expected at most 2 successors, but got {:?}",
            successors
        );
    }
}
