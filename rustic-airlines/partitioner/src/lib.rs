use murmur3::murmur3_32;
use std::collections::BTreeMap;
use std::io::Cursor;
use std::net::Ipv4Addr;
use std::fmt;
use errors::PartitionerError;
pub mod errors;

#[derive(Clone)]  // Agregamos Copy y Clone
pub struct Partitioner {
    nodes: BTreeMap<u64, Ipv4Addr>,
}

impl Partitioner {
    /// Crea una nueva instancia de Partitioner
    pub fn new() -> Self {
        Partitioner {
            nodes: BTreeMap::new(),
        }
    }

    /// Genera un hash a partir de un valor genérico utilizando `murmur3`
    fn hash_value<T: AsRef<[u8]>>(value: T) -> Result<u64, PartitionerError> {
        let mut hasher = Cursor::new(value);
        murmur3_32(&mut hasher, 0).map(|hash| hash as u64).map_err(|_| PartitionerError::HashError)
    }

    /// Agrega una IP a la estructura y genera el hash automáticamente, manejando el error si ya existe
    pub fn add_node(&mut self, ip: Ipv4Addr) -> Result<(), PartitionerError> {
        let hash = Self::hash_value(ip.octets())?;
        if self.nodes.contains_key(&hash) {
            return Err(PartitionerError::NodeAlreadyExists);
        }
        self.nodes.insert(hash, ip);
        println!("{:?}", self);
        Ok(())
    }

    /// Elimina una IP de la estructura calculando el hash, devuelve error si no existe
    pub fn remove_node(&mut self, ip: Ipv4Addr) -> Result<Ipv4Addr, PartitionerError> {
        let hash = Self::hash_value(ip.octets())?;
        self.nodes.remove(&hash).ok_or(PartitionerError::NodeNotFound)
    }

    /// Devuelve la IP correspondiente para un valor dado.
    /// Encuentra el nodo sucesor más cercano al hash calculado o devuelve un error si el Partitioner está vacío.
    pub fn get_ip<T: AsRef<[u8]>>(&self, value: T) -> Result<Ipv4Addr, PartitionerError> {
        let hash = Self::hash_value(value)?;
        if self.nodes.is_empty() {
            return Err(PartitionerError::EmptyPartitioner);
        }

        // Busca el primer hash que sea mayor o igual al hash dado
        match self.nodes.range(hash..).next() {
            Some((_key, ip)) => Ok(*ip),
            None => {
                // Si no se encontró un sucesor, devolver el primer nodo (envolver al inicio del ring)
                self.nodes.values().next().cloned().ok_or(PartitionerError::EmptyPartitioner)
            }
        }
    }

    /// Devuelve todas las IPs de los nodos, sin las claves (hashes).
    pub fn get_nodes(&self) -> Vec<Ipv4Addr> {
        self.nodes.values().cloned().collect()
    }

    /// Verifica si una IP ya pertenece al Partitioner
    pub fn contains_node(&self, ip: &Ipv4Addr) -> bool {
        let hash = Self::hash_value(ip.octets()).unwrap_or_default();
        self.nodes.contains_key(&hash)
    }
}

/// Implementación personalizada del trait `Debug` para la estructura `Partitioner`
impl fmt::Debug for Partitioner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ips: Vec<String> = self.nodes.values().map(|ip| ip.to_string()).collect();
        if !ips.is_empty() {
            write!(f, "{}", ips.join(" -> "))
        } else {
            write!(f, "No nodes available")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;
    use errors::PartitionerError;

    #[test]
    fn test_add_and_get_ip_with_string() {
        let mut partitioner = Partitioner::new();
        partitioner.add_node(Ipv4Addr::new(192, 168, 0, 1)).unwrap();
        partitioner.add_node(Ipv4Addr::new(192, 168, 0, 2)).unwrap();
        partitioner.add_node(Ipv4Addr::new(192, 168, 0, 3)).unwrap();

        let string_value = String::from("test_string");
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
        let ip = Ipv4Addr::new(192, 168, 0, 1);
        partitioner.add_node(ip).unwrap();
        let result = partitioner.remove_node(ip);
        assert_eq!(result, Ok(ip), "Expected Ok result with IP, got {:?}", result);
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
    fn test_get_ip_wrapping() {
        let mut partitioner = Partitioner::new();
        partitioner.add_node(Ipv4Addr::new(192, 168, 0, 1)).unwrap();
        partitioner.add_node(Ipv4Addr::new(192, 168, 0, 2)).unwrap();
    
        // Hash mayor al mayor nodo existente, debería devolver el primer nodo
        let hash = Partitioner::hash_value(Ipv4Addr::new(255, 255, 255, 255).octets()).unwrap();
        assert_eq!(partitioner.get_ip(hash.to_be_bytes()), Ok(Ipv4Addr::new(192, 168, 0, 1)));
    }

    #[test]
    
    fn test_get_ip_within_range() {
        let mut partitioner = Partitioner::new();
        let ip1 = Ipv4Addr::new(192, 168, 0, 1);
        let ip2 = Ipv4Addr::new(192, 168, 0, 2);
        partitioner.add_node(ip1).unwrap();
        partitioner.add_node(ip2).unwrap();

        // Usar el valor de hash calculado en lugar de `ip1` para simular el valor a buscar
        let hash_to_search = Partitioner::hash_value(ip1.octets()).unwrap();
        let result_ip = partitioner.get_ip(hash_to_search.to_be_bytes());

        // Como puede devolver el sucesor más cercano (por el tipo de búsqueda), vamos a comprobar que
        // el resultado sea o `ip1` o `ip2` (el sucesor de acuerdo a la lógica).
        assert!(
            result_ip == Ok(ip1) || result_ip == Ok(ip2),
            "Expected node IP {} or {}, got {:?}",
            ip1,
            ip2,
            result_ip
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
        let string_value = String::from("test_string");
        let hash = Partitioner::hash_value(string_value.as_bytes());
        assert!(hash.is_ok(), "Expected hash calculation to succeed, got {:?}", hash);
    }

    #[test]
    fn test_debug_trait() {
        let mut partitioner = Partitioner::new();
        partitioner.add_node(Ipv4Addr::new(192, 168, 0, 1)).unwrap();
        partitioner.add_node(Ipv4Addr::new(192, 168, 0, 2)).unwrap();

        let debug_string = format!("{:?}", partitioner);
        assert!(debug_string.contains("192.168.0.1 -> 192.168.0.2") || debug_string.contains("192.168.0.2 -> 192.168.0.1"));
    }
}

