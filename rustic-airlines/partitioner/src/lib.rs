use murmur3::murmur3_32;
use std::collections::BTreeMap;
use std::io::Cursor;
use std::net::Ipv4Addr;
use std::fmt;

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
    fn hash_value<T: AsRef<[u8]>>(value: T) -> u64 {
        let mut hasher = Cursor::new(value);
        murmur3_32(&mut hasher, 0).unwrap() as u64
    }

    /// Agrega una IP a la estructura y genera el hash automáticamente
    pub fn add_node(&mut self, ip: Ipv4Addr) {
        let hash = Self::hash_value(ip.octets());
        self.nodes.insert(hash, ip);
        println!("{:?}", self);
    }

    /// Elimina una IP de la estructura calculando el hash
    pub fn remove_node(&mut self, ip: Ipv4Addr) -> Option<Ipv4Addr> {
        let hash = Self::hash_value(ip.octets());
        self.nodes.remove(&hash)
    }

    /// Devuelve la IP correspondiente para un valor de hash dado.
    /// Encuentra el nodo sucesor más cercano al hash dado.
    /// Si no hay un sucesor directo (porque el hash es mayor que todos los nodos), envuelve al primer nodo.
    pub fn get_ip(&self, hash: u64) -> Option<Ipv4Addr> {
        if self.nodes.is_empty() {
            return None;
        }

        // Busca el primer hash que sea mayor o igual al hash dado
        match self.nodes.range(hash..).next() {
            Some((_key, ip)) => Some(*ip),
            None => {
                // Si no se encontró un sucesor, devolver el primer nodo (envolver al inicio del ring)
                self.nodes.values().next().cloned()
            }
        }
    }

        /// Devuelve todas las IPs de los nodos, sin las claves (hashes).
    pub fn get_nodes(&self) -> Vec<Ipv4Addr> {
        self.nodes.values().cloned().collect()
    }


    /// Verifica si una IP ya pertenece al Partitioner
    pub fn contains_node(&self, ip: &Ipv4Addr) -> bool {
        let hash = Self::hash_value(ip.octets());
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

    #[test]
    fn test_add_and_get_ip() {
        let mut partitioner = Partitioner::new();
        partitioner.add_node(Ipv4Addr::new(192, 168, 0, 1));
        partitioner.add_node(Ipv4Addr::new(192, 168, 0, 2));
        partitioner.add_node(Ipv4Addr::new(192, 168, 0, 3));

        let hash1 = Partitioner::hash_value(Ipv4Addr::new(192, 168, 0, 1).octets());
        let hash2 = Partitioner::hash_value(Ipv4Addr::new(192, 168, 0, 2).octets());
        let hash3 = Partitioner::hash_value(Ipv4Addr::new(192, 168, 0, 3).octets());

        assert_eq!(partitioner.get_ip(hash1), Some(Ipv4Addr::new(192, 168, 0, 1)));
        assert_eq!(partitioner.get_ip(hash2), Some(Ipv4Addr::new(192, 168, 0, 2)));
        assert_eq!(partitioner.get_ip(hash3), Some(Ipv4Addr::new(192, 168, 0, 3)));
    }

    #[test]
    fn test_remove_node() {
        let mut partitioner = Partitioner::new();
        partitioner.add_node(Ipv4Addr::new(192, 168, 0, 1));
        partitioner.add_node(Ipv4Addr::new(192, 168, 0, 2));
        
        partitioner.remove_node(Ipv4Addr::new(192, 168, 0, 1));

        let hash = Partitioner::hash_value(Ipv4Addr::new(192, 168, 0, 2).octets());
        assert_eq!(partitioner.get_ip(hash), Some(Ipv4Addr::new(192, 168, 0, 2)));
    }

    #[test]
    fn test_get_ip_empty() {
        let partitioner = Partitioner::new();
        assert_eq!(partitioner.get_ip(100), None);
    }

    #[test]
    fn test_get_ip_wrapping() {
        let mut partitioner = Partitioner::new();
        partitioner.add_node(Ipv4Addr::new(192, 168, 0, 1));
        partitioner.add_node(Ipv4Addr::new(192, 168, 0, 2));

        // Hash mayor al mayor nodo existente, debería devolver la última IP
        assert_eq!(partitioner.get_ip(Partitioner::hash_value(Ipv4Addr::new(192, 168, 0, 2).octets())), Some(Ipv4Addr::new(192, 168, 0, 2)));
    }

    #[test]
    fn test_add_duplicate_ip() {
        let mut partitioner = Partitioner::new();
        partitioner.add_node(Ipv4Addr::new(192, 168, 0, 1));
        partitioner.add_node(Ipv4Addr::new(192, 168, 0, 1)); // Reemplaza la IP del hash

        let hash = Partitioner::hash_value(Ipv4Addr::new(192, 168, 0, 1).octets());
        assert_eq!(partitioner.get_ip(hash), Some(Ipv4Addr::new(192, 168, 0, 1)));
    }

    #[test]
    fn test_hash_string() {
        let string_value = String::from("test_string");
        let hash = Partitioner::hash_value(string_value.as_bytes());
        assert!(hash > 0);
    }

    #[test]
    fn test_get_nodes() {
        let mut partitioner = Partitioner::new();
        partitioner.add_node(Ipv4Addr::new(192, 168, 0, 1));
        partitioner.add_node(Ipv4Addr::new(192, 168, 0, 2));

        let nodes = partitioner.get_nodes();
        assert_eq!(nodes.len(), 2);
    }

    #[test]
    fn test_contains_node() {
        let mut partitioner = Partitioner::new();
        partitioner.add_node(Ipv4Addr::new(192, 168, 0, 1));

        assert!(partitioner.contains_node(&Ipv4Addr::new(192, 168, 0, 1)));
        assert!(!partitioner.contains_node(&Ipv4Addr::new(192, 168, 0, 2)));
    }
}
