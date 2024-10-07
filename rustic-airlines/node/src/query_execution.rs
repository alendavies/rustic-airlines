use std::net::{TcpStream, Ipv4Addr, SocketAddrV4};
use std::io:: Write;
use query_coordinator::clauses::{delete_sql::Delete, insert_sql::Insert, select_sql::Select, update_sql::Update};
use query_coordinator::Query;
use query_coordinator::errors::CQLError;
use std::sync::{Arc, Mutex};
use crate::Node;
use std::fs::{self, OpenOptions};
use crate::NodeError;
use std::path::Path;


pub struct QueryExecution {
    node_that_execute: Arc<Mutex<Node>>,
    connections: Arc<Mutex<Vec<TcpStream>>>,
}

impl QueryExecution {
    // Constructor de QueryExecution
    pub fn new(node_that_execute: Arc<Mutex<Node>>, connections: Arc<Mutex<Vec<TcpStream>>>) -> QueryExecution {
        QueryExecution {
            node_that_execute,
            connections,
        }
    }

    // Método para ejecutar la query según su tipo
    pub fn execute(&self, query: Query) -> Result<(), NodeError> {
        match query {
            Query::Select(select_query) => {
                self.execute_select(select_query)?;
            }
            Query::Insert(insert_query) => {
                self.execute_insert(insert_query)?;
            }
            Query::Update(update_query) => {
                self.execute_update(update_query)?;
            }
            Query::Delete(delete_query) => {
                self.execute_delete(delete_query)?;
            }
        }
        Ok(())
    }

    // Método para conectarse a un nodo
    pub fn connect(&self, peer_ip: Ipv4Addr, connections: Arc<Mutex<Vec<TcpStream>>>) -> Result<TcpStream, NodeError> {
        let address = SocketAddrV4::new(peer_ip, 8080);
        let stream = TcpStream::connect(address).map_err(NodeError::IoError)?;
        {
            let mut connections_guard = connections.lock().map_err(|_| NodeError::LockError)?;
            connections_guard.push(stream.try_clone().map_err(NodeError::IoError)?);
        }
        Ok(stream)
    }

    // Método para enviar un mensaje
    pub fn send_message(&self, stream: &mut TcpStream, message: &str) -> Result<(), NodeError> {
        stream.write_all(message.as_bytes()).map_err(NodeError::IoError)?;
        stream.write_all(b"\n").map_err(NodeError::IoError)?;
        Ok(())
    }

    fn execute_select(&self, select_query: Select) -> Result<(), NodeError> {
        println!("Ejecutando SELECT localmente: {:?}", select_query);
        Ok(())
    }

    fn execute_insert(&self, insert_query: Insert) -> Result<(), NodeError> {
        let columnas = insert_query.into_clause.columns.clone();
        let primary_key = "location";

        // Encuentra la posición de la clave primaria
        let pos = columnas
            .iter()
            .position(|x| x == &primary_key)
            .ok_or(NodeError::CQLError(CQLError::InvalidColumn))?;

        let values = insert_query.values.clone();
        let value_to_hash = values[pos].clone();

        let node = self.node_that_execute.lock().map_err(|_| NodeError::LockError)?;

        if node.is_seed() {
            // Aplica la función de hash con el partitioner y obtiene la IP correspondiente
            let ip = node.get_partitioner().get_ip(value_to_hash)?;

            if ip == node.get_ip() {
                QueryExecution::insert_in_this_node(values, ip, insert_query.into_clause.table_name)?;
                return Ok(());
            }

            let mut stream = self.connect(ip, self.connections.clone())?;
            let serialized_insert = insert_query.serialize();
            let message = format!("INSERT {}", serialized_insert);

            self.send_message(&mut stream, &message)?;
        } else {
            
            QueryExecution::insert_in_this_node(values, node.get_ip(), insert_query.into_clause.table_name)?;
        }

        Ok(())
    }

    fn insert_in_this_node(values: Vec<String>, ip: Ipv4Addr, table_name: String) -> Result<(), NodeError> {
        // Convertimos la IP a string para usar en el nombre de la carpeta
        let ip_str = ip.to_string().replace(".", "_");
        let folder_name = format!("keyspaces_{}", ip_str);

        // Carpeta "Airports" dentro de "keyspaces_{ip}"
        let airports_folder_name = format!("{}/PLANES", folder_name);
        let airports_folder_path = Path::new(&airports_folder_name);

        if !airports_folder_path.exists() {
            fs::create_dir_all(&airports_folder_path).map_err(NodeError::IoError)?;
        }

        // Nombre de la tabla para almacenar la data, agregando la extensión ".csv"
        let file_path = airports_folder_path.join(format!("{}.csv", table_name));

        // Abre el archivo en modo append (crear si no existe)
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(file_path)
            .map_err(NodeError::IoError)?;

        // Escribe los valores en el archivo separados por comas
        let values_str = values.join(", ");
        writeln!(file, "{}", values_str).map_err(NodeError::IoError)?;

        Ok(())
    }

    fn execute_update(&self, update_query: Update) -> Result<(), NodeError> {
        println!("Ejecutando UPDATE de manera distribuida: {:?}", update_query);
        Ok(())
    }

    fn execute_delete(&self, delete_query: Delete) -> Result<(), NodeError> {
        println!("Ejecutando DELETE de manera distribuida: {:?}", delete_query);
        Ok(())
    }
}
