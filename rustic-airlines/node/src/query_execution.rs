use std::net::{TcpStream, Ipv4Addr, SocketAddrV4};
use std::io:: Write;
use query_coordinator::clauses::table::alter_table_cql::AlterTable;
use query_coordinator::clauses::table::create_table_cql::CreateTable;
use query_coordinator::clauses::table::drop_table_cql::DropTable;
use query_coordinator::clauses::types::column::Column;
use query_coordinator::clauses::{delete_sql::Delete, insert_sql::Insert, select_sql::Select, update_sql::Update};
use query_coordinator::Query;
use query_coordinator::errors::CQLError;
use std::sync::{Arc, Mutex};
use crate::Node;
use std::fs::{self, OpenOptions, File};
use crate::NodeError;
use std::path::Path;
use std::io::{BufRead, BufReader};

use std::time::{SystemTime, UNIX_EPOCH};



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
    pub fn execute(&self, query: Query, internode: bool) -> Result<(), NodeError> {
        match query {
            Query::Select(select_query) => {
                self.execute_select(select_query)?;
            }
            Query::Insert(insert_query) => {
                let table_name = insert_query.into_clause.table_name.clone();
                let table = self.node_that_execute.lock()?.get_table(table_name)?;
                self.execute_insert(insert_query, table, internode)?;
            }
            Query::Update(update_query) => {
                self.execute_update(update_query)?;
            }
            Query::Delete(delete_query) => {
                self.execute_delete(delete_query)?;
            }
            Query::CreateTable(create_table) => {
                if self.node_that_execute.lock()?.table_already_exist(create_table.clone()){
                    return Err(NodeError::CQLError(CQLError::InvalidTable));
                }
                self.execute_create_table(create_table, internode)?;
            }
            Query::DropTable(drop_table) => {
                self.execute_drop_table(drop_table)?;
            }
            Query::AlterTable(alter_table) => {
                self.execute_alter_table(alter_table)?;
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

    pub fn execute_create_table(&self, create_table: CreateTable, internode: bool) -> Result<(), NodeError> {
        // Agrega la tabla al nodo
        self.node_that_execute.lock().map_err(|_| NodeError::LockError)?.add_table(create_table.clone())?;

        // Obtiene el nombre de la tabla y la estructura de columnas
        let table_name = create_table.get_name().clone();
        let columns = create_table.get_columns().clone();

        // Genera el nombre de archivo y la carpeta en la cual se almacenará la tabla
        let node = self.node_that_execute.lock().map_err(|_| NodeError::LockError)?;
        let ip_str = node.get_ip().to_string().replace(".", "_");
        let folder_name = format!("keyspaces_{}/PLANES", ip_str);
        let file_path = format!("{}/{}.csv", folder_name, table_name);

        // Crea la carpeta si no existe
        if let Err(e) = std::fs::create_dir_all(&folder_name) {
            return Err(NodeError::IoError(e));
        }

        // Crea el archivo y escribe las columnas como encabezado
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&file_path)
            .map_err(NodeError::IoError)?;
        
        let header: Vec<String> = columns.iter().map(|col| col.name.clone()).collect();
        writeln!(file, "{}", header.join(", ")).map_err(NodeError::IoError)?;

        // Si no es internode, comunicar a otros nodos
        if !internode {
        
            // Serializa la estructura `CreateTable`
            let serialized_create_table = create_table.serialize();

            // Envía el mensaje `CREATE_TABLE_INTERNODE` a cada nodo en el partitioner
            for ip in node.get_partitioner().get_nodes() {
                if ip != node.get_ip() {
                    let mut stream = self.connect(ip, self.connections.clone())?;
                    let message = format!("CREATE_TABLE_INTERNODE {}", serialized_create_table);
                    self.send_message(&mut stream, &message)?;
                }
            }
        }

        Ok(())
    }


    fn execute_drop_table(&self, drop_table: DropTable) -> Result<(), NodeError> {
        println!("Ejecutando SELECT localmente: {:?}", drop_table);
        Ok(())
    }

    fn execute_alter_table(&self, alter_table: AlterTable) -> Result<(), NodeError> {
        println!("Ejecutando SELECT localmente: {:?}", alter_table);
        Ok(())
    }

    fn execute_insert(&self, insert_query: Insert, table_to_insert: CreateTable, internode: bool) -> Result<(), NodeError> {
        let columns = table_to_insert.get_columns();
        let primary_key = columns.iter().find(|column| column.is_primary_key).ok_or(NodeError::CQLError(CQLError::InvalidSyntax))?;

        // Encuentra la posición de la clave primaria
        let pos = columns
            .iter()
            .position(|x| x == primary_key)
            .ok_or(NodeError::CQLError(CQLError::InvalidColumn))?;

        let values = insert_query.values.clone();
        let value_to_hash = values[pos].clone();

        let node = self.node_that_execute.lock().map_err(|_| NodeError::LockError)?;

        self.validate_values(columns, &values)?;

        if !internode {
            // Aplica la función de hash con el partitioner y obtiene la IP correspondiente
            let ip = node.get_partitioner().get_ip(value_to_hash)?;

            if ip == node.get_ip() {
                QueryExecution::insert_in_this_node(values, ip, insert_query.into_clause.table_name, pos)?;
                return Ok(());
            }

            let mut stream = self.connect(ip, self.connections.clone())?;
            let serialized_insert = insert_query.serialize();
            let message = format!("INSERT_INTERNODE {}", serialized_insert);

            self.send_message(&mut stream, &message)?;
        } else {
            
            QueryExecution::insert_in_this_node(values, node.get_ip(), insert_query.into_clause.table_name, pos)?;
        }

        Ok(())
    }

    pub fn validate_values(&self, columns: Vec<Column>, values: &[String]) -> Result<(), CQLError> {
        if values.len() != columns.len() {
            return Err(CQLError::InvalidColumn);
        }

        for (column, value) in columns.iter().zip(values) {
            if !column.data_type.is_valid_value(value) {
                return Err(CQLError::InvalidSyntax);
            }
        }
        Ok(())
    }

    fn insert_in_this_node(values: Vec<String>, ip: Ipv4Addr, table_name: String, index_of_primary_key: usize) -> Result<(), NodeError> {
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

        // Genera un nombre único para el archivo temporal
        let temp_file_path = airports_folder_path.join(format!("{}.tmp", SystemTime::now().duration_since(UNIX_EPOCH).map_err(|_| NodeError::OtherError)?.as_nanos()));
        
        // Abre el archivo temporal en modo de escritura
        let mut temp_file = File::create(&temp_file_path).map_err(NodeError::IoError)?;

        // Si el archivo de la tabla existe, lo abrimos en modo de lectura
        let file = OpenOptions::new().read(true).open(&file_path);
        let mut key_exists = false;
        
        if let Ok(file) = file {
            let reader = BufReader::new(file);

            for line in reader.lines() {
                let line = line.map_err(NodeError::IoError)?;
                let row_values: Vec<&str> = line.split(',').map(|s| s.trim()).collect();

                // Verifica si la clave primaria coincide
                if row_values.get(index_of_primary_key) == Some(&values[index_of_primary_key].as_str()) {
                    // Si coincide, escribe la nueva fila en lugar de la antigua
                    writeln!(temp_file, "{}", values.join(", ")).map_err(NodeError::IoError)?;
                    key_exists = true;
                } else {
                    // Si no coincide, copia la línea actual al archivo temporal
                    writeln!(temp_file, "{}", line).map_err(NodeError::IoError)?;
                }
            }
        }

        // Si no existe una fila con la clave primaria, agrega la nueva fila al final
        if !key_exists {
            writeln!(temp_file, "{}", values.join(", ")).map_err(NodeError::IoError)?;
        }

        // Renombramos el archivo temporal para que reemplace al archivo original
        fs::rename(&temp_file_path, &file_path).map_err(NodeError::IoError)?;

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
