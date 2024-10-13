use std::net::{TcpStream, Ipv4Addr, SocketAddrV4};
use std::io:: Write;
use query_coordinator::clauses::keyspace::create_keyspace_cql::CreateKeyspace;
use query_coordinator::clauses::keyspace::drop_keyspace_cql::DropKeyspace;
use query_coordinator::clauses::keyspace::alter_keyspace_cql::AlterKeyspace;
use query_coordinator::clauses::table::alter_table_cql::AlterTable;
use query_coordinator::clauses::table::create_table_cql::CreateTable;
use query_coordinator::clauses::table::drop_table_cql::DropTable;
use query_coordinator::clauses::types::column::Column;
use query_coordinator::clauses::types::alter_table_op::AlterTableOperation;
use query_coordinator::clauses::set_sql::Set;
use query_coordinator::clauses::{delete_sql::Delete, insert_sql::Insert, select_sql::Select, update_sql::Update};
use query_coordinator::Query;
use query_coordinator::errors::CQLError;
use std::sync::{Arc, Mutex, MutexGuard};
use crate::table::Table;
use crate::Node;
use std::fs::{self, OpenOptions, File};
use crate::NodeError;
use std::path::Path;
use std::io::{BufRead, BufReader};
use std::time::{SystemTime, UNIX_EPOCH};
use std::collections::HashMap;




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
                self.execute_update(update_query, internode)?;
            }
            Query::Delete(delete_query) => {
                self.execute_delete(delete_query)?;
            }
            Query::CreateTable(create_table) => {
                if self.node_that_execute.lock()?.table_already_exist(create_table.get_name())?{
                    return Err(NodeError::CQLError(CQLError::InvalidTable));
                }
                self.execute_create_table(create_table, internode)?;
            }
            Query::DropTable(drop_table) => {
                self.execute_drop_table(drop_table, internode)?;
            }
            Query::AlterTable(alter_table) => {
                self.execute_alter_table(alter_table, internode)?;
            }
            Query::CreateKeyspace(create_keyspace) => {
                self.execute_create_keyspace(create_keyspace, internode)?;
            }
            Query::DropKeyspace(drop_keyspace) => {
                self.execute_drop_keyspace(drop_keyspace, internode)?;
            }
            Query::AlterKeyspace(alter_keyspace) => {
                self.execute_alter_keyspace(alter_keyspace, internode)?;
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

    pub fn execute_create_keyspace(&self, create_keyspace: CreateKeyspace, internode: bool) -> Result<(), NodeError> {
        
        let mut node = self.node_that_execute.lock().map_err(|_| NodeError::LockError)?;
        node.add_keyspace(create_keyspace.clone())?;
    
        // Obtiene el nombre del keyspace
        let keyspace_name = create_keyspace.get_name().clone();
    
        // Genera el nombre de la carpeta donde se almacenará el keyspace
        let ip_str = node.get_ip().to_string().replace(".", "_");
        let folder_name = format!("keyspaces_{}", ip_str);
    
        // Crea la carpeta del keyspace si no existe
        let keyspace_path = format!("{}/{}", folder_name, keyspace_name);
        if let Err(e) = std::fs::create_dir_all(&keyspace_path) {
            return Err(NodeError::IoError(e));
        }
    
        // Si no es una operación de `internode`, comunicar a otros nodos
        if !internode {
            // Serializa la estructura `CreateKeyspace`
            let serialized_create_keyspace = create_keyspace.serialize();
            self.send_to_other_nodes(node,"CREATE_KEYSPACE_INTERNODE", serialized_create_keyspace)?;
        }
    
        Ok(())
    }

    pub fn execute_drop_keyspace(&self, drop_keyspace: DropKeyspace, internode: bool) -> Result<(), NodeError> {

        // Obtiene el nombre del keyspace a eliminar
        let keyspace_name = drop_keyspace.get_name().clone();
    
        // Bloquea el nodo y remueve el keyspace de la estructura interna
        let mut node = self.node_that_execute.lock().map_err(|_| NodeError::LockError)?;
        node.remove_keyspace(keyspace_name.clone())?;
    
        // Genera el nombre de la carpeta donde se almacena el keyspace
        let ip_str = node.get_ip().to_string().replace(".", "_");
        let folder_name = format!("keyspaces_{}", ip_str);
    
        // Define la ruta del keyspace y elimina la carpeta si existe
        let keyspace_path = format!("{}/{}", folder_name, keyspace_name);
        if let Err(e) = std::fs::remove_dir_all(&keyspace_path) {
            return Err(NodeError::IoError(e));
        }
    
        // Si no es una operación de `internode`, comunicar a otros nodos
        if !internode {
            // Serializa la estructura `DropKeyspace`
            let serialized_drop_keyspace = drop_keyspace.serialize();
            self.send_to_other_nodes(node,"DROP_KEYSPACE_INTERNODE", serialized_drop_keyspace)?;
        }
    
        Ok(())
    }


    pub fn execute_alter_keyspace(
        &self, 
        alter_keyspace: AlterKeyspace, 
        internode: bool
    ) -> Result<(), NodeError> {
        // Buscar el keyspace en la lista de keyspaces
        let mut node = self.node_that_execute.lock()?;
        let mut keyspace = node.actual_keyspace.clone().ok_or(NodeError::OtherError)?.clone();
    
        // Validar si la clase de replicación y el factor son los mismos para evitar operaciones innecesarias
        if keyspace.get_replication_class() == alter_keyspace.get_replication_class()
            && keyspace.get_replication_factor() == alter_keyspace.get_replication_factor()
        {
            return Ok(()); // No hay cambios, nada que ejecutar
        }
        
        keyspace.update_replication_class(alter_keyspace.get_replication_class());
        keyspace.update_replication_factor(alter_keyspace.get_replication_factor());
        node.update_keyspace(keyspace)?;      // Si no es internode, comunicar a otros nodos
        if !internode {
        
            let serialized_alter_keyspace = alter_keyspace.serialize();
            self.send_to_other_nodes(node,"ALTER_KEYSPACE_INTERNODE", serialized_alter_keyspace)?;
        }
    
        Ok(())
    }
    
    
    pub fn execute_create_table(&self, create_table: CreateTable, internode: bool) -> Result<(), NodeError> {
        // Agrega la tabla al nodo
        if self.node_that_execute.lock()?.has_no_actual_keyspace(){
            return Err(NodeError::CQLError(CQLError::Error));
        }
        self.node_that_execute.lock().map_err(|_| NodeError::LockError)?.add_table(create_table.clone())?;

        // Obtiene el nombre de la tabla y la estructura de columnas
        let table_name = create_table.get_name().clone();
        let columns = create_table.get_columns().clone();

        // Genera el nombre de archivo y la carpeta en la cual se almacenará la tabla
        let node = self.node_that_execute.lock().map_err(|_| NodeError::LockError)?;
        let ip_str = node.get_ip().to_string().replace(".", "_");
        let folder_name = format!("keyspaces_{}/{}", ip_str, node.actual_keyspace_name()?);
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
        writeln!(file, "{}", header.join(",")).map_err(NodeError::IoError)?;

        // Si no es internode, comunicar a otros nodos
        if !internode {
            // Serializa la estructura `CreateTable`
            let serialized_create_table = create_table.serialize();
            self.send_to_other_nodes(node,"CREATE_TABLE_INTERNODE", serialized_create_table)?;
        }

        Ok(())
    }


    pub fn execute_drop_table(&self, drop_table: DropTable, internode: bool) -> Result<(), NodeError> {
        // Agrega la tabla al nodo
        if self.node_that_execute.lock()?.has_no_actual_keyspace(){
            return Err(NodeError::CQLError(CQLError::Error));
        }
        // Obtiene el nombre de la tabla a eliminar
        let table_name = drop_table.get_table_name();
    
        // Bloquea el nodo y elimina la tabla de la lista interna
        self.node_that_execute
            .lock()
            .map_err(|_| NodeError::LockError)?
            .remove_table(table_name.clone())?;
    
        // Genera el nombre de archivo y la carpeta en la cual se almacenará la tabla
        let node = self.node_that_execute.lock().map_err(|_| NodeError::LockError)?;
        let ip_str = node.get_ip().to_string().replace(".", "_");
        let folder_name = format!("keyspaces_{}/{}", ip_str, node.actual_keyspace_name()?);
        let file_path = format!("{}/{}.csv", folder_name, table_name);
    
        // Borra el archivo de la tabla si existe
        if std::fs::remove_file(&file_path).is_err() {
            eprintln!("Warning: File {} does not exist or cannot be deleted", file_path);
        }
    
        // Si no es internode, comunicar a otros nodos
        if !internode {
            // Serializa el `DropTable` a un mensaje simple
            let serialized_drop_table = drop_table.serialize();
            self.send_to_other_nodes(node,"DROP_TABLE_INTERNODE", serialized_drop_table)?;
        }
    
        Ok(())
    }
    
    pub fn execute_alter_table(&self, alter_table: AlterTable, internode: bool) -> Result<(), NodeError> {
        // Agrega la tabla al nodo
        if self.node_that_execute.lock()?.has_no_actual_keyspace(){
            return Err(NodeError::CQLError(CQLError::Error));
        }
        // Obtiene el nombre de la tabla y bloquea el acceso a la misma
        let table_name = alter_table.get_table_name();
        let mut node = self.node_that_execute.lock().map_err(|_| NodeError::LockError)?;
        let mut table = node.get_table(table_name.clone())?.inner;
    
        // Ruta del archivo de la tabla
        let ip_str = node.get_ip().to_string().replace(".", "_");
        let folder_name = format!("keyspaces_{}/{}", ip_str, node.actual_keyspace_name()?);
        let file_path = format!("{}/{}.csv", folder_name, table_name);
    
        // Verifica que el archivo exista antes de proceder
        if !Path::new(&file_path).exists() {
            return Err(NodeError::CQLError(CQLError::InvalidTable));
        }
    
        // Aplica las operaciones de alteración
        for operation in alter_table.get_operations() {
            match operation {
                AlterTableOperation::AddColumn(column) => {
                    // Agrega la columna a la estructura interna de la tabla
                    table.add_column(column.clone())?;
                    // Agrega la columna al archivo (actualiza encabezado)
                    Self::add_column_to_file(&file_path, &column.name)?;
                }
                AlterTableOperation::DropColumn(column_name) => {
                    // Elimina la columna de la estructura interna de la tabla
                    table.remove_column(&column_name)?;
                    // Actualiza el archivo para eliminar la columna
                    Self::remove_column_from_file(&file_path, &column_name)?;
                }
                AlterTableOperation::ModifyColumn(_column_name, _new_data_type, _allows_null) => {
                    //no esta soportado todavia, ni se si es necesario que lo este
                    // Modifica la columna en la estructura interna de la tabla
                    return Err(NodeError::CQLError(CQLError::InvalidSyntax));
                    //table.modify_column(&column_name, new_data_type, allows_null)?;
                    // No es necesario modificar el archivo CSV ya que el tipo de dato no afecta el almacenamiento directo
                }
                AlterTableOperation::RenameColumn(old_name, new_name) => {
                    // Renombra la columna en la estructura interna de la tabla
                    table.rename_column(&old_name, &new_name)?;
                    // Actualiza el archivo CSV con el nuevo nombre en el encabezado
                    Self::rename_column_in_file(&file_path, &old_name, &new_name)?;
                }
            }
        }
    
        // Guarda los cambios en el nodo
        node.update_table(table)?;
    
        // Comunica a otros nodos si no es internode
        if !internode {
            let serialized_alter_table = alter_table.serialize();
            self.send_to_other_nodes(node,"ALTER_TABLE_INTERNODE", serialized_alter_table)?;
        }
    
        Ok(())
    }


     // Función auxiliar para agregar una columna al archivo CSV
     fn add_column_to_file(file_path: &str, column_name: &str) -> Result<(), NodeError> {
        let temp_path = format!("{}.temp", file_path);
        let mut temp_file = OpenOptions::new().create(true).write(true).open(&temp_path)?;
    
        // Lee el archivo original y agrega la nueva columna en el encabezado
        let file = OpenOptions::new().read(true).open(file_path)?;
        let reader = BufReader::new(file);
        let mut first_line = true;
    
        for line in reader.lines() {
            let mut line = line?;
            if first_line {
                line.push_str(&format!(",{}", column_name));
                first_line = false;
            } else {
                line.push_str(","); // Agrega una celda vacía en cada fila para la nueva columna
            }
            writeln!(temp_file, "{}", line)?;
        }
    
        fs::rename(temp_path, file_path).map_err(NodeError::IoError)
    }
    
    // Función auxiliar para eliminar una columna del archivo CSV
    fn remove_column_from_file(file_path: &str, column_name: &str) -> Result<(), NodeError> {
        let temp_path = format!("{}.temp", file_path);
        let mut temp_file = OpenOptions::new().create(true).write(true).open(&temp_path)?;
    
        let file = OpenOptions::new().read(true).open(file_path)?;
        let reader = BufReader::new(file);
        let mut col_index: Option<usize> = None;
    
        for line in reader.lines() {
            let line = line?;
            let cells: Vec<&str> = line.split(',').collect();
    
            if col_index.is_none() {
                // Encuentra el índice de la columna a eliminar
                col_index = cells.iter().position(|&col| col == column_name);
                if col_index.is_none() {
                    return Err(NodeError::CQLError(CQLError::InvalidColumn));
                }
            }
    
            let filtered_line: Vec<&str> = cells.iter().enumerate()
                .filter(|&(i, _)| Some(i) != col_index)
                .map(|(_, &cell)| cell)
                .collect();
            
            writeln!(temp_file, "{}", filtered_line.join(","))?;
        }
    
        fs::rename(temp_path, file_path).map_err(NodeError::IoError)
    }
    
    // Función auxiliar para renombrar una columna en el archivo CSV
    fn rename_column_in_file(file_path: &str, old_name: &str, new_name: &str) -> Result<(), NodeError> {
        let temp_path = format!("{}.temp", file_path);
        let mut temp_file = OpenOptions::new().create(true).write(true).open(&temp_path)?;
    
        let file = OpenOptions::new().read(true).open(file_path)?;
        let reader = BufReader::new(file);
    
        for (i, line) in reader.lines().enumerate() {
            let mut line = line?;
            if i == 0 {
                line = line.replace(old_name, new_name); // Renombra en la cabecera
            }
            writeln!(temp_file, "{}", line)?;
        }
    
        fs::rename(temp_path, file_path).map_err(NodeError::IoError)
    }

    fn execute_insert(&self, insert_query: Insert, table_to_insert: Table, internode: bool) -> Result<(), NodeError> {

        // Agrega la tabla al nodo
        if self.node_that_execute.lock()?.has_no_actual_keyspace(){
            return Err(NodeError::CQLError(CQLError::Error));
        }

        if !self.node_that_execute.lock()?.table_already_exist(table_to_insert.get_name())?{
            return Err(NodeError::CQLError(CQLError::Error));
        }

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
                QueryExecution::insert_in_this_node(values, ip, insert_query.into_clause.table_name, pos, node.actual_keyspace_name()?)?;
                return Ok(());
            }

            let mut stream = self.connect(ip, self.connections.clone())?;
            let serialized_insert = insert_query.serialize();
            let message = format!("INSERT_INTERNODE {}", serialized_insert);

            self.send_message(&mut stream, &message)?;
        } else {
            
            QueryExecution::insert_in_this_node(values, node.get_ip(), insert_query.into_clause.table_name, pos, node.actual_keyspace_name()? )?;
        }

        Ok(())
    }

    fn insert_in_this_node(values: Vec<String>, ip: Ipv4Addr, table_name: String, index_of_primary_key: usize, actual_keyspace_name: String) -> Result<(), NodeError> {
        // Convertimos la IP a string para usar en el nombre de la carpeta
        let ip_str = ip.to_string().replace(".", "_");
    
        let folder_name = format!("keyspaces_{}/{}", ip_str, actual_keyspace_name);
        let folder_path = Path::new(&folder_name);

        if !folder_path.exists() {
            fs::create_dir_all(&folder_path).map_err(NodeError::IoError)?;
        }

        // Nombre de la tabla para almacenar la data, agregando la extensión ".csv"
        let file_path = folder_path.join(format!("{}.csv", table_name));

        // Genera un nombre único para el archivo temporal
        let temp_file_path = folder_path.join(format!("{}.tmp", SystemTime::now().duration_since(UNIX_EPOCH).map_err(|_| NodeError::OtherError)?.as_nanos()));
        
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
                    writeln!(temp_file, "{}", values.join(",")).map_err(NodeError::IoError)?;
                    key_exists = true;
                } else {
                    // Si no coincide, copia la línea actual al archivo temporal
                    writeln!(temp_file, "{}", line).map_err(NodeError::IoError)?;
                }
            }
        }

        // Si no existe una fila con la clave primaria, agrega la nueva fila al final
        if !key_exists {
            writeln!(temp_file, "{}", values.join(",")).map_err(NodeError::IoError)?;
        }

        // Renombramos el archivo temporal para que reemplace al archivo original
        fs::rename(&temp_file_path, &file_path).map_err(NodeError::IoError)?;

        Ok(())
    }


     // Función auxiliar para enviar un mensaje a todos los nodos en el partitioner
     fn send_to_other_nodes(&self, peer_node: MutexGuard<'_, Node>,header: &str, serialized_message: String)-> Result<(), NodeError> {
        // Serializa el objeto que se quiere enviar
        let message = format!("{} {}", header, serialized_message);

        // Bloquea el nodo para obtener el partitioner y la IP
        let current_ip = peer_node.get_ip();

        // Recorre los nodos del partitioner y envía el mensaje a cada nodo excepto el actual
        for ip in peer_node.get_partitioner().get_nodes() {
            if ip != current_ip {
                let mut stream = self.connect(ip, self.connections.clone())?;
                self.send_message(&mut stream, &message)?;
            }
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

    pub fn validate_update_types(set_clause: Set, columns: Vec<Column>)->Result< (), NodeError>{
        for (column_name, value) in set_clause.get_pairs(){
            for column in &columns{
                if *column_name == column.name{
                    if !column.data_type.is_valid_value(value){
                        return Err(NodeError::CQLError(CQLError::InvalidSyntax))
                    }
                }
            }
        }
        Ok(())
    }

    pub fn execute_update(&self, update_query: Update, internode: bool) -> Result<(), NodeError> {
        // Obtiene el nombre de la tabla y genera la ruta del archivo
        let table_name = update_query.table_name.clone();
        let node = self.node_that_execute.lock().map_err(|_| NodeError::LockError)?;
        let table = node.get_table(table_name.clone())?;
        let ip_str = node.get_ip().to_string().replace(".", "_");
        let folder_name = format!("keyspaces_{}/{}", ip_str, node.actual_keyspace_name()?);
        let file_path = format!("{}/{}.csv", folder_name, table_name);
    
        // Genera un nombre único para el archivo temporal
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| NodeError::OtherError)?
            .as_nanos();
        let temp_file_path = format!("{}.{}.temp", file_path, timestamp);
    
        // Abre el archivo original y el archivo temporal
        let file = OpenOptions::new().read(true).open(&file_path)?;
        let reader = BufReader::new(file);
        let mut temp_file = match File::create(&temp_file_path) {
            Ok(file) => file,
            Err(e) => return Err(NodeError::from(e)),
        };
    
        // Lee el encabezado (primera línea) y lo escribe en el archivo temporal
        let mut lines = reader.lines();
        if let Some(header_line) = lines.next() {
            if let Err(e) = writeln!(temp_file, "{}", header_line?) {
                let _ = std::fs::remove_file(&temp_file_path);
                return Err(NodeError::from(e));
            }
        }
    
        // Itera sobre cada línea de datos del archivo original
        for line in lines {
            let line = line?;
            let mut columns: Vec<String> = line.split(',').map(|s| s.trim().to_string()).collect();
    
            // Crea un HashMap para representar la fila como columna-valor
            let mut column_value_map: HashMap<String, String> = HashMap::new();
            for (i, column) in table.get_columns().iter().enumerate() {
                if let Some(value) = columns.get(i) {
                    if column.is_primary_key {
                        column_value_map.insert(column.name.clone(), value.clone());
                    }
                }
            }
    
            // Verifica si la condición `WHERE` se cumple
            if let Some(where_clause) = &update_query.where_clause {
                let condition ;
                if let Ok(value) = where_clause.condition.execute(&column_value_map) {
                    condition = value;
                }else {
                    let _ = std::fs::remove_file(&temp_file_path);
                    return Err(NodeError::CQLError(CQLError::InvalidColumn));
                }
                if condition {
                    // Aplica las modificaciones de `SET`
                    for (column, new_value) in update_query.clone().set_clause.get_pairs() {
                        let index = table.get_column_index(column).ok_or(NodeError::CQLError(CQLError::InvalidColumn))?;
                        columns[index] = new_value.clone();
                    }
                }
            }
            
            if let Err(e) = Self::validate_update_types(update_query.clone().set_clause, table.get_columns()){
                let _ = std::fs::remove_file(&temp_file_path);
                 return Err(e);
            }
            // Escribe la línea modificada o la línea original en el archivo temporal
            if let Err(e) = writeln!(temp_file, "{}", columns.join(",")) {
                let _ = std::fs::remove_file(&temp_file_path);
                return Err(NodeError::from(e));
            }
        }
    
        // Reemplaza el archivo original con el archivo temporal
        if let Err(e) = std::fs::rename(&temp_file_path, &file_path) {
            let _ = std::fs::remove_file(&temp_file_path);
            return Err(NodeError::from(e));
        }
        
        // Si `internode` es false, envía el `UPDATE` a otros nodos
        if !internode {
            let serialized_update = update_query.serialize();
            if let Err(e) = self.send_to_other_nodes(node, "UPDATE_INTERNODE", serialized_update) {
                let _ = std::fs::remove_file(&temp_file_path);
                return Err(e);
            }
        }
    
        Ok(())
    }
    

    fn execute_delete(&self, delete_query: Delete) -> Result<(), NodeError> {
        println!("Ejecutando DELETE de manera distribuida: {:?}", delete_query);
        Ok(())
    }
}
