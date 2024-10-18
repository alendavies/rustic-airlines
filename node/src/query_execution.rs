use crate::internode_protocol_handler::InternodeProtocolHandler;
use crate::table::Table;
use crate::utils::{connect, send_message};
use crate::NodeError;
use crate::{Node, INTERNODE_PORT};
use query_coordinator::clauses::keyspace::alter_keyspace_cql::AlterKeyspace;
use query_coordinator::clauses::keyspace::create_keyspace_cql::CreateKeyspace;
use query_coordinator::clauses::keyspace::drop_keyspace_cql::DropKeyspace;
use query_coordinator::clauses::set_sql::Set;
use query_coordinator::clauses::table::alter_table_cql::AlterTable;
use query_coordinator::clauses::table::create_table_cql::CreateTable;
use query_coordinator::clauses::table::drop_table_cql::DropTable;
use query_coordinator::clauses::types::alter_table_op::AlterTableOperation;
use query_coordinator::clauses::types::column::Column;
use query_coordinator::clauses::{
    delete_sql::Delete, insert_sql::Insert, select_sql::Select, update_sql::Update,
};
use query_coordinator::errors::CQLError;
use query_coordinator::Query;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::io::{BufRead, BufReader};
use std::net::{Ipv4Addr, TcpStream};
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{SystemTime, UNIX_EPOCH};

pub struct QueryExecution {
    node_that_execute: Arc<Mutex<Node>>,
    connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
}

enum ExecutionResult {}

impl QueryExecution {
    // Constructor de QueryExecution
    pub fn new(
        node_that_execute: Arc<Mutex<Node>>,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
    ) -> QueryExecution {
        QueryExecution {
            node_that_execute,
            connections,
        }
    }

    // Método para ejecutar la query según su tipo
    pub fn execute(
        &self,
        query: Query,
        internode: bool,
        open_query_id: i32,
    ) -> Result<Option<String>, NodeError> {
        let mut response: Result<Option<String>, NodeError> = Ok(None);

        match query {
            Query::Select(select_query) => {
                if let Ok(select_querys) =
                    self.execute_select(select_query, internode, open_query_id)
                {
                    response = Ok(Some(select_querys.join("\n")));
                } else {
                    return Err(NodeError::OtherError);
                }
            }
            Query::Insert(insert_query) => {
                let table_name = insert_query.into_clause.table_name.clone();
                let table = self.node_that_execute.lock()?.get_table(table_name)?;
                self.execute_insert(insert_query, table, internode, open_query_id)?;
            }
            Query::Update(update_query) => {
                self.execute_update(update_query, internode, open_query_id)?;
            }
            Query::Delete(delete_query) => {
                self.execute_delete(delete_query, internode, open_query_id)?;
            }
            Query::CreateTable(create_table) => {
                if self
                    .node_that_execute
                    .lock()?
                    .table_already_exist(create_table.get_name())?
                {
                    return Err(NodeError::CQLError(CQLError::InvalidTable));
                }
                self.execute_create_table(create_table, internode, open_query_id)?;
            }
            Query::DropTable(drop_table) => {
                self.execute_drop_table(drop_table, internode, open_query_id)?;
            }
            Query::AlterTable(alter_table) => {
                self.execute_alter_table(alter_table, internode, open_query_id)?;
            }
            Query::CreateKeyspace(create_keyspace) => {
                self.execute_create_keyspace(create_keyspace, internode, open_query_id)?;
            }
            Query::DropKeyspace(drop_keyspace) => {
                self.execute_drop_keyspace(drop_keyspace, internode, open_query_id)?;
            }
            Query::AlterKeyspace(alter_keyspace) => {
                self.execute_alter_keyspace(alter_keyspace, internode, open_query_id)?;
            }
        }

        if internode {
            let protocol_response = InternodeProtocolHandler::create_protocol_response(
                "OK",
                &response?.unwrap_or("_".to_string()),
                open_query_id,
            );
            dbg!(&protocol_response);
            Ok(Some(protocol_response))
        } else {
            Ok(None)
        }
    }

    pub fn execute_create_keyspace(
        &self,
        create_keyspace: CreateKeyspace,
        internode: bool,
        open_query_id: i32,
    ) -> Result<(), NodeError> {
        let mut node = self
            .node_that_execute
            .lock()
            .map_err(|_| NodeError::LockError)?;
        node.add_keyspace(create_keyspace.clone())?;

        // Obtiene el nombre del keyspace
        let keyspace_name = create_keyspace.get_name().clone();

        // Genera el nombre de la carpeta donde se almacenará el keyspace
        let ip_str = node.get_ip_string().to_string().replace(".", "_");
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
            self.send_to_other_nodes(
                node,
                "CREATE_KEYSPACE",
                &serialized_create_keyspace,
                true,
                open_query_id,
            )?;
        }

        Ok(())
    }

    pub fn execute_drop_keyspace(
        &self,
        drop_keyspace: DropKeyspace,
        internode: bool,
        open_query_id: i32,
    ) -> Result<(), NodeError> {
        // Obtiene el nombre del keyspace a eliminar
        let keyspace_name = drop_keyspace.get_name().clone();

        // Bloquea el nodo y remueve el keyspace de la estructura interna
        let mut node = self
            .node_that_execute
            .lock()
            .map_err(|_| NodeError::LockError)?;
        node.remove_keyspace(keyspace_name.clone())?;

        // Genera el nombre de la carpeta donde se almacenará el keyspace
        let ip_str = node.get_ip_string().to_string().replace(".", "_");
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
            self.send_to_other_nodes(
                node,
                "DROP_KEYSPACE",
                &serialized_drop_keyspace,
                true,
                open_query_id,
            )?;
        }

        Ok(())
    }

    pub fn execute_alter_keyspace(
        &self,
        alter_keyspace: AlterKeyspace,
        internode: bool,
        open_query_id: i32,
    ) -> Result<(), NodeError> {
        // Buscar el keyspace en la lista de keyspaces
        let mut node = self.node_that_execute.lock()?;
        let mut keyspace = node
            .actual_keyspace
            .clone()
            .ok_or(NodeError::OtherError)?
            .clone();

        // Validar si la clase de replicación y el factor son los mismos para evitar operaciones innecesarias
        if keyspace.get_replication_class() == alter_keyspace.get_replication_class()
            && keyspace.get_replication_factor() == alter_keyspace.get_replication_factor()
        {
            return Ok(()); // No hay cambios, nada que ejecutar
        }

        keyspace.update_replication_class(alter_keyspace.get_replication_class());
        keyspace.update_replication_factor(alter_keyspace.get_replication_factor());
        node.update_keyspace(keyspace)?;
        // Si no es internode, comunicar a otros nodos
        if !internode {
            let serialized_alter_keyspace = alter_keyspace.serialize();
            self.send_to_other_nodes(
                node,
                "ALTER_KEYSPACE",
                &serialized_alter_keyspace,
                true,
                open_query_id,
            )?;
        }

        Ok(())
    }

    pub fn execute_create_table(
        &self,
        create_table: CreateTable,
        internode: bool,
        open_query_id: i32,
    ) -> Result<(), NodeError> {
        // Agrega la tabla al nodo

        let mut node = self
            .node_that_execute
            .lock()
            .map_err(|_| NodeError::LockError)?;

        if node.has_no_actual_keyspace() {
            return Err(NodeError::CQLError(CQLError::Error));
        }
        node.add_table(create_table.clone())?;

        // Obtiene el nombre de la tabla y la estructura de columnas
        let table_name = create_table.get_name().clone();
        let columns = create_table.get_columns().clone();

        // Genera el nombre de archivo y la carpeta en la cual se almacenará la tabla
        let ip_str = node.get_ip_string().replace(".", "_");
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
            self.send_to_other_nodes(
                node,
                "CREATE_TABLE",
                &serialized_create_table,
                true,
                open_query_id,
            )?;
        }

        Ok(())
    }

    pub fn execute_drop_table(
        &self,
        drop_table: DropTable,
        internode: bool,
        open_query_id: i32,
    ) -> Result<(), NodeError> {
        let mut node = self
            .node_that_execute
            .lock()
            .map_err(|_| NodeError::LockError)?;

        if node.has_no_actual_keyspace() {
            return Err(NodeError::CQLError(CQLError::Error));
        }
        // Obtiene el nombre de la tabla a eliminar
        let table_name = drop_table.get_table_name();

        // Bloquea el nodo y elimina la tabla de la lista interna
        node.remove_table(table_name.clone())?;

        // Genera el nombre de archivo y la carpeta en la cual se almacenará la tabla
        let ip_str = node.get_ip_string().replace(".", "_");
        let folder_name = format!("keyspaces_{}/{}", ip_str, node.actual_keyspace_name()?);
        let file_path = format!("{}/{}.csv", folder_name, table_name);

        // Borra el archivo de la tabla si existe
        if std::fs::remove_file(&file_path).is_err() {
            eprintln!(
                "Warning: File {} does not exist or cannot be deleted",
                file_path
            );
        }

        // Si no es internode, comunicar a otros nodos
        if !internode {
            // Serializa el `DropTable` a un mensaje simple
            let serialized_drop_table = drop_table.serialize();
            self.send_to_other_nodes(
                node,
                "DROP_TABLE",
                &serialized_drop_table,
                true,
                open_query_id,
            )?;
        }

        Ok(())
    }

    pub fn execute_alter_table(
        &self,
        alter_table: AlterTable,
        internode: bool,
        open_query_id: i32,
    ) -> Result<(), NodeError> {
        let mut node = self
            .node_that_execute
            .lock()
            .map_err(|_| NodeError::LockError)?;

        if node.has_no_actual_keyspace() {
            return Err(NodeError::CQLError(CQLError::Error));
        }

        // Obtiene el nombre de la tabla y bloquea el acceso a la misma
        let table_name = alter_table.get_table_name();
        let mut table = node.get_table(table_name.clone())?.inner;

        // Ruta del archivo de la tabla
        // Genera el nombre de archivo y la carpeta en la cual se almacenará la tabla
        let ip_str = node.get_ip_string().replace(".", "_");
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
            self.send_to_other_nodes(
                node,
                "ALTER_TABLE",
                &serialized_alter_table,
                true,
                open_query_id,
            )?;
        }

        Ok(())
    }

    // Función auxiliar para agregar una columna al archivo CSV
    fn add_column_to_file(file_path: &str, column_name: &str) -> Result<(), NodeError> {
        let temp_path = format!("{}.temp", file_path);
        let mut temp_file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&temp_path)?;

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
        let mut temp_file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&temp_path)?;

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

            let filtered_line: Vec<&str> = cells
                .iter()
                .enumerate()
                .filter(|&(i, _)| Some(i) != col_index)
                .map(|(_, &cell)| cell)
                .collect();

            writeln!(temp_file, "{}", filtered_line.join(","))?;
        }

        fs::rename(temp_path, file_path).map_err(NodeError::IoError)
    }

    // Función auxiliar para renombrar una columna en el archivo CSV
    fn rename_column_in_file(
        file_path: &str,
        old_name: &str,
        new_name: &str,
    ) -> Result<(), NodeError> {
        let temp_path = format!("{}.temp", file_path);
        let mut temp_file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&temp_path)?;

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

    fn execute_insert(
        &self,
        insert_query: Insert,
        table_to_insert: Table,
        internode: bool,
        open_query_id: i32,
    ) -> Result<(), NodeError> {
        let node = self.node_that_execute.lock()?;
        // Agrega la tabla al nodo
        if node.has_no_actual_keyspace() {
            return Err(NodeError::CQLError(CQLError::Error));
        }

        if !node.table_already_exist(table_to_insert.get_name())? {
            return Err(NodeError::CQLError(CQLError::Error));
        }

        let columns = table_to_insert.get_columns();
        let primary_key = columns
            .iter()
            .find(|column| column.is_primary_key)
            .ok_or(NodeError::CQLError(CQLError::InvalidSyntax))?;
        // Encuentra la posición de la clave primaria
        let pos = columns
            .iter()
            .position(|x| x == primary_key)
            .ok_or(NodeError::CQLError(CQLError::InvalidColumn))?;

        let values = insert_query.values.clone();
        let value_to_hash = values[pos].clone();

        self.validate_values(columns, &values)?;
        let ip = node.get_partitioner().get_ip(value_to_hash)?;

        if !internode && ip != node.get_ip() {
            let serialized_insert = insert_query.serialize();
            self.send_to_single_node(
                node.get_ip(),
                ip,
                "INSERT",
                &serialized_insert,
                internode,
                open_query_id,
            )?;
            return Ok(());
        }
        QueryExecution::insert_in_this_node(
            values,
            node.get_ip(),
            insert_query.into_clause.table_name,
            pos,
            node.actual_keyspace_name()?,
        )
    }

    fn insert_in_this_node(
        values: Vec<String>,
        ip: Ipv4Addr,
        table_name: String,
        index_of_primary_key: usize,
        actual_keyspace_name: String,
    ) -> Result<(), NodeError> {
        // Convertimos la IP a string para usar en el nombre de la carpeta
        let add_str = ip.to_string().replace(".", "_");

        let folder_name = format!("keyspaces_{}/{}", add_str, actual_keyspace_name);
        let folder_path = Path::new(&folder_name);

        if !folder_path.exists() {
            fs::create_dir_all(&folder_path).map_err(NodeError::IoError)?;
        }

        // Nombre de la tabla para almacenar la data, agregando la extensión ".csv"
        let file_path = folder_path.join(format!("{}.csv", table_name));

        // Genera un nombre único para el archivo temporal
        let temp_file_path = folder_path.join(format!(
            "{}.tmp",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| NodeError::OtherError)?
                .as_nanos()
        ));

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
                if row_values.get(index_of_primary_key)
                    == Some(&values[index_of_primary_key].as_str())
                {
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
    fn send_to_other_nodes(
        &self,
        local_node: MutexGuard<'_, Node>,
        header: &str,
        serialized_message: &str,
        internode: bool,
        open_query_id: i32,
    ) -> Result<(), NodeError> {
        // Serializa el objeto que se quiere enviar
        let message = InternodeProtocolHandler::create_protocol_message(
            &&local_node.get_ip_string(),
            open_query_id,
            header,
            &serialized_message,
            internode,
        );

        // Bloquea el nodo para obtener el partitioner y la IP
        let current_ip = local_node.get_ip();

        // Recorre los nodos del partitioner y envía el mensaje a cada nodo excepto el actual
        for ip in local_node.get_partitioner().get_nodes() {
            if ip != current_ip {
                let stream = connect(ip, INTERNODE_PORT, self.connections.clone())?;
                send_message(&stream, &message)?;
            }
        }
        Ok(())
    }

    // Función auxiliar para enviar un mensaje a un nodo específico en el partitioner
    fn send_to_single_node(
        &self,
        self_ip: Ipv4Addr,
        target_ip: Ipv4Addr,
        header: &str,
        serialized_message: &str,
        internode: bool,
        open_query_id: i32,
    ) -> Result<(), NodeError> {
        // Serializa el objeto que se quiere enviar
        let message = InternodeProtocolHandler::create_protocol_message(
            &self_ip.to_string(),
            open_query_id,
            header,
            serialized_message,
            internode,
        );

        // Conecta y envía el mensaje al nodo específico
        let stream = connect(target_ip, INTERNODE_PORT, self.connections.clone())?;
        send_message(&stream, &message)?;

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

    pub fn validate_update_types(set_clause: Set, columns: Vec<Column>) -> Result<(), NodeError> {
        for (column_name, value) in set_clause.get_pairs() {
            for column in &columns {
                if *column_name == column.name {
                    if !column.data_type.is_valid_value(value) {
                        return Err(NodeError::CQLError(CQLError::InvalidSyntax));
                    }
                }
            }
        }
        Ok(())
    }

    pub fn execute_update(
        &self,
        update_query: Update,
        internode: bool,
        open_query_id: i32,
    ) -> Result<(), NodeError> {
        let table;
        {
            // Obtiene el nombre de la tabla y genera la ruta del archivo
            let table_name = update_query.table_name.clone();
            let node = self
                .node_that_execute
                .lock()
                .map_err(|_| NodeError::LockError)?;
            table = node.get_table(table_name.clone())?;
            let primary_key = table.get_primary_key()?;
            let where_clause = update_query
                .clone()
                .where_clause
                .ok_or(NodeError::OtherError)?;
            where_clause.validate_cql_conditions(&primary_key, "")?;

            let value_to_hash = where_clause
                .get_value_primary_condition(&primary_key)?
                .ok_or(NodeError::OtherError)?;
            let node_to_update = node.partitioner.get_ip(value_to_hash.clone())?;

            if !internode && node_to_update != node.get_ip() {
                let serialized_update = update_query.serialize();
                self.send_to_single_node(
                    node.get_ip(),
                    node_to_update,
                    "UPDATE",
                    &serialized_update,
                    internode,
                    open_query_id,
                )?;
                return Ok(());
            }
        }

        // Ejecuta el update en este nodo
        let (file_path, temp_file_path) = self.get_file_paths(&update_query.table_name)?;
        if self
            .update_in_this_node(update_query, table, &file_path, &temp_file_path)
            .is_err()
        {
            let _ = std::fs::remove_file(temp_file_path);
            return Err(NodeError::OtherError);
        }
        Ok(())
    }

    fn update_in_this_node(
        &self,
        update_query: Update,
        table: Table,
        file_path: &str,
        temp_file_path: &str,
    ) -> Result<(), NodeError> {
        // Obtiene la ruta del archivo y el archivo temporal

        // Abre los archivos
        let file = OpenOptions::new().read(true).open(&file_path)?;
        let mut reader = BufReader::new(file);
        let mut temp_file = self.create_temp_file(&temp_file_path)?;

        // Escribe el encabezado en el archivo temporal
        self.write_header(&mut reader, &mut temp_file)?;

        Self::validate_update_types(update_query.clone().set_clause, table.get_columns())?;

        let mut found_match = false;

        // Itera sobre cada línea del archivo original y realiza la actualización
        for line in reader.lines() {
            let line = line?;
            found_match |=
                self.update_or_write_line(&table, &update_query, &line, &mut temp_file)?;
        }

        // Agrega una nueva fila si no se encontró coincidencia
        if !found_match {
            self.add_new_row(&table, &update_query, &mut temp_file)?;
        }

        // Reemplaza el archivo original con el temporal
        self.replace_original_file(&temp_file_path, &file_path)?;

        Ok(())
    }

    /// Obtiene las rutas del archivo principal y del temporal.
    fn get_file_paths(&self, table_name: &str) -> Result<(String, String), NodeError> {
        let node = self
            .node_that_execute
            .lock()
            .map_err(|_| NodeError::LockError)?;
        let add_str = node.get_ip_string().replace(".", "_");
        let folder_name = format!("keyspaces_{}/{}", add_str, node.actual_keyspace_name()?);
        let file_path = format!("{}/{}.csv", folder_name, table_name);

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| NodeError::OtherError)?
            .as_nanos();
        let temp_file_path = format!("{}.{}.temp", file_path, timestamp);

        Ok((file_path, temp_file_path))
    }

    /// Crea un archivo temporal para escribir.
    fn create_temp_file(&self, temp_file_path: &str) -> Result<File, NodeError> {
        File::create(temp_file_path).map_err(NodeError::from)
    }

    /// Escribe el encabezado en el archivo temporal.
    fn write_header<R: BufRead>(
        &self,
        reader: &mut R,
        temp_file: &mut File,
    ) -> Result<(), NodeError> {
        if let Some(header_line) = reader.lines().next() {
            writeln!(temp_file, "{}", header_line?).map_err(|e| NodeError::from(e))?;
        }
        Ok(())
    }

    /// Actualiza o escribe una línea en el archivo temporal.
    fn update_or_write_line(
        &self,
        table: &Table,
        update_query: &Update,
        line: &str,
        temp_file: &mut File,
    ) -> Result<bool, NodeError> {
        let mut columns: Vec<String> = line.split(',').map(|s| s.trim().to_string()).collect();
        let column_value_map = self.create_column_value_map(table, &columns, true);

        let mut found_match = false;
        if let Some(where_clause) = &update_query.where_clause {
            if where_clause
                .condition
                .execute(&column_value_map)
                .unwrap_or(false)
            {
                found_match = true;
                for (column, new_value) in update_query.clone().set_clause.get_pairs() {
                    if table.is_primary_key(&column)? {
                        return Err(NodeError::OtherError);
                    }
                    let index = table
                        .get_column_index(column)
                        .ok_or(NodeError::CQLError(CQLError::InvalidColumn))?;
                    columns[index] = new_value.clone();
                }
            }
        } else {
            return Err(NodeError::OtherError);
        }

        writeln!(temp_file, "{}", columns.join(",")).map_err(|e| NodeError::from(e))?;
        Ok(found_match)
    }

    /// Crea un mapa de valores de columna para una fila dada.
    fn create_column_value_map(
        &self,
        table: &Table,
        columns: &[String],
        only_primary_key: bool,
    ) -> HashMap<String, String> {
        let mut column_value_map = HashMap::new();
        for (i, column) in table.get_columns().iter().enumerate() {
            if let Some(value) = columns.get(i) {
                if column.is_primary_key || !only_primary_key {
                    column_value_map.insert(column.name.clone(), value.clone());
                }
            }
        }
        column_value_map
    }

    /// Agrega una nueva fila si no se encontró coincidencia.
    fn add_new_row(
        &self,
        table: &Table,
        update_query: &Update,
        temp_file: &mut File,
    ) -> Result<(), NodeError> {
        let mut new_row: Vec<String> = vec!["".to_string(); table.get_columns().len()];
        let primary_key = table.get_primary_key()?;
        let primary_key_index = table
            .get_column_index(&primary_key)
            .ok_or(NodeError::OtherError)?;

        let primary_key_value = update_query
            .where_clause
            .as_ref()
            .and_then(|where_clause| where_clause.get_value_primary_condition(&primary_key).ok())
            .flatten()
            .ok_or(NodeError::OtherError)?;

        new_row[primary_key_index] = primary_key_value;

        for (column, new_value) in update_query.set_clause.get_pairs() {
            if table.is_primary_key(&column)? {
                return Err(NodeError::OtherError);
            }
            let index = table
                .get_column_index(column)
                .ok_or(NodeError::CQLError(CQLError::InvalidColumn))?;
            new_row[index] = new_value.clone();
        }

        writeln!(temp_file, "{}", new_row.join(",")).map_err(|e| NodeError::from(e))
    }

    /// Reemplaza el archivo original con el archivo temporal.
    fn replace_original_file(
        &self,
        temp_file_path: &str,
        file_path: &str,
    ) -> Result<(), NodeError> {
        std::fs::rename(temp_file_path, file_path).map_err(NodeError::from)
    }

    pub fn execute_delete(
        &self,
        delete_query: Delete,
        internode: bool,
        open_query_id: i32,
    ) -> Result<(), NodeError> {
        let table;
        {
            // Obtiene el nombre de la tabla y genera la ruta del archivo
            let table_name = delete_query.table_name.clone();
            let node = self
                .node_that_execute
                .lock()
                .map_err(|_| NodeError::LockError)?;
            table = node.get_table(table_name.clone())?;
            let primary_key = table.get_primary_key()?;
            let where_clause = delete_query
                .clone()
                .where_clause
                .ok_or(NodeError::OtherError)?;
            where_clause.validate_cql_conditions(&primary_key, "")?;

            let value_to_hash = where_clause
                .get_value_primary_condition(&primary_key)?
                .ok_or(NodeError::OtherError)?;
            let node_to_delete = node.partitioner.get_ip(value_to_hash.clone())?;

            if !internode && node_to_delete != node.get_ip() {
                let serialized_delete = delete_query.serialize();
                self.send_to_single_node(
                    node.get_ip(),
                    node_to_delete,
                    "DELETE",
                    &serialized_delete,
                    internode,
                    open_query_id,
                )?;
                return Ok(());
            }
        }

        // Ejecuta el delete en este nodo
        let (file_path, temp_file_path) = self.get_file_paths(&delete_query.table_name)?;
        if self
            .delete_in_this_node(delete_query, table, &file_path, &temp_file_path)
            .is_err()
        {
            let _ = std::fs::remove_file(temp_file_path);
            return Err(NodeError::OtherError);
        }
        Ok(())
    }

    // Función para ejecutar el delete en este nodo
    fn delete_in_this_node(
        &self,
        delete_query: Delete,
        table: Table,
        file_path: &str,
        temp_file_path: &str,
    ) -> Result<(), NodeError> {
        // Abre los archivos
        let file = OpenOptions::new().read(true).open(&file_path)?;
        let mut reader = BufReader::new(file);
        let mut temp_file = self.create_temp_file(&temp_file_path)?;

        // Escribe el encabezado en el archivo temporal
        self.write_header(&mut reader, &mut temp_file)?;

        // Itera sobre cada línea del archivo original y realiza la eliminación
        for line in reader.lines() {
            let line = line?;
            if !self.should_delete_line(&table, &delete_query, &line)? {
                writeln!(temp_file, "{}", line)?;
            }
        }

        // Reemplaza el archivo original con el temporal
        self.replace_original_file(&temp_file_path, &file_path)?;
        Ok(())
    }

    // Verifica si la línea debe ser eliminada según la condición del where_clause
    fn should_delete_line(
        &self,
        table: &Table,
        delete_query: &Delete,
        line: &str,
    ) -> Result<bool, NodeError> {
        let columns: Vec<String> = line.split(',').map(|s| s.trim().to_string()).collect();
        let column_value_map = self.create_column_value_map(table, &columns, true);

        if let Some(where_clause) = &delete_query.where_clause {
            return Ok(where_clause
                .condition
                .execute(&column_value_map)
                .unwrap_or(false));
        }
        Err(NodeError::OtherError)
    }

    pub fn execute_select(
        &self,
        select_query: Select,
        internode: bool,
        open_query_id: i32,
    ) -> Result<Vec<String>, NodeError> {
        let table;
        {
            // Obtenemos el nombre de la tabla y creamos una referencia al nodo
            let table_name = select_query.table_name.clone();
            let node = self
                .node_that_execute
                .lock()
                .map_err(|_| NodeError::LockError)?;

            // Obtenemos la tabla y la clave primaria
            table = node.get_table(table_name.clone())?;
            let primary_key = table.get_primary_key()?;

            // Validamos la cláusula WHERE para asegurar que contiene la clave primaria
            let where_clause = select_query
                .where_clause
                .clone()
                .ok_or(NodeError::OtherError)?;
            where_clause.validate_cql_conditions(&primary_key, "")?;

            // Obtenemos el valor de la condición de la clave primaria para calcular la ubicación del nodo
            let value_to_hash = where_clause
                .get_value_primary_condition(&primary_key)?
                .ok_or(NodeError::OtherError)?;
            let node_to_query = node.partitioner.get_ip(value_to_hash.clone())?;

            // Si es `internode`, enviamos la consulta al nodo correspondiente y esperamos la respuesta
            if !internode && node_to_query != node.get_ip() {
                let serialized_query = select_query.serialize();
                self.send_to_single_node(
                    node.get_ip(),
                    node_to_query,
                    "SELECT",
                    &serialized_query,
                    true,
                    open_query_id,
                )?;
            }
        }
        // Ejecutamos el `SELECT` localmente si no es `internode`
        let result = self.execute_select_in_this_node(select_query, table)?;
        Ok(result)
    }

    fn execute_select_in_this_node(
        &self,
        select_query: Select,
        table: Table,
    ) -> Result<Vec<String>, NodeError> {
        let (file_path, _) = self.get_file_paths(&select_query.table_name)?;
        let file = OpenOptions::new().read(true).open(&file_path)?;
        let reader = BufReader::new(file);
        let mut results = Vec::new();
        results.push(select_query.columns.join(","));
        // Itera sobre cada línea del archivo y aplica la condición de la cláusula WHERE
        for line in reader.lines() {
            let line = line?;
            if self.line_matches_where_clause(&line, &table, &select_query)? {
                let selected_columns = self.extract_selected_columns(&line, &table, &select_query);
                results.push(selected_columns);
            }
        }
        //println!("{:?}", results);
        Ok(results)
    }

    // acá habría que devolver un array de hash maps con los key -> column
    fn line_matches_where_clause(
        &self,
        line: &str,
        table: &Table,
        select_query: &Select,
    ) -> Result<bool, NodeError> {
        // Convierte la línea en un mapa de columna a valor
        let columns: Vec<String> = line.split(',').map(|s| s.trim().to_string()).collect();
        let column_value_map = self.create_column_value_map(table, &columns, true);
        // Verifica la condición WHERE de la consulta `SELECT`
        if let Some(where_clause) = &select_query.where_clause {
            Ok(where_clause
                .condition
                .execute(&column_value_map)
                .unwrap_or(false))
        } else {
            Ok(true) // Si no hay cláusula WHERE, se considera que la línea coincide
        }
    }

    fn extract_selected_columns(&self, line: &str, table: &Table, select_query: &Select) -> String {
        let columns: Vec<String> = line.split(',').map(|s| s.trim().to_string()).collect();
        let column_value_map = self.create_column_value_map(table, &columns, false);

        // Filtra solo las columnas especificadas en `SELECT`
        let selected_columns: Vec<String> = select_query
            .columns
            .iter()
            .filter_map(|col| column_value_map.get(col).cloned())
            .collect();

        // Une las columnas seleccionadas en una sola cadena separada por comas
        selected_columns.join(",")
    }
}
