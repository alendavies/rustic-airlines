use std::{
    collections::HashMap,
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Write},
    net::{Ipv4Addr, TcpStream},
    sync::{Arc, Mutex},
};

use partitioner::Partitioner;

use crate::{
    internode_protocol::{
        message::{InternodeMessage, InternodeMessageContent},
        query::InternodeQuery,
    },
    keyspace::Keyspace,
    table::Table,
    utils::connect_and_send_message,
    INTERNODE_PORT,
};

use super::{errors::StorageEngineError, StorageEngine};

impl StorageEngine {
    pub fn redistribute_data(
        &self,
        keyspaces: Vec<Keyspace>,
        partitioner: &Partitioner,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
    ) -> Result<(), StorageEngineError> {
        println!("entro a redistribucion");
        for keyspace in keyspaces {
            let tables = keyspace.clone().get_tables();

            for table in tables {
                // Rutas de archivos
                let base_folder_path = self.get_keyspace_path(&keyspace.clone().get_name());
                let normal_file_path = base_folder_path.join(format!("{}.csv", table.get_name()));
                let replication_file_path = base_folder_path
                    .join("replication")
                    .join(format!("{}.csv", table.get_name()));

                // Procesar archivo normal
                if normal_file_path.exists() {
                    self.process_file(
                        &normal_file_path,
                        &partitioner,
                        false,
                        keyspace.clone(),
                        table.clone(),
                        self.ip.clone(),
                        connections.clone(),
                    )?;
                }

                // Procesar archivo de replicación
                if replication_file_path.exists() {
                    self.process_file(
                        &replication_file_path,
                        &partitioner,
                        true,
                        keyspace.clone(),
                        table.clone(),
                        self.ip.clone(),
                        connections.clone(),
                    )?;
                }
            }
        }
        Ok(())
    }

    fn process_file(
        &self,
        file_path: &std::path::Path,
        partitioner: &Partitioner,
        is_replication: bool,
        keyspace: Keyspace,
        table: Table,
        self_ip: String,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
    ) -> Result<(), StorageEngineError> {
        let self_ip: Ipv4Addr = self_ip
            .parse()
            .map_err(|_| StorageEngineError::UnsupportedOperation)?;

        let columns: Vec<String> = table
            .get_columns() // Obtiene las columnas (puede ser temporal)
            .iter()
            .map(|c| c.name.clone()) // Clona el nombre de cada columna
            .collect();

        let temp_file_path = file_path.with_extension("tmp");

        // Crear el archivo de índice con el formato `{nombre_archivo}_index.csv`
        let file_name = file_path.file_stem().ok_or(StorageEngineError::IoError)?;
        let index_file_path =
            file_path.with_file_name(format!("{}_index.csv", file_name.to_string_lossy()));

        let mut temp_file =
            BufWriter::new(File::create(&temp_file_path).map_err(|_| StorageEngineError::IoError)?);
        let mut index_file = BufWriter::new(
            File::create(&index_file_path).map_err(|_| StorageEngineError::IoError)?,
        );

        // Escribir el encabezado en el archivo de índice
        writeln!(index_file, "clustering_column,start_byte,end_byte")
            .map_err(|_| StorageEngineError::IoError)?;

        let file = File::open(file_path).map_err(|_| StorageEngineError::IoError)?;
        let reader = BufReader::new(file);

        let mut current_byte_offset: u64 = 0;
        let mut index_map: std::collections::BTreeMap<String, (u64, u64)> =
            std::collections::BTreeMap::new();

        // Obtén los índices de las columnas de clave de partición
        let partition_key_indices: Vec<usize> = table
            .get_columns()
            .iter()
            .enumerate()
            .filter(|(_, col)| col.is_partition_key)
            .map(|(idx, _)| idx)
            .collect();

        let clustering_key_indices: Vec<(usize, String)> = table
            .get_clustering_column_in_order()
            .iter()
            .filter_map(|col_name| {
                table
                    .get_columns()
                    .iter()
                    .position(|col| col.name == *col_name && col.is_clustering_column)
                    .map(|idx| {
                        let inverted_order =
                            if table.get_columns()[idx].get_clustering_order() == "ASC" {
                                "DESC".to_string()
                            } else {
                                "ASC".to_string()
                            };
                        (idx, inverted_order)
                    })
            })
            .collect();

        for (i, line) in reader.lines().enumerate() {
            let line = line.map_err(|_| StorageEngineError::IoError)?;
            let line_length = line.len() as u64;

            if i == 0 {
                println!("salto header: {:?}", line);
                writeln!(temp_file, "{}", line).map_err(|_| StorageEngineError::IoError)?;
                current_byte_offset += line_length + 1;
                continue;
            }

            if let Some((data, timestamp)) = line.split_once(";") {
                let row: Vec<&str> = data.split(',').collect();

                let mut partition_key = String::new();
                for partition_key_index in &partition_key_indices {
                    partition_key.push_str(row[partition_key_index.clone()]);
                }
                println!("estoy por hashear la partition {:?}", partition_key);
                // Calcula el nodo del hash de la clave de partición
                let current_node = partitioner
                    .get_ip(partition_key)
                    .map_err(|_| StorageEngineError::UnsupportedOperation)?;

                if !is_replication {
                    // Verificar si el hash coincide con el nodo actual
                    if current_node == self_ip {
                        println!("no hay que reubicar: {:?}", line);
                        writeln!(temp_file, "{};{}", data, timestamp)
                            .map_err(|_| StorageEngineError::IoError)?;

                        // Actualizar el índice para la primera columna de clustering
                        if let Some(&(idx, _)) = clustering_key_indices.first() {
                            let key = row[idx].to_string();
                            index_map.insert(
                                key,
                                (current_byte_offset, current_byte_offset + line_length),
                            );
                        }

                        current_byte_offset += line_length + 1;
                    } else {
                        let insert_string = Self::create_cql_insert(
                            &keyspace.get_name(),
                            &table.get_name(),
                            columns.clone(),
                            row,
                        )?;

                        println!(
                            "no es replicacion, reubicamos. Mandamos al nodo {:?} el insert: {:?}",
                            current_node, insert_string
                        );
                        let timestap_n: i64 = timestamp
                            .parse()
                            .map_err(|_| StorageEngineError::UnsupportedOperation)?;

                        Self::create_and_send_internode_message(
                            self_ip,
                            current_node,
                            &keyspace.get_name(),
                            &insert_string,
                            timestap_n,
                            false,
                            connections.clone(),
                        );
                    }
                } else {
                    //Verificar replicación

                    let successors = partitioner
                        .get_n_successors(current_node, keyspace.get_replication_factor() as usize)
                        .map_err(|_| StorageEngineError::UnsupportedOperation)?;

                    if successors.contains(&self_ip) {
                        writeln!(temp_file, "{};{}", data, timestamp)
                            .map_err(|_| StorageEngineError::IoError)?;

                        // Actualizar el índice para la primera columna de clustering
                        if let Some(&(idx, _)) = clustering_key_indices.first() {
                            let key = row[idx].to_string();
                            index_map.insert(
                                key,
                                (current_byte_offset, current_byte_offset + line_length),
                            );
                        }

                        current_byte_offset += line_length + 1;
                    } else {
                        // Aquí escribe tu lógica para enviar a otro nodo
                        let insert_string = Self::create_cql_insert(
                            &keyspace.get_name(),
                            &table.get_name(),
                            columns.clone(),
                            row,
                        )?;
                        let timestap_n: i64 = timestamp
                            .parse()
                            .map_err(|_| StorageEngineError::UnsupportedOperation)?;

                        Self::create_and_send_internode_message(
                            self_ip,
                            current_node,
                            &keyspace.get_name(),
                            &insert_string,
                            timestap_n,
                            true,
                            connections.clone(),
                        );
                    }
                }
            }
        }

        // Escribir el archivo de índice
        println!("escribo el archivo de indices");
        let mut sorted_indices: Vec<_> = index_map.into_iter().collect();
        for &(_, ref order) in &clustering_key_indices {
            if order == "ASC" {
                sorted_indices.sort_by(|a, b| a.0.cmp(&b.0));
            } else {
                sorted_indices.sort_by(|a, b| b.0.cmp(&a.0));
            }
        }

        for (key, (start_byte, end_byte)) in sorted_indices {
            writeln!(index_file, "{},{},{}", key, start_byte, end_byte)
                .map_err(|_| StorageEngineError::IoError)?;
        }

        fs::rename(&temp_file_path, file_path).map_err(|_| StorageEngineError::IoError)?;

        Ok(())
    }

    fn create_and_send_internode_message(
        self_ip: Ipv4Addr,
        target_ip: Ipv4Addr,
        keyspace_name: &str,
        serialized_message: &str,
        timestamp: i64,
        is_replication: bool,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>, // Ajusta el tipo si es necesario
    ) {
        // Crear el mensaje de internodo
        let message = InternodeMessage::new(
            self_ip,
            InternodeMessageContent::Query(InternodeQuery {
                query_string: serialized_message.to_string(),
                open_query_id: 0,
                client_id: 0,
                replication: is_replication,
                keyspace_name: keyspace_name.to_string(),
                timestamp,
            }),
        );

        // Enviar el mensaje al nodo objetivo
        let result = connect_and_send_message(target_ip, INTERNODE_PORT, connections, message);

        // Manejar errores o resultados
        _ = result;
    }

    fn create_cql_insert(
        keyspace: &str,
        table: &str,
        columns: Vec<String>,
        values: Vec<&str>,
    ) -> Result<String, StorageEngineError> {
        if columns.len() != values.len() {
            return Err(StorageEngineError::UnsupportedOperation);
        }

        // Generar la lista de columnas separadas por comas
        let columns_string = columns.join(",");

        // Escapar valores si es necesario, rodeándolos con comillas simples
        let values_string = values
            .iter()
            .map(|value| format!("'{}'", value.replace("'", "''"))) // Escapar comillas simples en los valores
            .collect::<Vec<String>>()
            .join(",");

        // Construir la sentencia CQL
        let cql = format!(
            "INSERT INTO {}.{} ({}) VALUES ({});",
            keyspace, table, columns_string, values_string
        );

        Ok(cql)
    }
}
