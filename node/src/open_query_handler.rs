use crate::errors::NodeError;
use crate::internode_protocol::response::InternodeResponse;
use crate::keyspace::Keyspace;
use crate::table::Table;
use query_creator::Query;
use std::collections::HashMap;
use std::fmt;
use std::net::{Ipv4Addr, TcpStream};

#[derive(Debug, PartialEq)]
pub enum ConsistencyLevel {
    Any,
    One,
    Two,
    Three,
    Quorum,
    All,
}

impl ConsistencyLevel {
    // Crea un ConsistencyLevel a partir de un string
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "any" => ConsistencyLevel::Any,
            "one" => ConsistencyLevel::One,
            "two" => ConsistencyLevel::Two,
            "three" => ConsistencyLevel::Three,
            "quorum" => ConsistencyLevel::Quorum,
            "all" => ConsistencyLevel::All,
            _ => ConsistencyLevel::All,
        }
    }

    // Verifica si el nivel de consistencia está listo basado en respuestas recibidas y necesarias
    pub fn is_query_ready(&self, responses_received: usize, responses_needed: usize) -> bool {
        match self {
            ConsistencyLevel::Any => responses_received >= 1,
            ConsistencyLevel::One => responses_received >= 1,
            ConsistencyLevel::Two => responses_received >= 2,
            ConsistencyLevel::Three => responses_received >= 3,
            ConsistencyLevel::Quorum => responses_received >= (responses_needed / 2 + 1),
            ConsistencyLevel::All => responses_received >= responses_needed,
        }
    }

    // Calcula cuántos OKs se necesitan para cumplir con el nivel de consistencia
    pub fn required_oks(&self, responses_needed: usize) -> usize {
        match self {
            ConsistencyLevel::Any => 1,
            ConsistencyLevel::One => 1,
            ConsistencyLevel::Two => 2,
            ConsistencyLevel::Three => 3,
            ConsistencyLevel::Quorum => responses_needed / 2 + 1,
            ConsistencyLevel::All => responses_needed,
        }
    }
}

/// Represents an open query, tracking the number of responses needed and received.
#[derive(Debug)]
pub struct OpenQuery {
    needed_responses: i32,
    ok_responses: i32,
    error_responses: i32,
    acumulated_ok_responses: Vec<(Ipv4Addr, InternodeResponse)>,
    connection: TcpStream,
    query: Query,
    consistency_level: ConsistencyLevel,
    table: Option<Table>,
}

impl OpenQuery {
    /// Creates a new `OpenQuery` with the required number of responses and an associated TCP connection.
    ///
    /// # Parameters
    /// - `needed_responses`: The number of responses needed to close the query.
    /// - `connection`: The TCP connection associated with this query.
    ///
    /// # Returns
    /// A new instance of `OpenQuery`.
    fn new(
        needed_responses: i32,
        connection: TcpStream,
        query: Query,
        consistencty: &str,
        table: Option<Table>,
    ) -> Self {
        Self {
            needed_responses,
            ok_responses: 0,
            error_responses: 0,
            acumulated_ok_responses: vec![],
            connection,
            query,
            consistency_level: ConsistencyLevel::from_str(consistencty),
            table,
        }
    }

    /// Adds a response to the query and increments the count of actual responses.
    ///
    /// # Parameters
    /// - `response`: The response to be added.
    fn add_ok_response(&mut self, response: InternodeResponse, from: Ipv4Addr) {
        self.acumulated_ok_responses.push((from, response));
        self.ok_responses += 1;
    }

    /// Adds a response to the query and increments the count of actual responses.
    ///
    /// # Parameters
    /// - `response`: The response to be added.
    fn add_error_response(&mut self) {
        self.error_responses += 1;
    }

    /// Checks if the query has received all needed responses.
    ///
    /// # Returns
    /// `true` if the query is closed (i.e., all responses have been received), `false` otherwise.
    fn is_close(&self) -> bool {
        self.consistency_level
            .is_query_ready(self.ok_responses as usize, self.needed_responses as usize)
            || !self.can_still_achieve_required_ok(
                self.needed_responses,
                self.error_responses,
                self.consistency_level
                    .required_oks(self.needed_responses as usize) as i32,
            )
    }

    fn can_still_achieve_required_ok(
        &self,
        total_responses: i32,
        error_responses: i32,
        required_ok: i32,
    ) -> bool {
        total_responses - error_responses >= required_ok
        //total rta - errores - ok >= oks necesarios - oks
    }

    /// Gets the TCP connection associated with this query.
    ///
    /// # Returns
    /// A reference to the `TcpStream` used by this query.
    pub fn get_connection(&self) -> &TcpStream {
        &self.connection
    }

    pub fn get_query(&self) -> Query {
        self.query.clone()
    }

    pub fn get_table(&self) -> Option<Table> {
        self.table.clone()
    }

    pub fn get_acumulated_responses(&self) -> Vec<(Ipv4Addr, InternodeResponse)> {
        self.acumulated_ok_responses.clone()
    }
}

/// Implements `fmt::Display` for `OpenQuery` to provide human-readable formatting for query status.
impl fmt::Display for OpenQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ID: con {:?} (Ok responses) y {:?} (Error responses) of {:?} needed responses",
            self.ok_responses, self.error_responses, self.needed_responses
        )
    }
}

/// Manages multiple `OpenQuery` instances, each identified by an ID.
pub struct OpenQueryHandler {
    queries: HashMap<i32, OpenQuery>,
    keyspaces_queries: HashMap<i32, Option<Keyspace>>,
    next_id: i32,
}

impl OpenQueryHandler {
    /// Creates a new `OpenQueryHandler`.
    ///
    /// # Returns
    /// A new instance of `OpenQueryHandler`.
    pub fn new() -> Self {
        Self {
            queries: HashMap::new(),
            keyspaces_queries: HashMap::new(),
            next_id: 1,
        }
    }

    /// Creates a new `OpenQuery`, assigning it a unique ID.
    ///
    /// # Parameters
    /// - `needed_responses`: The number of responses needed to close the query.
    /// - `connection`: The TCP connection associated with this query.
    ///
    /// # Returns
    /// The ID of the newly created query.
    pub fn new_open_query(
        &mut self,
        needed_responses: i32,
        connection: TcpStream,
        query: Query,
        consistency_level: &str,
        table: Option<Table>,
        keyspace: Option<Keyspace>,
    ) -> i32 {
        let new_id = self.next_id;
        self.next_id += 1;
        let query = OpenQuery::new(
            needed_responses,
            connection,
            query,
            consistency_level,
            table,
        );
        self.queries.insert(new_id, query);
        self.keyspaces_queries.insert(new_id, keyspace);
        new_id
    }

    /// Gets a mutable reference to the `OpenQuery` with the specified ID.
    ///
    /// # Parameters
    /// - `id`: The ID of the query.
    ///
    /// # Returns
    /// A mutable reference to the query, or `None` if it does not exist.
    pub fn get_query_mut(&mut self, id: &i32) -> Option<&mut OpenQuery> {
        self.queries.get_mut(id)
    }

    /// Removes and returns the `OpenQuery` with the specified ID.
    ///
    /// # Parameters
    /// - `id`: The ID of the query.
    ///
    /// # Returns
    /// The removed `OpenQuery`, or `None` if it does not exist.
    fn _get_query_and_delete(&mut self, id: i32) -> Option<OpenQuery> {
        self.queries.remove(&id)
    }

    /// Gets a cloned TCP connection for the query with the specified ID.
    ///
    /// # Parameters
    /// - `id`: The ID of the query.
    ///
    /// # Returns
    /// A cloned `TcpStream`, or an error if the query or connection is not available.
    fn _get_connection_mut(&mut self, id: i32) -> Result<TcpStream, NodeError> {
        let connection = self
            .get_query_mut(&id)
            .ok_or(NodeError::OpenQueryError)?
            .get_connection();

        connection.try_clone().map_err(|e| NodeError::IoError(e))
    }

    /// Removes the `OpenQuery` with the specified ID.
    ///
    /// # Parameters
    /// - `id`: The ID of the query.
    pub fn _remove_query(&mut self, id: &i32) {
        self.keyspaces_queries.remove(id);
        self.queries.remove(id);
    }

    pub fn get_keyspace_of_query(&self, open_query_id: i32) -> Result<Option<Keyspace>, NodeError> {
        self.keyspaces_queries
            .get(&open_query_id)
            .ok_or(NodeError::InternodeProtocolError)
            .cloned()
    }

    pub fn update_table_in_keyspace(
        &mut self,
        keyspace_name: &str,
        new_table: Table,
    ) -> Result<(), NodeError> {
        for (_, keyspace) in &mut self.keyspaces_queries {
            if let Some(key) = keyspace {
                if key.get_name() == keyspace_name {
                    let mut find = false;
                    for (i, table) in key.get_tables().iter_mut().enumerate() {
                        if table.get_name() == new_table.clone().get_name() {
                            key.tables[i] = new_table.clone();
                            find = true;
                        }
                    }
                    if !find {
                        key.add_table(new_table.clone())?;
                    }
                }
            }
        }
        Ok(())
    }

    pub fn set_keyspace_of_query(&mut self, open_query_id: i32, keyspace: Keyspace) {
        self.keyspaces_queries.insert(open_query_id, Some(keyspace));
    }

    /// Adds a response to the `OpenQuery` with the specified ID and returns it if the query is closed.
    ///
    /// # Parameters
    /// - `open_query_id`: The ID of the query.
    /// - `response`: The response to be added.
    ///
    /// # Returns
    /// The `OpenQuery` if it has been closed, or `None` if it is still open.
    ///
    pub fn add_ok_response_and_get_if_closed(
        &mut self,
        open_query_id: i32,
        response: InternodeResponse,
        from: Ipv4Addr,
    ) -> Option<OpenQuery> {
        match self.get_query_mut(&open_query_id) {
            Some(query) => {
                query.add_ok_response(response, from);
                if query.is_close() {
                    println!(
                        "con {:?} / {:?} OKS la query se cerro",
                        query.ok_responses, query.needed_responses
                    );

                    self.queries.remove(&open_query_id)
                } else {
                    None
                }
            }
            None => None,
        }
    }

    pub fn add_error_response_and_get_if_closed(
        &mut self,
        open_query_id: i32,
    ) -> Option<OpenQuery> {
        match self.get_query_mut(&open_query_id) {
            Some(query) => {
                query.add_error_response();

                if query.is_close() {
                    println!(
                        "con {:?} / {:?} ERRORES la query se cerro",
                        query.ok_responses, query.needed_responses
                    );
                    self.queries.remove(&open_query_id)
                } else {
                    None
                }
            }
            None => None,
        }
    }
}

impl fmt::Display for OpenQueryHandler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Open Queries:\n")?;
        for (id, query) in &self.queries {
            writeln!(
                f,
                "Query ID {}: {} OKs, {} Errors, {} Needed",
                id, query.ok_responses, query.error_responses, query.needed_responses
            )?;
        }
        Ok(())
    }
}
