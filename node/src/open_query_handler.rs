use std::collections::HashMap;
use std::fmt;
use std::net::TcpStream;

use query_coordinator::Query;

use crate::errors::NodeError;
use crate::table::Table;

/// Represents an open query, tracking the number of responses needed and received.
#[derive(Debug)]
pub struct OpenQuery {
    needed_responses: i32,
    actual_responses: i32,
    responses: Vec<String>,
    connection: TcpStream,
    query: Query,
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
        table: Option<Table>,
    ) -> Self {
        Self {
            needed_responses,
            actual_responses: 0,
            responses: vec![],
            connection,
            query,
            table,
        }
    }

    /// Adds a response to the query and increments the count of actual responses.
    ///
    /// # Parameters
    /// - `response`: The response to be added.
    fn add_response(&mut self, response: String) {
        self.responses.push(response);
        self.actual_responses += 1;
    }

    /// Checks if the query has received all needed responses.
    ///
    /// # Returns
    /// `true` if the query is closed (i.e., all responses have been received), `false` otherwise.
    fn is_close(&self) -> bool {
        self.actual_responses == self.needed_responses
    }

    /// Gets a clone of all the responses received for this query.
    ///
    /// # Returns
    /// A `Vec<String>` containing all responses.
    fn _get_responses(&self) -> Vec<String> {
        self.responses.clone()
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
}

/// Implements `fmt::Display` for `OpenQuery` to provide human-readable formatting for query status.
impl fmt::Display for OpenQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ID: responses {}/{} with responses: {:?}",
            self.actual_responses, self.needed_responses, self.responses
        )
    }
}

/// Manages multiple `OpenQuery` instances, each identified by an ID.
pub struct OpenQueryHandler {
    queries: HashMap<i32, OpenQuery>,
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
            next_id: 0,
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
        table: Option<Table>,
    ) -> i32 {
        let new_id = self.next_id;
        self.next_id += 1;
        let query = OpenQuery::new(needed_responses, connection, query, table);
        self.queries.insert(new_id, query);
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
        self.queries.remove(id);
    }

    /// Adds a response to the `OpenQuery` with the specified ID and returns it if the query is closed.
    ///
    /// # Parameters
    /// - `open_query_id`: The ID of the query.
    /// - `response`: The response to be added.
    ///
    /// # Returns
    /// The `OpenQuery` if it has been closed, or `None` if it is still open.
    pub fn add_response_and_get_if_closed(
        &mut self,
        open_query_id: i32,
        response: String,
    ) -> Option<OpenQuery> {
        match self.get_query_mut(&open_query_id) {
            Some(query) => {
                query.add_response(response);

                if query.is_close() {
                    self.queries.remove(&open_query_id)
                } else {
                    None
                }
            }
            None => None,
        }
    }
}

/// Implements `fmt::Debug` for `OpenQueryHandler` to show the active queries and their statuses.
impl fmt::Debug for OpenQueryHandler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let query_status: Vec<String> = self
            .queries
            .iter()
            .map(|(id, query)| {
                format!(
                    "ID {}: responses {}/{} with responses: {:?}",
                    id, query.actual_responses, query.needed_responses, query.responses
                )
            })
            .collect();

        write!(f, "Active Queries:\n{}", query_status.join("\n"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use query_coordinator::clauses::table::create_table_cql::CreateTable;
    use query_coordinator::Query;
    use std::net::TcpListener;

    // Helper function to create a dummy TCP stream for testing.
    fn get_dummy_tcpstream() -> TcpStream {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        TcpStream::connect(listener.local_addr().unwrap()).unwrap()
    }

    // Helper function to create a dummy Table for testing.
    // Function to create a sample table for testing.
    fn get_dummy_table() -> Table {
        let query_tokens = vec![
            "CREATE".to_string(),
            "TABLE".to_string(),
            "users".to_string(),
            "(id INT PRIMARY KEY, name TEXT, age INT)".to_string(),
        ];

        let create_table = CreateTable::new_from_tokens(query_tokens).unwrap();
        Table::new(create_table)
    }

    // Helper function to create a dummy Query for testing.
    fn get_dummy_query() -> Query {
        Query::Select(query_coordinator::clauses::select_sql::Select {
            table_name: "dummy_table".to_string(),
            columns: vec!["col1".to_string(), "col2".to_string()],
            where_clause: None,
            orderby_clause: None,
        })
    }

    #[test]
    fn test_open_query_initialization() {
        let stream = get_dummy_tcpstream();
        let query = get_dummy_query();
        let table = get_dummy_table();
        let open_query = OpenQuery::new(3, stream, query, Some(table));
        assert_eq!(open_query.needed_responses, 3);
        assert_eq!(open_query.actual_responses, 0);
        assert!(open_query.responses.is_empty());
    }

    #[test]
    fn test_open_query_add_response() {
        let stream = get_dummy_tcpstream();
        let query = get_dummy_query();
        let mut open_query = OpenQuery::new(2, stream, query, None);
        open_query.add_response("Response 1".to_string());
        assert_eq!(open_query.actual_responses, 1);
        assert_eq!(open_query.responses, vec!["Response 1".to_string()]);

        open_query.add_response("Response 2".to_string());
        assert_eq!(open_query.actual_responses, 2);
        assert_eq!(
            open_query.responses,
            vec!["Response 1".to_string(), "Response 2".to_string()]
        );
    }

    #[test]
    fn test_open_query_is_close() {
        let stream = get_dummy_tcpstream();
        let query = get_dummy_query();
        let table = get_dummy_table();
        let mut open_query = OpenQuery::new(2, stream, query, Some(table));
        assert!(!open_query.is_close());

        open_query.add_response("Response 1".to_string());
        assert!(!open_query.is_close());

        open_query.add_response("Response 2".to_string());
        assert!(open_query.is_close());
    }

    #[test]
    fn test_open_query_get_responses() {
        let stream = get_dummy_tcpstream();
        let query = get_dummy_query();
        let mut open_query = OpenQuery::new(2, stream, query, None);
        open_query.add_response("Response 1".to_string());
        open_query.add_response("Response 2".to_string());

        let responses = open_query._get_responses();
        assert_eq!(
            responses,
            vec!["Response 1".to_string(), "Response 2".to_string()]
        );
    }

    #[test]
    fn test_open_query_handler_create_query() {
        let stream = get_dummy_tcpstream();
        let query = get_dummy_query();
        let table = get_dummy_table();
        let mut handler = OpenQueryHandler::new();
        let query_id = handler.new_open_query(3, stream, query, Some(table));
        assert!(handler.queries.contains_key(&query_id));
        assert_eq!(handler.queries[&query_id].needed_responses, 3);
    }

    #[test]
    fn test_open_query_handler_remove_query() {
        let stream = get_dummy_tcpstream();
        let query = get_dummy_query();
        let table = get_dummy_table();
        let mut handler = OpenQueryHandler::new();
        let query_id = handler.new_open_query(2, stream, query, Some(table));
        assert!(handler.queries.contains_key(&query_id));

        handler._remove_query(&query_id);
        assert!(!handler.queries.contains_key(&query_id));
    }

    #[test]
    fn test_open_query_handler_debug_output() {
        let stream1 = get_dummy_tcpstream();
        let stream2 = get_dummy_tcpstream();
        let query1 = get_dummy_query();
        let query2 = get_dummy_query();
        let table1 = get_dummy_table();
        let table2 = get_dummy_table();
        let mut handler = OpenQueryHandler::new();
        let query_id1 = handler.new_open_query(2, stream1, query1, Some(table1));
        let query_id2 = handler.new_open_query(3, stream2, query2, Some(table2));

        handler.add_response_and_get_if_closed(query_id1, "Response A".to_string());
        handler.add_response_and_get_if_closed(query_id2, "Response B".to_string());

        let debug_output = format!("{:?}", handler);
        assert!(debug_output.contains(&format!("ID {}: responses 1/2", query_id1)));
        assert!(debug_output.contains(&format!("ID {}: responses 1/3", query_id2)));
    }
}
