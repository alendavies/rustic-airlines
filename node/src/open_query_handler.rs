use std::collections::HashMap;
use std::fmt;
use std::net::TcpStream;

use crate::errors::NodeError;

#[derive(Debug)]
pub struct OpenQuery {
    needed_responses: i32,
    actual_responses: i32,
    responses: Vec<String>,
    connection: TcpStream,
}

impl OpenQuery {
    pub fn new(needed_responses: i32, connection: TcpStream) -> Self {
        Self {
            needed_responses,
            actual_responses: 0,
            responses: vec![],
            connection,
        }
    }

    pub fn add_response(&mut self, response: String) {
        self.responses.push(response);
        self.actual_responses += 1;
    }

    pub fn is_close(&self) -> bool {
        self.actual_responses == self.needed_responses
    }

    pub fn get_responses(&self) -> Vec<String> {
        self.responses.clone()
    }

    pub fn get_connection(&self) -> &TcpStream {
        &self.connection
    }
}

// Implementación de fmt::Debug para OpenQuery para mostrar el ID y el estado de respuestas
impl fmt::Display for OpenQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ID: responses {}/{} with responses: {:?}",
            self.actual_responses, self.needed_responses, self.responses
        )
    }
}

pub struct OpenQueryHandler {
    queries: HashMap<i32, OpenQuery>,
    next_id: i32,
}

impl OpenQueryHandler {
    pub fn new() -> Self {
        Self {
            queries: HashMap::new(),
            next_id: 0,
        }
    }

    pub fn new_open_query(&mut self, needed_responses: i32, connection: TcpStream) -> i32 {
        let new_id = self.next_id;
        self.next_id += 1;
        let query = OpenQuery::new(needed_responses, connection);
        self.queries.insert(new_id, query);
        new_id
    }

    pub fn get_query_mut(&mut self, id: &i32) -> Option<&mut OpenQuery> {
        self.queries.get_mut(id)
    }

    pub fn get_query_and_delete(&mut self, id: i32) -> Option<OpenQuery> {
        self.queries.remove(&id)
    }

    pub fn get_connection_mut(&mut self, id: i32) -> Result<TcpStream, NodeError> {
        let connection = self
            .get_query_mut(&id)
            .ok_or(NodeError::OtherError)?
            .get_connection();

        connection.try_clone().map_err(|_| NodeError::OtherError)
    }

    pub fn remove_query(&mut self, id: &i32) {
        self.queries.remove(id);
    }

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

// Implementación de Debug personalizada para OpenQueryHandler para mostrar el vector de queries activos
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
    use std::net::TcpListener;

    fn get_dummy_tcpstream() -> TcpStream {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        TcpStream::connect(listener.local_addr().unwrap()).unwrap()
    }

    #[test]
    fn test_open_query_initialization() {
        let stream = get_dummy_tcpstream();
        let query = OpenQuery::new(3, stream);
        assert_eq!(query.needed_responses, 3);
        assert_eq!(query.actual_responses, 0);
        assert!(query.responses.is_empty());
    }

    #[test]
    fn test_open_query_add_response() {
        let stream = get_dummy_tcpstream();
        let mut query = OpenQuery::new(2, stream);
        query.add_response("Response 1".to_string());
        assert_eq!(query.actual_responses, 1);
        assert_eq!(query.responses, vec!["Response 1".to_string()]);

        query.add_response("Response 2".to_string());
        assert_eq!(query.actual_responses, 2);
        assert_eq!(
            query.responses,
            vec!["Response 1".to_string(), "Response 2".to_string()]
        );
    }

    #[test]
    fn test_open_query_is_close() {
        let stream = get_dummy_tcpstream();
        let mut query = OpenQuery::new(2, stream);
        assert!(!query.is_close());

        query.add_response("Response 1".to_string());
        assert!(!query.is_close());

        query.add_response("Response 2".to_string());
        assert!(query.is_close());
    }

    #[test]
    fn test_open_query_get_responses() {
        let stream = get_dummy_tcpstream();
        let mut query = OpenQuery::new(2, stream);
        query.add_response("Response 1".to_string());
        query.add_response("Response 2".to_string());

        let responses = query.get_responses();
        assert_eq!(
            responses,
            vec!["Response 1".to_string(), "Response 2".to_string()]
        );
    }

    #[test]
    fn test_open_query_handler_create_query() {
        let stream = get_dummy_tcpstream();
        let mut handler = OpenQueryHandler::new();
        let query_id = handler.new_open_query(3, stream);
        assert!(handler.queries.contains_key(&query_id));
        assert_eq!(handler.queries[&query_id].needed_responses, 3);
    }

    // #[test]
    // fn test_open_query_handler_add_response() {
    //     let stream = get_dummy_tcpstream();
    //     let mut handler = OpenQueryHandler::new();
    //     let query_id = handler.new_open_query(2, stream);

    //     // Add first response
    //     let (is_closed, responses) =
    //         handler.add_response_and_get_if_closed(query_id, "Response 1".to_string());
    //     assert!(!is_closed);
    //     assert!(responses.is_none());
    //     assert_eq!(handler.queries[&query_id].actual_responses, 1);

    //     // Add second response, should close the query
    //     let (is_closed, responses) =
    //         handler.add_response_and_get_if_closed(query_id, "Response 2".to_string());
    //     assert!(is_closed);
    //     assert_eq!(
    //         responses,
    //         Some(vec!["Response 1".to_string(), "Response 2".to_string()])
    //     );
    //     assert!(!handler.queries.contains_key(&query_id)); // Query should be removed after closing
    // }

    #[test]
    fn test_open_query_handler_remove_query() {
        let stream = get_dummy_tcpstream();
        let mut handler = OpenQueryHandler::new();
        let query_id = handler.new_open_query(2, stream);
        assert!(handler.queries.contains_key(&query_id));

        handler.remove_query(&query_id);
        assert!(!handler.queries.contains_key(&query_id));
    }

    #[test]
    fn test_open_query_handler_debug_output() {
        let stream1 = get_dummy_tcpstream();
        let stream2 = get_dummy_tcpstream();
        let mut handler = OpenQueryHandler::new();
        let query_id1 = handler.new_open_query(2, stream1);
        let query_id2 = handler.new_open_query(3, stream2);

        handler.add_response_and_get_if_closed(query_id1, "Response A".to_string());
        handler.add_response_and_get_if_closed(query_id2, "Response B".to_string());

        let debug_output = format!("{:?}", handler);
        assert!(debug_output.contains(&format!("ID {}: responses 1/2", query_id1)));
        assert!(debug_output.contains(&format!("ID {}: responses 1/3", query_id2)));
    }
}
