use native_protocol::{
    frame::Frame,
    messages::query::{Query, QueryParams},
    Serializable,
};

#[derive(Debug)]
pub enum Request {
    Startup,
    Query(Query),
}

pub fn handle_client_request(bytes: &[u8]) -> Request {
    let frame = Frame::from_bytes(bytes).unwrap();

    match frame {
        Frame::Startup => Request::Startup,
        Frame::Query(query) => Request::Query(query),
        _ => panic!(),
    }
}
