use native_protocol::{frame::Frame, messages::query::Query, types::Bytes, Serializable};

#[derive(Debug)]
pub enum RequestError {
    InvalidFrame,
}

#[derive(Debug)]
pub enum Request {
    Startup,
    Query(Query),
    AuthResponse(String),
}

pub fn handle_client_request(bytes: &[u8]) -> Result<Request, RequestError> {
    let frame = Frame::from_bytes(bytes).unwrap();

    match frame {
        Frame::Startup => Ok(Request::Startup),
        Frame::AuthResponse(auth_response) => {
            let r = if let Bytes::Vec(vec) = auth_response.token {
                String::from_utf8(vec).unwrap()
            } else {
                String::new()
            };

            Ok(Request::AuthResponse(r))
        }
        Frame::Query(query) => Ok(Request::Query(query)),
        _ => Err(RequestError::InvalidFrame),
    }
}
