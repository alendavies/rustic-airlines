#[derive(Debug, PartialEq)]
pub enum ClientState {
    Startup,
    Authentication,
    Authenticated,
}