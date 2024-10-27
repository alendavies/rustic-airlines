#[derive(Debug)]
pub enum NativeError {
    SerializationError,
    DeserializationError,
    NotEnoughBytes,
    CursorError,
    InvalidCode,
    InvalidVariant,
}
