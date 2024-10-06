use std::vec::Vec;
use crate::frame::header::FrameHeader;

#[derive(Debug)]
pub struct Frame {
    header: FrameHeader,
    body: String,        // Body (String should do for now)
}

impl Frame {
    pub fn new(header: FrameHeader, body: String) -> Self {
        Frame { header, body }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buffer = Vec::new();

        buffer.extend(self.header.to_bytes());

        buffer.extend(self.body.as_bytes());

        buffer
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {

        if bytes.len() < 9 {
            return Err("El buffer es demasiado pequeño para un paquete completo".to_string());
        }

        let header = FrameHeader::from_bytes(&bytes[0..9])
            .map_err(|e| e.to_string())?;

        let body_length = *header.body_length() as usize;
        if bytes.len() < 9 + body_length {
            return Err("El buffer no tiene suficientes datos para el body".to_string());
        }

        // (We'll asume UTF-8)
        let body = String::from_utf8(bytes[9..(9 + body_length)].to_vec())
            .map_err(|_| "Error al convertir el body a String".to_string())?;

        Ok(Frame{ header, body })
    }

    pub fn header(&self) -> &FrameHeader {
        &self.header
    }

    pub fn body(&self) -> &String {
        &self.body
    }
}
