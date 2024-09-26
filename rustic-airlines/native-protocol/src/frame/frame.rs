use std::vec::Vec;

#[derive(Debug)]
pub struct Frame {
    header: FrameHeader,
    body: Vec<u8>,        // Body (a definir más tarde)
}

impl Frame {
    pub fn new(header: FrameHeader, body: Vec<u8>) -> Self {
        Frame { header, body }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buffer = Vec::new();

        buffer.extend(self.header.to_bytes());

        buffer.extend(&self.body);

        buffer
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {

        if bytes.len() < 9 {
            return Err("El buffer es demasiado pequeño para un paquete completo");
        }
        let header = FrameHeader::from_bytes(&bytes[0..9])?;


        let body_length = header.body_length as usize;
        if bytes.len() < 9 + body_length {
            return Err("El buffer no tiene suficientes datos para el body");
        }
        let body = bytes[9..(9 + body_length)].to_vec();

        Ok(Frame { header, body })
    }

    pub fn header(&self) -> &FrameHeader {
        &self.header
    }

    pub fn body(&self) -> &Vec<u8> {
        &self.body
    }
}
