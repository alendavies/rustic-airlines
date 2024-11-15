use std::{
    io::{Cursor, Read},
    net::Ipv4Addr,
};

pub trait Serializable {
    fn as_bytes(&self) -> Vec<u8>;

    fn from_bytes(bytes: &[u8]) -> Result<Self, InternodeMessageError>
    where
        Self: Sized;
}

#[derive(Clone, Copy)]
enum Opcode {
    Query = 0x01,
    Response = 0x02,
    // Gossip = 0x03,
}

struct InternodeHeader {
    opcode: Opcode,
    ip: Ipv4Addr,
}

impl Serializable for InternodeHeader {
    /// 0    8    16   24   32
    /// +----+----+----+----+
    /// |         ip        |
    /// +----+----+----+----+
    /// | op |              |
    /// +----+----+----+----+
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        bytes.extend_from_slice(&self.ip.octets());
        bytes.push(self.opcode as u8);

        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, InternodeMessageError>
    where
        Self: Sized,
    {
        let mut cursor = Cursor::new(bytes);

        let mut ip_bytes = [0u8; 4];
        cursor
            .read_exact(&mut ip_bytes)
            .map_err(|_| InternodeMessageError)?;

        let ip = Ipv4Addr::from(ip_bytes);

        let mut opcode_byte = [0u8; 1];
        cursor
            .read_exact(&mut opcode_byte)
            .map_err(|_| InternodeMessageError)?;

        let opcode = match opcode_byte[0] {
            0x01 => Opcode::Query,
            0x02 => Opcode::Response,
            // 0x03 => Opcode::Gossip,
            _ => return Err(InternodeMessageError),
        };

        Ok(InternodeHeader { opcode, ip })
    }
}

#[derive(Debug, PartialEq)]
pub enum InternodeMessageContent {
    Query(InternodeQuery),
    Response(InternodeResponse),
}

pub struct InternodeMessage {
    from: Ipv4Addr,
    content: InternodeMessageContent,
}

/// A query sent by a coordinator node to other nodes.
#[derive(Debug, PartialEq)]
struct InternodeQuery {
    /// The CQL query string.
    query_string: String,
    /// The `id` of the query to be identified by the open queries handler.
    open_query_id: u32,
    /// The client that owns the query in this node.
    client_id: u32,
    /// This query should be executed over the replications stored by the node,
    /// not over its owned data.
    replication: bool,
    /// Keyspace on which the query acts.
    keyspace_name: String,
    /// The timestamp when the coordinator node received the query.
    timestamp: i64,
}

impl Serializable for InternodeQuery {
    // 0    8    16   24   32
    // +----+----+----+----+
    // |   open_query_id   |
    // +----+----+----+----+
    // |     client_id     |
    // +----+----+----+----+
    // |     timestamp     |
    // +----+----+----+----+
    // |     timestamp     |
    // +----+----+----+----+
    // |rep |     keyspace_
    // +----+----+----+----+
    // |len |keyspace_name |
    // |        ...        |
    // |   keyspace_name   |
    // +----+----+----+----+
    // |    query_length   |
    // +----+----+----+----+
    // |    query_string   |
    // |        ...        |
    // |    query_string   |
    // +----+----+----+----+
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        bytes.extend(&self.open_query_id.to_be_bytes());
        bytes.extend(&self.client_id.to_be_bytes());
        bytes.extend(&self.timestamp.to_be_bytes());

        bytes.push(self.replication as u8);

        let keyspace_name_len = self.keyspace_name.len() as u32;
        bytes.extend(&keyspace_name_len.to_be_bytes());
        bytes.extend(self.keyspace_name.as_bytes());

        let query_string_len = self.query_string.len() as u32;
        bytes.extend(&query_string_len.to_be_bytes());
        bytes.extend(self.query_string.as_bytes());

        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, InternodeMessageError>
    where
        Self: Sized,
    {
        let mut cursor = Cursor::new(bytes);

        let mut open_query_id_bytes = [0u8; 4];
        cursor
            .read_exact(&mut open_query_id_bytes)
            .map_err(|_| InternodeMessageError)?;
        let open_query_id = u32::from_be_bytes(open_query_id_bytes);

        let mut client_id_bytes = [0u8; 4];
        cursor
            .read_exact(&mut client_id_bytes)
            .map_err(|_| InternodeMessageError)?;
        let client_id = u32::from_be_bytes(client_id_bytes);

        let mut timestamp_bytes = [0u8; 8];
        cursor
            .read_exact(&mut timestamp_bytes)
            .map_err(|_| InternodeMessageError)?;
        let timestamp = i64::from_be_bytes(timestamp_bytes);

        let mut replication_byte = [0u8; 1];
        cursor
            .read_exact(&mut replication_byte)
            .map_err(|_| InternodeMessageError)?;
        let replication = replication_byte[0] != 0;

        let mut keyspace_name_len_bytes = [0u8; 4];
        cursor
            .read_exact(&mut keyspace_name_len_bytes)
            .map_err(|_| InternodeMessageError)?;
        let keyspace_name_len = u32::from_be_bytes(keyspace_name_len_bytes) as usize;

        let mut keyspace_name_bytes = vec![0u8; keyspace_name_len];
        cursor
            .read_exact(&mut keyspace_name_bytes)
            .map_err(|_| InternodeMessageError)?;
        let keyspace_name =
            String::from_utf8(keyspace_name_bytes).map_err(|_| InternodeMessageError)?;

        let mut query_string_len_bytes = [0u8; 4];
        cursor
            .read_exact(&mut query_string_len_bytes)
            .map_err(|_| InternodeMessageError)?;
        let query_string_len = u32::from_be_bytes(query_string_len_bytes) as usize;

        let mut query_string_bytes = vec![0u8; query_string_len];
        cursor
            .read_exact(&mut query_string_bytes)
            .map_err(|_| InternodeMessageError)?;
        let query_string =
            String::from_utf8(query_string_bytes).map_err(|_| InternodeMessageError)?;

        Ok(InternodeQuery {
            query_string,
            open_query_id,
            client_id,
            replication,
            keyspace_name,
            timestamp,
        })
    }
}

#[derive(Debug, PartialEq)]
enum InternodeResponseStatus {
    Ok = 0x00,
    Error = 0x01,
}

#[derive(Debug, PartialEq)]
struct InternodeResponseContent {
    columns: Vec<String>,
    select_columns: Vec<String>,
    values: Vec<Vec<String>>,
}

impl Serializable for InternodeResponseContent {
    // TODO: chequear la serialización del content
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        let columns_len = self.columns.len() as u32;
        bytes.extend(&columns_len.to_be_bytes());

        for column in &self.columns {
            let column_len = column.len() as u32;
            bytes.extend(&column_len.to_be_bytes());
            bytes.extend(column.as_bytes());
        }

        let select_columns_len = self.select_columns.len() as u32;
        bytes.extend(&select_columns_len.to_be_bytes());

        for select_column in &self.select_columns {
            let select_column_len = select_column.len() as u32;
            bytes.extend(&select_column_len.to_be_bytes());
            bytes.extend(select_column.as_bytes());
        }

        let values_len = self.values.len() as u32;
        bytes.extend(&values_len.to_be_bytes());

        for value in &self.values {
            let value_len = value.len() as u32;
            bytes.extend(&value_len.to_be_bytes());
            for value_part in value {
                let value_part_len = value_part.len() as u32;
                bytes.extend(&value_part_len.to_be_bytes());
                bytes.extend(value_part.as_bytes());
            }
        }

        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, InternodeMessageError> {
        let mut cursor = Cursor::new(bytes);

        let mut columns_len_bytes = [0u8; 4];
        cursor
            .read_exact(&mut columns_len_bytes)
            .map_err(|_| InternodeMessageError)?;
        let columns_len = u32::from_be_bytes(columns_len_bytes) as usize;

        let mut columns = Vec::with_capacity(columns_len);
        for _ in 0..columns_len {
            let mut column_len_bytes = [0u8; 4];
            cursor
                .read_exact(&mut column_len_bytes)
                .map_err(|_| InternodeMessageError)?;
            let column_len = u32::from_be_bytes(column_len_bytes) as usize;

            let mut column_bytes = vec![0u8; column_len];
            cursor
                .read_exact(&mut column_bytes)
                .map_err(|_| InternodeMessageError)?;
            let column = String::from_utf8(column_bytes).map_err(|_| InternodeMessageError)?;

            columns.push(column);
        }

        let mut select_columns_len_bytes = [0u8; 4];
        cursor
            .read_exact(&mut select_columns_len_bytes)
            .map_err(|_| InternodeMessageError)?;
        let select_columns_len = u32::from_be_bytes(select_columns_len_bytes) as usize;

        let mut select_columns = Vec::with_capacity(select_columns_len);
        for _ in 0..select_columns_len {
            let mut select_column_len_bytes = [0u8; 4];
            cursor
                .read_exact(&mut select_column_len_bytes)
                .map_err(|_| InternodeMessageError)?;
            let select_column_len = u32::from_be_bytes(select_column_len_bytes) as usize;

            let mut select_column_bytes = vec![0u8; select_column_len];
            cursor
                .read_exact(&mut select_column_bytes)
                .map_err(|_| InternodeMessageError)?;
            let select_column =
                String::from_utf8(select_column_bytes).map_err(|_| InternodeMessageError)?;

            select_columns.push(select_column);
        }

        let mut values_len_bytes = [0u8; 4];
        cursor
            .read_exact(&mut values_len_bytes)
            .map_err(|_| InternodeMessageError)?;
        let values_len = u32::from_be_bytes(values_len_bytes) as usize;

        let mut values = Vec::with_capacity(values_len);

        for _ in 0..values_len {
            let mut value_len_bytes = [0u8; 4];
            cursor
                .read_exact(&mut value_len_bytes)
                .map_err(|_| InternodeMessageError)?;
            let value_len = u32::from_be_bytes(value_len_bytes) as usize;

            let mut value = Vec::with_capacity(value_len);
            for _ in 0..value_len {
                let mut value_part_len_bytes = [0u8; 4];
                cursor
                    .read_exact(&mut value_part_len_bytes)
                    .map_err(|_| InternodeMessageError)?;
                let value_part_len = u32::from_be_bytes(value_part_len_bytes) as usize;

                let mut value_part_bytes = vec![0u8; value_part_len];
                cursor
                    .read_exact(&mut value_part_bytes)
                    .map_err(|_| InternodeMessageError)?;
                let value_part =
                    String::from_utf8(value_part_bytes).map_err(|_| InternodeMessageError)?;

                value.push(value_part);
            }

            values.push(value);
        }

        Ok(InternodeResponseContent {
            columns,
            select_columns,
            values,
        })
    }
}

/// A response sent by a node in response of a coordinator query.
#[derive(Debug, PartialEq)]
struct InternodeResponse {
    /// The `id` of the query to be identified by the open queries handler.
    open_query_id: u32,
    /// If the query was successful.
    status: InternodeResponseStatus,
    /// The response content, if any (for example a `SELECT`).
    content: Option<InternodeResponseContent>,
}

impl Serializable for InternodeResponse {
    // 0    8    16   24   32
    // +----+----+----+----+
    // |   open_query_id   |
    // +----+----+----+----+
    // |stat|cont_len |cont|
    // +----+----+----+----+
    // |      content      |
    // |        ...        |
    // |      content      |
    // +----+----+----+----+
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        bytes.extend(&self.open_query_id.to_be_bytes());

        let status_byte = match self.status {
            InternodeResponseStatus::Ok => 0x00,
            InternodeResponseStatus::Error => 0x01,
        };
        bytes.push(status_byte);

        let content_bytes = if let Some(content) = &self.content {
            Some(content.as_bytes())
        } else {
            None
        };

        if let Some(c_bytes) = content_bytes {
            bytes.extend((c_bytes.len() as u16).to_be_bytes());
            bytes.extend(&c_bytes);
        } else {
            bytes.push(0);
        }

        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, InternodeMessageError>
    where
        Self: Sized,
    {
        let mut cursor = Cursor::new(bytes);

        let mut open_query_id_bytes = [0u8; 4];
        cursor
            .read_exact(&mut open_query_id_bytes)
            .map_err(|_| InternodeMessageError)?;
        let open_query_id = u32::from_be_bytes(open_query_id_bytes);

        let mut status_byte = [0u8; 1];
        cursor
            .read_exact(&mut status_byte)
            .map_err(|_| InternodeMessageError)?;
        let status = match status_byte[0] {
            0x00 => InternodeResponseStatus::Ok,
            0x01 => InternodeResponseStatus::Error,
            _ => return Err(InternodeMessageError),
        };

        let mut content_len_bytes = [0u8; 2];

        cursor
            .read_exact(&mut content_len_bytes)
            .map_err(|_| InternodeMessageError)?;

        let content_len = u16::from_be_bytes(content_len_bytes);

        let mut content_bytes = vec![0u8; content_len as usize];
        cursor
            .read_exact(&mut content_bytes)
            .map_err(|_| InternodeMessageError)?;
        let content = if content_bytes.is_empty() {
            None
        } else {
            Some(
                InternodeResponseContent::from_bytes(&content_bytes)
                    .map_err(|_| InternodeMessageError)?,
            )
        };

        Ok(InternodeResponse {
            open_query_id,
            status,
            content,
        })
    }
}

#[derive(Debug)]
pub struct InternodeMessageError;

impl Serializable for InternodeMessage {
    // 0    8    16   24   32
    // +----+----+----+----+
    // |       header      |
    // +----+----+----+----+
    // |head|  content...
    // +----+----+----+----+
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        let opcode = match self.content {
            InternodeMessageContent::Query(_) => Opcode::Query,
            InternodeMessageContent::Response(_) => Opcode::Response,
        };

        let header = InternodeHeader {
            ip: self.from,
            opcode,
        };

        let content_bytes = match &self.content {
            InternodeMessageContent::Query(internode_query) => internode_query.as_bytes(),
            InternodeMessageContent::Response(internode_response) => internode_response.as_bytes(),
        };

        bytes.extend_from_slice(&header.as_bytes());
        bytes.extend_from_slice(&content_bytes);

        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, InternodeMessageError> {
        let mut cursor = Cursor::new(bytes);

        let mut header_bytes = [0u8; 5];
        cursor
            .read_exact(&mut header_bytes)
            .map_err(|_| InternodeMessageError)?;

        let header =
            InternodeHeader::from_bytes(&header_bytes).map_err(|_| InternodeMessageError)?;

        let content = match header.opcode {
            Opcode::Query => InternodeMessageContent::Query(
                InternodeQuery::from_bytes(&bytes[5..]).map_err(|_| InternodeMessageError)?,
            ),
            Opcode::Response => InternodeMessageContent::Response(
                InternodeResponse::from_bytes(&bytes[5..]).map_err(|_| InternodeMessageError)?,
            ),
        };

        let message = InternodeMessage {
            from: header.ip,
            content,
        };

        Ok(message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_to_bytes() {
        let query = InternodeQuery {
            query_string: "SELECT * FROM something".to_string(),
            open_query_id: 1,
            client_id: 1,
            replication: false,
            keyspace_name: "keyspace".to_string(),
            timestamp: 1,
        };

        let bytes = query.as_bytes();

        let mut expected_bytes = Vec::new();
        expected_bytes.extend(query.query_string.len().to_be_bytes());
        expected_bytes.extend(query.query_string.as_bytes());
        expected_bytes.extend(query.open_query_id.to_be_bytes());
        expected_bytes.extend(query.client_id.to_be_bytes());
        expected_bytes.push(query.replication as u8);
        expected_bytes.extend(query.keyspace_name.len().to_be_bytes());
        expected_bytes.extend(query.keyspace_name.as_bytes());
        expected_bytes.extend(query.timestamp.to_be_bytes());

        assert_eq!(bytes, expected_bytes);
    }

    // #[test]
    // fn test_query_from_bytes() {
    //     let query = "SELECT * FROM something";
    //     let bytes = [vec![0x01], query.as_bytes().to_vec()].concat();
    //     let msg = InternodeMessage::from_bytes(&bytes).unwrap();

    //     assert_eq!(msg, InternodeMessage::Query(query.to_string()));
    // }

    // #[test]
    // fn test_response_to_bytes() {
    //     let response = "DATA DATA DATA";
    //     let msg = InternodeMessage::Response(response.to_string());
    //     let bytes = msg.as_bytes();

    //     assert_eq!(bytes, [vec![0x02], response.as_bytes().to_vec()].concat());
    // }

    // #[test]
    // fn test_response_from_bytes() {
    //     let response = "DATA DATA DATA";
    //     let bytes = [vec![0x02], response.as_bytes().to_vec()].concat();
    //     let msg = InternodeMessage::from_bytes(&bytes).unwrap();

    //     assert_eq!(msg, InternodeMessage::Response(response.to_string()));
    // }
}
