use std::io::Read;

enum ConsistencyCode {
    Any = 0x0000,
    One = 0x0001,
    Two = 0x0002,
    Three = 0x0003,
    Quorum = 0x0004,
    All = 0x0005,
    LocalQuorum = 0x0006,
    EachQuorum = 0x0007,
    Serial = 0x0008,
    LocalSerial = 0x0009,
    LocalOne = 0x000A,
}

enum Consistency {
    Any,
    One,
    Two,
    Three,
    Quorum,
    All,
    LocalQuorum,
    EachQuorum,
    Serial,
    LocalSerial,
    LocalOne,
}

impl Consistency {
    fn to_code(&self) -> ConsistencyCode {
        match self {
            Consistency::Any => ConsistencyCode::Any,
            Consistency::One => ConsistencyCode::One,
            Consistency::Two => ConsistencyCode::Two,
            Consistency::Three => ConsistencyCode::Three,
            Consistency::Quorum => ConsistencyCode::Quorum,
            Consistency::All => ConsistencyCode::All,
            Consistency::LocalQuorum => ConsistencyCode::LocalQuorum,
            Consistency::EachQuorum => ConsistencyCode::EachQuorum,
            Consistency::Serial => ConsistencyCode::Serial,
            Consistency::LocalSerial => ConsistencyCode::LocalSerial,
            Consistency::LocalOne => ConsistencyCode::LocalOne,
        }
    }

    fn from_code(consistency_code: u16) -> Self {
        let consistency = match consistency_code {
            0x0000 => Consistency::Any,
            0x0001 => Consistency::One,
            0x0002 => Consistency::Two,
            0x0003 => Consistency::Three,
            0x0004 => Consistency::Quorum,
            0x0005 => Consistency::All,
            0x0006 => Consistency::LocalQuorum,
            0x0007 => Consistency::EachQuorum,
            0x0008 => Consistency::Serial,
            0x0009 => Consistency::LocalSerial,
            0x000A => Consistency::LocalOne,
            _ => panic!("Invalid consistency code"),
        };

        consistency
    }
}

enum FlagCode {
    Values = 0x01,
    SkipMetadata = 0x02,
    PageSize = 0x04,
    WithPagingState = 0x08,
    WithSerialConsistency = 0x10,
    WithDefaultTimestamp = 0x20,
    WithNamesForValues = 0x40,
}

enum Flag {
    /// If set, a [short] <n> followed by <n> [value]
    /// values are provided. Those values are used for bound variables in
    /// the query.
    Values,
    /// If set, the Result Set returned as a response
    /// to the query (if any) will have the NO_METADATA flag.
    SkipMetadata,
    /// If set, <result_page_size> is an [int]
    /// controlling the desired page size of the result (in CQL3 rows).
    PageSize,
    /// If set, <paging_state> should be present.
    /// <paging_state> is a [bytes] value that should have been returned
    /// in a result set.
    WithPagingState,
    /// If set, <serial_consistency> should be
    /// present. <serial_consistency> is the [consistency] level for the
    /// serial phase of conditional updates.
    WithSerialConsistency,
    /// If set, <timestamp> should be present.
    /// <timestamp> is a [long] representing the default timestamp for the query
    /// in microseconds (negative values are forbidden). This will
    /// replace the server side assigned timestamp as default timestamp.
    WithDefaultTimestamp,
    /// This only makes sense if the 0x01 flag is set and
    /// is ignored otherwise. If present, the values from the 0x01 flag will
    /// be preceded by a name.
    WithNamesForValues,
}

struct QueryParams {
    /// Is the consistency level for the operation.
    consistency: Consistency,
    /// Is a byte whose bits define the options for this query.
    flags: Vec<Flag>,
}

impl QueryParams {
    fn flags_to_byte(&self) -> u8 {
        let mut flags_byte: u8 = 0;

        for flag in &self.flags {
            flags_byte |= match flag {
                Flag::Values => FlagCode::Values as u8,
                Flag::SkipMetadata => FlagCode::SkipMetadata as u8,
                Flag::PageSize => FlagCode::PageSize as u8,
                Flag::WithPagingState => FlagCode::WithPagingState as u8,
                Flag::WithSerialConsistency => FlagCode::WithSerialConsistency as u8,
                Flag::WithDefaultTimestamp => FlagCode::WithDefaultTimestamp as u8,
                Flag::WithNamesForValues => FlagCode::WithNamesForValues as u8,
            }
        }

        flags_byte
    }

    fn byte_to_flags(flags_byte: u8) -> Vec<Flag> {
        let mut flags = Vec::new();

        if flags_byte & FlagCode::Values as u8 != 0 {
            flags.push(Flag::Values);
        }
        if flags_byte & FlagCode::SkipMetadata as u8 != 0 {
            flags.push(Flag::SkipMetadata);
        }
        if flags_byte & FlagCode::PageSize as u8 != 0 {
            flags.push(Flag::PageSize);
        }
        if flags_byte & FlagCode::WithPagingState as u8 != 0 {
            flags.push(Flag::WithPagingState);
        }
        if flags_byte & FlagCode::WithSerialConsistency as u8 != 0 {
            flags.push(Flag::WithSerialConsistency);
        }
        if flags_byte & FlagCode::WithDefaultTimestamp as u8 != 0 {
            flags.push(Flag::WithDefaultTimestamp);
        }
        if flags_byte & FlagCode::WithNamesForValues as u8 != 0 {
            flags.push(Flag::WithNamesForValues);
        }

        flags
    }
}

struct Query {
    query: String,
    params: QueryParams,
}

impl Query {
    /// 0         8        16        24        32
    /// +---------+---------+---------+---------+
    /// |        query length (4 bytes)         |
    /// +---------+---------+---------+---------+
    /// |              query bytes              |
    /// +                                       +
    /// |                 ...                   |
    /// +---------+---------+---------+---------+
    /// |  consistency (2)  | flag (1)|         |
    /// +---------+---------+---------+---------+
    /// |         optional parameters           |
    /// +                                       +
    /// |                 ...                   |
    /// +---------+---------+---------+---------+

    /// Serialize the `Query` struct to a byte vector.
    /// The byte vector will contain the query in the format described above.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // Add query string length (4 bytes) and query string
        let query_len = self.query.len() as u32;
        bytes.extend_from_slice(&query_len.to_be_bytes());
        bytes.extend_from_slice(self.query.as_bytes());

        // Add consistency (2 bytes)
        let consistency_code = self.params.consistency.to_code();
        bytes.extend_from_slice(&(consistency_code as u16).to_be_bytes());

        // Add flags (1 byte)
        let flags_byte = self.params.flags_to_byte();
        bytes.push(flags_byte);

        // TODO: Add optional parameters based on flags.

        bytes
    }

    /// Parse a `Query` struct from a byte slice.
    /// The byte slice must contain a query in the format described in `to_bytes`.
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let mut cursor = std::io::Cursor::new(bytes);

        // Read query length (4 bytes)
        let mut query_len_bytes = [0u8; 4];
        cursor.read_exact(&mut query_len_bytes).unwrap();
        let query_len = u32::from_be_bytes(query_len_bytes) as usize;

        // Read the query string (UTF-8)
        let mut query_bytes = vec![0u8; query_len];
        cursor.read_exact(&mut query_bytes).unwrap();
        let query = String::from_utf8(query_bytes).unwrap();

        // Read the consistency level (2 bytes)
        let mut consistency_code_bytes = [0u8; 2];
        cursor.read_exact(&mut consistency_code_bytes).unwrap();
        let consistency_code = u16::from_be_bytes(consistency_code_bytes);

        // Convert the consistency code to the corresponding `Consistency`
        let consistency = Consistency::from_code(consistency_code);

        // Read flags (1 byte)
        let mut flags_byte = [0u8; 1];
        cursor.read_exact(&mut flags_byte).unwrap();
        let flags_byte = flags_byte[0];

        // Convert the flags byte to a vector of `Flag`
        let flags = QueryParams::byte_to_flags(flags_byte);

        // Create the `QueryParams` and the `Query` struct
        let params = QueryParams { consistency, flags };

        Query { query, params }
    }
}
