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

struct Query {
    query: String,
    params: QueryParams,
}
