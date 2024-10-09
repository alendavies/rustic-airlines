pub enum ResultCode {
    Void = 0x0001,
    Rows = 0x0002,
    SetKeyspace = 0x0003,
    Prepared = 0x0004,
    SchemaChange = 0x0005,
}

/// Indicates a set of rows.
struct Rows;

/// The result to a `use` query.
type SetKeyspace = String;

/// The result to a PREPARE message.
struct Prepared;

///  The result to a schema altering query
/// (creation/update/drop of a keyspace/table/index).
struct SchemaChange;

pub enum Result {
    /// For results carrying no information.
    Void,
    /// For results to select queries, returning a set of rows.
    Rows(Rows),
    /// The result to a `use` query.
    SetKeyspace(SetKeyspace),
    /// Result to a PREPARE message.
    Prepared(Prepared),
    /// The result to a schema altering query.
    SchemaChange(SchemaChange),
}
