// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Source-owned conversion from execution error kinds to MySQL ERR fields.
//!
//! Go's `clientConn.writeError` first unwraps a `terror.Error`, calls
//! `terror.ToSQLError`, and only then writes the code, SQLSTATE, and message
//! fields (`pkg/server/conn.go:1725-1768`).  This module owns the middle step
//! for Rust callers.  It deliberately does not encode bytes (that remains
//! [`crate::encode_error_packet`]) and does not inspect an executor error type;
//! the dependency direction stays `tidb-exec` -> `tidb-protocol`.
//!
//! `ErrorDescriptor` is the small typed hand-off that a future executor/server
//! adapter can construct from `ExecError` or a richer session error context.
//! The message is supplied by that adapter and is preserved byte-for-byte.
//! This avoids guessing missing database/table/column context while the
//! session error context is still being ported.

use crate::ErrorPacket;
use tidb_error::mysql::mysql_state;

/// Unknown column error.
pub use tidb_error::mysql::errcode::ErrBadField as MYSQL_ERR_BAD_FIELD;
/// String value exceeds the declared width.
pub use tidb_error::mysql::errcode::ErrDataTooLong as MYSQL_ERR_DATA_TOO_LONG;
/// Duplicate primary/unique key entry error.
pub use tidb_error::mysql::errcode::ErrDupEntry as MYSQL_ERR_DUP_ENTRY;
/// Duplicate index name error.
pub use tidb_error::mysql::errcode::ErrDupKeyName as MYSQL_ERR_DUP_KEY_NAME;
/// Referenced table does not exist.
pub use tidb_error::mysql::errcode::ErrNoSuchTable as MYSQL_ERR_UNKNOWN_TABLE;
/// Feature is not supported yet.
pub use tidb_error::mysql::errcode::ErrNotSupportedYet as MYSQL_ERR_NOT_SUPPORTED_YET;
/// SQL parser error.
pub use tidb_error::mysql::errcode::ErrParse as MYSQL_ERR_PARSE;
/// MySQL error numbers used by the source-shaped conversion table.
///
/// These values are the existing `pkg/parser/mysql/errcode.go` constants.  Do
/// not add a number here merely because a Rust error looks similar: a variant
/// without an exact TiDB source mapping resolves to [`MYSQL_ERR_UNKNOWN`].
/// Generic TiDB/MySQL error used when no narrower source mapping exists.
pub use tidb_error::mysql::errcode::ErrUnknown as MYSQL_ERR_UNKNOWN;
/// Numeric value is outside the declared range.
pub use tidb_error::mysql::errcode::ErrWarnDataOutOfRange as MYSQL_ERR_WARN_DATA_OUT_OF_RANGE;
/// Inserted row has the wrong number of values.
pub use tidb_error::mysql::errcode::ErrWrongValueCountOnRow as MYSQL_ERR_WRONG_VALUE_COUNT_ON_ROW;

/// The source-shaped execution/error categories currently available to the
/// Rust rewrite.
///
/// The variants intentionally mirror the current `tidb-exec::ExecError`
/// vocabulary without importing that crate.  A later adapter can convert
/// `ExecError` structurally rather than passing an untyped errno or formatting
/// an arbitrary error string.  Categories whose Go `terror`/MySQL code is not
/// unambiguous remain explicit and resolve to `Unknown`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorKind {
    /// SQL parser rejected the statement (`mysql.ErrParse`).
    Parse,
    /// The executor received a non-query statement where a query was needed.
    NotSelect,
    /// A table-backed operation is outside the current executor boundary.
    RequiresTable,
    /// A wildcard could not be expanded without schema metadata.
    Wildcard,
    /// A feature is outside the implemented subset, but without an exact
    /// source errno.  This remains `Unknown`; use [`Self::NotSupportedYet`]
    /// only when the source error was explicitly `ErrNotSupportedYet`.
    Unsupported,
    /// The source explicitly used `mysql.ErrNotSupportedYet`.
    NotSupportedYet,
    /// The request or packet failed before SQL execution.
    Protocol,
    /// A referenced table was not found (`mysql.ErrNoSuchTable`).
    UnknownTable,
    /// A savepoint name was not found; no exact MySQL code is assigned yet.
    UnknownSavepoint,
    /// A referenced column was not found (`mysql.ErrBadField`).
    UnknownColumn,
    /// An index name was already present (`mysql.ErrDupKeyName`).
    DuplicateIndex,
    /// A row violated a primary/unique key (`mysql.ErrDupEntry`).
    DuplicateKey,
    /// A shared-session optimistic publish lost its retry budget.
    WriteConflict,
    /// A grouped query references a non-grouped column.
    UngroupedColumn,
    /// An inserted row has the wrong number of values (`ErrWrongValueCountOnRow`).
    ColumnCountMismatch,
    /// A string value exceeded a column's declared width (`ErrDataTooLong`).
    DataTooLong,
    /// A numeric value exceeded its declared range (`ErrWarnDataOutOfRange`).
    OutOfRange,
    /// A foreign-key check failed; child/parent errors have distinct source
    /// errno values, so the generic executor variant is intentionally unknown.
    ForeignKeyViolation,
    /// Expression evaluation failed without a single source errno.
    Eval,
    /// An error category not yet represented by the Rust source vocabulary.
    Unknown,
}

/// Typed error hand-off between execution/session code and the protocol.
///
/// `message` is kept as bytes because Go's `SQLError.Message` is ultimately
/// appended as bytes by `writeError`; no implicit UTF-8 replacement belongs in
/// this conversion seam.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ErrorDescriptor {
    /// Source error category used for the exact code/SQLSTATE lookup.
    pub kind: ErrorKind,
    /// Already-rendered source message bytes.
    pub message: Vec<u8>,
}

impl ErrorDescriptor {
    /// Builds a descriptor while preserving the caller's message bytes.
    pub fn new(kind: ErrorKind, message: impl Into<Vec<u8>>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

/// Converts a typed source error into the fields consumed by
/// [`crate::encode_error_packet`].
///
/// `protocol_41` is copied into the returned packet so legacy clients retain
/// the source behavior of omitting `#` and SQLSTATE.  Packet framing, errno
/// metrics, error redaction, connection state, writes, and flushes remain
/// outside this leaf.
pub fn error_packet_from_descriptor(
    descriptor: &ErrorDescriptor,
    protocol_41: bool,
) -> ErrorPacket {
    let code = code_for_kind(descriptor.kind);
    ErrorPacket::new(
        code,
        mysql_state(code).as_bytes(),
        descriptor.message.clone(),
        protocol_41,
    )
}

fn code_for_kind(kind: ErrorKind) -> u16 {
    match kind {
        ErrorKind::Parse => MYSQL_ERR_PARSE,
        ErrorKind::NotSupportedYet => MYSQL_ERR_NOT_SUPPORTED_YET,
        ErrorKind::UnknownTable => MYSQL_ERR_UNKNOWN_TABLE,
        ErrorKind::UnknownColumn => MYSQL_ERR_BAD_FIELD,
        ErrorKind::DuplicateIndex => MYSQL_ERR_DUP_KEY_NAME,
        ErrorKind::DuplicateKey => MYSQL_ERR_DUP_ENTRY,
        ErrorKind::ColumnCountMismatch => MYSQL_ERR_WRONG_VALUE_COUNT_ON_ROW,
        ErrorKind::DataTooLong => MYSQL_ERR_DATA_TOO_LONG,
        ErrorKind::OutOfRange => MYSQL_ERR_WARN_DATA_OUT_OF_RANGE,
        ErrorKind::NotSelect
        | ErrorKind::RequiresTable
        | ErrorKind::Wildcard
        | ErrorKind::Unsupported
        | ErrorKind::Protocol
        | ErrorKind::UnknownSavepoint
        | ErrorKind::WriteConflict
        | ErrorKind::UngroupedColumn
        | ErrorKind::ForeignKeyViolation
        | ErrorKind::Eval
        | ErrorKind::Unknown => MYSQL_ERR_UNKNOWN,
    }
}
