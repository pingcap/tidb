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

//! MySQL binary prepared-statement packet framing.
//!
//! Packet splitting uses [`crate::BinaryParam`] as the one raw parameter
//! authority. Typed values are derived only after that split, matching Go's
//! `parseBinaryParams` -> `expression.ExecBinaryParam` boundary.

use crate::{
    append_length_encoded_bytes, append_length_encoded_int, encode_eof_packet, ColumnInfo,
    EofPacket, ResultSetOptions, TYPE_BIT, TYPE_BLOB, TYPE_DATE, TYPE_DATETIME, TYPE_DOUBLE,
    TYPE_DURATION, TYPE_ENUM, TYPE_FLOAT, TYPE_GEOMETRY, TYPE_INT24, TYPE_JSON, TYPE_LONG,
    TYPE_LONGLONG, TYPE_LONG_BLOB, TYPE_MEDIUM_BLOB, TYPE_NEW_DECIMAL, TYPE_SET, TYPE_SHORT,
    TYPE_STRING, TYPE_TIDB_VECTOR_FLOAT32, TYPE_TIMESTAMP, TYPE_TINY, TYPE_TINY_BLOB,
    TYPE_UNSPECIFIED, TYPE_VARCHAR, TYPE_VAR_STRING, TYPE_YEAR,
};
use tidb_datatype::{Decimal, PackedTime};

/// MySQL binary-protocol type tag for a signed or unsigned 64-bit integer.
pub const MYSQL_TYPE_LONGLONG: u8 = 0x08;

/// MySQL binary-protocol parameter flag bit that marks an integer unsigned.
pub const MYSQL_UNSIGNED_FLAG: u8 = 0x80;

/// One remembered two-byte parameter type from `COM_STMT_EXECUTE`.
///
/// Go retains the packet's raw type vector on the prepared statement. Only
/// the unsigned bit is observable while values are interpreted, so the Rust
/// representation keeps the type code and that bit without inventing a
/// parallel enum of wire families.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PreparedParameterType {
    type_code: u8,
    is_unsigned: bool,
}

impl PreparedParameterType {
    /// Creates one remembered parameter type.
    #[must_use]
    pub const fn new(type_code: u8, is_unsigned: bool) -> Self {
        Self {
            type_code,
            is_unsigned,
        }
    }

    /// Returns the MySQL field type code supplied by the client.
    #[must_use]
    pub const fn type_code(self) -> u8 {
        self.type_code
    }

    /// Returns whether the packet set MySQL's unsigned flag bit.
    #[must_use]
    pub const fn is_unsigned(self) -> bool {
        self.is_unsigned
    }

    const fn wire_flags(self) -> u8 {
        if self.is_unsigned {
            MYSQL_UNSIGNED_FLAG
        } else {
            0
        }
    }
}

/// A decoded prepared-statement value admitted by this protocol leaf.
///
/// A signed integer width interprets to one `i64` (Go `ExecBinaryParam`
/// sign-extends `int8`/`int16`/`int32`/`int64`); a string parameter carries its
/// raw length-encoded bytes.
#[derive(Clone, Debug, PartialEq)]
pub enum PreparedValue {
    /// A signed integer parameter, sign-extended to 64 bits from its wire width.
    SignedLongLong(i64),
    /// A string parameter's raw bytes (utf8 for the configured node).
    String(Vec<u8>),
    /// An unsigned integer parameter, widened to 64 bits.
    UnsignedLongLong(u64),
    /// A `FLOAT` parameter, widened to `f64` the way Go widens `float32`.
    Float(f32),
    /// A `DOUBLE` parameter.
    Double(f64),
    /// A `DECIMAL` parameter's digits, which Go parses with
    /// `MyDecimal.FromString`.
    Decimal(Vec<u8>),
    /// A parameter the execute packet marked NULL in its bitmap.
    Null,
    /// A temporal parameter, rendered the way Go renders it before parsing:
    /// `binaryDate`/`binaryDateTime`/`binaryTimestamp` produce the date-time
    /// text and `binaryDuration` the day/time span text.
    Temporal(String),
}

/// Whether an execute packet supplied a new parameter type vector.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PreparedParameterTypes {
    /// The packet supplied these types, which the owning connection may retain
    /// for a later type-reuse execute.
    New(Vec<PreparedParameterType>),
    /// The packet explicitly reused the connection-owned prior type vector.
    Reuse,
}

/// A decoded bounded `COM_STMT_EXECUTE` payload.
#[derive(Clone, Debug, PartialEq)]
pub struct PreparedStatementExecute {
    /// The per-connection statement handle selected by the client.
    pub statement_id: u32,
    /// The cursor flags after rejecting the unsupported update/scroll modes.
    pub cursor_flags: u8,
    /// Whether the packet supplied a new type vector or reused the prior one.
    pub parameter_types: PreparedParameterTypes,
    /// Typed values decoded using the new or supplied prior type vector.
    pub values: Vec<PreparedValue>,
}

/// Packet framing or bounded-type failures for prepared statements.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PreparedStatementError {
    /// The payload ended before a required field could be read.
    Truncated {
        /// Name of the field that was incomplete.
        field: &'static str,
        /// Minimum bytes required for that field at its packet position.
        required: usize,
        /// Bytes available from that packet position.
        available: usize,
    },
    /// A field had an invalid bounded value.
    InvalidField {
        /// Name of the invalid field.
        field: &'static str,
        /// Invalid value.
        value: u8,
    },
    /// A statement handle was zero, which this bounded server never allocates.
    ZeroStatementId,
    /// The packet requested a cursor, which the bounded protocol does not own.
    UnsupportedCursorFlag(u8),
    /// A type-reuse execute arrived before the connection had saved a vector.
    MissingPreviousTypeVector,
    /// The saved type vector did not match the prepared parameter count.
    PreviousTypeVectorLength {
        /// Expected type count.
        expected: usize,
        /// Saved type count.
        actual: usize,
    },
    /// The package-level raw parameter splitter rejected the value section.
    BinaryParameter(crate::BinaryParamError),
    /// The packet contained bytes after every declared value was decoded.
    TrailingBytes {
        /// Number of unexpected bytes.
        bytes: usize,
    },
    /// A prepare response cannot represent this many metadata columns.
    MetadataCountOverflow {
        /// Which metadata group overflowed the protocol u16 field.
        field: &'static str,
        /// Actual count.
        count: usize,
    },
    /// A binary signed-BIGINT row did not match its advertised width.
    RowColumnCount {
        /// Advertised metadata count.
        expected: usize,
        /// Values supplied by the caller.
        actual: usize,
    },
    /// A binary result column type is outside the bounded LONGLONG-or-string set.
    UnsupportedBinaryResultColumn {
        /// Zero-based metadata column index.
        column: usize,
        /// Advertised MySQL type code.
        type_code: u8,
    },
    /// A supplied cell's kind did not match its column's advertised type.
    MismatchedBinaryResultCell {
        /// Zero-based column index.
        column: usize,
        /// Advertised MySQL type code the cell did not fit.
        type_code: u8,
    },
}

impl std::fmt::Display for PreparedStatementError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Truncated {
                field,
                required,
                available,
            } => write!(
                formatter,
                "truncated prepared-statement {field}: need {required} bytes, have {available}"
            ),
            Self::InvalidField { field, value } => {
                write!(formatter, "invalid prepared-statement {field}: {value}")
            }
            Self::ZeroStatementId => formatter.write_str("prepared statement ID must be nonzero"),
            Self::UnsupportedCursorFlag(flag) => {
                write!(
                    formatter,
                    "unsupported prepared-statement cursor flag: {flag}"
                )
            }
            Self::MissingPreviousTypeVector => {
                formatter.write_str("prepared-statement type reuse has no prior type vector")
            }
            Self::PreviousTypeVectorLength { expected, actual } => write!(
                formatter,
                "prepared-statement prior type vector has {actual} types, expected {expected}"
            ),
            Self::BinaryParameter(error) => error.fmt(formatter),
            Self::TrailingBytes { bytes } => {
                write!(
                    formatter,
                    "prepared-statement packet has {bytes} trailing bytes"
                )
            }
            Self::MetadataCountOverflow { field, count } => write!(
                formatter,
                "prepared-statement {field} metadata count {count} exceeds u16"
            ),
            Self::RowColumnCount { expected, actual } => write!(
                formatter,
                "binary signed-BIGINT row has {actual} values, expected {expected}"
            ),
            Self::UnsupportedBinaryResultColumn { column, type_code } => write!(
                formatter,
                "binary result column {column} has unsupported type {type_code}"
            ),
            Self::MismatchedBinaryResultCell { column, type_code } => write!(
                formatter,
                "binary result cell {column} does not match column type {type_code}"
            ),
        }
    }
}

impl std::error::Error for PreparedStatementError {}

impl PreparedStatementError {
    /// Returns the dedicated TiDB errno carried by this error, when present.
    /// Plain malformed-packet and protocol errors use the caller's generic
    /// boundary, just as Go's `writeError` does for non-`terror` errors.
    #[must_use]
    pub const fn mysql_error_code(&self) -> Option<u16> {
        match self {
            Self::BinaryParameter(error) => error.mysql_error_code(),
            _ => None,
        }
    }
}

impl From<crate::BinaryParamError> for PreparedStatementError {
    fn from(error: crate::BinaryParamError) -> Self {
        Self::BinaryParameter(error)
    }
}

/// The header, bitmap, remembered types, and remaining value bytes of one
/// `COM_STMT_EXECUTE` packet.
///
/// Splitting the packet before interpreting values is load-bearing: Go saves
/// a newly supplied type vector on the statement before `parseBinaryParams`
/// can fail, so a later type-reuse execute observes the same vector.
pub struct PreparedStatementExecutePacket<'a> {
    statement_id: u32,
    cursor_flags: u8,
    parameter_types: PreparedParameterTypes,
    types: Vec<PreparedParameterType>,
    null_bitmap: Vec<u8>,
    parameter_values: &'a [u8],
}

impl PreparedStatementExecutePacket<'_> {
    /// Returns the statement handle carried by the packet.
    #[must_use]
    pub const fn statement_id(&self) -> u32 {
        self.statement_id
    }

    /// Returns whether this packet supplied a new remembered type vector.
    #[must_use]
    pub const fn parameter_types(&self) -> &PreparedParameterTypes {
        &self.parameter_types
    }

    /// Splits and interprets this packet's raw values through `BinaryParam`.
    pub fn decode(
        self,
        bound_params: &[Option<Vec<u8>>],
        input_charset: &str,
    ) -> Result<PreparedStatementExecute, PreparedStatementError> {
        let mut encoded_types = Vec::with_capacity(self.types.len() * 2);
        for parameter_type in &self.types {
            encoded_types.push(parameter_type.type_code());
            encoded_types.push(parameter_type.wire_flags());
        }
        let bound_params: Vec<Option<&[u8]>> =
            bound_params.iter().map(|bound| bound.as_deref()).collect();
        let values = crate::parse_binary_params(
            self.types.len(),
            &bound_params,
            &self.null_bitmap,
            &encoded_types,
            self.parameter_values,
            input_charset,
        )?
        .into_iter()
        .map(prepared_value_from_binary_param)
        .collect::<Result<Vec<_>, _>>()?;
        Ok(PreparedStatementExecute {
            statement_id: self.statement_id,
            cursor_flags: self.cursor_flags,
            parameter_types: self.parameter_types,
            values,
        })
    }
}

/// Splits the connection-owned part of one `COM_STMT_EXECUTE` packet.
///
/// The iteration count, unused NULL-bitmap padding, unrecognized type-flag
/// bits, and bytes after the declared values are ignored because Go ignores
/// them. Only the forward-only read cursor bit is observed; the two cursor
/// modes Go explicitly rejects remain errors.
pub fn split_prepared_statement_execute<'a>(
    payload: &'a [u8],
    parameter_count: usize,
    previous_types: Option<&[PreparedParameterType]>,
) -> Result<PreparedStatementExecutePacket<'a>, PreparedStatementError> {
    let mut cursor = PacketCursor::new(payload);
    let statement_id = cursor.read_u32("statement ID")?;
    if statement_id == 0 {
        return Err(PreparedStatementError::ZeroStatementId);
    }
    let cursor_flags = cursor.read_u8("cursor flags")?;
    if cursor_flags & 0x06 != 0 {
        return Err(PreparedStatementError::UnsupportedCursorFlag(cursor_flags));
    }
    let _iteration_count = cursor.read_u32("iteration count")?;
    let null_bitmap = cursor
        .read_exact(parameter_count.div_ceil(8), "null bitmap")?
        .to_vec();

    if parameter_count == 0 {
        return Ok(PreparedStatementExecutePacket {
            statement_id,
            cursor_flags,
            parameter_types: PreparedParameterTypes::New(Vec::new()),
            types: Vec::new(),
            null_bitmap,
            parameter_values: cursor.remaining_bytes(),
        });
    }

    let new_types = cursor.read_u8("new parameter bound flag")?;
    let (parameter_types, types) = if new_types == 1 {
        let encoded = cursor.read_exact(parameter_count * 2, "parameter type vector")?;
        let types: Vec<_> = encoded
            .chunks_exact(2)
            .map(|bytes| PreparedParameterType::new(bytes[0], bytes[1] & MYSQL_UNSIGNED_FLAG != 0))
            .collect();
        (PreparedParameterTypes::New(types.clone()), types)
    } else {
        let types = previous_types.ok_or(PreparedStatementError::MissingPreviousTypeVector)?;
        if types.len() != parameter_count {
            return Err(PreparedStatementError::PreviousTypeVectorLength {
                expected: parameter_count,
                actual: types.len(),
            });
        }
        (PreparedParameterTypes::Reuse, types.to_vec())
    };

    Ok(PreparedStatementExecutePacket {
        statement_id,
        cursor_flags,
        parameter_types,
        types,
        null_bitmap,
        parameter_values: cursor.remaining_bytes(),
    })
}

/// Decodes one execute packet using the default UTF-8 client charset.
pub fn decode_prepared_statement_execute(
    payload: &[u8],
    parameter_count: usize,
    previous_types: Option<&[PreparedParameterType]>,
) -> Result<PreparedStatementExecute, PreparedStatementError> {
    decode_prepared_statement_execute_with_bound_params(
        payload,
        parameter_count,
        previous_types,
        &[],
    )
}

/// Decodes one execute packet with `COM_STMT_SEND_LONG_DATA` buffers.
pub fn decode_prepared_statement_execute_with_bound_params(
    payload: &[u8],
    parameter_count: usize,
    previous_types: Option<&[PreparedParameterType]>,
    bound_params: &[Option<Vec<u8>>],
) -> Result<PreparedStatementExecute, PreparedStatementError> {
    split_prepared_statement_execute(payload, parameter_count, previous_types)?
        .decode(bound_params, "utf8mb4")
}

fn prepared_value_from_binary_param(
    parameter: crate::BinaryParam,
) -> Result<PreparedValue, PreparedStatementError> {
    if parameter.tp == crate::TYPE_NULL {
        return Ok(PreparedValue::Null);
    }
    if parameter.is_null {
        // Go's blob arm constructs a bytes datum from nil, whose observable
        // value is an empty byte string. String-like and decimal NULL markers
        // remain SQL NULL at the typed boundary.
        return Ok(
            if matches!(
                parameter.tp,
                TYPE_BLOB | TYPE_TINY_BLOB | TYPE_MEDIUM_BLOB | TYPE_LONG_BLOB
            ) {
                PreparedValue::String(Vec::new())
            } else {
                PreparedValue::Null
            },
        );
    }
    let value = match parameter.tp {
        TYPE_TINY if parameter.is_unsigned => {
            PreparedValue::UnsignedLongLong(u64::from(parameter.val[0]))
        }
        TYPE_TINY => PreparedValue::SignedLongLong(i64::from(parameter.val[0] as i8)),
        TYPE_SHORT | TYPE_YEAR if parameter.is_unsigned => {
            PreparedValue::UnsignedLongLong(u64::from(u16::from_le_bytes(
                parameter.val[..2].try_into().expect("short width"),
            )))
        }
        TYPE_SHORT | TYPE_YEAR => PreparedValue::SignedLongLong(i64::from(i16::from_le_bytes(
            parameter.val[..2].try_into().expect("short width"),
        ))),
        TYPE_INT24 | TYPE_LONG if parameter.is_unsigned => {
            PreparedValue::UnsignedLongLong(u64::from(u32::from_le_bytes(
                parameter.val[..4].try_into().expect("long width"),
            )))
        }
        TYPE_INT24 | TYPE_LONG => PreparedValue::SignedLongLong(i64::from(i32::from_le_bytes(
            parameter.val[..4].try_into().expect("long width"),
        ))),
        TYPE_LONGLONG if parameter.is_unsigned => PreparedValue::UnsignedLongLong(
            u64::from_le_bytes(parameter.val[..8].try_into().expect("longlong width")),
        ),
        TYPE_LONGLONG => PreparedValue::SignedLongLong(i64::from_le_bytes(
            parameter.val[..8].try_into().expect("longlong width"),
        )),
        TYPE_FLOAT => PreparedValue::Float(f32::from_bits(u32::from_le_bytes(
            parameter.val[..4].try_into().expect("float width"),
        ))),
        TYPE_DOUBLE => PreparedValue::Double(f64::from_bits(u64::from_le_bytes(
            parameter.val[..8].try_into().expect("double width"),
        ))),
        TYPE_DATE | TYPE_DATETIME | TYPE_TIMESTAMP => {
            PreparedValue::Temporal(render_binary_datetime(&parameter.val)?)
        }
        TYPE_DURATION => PreparedValue::Temporal(render_binary_duration(&parameter.val)?),
        TYPE_NEW_DECIMAL => PreparedValue::Decimal(parameter.val),
        TYPE_BLOB | TYPE_TINY_BLOB | TYPE_MEDIUM_BLOB | TYPE_LONG_BLOB | TYPE_UNSPECIFIED
        | TYPE_VARCHAR | TYPE_VAR_STRING | TYPE_STRING | TYPE_ENUM | TYPE_SET | TYPE_GEOMETRY
        | TYPE_BIT => PreparedValue::String(parameter.val),
        type_code => {
            return Err(crate::BinaryParamError::UnknownFieldType { type_code }.into());
        }
    };
    Ok(value)
}

/// `CURSOR_TYPE_READ_ONLY`: the one cursor kind Go supports.
pub const CURSOR_TYPE_READ_ONLY: u8 = 0x01;

/// Maximum row count accepted from one `COM_STMT_FETCH` request.
///
/// Source: `pkg/server/internal/parse/parse.go::maxFetchSize`.
pub const MAX_STMT_FETCH_SIZE: u32 = 1024;

/// Decodes the eight-byte `COM_STMT_FETCH` payload: the statement id and the
/// requested row count. Go's `parse.StmtFetchCmd` rejects any other length as
/// a malformed packet.
pub fn decode_prepared_statement_fetch(
    payload: &[u8],
) -> Result<(u32, u32), PreparedStatementError> {
    if payload.len() != 8 {
        return Err(PreparedStatementError::Truncated {
            field: "COM_STMT_FETCH payload",
            required: 8,
            available: payload.len(),
        });
    }
    let statement_id = u32::from_le_bytes(payload[0..4].try_into().expect("four bytes"));
    let fetch_size =
        u32::from_le_bytes(payload[4..8].try_into().expect("four bytes")).min(MAX_STMT_FETCH_SIZE);
    Ok((statement_id, fetch_size))
}

/// One decoded `COM_STMT_SEND_LONG_DATA` command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedStatementSendLongData {
    /// The prepared statement the chunk belongs to.
    pub statement_id: u32,
    /// The zero-based parameter the chunk is appended to.
    pub parameter_id: u16,
    /// The chunk itself: every byte after the six-byte header, which may be
    /// empty (Go stores an empty buffer to distinguish "bound to nothing"
    /// from "not bound").
    pub chunk: Vec<u8>,
}

/// Decodes a `COM_STMT_SEND_LONG_DATA` payload.
///
/// Go `handleStmtSendLongData` (`pkg/server/conn_stmt.go:610-625`): fewer than
/// six bytes is `mysql.ErrMalformPacket`; the first four are the statement ID
/// and the next two the parameter ID, both little-endian; everything after is
/// the payload, appended verbatim. No length field and no terminator -- the
/// packet boundary is the chunk boundary.
pub fn decode_prepared_statement_send_long_data(
    payload: &[u8],
) -> Result<PreparedStatementSendLongData, PreparedStatementError> {
    if payload.len() < 6 {
        return Err(PreparedStatementError::Truncated {
            field: "COM_STMT_SEND_LONG_DATA payload",
            required: 6,
            available: payload.len(),
        });
    }
    Ok(PreparedStatementSendLongData {
        statement_id: u32::from_le_bytes(payload[0..4].try_into().expect("four bytes")),
        parameter_id: u16::from_le_bytes(payload[4..6].try_into().expect("two bytes")),
        chunk: payload[6..].to_vec(),
    })
}

/// Decodes the four-byte `COM_STMT_CLOSE` payload.
///
/// A successful decode intentionally returns no response payload: MySQL close
/// is silent, and the server registry alone owns idempotent handle removal.
pub fn decode_prepared_statement_close(payload: &[u8]) -> Result<u32, PreparedStatementError> {
    let mut cursor = PacketCursor::new(payload);
    let statement_id = cursor.read_u32("statement ID")?;
    if statement_id == 0 {
        return Err(PreparedStatementError::ZeroStatementId);
    }
    if cursor.remaining() != 0 {
        return Err(PreparedStatementError::TrailingBytes {
            bytes: cursor.remaining(),
        });
    }
    Ok(statement_id)
}

/// Encodes a `COM_STMT_PREPARE` success response and its metadata packets.
///
/// The returned values are unframed packet payloads in wire order: success,
/// parameter definitions, optional parameter EOF, result definitions, and
/// optional result EOF. Packet sequence numbers remain the connection's job.
pub fn encode_prepared_statement_prepare_response(
    statement_id: u32,
    parameter_columns: &[ColumnInfo],
    result_columns: &[ColumnInfo],
    options: ResultSetOptions,
) -> Result<Vec<Vec<u8>>, PreparedStatementError> {
    if statement_id == 0 {
        return Err(PreparedStatementError::ZeroStatementId);
    }
    let parameter_count = u16::try_from(parameter_columns.len()).map_err(|_| {
        PreparedStatementError::MetadataCountOverflow {
            field: "parameter",
            count: parameter_columns.len(),
        }
    })?;
    let result_count = u16::try_from(result_columns.len()).map_err(|_| {
        PreparedStatementError::MetadataCountOverflow {
            field: "result",
            count: result_columns.len(),
        }
    })?;

    let mut packets = Vec::with_capacity(
        1 + parameter_columns.len()
            + result_columns.len()
            + usize::from(!options.deprecate_eof) * 2,
    );
    packets.push(prepare_ok_payload(
        statement_id,
        result_count,
        parameter_count,
    ));
    append_metadata_packets(&mut packets, parameter_columns, options);
    append_metadata_packets(&mut packets, result_columns, options);
    Ok(packets)
}

/// Encodes one binary result-row payload containing signed BIGINT cells.
///
/// The leading zero byte is the binary-row header, followed by the MySQL
/// result null bitmap (offset by two reserved bits) and little-endian cells.
pub fn encode_binary_signed_longlong_row(values: &[i64]) -> Vec<u8> {
    let null_bitmap_len = (values.len() + 7 + 2) / 8;
    let mut encoded = Vec::with_capacity(1 + null_bitmap_len + values.len() * 8);
    encoded.push(0);
    encoded.resize(1 + null_bitmap_len, 0);
    for value in values {
        encoded.extend_from_slice(&value.to_le_bytes());
    }
    encoded
}

/// Encodes a `TIME`/`Duration` value as the MySQL binary wire form.
///
/// Ported whole from TiDB's `dump.BinaryTime`
/// (`pkg/server/internal/dump/dump.go`), which takes a signed nanosecond
/// duration: zero is a single `0` length byte; otherwise a 12- or 8-byte body
/// with a sign flag, whole days/hours/minutes/seconds, and a little-endian u32
/// of the sub-second microseconds (dropped, with the length shortened to 8,
/// when there is no fractional part).
#[must_use]
pub fn encode_binary_time(nanoseconds: i64) -> Vec<u8> {
    const NS_PER_MICRO: i64 = 1_000;
    const NS_PER_SECOND: i64 = 1_000_000_000;
    const NS_PER_MINUTE: i64 = 60 * NS_PER_SECOND;
    const NS_PER_HOUR: i64 = 60 * NS_PER_MINUTE;
    const NS_PER_DAY: i64 = 24 * NS_PER_HOUR;

    if nanoseconds == 0 {
        return vec![0];
    }
    let mut data = vec![0u8; 13];
    data[0] = 12;
    let mut remaining = nanoseconds;
    if remaining < 0 {
        data[1] = 1;
        remaining = remaining.wrapping_neg();
    }
    let days = remaining / NS_PER_DAY;
    remaining -= days * NS_PER_DAY;
    data[2] = days as u8;
    let hours = remaining / NS_PER_HOUR;
    remaining -= hours * NS_PER_HOUR;
    data[6] = hours as u8;
    let minutes = remaining / NS_PER_MINUTE;
    remaining -= minutes * NS_PER_MINUTE;
    data[7] = minutes as u8;
    let seconds = remaining / NS_PER_SECOND;
    remaining -= seconds * NS_PER_SECOND;
    data[8] = seconds as u8;
    if remaining == 0 {
        data[0] = 8;
        data.truncate(9);
        return data;
    }
    let micros = (remaining / NS_PER_MICRO) as u32;
    data[9..13].copy_from_slice(&micros.to_le_bytes());
    data
}

/// The temporal field type that selects `dump.BinaryDateTime`'s output shape.
///
/// Go switches on `t.Type()`: `DATETIME` and `TIMESTAMP` render the time and
/// microsecond components, while `DATE` renders only the calendar date and
/// discards any time bits. The value's calendar fields and precision travel in
/// [`PackedTime`]; only this field-type discriminant is missing from the packed
/// payload (exactly as Go keeps it in the column schema, not the value).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BinaryDateTimeType {
    /// `MYSQL_TYPE_DATE`: only `YYYY-MM-DD` is emitted; time bits are ignored.
    Date,
    /// `MYSQL_TYPE_DATETIME`: full date, time, and microseconds.
    Datetime,
    /// `MYSQL_TYPE_TIMESTAMP`: byte-identical to `Datetime` on the wire.
    Timestamp,
}

/// Encodes a `DATE`/`DATETIME`/`TIMESTAMP` value as the MySQL binary wire form.
///
/// Ported whole from TiDB's `dump.BinaryDateTime`
/// (`pkg/server/internal/dump/dump.go`). A `DATETIME`/`TIMESTAMP` emits the
/// shortest faithful body: a single `0` length byte when zero; an 11-byte body
/// (little-endian `u16` year, then month/day/hour/minute/second, then a
/// little-endian `u32` of microseconds) when microseconds are present; a 7-byte
/// body through the seconds when any of hour/minute/second is set; otherwise a
/// 4-byte date-only body. A `DATE` emits `0` when zero and the 4-byte date-only
/// body otherwise, ignoring every time field exactly as Go does.
#[must_use]
pub fn encode_binary_datetime(time: PackedTime, kind: BinaryDateTimeType) -> Vec<u8> {
    // Date renders date-only regardless of any time bits, so the two field-type
    // groups collapse to "may the time be emitted?" — the zero and date-only
    // shapes are shared, matching Go's two switch arms.
    let parts = time.parts();
    if time.is_zero() {
        return vec![0];
    }
    let date_only = |length: u8| {
        let mut data = Vec::with_capacity(5);
        data.push(length);
        data.extend_from_slice(&parts.year.to_le_bytes());
        data.extend_from_slice(&[parts.month, parts.day]);
        data
    };
    match kind {
        BinaryDateTimeType::Date => date_only(4),
        BinaryDateTimeType::Datetime | BinaryDateTimeType::Timestamp => {
            if parts.microsecond != 0 {
                let mut data = Vec::with_capacity(12);
                data.push(11);
                data.extend_from_slice(&parts.year.to_le_bytes());
                data.extend_from_slice(&[
                    parts.month,
                    parts.day,
                    parts.hour,
                    parts.minute,
                    parts.second,
                ]);
                data.extend_from_slice(&parts.microsecond.to_le_bytes());
                data
            } else if parts.hour != 0 || parts.minute != 0 || parts.second != 0 {
                let mut data = Vec::with_capacity(8);
                data.push(7);
                data.extend_from_slice(&parts.year.to_le_bytes());
                data.extend_from_slice(&[
                    parts.month,
                    parts.day,
                    parts.hour,
                    parts.minute,
                    parts.second,
                ]);
                data
            } else {
                date_only(4)
            }
        }
    }
}

/// One non-null cell of a binary result row.
///
/// The variants mirror TiDB's `DumpBinaryRow` numeric and string cases
/// (`pkg/server/internal/column/column.go`): each integer width matches the
/// `dump.Uint*` the matching column type uses, a `Float`/`Double` matches the
/// IEEE-754 bit dump, and a `String` is a length-encoded byte string. SQL `NULL`
/// is [`Self::Null`], which writes no value bytes and instead sets its column's
/// null-bitmap bit — the case a nullable aggregate such as `SUM` over an empty
/// group produces.
///
/// Every arm of Go's switch is represented. `Enum`/`Set`/`JSON`/
/// `TiDBVectorFloat32` need no variant of their own: Go stringifies each and
/// then takes the same `dump.LengthEncodedString` exit as the string group, so
/// they arrive here as [`Self::String`] already stringified.
#[derive(Clone, Debug, PartialEq)]
pub enum BinaryResultCell {
    /// SQL `NULL` → no value bytes; the row's null bitmap marks this column.
    Null,
    /// `TypeTiny` → one byte (`byte(GetInt64)`).
    Tiny(i64),
    /// `TypeShort`/`TypeYear` → two little-endian bytes (`dump.Uint16`).
    Short(i64),
    /// `TypeInt24`/`TypeLong` → four little-endian bytes (`dump.Uint32`).
    Long(i64),
    /// `TypeLonglong` → eight little-endian bytes (`dump.Uint64`).
    LongLong(i64),
    /// `TypeFloat` → four little-endian bytes of the float32 bit pattern.
    Float(f32),
    /// `TypeDouble` → eight little-endian bytes of the float64 bit pattern.
    Double(f64),
    /// `TypeNewDecimal` → the length-encoded string of `MyDecimal.String()`.
    /// Unlike the string group, Go dumps this without `EncodeData` (a decimal is
    /// ASCII), so the canonical string is length-encoded directly.
    NewDecimal(Decimal),
    /// A string/blob cell carrying its raw stored bytes.
    String(Vec<u8>),
    /// `TypeDate`/`TypeDatetime`/`TypeTimestamp` → `dump.BinaryDateTime`.
    ///
    /// The discriminant is the *value's* `types.Time.Type()`, which is what Go
    /// switches on inside `BinaryDateTime`; the column type only decides that
    /// this encoder runs.
    Datetime(PackedTime, BinaryDateTimeType),
    /// `TypeDuration` → `dump.BinaryTime` over the signed nanosecond span
    /// (Go's `row.GetDuration(i, 0).Duration`).
    Duration(i64),
}

/// Encodes one binary result row from typed cells.
///
/// Ported from TiDB's `DumpBinaryRow`
/// (`pkg/server/internal/column/column.go`): the `mysql.OKHeader` byte, then a
/// null bitmap of `(len + 7 + 2) / 8` bytes whose first two bits are reserved
/// (so a value at column `i` occupies bit `i + 2`), then each cell in column
/// order — a `TypeLonglong` as `dump.Uint64` (eight little-endian bytes) and a
/// string type as `dump.LengthEncodedString`.
///
/// Scope note from the Go source: `DumpBinaryRow` writes the string case as
/// `dump.LengthEncodedString(d.EncodeData(row.GetBytes(i)))`, re-encoding the
/// value into the *result* charset. This encoder passes the raw bytes through,
/// which equals `EncodeData` only when the result charset matches the column
/// charset — the configured node's fixed `utf8mb4` case. A differing client
/// charset is out of scope here and must re-encode before calling this.
#[must_use]
pub fn encode_binary_result_row(cells: &[BinaryResultCell]) -> Vec<u8> {
    let null_bitmap_len = (cells.len() + 7 + 2) / 8;
    let mut encoded = Vec::with_capacity(1 + null_bitmap_len + cells.len() * 8);
    encoded.push(0);
    encoded.resize(1 + null_bitmap_len, 0);
    for (index, cell) in cells.iter().enumerate() {
        match cell {
            // A NULL writes no value bytes; instead its column's bit is set in
            // the null bitmap. Go `DumpBinaryRow` reserves the first two bits, so
            // column `index` occupies bit `index + 2` (byte `(index + 2) / 8`).
            BinaryResultCell::Null => {
                let bit = index + 2;
                encoded[1 + bit / 8] |= 1 << (bit % 8);
            }
            // Each width reinterprets the value's low bytes exactly as the
            // matching `dump.Uint*` does over `GetInt64`/`GetUint64`.
            BinaryResultCell::Tiny(value) => encoded.push(*value as u8),
            BinaryResultCell::Short(value) => {
                encoded.extend_from_slice(&(*value as u16).to_le_bytes());
            }
            BinaryResultCell::Long(value) => {
                encoded.extend_from_slice(&(*value as u32).to_le_bytes());
            }
            BinaryResultCell::LongLong(value) => {
                encoded.extend_from_slice(&(*value as u64).to_le_bytes());
            }
            BinaryResultCell::Float(value) => {
                encoded.extend_from_slice(&value.to_bits().to_le_bytes());
            }
            BinaryResultCell::Double(value) => {
                encoded.extend_from_slice(&value.to_bits().to_le_bytes());
            }
            // `dump.LengthEncodedString(hack.Slice(row.GetMyDecimal(i).String()))`:
            // stringify at dump time exactly as Go does, then length-encode.
            BinaryResultCell::NewDecimal(value) => {
                append_length_encoded_bytes(&mut encoded, Some(value.to_string().as_bytes()));
            }
            BinaryResultCell::String(bytes) => {
                append_length_encoded_bytes(&mut encoded, Some(bytes));
            }
            // `dump.BinaryDateTime(buffer, row.GetTime(i))` and
            // `append(buffer, dump.BinaryTime(row.GetDuration(i, 0).Duration)...)`:
            // both write a length-prefixed body of their own, not a
            // length-encoded string, so the bytes append verbatim.
            BinaryResultCell::Datetime(packed, kind) => {
                encoded.extend_from_slice(&encode_binary_datetime(*packed, *kind));
            }
            BinaryResultCell::Duration(nanoseconds) => {
                encoded.extend_from_slice(&encode_binary_time(*nanoseconds));
            }
        }
    }
    encoded
}

/// Whether a result column type is dumped as a length-encoded string by TiDB's
/// `DumpBinaryRow`.
///
/// Go writes five separate switch arms here -- the `TypeString`/`TypeBit`/blob
/// group, `TypeEnum`, `TypeSet`, `TypeJSON`, and `TypeTiDBVectorFloat32` -- but
/// every one of them ends in the same `dump.LengthEncodedString(d.EncodeData(..))`
/// call. They differ only in how the value is stringified before the dump (a
/// `GetBytes` versus an `Enum.String()`/`Set.String()`/`JSON.String()`) and in
/// which collation `EncodeData` is told to use, and both of those are the
/// caller's job: the cell already carries finished bytes. So the arms collapse
/// into one predicate rather than five.
#[must_use]
pub const fn is_binary_string_result_type(type_code: u8) -> bool {
    matches!(
        type_code,
        TYPE_STRING
            | TYPE_VAR_STRING
            | TYPE_VARCHAR
            | TYPE_BIT
            | TYPE_TINY_BLOB
            | TYPE_MEDIUM_BLOB
            | TYPE_LONG_BLOB
            | TYPE_BLOB
            | TYPE_ENUM
            | TYPE_SET
            | TYPE_JSON
            | TYPE_TIDB_VECTOR_FLOAT32
    )
}

/// Whether a result column type is one of `DumpBinaryRow`'s `dump.BinaryDateTime`
/// cases (`TypeDate`, `TypeDatetime`, `TypeTimestamp`).
///
/// The three share one arm because `BinaryDateTime` switches on the *value's*
/// `t.Type()`, not the column's -- the column type only decides that the
/// datetime encoder runs at all.
#[must_use]
pub const fn is_binary_datetime_result_type(type_code: u8) -> bool {
    matches!(type_code, TYPE_DATE | TYPE_DATETIME | TYPE_TIMESTAMP)
}

/// Whether a result column type is `DumpBinaryRow`'s `TypeDuration` case,
/// dumped through `dump.BinaryTime`.
#[must_use]
pub const fn is_binary_duration_result_type(type_code: u8) -> bool {
    type_code == TYPE_DURATION
}

/// Whether a result column type is one of `DumpBinaryRow`'s fixed-width signed
/// integer cases.
#[must_use]
pub const fn is_binary_integer_result_type(type_code: u8) -> bool {
    matches!(
        type_code,
        TYPE_TINY | TYPE_SHORT | TYPE_YEAR | TYPE_INT24 | TYPE_LONG | TYPE_LONGLONG
    )
}

/// Whether a result column type is a `DumpBinaryRow` IEEE-754 float case.
#[must_use]
pub const fn is_binary_float_result_type(type_code: u8) -> bool {
    matches!(type_code, TYPE_FLOAT | TYPE_DOUBLE)
}

/// Whether a result column type is `DumpBinaryRow`'s `TypeNewDecimal` case,
/// dumped as the length-encoded `MyDecimal.String()`.
#[must_use]
pub const fn is_binary_decimal_result_type(type_code: u8) -> bool {
    type_code == TYPE_NEW_DECIMAL
}

const fn cell_matches_result_type(cell: &BinaryResultCell, type_code: u8) -> bool {
    match cell {
        // A NULL carries no value bytes, so it is valid against any column type.
        BinaryResultCell::Null => true,
        BinaryResultCell::Tiny(_) => type_code == TYPE_TINY,
        BinaryResultCell::Short(_) => matches!(type_code, TYPE_SHORT | TYPE_YEAR),
        BinaryResultCell::Long(_) => matches!(type_code, TYPE_INT24 | TYPE_LONG),
        BinaryResultCell::LongLong(_) => type_code == TYPE_LONGLONG,
        BinaryResultCell::Float(_) => type_code == TYPE_FLOAT,
        BinaryResultCell::Double(_) => type_code == TYPE_DOUBLE,
        BinaryResultCell::NewDecimal(_) => is_binary_decimal_result_type(type_code),
        BinaryResultCell::String(_) => is_binary_string_result_type(type_code),
        // Go dumps by the VALUE's `t.Type()`, so a `DATETIME` value under a
        // `DATE` column still writes the datetime body. The column type only
        // gates which encoder runs, which is what this checks.
        BinaryResultCell::Datetime(_, _) => is_binary_datetime_result_type(type_code),
        BinaryResultCell::Duration(_) => is_binary_duration_result_type(type_code),
    }
}

/// Incremental binary result-set framing for the bounded column types.
#[derive(Debug)]
pub struct BinaryResultSetStream {
    columns: Vec<ColumnInfo>,
    options: ResultSetOptions,
    state: BinaryResultSetState,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BinaryResultSetState {
    Initial,
    Rows,
    Finished,
}

impl BinaryResultSetStream {
    /// Creates a binary stream after verifying every advertised result column is
    /// one `DumpBinaryRow` has an arm for.
    ///
    /// This is Go's `default: return nil, err.ErrInvalidType.GenWithStack(
    /// "invalid type %v", columns[i].Type)`, hoisted from the per-row dump to
    /// stream construction so an unencodable column is rejected before any byte
    /// of the result reaches the client.
    pub fn new(
        columns: Vec<ColumnInfo>,
        options: ResultSetOptions,
    ) -> Result<Self, PreparedStatementError> {
        for (column, metadata) in columns.iter().enumerate() {
            if !is_binary_integer_result_type(metadata.type_code)
                && !is_binary_float_result_type(metadata.type_code)
                && !is_binary_decimal_result_type(metadata.type_code)
                && !is_binary_string_result_type(metadata.type_code)
                && !is_binary_datetime_result_type(metadata.type_code)
                && !is_binary_duration_result_type(metadata.type_code)
            {
                return Err(PreparedStatementError::UnsupportedBinaryResultColumn {
                    column,
                    type_code: metadata.type_code,
                });
            }
        }
        Ok(Self {
            columns,
            options,
            state: BinaryResultSetState::Initial,
        })
    }

    /// Emits column count, result definitions, and legacy metadata EOF.
    pub fn metadata_packets(&mut self) -> Result<Vec<Vec<u8>>, PreparedStatementError> {
        if self.state != BinaryResultSetState::Initial {
            return Err(PreparedStatementError::InvalidField {
                field: "binary result-set state",
                value: self.state as u8,
            });
        }
        let mut packets = Vec::with_capacity(self.columns.len() + 2);
        let mut count = Vec::new();
        append_length_encoded_int(&mut count, self.columns.len() as u64);
        packets.push(count);
        for column in &self.columns {
            let mut payload = Vec::new();
            column.dump(&mut payload, &self.options.result_encoder);
            packets.push(payload);
        }
        if !self.options.deprecate_eof {
            packets.push(encode_eof_packet(&self.eof()));
        }
        self.state = BinaryResultSetState::Rows;
        Ok(packets)
    }

    /// Emits one binary row after checking each cell matches its column type.
    pub fn row_packet(
        &self,
        cells: &[BinaryResultCell],
    ) -> Result<Vec<u8>, PreparedStatementError> {
        if self.state != BinaryResultSetState::Rows {
            return Err(PreparedStatementError::InvalidField {
                field: "binary result-set state",
                value: self.state as u8,
            });
        }
        if cells.len() != self.columns.len() {
            return Err(PreparedStatementError::RowColumnCount {
                expected: self.columns.len(),
                actual: cells.len(),
            });
        }
        for (column, (cell, metadata)) in cells.iter().zip(&self.columns).enumerate() {
            if !cell_matches_result_type(cell, metadata.type_code) {
                return Err(PreparedStatementError::MismatchedBinaryResultCell {
                    column,
                    type_code: metadata.type_code,
                });
            }
        }
        Ok(encode_binary_result_row(cells))
    }

    /// Emits the terminal EOF exactly once.
    pub fn finish_packet(&mut self) -> Result<Vec<u8>, PreparedStatementError> {
        if self.state != BinaryResultSetState::Rows {
            return Err(PreparedStatementError::InvalidField {
                field: "binary result-set state",
                value: self.state as u8,
            });
        }
        self.state = BinaryResultSetState::Finished;
        Ok(encode_eof_packet(&self.eof()))
    }

    fn eof(&self) -> EofPacket {
        EofPacket {
            warnings: self.options.warnings,
            status_flags: self.options.status_flags,
            deprecate_eof: self.options.deprecate_eof,
            protocol_41: self.options.protocol_41,
            info: Vec::new(),
        }
    }
}

/// Go `binaryDate`/`binaryDateTime`/`binaryTimestamp`/`binaryTimestampWithTZ`:
/// the payload's own length picks how much of the date-time it carries, and
/// the text produced here is exactly what Go parses into its `Time` datum.
fn render_binary_datetime(payload: &[u8]) -> Result<String, PreparedStatementError> {
    let length = payload.len();
    // Go `types.ZeroDatetimeStr` for an empty payload.
    if length == 0 {
        return Ok("0000-00-00 00:00:00".to_owned());
    }
    if !matches!(length, 4 | 7 | 11 | 13) {
        return Err(PreparedStatementError::InvalidField {
            field: "temporal payload length",
            value: u8::try_from(length).unwrap_or(u8::MAX),
        });
    }
    let mut cursor = PacketCursor::new(payload);
    let year = cursor.read_u16("temporal year")?;
    let month = cursor.read_u8("temporal month")?;
    let day = cursor.read_u8("temporal day")?;
    let mut rendered = format!("{year:04}-{month:02}-{day:02}");
    if length >= 7 {
        let hour = cursor.read_u8("temporal hour")?;
        let minute = cursor.read_u8("temporal minute")?;
        let second = cursor.read_u8("temporal second")?;
        rendered.push_str(&format!(" {hour:02}:{minute:02}:{second:02}"));
    }
    if length >= 11 {
        let microseconds = cursor.read_u32("temporal microseconds")?;
        rendered.push_str(&format!(".{microseconds:06}"));
    }
    if length == 13 {
        // Go renders the zone shift as +HH:MM, with the minutes always
        // positive even when the shift itself is negative.
        let shift_minutes = cursor.read_i16("temporal zone shift")?;
        let shift_hours = shift_minutes / 60;
        let shift_abs_minutes = (shift_minutes % 60).abs();
        rendered.push_str(&format!("{shift_hours:+03}:{shift_abs_minutes:02}"));
    }
    Ok(rendered)
}

/// Go `binaryDuration`/`binaryDurationWithMS`: a signed day/time span, whose
/// payload length says whether microseconds follow.
fn render_binary_duration(payload: &[u8]) -> Result<String, PreparedStatementError> {
    let length = payload.len();
    if length == 0 {
        return Ok("0".to_owned());
    }
    if !matches!(length, 8 | 12) {
        return Err(PreparedStatementError::InvalidField {
            field: "duration payload length",
            value: u8::try_from(length).unwrap_or(u8::MAX),
        });
    }
    let mut cursor = PacketCursor::new(payload);
    let negative = cursor.read_u8("duration sign")?;
    if negative > 1 {
        return Err(PreparedStatementError::InvalidField {
            field: "duration sign",
            value: negative,
        });
    }
    let days = cursor.read_u32("duration days")?;
    let hours = cursor.read_u8("duration hours")?;
    let minutes = cursor.read_u8("duration minutes")?;
    let seconds = cursor.read_u8("duration seconds")?;
    let sign = if negative == 1 { "-" } else { "" };
    let mut rendered = format!("{sign}{days} {hours:02}:{minutes:02}:{seconds:02}");
    if length == 12 {
        let microseconds = cursor.read_u32("duration microseconds")?;
        rendered.push_str(&format!(".{microseconds:06}"));
    }
    Ok(rendered)
}

fn prepare_ok_payload(statement_id: u32, result_count: u16, parameter_count: u16) -> Vec<u8> {
    let mut payload = Vec::with_capacity(12);
    payload.push(0);
    payload.extend_from_slice(&statement_id.to_le_bytes());
    payload.extend_from_slice(&result_count.to_le_bytes());
    payload.extend_from_slice(&parameter_count.to_le_bytes());
    payload.push(0);
    payload.extend_from_slice(&0_u16.to_le_bytes());
    payload
}

fn append_metadata_packets(
    packets: &mut Vec<Vec<u8>>,
    columns: &[ColumnInfo],
    options: ResultSetOptions,
) {
    if columns.is_empty() {
        return;
    }
    for column in columns {
        let mut payload = Vec::new();
        column.dump(&mut payload, &options.result_encoder);
        packets.push(payload);
    }
    if !options.deprecate_eof {
        packets.push(encode_eof_packet(&EofPacket {
            warnings: options.warnings,
            status_flags: options.status_flags,
            deprecate_eof: options.deprecate_eof,
            protocol_41: options.protocol_41,
            info: Vec::new(),
        }));
    }
}

struct PacketCursor<'a> {
    remaining: &'a [u8],
}

impl<'a> PacketCursor<'a> {
    const fn new(payload: &'a [u8]) -> Self {
        Self { remaining: payload }
    }

    fn read_u8(&mut self, field: &'static str) -> Result<u8, PreparedStatementError> {
        Ok(self.read_exact(1, field)?[0])
    }

    fn read_u32(&mut self, field: &'static str) -> Result<u32, PreparedStatementError> {
        let bytes = self.read_exact(4, field)?;
        Ok(u32::from_le_bytes(
            bytes.try_into().expect("four-byte slice"),
        ))
    }

    fn read_u16(&mut self, field: &'static str) -> Result<u16, PreparedStatementError> {
        Ok(u16::from_le_bytes(
            self.read_exact(2, field)?
                .try_into()
                .expect("two bytes were read"),
        ))
    }

    fn read_i16(&mut self, field: &'static str) -> Result<i16, PreparedStatementError> {
        let bytes = self.read_exact(2, field)?;
        Ok(i16::from_le_bytes(
            bytes.try_into().expect("two-byte slice"),
        ))
    }

    fn read_exact(
        &mut self,
        length: usize,
        field: &'static str,
    ) -> Result<&'a [u8], PreparedStatementError> {
        if self.remaining.len() < length {
            return Err(PreparedStatementError::Truncated {
                field,
                required: length,
                available: self.remaining.len(),
            });
        }
        let (head, tail) = self.remaining.split_at(length);
        self.remaining = tail;
        Ok(head)
    }

    const fn remaining(&self) -> usize {
        self.remaining.len()
    }

    const fn remaining_bytes(&self) -> &'a [u8] {
        self.remaining
    }
}
