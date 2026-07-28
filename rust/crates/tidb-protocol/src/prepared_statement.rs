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

//! Bounded MySQL binary prepared-statement packet framing.
//!
//! This module owns the bounded execute/result wire shapes: non-NULL signed
//! integer parameters of any fixed width (`TYPE_TINY`/`SHORT`/`YEAR`/`INT24`/
//! `LONG`/`LONGLONG`, each sign-extended to `i64` as Go `ExecBinaryParam` does)
//! and the numeric/decimal/string result cells. Unsigned integers, the
//! string/decimal/temporal parameter families, statement IDs, SQL
//! parsing/binding, execution, and per-connection type-vector storage remain
//! server responsibilities or later slices of the wired param path.

use crate::{
    append_length_encoded_bytes, append_length_encoded_int, encode_eof_packet, ColumnInfo,
    EofPacket, ResultSetOptions, TYPE_DOUBLE, TYPE_FLOAT, TYPE_INT24, TYPE_LONG, TYPE_LONGLONG,
    TYPE_NEW_DECIMAL, TYPE_SHORT, TYPE_STRING, TYPE_TINY, TYPE_VARCHAR, TYPE_VAR_STRING, TYPE_YEAR,
};
use tidb_datatype::{Decimal, PackedTime};

/// MySQL binary-protocol type tag for a signed or unsigned 64-bit integer.
pub const MYSQL_TYPE_LONGLONG: u8 = 0x08;

/// MySQL binary-protocol parameter flag bit that marks an integer unsigned.
pub const MYSQL_UNSIGNED_FLAG: u8 = 0x80;

/// A signed integer parameter type admitted by the bounded prepared protocol.
///
/// The width mirrors Go `ExecBinaryParam`'s signed integer arms
/// (`pkg/expression/util.go`): each fixed-width MySQL integer type sign-extends
/// to `int64`. Unsigned integers and every non-integer type stay fail closed
/// (see the HANDOFF risk register); the string/decimal/temporal families are
/// the next slice of the wired param path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PreparedParameterType {
    /// A signed `MYSQL_TYPE_TINY` parameter (one byte, sign-extended).
    SignedTiny,
    /// A signed `MYSQL_TYPE_SHORT`/`YEAR` parameter (two little-endian bytes).
    SignedShort,
    /// A signed `MYSQL_TYPE_INT24`/`LONG` parameter (four little-endian bytes).
    SignedLong,
    /// A signed `MYSQL_TYPE_LONGLONG` parameter (eight little-endian bytes).
    SignedLongLong,
    /// A `MYSQL_TYPE_VARCHAR`/`VAR_STRING`/`STRING` parameter carried as a
    /// length-encoded string (`ExecBinaryParam`'s string arm; utf8 for this
    /// node).
    String,
    /// The unsigned counterpart of each fixed-width integer, which Go reads
    /// into a `uint64` datum rather than sign-extending.
    UnsignedTiny,
    /// An unsigned `MYSQL_TYPE_SHORT`/`YEAR` parameter.
    UnsignedShort,
    /// An unsigned `MYSQL_TYPE_INT24`/`LONG` parameter.
    UnsignedLong,
    /// An unsigned `MYSQL_TYPE_LONGLONG` parameter.
    UnsignedLongLong,
    /// A `MYSQL_TYPE_FLOAT` parameter (four little-endian bytes).
    Float,
    /// A `MYSQL_TYPE_DOUBLE` parameter (eight little-endian bytes).
    Double,
    /// A `MYSQL_TYPE_NEWDECIMAL`/`DECIMAL` parameter, which the protocol
    /// carries as a length-encoded string of digits.
    Decimal,
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
    /// The only accepted cursor flag is zero.
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
    /// This campaign owns exactly one prepared signed-BIGINT parameter.
    UnsupportedParameterCount {
        /// Parameter count supplied by the prepared statement registry.
        count: usize,
    },
    /// The packet requested a cursor, which the bounded protocol does not own.
    UnsupportedCursorFlag(u8),
    /// The iteration count differed from the only legal value, one.
    UnsupportedIterationCount(u32),
    /// A parameter was marked NULL.
    NullParameter {
        /// Zero-based parameter index.
        parameter: usize,
    },
    /// Padding bits outside the parameter null bitmap were nonzero.
    NonzeroNullBitmapPadding,
    /// A type-reuse execute arrived before the connection had saved a vector.
    MissingPreviousTypeVector,
    /// The saved type vector did not match the prepared parameter count.
    PreviousTypeVectorLength {
        /// Expected type count.
        expected: usize,
        /// Saved type count.
        actual: usize,
    },
    /// A parameter type tag is outside the bounded signed-BIGINT contract.
    UnsupportedParameterType {
        /// Zero-based parameter index.
        parameter: usize,
        /// MySQL type tag supplied by the client.
        type_code: u8,
    },
    /// A parameter's type flags requested unsigned integer semantics.
    UnsignedParameter {
        /// Zero-based parameter index.
        parameter: usize,
    },
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
            Self::UnsupportedParameterCount { count } => write!(
                formatter,
                "unsupported prepared-statement parameter count: {count}; expected at least one"
            ),
            Self::UnsupportedCursorFlag(flag) => {
                write!(
                    formatter,
                    "unsupported prepared-statement cursor flag: {flag}"
                )
            }
            Self::UnsupportedIterationCount(count) => write!(
                formatter,
                "unsupported prepared-statement iteration count: {count}"
            ),
            Self::NullParameter { parameter } => {
                write!(
                    formatter,
                    "prepared-statement parameter {parameter} is NULL"
                )
            }
            Self::NonzeroNullBitmapPadding => {
                formatter.write_str("prepared-statement null bitmap has nonzero padding")
            }
            Self::MissingPreviousTypeVector => {
                formatter.write_str("prepared-statement type reuse has no prior type vector")
            }
            Self::PreviousTypeVectorLength { expected, actual } => write!(
                formatter,
                "prepared-statement prior type vector has {actual} types, expected {expected}"
            ),
            Self::UnsupportedParameterType {
                parameter,
                type_code,
            } => write!(
                formatter,
                "unsupported prepared-statement parameter {parameter} type: {type_code}"
            ),
            Self::UnsignedParameter { parameter } => write!(
                formatter,
                "unsupported unsigned prepared-statement parameter {parameter}"
            ),
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

/// Decodes the exact Campaign 27 `COM_STMT_EXECUTE` payload.
///
/// `previous_types` is owned by the server's per-connection statement
/// registry. It is read only when the packet has `new_params_bound_flag = 0`;
/// the returned [`PreparedParameterTypes`] tells that registry whether to save
/// the vector from this packet.
pub fn decode_prepared_statement_execute(
    payload: &[u8],
    parameter_count: usize,
    previous_types: Option<&[PreparedParameterType]>,
) -> Result<PreparedStatementExecute, PreparedStatementError> {
    // The body below is already count-generic: the null bitmap, the type
    // vector, and the value loop are all driven by `parameter_count`. Only a
    // zero-marker execute is rejected, because every admitted template binds at
    // least one value.
    if parameter_count == 0 {
        return Err(PreparedStatementError::UnsupportedParameterCount {
            count: parameter_count,
        });
    }
    let mut cursor = PacketCursor::new(payload);
    let statement_id = cursor.read_u32("statement ID")?;
    if statement_id == 0 {
        return Err(PreparedStatementError::ZeroStatementId);
    }
    let cursor_flags = cursor.read_u8("cursor flags")?;
    if cursor_flags != 0 {
        return Err(PreparedStatementError::UnsupportedCursorFlag(cursor_flags));
    }
    let iteration_count = cursor.read_u32("iteration count")?;
    if iteration_count != 1 {
        return Err(PreparedStatementError::UnsupportedIterationCount(
            iteration_count,
        ));
    }

    let null_bitmap_len = parameter_count.div_ceil(8);
    let null_bitmap = cursor.read_exact(null_bitmap_len, "null bitmap")?.to_vec();
    // A bit set in the bitmap is Go's `mysql.TypeNull` arm: the value is NULL
    // and carries no bytes in the value section at all.
    let is_null: Vec<bool> = (0..parameter_count)
        .map(|parameter| null_bitmap[parameter / 8] & (1 << (parameter % 8)) != 0)
        .collect();
    if !parameter_count.is_multiple_of(8)
        && null_bitmap
            .last()
            .is_some_and(|last| *last >> (parameter_count % 8) != 0)
    {
        return Err(PreparedStatementError::NonzeroNullBitmapPadding);
    }

    let new_types = cursor.read_u8("new parameter bound flag")?;
    let (parameter_types, types): (PreparedParameterTypes, Vec<PreparedParameterType>) =
        match new_types {
            0 => {
                let types =
                    previous_types.ok_or(PreparedStatementError::MissingPreviousTypeVector)?;
                if types.len() != parameter_count {
                    return Err(PreparedStatementError::PreviousTypeVectorLength {
                        expected: parameter_count,
                        actual: types.len(),
                    });
                }
                (PreparedParameterTypes::Reuse, types.to_vec())
            }
            1 => {
                let encoded = cursor.read_exact(parameter_count * 2, "parameter type vector")?;
                let mut types = Vec::with_capacity(parameter_count);
                for (parameter, type_bytes) in encoded.chunks_exact(2).enumerate() {
                    let parameter_type =
                        decode_parameter_type(parameter, type_bytes[0], type_bytes[1])?;
                    types.push(parameter_type);
                }
                (PreparedParameterTypes::New(types.clone()), types)
            }
            value => {
                return Err(PreparedStatementError::InvalidField {
                    field: "new parameter bound flag",
                    value,
                });
            }
        };

    let mut values = Vec::with_capacity(types.len());
    for (parameter, parameter_type) in types.iter().enumerate() {
        // A NULL parameter occupies no bytes in the value section.
        if is_null.get(parameter).copied().unwrap_or(false) {
            values.push(PreparedValue::Null);
            continue;
        }
        // Each integer width sign-extends to one i64, exactly as Go
        // `ExecBinaryParam` widens int8/int16/int32/int64 with `int64(intN(...))`;
        // a string carries its length-encoded bytes.
        let value = match parameter_type {
            PreparedParameterType::SignedTiny => {
                PreparedValue::SignedLongLong(i64::from(cursor.read_i8("signed TINYINT value")?))
            }
            PreparedParameterType::SignedShort => {
                PreparedValue::SignedLongLong(i64::from(cursor.read_i16("signed SMALLINT value")?))
            }
            PreparedParameterType::SignedLong => {
                PreparedValue::SignedLongLong(i64::from(cursor.read_i32("signed INT value")?))
            }
            PreparedParameterType::SignedLongLong => {
                PreparedValue::SignedLongLong(cursor.read_i64("signed BIGINT value")?)
            }
            PreparedParameterType::String => {
                PreparedValue::String(cursor.read_length_encoded_string("string value")?)
            }
            PreparedParameterType::UnsignedTiny => PreparedValue::UnsignedLongLong(u64::from(
                cursor.read_u8("unsigned TINYINT value")?,
            )),
            PreparedParameterType::UnsignedShort => PreparedValue::UnsignedLongLong(u64::from(
                cursor.read_u16("unsigned SMALLINT value")?,
            )),
            PreparedParameterType::UnsignedLong => {
                PreparedValue::UnsignedLongLong(u64::from(cursor.read_u32("unsigned INT value")?))
            }
            PreparedParameterType::UnsignedLongLong => {
                PreparedValue::UnsignedLongLong(cursor.read_u64("unsigned BIGINT value")?)
            }
            PreparedParameterType::Float => {
                PreparedValue::Float(f32::from_bits(cursor.read_u32("FLOAT value")?))
            }
            PreparedParameterType::Double => {
                PreparedValue::Double(f64::from_bits(cursor.read_u64("DOUBLE value")?))
            }
            PreparedParameterType::Decimal => {
                PreparedValue::Decimal(cursor.read_length_encoded_string("DECIMAL value")?)
            }
        };
        values.push(value);
    }
    if cursor.remaining() != 0 {
        return Err(PreparedStatementError::TrailingBytes {
            bytes: cursor.remaining(),
        });
    }

    Ok(PreparedStatementExecute {
        statement_id,
        cursor_flags,
        parameter_types,
        values,
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
        remaining = -remaining;
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
///
/// This is the encoder alone; like [`encode_binary_time`] it is not yet wired
/// into [`BinaryResultCell`] (no temporal result cell / `Datum` time variant
/// exists), and that gap is tracked as partial-transcreation debt in HANDOFF.
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
/// Not yet represented (fail closed in the stream, see the risk register in
/// HANDOFF): temporal (`Date`/`Datetime`/`Timestamp` via `dump.BinaryDateTime`),
/// `Duration` (`dump.BinaryTime`), `Enum`/`Set`/`JSON`/`TiDBVectorFloat32`.
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
        }
    }
    encoded
}

/// Whether a result column type is dumped as a length-encoded string by TiDB's
/// `DumpBinaryRow` string group (the `CHAR`/`VARCHAR` subset this node projects;
/// the blob/bit members of that group are not produced here).
#[must_use]
pub const fn is_binary_string_result_type(type_code: u8) -> bool {
    matches!(type_code, TYPE_STRING | TYPE_VAR_STRING | TYPE_VARCHAR)
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
    /// one this path can dump: a `DumpBinaryRow` fixed-width integer, IEEE-754
    /// float, `NewDecimal`, or string type. Temporal and enum/set/json/vector
    /// are fail closed here — see the HANDOFF risk register — because their cell
    /// source (a temporal codec, an `EncodeData` charset re-encode) is not
    /// ported yet.
    pub fn new(
        columns: Vec<ColumnInfo>,
        options: ResultSetOptions,
    ) -> Result<Self, PreparedStatementError> {
        for (column, metadata) in columns.iter().enumerate() {
            if !is_binary_integer_result_type(metadata.type_code)
                && !is_binary_float_result_type(metadata.type_code)
                && !is_binary_decimal_result_type(metadata.type_code)
                && !is_binary_string_result_type(metadata.type_code)
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
            column.dump(&mut payload);
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

fn decode_parameter_type(
    parameter: usize,
    type_code: u8,
    type_flags: u8,
) -> Result<PreparedParameterType, PreparedStatementError> {
    // The unsigned bit is rejected first (Go reads only this flag bit); any
    // other nonzero flag byte is an unmodelled encoding for this bounded path.
    let unsigned = type_flags & MYSQL_UNSIGNED_FLAG != 0;
    if type_flags & !MYSQL_UNSIGNED_FLAG != 0 {
        return Err(PreparedStatementError::InvalidField {
            field: "parameter type flags",
            value: type_flags,
        });
    }
    // Go `ExecBinaryParam` reads the unsigned flag per integer width and
    // produces a uint64 datum; every other family ignores it.
    match type_code {
        TYPE_TINY if unsigned => Ok(PreparedParameterType::UnsignedTiny),
        TYPE_TINY => Ok(PreparedParameterType::SignedTiny),
        TYPE_SHORT | TYPE_YEAR if unsigned => Ok(PreparedParameterType::UnsignedShort),
        TYPE_SHORT | TYPE_YEAR => Ok(PreparedParameterType::SignedShort),
        TYPE_INT24 | TYPE_LONG if unsigned => Ok(PreparedParameterType::UnsignedLong),
        TYPE_INT24 | TYPE_LONG => Ok(PreparedParameterType::SignedLong),
        TYPE_LONGLONG if unsigned => Ok(PreparedParameterType::UnsignedLongLong),
        TYPE_LONGLONG => Ok(PreparedParameterType::SignedLongLong),
        TYPE_FLOAT => Ok(PreparedParameterType::Float),
        TYPE_DOUBLE => Ok(PreparedParameterType::Double),
        TYPE_NEW_DECIMAL => Ok(PreparedParameterType::Decimal),
        TYPE_VARCHAR | TYPE_VAR_STRING | TYPE_STRING => Ok(PreparedParameterType::String),
        _ => Err(PreparedStatementError::UnsupportedParameterType {
            parameter,
            type_code,
        }),
    }
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
        column.dump(&mut payload);
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

    fn read_i8(&mut self, field: &'static str) -> Result<i8, PreparedStatementError> {
        Ok(self.read_exact(1, field)?[0] as i8)
    }

    fn read_u16(&mut self, field: &'static str) -> Result<u16, PreparedStatementError> {
        Ok(u16::from_le_bytes(
            self.read_exact(2, field)?
                .try_into()
                .expect("two bytes were read"),
        ))
    }

    fn read_u64(&mut self, field: &'static str) -> Result<u64, PreparedStatementError> {
        Ok(u64::from_le_bytes(
            self.read_exact(8, field)?
                .try_into()
                .expect("eight bytes were read"),
        ))
    }

    fn read_i16(&mut self, field: &'static str) -> Result<i16, PreparedStatementError> {
        let bytes = self.read_exact(2, field)?;
        Ok(i16::from_le_bytes(
            bytes.try_into().expect("two-byte slice"),
        ))
    }

    fn read_i32(&mut self, field: &'static str) -> Result<i32, PreparedStatementError> {
        let bytes = self.read_exact(4, field)?;
        Ok(i32::from_le_bytes(
            bytes.try_into().expect("four-byte slice"),
        ))
    }

    fn read_i64(&mut self, field: &'static str) -> Result<i64, PreparedStatementError> {
        let bytes = self.read_exact(8, field)?;
        Ok(i64::from_le_bytes(
            bytes.try_into().expect("eight-byte slice"),
        ))
    }

    /// Reads a MySQL length-encoded string: a length-encoded integer header
    /// followed by that many bytes. A NULL marker (`0xfb`) cannot occur for a
    /// bitmap-non-null parameter, so it decodes as a zero-length string.
    fn read_length_encoded_string(
        &mut self,
        field: &'static str,
    ) -> Result<Vec<u8>, PreparedStatementError> {
        let (length, _is_null, consumed) = crate::parse_length_encoded_int(self.remaining).ok_or(
            PreparedStatementError::Truncated {
                field,
                required: 1,
                available: self.remaining.len(),
            },
        )?;
        // Advance past the length header, then take the payload bytes.
        self.read_exact(consumed, field)?;
        let length = usize::try_from(length).map_err(|_| PreparedStatementError::Truncated {
            field,
            required: usize::MAX,
            available: self.remaining.len(),
        })?;
        Ok(self.read_exact(length, field)?.to_vec())
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
}
