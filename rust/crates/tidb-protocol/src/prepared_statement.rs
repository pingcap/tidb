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
//! This module deliberately owns only the one Campaign 27 wire shape: one
//! non-NULL, signed `MYSQL_TYPE_LONGLONG` parameter and signed-BIGINT result
//! cells. Statement IDs, SQL parsing/binding, execution, and per-connection
//! type-vector storage remain server responsibilities.

use crate::{
    append_length_encoded_int, encode_eof_packet, ColumnInfo, EofPacket, ResultSetOptions,
    TYPE_LONGLONG,
};

/// MySQL binary-protocol type tag for a signed or unsigned 64-bit integer.
pub const MYSQL_TYPE_LONGLONG: u8 = 0x08;

/// MySQL binary-protocol parameter flag bit that marks an integer unsigned.
pub const MYSQL_UNSIGNED_FLAG: u8 = 0x80;

/// The only parameter type admitted by the bounded prepared-read protocol.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PreparedParameterType {
    /// A signed `MYSQL_TYPE_LONGLONG` parameter.
    SignedLongLong,
}

/// The only decoded prepared-statement value admitted by this protocol leaf.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PreparedValue {
    /// A signed 64-bit integer encoded little-endian in the execute packet.
    SignedLongLong(i64),
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
#[derive(Clone, Debug, Eq, PartialEq)]
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
    /// A binary signed-BIGINT row was requested for non-BIGINT metadata.
    UnsupportedBinaryResultColumn {
        /// Zero-based metadata column index.
        column: usize,
        /// Advertised MySQL type code.
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
                "unsupported prepared-statement parameter count: {count}; expected one"
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
                "binary signed-BIGINT row column {column} has unsupported type {type_code}"
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
    if parameter_count != 1 {
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
    let null_bitmap = cursor.read_exact(null_bitmap_len, "null bitmap")?;
    for parameter in 0..parameter_count {
        if null_bitmap[parameter / 8] & (1 << (parameter % 8)) != 0 {
            return Err(PreparedStatementError::NullParameter { parameter });
        }
    }
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
    for parameter_type in &types {
        match parameter_type {
            PreparedParameterType::SignedLongLong => {
                values.push(PreparedValue::SignedLongLong(
                    cursor.read_i64("signed BIGINT value")?,
                ));
            }
        }
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

/// Incremental binary result-set framing for signed-BIGINT rows.
#[derive(Debug)]
pub struct BinarySignedLongLongResultSetStream {
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

impl BinarySignedLongLongResultSetStream {
    /// Creates a binary stream after verifying every advertised result column
    /// is a signed-BIGINT column owned by this bounded protocol path.
    pub fn new(
        columns: Vec<ColumnInfo>,
        options: ResultSetOptions,
    ) -> Result<Self, PreparedStatementError> {
        for (column, metadata) in columns.iter().enumerate() {
            if metadata.type_code != TYPE_LONGLONG {
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

    /// Emits one signed-BIGINT binary row.
    pub fn row_packet(&self, values: &[i64]) -> Result<Vec<u8>, PreparedStatementError> {
        if self.state != BinaryResultSetState::Rows {
            return Err(PreparedStatementError::InvalidField {
                field: "binary result-set state",
                value: self.state as u8,
            });
        }
        if values.len() != self.columns.len() {
            return Err(PreparedStatementError::RowColumnCount {
                expected: self.columns.len(),
                actual: values.len(),
            });
        }
        Ok(encode_binary_signed_longlong_row(values))
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
    if type_code != MYSQL_TYPE_LONGLONG {
        return Err(PreparedStatementError::UnsupportedParameterType {
            parameter,
            type_code,
        });
    }
    if type_flags & MYSQL_UNSIGNED_FLAG != 0 {
        return Err(PreparedStatementError::UnsignedParameter { parameter });
    }
    if type_flags != 0 {
        return Err(PreparedStatementError::InvalidField {
            field: "parameter type flags",
            value: type_flags,
        });
    }
    Ok(PreparedParameterType::SignedLongLong)
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

    fn read_i64(&mut self, field: &'static str) -> Result<i64, PreparedStatementError> {
        let bytes = self.read_exact(8, field)?;
        Ok(i64::from_le_bytes(
            bytes.try_into().expect("eight-byte slice"),
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
}
