// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Incremental protocol result-set framing contracts.
//!
//! Unlike `encode_text_result_set`, this source-shaped writer never owns all
//! rows. Metadata, each row, and the terminal EOF are emitted independently so
//! a server can preserve `clientConn.writeChunks`' lazy pull/write ordering.

use crate::result_encoder::is_string_column_type;
use crate::textrow::{append_datum_text_owned, OwnedDatumText, TextColumn, TextFormatError};
use crate::{
    append_length_encoded_bytes, append_length_encoded_int, encode_eof_packet, encode_text_row,
    ColumnInfo, EofPacket, ResultSetOptions, NULL_MARKER,
};
use tidb_datatype::{Datum, MyDecimal};

/// Lifecycle of an incremental result-set payload stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResultSetStreamState {
    /// No metadata has escaped yet.
    Initial,
    /// Metadata has been emitted and rows may be appended.
    Rows,
    /// The terminal EOF has been emitted.
    Finished,
}

/// A protocol lifecycle or row-shape error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ResultSetStreamError {
    /// An operation was attempted in the wrong lifecycle state.
    InvalidState {
        /// Current stream state.
        state: ResultSetStreamState,
        /// Attempted operation.
        operation: &'static str,
    },
    /// A row did not match the advertised metadata width.
    RowColumnCount {
        /// Zero-based row index in this stream.
        row: usize,
        /// Advertised column count.
        expected: usize,
        /// Actual value count.
        actual: usize,
    },
    /// A datum could not be rendered according to its result column type.
    TextFormat {
        /// Zero-based row index in this stream.
        row: usize,
        /// Zero-based column index in the row.
        column: usize,
        /// Source-shaped text formatting failure.
        error: TextFormatError,
    },
}

impl ResultSetStreamError {
    /// The protocol error category Go's server derives from the same
    /// failure: an unrenderable datum is `err.ErrInvalidType` (8057,
    /// column.go:175/238); every other variant has no single source errno
    /// and stays `Unknown`.
    pub fn error_kind(&self) -> crate::ErrorKind {
        match self {
            Self::TextFormat { .. } => crate::ErrorKind::InvalidType,
            _ => crate::ErrorKind::Unknown,
        }
    }
}

impl std::fmt::Display for ResultSetStreamError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidState { state, operation } => {
                write!(
                    formatter,
                    "cannot {operation} result set in {state:?} state"
                )
            }
            Self::RowColumnCount {
                row,
                expected,
                actual,
            } => write!(
                formatter,
                "result row {row} has {actual} values, expected {expected}"
            ),
            Self::TextFormat { error, .. } => match error {
                TextFormatError::UnsupportedType(type_code) => {
                    write!(formatter, "invalid type {type_code}")
                }
                // Keep the server-visible error text identical to the prior
                // Datum formatter. Row/column remain structured fields for
                // diagnostics, but Go's source error itself carries no
                // positional prefix.
                _ => write!(formatter, "{error}"),
            },
        }
    }
}

impl std::error::Error for ResultSetStreamError {}

/// Incremental producer of unframed MySQL result-set packet payloads.
#[derive(Debug)]
pub struct ResultSetStream {
    columns: Vec<ColumnInfo>,
    options: ResultSetOptions,
    state: ResultSetStreamState,
    rows: usize,
}

/// Incremental text-row encoder used by chunk-backed result sources.
///
/// Go's `DumpTextRow` receives a `chunk.Row` and writes each cell into the
/// connection buffer while the row is still borrowed.  This builder exposes
/// the same ownership boundary to callers that can keep a decoded chunk alive:
/// byte cells are copied directly into the final packet and only a configured
/// result-charset conversion allocates an intermediate value.
pub struct TextRowWriter<'a> {
    stream: &'a mut ResultSetStream,
    payload: Vec<u8>,
    next_column: usize,
    row_index: usize,
}

impl ResultSetStream {
    /// Creates a stream whose metadata has not yet been emitted.
    #[must_use]
    pub fn new(columns: Vec<ColumnInfo>, options: ResultSetOptions) -> Self {
        Self {
            columns,
            options,
            state: ResultSetStreamState::Initial,
            rows: 0,
        }
    }

    /// Starts one borrowed text row. The returned writer must be finished
    /// before the next source batch is pulled so all `TextScalar` byte views
    /// remain valid for the duration of each append.
    pub fn text_row(&mut self) -> Result<TextRowWriter<'_>, ResultSetStreamError> {
        self.text_row_with_capacity(self.columns.len() * 16)
    }

    /// Starts one borrowed text row with a caller-provided payload capacity.
    /// Chunk-backed callers can use the source row's raw cell lengths to avoid
    /// repeated growth when a row contains a wide VARBINARY/BLOB value.
    pub fn text_row_with_capacity(
        &mut self,
        capacity: usize,
    ) -> Result<TextRowWriter<'_>, ResultSetStreamError> {
        if self.state != ResultSetStreamState::Rows {
            return Err(ResultSetStreamError::InvalidState {
                state: self.state,
                operation: "emit row for",
            });
        }
        Ok(TextRowWriter {
            payload: Vec::with_capacity(capacity),
            next_column: 0,
            row_index: self.rows,
            stream: self,
        })
    }

    /// Returns the current lifecycle state.
    #[must_use]
    pub const fn state(&self) -> ResultSetStreamState {
        self.state
    }

    /// Returns the number of successfully encoded row payloads.
    #[must_use]
    pub const fn row_count(&self) -> usize {
        self.rows
    }

    /// Emits column count, column definitions, and legacy metadata EOF.
    pub fn metadata_packets(&mut self) -> Result<Vec<Vec<u8>>, ResultSetStreamError> {
        if self.state != ResultSetStreamState::Initial {
            return Err(ResultSetStreamError::InvalidState {
                state: self.state,
                operation: "emit metadata for",
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
        self.state = ResultSetStreamState::Rows;
        Ok(packets)
    }

    /// Encodes one text row without retaining it.
    pub fn row_packet(&mut self, row: &[Option<Vec<u8>>]) -> Result<Vec<u8>, ResultSetStreamError> {
        if self.state != ResultSetStreamState::Rows {
            return Err(ResultSetStreamError::InvalidState {
                state: self.state,
                operation: "emit row for",
            });
        }
        if row.len() != self.columns.len() {
            return Err(ResultSetStreamError::RowColumnCount {
                row: self.rows,
                expected: self.columns.len(),
                actual: row.len(),
            });
        }
        // Go `FormatValueText`: a string-ish cell is written through the
        // connection's `@@character_set_results` policy, and every other type
        // is already ASCII text and goes out untouched. The DATA encoding is
        // reset per cell from that column's own charset (`UpdateDataEncoding`),
        // because `EncodeData` falls back to it for a binary column even when
        // the session asked for something else.
        let encoded = self.encode_row_data(row);
        let values = encoded.iter().map(Option::as_deref).collect::<Vec<_>>();
        let payload = encode_text_row(&values);
        self.rows += 1;
        Ok(payload)
    }

    /// Emits one text row by consuming its already-owned value bytes.
    ///
    /// Go's `DumpTextRow` appends each value directly into the connection's
    /// reusable packet buffer (`pkg/server/internal/column/column.go:162-177`)
    /// after `FormatValueText` returns its scratch-backed bytes.  The owned
    /// path keeps that source ordering while avoiding an intermediate clone
    /// for each Rust cell; charset rewriting and lifecycle checks remain the
    /// same as [`Self::row_packet`].
    pub fn row_packet_owned(
        &mut self,
        row: Vec<Option<Vec<u8>>>,
    ) -> Result<Vec<u8>, ResultSetStreamError> {
        if self.state != ResultSetStreamState::Rows {
            return Err(ResultSetStreamError::InvalidState {
                state: self.state,
                operation: "emit row for",
            });
        }
        if row.len() != self.columns.len() {
            return Err(ResultSetStreamError::RowColumnCount {
                row: self.rows,
                expected: self.columns.len(),
                actual: row.len(),
            });
        }
        // The owned cells already know their final byte lengths. Reserve the
        // complete row payload up front so appending 51-column wide rows does
        // not repeatedly grow and copy the packet buffer.
        let capacity = row
            .iter()
            .map(|value| match value {
                None => 1,
                Some(value) => value.len() + length_encoded_int_size(value.len()),
            })
            .sum();
        let mut payload = Vec::with_capacity(capacity);
        self.append_row_data_owned(&mut payload, row);
        self.rows += 1;
        Ok(payload)
    }

    /// Encodes one row directly from owned TiDB datums.
    ///
    /// Go's `DumpTextRow` writes numeric and temporal text into the connection
    /// buffer and transfers string/blob bytes through `ResultEncoder`. Keeping
    /// that ownership boundary here avoids a temporary `Vec` for every scalar
    /// cell while preserving the checked `format_datum_text` type matrix.
    pub fn row_packet_datums_owned(
        &mut self,
        row: Vec<Datum>,
    ) -> Result<Vec<u8>, ResultSetStreamError> {
        if self.state != ResultSetStreamState::Rows {
            return Err(ResultSetStreamError::InvalidState {
                state: self.state,
                operation: "emit row for",
            });
        }
        if row.len() != self.columns.len() {
            return Err(ResultSetStreamError::RowColumnCount {
                row: self.rows,
                expected: self.columns.len(),
                actual: row.len(),
            });
        }

        // The Go connection allocator grows one reusable row buffer. Estimate
        // the same final size from the owned datum payloads so a wide row does
        // not repeatedly reallocate while its text cells are appended.
        let capacity = row
            .iter()
            .map(datum_text_capacity)
            .fold(0usize, usize::saturating_add);
        let mut payload = Vec::with_capacity(capacity);
        for (column_index, (column, datum)) in self.columns.iter().zip(row).enumerate() {
            let text_column = TextColumn {
                type_code: column.type_code,
                flag: column.flag,
                decimal: column.decimal,
                table_is_empty: column.table.is_empty(),
            };
            let prefix_start = payload.len();
            // Go's `dump.LengthEncodedString` writes the one-byte prefix for
            // the common short-value case. Reserve that prefix here; only a
            // value longer than 250 bytes needs the slower prefix expansion.
            payload.push(0);
            let value_start = payload.len();
            let formatted =
                append_datum_text_owned(&mut payload, text_column, datum).map_err(|error| {
                    ResultSetStreamError::TextFormat {
                        row: self.rows,
                        column: column_index,
                        error,
                    }
                })?;
            match formatted {
                OwnedDatumText::Null => {
                    payload.truncate(prefix_start);
                    payload.push(NULL_MARKER);
                }
                OwnedDatumText::Plain => {
                    finish_cell_prefix(&mut payload, prefix_start, value_start);
                }
                OwnedDatumText::Bytes(value) => {
                    payload.truncate(prefix_start);
                    let value = encode_owned_cell(column, value, self.options.result_encoder);
                    append_length_encoded_bytes(&mut payload, Some(&value));
                }
            }
        }
        self.rows += 1;
        Ok(payload)
    }

    /// Applies `@@character_set_results` to the string cells of one row.
    fn encode_row_data(&self, row: &[Option<Vec<u8>>]) -> Vec<Option<Vec<u8>>> {
        let encoder = self.options.result_encoder;
        if encoder.result_charset().is_none() {
            // Go's `isNull` state: `EncodeData` returns the column charset's
            // bytes, which for every charset this tier serves is the input.
            return row.to_vec();
        }
        row.iter()
            .zip(&self.columns)
            .map(|(value, column)| {
                let value = value.as_ref()?;
                if !crate::result_encoder::is_string_column_type(column.type_code) {
                    return Some(value.clone());
                }
                let mut encoder = encoder;
                // Go treats JSON and VECTOR as utf8mb4 regardless of the
                // column's own (binary) collation.
                let collation = match column.type_code {
                    crate::column::TYPE_JSON | crate::column::TYPE_TIDB_VECTOR_FLOAT32 => {
                        crate::result_encoder::UTF8MB4_DEFAULT_COLLATION_ID
                    }
                    _ => column.charset,
                };
                if encoder.update_data_encoding(collation).is_err() {
                    return Some(value.clone());
                }
                Some(encoder.encode_data(value).unwrap_or_else(|_| value.clone()))
            })
            .collect()
    }

    /// Appends an owned row directly into its final text-protocol payload.
    ///
    /// This mirrors Go's `DumpTextRow` loop: `FormatValueText` produces one
    /// cell and `dump.LengthEncodedString` immediately appends it to the
    /// connection buffer (`pkg/server/internal/column/column.go:162-177`).
    /// Keeping the framing in this same loop avoids a second owned cell vector
    /// on the common result path.
    fn append_row_data_owned(&self, payload: &mut Vec<u8>, row: Vec<Option<Vec<u8>>>) {
        let encoder = self.options.result_encoder;
        if encoder.result_charset().is_none() {
            // Go's `isNull` state leaves the column bytes untouched; retain
            // the caller's allocation for the final length-encoded append.
            for value in row {
                append_length_encoded_bytes(payload, value.as_deref());
            }
            return;
        }
        for (value, column) in row.into_iter().zip(&self.columns) {
            let Some(value) = value else {
                append_length_encoded_bytes(payload, None);
                continue;
            };
            if !crate::result_encoder::is_string_column_type(column.type_code) {
                append_length_encoded_bytes(payload, Some(&value));
                continue;
            }
            let mut encoder = encoder;
            // Go treats JSON and VECTOR as utf8mb4 regardless of the
            // column's own (binary) collation.
            let collation = match column.type_code {
                crate::column::TYPE_JSON | crate::column::TYPE_TIDB_VECTOR_FLOAT32 => {
                    crate::result_encoder::UTF8MB4_DEFAULT_COLLATION_ID
                }
                _ => column.charset,
            };
            if encoder.update_data_encoding(collation).is_err() {
                append_length_encoded_bytes(payload, Some(&value));
                continue;
            }
            // `update_data_encoding` initializes the encoder, and all
            // registered conversions are infallible. This is the owned
            // equivalent of Go's EncodeData fallback without cloning the
            // source bytes on the successful path.
            let value = encoder
                .encode_data_owned(value)
                .expect("data encoding was initialized above");
            append_length_encoded_bytes(payload, Some(&value));
        }
    }

    /// Emits the terminal EOF exactly once.
    pub fn finish_packet(&mut self) -> Result<Vec<u8>, ResultSetStreamError> {
        if self.state != ResultSetStreamState::Rows {
            return Err(ResultSetStreamError::InvalidState {
                state: self.state,
                operation: "finish",
            });
        }
        self.state = ResultSetStreamState::Finished;
        Ok(encode_eof_packet(&self.eof()))
    }

    fn eof(&self) -> EofPacket {
        EofPacket {
            affected_rows: self.options.affected_rows,
            last_insert_id: self.options.last_insert_id,
            warnings: self.options.warnings,
            status_flags: self.options.status_flags,
            deprecate_eof: self.options.deprecate_eof,
            protocol_41: self.options.protocol_41,
            info: self.options.info.clone(),
        }
    }
}

const fn length_encoded_int_size(value: usize) -> usize {
    match value {
        0..=250 => 1,
        251..=0xffff => 3,
        0x1_0000..=0xff_ffff => 4,
        _ => 9,
    }
}

fn encode_owned_cell(
    column: &ColumnInfo,
    value: Vec<u8>,
    mut encoder: crate::ResultEncoder,
) -> Vec<u8> {
    if encoder.result_charset().is_none() || !is_string_column_type(column.type_code) {
        return value;
    }
    // Go treats JSON and VECTOR as utf8mb4 regardless of their binary column
    // collation; all other string columns use their declared charset.
    let collation = match column.type_code {
        crate::TYPE_JSON | crate::TYPE_TIDB_VECTOR_FLOAT32 => crate::UTF8MB4_DEFAULT_COLLATION_ID,
        _ => column.charset,
    };
    if encoder.update_data_encoding(collation).is_err() {
        return value;
    }
    let fallback = value.clone();
    encoder.encode_data_owned(value).unwrap_or(fallback)
}

/// Conservative size estimate for one Go `DumpTextRow` cell, including its
/// length-encoded prefix. It is intentionally an upper bound for fixed-width
/// text and a payload-length estimate for byte-preserving values; an estimate
/// can only affect allocation growth, never the encoded bytes.
fn datum_text_capacity(datum: &Datum) -> usize {
    let value_len = match datum {
        Datum::Null => 0,
        Datum::Int(_) | Datum::UInt(_) => 20,
        Datum::Decimal(_) => 72,
        Datum::Real(_) | Datum::Float32(_) => 32,
        Datum::String(_)
        | Datum::Bytes(_)
        | Datum::BinaryLiteral(_)
        | Datum::Bit(_)
        | Datum::Enum(_, _)
        | Datum::Set(_, _) => datum.go_bytes().len(),
        Datum::Duration(_) | Datum::Time(_) => 32,
        Datum::Json(_) | Datum::VectorFloat32(_) => 72,
        Datum::MinNotNull | Datum::MaxValue | Datum::Raw(_) => 0,
    };
    value_len.saturating_add(length_encoded_int_size(value_len))
}

fn finish_cell_prefix(payload: &mut Vec<u8>, prefix_start: usize, value_start: usize) {
    let value_len = payload.len() - value_start;
    let prefix = length_encoded_prefix(value_len);
    let reserved = value_start - prefix_start;
    if prefix.len > reserved {
        let extra = prefix.len - reserved;
        let old_len = payload.len();
        payload.reserve(extra);
        payload.resize(old_len + extra, 0);
        payload.copy_within(value_start..old_len, value_start + extra);
    } else if prefix.len < reserved {
        payload.copy_within(value_start.., prefix_start + prefix.len);
        payload.truncate(payload.len() - (reserved - prefix.len));
    }
    payload[prefix_start..prefix_start + prefix.len].copy_from_slice(&prefix.bytes[..prefix.len]);
}

struct LengthEncodedPrefix {
    bytes: [u8; 9],
    len: usize,
}

fn length_encoded_prefix(value: usize) -> LengthEncodedPrefix {
    let value = value as u64;
    let mut prefix = LengthEncodedPrefix {
        bytes: [0; 9],
        len: 0,
    };
    match value {
        0..=250 => {
            prefix.bytes[0] = value as u8;
            prefix.len = 1;
        }
        251..=0xffff => {
            prefix.bytes[0] = 0xfc;
            prefix.bytes[1..3].copy_from_slice(&(value as u16).to_le_bytes());
            prefix.len = 3;
        }
        0x1_0000..=0xff_ffff => {
            prefix.bytes[0] = 0xfd;
            prefix.bytes[1] = value as u8;
            prefix.bytes[2] = (value >> 8) as u8;
            prefix.bytes[3] = (value >> 16) as u8;
            prefix.len = 4;
        }
        _ => {
            prefix.bytes[0] = 0xfe;
            prefix.bytes[1..9].copy_from_slice(&value.to_le_bytes());
            prefix.len = 9;
        }
    }
    prefix
}

impl TextRowWriter<'_> {
    /// Appends one Go `MyDecimal.String()` value without allocating a
    /// temporary rendered cell. The caller keeps the source chunk borrowed
    /// until [`Self::finish`] just as `DumpTextRow` does.
    pub fn append_my_decimal(&mut self, value: &MyDecimal) -> Result<(), ResultSetStreamError> {
        let Some(column) = self.stream.columns.get(self.next_column) else {
            return Err(ResultSetStreamError::RowColumnCount {
                row: self.row_index,
                expected: self.stream.columns.len(),
                actual: self.next_column + 1,
            });
        };
        if column.type_code != crate::TYPE_NEW_DECIMAL {
            return Err(ResultSetStreamError::TextFormat {
                row: self.row_index,
                column: self.next_column,
                error: TextFormatError::ScalarTypeMismatch(column.type_code),
            });
        }
        let prefix_start = self.payload.len();
        self.payload.push(0);
        let value_start = self.payload.len();
        value.append_result_string_bytes(&mut self.payload);
        finish_cell_prefix(&mut self.payload, prefix_start, value_start);
        self.next_column += 1;
        Ok(())
    }

    /// Appends one source-shaped scalar in the advertised column order.
    pub fn append(&mut self, value: crate::TextScalar<'_>) -> Result<(), ResultSetStreamError> {
        let Some(column) = self.stream.columns.get(self.next_column) else {
            return Err(ResultSetStreamError::RowColumnCount {
                row: self.row_index,
                expected: self.stream.columns.len(),
                actual: self.next_column + 1,
            });
        };
        let text_column = TextColumn {
            type_code: column.type_code,
            flag: column.flag,
            decimal: column.decimal,
            table_is_empty: column.table.is_empty(),
        };

        // A result-charset conversion needs ownership of the source bytes.
        // When no conversion is configured, append the known-length bytes
        // directly with their final prefix; this is Go's
        // `DumpTextRow`/`LengthEncodedString` fast path.
        if let crate::TextScalar::Bytes(bytes) = value {
            // Go's `FormatValueText` enters this branch only for its complete
            // string-like type family. Keep unsupported/typed columns on the
            // checked scalar matrix so a raw byte cannot hide `invalid type`.
            if is_string_column_type(column.type_code) {
                if self
                    .stream
                    .options
                    .result_encoder
                    .result_charset()
                    .is_none()
                {
                    append_length_encoded_bytes(&mut self.payload, Some(bytes));
                    self.next_column += 1;
                    return Ok(());
                }
                let encoded =
                    encode_owned_cell(column, bytes.to_vec(), self.stream.options.result_encoder);
                append_length_encoded_bytes(&mut self.payload, Some(&encoded));
                self.next_column += 1;
                return Ok(());
            }
        }

        let prefix_start = self.payload.len();
        self.payload.push(0);
        let value_start = self.payload.len();
        let rendered = crate::textrow::append_text_value(&mut self.payload, text_column, value)
            .map_err(|error| ResultSetStreamError::TextFormat {
                row: self.row_index,
                column: self.next_column,
                error,
            })?;
        if rendered {
            finish_cell_prefix(&mut self.payload, prefix_start, value_start);
        } else {
            self.payload.truncate(prefix_start);
            self.payload.push(NULL_MARKER);
        }
        self.next_column += 1;
        Ok(())
    }

    /// Finishes the row and advances the stream's row counter.
    pub fn finish(self) -> Result<Vec<u8>, ResultSetStreamError> {
        if self.next_column != self.stream.columns.len() {
            return Err(ResultSetStreamError::RowColumnCount {
                row: self.row_index,
                expected: self.stream.columns.len(),
                actual: self.next_column,
            });
        }
        self.stream.rows += 1;
        Ok(self.payload)
    }
}
