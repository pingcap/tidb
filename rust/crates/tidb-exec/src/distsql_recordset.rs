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

//! DistSQL-backed implementation of the executor RecordSet lifecycle.
//!
//! This adapter consumes the existing decoded `SelectResponseIter`; it does
//! not construct requests or invent a TiKV transport. Raw datum rows are
//! exposed only as they are pulled, preserving the source's bounded lifecycle.

use tidb_chunk::{chunk::Chunk, row::Row};
use tidb_datatype::{Datum, FieldType, FieldTypeCode, MyDecimal, MYDECIMAL_STRUCT_SIZE};
use tidb_distsql::{ResponseChannelError, SelectResponseIter, SelectResultRuntimeStats};
use tidb_protocol::resultset_stream::{ResultSetStream, ResultSetStreamError, TextRowWriter};
use tidb_protocol::{ColumnInfo, TextScalar};

use crate::recordset_lifecycle::RecordSetLifecycle;

/// Error returned while consuming or closing a DistSQL record set.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DistSqlRecordSetError {
    /// The checked DistSQL response iterator failed.
    Source(String),
}

/// A typed response chunk retained for direct Go-shaped text serialization.
pub trait TextResultBatch {
    /// Returns whether this batch has no rows.
    fn is_empty(&self) -> bool;

    /// Formats all rows into the supplied result stream while the chunk is
    /// still borrowed. Implementations validate untrusted cells before using
    /// the infallible Go `chunk.Row` getters.
    fn write_rows(&self, stream: &mut ResultSetStream) -> Result<Vec<Vec<u8>>, String>;
}

struct DistSqlTextBatch {
    chunk: Chunk,
    field_types: Vec<FieldType>,
}

impl TextResultBatch for DistSqlTextBatch {
    fn is_empty(&self) -> bool {
        self.chunk.num_rows() == 0
    }

    fn write_rows(&self, stream: &mut ResultSetStream) -> Result<Vec<Vec<u8>>, String> {
        let mut payloads = Vec::with_capacity(self.chunk.num_rows());
        for row_index in 0..self.chunk.num_rows() {
            let row = self.chunk.get_row(row_index);
            validate_chunk_row(row, &self.field_types)?;
            // The Go text writer appends directly into its reusable packet
            // buffer.  Reserve a conservative scalar bound once instead of
            // re-entering the chunk column view for every cell just to size a
            // temporary row packet.  This changes only allocation sizing,
            // never the encoded bytes.
            let row_capacity = self
                .field_types
                .iter()
                .enumerate()
                .map(|(column, field_type)| match field_type.code() {
                    FieldTypeCode::Varchar
                    | FieldTypeCode::VarString
                    | FieldTypeCode::String
                    | FieldTypeCode::Blob
                    | FieldTypeCode::TinyBlob
                    | FieldTypeCode::MediumBlob
                    | FieldTypeCode::LongBlob
                    | FieldTypeCode::Bit
                    | FieldTypeCode::Json
                    | FieldTypeCode::Enum
                    | FieldTypeCode::Set
                    | FieldTypeCode::VectorFloat32 => {
                        row.get_raw_len(column).saturating_add(9)
                    }
                    FieldTypeCode::NewDecimal => 72,
                    FieldTypeCode::Date
                    | FieldTypeCode::Datetime
                    | FieldTypeCode::Timestamp
                    | FieldTypeCode::Duration => 32,
                    _ => 24,
                })
                .sum();
            let mut writer = stream
                .text_row_with_capacity(row_capacity)
                .map_err(|error| error.to_string())?;
            for (column, field_type) in self.field_types.iter().enumerate() {
                append_chunk_cell(&mut writer, row, column, field_type)
                    .map_err(|error| error.to_string())?;
            }
            payloads.push(writer.finish().map_err(|error| error.to_string())?);
        }
        Ok(payloads)
    }
}

impl std::fmt::Display for DistSqlRecordSetError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Source(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for DistSqlRecordSetError {}

/// Lazy RecordSet over one already-injected select response iterator.
pub struct DistSqlRecordSet {
    iter: SelectResponseIter,
    columns: Vec<ColumnInfo>,
    lifecycle: RecordSetLifecycle,
}

impl DistSqlRecordSet {
    /// Binds resolved result metadata to an existing decoded response stream.
    #[must_use]
    pub fn new(iter: SelectResponseIter, columns: Vec<ColumnInfo>) -> Self {
        Self {
            iter,
            columns,
            lifecycle: RecordSetLifecycle::default(),
        }
    }

    /// Returns source-derived metadata. The server controls when this is read.
    #[must_use]
    pub fn columns(&self) -> &[ColumnInfo] {
        &self.columns
    }

    /// Borrows runtime statistics accumulated while the source iterator drains.
    #[must_use]
    pub fn runtime_stats(&self) -> &SelectResultRuntimeStats {
        self.iter.runtime_stats()
    }

    /// Pulls at most `max_rows` rows without reading the remainder.
    pub fn next_batch(
        &mut self,
        max_rows: usize,
    ) -> Result<Vec<Vec<Datum>>, DistSqlRecordSetError> {
        self.lifecycle.mark_advanced();
        let mut rows = Vec::with_capacity(max_rows);
        while rows.len() < max_rows {
            let next = self.iter.next_row().map_err(map_source_error)?;
            let Some(row) = next else {
                break;
            };
            rows.push(row.row);
        }
        Ok(rows)
    }

    /// Pulls one response chunk for the server's direct text writer. `None`
    /// means the response source is exhausted; unlike `next_batch`, this path
    /// keeps variable-length cells borrowed from the decoded chunk until the
    /// row packet has been built.
    pub fn next_text_batch(
        &mut self,
        max_rows: usize,
    ) -> Result<Option<Box<dyn TextResultBatch>>, DistSqlRecordSetError> {
        self.lifecycle.mark_advanced();
        let Some(result) = self
            .iter
            .next_chunk_with_required_rows(max_rows)
            .map_err(map_source_error)?
        else {
            return Ok(None);
        };
        let field_types = self
            .iter
            .field_types_for_channel(result.channel_index)
            .ok_or_else(|| {
                DistSqlRecordSetError::Source(format!(
                    "missing field types for response channel {}",
                    result.channel_index
                ))
            })?
            .to_vec();
        Ok(Some(Box::new(DistSqlTextBatch {
            chunk: result.row,
            field_types,
        })))
    }

    /// Runs statement finish once. Resource close remains a separate phase.
    pub fn finish(&mut self) -> Result<(), DistSqlRecordSetError> {
        self.lifecycle.begin_finish();
        Ok(())
    }

    /// Closes the injected response iterator exactly once.
    pub fn close(&mut self) -> Result<(), DistSqlRecordSetError> {
        if self.lifecycle.begin_close() {
            // Go recordSet.Close always enters Finish first, including error
            // paths where the server never reached terminal EOF. Keep that
            // cleanup invariant structural instead of relying on every caller
            // to remember a separate finish call.
            self.finish()?;
            self.iter.close();
        }
        Ok(())
    }

    /// Exposes lifecycle state for connection adapters and focused tests.
    #[must_use]
    pub const fn lifecycle(&self) -> &RecordSetLifecycle {
        &self.lifecycle
    }
}

fn map_source_error(error: ResponseChannelError) -> DistSqlRecordSetError {
    DistSqlRecordSetError::Source(error.to_string())
}

fn validate_chunk_row(row: Row<'_>, field_types: &[FieldType]) -> Result<(), String> {
    if field_types.len() != row.len() {
        return Err(format!(
            "chunk row has {} columns but {} field types were supplied",
            row.len(),
            field_types.len()
        ));
    }
    for (column, field_type) in field_types.iter().enumerate().take(row.len()) {
        // Go's checked TypeChunk decoder owns the structural boundary: it
        // validates each fixed-width column's complete byte count and every
        // variable-column offset before a `chunk.Row` exists. Primitive
        // numeric getters and raw byte getters therefore need no second
        // per-cell materialization here. Only values whose payload has an
        // internal semantic layout still need validation before their Go
        // formatter-shaped getters are used below.
        let needs_semantic_validation = matches!(
            field_type.code(),
            FieldTypeCode::Json
                | FieldTypeCode::Enum
                | FieldTypeCode::Set
                | FieldTypeCode::Date
                | FieldTypeCode::Datetime
                | FieldTypeCode::Timestamp
                | FieldTypeCode::Duration
                | FieldTypeCode::NewDecimal
                | FieldTypeCode::VectorFloat32
        );
        if needs_semantic_validation {
            if field_type.code() == FieldTypeCode::NewDecimal {
                let raw = row.get_raw(column);
                let raw: [u8; MYDECIMAL_STRUCT_SIZE] = raw.as_ref().try_into().map_err(|_| {
                    format!(
                        "chunk row column {column} ({:?}) has an invalid payload: expected {MYDECIMAL_STRUCT_SIZE} bytes",
                        field_type.code()
                    )
                })?;
                MyDecimal::from_raw_bytes(raw).map_err(|error| {
                    format!(
                        "chunk row column {column} ({:?}) has an invalid payload: {error}",
                        field_type.code()
                    )
                })?;
            } else {
                let mut datum = Datum::Null;
                row.try_datum_with_buffer(column, field_type, &mut datum)
                    .map_err(|error| error.to_string())?;
            }
        }
    }
    Ok(())
}

fn append_chunk_cell(
    writer: &mut TextRowWriter<'_>,
    row: Row<'_>,
    column: usize,
    field_type: &FieldType,
) -> Result<(), ResultSetStreamError> {
    if row.is_null(column) {
        return writer.append(TextScalar::Null);
    }
    let value = match field_type.code() {
        FieldTypeCode::Tiny | FieldTypeCode::Short | FieldTypeCode::Int24 | FieldTypeCode::Long => {
            TextScalar::Signed(row.get_int64(column))
        }
        FieldTypeCode::LongLong => {
            if field_type.is_unsigned() {
                TextScalar::Unsigned(row.get_uint64(column))
            } else {
                TextScalar::Signed(row.get_int64(column))
            }
        }
        FieldTypeCode::Year => TextScalar::Signed(row.get_int64(column)),
        FieldTypeCode::Float => TextScalar::Float {
            value: f64::from(row.get_float32(column)),
            bit_size: 32,
        },
        FieldTypeCode::Double => TextScalar::Float {
            value: row.get_float64(column),
            bit_size: 64,
        },
        FieldTypeCode::NewDecimal => {
            // Go `DumpTextRow` calls `MyDecimal.String()`, which rounds to the
            // value's result fraction before writing.  Keep the raw MyDecimal
            // layout and use its source-shaped clone/round/ToString boundary;
            // reconstructing a value-layer Decimal here would allocate a
            // second digit representation for every decimal cell.
            return writer.append_my_decimal(&row.get_my_decimal(column));
        }
        FieldTypeCode::Varchar
        | FieldTypeCode::VarString
        | FieldTypeCode::String
        | FieldTypeCode::Blob
        | FieldTypeCode::TinyBlob
        | FieldTypeCode::MediumBlob
        | FieldTypeCode::LongBlob
        | FieldTypeCode::Bit => {
            let bytes = row.get_bytes(column);
            return writer.append(TextScalar::Bytes(bytes.as_ref()));
        }
        FieldTypeCode::Json => {
            return append_owned_chunk_text(
                writer,
                row.get_json(column).to_string(),
                OwnedTextKind::Bytes,
            )
        }
        FieldTypeCode::Enum => {
            return append_owned_chunk_text(
                writer,
                row.get_enum(column).name_bytes().to_vec(),
                OwnedTextKind::Bytes,
            )
        }
        FieldTypeCode::Set => {
            return append_owned_chunk_text(
                writer,
                row.get_set(column).name_bytes().to_vec(),
                OwnedTextKind::Bytes,
            )
        }
        FieldTypeCode::Date | FieldTypeCode::Datetime | FieldTypeCode::Timestamp => {
            return append_owned_chunk_text(
                writer,
                row.get_time(column).to_string(),
                OwnedTextKind::Temporal,
            )
        }
        FieldTypeCode::Duration => {
            return append_owned_chunk_text(
                writer,
                row.get_duration(column, field_type.decimal()).to_string(),
                OwnedTextKind::Temporal,
            )
        }
        FieldTypeCode::VectorFloat32 => {
            return append_owned_chunk_text(
                writer,
                row.get_vector_float32(column).to_string(),
                OwnedTextKind::Bytes,
            )
        }
        _ => TextScalar::Bytes(&[]),
    };
    writer.append(value)
}

#[derive(Clone, Copy)]
enum OwnedTextKind {
    Bytes,
    Temporal,
}

fn append_owned_chunk_text(
    writer: &mut TextRowWriter<'_>,
    value: impl Into<Vec<u8>>,
    kind: OwnedTextKind,
) -> Result<(), ResultSetStreamError> {
    let value = value.into();
    let scalar = match kind {
        OwnedTextKind::Bytes => TextScalar::Bytes(&value),
        OwnedTextKind::Temporal => TextScalar::Temporal(&value),
    };
    writer.append(scalar)
}
