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

//! Incremental MySQL result-set writer lifecycle.

use tidb_datatype::Datum;
use tidb_protocol::resultset_stream::ResultSetStream;
use tidb_protocol::{
    format_text_value, PacketWriter, ResultSetOptions, TextColumn, TextFormatError, TextScalar,
    TYPE_FLOAT, TYPE_LONGLONG, UNSIGNED_FLAG,
};

use crate::resultset_source::ResultSetSource;

/// A packet sink that can report whether a failed write may have escaped.
pub trait ResultSetSink {
    /// Writes one logical packet payload.
    fn write_payload(&mut self, payload: &[u8]) -> Result<(), SinkWriteError>;

    /// Returns the number of complete logical packets written so far.
    fn packets_written(&self) -> usize;
}

/// Failure from a connection-owned packet sink.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SinkWriteError {
    /// Source error text.
    pub message: String,
    /// Whether any bytes from the failed write may have escaped.
    pub bytes_escaped: bool,
}

/// In-memory framed sink used by the connected server boundary and tests.
#[derive(Debug)]
pub struct FramedResultSetSink {
    framed: Vec<u8>,
    sequence: u8,
    packets: usize,
}

impl FramedResultSetSink {
    /// Creates a response sink beginning at the supplied server sequence.
    #[must_use]
    pub fn new(sequence: u8) -> Self {
        Self {
            framed: Vec::new(),
            sequence,
            packets: 0,
        }
    }

    /// Returns the complete uncompressed MySQL frames.
    #[must_use]
    pub fn framed(&self) -> &[u8] {
        &self.framed
    }

    /// Consumes the sink and returns the complete frames.
    #[must_use]
    pub fn into_framed(self) -> Vec<u8> {
        self.framed
    }
}

impl ResultSetSink for FramedResultSetSink {
    fn write_payload(&mut self, payload: &[u8]) -> Result<(), SinkWriteError> {
        let mut writer = PacketWriter::with_sequence(&mut self.framed, self.sequence);
        writer
            .write_packet(payload)
            .map_err(|error| SinkWriteError {
                message: error.to_string(),
                bytes_escaped: true,
            })?;
        self.sequence = writer.sequence();
        self.packets += 1;
        Ok(())
    }

    fn packets_written(&self) -> usize {
        self.packets
    }
}

/// Successful incremental write accounting.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResultSetWriteOutcome {
    /// Number of row packets emitted.
    pub rows_written: usize,
    /// Number of logical packets emitted, including metadata and EOF packets.
    pub packets_written: usize,
}

/// Result-set failure plus source-shaped retry classification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResultSetWriteError {
    /// Error text from source, protocol encoder, or packet sink.
    pub message: String,
    /// True only when the first `Next` failed before any packet escaped.
    pub retryable: bool,
    /// Whether any response bytes may already be visible to the client.
    pub bytes_escaped: bool,
}

/// Internal failure state used to avoid repeating a Finish attempt in cleanup.
pub(crate) struct TrackedResultSetWriteError {
    /// Client-visible write failure.
    pub(crate) error: ResultSetWriteError,
    /// Whether statement Finish was already attempted before the failure.
    pub(crate) finish_attempted: bool,
}

/// Pulls and writes a result set without buffering all rows.
pub fn write_result_set<S: ResultSetSource, W: ResultSetSink>(
    source: &mut S,
    sink: &mut W,
    options: ResultSetOptions,
    batch_size: usize,
) -> Result<ResultSetWriteOutcome, ResultSetWriteError> {
    write_result_set_tracked(source, sink, options, batch_size).map_err(|tracked| tracked.error)
}

/// Writes a result set while retaining the cleanup-relevant Finish boundary.
pub(crate) fn write_result_set_tracked<S: ResultSetSource, W: ResultSetSink>(
    source: &mut S,
    sink: &mut W,
    options: ResultSetOptions,
    batch_size: usize,
) -> Result<ResultSetWriteOutcome, TrackedResultSetWriteError> {
    let batch_size = batch_size.max(1);
    let mut batch =
        source
            .next_batch(batch_size)
            .map_err(|message| TrackedResultSetWriteError {
                error: ResultSetWriteError {
                    message,
                    retryable: true,
                    bytes_escaped: false,
                },
                finish_attempted: false,
            })?;

    // Go deliberately calls Next before Columns because lazy execution can
    // finalize field metadata during the first pull.
    let columns = source
        .columns()
        .map_err(|message| tracked_after_pull(message, sink, false))?;
    let text_columns = columns
        .iter()
        .map(|column| TextColumn {
            type_code: column.type_code,
            flag: column.flag,
            decimal: column.decimal,
            table_is_empty: column.table.is_empty(),
        })
        .collect::<Vec<_>>();
    let mut stream = ResultSetStream::new(columns, options);
    for payload in stream
        .metadata_packets()
        .map_err(|error| tracked_after_pull(error.to_string(), sink, false))?
    {
        write_payload(sink, &payload).map_err(|error| tracked(error, false))?;
    }

    loop {
        if batch.is_empty() {
            break;
        }
        for row in batch {
            let row = format_row(row, &text_columns, stream.row_count())
                .map_err(|message| tracked_after_pull(message, sink, false))?;
            let payload = stream
                .row_packet(&row)
                .map_err(|error| tracked_after_pull(error.to_string(), sink, false))?;
            write_payload(sink, &payload).map_err(|error| tracked(error, false))?;
        }
        batch = source
            .next_batch(batch_size)
            .map_err(|message| tracked_after_pull(message, sink, false))?;
    }

    source
        .finish()
        .map_err(|message| tracked_after_pull(message, sink, true))?;
    let terminal = stream
        .finish_packet()
        .map_err(|error| tracked_after_pull(error.to_string(), sink, true))?;
    write_payload(sink, &terminal).map_err(|error| tracked(error, true))?;

    Ok(ResultSetWriteOutcome {
        rows_written: stream.row_count(),
        packets_written: sink.packets_written(),
    })
}

fn tracked(error: ResultSetWriteError, finish_attempted: bool) -> TrackedResultSetWriteError {
    TrackedResultSetWriteError {
        error,
        finish_attempted,
    }
}

fn tracked_after_pull<W: ResultSetSink>(
    message: String,
    sink: &W,
    finish_attempted: bool,
) -> TrackedResultSetWriteError {
    tracked(failed_after_pull(message, sink), finish_attempted)
}

fn format_row(
    row: Vec<Datum>,
    columns: &[TextColumn],
    row_index: usize,
) -> Result<Vec<Option<Vec<u8>>>, String> {
    if row.len() != columns.len() {
        return Err(format!(
            "result row {row_index} has {} values, expected {}",
            row.len(),
            columns.len()
        ));
    }
    row.into_iter()
        .zip(columns.iter().copied())
        .map(|(datum, column)| format_datum(column, datum))
        .collect()
}

fn format_datum(column: TextColumn, datum: Datum) -> Result<Option<Vec<u8>>, String> {
    match datum {
        Datum::Null => Ok(None),
        Datum::MinNotNull => Err("cannot render MinNotNull as a SQL row".to_owned()),
        Datum::MaxValue => Err("cannot render MaxValue as a SQL row".to_owned()),
        Datum::Int(value) => format_scalar(column, TextScalar::Signed(value)),
        Datum::UInt(value)
            if column.type_code == TYPE_LONGLONG && column.flag & UNSIGNED_FLAG != 0 =>
        {
            format_scalar(column, TextScalar::Unsigned(value))
        }
        Datum::UInt(value) => format_scalar(column, TextScalar::Signed(value as i64)),
        Datum::Real(value) => format_scalar(
            column,
            TextScalar::Float {
                value,
                bit_size: if column.type_code == TYPE_FLOAT {
                    32
                } else {
                    64
                },
            },
        ),
        Datum::Float32(value) => format_scalar(
            column,
            TextScalar::Float {
                value,
                bit_size: 32,
            },
        ),
        Datum::Decimal(value) => {
            let value = value.to_string();
            format_scalar(column, TextScalar::Decimal(value.as_bytes()))
        }
        Datum::String(value) => format_scalar(column, TextScalar::Bytes(value.bytes())),
        Datum::Bytes(value) => format_scalar(column, TextScalar::Bytes(&value)),
        Datum::BinaryLiteral(value) | Datum::Bit(value) => {
            format_scalar(column, TextScalar::Bytes(value.as_bytes()))
        }
        Datum::Duration(value) => {
            let value = tidb_datatype::MySqlDuration::from_nanoseconds(
                value.nanoseconds(),
                i64::from(column.decimal),
            )
            .map_err(|error| error.to_string())?
            .to_string();
            format_scalar(column, TextScalar::Temporal(value.as_bytes()))
        }
        Datum::Enum(value, _) => format_scalar(column, TextScalar::Bytes(value.name_bytes())),
        Datum::Set(value, _) => format_scalar(column, TextScalar::Bytes(value.name_bytes())),
        Datum::Time(value) => {
            let value = value.to_string();
            format_scalar(column, TextScalar::Temporal(value.as_bytes()))
        }
        Datum::Json(value) => {
            let value = value.to_string();
            format_scalar(column, TextScalar::Bytes(value.as_bytes()))
        }
        Datum::VectorFloat32(value) => {
            let value = value.to_string();
            format_scalar(column, TextScalar::Bytes(value.as_bytes()))
        }
        Datum::Raw(_) => Err("cannot render Raw as a SQL row".to_owned()),
    }
}

fn format_scalar(column: TextColumn, value: TextScalar<'_>) -> Result<Option<Vec<u8>>, String> {
    format_text_value(column, value).map_err(|error| match error {
        TextFormatError::UnsupportedType(type_code) => format!("invalid type {type_code}"),
        TextFormatError::ScalarTypeMismatch(_) => error.to_string(),
    })
}

fn write_payload<W: ResultSetSink>(
    sink: &mut W,
    payload: &[u8],
) -> Result<(), ResultSetWriteError> {
    sink.write_payload(payload).map_err(|error| {
        let bytes_escaped = sink.packets_written() > 0 || error.bytes_escaped;
        ResultSetWriteError {
            message: error.message,
            retryable: false,
            bytes_escaped,
        }
    })
}

fn failed_after_pull<W: ResultSetSink>(message: String, sink: &W) -> ResultSetWriteError {
    ResultSetWriteError {
        message,
        retryable: false,
        bytes_escaped: sink.packets_written() > 0,
    }
}
