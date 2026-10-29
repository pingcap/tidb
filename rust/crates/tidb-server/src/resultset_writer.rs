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

use tidb_chunk::row::Row;
use tidb_datatype::{FieldType, FieldTypeCode};
use tidb_protocol::resultset_stream::{ResultSetStream, ResultSetStreamError, TextRowWriter};
use tidb_format_textrow::TextScalar;
use tidb_protocol::{PacketWriter, ResultSetOptions};

use tidb_session::domain::resultset_source::ResultSetSource;

/// A packet sink that can report whether a failed write may have escaped.
pub trait ResultSetSink {
    /// Writes one logical packet payload.
    fn write_payload(&mut self, payload: &[u8]) -> Result<(), SinkWriteError>;

    /// Writes several payloads while allowing a connection sink to coalesce
    /// their transport flush. In-memory sinks retain the one-packet behavior.
    fn write_payloads(&mut self, payloads: &[&[u8]]) -> Result<(), SinkWriteError> {
        for payload in payloads {
            self.write_payload(payload)?;
        }
        Ok(())
    }

    /// Writes owned logical packets without forcing a sink that already owns
    /// its transport queue to clone each payload. The default keeps the
    /// borrowed behavior for in-memory/test sinks.
    fn write_payload_owned(&mut self, payload: Vec<u8>) -> Result<(), SinkWriteError> {
        self.write_payload(&payload)
    }

    /// Writes several owned payloads while retaining ownership transfer to
    /// sinks that can queue the packet buffers directly.
    fn write_payloads_owned(&mut self, payloads: Vec<Vec<u8>>) -> Result<(), SinkWriteError> {
        for payload in payloads {
            self.write_payload_owned(payload)?;
        }
        Ok(())
    }

    /// Flushes any connection-owned result-set buffering.
    ///
    /// In-memory sinks do not need a flush. A socket sink uses this boundary
    /// after the terminal EOF/OK packet so one result set is sent with a
    /// single transport flush rather than one flush per MySQL packet.
    fn flush(&mut self) -> Result<(), SinkWriteError> {
        Ok(())
    }

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
    /// Typed SQL error from the source, protocol encoder, or packet sink.
    pub cause: tidb_terror::MysqlError,
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
    if let Some(chunk) = source.new_chunk() {
        return write_chunk_result_set(source, sink, options, chunk);
    }
    let batch_size = batch_size.max(1);
    let mut batch = source
        .next_batch(batch_size)
        .map_err(|message| TrackedResultSetWriteError {
            error: ResultSetWriteError {
                cause: message.into(),
                retryable: true,
                bytes_escaped: false,
            },
            finish_attempted: false,
        })?;

    // Go deliberately calls Next before Columns because lazy execution can
    // finalize field metadata during the first pull.
    let columns = source
        .fields()
        .map(|fields| crate::column_convert::convert_column_infos(&fields))
        .map_err(|message| tracked_after_pull(message, sink, false))?;
    let mut options = options;
    if let Some(status) = source.statement_status() {
        apply_statement_status(&status, &mut options);
    }
    let mut stream = ResultSetStream::new(columns, options);
    // One coalesced transport write for ALL metadata packets. A socket sink
    // flushes per `write_payload`, which used to send every column definition
    // as its own segment; batching them here (and letting the first row
    // batch's boundary below carry them onto the wire together) turns a
    // small SELECT into two segments instead of one per packet. Framed sinks
    // keep the identical byte sequence either way.
    let metadata = stream
        .metadata_packets()
        .map_err(|error| tracked_after_pull(error.to_string(), sink, false))?;
    if !metadata.is_empty() {
        // A failed metadata write already put packets on the wire: the sink
        // knows whether bytes escaped, and the answer can no longer be
        // retried on this connection.
        sink.write_payloads_owned(metadata)
            .map_err(|error| payloads_failed(error, sink))?;
    }

    // A zero-row answer never enters the row loop below. Its terminal EOF is
    // still owed to the client: skipping the loop left the buffered metadata
    // unflushed forever and the client waiting on bytes that never come
    // (the regression `88ff511` shipped).
    if batch.is_empty() {
        source
            .finish()
            .map_err(|message| tracked_after_pull(message, sink, true))?;
        if let Some(status) = source.statement_status() {
            stream.update_statement_status(|options| apply_statement_status(&status, options));
        }
        let terminal = stream
            .finish_packet()
            .map_err(|error| tracked_after_pull(error.to_string(), sink, true))?;
        write_payload(sink, &terminal).map_err(|error| tracked(error, true))?;
        flush_sink(sink).map_err(|error| tracked(error, true))?;
        return Ok(ResultSetWriteOutcome {
            rows_written: stream.row_count(),
            packets_written: sink.packets_written(),
        });
    }

    // An empty first pull still has a complete result-set lifecycle. The
    // prefetch path below emits the terminal packet when the *next* pull is
    // empty, but there is no row batch to enter that loop for an empty result
    // set. Finish the source and write EOF explicitly so clients do not wait
    // forever for the response terminator.
    loop {
        // Go streams a result set through a bufio.Writer that flushes only
        // when it fills, so 90k rows leave the server as a few megabyte-scale
        // transport writes. Flushing PER PACKET here turned every row into
        // its own TCP syscall -- the dominant cost of any wide streaming
        // read. Coalesce one batch of row packets into a single coalesced
        // transport write, then flush once at the batch boundary so the
        // client still sees progressive delivery while the source fetches
        // its next chunk.
        let rows = std::mem::take(&mut batch);
        let mut payloads = Vec::with_capacity(rows.len());
        for row in rows {
            let payload = stream
                .row_packet_datums_owned(row)
                .map_err(|error| tracked_after_pull(error.to_string(), sink, false))?;
            payloads.push(payload);
        }
        // Prefetch the NEXT batch before writing this one: when it comes
        // back empty this batch is the last, and the terminal packet can join
        // its coalesced write instead of forcing a second segment. A small
        // result set therefore leaves as ONE transport write, which is what
        // Go's buffered connection gives a small SELECT.
        batch = match source.next_batch(batch_size) {
            Ok(batch) => batch,
            Err(message) => {
                // Rows this batch already formatted still belong to the wire:
                // go streams them ahead of the later pull's error, and the
                // retry classifier reads them back as escaped bytes.
                sink.write_payloads_owned(payloads)
                    .map_err(|error| payloads_failed(error, sink))?;
                flush_sink(sink).map_err(|error| tracked(error, false))?;
                return Err(tracked_after_pull(message, sink, false));
            }
        };
        if batch.is_empty() {
            if let Err(message) = source.finish() {
                // The same duty to the wire when finishing fails mid-stream.
                sink.write_payloads_owned(payloads)
                    .map_err(|error| payloads_failed(error, sink))?;
                flush_sink(sink).map_err(|error| tracked(error, false))?;
                return Err(tracked_after_pull(message, sink, true));
            }
            if let Some(status) = source.statement_status() {
                stream.update_statement_status(|options| apply_statement_status(&status, options));
            }
            let terminal = stream
                .finish_packet()
                .map_err(|error| tracked_after_pull(error.to_string(), sink, true))?;
            payloads.push(terminal);
        }
        sink.write_payloads_owned(payloads)
            .map_err(|error| payloads_failed(error, sink))?;
        flush_sink(sink).map_err(|error| tracked(error, false))?;
        if batch.is_empty() {
            return Ok(ResultSetWriteOutcome {
                rows_written: stream.row_count(),
                packets_written: sink.packets_written(),
            });
        }
    }
}

/// One reusable chunk and one row buffer, in Go writeChunks order.
fn write_chunk_result_set<S: ResultSetSource, W: ResultSetSink>(
    source: &mut S,
    sink: &mut W,
    mut options: ResultSetOptions,
    mut chunk: tidb_chunk::chunk::Chunk,
) -> Result<ResultSetWriteOutcome, TrackedResultSetWriteError> {
    source
        .next_chunk(&mut chunk)
        .map_err(|cause| TrackedResultSetWriteError {
            error: ResultSetWriteError {
                cause,
                retryable: true,
                bytes_escaped: false,
            },
            finish_attempted: false,
        })?;
    let columns = source
        .fields()
        .map(|fields| crate::column_convert::convert_column_infos(&fields))
        .map_err(|error| tracked_after_pull(error, sink, false))?;
    if let Some(status) = source.statement_status() {
        apply_statement_status(&status, &mut options);
    }
    let mut stream = ResultSetStream::new(columns, options);
    let metadata = stream
        .metadata_packets()
        .map_err(|error| tracked_after_pull(error.to_string(), sink, false))?;
    sink.write_payloads_owned(metadata)
        .map_err(|error| payloads_failed(error, sink))?;
    let mut payload = Vec::with_capacity(1024);
    while chunk.num_rows() != 0 {
        for index in 0..chunk.num_rows() {
            let row = chunk.get_row(index);
            let mut writer = stream
                .text_row_with_buffer(std::mem::take(&mut payload))
                .map_err(|error| tracked_after_pull(error.to_string(), sink, false))?;
            for (column, field) in source.field_types().iter().enumerate() {
                append_chunk_cell(&mut writer, row, column, field)
                    .map_err(|error| tracked_after_pull(error.to_string(), sink, false))?;
            }
            payload = writer
                .finish()
                .map_err(|error| tracked_after_pull(error.to_string(), sink, false))?;
            write_payload(sink, &payload).map_err(|error| tracked(error, false))?;
        }
        source
            .next_chunk(&mut chunk)
            .map_err(|error| tracked_after_pull(error, sink, false))?;
    }
    source
        .finish()
        .map_err(|error| tracked_after_pull(error, sink, true))?;
    if let Some(status) = source.statement_status() {
        stream.update_statement_status(|options| apply_statement_status(&status, options));
    }
    let terminal = stream
        .finish_packet()
        .map_err(|error| tracked_after_pull(error.to_string(), sink, true))?;
    write_payload(sink, &terminal).map_err(|error| tracked(error, true))?;
    flush_sink(sink).map_err(|error| tracked(error, true))?;
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
    message: impl Into<tidb_terror::MysqlError>,
    sink: &W,
    finish_attempted: bool,
) -> TrackedResultSetWriteError {
    tracked(failed_after_pull(message, sink), finish_attempted)
}

fn write_payload<W: ResultSetSink>(
    sink: &mut W,
    payload: &[u8],
) -> Result<(), ResultSetWriteError> {
    sink.write_payload(payload).map_err(|error| {
        let bytes_escaped = sink.packets_written() > 0 || error.bytes_escaped;
        ResultSetWriteError {
            cause: error.message.into(),
            retryable: false,
            bytes_escaped,
        }
    })
}

/// A failed coalesced `write_payloads` classifies exactly like a failed
/// single-packet write: never retryable, and escaped only when the sink says
/// so or any packet already left.
fn payloads_failed<W: ResultSetSink>(
    error: SinkWriteError,
    sink: &W,
) -> TrackedResultSetWriteError {
    tracked(
        ResultSetWriteError {
            cause: error.message.into(),
            retryable: false,
            bytes_escaped: error.bytes_escaped || sink.packets_written() > 0,
        },
        false,
    )
}

fn flush_sink<W: ResultSetSink>(sink: &mut W) -> Result<(), ResultSetWriteError> {
    sink.flush().map_err(|error| ResultSetWriteError {
        cause: error.message.into(),
        retryable: false,
        bytes_escaped: sink.packets_written() > 0 || error.bytes_escaped,
    })
}

fn failed_after_pull<W: ResultSetSink>(
    message: impl Into<tidb_terror::MysqlError>,
    sink: &W,
) -> ResultSetWriteError {
    ResultSetWriteError {
        cause: message.into(),
        retryable: false,
        bytes_escaped: sink.packets_written() > 0,
    }
}

/// Appends one typed chunk cell with Go `DumpTextRow` formatting.
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

/// Go `writeResultSet`/`writeEOF`'s status word and OK fields: the statement's
/// live session bits and counters over the command-owned flags (more results,
/// cursor) the options already carry.
pub(crate) fn apply_statement_status(
    status: &tidb_session::domain::resultset_source::StatementStatus<'_>,
    options: &mut tidb_protocol::ResultSetOptions,
) {
    const SESSION_BITS: u16 = tidb_session::domain::wire_status::SERVER_STATUS_IN_TRANS
        | tidb_session::domain::wire_status::SERVER_STATUS_AUTOCOMMIT;
    options.status_flags = (options.status_flags & !SESSION_BITS) | status.status.bits();
    options.warnings = status.warnings;
    options.affected_rows = status.affected_rows;
    options.last_insert_id = status.last_insert_id;
    options.info.clear();
    options.info.extend_from_slice(status.info);
}
