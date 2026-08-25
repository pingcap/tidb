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
    format_datum_text, PacketWriter, ResultSetOptions, TextColumn, TextFormatError,
};

use crate::resultset_source::ResultSetSource;

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
        let refs: Vec<&[u8]> = metadata.iter().map(|payload| payload.as_slice()).collect();
        sink.write_payloads(&refs).map_err(|error| {
            tracked(
                ResultSetWriteError {
                    message: error.message,
                    retryable: true,
                    bytes_escaped: false,
                },
                false,
            )
        })?;
    }

    loop {
        if batch.is_empty() {
            break;
        }
        // Go streams a result set through a bufio.Writer that flushes only
        // when it fills, so 90k rows leave the server as a few megabyte-scale
        // transport writes. Flushing PER PACKET here turned every row into
        // its own TCP syscall -- the dominant cost of any wide streaming
        // read. Coalesce one batch of row packets into a single coalesced
        // transport write, then flush once at the batch boundary so the
        // client still sees progressive delivery while the source fetches
        // its next chunk.
        let mut payloads = Vec::with_capacity(batch.len());
        for row in batch {
            let row = format_row(row, &text_columns, stream.row_count())
                .map_err(|message| tracked_after_pull(message, sink, false))?;
            let payload = stream
                .row_packet(&row)
                .map_err(|error| tracked_after_pull(error.to_string(), sink, false))?;
            payloads.push(payload);
        }
        // Prefetch the NEXT batch before writing this one: when it comes
        // back empty this batch is the last, and the terminal packet can join
        // its coalesced write instead of forcing a second segment. A small
        // result set therefore leaves as ONE transport write, which is what
        // Go's buffered connection gives a small SELECT.
        batch = source
            .next_batch(batch_size)
            .map_err(|message| tracked_after_pull(message, sink, false))?;
        if batch.is_empty() {
            source
                .finish()
                .map_err(|message| tracked_after_pull(message, sink, true))?;
            let terminal = stream
                .finish_packet()
                .map_err(|error| tracked_after_pull(error.to_string(), sink, true))?;
            payloads.push(terminal);
        }
        let refs: Vec<&[u8]> = payloads.iter().map(|payload| payload.as_slice()).collect();
        sink.write_payloads(&refs).map_err(|error| {
            tracked(
                ResultSetWriteError {
                    message: error.message,
                    retryable: true,
                    bytes_escaped: error.bytes_escaped,
                },
                false,
            )
        })?;
        flush_sink(sink).map_err(|error| tracked(error, false))?;
    }

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

/// Go `dumpTextRow` for one cell, over the ONE renderer both this writer and
/// the recorded-output harness use ([`tidb_protocol::format_datum_text`]).
fn format_datum(column: TextColumn, datum: Datum) -> Result<Option<Vec<u8>>, String> {
    format_datum_text(column, &datum).map_err(|error| match error {
        TextFormatError::UnsupportedType(type_code) => format!("invalid type {type_code}"),
        TextFormatError::NotARowValue(_) | TextFormatError::ScalarTypeMismatch(_) => {
            error.to_string()
        }
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

fn flush_sink<W: ResultSetSink>(sink: &mut W) -> Result<(), ResultSetWriteError> {
    sink.flush().map_err(|error| ResultSetWriteError {
        message: error.message,
        retryable: false,
        bytes_escaped: sink.packets_written() > 0 || error.bytes_escaped,
    })
}

fn failed_after_pull<W: ResultSetSink>(message: String, sink: &W) -> ResultSetWriteError {
    ResultSetWriteError {
        message,
        retryable: false,
        bytes_escaped: sink.packets_written() > 0,
    }
}
