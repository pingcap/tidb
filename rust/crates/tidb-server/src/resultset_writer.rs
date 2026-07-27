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
use tidb_protocol::{PacketWriter, ResultSetOptions};

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
            let row =
                format_row(row).map_err(|message| tracked_after_pull(message, sink, false))?;
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

fn format_row(row: Vec<Datum>) -> Result<Vec<Option<Vec<u8>>>, String> {
    row.into_iter().map(format_datum).collect()
}

fn format_datum(datum: Datum) -> Result<Option<Vec<u8>>, String> {
    match datum {
        Datum::Null => Ok(None),
        Datum::MinNotNull => Err("cannot render MinNotNull as a SQL row".to_owned()),
        Datum::MaxValue => Err("cannot render MaxValue as a SQL row".to_owned()),
        value => value
            .to_bytes()
            .map(Some)
            .map_err(|error| error.to_string()),
    }
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
