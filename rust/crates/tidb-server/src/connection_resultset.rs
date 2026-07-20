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

//! Connection routing for incremental result-set responses.

use tidb_datatype::Datum;
use tidb_protocol::{BinarySignedLongLongResultSetStream, ResultSetOptions};

use crate::resultset_source::ResultSetSource;
use crate::resultset_writer::{
    write_result_set_tracked, FramedResultSetSink, ResultSetSink, ResultSetWriteError,
    ResultSetWriteOutcome,
};

/// Complete outcome of a connection-owned streaming response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectionResultSetResponse {
    /// Complete MySQL frames beginning at server packet sequence one.
    pub framed: Vec<u8>,
    /// Incremental packet/row accounting.
    pub outcome: ResultSetWriteOutcome,
}

/// Writes one lazy result set and closes it exactly once on every path.
pub fn write_connection_result_set<S: ResultSetSource>(
    source: &mut S,
    options: ResultSetOptions,
    batch_size: usize,
) -> Result<ConnectionResultSetResponse, ResultSetWriteError> {
    let mut sink = FramedResultSetSink::new(1);
    let outcome = write_connection_result_set_to_sink(source, &mut sink, options, batch_size)?;
    Ok(ConnectionResultSetResponse {
        framed: sink.into_framed(),
        outcome,
    })
}

/// Streams one lazy result set into a caller-owned connection sink.
pub fn write_connection_result_set_to_sink<S: ResultSetSource, W: ResultSetSink>(
    source: &mut S,
    sink: &mut W,
    options: ResultSetOptions,
    batch_size: usize,
) -> Result<ResultSetWriteOutcome, ResultSetWriteError> {
    let result = write_result_set_tracked(source, sink, options, batch_size);
    let finish_result = match &result {
        Err(error) if !error.finish_attempted => source.finish(),
        _ => Ok(()),
    };
    let close_result = source.close();

    match (result, finish_result, close_result) {
        (Err(error), _, _) => Err(error.error),
        (Ok(_), Err(message), _) | (Ok(_), Ok(()), Err(message)) => Err(ResultSetWriteError {
            message,
            retryable: false,
            bytes_escaped: sink.packets_written() > 0,
        }),
        (Ok(outcome), Ok(()), Ok(())) => Ok(outcome),
    }
}

/// Streams one prepared signed-`BIGINT` result using MySQL binary rows and
/// preserves the ordinary connection-owned finish/close lifecycle.
pub fn write_connection_binary_result_set_to_sink<S: ResultSetSource, W: ResultSetSink>(
    source: &mut S,
    sink: &mut W,
    options: ResultSetOptions,
    batch_size: usize,
) -> Result<ResultSetWriteOutcome, ResultSetWriteError> {
    let result = write_binary_result_set_tracked(source, sink, options, batch_size);
    let finish_result = match &result {
        Err(error) if !error.finish_attempted => source.finish(),
        _ => Ok(()),
    };
    let close_result = source.close();

    match (result, finish_result, close_result) {
        (Err(error), _, _) => Err(error.error),
        (Ok(_), Err(message), _) | (Ok(_), Ok(()), Err(message)) => Err(ResultSetWriteError {
            message,
            retryable: false,
            bytes_escaped: sink.packets_written() > 0,
        }),
        (Ok(outcome), Ok(()), Ok(())) => Ok(outcome),
    }
}

struct BinaryTrackedError {
    error: ResultSetWriteError,
    finish_attempted: bool,
}

fn write_binary_result_set_tracked<S: ResultSetSource, W: ResultSetSink>(
    source: &mut S,
    sink: &mut W,
    options: ResultSetOptions,
    batch_size: usize,
) -> Result<ResultSetWriteOutcome, BinaryTrackedError> {
    let mut batch = source
        .next_batch(batch_size.max(1))
        .map_err(|message| BinaryTrackedError {
            error: ResultSetWriteError {
                message,
                retryable: true,
                bytes_escaped: false,
            },
            finish_attempted: false,
        })?;
    let columns = source
        .columns()
        .map_err(|message| binary_failure(message, sink, false))?;
    let mut stream = BinarySignedLongLongResultSetStream::new(columns, options)
        .map_err(|error| binary_failure(error.to_string(), sink, false))?;
    for payload in stream
        .metadata_packets()
        .map_err(|error| binary_failure(error.to_string(), sink, false))?
    {
        write_binary_payload(sink, &payload, false)?;
    }

    let mut rows_written = 0;
    loop {
        if batch.is_empty() {
            break;
        }
        for row in batch {
            let values = row
                .into_iter()
                .enumerate()
                .map(|(column, datum)| match datum {
                    Datum::Int(value) => Ok(value),
                    other => Err(format!(
                        "prepared binary result column {column} is not signed BIGINT: {other:?}"
                    )),
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(|message| binary_failure(message, sink, false))?;
            let payload = stream
                .row_packet(&values)
                .map_err(|error| binary_failure(error.to_string(), sink, false))?;
            write_binary_payload(sink, &payload, false)?;
            rows_written += 1;
        }
        batch = source
            .next_batch(batch_size.max(1))
            .map_err(|message| binary_failure(message, sink, false))?;
    }

    source
        .finish()
        .map_err(|message| binary_failure(message, sink, true))?;
    let terminal = stream
        .finish_packet()
        .map_err(|error| binary_failure(error.to_string(), sink, true))?;
    write_binary_payload(sink, &terminal, true)?;
    Ok(ResultSetWriteOutcome {
        rows_written,
        packets_written: sink.packets_written(),
    })
}

fn binary_failure<W: ResultSetSink>(
    message: String,
    sink: &W,
    finish_attempted: bool,
) -> BinaryTrackedError {
    BinaryTrackedError {
        error: ResultSetWriteError {
            message,
            retryable: false,
            bytes_escaped: sink.packets_written() > 0,
        },
        finish_attempted,
    }
}

fn write_binary_payload<W: ResultSetSink>(
    sink: &mut W,
    payload: &[u8],
    finish_attempted: bool,
) -> Result<(), BinaryTrackedError> {
    sink.write_payload(payload)
        .map_err(|error| BinaryTrackedError {
            error: ResultSetWriteError {
                message: error.message,
                retryable: false,
                bytes_escaped: sink.packets_written() > 0 || error.bytes_escaped,
            },
            finish_attempted,
        })
}
