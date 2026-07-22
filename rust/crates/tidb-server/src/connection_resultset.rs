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
use tidb_protocol::{
    is_binary_decimal_result_type, is_binary_string_result_type, BinaryResultCell,
    BinaryResultSetStream, ResultSetOptions, TYPE_DOUBLE, TYPE_FLOAT, TYPE_INT24, TYPE_LONG,
    TYPE_LONGLONG, TYPE_SHORT, TYPE_TINY, TYPE_YEAR,
};

/// Maps one decoded `Datum` to the binary cell its column type dumps, following
/// TiDB's `DumpBinaryRow` switch on `columns[i].Type`. Returns `None` when the
/// datum and column type disagree (a caller-surfaced error, never silent).
fn datum_to_binary_cell(datum: Datum, type_code: u8) -> Option<BinaryResultCell> {
    match datum {
        // NULL is type-agnostic: it writes no value bytes and only sets the
        // row's null-bitmap bit, so any result column admits it (a nullable
        // aggregate such as SUM over an empty group yields one).
        Datum::Null => Some(BinaryResultCell::Null),
        // GetInt64 feeds the fixed-width integer cases; the cell width matches
        // the `dump.Uint*` the column type selects.
        Datum::Int(value) => integer_cell(value, type_code),
        // TypeLonglong reads GetUint64; an unsigned value reuses the same
        // little-endian widths by bit reinterpretation.
        Datum::UInt(value) => integer_cell(value as i64, type_code),
        // TypeFloat dumps Float32bits(GetFloat32); TypeDouble dumps
        // Float64bits(GetFloat64). The real datum is f64; a float column
        // narrows to f32 exactly as `GetFloat32` does.
        Datum::Real(value) => match type_code {
            TYPE_FLOAT => Some(BinaryResultCell::Float(value as f32)),
            TYPE_DOUBLE => Some(BinaryResultCell::Double(value)),
            _ => None,
        },
        // TypeNewDecimal dumps LengthEncodedString(GetMyDecimal(i).String()); the
        // encoder stringifies the decimal, so the cell carries the value itself.
        Datum::Decimal(value) if is_binary_decimal_result_type(type_code) => {
            Some(BinaryResultCell::NewDecimal(value))
        }
        Datum::String(value) if is_binary_string_result_type(type_code) => {
            Some(BinaryResultCell::String(value.into_bytes()))
        }
        Datum::Bytes(value) if is_binary_string_result_type(type_code) => {
            Some(BinaryResultCell::String(value))
        }
        _ => None,
    }
}

fn integer_cell(value: i64, type_code: u8) -> Option<BinaryResultCell> {
    match type_code {
        TYPE_TINY => Some(BinaryResultCell::Tiny(value)),
        TYPE_SHORT | TYPE_YEAR => Some(BinaryResultCell::Short(value)),
        TYPE_INT24 | TYPE_LONG => Some(BinaryResultCell::Long(value)),
        TYPE_LONGLONG => Some(BinaryResultCell::LongLong(value)),
        _ => None,
    }
}

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
    let mut stream = BinaryResultSetStream::new(columns.clone(), options)
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
            // One Datum -> one binary cell, dispatched by the column type exactly
            // as Go's DumpBinaryRow switches on `columns[i].Type`: an integer
            // column picks the matching fixed width, a string column takes its
            // raw bytes.
            let cells = row
                .into_iter()
                .zip(&columns)
                .enumerate()
                .map(|(column, (datum, metadata))| {
                    datum_to_binary_cell(datum, metadata.type_code).ok_or_else(|| {
                        format!(
                            "prepared binary result column {column} datum does not match type {}",
                            metadata.type_code
                        )
                    })
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(|message| binary_failure(message, sink, false))?;
            let payload = stream
                .row_packet(&cells)
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
