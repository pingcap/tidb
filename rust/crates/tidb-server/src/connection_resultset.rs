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

use tidb_protocol::ResultSetOptions;

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
