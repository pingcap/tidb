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

use crate::{
    append_length_encoded_int, encode_eof_packet, encode_text_row, ColumnInfo, EofPacket,
    ResultSetOptions,
};

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
            column.dump(&mut payload);
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
        let values = row.iter().map(|value| value.as_deref()).collect::<Vec<_>>();
        let payload = encode_text_row(&values);
        self.rows += 1;
        Ok(payload)
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
            warnings: self.options.warnings,
            status_flags: self.options.status_flags,
            deprecate_eof: self.options.deprecate_eof,
            protocol_41: self.options.protocol_41,
            info: Vec::new(),
        }
    }
}
