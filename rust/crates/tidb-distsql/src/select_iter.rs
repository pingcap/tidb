// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Dependency-closed DistSQL row iteration.
//!
//! The Go `SelectResultIter` combines rows from one coprocessor response's
//! intermediate channels and final channel.  This leaf keeps the part that is
//! useful before a TiKV client exists: an owned row, a source iterator, and
//! the source-shaped serial composition used by callers that have several
//! already-decoded result streams.  It intentionally does not define packet,
//! chunk, protobuf, or transport types.
//!
//! `None` is the Rust equivalent of Go's zero-valued `SelectResultRow` whose
//! `IsEmpty()` method reports that the iterator is drained.  Returning an
//! option removes the sentinel-row edge case while preserving the observable
//! source contract: empty sources are skipped and a fully drained serial
//! iterator keeps returning `Ok(None)`.

use std::error::Error;
use std::fmt;

/// A decoded row together with the source channel that produced it.
///
/// Go's `SelectResultRow` embeds a chunk row and carries `ChannelIndex`.  The
/// Rust rewrite owns the row value directly so a future decoder can choose its
/// own representation without coupling this iterator to `chunk::Row`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SelectResultRow<T> {
    /// Index of the intermediate/final result channel that produced the row.
    pub channel_index: usize,
    /// The owned decoded row.
    pub row: T,
}

impl<T> SelectResultRow<T> {
    /// Creates a row with its source channel index.
    #[must_use]
    pub const fn new(channel_index: usize, row: T) -> Self {
        Self { channel_index, row }
    }

    /// Maps the owned row while retaining its source channel.
    #[must_use]
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> SelectResultRow<U> {
        SelectResultRow::new(self.channel_index, f(self.row))
    }
}

/// Capabilities that remain outside this dependency-closed iterator leaf.
///
/// These names are deliberate boundaries.  A caller must provide the owner
/// for raw partial responses, chunk decoding, TiKV transport, or sorted
/// partition merging instead of silently receiving an invented implementation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnsupportedCapability {
    /// Go `SelectResult.NextRaw` and raw protobuf response bytes.
    NextRaw,
    /// Go `SelectResult.Next` and TiDB chunk decoding.
    Chunk,
    /// TiKV client/RPC response ownership.
    TiKvTransport,
    /// The heap merge used by Go `sortedSelectResults`.
    SortedHeap,
}

impl fmt::Display for UnsupportedCapability {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::NextRaw => "NextRaw",
            Self::Chunk => "chunk decoding",
            Self::TiKvTransport => "TiKV transport",
            Self::SortedHeap => "sorted result heap",
        };
        f.write_str(name)
    }
}

/// Errors returned by an owned result iterator.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SelectResultError {
    /// The requested operation belongs to a future owner.
    Unsupported(UnsupportedCapability),
    /// An underlying result source failed while producing or closing rows.
    Source(String),
    /// The canonical query cancellation interrupted row production.
    Cancelled,
}

impl SelectResultError {
    /// Creates an error preserving the source's owned error text.
    #[must_use]
    pub fn source(message: impl Into<String>) -> Self {
        Self::Source(message.into())
    }

    /// Creates an explicit capability-boundary error.
    #[must_use]
    pub const fn unsupported(capability: UnsupportedCapability) -> Self {
        Self::Unsupported(capability)
    }
}

impl fmt::Display for SelectResultError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unsupported(capability) => {
                write!(f, "unsupported DistSQL result capability: {capability}")
            }
            Self::Source(message) => f.write_str(message),
            Self::Cancelled => f.write_str("query cancelled by caller"),
        }
    }
}

impl Error for SelectResultError {}

/// A source of already-decoded owned result rows.
///
/// `next_row` returns `Ok(None)` when this source is exhausted.  Implementors
/// should not use an empty row sentinel: an empty result and a row containing
/// an empty payload are distinct values in Rust.  `close` is always allowed;
/// the serial wrapper calls it for every source, including sources after the
/// current one, matching Go's `serialSelectResults.Close` behavior.
pub trait SelectResultSource {
    /// The owned decoded row type.
    type Row;

    /// Returns the next row, or `Ok(None)` after this source is drained.
    fn next_row(&mut self) -> Result<Option<SelectResultRow<Self::Row>>, SelectResultError>;

    /// Returns the next row while forwarding the caller's remaining row
    /// budget to sources that can avoid over-producing decoded data.
    ///
    /// Existing sources remain compatible through the unbudgeted default. A
    /// zero budget consumes nothing and does not mark the source drained.
    fn next_row_with_required_rows(
        &mut self,
        required_rows: usize,
    ) -> Result<Option<SelectResultRow<Self::Row>>, SelectResultError> {
        if required_rows == 0 {
            return Ok(None);
        }
        self.next_row()
    }

    /// Closes this source and releases its owned resources.
    fn close(&mut self) -> Result<(), SelectResultError>;
}

/// Serial composition of homogeneous result sources.
///
/// This is the dependency-closed equivalent of Go's `serialSelectResults`.
/// It consumes every row from source zero before moving to source one, skips
/// empty sources, preserves the first error encountered by `next_row`, and calls
/// `close` on all sources.  If multiple closes fail, the last error is
/// returned, matching the source loop's final assignment semantics.
#[derive(Debug)]
pub struct SerialSelectResults<S> {
    select_results: Vec<S>,
    current: usize,
}

impl<S> SerialSelectResults<S> {
    /// Creates a serial result stream from source-ordered result sources.
    #[must_use]
    pub fn new(select_results: impl IntoIterator<Item = S>) -> Self {
        Self {
            select_results: select_results.into_iter().collect(),
            current: 0,
        }
    }

    /// Returns the index of the source currently being consumed.
    #[must_use]
    pub const fn current_source(&self) -> usize {
        self.current
    }

    /// Returns whether all sources have been drained.
    #[must_use]
    pub fn is_drained(&self) -> bool {
        self.current >= self.select_results.len()
    }
}

impl<S> SerialSelectResults<S>
where
    S: SelectResultSource,
{
    /// Returns the next row from the first non-drained source.
    pub fn next_row(&mut self) -> Result<Option<SelectResultRow<S::Row>>, SelectResultError> {
        self.next_row_with_required_rows(1)
    }

    /// Returns the next row while forwarding the caller's remaining row budget
    /// through the currently active source.
    pub fn next_row_with_required_rows(
        &mut self,
        required_rows: usize,
    ) -> Result<Option<SelectResultRow<S::Row>>, SelectResultError> {
        if required_rows == 0 {
            return Ok(None);
        }
        while self.current < self.select_results.len() {
            match self.select_results[self.current].next_row_with_required_rows(required_rows)? {
                Some(row) => return Ok(Some(row)),
                None => self.current += 1,
            }
        }
        Ok(None)
    }

    /// Closes every source and returns the last close error, if any.
    pub fn close(&mut self) -> Result<(), SelectResultError> {
        let mut error = None;
        for source in &mut self.select_results {
            if let Err(source_error) = source.close() {
                error = Some(source_error);
            }
        }
        error.map_or(Ok(()), Err)
    }
}

impl<S> SelectResultSource for SerialSelectResults<S>
where
    S: SelectResultSource,
{
    type Row = S::Row;

    fn next_row(&mut self) -> Result<Option<SelectResultRow<Self::Row>>, SelectResultError> {
        SerialSelectResults::next_row(self)
    }

    fn next_row_with_required_rows(
        &mut self,
        required_rows: usize,
    ) -> Result<Option<SelectResultRow<Self::Row>>, SelectResultError> {
        SerialSelectResults::next_row_with_required_rows(self, required_rows)
    }

    fn close(&mut self) -> Result<(), SelectResultError> {
        self.close()
    }
}

/// Returns the explicit boundary error for raw partial-result bytes.
#[must_use]
pub const fn unsupported_next_raw() -> SelectResultError {
    SelectResultError::unsupported(UnsupportedCapability::NextRaw)
}

/// Returns the explicit boundary error for chunk decoding.
#[must_use]
pub const fn unsupported_chunk() -> SelectResultError {
    SelectResultError::unsupported(UnsupportedCapability::Chunk)
}

/// Returns the explicit boundary error for TiKV transport.
#[must_use]
pub const fn unsupported_tikv_transport() -> SelectResultError {
    SelectResultError::unsupported(UnsupportedCapability::TiKvTransport)
}

/// Returns the explicit boundary error for sorted partition merging.
#[must_use]
pub const fn unsupported_sorted_heap() -> SelectResultError {
    SelectResultError::unsupported(UnsupportedCapability::SortedHeap)
}
