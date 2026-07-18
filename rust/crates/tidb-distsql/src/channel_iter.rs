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

//! Dependency-closed iteration for one DistSQL response channel.
//!
//! TiDB's Go `selRespChannelIter` walks the chunks belonging to one
//! intermediate (or final) channel, skips empty chunks, and returns rows with
//! the channel index attached.  This leaf preserves that state machine over
//! already-owned rows.  It deliberately does not depend on tipb, TiDB's
//! chunk decoder, or a TiKV response channel; those boundaries are represented
//! by explicit [`ChannelIterError::Unsupported`] values instead of a partial
//! decoder.

use std::collections::VecDeque;
use std::error::Error;
use std::fmt;

/// A row returned by a [`ChannelIter`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChannelRow<T> {
    /// Index of the intermediate/final channel that produced the row.
    pub channel_index: usize,
    /// The already-decoded owned row.
    pub row: T,
}

impl<T> ChannelRow<T> {
    /// Creates a row with its source channel index.
    #[must_use]
    pub const fn new(channel_index: usize, row: T) -> Self {
        Self { channel_index, row }
    }

    /// Maps the owned row while retaining its source channel.
    #[must_use]
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> ChannelRow<U> {
        ChannelRow::new(self.channel_index, f(self.row))
    }
}

/// Capabilities intentionally left to the future response/decoder owners.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChannelIterUnsupported {
    /// Decoding a raw tipb `SelectResponse` is outside this leaf.
    RawTipbResponse,
    /// Decoding TiDB's default/chunk encodings is outside this leaf.
    ChunkDecoding,
    /// Receiving rows from a TiKV response channel is outside this leaf.
    TiKvResponseChannel,
}

impl fmt::Display for ChannelIterUnsupported {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::RawTipbResponse => "raw tipb response",
            Self::ChunkDecoding => "chunk decoding",
            Self::TiKvResponseChannel => "TiKV response channel",
        };
        f.write_str(name)
    }
}

/// Errors returned by a channel iterator.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ChannelIterError {
    /// The requested channel is not present in the response layout.
    InvalidChannel {
        /// Requested channel index.
        channel: usize,
        /// Number of channels available (intermediate channels plus final).
        available_channels: usize,
    },
    /// The caller crossed a boundary that this dependency-closed leaf does
    /// not own.
    Unsupported(ChannelIterUnsupported),
    /// An already-owned input failed while producing a row.
    Source(String),
}

impl ChannelIterError {
    /// Creates an owned source error.
    #[must_use]
    pub fn source(message: impl Into<String>) -> Self {
        Self::Source(message.into())
    }

    /// Creates the explicit raw tipb response boundary error.
    #[must_use]
    pub const fn unsupported_raw_tipb_response() -> Self {
        Self::Unsupported(ChannelIterUnsupported::RawTipbResponse)
    }

    /// Creates the explicit chunk-decoding boundary error.
    #[must_use]
    pub const fn unsupported_chunk_decoding() -> Self {
        Self::Unsupported(ChannelIterUnsupported::ChunkDecoding)
    }

    /// Creates the explicit TiKV response-channel boundary error.
    #[must_use]
    pub const fn unsupported_tikv_response_channel() -> Self {
        Self::Unsupported(ChannelIterUnsupported::TiKvResponseChannel)
    }
}

impl fmt::Display for ChannelIterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidChannel {
                channel,
                available_channels,
            } => write!(
                f,
                "invalid channel {channel} for response with {available_channels} channels"
            ),
            Self::Unsupported(capability) => {
                write!(f, "unsupported DistSQL channel capability: {capability}")
            }
            Self::Source(message) => f.write_str(message),
        }
    }
}

impl Error for ChannelIterError {}

/// Iterator over the already-decoded rows belonging to one response channel.
///
/// Each inner collection represents one response chunk.  Empty chunks are
/// skipped, while row order within and across chunks is preserved.  Entries
/// supplied through [`Self::from_results`] may carry a source error; the
/// failing entry is consumed and the next call resumes with the following
/// entry, which keeps the state transition explicit and deterministic.
#[derive(Debug)]
pub struct ChannelIter<T> {
    channel_index: usize,
    chunks: VecDeque<VecDeque<Result<T, ChannelIterError>>>,
    closed: bool,
}

impl<T> ChannelIter<T> {
    /// Creates an iterator from owned rows grouped into response chunks.
    ///
    /// This constructor does not perform channel-layout validation.  Use
    /// [`Self::try_new`] when the response's total channel count is known.
    #[must_use]
    pub fn new<I, Rows>(channel_index: usize, chunks: I) -> Self
    where
        I: IntoIterator<Item = Rows>,
        Rows: IntoIterator<Item = T>,
    {
        Self::from_results(
            channel_index,
            chunks
                .into_iter()
                .map(|rows| rows.into_iter().map(Ok::<T, ChannelIterError>)),
        )
    }

    /// Creates an iterator after validating its channel index.
    pub fn try_new<I, Rows>(
        channel_index: usize,
        available_channels: usize,
        chunks: I,
    ) -> Result<Self, ChannelIterError>
    where
        I: IntoIterator<Item = Rows>,
        Rows: IntoIterator<Item = T>,
    {
        if channel_index >= available_channels {
            return Err(ChannelIterError::InvalidChannel {
                channel: channel_index,
                available_channels,
            });
        }
        Ok(Self::new(channel_index, chunks))
    }

    /// Creates an iterator from one unchunked row sequence.
    #[must_use]
    pub fn from_rows(channel_index: usize, rows: impl IntoIterator<Item = T>) -> Self {
        Self::new(channel_index, [rows])
    }

    /// Creates an iterator from rows or source errors grouped into chunks.
    #[must_use]
    pub fn from_results<I, Rows>(channel_index: usize, chunks: I) -> Self
    where
        I: IntoIterator<Item = Rows>,
        Rows: IntoIterator<Item = Result<T, ChannelIterError>>,
    {
        Self {
            channel_index,
            chunks: chunks
                .into_iter()
                .map(|rows| rows.into_iter().collect())
                .collect(),
            closed: false,
        }
    }

    /// Returns the source channel index attached to every returned row.
    #[must_use]
    pub const fn channel(&self) -> usize {
        self.channel_index
    }

    /// Returns whether this channel has no rows or has been closed.
    #[must_use]
    pub fn is_drained(&self) -> bool {
        self.closed || self.chunks.iter().all(VecDeque::is_empty)
    }

    /// Returns the next owned row, skipping empty chunks.
    pub fn next_row(&mut self) -> Result<Option<ChannelRow<T>>, ChannelIterError> {
        if self.closed {
            return Ok(None);
        }

        loop {
            let Some(chunk) = self.chunks.front_mut() else {
                return Ok(None);
            };
            let Some(entry) = chunk.pop_front() else {
                self.chunks.pop_front();
                continue;
            };
            return match entry {
                Ok(row) => Ok(Some(ChannelRow::new(self.channel_index, row))),
                Err(error) => Err(error),
            };
        }
    }

    /// Closes the channel and drops all remaining owned rows.
    ///
    /// Closing is idempotent and does not produce a source error because this
    /// leaf owns no external transport resource.  A transport-backed owner
    /// can report its failure through [`ChannelIterError::Source`] before
    /// constructing or wrapping this iterator.
    pub fn close(&mut self) {
        self.closed = true;
        self.chunks.clear();
    }
}
