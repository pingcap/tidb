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

//! Rows returned by the live DistSQL response decoder.

/// A decoded row together with the source channel that produced it.
///
/// Go's `SelectResultRow` embeds a chunk row and carries `ChannelIndex`.  The
/// response decoder owns the datum row and preserves its output channel.
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
}
