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

//! Dependency-closed integer group-boundary state from
//! `pkg/executor/internal/vecgroupchecker/vec_group_checker.go`.
//!
//! The Go checker evaluates expressions over a chunk, then records the end
//! offset of every consecutive group and exposes a cursor over those offsets.
//! This leaf keeps the source's integer/null equality and cross-chunk boundary
//! behavior typed. Expression evaluation, chunk allocation, datum/key codec,
//! collations, non-integer types, and stream-aggregation wiring remain
//! external.

use std::fmt;

/// Errors for the source contract's non-empty chunk precondition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IntGroupError {
    /// `SplitIntoGroups` is only called after the executor fetched a row.
    EmptyChunk,
}

impl fmt::Display for IntGroupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyChunk => f.write_str("group checker requires a non-empty chunk"),
        }
    }
}

impl std::error::Error for IntGroupError {}

/// Integer-key group offsets and the cursor used to consume them.
#[derive(Clone, Debug, Default)]
pub struct IntGroupChecker {
    /// The final key from the previous chunk, used for boundary continuity.
    previous_last: Option<Option<i64>>,
    /// End offset (exclusive) for each group in the current chunk.
    group_offsets: Vec<usize>,
    /// Next group consumed by [`Self::next_group`].
    next_group_id: usize,
}

impl IntGroupChecker {
    /// Creates an empty checker.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Splits one non-empty integer-key chunk into consecutive groups.
    ///
    /// The return value preserves `isFirstGroupSameAsPrev`: it is true only
    /// when the first key in this chunk equals the final key of the preceding
    /// chunk. `None` is the SQL NULL key and compares equal only to `None`.
    pub fn split_into_groups(&mut self, values: &[Option<i64>]) -> Result<bool, IntGroupError> {
        let Some(first) = values.first().copied() else {
            return Err(IntGroupError::EmptyChunk);
        };

        // Go's SplitIntoGroups calls Reset before evaluating each chunk. The
        // previous chunk's final key intentionally survives that reset.
        self.group_offsets.clear();
        self.next_group_id = 0;

        let first_same_as_previous = self.previous_last == Some(first);
        for (index, pair) in values.windows(2).enumerate() {
            if pair[0] != pair[1] {
                self.group_offsets.push(index + 1);
            }
        }
        self.group_offsets.push(values.len());
        self.previous_last = Some(values[values.len() - 1]);
        Ok(first_same_as_previous)
    }

    /// Returns the next `(begin, end)` row range, or `None` when exhausted.
    #[must_use]
    pub fn next_group(&mut self) -> Option<(usize, usize)> {
        let end = *self.group_offsets.get(self.next_group_id)?;
        let begin = if self.next_group_id == 0 {
            0
        } else {
            self.group_offsets[self.next_group_id - 1]
        };
        self.next_group_id += 1;
        Some((begin, end))
    }

    /// Returns true when every current-chunk group has been consumed.
    #[must_use]
    pub fn is_exhausted(&self) -> bool {
        self.next_group_id >= self.group_offsets.len()
    }

    /// Clears current-chunk offsets and cursor state.
    ///
    /// As in the source `Reset`, the previous chunk's final key is retained so
    /// the next split still reports cross-chunk group continuity.
    pub fn reset(&mut self) {
        self.group_offsets.clear();
        self.next_group_id = 0;
    }

    /// Returns the number of groups in the current chunk.
    #[must_use]
    pub fn group_count(&self) -> usize {
        self.group_offsets.len()
    }
}
