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

//! `LAG`/`LEAD` row-offset state from `pkg/executor/aggfuncs/func_lead_lag.go`.
//!
//! The Go implementation buffers the rows in a partition, then evaluates
//! either the row at a physical offset or a default expression against the
//! current row. This public source-shaped state is retained as a compatibility
//! and memory-evidence seam. The live executor instead consumes
//! `window::ranking_runtime::WindowPartitionRuntime`, which co-owns these
//! selections with ranking geometry and NTILE positions. Expression evaluation
//! and chunk representation remain outside this compatibility seam.

use std::mem::size_of;

/// The direction of a row-offset window function.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LeadLagDirection {
    /// Read a row before the current row.
    Lag,
    /// Read a row after the current row.
    Lead,
}

/// The source fallback expression for an out-of-range offset.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum LeadLagDefault {
    /// No third argument was supplied; the result is NULL.
    #[default]
    Null,
    /// Evaluate the third argument against the current row.
    CurrentRow,
}

/// The dependency-closed output selection for one partition row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LeadLagSelection {
    /// Evaluate the value argument against this buffered row.
    Source(usize),
    /// Evaluate the default argument against this current row.
    Default(usize),
    /// Emit NULL because no default argument was supplied.
    Null,
}

/// The source-shaped `partialResult4LeadLag` state.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LeadLagPartialState {
    rows: Vec<usize>,
    current_index: u64,
}

/// Stateful `LAG`/`LEAD` row selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LeadLag {
    direction: LeadLagDirection,
    offset: u64,
    default: LeadLagDefault,
    partial: LeadLagPartialState,
}

impl LeadLag {
    /// Creates a row-offset selector with an already-validated offset.
    #[must_use]
    pub const fn new(direction: LeadLagDirection, offset: u64, default: LeadLagDefault) -> Self {
        Self {
            direction,
            offset,
            default,
            partial: LeadLagPartialState {
                rows: Vec::new(),
                current_index: 0,
            },
        }
    }

    /// Resets the buffered rows and current-row cursor.
    pub fn reset(&mut self) {
        self.partial.rows.clear();
        self.partial.current_index = 0;
    }

    /// Appends one source update batch of sequential opaque row handles.
    ///
    /// The returned memory delta charges one Rust-native `usize` row handle per
    /// appended row. Go instead charges one 16-byte `chunk.Row`; preserving
    /// that representation and accounting remains outside this partial seam.
    pub fn update(&mut self, rows_in_batch: usize) -> usize {
        let first = self.partial.rows.len();
        self.update_rows(first..first.saturating_add(rows_in_batch))
    }

    /// Appends the physical row handles owned by one executor partition batch.
    ///
    /// Returning logical handle bytes, rather than `Vec` capacity growth,
    /// follows the shape of `UpdatePartialResult` accounting without claiming
    /// byte parity: a Rust handle is 8 bytes on the supported 64-bit target,
    /// while the source Go `chunk.Row` is 16 bytes.
    pub fn update_rows(&mut self, rows: impl IntoIterator<Item = usize>) -> usize {
        let before = self.partial.rows.len();
        self.partial.rows.extend(rows);
        self.partial
            .rows
            .len()
            .saturating_sub(before)
            .saturating_mul(Self::buffered_row_size())
    }

    /// Selects the source or fallback row for the next partition position.
    #[must_use]
    pub fn next_selection(&mut self) -> Option<LeadLagSelection> {
        let current = usize::try_from(self.partial.current_index).ok()?;
        if current >= self.partial.rows.len() {
            return None;
        }

        let target = match self.direction {
            LeadLagDirection::Lag => usize::try_from(self.offset)
                .ok()
                .and_then(|offset| current.checked_sub(offset)),
            // Go adds the two uint64 values before comparing the result with
            // len(rows), so overflow wraps and may address an earlier row.
            // Keep the bounds check after that source-shaped addition.
            LeadLagDirection::Lead => self
                .partial
                .current_index
                .wrapping_add(self.offset)
                .try_into()
                .ok()
                .filter(|&index| index < self.partial.rows.len()),
        };
        let selection = match target {
            Some(index) => LeadLagSelection::Source(self.partial.rows[index]),
            None => match self.default {
                LeadLagDefault::Null => LeadLagSelection::Null,
                LeadLagDefault::CurrentRow => LeadLagSelection::Default(self.partial.rows[current]),
            },
        };
        self.partial.current_index = self.partial.current_index.wrapping_add(1);
        Some(selection)
    }

    /// Returns the source partial-state allocation size.
    #[must_use]
    pub const fn partial_state_size() -> usize {
        size_of::<LeadLagPartialState>()
    }

    /// Returns the Rust-native logical charge for one buffered row handle.
    #[must_use]
    pub const fn buffered_row_size() -> usize {
        size_of::<usize>()
    }

    /// Returns the logical memory currently owned by buffered row handles.
    #[must_use]
    pub fn buffered_row_memory(&self) -> usize {
        self.partial
            .rows
            .len()
            .saturating_mul(Self::buffered_row_size())
    }

    /// Returns the next physical partition position the cursor will consume.
    #[must_use]
    pub fn current_position(&self) -> Option<usize> {
        usize::try_from(self.partial.current_index).ok()
    }
}
