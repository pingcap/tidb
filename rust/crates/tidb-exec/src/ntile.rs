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

//! `NTILE` group assignment state from `pkg/executor/aggfuncs/func_ntile.go`.
//!
//! The Go implementation receives rows in update batches and emits one group
//! number per row. This compatibility leaf keeps the same five-field partial
//! state and quotient/remainder transitions for direct source/memory evidence.
//! The live executor derives the same buckets from canonical physical positions
//! in `window::ranking_runtime::WindowPartitionRuntime`, leaving typed chunk
//! output, broader argument coercion, and aggregate construction external.

use std::mem::size_of;

/// The source-shaped `NTILE` partial state.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NtilePartialState {
    current_index: u64,
    current_group: u64,
    remainder: u64,
    quotient: u64,
    row_count: u64,
}

/// Stateful `NTILE(n)` group assignment.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Ntile {
    divisor: u64,
    partial: NtilePartialState,
}

impl Ntile {
    /// Creates an `NTILE` state with its first group numbered one.
    #[must_use]
    pub const fn new(divisor: u64) -> Self {
        Self {
            divisor,
            partial: NtilePartialState {
                current_index: 0,
                current_group: 1,
                remainder: 0,
                quotient: 0,
                row_count: 0,
            },
        }
    }

    /// Resets row/group counters while preserving source quotient state.
    ///
    /// TiDB's Go reset path intentionally resets only `curIdx`,
    /// `curGroupIdx`, and `numRows`; the next update recomputes quotient and
    /// remainder when the divisor is nonzero.
    pub fn reset(&mut self) {
        self.partial.current_index = 0;
        self.partial.current_group = 1;
        self.partial.row_count = 0;
    }

    /// Adds one source update batch and refreshes quotient/remainder.
    pub fn update(&mut self, rows_in_batch: u64) {
        self.partial.row_count = self.partial.row_count.wrapping_add(rows_in_batch);
        if let Some(quotient) = self.partial.row_count.checked_div(self.divisor) {
            self.partial.quotient = quotient;
            self.partial.remainder = self.partial.row_count % self.divisor;
        }
    }

    /// Emits the next group number, or NULL for `NTILE(0)`.
    #[must_use]
    pub fn next_value(&mut self) -> Option<u64> {
        if self.divisor == 0 {
            return None;
        }

        let group = self.partial.current_group;
        self.partial.current_index = self.partial.current_index.wrapping_add(1);
        let mut current_max = self.partial.quotient;
        if self.partial.current_group <= self.partial.remainder {
            current_max = current_max.wrapping_add(1);
        }
        if self.partial.current_index == current_max {
            self.partial.current_index = 0;
            self.partial.current_group = self.partial.current_group.wrapping_add(1);
        }
        Some(group)
    }

    /// Returns the source partial-state allocation size.
    #[must_use]
    pub const fn partial_state_size() -> usize {
        size_of::<NtilePartialState>()
    }
}
