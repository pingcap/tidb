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

//! Dependency-closed input-volume accounting from `executor.go`.
//!
//! TiDB's `nextIOAcc` counts rows and cells for one `Executor.Next` call. A
//! non-positive row count contributes nothing, while a positive row count
//! always contributes rows even when the column count is zero or negative.
//! This leaf preserves those transitions and the child/parent admission gate;
//! source atomics, executor provider/pool reuse, and RUV2 metric publication
//! remain external.

/// Returns `rows * cols` when both dimensions are positive, otherwise zero.
#[must_use]
pub fn calc_cell_count(rows: i64, cols: i64) -> i64 {
    if rows <= 0 || cols <= 0 {
        return 0;
    }
    rows * cols
}

/// Whether a child executor needs a local input accumulator.
#[must_use]
pub fn need_next_io_acc(
    track_ruv2: bool,
    has_parent_accumulator: bool,
    child_count: usize,
) -> bool {
    child_count > 0 && (track_ruv2 || has_parent_accumulator)
}

/// Source-shaped row/cell accumulator for one executor input batch.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NextIoAccumulator {
    rows: i64,
    cells: i64,
}

impl NextIoAccumulator {
    /// Creates an empty accumulator.
    #[must_use]
    pub const fn new() -> Self {
        Self { rows: 0, cells: 0 }
    }

    /// Resets rows and cells to zero for reuse by the next invocation.
    pub fn reset(&mut self) {
        self.rows = 0;
        self.cells = 0;
    }

    /// Adds one input batch using the source positive-row guard.
    pub fn add_input(&mut self, rows: i64, cols: i64) {
        if rows <= 0 {
            return;
        }
        self.rows = self.rows.wrapping_add(rows);
        self.cells = self.cells.wrapping_add(calc_cell_count(rows, cols));
    }

    /// Returns the accumulated input row count.
    #[must_use]
    pub const fn rows(&self) -> i64 {
        self.rows
    }

    /// Returns the accumulated input cell count.
    #[must_use]
    pub const fn cells(&self) -> i64 {
        self.cells
    }
}
