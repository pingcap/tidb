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

//! Dependency-closed integer state transitions for FIRST_VALUE, LAST_VALUE,
//! and NTH_VALUE from `pkg/executor/aggfuncs/func_value.go`.
//!
//! The Go window functions evaluate only the first row, the last row, or the
//! configured one-based row across update batches. A selected NULL remains a
//! selected NULL, while an empty/unreached state emits SQL NULL as well. This
//! leaf accepts already-evaluated `Option<i64>` values so the evaluator,
//! typed value storage, memory deltas, chunk output, and window dispatch remain
//! external.

use std::mem::size_of;

/// Source-shaped FIRST_VALUE state for an already-evaluated integer value.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FirstValueIntState {
    selected: bool,
    value: Option<i64>,
}

impl FirstValueIntState {
    /// Creates an unset first-value state.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            selected: false,
            value: None,
        }
    }

    /// Captures the first row of the first non-empty batch, including NULL.
    pub fn update(&mut self, rows: &[Option<i64>]) {
        if !self.selected {
            if let Some(value) = rows.first() {
                self.selected = true;
                self.value = *value;
            }
        }
    }

    /// Resets the state to its source initial condition.
    pub fn reset(&mut self) {
        self.selected = false;
        self.value = None;
    }

    /// Returns the selected value; selected NULL and unset both return None.
    #[must_use]
    pub const fn result(&self) -> Option<i64> {
        self.value
    }

    /// Reports whether a row was selected, even if it was NULL.
    #[must_use]
    pub const fn is_selected(&self) -> bool {
        self.selected
    }

    /// Returns the source partial-state size without typed evaluator storage.
    #[must_use]
    pub const fn partial_state_size() -> usize {
        size_of::<Self>()
    }
}

/// Source-shaped LAST_VALUE state for an already-evaluated integer value.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LastValueIntState {
    selected: bool,
    value: Option<i64>,
}

impl LastValueIntState {
    /// Creates an unset last-value state.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            selected: false,
            value: None,
        }
    }

    /// Captures the last row of each non-empty batch, including NULL.
    pub fn update(&mut self, rows: &[Option<i64>]) {
        if let Some(value) = rows.last() {
            self.selected = true;
            self.value = *value;
        }
    }

    /// Resets the state to its source initial condition.
    pub fn reset(&mut self) {
        self.selected = false;
        self.value = None;
    }

    /// Returns the selected value; selected NULL and unset both return None.
    #[must_use]
    pub const fn result(&self) -> Option<i64> {
        self.value
    }

    /// Reports whether a row was selected, even if it was NULL.
    #[must_use]
    pub const fn is_selected(&self) -> bool {
        self.selected
    }

    /// Returns the source partial-state size without typed evaluator storage.
    #[must_use]
    pub const fn partial_state_size() -> usize {
        size_of::<Self>()
    }
}

/// Source-shaped NTH_VALUE state for an already-evaluated integer value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NthValueIntState {
    nth: u64,
    seen_rows: u64,
    selected: Option<i64>,
    reached: bool,
}

impl NthValueIntState {
    /// Creates a one-based NTH_VALUE state. `nth == 0` is source-invalid and
    /// therefore remains an always-NULL result.
    #[must_use]
    pub const fn new(nth: u64) -> Self {
        Self {
            nth,
            seen_rows: 0,
            selected: None,
            reached: false,
        }
    }

    /// Captures the configured row when it falls inside this update batch.
    pub fn update(&mut self, rows: &[Option<i64>]) {
        if self.nth == 0 {
            return;
        }
        let num_rows = rows.len() as u64;
        if self.nth > self.seen_rows {
            let offset = self.nth - self.seen_rows;
            if offset <= num_rows {
                self.selected = rows[(offset - 1) as usize];
                self.reached = true;
            }
        }
        self.seen_rows = self.seen_rows.wrapping_add(num_rows);
    }

    /// Resets seen-row and selected-value state.
    pub fn reset(&mut self) {
        self.seen_rows = 0;
        self.selected = None;
        self.reached = false;
    }

    /// Returns the selected value only after the configured row is reached.
    #[must_use]
    pub const fn result(&self) -> Option<i64> {
        if self.nth == 0 || !self.reached {
            None
        } else {
            self.selected
        }
    }

    /// Returns the source count of rows observed across update batches.
    #[must_use]
    pub const fn seen_rows(&self) -> u64 {
        self.seen_rows
    }

    /// Returns the source partial-state size without typed evaluator storage.
    #[must_use]
    pub const fn partial_state_size() -> usize {
        size_of::<Self>()
    }
}
