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

//! Bounded integer/real `APPROX_PERCENTILE` partial states from
//! `pkg/executor/aggfuncs/func_percentile.go`.
//!
//! The Go implementation collects non-NULL values, concatenates partial
//! slices in destination-then-source order, and selects ordinal rank
//! `ceil(P / 100 * N)` (capped at `N`) from the ordered values. This leaf
//! preserves those state transitions for signed integers and finite `f64`
//! values. Typed `EvalInt`/`EvalReal` coercion, chunk output, memory deltas,
//! decimal/time/duration variants, enum/set/bit string routing, and the
//! dependency-specific introselect implementation remain external.

use std::mem::size_of;

/// Returns the source percentile index for a non-empty value slice.
///
/// Go's `selection.Select` returns the zero-based index of the `k`th smallest
/// value, where `k = min(ceil(P / 100 * N), N)`. The aggregate validates its
/// percentage argument before reaching this helper; percentages at or below
/// zero use the first value here so the index remains representable.
#[must_use]
pub fn percentile_index(len: usize, percent: i64) -> Option<usize> {
    if len == 0 {
        return None;
    }
    let rank = ((len as f64) * (percent as f64 / 100.0)).ceil();
    let rank = if rank <= 1.0 {
        1
    } else {
        (rank as usize).min(len)
    };
    Some(rank - 1)
}

/// Source-shaped integer percentile partial state.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PercentileIntState {
    values: Vec<i64>,
}

impl PercentileIntState {
    /// Creates an empty integer percentile state.
    #[must_use]
    pub const fn new() -> Self {
        Self { values: Vec::new() }
    }

    /// Appends source non-NULL integer values in row order.
    pub fn update(&mut self, values: &[Option<i64>]) {
        self.values.extend(values.iter().flatten().copied());
    }

    /// Merges source values after destination values and clears the source.
    pub fn merge_from(&mut self, source: &mut Self) {
        self.values.append(&mut source.values);
    }

    /// Resets the source slice to empty.
    pub fn reset(&mut self) {
        self.values.clear();
    }

    /// Returns the selected ordinal value, or SQL NULL for an empty state.
    #[must_use]
    pub fn finish(&self, percent: i64) -> Option<i64> {
        let index = percentile_index(self.values.len(), percent)?;
        let mut ordered = self.values.clone();
        ordered.sort_unstable();
        ordered.get(index).copied()
    }

    /// Returns the number of collected non-NULL values.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns whether no non-NULL values were collected.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Returns the source slice allocation size for this leaf.
    #[must_use]
    pub const fn partial_state_size() -> usize {
        size_of::<Self>()
    }

    /// Returns the selected index after sorting the supplied integer values.
    /// This is the source `PercentileForTesting` boundary.
    pub fn select_index(values: &mut [i64], percent: i64) -> Option<usize> {
        let index = percentile_index(values.len(), percent)?;
        values.sort_unstable();
        Some(index)
    }
}

/// Source-shaped real percentile partial state.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PercentileRealState {
    values: Vec<f64>,
}

impl PercentileRealState {
    /// Creates an empty real percentile state.
    #[must_use]
    pub const fn new() -> Self {
        Self { values: Vec::new() }
    }

    /// Appends source non-NULL finite real values in row order.
    pub fn update(&mut self, values: &[Option<f64>]) {
        self.values.extend(values.iter().flatten().copied());
    }

    /// Merges source values after destination values and clears the source.
    pub fn merge_from(&mut self, source: &mut Self) {
        self.values.append(&mut source.values);
    }

    /// Resets the source slice to empty.
    pub fn reset(&mut self) {
        self.values.clear();
    }

    /// Returns the selected ordinal value, or SQL NULL for an empty state.
    #[must_use]
    pub fn finish(&self, percent: i64) -> Option<f64> {
        let index = percentile_index(self.values.len(), percent)?;
        let mut ordered = self.values.clone();
        ordered.sort_unstable_by(f64::total_cmp);
        ordered.get(index).copied()
    }

    /// Returns the number of collected non-NULL values.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns whether no non-NULL values were collected.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Returns the source slice allocation size for this leaf.
    #[must_use]
    pub const fn partial_state_size() -> usize {
        size_of::<Self>()
    }
}
