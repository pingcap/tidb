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

//! Dependency-closed statistics-property arithmetic from
//! `pkg/planner/property/stats_info.go`.
//!
//! The Go `StatsInfo` also owns histogram handles and session-driven NDV
//! scaling. This leaf keeps row-count truncation and limit-derived NDV caps
//! over caller-supplied scalar maps, without reconstructing those owners.

use std::collections::BTreeMap;

/// Minimal statistics profile needed by `DeriveLimitStats`.
#[derive(Clone, Debug, PartialEq)]
pub struct StatsInfo {
    row_count: f64,
    col_ndvs: BTreeMap<i64, f64>,
}

impl StatsInfo {
    /// Creates a profile from row count and column NDVs.
    #[must_use]
    pub fn new(row_count: f64, col_ndvs: impl IntoIterator<Item = (i64, f64)>) -> Self {
        Self {
            row_count,
            col_ndvs: col_ndvs.into_iter().collect(),
        }
    }

    /// Returns the source row count.
    #[must_use]
    pub const fn row_count(&self) -> f64 {
        self.row_count
    }

    /// Returns a deterministic column-NDV map.
    #[must_use]
    pub const fn col_ndvs(&self) -> &BTreeMap<i64, f64> {
        &self.col_ndvs
    }

    /// Returns the source `int64(RowCount)` truncation toward zero.
    #[must_use]
    pub fn count(&self) -> i64 {
        self.row_count as i64
    }

    /// Derives limit statistics by capping row count and every column NDV.
    #[must_use]
    pub fn derive_limit_stats(&self, limit_count: f64) -> Self {
        let row_count = source_min(limit_count, self.row_count);
        let col_ndvs = self
            .col_ndvs
            .iter()
            .map(|(id, ndv)| (*id, source_min(*ndv, row_count)))
            .collect();
        Self {
            row_count,
            col_ndvs,
        }
    }
}

// Go's math.Min returns NaN for NaN inputs and otherwise follows ordered
// comparison semantics, including signed zero.
fn source_min(left: f64, right: f64) -> f64 {
    if left.is_nan() || right.is_nan() {
        return f64::NAN;
    }
    if left == 0.0 && right == 0.0 {
        return if left.is_sign_negative() || right.is_sign_negative() {
            -0.0
        } else {
            0.0
        };
    }
    if left < right {
        left
    } else {
        right
    }
}
