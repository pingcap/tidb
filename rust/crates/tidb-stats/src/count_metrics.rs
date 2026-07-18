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

//! Histogram count arithmetic from `pkg/statistics/histogram.go`.
//!
//! This value object starts with the source's already-materialized null count,
//! last-bucket count, and bucket-presence bit. It does not own histogram
//! buckets, Datum values, TopN/CMSketch counts, or planner decisions.

/// Count metadata needed by the source histogram row-count helpers.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HistogramCountSummary {
    /// Whether the source histogram has at least one bucket.
    pub has_buckets: bool,
    /// Count in the last bucket when `has_buckets` is true.
    pub last_bucket_count: i64,
    /// Number of null values.
    pub null_count: i64,
}

impl HistogramCountSummary {
    /// Creates count metadata from the source histogram boundary facts.
    #[must_use]
    pub const fn new(has_buckets: bool, last_bucket_count: i64, null_count: i64) -> Self {
        Self {
            has_buckets,
            last_bucket_count,
            null_count,
        }
    }

    /// Returns the source non-null count boundary.
    #[must_use]
    pub const fn not_null_count(self) -> f64 {
        if self.has_buckets {
            self.last_bucket_count as f64
        } else {
            0.0
        }
    }

    /// Returns the source histogram total row count.
    #[must_use]
    pub const fn total_row_count(self) -> f64 {
        self.not_null_count() + self.null_count as f64
    }

    /// Returns the absolute difference from a realtime row count.
    #[must_use]
    pub fn abs_row_count_difference(self, realtime_row_count: i64) -> f64 {
        ((realtime_row_count as f64) - self.total_row_count()).abs()
    }

    /// Returns the source post-analyze data-increase factor.
    #[must_use]
    pub fn increase_factor(self, realtime_row_count: i64) -> f64 {
        let column_count = self.total_row_count();
        if column_count == 0.0 {
            1.0
        } else {
            realtime_row_count as f64 / column_count
        }
    }
}
