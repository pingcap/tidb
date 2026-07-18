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

//! Global statistics merge layout from
//! `pkg/statistics/handle/globalstats/global_stats.go`.
//!
//! The Go constructor allocates one nil slot for each requested histogram in
//! the histogram, CMSketch, TopN, and FMSketch arrays, while leaving counts at
//! zero and missing-partition metadata nil. This leaf preserves that shape
//! without importing statistics payloads, partition metadata, or merge code.

/// Dependency-closed representation of Go's `GlobalStats` zero layout.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct GlobalStatsLayout {
    /// Number of histogram/statistics slots requested by the caller.
    pub num: usize,
    /// Nil histogram pointers allocated by the Go constructor.
    pub histogram_slots: Vec<Option<()>>,
    /// Nil CMSketch pointers allocated by the Go constructor.
    pub cmsketch_slots: Vec<Option<()>>,
    /// Nil TopN pointers allocated by the Go constructor.
    pub topn_slots: Vec<Option<()>>,
    /// Nil FMSketch pointers allocated by the Go constructor.
    pub fmsketch_slots: Vec<Option<()>>,
    /// Go's nil `MissingPartitionStats` slice before any merge appends.
    pub missing_partition_stats: Option<Vec<String>>,
    /// Initial aggregate row count.
    pub count: i64,
    /// Initial aggregate modify count.
    pub modify_count: i64,
}

/// Creates the source-shaped zero layout for `hist_count` statistics slots.
#[must_use]
pub fn new_global_stats_layout(hist_count: usize) -> GlobalStatsLayout {
    let nil_slots = || vec![None; hist_count];
    GlobalStatsLayout {
        num: hist_count,
        histogram_slots: nil_slots(),
        cmsketch_slots: nil_slots(),
        topn_slots: nil_slots(),
        fmsketch_slots: nil_slots(),
        missing_partition_stats: None,
        count: 0,
        modify_count: 0,
    }
}
