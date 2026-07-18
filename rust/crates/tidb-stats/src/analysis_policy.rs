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

//! Table analysis eligibility predicates from `pkg/statistics/table.go`.
//!
//! The source threshold is mutable global configuration. Rust therefore takes
//! the caller-owned threshold explicitly and keeps this leaf free of cache,
//! scheduler, handle, and table-schema state.

/// Source's default `AutoAnalyzeMinCnt` value.
pub const DEFAULT_AUTO_ANALYZE_MIN_COUNT: i64 = 1_000;

/// Returns whether a table has a valid last-analyze timestamp.
#[must_use]
pub const fn table_is_analyzed(last_analyze_version: u64) -> bool {
    last_analyze_version > 0
}

/// Returns whether an optional table has enough realtime rows for auto-analyze.
///
/// `None` models the source's nil receiver, which returns false.
#[must_use]
pub fn meets_auto_analyze_min_count(
    realtime_count: Option<i64>,
    auto_analyze_min_count: i64,
) -> bool {
    realtime_count.is_some_and(|count| count >= auto_analyze_min_count)
}

/// Returns whether an optional table is eligible for analysis.
#[must_use]
pub fn is_eligible_for_analysis(
    realtime_count: Option<i64>,
    pseudo: bool,
    auto_analyze_min_count: i64,
) -> bool {
    meets_auto_analyze_min_count(realtime_count, auto_analyze_min_count) && !pseudo
}
