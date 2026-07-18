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

//! Predicate-column usage query shapes from
//! `pkg/statistics/handle/usage/predicatecolumn/predicate_column.go`.
//!
//! This leaf keeps the exact SQL markers and source-ordered column-ID argument
//! formatting. Session execution, schema lookup, row/time decoding, and
//! cleanup persistence remain external boundaries.

/// Query used to load all persisted predicate-column usage rows.
pub const LOAD_COLUMN_STATS_USAGE_QUERY: &str = "SELECT table_id, column_id, CONVERT_TZ(last_used_at, @@TIME_ZONE, '+00:00'), CONVERT_TZ(last_analyzed_at, @@TIME_ZONE, '+00:00') FROM mysql.column_stats_usage";

/// Query used to load persisted predicate-column usage rows for one table.
pub const LOAD_COLUMN_STATS_USAGE_FOR_TABLE_QUERY: &str = "SELECT table_id, column_id, CONVERT_TZ(last_used_at, @@TIME_ZONE, '+00:00'), CONVERT_TZ(last_analyzed_at, @@TIME_ZONE, '+00:00') FROM mysql.column_stats_usage WHERE table_id = %?";

/// Query used to select non-null predicate columns for one table.
pub const GET_PREDICATE_COLUMNS_QUERY: &str = "SELECT column_id, CONVERT_TZ(last_used_at, @@TIME_ZONE, '+00:00') FROM mysql.column_stats_usage WHERE table_id = %? AND last_used_at IS NOT NULL";

/// Query used to remove usage rows for columns that no longer exist.
pub const CLEANUP_DROPPED_COLUMN_STATS_USAGE_QUERY: &str =
    "DELETE FROM mysql.column_stats_usage WHERE table_id = %? AND column_id NOT IN (%?)";

/// Formats the ordered column-ID argument passed to the source `%?` list
/// placeholder in `cleanupDroppedColumnStatsUsage`.
#[must_use]
pub fn cleanup_column_ids_argument(column_ids: &[i64]) -> String {
    column_ids
        .iter()
        .map(i64::to_string)
        .collect::<Vec<_>>()
        .join(",")
}
