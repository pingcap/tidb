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

//! `mysql.stats_meta` count metadata from
//! `pkg/statistics/handle/storage/read.go`.
//!
//! The Go helper selects one row by table ID, optionally appending `FOR
//! UPDATE`, and treats an empty result as a null/missing metadata row. This
//! leaf keeps the exact query selector and row-shape conversion over
//! caller-owned storage results. SQL execution, transactions, row decoding,
//! DDL concurrency, and statistics-handle lifecycle remain external.

/// Query used by `StatsMetaCountAndModifyCount`.
pub const STATS_META_COUNT_QUERY: &str =
    "select count, modify_count from mysql.stats_meta where table_id = %?";

/// Query used by `StatsMetaCountAndModifyCountForUpdate`.
pub const STATS_META_COUNT_FOR_UPDATE_QUERY: &str =
    "select count, modify_count from mysql.stats_meta where table_id = %? for update";

/// Count and modification metadata returned by the source helper.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StatsMetaCounts {
    /// Row count from `mysql.stats_meta.count`, represented as Go `int64`.
    pub count: i64,
    /// Modification count from `mysql.stats_meta.modify_count`.
    pub modify_count: i64,
    /// Whether no metadata row was returned.
    pub is_null: bool,
}

impl StatsMetaCounts {
    /// Returns the source's empty-result sentinel.
    #[must_use]
    pub const fn missing() -> Self {
        Self {
            count: 0,
            modify_count: 0,
            is_null: true,
        }
    }

    /// Returns a populated metadata result.
    #[must_use]
    pub const fn present(count: u64, modify_count: i64) -> Self {
        Self {
            // Go's uint64-to-int64 conversion wraps modulo 2^64; Rust's `as`
            // cast has the same two's-complement conversion semantics.
            count: count as i64,
            modify_count,
            is_null: false,
        }
    }
}

/// Selects the exact source SQL shape for normal or locking reads.
#[must_use]
pub const fn stats_meta_query(for_update: bool) -> &'static str {
    if for_update {
        STATS_META_COUNT_FOR_UPDATE_QUERY
    } else {
        STATS_META_COUNT_QUERY
    }
}

/// Converts the first caller-owned storage row, or the empty result, to the
/// source-shaped count/modify-count metadata.
#[must_use]
pub const fn stats_meta_counts(row: Option<(u64, i64)>) -> StatsMetaCounts {
    match row {
        Some((count, modify_count)) => StatsMetaCounts::present(count, modify_count),
        None => StatsMetaCounts::missing(),
    }
}
