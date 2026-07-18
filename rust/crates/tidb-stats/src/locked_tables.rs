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

//! Statistics-table lock filtering from
//! `pkg/statistics/handle/lockstats/query_lock.go`.
//!
//! The Go owner loads locked IDs through a SQL executor, then filters a caller
//! list for fast membership checks. This leaf keeps only the deterministic
//! filter and query marker; session, row decoding, and lock/unlock mutations
//! remain external boundaries.

use std::collections::HashSet;

/// Query used by the source lock-status loader.
pub const SELECT_LOCKED_TABLES_SQL: &str = "SELECT table_id FROM mysql.stats_table_locked";

/// Returns the requested table IDs that are present in the locked-ID set.
///
/// Hash-set output matches the source map's set-valued result: duplicate input
/// IDs collapse, and an empty locked set produces an empty result.
#[must_use]
pub fn get_locked_tables(table_locked: &HashSet<i64>, table_ids: &[i64]) -> HashSet<i64> {
    if table_locked.is_empty() {
        return HashSet::new();
    }
    table_ids
        .iter()
        .copied()
        .filter(|table_id| table_locked.contains(table_id))
        .collect()
}
