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

//! DDL statistics count/modify-count delta SQL descriptors.
//!
//! The Go DDL handler first checks locked statistics, then either inserts a
//! missing `stats_meta` row or updates an existing row after adding the
//! deltas. This leaf preserves those three branch-specific statements and
//! argument order while leaving lock discovery, row reads, and SQL execution
//! to the DDL/storage owner.

/// Upsert used when a table is present in `mysql.stats_table_locked`.
pub const LOCKED_STATS_DELTA_QUERY: &str = "INSERT INTO mysql.stats_table_locked (version, count, modify_count, table_id) VALUES (%?, %?, %?, %?) ON DUPLICATE KEY UPDATE version = VALUES(version), count = count + VALUES(count), modify_count = modify_count + VALUES(modify_count)";

/// Insert used when an unlocked table has no `mysql.stats_meta` row.
pub const MISSING_STATS_META_DELTA_QUERY: &str = "INSERT INTO mysql.stats_meta (version, count, modify_count, table_id) VALUES (%?, GREATEST(0, %?), GREATEST(0, %?), %?)";

/// Update used when an unlocked table already has a `mysql.stats_meta` row.
pub const EXISTING_STATS_META_DELTA_QUERY: &str = "UPDATE mysql.stats_meta SET version = %?, count = GREATEST(0, %?), modify_count = GREATEST(0, %?) WHERE table_id = %?";

/// One source-shaped SQL statement and its ordered scalar arguments.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct DdlStatsDeltaUpdate {
    /// Exact statement selected by the locked/missing/existing branch.
    pub query: &'static str,
    /// Transaction start timestamp passed as the first `%?` argument.
    pub start_ts: u64,
    /// Count delta, or existing count plus count delta for the update branch.
    pub count_value: i64,
    /// Modify-count delta, or existing modify count plus its delta.
    pub modify_count_value: i64,
    /// Table or partition identifier passed as the final `%?` argument.
    pub table_id: i64,
}

impl DdlStatsDeltaUpdate {
    /// Returns arguments in the exact order used by the Go `util.Exec` call.
    #[must_use]
    pub const fn parameters(self) -> (u64, i64, i64, i64) {
        (
            self.start_ts,
            self.count_value,
            self.modify_count_value,
            self.table_id,
        )
    }
}

/// Builds the exact DDL stats-delta statement for one source branch.
///
/// `existing_counts` is the result of the caller-owned `stats_meta ... FOR
/// UPDATE` read. It is ignored for locked tables, absent selects the missing
/// row `INSERT`, and present selects the nonnegative-clamped `UPDATE`. Go's
/// signed `int64` additions wrap; `wrapping_add` keeps that behavior at
/// synthetic overflow boundaries instead of introducing a debug panic.
#[must_use]
pub fn ddl_stats_delta_update(
    is_locked: bool,
    existing_counts: Option<(i64, i64)>,
    start_ts: u64,
    table_id: i64,
    count_delta: i64,
    modify_count_delta: i64,
) -> DdlStatsDeltaUpdate {
    let (query, count_value, modify_count_value) = if is_locked {
        (LOCKED_STATS_DELTA_QUERY, count_delta, modify_count_delta)
    } else if let Some((count, modify_count)) = existing_counts {
        (
            EXISTING_STATS_META_DELTA_QUERY,
            count.wrapping_add(count_delta),
            modify_count.wrapping_add(modify_count_delta),
        )
    } else {
        (
            MISSING_STATS_META_DELTA_QUERY,
            count_delta,
            modify_count_delta,
        )
    };

    DdlStatsDeltaUpdate {
        query,
        start_ts,
        count_value,
        modify_count_value,
        table_id,
    }
}
