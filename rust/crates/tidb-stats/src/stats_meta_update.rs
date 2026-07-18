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

//! Statistics-meta update SQL assembly from
//! `pkg/statistics/handle/storage/update.go`.
//!
//! The Go owner separates locked updates from unlocked positive and negative
//! deltas before issuing its transaction-bound SQL. This leaf preserves that
//! ordering, tuple formatting, and signed overflow behavior while leaving
//! session execution, transaction ownership, and `variable.TableDelta`
//! conversion to the storage owner.

/// The exact update statement used by UpdateStatsMetaVerAndLastHistUpdateVer.
pub const UPDATE_STATS_META_VERSION_QUERY: &str =
    "update mysql.stats_meta set version=%?, last_stats_histograms_version=%? where table_id =%?";

/// Caller-owned scalar form of a Go `DeltaUpdate`.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct DeltaUpdate {
    /// Table or partition identifier whose statistics metadata changes.
    pub table_id: i64,
    /// Current row count supplied by the caller.
    pub count: i64,
    /// Signed row-count delta to persist.
    pub delta: i64,
    /// Whether the update belongs to the locked statistics table.
    pub is_locked: bool,
}

impl DeltaUpdate {
    /// Creates a source-shaped delta update without importing session types.
    #[must_use]
    pub const fn new(table_id: i64, count: i64, delta: i64, is_locked: bool) -> Self {
        Self {
            table_id,
            count,
            delta,
            is_locked,
        }
    }
}

/// SQL statements and cache invalidation IDs assembled for one source update.
#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct StatsMetaUpdateSql {
    /// `SELECT ... FOR UPDATE` statement for locked statistics rows.
    pub locked_select: Option<String>,
    /// `SELECT ... FOR UPDATE` statement for unlocked statistics rows.
    pub unlocked_select: Option<String>,
    /// Upsert statement for locked statistics rows.
    pub locked_insert: Option<String>,
    /// Upsert statement for positive unlocked deltas.
    pub unlocked_positive_insert: Option<String>,
    /// Upsert statement for negative unlocked deltas.
    pub unlocked_negative_insert: Option<String>,
    /// Unlocked table IDs whose stats cache entries must be invalidated.
    pub cache_invalidate_ids: Vec<i64>,
}

/// Builds the exact SQL text that Go's `UpdateStatsMeta` would execute.
///
/// The returned statements are descriptors only; callers still execute them
/// in one transaction and supply the source session context. Empty input
/// yields no statements, matching Go's early return.
#[must_use]
pub fn stats_meta_update_sql(start_ts: u64, updates: &[DeltaUpdate]) -> StatsMetaUpdateSql {
    let mut locked_ids = Vec::new();
    let mut unlocked_ids = Vec::new();
    let mut locked_values = Vec::new();
    let mut unlocked_positive_values = Vec::new();
    let mut unlocked_negative_values = Vec::new();
    let mut cache_invalidate_ids = Vec::new();

    for update in updates {
        if update.is_locked {
            locked_ids.push(update.table_id.to_string());
            locked_values.push(value_tuple(
                start_ts,
                update.table_id,
                update.count,
                update.delta,
            ));
            continue;
        }

        unlocked_ids.push(update.table_id.to_string());
        cache_invalidate_ids.push(update.table_id);
        if update.delta < 0 {
            // Go's unary minus on int64 wraps for MinInt64; preserve that
            // behavior instead of introducing a Rust debug-overflow panic.
            unlocked_negative_values.push(value_tuple(
                start_ts,
                update.table_id,
                update.count,
                update.delta.wrapping_neg(),
            ));
        } else {
            unlocked_positive_values.push(value_tuple(
                start_ts,
                update.table_id,
                update.count,
                update.delta,
            ));
        }
    }

    let mut result = StatsMetaUpdateSql {
        cache_invalidate_ids,
        ..StatsMetaUpdateSql::default()
    };
    if !locked_ids.is_empty() {
        result.locked_select = Some(format!(
            "select * from mysql.stats_table_locked where table_id in ({}) for update",
            locked_ids.join(",")
        ));
    }
    if !unlocked_ids.is_empty() {
        result.unlocked_select = Some(format!(
            "select * from mysql.stats_meta where table_id in ({}) for update",
            unlocked_ids.join(",")
        ));
    }
    if !locked_values.is_empty() {
        result.locked_insert = Some(format!(
            "insert into mysql.stats_table_locked (version, table_id, modify_count, count) values {} on duplicate key update version = values(version), modify_count = modify_count + values(modify_count), count = count + values(count)",
            locked_values.join(",")
        ));
    }
    if !unlocked_positive_values.is_empty() {
        result.unlocked_positive_insert = Some(format!(
            "insert into mysql.stats_meta (version, table_id, modify_count, count) values {} on duplicate key update version = values(version), modify_count = modify_count + values(modify_count), count = count + values(count)",
            unlocked_positive_values.join(",")
        ));
    }
    if !unlocked_negative_values.is_empty() {
        result.unlocked_negative_insert = Some(format!(
            "insert into mysql.stats_meta (version, table_id, modify_count, count) values {} on duplicate key update version = values(version), modify_count = modify_count + values(modify_count), count = if(count > values(count), count - values(count), 0)",
            unlocked_negative_values.join(",")
        ));
    }
    result
}

fn value_tuple(start_ts: u64, table_id: i64, count: i64, delta: i64) -> String {
    format!("({start_ts}, {table_id}, {count}, {delta})")
}

/// Arguments passed to `UpdateStatsMetaVerAndLastHistUpdateVer`'s query.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct StatsMetaVersionUpdate {
    /// Transaction start timestamp used for both version parameters.
    pub start_ts: u64,
    /// Physical table or partition identifier in the `WHERE` clause.
    pub physical_id: i64,
}

impl StatsMetaVersionUpdate {
    /// Creates the source-shaped query arguments.
    #[must_use]
    pub const fn new(start_ts: u64, physical_id: i64) -> Self {
        Self {
            start_ts,
            physical_id,
        }
    }

    /// Returns the three ordered SQL arguments `(startTS, startTS, physicalID)`.
    #[must_use]
    pub const fn parameters(self) -> (u64, u64, i64) {
        (self.start_ts, self.start_ts, self.physical_id)
    }
}
