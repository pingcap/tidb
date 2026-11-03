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

//! Go `pkg/statistics/handle/usage/session_stats_collect.go`: dumping the
//! session-collected stats deltas (`DumpStatsDeltaToKV`) and column usage
//! (`DumpColStatsUsageEntries`) into `mysql.stats_meta` and
//! `mysql.column_stats_usage`.

use std::collections::BTreeMap;
use std::time::SystemTime;

use tidb_sqlescape::{must_escape_sql_text, SqlArg};
use tidb_sqlexec::SqlExecError;
use tidb_stats_handle_util::{exec_rows, StatsSessionContext};

use tidb_datatype::{
    Datum, Time,
};
use tidb_model::TableItemID;
use tidb_txnkv::transaction::BufferMutation;

use tidb_infoschema::cluster_catalog::MetaSnapshot;
use tidb_infoschema::SessionExtendedInfoSchema;
use tidb_syssession::system_row_write::defaults_row;
use tidb_table::origin_default::RowEncodeError;
use tidb_stats_handle_storage::cluster_stats_write::*;
use tidb_syssession::stats_table_rows::*;

/// One SQL statement in pinned Go `storage.UpdateStatsMeta`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StatsDeltaStatement {
    /// Locks existing `stats_table_locked` rows selected by table ID.
    LockLocked(Vec<i64>),
    /// Locks existing `stats_meta` rows selected by table ID.
    LockUnlocked(Vec<i64>),
    /// Upserts every locked-table delta in one statement.
    UpsertLocked(Vec<crate::DeltaUpdate>),
    /// Upserts nonnegative unlocked deltas in one statement.
    UpsertUnlockedPositive(Vec<crate::DeltaUpdate>),
    /// Upserts negative unlocked deltas in one statement.
    UpsertUnlockedNegative(Vec<crate::DeltaUpdate>),
}

/// Builds pinned Go `storage.UpdateStatsMeta`'s nonempty statements in order.
#[must_use]
pub fn stats_delta_statements(
    updates: &[crate::DeltaUpdate],
) -> Vec<StatsDeltaStatement> {
    let mut locked_ids = Vec::new();
    let mut unlocked_ids = Vec::new();
    let mut locked = Vec::new();
    let mut positive = Vec::new();
    let mut negative = Vec::new();
    for update in updates {
        if update.is_locked {
            locked_ids.push(update.table_id);
            locked.push(*update);
        } else {
            unlocked_ids.push(update.table_id);
            if update.delta.delta < 0 {
                negative.push(*update);
            } else {
                positive.push(*update);
            }
        }
    }
    let mut statements = Vec::with_capacity(5);
    if !locked_ids.is_empty() {
        statements.push(StatsDeltaStatement::LockLocked(locked_ids));
    }
    if !unlocked_ids.is_empty() {
        statements.push(StatsDeltaStatement::LockUnlocked(unlocked_ids));
    }
    if !locked.is_empty() {
        statements.push(StatsDeltaStatement::UpsertLocked(locked));
    }
    if !positive.is_empty() {
        statements.push(StatsDeltaStatement::UpsertUnlockedPositive(positive));
    }
    if !negative.is_empty() {
        statements.push(StatsDeltaStatement::UpsertUnlockedNegative(negative));
    }
    statements
}

/// Plans one pinned Go `storage.UpdateStatsMeta` statement.
pub fn plan_stats_delta_statement<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &SessionExtendedInfoSchema,
    statement: &StatsDeltaStatement,
    version: u64,
    now: Time,
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
) -> Result<StatsWritePlan, StatsWriteError> {
    let (table_name, updates, negative) = match statement {
        StatsDeltaStatement::LockLocked(table_ids)
        | StatsDeltaStatement::LockUnlocked(table_ids) => {
            let table_name = if matches!(statement, StatsDeltaStatement::LockLocked(_)) {
                "stats_table_locked"
            } else {
                "stats_meta"
            };
            let table = locate(catalog, table_name)?;
            let table = table.read();
            let mut plan = StatsWritePlan::default();
            for table_id in table_ids {
                let rows = StatsRows::open(snapshot, &table, &["table_id"], *table_id)?;
                for (key, _) in rows.existing.into_values() {
                    plan.mutations
                        .push(BufferMutation::lock_only(key).map_err(|error| {
                            StatsWriteError::Encode(RowEncodeError(error.to_string()))
                        })?);
                }
            }
            return Ok(plan);
        }
        StatsDeltaStatement::UpsertLocked(updates) => {
            ("stats_table_locked", updates.as_slice(), false)
        }
        StatsDeltaStatement::UpsertUnlockedPositive(updates) => {
            ("stats_meta", updates.as_slice(), false)
        }
        StatsDeltaStatement::UpsertUnlockedNegative(updates) => {
            ("stats_meta", updates.as_slice(), true)
        }
    };
    let mut grouped = BTreeMap::<i64, Vec<crate::TableDelta>>::new();
    for update in updates {
        grouped
            .entry(update.table_id)
            .or_default()
            .push(update.delta);
    }
    let mut plan = StatsWritePlan::default();
    let table = locate(catalog, table_name)?;
    let table = table.read();
    for (table_id, deltas) in grouped {
        let mut rows = StatsRows::open(snapshot, &table, &["table_id"], table_id)?;
        let mut values = defaults_row(&table, now)?;
        set(&table, &mut values, "table_id", Datum::Int(table_id));
        let identity = identity_of(&table, &["table_id"], &values)?;
        let existed = rows.existing_values(&identity).is_some();
        if let Some(stored) = rows.existing_values(&identity) {
            values = stored.clone();
        }
        let current_modify =
            signed_value(values.get(&column_id(&table, "modify_count")?)).unwrap_or_default();
        let mut count = signed_value(values.get(&column_id(&table, "count")?)).unwrap_or_default();
        let mut modify_count = current_modify;
        for (index, delta) in deltas.into_iter().enumerate() {
            modify_count = modify_count.wrapping_add(delta.count);
            count = if !negative {
                count.wrapping_add(delta.delta)
            } else if existed || index > 0 {
                count.saturating_sub(delta.delta.wrapping_neg()).max(0)
            } else {
                // In one multi-row INSERT, the first missing-key tuple inserts
                // `-delta`; later tuples for that key take the duplicate path.
                delta.delta.wrapping_neg()
            };
        }
        set(&table, &mut values, "version", Datum::UInt(version));
        set(&table, &mut values, "modify_count", Datum::Int(modify_count));
        set(&table, &mut values, "count", Datum::Int(count));
        rows.store_row(catalog, &values, &mut plan, row_ids)?;
    }
    Ok(plan)
}

/// One batch of Go `DumpColStatsUsageEntries`: a single upsert that advances
/// a stored `last_used_at` only once the throttle interval has passed.
pub fn dump_col_stats_usage_batch<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    batch: &[(TableItemID, SystemTime)],
) -> Result<(), SqlExecError> {
    let threshold_minutes =
        (crate::COL_STATS_USAGE_LAST_USED_THROTTLE_INTERVAL.as_secs() / 60) as i64;
    let mut sql = String::from(
        "INSERT INTO mysql.column_stats_usage (table_id, column_id, last_used_at) VALUES ",
    );
    for (position, (item, used_at)) in batch.iter().enumerate() {
        // UTC, converted into the session's zone by the statement, so the
        // stored instant is right whatever session runs it.
        let last_used_at = chrono::DateTime::<chrono::Utc>::from(*used_at)
            .format("%Y-%m-%d %H:%M:%S")
            .to_string();
        sql.push_str(&must_escape_sql_text(
            "(%?, %?, CONVERT_TZ(%?, '+00:00', @@TIME_ZONE))",
            &[
                SqlArg::Signed(item.table_id),
                SqlArg::Signed(item.id),
                SqlArg::from(last_used_at.as_str()),
            ],
        ));
        if position + 1 < batch.len() {
            sql.push(',');
        }
    }
    sql.push_str(&must_escape_sql_text(
        " ON DUPLICATE KEY UPDATE last_used_at = CASE WHEN last_used_at IS NULL OR TIMESTAMPDIFF(MINUTE, last_used_at, VALUES(last_used_at)) >= %? THEN VALUES(last_used_at) ELSE last_used_at END",
        &[SqlArg::Signed(threshold_minutes)],
    ));
    exec_rows(sctx, &sql, &[])?;
    Ok(())
}
