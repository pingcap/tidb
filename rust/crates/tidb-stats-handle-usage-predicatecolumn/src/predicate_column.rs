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

//! Go `pkg/statistics/handle/usage/predicatecolumn/predicate_column.go`'s
//! statements on a statistics session.

use std::collections::HashMap;

use tidb_model::TableItemID;
use tidb_sqlescape::SqlArg;
use tidb_sqlexec::SqlExecError;
use tidb_stats_handle_util::{exec_rows, StatsSessionContext};

use crate::cluster_predicate_column::ColStatsTimeInfo;

/// Go `SaveColumnStatsUsageForTable`: every entry replaced, a missing
/// timestamp written as Go's `"NULL"` text (which `CONVERT_TZ` turns into
/// NULL).
pub fn save_column_stats_usage_for_table<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    col_stats_usage: &HashMap<TableItemID, ColStatsTimeInfo>,
) -> Result<(), SqlExecError> {
    for (col_id, stats_usage) in col_stats_usage {
        let text = |time: Option<tidb_datatype::Time>| {
            time.map_or_else(|| "NULL".to_owned(), |time| time.to_string())
        };
        let last_used_at = text(stats_usage.last_used_at);
        let last_analyzed_at = text(stats_usage.last_analyzed_at);
        exec_rows(
            sctx,
            "REPLACE INTO mysql.column_stats_usage (table_id, column_id, last_used_at, last_analyzed_at) VALUES (%?, %?, CONVERT_TZ(%?, '+00:00', @@TIME_ZONE), CONVERT_TZ(%?, '+00:00', @@TIME_ZONE))",
            &[
                SqlArg::Signed(col_id.table_id),
                SqlArg::Signed(col_id.id),
                SqlArg::from(last_used_at.as_str()),
                SqlArg::from(last_analyzed_at.as_str()),
            ],
        )?;
    }
    Ok(())
}

/// Go `GetPredicateColumns`: the IDs of the table's predicate columns, after
/// the usage of its dropped columns is deleted.
pub fn get_predicate_columns<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    table_id: i64,
) -> Result<Vec<i64>, SqlExecError> {
    // Each time we retrieve the predicate columns, we also attempt to remove
    // any column stats usage information whose column is dropped.
    cleanup_dropped_column_stats_usage(sctx, table_id)?;
    let (rows, _) = exec_rows(
        sctx,
        "SELECT column_id, CONVERT_TZ(last_used_at, @@TIME_ZONE, '+00:00') FROM mysql.column_stats_usage WHERE table_id = %? AND last_used_at IS NOT NULL",
        &[SqlArg::Signed(table_id)],
    )?;
    let mut column_ids = Vec::with_capacity(rows.len());
    for row in &rows {
        // Usually, it should not be NULL. This only happens when the
        // last_used_at is not a valid time.
        if row.get(1).is_none_or(tidb_datatype::Datum::is_null) {
            continue;
        }
        column_ids.push(row.first().map_or(0, tidb_datatype::Datum::get_int64));
    }
    Ok(column_ids)
}

/// Go `cleanupDroppedColumnStatsUsage`.
fn cleanup_dropped_column_stats_usage<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    table_id: i64,
) -> Result<(), SqlExecError> {
    let Some(table) = sctx.latest_table_meta_by_id(table_id) else {
        // Usually, it should not happen. But if it happens, we can safely do
        // nothing.
        return Ok(());
    };
    // Due to SQL limitations, column IDs must be converted to strings for
    // proper escaping in the query.
    let column_ids = table
        .read()
        .columns
        .iter_deref()
        .map(|column| column.read().id.to_string())
        .collect::<Vec<_>>();
    let column_ids = column_ids.iter().map(String::as_bytes).collect::<Vec<_>>();
    exec_rows(
        sctx,
        "DELETE FROM mysql.column_stats_usage WHERE table_id = %? AND column_id NOT IN (%?)",
        &[SqlArg::Signed(table_id), SqlArg::Strings(&column_ids)],
    )?;
    Ok(())
}

/// Go `loadColumnStatsUsage`: the usage rows the query answers, their UTC
/// times shown in `location`.
fn load_column_stats_usage_rows<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    location: &tidb_datatype::SessionTimeZone,
    query: &str,
    args: &[SqlArg<'_>],
) -> Result<HashMap<TableItemID, ColStatsTimeInfo>, SqlExecError> {
    let (rows, _) = exec_rows(sctx, query, args)?;
    let mut col_stats_map = HashMap::with_capacity(rows.len());
    for row in &rows {
        let null = |index: usize| row.get(index).is_none_or(tidb_datatype::Datum::is_null);
        if null(0) || null(1) {
            continue;
        }
        let tbl_col_id = TableItemID {
            table_id: row[0].get_int64(),
            id: row[1].get_int64(),
            is_index: false,
            is_sync_load_failed: false,
        };
        col_stats_map.insert(
            tbl_col_id,
            ColStatsTimeInfo {
                last_used_at: usage_time_in(row.get(2), location)?,
                last_analyzed_at: usage_time_in(row.get(3), location)?,
            },
        );
    }
    Ok(col_stats_map)
}

/// Go `row.GetTime(i).GoTime(time.UTC)` shown as a `TIMESTAMP` in `loc`.
fn usage_time_in(
    datum: Option<&tidb_datatype::Datum>,
    location: &tidb_datatype::SessionTimeZone,
) -> Result<Option<tidb_datatype::Time>, SqlExecError> {
    let Some(tidb_datatype::Datum::Time(value)) = datum else {
        return Ok(None);
    };
    let mut value = *value;
    value
        .convert_time_zone(&tidb_datatype::SessionTimeZone::utc(), location)
        .map_err(|error| Box::new(error) as SqlExecError)?;
    value.set_kind(tidb_datatype::TimeType::Timestamp);
    value
        .set_fsp(tidb_datatype::DEFAULT_FSP)
        .map_err(|error| Box::new(error) as SqlExecError)?;
    Ok(Some(value))
}

/// Go `LoadColumnStatsUsage`.
pub fn load_column_stats_usage<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    location: &tidb_datatype::SessionTimeZone,
) -> Result<HashMap<TableItemID, ColStatsTimeInfo>, SqlExecError> {
    load_column_stats_usage_rows(
        sctx,
        location,
        "SELECT table_id, column_id, CONVERT_TZ(last_used_at, @@TIME_ZONE, '+00:00'), CONVERT_TZ(last_analyzed_at, @@TIME_ZONE, '+00:00') FROM mysql.column_stats_usage",
        &[],
    )
}

/// Go `LoadColumnStatsUsageForTable`.
pub fn load_column_stats_usage_for_table<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    location: &tidb_datatype::SessionTimeZone,
    table_id: i64,
) -> Result<HashMap<TableItemID, ColStatsTimeInfo>, SqlExecError> {
    load_column_stats_usage_rows(
        sctx,
        location,
        "SELECT table_id, column_id, CONVERT_TZ(last_used_at, @@TIME_ZONE, '+00:00'), CONVERT_TZ(last_analyzed_at, @@TIME_ZONE, '+00:00') FROM mysql.column_stats_usage WHERE table_id = %?",
        &[SqlArg::Signed(table_id)],
    )
}
