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

//! Pinned Go `pkg/statistics/handle/usage/predicatecolumn` storage reads.
//!
//! The stored `TIMESTAMP`s are decoded as UTC instants and then projected into
//! the caller's location, matching Go's `CONVERT_TZ(..., @@TIME_ZONE,
//! '+00:00')`, `GoTime(time.UTC)`, and final `gt.In(loc)` sequence.

use std::collections::HashMap;
use tidb_syssession::stats_table_rows::*;

use tidb_datatype::{Datum, SessionTimeZone, Time, TimeType, DEFAULT_FSP};
use tidb_model::TableItemID;

use tidb_infoschema::cluster_catalog::{ClusterCatalog, MetaSnapshot};
use tidb_syssession::mysql_system_tables::{
    scan_system_table, scan_system_table_prefixed, SystemRow, SystemTableError, SystemTableView,
};

/// Go `statstypes.ColStatsTimeInfo`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ColumnStatsTimeInfo {
    /// Most recent predicate use, in the requested session location.
    pub last_used_at: Option<Time>,
    /// Most recent analyze, in the requested session location.
    pub last_analyzed_at: Option<Time>,
}

/// Loads every persisted column-usage row.
pub fn load_column_stats_usage<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    location: &SessionTimeZone,
) -> Result<HashMap<TableItemID, ColumnStatsTimeInfo>, SystemTableError> {
    load(snapshot, catalog, location, None)
}

/// Loads persisted column usage for one physical or logical table.
pub fn load_column_stats_usage_for_table<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    location: &SessionTimeZone,
    table_id: i64,
) -> Result<HashMap<TableItemID, ColumnStatsTimeInfo>, SystemTableError> {
    load(snapshot, catalog, location, Some(table_id))
}

/// Returns the stored predicate-column IDs for a table after the caller has
/// removed dropped-column rows in the same transaction.
pub fn predicate_columns<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    table_id: i64,
) -> Result<Vec<i64>, SystemTableError> {
    let view = usage_view(catalog)?;
    let pairs = scan_system_table_prefixed(snapshot, &view, &[Datum::Int(table_id)])?;
    let utc = SessionTimeZone::utc();
    let mut columns = Vec::new();
    for (key, value) in pairs {
        let row = SystemRow::parse_in_timezone(&view, &key, &value, Some(&utc))?;
        if row.i64("table_id")? != Some(table_id)
            || usage_time(&row, "last_used_at", &utc, &utc)?.is_none()
        {
            continue;
        }
        if let Some(column_id) = row.i64("column_id")? {
            columns.push(column_id);
        }
    }
    Ok(columns)
}

fn load<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    location: &SessionTimeZone,
    table_id: Option<i64>,
) -> Result<HashMap<TableItemID, ColumnStatsTimeInfo>, SystemTableError> {
    let view = usage_view(catalog)?;
    let pairs = match table_id {
        Some(table_id) => scan_system_table_prefixed(snapshot, &view, &[Datum::Int(table_id)])?,
        None => scan_system_table(snapshot, &view)?,
    };
    let utc = SessionTimeZone::utc();
    let mut usage = HashMap::with_capacity(pairs.len());
    for (key, value) in pairs {
        let row = SystemRow::parse_in_timezone(&view, &key, &value, Some(&utc))?;
        let (Some(table_id), Some(column_id)) = (row.i64("table_id")?, row.i64("column_id")?)
        else {
            continue;
        };
        usage.insert(
            TableItemID {
                table_id,
                id: column_id,
                is_index: false,
                is_sync_load_failed: false,
            },
            ColumnStatsTimeInfo {
                last_used_at: usage_time(&row, "last_used_at", &utc, location)?,
                last_analyzed_at: usage_time(&row, "last_analyzed_at", &utc, location)?,
            },
        );
    }
    Ok(usage)
}

fn usage_view(catalog: &ClusterCatalog) -> Result<SystemTableView, SystemTableError> {
    SystemTableView::locate(
        catalog,
        "column_stats_usage",
        &["table_id", "column_id", "last_used_at", "last_analyzed_at"],
    )
}

fn usage_time(
    row: &SystemRow<'_>,
    column: &str,
    utc: &SessionTimeZone,
    location: &SessionTimeZone,
) -> Result<Option<Time>, SystemTableError> {
    let Some(datum) = row.datum(column)? else {
        return Ok(None);
    };
    let Datum::Time(mut value) = datum.clone() else {
        return Err(SystemTableError::UnexpectedColumnValue {
            name: "mysql.column_stats_usage".to_owned(),
            column: column.to_owned(),
            wanted: "TIMESTAMP",
            stored: format!("{datum:?}"),
        });
    };
    // Both pinned SELECTs apply CONVERT_TZ. Go's builtin returns SQL NULL for
    // every `types.Time::InvalidZero` value (month or day is zero), so these
    // rows are not conversion errors and cannot make a predicate column.
    if value.invalid_zero() {
        return Ok(None);
    }
    value
        .convert_time_zone(utc, location)
        .map_err(|error| SystemTableError::Decode {
            name: "mysql.column_stats_usage".to_owned(),
            detail: error.to_string(),
        })?;
    value.set_kind(TimeType::Timestamp);
    value
        .set_fsp(DEFAULT_FSP)
        .map_err(|error| SystemTableError::Decode {
            name: "mysql.column_stats_usage".to_owned(),
            detail: error.to_string(),
        })?;
    Ok(Some(value))
}

/// Plans pinned Go `GetPredicateColumns`, including its same-transaction
/// cleanup of usage rows for columns no longer present in the latest schema.
///
/// `None` means Go's latest infoschema did not contain the table. In that
/// case `cleanupDroppedColumnStatsUsage` is a no-op, but the following SELECT
/// still returns persisted predicate-column IDs.
///
/// Go executes DELETE before SELECT. This mutation planner cannot make staged
/// mutations visible through `MetaSnapshot`, so it filters the selected IDs
/// against the same current-column set while returning the exact deletes for
/// the caller to commit with the read transaction.
pub fn plan_get_predicate_columns<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    table_id: i64,
    current_column_ids: Option<&[i64]>,
) -> Result<(Vec<i64>, StatsWritePlan), StatsWriteError> {
    let columns = predicate_columns(snapshot, catalog, table_id)?;
    let Some(current_column_ids) = current_column_ids else {
        return Ok((columns, StatsWritePlan::default()));
    };
    let current = current_column_ids
        .iter()
        .map(|column_id| format!("{:?}", Datum::Int(*column_id)))
        .collect::<std::collections::HashSet<_>>();
    let columns = columns
        .into_iter()
        .filter(|column_id| current_column_ids.contains(column_id))
        .collect();
    let table = locate(catalog, "column_stats_usage")?;
    let mut rows = StatsRows::open(snapshot, table, &["table_id", "column_id"], table_id)?;
    let dropped = rows
        .existing
        .keys()
        .filter(|identity| {
            identity
                .get(1)
                .is_some_and(|column| !current.contains(column))
        })
        .cloned()
        .collect::<Vec<_>>();
    let mut plan = StatsWritePlan::default();
    for identity in dropped {
        rows.retract_prefix(&identity, &mut plan)?;
    }
    Ok((columns, plan))
}

/// Storage-backed reader used by pinned Go `SHOW COLUMN_STATS_USAGE`.
pub trait ColumnStatsUsageProvider: Send + Sync {
    /// Loads the complete shared usage table at a fresh statement snapshot.
    fn load_column_stats_usage(
        &self,
        location: &tidb_datatype::SessionTimeZone,
        resource_group: &str,
    ) -> Result<
        std::collections::HashMap<
            tidb_model::TableItemID,
            (Option<tidb_datatype::Time>, Option<tidb_datatype::Time>),
        >,
        String,
    >;
}
