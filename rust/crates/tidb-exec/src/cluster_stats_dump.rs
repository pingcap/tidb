// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Pinned Go `statsReadWriter.TableStatsToJSON` and `DumpStatsToJSON` over a
//! cluster metadata snapshot.

use std::collections::BTreeMap;
use std::fmt;

use tidb_datatype::{Datum, SessionTimeZone};
use tidb_executor::load_stats::{blocks_to_json_table, gen_json_table_from_stats, LoadStatsError};
use tidb_model::table_info::TableInfo;
use tidb_stats::{JsonPredicateColumn, JsonTable, Table, TIDB_GLOBAL_STATS};

use crate::cluster_catalog::{ClusterCatalog, MetaSnapshot};
use crate::cluster_predicate_column::load_column_stats_usage_for_table;
use crate::cluster_stats_load::{column_types_of, ClusterStatsLoader};
use crate::mysql_system_tables::{
    scan_system_table_prefixed, SystemRow, SystemTableError, SystemTableView,
};

/// A live statistics table could not be converted to the canonical JSON dump.
#[derive(Debug)]
pub enum StatsDumpError {
    /// A `mysql.stats_*` row could not be read or decoded.
    Storage(SystemTableError),
    /// A canonical statistics value could not be rendered as dump JSON.
    Json(LoadStatsError),
}

impl fmt::Display for StatsDumpError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(error) => error.fmt(formatter),
            Self::Json(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for StatsDumpError {}

impl From<SystemTableError> for StatsDumpError {
    fn from(error: SystemTableError) -> Self {
        Self::Storage(error)
    }
}

impl From<LoadStatsError> for StatsDumpError {
    fn from(error: LoadStatsError) -> Self {
        Self::Json(error)
    }
}

/// Go `statsReadWriter.TableStatsToJSON`: reloads one physical table with all
/// payload, refreshes its metadata and predicate usage from the same snapshot,
/// and converts it to the shared JSON object model.
pub fn table_stats_to_json<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    database_name: &str,
    table_info: &TableInfo,
    physical_id: i64,
) -> Result<Option<JsonTable>, StatsDumpError> {
    let Some(table) = load_table_stats_payload(snapshot, catalog, table_info, physical_id)? else {
        return Ok(None);
    };
    table_stats_to_json_from_loaded(
        snapshot,
        catalog,
        database_name,
        table_info,
        physical_id,
        table,
    )
}

/// The first restricted transaction in Go `TableStatsToJSON`: load the full
/// histogram payload, including FM sketches.
pub fn load_table_stats_payload<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    table_info: &TableInfo,
    physical_id: i64,
) -> Result<Option<Table>, StatsDumpError> {
    let loader = ClusterStatsLoader::locate(catalog)?;
    let Some(stored) =
        loader.load_table_with_fm(snapshot, physical_id, &column_types_of(table_info))?
    else {
        return Ok(None);
    };
    Ok(Some(
        stored.to_statistics_table(table_info).as_ref().clone(),
    ))
}

/// The second restricted transaction in Go `TableStatsToJSON`: refresh the
/// mutable stats-meta counters and predicate-column usage before rendering.
pub fn table_stats_to_json_from_loaded<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    database_name: &str,
    table_info: &TableInfo,
    physical_id: i64,
    mut table: Table,
) -> Result<Option<JsonTable>, StatsDumpError> {
    let loader = ClusterStatsLoader::locate(catalog)?;
    let (version, _, modify_count, count, _) =
        loader.load_meta(snapshot, physical_id)?.unwrap_or_default();
    table.version = version;
    table.hist_coll.modify_count = modify_count;
    table.hist_coll.realtime_count = i64::try_from(count).unwrap_or(i64::MAX);
    let usage =
        load_column_stats_usage_for_table(snapshot, catalog, &SessionTimeZone::utc(), physical_id)?;
    let predicate_columns = Some(
        usage
            .into_iter()
            .map(|(item, usage)| {
                Some(JsonPredicateColumn {
                    id: item.id,
                    last_used_at: usage.last_used_at.map(|value| value.to_string()),
                    last_analyzed_at: usage.last_analyzed_at.map(|value| value.to_string()),
                })
            })
            .collect(),
    );
    Ok(Some(gen_json_table_from_stats(
        database_name,
        table_info.name.lowercase(),
        &table,
        predicate_columns,
    )?))
}

/// Go `statsReadWriter.DumpStatsToJSONBySnapshot`.
///
/// `is_dynamic_mode` is the already-read partition-prune mode. In dynamic
/// mode partition entries are omitted unless `dump_partition_stats` is true;
/// the optional logical-table `global` entry is always attempted.
pub fn dump_stats_to_json<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    database_name: &str,
    table_info: &TableInfo,
    is_dynamic_mode: bool,
    dump_partition_stats: bool,
) -> Result<Option<JsonTable>, StatsDumpError> {
    let Some(partition) = table_info.get_partition_info() else {
        return table_stats_to_json(snapshot, catalog, database_name, table_info, table_info.id);
    };
    let partition = partition.read();
    let mut partitions = BTreeMap::new();
    if !is_dynamic_mode || dump_partition_stats {
        for definition in partition.definitions.snapshot() {
            if let Some(table) =
                table_stats_to_json(snapshot, catalog, database_name, table_info, definition.id)?
            {
                partitions.insert(definition.name.lowercase().to_owned(), Some(table));
            }
        }
    }
    if let Some(global) =
        table_stats_to_json(snapshot, catalog, database_name, table_info, table_info.id)?
    {
        partitions.insert(TIDB_GLOBAL_STATS.to_owned(), Some(global));
    }
    Ok(Some(JsonTable {
        database_name: database_name.to_owned(),
        table_name: table_info.name.lowercase().to_owned(),
        partitions: Some(partitions),
        ..JsonTable::default()
    }))
}

/// Pinned Go `storage.TableHistoricalStatsToJSON`.
///
/// Metadata and payload versions are deliberately selected independently:
/// a stats-delta flush can create a newer metadata history row without a new
/// histogram dump. Go then combines those newer counts with the newest data
/// dump at or before `snapshot`, which this preserves.
pub fn table_historical_stats_to_json<S: MetaSnapshot>(
    snapshot_store: &mut S,
    catalog: &ClusterCatalog,
    physical_id: i64,
    snapshot: u64,
) -> Result<Option<JsonTable>, StatsDumpError> {
    let meta = SystemTableView::locate(
        catalog,
        "stats_meta_history",
        &["table_id", "modify_count", "count", "version"],
    )?;
    let mut meta_at_snapshot = None;
    for (key, value) in
        scan_system_table_prefixed(snapshot_store, &meta, &[Datum::Int(physical_id)])?
    {
        let row = SystemRow::parse(&meta, &key, &value)?;
        if row.i64("table_id")? != Some(physical_id) {
            continue;
        }
        let Some(version) = row.u64("version")? else {
            continue;
        };
        if version > snapshot
            || meta_at_snapshot
                .as_ref()
                .is_some_and(|(current, _, _)| version <= *current)
        {
            continue;
        }
        let modify_count = row.i64("modify_count")?.unwrap_or_default();
        let count = row.i64("count")?.unwrap_or_default();
        meta_at_snapshot = Some((version, modify_count, count));
    }
    let Some((_, modify_count, count)) = meta_at_snapshot else {
        return Ok(None);
    };

    let history = SystemTableView::locate(
        catalog,
        "stats_history",
        &["table_id", "stats_data", "seq_no", "version"],
    )?;
    let rows = scan_system_table_prefixed(snapshot_store, &history, &[Datum::Int(physical_id)])?;
    let mut data_version = None;
    let mut decoded = Vec::with_capacity(rows.len());
    for (key, value) in rows {
        let row = SystemRow::parse(&history, &key, &value)?;
        if row.i64("table_id")? != Some(physical_id) {
            continue;
        }
        let Some(version) = row.u64("version")? else {
            continue;
        };
        if version <= snapshot {
            data_version = Some(data_version.map_or(version, |current: u64| current.max(version)));
        }
        decoded.push(row);
    }
    let Some(data_version) = data_version else {
        return Ok(None);
    };
    let mut blocks = Vec::new();
    for row in decoded {
        if row.u64("version")? != Some(data_version) {
            continue;
        }
        blocks.push((
            row.i64("seq_no")?.unwrap_or_default(),
            row.bytes("stats_data")?.unwrap_or_default(),
        ));
    }
    blocks.sort_by_key(|(sequence, _)| *sequence);
    let mut table = blocks_to_json_table(
        &blocks
            .into_iter()
            .map(|(_, block)| block)
            .collect::<Vec<_>>(),
    )?;
    table.count = count;
    table.modify_count = modify_count;
    table.is_historical_stats = true;
    Ok(Some(table))
}
