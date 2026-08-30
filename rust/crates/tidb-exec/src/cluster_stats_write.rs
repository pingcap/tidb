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

//! Storing one table's statistics in `mysql.stats_meta`,
//! `mysql.stats_histograms`, `mysql.stats_buckets` and `mysql.stats_top_n`.
//!
//! Go source of truth is
//! `pkg/statistics/handle/storage/save.go::SaveAnalyzeResultToStorage`, whose
//! four statements this follows column for column, minus the SQL: the
//! mutations go straight to the record and index keys, the way every
//! `mysql.*` write on this node does (see [`crate::system_row_write`]).
//!
//! # The two encodings that make or break the round trip
//!
//! **Bucket counts are stored as deltas.** `Bucket.Count` in memory is
//! cumulative through the bucket; `save.go` subtracts the previous bucket's
//! count before writing, and `read.go` adds them back up. A writer that
//! stored the cumulative value would make every bucket after the first look
//! enormous to a Go server -- and to
//! [`crate::cluster_stats_load`], which already re-accumulates on read.
//!
//! **Bounds are stored as blobs, not as datums.** `convertBoundToBlob` is
//! `Datum.ConvertTo(TypeBlob)`, so a `BIGINT` bound is stored as `"90"` and a
//! `DATETIME` bound as `"2024-05-05 05:05:05"`. An index bound and a
//! new-collation string bound are already bytes and convert to themselves.
//! [`crate::cluster_stats_load::decode_bound`]'s inverse is what this must
//! satisfy, and the round trip is what the module's tests assert.
//!
//! # Which handle shape the four tables have
//!
//! Not a fixed answer. `mysql.stats_meta` and its histogram tables became
//! `CLUSTERED` only in a recent TiDB; on a v8.5 cluster they are still a
//! `UNIQUE INDEX` over a `_tidb_rowid`, which a writer that assumed the newer
//! shape cannot address at all. This module reads the cluster's own
//! `TableInfo` and writes whichever shape it finds -- see [`StatsRows`].
//!
//! Partition ANALYZE also writes `mysql.stats_fm_sketch`; logical/global
//! writes delete stale sketches and leave no row, matching `needDumpFMS` in
//! Go. `mysql.column_stats_usage` remains a separate predicate-column input.
//! `cm_sketch` is left NULL because analyze v2 stores no CMSketch.

use std::collections::{BTreeMap, HashMap};

use tidb_datatype::{
    parse_time, ConversionFlags, Datum, FieldType, FieldTypeCode, Time, TimeType, MAX_FSP,
    UNSPECIFIED_LENGTH,
};
use tidb_executor::analyze::{AnalyzeColumnChoice, AnalyzeOptionOverrides, SavedAnalyzeOptions};
use tidb_meta::{key, value};
use tidb_model::table_info::TableInfo;
use tidb_model::TableItemID;
use tidb_stats::histogram::Bucket;
use tidb_stats::{encode_cmsketch_without_topn, encode_fm_sketch};
use tidb_stats::{JsonPredicateColumn, JsonTable};
use tidb_txnkv::transaction::OptimisticMutation;

use crate::cluster_catalog::{ClusterCatalog, MetaSnapshot};
use crate::cluster_predicate_column::ColumnStatsTimeInfo;
use crate::cluster_stats_load::{ClusterStatsItem, ClusterTableStats};
use crate::mysql_system_tables::{
    scan_system_table, scan_system_table_index_prefixed, scan_system_table_prefixed, HandleLayout,
    SystemRow, SystemTableError, SystemTableView,
};
use crate::system_row_write::{
    defaults_row, delete_clustered_row, delete_row, insert_row, rewrite_rowid_row, row_id_of,
    store_clustered_row, RowEncodeError, RowValues,
};

/// Go's `mysql` schema name.
const SYSTEM_DB: &str = "mysql";

/// Why one table's statistics could not be stored.
#[derive(Debug)]
pub enum StatsWriteError {
    /// A `mysql.stats_*` table the cluster does not have.
    MissingTable(String),
    /// The current rows could not be read.
    Read(SystemTableError),
    /// A row could not be encoded.
    Encode(RowEncodeError),
    /// A histogram bound could not be converted to the blob the column stores.
    Bound(String),
    /// A loaded CMSketch could not be encoded for `stats_histograms`.
    Cms(String),
    /// A predicate-column timestamp could not be parsed as Go TIMESTAMP(6).
    PredicateTime(String),
    /// The exact statistics version needed for a history row is no longer current.
    HistoricalMeta(String),
    /// A historical statistics JSON object could not be compressed.
    HistoricalData(String),
}

impl std::fmt::Display for StatsWriteError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingTable(name) => write!(formatter, "this cluster has no `{name}`"),
            Self::Read(error) => write!(formatter, "{error}"),
            Self::Encode(error) => write!(formatter, "{error}"),
            Self::Bound(detail) => write!(formatter, "a histogram bound did not convert: {detail}"),
            Self::Cms(detail) => write!(formatter, "a CMSketch did not encode: {detail}"),
            Self::PredicateTime(detail) => {
                write!(
                    formatter,
                    "a predicate-column timestamp did not parse: {detail}"
                )
            }
            Self::HistoricalMeta(detail) => formatter.write_str(detail),
            Self::HistoricalData(detail) => formatter.write_str(detail),
        }
    }
}

impl std::error::Error for StatsWriteError {}

impl From<SystemTableError> for StatsWriteError {
    fn from(error: SystemTableError) -> Self {
        Self::Read(error)
    }
}

impl From<RowEncodeError> for StatsWriteError {
    fn from(error: RowEncodeError) -> Self {
        Self::Encode(error)
    }
}

/// The mutations that make one table's `mysql.stats_*` rows equal `stats`.
#[derive(Debug, Default)]
pub struct StatsWritePlan {
    /// Every mutation, to be committed on the snapshot's own transaction.
    pub mutations: Vec<OptimisticMutation>,
    /// How many histograms the plan stores, for the statement's log line.
    pub histogram_count: usize,
    /// How many buckets, across every histogram.
    pub bucket_count: usize,
    /// How many TopN entries, across every histogram.
    pub topn_count: usize,
}

/// One row read from pinned Go's `mysql.analyze_options`.
pub type PersistedAnalyzeOptions = SavedAnalyzeOptions;

impl StatsWritePlan {
    /// Whether the plan writes nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.mutations.is_empty()
    }
}

/// Loads and schema-filters one persisted ANALYZE option row.
pub fn load_analyze_options<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    table: &TableInfo,
    physical_id: i64,
) -> Result<Option<PersistedAnalyzeOptions>, StatsWriteError> {
    let view = SystemTableView::locate(
        catalog,
        "analyze_options",
        &[
            "table_id",
            "sample_num",
            "sample_rate",
            "buckets",
            "topn",
            "column_choice",
            "column_ids",
        ],
    )?;
    let pairs = scan_system_table_prefixed(snapshot, &view, &[Datum::Int(physical_id)])?;
    let Some((key, value)) = pairs.into_iter().next() else {
        return Ok(None);
    };
    let row = SystemRow::parse_in_timezone(&view, &key, &value, None)?;
    if row.i64("table_id")? != Some(physical_id) {
        return Ok(None);
    }
    let sample_num = row.i64("sample_num")?.filter(|value| *value > 0);
    let sample_rate = row.f64("sample_rate")?.filter(|value| *value > 0.0);
    let buckets = row.i64("buckets")?.filter(|value| *value > 0);
    let topn = row.i64("topn")?.filter(|value| *value >= 0);
    let raw = AnalyzeOptionOverrides {
        num_buckets: buckets.and_then(|value| isize::try_from(value).ok()),
        num_topn: topn.and_then(|value| isize::try_from(value).ok()),
        num_samples: sample_num.and_then(|value| usize::try_from(value).ok()),
        sample_rate,
    };
    let columns = match row.enum_label("column_choice")?.as_deref() {
        Some("ALL") => AnalyzeColumnChoice::All,
        Some("PREDICATE") => AnalyzeColumnChoice::Predicate,
        Some("LIST") => {
            let ids = row
                .bytes("column_ids")?
                .and_then(|bytes| String::from_utf8(bytes).ok())
                .unwrap_or_default();
            let mut names = Vec::new();
            for id in ids.split(',').filter_map(|id| id.parse::<i64>().ok()) {
                if let Some(column) = table
                    .cols()
                    .iter_deref()
                    .find(|column| column.read().id == id)
                {
                    names.push(column.read().name.original().to_owned());
                }
            }
            AnalyzeColumnChoice::Explicit(names)
        }
        _ => AnalyzeColumnChoice::Default,
    };
    Ok(Some(PersistedAnalyzeOptions { raw, columns }))
}

/// Plans pinned Go `saveAnalyzeOptions` for one physical table.
pub fn plan_analyze_options_write<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    table: &TableInfo,
    physical_id: i64,
    raw: AnalyzeOptionOverrides,
    columns: &AnalyzeColumnChoice,
    now: Time,
) -> Result<StatsWritePlan, StatsWriteError> {
    let stored = locate(catalog, "analyze_options")?;
    let mut rows = StatsRows::open(snapshot, stored, &["table_id"], physical_id)?;
    let identity = vec![format!("{:?}", Datum::Int(physical_id))];
    let mut values = rows
        .existing_values(&identity)
        .cloned()
        .unwrap_or(defaults_row(stored, now)?);
    set(stored, &mut values, "table_id", Datum::Int(physical_id));
    set(
        stored,
        &mut values,
        "sample_num",
        Datum::Int(raw.num_samples.map_or(0, |value| value as i64)),
    );
    set(
        stored,
        &mut values,
        "sample_rate",
        Datum::Real(raw.sample_rate.unwrap_or(0.0)),
    );
    set(
        stored,
        &mut values,
        "buckets",
        Datum::Int(raw.num_buckets.map_or(0, |value| value as i64)),
    );
    set(
        stored,
        &mut values,
        "topn",
        Datum::Int(raw.num_topn.map_or(-1, |value| value as i64)),
    );
    let (choice, ids) = match columns {
        AnalyzeColumnChoice::Default => ("DEFAULT", String::new()),
        AnalyzeColumnChoice::All => ("ALL", String::new()),
        AnalyzeColumnChoice::Predicate => ("PREDICATE", String::new()),
        AnalyzeColumnChoice::Explicit(names) => {
            let ids = names
                .iter()
                .filter_map(|name| {
                    table
                        .cols()
                        .iter_deref()
                        .find(|column| column.read().name.lowercase() == name.to_lowercase())
                        .map(|column| column.read().id.to_string())
                })
                .collect::<Vec<_>>()
                .join(",");
            ("LIST", ids)
        }
    };
    set(
        stored,
        &mut values,
        "column_choice",
        Datum::Bytes(choice.as_bytes().to_vec()),
    );
    set(
        stored,
        &mut values,
        "column_ids",
        Datum::Bytes(ids.into_bytes()),
    );
    let mut plan = StatsWritePlan::default();
    rows.store(snapshot, catalog, &values, &mut plan)?;
    rows.publish_watermark(catalog, &mut plan)?;
    Ok(plan)
}

/// Plans the write of one table's statistics.
///
/// `snapshot` must be the snapshot `stats` was built from, and the mutations
/// must be committed on that snapshot's transaction: that is what makes a
/// concurrent `ANALYZE` on a Go node a write conflict at prewrite rather than
/// a silent half-overwrite of somebody else's histograms.
///
/// The rows this table already has are read and reconciled rather than
/// blindly inserted, because TiKV's own assertions demand it -- an `Insert`
/// over an existing key and a `Delete` over an absent one are both rejected --
/// and because a re-`ANALYZE` that produced fewer buckets than the last one
/// must retract the buckets it no longer has.
pub fn plan_stats_write<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    stats: &ClusterTableStats,
    now: Time,
) -> Result<StatsWritePlan, StatsWriteError> {
    plan_stats_write_impl(snapshot, catalog, stats, now, true, false)
}

/// Plans a full physical-partition ANALYZE write, including FM sketches.
pub fn plan_partition_stats_write<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    stats: &ClusterTableStats,
    now: Time,
) -> Result<StatsWritePlan, StatsWriteError> {
    plan_stats_write_impl(snapshot, catalog, stats, now, true, true)
}

/// Plans an ANALYZE write that replaces only the histogram items present in
/// `stats`, matching Go's partial-column analyze storage behavior.
pub fn plan_partial_stats_write<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    stats: &ClusterTableStats,
    now: Time,
) -> Result<StatsWritePlan, StatsWriteError> {
    plan_stats_write_impl(snapshot, catalog, stats, now, false, false)
}

/// Plans a partial physical-partition ANALYZE write, including FM sketches.
pub fn plan_partial_partition_stats_write<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    stats: &ClusterTableStats,
    now: Time,
) -> Result<StatsWritePlan, StatsWriteError> {
    plan_stats_write_impl(snapshot, catalog, stats, now, false, true)
}

/// Plans pinned Go's `ForMVIndexOrGlobalIndex` ANALYZE write.
///
/// A special global-index task replaces only the index item it produced. If
/// `stats_meta` already exists, Go advances only its version markers; if it
/// does not, Go creates it with count and snapshot zero. In both cases the
/// independently scanned index must not overwrite table row-count metadata.
pub fn plan_independent_index_stats_write<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    stats: &ClusterTableStats,
    now: Time,
) -> Result<(StatsWritePlan, bool), StatsWriteError> {
    let mut plan = StatsWritePlan::default();
    let inserted_meta = plan_independent_index_meta(snapshot, catalog, stats, now, &mut plan)?;
    plan_histograms(snapshot, catalog, stats, now, false, &mut plan)?;
    plan_buckets(snapshot, catalog, stats, now, false, &mut plan)?;
    plan_topn(snapshot, catalog, stats, now, false, &mut plan)?;
    plan_fm_sketches(snapshot, catalog, stats, now, false, false, &mut plan)?;
    Ok((plan, inserted_meta))
}

fn plan_stats_write_impl<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    stats: &ClusterTableStats,
    now: Time,
    replace_all: bool,
    dump_fm: bool,
) -> Result<StatsWritePlan, StatsWriteError> {
    let mut plan = StatsWritePlan::default();
    plan_meta(snapshot, catalog, stats, now, &mut plan)?;
    plan_histograms(snapshot, catalog, stats, now, replace_all, &mut plan)?;
    plan_buckets(snapshot, catalog, stats, now, replace_all, &mut plan)?;
    plan_topn(snapshot, catalog, stats, now, replace_all, &mut plan)?;
    plan_fm_sketches(
        snapshot,
        catalog,
        stats,
        now,
        replace_all,
        dump_fm,
        &mut plan,
    )?;
    Ok(plan)
}

/// Plans Go `SaveColOrIdxStatsToStorage` for one object loaded from JSON.
///
/// Unlike ANALYZE, LOAD STATS replaces only the named histogram and its
/// bucket/TopN/FM rows. Statistics for schema objects absent from the dump
/// remain untouched. Go runs this once in an independent restricted-session
/// transaction for every loaded column and index.
pub fn plan_loaded_stats_item_write<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    table_id: i64,
    count: i64,
    item: &ClusterStatsItem,
    version: u64,
    now: Time,
) -> Result<StatsWritePlan, StatsWriteError> {
    let mut plan = StatsWritePlan::default();
    plan_loaded_meta(snapshot, catalog, table_id, count, version, now, &mut plan)?;
    plan_loaded_topn(snapshot, catalog, table_id, item, now, &mut plan)?;
    plan_loaded_fm_delete(snapshot, catalog, table_id, item, &mut plan)?;
    plan_loaded_histogram(snapshot, catalog, table_id, item, version, now, &mut plan)?;
    plan_loaded_buckets(snapshot, catalog, table_id, item, now, &mut plan)?;
    Ok(plan)
}

/// Plans Go `SaveMetaToStorage(..., refreshLastHistVer=true)` after every
/// object and predicate-usage write for one JSON table has completed.
/// Existing columns not named by that statement (notably `snapshot`) are
/// preserved by the `ON DUPLICATE KEY UPDATE` branch.
pub fn plan_loaded_stats_meta_write<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    table_id: i64,
    count: i64,
    modify_count: i64,
    version: u64,
    now: Time,
) -> Result<StatsWritePlan, StatsWriteError> {
    let mut plan = StatsWritePlan::default();
    let table = locate(catalog, "stats_meta")?;
    let mut rows = StatsRows::open(snapshot, table, &["table_id"], table_id)?;
    let identity = vec![format!("{:?}", Datum::Int(table_id))];
    let mut values = rows
        .existing_values(&identity)
        .cloned()
        .unwrap_or(defaults_row(table, now)?);
    set(table, &mut values, "table_id", Datum::Int(table_id));
    set(table, &mut values, "version", Datum::UInt(version));
    set(table, &mut values, "count", Datum::Int(count));
    set(table, &mut values, "modify_count", Datum::Int(modify_count));
    set(
        table,
        &mut values,
        "last_stats_histograms_version",
        Datum::UInt(version),
    );
    rows.store(snapshot, catalog, &values, &mut plan)?;
    rows.publish_watermark(catalog, &mut plan)?;
    Ok(plan)
}

/// Plans pinned Go's statistics DDL subscriber for
/// `ActionTruncateTablePartition`.
pub fn plan_truncate_partition_stats_write<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    logical_table_id: i64,
    replacements: &[(i64, i64)],
    version: u64,
    now: Time,
) -> Result<StatsWritePlan, StatsWriteError> {
    let table = locate(catalog, "stats_meta")?;
    let mut rows = StatsRows::open_all(snapshot, table, &["table_id"])?;
    let mut plan = StatsWritePlan::default();
    let mut truncated_count = 0i64;
    let signed = |datum: Option<&Datum>, name: &str| -> Result<i64, StatsWriteError> {
        match datum.unwrap_or(&Datum::Int(0)) {
            Datum::Int(value) => Ok(*value),
            Datum::UInt(value) => i64::try_from(*value).map_err(|_| {
                StatsWriteError::HistoricalMeta(format!("stats_meta.{name} overflows int64"))
            }),
            other => Err(StatsWriteError::HistoricalMeta(format!(
                "invalid stats_meta.{name} {other:?}"
            ))),
        }
    };
    let count_id = column_id(table, "count")?;
    let modify_id = column_id(table, "modify_count")?;

    for &(old_id, new_id) in replacements {
        let old_identity = vec![format!("{:?}", Datum::Int(old_id))];
        if let Some(mut values) = rows.existing_values(&old_identity).cloned() {
            truncated_count = truncated_count
                .checked_add(signed(values.get(&count_id), "count")?)
                .ok_or_else(|| {
                    StatsWriteError::HistoricalMeta(
                        "truncated partition count overflows int64".to_owned(),
                    )
                })?;
            set(table, &mut values, "version", Datum::UInt(version));
            rows.store(snapshot, catalog, &values, &mut plan)?;
        }

        let mut values = defaults_row(table, now)?;
        set(table, &mut values, "table_id", Datum::Int(new_id));
        set(table, &mut values, "version", Datum::UInt(version));
        set(table, &mut values, "modify_count", Datum::Int(0));
        set(table, &mut values, "count", Datum::Int(0));
        rows.store(snapshot, catalog, &values, &mut plan)?;
    }

    let global_identity = vec![format!("{:?}", Datum::Int(logical_table_id))];
    if let Some(mut values) = rows.existing_values(&global_identity).cloned() {
        let count = signed(values.get(&count_id), "count")?;
        let modify_count = signed(values.get(&modify_id), "modify_count")?;
        set(table, &mut values, "version", Datum::UInt(version));
        set(
            table,
            &mut values,
            "count",
            Datum::Int(if count > truncated_count {
                count - truncated_count
            } else {
                0
            }),
        );
        set(
            table,
            &mut values,
            "modify_count",
            Datum::Int(
                modify_count
                    .checked_add(truncated_count)
                    .ok_or_else(|| {
                        StatsWriteError::HistoricalMeta(
                            "stats_meta.modify_count overflows int64".to_owned(),
                        )
                    })?
                    .max(0),
            ),
        );
        rows.store(snapshot, catalog, &values, &mut plan)?;
    }
    rows.publish_watermark(catalog, &mut plan)?;
    Ok(plan)
}

/// Plans Go `UpdateStatsMetaVerAndLastHistUpdateVer`.
///
/// The update deliberately does not create a missing metadata row and leaves
/// its count, modify count, and snapshot untouched.
pub fn plan_stats_meta_version_refresh<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    table_id: i64,
    version: u64,
) -> Result<StatsWritePlan, StatsWriteError> {
    let mut plan = StatsWritePlan::default();
    let table = locate(catalog, "stats_meta")?;
    let mut rows = StatsRows::open(snapshot, table, &["table_id"], table_id)?;
    let identity = vec![format!("{:?}", Datum::Int(table_id))];
    let Some(mut values) = rows.existing_values(&identity).cloned() else {
        return Ok(plan);
    };
    set(table, &mut values, "version", Datum::UInt(version));
    set(
        table,
        &mut values,
        "last_stats_histograms_version",
        Datum::UInt(version),
    );
    rows.store(snapshot, catalog, &values, &mut plan)?;
    rows.publish_watermark(catalog, &mut plan)?;
    Ok(plan)
}

/// Plans Go `SaveColumnStatsUsageToStorage` for one physical table.
///
/// The Go helper uses one restricted-session transaction for the complete
/// predicate-column slice and `REPLACE`s each four-column row, including
/// explicit NULL timestamps. It parses dump timestamps as UTC TIMESTAMP(6)
/// before converting them through the restricted session's time zone; this
/// cluster path uses UTC too, so the stored wall-clock value is identical.
pub fn plan_loaded_stats_usage_write<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    table_id: i64,
    predicate_columns: &[Option<JsonPredicateColumn>],
    now: Time,
) -> Result<StatsWritePlan, StatsWriteError> {
    let mut usage = HashMap::with_capacity(predicate_columns.len());
    for column in predicate_columns.iter().flatten() {
        usage.insert(
            TableItemID {
                table_id,
                id: column.id,
                is_index: false,
                is_sync_load_failed: false,
            },
            ColumnStatsTimeInfo {
                last_used_at: parse_predicate_time(column.last_used_at.as_deref())?,
                last_analyzed_at: parse_predicate_time(column.last_analyzed_at.as_deref())?,
            },
        );
    }
    plan_column_stats_usage_write(snapshot, catalog, &usage, now)
}

/// Plans pinned Go `SaveColumnStatsUsageForTable` for an arbitrary usage map.
///
/// Every entry is replaced, including explicit NULL timestamps. The map's
/// iteration order is intentionally unspecified, as it is in Go.
pub fn plan_column_stats_usage_write<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    usage: &HashMap<TableItemID, ColumnStatsTimeInfo>,
    now: Time,
) -> Result<StatsWritePlan, StatsWriteError> {
    let mut plan = StatsWritePlan::default();
    let table = locate(catalog, "column_stats_usage")?;
    let mut rows_by_table = BTreeMap::new();
    for item in usage.keys() {
        if !rows_by_table.contains_key(&item.table_id) {
            rows_by_table.insert(
                item.table_id,
                StatsRows::open(snapshot, table, &["table_id", "column_id"], item.table_id)?,
            );
        }
    }
    for (item, times) in usage {
        let rows = rows_by_table
            .get_mut(&item.table_id)
            .expect("the table's rows were opened above");
        let mut values = defaults_row(table, now)?;
        set(table, &mut values, "table_id", Datum::Int(item.table_id));
        set(table, &mut values, "column_id", Datum::Int(item.id));
        set(
            table,
            &mut values,
            "last_used_at",
            predicate_time_value(table, "last_used_at", times.last_used_at)?,
        );
        set(
            table,
            &mut values,
            "last_analyzed_at",
            predicate_time_value(table, "last_analyzed_at", times.last_analyzed_at)?,
        );
        rows.store(snapshot, catalog, &values, &mut plan)?;
    }
    for rows in rows_by_table.values() {
        rows.publish_watermark(catalog, &mut plan)?;
    }
    Ok(plan)
}

/// Plans one pinned Go `DumpColStatsUsageEntries` batch.
///
/// The caller owns Go's 2,048-row batching and commits each returned plan in
/// its own restricted transaction. Existing non-NULL timestamps advance only
/// after the pinned twelve-hour threshold; a missing/NULL value is written
/// immediately.
pub fn plan_column_stats_usage_dump<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    entries: &[(TableItemID, Time)],
    now: Time,
) -> Result<StatsWritePlan, StatsWriteError> {
    let mut plan = StatsWritePlan::default();
    let table = locate(catalog, "column_stats_usage")?;
    let mut rows_by_table = BTreeMap::new();
    for (item, _) in entries {
        if !rows_by_table.contains_key(&item.table_id) {
            rows_by_table.insert(
                item.table_id,
                StatsRows::open(snapshot, table, &["table_id", "column_id"], item.table_id)?,
            );
        }
    }
    let last_used_id = column_id(table, "last_used_at")?;
    let threshold_nanoseconds = i64::try_from(
        tidb_stats_handle_usage::COL_STATS_USAGE_LAST_USED_THROTTLE_INTERVAL.as_nanos(),
    )
    .expect("twelve hours fits in i64 nanoseconds");
    for (item, incoming) in entries {
        let rows = rows_by_table
            .get_mut(&item.table_id)
            .expect("the table's rows were opened above");
        let mut values = defaults_row(table, now)?;
        set(table, &mut values, "table_id", Datum::Int(item.table_id));
        set(table, &mut values, "column_id", Datum::Int(item.id));
        let identity = identity_of(table, &["table_id", "column_id"], &values)?;
        if let Some(stored) = rows.existing_values(&identity) {
            values = stored.clone();
        }
        let should_update = match values.get(&last_used_id) {
            None | Some(Datum::Null) => true,
            Some(Datum::Time(stored)) => incoming
                .sub(*stored, &chrono::Utc)
                .is_ok_and(|elapsed| elapsed.nanoseconds() >= threshold_nanoseconds),
            Some(_) => true,
        };
        if !should_update {
            continue;
        }
        set(
            table,
            &mut values,
            "last_used_at",
            predicate_time_value(table, "last_used_at", Some(*incoming))?,
        );
        rows.store(snapshot, catalog, &values, &mut plan)?;
    }
    for rows in rows_by_table.values() {
        rows.publish_watermark(catalog, &mut plan)?;
    }
    Ok(plan)
}

/// Plans pinned Go `storage.UpdateStatsMeta` for one prepared delta batch.
pub fn plan_stats_delta_updates<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    updates: &[tidb_stats_handle_usage::DeltaUpdate],
    version: u64,
    now: Time,
) -> Result<StatsWritePlan, StatsWriteError> {
    let mut grouped = BTreeMap::<(bool, i64), tidb_stats_handle_usage::TableDelta>::new();
    for update in updates.iter().filter(|update| update.delta.count != 0) {
        grouped
            .entry((update.is_locked, update.table_id))
            .or_default()
            .merge_from(update.delta);
    }
    let mut plan = StatsWritePlan::default();
    for ((locked, table_id), delta) in grouped {
        let table = locate(
            catalog,
            if locked {
                "stats_table_locked"
            } else {
                "stats_meta"
            },
        )?;
        let mut rows = StatsRows::open(snapshot, table, &["table_id"], table_id)?;
        let mut values = defaults_row(table, now)?;
        set(table, &mut values, "table_id", Datum::Int(table_id));
        let identity = identity_of(table, &["table_id"], &values)?;
        let existed = rows.existing_values(&identity).is_some();
        if let Some(stored) = rows.existing_values(&identity) {
            values = stored.clone();
        }
        let current_modify =
            signed_value(values.get(&column_id(table, "modify_count")?)).unwrap_or_default();
        let current_count =
            signed_value(values.get(&column_id(table, "count")?)).unwrap_or_default();
        let count = if locked || delta.delta >= 0 {
            current_count.wrapping_add(delta.delta)
        } else if existed {
            current_count
                .saturating_sub(delta.delta.wrapping_neg())
                .max(0)
        } else {
            delta.delta.wrapping_neg()
        };
        set(table, &mut values, "version", Datum::UInt(version));
        set(
            table,
            &mut values,
            "modify_count",
            Datum::Int(current_modify.wrapping_add(delta.count)),
        );
        set(table, &mut values, "count", Datum::Int(count));
        rows.store(snapshot, catalog, &values, &mut plan)?;
        rows.publish_watermark(catalog, &mut plan)?;
    }
    Ok(plan)
}

/// Reads Go `GetLockedTables`' stored ID set from the dump transaction snapshot.
pub fn load_stats_locked_table_ids<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
) -> Result<std::collections::HashSet<i64>, StatsWriteError> {
    let table = locate(catalog, "stats_table_locked")?;
    let table_id_column = column_id(table, "table_id")?;
    read_rows_with_keys(snapshot, table, None).map(|rows| {
        rows.into_iter()
            .filter_map(|(_, values)| signed_value(values.get(&table_id_column)))
            .collect()
    })
}

/// Plans pinned Go `GetPredicateColumns`, including its same-transaction
/// cleanup of usage rows for columns no longer present in the latest schema.
///
/// Go executes DELETE before SELECT. This mutation planner cannot make staged
/// mutations visible through `MetaSnapshot`, so it filters the selected IDs
/// against the same current-column set while returning the exact deletes for
/// the caller to commit with the read transaction.
pub fn plan_get_predicate_columns<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    table_id: i64,
    current_column_ids: &[i64],
) -> Result<(Vec<i64>, StatsWritePlan), StatsWriteError> {
    let current = current_column_ids
        .iter()
        .map(|column_id| format!("{:?}", Datum::Int(*column_id)))
        .collect::<std::collections::HashSet<_>>();
    let columns = crate::cluster_predicate_column::predicate_columns(snapshot, catalog, table_id)?
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

/// Plans pinned Go `RecordHistoricalStatsMeta` for one committed statistics
/// version. The caller owns Go's feature switch and nonfatal error policy;
/// this function states only the transaction's exact `SELECT ... FOR UPDATE`
/// and `REPLACE` behavior.
pub fn plan_historical_stats_meta_write<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    table_id: i64,
    version: u64,
    source: &str,
    now: Time,
) -> Result<StatsWritePlan, StatsWriteError> {
    let meta = locate(catalog, "stats_meta")?;
    let meta_rows = StatsRows::open(snapshot, meta, &["table_id"], table_id)?;
    let identity = vec![format!("{:?}", Datum::Int(table_id))];
    let values = meta_rows.existing_values(&identity).ok_or_else(|| {
        StatsWriteError::HistoricalMeta("no historical meta stats can be recorded".to_owned())
    })?;
    let stored_version = unsigned_value(values.get(&column_id(meta, "version")?));
    if stored_version != Some(version) {
        return Err(StatsWriteError::HistoricalMeta(
            "no historical meta stats can be recorded".to_owned(),
        ));
    }
    let modify_count =
        signed_value(values.get(&column_id(meta, "modify_count")?)).ok_or_else(|| {
            StatsWriteError::HistoricalMeta("invalid stats_meta.modify_count".to_owned())
        })?;
    let count = signed_value(values.get(&column_id(meta, "count")?))
        .ok_or_else(|| StatsWriteError::HistoricalMeta("invalid stats_meta.count".to_owned()))?;

    let history = locate(catalog, "stats_meta_history")?;
    let mut rows = StatsRows::open(snapshot, history, &["table_id", "version"], table_id)?;
    let mut history_values = defaults_row(history, now)?;
    set(
        history,
        &mut history_values,
        "table_id",
        Datum::Int(table_id),
    );
    set(
        history,
        &mut history_values,
        "modify_count",
        Datum::Int(modify_count),
    );
    set(history, &mut history_values, "count", Datum::Int(count));
    set(
        history,
        &mut history_values,
        "version",
        Datum::UInt(version),
    );
    set(
        history,
        &mut history_values,
        "source",
        Datum::Bytes(source.as_bytes().to_vec()),
    );
    set(
        history,
        &mut history_values,
        "create_time",
        Datum::Time(now),
    );
    let mut plan = StatsWritePlan::default();
    rows.store(snapshot, catalog, &history_values, &mut plan)?;
    rows.publish_watermark(catalog, &mut plan)?;
    Ok(plan)
}

/// Pinned Go `history.RecordHistoricalStatsToStorage`: gzip one statistics
/// JSON object, split it below the `LONG_BLOB` row limit, and upsert each
/// `(table_id, version, seq_no)` block with one shared microsecond timestamp.
pub fn plan_historical_stats_data_write<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    physical_id: i64,
    json: &JsonTable,
    now: Time,
) -> Result<(u64, StatsWritePlan), StatsWriteError> {
    const MAX_COLUMN_SIZE: usize = 5 << 20;
    let version = match json.partitions.as_ref() {
        Some(partitions) if !partitions.is_empty() => partitions
            .values()
            .map(|partition| {
                partition
                    .as_ref()
                    .expect("nil partition in historical statistics JSON")
                    .version
            })
            .max()
            .unwrap_or(0),
        _ => json.version,
    };
    let blocks = tidb_executor::load_stats::json_table_to_blocks(json, MAX_COLUMN_SIZE)
        .map_err(|error| StatsWriteError::HistoricalData(error.to_string()))?;
    let history = locate(catalog, "stats_history")?;
    let mut rows = StatsRows::open(
        snapshot,
        history,
        &["table_id", "version", "seq_no"],
        physical_id,
    )?;
    let mut plan = StatsWritePlan::default();
    for (sequence, block) in blocks.into_iter().enumerate() {
        let mut values = defaults_row(history, now)?;
        set(history, &mut values, "table_id", Datum::Int(physical_id));
        set(history, &mut values, "stats_data", Datum::Bytes(block));
        set(history, &mut values, "seq_no", Datum::Int(sequence as i64));
        set(history, &mut values, "version", Datum::UInt(version));
        set(history, &mut values, "create_time", Datum::Time(now));
        rows.store(snapshot, catalog, &values, &mut plan)?;
    }
    rows.publish_watermark(catalog, &mut plan)?;
    Ok((version, plan))
}

/// Plans one pinned Go `ClearOutdatedHistoryStats` delete statement.
///
/// Go deletes metadata in batches of 1,000 and payload rows in batches of 50,
/// using `idx_create_time` for both. The caller deliberately owns the loop
/// and transaction boundary: every invocation corresponds to one restricted
/// SQL statement and therefore one independent autocommit transaction.
fn plan_outdated_historical_stats_delete<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    table_name: &str,
    cutoff: Time,
    limit: usize,
) -> Result<StatsWritePlan, StatsWriteError> {
    let table = locate(catalog, table_name)?;
    let create_time_id = column_id(table, "create_time")?;
    let stale = read_rows_with_keys_from_index(snapshot, table, "idx_create_time")?
        .into_iter()
        .filter_map(|(key, values)| {
            let Datum::Time(create_time) = values.get(&create_time_id)? else {
                return None;
            };
            (create_time.compare(cutoff) != std::cmp::Ordering::Greater).then_some((key, values))
        })
        .collect::<Vec<_>>();

    let clustered = !matches!(HandleLayout::of(table), HandleLayout::RowId);
    let mut plan = StatsWritePlan::default();
    for (key, values) in stale.into_iter().take(limit) {
        if clustered {
            plan.mutations.extend(delete_clustered_row(table, &values)?);
        } else {
            plan.mutations.extend(delete_row(table, &key, &values)?);
        }
    }
    Ok(plan)
}

/// Plans one pinned Go 1,000-row `stats_meta_history` expiry statement.
pub fn plan_outdated_historical_meta_delete<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    cutoff: Time,
    limit: usize,
) -> Result<StatsWritePlan, StatsWriteError> {
    plan_outdated_historical_stats_delete(snapshot, catalog, "stats_meta_history", cutoff, limit)
}

/// Plans one pinned Go 50-row `stats_history` expiry statement.
pub fn plan_outdated_historical_data_delete<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    cutoff: Time,
    limit: usize,
) -> Result<StatsWritePlan, StatsWriteError> {
    plan_outdated_historical_stats_delete(snapshot, catalog, "stats_history", cutoff, limit)
}

/// Counts the metadata rows selected by pinned Go
/// `ClearOutdatedHistoryStats`' opening `SELECT count(*)` statement.
pub fn count_outdated_historical_stats<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    cutoff: Time,
) -> Result<usize, StatsWriteError> {
    let table = locate(catalog, "stats_meta_history")?;
    let create_time_id = column_id(table, "create_time")?;
    Ok(
        read_rows_with_keys_from_index(snapshot, table, "idx_create_time")?
            .into_iter()
            .filter(|(_, values)| {
                matches!(
                    values.get(&create_time_id),
                    Some(Datum::Time(create_time))
                        if create_time.compare(cutoff) != std::cmp::Ordering::Greater
                )
            })
            .count(),
    )
}

fn plan_historical_stats_table_delete<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    table_name: &str,
    identity: &'static [&'static str],
    physical_id: i64,
) -> Result<StatsWritePlan, StatsWriteError> {
    let table = locate(catalog, table_name)?;
    let mut rows = StatsRows::open(snapshot, table, identity, physical_id)?;
    let mut plan = StatsWritePlan::default();
    rows.retract_remaining(&mut plan)?;
    Ok(plan)
}

/// Plans pinned Go `gcHistoryStatsFromKV`'s first statement, which deletes
/// every payload block for one dropped physical table.
pub fn plan_historical_stats_data_delete_for_table<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    physical_id: i64,
) -> Result<StatsWritePlan, StatsWriteError> {
    plan_historical_stats_table_delete(
        snapshot,
        catalog,
        "stats_history",
        &["table_id", "version", "seq_no"],
        physical_id,
    )
}

/// Plans pinned Go `gcHistoryStatsFromKV`'s second statement, which deletes
/// every metadata version for one dropped physical table.
pub fn plan_historical_stats_meta_delete_for_table<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    physical_id: i64,
) -> Result<StatsWritePlan, StatsWriteError> {
    plan_historical_stats_table_delete(
        snapshot,
        catalog,
        "stats_meta_history",
        &["table_id", "version"],
        physical_id,
    )
}

/// Reads pinned Go `GCStats`' version window from `mysql.stats_meta`.
pub fn load_stats_gc_candidates<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    last_gc: u64,
    gc_version: u64,
) -> Result<Vec<i64>, StatsWriteError> {
    let table = locate(catalog, "stats_meta")?;
    let view = SystemTableView::project("mysql.stats_meta", table, &["table_id", "version"]);
    let mut candidates = Vec::new();
    for (key, value) in scan_system_table(snapshot, &view)? {
        let row = SystemRow::parse(&view, &key, &value)?;
        let Some(table_id) = row.i64("table_id")? else {
            continue;
        };
        let version = row.u64("version")?.unwrap_or_default();
        if version >= last_gc && version < gc_version {
            candidates.push(table_id);
        }
    }
    Ok(candidates)
}

/// Reads pinned Go `gcTableStats`' `(is_index, hist_id)` rows for one table.
pub fn load_stats_gc_histograms<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    physical_id: i64,
) -> Result<Vec<(bool, i64)>, StatsWriteError> {
    let view = SystemTableView::locate(
        catalog,
        "stats_histograms",
        &["table_id", "is_index", "hist_id"],
    )?;
    let mut histograms = Vec::new();
    for (key, value) in scan_system_table_prefixed(snapshot, &view, &[Datum::Int(physical_id)])? {
        let row = SystemRow::parse(&view, &key, &value)?;
        if row.i64("table_id")? != Some(physical_id) {
            continue;
        }
        let Some(hist_id) = row.i64("hist_id")? else {
            continue;
        };
        histograms.push((row.i64("is_index")?.unwrap_or_default() == 1, hist_id));
    }
    Ok(histograms)
}

/// Plans pinned Go `gcTableStats`' second dropped-table phase: delete only
/// the `stats_meta` row after a prior pass removed every histogram row.
pub fn plan_stats_meta_delete_for_table<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    physical_id: i64,
) -> Result<StatsWritePlan, StatsWriteError> {
    plan_historical_stats_table_delete(snapshot, catalog, "stats_meta", &["table_id"], physical_id)
}

const STATS_GC_LAST_TS: &str = "tidb_stats_gc_last_ts";

/// Reads pinned Go `getLastGCTimestamp` from `mysql.tidb`.
pub fn load_stats_gc_timestamp<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
) -> Result<u64, StatsWriteError> {
    let view = SystemTableView::locate(catalog, "tidb", &["variable_name", "variable_value"])?;
    for (key, value) in scan_system_table(snapshot, &view)? {
        let row = SystemRow::parse(&view, &key, &value)?;
        if row.text("variable_name")?.as_deref() != Some(STATS_GC_LAST_TS) {
            continue;
        }
        let value = row.text("variable_value")?.unwrap_or_default();
        return value.parse::<u64>().map_err(|error| {
            StatsWriteError::HistoricalMeta(format!(
                "invalid {STATS_GC_LAST_TS} value {value:?}: {error}"
            ))
        });
    }
    Ok(0)
}

/// Plans pinned Go `writeGCTimestampToKV`'s one upsert into `mysql.tidb`.
pub fn plan_stats_gc_timestamp_write<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    gc_version: u64,
    now: Time,
) -> Result<StatsWritePlan, StatsWriteError> {
    let table = locate(catalog, "tidb")?;
    let mut rows = StatsRows::open_all(snapshot, table, &["variable_name"])?;
    let name_type = table
        .cols()
        .iter_deref()
        .find(|column| column.read().name.lowercase() == "variable_name")
        .map(|column| column.read().field_type.clone())
        .ok_or_else(|| StatsWriteError::MissingTable("mysql.tidb.variable_name".to_owned()))?;
    let value_type = table
        .cols()
        .iter_deref()
        .find(|column| column.read().name.lowercase() == "variable_value")
        .map(|column| column.read().field_type.clone())
        .ok_or_else(|| StatsWriteError::MissingTable("mysql.tidb.variable_value".to_owned()))?;
    let name = Datum::String(tidb_datatype::StringDatum::new(
        STATS_GC_LAST_TS.as_bytes().to_vec(),
        name_type.collation(),
    ));
    let identity = vec![format!("{name:?}")];
    let mut values = rows
        .existing_values(&identity)
        .cloned()
        .unwrap_or(defaults_row(table, now)?);
    set(table, &mut values, "variable_name", name);
    set(
        table,
        &mut values,
        "variable_value",
        Datum::String(tidb_datatype::StringDatum::new(
            gc_version.to_string().into_bytes(),
            value_type.collation(),
        )),
    );
    let mut plan = StatsWritePlan::default();
    rows.store(snapshot, catalog, &values, &mut plan)?;
    rows.publish_watermark(catalog, &mut plan)?;
    Ok(plan)
}

fn retract_stats_item_rows<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    table_name: &str,
    identity: &'static [&'static str],
    physical_id: i64,
    prefix: &[String],
    plan: &mut StatsWritePlan,
) -> Result<(), StatsWriteError> {
    let table = locate(catalog, table_name)?;
    let mut rows = StatsRows::open(snapshot, table, identity, physical_id)?;
    rows.retract_prefix(prefix, plan)
}

/// Plans pinned Go `deleteHistStatsFromKV` as its one wrapped transaction:
/// advance both metadata versions, then delete one column/index histogram and
/// every dependent TopN, bucket, FM-sketch, and column-usage row.
pub fn plan_stats_item_delete<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    physical_id: i64,
    hist_id: i64,
    is_index: bool,
    version: u64,
) -> Result<StatsWritePlan, StatsWriteError> {
    let mut plan = plan_stats_meta_version_refresh(snapshot, catalog, physical_id, version)?;
    let prefix = [
        format!("{:?}", Datum::Int(physical_id)),
        format!("{:?}", Datum::Int(i64::from(is_index))),
        format!("{:?}", Datum::Int(hist_id)),
    ];
    for (table_name, identity) in [
        ("stats_histograms", &["table_id", "is_index", "hist_id"][..]),
        (
            "stats_top_n",
            &["table_id", "is_index", "hist_id", "value"][..],
        ),
        (
            "stats_buckets",
            &["table_id", "is_index", "hist_id", "bucket_id"][..],
        ),
        ("stats_fm_sketch", &["table_id", "is_index", "hist_id"][..]),
    ] {
        retract_stats_item_rows(
            snapshot,
            catalog,
            table_name,
            identity,
            physical_id,
            &prefix,
            &mut plan,
        )?;
    }
    if !is_index {
        let usage_prefix = [
            format!("{:?}", Datum::Int(physical_id)),
            format!("{:?}", Datum::Int(hist_id)),
        ];
        retract_stats_item_rows(
            snapshot,
            catalog,
            "column_stats_usage",
            &["table_id", "column_id"],
            physical_id,
            &usage_prefix,
            &mut plan,
        )?;
    }
    Ok(plan)
}

fn retract_stats_table_rows<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    table_name: &str,
    identity: &'static [&'static str],
    physical_id: i64,
    plan: &mut StatsWritePlan,
) -> Result<(), StatsWriteError> {
    let table = locate(catalog, table_name)?;
    let mut rows = StatsRows::open(snapshot, table, identity, physical_id)?;
    rows.retract_remaining(plan)
}

fn reset_table_histograms<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    physical_id: i64,
    version: u64,
    plan: &mut StatsWritePlan,
) -> Result<(), StatsWriteError> {
    let table = locate(catalog, "stats_histograms")?;
    let mut rows = StatsRows::open(
        snapshot,
        table,
        &["table_id", "is_index", "hist_id"],
        physical_id,
    )?;
    let stored = rows
        .existing
        .values()
        .map(|(_, values)| values.clone())
        .collect::<Vec<_>>();
    for mut values in stored {
        for column in [
            "distinct_count",
            "null_count",
            "tot_col_size",
            "modify_count",
            "stats_ver",
            "flag",
        ] {
            set(table, &mut values, column, Datum::Int(0));
        }
        set(table, &mut values, "version", Datum::UInt(version));
        set(table, &mut values, "cm_sketch", Datum::Null);
        set(table, &mut values, "correlation", Datum::Real(0.0));
        set(table, &mut values, "last_analyze_pos", Datum::Null);
        rows.store(snapshot, catalog, &values, plan)?;
    }
    rows.publish_watermark(catalog, plan)
}

/// Plans pinned Go `DeleteTableStatsFromKV` for every supplied physical ID in
/// its single wrapped transaction. `soft` retains zeroed histogram metadata;
/// hard deletion removes it. Both modes remove all dependent payload, usage,
/// analyze-option, and statistics-lock rows after advancing metadata versions.
pub fn plan_delete_table_stats<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    physical_ids: &[i64],
    soft: bool,
    version: u64,
) -> Result<StatsWritePlan, StatsWriteError> {
    let mut plan = StatsWritePlan::default();
    for &physical_id in physical_ids {
        plan.mutations.extend(
            plan_stats_meta_version_refresh(snapshot, catalog, physical_id, version)?.mutations,
        );
        if soft {
            reset_table_histograms(snapshot, catalog, physical_id, version, &mut plan)?;
        } else {
            retract_stats_table_rows(
                snapshot,
                catalog,
                "stats_histograms",
                &["table_id", "is_index", "hist_id"],
                physical_id,
                &mut plan,
            )?;
        }
        for (table_name, identity) in [
            (
                "stats_buckets",
                &["table_id", "is_index", "hist_id", "bucket_id"][..],
            ),
            (
                "stats_top_n",
                &["table_id", "is_index", "hist_id", "value"][..],
            ),
            ("stats_fm_sketch", &["table_id", "is_index", "hist_id"][..]),
            ("column_stats_usage", &["table_id", "column_id"][..]),
            ("analyze_options", &["table_id"][..]),
            ("stats_table_locked", &["table_id"][..]),
        ] {
            retract_stats_table_rows(
                snapshot,
                catalog,
                table_name,
                identity,
                physical_id,
                &mut plan,
            )?;
        }
    }
    Ok(plan)
}

fn signed_value(value: Option<&Datum>) -> Option<i64> {
    match value {
        Some(Datum::Int(value)) => Some(*value),
        Some(Datum::UInt(value)) => i64::try_from(*value).ok(),
        _ => None,
    }
}

fn unsigned_value(value: Option<&Datum>) -> Option<u64> {
    match value {
        Some(Datum::UInt(value)) => Some(*value),
        Some(Datum::Int(value)) => u64::try_from(*value).ok(),
        _ => None,
    }
}

fn parse_predicate_time(value: Option<&str>) -> Result<Option<Time>, StatsWriteError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let parsed = parse_time(
        value,
        TimeType::Timestamp,
        MAX_FSP,
        false,
        false,
        false,
        &chrono::Utc,
    )
    .map_err(|error| StatsWriteError::PredicateTime(format!("{value:?}: {error}")))?;
    Ok(Some(parsed.time))
}

fn predicate_time_value(
    table: &TableInfo,
    column: &str,
    value: Option<Time>,
) -> Result<Datum, StatsWriteError> {
    let Some(value) = value else {
        return Ok(Datum::Null);
    };
    let field_type = table
        .find_public_column_by_name(column)
        .ok_or_else(|| StatsWriteError::MissingTable(format!("mysql.column_stats_usage.{column}")))?
        .read()
        .field_type
        .clone();
    Datum::Time(value)
        .convert_to(&field_type, ConversionFlags::from_bits(0))
        .map(|converted| converted.value)
        .map_err(|error| StatsWriteError::PredicateTime(format!("{value:?}: {error}")))
}

fn plan_loaded_meta<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    table_id: i64,
    count: i64,
    version: u64,
    now: Time,
    plan: &mut StatsWritePlan,
) -> Result<(), StatsWriteError> {
    let table = locate(catalog, "stats_meta")?;
    let mut rows = StatsRows::open(snapshot, table, &["table_id"], table_id)?;
    if count < 0 {
        let identity = vec![format!("{:?}", Datum::Int(table_id))];
        let Some(mut values) = rows.existing_values(&identity).cloned() else {
            // Go's UPDATE matches no row and still lets the histogram write
            // continue; it does not synthesize a stats_meta row here.
            return Ok(());
        };
        set(table, &mut values, "version", Datum::UInt(version));
        set(
            table,
            &mut values,
            "last_stats_histograms_version",
            Datum::UInt(version),
        );
        rows.store(snapshot, catalog, &values, plan)?;
        return rows.publish_watermark(catalog, plan);
    }
    let mut values = defaults_row(table, now)?;
    set(table, &mut values, "table_id", Datum::Int(table_id));
    set(table, &mut values, "version", Datum::UInt(version));
    set(table, &mut values, "count", Datum::Int(count));
    set(table, &mut values, "modify_count", Datum::Int(0));
    set(
        table,
        &mut values,
        "last_stats_histograms_version",
        Datum::UInt(version),
    );
    rows.store(snapshot, catalog, &values, plan)?;
    rows.publish_watermark(catalog, plan)
}

fn plan_loaded_histogram<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    table_id: i64,
    item: &ClusterStatsItem,
    version: u64,
    now: Time,
    plan: &mut StatsWritePlan,
) -> Result<(), StatsWriteError> {
    let table = locate(catalog, "stats_histograms")?;
    let mut rows = StatsRows::open(
        snapshot,
        table,
        &["table_id", "is_index", "hist_id"],
        table_id,
    )?;
    let mut values = defaults_row(table, now)?;
    set(table, &mut values, "table_id", Datum::Int(table_id));
    set(
        table,
        &mut values,
        "is_index",
        Datum::Int(i64::from(item.is_index)),
    );
    set(table, &mut values, "hist_id", Datum::Int(item.id));
    set(
        table,
        &mut values,
        "distinct_count",
        Datum::Int(item.histogram.ndv),
    );
    set(
        table,
        &mut values,
        "null_count",
        Datum::Int(item.histogram.null_count),
    );
    set(
        table,
        &mut values,
        "tot_col_size",
        Datum::Int(item.histogram.tot_col_size.max(0)),
    );
    set(table, &mut values, "version", Datum::UInt(version));
    let cms = encode_cmsketch_without_topn(item.cms.as_ref())
        .map_err(|error| StatsWriteError::Cms(error.to_string()))?;
    set(
        table,
        &mut values,
        "cm_sketch",
        cms.map_or(Datum::Null, Datum::Bytes),
    );
    set(table, &mut values, "stats_ver", Datum::Int(item.stats_ver));
    set(
        table,
        &mut values,
        "correlation",
        Datum::Real(item.histogram.correlation),
    );
    rows.store(snapshot, catalog, &values, plan)?;
    rows.publish_watermark(catalog, plan)?;
    plan.histogram_count += 1;
    Ok(())
}

fn loaded_item_prefix(table_id: i64, item: &ClusterStatsItem) -> Vec<String> {
    [
        Datum::Int(table_id),
        Datum::Int(i64::from(item.is_index)),
        Datum::Int(item.id),
    ]
    .iter()
    .map(|value| format!("{value:?}"))
    .collect()
}

fn plan_loaded_buckets<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    table_id: i64,
    item: &ClusterStatsItem,
    now: Time,
    plan: &mut StatsWritePlan,
) -> Result<(), StatsWriteError> {
    let table = locate(catalog, "stats_buckets")?;
    let mut rows = StatsRows::open(
        snapshot,
        table,
        &["table_id", "is_index", "hist_id", "bucket_id"],
        table_id,
    )?;
    let mut previous_count = 0_i64;
    for (bucket_id, bucket) in item.histogram.buckets.iter().enumerate() {
        let mut values = defaults_row(table, now)?;
        set(table, &mut values, "table_id", Datum::Int(table_id));
        set(
            table,
            &mut values,
            "is_index",
            Datum::Int(i64::from(item.is_index)),
        );
        set(table, &mut values, "hist_id", Datum::Int(item.id));
        set(
            table,
            &mut values,
            "bucket_id",
            Datum::Int(bucket_id as i64),
        );
        set(
            table,
            &mut values,
            "count",
            Datum::Int(bucket.count - previous_count),
        );
        previous_count = bucket.count;
        set(table, &mut values, "repeats", Datum::Int(bucket.repeat));
        set(table, &mut values, "ndv", Datum::Int(bucket.ndv));
        set(
            table,
            &mut values,
            "lower_bound",
            bound_blob(&bucket.lower_bound)?,
        );
        set(
            table,
            &mut values,
            "upper_bound",
            bound_blob(&bucket.upper_bound)?,
        );
        rows.store(snapshot, catalog, &values, plan)?;
        plan.bucket_count += 1;
    }
    rows.retract_prefix(&loaded_item_prefix(table_id, item), plan)?;
    rows.publish_watermark(catalog, plan)
}

fn plan_loaded_topn<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    table_id: i64,
    item: &ClusterStatsItem,
    now: Time,
    plan: &mut StatsWritePlan,
) -> Result<(), StatsWriteError> {
    let table = locate(catalog, "stats_top_n")?;
    let mut rows = StatsRows::open(
        snapshot,
        table,
        &["table_id", "is_index", "hist_id", "value"],
        table_id,
    )?;
    if let Some(topn) = &item.topn {
        for entry in topn.entries() {
            let mut values = defaults_row(table, now)?;
            set(table, &mut values, "table_id", Datum::Int(table_id));
            set(
                table,
                &mut values,
                "is_index",
                Datum::Int(i64::from(item.is_index)),
            );
            set(table, &mut values, "hist_id", Datum::Int(item.id));
            set(
                table,
                &mut values,
                "value",
                Datum::Bytes(entry.encoded.clone()),
            );
            set(table, &mut values, "count", Datum::UInt(entry.count));
            rows.store(snapshot, catalog, &values, plan)?;
            plan.topn_count += 1;
        }
    }
    rows.retract_prefix(&loaded_item_prefix(table_id, item), plan)?;
    rows.publish_watermark(catalog, plan)
}

fn plan_loaded_fm_delete<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    table_id: i64,
    item: &ClusterStatsItem,
    plan: &mut StatsWritePlan,
) -> Result<(), StatsWriteError> {
    let table = locate(catalog, "stats_fm_sketch")?;
    let mut rows = StatsRows::open(
        snapshot,
        table,
        &["table_id", "is_index", "hist_id"],
        table_id,
    )?;
    rows.retract_prefix(&loaded_item_prefix(table_id, item), plan)?;
    rows.publish_watermark(catalog, plan)
}

fn plan_meta<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    stats: &ClusterTableStats,
    now: Time,
    plan: &mut StatsWritePlan,
) -> Result<(), StatsWriteError> {
    let table = locate(catalog, "stats_meta")?;
    let mut rows = StatsRows::open(snapshot, table, &["table_id"], stats.table_id)?;
    let mut values = defaults_row(table, now)?;
    set(table, &mut values, "table_id", Datum::Int(stats.table_id));
    set(table, &mut values, "version", Datum::UInt(stats.version));
    set(
        table,
        &mut values,
        "modify_count",
        Datum::Int(stats.modify_count),
    );
    set(table, &mut values, "count", Datum::UInt(stats.row_count));
    // Go always stores `AnalyzeResults.Snapshot`. The
    // `tidb_enable_analyze_snapshot` switch changes count reconciliation,
    // not this metadata field (`save.go:193-250`).
    set(table, &mut values, "snapshot", Datum::UInt(stats.snapshot));
    // Added after `mysql.stats_meta` was first defined, so a cluster old
    // enough not to have it simply does not get it set.
    set(
        table,
        &mut values,
        "last_stats_histograms_version",
        Datum::UInt(stats.version),
    );
    rows.store(snapshot, catalog, &values, plan)?;
    // Deliberately no retraction: `mysql.stats_meta` holds one row per table
    // and the identity IS the table, so there is never a leftover. Retracting
    // here would only be a way to delete a row this `ANALYZE` did not write.
    rows.publish_watermark(catalog, plan)
}

fn plan_independent_index_meta<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    stats: &ClusterTableStats,
    now: Time,
    plan: &mut StatsWritePlan,
) -> Result<bool, StatsWriteError> {
    let table = locate(catalog, "stats_meta")?;
    let mut rows = StatsRows::open(snapshot, table, &["table_id"], stats.table_id)?;
    let identity = vec![format!("{:?}", Datum::Int(stats.table_id))];
    let inserted = rows.existing_values(&identity).is_none();
    let mut values = match rows.existing_values(&identity).cloned() {
        Some(values) => values,
        None => {
            let mut values = defaults_row(table, now)?;
            set(table, &mut values, "table_id", Datum::Int(stats.table_id));
            set(table, &mut values, "count", Datum::Int(0));
            set(table, &mut values, "snapshot", Datum::UInt(0));
            values
        }
    };
    set(table, &mut values, "version", Datum::UInt(stats.version));
    set(
        table,
        &mut values,
        "last_stats_histograms_version",
        Datum::UInt(stats.version),
    );
    rows.store(snapshot, catalog, &values, plan)?;
    rows.publish_watermark(catalog, plan)?;
    Ok(inserted)
}

fn plan_histograms<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    stats: &ClusterTableStats,
    now: Time,
    replace_all: bool,
    plan: &mut StatsWritePlan,
) -> Result<(), StatsWriteError> {
    let table = locate(catalog, "stats_histograms")?;
    let mut rows = StatsRows::open(
        snapshot,
        table,
        &["table_id", "is_index", "hist_id"],
        stats.table_id,
    )?;
    for item in stats.columns.iter().chain(&stats.indexes) {
        let mut values = defaults_row(table, now)?;
        set(table, &mut values, "table_id", Datum::Int(stats.table_id));
        set(
            table,
            &mut values,
            "is_index",
            Datum::Int(i64::from(item.is_index)),
        );
        set(table, &mut values, "hist_id", Datum::Int(item.id));
        set(
            table,
            &mut values,
            "distinct_count",
            Datum::Int(item.histogram.ndv),
        );
        set(
            table,
            &mut values,
            "null_count",
            Datum::Int(item.histogram.null_count),
        );
        // Go writes `GREATEST(tot_col_size, 0)`.
        set(
            table,
            &mut values,
            "tot_col_size",
            Datum::Int(item.histogram.tot_col_size.max(0)),
        );
        set(table, &mut values, "modify_count", Datum::Int(0));
        set(table, &mut values, "version", Datum::UInt(stats.version));
        // Analyze v2 stores no CMSketch, and Go's encoder turns its nil
        // sketch into a NULL column.
        set(table, &mut values, "cm_sketch", Datum::Null);
        set(table, &mut values, "stats_ver", Datum::Int(item.stats_ver));
        set(table, &mut values, "flag", Datum::Int(item.flag));
        set(
            table,
            &mut values,
            "correlation",
            Datum::Real(item.histogram.correlation),
        );
        rows.store(snapshot, catalog, &values, plan)?;
        plan.histogram_count += 1;
    }
    // A histogram this `ANALYZE` no longer produces -- a dropped index, a
    // column that became unanalyzable -- must lose its row, or the loader
    // would keep handing the planner statistics for something that is gone.
    retract_analyzed_items(&mut rows, stats, replace_all, plan)?;
    rows.publish_watermark(catalog, plan)
}

fn plan_buckets<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    stats: &ClusterTableStats,
    now: Time,
    replace_all: bool,
    plan: &mut StatsWritePlan,
) -> Result<(), StatsWriteError> {
    let table = locate(catalog, "stats_buckets")?;
    let mut rows = StatsRows::open(
        snapshot,
        table,
        &["table_id", "is_index", "hist_id", "bucket_id"],
        stats.table_id,
    )?;
    for item in stats.columns.iter().chain(&stats.indexes) {
        let mut previous_count = 0_i64;
        for (bucket_id, bucket) in item.histogram.buckets.iter().enumerate() {
            let mut values = defaults_row(table, now)?;
            set(table, &mut values, "table_id", Datum::Int(stats.table_id));
            set(
                table,
                &mut values,
                "is_index",
                Datum::Int(i64::from(item.is_index)),
            );
            set(table, &mut values, "hist_id", Datum::Int(item.id));
            set(
                table,
                &mut values,
                "bucket_id",
                Datum::Int(bucket_id as i64),
            );
            // The delta, not the cumulative count. See the module doc.
            set(
                table,
                &mut values,
                "count",
                Datum::Int(bucket.count - previous_count),
            );
            previous_count = bucket.count;
            set(table, &mut values, "repeats", Datum::Int(bucket.repeat));
            set(table, &mut values, "ndv", Datum::Int(bucket.ndv));
            set(
                table,
                &mut values,
                "lower_bound",
                bound_blob(&bucket.lower_bound)?,
            );
            set(
                table,
                &mut values,
                "upper_bound",
                bound_blob(&bucket.upper_bound)?,
            );
            rows.store(snapshot, catalog, &values, plan)?;
            plan.bucket_count += 1;
        }
    }
    // A shorter histogram than last time leaves its tail behind, and a stale
    // bucket read back as part of this histogram would be a range the table
    // no longer has.
    retract_analyzed_items(&mut rows, stats, replace_all, plan)?;
    rows.publish_watermark(catalog, plan)
}

fn plan_topn<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    stats: &ClusterTableStats,
    now: Time,
    replace_all: bool,
    plan: &mut StatsWritePlan,
) -> Result<(), StatsWriteError> {
    let table = locate(catalog, "stats_top_n")?;
    // A TopN row is named by its VALUE as well as by its histogram: one
    // histogram has many of them, and they are not numbered.
    let mut rows = StatsRows::open(
        snapshot,
        table,
        &["table_id", "is_index", "hist_id", "value"],
        stats.table_id,
    )?;
    for item in stats.columns.iter().chain(&stats.indexes) {
        let Some(topn) = item.topn.as_ref() else {
            continue;
        };
        for entry in topn.entries() {
            let mut values = defaults_row(table, now)?;
            set(table, &mut values, "table_id", Datum::Int(stats.table_id));
            set(
                table,
                &mut values,
                "is_index",
                Datum::Int(i64::from(item.is_index)),
            );
            set(table, &mut values, "hist_id", Datum::Int(item.id));
            set(
                table,
                &mut values,
                "value",
                Datum::Bytes(entry.encoded.clone()),
            );
            set(table, &mut values, "count", Datum::UInt(entry.count));
            rows.store(snapshot, catalog, &values, plan)?;
            plan.topn_count += 1;
        }
    }
    retract_analyzed_items(&mut rows, stats, replace_all, plan)?;
    rows.publish_watermark(catalog, plan)
}

fn plan_fm_sketches<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    stats: &ClusterTableStats,
    now: Time,
    replace_all: bool,
    dump_fm: bool,
    plan: &mut StatsWritePlan,
) -> Result<(), StatsWriteError> {
    let table = locate(catalog, "stats_fm_sketch")?;
    let mut rows = StatsRows::open(
        snapshot,
        table,
        &["table_id", "is_index", "hist_id"],
        stats.table_id,
    )?;
    if dump_fm {
        for item in stats.columns.iter().chain(&stats.indexes) {
            let Some(encoded) = encode_fm_sketch(item.fm_sketch.as_ref()) else {
                continue;
            };
            let mut values = defaults_row(table, now)?;
            set(table, &mut values, "table_id", Datum::Int(stats.table_id));
            set(
                table,
                &mut values,
                "is_index",
                Datum::Int(i64::from(item.is_index)),
            );
            set(table, &mut values, "hist_id", Datum::Int(item.id));
            set(table, &mut values, "value", Datum::Bytes(encoded));
            rows.store(snapshot, catalog, &values, plan)?;
        }
    }
    // Go deletes the previous row for every analyzed item even when this is
    // a logical/global write and `needDumpFMS` is false.
    retract_analyzed_items(&mut rows, stats, replace_all, plan)?;
    rows.publish_watermark(catalog, plan)
}

fn retract_analyzed_items(
    rows: &mut StatsRows<'_>,
    stats: &ClusterTableStats,
    replace_all: bool,
    plan: &mut StatsWritePlan,
) -> Result<(), StatsWriteError> {
    if replace_all {
        return rows.retract_remaining(plan);
    }
    for item in stats.columns.iter().chain(&stats.indexes) {
        rows.retract_prefix(
            &[
                format!("{:?}", Datum::Int(stats.table_id)),
                format!("{:?}", Datum::Int(i64::from(item.is_index))),
                format!("{:?}", Datum::Int(item.id)),
            ],
            plan,
        )?;
    }
    Ok(())
}

/// One `mysql.stats_*` table's current rows for one analyzed table, addressed
/// by the LOGICAL identity that names a row rather than by a physical handle.
///
/// # Why the identity, and not the key
///
/// The four statistics tables have changed handle shape across TiDB versions:
/// what is `PRIMARY KEY (table_id, is_index, hist_id) CLUSTERED` on a recent
/// server was `UNIQUE INDEX tbl(...)` over a `_tidb_rowid` on an older one,
/// and `mysql.stats_top_n` has never had a clustered handle at all. A writer
/// that derived a record key from a row's values would work on exactly one of
/// those shapes and silently store unaddressable rows on the others -- so
/// this reads what is there and matches on the columns that MEAN "this
/// histogram's third bucket", which every version agrees on.
///
/// # Two replace strategies, because there are two handle shapes
///
/// A clustered row's key is derived from its identity, so rewriting it is a
/// put over the same key. A `_tidb_rowid` row's handle is arbitrary, and any
/// secondary index over a column this write moves would be left pointing at
/// the old value -- so the old row is deleted whole and a new one inserted
/// under a freshly allocated handle. That is deliberately more churn than an
/// in-place update, and it is the only version-independent way to be correct.
struct StatsRows<'table> {
    table: &'table TableInfo,
    identity: &'static [&'static str],
    clustered: bool,
    /// Identity -> (record key, stored values), drained as rows are matched.
    existing: BTreeMap<Vec<String>, (Vec<u8>, RowValues)>,
    /// The next `_tidb_rowid` to hand out, once one has been reserved.
    next_row_id: Option<i64>,
}

impl<'table> StatsRows<'table> {
    fn open_all<S: MetaSnapshot>(
        snapshot: &mut S,
        table: &'table TableInfo,
        identity: &'static [&'static str],
    ) -> Result<Self, StatsWriteError> {
        let clustered = !matches!(HandleLayout::of(table), HandleLayout::RowId);
        let mut existing = BTreeMap::new();
        for (key, values) in read_rows_with_keys(snapshot, table, None)? {
            existing.insert(identity_of(table, identity, &values)?, (key, values));
        }
        Ok(Self {
            table,
            identity,
            clustered,
            existing,
            next_row_id: None,
        })
    }

    fn open<S: MetaSnapshot>(
        snapshot: &mut S,
        table: &'table TableInfo,
        identity: &'static [&'static str],
        table_id: i64,
    ) -> Result<Self, StatsWriteError> {
        let clustered = !matches!(HandleLayout::of(table), HandleLayout::RowId);
        let mut existing = BTreeMap::new();
        for (key, values) in read_rows_with_keys(snapshot, table, Some(table_id))? {
            existing.insert(identity_of(table, identity, &values)?, (key, values));
        }
        Ok(Self {
            table,
            identity,
            clustered,
            existing,
            next_row_id: None,
        })
    }

    fn store<S: MetaSnapshot>(
        &mut self,
        snapshot: &mut S,
        catalog: &ClusterCatalog,
        values: &RowValues,
        plan: &mut StatsWritePlan,
    ) -> Result<(), StatsWriteError> {
        let identity = identity_of(self.table, self.identity, values)?;
        let previous = self.existing.remove(&identity);
        if self.clustered {
            plan.mutations.extend(store_clustered_row(
                self.table,
                previous.as_ref().map(|(_, stored)| stored),
                values,
            )?);
            return Ok(());
        }
        // A row that is already there keeps its handle: see
        // [`rewrite_rowid_row`] for why a delete-and-reinsert would collide
        // with itself on any index whose columns did not move.
        if let Some((key, stored)) = previous {
            plan.mutations
                .extend(rewrite_rowid_row(self.table, &key, &stored, values)?);
            return Ok(());
        }
        let row_id = self.reserve_row_id(snapshot, catalog)?;
        plan.mutations
            .extend(insert_row(self.table, row_id, values)?);
        Ok(())
    }

    fn existing_values(&self, identity: &[String]) -> Option<&RowValues> {
        self.existing.get(identity).map(|(_, values)| values)
    }

    fn retract_remaining(&mut self, plan: &mut StatsWritePlan) -> Result<(), StatsWriteError> {
        for (key, stored) in std::mem::take(&mut self.existing).into_values() {
            if self.clustered {
                plan.mutations
                    .extend(delete_clustered_row(self.table, &stored)?);
            } else {
                plan.mutations
                    .extend(delete_row(self.table, &key, &stored)?);
            }
        }
        Ok(())
    }

    fn retract_prefix(
        &mut self,
        prefix: &[String],
        plan: &mut StatsWritePlan,
    ) -> Result<(), StatsWriteError> {
        let identities = self
            .existing
            .keys()
            .filter(|identity| identity.starts_with(prefix))
            .cloned()
            .collect::<Vec<_>>();
        for identity in identities {
            let (key, stored) = self
                .existing
                .remove(&identity)
                .expect("identity came from the existing map");
            if self.clustered {
                plan.mutations
                    .extend(delete_clustered_row(self.table, &stored)?);
            } else {
                plan.mutations
                    .extend(delete_row(self.table, &key, &stored)?);
            }
        }
        Ok(())
    }

    /// Records the highest `_tidb_rowid` this plan used, so the next writer
    /// does not hand out a handle this one already did.
    fn publish_watermark(
        &self,
        catalog: &ClusterCatalog,
        plan: &mut StatsWritePlan,
    ) -> Result<(), StatsWriteError> {
        let Some(next) = self.next_row_id else {
            return Ok(());
        };
        plan.mutations.push(
            OptimisticMutation::meta_put(
                key::auto_table_id_kv_key(system_db_id(catalog)?, self.table.id),
                value::encode_int_value(next - 1),
            )
            .map_err(|error| StatsWriteError::Encode(RowEncodeError(error.to_string())))?,
        );
        Ok(())
    }

    fn reserve_row_id<S: MetaSnapshot>(
        &mut self,
        snapshot: &mut S,
        catalog: &ClusterCatalog,
    ) -> Result<i64, StatsWriteError> {
        let row_id = match self.next_row_id {
            Some(next) => next,
            None => first_free_row_id(snapshot, catalog, self.table)?,
        };
        self.next_row_id = Some(row_id + 1);
        Ok(row_id)
    }
}

/// The values of the columns that name one statistics row, rendered so that
/// two rows compare equal exactly when they mean the same thing.
fn identity_of(
    table: &TableInfo,
    identity: &'static [&'static str],
    values: &RowValues,
) -> Result<Vec<String>, StatsWriteError> {
    identity
        .iter()
        .map(|name| {
            let id = column_id(table, name)?;
            Ok(format!("{:?}", values.get(&id).unwrap_or(&Datum::Null)))
        })
        .collect()
}

/// Go `convertBoundToBlob`: `Datum.ConvertTo(TypeBlob)`.
///
/// An index bound and a new-collation string bound are already `Bytes` and
/// convert to themselves; every other column bound becomes its text form,
/// which is what [`crate::cluster_stats_load::decode_bound`] parses back at
/// the column's declared type.
fn bound_blob(bound: &Datum) -> Result<Datum, StatsWriteError> {
    if bound.is_null() {
        return Ok(Datum::Null);
    }
    let blob = FieldType::new(FieldTypeCode::Blob).with_flen(UNSPECIFIED_LENGTH);
    bound
        .convert_to(&blob, ConversionFlags::from_bits(0))
        .map(|converted| match converted.value {
            Datum::String(string) => Datum::Bytes(string.bytes().to_vec()),
            other => other,
        })
        .map_err(|error| StatsWriteError::Bound(error.to_string()))
}

fn locate<'catalog>(
    catalog: &'catalog ClusterCatalog,
    name: &str,
) -> Result<&'catalog TableInfo, StatsWriteError> {
    catalog
        .databases
        .iter()
        .find(|database| database.info.name.lowercase() == SYSTEM_DB)
        .and_then(|database| {
            database
                .tables
                .iter()
                .find(|stored| stored.name.lowercase() == name)
        })
        .ok_or_else(|| StatsWriteError::MissingTable(format!("{SYSTEM_DB}.{name}")))
}

fn system_db_id(catalog: &ClusterCatalog) -> Result<i64, StatsWriteError> {
    catalog
        .databases
        .iter()
        .find(|database| database.info.name.lowercase() == SYSTEM_DB)
        .map(|database| database.info.id)
        .ok_or_else(|| StatsWriteError::MissingTable(SYSTEM_DB.to_owned()))
}

fn column_id(table: &TableInfo, name: &str) -> Result<i64, StatsWriteError> {
    table
        .find_public_column_by_name(name)
        .map(|column| column.read().id)
        .ok_or_else(|| {
            StatsWriteError::MissingTable(format!("{SYSTEM_DB}.{}.{name}", table.name.original()))
        })
}

/// Sets one column, if the cluster's table has it.
///
/// A column a newer TiDB added -- `last_stats_histograms_version` -- is
/// simply absent on an older cluster, and a writer that insisted on it could
/// not store statistics there at all.
fn set(table: &TableInfo, values: &mut RowValues, name: &str, value: Datum) {
    if let Ok(id) = column_id(table, name) {
        values.insert(id, value);
    }
}

fn full_view(table: &TableInfo) -> SystemTableView {
    let names: Vec<String> = table
        .cols()
        .iter_deref()
        .map(|column| column.read().name.lowercase().to_owned())
        .collect();
    let borrowed: Vec<&str> = names.iter().map(String::as_str).collect();
    SystemTableView::project(
        &format!("{SYSTEM_DB}.{}", table.name.original()),
        table,
        &borrowed,
    )
}

/// The table's stored rows for one analyzed table, each with the record key
/// it lives under.
///
/// `table_id` narrows the scan by the clustered prefix when the table has
/// one, and filters afterwards when it does not -- which is exactly the
/// situation `mysql.stats_top_n` is always in and the older statistics
/// schemas are in too.
fn read_rows_with_keys<S: MetaSnapshot>(
    snapshot: &mut S,
    table: &TableInfo,
    table_id: Option<i64>,
) -> Result<Vec<(Vec<u8>, RowValues)>, StatsWriteError> {
    let view = full_view(table);
    let pairs = match table_id {
        Some(id) => scan_system_table_prefixed(snapshot, &view, &[Datum::Int(id)])?,
        None => scan_system_table(snapshot, &view)?,
    };
    decode_rows_with_keys(table, table_id, &view, pairs)
}

/// Reads all rows in one named secondary-index order. Go's historical GC
/// forces `idx_create_time`; using that same ordered access path is what makes
/// each bounded delete remove the oldest rows first without a Rust-only sort.
fn read_rows_with_keys_from_index<S: MetaSnapshot>(
    snapshot: &mut S,
    table: &TableInfo,
    index_name: &str,
) -> Result<Vec<(Vec<u8>, RowValues)>, StatsWriteError> {
    let view = full_view(table);
    let pairs = scan_system_table_index_prefixed(snapshot, &view, index_name, &[])?;
    decode_rows_with_keys(table, None, &view, pairs)
}

fn decode_rows_with_keys(
    table: &TableInfo,
    table_id: Option<i64>,
    view: &SystemTableView,
    pairs: crate::cluster_catalog::MetaPairs,
) -> Result<Vec<(Vec<u8>, RowValues)>, StatsWriteError> {
    let mut rows = Vec::new();
    for (key, value) in pairs {
        let timezone = tidb_datatype::SessionTimeZone::utc();
        let parsed = SystemRow::parse_in_timezone(view, &key, &value, Some(&timezone))?;
        let mut values = RowValues::new();
        for column in table.cols().iter_deref() {
            let (id, name) = {
                let column = column.read();
                (column.id, column.name.lowercase().to_owned())
            };
            let stored = parsed.datum(&name)?.cloned().unwrap_or(Datum::Null);
            values.insert(id, stored);
        }
        if let Some(id) = table_id {
            if values.get(&column_id(table, "table_id")?) != Some(&Datum::Int(id)) {
                continue;
            }
        }
        rows.push((key, values));
    }
    Ok(rows)
}

/// The next free `_tidb_rowid` for `mysql.stats_top_n`, as
/// [`crate::cluster_account_write`] computes it, and for the same reason:
/// reserving from the value this snapshot read is what makes a competing
/// allocation a write conflict rather than a duplicate handle.
///
/// Every handle currently stored is counted, including the ones this plan is
/// about to retract: a key deleted and reinserted in one transaction is two
/// mutations on one key, which the mutation set rejects.
fn first_free_row_id<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    table: &TableInfo,
) -> Result<i64, StatsWriteError> {
    let stored = snapshot
        .get(&key::auto_table_id_kv_key(system_db_id(catalog)?, table.id))
        .map_err(|error| StatsWriteError::Read(error.into()))?;
    let current = match stored {
        Some(bytes) => value::parse_int_value(&bytes).map_err(|error| {
            StatsWriteError::Encode(RowEncodeError(format!(
                "{}'s row-ID allocator: {error}",
                table.name.original()
            )))
        })?,
        None => 0,
    };
    let highest = read_rows_with_keys(snapshot, table, None)?
        .iter()
        .map(|(key, _)| row_id_of(key))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .max()
        .unwrap_or(0);
    Ok(current.max(highest) + 1)
}

/// The buckets a stored histogram would read back as, for a caller that wants
/// to check a round trip without a cluster.
#[must_use]
pub fn stored_bucket_counts(buckets: &[Bucket]) -> Vec<i64> {
    let mut previous = 0;
    buckets
        .iter()
        .map(|bucket| {
            let delta = bucket.count - previous;
            previous = bucket.count;
            delta
        })
        .collect()
}

/// Whether one loaded item and one built item describe the same histogram.
///
/// Used by the round-trip tests: what was planned must be what the loader
/// reads back.
#[must_use]
pub fn same_histogram(left: &ClusterStatsItem, right: &ClusterStatsItem) -> bool {
    left.id == right.id
        && left.is_index == right.is_index
        && left.stats_ver == right.stats_ver
        && left.histogram.ndv == right.histogram.ndv
        && left.histogram.null_count == right.histogram.null_count
        && left.histogram.buckets.len() == right.histogram.buckets.len()
}
