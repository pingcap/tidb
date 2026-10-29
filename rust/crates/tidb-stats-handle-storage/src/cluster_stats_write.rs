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
//! `mysql.*` write on this node does (see [`tidb_syssession::system_row_write`]).
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
use std::ops::Range;

use tidb_datatype::{
    parse_time, ConversionFlags, Datum, FieldType, FieldTypeCode, Time, TimeType, MAX_FSP,
    UNSPECIFIED_LENGTH,
};
use tidb_model::table_info::TableInfo;
use tidb_model::TableItemID;
use tidb_stats::{encode_cmsketch_without_topn, encode_fm_sketch};
use tidb_stats::JsonPredicateColumn;
use tidb_sqlescape::{must_escape_sql, must_escape_sql_text, SqlArg};

use tidb_infoschema::cluster_catalog::{ClusterCatalog, MetaSnapshot};
use tidb_stats_handle_usage_predicatecolumn::cluster_predicate_column::ColumnStatsTimeInfo;
use crate::cluster_stats_load::{ClusterStatsItem, ClusterTableStats};
use tidb_syssession::mysql_system_tables::{
    scan_system_table, scan_system_table_prefixed, HandleLayout,
    SystemRow, SystemTableView,
};
use tidb_syssession::system_row_write::{
    defaults_row, delete_clustered_row, delete_row, rewrite_rowid_row,
    store_clustered_row,
};
use tidb_syssession::stats_table_rows::*;
use tidb_hack::GoToLower;

/// Go's `mysql` schema name.
const SYSTEM_DB: &str = "mysql";

/// Go `batchInsertSize` in `pkg/statistics/handle/storage/save.go`.
const LOADED_STATS_BATCH_INSERT_SIZE: usize = 10;

/// Go `maxInsertLength` in `pkg/statistics/handle/storage/save.go`.
const LOADED_STATS_MAX_INSERT_LENGTH: usize = 1024 * 1024;

/// The `AnalyzeResults` metadata Go carries from sampling into its later
/// statistics-save transaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AnalyzeStatsMeta {
    /// `AnalyzeResults.BaseCount`.
    pub base_count: i64,
    /// `AnalyzeResults.BaseModifyCnt`.
    pub base_modify_count: i64,
    /// Session `tidb_enable_analyze_snapshot` at execution time.
    pub analyze_snapshot: bool,
}

/// Which ordinary Analyze v2 payload replacement Go is saving.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AnalyzeStatsWriteScope {
    /// A complete non-partitioned table result.
    FullTable,
    /// A complete physical-partition result, including FM sketches.
    FullPartition,
    /// Selected histogram items for a non-partitioned table.
    PartialTable,
    /// Selected histogram items for a physical partition, including FM sketches.
    PartialPartition,
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
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
) -> Result<StatsWritePlan, StatsWriteError> {
    plan_stats_write_impl(snapshot, catalog, stats, now, true, false, None, row_ids)
}

/// Plans a full physical-partition ANALYZE write, including FM sketches.
pub fn plan_partition_stats_write<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    stats: &ClusterTableStats,
    now: Time,
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
) -> Result<StatsWritePlan, StatsWriteError> {
    plan_stats_write_impl(snapshot, catalog, stats, now, true, true, None, row_ids)
}

/// Plans an ANALYZE write that replaces only the histogram items present in
/// `stats`, matching Go's partial-column analyze storage behavior.
pub fn plan_partial_stats_write<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    stats: &ClusterTableStats,
    now: Time,
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
) -> Result<StatsWritePlan, StatsWriteError> {
    plan_stats_write_impl(snapshot, catalog, stats, now, false, false, None, row_ids)
}

/// Plans a partial physical-partition ANALYZE write, including FM sketches.
pub fn plan_partial_partition_stats_write<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    stats: &ClusterTableStats,
    now: Time,
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
) -> Result<StatsWritePlan, StatsWriteError> {
    plan_stats_write_impl(snapshot, catalog, stats, now, false, true, None, row_ids)
}

/// Plans pinned Go `SaveAnalyzeResultToStorage` after sampling has completed.
///
/// Unlike the raw storage helpers above, this reads the `stats_meta` row as it
/// exists in the later save transaction and reconciles modifications that
/// committed after the sampling snapshot.
pub fn plan_analyze_stats_write<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    stats: &ClusterTableStats,
    meta: AnalyzeStatsMeta,
    scope: AnalyzeStatsWriteScope,
    now: Time,
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
) -> Result<StatsWritePlan, StatsWriteError> {
    let (replace_all, dump_fm) = match scope {
        AnalyzeStatsWriteScope::FullTable => (true, false),
        AnalyzeStatsWriteScope::FullPartition => (true, true),
        AnalyzeStatsWriteScope::PartialTable => (false, false),
        AnalyzeStatsWriteScope::PartialPartition => (false, true),
    };
    plan_stats_write_impl(
        snapshot,
        catalog,
        stats,
        now,
        replace_all,
        dump_fm,
        Some(meta),
        row_ids,
    )
}

/// Plans pinned Go's `ForMVIndexOrGlobalIndex` ANALYZE write.
///
/// A special global-index task replaces only the index item it produced. If
/// `stats_meta` already exists, Go advances only its version markers; if it
/// does not, Go creates it with count and snapshot zero. In both cases the
/// independently scanned index must not overwrite table row-count metadata.
///
/// `dump_fm` is Go's `needDumpFMS`: a partition's result keeps its FM
/// sketches for the global-stats merge; a table-level one does not.
pub fn plan_independent_index_stats_write<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    stats: &ClusterTableStats,
    now: Time,
    dump_fm: bool,
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
) -> Result<(StatsWritePlan, bool), StatsWriteError> {
    let mut plan = StatsWritePlan::default();
    let inserted_meta =
        plan_independent_index_meta(snapshot, catalog, stats, now, &mut plan, row_ids)?;
    plan_histograms(snapshot, catalog, stats, now, false, &mut plan, row_ids)?;
    plan_buckets(snapshot, catalog, stats, now, false, &mut plan, row_ids)?;
    plan_topn(snapshot, catalog, stats, now, false, &mut plan, row_ids)?;
    plan_fm_sketches(
        snapshot, catalog, stats, now, false, dump_fm, &mut plan, row_ids,
    )?;
    Ok((plan, inserted_meta))
}

fn plan_stats_write_impl<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    stats: &ClusterTableStats,
    now: Time,
    replace_all: bool,
    dump_fm: bool,
    analyze_meta: Option<AnalyzeStatsMeta>,
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
) -> Result<StatsWritePlan, StatsWriteError> {
    let mut plan = StatsWritePlan::default();
    if !plan_meta(
        snapshot,
        catalog,
        stats,
        analyze_meta,
        now,
        &mut plan,
        row_ids,
    )? {
        plan.skipped_newer_snapshot = true;
        return Ok(plan);
    }
    plan_histograms(
        snapshot,
        catalog,
        stats,
        now,
        replace_all,
        &mut plan,
        row_ids,
    )?;
    plan_buckets(
        snapshot,
        catalog,
        stats,
        now,
        replace_all,
        &mut plan,
        row_ids,
    )?;
    plan_topn(
        snapshot,
        catalog,
        stats,
        now,
        replace_all,
        &mut plan,
        row_ids,
    )?;
    plan_fm_sketches(
        snapshot,
        catalog,
        stats,
        now,
        replace_all,
        dump_fm,
        &mut plan,
        row_ids,
    )?;
    Ok(plan)
}

/// One SQL statement in pinned Go `SaveColOrIdxStatsToStorage`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LoadedStatsItemStatement {
    /// `REPLACE` or `UPDATE mysql.stats_meta`.
    Meta,
    /// Delete the named histogram's old TopN rows.
    TopNDelete,
    /// One `INSERT mysql.stats_top_n` batch.
    TopNInsert(Range<usize>),
    /// Delete the named histogram's FM sketch.
    FmDelete,
    /// Insert the supplied FM sketch after deleting the previous row.
    FmInsert,
    /// Replace the named histogram row.
    HistogramReplace,
    /// Delete the named histogram's old bucket rows.
    BucketsDelete,
    /// One `INSERT mysql.stats_buckets` batch.
    BucketsInsert(Range<usize>),
}

/// Go `saveTopNToStorage`'s `INSERT mysql.stats_top_n` statements for one
/// histogram, in its batches (ten rows, the escaped 1 MiB boundary), as
/// text for this tier's parser (`must_escape_sql_text`).
pub fn save_topn_sql(table_id: i64, item: &ClusterStatsItem) -> Vec<String> {
    let Some(topn) = &item.topn else {
        return Vec::new();
    };
    let entries = topn.entries();
    loaded_topn_batches(table_id, item)
        .into_iter()
        .map(|range| {
            let mut sql = String::from(
                "insert into mysql.stats_top_n (table_id, is_index, hist_id, value, count) values ",
            );
            for (offset, entry) in entries[range].iter().enumerate() {
                if offset > 0 {
                    sql.push(',');
                }
                sql.push_str(&must_escape_sql_text(
                    "(%?, %?, %?, %?, %?)",
                    &[
                        SqlArg::Signed(table_id),
                        SqlArg::Signed(i64::from(item.is_index)),
                        SqlArg::Signed(item.id),
                        SqlArg::Bytes(Some(&entry.encoded)),
                        SqlArg::Unsigned(entry.count),
                    ],
                ));
            }
            sql
        })
        .collect()
}

/// Go `saveBucketsToStorage`'s `INSERT mysql.stats_buckets` statements for
/// one histogram: each bucket's own count (the stored counts are
/// cumulative) and its bounds as blobs (`convertBoundToBlob`).
pub fn save_buckets_sql(
    table_id: i64,
    item: &ClusterStatsItem,
) -> Result<Vec<String>, StatsWriteError> {
    let buckets = &item.histogram.buckets;
    let mut statements = Vec::new();
    for range in loaded_bucket_batches(table_id, item)? {
        let mut sql = String::from("insert into mysql.stats_buckets (table_id, is_index, hist_id, bucket_id, count, repeats, lower_bound, upper_bound, ndv) values ");
        for index in range.clone() {
            let bucket = &buckets[index];
            let lower = bound_blob(&bucket.lower_bound)?;
            let upper = bound_blob(&bucket.upper_bound)?;
            let blob = |value: &Datum| match value {
                Datum::Bytes(bytes) => Some(bytes.clone()),
                _ => None,
            };
            let (lower, upper) = (blob(&lower), blob(&upper));
            let previous_count = index
                .checked_sub(1)
                .map_or(0, |previous| buckets[previous].count);
            if index > range.start {
                sql.push(',');
            }
            sql.push_str(&must_escape_sql_text(
                "(%?, %?, %?, %?, %?, %?, %?, %?, %?)",
                &[
                    SqlArg::Signed(table_id),
                    SqlArg::Signed(i64::from(item.is_index)),
                    SqlArg::Signed(item.id),
                    SqlArg::Signed(index as i64),
                    SqlArg::Signed(bucket.count - previous_count),
                    SqlArg::Signed(bucket.repeat),
                    SqlArg::Bytes(lower.as_deref()),
                    SqlArg::Bytes(upper.as_deref()),
                    SqlArg::Signed(bucket.ndv),
                ],
            ));
        }
        statements.push(sql);
    }
    Ok(statements)
}

/// The statements Go's `SaveAnalyzeResultToStorage` (`analyze = true`) or
/// `SaveColOrIdxStatsToStorage` runs for one histogram after the
/// `stats_meta` write, in their order: TopN replaced, the FM sketch
/// deleted (and, for an analyze result whose table is a partition, stored),
/// the histogram row replaced, buckets replaced, and a column's
/// `column_stats_usage.last_analyzed_at` stamped.
pub fn save_stats_item_sql(
    table_id: i64,
    item: &ClusterStatsItem,
    version: u64,
    stats_ver: i64,
    dump_fm_sketch: bool,
    analyze: bool,
) -> Result<Vec<String>, StatsWriteError> {
    let key = [
        SqlArg::Signed(table_id),
        SqlArg::Signed(i64::from(item.is_index)),
        SqlArg::Signed(item.id),
    ];
    let mut statements = vec![must_escape_sql_text(
        "delete from mysql.stats_top_n where table_id = %? and is_index = %? and hist_id = %?",
        &key,
    )];
    statements.extend(save_topn_sql(table_id, item));
    statements.push(must_escape_sql_text(
        "delete from mysql.stats_fm_sketch where table_id = %? and is_index = %? and hist_id = %?",
        &key,
    ));
    if dump_fm_sketch {
        if let Some(fm_sketch) =
            tidb_stats::fmsketch_codec::encode_fm_sketch(item.fm_sketch.as_ref())
        {
            statements.push(must_escape_sql_text(
                "insert into mysql.stats_fm_sketch (table_id, is_index, hist_id, value) values (%?, %?, %?, %?)",
                &[
                    SqlArg::Signed(table_id),
                    SqlArg::Signed(i64::from(item.is_index)),
                    SqlArg::Signed(item.id),
                    SqlArg::Bytes(Some(&fm_sketch)),
                ],
            ));
        }
    }
    // Analyze v2 keeps no CMSketch; Go's encoder turns the nil one into NULL.
    let cm_sketch = if stats_ver == 2 {
        None
    } else {
        tidb_stats::cmsketch::encode_cmsketch_without_topn(item.cms.as_ref())
            .map_err(|error| StatsWriteError::Cms(error.to_string()))?
    };
    statements.push(must_escape_sql_text(
        "replace into mysql.stats_histograms (table_id, is_index, hist_id, distinct_count, version, null_count, cm_sketch, tot_col_size, stats_ver, correlation) values (%?, %?, %?, %?, %?, %?, %?, GREATEST(%?, 0), %?, %?)",
        &[
            SqlArg::Signed(table_id),
            SqlArg::Signed(i64::from(item.is_index)),
            SqlArg::Signed(item.id),
            SqlArg::Signed(item.histogram.ndv),
            SqlArg::Unsigned(version),
            SqlArg::Signed(item.histogram.null_count),
            SqlArg::Bytes(cm_sketch.as_deref()),
            SqlArg::Signed(item.histogram.tot_col_size),
            SqlArg::Signed(stats_ver),
            SqlArg::Float64(item.histogram.correlation),
        ],
    ));
    statements.push(must_escape_sql_text(
        "delete from mysql.stats_buckets where table_id = %? and is_index = %? and hist_id = %?",
        &key,
    ));
    statements.extend(save_buckets_sql(table_id, item)?);
    if !item.is_index {
        statements.push(must_escape_sql_text(
            if analyze {
                "insert into mysql.column_stats_usage (table_id, column_id, last_analyzed_at) values(%?, %?, current_timestamp()) on duplicate key update last_analyzed_at = values(last_analyzed_at)"
            } else {
                "insert into mysql.column_stats_usage (table_id, column_id, last_analyzed_at) values(%?, %?, current_timestamp()) on duplicate key update last_analyzed_at = current_timestamp()"
            },
            &[SqlArg::Signed(table_id), SqlArg::Signed(item.id)],
        ));
    }
    Ok(statements)
}

/// Returns pinned Go's exact SQL statement sequence for one loaded item.
///
/// The two insert loops use Go's ten-row cap and escaped-SQL 1 MiB boundary.
/// A first row is always accepted even when it alone exceeds that boundary.
pub fn loaded_stats_item_statements(
    table_id: i64,
    item: &ClusterStatsItem,
) -> Result<Vec<LoadedStatsItemStatement>, StatsWriteError> {
    let mut statements = vec![
        LoadedStatsItemStatement::Meta,
        LoadedStatsItemStatement::TopNDelete,
    ];
    statements.extend(
        loaded_topn_batches(table_id, item)
            .into_iter()
            .map(LoadedStatsItemStatement::TopNInsert),
    );
    statements.push(LoadedStatsItemStatement::FmDelete);
    if item.fm_sketch.is_some() {
        statements.push(LoadedStatsItemStatement::FmInsert);
    }
    statements.extend([
        LoadedStatsItemStatement::HistogramReplace,
        LoadedStatsItemStatement::BucketsDelete,
    ]);
    statements.extend(
        loaded_bucket_batches(table_id, item)?
            .into_iter()
            .map(LoadedStatsItemStatement::BucketsInsert),
    );
    Ok(statements)
}

/// Plans exactly one pinned Go SQL statement for a loaded statistics item.
pub fn plan_loaded_stats_item_statement<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    table_id: i64,
    count: i64,
    item: &ClusterStatsItem,
    version: u64,
    now: Time,
    statement: &LoadedStatsItemStatement,
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
) -> Result<StatsWritePlan, StatsWriteError> {
    let mut plan = StatsWritePlan::default();
    match statement {
        LoadedStatsItemStatement::Meta => {
            plan_loaded_meta(
                snapshot, catalog, table_id, count, version, now, &mut plan, row_ids,
            )?;
        }
        LoadedStatsItemStatement::TopNDelete => {
            plan_loaded_topn_delete(snapshot, catalog, table_id, item, &mut plan)?;
        }
        LoadedStatsItemStatement::TopNInsert(range) => {
            plan_loaded_topn_insert(
                snapshot, catalog, table_id, item, range, now, &mut plan, row_ids,
            )?;
        }
        LoadedStatsItemStatement::FmDelete => {
            plan_loaded_fm_delete(snapshot, catalog, table_id, item, &mut plan)?;
        }
        LoadedStatsItemStatement::FmInsert => {
            plan_loaded_fm_insert(snapshot, catalog, table_id, item, now, &mut plan, row_ids)?;
        }
        LoadedStatsItemStatement::HistogramReplace => {
            plan_loaded_histogram(
                snapshot, catalog, table_id, item, version, now, &mut plan, row_ids,
            )?;
        }
        LoadedStatsItemStatement::BucketsDelete => {
            plan_loaded_buckets_delete(snapshot, catalog, table_id, item, &mut plan)?;
        }
        LoadedStatsItemStatement::BucketsInsert(range) => {
            plan_loaded_buckets_insert(
                snapshot, catalog, table_id, item, range, now, &mut plan, row_ids,
            )?;
        }
    }
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
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
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
    rows.store_row(catalog, &values, &mut plan, row_ids)?;
    Ok(plan)
}

/// One SQL statement from pinned Go `InsertTableStats2KV`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InsertTableStatsStatement {
    /// `INSERT IGNORE` of the physical table's metadata row.
    Meta {
        /// New table or partition physical ID.
        physical_id: i64,
    },
    /// `INSERT IGNORE` of one column or index histogram placeholder.
    Histogram {
        /// New table or partition physical ID.
        physical_id: i64,
        /// Whether the placeholder belongs to an index rather than a column.
        is_index: bool,
        /// Column or index ID.
        hist_id: i64,
    },
}

/// Builds pinned Go `InsertTableStats2KV`'s statement sequence.
#[must_use]
pub fn insert_table_stats_statements(
    table: &TableInfo,
    physical_id: i64,
) -> Vec<InsertTableStatsStatement> {
    std::iter::once(InsertTableStatsStatement::Meta { physical_id })
        .chain(
            table
                .columns
                .iter_deref()
                .map(|column| InsertTableStatsStatement::Histogram {
                    physical_id,
                    is_index: false,
                    hist_id: column.read().id,
                }),
        )
        .chain(
            table
                .indices
                .iter_deref()
                .map(|index| InsertTableStatsStatement::Histogram {
                    physical_id,
                    is_index: true,
                    hist_id: index.read().id,
                }),
        )
        .collect()
}

/// Plans one pinned Go `InsertTableStats2KV` statement.
pub fn plan_insert_table_stats_statement<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    statement: &InsertTableStatsStatement,
    version: u64,
    now: Time,
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
) -> Result<StatsWritePlan, StatsWriteError> {
    let mut plan = StatsWritePlan::default();
    let (table_name, identity, physical_id, histogram) = match statement {
        InsertTableStatsStatement::Meta { physical_id } => {
            ("stats_meta", &["table_id"][..], *physical_id, None)
        }
        InsertTableStatsStatement::Histogram {
            physical_id,
            is_index,
            hist_id,
        } => (
            "stats_histograms",
            &["table_id", "is_index", "hist_id"][..],
            *physical_id,
            Some((*is_index, *hist_id)),
        ),
    };
    let table = locate(catalog, table_name)?;
    let mut rows = StatsRows::open(snapshot, table, identity, physical_id)?;
    let mut values = defaults_row(table, now)?;
    set(table, &mut values, "table_id", Datum::Int(physical_id));
    if let Some((is_index, hist_id)) = histogram {
        set(
            table,
            &mut values,
            "is_index",
            Datum::Int(i64::from(is_index)),
        );
        set(table, &mut values, "hist_id", Datum::Int(hist_id));
        set(table, &mut values, "distinct_count", Datum::Int(0));
        set(table, &mut values, "version", Datum::UInt(version));
        set(table, &mut values, "null_count", Datum::Int(0));
        set(table, &mut values, "tot_col_size", Datum::Int(0));
        set(table, &mut values, "modify_count", Datum::Int(0));
        set(table, &mut values, "cm_sketch", Datum::Null);
        set(table, &mut values, "stats_ver", Datum::Int(0));
        set(table, &mut values, "flag", Datum::Int(0));
        set(table, &mut values, "correlation", Datum::Real(0.0));
    } else {
        set(table, &mut values, "version", Datum::UInt(version));
        set(
            table,
            &mut values,
            "last_stats_histograms_version",
            Datum::UInt(version),
        );
    }
    let row_identity = identity_of(table, identity, &values)?;
    if rows.existing_values(&row_identity).is_none() {
        rows.store_row(catalog, &values, &mut plan, row_ids)?;
    }
    Ok(plan)
}

/// Plans one histogram `INSERT IGNORE` from pinned Go `InsertColStats2KV`.
///
/// The caller has already run Go's preceding `SELECT count` statement. The
/// returned boolean is that SQL statement's affected-row verdict, which
/// decides whether `stats_meta` is version-refreshed afterwards.
/// `None` represents a virtual generated column; `Some(NULL)` and a non-NULL
/// datum select Go's two origin-default branches.
pub fn plan_insert_column_stats<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    physical_id: i64,
    column_id: i64,
    count: i64,
    origin_default: Option<&Datum>,
    version: u64,
    now: Time,
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
) -> Result<(bool, StatsWritePlan), StatsWriteError> {
    let table = locate(catalog, "stats_histograms")?;
    let identity_columns = &["table_id", "is_index", "hist_id"];
    let mut rows = StatsRows::open(snapshot, table, identity_columns, physical_id)?;
    let mut values = defaults_row(table, now)?;
    set(table, &mut values, "table_id", Datum::Int(physical_id));
    set(table, &mut values, "is_index", Datum::Int(0));
    set(table, &mut values, "hist_id", Datum::Int(column_id));
    let is_non_null = origin_default.is_some_and(|value| !value.is_null());
    set(
        table,
        &mut values,
        "distinct_count",
        Datum::Int(i64::from(is_non_null)),
    );
    set(
        table,
        &mut values,
        "null_count",
        Datum::Int(if origin_default.is_some_and(Datum::is_null) {
            count
        } else {
            0
        }),
    );
    set(table, &mut values, "version", Datum::UInt(version));
    let total_size = origin_default
        .filter(|value| !value.is_null())
        .map_or(0, |value| {
            i64::try_from(value.go_bytes().len())
                .unwrap_or(i64::MAX)
                .wrapping_mul(count)
                .max(0)
        });
    set(table, &mut values, "tot_col_size", Datum::Int(total_size));
    set(table, &mut values, "modify_count", Datum::Int(0));
    set(table, &mut values, "cm_sketch", Datum::Null);
    set(table, &mut values, "stats_ver", Datum::Int(0));
    set(table, &mut values, "flag", Datum::Int(0));
    set(table, &mut values, "correlation", Datum::Real(0.0));
    let identity = identity_of(table, identity_columns, &values)?;
    if rows.existing_values(&identity).is_some() {
        return Ok((false, StatsWritePlan::default()));
    }
    let mut plan = StatsWritePlan::default();
    rows.store_row(catalog, &values, &mut plan, row_ids)?;
    Ok((true, plan))
}

/// Plans the default-value bucket inserted after a successful non-NULL
/// histogram insert in pinned Go `InsertColStats2KV`.
pub fn plan_insert_column_default_bucket<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    physical_id: i64,
    column_id: i64,
    count: i64,
    origin_default: &Datum,
    now: Time,
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
) -> Result<StatsWritePlan, StatsWriteError> {
    let table = locate(catalog, "stats_buckets")?;
    let identity_columns = &["table_id", "is_index", "hist_id", "bucket_id"];
    let mut rows = StatsRows::open(snapshot, table, identity_columns, physical_id)?;
    let mut values = defaults_row(table, now)?;
    set(table, &mut values, "table_id", Datum::Int(physical_id));
    set(table, &mut values, "is_index", Datum::Int(0));
    set(table, &mut values, "hist_id", Datum::Int(column_id));
    set(table, &mut values, "bucket_id", Datum::Int(0));
    set(table, &mut values, "repeats", Datum::Int(count));
    set(table, &mut values, "count", Datum::Int(count));
    let bound = bound_blob(origin_default)?;
    set(table, &mut values, "lower_bound", bound.clone());
    set(table, &mut values, "upper_bound", bound);
    let identity = identity_of(table, identity_columns, &values)?;
    let mut plan = StatsWritePlan::default();
    if rows.existing_values(&identity).is_none() {
        rows.store_row(catalog, &values, &mut plan, row_ids)?;
    }
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
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
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
    rows.store_row(catalog, &values, &mut plan, row_ids)?;
    Ok(plan)
}

/// One ordered statement in pinned Go `UpdateStatsVersion`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StatsVersionTable {
    /// `UPDATE mysql.stats_meta SET version = ?`.
    Meta,
    /// `UPDATE mysql.stats_histograms SET version = ?`.
    Histograms,
}

/// Pinned Go `UpdateStatsVersion` statement order.
pub const STATS_VERSION_TABLES: [StatsVersionTable; 2] =
    [StatsVersionTable::Meta, StatsVersionTable::Histograms];

/// Plans one pinned Go `UpdateStatsVersion` table-wide UPDATE.
///
/// Only the `version` column changes. In particular, the `stats_meta`
/// statement does not refresh `last_stats_histograms_version`.
pub fn plan_update_stats_table_version<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    target: StatsVersionTable,
    version: u64,
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
) -> Result<StatsWritePlan, StatsWriteError> {
    let mut plan = StatsWritePlan::default();
    let (table_name, identity) = match target {
        StatsVersionTable::Meta => ("stats_meta", &["table_id"][..]),
        StatsVersionTable::Histograms => {
            ("stats_histograms", &["table_id", "is_index", "hist_id"][..])
        }
    };
    let table = locate(catalog, table_name)?;
    let mut rows = StatsRows::open_all(snapshot, table, identity)?;
    let stored = rows
        .existing
        .values()
        .map(|(_, values)| values.clone())
        .collect::<Vec<_>>();
    for mut values in stored {
        set(table, &mut values, "version", Datum::UInt(version));
        rows.store_row(catalog, &values, &mut plan, row_ids)?;
    }
    Ok(plan)
}

/// Plans pinned Go `UpdateStatsVersion` as its two ordered table-wide UPDATEs.
pub fn plan_update_stats_version<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    version: u64,
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
) -> Result<StatsWritePlan, StatsWriteError> {
    let mut plan = StatsWritePlan::default();
    for target in STATS_VERSION_TABLES {
        let statement =
            plan_update_stats_table_version(snapshot, catalog, target, version, row_ids)?;
        plan.mutations.extend(statement.mutations);
    }
    Ok(plan)
}

/// One ordered statement in pinned Go `ChangeGlobalStatsID`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GlobalStatsIdTable {
    /// `mysql.stats_meta`.
    Meta,
    /// `mysql.stats_top_n`.
    TopN,
    /// `mysql.stats_fm_sketch`.
    FmSketch,
    /// `mysql.stats_buckets`.
    Buckets,
    /// `mysql.stats_histograms`.
    Histograms,
    /// `mysql.column_stats_usage`.
    ColumnUsage,
}

/// Pinned Go `changeGlobalStatsTables` order.
pub const GLOBAL_STATS_ID_TABLES: [GlobalStatsIdTable; 6] = [
    GlobalStatsIdTable::Meta,
    GlobalStatsIdTable::TopN,
    GlobalStatsIdTable::FmSketch,
    GlobalStatsIdTable::Buckets,
    GlobalStatsIdTable::Histograms,
    GlobalStatsIdTable::ColumnUsage,
];

/// Plans one pinned Go `ChangeGlobalStatsID` table UPDATE.
pub fn plan_change_global_stats_table_id<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    target: GlobalStatsIdTable,
    from: i64,
    to: i64,
) -> Result<StatsWritePlan, StatsWriteError> {
    let (table_name, identity) = match target {
        GlobalStatsIdTable::Meta => ("stats_meta", &["table_id"][..]),
        GlobalStatsIdTable::TopN => (
            "stats_top_n",
            &["table_id", "is_index", "hist_id", "value"][..],
        ),
        GlobalStatsIdTable::FmSketch => {
            ("stats_fm_sketch", &["table_id", "is_index", "hist_id"][..])
        }
        GlobalStatsIdTable::Buckets => (
            "stats_buckets",
            &["table_id", "is_index", "hist_id", "bucket_id"][..],
        ),
        GlobalStatsIdTable::Histograms => {
            ("stats_histograms", &["table_id", "is_index", "hist_id"][..])
        }
        GlobalStatsIdTable::ColumnUsage => ("column_stats_usage", &["table_id", "column_id"][..]),
    };
    let mut plan = StatsWritePlan::default();
    change_stats_table_id(snapshot, catalog, table_name, identity, from, to, &mut plan)?;
    Ok(plan)
}

/// Plans pinned Go `ChangeGlobalStatsID` as six ordered table updates in one
/// transaction.
pub fn plan_change_global_stats_id<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    from: i64,
    to: i64,
) -> Result<StatsWritePlan, StatsWriteError> {
    let mut plan = StatsWritePlan::default();
    for target in GLOBAL_STATS_ID_TABLES {
        let statement = plan_change_global_stats_table_id(snapshot, catalog, target, from, to)?;
        plan.mutations.extend(statement.mutations);
    }
    Ok(plan)
}

fn change_stats_table_id<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    table_name: &str,
    identity: &'static [&'static str],
    from: i64,
    to: i64,
    plan: &mut StatsWritePlan,
) -> Result<(), StatsWriteError> {
    if from == to {
        return Ok(());
    }
    let table = locate(catalog, table_name)?;
    let mut rows = StatsRows::open_all(snapshot, table, identity)?;
    let table_id = column_id(table, "table_id")?;
    let source_identities = rows
        .existing
        .iter()
        .filter_map(|(identity, (_, values))| {
            (values.get(&table_id) == Some(&Datum::Int(from))).then(|| identity.clone())
        })
        .collect::<Vec<_>>();
    for source_identity in source_identities {
        let (key, stored) = rows
            .existing
            .remove(&source_identity)
            .expect("source identity came from the existing map");
        let mut moved = stored.clone();
        set(table, &mut moved, "table_id", Datum::Int(to));
        let target_identity = identity_of(table, identity, &moved)?;
        if rows.existing.contains_key(&target_identity) {
            return Err(StatsWriteError::DuplicateIdentity(format!(
                "updating mysql.{table_name} table_id from {from} to {to} conflicts with an existing row"
            )));
        }
        if rows.clustered {
            plan.mutations.extend(delete_clustered_row(table, &stored)?);
            plan.mutations
                .extend(store_clustered_row(table, None, &moved)?);
        } else {
            plan.mutations
                .extend(rewrite_rowid_row(table, &key, &stored, &moved)?);
        }
    }
    Ok(())
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
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
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
    plan_column_stats_usage_write(snapshot, catalog, &usage, now, row_ids)
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
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
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
        rows.store_row(catalog, &values, &mut plan, row_ids)?;
    }
    Ok(plan)
}

/// Plans the count and modify-count adjustment used by pinned Go's exchange
/// partition DDL subscriber.
pub fn plan_exchange_partition_stats_update<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    table_id: i64,
    count_delta: i64,
    modify_count_delta: i64,
    locked: bool,
    version: u64,
    now: Time,
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
) -> Result<StatsWritePlan, StatsWriteError> {
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
    if let Some(stored) = rows.existing_values(&identity) {
        values = stored.clone();
    }
    let count = signed_value(values.get(&column_id(table, "count")?))
        .unwrap_or_default()
        .wrapping_add(count_delta);
    let modify_count = signed_value(values.get(&column_id(table, "modify_count")?))
        .unwrap_or_default()
        .wrapping_add(modify_count_delta);
    set(table, &mut values, "version", Datum::UInt(version));
    set(
        table,
        &mut values,
        "count",
        Datum::Int(if locked { count } else { count.max(0) }),
    );
    set(
        table,
        &mut values,
        "modify_count",
        Datum::Int(if locked {
            modify_count
        } else {
            modify_count.max(0)
        }),
    );
    let mut plan = StatsWritePlan::default();
    rows.store_row(catalog, &values, &mut plan, row_ids)?;
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
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
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
    rows.store_row(catalog, &values, &mut plan, row_ids)?;
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
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
) -> Result<StatsWritePlan, StatsWriteError> {
    let mut plan =
        plan_stats_meta_version_refresh(snapshot, catalog, physical_id, version, row_ids)?;
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
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
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
        rows.store_row(catalog, &values, plan, row_ids)?;
    }
    Ok(())
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
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
) -> Result<StatsWritePlan, StatsWriteError> {
    let mut plan = StatsWritePlan::default();
    for &physical_id in physical_ids {
        plan.mutations.extend(
            plan_stats_meta_version_refresh(snapshot, catalog, physical_id, version, row_ids)?
                .mutations,
        );
        if soft {
            reset_table_histograms(snapshot, catalog, physical_id, version, &mut plan, row_ids)?;
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

pub fn signed_value(value: Option<&Datum>) -> Option<i64> {
    match value {
        Some(Datum::Int(value)) => Some(*value),
        Some(Datum::UInt(value)) => i64::try_from(*value).ok(),
        _ => None,
    }
}

pub fn unsigned_value(value: Option<&Datum>) -> Option<u64> {
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

pub fn predicate_time_value(
    table: &TableInfo,
    column: &str,
    value: Option<Time>,
) -> Result<Datum, StatsWriteError> {
    let Some(value) = value else {
        return Ok(Datum::Null);
    };
    // Pinned SaveColumnStatsUsageForTable binds the Time string through
    // CONVERT_TZ. That builtin returns SQL NULL for an incomplete date.
    if value.invalid_zero() {
        return Ok(Datum::Null);
    }
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
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
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
        rows.store_row(catalog, &values, plan, row_ids)?;
        return Ok(());
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
    rows.store_row(catalog, &values, plan, row_ids)?;
    Ok(())
}

fn plan_loaded_histogram<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    table_id: i64,
    item: &ClusterStatsItem,
    version: u64,
    now: Time,
    plan: &mut StatsWritePlan,
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
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
    rows.store_row(catalog, &values, plan, row_ids)?;
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

fn loaded_topn_batches(table_id: i64, item: &ClusterStatsItem) -> Vec<Range<usize>> {
    const PREFIX: &str =
        "insert into mysql.stats_top_n (table_id, is_index, hist_id, value, count) values ";
    let Some(topn) = &item.topn else {
        return Vec::new();
    };
    let entries = topn.entries();
    let mut batches = Vec::new();
    let mut start = 0;
    while start < entries.len() {
        let mut end = (start + LOADED_STATS_BATCH_INSERT_SIZE).min(entries.len());
        let mut sql_len = PREFIX.len();
        for (index, entry) in entries.iter().enumerate().take(end).skip(start) {
            let row = must_escape_sql(
                "(%?, %?, %?, %?, %?)",
                &[
                    SqlArg::Signed(table_id),
                    SqlArg::Signed(i64::from(item.is_index)),
                    SqlArg::Signed(item.id),
                    SqlArg::Bytes(Some(&entry.encoded)),
                    SqlArg::Unsigned(entry.count),
                ],
            );
            let row_len = row.len() + usize::from(index > start);
            if index > start && sql_len + row_len > LOADED_STATS_MAX_INSERT_LENGTH {
                end = index;
                break;
            }
            sql_len += row_len;
        }
        batches.push(start..end);
        start = end;
    }
    batches
}

fn loaded_bucket_batches(
    table_id: i64,
    item: &ClusterStatsItem,
) -> Result<Vec<Range<usize>>, StatsWriteError> {
    const PREFIX: &str = "insert into mysql.stats_buckets (table_id, is_index, hist_id, bucket_id, count, repeats, lower_bound, upper_bound, ndv) values ";
    let buckets = &item.histogram.buckets;
    let mut batches = Vec::new();
    let mut start = 0;
    while start < buckets.len() {
        let mut end = (start + LOADED_STATS_BATCH_INSERT_SIZE).min(buckets.len());
        let mut sql_len = PREFIX.len();
        for (index, bucket) in buckets.iter().enumerate().take(end).skip(start) {
            let lower = bound_blob(&bucket.lower_bound)?;
            let upper = bound_blob(&bucket.upper_bound)?;
            let lower = match &lower {
                Datum::Null => None,
                Datum::Bytes(value) => Some(value.as_slice()),
                other => unreachable!("blob conversion returned {other:?}"),
            };
            let upper = match &upper {
                Datum::Null => None,
                Datum::Bytes(value) => Some(value.as_slice()),
                other => unreachable!("blob conversion returned {other:?}"),
            };
            let previous_count = index
                .checked_sub(1)
                .map_or(0, |previous| buckets[previous].count);
            let row = must_escape_sql(
                "(%?, %?, %?, %?, %?, %?, %?, %?, %?)",
                &[
                    SqlArg::Signed(table_id),
                    SqlArg::Signed(i64::from(item.is_index)),
                    SqlArg::Signed(item.id),
                    SqlArg::Signed(index as i64),
                    SqlArg::Signed(bucket.count - previous_count),
                    SqlArg::Signed(bucket.repeat),
                    SqlArg::Bytes(lower),
                    SqlArg::Bytes(upper),
                    SqlArg::Signed(bucket.ndv),
                ],
            );
            let row_len = row.len() + usize::from(index > start);
            if index > start && sql_len + row_len > LOADED_STATS_MAX_INSERT_LENGTH {
                end = index;
                break;
            }
            sql_len += row_len;
        }
        batches.push(start..end);
        start = end;
    }
    Ok(batches)
}

fn plan_loaded_buckets_delete<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    table_id: i64,
    item: &ClusterStatsItem,
    plan: &mut StatsWritePlan,
) -> Result<(), StatsWriteError> {
    let table = locate(catalog, "stats_buckets")?;
    let mut rows = StatsRows::open(
        snapshot,
        table,
        &["table_id", "is_index", "hist_id", "bucket_id"],
        table_id,
    )?;
    rows.retract_prefix(&loaded_item_prefix(table_id, item), plan)?;
    Ok(())
}

fn plan_loaded_buckets_insert<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    table_id: i64,
    item: &ClusterStatsItem,
    range: &Range<usize>,
    now: Time,
    plan: &mut StatsWritePlan,
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
) -> Result<(), StatsWriteError> {
    let table = locate(catalog, "stats_buckets")?;
    let mut rows = StatsRows::open(
        snapshot,
        table,
        &["table_id", "is_index", "hist_id", "bucket_id"],
        table_id,
    )?;
    for bucket_id in range.clone() {
        let bucket = &item.histogram.buckets[bucket_id];
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
        let previous_count = bucket_id
            .checked_sub(1)
            .map_or(0, |previous| item.histogram.buckets[previous].count);
        set(
            table,
            &mut values,
            "count",
            Datum::Int(bucket.count - previous_count),
        );
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
        rows.store_row(catalog, &values, plan, row_ids)?;
        plan.bucket_count += 1;
    }
    Ok(())
}

fn plan_loaded_topn_delete<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    table_id: i64,
    item: &ClusterStatsItem,
    plan: &mut StatsWritePlan,
) -> Result<(), StatsWriteError> {
    let table = locate(catalog, "stats_top_n")?;
    let mut rows = StatsRows::open(
        snapshot,
        table,
        &["table_id", "is_index", "hist_id", "value"],
        table_id,
    )?;
    rows.retract_prefix(&loaded_item_prefix(table_id, item), plan)?;
    Ok(())
}

fn plan_loaded_topn_insert<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    table_id: i64,
    item: &ClusterStatsItem,
    range: &Range<usize>,
    now: Time,
    plan: &mut StatsWritePlan,
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
) -> Result<(), StatsWriteError> {
    let table = locate(catalog, "stats_top_n")?;
    let mut rows = StatsRows::open(
        snapshot,
        table,
        &["table_id", "is_index", "hist_id", "value"],
        table_id,
    )?;
    let entries = item
        .topn
        .as_ref()
        .expect("a TopN insert statement has a TopN")
        .entries();
    for entry in &entries[range.clone()] {
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
        rows.store_row(catalog, &values, plan, row_ids)?;
        plan.topn_count += 1;
    }
    Ok(())
}

fn plan_loaded_fm_insert<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    table_id: i64,
    item: &ClusterStatsItem,
    now: Time,
    plan: &mut StatsWritePlan,
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
) -> Result<(), StatsWriteError> {
    let table = locate(catalog, "stats_fm_sketch")?;
    let Some(encoded) = encode_fm_sketch(item.fm_sketch.as_ref()) else {
        return Ok(());
    };
    let mut rows = StatsRows::open(
        snapshot,
        table,
        &["table_id", "is_index", "hist_id"],
        table_id,
    )?;
    store_fm_sketch(
        catalog, &mut rows, table, table_id, item, encoded, now, plan, row_ids,
    )?;
    Ok(())
}

fn store_fm_sketch(
    catalog: &ClusterCatalog,
    rows: &mut StatsRows<'_>,
    table: &TableInfo,
    table_id: i64,
    item: &ClusterStatsItem,
    encoded: Vec<u8>,
    now: Time,
    plan: &mut StatsWritePlan,
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
) -> Result<(), StatsWriteError> {
    let mut values = defaults_row(table, now)?;
    set(table, &mut values, "table_id", Datum::Int(table_id));
    set(
        table,
        &mut values,
        "is_index",
        Datum::Int(i64::from(item.is_index)),
    );
    set(table, &mut values, "hist_id", Datum::Int(item.id));
    set(table, &mut values, "value", Datum::Bytes(encoded));
    rows.store_row(catalog, &values, plan, row_ids)
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
    Ok(())
}

fn plan_meta<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    stats: &ClusterTableStats,
    analyze_meta: Option<AnalyzeStatsMeta>,
    now: Time,
    plan: &mut StatsWritePlan,
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
) -> Result<bool, StatsWriteError> {
    let table = locate(catalog, "stats_meta")?;
    let mut rows = StatsRows::open(snapshot, table, &["table_id"], stats.table_id)?;
    let identity = vec![format!("{:?}", Datum::Int(stats.table_id))];
    if let (Some(meta), Some(existing)) = (analyze_meta, rows.existing_values(&identity)) {
        let current_snapshot = row_u64(table, existing, "snapshot").unwrap_or_default();
        if current_snapshot >= stats.snapshot {
            return Ok(false);
        }
        let current_count = row_i64(table, existing, "count").unwrap_or_default();
        let current_modify_count = row_i64(table, existing, "modify_count").unwrap_or_default();
        let result_count = i64::try_from(stats.row_count).unwrap_or(i64::MAX);
        let count = if meta.analyze_snapshot {
            current_count
                .wrapping_add(result_count)
                .wrapping_sub(meta.base_count)
                .max(0)
        } else {
            result_count.max(0)
        };
        let modify_count = current_modify_count
            .wrapping_sub(meta.base_modify_count)
            .max(0);
        let mut values = existing.clone();
        set(table, &mut values, "version", Datum::UInt(stats.version));
        set(table, &mut values, "modify_count", Datum::Int(modify_count));
        set(table, &mut values, "count", Datum::Int(count));
        set(table, &mut values, "snapshot", Datum::UInt(stats.snapshot));
        set(
            table,
            &mut values,
            "last_stats_histograms_version",
            Datum::UInt(stats.version),
        );
        rows.store_row(catalog, &values, plan, row_ids)?;
        return Ok(true);
    }

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
    rows.store_row(catalog, &values, plan, row_ids)?;
    // Deliberately no retraction: `mysql.stats_meta` holds one row per table
    // and the identity IS the table, so there is never a leftover. Retracting
    // here would only be a way to delete a row this `ANALYZE` did not write.
    Ok(true)
}

fn plan_independent_index_meta<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    stats: &ClusterTableStats,
    now: Time,
    plan: &mut StatsWritePlan,
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
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
    rows.store_row(catalog, &values, plan, row_ids)?;
    Ok(inserted)
}

fn plan_histograms<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    stats: &ClusterTableStats,
    now: Time,
    replace_all: bool,
    plan: &mut StatsWritePlan,
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
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
        rows.store_row(catalog, &values, plan, row_ids)?;
        plan.histogram_count += 1;
    }
    // A histogram this `ANALYZE` no longer produces -- a dropped index, a
    // column that became unanalyzable -- must lose its row, or the loader
    // would keep handing the planner statistics for something that is gone.
    retract_analyzed_items(&mut rows, stats, replace_all, plan)?;
    Ok(())
}

fn plan_buckets<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    stats: &ClusterTableStats,
    now: Time,
    replace_all: bool,
    plan: &mut StatsWritePlan,
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
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
            rows.store_row(catalog, &values, plan, row_ids)?;
            plan.bucket_count += 1;
        }
    }
    // A shorter histogram than last time leaves its tail behind, and a stale
    // bucket read back as part of this histogram would be a range the table
    // no longer has.
    retract_analyzed_items(&mut rows, stats, replace_all, plan)?;
    Ok(())
}

fn plan_topn<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    stats: &ClusterTableStats,
    now: Time,
    replace_all: bool,
    plan: &mut StatsWritePlan,
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
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
            rows.store_row(catalog, &values, plan, row_ids)?;
            plan.topn_count += 1;
        }
    }
    retract_analyzed_items(&mut rows, stats, replace_all, plan)?;
    Ok(())
}

fn plan_fm_sketches<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    stats: &ClusterTableStats,
    now: Time,
    replace_all: bool,
    dump_fm: bool,
    plan: &mut StatsWritePlan,
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
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
            store_fm_sketch(
                catalog,
                &mut rows,
                table,
                stats.table_id,
                item,
                encoded,
                now,
                plan,
                row_ids,
            )?;
        }
    }
    // Go deletes the previous row for every analyzed item even when this is
    // a logical/global write and `needDumpFMS` is false.
    retract_analyzed_items(&mut rows, stats, replace_all, plan)?;
    Ok(())
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

