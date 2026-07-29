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
//! # What it does not write
//!
//! `mysql.stats_fm_sketch` is written only for a partitioned table's
//! partitions, which this node does not analyze; `mysql.column_stats_usage`
//! records when a column was last analyzed and is a predicate-column input,
//! not a statistic. `cm_sketch` is left NULL because analyze v2 stores no
//! CMSketch (`save.go:266`).

use std::collections::BTreeMap;

use tidb_datatype::{ConversionFlags, Datum, FieldType, FieldTypeCode, Time, UNSPECIFIED_LENGTH};
use tidb_meta::{key, value};
use tidb_model::table_info::TableInfo;
use tidb_stats::histogram::Bucket;
use tidb_txnkv::transaction::OptimisticMutation;

use crate::cluster_catalog::{ClusterCatalog, MetaSnapshot};
use crate::cluster_stats_load::{ClusterStatsItem, ClusterTableStats};
use crate::mysql_system_tables::{
    scan_system_table, scan_system_table_prefixed, HandleLayout, SystemRow, SystemTableError,
    SystemTableView,
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
}

impl std::fmt::Display for StatsWriteError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingTable(name) => write!(formatter, "this cluster has no `{name}`"),
            Self::Read(error) => write!(formatter, "{error}"),
            Self::Encode(error) => write!(formatter, "{error}"),
            Self::Bound(detail) => write!(formatter, "a histogram bound did not convert: {detail}"),
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

impl StatsWritePlan {
    /// Whether the plan writes nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.mutations.is_empty()
    }
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
    let mut plan = StatsWritePlan::default();
    plan_meta(snapshot, catalog, stats, now, &mut plan)?;
    plan_histograms(snapshot, catalog, stats, now, &mut plan)?;
    plan_buckets(snapshot, catalog, stats, now, &mut plan)?;
    plan_topn(snapshot, catalog, stats, now, &mut plan)?;
    Ok(plan)
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
    // Go stores the analyze snapshot only when `tidb_enable_analyze_snapshot`
    // is on, which it is not by default (`save.go:188`). Storing one here
    // would make a later Go `ANALYZE` whose snapshot is older *skip its own
    // write* (`save.go:181`), so the default's zero is what this writes.
    set(table, &mut values, "snapshot", Datum::UInt(0));
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

fn plan_histograms<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    stats: &ClusterTableStats,
    now: Time,
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
    rows.retract_remaining(plan)?;
    rows.publish_watermark(catalog, plan)
}

fn plan_buckets<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    stats: &ClusterTableStats,
    now: Time,
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
    rows.retract_remaining(plan)?;
    rows.publish_watermark(catalog, plan)
}

fn plan_topn<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    stats: &ClusterTableStats,
    now: Time,
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
    rows.retract_remaining(plan)?;
    rows.publish_watermark(catalog, plan)
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
        .cols()
        .into_iter()
        .find(|column| column.name.lowercase() == name)
        .map(|column| column.id)
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
        .into_iter()
        .map(|column| column.name.lowercase().to_owned())
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
    let mut rows = Vec::new();
    for (key, value) in pairs {
        let parsed = SystemRow::parse(&view, &key, &value)?;
        let mut values = RowValues::new();
        for column in table.cols() {
            let stored = parsed
                .datum(column.name.lowercase())?
                .cloned()
                .unwrap_or(Datum::Null);
            values.insert(column.id, stored);
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
