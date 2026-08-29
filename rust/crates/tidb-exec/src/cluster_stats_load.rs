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

//! Reading a table's statistics out of a cluster's `mysql.stats_*`.
//!
//! This is the supply line for [`tidb_stats::histogram`]: `ANALYZE TABLE` runs
//! on a Go node and writes rows; this reads those rows back and rebuilds the
//! `Histogram`/`TopN`/`CMSketch` triple the estimator consumes. Go source of
//! truth is `pkg/statistics/handle/storage/read.go`, whose
//! `HistogramFromStorageWithPriority`, `CMSketchAndTopNFromStorageWithHighPriority`,
//! and `StatsMetaCountAndModifyCount` this file follows statement for
//! statement, minus the `SELECT` — the rows come from the record range, the
//! way every `mysql.*` read on this node does (see
//! [`crate::mysql_system_tables`]).
//!
//! # The bound encoding, which is the whole correctness question
//!
//! `mysql.stats_buckets.lower_bound`/`upper_bound` are `LONGBLOB`, and what
//! those blobs hold depends on `is_index`. Go's writer is
//! `storage/save.go::convertBoundToBlob`, which is
//! `Datum.ConvertTo(TypeBlob)` — *not* the datum codec — and its reader is
//! `read.go::convertBoundFromBlob`. So:
//!
//! * **`is_index = 1`**: the histogram's values already *are* index-key bytes
//!   (`codec.EncodeKey` output). Converting those bytes to a blob is the
//!   identity, and Go reads them straight back as a blob without decoding.
//!   The bound stays raw bytes here too, because that is the domain every
//!   index-histogram comparison happens in.
//! * **`is_index = 0`**: the blob is the column value's *text* form — a
//!   `BIGINT` bound is stored as `"90"`, a `DECIMAL(10,2)` bound as `"11.00"`,
//!   a `DATETIME` bound as `"2024-05-05 05:05:05"`. Reading it back is a
//!   string-to-type conversion at the column's declared type. Verified
//!   against a live v8.5.7 cluster (see the fixtures in this module's tests,
//!   captured from its `mysql.stats_buckets`).
//!
//! Two exceptions Go carves out of that second rule, both reproduced here:
//!
//! * **String-evaluated columns** (but not `ENUM`/`SET`): Go replaces the
//!   target type with `TypeBlob` before converting. Under a new collation the
//!   stored bound is the *collation sort key*, not the original text, and it
//!   can be longer than the column's `flen` and not valid in the column's
//!   charset; converting at the declared type would truncate or reject it.
//!   The bound therefore stays raw bytes.
//! * **`BIT`**: converting `BIT` to a blob formats it as a decimal integer,
//!   so the blob is parsed back as one.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use tidb_datatype::{
    BinaryLiteral, BinaryLiteralWidth, ConversionFlags, Datum, EvalType, FieldType, FieldTypeCode,
};
use tidb_model::table_info::TableInfo;
use tidb_stats::cmsketch::{decode_cmsketch_and_topn, CmsSketch, TopN};
use tidb_stats::histogram::{Bucket, Histogram};
use tidb_stats::{
    ColAndIdxExistenceMap, Column, ColumnInfo, HistColl, Index, IndexInfo, StatsLoadedStatus, Table,
};

use crate::cluster_catalog::{ClusterCatalog, MetaSnapshot};
use crate::mysql_system_tables::{
    scan_system_table, scan_system_table_index_prefixed, scan_system_table_prefixed, SystemRow,
    SystemTableError, SystemTableView,
};

/// Go `statistics.UTCWithAllowInvalidDateCtx`, as conversion flags.
///
/// A stored bound is a value a previous `ANALYZE` already accepted; refusing
/// to read it back because it is, say, a zero date would lose the whole
/// histogram over a value the table legitimately contains.
const BOUND_CONVERSION_FLAGS: ConversionFlags = ConversionFlags::from_bits(
    ConversionFlags::ALLOW_NEGATIVE_TO_UNSIGNED
        | ConversionFlags::IGNORE_ZERO_DATE_ERR
        | ConversionFlags::IGNORE_ZERO_IN_DATE_ERR
        | ConversionFlags::IGNORE_INVALID_DATE_ERR,
);

/// One column's or one index's statistics, as one cluster snapshot holds them.
#[derive(Clone, Debug)]
pub struct ClusterStatsItem {
    /// `hist_id`: the column ID, or the index ID when [`Self::is_index`].
    pub id: i64,
    /// Whether this is an index histogram rather than a column histogram.
    pub is_index: bool,
    /// `stats_ver`: which `ANALYZE` generation wrote this.
    pub stats_ver: i64,
    /// `flag`, Go's `statistics.AnalyzeFlag` bitset.
    pub flag: i64,
    /// Go `StatsLoadedStatus`: whether the item has its full payload or only
    /// histogram metadata resident.
    pub load_status: StatsLoadedStatus,
    /// The histogram, with its buckets in ascending bound order and its
    /// counts made cumulative the way Go's in-memory `Bucket.Count` is.
    pub histogram: Histogram,
    /// `mysql.stats_top_n`'s rows, when any.
    pub topn: Option<TopN>,
    /// `mysql.stats_histograms.cm_sketch`, when it holds one.
    pub cms: Option<CmsSketch>,
}

/// Everything one cluster snapshot says about one table's statistics.
#[derive(Clone, Debug)]
pub struct ClusterTableStats {
    /// The physical table ID these rows key on.
    pub table_id: i64,
    /// `mysql.stats_meta.version`, the TSO the stats were last written at.
    pub version: u64,
    /// `mysql.stats_meta.snapshot`, the snapshot TSO whose rows ANALYZE read.
    pub snapshot: u64,
    /// `mysql.stats_meta.modify_count`: rows changed since that write.
    pub modify_count: i64,
    /// `mysql.stats_meta.count`: the table's estimated row count.
    pub row_count: u64,
    /// Go `statistics.Table.LastAnalyzeVersion`: initialized from the analyze
    /// snapshot and advanced by analyzed column/index histogram versions.
    pub last_analyze_version: u64,
    /// `mysql.stats_meta.last_stats_histograms_version`, Go's independent
    /// table-level histogram refresh marker.
    pub last_stats_hist_version: u64,
    /// Column histograms, in `hist_id` order.
    pub columns: Vec<ClusterStatsItem>,
    /// Index histograms, in `hist_id` order.
    pub indexes: Vec<ClusterStatsItem>,
}

impl ClusterTableStats {
    fn analyzed_histogram_version(&self) -> u64 {
        self.columns
            .iter()
            .chain(&self.indexes)
            .filter(|item| item.stats_ver != 0)
            .map(|item| item.histogram.last_update_version)
            .max()
            .unwrap_or_default()
    }

    fn refresh_last_analyze_version(&self, current: u64) -> u64 {
        let result = current.max(self.analyzed_histogram_version());
        if result == 0 {
            self.snapshot
        } else {
            result
        }
    }

    /// The column histogram for one column ID.
    #[must_use]
    pub fn column(&self, id: i64) -> Option<&ClusterStatsItem> {
        self.columns.iter().find(|item| item.id == id)
    }

    /// The index histogram for one index ID.
    #[must_use]
    pub fn index(&self, id: i64) -> Option<&ClusterStatsItem> {
        self.indexes.iter().find(|item| item.id == id)
    }

    /// Converts the storage row image into Go's canonical `statistics.Table`.
    /// Planner-specific views must be derived from this full table rather
    /// than cached independently.
    #[must_use]
    pub fn to_statistics_table(&self, table_info: &TableInfo) -> Arc<Table> {
        let row_count = i64::try_from(self.row_count).unwrap_or(i64::MAX);
        let stats_version = self
            .columns
            .iter()
            .chain(&self.indexes)
            .map(|item| item.stats_ver)
            .max()
            .unwrap_or_default();
        let mut hist_coll = HistColl::new(
            self.table_id,
            row_count,
            self.modify_count,
            self.columns.len(),
            self.indexes.len(),
        );
        hist_coll.stats_version = i32::try_from(stats_version).unwrap_or_else(|_| {
            if stats_version.is_negative() {
                i32::MIN
            } else {
                i32::MAX
            }
        });
        let mut existence = ColAndIdxExistenceMap::new(self.columns.len(), self.indexes.len());

        for item in &self.columns {
            let Some(column) = item.to_column(self.table_id, table_info) else {
                continue;
            };
            existence.insert_column(
                item.id,
                item.stats_ver != 0 || item.histogram.ndv > 0 || item.histogram.null_count > 0,
            );
            hist_coll.set_column(item.id, column);
        }
        for item in &self.indexes {
            let Some(index) = item.to_index(self.table_id, table_info) else {
                continue;
            };
            existence.insert_index(item.id, item.stats_ver != 0);
            hist_coll.set_index(item.id, index);
        }

        Arc::new(Table {
            existence_map: Some(Arc::new(RwLock::new(existence))),
            hist_coll,
            version: self.version,
            last_analyze_version: self
                .last_analyze_version
                .max(self.analyzed_histogram_version()),
            last_stats_hist_version: self.last_stats_hist_version.max(self.snapshot),
            table_info_update_ts: table_info.update_ts,
            is_pk_handle: table_info.pk_is_handle,
        })
    }
}

impl ClusterStatsItem {
    fn to_column(&self, table_id: i64, table_info: &TableInfo) -> Option<Column> {
        let (metadata, is_handle) = table_info.cols().iter_deref().find_map(|column| {
            let column = column.read();
            (column.id == self.id).then(|| {
                let primary_key =
                    column.field_type.flags() & tidb_datatype::FieldTypeFlags::PRI_KEY != 0;
                (
                    ColumnInfo {
                        id: column.id,
                        name: column.name.lowercase().to_owned(),
                        primary_key,
                    },
                    table_info.pk_is_handle && primary_key,
                )
            })
        })?;
        Some(Column {
            cmsketch: self.cms.clone(),
            top_n: self.topn.clone(),
            info: Some(metadata),
            histogram: self.histogram.clone(),
            stats_loaded_status: self.load_status,
            physical_id: table_id,
            stats_version: self.stats_ver,
            is_handle,
            ..Column::default()
        })
    }

    fn to_index(&self, table_id: i64, table_info: &TableInfo) -> Option<Index> {
        let metadata = table_info.indices.iter_deref().find_map(|index| {
            let index = index.read();
            (index.id == self.id).then(|| IndexInfo {
                id: index.id,
                name: index.name.lowercase().to_owned(),
                columns: index
                    .columns
                    .iter_deref()
                    .map(|column| column.read().name.lowercase().to_owned())
                    .collect(),
                mv_index: index.mv_index,
            })
        })?;
        Some(Index {
            cmsketch: self.cms.clone(),
            top_n: self.topn.clone(),
            info: Some(metadata),
            histogram: self.histogram.clone(),
            stats_loaded_status: self.load_status,
            stats_version: self.stats_ver,
            physical_id: table_id,
            ..Index::default()
        })
    }
}

/// The declared type of each column a table's column histograms are keyed by.
///
/// A column histogram's `hist_id` is the stored column ID, and the bound
/// decode needs that column's declared type — so this is exactly the map a
/// caller must hand [`ClusterStatsLoader::load_table`], and building it from
/// the analyzed table's own `TableInfo` is how a caller gets the cluster's
/// types rather than a guess at them.
#[must_use]
pub fn column_types_of(table: &TableInfo) -> BTreeMap<i64, FieldType> {
    table
        .cols()
        .iter_deref()
        .map(|column| {
            let column = column.read();
            (column.id, column.field_type.clone())
        })
        .collect()
}

/// `mysql.stats_top_n`'s `(value, count)` rows, keyed by
/// `(is_index, hist_id)` — the pair that names one histogram within a table.
type TopNRowsByHistogram = BTreeMap<(bool, i64), Vec<(Vec<u8>, u64)>>;

/// The `mysql.stats_*` tables located once in a loaded catalog.
#[derive(Clone, Debug)]
pub struct ClusterStatsLoader {
    meta: SystemTableView,
    histograms: SystemTableView,
    buckets: SystemTableView,
    topn: SystemTableView,
}

impl ClusterStatsLoader {
    /// Locates the statistics tables in one loaded catalog.
    pub fn locate(catalog: &ClusterCatalog) -> Result<Self, SystemTableError> {
        Ok(Self {
            meta: SystemTableView::locate(
                catalog,
                "stats_meta",
                &[
                    "table_id",
                    "version",
                    "snapshot",
                    "modify_count",
                    "count",
                    // Go stamps this with the same start TS as `version` on
                    // every stats write (`save.go:200`) and a GC pass
                    // re-stamps it (`gc.go:144`); SHOW STATS_META renders it
                    // as `Last_analyze_time`. Nullable: rows written by
                    // pre-8.x TiDBs have no value, which reads as zero --
                    // never analyzed.
                    "last_stats_histograms_version",
                ],
            )?,
            histograms: SystemTableView::locate(
                catalog,
                "stats_histograms",
                &[
                    "table_id",
                    "is_index",
                    "hist_id",
                    "distinct_count",
                    "null_count",
                    "tot_col_size",
                    "version",
                    "cm_sketch",
                    "stats_ver",
                    "flag",
                    "correlation",
                ],
            )?,
            buckets: SystemTableView::locate(
                catalog,
                "stats_buckets",
                &[
                    "table_id",
                    "is_index",
                    "hist_id",
                    "bucket_id",
                    "count",
                    "repeats",
                    "lower_bound",
                    "upper_bound",
                    "ndv",
                ],
            )?,
            topn: SystemTableView::locate(
                catalog,
                "stats_top_n",
                &["table_id", "is_index", "hist_id", "value", "count"],
            )?,
        })
    }

    /// Go `stats_meta`'s rows, ONE range scan: `(table_id, version, modify_count,
    /// count)` for every table that has one.
    ///
    /// This is the cheap half of Go's `Handle.Update`
    /// (`pkg/statistics/handle/update.go`): a tick first reads only this table
    /// and compares versions against its cache, and touches histograms,
    /// buckets and top-n for a table ONLY when that table's version moved. A
    /// pass whose versions all match costs exactly this one scan.
    pub fn load_all_meta<S: MetaSnapshot>(
        &self,
        snapshot: &mut S,
    ) -> Result<BTreeMap<i64, (u64, i64, u64)>, SystemTableError> {
        let mut result = BTreeMap::new();
        for (key, value) in scan_system_table(snapshot, &self.meta)? {
            let row = SystemRow::parse(&self.meta, &key, &value)?;
            let Some(table_id) = row.i64("table_id")? else {
                continue;
            };
            result.insert(
                table_id,
                (
                    row.u64("version")?.unwrap_or_default(),
                    row.i64("modify_count")?.unwrap_or_default(),
                    row.u64("count")?.unwrap_or_default(),
                ),
            );
        }
        Ok(result)
    }

    /// Reads every histogram one table has, at one snapshot.
    ///
    /// `None` means the table has no `mysql.stats_meta` row at all — it was
    /// never analyzed and never had a row-count delta flushed — which is not
    /// the same as an analyzed table with no histograms, and the planner must
    /// be able to tell the two apart before it falls back to pseudo stats.
    pub fn load_table<S: MetaSnapshot>(
        &self,
        snapshot: &mut S,
        table_id: i64,
        column_types: &BTreeMap<i64, FieldType>,
    ) -> Result<Option<ClusterTableStats>, SystemTableError> {
        self.load_table_with_payload(snapshot, table_id, column_types, true)
    }

    /// Go's leased `tableStatsFromStorage(..., loadAll=false)` initialization:
    /// read histogram metadata but leave buckets, TopN, and CMSketch evicted.
    pub fn load_table_lite<S: MetaSnapshot>(
        &self,
        snapshot: &mut S,
        table_id: i64,
        column_types: &BTreeMap<i64, FieldType>,
    ) -> Result<Option<ClusterTableStats>, SystemTableError> {
        self.load_table_with_payload(snapshot, table_id, column_types, false)
    }

    /// Pinned Go `TableStatsFromStorage(..., loadAll=false)` during a cache
    /// update. Metadata-only refreshes preserve resident payload, while a
    /// newer histogram is reloaded in full only when its old item was already
    /// resident. Items that were evicted remain metadata-only.
    pub fn load_statistics_table_for_update<S: MetaSnapshot>(
        &self,
        snapshot: &mut S,
        table_info: &TableInfo,
        column_types: &BTreeMap<i64, FieldType>,
        current: Option<&Table>,
    ) -> Result<Option<Arc<Table>>, SystemTableError> {
        let table_id = table_info.id;
        let Some(lite) = self.load_table_lite(snapshot, table_id, column_types)? else {
            return Ok(None);
        };
        let Some(current) = current else {
            let mut table = lite.to_statistics_table(table_info).as_ref().clone();
            table.last_analyze_version = lite.refresh_last_analyze_version(0);
            table.last_stats_hist_version = lite.last_stats_hist_version;
            return Ok(Some(Arc::new(table)));
        };

        let mut updated = current.copy_as(tidb_stats::CopyIntent::BothMapsWritable);
        updated.version = lite.version;
        updated.hist_coll.realtime_count = i64::try_from(lite.row_count).unwrap_or(i64::MAX);
        updated.hist_coll.modify_count = lite.modify_count;
        updated.last_analyze_version =
            lite.refresh_last_analyze_version(current.last_analyze_version);
        updated.last_stats_hist_version = lite.last_stats_hist_version;
        updated.table_info_update_ts = table_info.update_ts;

        for metadata in lite.columns.into_iter().chain(lite.indexes) {
            if metadata.is_index {
                let current_item = current.hist_coll.get_index(metadata.id);
                let replacement = match current_item {
                    Some(item)
                        if item
                            .read()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .histogram
                            .last_update_version
                            >= metadata.histogram.last_update_version =>
                    {
                        Some(
                            item.read()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .clone(),
                        )
                    }
                    Some(item)
                        if item
                            .read()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .is_full_load() =>
                    {
                        self.load_item(snapshot, table_id, true, metadata.id, None, true)?
                            .and_then(|item| item.to_index(table_id, table_info))
                    }
                    Some(_) => metadata.to_index(table_id, table_info),
                    None => None,
                };
                if let Some(existence) = &updated.existence_map {
                    existence
                        .write()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .insert_index(metadata.id, metadata.stats_ver != 0);
                }
                if metadata.stats_ver != 0 {
                    updated.hist_coll.stats_version =
                        i32::try_from(metadata.stats_ver).unwrap_or(i32::MAX);
                }
                if let Some(replacement) = replacement {
                    updated.hist_coll.set_index(metadata.id, replacement);
                }
            } else {
                let current_item = current.hist_coll.get_column(metadata.id);
                let replacement = match current_item {
                    Some(item)
                        if item
                            .read()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .histogram
                            .last_update_version
                            >= metadata.histogram.last_update_version =>
                    {
                        let mut item = item
                            .read()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .clone();
                        item.histogram.tot_col_size = metadata.histogram.tot_col_size;
                        Some(item)
                    }
                    Some(item)
                        if item
                            .read()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .is_full_load() =>
                    {
                        self.load_item(
                            snapshot,
                            table_id,
                            false,
                            metadata.id,
                            column_types.get(&metadata.id),
                            true,
                        )?
                        .and_then(|item| item.to_column(table_id, table_info))
                    }
                    Some(_) => metadata.to_column(table_id, table_info),
                    None => None,
                };
                if let Some(existence) = &updated.existence_map {
                    existence
                        .write()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .insert_column(
                            metadata.id,
                            metadata.stats_ver != 0
                                || metadata.histogram.ndv > 0
                                || metadata.histogram.null_count > 0,
                        );
                }
                if metadata.stats_ver != 0 {
                    updated.hist_coll.stats_version =
                        i32::try_from(metadata.stats_ver).unwrap_or(i32::MAX);
                }
                if let Some(replacement) = replacement {
                    updated.hist_coll.set_column(metadata.id, replacement);
                }
            }
        }
        Ok(Some(Arc::new(updated)))
    }

    fn load_table_with_payload<S: MetaSnapshot>(
        &self,
        snapshot: &mut S,
        table_id: i64,
        column_types: &BTreeMap<i64, FieldType>,
        full_load: bool,
    ) -> Result<Option<ClusterTableStats>, SystemTableError> {
        let Some((version, snapshot_ts, modify_count, row_count, last_stats_hist_version)) =
            self.load_meta(snapshot, table_id)?
        else {
            return Ok(None);
        };
        let buckets = if full_load {
            self.load_buckets(snapshot, table_id, column_types)?
        } else {
            BTreeMap::new()
        };
        let topn = if full_load {
            self.load_topn(snapshot, table_id)?
        } else {
            BTreeMap::new()
        };

        let mut stats = ClusterTableStats {
            table_id,
            version,
            snapshot: snapshot_ts,
            modify_count,
            row_count,
            last_analyze_version: snapshot_ts,
            last_stats_hist_version,
            columns: Vec::new(),
            indexes: Vec::new(),
        };
        let key_prefix = [Datum::Int(table_id)];
        for (key, value) in scan_system_table_prefixed(snapshot, &self.histograms, &key_prefix)? {
            let row = SystemRow::parse(&self.histograms, &key, &value)?;
            if row.i64("table_id")?.unwrap_or_default() != table_id {
                continue;
            }
            let is_index = row.i64("is_index")?.unwrap_or_default() != 0;
            let id = row.i64("hist_id")?.unwrap_or_default();
            let mut histogram = Histogram {
                id,
                ndv: row.i64("distinct_count")?.unwrap_or_default(),
                null_count: row.i64("null_count")?.unwrap_or_default(),
                last_update_version: row.u64("version")?.unwrap_or_default(),
                tot_col_size: row.i64("tot_col_size")?.unwrap_or_default(),
                correlation: row.f64("correlation")?.unwrap_or_default(),
                buckets: buckets.get(&(is_index, id)).cloned().unwrap_or_default(),
            };
            // Go stores `tot_col_size`/`correlation` only for columns and
            // passes literal zeros for an index (`read.go::LoadHistogram`).
            if is_index {
                histogram.tot_col_size = 0;
                histogram.correlation = 0.0;
            }
            let stats_ver = row.i64("stats_ver")?.unwrap_or_default();
            let (cms, top) = if full_load {
                let cms_bytes = (stats_ver <= 1)
                    .then(|| row.bytes("cm_sketch"))
                    .transpose()?
                    .flatten();
                decode_cmsketch_and_topn(
                    cms_bytes.as_deref(),
                    topn.get(&(is_index, id)).map_or(&[][..], Vec::as_slice),
                )
                .map_err(|error| SystemTableError::Decode {
                    name: self.histograms.name().to_owned(),
                    detail: error.to_string(),
                })?
            } else {
                (None, None)
            };
            let initialized = if is_index {
                stats_ver != 0
            } else {
                stats_ver != 0 || histogram.ndv > 0 || histogram.null_count > 0
            };
            let load_status = if !initialized {
                StatsLoadedStatus::default()
            } else if full_load {
                StatsLoadedStatus::full_load()
            } else {
                StatsLoadedStatus::all_evicted()
            };
            let item = ClusterStatsItem {
                id,
                is_index,
                stats_ver,
                flag: row.i64("flag")?.unwrap_or_default(),
                load_status,
                histogram,
                topn: top,
                cms,
            };
            if is_index {
                stats.indexes.push(item);
            } else {
                stats.columns.push(item);
            }
        }
        stats.columns.sort_by_key(|item| item.id);
        stats.indexes.sort_by_key(|item| item.id);
        Ok(Some(stats))
    }

    /// Go `readStatsForOneItem`: load one column or index from its exact
    /// histogram/bucket/TopN key ranges. `full_load=false` is the metadata-only
    /// request used by asynchronous initialization.
    pub fn load_item<S: MetaSnapshot>(
        &self,
        snapshot: &mut S,
        table_id: i64,
        is_index: bool,
        id: i64,
        column_type: Option<&FieldType>,
        full_load: bool,
    ) -> Result<Option<ClusterStatsItem>, SystemTableError> {
        let prefix = [
            Datum::Int(table_id),
            Datum::Int(i64::from(is_index)),
            Datum::Int(id),
        ];
        let Some((key, value)) = scan_system_table_prefixed(snapshot, &self.histograms, &prefix)?
            .into_iter()
            .next()
        else {
            return Ok(None);
        };
        let row = SystemRow::parse(&self.histograms, &key, &value)?;
        if row.i64("table_id")?.unwrap_or_default() != table_id
            || (row.i64("is_index")?.unwrap_or_default() != 0) != is_index
            || row.i64("hist_id")?.unwrap_or_default() != id
        {
            return Ok(None);
        }
        let stats_ver = row.i64("stats_ver")?.unwrap_or_default();
        let mut histogram = Histogram {
            id,
            ndv: row.i64("distinct_count")?.unwrap_or_default(),
            null_count: row.i64("null_count")?.unwrap_or_default(),
            last_update_version: row.u64("version")?.unwrap_or_default(),
            tot_col_size: row.i64("tot_col_size")?.unwrap_or_default(),
            correlation: row.f64("correlation")?.unwrap_or_default(),
            buckets: Vec::new(),
        };
        if is_index {
            histogram.tot_col_size = 0;
            histogram.correlation = 0.0;
        }
        let (cms, topn) = if full_load {
            let column_types = column_type
                .map(|field_type| BTreeMap::from([(id, field_type.clone())]))
                .unwrap_or_default();
            histogram.buckets = self
                .load_buckets_prefixed(snapshot, &prefix, &column_types)?
                .remove(&(is_index, id))
                .unwrap_or_default();
            let topn = self.load_topn_prefixed(snapshot, &prefix)?;
            let cms_bytes = (stats_ver <= 1)
                .then(|| row.bytes("cm_sketch"))
                .transpose()?
                .flatten();
            decode_cmsketch_and_topn(
                cms_bytes.as_deref(),
                topn.get(&(is_index, id)).map_or(&[][..], Vec::as_slice),
            )
            .map_err(|error| SystemTableError::Decode {
                name: self.histograms.name().to_owned(),
                detail: error.to_string(),
            })?
        } else {
            (None, None)
        };
        let initialized = if is_index {
            stats_ver != 0
        } else {
            stats_ver != 0 || histogram.ndv > 0 || histogram.null_count > 0
        };
        let load_status = if !initialized {
            StatsLoadedStatus::default()
        } else if full_load {
            StatsLoadedStatus::full_load()
        } else {
            StatsLoadedStatus::all_evicted()
        };
        Ok(Some(ClusterStatsItem {
            id,
            is_index,
            stats_ver,
            flag: row.i64("flag")?.unwrap_or_default(),
            load_status,
            histogram,
            topn,
            cms,
        }))
    }

    /// Go `StatsMetaCountAndModifyCount`.
    /// One table's `(version, snapshot, modify_count, count,
    /// last_stats_histograms_version)`,
    /// or `None` for a table with no row -- the public single-table form of
    /// [`Self::load_all_meta`], and the read behind
    /// [`Self::load_table`]'s presence check.
    pub fn load_meta<S: MetaSnapshot>(
        &self,
        snapshot: &mut S,
        table_id: i64,
    ) -> Result<Option<(u64, u64, i64, u64, u64)>, SystemTableError> {
        // `mysql.stats_meta` declares `PRIMARY KEY (table_id) CLUSTERED`, so
        // this prefix is the row's exact key: one point read, not a scan.
        let rows = scan_system_table_prefixed(snapshot, &self.meta, &[Datum::Int(table_id)])?;
        for (key, value) in rows {
            let row = SystemRow::parse(&self.meta, &key, &value)?;
            if row.i64("table_id")?.unwrap_or_default() != table_id {
                continue;
            }
            return Ok(Some((
                row.u64("version")?.unwrap_or_default(),
                row.u64("snapshot")?.unwrap_or_default(),
                row.i64("modify_count")?.unwrap_or_default(),
                row.u64("count")?.unwrap_or_default(),
                row.u64("last_stats_histograms_version")?
                    .unwrap_or_default(),
            )));
        }
        Ok(None)
    }

    /// Reads one table's buckets, grouped by `(is_index, hist_id)` and left in
    /// `bucket_id` order with cumulative counts.
    fn load_buckets<S: MetaSnapshot>(
        &self,
        snapshot: &mut S,
        table_id: i64,
        column_types: &BTreeMap<i64, FieldType>,
    ) -> Result<BTreeMap<(bool, i64), Vec<Bucket>>, SystemTableError> {
        self.load_buckets_prefixed(snapshot, &[Datum::Int(table_id)], column_types)
    }

    fn load_buckets_prefixed<S: MetaSnapshot>(
        &self,
        snapshot: &mut S,
        prefix: &[Datum],
        column_types: &BTreeMap<i64, FieldType>,
    ) -> Result<BTreeMap<(bool, i64), Vec<Bucket>>, SystemTableError> {
        // `(bucket_id, lower, upper, repeats, ndv, per-bucket count)` keyed by
        // histogram; `bucket_id` leads so the sort restores Go's
        // `order by bucket_id`, which is what makes the running total below
        // reproduce Go's cumulative `Bucket.Count`.
        let mut collected: BTreeMap<(bool, i64), Vec<(i64, Bucket)>> = BTreeMap::new();
        let table_id = match prefix.first() {
            Some(Datum::Int(table_id)) => *table_id,
            _ => 0,
        };
        for (key, value) in scan_system_table_prefixed(snapshot, &self.buckets, prefix)? {
            let row = SystemRow::parse(&self.buckets, &key, &value)?;
            if row.i64("table_id")?.unwrap_or_default() != table_id {
                continue;
            }
            let is_index = row.i64("is_index")?.unwrap_or_default() != 0;
            let id = row.i64("hist_id")?.unwrap_or_default();
            let field_type = column_types.get(&id);
            let lower = decode_bound(
                row.bytes("lower_bound")?.unwrap_or_default(),
                is_index,
                field_type,
                self.buckets.name(),
            )?;
            let upper = decode_bound(
                row.bytes("upper_bound")?.unwrap_or_default(),
                is_index,
                field_type,
                self.buckets.name(),
            )?;
            collected.entry((is_index, id)).or_default().push((
                row.i64("bucket_id")?.unwrap_or_default(),
                Bucket {
                    count: row.i64("count")?.unwrap_or_default(),
                    repeat: row.i64("repeats")?.unwrap_or_default(),
                    ndv: row.i64("ndv")?.unwrap_or_default(),
                    lower_bound: lower,
                    upper_bound: upper,
                },
            ));
        }
        Ok(collected
            .into_iter()
            .map(|(histogram, mut rows)| {
                rows.sort_by_key(|(bucket_id, _)| *bucket_id);
                // The stored `count` is the bucket's *own* row count
                // (`save.go` subtracts the previous bucket's cumulative count
                // before writing), so reading it back means adding them up
                // again. A loader that skipped this would report the last
                // bucket as holding one bucket's worth of rows and make every
                // range estimate over the histogram far too small.
                let mut running = 0_i64;
                let buckets = rows
                    .into_iter()
                    .map(|(_, mut bucket)| {
                        running += bucket.count;
                        bucket.count = running;
                        bucket
                    })
                    .collect();
                (histogram, buckets)
            })
            .collect())
    }

    /// Reads one table's TopN rows, grouped by `(is_index, hist_id)`.
    fn load_topn<S: MetaSnapshot>(
        &self,
        snapshot: &mut S,
        table_id: i64,
    ) -> Result<TopNRowsByHistogram, SystemTableError> {
        self.load_topn_prefixed(snapshot, &[Datum::Int(table_id)])
    }

    fn load_topn_prefixed<S: MetaSnapshot>(
        &self,
        snapshot: &mut S,
        prefix: &[Datum],
    ) -> Result<TopNRowsByHistogram, SystemTableError> {
        let table_id = match prefix.first() {
            Some(Datum::Int(table_id)) => *table_id,
            _ => 0,
        };
        let mut collected = TopNRowsByHistogram::new();
        for (key, value) in scan_system_table_index_prefixed(snapshot, &self.topn, "tbl", prefix)? {
            let row = SystemRow::parse(&self.topn, &key, &value)?;
            if row.i64("table_id")?.unwrap_or_default() != table_id {
                continue;
            }
            let is_index = row.i64("is_index")?.unwrap_or_default() != 0;
            let id = row.i64("hist_id")?.unwrap_or_default();
            collected.entry((is_index, id)).or_default().push((
                row.bytes("value")?.unwrap_or_default(),
                row.u64("count")?.unwrap_or_default(),
            ));
        }
        Ok(collected)
    }
}

/// Go `read.go::convertBoundFromBlob`, plus the `is_index`/string-type
/// branches its caller `HistogramFromStorageWithPriority` wraps it in.
///
/// See this module's doc for why each branch exists.
fn decode_bound(
    stored: Vec<u8>,
    is_index: bool,
    field_type: Option<&FieldType>,
    table: &str,
) -> Result<Datum, SystemTableError> {
    // An index bound is already index-key bytes, and a column whose type the
    // caller could not name is one this node has no conversion target for;
    // in both cases the stored bytes are the most faithful answer available.
    let Some(field_type) = field_type.filter(|_| !is_index) else {
        return Ok(Datum::Bytes(stored));
    };
    match field_type.code() {
        // Converting BIT to a blob formats it as a decimal integer, so that
        // is what comes back.
        FieldTypeCode::Bit => {
            let text = String::from_utf8_lossy(&stored);
            let Ok(mut value) = text.parse::<u64>() else {
                // Go falls back to the blob's own bytes as the literal.
                return Ok(Datum::BinaryLiteral(BinaryLiteral::from(stored)));
            };
            let flen = field_type.flen();
            // Go saturates a bound wider than the column rather than dropping
            // the histogram; a `BIT(n)` value can only ever be `2^n - 1`.
            if (1..64).contains(&flen) && value >= 1 << flen {
                value = (1 << flen) - 1;
            }
            let byte_size = ((flen.clamp(1, 64) + 7) >> 3) as u8;
            let width = BinaryLiteralWidth::try_from(byte_size).map_err(|error| {
                SystemTableError::Decode {
                    name: table.to_owned(),
                    detail: format!("a BIT bound has an impossible width: {error}"),
                }
            })?;
            Ok(Datum::Bit(BinaryLiteral::from_uint(value, Some(width))))
        }
        // Go swaps the target for `TypeBlob` here: under a new collation the
        // stored bound is a collation sort key, which the declared string type
        // would truncate or reject.
        code if field_type.eval_type() == EvalType::String
            && code != FieldTypeCode::Enum
            && code != FieldTypeCode::Set =>
        {
            Ok(Datum::Bytes(stored))
        }
        _ => Datum::Bytes(stored)
            .convert_to(field_type, BOUND_CONVERSION_FLAGS)
            .map(|converted| converted.value)
            .map_err(|error| SystemTableError::Decode {
                name: table.to_owned(),
                detail: format!("a histogram bound did not convert: {error}"),
            }),
    }
}

#[cfg(test)]
mod tests {
    use tidb_ast::CiString;
    use tidb_datatype::FieldTypeCode;
    use tidb_model::{GoShared, GoSharedPointerSlice};

    use super::*;

    /// Every bound fixture below is the exact `mysql.stats_buckets` blob a
    /// live v8.5.7 playground wrote for `ANALYZE TABLE` over a known
    /// distribution, read back with `hex(lower_bound)`. They are the reason
    /// this module claims to know the encoding rather than assume it.
    fn field_type(code: FieldTypeCode) -> FieldType {
        FieldType::new(code)
    }

    #[test]
    fn storage_image_builds_the_canonical_full_statistics_table() {
        let mut primary_type = FieldType::new(FieldTypeCode::LongLong);
        primary_type.set_flags(tidb_datatype::FieldTypeFlags::PRI_KEY);
        let table_info = TableInfo {
            id: 7,
            update_ts: 55,
            pk_is_handle: true,
            columns: GoSharedPointerSlice::from_handles(vec![Some(GoShared::new(
                tidb_model::column::ColumnInfo {
                    id: 1,
                    name: CiString::new("A"),
                    field_type: primary_type,
                    state: tidb_model::SchemaState::PUBLIC,
                    ..tidb_model::column::ColumnInfo::default()
                },
            ))]),
            ..TableInfo::default()
        };
        let stats = ClusterTableStats {
            table_id: 7,
            version: 42,
            snapshot: 40,
            modify_count: 3,
            row_count: 100,
            last_analyze_version: 40,
            last_stats_hist_version: 43,
            columns: vec![
                ClusterStatsItem {
                    id: 1,
                    is_index: false,
                    stats_ver: 2,
                    flag: 1,
                    load_status: StatsLoadedStatus::full_load(),
                    histogram: Histogram {
                        id: 1,
                        ndv: 10,
                        last_update_version: 41,
                        ..Histogram::default()
                    },
                    topn: Some(TopN::new(0)),
                    cms: None,
                },
                // A dropped column's stale mysql.stats_histograms row is not
                // retained in Go's statistics.Table.
                ClusterStatsItem {
                    id: 99,
                    is_index: false,
                    stats_ver: 2,
                    flag: 0,
                    load_status: StatsLoadedStatus::all_evicted(),
                    histogram: Histogram {
                        id: 99,
                        ..Histogram::default()
                    },
                    topn: None,
                    cms: None,
                },
            ],
            indexes: Vec::new(),
        };

        let table = stats.to_statistics_table(&table_info);
        assert_eq!(table.hist_coll.physical_id, 7);
        assert_eq!(table.hist_coll.realtime_count, 100);
        assert_eq!(table.hist_coll.modify_count, 3);
        assert_eq!(table.hist_coll.stats_version, 2);
        assert_eq!(table.version, 42);
        assert_eq!(table.last_analyze_version, 41);
        assert_eq!(table.last_stats_hist_version, 43);
        assert_eq!(table.table_info_update_ts, 55);
        assert!(table.is_pk_handle);
        let column = table.hist_coll.get_column(1).unwrap();
        let column = column.read().unwrap();
        assert_eq!(column.info.as_ref().unwrap().name, "a");
        assert!(column.is_handle);
        assert!(column.is_full_load());
        assert!(table.hist_coll.get_column(99).is_none());
        let existence = table.existence_map.as_ref().unwrap().read().unwrap();
        assert!(existence.has_analyzed(1, false));
        assert!(!existence.has(99, false));
    }

    #[test]
    fn refresh_keeps_last_analyze_separate_from_the_histogram_refresh_marker() {
        let stats = ClusterTableStats {
            table_id: 7,
            version: 80,
            snapshot: 50,
            modify_count: 0,
            row_count: 1,
            last_analyze_version: 50,
            last_stats_hist_version: 70,
            columns: vec![ClusterStatsItem {
                id: 1,
                is_index: false,
                stats_ver: 2,
                flag: 0,
                load_status: StatsLoadedStatus::all_evicted(),
                histogram: Histogram {
                    id: 1,
                    last_update_version: 37,
                    ..Histogram::default()
                },
                topn: None,
                cms: None,
            }],
            indexes: Vec::new(),
        };

        assert_eq!(stats.refresh_last_analyze_version(23), 37);
        assert_eq!(stats.last_stats_hist_version, 70);

        let mut no_histograms = stats.clone();
        no_histograms.columns.clear();
        assert_eq!(no_histograms.refresh_last_analyze_version(23), 23);
        assert_eq!(no_histograms.refresh_last_analyze_version(0), 50);
    }

    #[test]
    fn a_column_bound_is_the_values_text_form_not_its_datum_encoding() {
        // `id BIGINT`: the cluster stored bucket 2's bounds as "11" and "12".
        let bound = decode_bound(
            b"11".to_vec(),
            false,
            Some(&field_type(FieldTypeCode::LongLong)),
            "t",
        )
        .expect("the bound converts");
        assert_eq!(bound, Datum::Int(11));
        // Read as a datum-codec value instead, `0x31 0x31` would be a
        // nonsense flag byte — which is exactly the silent misread this
        // fixture exists to rule out.
    }

    #[test]
    fn a_decimal_bound_keeps_its_stored_scale() {
        // `c DECIMAL(10,2)`: stored as "11.00", not as 1100 or 11.
        let mut decimal = field_type(FieldTypeCode::NewDecimal);
        decimal.set_flen(10);
        decimal.set_decimal(2);
        let bound = decode_bound(b"11.00".to_vec(), false, Some(&decimal), "t")
            .expect("the bound converts");
        assert_eq!(bound.sql_string().unwrap(), "11.00");
    }

    #[test]
    fn a_datetime_bound_parses_back_from_its_printed_form() {
        // `d DATETIME`: stored as "2024-05-05 05:05:05".
        let bound = decode_bound(
            b"2024-05-05 05:05:05".to_vec(),
            false,
            Some(&field_type(FieldTypeCode::Datetime)),
            "t",
        )
        .expect("the bound converts");
        assert_eq!(bound.sql_string().unwrap(), "2024-05-05 05:05:05");
    }

    #[test]
    fn a_string_column_bound_stays_raw_bytes_because_it_may_be_a_collation_key() {
        // `b VARCHAR(32)`: the cluster stored "charlie". Under a new
        // collation the same slot holds a sort key that the declared VARCHAR
        // would truncate, so neither is converted.
        let mut varchar = field_type(FieldTypeCode::Varchar);
        varchar.set_flen(32);
        let bound = decode_bound(b"charlie".to_vec(), false, Some(&varchar), "t")
            .expect("the bound converts");
        assert_eq!(bound, Datum::Bytes(b"charlie".to_vec()));
    }

    #[test]
    fn an_index_bound_stays_the_index_key_bytes_the_cluster_stored() {
        // Index `ia(a)`, bucket 0 upper bound, exactly as the cluster wrote
        // it: `0x03` is the datum codec's INT flag and the eight bytes are
        // 50 with its sign bit flipped. Converting this at column `a`'s INT
        // type would read the flag byte as text and produce garbage, so an
        // index bound is never converted.
        let stored = vec![0x03, 0x80, 0, 0, 0, 0, 0, 0, 0x32];
        let bound = decode_bound(
            stored.clone(),
            true,
            Some(&field_type(FieldTypeCode::Long)),
            "t",
        )
        .expect("an index bound never converts");
        assert_eq!(bound, Datum::Bytes(stored));
    }

    #[test]
    fn a_bound_for_a_column_the_caller_could_not_type_stays_bytes() {
        let bound = decode_bound(b"90".to_vec(), false, None, "t").expect("the bound is kept");
        assert_eq!(bound, Datum::Bytes(b"90".to_vec()));
    }
}
