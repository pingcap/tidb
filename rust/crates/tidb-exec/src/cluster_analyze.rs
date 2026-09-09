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

//! `ANALYZE TABLE` over CLUSTER storage: reading a table's rows out of a
//! transaction's snapshot and storing the statistics as `mysql.stats_*` rows.
//!
//! This is the WRITE half of [`crate::cluster_stats_load`]. That module reads
//! `mysql.stats_*` rows a Go `ANALYZE` wrote; this one produces the same
//! rows, in the same shapes, so that a Go TiDB reading them back cannot tell
//! which node wrote them. The value of the pair is the differential: `SHOW
//! STATS_BUCKETS` on a Go server renders what this builds, and its `EXPLAIN`
//! estimates from it.
//!
//! # What is here, and what is not
//!
//! Only the storage-facing half. WHICH columns and indexes an `ANALYZE`
//! covers, what a scanned row contributes to the sample, and how a histogram
//! and TopN are built from it all live in [`tidb_executor::analyze`], which
//! the in-process tier drives too -- so a table analyzed here and the same
//! table analyzed in-process estimate identically rather than nearly so. This
//! module turns a `TableInfo` into that module's [`AnalyzePlan`], feeds it the
//! rows of the record range, and dresses the result as `ClusterStatsItem`s.
//!
//! # Where the samples come from, and how that differs from Go
//!
//! Go pushes sampling into TiKV: the coprocessor runs `AnalyzeReq` against
//! each region, samples there, and TiDB merges the per-region collectors
//! (`analyze_col_sampling.go`). This node scans the table's record range
//! through the same snapshot every other read on it uses and samples
//! in-process. The *mechanism* differs; the *algorithm* does not --
//! [`tidb_stats::row_sample_collector`] is Go's collector, weight rule and
//! all -- and the sample it draws has the same distribution, because both
//! Bernoulli and reservoir selection are indifferent to where the rows were
//! split.
//!
//! Two consequences are worth stating rather than hiding. Every row crosses
//! the wire, so this costs a full table read where Go costs a sampled one;
//! and the transaction that reads the rows is the transaction that writes the
//! statistics, so the row count and the histograms are one consistent view
//! rather than two.
//!
//! # The one deliberate divergence, and why it is harmless
//!
//! Go's FM sketch hashes a value's *doubly* encoded form -- its column
//! encoding, wrapped again as a byte string by `codec.EncodeValue` -- because
//! the collector receives a coprocessor result set whose fields are already
//! bytes. This hashes the value's own `codec.EncodeValue` once. An FM
//! sketch's NDV depends only on the *set* of distinct hashes, so any
//! injective encoding gives the same estimator; only the sketch's
//! randomisation differs, which is already different between two Go runs.
//! What must match, and does, is which values are considered the same: a
//! new-collation string column is hashed by its collation key here exactly as
//! it is there, so `'a'` and `'A'` are one distinct value under
//! `utf8mb4_general_ci` on both.
//!
//! # What this refuses, and why refusing is the honest answer
//!
//! Anything whose sampled value this node cannot reproduce *exactly* is
//! refused by name rather than approximated, because a wrong histogram is
//! worse than no histogram: the planner trusts it. See [`AnalyzeError`].

use std::collections::{BTreeMap, HashSet};

use tidb_datatype::UNSPECIFIED_LENGTH;
use tidb_executor::analyze::AnalyzeError as ComputeError;
use tidb_executor::analyze::{
    AnalyzePlan, AnalyzeRun, AnalyzedColumn, AnalyzedHistogram, AnalyzedIndex,
};
use tidb_model::index::IndexInfo;
use tidb_model::table_info::TableInfo;
use tidb_model::SchemaState;

use crate::cluster_catalog::{prefix_scan_end, PagedMetaSnapshot, RegionPagedMetaSnapshot};
use crate::cluster_stats_load::{ClusterStatsItem, ClusterTableStats};
use crate::mysql_system_tables::{SystemRow, SystemTableError, SystemTableView};
use crate::system_row_write::origin_default;

pub use tidb_executor::analyze::{
    resolve_analyze_options, AnalyzeColumnChoice, AnalyzeOptionOverrides, AnalyzeOptions,
    AnalyzeStatement, SampleMemoryExceeded, SampleMemoryQuota, MEM_QUOTA_ANALYZE_VARIABLE,
    STATS_VERSION_2,
};

/// Whether this statement is an `ANALYZE TABLE` this node runs, and against
/// which tables.
///
/// [`tidb_executor::analyze::lower_analyze`] answers it -- one refusal set for
/// both tiers -- and this only carries the answer in this module's error type.
pub fn lower_analyze(
    statement: &tidb_ast::Stmt,
    default_schema: &str,
) -> Result<Option<Vec<AnalyzeStatement>>, AnalyzeError> {
    Ok(tidb_executor::analyze::lower_analyze(
        statement,
        default_schema,
    )?)
}

/// Why one `ANALYZE TABLE` could not run on this tier.
///
/// The computation's own refusals ([`ComputeError`]) are shared with the
/// in-process tier and carried through unchanged; only the read failure is
/// this tier's own.
#[derive(Debug)]
pub enum AnalyzeError {
    /// The table's rows could not be read.
    Read(SystemTableError),
    /// The shared computation refused, or a sampled value did not encode.
    Compute(ComputeError),
}

impl AnalyzeError {
    /// A table shape whose statistics this node does not produce.
    fn unsupported(detail: String) -> Self {
        Self::Compute(ComputeError::Unsupported(detail))
    }
}

impl std::fmt::Display for AnalyzeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Read(error) => write!(formatter, "{error}"),
            Self::Compute(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for AnalyzeError {}

impl From<SystemTableError> for AnalyzeError {
    fn from(error: SystemTableError) -> Self {
        Self::Read(error)
    }
}

impl From<ComputeError> for AnalyzeError {
    fn from(error: ComputeError) -> Self {
        Self::Compute(error)
    }
}

/// What one `ANALYZE TABLE` produced, plus the receipt of how it got there.
#[derive(Clone, Debug)]
pub struct AnalyzeReport {
    /// The statistics, in exactly the shape
    /// [`crate::cluster_stats_load::ClusterStatsLoader`] reads back.
    pub stats: ClusterTableStats,
    /// Rows read.
    pub scanned_rows: i64,
    /// Rows the sampler kept.
    pub sampled_rows: i64,
    /// The Bernoulli rate in force, or `1.0` under `WITH n SAMPLES`.
    pub sample_rate: f64,
}

/// Runs one `ANALYZE TABLE` against one snapshot.
///
/// `realtime_count` is the table's `mysql.stats_meta.count` as this snapshot
/// sees it -- what Go's `getAdjustedSampleRate` calls `RealtimeCount` -- and
/// `None` means the table has no row there at all.
///
/// `version` is the sampling snapshot TSO stored in `AnalyzeResults.Snapshot`.
/// Pinned Go stamps the histogram rows with the later statistics-save
/// transaction version; the real cluster boundary performs that replacement
/// after this snapshot-only sampler returns.
/// Rows one paged read of the analyzed table returns.
///
/// Go's analyze consumes coprocessor batches as they land and never holds
/// every row of the table; this page size is the same idea over a record-range
/// walk. A few thousand rows keeps peak residency at a few megabytes while
/// still amortizing the per-page cursor bookkeeping.
const ANALYZE_SCAN_PAGE_ROWS: usize = 8_192;

fn finite_successor(key: &[u8]) -> Result<Vec<u8>, AnalyzeError> {
    prefix_scan_end(key).ok_or_else(|| {
        AnalyzeError::Read(SystemTableError::Snapshot(
            "a scanned record key has no successor".to_owned(),
        ))
    })
}

pub fn analyze_table<S: PagedMetaSnapshot>(
    snapshot: &mut S,
    table: &TableInfo,
    options: &AnalyzeOptions,
    realtime_count: Option<i64>,
    version: u64,
    selected_column_ids: Option<&HashSet<i64>>,
) -> Result<AnalyzeReport, AnalyzeError> {
    if table.partition.is_some() {
        return Err(AnalyzeError::unsupported(format!(
            "analyzing the partitioned table `{}` requires an explicit physical partition target",
            table.name.original()
        )));
    }
    analyze_physical_table(
        snapshot,
        table,
        table.id,
        options,
        realtime_count,
        version,
        selected_column_ids,
    )
}

/// Runs one physical table or partition using the logical table's schema.
pub fn analyze_physical_table<S: PagedMetaSnapshot>(
    snapshot: &mut S,
    table: &TableInfo,
    physical_id: i64,
    options: &AnalyzeOptions,
    realtime_count: Option<i64>,
    version: u64,
    selected_column_ids: Option<&HashSet<i64>>,
) -> Result<AnalyzeReport, AnalyzeError> {
    analyze_physical_table_with_progress(
        snapshot,
        table,
        physical_id,
        options,
        realtime_count,
        version,
        selected_column_ids,
        |_| {},
    )
}

pub(crate) fn analyze_physical_table_with_progress<S: PagedMetaSnapshot>(
    snapshot: &mut S,
    table: &TableInfo,
    physical_id: i64,
    options: &AnalyzeOptions,
    realtime_count: Option<i64>,
    version: u64,
    selected_column_ids: Option<&HashSet<i64>>,
    mut progress: impl FnMut(i64),
) -> Result<AnalyzeReport, AnalyzeError> {
    let plan = cluster_analyze_plan(table, selected_column_ids)?;
    let mut run = AnalyzeRun::start(&plan, options, realtime_count)?;

    let names: Vec<&str> = plan
        .columns()
        .iter()
        .map(|column| column.name.as_str())
        .collect();
    let mut physical_table = table.clone();
    physical_table.id = physical_id;
    let view = SystemTableView::project(table.name.original(), &physical_table, &names);
    // The rows stream into the sampler one page at a time, exactly as Go's
    // region collectors feed it: nothing here may materialize the whole table
    // first, because a table this engine analyzes can be the largest thing the
    // node ever reads at once.
    let mut cursor = view.record_prefix(&[])?;
    let range_end = finite_successor(&cursor)?;
    loop {
        if cursor.as_slice() >= range_end.as_slice() {
            break;
        }
        let page = snapshot
            .scan_page(&cursor, &range_end, ANALYZE_SCAN_PAGE_ROWS)
            .map_err(|error| AnalyzeError::Read(error.into()))?;
        if page.is_empty() {
            break;
        }
        for (key, value) in &page {
            let stored = SystemRow::parse(&view, key, value)?;
            let mut columns = Vec::with_capacity(plan.columns().len());
            for column in plan.columns() {
                // `stored_datum`, not `datum`: a stored NULL is NULL, while a
                // column the row has no entry for reads as its origin default.
                columns.push(
                    stored
                        .stored_datum(&column.name)?
                        .cloned()
                        .unwrap_or_else(|| column.absent_value.clone()),
                );
            }
            run.push(&columns)?;
        }
        progress(i64::try_from(page.len()).unwrap_or(i64::MAX));
        let last_key = page
            .last()
            .map(|(key, _)| key.clone())
            .expect("a non-empty page has a last pair");
        cursor = finite_successor(&last_key)?;
    }
    let analyzed = run.finish()?;

    let stats = ClusterTableStats {
        table_id: physical_id,
        version,
        snapshot: version,
        last_analyze_version: version,
        last_stats_hist_version: version,
        // Go's `SaveAnalyzeResultToStorage` stores
        // `max(curModifyCnt - results.BaseModifyCnt, 0)`: the modifications
        // that arrived *while the analyze ran*, which its sample therefore
        // does not describe. Go has such a window because it reads the rows
        // at one timestamp and saves at a later one, so a `dumpStatsDelta`
        // from another node can land in between and must be preserved.
        //
        // This path has no such window. It reads the rows, reads the previous
        // `stats_meta` and writes the new one all at the single `start_ts` of
        // one transaction, so `curModifyCnt` IS `BaseModifyCnt` and the
        // difference is zero -- not a discarded count. A Go node's delta flush
        // that commits after that `start_ts` writes the same `stats_meta` row
        // and is a plain write conflict at prewrite, so exactly one of the two
        // survives rather than one silently erasing the other.
        modify_count: 0,
        row_count: u64::try_from(analyzed.scanned_rows).unwrap_or_default(),
        columns: analyzed
            .columns
            .into_iter()
            .map(|built| stored_item(built, false))
            .collect(),
        indexes: analyzed
            .indexes
            .into_iter()
            .map(|built| stored_item(built, true))
            .collect(),
    };

    Ok(AnalyzeReport {
        stats,
        scanned_rows: analyzed.scanned_rows,
        sampled_rows: analyzed.sampled_rows,
        sample_rate: analyzed.sample_rate,
    })
}

/// Runs one pinned-Go independent global-index task.
pub fn analyze_independent_index<S: RegionPagedMetaSnapshot>(
    snapshot: &mut S,
    table: &TableInfo,
    index: &IndexInfo,
    options: &AnalyzeOptions,
    version: u64,
) -> Result<AnalyzeReport, AnalyzeError> {
    analyze_independent_index_with_progress(snapshot, table, index, options, version, |_| {})
}

pub(crate) fn analyze_independent_index_with_progress<S: RegionPagedMetaSnapshot>(
    snapshot: &mut S,
    table: &TableInfo,
    index: &IndexInfo,
    options: &AnalyzeOptions,
    version: u64,
    mut progress: impl FnMut(i64),
) -> Result<AnalyzeReport, AnalyzeError> {
    let bucket_count = usize::try_from(options.num_buckets)
        .map_err(|_| AnalyzeError::unsupported("invalid ANALYZE bucket count".to_owned()))?;
    let topn_count = usize::try_from(options.num_topn)
        .map_err(|_| AnalyzeError::unsupported("invalid ANALYZE TopN count".to_owned()))?;
    let column_count = index.columns.len();
    let prefix = tidb_codec::table_key::encode_index_seek_key(table.id, index.id, &[]);
    let range_end = finite_successor(&prefix)?;
    let regions = snapshot
        .scan_regions(&prefix, &range_end)
        .map_err(|error| AnalyzeError::Read(error.into()))?;
    let mut fragments = Vec::with_capacity(regions.len());
    for region in regions {
        let mut processor = tidb_stats::IndependentIndexAnalyze::new(
            index.id,
            column_count,
            bucket_count,
            topn_count,
        );
        let row_count = region.pairs.len();
        for (key, _) in region.pairs {
            let (encoded_columns, _) = tidb_tablecodec::cut_index_key(&key, column_count)
                .map_err(|error| AnalyzeError::unsupported(error.to_string()))?;
            processor
                .push(&encoded_columns)
                .map_err(|error| AnalyzeError::unsupported(error.to_string()))?;
        }
        progress(i64::try_from(row_count).unwrap_or(i64::MAX));
        fragments.push(processor.finish_fragment());
    }
    let built = tidb_stats::merge_independent_index_fragments(
        index.id,
        bucket_count,
        topn_count,
        fragments,
    )
    .map_err(|error| AnalyzeError::unsupported(error.to_string()))?;
    let count = built.count;
    Ok(AnalyzeReport {
        stats: ClusterTableStats {
            table_id: table.id,
            version,
            snapshot: version,
            last_analyze_version: version,
            last_stats_hist_version: version,
            modify_count: 0,
            row_count: u64::try_from(count).unwrap_or_default(),
            columns: Vec::new(),
            indexes: vec![ClusterStatsItem {
                id: index.id,
                is_index: true,
                stats_ver: 2,
                flag: 0,
                load_status: tidb_stats::StatsLoadedStatus::full_load(),
                histogram: built.histogram,
                topn: Some(built.topn),
                cms: None,
                fm_sketch: Some(built.fm_sketch),
            }],
        },
        scanned_rows: count,
        sampled_rows: count,
        sample_rate: 1.0,
    })
}

/// One built histogram as `mysql.stats_histograms` holds it.
///
/// Analyze v2 stores no CMSketch, and `flag` is Go's `AnalyzeFlag`, which a
/// full rebuild leaves at zero (it marks a histogram SYNTHESIZED from a
/// default value, which this path never produces).
fn stored_item(built: AnalyzedHistogram, is_index: bool) -> ClusterStatsItem {
    ClusterStatsItem {
        id: built.id,
        is_index,
        stats_ver: built.stats_ver,
        flag: 0,
        load_status: tidb_stats::StatsLoadedStatus::full_load(),
        histogram: built.histogram,
        topn: built.topn,
        cms: None,
        fm_sketch: built.fm_sketch,
    }
}

/// Which columns and indexes an `ANALYZE` of this stored table covers.
///
/// Every refusal here is about a value this tier cannot reproduce from the
/// stored bytes; the shape rules the two tiers share (which TopN is
/// suppressed, which slots exist) are [`AnalyzePlan`]'s own.
fn cluster_analyze_plan(
    table: &TableInfo,
    selected_column_ids: Option<&HashSet<i64>>,
) -> Result<AnalyzePlan, AnalyzeError> {
    let mut columns = Vec::new();
    let mut by_offset: BTreeMap<i64, usize> = BTreeMap::new();
    for column in table.cols().iter_deref() {
        let column = column.read();
        if column.state != SchemaState::PUBLIC || column.hidden {
            continue;
        }
        if selected_column_ids.is_some_and(|selected| !selected.contains(&column.id)) {
            continue;
        }
        // A VIRTUAL generated column has no bytes in the stored row to
        // sample -- its value lives only in the writer's evaluation, which
        // this scan cannot reproduce. Go gives it no histogram either
        // (`analyze_col.go` :755-758); the difference is that Go keeps going
        // and so does this plan now, rather than refusing every ANALYZE of
        // any table that carries one. A STORED generated column stays a
        // normal column: its value IS in the row.
        if column.is_virtual_generated() {
            continue;
        }
        let qualified = format!("`{}`.`{}`", table.name.original(), column.name.original());
        let collation = AnalyzedColumn::sampling_collation(&column.field_type, &qualified)?;
        // Materialised once, here, so a column whose origin default this
        // node cannot express refuses the whole ANALYZE instead of
        // silently analyzing its pre-DDL rows as NULL.
        let absent_value = origin_default(&column, table.name.original()).map_err(|error| {
            AnalyzeError::unsupported(format!(
                "this node does not analyze `{}`.`{}`: a row written before the column existed \
                 reads as its origin default, which it cannot materialise ({error})",
                table.name.original(),
                column.name.original()
            ))
        })?;
        by_offset.insert(column.offset, columns.len());
        columns.push(AnalyzedColumn {
            id: column.id,
            name: column.name.lowercase().to_owned(),
            field_type: column.field_type.clone(),
            collation,
            absent_value,
        });
    }

    let mut indexes = Vec::new();
    for index in table.indices.iter_deref() {
        let index = index.read();
        if index.state != SchemaState::PUBLIC {
            continue;
        }
        let is_special_global = index.global
            && index.columns.iter_deref().any(|index_column| {
                let index_column = index_column.read();
                index_column.length != UNSPECIFIED_LENGTH
                    || table
                        .cols()
                        .get(index_column.offset as usize)
                        .is_some_and(|column| column.read().is_virtual_generated())
            });
        if is_special_global {
            // Pinned Go removes this index from the ordinary column-sampling
            // task and creates one independent ordered index task for it.
            continue;
        }
        if index.mv_index
            || index.vector_info.is_some()
            || index.inverted_info.is_some()
            || index.full_text_info.is_some()
        {
            return Err(AnalyzeError::unsupported(format!(
                "this node does not analyze index `{}` on `{}`: its keys are not the plain \
                 column encoding a stored histogram bound is read back as",
                index.name.original(),
                table.name.original()
            )));
        }
        // An index whose key parts are virtual generated columns reads values
        // the stored row does not carry. Prefix values are cut below with the
        // same rule as Go's sampling builder.
        let covers_unsampled_part = index.columns.iter_deref().any(|index_column| {
            let index_column = index_column.read();
            !by_offset.contains_key(&index_column.offset)
        });
        if covers_unsampled_part {
            continue;
        }
        let mut column_positions = Vec::with_capacity(index.columns.len());
        let mut prefix_lengths = Vec::with_capacity(index.columns.len());
        for index_column in index.columns.iter_deref() {
            let index_column = index_column.read();
            let position = by_offset
                .get(&index_column.offset)
                .copied()
                .ok_or_else(|| {
                    AnalyzeError::unsupported(format!(
                        "index `{}` covers a column of `{}` that is not analyzable",
                        index.name.original(),
                        table.name.original()
                    ))
                })?;
            column_positions.push(position);
            prefix_lengths.push(index_column.length);
        }
        let has_prefix = prefix_lengths
            .iter()
            .any(|length| *length != UNSPECIFIED_LENGTH);
        indexes.push(AnalyzedIndex {
            id: index.id,
            single_column_unique: index.unique && column_positions.len() == 1 && !has_prefix,
            column_positions,
            prefix_lengths,
        });
    }

    Ok(AnalyzePlan::new(columns, indexes, table.name.original())?)
}

#[cfg(test)]
mod tests {
    use super::{analyze_independent_index, cluster_analyze_plan, AnalyzeOptions};
    use crate::cluster_catalog::{
        ClusterCatalogError, MetaPairs, MetaSnapshot, PagedMetaSnapshot, RegionPagedMetaSnapshot,
    };
    use tidb_ast::CiString;
    use tidb_codec::table_key::encode_index_seek_key;
    use tidb_codec::Encoder;
    use tidb_datatype::UNSPECIFIED_LENGTH;
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};
    use tidb_model::column::ColumnInfo;
    use tidb_model::index::{IndexColumn, IndexInfo};
    use tidb_model::table_info::TableInfo;
    use tidb_model::SchemaState;

    #[test]
    fn ordinary_global_index_is_sampled_with_partition_rows_like_go() {
        let column = ColumnInfo::new(1, "a", FieldType::new(FieldTypeCode::LongLong));
        let table = TableInfo {
            name: CiString::new("t"),
            columns: vec![column].into(),
            indices: vec![IndexInfo {
                id: 2,
                name: CiString::new("idx_a"),
                state: SchemaState::PUBLIC,
                global: true,
                columns: vec![IndexColumn {
                    name: CiString::new("a"),
                    offset: 0,
                    length: UNSPECIFIED_LENGTH,
                    use_changing_type: false,
                }]
                .into(),
                ..IndexInfo::default()
            }]
            .into(),
            ..TableInfo::default()
        };

        let plan = cluster_analyze_plan(&table, None)
            .expect("an ordinary global index uses the column sampling task");
        assert_eq!(plan.indexes().len(), 1);
        assert_eq!(plan.indexes()[0].id, 2);
    }

    #[test]
    fn special_global_index_is_not_in_the_ordinary_sampling_plan() {
        let column = ColumnInfo::new(1, "a", FieldType::new(FieldTypeCode::Varchar));
        let table = TableInfo {
            name: CiString::new("t"),
            columns: vec![column].into(),
            indices: vec![IndexInfo {
                id: 2,
                name: CiString::new("idx_a"),
                state: SchemaState::PUBLIC,
                global: true,
                columns: vec![IndexColumn {
                    name: CiString::new("a"),
                    offset: 0,
                    length: 3,
                    use_changing_type: false,
                }]
                .into(),
                ..IndexInfo::default()
            }]
            .into(),
            ..TableInfo::default()
        };

        let plan = cluster_analyze_plan(&table, None)
            .expect("the independent task owns a special global index");
        assert!(plan.indexes().is_empty());
    }

    struct RegionSnapshot(Vec<tidb_txnkv::transaction::SnapshotScanRegion>);

    impl MetaSnapshot for RegionSnapshot {
        fn get(&mut self, _key: &[u8]) -> Result<Option<Vec<u8>>, ClusterCatalogError> {
            Ok(None)
        }

        fn scan_prefix(&mut self, _prefix: &[u8]) -> Result<MetaPairs, ClusterCatalogError> {
            Ok(Vec::new())
        }
    }

    impl PagedMetaSnapshot for RegionSnapshot {
        fn scan_page(
            &mut self,
            _start: &[u8],
            _end: &[u8],
            _limit: usize,
        ) -> Result<MetaPairs, ClusterCatalogError> {
            Ok(Vec::new())
        }
    }

    impl RegionPagedMetaSnapshot for RegionSnapshot {
        fn scan_regions(
            &mut self,
            _start: &[u8],
            _end: &[u8],
        ) -> Result<Vec<tidb_txnkv::transaction::SnapshotScanRegion>, ClusterCatalogError> {
            Ok(self.0.clone())
        }
    }

    #[test]
    fn independent_index_keeps_region_topn_boundaries() {
        let table = TableInfo {
            id: 42,
            name: CiString::new("t"),
            ..TableInfo::default()
        };
        let index = IndexInfo {
            id: 7,
            name: CiString::new("idx_a"),
            state: SchemaState::PUBLIC,
            global: true,
            columns: vec![IndexColumn {
                name: CiString::new("a"),
                offset: 0,
                length: 3,
                use_changing_type: false,
            }]
            .into(),
            ..IndexInfo::default()
        };
        let encoder = Encoder::new(false);
        let one = encoder.encode_key(&[Datum::Int(1)]).unwrap();
        let two = encoder.encode_key(&[Datum::Int(2)]).unwrap();
        let key = |value: &[u8], handle: i64| {
            let mut encoded = value.to_vec();
            encoded.extend(encoder.encode_key(&[Datum::Int(handle)]).unwrap());
            encode_index_seek_key(table.id, index.id, &encoded)
        };
        let mut snapshot = RegionSnapshot(vec![
            tidb_txnkv::transaction::SnapshotScanRegion {
                region: tidb_txnkv::region::RegionVerId::new(1, 1, 1),
                end_key: key(&two, 1),
                pairs: vec![(key(&one, 1), Vec::new()), (key(&two, 1), Vec::new())],
            },
            tidb_txnkv::transaction::SnapshotScanRegion {
                region: tidb_txnkv::region::RegionVerId::new(2, 1, 1),
                end_key: Vec::new(),
                pairs: vec![(key(&two, 2), Vec::new()), (key(&two, 3), Vec::new())],
            },
        ]);
        let mut options = AnalyzeOptions::default();
        options.num_buckets = 2;
        options.num_topn = 1;

        let report =
            analyze_independent_index(&mut snapshot, &table, &index, &options, 100).unwrap();
        let item = &report.stats.indexes[0];
        assert_eq!(report.scanned_rows, 4);
        assert_eq!(item.topn.as_ref().unwrap().entries()[0].count, 2);
        assert_eq!(item.histogram.buckets.last().unwrap().count, 2);
    }
}
