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

use std::collections::BTreeMap;

use tidb_datatype::UNSPECIFIED_LENGTH;
use tidb_executor::analyze::AnalyzeError as ComputeError;
use tidb_executor::analyze::{
    AnalyzePlan, AnalyzeRun, AnalyzedColumn, AnalyzedHistogram, AnalyzedIndex,
};
use tidb_model::table_info::TableInfo;
use tidb_model::SchemaState;

use crate::cluster_catalog::MetaSnapshot;
use crate::cluster_stats_load::{ClusterStatsItem, ClusterTableStats};
use crate::mysql_system_tables::{scan_system_table, SystemRow, SystemTableError, SystemTableView};
use crate::system_row_write::origin_default;

pub use tidb_executor::analyze::{
    AnalyzeOptions, AnalyzeStatement, SampleMemoryExceeded, SampleMemoryQuota,
    MEM_QUOTA_ANALYZE_VARIABLE, STATS_VERSION_2,
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
/// `version` is the TSO the statistics are stamped with, and must be the
/// `start_ts` of the transaction that will store them: that is what makes a
/// concurrent `ANALYZE` on a Go node either lose the write conflict or be
/// ordered after this one, rather than interleave with it.
pub fn analyze_table<S: MetaSnapshot>(
    snapshot: &mut S,
    table: &TableInfo,
    options: &AnalyzeOptions,
    realtime_count: Option<i64>,
    version: u64,
) -> Result<AnalyzeReport, AnalyzeError> {
    let plan = cluster_analyze_plan(table)?;
    let mut run = AnalyzeRun::start(&plan, options, realtime_count)?;

    let names: Vec<&str> = plan
        .columns()
        .iter()
        .map(|column| column.name.as_str())
        .collect();
    let view = SystemTableView::project(table.name.original(), table, &names);
    for (key, value) in scan_system_table(snapshot, &view)? {
        let stored = SystemRow::parse(&view, &key, &value)?;
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
    let analyzed = run.finish()?;

    let stats = ClusterTableStats {
        table_id: table.id,
        version,
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
        histogram: built.histogram,
        topn: built.topn,
        cms: None,
    }
}

/// Which columns and indexes an `ANALYZE` of this stored table covers.
///
/// Every refusal here is about a value this tier cannot reproduce from the
/// stored bytes; the shape rules the two tiers share (which TopN is
/// suppressed, which slots exist) are [`AnalyzePlan`]'s own.
fn cluster_analyze_plan(table: &TableInfo) -> Result<AnalyzePlan, AnalyzeError> {
    if table.partition.is_some() {
        return Err(AnalyzeError::unsupported(format!(
            "this node does not analyze the partitioned table `{}`: its statistics are one set \
             per partition plus a merged global set, which is a separate write path",
            table.name.original()
        )));
    }
    let mut columns = Vec::new();
    let mut by_offset: BTreeMap<i64, usize> = BTreeMap::new();
    for column in table.cols().iter_deref() {
        let column = column.read();
        if column.state != SchemaState::PUBLIC || column.hidden {
            continue;
        }
        if column.is_generated() {
            return Err(AnalyzeError::unsupported(format!(
                "this node does not analyze `{}`.`{}`: a generated column's value is an \
                 expression it does not evaluate over stored rows",
                table.name.original(),
                column.name.original()
            )));
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
        if index.mv_index
            || index.global
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
        let mut column_positions = Vec::with_capacity(index.columns.len());
        for index_column in index.columns.iter_deref() {
            let index_column = index_column.read();
            if i64::from(index_column.length) != UNSPECIFIED_LENGTH {
                return Err(AnalyzeError::unsupported(format!(
                    "this node does not analyze the prefix index `{}` on `{}`: its sampled \
                     value is each column value cut to the prefix, which it does not cut",
                    index.name.original(),
                    table.name.original()
                )));
            }
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
        }
        indexes.push(AnalyzedIndex {
            id: index.id,
            single_column_unique: index.unique && column_positions.len() == 1,
            column_positions,
        });
    }

    Ok(AnalyzePlan::new(columns, indexes, table.name.original())?)
}
