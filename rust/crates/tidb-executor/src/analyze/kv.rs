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

//! `ANALYZE TABLE` over the IN-PROCESS catalog: a [`KvTable`]'s own rows in,
//! the [`TableStatistics`] the planner reads out.
//!
//! Everything that decides a NUMBER is [`super`]'s, shared with the cluster
//! tier. This file is only the two translations either side of it: a
//! `KvTable`'s columns and indexes into an [`AnalyzePlan`], and the built
//! histograms into the map `EXPLAIN` estimates from. That split is the
//! guarantee: the same ten rows analyzed here and analyzed through
//! `tidb_exec::cluster_analyze` produce the same histogram, so a query does
//! not change its plan when the storage under it does.
//!
//! # Where the result goes
//!
//! Into [`crate::driver::Catalog::set_table_statistics`], which is the ONE
//! place the planner reads statistics from -- so publishing there is what
//! makes `stats:pseudo` disappear from `EXPLAIN`, and nothing else has to
//! learn about `ANALYZE`. This tier keeps no `mysql.stats_*` rows: those are
//! the cluster's transport for statistics BETWEEN nodes, and an in-process
//! catalog has no other node to send them to. Writing them here would be a
//! second copy of the same facts with no reader.
//!
//! # A plan position IS a row offset
//!
//! A `KvTable` keeps its hidden columns contiguous at the END of `columns`
//! (see [`crate::expression_index`]), and this plan covers every visible
//! column -- one it cannot analyze refuses the statement rather than being
//! skipped. So the *n*th analyzed column is the *n*th value of a scanned row,
//! with no offset table in between, and an index over a hidden column is the
//! one case that has no position to map to.

use std::collections::BTreeMap;

use tidb_planner::cardinality::row_count_estimator::{ColumnStats, IndexStats};
use tidb_stats::cmsketch::TopN;

use crate::access_cost::TableStatistics;
use crate::analyze::{
    AnalyzeError, AnalyzeOptions, AnalyzePlan, AnalyzeRun, AnalyzedColumn, AnalyzedHistogram,
    AnalyzedIndex,
};
use crate::kv_table::{KvTable, RowDecodeContext};

/// Runs one `ANALYZE TABLE` over a table's stored rows.
///
/// `realtime_count` is what this table's statistics currently say its row
/// count is, `None` when it has never been analyzed -- Go's
/// `getAdjustedSampleRate` input, which is what decides whether the scan
/// samples at all.
///
/// The table is taken by `&mut` only because reading its rows drains a
/// cursor; nothing here writes to it.
pub fn analyze_kv_table(
    table: &mut KvTable,
    options: &AnalyzeOptions,
    realtime_count: Option<i64>,
    ctx: &crate::StmtContext,
) -> Result<TableStatistics, AnalyzeError> {
    let decode_context = RowDecodeContext::for_analyze(ctx);
    let plan = kv_analyze_plan(table, &decode_context)?;
    let rows = table
        .scan_rows_with_context(&decode_context)
        .map_err(|error| {
            AnalyzeError::Unsupported(format!(
                "this node could not read `{}` to analyze it: {error:?}",
                table.name
            ))
        })?;
    let analyzed_columns = plan.columns().len();
    let mut run = AnalyzeRun::start(&plan, options, realtime_count)?;
    for row in &rows {
        run.push(&row[..analyzed_columns])?;
    }
    let analyzed = run.finish()?;

    let visible = table.visible_columns();
    let mut columns = BTreeMap::new();
    for (position, built) in analyzed.columns.into_iter().enumerate() {
        let unsigned = visible[position].field_type.is_unsigned();
        columns.insert(built.id, column_statistics(built, unsigned));
    }
    let mut indexes = BTreeMap::new();
    for (position, built) in analyzed.indexes.into_iter().enumerate() {
        let stored = &table.indexes()[position];
        indexes.insert(
            built.id,
            index_statistics(built, stored.column_offsets.len(), stored.unique),
        );
    }

    Ok(TableStatistics::new(
        analyzed.scanned_rows,
        // A fresh `ANALYZE` describes exactly the rows it just read, so
        // nothing has been modified since. `tidb_exec::cluster_analyze` stores
        // the same zero, and says at length why that is a fact rather than a
        // discarded count.
        0,
        columns,
        indexes,
    ))
}

/// Go's `DecodeTopN`, which is the step this tier would otherwise skip.
///
/// `BuildHistAndTopN` returns its entries in PRUNING order -- by descending
/// count -- and Go never reads one in that order: every consumer gets its TopN
/// back from storage through `DecodeTopN`, which ends in `topN.Sort()`. The
/// estimator then binary-searches it (`TopN.LowerBound`), so an unsorted list
/// is not slower, it is WRONG: `WHERE a = 4` on a column whose counts happen
/// to rise with its values misses the entry entirely and estimates one row
/// instead of four. Measured exactly that way before this line existed.
///
/// The cluster tier gets the sort for free because its statistics really do go
/// through `mysql.stats_top_n`; this tier hands the built TopN straight to the
/// planner, so it performs the same last step of the load path itself.
fn sorted_topn(topn: Option<TopN>) -> Option<TopN> {
    topn.map(|mut topn| {
        topn.sort();
        topn
    })
}

/// One built column histogram, as the estimator reads it.
fn column_statistics(built: AnalyzedHistogram, unsigned: bool) -> ColumnStats {
    ColumnStats {
        histogram: built.histogram,
        topn: sorted_topn(built.topn),
        // Analyze v2 builds no CMSketch: its TopN and histogram carry what the
        // CMSketch used to.
        cms: None,
        stats_ver: built.stats_ver,
        unsigned,
    }
}

/// One built index histogram, with the two schema facts the estimator needs
/// that the histogram itself does not carry.
fn index_statistics(built: AnalyzedHistogram, num_columns: usize, unique: bool) -> IndexStats {
    IndexStats {
        histogram: built.histogram,
        topn: sorted_topn(built.topn),
        cms: None,
        stats_ver: built.stats_ver,
        num_columns,
        unique,
    }
}

/// Which columns and indexes an `ANALYZE` of this in-process table covers.
fn kv_analyze_plan(
    table: &KvTable,
    context: &RowDecodeContext,
) -> Result<AnalyzePlan, AnalyzeError> {
    let visible = table.visible_columns();
    let mut columns = Vec::with_capacity(visible.len());
    for column in visible {
        if column.generated.is_some() {
            return Err(AnalyzeError::Unsupported(format!(
                "this node does not analyze `{}`.`{}`: a generated column's value is an \
                 expression it does not evaluate over stored rows",
                table.name, column.name
            )));
        }
        let qualified = format!("`{}`.`{}`", table.name, column.name);
        let collation = AnalyzedColumn::sampling_collation(&column.field_type, &qualified)?;
        columns.push(AnalyzedColumn {
            id: column.id,
            name: column.name.to_ascii_lowercase(),
            collation,
            // A row written before an `ADD COLUMN` carries no entry for it and
            // reads back as the column's `OriginDefaultValue`. This tier's row
            // decoder already substitutes it, so a value never reaches the
            // sampler missing -- but the plan carries it anyway, because what
            // an absent value IS belongs to the shared core.
            absent_value: column
                .origin_default_value(context.origin_default_flags(), context.zone())
                .map_err(|error| {
                    AnalyzeError::Unsupported(format!(
                        "this node could not materialize `{qualified}`'s origin default: {error}"
                    ))
                })?,
        });
    }

    let mut indexes = Vec::with_capacity(table.indexes().len());
    for index in table.indexes() {
        let mut column_positions = Vec::with_capacity(index.column_offsets.len());
        for offset in &index.column_offsets {
            if *offset >= visible.len() {
                return Err(AnalyzeError::Unsupported(format!(
                    "this node does not analyze index `{}` on `{}`: it covers a hidden column an \
                     expression index was rewritten into, which has no histogram",
                    index.name, table.name
                )));
            }
            column_positions.push(*offset);
        }
        indexes.push(AnalyzedIndex {
            id: index.id,
            single_column_unique: index.unique && column_positions.len() == 1,
            column_positions,
        });
    }

    AnalyzePlan::new(columns, indexes, &table.name)
}
