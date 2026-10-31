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

//! Statistics and predicate-estimation support shared by executor lowering.
//!
//! Access-path enumeration, skyline pruning, reader costing, and path choice
//! live in `tidb-planner`, matching Go's `DataSource.findBestTask` boundary.
//! This module deliberately contains no second executor-local access planner.
//! The table statistics type itself is `tidb_planner::table_statistics`.


use crate::StatsStmtCtx as _;
use tidb_planner::stmt_ctx::PlannerStmtCtx as _;
use tidb_stats::Table;

/// One table's loaded statistics as the statement's statistics handle holds
/// them (Go `sctx.GetStatsHandle().GetPhysicalTableStats`); `None` is Go's
/// `PseudoTable`.
#[must_use]
pub fn table_statistics(
    ctx: &crate::StmtContext,
    catalog: &tidb_infoschema::Catalog,
    table_id: i64,
) -> Option<std::sync::Arc<Table>> {
    ctx.stats_handle()
        .table_statistics(table_id, || catalog.statistics_schema(table_id))
}

/// Go `pkg/planner/core/stats.GetStatsTable`: the statistics one statement
/// plans `physical_table_id` from. `None` is a fresh `statistics.PseudoTable`
/// (`RealtimeCount` 10000, `ModifyCount` 0, uninitialized pseudo items).
///
/// The caller passes the id Go's `pid == tblInfo.ID ||
/// UseDynamicPartitionPrune()` choice resolves to: the logical id for an
/// ordinary or dynamic-pruning source, one partition's id for a
/// static-pruning child. Rule 1 (no statistics handle yet) has no
/// counterpart, since the catalog always answers; a table the cache does not
/// hold is the same `PseudoTable`.
///
/// The cached object is never mutated: the determinate-objective and pseudo
/// decisions are session-local, so they land on a copy as Go's `CopyAs` and
/// `tbl := *statsTbl` do.
#[must_use]
pub fn get_stats_table(
    ctx: &crate::StmtContext,
    catalog: &tidb_infoschema::Catalog,
    physical_table_id: i64,
) -> Option<std::sync::Arc<Table>> {
    let opt_objective_determinate = !ctx
        .optimizer_cost_env()
        .session
        .estimator_options
        .allow_use_modify_count;
    stats_table_for_session(
        table_statistics(ctx, catalog, physical_table_id)?,
        opt_objective_determinate,
        ctx.enable_pseudo_for_outdated_stats(),
    )
}

/// The session rules of [`get_stats_table`] over one cached table.
fn stats_table_for_session(
    mut statistics: std::sync::Arc<Table>,
    opt_objective_determinate: bool,
    enable_pseudo_for_outdated_stats: bool,
) -> Option<std::sync::Arc<Table>> {
    // `OptObjectiveDeterminate` ignores the real-time statistics: the copy's
    // `RealtimeCount` is the row count the loaded histograms observed at
    // ANALYZE time, and its `ModifyCount` is zero.
    if opt_objective_determinate {
        let analyze_count = statistics.hist_coll.analyze_row_count().max(0.0) as i64;
        if statistics.hist_coll.realtime_count != analyze_count
            || statistics.hist_coll.modify_count != 0
        {
            let mut copied = statistics.copy_as(tidb_stats::CopyIntent::MetaOnly);
            copied.hist_coll.realtime_count = analyze_count;
            copied.hist_coll.modify_count = 0;
            statistics = std::sync::Arc::new(copied);
        }
    }
    // Rule 2: a zero `RealtimeCount` plans from a fresh `PseudoTable`; none of
    // the cached objects survive.
    if statistics.hist_coll.realtime_count == 0 {
        return None;
    }
    // Rule 3: an uninitialized or (when enabled) outdated table plans from
    // Go's struct copy `tbl := *statsTbl` with `Pseudo` set. Outdatedness is
    // judged on the table this statement plans from, so the determinate
    // copy's zero `ModifyCount` is never outdated.
    if !statistics.is_initialized()
        || (enable_pseudo_for_outdated_stats && statistics.is_outdated())
    {
        let mut copied = (*statistics).clone();
        copied.hist_coll.pseudo = true;
        statistics = std::sync::Arc::new(copied);
    }
    Some(statistics)
}

/// Go `getLatestVersionFromStatsTable` (`logical_plan_builder.go`), the
/// plan-cache key's statistics version for one table: zero for any table
/// `GetStatsTable` answers with a pseudo table by rule 2, otherwise the
/// largest `LastUpdateVersion` among the cached column and index objects.
///
/// It is NOT the `stats_meta` version: a stats-delta flush moves that, and Go
/// keeps the cached plan across it.
#[must_use]
pub fn get_latest_version_from_stats_table(
    handle: &tidb_stats_handle::StatsHandle,
    catalog: &tidb_infoschema::Catalog,
    table_id: i64,
    opt_objective_determinate: bool,
) -> u64 {
    let Some(statistics) = handle.table_statistics(table_id, || catalog.statistics_schema(table_id))
    else {
        return 0;
    };
    let realtime_row_count = if opt_objective_determinate {
        statistics.hist_coll.analyze_row_count().max(0.0) as i64
    } else {
        statistics.hist_coll.realtime_count
    };
    if realtime_row_count == 0 {
        return 0;
    }
    let mut version = 0;
    statistics.hist_coll.for_each_column(|_, column| {
        version = version.max(column.histogram.last_update_version);
        false
    });
    statistics.hist_coll.for_each_index(|_, index| {
        version = version.max(index.histogram.last_update_version);
        false
    });
    version
}

/// The estimate one committed access path carries into `EXPLAIN`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ScanEstimate {
    /// Go's `estRows`: the rows the READ itself produces, before any filter
    /// above it.
    pub rows: f64,
    /// Whether these numbers came from `statistics.PseudoTable` rather than
    /// from loaded statistics -- what makes `EXPLAIN` print `stats:pseudo`.
    pub pseudo: bool,
}

/// The table's realtime row count: `stats_meta.count` when analyzed, Go's
/// `statistics.PseudoRowCount` when not.
pub(crate) fn realtime_row_count(stats: Option<&Table>) -> f64 {
    match stats {
        Some(stats) if stats.hist_coll.realtime_count > 0 => stats.hist_coll.realtime_count as f64,
        // A count of zero on a table with no histograms is not the statement
        // "this table is empty" -- it is `mysql.stats_meta` not having caught
        // up with the rows yet, and Go answers it with
        // `PseudoHistColl`'s `RealtimeCount: PseudoRowCount`
        // (`pkg/statistics/table.go`). Taking the zero literally makes every
        // path cost nothing and the choice arbitrary; a live playground shows
        // exactly that on a freshly loaded table, where Go still says 10000.
        //
        // An ANALYZED table that really is empty lands here too, because a
        // zero count is one of Go's own routes to `PseudoTable` -- see
        // [`get_stats_table`], where that rule lives.
        Some(stats) if stats.hist_coll.pseudo => tidb_stats::PSEUDO_ROW_COUNT as f64,
        Some(stats) => stats.hist_coll.realtime_count.max(0) as f64,
        None => tidb_stats::PSEUDO_ROW_COUNT as f64,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use tidb_planner_cardinality::ndv::estimate_column_ndv;
    use tidb_datatype::Datum;


    fn histogram_with_count(id: i64, ndv: i64, count: i64, version: u64) -> tidb_stats::Histogram {
        tidb_stats::Histogram {
            id,
            ndv,
            last_update_version: version,
            buckets: vec![tidb_stats::Bucket {
                count,
                repeat: 1,
                ndv,
                lower_bound: Datum::Int(1),
                upper_bound: Datum::Int(ndv),
            }],
            ..tidb_stats::Histogram::default()
        }
    }

    /// Go `Table.GetStatsHealthy` reads the cache object: Go's synthetic
    /// `PseudoTable` has no health, while an analyzed empty table is healthy.
    #[test]
    fn initialized_empty_stats_are_not_a_synthetic_cache_pseudo() {
        let synthetic = tidb_stats::pseudo_table(&Default::default(), false, true);
        assert_eq!(synthetic.stats_healthy(), (0, false));

        let mut analyzed_empty = Table::from_analyzed_items(
            1,
            0,
            0,
            [],
            [(1, tidb_stats::index::Index::default())],
        );
        assert_eq!(analyzed_empty.stats_healthy(), (0, true));
        analyzed_empty.last_analyze_version = 42;
        assert_eq!(analyzed_empty.stats_healthy(), (100, true));
    }

    /// Go `EstimateColumnNDV` reads the histogram NDV only when the column
    /// `IsStatsInitialized()`. A never-analyzed column a load request
    /// published is Go's `EmptyColumn` (zero NDV, zero load status), and the
    /// estimate falls back to `RealtimeCount * distinctFactor`.
    ///
    /// Captured live: on a never-analyzed table with `stats_meta` count 8,
    /// Go's `GROUP BY k` estimates 6.40 groups where the NDV of the
    /// placeholder made this engine estimate 1.00.
    #[test]
    fn estimate_column_ndv_skips_an_uninitialized_column() {
        let column = tidb_stats::column::Column {
            histogram: tidb_stats::Histogram {
                id: 3,
                ..tidb_stats::Histogram::default()
            },
            top_n: None,
            cmsketch: None,
            stats_version: 0,
            ..Default::default()
        };
        let stats = Table::from_analyzed_items(1, 8, 8, [(3, column)], []);
        let set_status = |status| {
            stats
                .hist_coll
                .get_column(3)
                .unwrap()
                .write()
                .unwrap()
                .stats_loaded_status = status;
        };
        set_status(tidb_stats::StatsLoadedStatus::default());
        assert_eq!(
            estimate_column_ndv(&stats, 3, &BTreeSet::new(), &BTreeSet::new()),
            None
        );
        set_status(tidb_stats::StatsLoadedStatus::full_load());
        assert_eq!(
            estimate_column_ndv(&stats, 3, &BTreeSet::new(), &BTreeSet::new()),
            Some(0.0)
        );
    }

    /// Go `HistColl.GetAnalyzeRowCount` skips a multi-valued index, whose
    /// histogram counts array elements rather than rows.
    #[test]
    fn analyze_row_count_skips_multi_valued_indexes() {
        let index = |id, count, mv_index| tidb_stats::index::Index {
            histogram: histogram_with_count(id, 1, count, 42),
            top_n: None,
            cmsketch: None,
            stats_version: 2,
            info: Some(tidb_model::GoShared::new(tidb_model::IndexInfo {
                columns: (vec![String::new(); 1])
                    .into_iter()
                    .map(|name: String| tidb_model::IndexColumn {
                        name: tidb_ast::CiString::new(name),
                        ..Default::default()
                    })
                    .collect::<Vec<_>>()
                    .into(),
                mv_index: mv_index,
                ..tidb_model::IndexInfo::default()
            })),
            ..Default::default()
        };
        let statistics = |first_is_mv| {
            Table::from_analyzed_items(
                1,
                4,
                0,
                [],
                [(1, index(1, 10, first_is_mv)), (2, index(2, 4, false))],
            )
        };
        assert_eq!(statistics(false).hist_coll.analyze_row_count(), 10.0);
        assert_eq!(statistics(true).hist_coll.analyze_row_count(), 4.0);
    }

    #[test]
    fn estimate_column_ndv_borrows_a_same_version_full_load_count() {
        let target_id = 1;
        let predicate_id = 2;
        let index_id = 3;
        let version = 42;
        let column = |id, ndv, count| tidb_stats::column::Column {
            histogram: histogram_with_count(id, ndv, count, version),
            top_n: None,
            cmsketch: None,
            stats_version: 2,
            ..Default::default()
        };
        let statistics = Table::from_analyzed_items(
            1,
            300_000,
            0,
            [
                (target_id, column(target_id, 3_000, 300_000)),
                (predicate_id, column(predicate_id, 10, 299_995)),
            ],
            [(
                index_id,
                tidb_stats::index::Index {
                    histogram: histogram_with_count(index_id, 10, 299_995, version),
                    top_n: None,
                    cmsketch: None,
                    stats_version: 2,
                    info: Some(tidb_model::GoShared::new(tidb_model::IndexInfo {
                        columns: (vec![String::new(); 1])
                            .into_iter()
                            .map(|name: String| tidb_model::IndexColumn {
                                name: tidb_ast::CiString::new(name),
                                ..Default::default()
                            })
                            .collect::<Vec<_>>()
                            .into(),
                        ..tidb_model::IndexInfo::default()
                    })),
                    ..Default::default()
                },
            )],
        );
        let full_columns = [predicate_id].into_iter().collect();
        let full_indexes = [index_id].into_iter().collect();
        let ndv = estimate_column_ndv(&statistics, target_id, &full_columns, &full_indexes).unwrap();
        assert!((ndv - 3000.0500008333474).abs() < 1e-12, "{ndv:.17}");

        let target_full = [target_id].into_iter().collect();
        assert_eq!(
            estimate_column_ndv(&statistics, target_id, &target_full, &BTreeSet::new()),
            Some(3000.0)
        );
    }

}
