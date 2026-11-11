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

//! Go `pkg/planner/core/stats.go`: a `DataSource`'s statistics and access
//! paths, which `logicalop.DataSource.DeriveStats` reaches through
//! `utilfuncp.DeriveStats4DataSource` ([`CoreDataSourceStats`]), and the
//! `cardinality.Selectivity` wrappers it estimates with.

pub mod init_stats;
#[cfg(test)]
mod access_path_context_tests;
#[cfg(test)]
mod derive_stats_tests;

use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_planner_cardinality::derive_stats::StatsInfoScale;
use tidb_planner_core_base::PlanError;
use tidb_planner_property::stats_info::StatsInfo;

use tidb_planner_core_operator_logicalop::data_source::DataSource;
use tidb_planner_core_operator_logicalop::rewrite::{DataSourceStatsDeriver, DeriveStatsEnv};

pub(crate) fn analyzed_filter_selectivity(
    table_stats: &StatsInfo,
    conditions: &[Expression],
) -> Option<f64> {
    analyzed_filter_selectivity_with_options(
        table_stats,
        conditions,
        &tidb_planner_cardinality::row_count_estimator::EstimatorOptions::default(),
    )
}

pub(crate) fn analyzed_filter_selectivity_with_options(
    table_stats: &StatsInfo,
    conditions: &[Expression],
    options: &tidb_planner_cardinality::row_count_estimator::EstimatorOptions,
) -> Option<f64> {
    analyzed_filter_selectivity_with_evaluator(
        table_stats,
        conditions,
        options,
        &tidb_ranger::points::evaluate_static,
    )
}

pub(crate) fn analyzed_filter_selectivity_in(
    table_stats: &StatsInfo,
    conditions: &[Expression],
    context: &crate::access_path::AccessPathDerivationContext<'_>,
    filled_paths: Option<&tidb_planner_cardinality::selectivity::FilledPaths>,
    record_used_item: Option<&dyn Fn(i64, bool)>,
) -> Option<f64> {
    // The derivation context carries the statement's own snapshot of these
    // session variables.
    let options = tidb_planner_cardinality::row_count_estimator::EstimatorOptions {
        selectivity_factor: context.selectivity_factor,
        range_max_size: context.range_max_size,
        opt_prefix_index_single_scan: context.opt_prefix_index_single_scan,
        ..context.estimator_options.clone()
    };
    filter_selectivity(
        table_stats,
        conditions,
        &tidb_planner_cardinality::selectivity::SelectivityContext {
            range_fallback_handler: context.range_fallback_handler,
            // The derivation context carries a marker only when the
            // statement uses the plan cache.
            use_plan_cache: context.plan_cache_marker.is_some(),
            plan_cache_marker: context.plan_cache_marker,
            record_used_item,
            ..tidb_planner_cardinality::selectivity::SelectivityContext::new(
                &options,
                context.expression_evaluator,
                &crate::access_path::mv_index::PlannerMvIndexFilters,
            )
        },
        filled_paths,
    )
}

/// Estimates expression predicates with the caller's prepared-parameter evaluator.
pub fn analyzed_filter_selectivity_with_evaluator(
    table_stats: &StatsInfo,
    conditions: &[Expression],
    options: &tidb_planner_cardinality::row_count_estimator::EstimatorOptions,
    evaluate: &tidb_ranger::points::ExpressionEvaluator<'_>,
) -> Option<f64> {
    filter_selectivity(
        table_stats,
        conditions,
        &tidb_planner_cardinality::selectivity::SelectivityContext::new(
            options,
            evaluate,
            &crate::access_path::mv_index::PlannerMvIndexFilters,
        ),
        None,
    )
}

/// Go `deriveStatsByFilter`'s `cardinality.Selectivity` over the source's
/// `HistColl`; an estimation error falls back to `cost.SelectionFactor`.
/// `None` only for a profile without a collection, which Go never builds
/// for a table.
pub fn filter_selectivity(
    table_stats: &StatsInfo,
    conditions: &[Expression],
    ctx: &tidb_planner_cardinality::selectivity::SelectivityContext<'_>,
    filled_paths: Option<&tidb_planner_cardinality::selectivity::FilledPaths>,
) -> Option<f64> {
    let hist_coll = table_stats.hist_coll()?;
    Some(
        tidb_planner_cardinality::selectivity::selectivity(
            ctx,
            hist_coll,
            conditions,
            filled_paths,
        )
        .unwrap_or(tidb_planner_core_cost::SELECTION_FACTOR),
    )
}

/// Go `deriveStats4DataSource`, registered as `utilfuncp.DeriveStats4DataSource`.
#[derive(Clone, Copy, Debug, Default)]
pub struct CoreDataSourceStats;

impl DataSourceStatsDeriver for CoreDataSourceStats {
    fn derive_stats_4_data_source(
        &self,
        op: &mut DataSource,
        self_schema: &Schema,
        source_had_stats: bool,
        env: &DeriveStatsEnv<'_>,
    ) -> Result<(StatsInfo, bool), PlanError> {
        let mut result = match op.table_stats.clone() {
            // Go `initStats` (`core/stats.go`) always attaches at least
            // the pseudo table before DeriveStats runs; a source with no
            // `table_stats` means the builder skipped that step.
            None => Err(PlanError::internal(
                "recursive_derive_stats: DataSource.table_stats is absent; \
                     Go's initStats attaches at least the pseudo table first",
            )),
            Some(table_stats) => {
                if let Some(stats) = op.base.base.stats_info().cloned() {
                    // Statistics the executor installed before this first
                    // derivation stand in for Go's `deriveStatsByFilter`,
                    // but Go's first `DeriveStats` also fixes each index
                    // path's `IsSingleScan` here -- at join reorder for a
                    // join, after the final column pruning otherwise.
                    let missing = op
                        .all_possible_access_paths
                        .iter()
                        .enumerate()
                        .filter(|(_, path)| path.is_single_scan.is_none())
                        .filter_map(|(position, path)| {
                            let index = path.index.as_ref()?;
                            Some((
                                position,
                                tidb_planner_core_operator_logicalop::data_source::index_path_is_single_scan(
                                    op,
                                    index,
                                    env.opt_prefix_index_single_scan,
                                ),
                            ))
                        })
                        .collect::<Vec<_>>();
                    for (position, single_scan) in missing {
                        op.all_possible_access_paths[position].is_single_scan = Some(single_scan);
                    }
                    Ok((stats, op.all_conds.is_empty()))
                } else {
                    // Go preprocesses pushed predicates before deriving
                    // ranges and selectivity, retaining casts in projections.
                    for condition in &mut op.pushed_down_conds {
                        *condition = tidb_expr::expr_util::eliminate_no_precision_loss_cast(
                            condition,
                            env.builder,
                        );
                    }
                    // Go `deriveStats4DataSource`: `ds.stats =
                    // deriveStatsByFilter(ds, ds.PushedDownConds, nil)`.
                    let schema_ids: Vec<i64> = self_schema
                        .columns
                        .iter()
                        .map(|col| col.unique_id)
                        .collect();
                    let resolve = |unique_id: i64| {
                        let position = schema_ids.iter().position(|id| *id == unique_id)?;
                        let column = op.columns.get(position)?;
                        Some(tidb_planner_property::stats_info::PseudoColumn {
                            lower_name: column.read().name.lowercase().to_owned(),
                            unique_key_flag: false,
                        })
                    };
                    let evaluate = tidb_ranger::points::StatementRangerEvaluator(env.eval_context);
                    let derivation = crate::access_path::AccessPathDerivationContext {
                        opt_prefix_index_single_scan: env.opt_prefix_index_single_scan,
                        expr_pushdown_blacklist: env.expr_pushdown_blacklist,
                        selectivity_factor: env.selectivity_factor,
                        estimator_options: env.estimator_options.clone(),
                        range_max_size: env.range_max_size,
                        range_fallback_handler: env.range_fallback_handler,
                        expression_evaluator: &evaluate,
                        plan_cache_marker: env.plan_cache_marker(),
                        used_stats_recorder: env
                            .rule_context
                            .and_then(|context| context.used_stats_recorder),
                        push_down_warning_sink: env
                            .rule_context
                            .and_then(|context| context.derive_stats_sink)
                            .map(|sink| sink as _),
                    };
                    let filled_paths = op.derive_stats_filled_paths();
                    // Go `deriveStatsByFilter`'s `Selectivity` reports the
                    // columns and indexes it reads to the statement's
                    // used-stats ledger.
                    let record_used_item = |item_id, is_index| {
                        op.record_used_item(derivation.used_stats_recorder, item_id, is_index);
                    };
                    let range_selectivity = analyzed_filter_selectivity_in(
                        &table_stats,
                        &op.pushed_down_conds,
                        &derivation,
                        Some(&filled_paths),
                        Some(&record_used_item),
                    );
                    let stats = range_selectivity.map_or_else(
                        || {
                            tidb_planner_cardinality::pseudo::derive_stats_by_filter(
                                &table_stats,
                                &op.pushed_down_conds,
                                &resolve,
                                &[],
                                tidb_planner_core_cost::SELECTION_FACTOR,
                                env.scale_ndv_skew_ratio,
                            )
                        },
                        |selectivity| table_stats.scale(selectivity, env.scale_ndv_skew_ratio),
                    );
                    // Go derivePathStatsAndTryHeuristics fixes IsSingleScan
                    // with the first stats profile. Join reorder can derive
                    // this before the final column-pruning pass.
                    for position in 0..op.all_possible_access_paths.len() {
                        let path = &op.all_possible_access_paths[position];
                        let is_single_scan = path.index.as_ref().map(|index| {
                            tidb_planner_core_operator_logicalop::data_source::index_path_is_single_scan(
                                op,
                                index,
                                env.opt_prefix_index_single_scan,
                            )
                        });
                        op.all_possible_access_paths[position].is_single_scan = is_single_scan;
                    }
                    op.base.base.set_stats(Some(stats.clone()));
                    Ok((stats, op.all_conds.is_empty()))
                }
            }
        };
        if result.is_ok() && (!source_had_stats || op.possible_access_paths.is_none()) {
            let evaluate = tidb_ranger::points::StatementRangerEvaluator(env.eval_context);
            let derivation = crate::access_path::AccessPathDerivationContext {
                opt_prefix_index_single_scan: env.opt_prefix_index_single_scan,
                expr_pushdown_blacklist: env.expr_pushdown_blacklist,
                selectivity_factor: env.selectivity_factor,
                estimator_options: env.estimator_options.clone(),
                range_max_size: env.range_max_size,
                range_fallback_handler: env.range_fallback_handler,
                expression_evaluator: &evaluate,
                plan_cache_marker: env.plan_cache_marker(),
                used_stats_recorder: env
                    .rule_context
                    .and_then(|context| context.used_stats_recorder),
                push_down_warning_sink: env
                    .rule_context
                    .and_then(|context| context.derive_stats_sink)
                    .map(|sink| sink as _),
            };
            // Go `derivePathStatsAndTryHeuristics`: the heuristic-pruned
            // paths become `ds.PossibleAccessPaths`.
            let derived = derive_path_stats_and_try_heuristics(op, &derivation, env).and_then(|()| {
                // Go `generateIndexMergePath` over the pruned paths.
                crate::access_path::generate_index_merge_path(
                    op,
                    &derivation,
                    env.index_merge_enabled,
                    env.rule_context
                        .is_some_and(|context| context.no_index_merge_hint),
                    env.use_plan_cache,
                    // Go fix control 52869, true when unset.
                    env.rule_context
                        .is_none_or(|context| context.index_merge_skip_range_scan_check),
                )
                .and_then(|merged| crate::access_path::clean_access_path_for_fts(op).map(|()| merged))
            });
            match derived {
                Ok((index_merge_max_count, warning)) => {
                    if let Some(warning) = warning {
                        env.eval_context.append_warning(1105, &warning);
                    }
                    // Go `generateIndexMergePath`: merges built from
                    // AllConds may see more filters than the stats
                    // derived from PushedDownConds, so the largest
                    // merge row count caps the DataSource's rows.
                    if let (Some(max_rows), Ok((stats, _)), Some(table_stats)) = (
                        index_merge_max_count,
                        result.as_mut(),
                        op.table_stats.as_ref(),
                    ) {
                        if stats.row_count() > max_rows {
                            let scaled =
                                table_stats.scale_by_expect_cnt(max_rows, env.scale_ndv_skew_ratio);
                            op.base.base.set_stats(Some(scaled.clone()));
                            *stats = scaled;
                        }
                    }
                    let paths = op.possible_paths().unwrap_or_default();
                    // Go `deriveStats4DataSource`: the access paths left
                    // after the heuristics and the index merges, counted for
                    // RU v2.
                    if let Some(metrics) =
                        env.rule_context.and_then(|context| context.ruv2_metrics.as_ref())
                    {
                        metrics.add_plan_derive_stats_paths(paths.len() as i64);
                    }
                    let (min_selectivity, index_force) = get_general_attributes_from_paths(
                        &paths,
                        op.table_stats.as_ref().map_or(0.0, StatsInfo::row_count),
                    );
                    op.access_path_min_selectivity = min_selectivity;
                    if index_force {
                        if let Some(sink) =
                            env.rule_context.and_then(|context| context.derive_stats_sink)
                        {
                            sink.set_index_force();
                        }
                    }
                }
                Err(error) => result = Err(error),
            }
        }
        result
    }
}

/// Go `getGeneralAttributesFromPaths`: the smallest fraction of
/// `total_row_count` a path reads (a table or finished index-merge path's
/// `CountAfterAccess`, an index or unfinished OR index-merge path's
/// `CountAfterIndex`), and whether any path is forced.
fn get_general_attributes_from_paths(
    paths: &[&tidb_planner_util::path::AccessPath],
    total_row_count: f64,
) -> (f64, bool) {
    let mut min_selectivity = 1.0_f64;
    let mut index_force = false;
    for path in paths {
        if total_row_count > 0.0 {
            let rows = if path.is_table_path() {
                path.count_after_access()
            } else if !path.partial_index_paths.is_empty() {
                Some(path.count_after_access().unwrap_or(0.0))
            } else if !path.partial_alternative_index_paths.is_empty() {
                // Go's unfinished OR path has no CountAfterIndex yet.
                Some(0.0)
            } else {
                path.count_after_index.or_else(|| path.count_after_access())
            };
            if let Some(rows) = rows {
                min_selectivity = min_selectivity.min(rows / total_row_count);
            }
        }
        index_force |= path.forced;
    }
    (min_selectivity, index_force)
}

/// Go `AccessPath.OnlyPointRange`.
fn only_point_range(path: &tidb_planner_util::path::AccessPath) -> bool {
    if path.is_int_handle_path {
        return path.ranges.iter().all(tidb_ranger::Range::is_point_nullable);
    }
    let columns = path.index.as_ref().map_or(0, |index| index.read().columns.len());
    path.ranges
        .iter()
        .all(|range| range.is_point_non_nullable() && range.high_val.len() == columns)
}

/// Go `derivePathStatsAndTryHeuristics` (`stats.go`): derives each path's
/// statistics in `AllPossibleAccessPaths` order and stops at the first path
/// a heuristic selects outright -- one with empty ranges, or a unique point
/// path that is a single scan. Otherwise the unique point path with the
/// fewest ranges is selected, unless a single-scan path strictly refining it
/// needs fewer than twice its ranges. A selection prunes
/// `ds.PossibleAccessPaths` to that path (and the TiFlash path, which a
/// TiFlash preference keeps without a note); with none, every path stays.
fn derive_path_stats_and_try_heuristics(
    ds: &mut DataSource,
    context: &crate::access_path::AccessPathDerivationContext<'_>,
    env: &DeriveStatsEnv<'_>,
) -> Result<(), PlanError> {
    use tidb_planner_core_operator_logicalop::data_source::{PossibleAccessPath, PREFER_TIFLASH};
    use crate::access_path::ordinary::{derive_index_path_stats, fill_table_path};
    let derive_table_path_stats = |ds: &mut DataSource, position: usize| -> Result<(), PlanError> {
        let mut path = ds.all_possible_access_paths[position].clone();
        fill_table_path(ds, &mut path, context)?;
        ds.all_possible_access_paths[position] = path;
        Ok(())
    };
    let mut unique_idxs_with_double_scan = Vec::new();
    let mut single_scan_idxs = Vec::new();
    let mut selected = None;
    let mut is_refined_path = false;
    // step1: a TiFlash preference builds the TiFlash path ahead.
    let mut tiflash_path = None;
    let is_mpp_enforced = env.rule_context.is_some_and(|context| context.mpp_enforced);
    let prefer_tiflash = ds.prefer_store_type & PREFER_TIFLASH != 0;
    if (prefer_tiflash || is_mpp_enforced) && ds.has_tiflash() {
        if let Some(position) = ds
            .all_possible_access_paths
            .iter()
            .position(tidb_planner_util::path::AccessPath::is_tiflash_path)
        {
            derive_table_path_stats(ds, position)?;
            tiflash_path = Some(position);
        }
    }
    // step2: the kv paths follow the heuristic rules.
    for position in 0..ds.all_possible_access_paths.len() {
        let single_scan = if ds.all_possible_access_paths[position].is_table_path() {
            derive_table_path_stats(ds, position)?;
            true
        } else {
            derive_index_path_stats(ds, position, context, env.opt_prefix_index_single_scan)?;
            let path = &ds.all_possible_access_paths[position];
            path.is_single_scan.unwrap_or_else(|| {
                tidb_planner_core_operator_logicalop::data_source::index_path_is_single_scan(
                    ds,
                    path.index.as_ref().expect("an index path reads its index"),
                    env.opt_prefix_index_single_scan,
                )
            })
        };
        // step3: the heuristic rules, the TiFlash path's point range
        // included.
        let path = &ds.all_possible_access_paths[position];
        if path.ranges.is_empty() {
            selected = Some((position, single_scan));
            break;
        }
        if only_point_range(path) {
            if path.is_table_path() || path.index.as_ref().is_some_and(|index| index.read().unique) {
                if single_scan {
                    selected = Some((position, single_scan));
                    break;
                }
                unique_idxs_with_double_scan.push((position, single_scan));
            }
        } else if single_scan {
            single_scan_idxs.push((position, single_scan));
        }
    }
    if selected.is_none() && !unique_idxs_with_double_scan.is_empty() {
        let paths = &ds.all_possible_access_paths;
        let mut unique_idx_access_cols = Vec::with_capacity(unique_idxs_with_double_scan.len());
        // The unique path with the fewest ranges; among equals, the one
        // with the fewest table filters.
        let mut unique_best: Option<(usize, bool)> = None;
        for &(position, single_scan) in &unique_idxs_with_double_scan {
            let unique_idx = &paths[position];
            unique_idx_access_cols.push(crate::find_best_task::candidate::get_col2_len_from_access_conds(unique_idx));
            if unique_best.is_none_or(|(best, _)| {
                unique_idx.ranges.len() < paths[best].ranges.len()
                    || (unique_idx.ranges.len() == paths[best].ranges.len()
                        && unique_idx.table_filters.len() < paths[best].table_filters.len())
            }) {
                unique_best = Some((position, single_scan));
            }
        }
        // A single-scan path whose access columns strictly cover a unique
        // path's is a refined one; the refined path with the fewest ranges.
        let mut refined_best: Option<(usize, bool)> = None;
        for &(position, single_scan) in &single_scan_idxs {
            let col2len = crate::find_best_task::candidate::get_col2_len_from_access_conds(&paths[position]);
            for unique_idx_col2len in &unique_idx_access_cols {
                if tidb_planner_util::column_length::compare_col2_len(&col2len, unique_idx_col2len)
                    == (1, true)
                    && refined_best
                        .is_none_or(|(best, _)| paths[position].ranges.len() < paths[best].ranges.len())
                {
                    refined_best = Some((position, single_scan));
                }
            }
        }
        // The refined path wins only below twice the unique path's ranges.
        match (refined_best, unique_best) {
            (Some(refined), unique)
                if unique.is_none_or(|(unique, _)| {
                    paths[refined.0].ranges.len() < 2 * paths[unique].ranges.len()
                }) =>
            {
                selected = Some(refined);
                is_refined_path = true;
            }
            (_, unique) => selected = unique,
        }
    }
    let Some((selected, single_scan)) = selected else {
        ds.possible_access_paths = Some(
            (0..ds.all_possible_access_paths.len())
                .map(PossibleAccessPath::Shared)
                .collect(),
        );
        return Ok(());
    };
    // The heuristic prunes only this DataSource's `PossibleAccessPaths`.
    let mut possible = vec![PossibleAccessPath::Shared(selected)];
    // A TiFlash read the user asked for keeps the TiFlash path next to a
    // selected TiKV one, once it was built.
    let path = &ds.all_possible_access_paths[selected];
    let keep = (prefer_tiflash || is_mpp_enforced) && !path.is_tiflash_path();
    if let (true, Some(tiflash_path)) = (keep, tiflash_path) {
        possible.push(PossibleAccessPath::Shared(tiflash_path));
        ds.possible_access_paths = Some(possible);
        return Ok(());
    }
    let table_name = ds
        .table_as_name
        .clone()
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| ds.table_name());
    let note = if path.is_table_path() {
        format!("handle of {table_name} is selected since the path only has point ranges")
    } else {
        let index = path.index.as_ref().expect("an index path reads its index").read();
        format!(
            "{}index {} of {table_name} is selected since the path {}{}",
            if index.unique { "unique " } else { "" },
            index.name.original(),
            if is_refined_path {
                "only fetches limited number of rows"
            } else {
                "only has point ranges"
            },
            if single_scan { " with single scan" } else { " with double scan" },
        )
    };
    if let Some(sink) = env.rule_context.and_then(|context| context.derive_stats_sink) {
        sink.append_planner_note(&note);
    }
    ds.possible_access_paths = Some(possible);
    Ok(())
}

#[cfg(test)]
mod analyzed_filter_selectivity_tests {
    /// Go `HistColl` holding one column object with only its `Info` (no
    /// analyzed statistics), as `PseudoTable` holds a public column.
    fn info_only_hist_coll(
        realtime_count: i64,
        unique_id: i64,
        name: &str,
    ) -> std::sync::Arc<tidb_planner_property::stats_info::HistColl> {
        let column = tidb_stats::column::Column {
            info: Some(tidb_model::GoShared::new(tidb_model::ColumnInfo {
                name: tidb_ast::CiString::new(name),
                ..tidb_model::ColumnInfo::default()
            })),
            ..tidb_stats::column::Column::default()
        };
        std::sync::Arc::new(tidb_planner_property::stats_info::HistColl::new_hist_coll_with_cols_and_idxs(
            0,
            realtime_count,
            0,
            [(unique_id, column)],
            [],
        ))
    }

    use super::{analyzed_filter_selectivity, analyzed_filter_selectivity_with_options};
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};
    use tidb_expr::column::Column;
    use tidb_expr::constant::Constant;
    use tidb_expr::expression::Expression;
    use tidb_expr::scalar_function::ScalarFunction;
    use tidb_planner_cardinality::row_count_estimator::EstimatorOptions;
    use tidb_planner_property::stats_info::{HistCollExt as _, StatsInfo};
    use tidb_stats::{Bucket, Histogram};

    #[test]
    fn invalid_column_bounds_fall_back_once_for_the_complete_filter() {
        let histogram = Histogram {
            ndv: 10,
            buckets: vec![Bucket {
                count: 100,
                repeat: 10,
                ndv: 10,
                lower_bound: Datum::Int(1),
                upper_bound: Datum::Int(10),
            }],
            ..Histogram::default()
        };
        let column_stats = std::sync::Arc::new(tidb_stats::column::Column {
            histogram,
            top_n: None,
            cmsketch: None,
            stats_version: 2,
            ..Default::default()
        });
        let stats = StatsInfo::new(100.0, [(7, 10.0), (8, 10.0)]).with_hist_coll(
            tidb_planner_property::stats_info::loaded_hist_coll_for_test(false, 100, 0, [(7, std::sync::Arc::unwrap_or_clone(column_stats.clone())), (8, std::sync::Arc::unwrap_or_clone(column_stats))], []),
        );
        let scalar = |name: &str, args| {
            tidb_expr::new_function_base(
                &tidb_exprctx::PlanCacheParams(&[tidb_datatype::Datum::Int(0), tidb_datatype::Datum::Int(0)]),
                name,
                FieldType::new(FieldTypeCode::Tiny),
                args,
            ).unwrap()
        };
        // Table columns carry their `ColumnInfo.ID`, which `Selectivity`
        // sorts and deduplicates on.
        let column = |id| {
            let mut column = Column::new(id, FieldType::new(FieldTypeCode::LongLong));
            column.id = id;
            column
        };
        let equality = |id, datum| {
            scalar(
                "eq",
                vec![
                    Expression::Column(column(id)),
                    Expression::Constant(Constant::new(
                        datum,
                        FieldType::new(FieldTypeCode::LongLong),
                    )),
                ],
            )
        };
        let valid = equality(7, Datum::Int(1));
        // An `IN` constant that cannot be evaluated fails range construction
        // (`points.go:641-644`); a comparison would only build no point.
        let invalid = {
            let mut missing = Constant::new(Datum::Null, FieldType::new(FieldTypeCode::LongLong));
            missing.param_marker = Some(tidb_expr::constant::ParamMarker { order: 0 });
            scalar(
                "in",
                vec![Expression::Column(column(8)), Expression::Constant(missing)],
            )
        };
        let cnf = vec![valid.clone(), invalid.clone()];
        let options = EstimatorOptions::default();
        let ctx = tidb_planner_cardinality::selectivity::SelectivityContext::new(
            &options,
            &tidb_ranger::points::evaluate_static,
            &crate::access_path::mv_index::PlannerMvIndexFilters,
        );
        let hist = stats.hist_coll().unwrap();
        let estimate = |exprs: &[Expression]| {
            tidb_planner_cardinality::selectivity::selectivity(&ctx, hist, exprs, None)
        };
        assert!(estimate(&cnf).is_err());
        assert_eq!(analyzed_filter_selectivity(&stats, &cnf), Some(0.8));
        let dnf = scalar(
            "or",
            vec![
                scalar("and", cnf),
                scalar(
                    "and",
                    vec![equality(7, Datum::Int(2)), equality(8, Datum::Int(2))],
                ),
            ],
        );
        // Inside a DNF, Go swallows a failing item's error and charges it
        // `SelectivityFactor` (`selectivity.go:365-368`), so the DNF still
        // estimates and only a top-level failure falls back.
        let dnf_selectivity = estimate(std::slice::from_ref(&dnf)).unwrap();
        let valid_selectivity = estimate(std::slice::from_ref(&valid)).unwrap();
        assert_eq!(
            analyzed_filter_selectivity(&stats, &[valid, dnf]),
            Some(valid_selectivity * dnf_selectivity)
        );
    }

    /// Go's string-match regression protects a cached histogram from a
    /// transient vector-evaluation selection. Rust histograms contain no
    /// shared chunk-selection state, so the equivalent contract is that LIKE
    /// estimation returns the same 2/3 sample estimate without changing the
    /// shared histogram buckets.
    #[test]
    fn string_match_estimation_does_not_mutate_shared_histogram() {
        let histogram = Histogram {
            id: 1,
            ndv: 6,
            buckets: ["BRASS", "IRON", "STEEL"]
                .into_iter()
                .enumerate()
                .map(|(index, value)| Bucket {
                    count: (index as i64 + 1) * 2,
                    repeat: 1,
                    ndv: 1,
                    lower_bound: Datum::Bytes(value.as_bytes().to_vec()),
                    upper_bound: Datum::Bytes(value.as_bytes().to_vec()),
                })
                .collect(),
            ..Histogram::default()
        };
        let histogram_before = histogram.clone();
        let column_stats = std::sync::Arc::new(tidb_stats::column::Column {
            histogram,
            top_n: None,
            cmsketch: None,
            stats_version: 2,
            ..Default::default()
        });
        let stats = StatsInfo::new(6.0, [(1, 3.0)]).with_hist_coll(
            tidb_planner_property::stats_info::loaded_hist_coll_for_test(false, 6, 0, [(1, std::sync::Arc::unwrap_or_clone(column_stats.clone()))], []),
        );
        let varchar = FieldType::new(FieldTypeCode::Varchar);
        let escape_type = FieldType::new(FieldTypeCode::LongLong);
        let like = tidb_expr::new_function_base(
            &tidb_exprctx::NoColumns,
            "like",
            FieldType::new(FieldTypeCode::Tiny),
            vec![
                Expression::Column(Column::new(1, varchar.clone())),
                Expression::Constant(Constant::new(Datum::Bytes(b"%R%".to_vec()), varchar)),
                Expression::Constant(Constant::new(Datum::Int(i64::from(b'\\')), escape_type)),
            ],
        ).unwrap();

        let selectivity = analyzed_filter_selectivity(&stats, &[like]).unwrap();
        assert!((selectivity - 2.0 / 3.0).abs() < 1e-12);
        assert_eq!(column_stats.histogram, histogram_before);
    }

    #[test]
    fn range_with_metadata_only_statistics_uses_range_estimator() {
        let rows = 6_001_215;
        // The column is in `HistColl.Columns` without buckets; an EMPTY
        // collection would take `pseudoSelectivity` instead.
        let stats = StatsInfo::new(rows as f64, [(7, 1_500_000.0)]).with_hist_coll(
            info_only_hist_coll(rows, 7, "l_orderkey"),
        );
        let condition = tidb_expr::new_function_base(
            &tidb_exprctx::NoColumns,
            "lt",
            FieldType::new(FieldTypeCode::Tiny),
            vec![
                Expression::Column(Column::new(7, FieldType::new(FieldTypeCode::LongLong))),
                Expression::Constant(Constant::new(
                    Datum::Int(100_000),
                    FieldType::new(FieldTypeCode::LongLong),
                )),
            ],
        ).unwrap();
        let selectivity = analyzed_filter_selectivity(&stats, &[condition]).unwrap();
        let expected = (rows as f64 / 3.0 - rows as f64 / 1_000.0) / rows as f64;
        assert_eq!(
            selectivity, expected,
            "Go excludes pseudo NULL rows from the open lower bound"
        );
    }

    #[test]
    fn range_estimation_uses_the_session_risk_and_objective_options() {
        let unique_id = 7;
        let histogram = Histogram {
            id: 1,
            ndv: 10,
            last_update_version: 1,
            buckets: vec![Bucket {
                count: 100,
                repeat: 10,
                ndv: 10,
                lower_bound: Datum::Int(1),
                upper_bound: Datum::Int(10),
            }],
            ..Histogram::default()
        };
        let hist_coll = tidb_planner_property::stats_info::loaded_hist_coll_for_test(false, 1_000, 2_000, [(unique_id, tidb_stats::column::Column {
                    histogram,
                    top_n: None,
                    cmsketch: None,
                    stats_version: 2,
                    ..Default::default()
                })], []);
        let table_stats = StatsInfo::new(1_000.0, [(unique_id, 10.0)]).with_hist_coll(hist_coll);
        let long = || FieldType::new(FieldTypeCode::LongLong);
        let compare = |name: &str, value| {
            tidb_expr::new_function_base(
                &tidb_exprctx::NoColumns,
                name,
                long(),
                vec![
                    Expression::Column(Column::new(unique_id, long())),
                    Expression::Constant(Constant::new(Datum::Int(value), long())),
                ],
            ).unwrap()
        };
        let conditions = [compare("gt", 12), compare("lt", 15)];
        let baseline = analyzed_filter_selectivity_with_options(
            &table_stats,
            &conditions,
            &EstimatorOptions::default(),
        )
        .unwrap();
        let risk_adjusted = analyzed_filter_selectivity_with_options(
            &table_stats,
            &conditions,
            &EstimatorOptions {
                risk_range_skew_ratio: 0.5,
                ..EstimatorOptions::default()
            },
        )
        .unwrap();
        let determinate = analyzed_filter_selectivity_with_options(
            &table_stats,
            &conditions,
            &EstimatorOptions {
                risk_range_skew_ratio: 0.5,
                allow_use_modify_count: false,
                ..EstimatorOptions::default()
            },
        )
        .unwrap();

        assert!(risk_adjusted > baseline);
        assert_ne!(risk_adjusted, determinate);
    }

    /// Go `cardinality.Selectivity`'s equality arm reads the loaded
    /// histogram: when the value is a bucket's upper bound the estimate is
    /// that bucket's `Repeat`, not `1/NDV`. The rule used to ignore the
    /// histogram and divide by the NDV, so a 299,995-row sampled histogram
    /// with a 29,702-value repeat estimated 0.1 instead of 0.099008.
    #[test]
    fn equality_uses_the_loaded_histogram_repeat() {
        let unique_id = 7;
        let histogram = Histogram {
            id: 1,
            ndv: 10,
            last_update_version: 1,
            buckets: vec![Bucket {
                count: 299_995,
                repeat: 29_702,
                ndv: 10,
                lower_bound: Datum::Int(1),
                upper_bound: Datum::Int(1),
            }],
            ..Histogram::default()
        };
        let hist_coll = tidb_planner_property::stats_info::loaded_hist_coll_for_test(false, 300_000, 0, [(unique_id, tidb_stats::column::Column {
                    histogram,
                    top_n: None,
                    cmsketch: None,
                    stats_version: 2,
                    ..Default::default()
                })], []);
        let table_stats = StatsInfo::new(300_000.0, [(unique_id, 10.0)]).with_hist_coll(hist_coll);

        let long = || FieldType::new(FieldTypeCode::LongLong);
        let condition = tidb_expr::new_function_base(
            &tidb_exprctx::NoColumns,
            "eq",
            long(),
            vec![
                Expression::Column(Column::new(unique_id, long())),
                Expression::Constant(Constant::new(Datum::Int(1), long())),
            ],
        ).unwrap();
        let selectivity = analyzed_filter_selectivity(&table_stats, &[condition])
            .expect("an analyzed profile keeps the equality");

        // 29_702 * (300_000 / 299_995) / 300_000.
        let expected = 29_702.0 * (300_000.0 / 299_995.0) / 300_000.0;
        assert!(
            (selectivity - expected).abs() < 1e-12,
            "{selectivity} != {expected}"
        );
        assert!(
            (selectivity - 0.1).abs() > 1e-9,
            "the NDV fallback must not answer a loaded histogram"
        );
    }

    /// Go `Selectivity` builds one range set per column from every
    /// condition on it: `a >= 2 AND a < 3` is estimated as the single range
    /// `[2, 3)` against the histogram, not as the two conditions' estimates
    /// multiplied as if they were independent.
    #[test]
    fn two_range_conditions_on_one_column_estimate_as_one_range() {
        let unique_id = 7;
        let histogram = Histogram {
            id: 1,
            ndv: 10,
            last_update_version: 1,
            buckets: vec![
                Bucket {
                    count: 30,
                    repeat: 30,
                    ndv: 1,
                    lower_bound: Datum::Int(1),
                    upper_bound: Datum::Int(1),
                },
                Bucket {
                    count: 100,
                    repeat: 70,
                    ndv: 1,
                    lower_bound: Datum::Int(2),
                    upper_bound: Datum::Int(2),
                },
                Bucket {
                    count: 1_000,
                    repeat: 100,
                    ndv: 8,
                    lower_bound: Datum::Int(3),
                    upper_bound: Datum::Int(10),
                },
            ],
            ..Histogram::default()
        };
        let hist_coll = tidb_planner_property::stats_info::loaded_hist_coll_for_test(false, 1_000, 0, [(unique_id, tidb_stats::column::Column {
                    histogram,
                    top_n: None,
                    cmsketch: None,
                    stats_version: 2,
                    ..Default::default()
                })], []);
        let table_stats = StatsInfo::new(1_000.0, [(unique_id, 10.0)]).with_hist_coll(hist_coll);
        let long = || FieldType::new(FieldTypeCode::LongLong);
        let compare = |name: &str, value: i64| {
            tidb_expr::new_function_base(
                &tidb_exprctx::NoColumns,
                name,
                long(),
                vec![
                    Expression::Column(Column::new(unique_id, long())),
                    Expression::Constant(Constant::new(Datum::Int(value), long())),
                ],
            ).unwrap()
        };
        let joint =
            analyzed_filter_selectivity(&table_stats, &[compare("ge", 2), compare("lt", 3)])
                .expect("an analyzed profile keeps the ranges");
        let ge_alone = analyzed_filter_selectivity(&table_stats, &[compare("ge", 2)]).unwrap();
        let lt_alone = analyzed_filter_selectivity(&table_stats, &[compare("lt", 3)]).unwrap();
        // The estimator's own answer for the one range the ranger builds.
        let hist = table_stats.hist_coll().unwrap();
        let one_range =
            tidb_planner_cardinality::row_count_estimator::get_row_count_by_column_ranges(
                hist.histogram(unique_id)
                    .as_ref()
                    .map(|stats| stats.read().unwrap())
                    .as_deref(),
                &[
                    tidb_planner_cardinality::row_count_estimator::ColumnRange::new(
                        Datum::Int(2),
                        Datum::Int(3),
                        false,
                        true,
                    ),
                ],
                long().collation(),
                hist.realtime_count,
                hist.modify_count,
                false,
                &tidb_planner_cardinality::row_count_estimator::EstimatorOptions::default(),
            )
            .unwrap()
            .est / 1_000.0;
        assert!(
            (joint - one_range).abs() < 1e-12,
            "{joint} != {one_range} (the [2, 3) range)"
        );
        assert!(
            (joint - ge_alone * lt_alone).abs() > 1e-6,
            "{joint} is the independent product {ge_alone} * {lt_alone}"
        );
    }

    /// Same shape as
    /// [`two_range_conditions_on_one_column_estimate_as_one_range`] but over
    /// a `DATE` column with a v2 histogram plus TopN, reproducing TPC-H
    /// Q15's `l_shipdate >= '1996-01-01' AND l_shipdate < '1996-04-01'`
    /// window against `lineitem`'s real ANALYZEd stats.
    #[test]
    fn two_range_conditions_on_one_date_column_estimate_as_one_range() {
        use tidb_datatype::{CoreTime, Time, TimeType};

        let unique_id = 9;
        let date = |y: u16, m: u8, d: u8| {
            Datum::Time(
                Time::new(CoreTime::from_date(y, m, d, 0, 0, 0, 0), TimeType::Date, 0).unwrap(),
            )
        };
        let histogram = Histogram {
            id: 1,
            ndv: 3,
            last_update_version: 1,
            buckets: vec![
                Bucket {
                    count: 3_349_756,
                    repeat: 2_647,
                    ndv: 0,
                    lower_bound: date(1995, 12, 31),
                    upper_bound: date(1996, 1, 9),
                },
                Bucket {
                    count: 3_565_968,
                    repeat: 2_431,
                    ndv: 0,
                    lower_bound: date(1996, 4, 1),
                    upper_bound: date(1996, 4, 9),
                },
            ],
            ..Histogram::default()
        };
        // The TopN rows Q15's window crosses (real lineitem values), so the
        // fix's grouped estimate is checked against the same histogram +
        // TopN sum the production estimator computes.
        let mut topn = tidb_stats::cmsketch::TopN::new(3);
        let encode =
            |value: &Datum| tidb_codec::encode_key(std::slice::from_ref(value)).unwrap_or_default();
        topn.append(&encode(&date(1996, 1, 20)), 3_450);
        topn.append(&encode(&date(1996, 2, 16)), 3_669);
        topn.append(&encode(&date(1996, 3, 2)), 3_340);
        topn.sort();
        let hist_coll = tidb_planner_property::stats_info::loaded_hist_coll_for_test(false, 6_001_215, 0, [(unique_id, tidb_stats::column::Column {
                    histogram,
                    top_n: Some(topn),
                    cmsketch: None,
                    stats_version: 2,
                    ..Default::default()
                })], []);
        let table_stats =
            StatsInfo::new(6_001_215.0, [(unique_id, 2_526.0)]).with_hist_coll(hist_coll);
        let date_type = || FieldType::new(FieldTypeCode::Date);
        let compare = |name: &str, value: Datum| {
            tidb_expr::new_function_base(
                &tidb_exprctx::NoColumns,
                name,
                date_type(),
                vec![
                    Expression::Column(Column::new(unique_id, date_type())),
                    Expression::Constant(Constant::new(value, date_type())),
                ],
            ).unwrap()
        };
        let joint = analyzed_filter_selectivity(
            &table_stats,
            &[
                compare("ge", date(1996, 1, 1)),
                compare("lt", date(1996, 4, 1)),
            ],
        )
        .expect("an analyzed profile keeps the ranges");
        let ge_alone =
            analyzed_filter_selectivity(&table_stats, &[compare("ge", date(1996, 1, 1))]).unwrap();
        let lt_alone =
            analyzed_filter_selectivity(&table_stats, &[compare("lt", date(1996, 4, 1))]).unwrap();
        let hist = table_stats.hist_coll().unwrap();
        let one_range =
            tidb_planner_cardinality::row_count_estimator::get_row_count_by_column_ranges(
                hist.histogram(unique_id)
                    .as_ref()
                    .map(|stats| stats.read().unwrap())
                    .as_deref(),
                &[
                    tidb_planner_cardinality::row_count_estimator::ColumnRange::new(
                        date(1996, 1, 1),
                        date(1996, 4, 1),
                        false,
                        true,
                    ),
                ],
                date_type().collation(),
                hist.realtime_count,
                hist.modify_count,
                false,
                &tidb_planner_cardinality::row_count_estimator::EstimatorOptions::default(),
            )
            .unwrap()
            .est / 6_001_215.0;
        assert!(
            (joint - one_range).abs() < 1e-9,
            "{joint} != {one_range} (the [1996-01-01, 1996-04-01) range)"
        );
        assert!(
            (joint - ge_alone * lt_alone).abs() > 1e-6,
            "{joint} is the independent product {ge_alone} * {lt_alone} \
             (this is TPC-H Q15's bug: 27% of lineitem instead of ~3.8%)"
        );
    }

    /// Go `Selectivity` runs a single-column DNF through the ranger: two
    /// equalities under `OR` become two point ranges against the histogram.
    /// The rule used to answer any `OR` with the 0.8 default, which is what
    /// flipped TPC-H Q7's build side (see the ExecPlan).
    #[test]
    fn a_single_column_dnf_estimates_from_point_ranges() {
        let unique_id = 7;
        let histogram = Histogram {
            id: 1,
            ndv: 10,
            last_update_version: 1,
            // Bucket counts are cumulative: 30 rows of 1, 70 of 2, 900 of
            // the rest, 1,000 in all.
            buckets: vec![
                Bucket {
                    count: 30,
                    repeat: 30,
                    ndv: 1,
                    lower_bound: Datum::Int(1),
                    upper_bound: Datum::Int(1),
                },
                Bucket {
                    count: 100,
                    repeat: 70,
                    ndv: 1,
                    lower_bound: Datum::Int(2),
                    upper_bound: Datum::Int(2),
                },
                Bucket {
                    count: 1_000,
                    repeat: 100,
                    ndv: 8,
                    lower_bound: Datum::Int(3),
                    upper_bound: Datum::Int(10),
                },
            ],
            ..Histogram::default()
        };
        let hist_coll = tidb_planner_property::stats_info::loaded_hist_coll_for_test(false, 1_000, 0, [(unique_id, tidb_stats::column::Column {
                    histogram,
                    top_n: None,
                    cmsketch: None,
                    stats_version: 2,
                    ..Default::default()
                })], []);
        let table_stats = StatsInfo::new(1_000.0, [(unique_id, 10.0)]).with_hist_coll(hist_coll);
        let long = || FieldType::new(FieldTypeCode::LongLong);
        let eq = |value: i64| {
            tidb_expr::new_function_base(
                &tidb_exprctx::NoColumns,
                "eq",
                long(),
                vec![
                    Expression::Column(Column::new(unique_id, long())),
                    Expression::Constant(Constant::new(Datum::Int(value), long())),
                ],
            ).unwrap()
        };
        let dnf = tidb_expr::new_function_base(
            &tidb_exprctx::NoColumns,
            "or",
            FieldType::new(FieldTypeCode::Tiny),
            vec![eq(1), eq(2)],
        ).unwrap();
        let selectivity =
            analyzed_filter_selectivity(&table_stats, &[dnf]).expect("an analyzed profile");
        assert!(
            (selectivity - tidb_planner_core_cost::SELECTION_FACTOR).abs() > 1e-9,
            "a single-column OR must not take the default: {selectivity}"
        );
        // Both values are bucket bounds, so the estimate is their repeats:
        // (30 + 70) / 1000.
        assert!((selectivity - 0.1).abs() < 1e-9, "{selectivity} != 0.1");
    }

    /// Go `GetSelectivityByFilter` -> `GetStrMatchSelectivity`: a plain
    /// `prefix%` LIKE estimates from the histogram's bucket bounds, and a
    /// pattern the histogram cannot answer falls back to
    /// `GetStrMatchDefaultSelectivity` (0.1), never the generic 0.8.
    #[test]
    fn a_prefix_like_uses_the_histogram_then_the_string_match_default() {
        let unique_id = 7;
        let varchar = || FieldType::new(FieldTypeCode::VarString);
        let histogram = Histogram {
            id: 1,
            ndv: 3,
            last_update_version: 1,
            buckets: vec![
                Bucket {
                    count: 1,
                    repeat: 1,
                    ndv: 1,
                    lower_bound: Datum::Bytes(b"green alpha".to_vec()),
                    upper_bound: Datum::Bytes(b"green alpha".to_vec()),
                },
                Bucket {
                    count: 101,
                    repeat: 1,
                    ndv: 100,
                    lower_bound: Datum::Bytes(b"red 002".to_vec()),
                    upper_bound: Datum::Bytes(b"red 101".to_vec()),
                },
            ],
            ..Histogram::default()
        };
        let hist_coll = tidb_planner_property::stats_info::loaded_hist_coll_for_test(false, 128, 0, [(unique_id, tidb_stats::column::Column {
                    histogram,
                    top_n: None,
                    cmsketch: None,
                    stats_version: 2,
                    ..Default::default()
                })], []);
        let table_stats = StatsInfo::new(128.0, [(unique_id, 3.0)]).with_hist_coll(hist_coll);
        // Built as the rewriter builds it: the escape argument and the
        // derived collation decide whether the ranger accepts the pattern.
        let like = |pattern: &str| {
            tidb_expr::new_function::new_function(
                &tidb_exprctx::NoColumns,
                "like",
                FieldType::new(FieldTypeCode::LongLong),
                vec![
                    Expression::Column(Column::new(unique_id, varchar())),
                    Expression::Constant(Constant::new(
                        Datum::Bytes(pattern.as_bytes().to_vec()),
                        varchar(),
                    )),
                    Expression::Constant(Constant::new(
                        Datum::Int(i64::from(b'\\')),
                        FieldType::new(FieldTypeCode::LongLong),
                    )),
                ],
            )
            .unwrap()
        };

        // One bucket's bounds start with `green`; its single row over 128.
        // The range path [green, greo) covers that bucket (1 row), while
        // `green` sits below every bucket bound: GetColumnRowCount adds the
        // out-of-range estimate (not_null/ndv = 101/100 = 1.01) and scales
        // by the increase factor (128/101), landing at 2.01 rows:
        // selectivity = 2.01 × (128/101) / 128 = 2.01/101.
        let selectivity =
            analyzed_filter_selectivity(&table_stats, &[like("green%")]).expect("analyzed");
        assert!(
            (selectivity - 2.01 / 101.0).abs() < 1e-9,
            "{selectivity} != {}",
            2.01 / 101.0
        );

        // No bucket bound starts with `blue`, so no bucket is counted; the
        // below-histogram out-of-range estimate alone gives 1.01 rows over
        // the same factor: selectivity = 1.01 × (128/101) / 128 = 1.01/101.
        let selectivity =
            analyzed_filter_selectivity(&table_stats, &[like("blue%")]).expect("analyzed");
        assert!(
            (selectivity - 1.01 / 101.0).abs() < 1e-9,
            "an empty histogram sample prices the out-of-range tail: {selectivity}"
        );

        // A collection without the column gives it no node, and the
        // uncovered pattern answers the 0.1 string-match default.
        let bare = StatsInfo::new(128.0, [(unique_id, 3.0)]).with_hist_coll(
            info_only_hist_coll(128, unique_id + 1, "other"),
        );
        let selectivity = analyzed_filter_selectivity(&bare, &[like("green%")]).expect("analyzed");
        assert!(
            (selectivity - 0.1).abs() < 1e-12,
            "the default string-match selectivity is 0.1, not 0.8: {selectivity}"
        );
    }
}
