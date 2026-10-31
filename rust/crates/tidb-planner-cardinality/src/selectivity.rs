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

//! Go `pkg/planner/cardinality/selectivity.go`: `Selectivity` over one
//! table's `HistColl`, with `getMaskAndRanges`, `findPrefixOfIndex` and
//! `GetSelectivityByFilter`.
//!
//! One estimator serves analyzed and pseudo collections alike, as in Go:
//! `GetRowCountByColumnRanges` and `GetRowCountByIndexRanges` fall back to
//! the pseudo row counts themselves when the statistics are invalid.
//!
//! A multi-valued index is estimated as Go's `getMaskAndSelectivityForMVIndex`
//! does, through the access-path generator's own `CollectFilters4MVIndex` and
//! `BuildPartialPaths4MVIndex` halves.

use tidb_planner_property::stats_info::HistCollExt as _;
use tidb_expr::column::Column;
use tidb_expr::constant::Constant;
use tidb_expr::expression::{Expression, ScalarFunction};

use super::pseudo::{PSEUDO_EQUAL_RATE, hist_coll_pseudo_selectivity};
use super::row_count_estimator::{
    ColumnRange, EstimationError, EstimatorOptions, get_index_row_count,
    get_row_count_by_column_ranges,
};
use tidb_ranger::Range;
use tidb_ranger::points::{ExpressionEvaluator, PointBuilderError};
use crate::selectivity_greedy::{StatsNode, StatsNodeType, get_usable_sets_by_greedy};
use tidb_planner_property::stats_info::HistColl;
use crate::row_count_estimator::IndexEstimationPins;

/// The `PlanContext` state `Selectivity` reads.
pub struct SelectivityContext<'a> {
    /// Session estimator inputs, including `SelectivityFactor` and
    /// `DefaultStrMatchSelectivity`.
    pub options: &'a EstimatorOptions,
    /// Constant evaluation in the statement's expression context.
    pub evaluate: &'a ExpressionEvaluator<'a>,
    /// Records a range-memory fallback the way the ranger context does.
    pub range_fallback_handler: Option<&'a tidb_context::RangeFallbackHandler>,
    /// `ExprCtx.IsUseCache()`, consulted by `MaybeOverOptimized4PlanCache`.
    pub use_plan_cache: bool,
    /// Go `recordUsedItemStatsStatus`: told of every column (by its
    /// `ColumnInfo` ID, `false`) and index (`true`) the estimate consults.
    pub record_used_item: Option<&'a dyn Fn(i64, bool)>,
    /// Go `sctx.SetSkipPlanCache` from the ranger calls the estimate makes;
    /// `None` unless the statement uses the plan cache.
    pub plan_cache_marker: Option<&'a dyn tidb_planner_core_base::PlanCacheMarker>,
    /// Go's `CollectFilters4MVIndex` and `BuildPartialPaths4MVIndex`, which
    /// `planner/core` registers for this package (`core_init.go`).
    pub mv_index: &'a dyn MvIndexFilters,
}

/// The multi-valued index matching `planner/core` performs for the estimator
/// (Go `cardinality.CollectFilters4MVIndex` / `BuildPartialPaths4MVIndex`).
pub trait MvIndexFilters {
    /// The filters that can access the index (`collectFilters4MVIndex`'s first
    /// result).
    fn collect_access_filters(
        &self,
        evaluate: &ExpressionEvaluator<'_>,
        filters: &[Expression],
        idx_cols: &[Column],
    ) -> Vec<Expression>;
    /// The partial paths' filter sets and whether they intersect
    /// (`buildPartialPaths4MVIndex`).
    fn partial_access_filters(
        &self,
        evaluate: &ExpressionEvaluator<'_>,
        access_filters: &[Expression],
        idx_cols: &[Column],
    ) -> Result<Option<(Vec<Vec<Expression>>, bool)>, tidb_exprctx::EvalError>;
    /// The index columns' lengths as the partial paths read them.
    fn column_lengths(&self, idx_cols: &[Column], declared_lengths: &[i64]) -> Vec<i64>;
}

/// An estimator with no multi-valued index matching registered: no filter
/// accesses a multi-valued index, so such an index adds no estimate.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoMvIndexFilters;

impl MvIndexFilters for NoMvIndexFilters {
    fn collect_access_filters(
        &self,
        _evaluate: &ExpressionEvaluator<'_>,
        _filters: &[Expression],
        _idx_cols: &[Column],
    ) -> Vec<Expression> {
        Vec::new()
    }

    fn partial_access_filters(
        &self,
        _evaluate: &ExpressionEvaluator<'_>,
        _access_filters: &[Expression],
        _idx_cols: &[Column],
    ) -> Result<Option<(Vec<Vec<Expression>>, bool)>, tidb_exprctx::EvalError> {
        Ok(None)
    }

    fn column_lengths(&self, _idx_cols: &[Column], declared_lengths: &[i64]) -> Vec<i64> {
        declared_lengths.to_vec()
    }
}

/// Go `Selectivity`'s `filledPaths`, keyed as its `id2Paths`: each possible
/// access path that carries an index, with the unique IDs of that path's
/// `IdxCols` when the estimate runs. A common-handle table path carries its
/// PRIMARY index with no columns until `deriveCommonHandleTablePathStats`.
pub type FilledPaths = std::collections::BTreeMap<i64, Vec<Column>>;

impl<'a> SelectivityContext<'a> {
    /// A context with no range-fallback recorder and plan caching off.
    pub fn new(
        options: &'a EstimatorOptions,
        evaluate: &'a ExpressionEvaluator<'a>,
        mv_index: &'a dyn MvIndexFilters,
    ) -> Self {
        Self {
            options,
            evaluate,
            mv_index,
            range_fallback_handler: None,
            use_plan_cache: false,
            record_used_item: None,
            plan_cache_marker: None,
        }
    }

    /// Forwards a ranger's `SetSkipPlanCache` reason to the statement.
    fn skip_plan_cache(&self, reason: Option<&str>) {
        if let (Some(marker), Some(reason)) = (self.plan_cache_marker, reason) {
            marker.set_skip_plan_cache(reason);
        }
    }

    fn record_used_item(&self, item_id: i64, is_index: bool) {
        // Go skips `_tidb_rowid` (ID -1) and other non-positive IDs.
        if item_id > 0 {
            if let Some(record) = self.record_used_item {
                record(item_id, is_index);
            }
        }
    }
}

/// Go `Selectivity(ctx, coll, exprs, filledPaths)`.
pub fn selectivity(
    ctx: &SelectivityContext<'_>,
    coll: &HistColl,
    exprs: &[Expression],
    filled_paths: Option<&FilledPaths>,
) -> Result<f64, EstimationError> {
    let realtime = coll.realtime_count();
    if realtime == 0 || exprs.is_empty() {
        return Ok(1.0);
    }
    let realtime = realtime as f64;
    if exprs.len() > 63 || coll.has_no_column_or_index() {
        return Ok(hist_coll_pseudo_selectivity(
            coll,
            exprs,
            ctx.options.selectivity_factor,
        ));
    }

    let mut ret = 1.0_f64;
    let mut remained: Vec<Expression> = Vec::with_capacity(exprs.len());
    // Deal with the correlated column.
    for expr in exprs {
        let Some(column) = is_col_eq_cor_col(expr) else {
            remained.push(expr.clone());
            continue;
        };
        let ndv = coll
            .histogram_for_estimation(column.unique_id)
            .map_or(0, |stats| crate::read(&stats).histogram.ndv);
        ret *= if ndv > 0 {
            1.0 / ndv as f64
        } else {
            1.0 / PSEUDO_EQUAL_RATE
        };
    }

    let mut extracted = tidb_expr::simple_expr::extract_columns_from_expressions(&remained, None);
    extracted.sort_by_key(|column| column.id);
    extracted.dedup_by_key(|column| column.id);

    let mut nodes: Vec<StatsNode> = Vec::new();
    for column in &extracted {
        // For an expression index only the index statistics are used.
        if column.is_hidden && column.virtual_expr.is_some() {
            continue;
        }
        if !coll.has_column(column.unique_id) {
            if !column.is_hidden {
                ctx.record_used_item(column.id, false);
            }
            continue;
        }
        let Some(field_type) = column.ret_type.as_ref() else {
            continue;
        };
        let (mask, ranges) = column_mask_and_ranges(ctx, &remained, column, field_type)?;
        ctx.record_used_item(column.id, false);
        let is_handle = column_is_handle(coll, column);
        let column_stats = coll.histogram_for_estimation(column.unique_id);
        let column_stats = column_stats.as_ref().map(|stats| crate::read(stats));
        let estimate = get_row_count_by_column_ranges(
            column_stats.as_deref(),
            &ranges,
            field_type.collation(),
            coll.realtime_count,
            coll.modify_count,
            is_handle,
            ctx.options,
        )?;
        let node_type = if is_handle {
            StatsNodeType::PrimaryKey
        } else {
            StatsNodeType::Column
        };
        nodes.push(StatsNode {
            selectivity: estimate.est / realtime,
            ..StatsNode::new(node_type, column.unique_id, mask, 1)
        });
    }

    for index_id in coll.index_ids() {
        // `idx.Info`: whether it is multi-valued and its key columns' prefix
        // lengths, read once.
        let info = coll.index_info(index_id).map(|info| {
            let info = info.read();
            (
                info.mv_index,
                info.columns
                    .iter_deref()
                    .map(|column| column.read().length)
                    .collect::<Vec<_>>(),
            )
        });
        if let Some((_, column_lengths)) = info.as_ref().filter(|(mv_index, _)| *mv_index) {
            if let Some((selectivity, mask)) =
                mask_and_selectivity_for_mv_index(ctx, coll, index_id, column_lengths, &remained)
            {
                nodes.push(StatsNode {
                    selectivity,
                    ..StatsNode::new(StatsNodeType::Index, index_id, mask, column_lengths.len())
                });
            }
            continue;
        }
        let info = info.as_ref().map(|(_, column_lengths)| column_lengths);
        // Go `findPrefixOfIndexByCol`: a possible path decides by its own
        // `IdxCols`; an index without one by `Idx2ColUniqueIDs`.
        let index_columns = match filled_paths.and_then(|paths| paths.get(&index_id)) {
            Some(path_columns) => find_prefix_of_index_by_col(&extracted, path_columns),
            None => find_prefix_of_index(&extracted, coll.index_columns(index_id)),
        };
        if index_columns.is_empty() {
            continue;
        }
        let declared = info.map_or(index_columns.len(), Vec::len);
        let mut lengths: Vec<i64> = (0..index_columns.len().min(declared))
            .map(|position| {
                info.and_then(|lengths| lengths.get(position).copied())
                    .unwrap_or(tidb_ranger::checker::UNSPECIFIED_LENGTH)
            })
            .collect();
        // The found columns exceed the index's own: the integer handle the
        // key stores after the index columns.
        if index_columns.len() > declared {
            lengths.push(tidb_ranger::checker::UNSPECIFIED_LENGTH);
        }
        // Go `getMaskAndRanges` takes a filled access path's own ranges
        // (`cachedPath`) instead of building them again; this port derives
        // them again here, before the path is filled, so only an index
        // without a path converts points under the statement's warnings.
        let cached_path = filled_paths.is_some_and(|paths| paths.contains_key(&index_id));
        let detached = detach_index_range(ctx, &remained, &index_columns, &lengths, cached_path)?;
        let (mask, partial_cover, min_access_conditions_for_dnf) =
            if detached.is_dnf_cond && !detached.access_conds.is_empty() {
                (
                    1,
                    !detached.remained_conds.is_empty(),
                    i32::try_from(detached.min_access_conds_for_dnf_cond).unwrap_or(i32::MAX),
                )
            } else {
                (covered_mask(&remained, &detached.access_conds), false, 0)
            };
        ctx.record_used_item(index_id, true);
        let estimate = get_row_count_by_index_ranges(
            coll,
            index_id,
            &detached.ranges,
            &index_columns,
            ctx.options,
        )?
        .est;
        nodes.push(StatsNode {
            selectivity: estimate / realtime,
            partial_cover,
            min_access_conditions_for_dnf,
            ..StatsNode::new(
                StatsNodeType::Index,
                index_id,
                mask,
                info.map_or(coll.index_columns(index_id).len(), Vec::len),
            )
        });
    }

    // The full set; `remained` holds at most 63 conditions.
    let mut mask: i64 = (1_i64 << remained.len()) - 1;
    for set in get_usable_sets_by_greedy(&mut nodes) {
        mask &= !set.mask;
        ret *= set.selectivity;
        // Only part of a DNF became access conditions; the residual takes
        // another selection factor.
        if set.partial_cover {
            ret *= ctx.options.selectivity_factor;
        }
    }

    let mut not_covered_constants: Vec<(usize, &Constant)> = Vec::new();
    let mut not_covered_dnf: Vec<(usize, &ScalarFunction)> = Vec::new();
    let mut not_covered_str_match: Vec<(usize, &Expression)> = Vec::new();
    let mut not_covered_negate_str_match: Vec<(usize, &Expression)> = Vec::new();
    let mut not_covered_other = false;
    if mask > 0 {
        for (offset, expr) in remained.iter().enumerate() {
            if mask & (1 << offset) == 0 {
                continue;
            }
            match expr {
                Expression::Constant(constant) => {
                    not_covered_constants.push((offset, constant));
                    continue;
                }
                Expression::ScalarFunction(function) => match function.func_name.lowercase() {
                    "or" => {
                        not_covered_dnf.push((offset, function));
                        continue;
                    }
                    "like" | "ilike" | "regexp" | "regexp_like" => {
                        not_covered_str_match.push((offset, expr));
                        continue;
                    }
                    "not" => {
                        if let Some(Expression::ScalarFunction(inner)) = function
                            .args
                            .first()
                            .map(tidb_expr::expr_util::predicates::get_expr_inside_is_truth)
                        {
                            if matches!(
                                inner.func_name.lowercase(),
                                "like" | "ilike" | "regexp" | "regexp_like"
                            ) {
                                not_covered_negate_str_match.push((offset, expr));
                                continue;
                            }
                        }
                    }
                    _ => {}
                },
                _ => {}
            }
            not_covered_other = true;
        }
    }

    // Try to cover the remaining constants.
    not_covered_constants.retain(|(offset, constant)| {
        let expr = Expression::Constant((*constant).clone());
        if tidb_expr::expr_util::predicates::maybe_over_optimized_4_plan_cache(
            ctx.use_plan_cache,
            std::slice::from_ref(&expr),
        ) {
            return true;
        }
        if constant.value.is_null() {
            ret = 0.0;
            mask &= !(1 << offset);
            return false;
        }
        match constant.value.to_bool() {
            Ok(converted) => {
                if converted.value == 0 {
                    ret = 0.0;
                }
                mask &= !(1 << offset);
                false
            }
            Err(_) => true,
        }
    });

    // Cover the remaining DNF conditions under the independence assumption:
    // sel(A or B) = sel(A) + sel(B) - sel(A) * sel(B).
    not_covered_dnf.retain(|(offset, function)| {
        let condition = Expression::ScalarFunction((*function).clone());
        if tidb_expr::simple_expr::extract_columns(&condition)
            .iter()
            .any(|column| !coll.has_column(column.unique_id))
        {
            return true;
        }
        let items = tidb_ranger::detacher::merge_dnf_items_4_col(
            &tidb_expr::expr_util::normal_form::flatten_dnf_conditions(function),
            ctx.options.opt_prefix_index_single_scan,
        );
        if items.len() <= 1 {
            return true;
        }
        let mut dnf_selectivity = 0.0_f64;
        for item in &items {
            if matches!(item, Expression::CorrelatedColumn(_)) {
                continue;
            }
            let cnf = match item {
                Expression::ScalarFunction(scalar) if scalar.func_name.lowercase() == "and" => {
                    tidb_expr::expr_util::normal_form::flatten_cnf_conditions(scalar)
                }
                _ => vec![item.clone()],
            };
            let current =
                selectivity(ctx, coll, &cnf, None).unwrap_or(ctx.options.selectivity_factor);
            dnf_selectivity = dnf_selectivity + current - dnf_selectivity * current;
        }
        if dnf_selectivity != 0.0 {
            ret *= dnf_selectivity;
            mask &= !(1 << offset);
            return false;
        }
        true
    });

    // Cover the remaining string matches by evaluating them over TopN.
    if ctx.options.enable_eval_top_n_estimation_for_str_match() {
        for list in [&mut not_covered_str_match, &mut not_covered_negate_str_match] {
            list.retain(|(offset, condition)| {
                match get_selectivity_by_filter(ctx, coll, condition) {
                    Ok(Some(filter_selectivity)) => {
                        ret *= filter_selectivity;
                        mask &= !(1 << offset);
                        false
                    }
                    _ => true,
                }
            });
        }
    }

    // Whatever is still uncovered takes the minimum default selectivity of
    // its kinds, once.
    if mask > 0 {
        let mut min_selectivity = 1.0_f64;
        if !not_covered_constants.is_empty() || !not_covered_dnf.is_empty() || not_covered_other {
            min_selectivity = min_selectivity.min(ctx.options.selectivity_factor);
        }
        if !not_covered_str_match.is_empty() {
            min_selectivity = min_selectivity.min(ctx.options.str_match_default_selectivity());
        }
        if !not_covered_negate_str_match.is_empty() {
            min_selectivity =
                min_selectivity.min(ctx.options.negate_str_match_default_selectivity());
        }
        ret *= min_selectivity;
    }

    // Don't allow the result to be less than one row.
    Ok(ret.max(1.0 / realtime))
}

/// Go `isColEqCorCol`: `col = correlated` in either order.
fn is_col_eq_cor_col(expr: &Expression) -> Option<&Column> {
    let Expression::ScalarFunction(function) = expr else {
        return None;
    };
    if function.func_name.lowercase() != "eq" {
        return None;
    }
    match function.args.as_slice() {
        [Expression::Column(column), Expression::CorrelatedColumn(_)]
        | [Expression::CorrelatedColumn(_), Expression::Column(column)] => Some(column),
        _ => None,
    }
}

/// Go `colStats.IsHandle`: the table's single integer handle (`PKIsHandle`
/// and the column's `PriKeyFlag`, as the loader and `PseudoTable` set it).
fn column_is_handle(coll: &HistColl, column: &Column) -> bool {
    coll.column(column.unique_id).is_some_and(|stats| stats.is_handle)
}

/// Go `getMaskAndRanges(..., ranger.ColumnRangeType, ...)`.
fn column_mask_and_ranges(
    ctx: &SelectivityContext<'_>,
    exprs: &[Expression],
    column: &Column,
    field_type: &tidb_datatype::FieldType,
) -> Result<(i64, Vec<ColumnRange>), EstimationError> {
    let access: Vec<Expression> = tidb_ranger::detacher::extract_access_conditions_for_column(
        exprs,
        column,
        ctx.options.opt_prefix_index_single_scan,
    )
    .into_iter()
    .cloned()
    .collect();
    let built = tidb_ranger::ranger::build_column_range_in(
        &access,
        field_type,
        tidb_ranger::checker::UNSPECIFIED_LENGTH,
        ctx.options.range_max_size,
        ctx.evaluate,
    )?;
    ctx.skip_plan_cache(built.skip_plan_cache_reason.as_deref());
    if !built.remained_conds.is_empty() {
        if let Some(handler) = ctx.range_fallback_handler {
            handler.record_range_fallback(ctx.options.range_max_size);
        }
    }
    let ranges = built
        .ranges
        .iter()
        .map(|range| {
            ColumnRange::new(
                range.low_val[0].clone(),
                range.high_val[0].clone(),
                range.low_exclude,
                range.high_exclude,
            )
        })
        .collect();
    Ok((covered_mask(exprs, built.access_conds), ranges))
}

/// Go `ranger.DetachCondAndBuildRangeForIndex` under the session's range
/// memory quota.
fn detach_index_range(
    ctx: &SelectivityContext<'_>,
    exprs: &[Expression],
    columns: &[Column],
    lengths: &[i64],
    cached_path: bool,
) -> Result<tidb_ranger::detacher::DetachRangeResult, PointBuilderError> {
    let quiet = tidb_ranger::points::QuietRangerEvaluator(ctx.evaluate);
    let evaluate: &tidb_ranger::points::ExpressionEvaluator<'_> =
        if cached_path { &quiet } else { ctx.evaluate };
    let detached = match ctx.range_fallback_handler {
        Some(handler) => tidb_ranger::detacher::detach_index_range_with_fallback_handler_in(
            exprs,
            columns,
            lengths,
            ctx.options.range_max_size,
            handler,
            evaluate,
            ctx.options.ranger_options(),
        ),
        None => tidb_ranger::detacher::detach_cond_and_build_range_for_index_in(
            exprs,
            columns,
            lengths,
            ctx.options.range_max_size,
            evaluate,
            ctx.options.ranger_options(),
        ),
    }?;
    ctx.skip_plan_cache(detached.skip_plan_cache_reason.as_deref());
    Ok(detached)
}

/// Go `getMaskAndSelectivityForMVIndex`: the selectivity of the IndexMerge
/// partial paths the MV index would serve for `exprs`, and the mask of the
/// conditions they cover. `None` where Go returns `ok == false`, including
/// every error on the way.
fn mask_and_selectivity_for_mv_index(
    ctx: &SelectivityContext<'_>,
    coll: &HistColl,
    index_id: i64,
    column_lengths: &[i64],
    exprs: &[Expression],
) -> Option<(f64, i64)> {
    let cols = coll.mv_index_columns(index_id);
    if cols.is_empty() {
        return None;
    }
    let access_conds = ctx.mv_index.collect_access_filters(ctx.evaluate, exprs, cols);
    let (filter_sets, is_intersection) = ctx
        .mv_index
        .partial_access_filters(ctx.evaluate, &access_conds, cols)
        .ok()??;
    let lengths = ctx.mv_index.column_lengths(cols, column_lengths);
    let realtime = coll.realtime_count as f64;
    // Go `CalcTotalSelectivityForMVIdxPath`: each partial reads the virtual
    // column, so its selectivity is over the table's realtime count.
    let mut total = if is_intersection { 1.0 } else { 0.0 };
    for filters in filter_sets {
        // Go `buildPartialPath4MVIndex`: every filter must become an access
        // condition.
        let detached = detach_index_range(ctx, &filters, cols, &lengths, false).ok()?;
        if detached.access_conds.len() != filters.len() || !detached.remained_conds.is_empty() {
            return None;
        }
        ctx.record_used_item(index_id, true);
        let count =
            get_row_count_by_index_ranges(coll, index_id, &detached.ranges, cols, ctx.options)
                .ok()?
                .est;
        let selectivity = (count / realtime).clamp(0.0, 1.0);
        total = if is_intersection {
            total * selectivity
        } else {
            (selectivity + total) - total * selectivity
        };
    }
    Some((total, covered_mask(exprs, &access_conds)))
}

/// The `exprs[i].Equal(accessConds[j])` mask `getMaskAndRanges` returns.
fn covered_mask(exprs: &[Expression], access: &[Expression]) -> i64 {
    exprs
        .iter()
        .enumerate()
        .filter(|(_, expr)| tidb_expr::expr_util::predicates::contains(access, expr))
        .fold(0, |mask, (offset, _)| mask | (1 << offset))
}

/// Go `findPrefixOfIndex`: the index's leading columns that appear among
/// `columns`, stopping at the first one that does not.
/// Go `findPrefixOfIndexByCol` with a cached path: the extracted columns
/// matching the path's `IdxCols` in order, by UniqueID or by virtual
/// expression, stopping at the first index column none matches.
fn find_prefix_of_index_by_col(columns: &[Column], index_columns: &[Column]) -> Vec<Column> {
    let mut prefix = Vec::with_capacity(index_columns.len());
    for index_column in index_columns {
        match columns
            .iter()
            .find(|column| column.equal_by_expr_and_id_column(index_column))
        {
            Some(column) => prefix.push(column.clone()),
            None => break,
        }
    }
    prefix
}

fn find_prefix_of_index(columns: &[Column], index_column_ids: &[i64]) -> Vec<Column> {
    let mut prefix = Vec::with_capacity(index_column_ids.len());
    for id in index_column_ids {
        match columns.iter().find(|column| column.unique_id == *id) {
            Some(column) => prefix.push(column.clone()),
            None => break,
        }
    }
    prefix
}

/// Go `GetRowCountByIndexRanges(sctx, coll, idxID, ranges, idxCols)`.
pub fn get_row_count_by_index_ranges(
    coll: &HistColl,
    index_id: i64,
    ranges: &[Range],
    index_columns: &[Column],
    options: &EstimatorOptions,
) -> Result<tidb_stats::RowEstimate, EstimationError> {
    let mut pins = IndexEstimationPins::default();
    // `hasColumnStats`, the partial-statistics estimate and the virtual-column
    // check read `idxCols`; an index the collection does not hold still has
    // them.
    let valid_columns = index_columns
        .iter()
        .map(|column| column.unique_id)
        .collect::<Vec<_>>();
    let main = pins.pin(coll, index_id, Some(&valid_columns));
    let mut recursive_handles = Vec::new();
    for id in coll.index_columns(index_id) {
        let mut candidates = Vec::new();
        for candidate in coll.index_ids_for_column(*id) {
            candidates.push(pins.pin(coll, *candidate, None));
        }
        recursive_handles.push(candidates);
    }
    let guards = pins.read();
    let stats = guards.stats(main);
    let recursive = recursive_handles
        .iter()
        .map(|candidates| {
            candidates
                .iter()
                .map(|candidate| guards.stats(*candidate))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let virtual_columns = index_columns
        .iter()
        .map(|column| column.virtual_expr.is_some())
        .collect::<Vec<_>>();
    get_index_row_count(&stats, &virtual_columns, &recursive, ranges, options)
}

/// Go `GetSelectivityByFilter`: a single-column filter evaluated over the
/// stats-version-2 TopN values, the histogram bucket bounds and NULL.
/// `None` is Go's `ok == false`.
pub fn get_selectivity_by_filter(
    ctx: &SelectivityContext<'_>,
    coll: &HistColl,
    filter: &Expression,
) -> Result<Option<f64>, EstimationError> {
    if tidb_expr::expr_util::predicates::is_mutable_effects_expr(filter)
        || tidb_expr::expr_util::predicates::contain_correlated_column(std::slice::from_ref(filter))
    {
        return Ok(None);
    }
    let columns = tidb_expr::simple_expr::extract_columns(filter);
    let [column] = columns.as_slice() else {
        return Ok(None);
    };
    let Some(field_type) = column.ret_type.as_ref() else {
        return Ok(None);
    };
    if field_type.is_string()
        && tidb_datatype::new_collation_enabled()
        && !tidb_datatype::is_bin_collation(field_type.collation_name())
    {
        return Ok(None);
    }
    let Some(available) = find_available_stats_for_col(coll, column.unique_id) else {
        return Ok(None);
    };
    let guard = available.read();
    let stats = guard.stats();
    // Only stats version 2 guarantees TopN + histogram + NULL covers all.
    if stats.stats_ver != 2 {
        return Ok(None);
    }
    let topn_total = stats.topn.map_or(0, tidb_stats::cmsketch::TopN::total_count) as f64;
    let hist_total = stats.histogram.not_null_count();
    let null_count = stats.histogram.null_count;
    let total = topn_total + hist_total + null_count as f64;

    let keeps = |value: &tidb_datatype::Datum| -> Result<bool, EstimationError> {
        let substituted = substitute_column(filter, column.unique_id, value, field_type);
        let result = ctx
            .evaluate
            .eval(&substituted)
            .map_err(PointBuilderError::Eval)?;
        if result.is_null() {
            return Ok(false);
        }
        Ok(result.to_bool()?.value != 0)
    };

    let mut topn_selected = 0_u64;
    if let Some(topn) = stats.topn {
        for entry in topn.entries() {
            let (_, value) = tidb_codec::decode_one(&entry.encoded)?;
            if keeps(&value)? {
                topn_selected += entry.count;
            }
        }
    }
    let topn_selectivity = topn_selected as f64 / total;

    let mut hist_selectivity = 0.0;
    if hist_total > 0.0 {
        let buckets = &stats.histogram.buckets;
        let mut repeat_total = 0_i64;
        let mut repeat_selected = 0_i64;
        let mut lower_bound_matches = 0_i64;
        for bucket in buckets {
            repeat_total += bucket.repeat;
            if keeps(&bucket.lower_bound)? {
                lower_bound_matches += 1;
            }
            if keeps(&bucket.upper_bound)? {
                repeat_selected += bucket.repeat;
            }
        }
        let upper_bounds_ratio = (repeat_total as f64 / hist_total).min(1.0);
        let lower_bounds_ratio = 1.0 - upper_bounds_ratio;
        let upper_bounds_selectivity = if repeat_total > 0 {
            repeat_selected as f64 / repeat_total as f64
        } else {
            0.0
        };
        let lower_bounds_selectivity = lower_bound_matches as f64 / buckets.len() as f64;
        hist_selectivity = (lower_bounds_selectivity * lower_bounds_ratio
            + upper_bounds_selectivity * upper_bounds_ratio)
            * (hist_total / total);
    }

    // An error here does not fail the estimate; it only zeroes the NULL part.
    let null_selectivity = match keeps(&tidb_datatype::Datum::Null) {
        Ok(true) => null_count as f64 / total,
        _ => 0.0,
    };
    Ok(Some(topn_selectivity + hist_selectivity + null_selectivity))
}

/// The statistics object `findAvailableStatsForCol` settles on.
enum AvailableStats {
    Column(tidb_stats::SharedColumn),
    Index(tidb_stats::SharedIndex),
}

/// [`AvailableStats`] read-locked.
enum AvailableStatsGuard<'a> {
    Column(std::sync::RwLockReadGuard<'a, tidb_stats::column::Column>),
    Index(std::sync::RwLockReadGuard<'a, tidb_stats::index::Index>),
}

/// The parts of the available statistics a filter selectivity reads.
struct FilterStats<'a> {
    stats_ver: i64,
    histogram: &'a tidb_stats::histogram::Histogram,
    topn: Option<&'a tidb_stats::cmsketch::TopN>,
}

impl AvailableStats {
    fn read(&self) -> AvailableStatsGuard<'_> {
        match self {
            Self::Column(column) => AvailableStatsGuard::Column(crate::read(column)),
            Self::Index(index) => AvailableStatsGuard::Index(crate::read(index)),
        }
    }
}

impl AvailableStatsGuard<'_> {
    fn stats(&self) -> FilterStats<'_> {
        match self {
            Self::Column(column) => FilterStats {
                stats_ver: column.stats_version,
                histogram: &column.histogram,
                topn: column.top_n.as_ref(),
            },
            Self::Index(index) => FilterStats {
                stats_ver: index.stats_version,
                histogram: &index.histogram,
                topn: index.top_n.as_ref(),
            },
        }
    }
}

/// Go `findAvailableStatsForCol`: the column's own fully loaded statistics,
/// else a fully loaded single-column, non-prefix index on it.
fn find_available_stats_for_col(coll: &HistColl, unique_id: i64) -> Option<AvailableStats> {
    if let Some(column) = coll
        .histogram_for_estimation(unique_id)
        .filter(|_| coll.column_is_full_load(unique_id))
    {
        return Some(AvailableStats::Column(column));
    }
    coll.index_ids().into_iter().find_map(|index_id| {
        if coll.index_columns(index_id) != [unique_id] {
            return None;
        }
        let prefix = coll
            .index_info(index_id)
            .and_then(|info| info.read().columns.get(0).map(|column| column.read().length))
            .is_some_and(|length| length != tidb_ranger::checker::UNSPECIFIED_LENGTH);
        if prefix || coll.pseudo || !coll.index_is_full_load(index_id) {
            return None;
        }
        let index = coll
            .index_histogram(index_id)
            .filter(|index| crate::read(index).total_row_count() != 0.0)?;
        Some(AvailableStats::Index(index))
    })
}

/// `filter` with the column `unique_id` replaced by `value`: Go evaluates the
/// filter over a one-column chunk holding the value; a constant of the
/// column's type is the same input to every builtin.
fn substitute_column(
    filter: &Expression,
    unique_id: i64,
    value: &tidb_datatype::Datum,
    field_type: &tidb_datatype::FieldType,
) -> Expression {
    match filter {
        Expression::Column(column) if column.unique_id == unique_id => {
            Expression::Constant(Constant::new(value.clone(), field_type.clone()))
        }
        Expression::ScalarFunction(function) => {
            let mut function = function.clone();
            for arg in &mut function.args {
                *arg = substitute_column(arg, unique_id, value, field_type);
            }
            Expression::ScalarFunction(function)
        }
        other => other.clone(),
    }
}

/// `GetRowCountByIndexRanges` over a `HistColl`, the call `fillIndexPath`'s
/// `detachCondAndBuildRangeForPath` makes for every filled index path.
#[cfg(test)]
mod index_range_estimate_tests {
    use std::sync::Arc;
    use tidb_planner_property::stats_info::loaded_hist_coll_for_test;

    use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_charset::Collation;
    use tidb_expr::column::Column;
    use tidb_stats::{Bucket, Histogram, TopN};

    use super::get_row_count_by_index_ranges;
    use crate::row_count_estimator::EstimatorOptions;
    use tidb_ranger::Range;

    fn column(unique_id: i64) -> Column {
        Column::new(unique_id, FieldType::new(FieldTypeCode::LongLong))
    }

    fn range(low: Vec<Datum>, high: Vec<Datum>) -> Range {
        Range {
            collators: vec![Collation::Binary; low.len()],
            low_val: low,
            high_val: high,
            low_exclude: false,
            high_exclude: false,
        }
    }

    fn two_column_histogram(
        id: i64,
        first_low: i64,
        first_high: i64,
    ) -> Arc<tidb_stats::index::Index> {
        Arc::new(tidb_stats::index::Index {
            histogram: Histogram {
                id,
                ndv: 100,
                buckets: vec![Bucket {
                    count: 1_000,
                    repeat: 1,
                    ndv: 100,
                    lower_bound: Datum::Bytes(
                        tidb_codec::encode_key(&[Datum::Int(first_low), Datum::Int(1)]).unwrap(),
                    ),
                    upper_bound: Datum::Bytes(
                        tidb_codec::encode_key(&[Datum::Int(first_high), Datum::Int(100)]).unwrap(),
                    ),
                }],
                ..Histogram::default()
            },
            top_n: None,
            cmsketch: None,
            stats_version: 2,
            info: Some(tidb_model::GoShared::new(tidb_model::IndexInfo {
                columns: (vec![String::new(); 2])
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
        })
    }

    /// Go `ColUniqueID2IdxIDs` only maps an index to the column it LEADS
    /// with: a byte histogram cannot answer a standalone bound on a later key
    /// column, so only an index leading with the missing column contributes.
    #[test]
    fn recursive_index_statistics_only_estimate_the_leading_column() {
        let columns = [column(1), column(2)];
        let ranges = [range(
            vec![Datum::Int(1), Datum::Int(2)],
            vec![Datum::Int(1), Datum::Int(8)],
        )];
        let options = EstimatorOptions::default();
        let collection = |indexes: &[(i64, Vec<i64>)]| {
            loaded_hist_coll_for_test(
                false,
                1_000,
                0,
                [],
                indexes.iter().map(|(id, columns)| {
                    let histogram = two_column_histogram(
                        *id,
                        if *id == 7 { 1 } else { 1_000 },
                        if *id == 7 { 100 } else { 2_000 },
                    );
                    (*id, Arc::unwrap_or_clone(histogram), columns.clone())
                }),
            )
        };
        let baseline = get_row_count_by_index_ranges(
            &collection(&[(7, vec![1, 2])]),
            7,
            &ranges,
            &columns,
            &options,
        )
        .unwrap();
        let with_nonleading = get_row_count_by_index_ranges(
            &collection(&[(7, vec![1, 2]), (1, vec![3, 2])]),
            7,
            &ranges,
            &columns,
            &options,
        )
        .unwrap();
        assert_eq!(with_nonleading.est, baseline.est);
        assert_eq!(with_nonleading.min_est, baseline.min_est);
        assert_eq!(with_nonleading.max_est, baseline.max_est);
        let with_leading = get_row_count_by_index_ranges(
            &collection(&[(7, vec![1, 2]), (1, vec![3, 2]), (2, vec![2, 3])]),
            7,
            &ranges,
            &columns,
            &options,
        )
        .unwrap();
        // Two contributing columns activate Go's index-NDV bound: 1000 / 100.
        assert_eq!(with_leading.max_est, 10.0);
        assert_ne!(with_leading.max_est, baseline.max_est);
    }

    /// A one-column bound on a two-column index whose TopN holds only
    /// `(v, v)` pairs: an absent value is estimated from the modify count.
    #[test]
    fn unknown_values_in_composite_index_ranges_match_go() {
        let mut topn = TopN::new(10);
        for value in 0..10 {
            let encoded = tidb_codec::encode_key(&[Datum::Int(value), Datum::Int(value)]).unwrap();
            topn.append(&encoded, 1);
        }
        topn.sort();
        let index = Arc::new(tidb_stats::index::Index {
            histogram: Histogram {
                id: 7,
                ndv: 10,
                ..Default::default()
            },
            top_n: Some(topn),
            cmsketch: None,
            stats_version: 2,
            info: Some(tidb_model::GoShared::new(tidb_model::IndexInfo {
                columns: (vec![String::new(); 2])
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
        });
        let collection =
            loaded_hist_coll_for_test(false, 20, 10, [], [(7, Arc::unwrap_or_clone(index), vec![1, 2])]);
        let columns = [column(1), column(2)];
        let estimate = |low, high| {
            get_row_count_by_index_ranges(
                &collection,
                7,
                &[range(vec![Datum::Int(low)], vec![Datum::Int(high)])],
                &columns,
                &EstimatorOptions::default(),
            )
            .unwrap()
            .est
        };
        assert_eq!(estimate(30, 30), 1.0);
        assert_eq!(estimate(9, 30), 2.0);
    }

    /// Go `canSkipIndexEstimation`: a full range over an ordinary index
    /// includes every table row, so the realtime count answers it even when
    /// the index histogram counts fewer entries; a not-null range does not
    /// take that shortcut.
    #[test]
    fn a_full_index_range_is_the_realtime_row_count() {
        let index = Arc::new(tidb_stats::index::Index {
            histogram: Histogram {
                id: 7,
                ndv: 50,
                null_count: 10,
                buckets: vec![Bucket {
                    count: 50,
                    repeat: 1,
                    ndv: 50,
                    lower_bound: Datum::Bytes(tidb_codec::encode_key(&[Datum::Int(1)]).unwrap()),
                    upper_bound: Datum::Bytes(tidb_codec::encode_key(&[Datum::Int(50)]).unwrap()),
                }],
                ..Default::default()
            },
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
        });
        let collection =
            loaded_hist_coll_for_test(false, 80, 0, [], [(7, Arc::unwrap_or_clone(index), vec![1])]);
        let columns = [column(1)];
        let estimate = |low: Datum, high: Datum| {
            get_row_count_by_index_ranges(
                &collection,
                7,
                &[range(vec![low], vec![high])],
                &columns,
                &EstimatorOptions::default(),
            )
            .unwrap()
            .est
        };
        assert_eq!(estimate(Datum::Null, Datum::MaxValue), 80.0);
        assert_ne!(estimate(Datum::MinNotNull, Datum::MaxValue), 80.0);
        assert_ne!(estimate(Datum::Int(1), Datum::Int(10)), 80.0);
    }
}
