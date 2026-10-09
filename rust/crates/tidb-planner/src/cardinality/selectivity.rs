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
//! Multi-valued indexes are skipped: Go's `getMaskAndSelectivityForMVIndex`
//! needs `CollectFilters4MVIndex`/`BuildPartialPaths4MVIndex`, which this
//! planner does not have yet.

use tidb_expr::column::Column;
use tidb_expr::constant::Constant;
use tidb_expr::expression::{Expression, ScalarFunction};

use super::pseudo::{PSEUDO_EQUAL_RATE, hist_coll_pseudo_selectivity};
use super::row_count_estimator::{
    ColumnRange, EstimationError, EstimatorOptions, IndexRangeDatums, get_index_row_count,
    get_row_count_by_column_ranges,
};
use crate::ranger::points::{ExpressionEvaluator, PointBuilderError};
use crate::selectivity_greedy::{StatsNode, StatsNodeType, get_usable_sets_by_greedy};
use crate::stats_info::HistColl;

/// The `PlanContext` state `Selectivity` reads.
pub struct SelectivityContext<'a> {
    /// Session estimator inputs, including `SelectivityFactor` and
    /// `DefaultStrMatchSelectivity`.
    pub options: &'a EstimatorOptions,
    /// Constant evaluation in the statement's expression context.
    pub evaluate: &'a ExpressionEvaluator<'a>,
    /// Records a range-memory fallback the way the ranger context does.
    pub range_fallback_handler: Option<&'a tidb_util::context::RangeFallbackHandler>,
    /// `ExprCtx.IsUseCache()`, consulted by `MaybeOverOptimized4PlanCache`.
    pub use_plan_cache: bool,
}

impl<'a> SelectivityContext<'a> {
    /// A context with no range-fallback recorder and plan caching off.
    pub fn new(options: &'a EstimatorOptions, evaluate: &'a ExpressionEvaluator<'a>) -> Self {
        Self {
            options,
            evaluate,
            range_fallback_handler: None,
            use_plan_cache: false,
        }
    }
}

/// Go `Selectivity(ctx, coll, exprs, nil)`.
pub fn selectivity(
    ctx: &SelectivityContext<'_>,
    coll: &HistColl,
    exprs: &[Expression],
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
        ret *= match coll.histogram_for_estimation(column.unique_id) {
            Some(stats) if stats.histogram.ndv > 0 => 1.0 / stats.histogram.ndv as f64,
            _ => 1.0 / PSEUDO_EQUAL_RATE,
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
            continue;
        }
        let Some(field_type) = column.ret_type.as_ref() else {
            continue;
        };
        let (mask, ranges) = column_mask_and_ranges(ctx, &remained, column, field_type)?;
        let is_handle = column_is_handle(coll, column, field_type);
        let estimate = get_row_count_by_column_ranges(
            coll.histogram_for_estimation(column.unique_id)
                .map(AsRef::as_ref),
            &ranges,
            field_type.collation(),
            coll.realtime_count(),
            coll.modify_count(),
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
        let info = coll.index_info(index_id);
        if info.is_some_and(|info| info.mv_index) {
            continue;
        }
        let index_columns = find_prefix_of_index(&extracted, coll.index_columns(index_id));
        if index_columns.is_empty() {
            continue;
        }
        let declared = info.map_or(index_columns.len(), |info| info.column_lengths.len());
        let mut lengths: Vec<i64> = (0..index_columns.len().min(declared))
            .map(|position| {
                info.and_then(|info| info.column_lengths.get(position).copied())
                    .unwrap_or(crate::ranger::checker::UNSPECIFIED_LENGTH)
            })
            .collect();
        // The found columns exceed the index's own: the integer handle the
        // key stores after the index columns.
        if index_columns.len() > declared {
            lengths.push(crate::ranger::checker::UNSPECIFIED_LENGTH);
        }
        let detached = detach_index_range(ctx, &remained, &index_columns, &lengths)?;
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
        let estimate =
            get_row_count_by_index_ranges(ctx, coll, index_id, &detached.ranges, &index_columns)?;
        nodes.push(StatsNode {
            selectivity: estimate / realtime,
            partial_cover,
            min_access_conditions_for_dnf,
            ..StatsNode::new(
                StatsNodeType::Index,
                index_id,
                mask,
                info.map_or(coll.index_columns(index_id).len(), |info| {
                    info.column_lower_names.len()
                }),
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
        let items = crate::ranger::detacher::merge_dnf_items_4_col(
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
            let current = selectivity(ctx, coll, &cnf).unwrap_or(ctx.options.selectivity_factor);
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

/// Go `colStats.IsHandle`: the table's single integer handle. `PseudoTable`
/// derives it from `PKIsHandle && HasPriKeyFlag`.
fn column_is_handle(coll: &HistColl, column: &Column, field_type: &tidb_datatype::FieldType) -> bool {
    coll.pk_is_handle()
        && coll.column(column.unique_id).map_or(
            field_type.flags() & tidb_datatype::FieldTypeFlags::PRI_KEY != 0,
            |stats| stats.is_handle,
        )
}

/// Go `getMaskAndRanges(..., ranger.ColumnRangeType, ...)`.
fn column_mask_and_ranges(
    ctx: &SelectivityContext<'_>,
    exprs: &[Expression],
    column: &Column,
    field_type: &tidb_datatype::FieldType,
) -> Result<(i64, Vec<ColumnRange>), EstimationError> {
    let access: Vec<Expression> = crate::ranger::detacher::extract_access_conditions_for_column(
        exprs,
        column,
        ctx.options.opt_prefix_index_single_scan,
    )
    .into_iter()
    .cloned()
    .collect();
    let built = crate::ranger::ranger::build_column_range_in(
        &access,
        field_type,
        crate::ranger::checker::UNSPECIFIED_LENGTH,
        ctx.options.range_max_size,
        ctx.evaluate,
    )?;
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
) -> Result<crate::ranger::detacher::DetachRangeResult, PointBuilderError> {
    match ctx.range_fallback_handler {
        Some(handler) => crate::ranger::detacher::detach_index_range_with_fallback_handler_in(
            exprs,
            columns,
            lengths,
            ctx.options.range_max_size,
            handler,
            ctx.evaluate,
        ),
        None => crate::ranger::detacher::detach_cond_and_build_range_for_index_in(
            exprs,
            columns,
            lengths,
            ctx.options.range_max_size,
            ctx.evaluate,
        ),
    }
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

/// Go `GetRowCountByIndexRanges(sctx, coll, idxID, ranges, idxCols).Est`.
fn get_row_count_by_index_ranges(
    ctx: &SelectivityContext<'_>,
    coll: &HistColl,
    index_id: i64,
    ranges: &[IndexRangeDatums],
    index_columns: &[Column],
) -> Result<f64, EstimationError> {
    let mut stats = coll.index_estimation_stats(index_id);
    // `hasColumnStats` and the partial-statistics estimate read `idxCols`,
    // the matched prefix, not every declared column.
    stats.columns.truncate(index_columns.len());
    let recursive = coll
        .index_columns(index_id)
        .iter()
        .map(|id| {
            coll.index_ids_for_column(*id)
                .iter()
                .map(|candidate| coll.index_estimation_stats(*candidate))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let virtual_columns = index_columns
        .iter()
        .map(|column| column.virtual_expr.is_some())
        .collect::<Vec<_>>();
    Ok(get_index_row_count(&stats, &virtual_columns, &recursive, ranges, ctx.options)?.est)
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
    let Some(stats) = find_available_stats_for_col(coll, column.unique_id) else {
        return Ok(None);
    };
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
        let result = (ctx.evaluate)(&substituted).map_err(PointBuilderError::Eval)?;
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

/// The statistics `findAvailableStatsForCol` settles on.
struct FilterStats<'a> {
    stats_ver: i64,
    histogram: &'a tidb_stats::histogram::Histogram,
    topn: Option<&'a tidb_stats::cmsketch::TopN>,
}

/// Go `findAvailableStatsForCol`: the column's own fully loaded statistics,
/// else a fully loaded single-column, non-prefix index on it.
fn find_available_stats_for_col(coll: &HistColl, unique_id: i64) -> Option<FilterStats<'_>> {
    if let Some(column) = coll
        .histogram_for_estimation(unique_id)
        .filter(|_| coll.column_is_full_load(unique_id))
    {
        return Some(FilterStats {
            stats_ver: column.stats_ver,
            histogram: &column.histogram,
            topn: column.topn.as_ref(),
        });
    }
    coll.index_ids().into_iter().find_map(|index_id| {
        if coll.index_columns(index_id) != [unique_id] {
            return None;
        }
        let prefix = coll
            .index_info(index_id)
            .and_then(|info| info.column_lengths.first().copied())
            .is_some_and(|length| length != crate::ranger::checker::UNSPECIFIED_LENGTH);
        if prefix || coll.pseudo() || !coll.index_is_full_load(index_id) {
            return None;
        }
        let index = coll
            .index_histogram(index_id)
            .filter(|index| index.total_row_count() != 0.0)?;
        Some(FilterStats {
            stats_ver: index.stats_ver,
            histogram: &index.histogram,
            topn: index.topn.as_ref(),
        })
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
