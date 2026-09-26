// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Logical OR access alternatives from Go `indexmerge_path.go`.
//!
//! Derivation consumes expression, range and statistics inputs only. Physical
//! property convergence and task construction belong to `find_best_task`.

use super::AccessPathDerivationContext;
use crate::logical::DataSource;
use crate::plan_base::PlanError;
use tidb_expr::expression::Expression;

pub(crate) fn index_merge_hint_allows(ds: &DataSource, index_name: &str) -> bool {
    if ds.index_merge_hints.is_empty() {
        return true;
    }
    ds.index_merge_hints.iter().any(|hint| {
        hint.index_names.is_empty()
            || hint
                .index_names
                .iter()
                .any(|name| name.eq_ignore_ascii_case(index_name))
    })
}

/// Go `isSpecifiedInIndexMergeHints`: the normal-index intersection path only
/// considers indexes named by a USE_INDEX_MERGE hint, even if another merge
/// hint has no explicit index list.
pub(crate) fn index_merge_hint_specifies(ds: &DataSource, index_name: &str) -> bool {
    ds.index_merge_hints.iter().any(|hint| {
        !hint.index_names.is_empty()
            && hint
                .index_names
                .iter()
                .any(|name| name.eq_ignore_ascii_case(index_name))
    })
}

/// Go's union and intersection generators both consume the ordinary
/// session-filtered `PossibleAccessPaths` list.
pub(crate) fn enumerated_index_mask(ds: &DataSource) -> Vec<bool> {
    let mut indexes = vec![false; ds.indexes.len()];
    for path in &ds.enumerated_paths {
        if let crate::access_path::PossiblePath::Index { index } = path {
            if let Some(enumerated) = indexes.get_mut(*index) {
                *enumerated = true;
            }
        }
    }
    indexes
}

/// One viable partial access for a single disjunct.
#[derive(Clone, Debug)]
pub(crate) enum Partial {
    /// A secondary-index range scan; rows are (index cols..., handle...).
    Index {
        index_pos: usize,
        filled: super::ordinary::FilledIndexPath,
        rows: f64,
        keep_source_filter: bool,
    },
    /// A table-handle alternative retaining residuals until schema narrowing.
    Table {
        filled: super::ordinary::FilledTablePath,
        rows: f64,
        keep_source_filter: bool,
    },
}

/// Logical access ranges and counts, prepared before physical path costing.
#[derive(Clone, Debug)]
pub struct UnionIndexMergePath {
    pub(crate) alternatives: Vec<Vec<Partial>>,
    pub(crate) count_after_access: f64,
    pub(crate) source_filter: Expression,
    pub(crate) table_filters: Vec<Expression>,
    pub(crate) use_plan_cache: bool,
}

#[cfg(test)]
pub(crate) fn prepare_union_index_merge_path(
    ds: &DataSource,
    ctx: &AccessPathDerivationContext<'_>,
) -> Result<Option<UnionIndexMergePath>, PlanError> {
    prepare_union_index_merge_path_for_or(ds, ctx, 0, false)
}

pub(crate) fn prepare_union_index_merge_paths(
    ds: &DataSource,
    ctx: &AccessPathDerivationContext<'_>,
    use_plan_cache: bool,
) -> Result<Vec<UnionIndexMergePath>, PlanError> {
    let mut paths = Vec::new();
    for position in 0..ds.pushed_down_conds.len() {
        if let Some(path) =
            prepare_union_index_merge_path_for_or(ds, ctx, position, use_plan_cache)?
        {
            paths.push(path);
        }
    }
    Ok(paths)
}

fn prepare_union_index_merge_path_for_or(
    ds: &DataSource,
    ctx: &AccessPathDerivationContext<'_>,
    or_position: usize,
    use_plan_cache: bool,
) -> Result<Option<UnionIndexMergePath>, PlanError> {
    if ds.is_local_temporary {
        return Ok(None);
    }
    let Some(Expression::ScalarFunction(or_func)) = ds.pushed_down_conds.get(or_position) else {
        return Ok(None);
    };
    if or_func.func_name.lowercase() != "or" {
        return Ok(None);
    }
    let table_filters = ds
        .pushed_down_conds
        .iter()
        .enumerate()
        .filter(|(position, _)| *position != or_position)
        .map(|(_, condition)| condition.clone())
        .collect::<Vec<_>>();
    let disjuncts = tidb_expr::expr_util::normal_form::flatten_dnf_conditions(or_func);
    if disjuncts.len() < 2 {
        return Ok(None);
    }

    let enumerated_indexes = enumerated_index_mask(ds);
    let mut alternatives: Vec<Vec<Partial>> = Vec::with_capacity(disjuncts.len());
    let mut any_index_partial = false;
    for disjunct in &disjuncts {
        let mut branch = Vec::new();
        // Keep every ordinary alternative. Property convergence, rather than
        // catalog enumeration order, determines the physical partial later.
        for (index_pos, source_index) in ds.indexes.iter().enumerate() {
            if !enumerated_indexes[index_pos]
                || source_index.is_columnar
                || source_index.is_multi_valued
                || !source_index.condition_expr_string.is_empty()
            {
                continue;
            }
            // Go models an int-clustered table's PRIMARY key AS the handle,
            // never as a secondary index partial; the handle fallback below
            // builds that disjunct's TableRangeScan instead.
            if ds.handle_is_int && source_index.primary {
                continue;
            }
            if !index_merge_hint_allows(ds, &source_index.name) {
                continue;
            }
            let Some((mut usable, keep_branch_filter)) =
                collect_unfinished_filters(ds, Some(source_index), disjunct, ctx)
            else {
                continue;
            };
            for filter in &table_filters {
                if let Some((filters, _)) =
                    collect_unfinished_filters(ds, Some(source_index), filter, ctx)
                {
                    usable.extend(filters);
                }
            }
            let (pushable, rejected) = partition_partial_filters(&usable, ctx);
            let Ok(mut filled) = super::ordinary::fill_index_path(
                ds,
                source_index,
                &pushable,
                ctx,
                ctx.opt_prefix_index_single_scan,
            ) else {
                // Go accessPathsForConds declines a partial when derivation fails.
                continue;
            };
            let result = &filled.detached;
            if result.ranges.is_empty()
                || result.ranges.iter().any(|range| range.is_full_range(false))
            {
                continue;
            }
            let Ok(rows) = estimate_partial_index_ranges(ds, source_index, &filled, ctx) else {
                continue;
            };
            let mut keep_source_filter =
                keep_branch_filter || !rejected.is_empty() || !filled.table_filters.is_empty();
            filled.table_filters.clear();
            if !crate::pushdown::can_exprs_push_down(
                &filled.index_filters,
                tidb_expr::infer_pushdown::PushDownStore::TiKv,
                ctx.expr_pushdown_blacklist,
            ) {
                keep_source_filter = true;
                filled.index_filters.clear();
            }
            filled.count_after_index =
                Some(rows * super::ordinary::filter_selectivity(ds, &filled.index_filters, ctx));
            branch.push(Partial::Index {
                index_pos,
                filled,
                rows,
                keep_source_filter,
            });
        }
        if ds.handle_is_int
            && !ds.handle_cols.is_empty()
            && index_merge_hint_allows(ds, "primary")
            && ds.enumerated_paths.iter().any(|path| {
                matches!(
                    path,
                    crate::access_path::PossiblePath::Table {
                        is_int_handle: true,
                        ..
                    }
                )
            })
        {
            let collected = collect_unfinished_filters(ds, None, disjunct, ctx);
            if let Some((mut usable, keep_branch_filter)) = collected {
                for filter in &table_filters {
                    if let Some((filters, _)) = collect_unfinished_filters(ds, None, filter, ctx) {
                        usable.extend(filters);
                    }
                }
                let (pushable, rejected) = partition_partial_filters(&usable, ctx);
                if let Ok(mut filled) = super::ordinary::detach_table_path(ds, None, &pushable, ctx)
                    .and_then(|mut filled| {
                        filled.count_after_access =
                            Some(super::ordinary::estimate_int_table_path(ds, &filled, ctx)?);
                        Ok(filled)
                    })
                {
                    if !filled.detached.ranges.is_empty()
                        && !filled
                            .detached
                            .ranges
                            .iter()
                            .any(|range| range.is_full_range(false))
                    {
                        let rows = filled
                            .count_after_access
                            .expect("table partial was estimated");
                        let result = &mut filled.detached;
                        // Go conservatively rechecks the source OR even when the
                        // table partial can evaluate its residuals: conversion
                        // narrows the partial schema to handles only.
                        let keep_source_filter = keep_branch_filter
                            || !rejected.is_empty()
                            || !result.remained_conds.is_empty();
                        if !crate::pushdown::can_exprs_push_down(
                            &result.remained_conds,
                            tidb_expr::infer_pushdown::PushDownStore::TiKv,
                            ctx.expr_pushdown_blacklist,
                        ) {
                            result.remained_conds.clear();
                        }
                        branch.push(Partial::Table {
                            filled,
                            rows,
                            keep_source_filter,
                        });
                    }
                }
            }
        }
        if branch.is_empty() {
            return Ok(None);
        }
        any_index_partial |= branch
            .iter()
            .any(|partial| matches!(partial, Partial::Index { .. }));
        alternatives.push(branch);
    }

    // A union whose partials include an index scan must fetch rows by
    // handle; all-table partials need no final row fetch — and add no
    // value over the ordinary handle-range path, so leave those to it.
    if !any_index_partial {
        return Ok(None);
    }

    let selected = alternatives
        .iter()
        .map(|branch| {
            branch
                .iter()
                .min_by(|left, right| compare_alternatives(ds, left, right, false))
                .expect("nonempty branch")
        })
        .collect::<Vec<_>>();
    let total_rows = estimate_union_access(ds, &selected, ctx);

    Ok(Some(UnionIndexMergePath {
        alternatives,
        count_after_access: total_rows,
        source_filter: ds.pushed_down_conds[or_position].clone(),
        table_filters,
        use_plan_cache,
    }))
}

pub(crate) fn estimate_union_access(
    ds: &DataSource,
    selected: &[&Partial],
    ctx: &AccessPathDerivationContext<'_>,
) -> f64 {
    let mut total_rows = selected
        .iter()
        .map(|partial| match partial {
            Partial::Index { rows, .. } | Partial::Table { rows, .. } => *rows,
        })
        .sum::<f64>();
    let access_branches = selected
        .iter()
        .map(|partial| match partial {
            Partial::Index { filled, .. } => {
                let mut conditions = filled.detached.access_conds.clone();
                conditions.extend(filled.index_filters.iter().cloned());
                tidb_expr::simple_expr::compose_cnf_condition(conditions)
            }
            Partial::Table { filled, .. } => {
                tidb_expr::simple_expr::compose_cnf_condition(filled.detached.access_conds.clone())
            }
        })
        .collect::<Option<Vec<_>>>();
    if let Some(stats) = &ds.table_stats {
        let access_dnf = access_branches.and_then(tidb_expr::simple_expr::compose_dnf_condition);
        // Estimate only the predicates enforced before the row-ID probe.
        // Residual source-OR predicates must not reduce this access count.
        if let Some(access_dnf) = access_dnf {
            total_rows =
                stats.row_count() * super::ordinary::filter_selectivity(ds, &[access_dnf], ctx);
        }
    }

    total_rows
}

fn partition_partial_filters(
    conditions: &[Expression],
    ctx: &AccessPathDerivationContext<'_>,
) -> (Vec<Expression>, Vec<Expression>) {
    conditions
        .iter()
        .flat_map(tidb_expr::expr_util::normal_form::split_cnf_items)
        .partition(|condition| {
            crate::pushdown::can_exprs_push_down(
                std::slice::from_ref(condition),
                tidb_expr::infer_pushdown::PushDownStore::TiKv,
                ctx.expr_pushdown_blacklist,
            )
        })
}

/// Go's normal-index unfinished path: retain an immediately usable range, or
/// collect one equality/IN per index column until top-level AND predicates
/// supply a missing leading key. Keep incomplete branches until that stage.
fn collect_unfinished_filters(
    ds: &DataSource,
    index: Option<&crate::plan_builder::catalog::SourceIndex>,
    expression: &Expression,
    ctx: &AccessPathDerivationContext<'_>,
) -> Option<(Vec<Expression>, bool)> {
    let conditions = tidb_expr::expr_util::normal_form::split_cnf_items(expression);
    let (pushable, rejected) = partition_partial_filters(&conditions, ctx);
    let detached = if let Some(index) = index {
        super::ordinary::fill_index_path(
            ds,
            index,
            &pushable,
            ctx,
            ctx.opt_prefix_index_single_scan,
        )
        .ok()?
        .detached
    } else {
        super::ordinary::detach_table_path(ds, None, &pushable, ctx)
            .ok()?
            .detached
    };
    if !detached
        .ranges
        .iter()
        .any(|range| range.is_full_range(false))
    {
        return Some((vec![expression.clone()], !rejected.is_empty()));
    }
    let index = index?;
    let mut collected = vec![false; conditions.len()];
    let mut filters = Vec::new();
    for key in &index.columns {
        let Some(column) = ds.table_columns.get(key.offset) else {
            continue;
        };
        if column.virtual_expr.is_some() {
            continue;
        }
        for (position, condition) in conditions.iter().enumerate() {
            if collected[position] {
                continue;
            }
            let Expression::ScalarFunction(function) = condition else {
                continue;
            };
            let is_column = |expression: &Expression| matches!(expression, Expression::Column(candidate) if candidate.unique_id == column.unique_id);
            let is_constant =
                |expression: &Expression| matches!(expression, Expression::Constant(_));
            let args = &function.args;
            let usable = match function.func_name.lowercase() {
                "eq" if args.len() == 2 => {
                    (is_column(&args[0]) && is_constant(&args[1]))
                        || (is_column(&args[1]) && is_constant(&args[0]))
                }
                "in" if args.len() >= 2 => is_column(&args[0]) && args[1..].iter().all(is_constant),
                _ => false,
            };
            if usable {
                collected[position] = true;
                filters.push(condition.clone());
                break;
            }
        }
    }
    (!filters.is_empty()).then(|| (filters, !rejected.is_empty() || collected.contains(&false)))
}

// Go cmpAlternatives prefers empty/unique-point alternatives before row
// count. Physical convergence additionally prefers global indexes on ties.
pub(crate) fn compare_alternatives(
    ds: &DataSource,
    left: &Partial,
    right: &Partial,
    physical: bool,
) -> std::cmp::Ordering {
    let attributes = |partial: &Partial| {
        let (point, rows, global) = match partial {
            Partial::Index {
                index_pos,
                filled,
                rows,
                ..
            } => {
                let ranges = &filled.detached.ranges;
                let index = &ds.indexes[*index_pos];
                let point = ranges.is_empty()
                    || (index.unique
                        && ranges.iter().all(|range| {
                            range.is_point_non_nullable()
                                && range.high_val.len() == index.columns.len()
                        }));
                (point, filled.count_after_index.unwrap_or(*rows), index.global)
            }
            Partial::Table { filled, rows, .. } => (
                filled.detached.ranges.iter().all(|range| range.is_point_nullable()),
                *rows,
                false,
            ),
        };
        (point, rows, global)
    };
    let (left_point, left_rows, left_global) = attributes(left);
    let (right_point, right_rows, right_global) = attributes(right);
    right_point
        .cmp(&left_point)
        .then_with(|| left_rows.total_cmp(&right_rows))
        .then_with(|| {
            if physical {
                right_global.cmp(&left_global)
            } else {
                std::cmp::Ordering::Equal
            }
        })
}

fn estimate_partial_index_ranges(
    ds: &DataSource,
    source_index: &crate::plan_builder::catalog::SourceIndex,
    filled: &super::ordinary::FilledIndexPath,
    ctx: &AccessPathDerivationContext<'_>,
) -> Result<f64, PlanError> {
    use crate::cardinality::row_count_estimator::{get_row_count_by_column_ranges, ColumnRange};
    let hist = ds.table_stats.as_ref().and_then(|stats| stats.hist_coll());
    crate::cardinality::estimate_index_path_ranges(
        &filled.detached.ranges,
        source_index.columns.len(),
        filled.columns.len(),
        ds.table_stats
            .as_ref()
            .map_or(0.0, |stats| stats.row_count()),
        |ranges| estimate_partial_index_prefix(ds, source_index, filled, ctx, ranges),
        |dimension, ranges| {
            let hist = hist.filter(|hist| !hist.pseudo())?;
            let column = hist.histogram_for_estimation(filled.columns[dimension].0.unique_id)?;
            let bounds = ranges
                .iter()
                .map(|range| {
                    ColumnRange::new(
                        range.low_val[0].clone(),
                        range.high_val[0].clone(),
                        range.low_exclude,
                        range.high_exclude,
                    )
                })
                .collect::<Vec<_>>();
            get_row_count_by_column_ranges(
                Some(column),
                &bounds,
                ranges.first()?.collators[0],
                hist.realtime_count(),
                hist.modify_count(),
                false,
                ctx.estimator_options,
            )
            .ok()
        },
    )
    .map(|estimate| estimate.est)
    .map_err(|error| PlanError::internal_coded(error.to_string()))
}

fn estimate_partial_index_prefix(
    ds: &DataSource,
    source_index: &crate::plan_builder::catalog::SourceIndex,
    filled: &super::ordinary::FilledIndexPath,
    ctx: &AccessPathDerivationContext<'_>,
    ranges: &[crate::ranger::Range],
) -> Result<
    crate::cardinality::row_count_column::RowEstimate,
    crate::cardinality::row_count_estimator::EstimationError,
> {
    use crate::cardinality::row_count_estimator::{
        get_index_row_count, get_index_row_count_with_partial_stats,
    };
    let stats = ds.table_stats.as_ref();
    let pseudo = || {
        crate::ranger::stats_bridge::pseudo_count_by_index_ranges(
            ranges,
            stats.map_or(ranges.len() as f64, |stats| stats.row_count()),
            source_index.unique.then_some(source_index.columns.len()),
        )
    };
    let Some(hist) = stats
        .and_then(|stats| stats.hist_coll())
        .filter(|hist| !hist.pseudo())
    else {
        return Ok(crate::cardinality::row_count_column::RowEstimate::default_est(pseudo()));
    };
    let index_id = source_index.id;
    let ids = filled
        .columns
        .iter()
        .take(source_index.columns.len())
        .map(|(column, _)| column.unique_id)
        .collect::<Vec<_>>();
    let columns = ids
        .iter()
        .map(|id| {
            hist.histogram_for_estimation(*id)
                .map(|column| column.as_ref())
        })
        .collect::<Vec<_>>();
    let Some(_index) = hist
        .index_histogram(index_id)
        .filter(|index| index.total_row_count() != 0.0)
    else {
        if columns
            .iter()
            .any(|column| column.is_some_and(|column| column.total_row_count() != 0.0))
        {
            let columns = columns
                .iter()
                .zip(&filled.columns)
                .map(|(stats, (column, _))| {
                    (
                        *stats,
                        column
                            .ret_type
                            .as_ref()
                            .map_or(tidb_datatype::Collation::Binary, |ty| ty.collation()),
                    )
                })
                .collect::<Vec<_>>();
            if let Some(estimate) = get_index_row_count_with_partial_stats(
                &columns,
                ranges,
                hist.realtime_count(),
                hist.modify_count(),
                ctx.estimator_options,
            )? {
                return Ok(estimate);
            }
        }
        return Ok(crate::cardinality::row_count_column::RowEstimate::default_est(pseudo()));
    };
    let context = hist.index_estimation_stats(index_id);
    let recursive = hist
        .index_columns(index_id)
        .iter()
        .map(|id| {
            hist.index_ids_for_column(*id)
                .iter()
                .map(|candidate| hist.index_estimation_stats(*candidate))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let virtual_columns = ids
        .iter()
        .map(|id| {
            ds.table_columns
                .iter()
                .find(|column| column.unique_id == *id)
                .is_some_and(|column| column.virtual_expr.is_some())
        })
        .collect::<Vec<_>>();
    get_index_row_count(
        &context,
        &virtual_columns,
        &recursive,
        ranges,
        ctx.estimator_options,
    )
}

#[derive(Clone, Debug)]
pub(crate) struct IntersectionPartial {
    pub(crate) index_pos: usize,
    pub(crate) path: super::IndexPathState,
}

/// A logically derived intersection candidate, before physical properties.
#[derive(Clone, Debug)]
pub struct IntersectionIndexMergePath {
    pub(crate) partials: Vec<IntersectionPartial>,
    pub(crate) count_after_access: f64,
    pub(crate) table_filters: Vec<Expression>,
}

pub(crate) fn prepare_intersection_index_merge_path(
    ds: &DataSource,
    ctx: &AccessPathDerivationContext<'_>,
    use_plan_cache: bool,
) -> Result<Option<IntersectionIndexMergePath>, PlanError> {
    use std::collections::HashSet;
    if ds.is_local_temporary
        || !ds
            .index_merge_hints
            .iter()
            .any(|hint| !hint.index_names.is_empty())
    {
        return Ok(None);
    }
    let enumerated = enumerated_index_mask(ds);
    let mut partials = Vec::new();
    let mut final_filters = Vec::new();
    let mut partial_filters = Vec::new();
    let mut covered = HashSet::new();
    for (index_pos, index) in ds.indexes.iter().enumerate() {
        if !enumerated[index_pos]
            || index.is_multi_valued
            || index.is_columnar
            || !index.condition_expr_string.is_empty()
            || !index_merge_hint_specifies(ds, &index.name)
        {
            continue;
        }
        let Some(mut path) = ds.derived_index_paths.get(&index.id).cloned() else {
            continue;
        };
        let Some(filled) = &mut path.filled else {
            continue;
        };
        if crate::ranger::types::has_full_range(&filled.detached.ranges, false) {
            continue;
        }
        // Go clones ordinary paths and retains only pushable index filters.
        // A prefix recheck can occur in access conditions AND table filters;
        // it must never be marked covered merely by the access condition.
        let mut not_covered = filled.table_filters.clone();
        filled.index_filters.retain(|condition| {
            let pushable =
                crate::pushdown::can_exprs_push_down(
                    std::slice::from_ref(condition),
                    tidb_expr::infer_pushdown::PushDownStore::TiKv,
                    ctx.expr_pushdown_blacklist,
                );
            if !pushable {
                not_covered.push(condition.clone());
            }
            pushable
        });
        let covered_conditions = filled
            .detached
            .access_conds
            .iter()
            .chain(&filled.index_filters);
        let not_covered_hashes = not_covered
            .iter()
            .map(Expression::canonical_hash_code)
            .collect::<HashSet<_>>();
        for condition in covered_conditions {
            let hash = condition.canonical_hash_code();
            if !not_covered_hashes.contains(&hash) {
                covered.insert(hash);
            }
            partial_filters.push(condition.clone());
        }
        final_filters.extend(not_covered);
        partials.push(IntersectionPartial { index_pos, path });
    }
    if partials.len() < 2 {
        return Ok(None);
    }
    final_filters.retain(|condition| covered.insert(condition.canonical_hash_code()));
    if tidb_expr::expr_util::predicates::maybe_over_optimized_4_plan_cache(
        use_plan_cache,
        &partial_filters,
    ) {
        final_filters.extend(partial_filters.iter().cloned());
    }
    let count_after_access = ds
        .table_stats
        .as_ref()
        .map_or(0.0, |stats| stats.row_count())
        * super::ordinary::filter_selectivity(ds, &partial_filters, ctx);
    Ok(Some(IntersectionIndexMergePath {
        partials,
        count_after_access,
        table_filters: final_filters,
    }))
}
