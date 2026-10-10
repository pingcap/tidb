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
    /// Go's MV alternative (`buildIntoAccessPath`'s MV arm): one partial path
    /// per value over a multi-valued index, whose handles unite.
    MvIndex {
        index_pos: usize,
        paths: Vec<IndexMergePartial>,
        keep_source_filter: bool,
    },
}

impl IndexMergePartial {
    /// Go `CountAfterAccess`.
    pub(crate) fn rows(&self) -> f64 {
        self.path.count_after_access().unwrap_or(0.0)
    }

    /// The partial's filled ranges and filters.
    pub(crate) fn filled(&self) -> &super::ordinary::FilledIndexPath {
        self.path
            .filled
            .as_ref()
            .expect("index merge partials are filled logical paths")
    }
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
    // Go `generateORIndexMerge` reads `indexMergeConds` = `AllConds`, so an
    // OR list that cannot be pushed (`json_overlaps(...) or ...`) still builds
    // an IndexMerge, the list itself staying in the Selection above.
    let mut paths = Vec::new();
    for position in 0..ds.all_conds.len() {
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
    let Some(Expression::ScalarFunction(or_func)) = ds.all_conds.get(or_position) else {
        return Ok(None);
    };
    if or_func.func_name.lowercase() != "or" {
        return Ok(None);
    }
    let table_filters = ds
        .all_conds
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
    for disjunct in &disjuncts {
        let mut branch = Vec::new();
        // Go walks `PossibleAccessPaths` in order, the table path first, and
        // `cmpAlternatives` keeps the first of equal alternatives: a table
        // partial wins a row-count tie against an index.
        // Go `accessPathsForConds` over the table path: an integer handle
        // (`IsIntHandlePath`) or a clustered common handle
        // (`IsCommonHandlePath`, ranged over its PRIMARY index's columns).
        let table_primary = ds.enumerated_paths.iter().find_map(|path| match path {
            crate::access_path::PossiblePath::Table {
                is_int_handle: true,
                ..
            } if ds.handle_is_int && !ds.handle_cols.is_empty() => Some(None),
            crate::access_path::PossiblePath::Table {
                is_int_handle: false,
                primary_index: Some(primary),
            } if ds.is_common_handle => Some(Some(*primary)),
            _ => None,
        });
        if let Some(primary) = table_primary.filter(|_| index_merge_hint_allows(ds, "primary")) {
            let primary_index = primary.and_then(|position| ds.indexes.get(position));
            let collected = collect_unfinished_filters(ds, primary_index, disjunct, ctx);
            if let Some((mut usable, keep_branch_filter)) = collected {
                for filter in &table_filters {
                    if let Some((filters, _)) =
                        collect_unfinished_filters(ds, primary_index, filter, ctx)
                    {
                        usable.extend(filters);
                    }
                }
                let (pushable, rejected) = partition_partial_filters(&usable, ctx);
                if let Ok(mut filled) =
                    super::ordinary::detach_table_path(ds, primary, &pushable, ctx, false)
                        .and_then(|mut filled| {
                            filled.count_after_access = Some(match primary_index {
                                // Go `deriveCommonHandleTablePathStats`: the
                                // PRIMARY index's histogram over these ranges.
                                Some(index) => estimate_index_ranges(
                                    ds,
                                    index,
                                    &filled.detached.ranges,
                                    &filled.common_columns,
                                    ctx,
                                )?,
                                None => super::ordinary::estimate_int_table_path(ds, &filled, ctx)?,
                            });
                            Ok(filled)
                        })
                {
                    // Go accessPathsForConds drops only a full range; an
                    // empty one (`a between 2 and 1`) stays a partial.
                    let unsigned_int_handle = primary.is_none()
                        && ds.pk_is_handle
                        && filled.handle_type.is_unsigned();
                    if !filled
                        .detached
                        .ranges
                        .iter()
                        .any(|range| range.is_full_range(unsigned_int_handle))
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
        for (index_pos, source_index) in ds.indexes.iter().enumerate() {
            if !enumerated_indexes[index_pos]
                || source_index.is_columnar
                || !source_index.condition_expr_string.is_empty()
            {
                continue;
            }
            if source_index.is_multi_valued {
                if !index_merge_hint_allows(ds, &source_index.name) {
                    continue;
                }
                // Go initUnfinishedPathsFromExpr, then handleTopLevelANDList,
                // then buildIntoAccessPath, for an MV candidate.
                let Some(mut unfinished) =
                    super::mv_index::init_unfinished_mv_path(ds, ctx, index_pos, disjunct)
                else {
                    continue;
                };
                for filter in &table_filters {
                    if let Some(item) =
                        super::mv_index::init_unfinished_mv_path(ds, ctx, index_pos, filter)
                    {
                        unfinished.merge_and_item(item);
                    }
                }
                if let Some((paths, keep_source_filter)) = super::mv_index::build_mv_alternative(
                    ds,
                    ctx,
                    index_pos,
                    &unfinished,
                    use_plan_cache,
                )? {
                    branch.push(Partial::MvIndex {
                        index_pos,
                        paths,
                        keep_source_filter,
                    });
                }
                continue;
            }
            // Go folds a clustered PRIMARY key into the table path
            // (`getPossibleAccessPaths`), never a secondary index partial; the
            // table partial above builds that disjunct's TableRangeScan.
            if source_index.primary && (ds.handle_is_int || ds.is_common_handle) {
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
                false,
            ) else {
                // Go accessPathsForConds declines a partial when derivation fails.
                continue;
            };
            let result = &filled.detached;
            // Go keeps a partial whose ranges came out empty: it reads
            // nothing, and only a full range disqualifies a partial.
            if result.ranges.iter().any(|range| range.is_full_range(false)) {
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
        if branch.is_empty() {
            return Ok(None);
        }
        alternatives.push(branch);
    }

    // Go `buildIntoAccessPath`: without an MV index, a union that reads
    // through at most one access object -- the table counting as one --
    // adds nothing over that object's own path.
    let mut possible_ids = std::collections::BTreeSet::new();
    let mut contain_mv_path = false;
    for partial in alternatives.iter().flatten() {
        match partial {
            Partial::Index { index_pos, .. } | Partial::MvIndex { index_pos, .. } => {
                if let Some(index) = ds.indexes.get(*index_pos) {
                    possible_ids.insert(index.id);
                    contain_mv_path |= index.is_multi_valued;
                }
            }
            Partial::Table { .. } => {
                possible_ids.insert(-1);
            }
        }
    }
    if !contain_mv_path && possible_ids.len() <= 1 {
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
        source_filter: ds.all_conds[or_position].clone(),
        table_filters,
        use_plan_cache,
    }))
}

pub(crate) fn estimate_union_access(
    ds: &DataSource,
    selected: &[&Partial],
    ctx: &AccessPathDerivationContext<'_>,
) -> f64 {
    // Go `estimateCountAfterAccessForIndexMergeOR`: with an MV partial the
    // decided paths unite by `CalcTotalSelectivityForMVIdxPath`.
    if selected
        .iter()
        .any(|partial| matches!(partial, Partial::MvIndex { .. }))
    {
        let rows = selected.iter().flat_map(|partial| match partial {
            Partial::Index { rows, .. } | Partial::Table { rows, .. } => vec![*rows],
            Partial::MvIndex { paths, .. } => paths.iter().map(IndexMergePartial::rows).collect(),
        });
        return ds.table_stats.as_ref().map_or(0.0, |stats| stats.row_count())
            * super::mv_index::union_selectivity(ds, rows);
    }
    let mut total_rows = selected
        .iter()
        .map(|partial| match partial {
            Partial::Index { rows, .. } | Partial::Table { rows, .. } => *rows,
            Partial::MvIndex { paths, .. } => paths.iter().map(IndexMergePartial::rows).sum(),
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
            Partial::MvIndex { paths, .. } => tidb_expr::simple_expr::compose_dnf_condition(
                paths
                    .iter()
                    .filter_map(|path| {
                        tidb_expr::simple_expr::compose_cnf_condition(
                            path.filled().detached.access_conds.clone(),
                        )
                    })
                    .collect(),
            ),
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
            false,
        )
        .ok()?
        .detached
    } else {
        super::ordinary::detach_table_path(ds, None, &pushable, ctx, false)
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
            // Go cmpAlternatives: every path must be empty or unique-point,
            // and a multi-path alternative compares by its largest count.
            Partial::MvIndex {
                index_pos, paths, ..
            } => {
                let index = &ds.indexes[*index_pos];
                let point = paths.iter().all(|path| {
                    let ranges = &path.filled().detached.ranges;
                    ranges.is_empty()
                        || (index.unique
                            && ranges.iter().all(|range| {
                                range.is_point_non_nullable()
                                    && range.high_val.len() == index.columns.len()
                            }))
                });
                let rows = paths
                    .iter()
                    .map(|path| {
                        let filled = path.filled();
                        if filled.index_filters.is_empty() {
                            path.rows()
                        } else {
                            filled.count_after_index.unwrap_or(0.0)
                        }
                    })
                    .fold(0.0, f64::max);
                (point, rows, index.global)
            }
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

/// Go `detachCondAndBuildRangeForPath`'s `CountAfterAccess`
/// (`core/stats.go:425-447`): the appended handle is trimmed from `IdxCols`
/// and each range pruned to the declared columns before
/// `GetRowCountByIndexRanges`.
pub(super) fn estimate_partial_index_ranges(
    ds: &DataSource,
    source_index: &crate::plan_builder::catalog::SourceIndex,
    filled: &super::ordinary::FilledIndexPath,
    ctx: &AccessPathDerivationContext<'_>,
) -> Result<f64, PlanError> {
    estimate_index_ranges(ds, source_index, &filled.detached.ranges, &filled.columns, ctx)
}

/// Go `GetRowCountByIndexRanges` over `ranges` of `source_index`, whose
/// usable key columns are `columns`.
fn estimate_index_ranges(
    ds: &DataSource,
    source_index: &crate::plan_builder::catalog::SourceIndex,
    ranges: &crate::ranger::types::Ranges,
    columns: &[(tidb_expr::column::Column, i64)],
    ctx: &AccessPathDerivationContext<'_>,
) -> Result<f64, PlanError> {
    let declared = source_index.columns.len();
    let ranges = &super::ordinary::prune_estimate_range(ranges, declared);
    let Some(hist) = ds.table_stats.as_ref().and_then(|stats| stats.hist_coll()) else {
        // Go's table always carries a collection; a profile without one
        // keeps the pseudo rate.
        return Ok(crate::ranger::stats_bridge::pseudo_count_by_index_ranges(
            ranges,
            ds.table_stats
                .as_ref()
                .map_or(ranges.len() as f64, |stats| stats.row_count()),
            source_index.unique.then_some(source_index.columns.len()),
        ));
    };
    let columns = columns
        .iter()
        .take(declared)
        .map(|(column, _)| column.clone())
        .collect::<Vec<_>>();
    crate::cardinality::selectivity::get_row_count_by_index_ranges(
        hist,
        source_index.id,
        ranges,
        &columns,
        &ctx.estimator_options,
    )
    .map(|estimate| estimate.est)
    .map_err(|error| PlanError::internal_coded(error.to_string()))
}

#[derive(Clone, Debug)]
pub(crate) struct IndexMergePartial {
    pub(crate) index_pos: usize,
    pub(crate) path: super::IndexPathState,
}

/// Go's finished index-merge `AccessPath` (`PartialIndexPaths` set): the
/// partials are fixed at logical derivation, unlike the OR alternatives of
/// [`UnionIndexMergePath`] that converge only under physical properties.
#[derive(Clone, Debug)]
pub struct IndexMergePath {
    pub(crate) partials: Vec<IndexMergePartial>,
    pub(crate) count_after_access: f64,
    pub(crate) table_filters: Vec<Expression>,
    /// Go `IndexMergeIsIntersection`.
    pub(crate) is_intersection: bool,
    /// Go `IndexMergeAccessMVIndex`.
    pub(crate) access_mv_index: bool,
    /// Go `NoncacheableReason`/`SetSkipPlanCache` for a path whose ranges
    /// depend on parameter values.
    pub(crate) noncacheable_reason: Option<String>,
}

/// Go `generateNormalIndexPartialPath4And`.
pub(crate) fn generate_normal_index_partial_paths_for_and(
    ds: &DataSource,
    ctx: &AccessPathDerivationContext<'_>,
    used_access: &mut std::collections::HashMap<Vec<u8>, Expression>,
) -> Vec<IndexMergePartial> {
    generate_and_index_merge_for_normal_index(ds, ctx, false, used_access)
        .map(|path| path.partials)
        .unwrap_or_default()
}

/// Go `generateANDIndexMerge4NormalIndex`: the hinted intersection over
/// ordinary indexes. Composed with MV partials (`used_access` non-empty), a
/// path whose access conditions an MV partial already covers is skipped and
/// one path suffices.
pub(crate) fn prepare_intersection_index_merge_path(
    ds: &DataSource,
    ctx: &AccessPathDerivationContext<'_>,
    use_plan_cache: bool,
) -> Result<Option<IndexMergePath>, PlanError> {
    Ok(generate_and_index_merge_for_normal_index(
        ds,
        ctx,
        use_plan_cache,
        &mut std::collections::HashMap::new(),
    ))
}

fn generate_and_index_merge_for_normal_index(
    ds: &DataSource,
    ctx: &AccessPathDerivationContext<'_>,
    use_plan_cache: bool,
    used_access: &mut std::collections::HashMap<Vec<u8>, Expression>,
) -> Option<IndexMergePath> {
    use std::collections::HashSet;
    if ds.is_local_temporary
        || !ds
            .index_merge_hints
            .iter()
            .any(|hint| !hint.index_names.is_empty())
    {
        return None;
    }
    let composed_with_mv_index = !used_access.is_empty();
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
        if composed_with_mv_index {
            // For `(1 member of a) and c = 1 and d = 2` over mv(c, a), idx(c)
            // and idx(c, d), idx(c)'s access is already covered by the MV
            // partial; idx(c, d) adds `d = 2`.
            let access_hashes = filled
                .detached
                .access_conds
                .iter()
                .map(Expression::canonical_hash_code)
                .collect::<Vec<_>>();
            if access_hashes.iter().all(|hash| used_access.contains_key(hash)) {
                continue;
            }
            for (hash, access) in access_hashes.into_iter().zip(&filled.detached.access_conds) {
                used_access.entry(hash).or_insert_with(|| access.clone());
            }
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
        partials.push(IndexMergePartial { index_pos, path });
    }
    // Even a single normal path can be composed with MV partials.
    if partials.is_empty() || (partials.len() == 1 && !composed_with_mv_index) {
        return None;
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
    Some(IndexMergePath {
        partials,
        count_after_access,
        table_filters: final_filters,
        is_intersection: true,
        access_mv_index: false,
        noncacheable_reason: None,
    })
}
