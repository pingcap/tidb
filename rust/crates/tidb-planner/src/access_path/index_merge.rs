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

use tidb_planner_util::path::AccessPath;
use super::AccessPathDerivationContext;
use tidb_planner_core_operator_logicalop::DataSource;
use tidb_planner_core_base::PlanError;
use tidb_expr::expression::Expression;

/// Go `isInIndexMergeHints`. Go passes the index's `Name.L` and compares
/// with `strings.EqualFold` against each hint name as written.
pub(crate) fn is_in_index_merge_hints(ds: &DataSource, index_name: &str) -> bool {
    if ds.index_merge_hints.is_empty() {
        return true;
    }
    let index_name = tidb_mysql::to_lowercase(index_name);
    ds.index_merge_hints.iter().any(|hint| {
        hint.index_names.is_empty()
            || hint
                .index_names
                .iter()
                .any(|name| tidb_mysql::equal_fold(&index_name, name))
    })
}

/// Go `isSpecifiedInIndexMergeHints`: the normal-index intersection path only
/// considers indexes named by a USE_INDEX_MERGE hint, even if another merge
/// hint has no explicit index list.
pub(crate) fn is_specified_in_index_merge_hints(ds: &DataSource, index_name: &str) -> bool {
    let index_name = tidb_mysql::to_lowercase(index_name);
    ds.index_merge_hints.iter().any(|hint| {
        !hint.index_names.is_empty()
            && hint
                .index_names
                .iter()
                .any(|name| tidb_mysql::equal_fold(&index_name, name))
    })
}

/// Go's union and intersection generators both walk the ordinary
/// session-filtered `PossibleAccessPaths` list; these are its index paths'
/// `path.Index`, in path order.
pub(crate) fn enumerated_indexes(
    ds: &DataSource,
) -> impl Iterator<Item = &tidb_model::GoShared<tidb_model::IndexInfo>> {
    ds.all_possible_access_paths.iter().filter_map(tidb_planner_util::path::AccessPath::index_info)
}

#[cfg(test)]
pub(crate) fn prepare_union_index_merge_path(
    ds: &DataSource,
    ctx: &AccessPathDerivationContext<'_>,
) -> Result<Option<AccessPath>, PlanError> {
    prepare_union_index_merge_path_for_or(ds, ctx, 0, false)
}

pub(crate) fn prepare_union_index_merge_paths(
    ds: &DataSource,
    ctx: &AccessPathDerivationContext<'_>,
    use_plan_cache: bool,
) -> Result<Vec<AccessPath>, PlanError> {
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
) -> Result<Option<AccessPath>, PlanError> {
    if ds.is_local_temporary() {
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

    let mut alternatives: Vec<Vec<Vec<AccessPath>>> = Vec::with_capacity(disjuncts.len());
    for disjunct in &disjuncts {
        let mut branch = Vec::new();
        // Go walks `PossibleAccessPaths` in order, the table path first, and
        // `cmpAlternatives` keeps the first of equal alternatives: a table
        // partial wins a row-count tie against an index.
        // Go `accessPathsForConds` over the table path: an integer handle
        // (`IsIntHandlePath`) or a clustered common handle
        // (`IsCommonHandlePath`, ranged over its PRIMARY index's columns).
        let table_primary = ds
            .all_possible_access_paths
            .iter()
            .filter(|path| path.is_tikv_table_path())
            .find_map(|path| {
                if path.is_int_handle_path && ds.handle_is_int && !ds.handle_cols.is_empty() {
                    Some(None)
                } else if path.is_common_handle_path && ds.is_common_handle() {
                    path.index.as_ref().map(Some)
                } else {
                    None
                }
            });
        if let Some(primary) = table_primary.filter(|_| is_in_index_merge_hints(ds, "primary")) {
            let primary_index = primary;
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
                let mut path = AccessPath::table_path(
                    tidb_kv::store_type::StoreType::TiKv,
                    primary.is_some(),
                    primary.cloned(),
                );
                let estimated =
                    super::ordinary::detach_table_path(ds, &mut path, &pushable, ctx, false)
                        .and_then(|()| {
                            Ok(match primary_index {
                                // Go `deriveCommonHandleTablePathStats`: the
                                // PRIMARY index's histogram over these ranges.
                                Some(index) => estimate_index_ranges(
                                    ds,
                                    index,
                                    &path.ranges,
                                    &path.idx_cols,
                                    ctx,
                                )?
                                .est,
                                None => super::ordinary::estimate_int_table_path(ds, &path, ctx)?,
                            })
                        });
                if let Ok(rows) = estimated {
                    // Go accessPathsForConds drops only a full range; an
                    // empty one (`a between 2 and 1`) stays a partial.
                    let unsigned_int_handle = primary.is_none()
                        && ds.pk_is_handle()
                        && super::ordinary::table_path_handle(ds).1.is_unsigned();
                    if !path
                        .ranges
                        .iter()
                        .any(|range| range.is_full_range(unsigned_int_handle))
                    {
                        // Go conservatively rechecks the source OR even when the
                        // table partial can evaluate its residuals: conversion
                        // narrows the partial schema to handles only.
                        let keep_source_filter = keep_branch_filter
                            || !rejected.is_empty()
                            || !path.table_filters.is_empty();
                        if !tidb_expr::infer_pushdown::can_exprs_push_down(
                            &path.table_filters,
                            tidb_kv::store_type::StoreType::TiKv,
                            ctx.expr_pushdown_blacklist,
                        ) {
                            path.table_filters.clear();
                        }
                        path.row_estimate = Some(tidb_stats::RowEstimate::new(rows, rows, rows));
                        path.keep_index_merge_or_source_filter = keep_source_filter;
                        branch.push(vec![path]);
                    }
                }
            }
        }
        for source_index in enumerated_indexes(ds) {
            if source_index.read().is_columnar_index()
                || !source_index.read().condition_expr_string.is_empty()
            {
                continue;
            }
            if source_index.read().mv_index {
                if !is_in_index_merge_hints(ds, source_index.read().name.original()) {
                    continue;
                }
                // Go initUnfinishedPathsFromExpr, then handleTopLevelANDList,
                // then buildIntoAccessPath, for an MV candidate.
                let Some(mut unfinished) =
                    super::mv_index::init_unfinished_paths_from_expr(ds, ctx, source_index, disjunct)
                else {
                    continue;
                };
                for filter in &table_filters {
                    if let Some(item) =
                        super::mv_index::init_unfinished_paths_from_expr(ds, ctx, source_index, filter)
                    {
                        unfinished.merge_and_item_into_unfinished_index_merge_path(item);
                    }
                }
                if let Some((mut paths, keep_source_filter)) = super::mv_index::build_mv_alternative(
                    ds,
                    ctx,
                    source_index,
                    &unfinished,
                    use_plan_cache,
                )? {
                    // Go sets only one of the paths' flag.
                    if let Some(first) = paths.first_mut() {
                        first.keep_index_merge_or_source_filter = keep_source_filter;
                        branch.push(paths);
                    }
                }
                continue;
            }
            // Go folds a clustered PRIMARY key into the table path
            // (`getPossibleAccessPaths`), never a secondary index partial; the
            // table partial above builds that disjunct's TableRangeScan.
            if source_index.read().primary && (ds.handle_is_int || ds.is_common_handle()) {
                continue;
            }
            if !is_in_index_merge_hints(ds, source_index.read().name.original()) {
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
            let mut path = AccessPath::index_path(source_index.clone());
            if super::ordinary::fill_index_path(
                ds,
                &mut path,
                &pushable,
                ctx,
                ctx.opt_prefix_index_single_scan,
                false,
            )
            .is_err()
            {
                // Go accessPathsForConds declines a partial when derivation fails.
                continue;
            }
            // Go keeps a partial whose ranges came out empty: it reads
            // nothing, and only a full range disqualifies a partial.
            if path.ranges.iter().any(|range| range.is_full_range(false)) {
                continue;
            }
            let Ok(rows) = estimate_partial_index_ranges(ds, &path, ctx) else {
                continue;
            };
            let mut keep_source_filter =
                keep_branch_filter || !rejected.is_empty() || !path.table_filters.is_empty();
            path.table_filters.clear();
            if !tidb_expr::infer_pushdown::can_exprs_push_down(
                &path.index_filters,
                tidb_kv::store_type::StoreType::TiKv,
                ctx.expr_pushdown_blacklist,
            ) {
                keep_source_filter = true;
                path.index_filters.clear();
            }
            path.count_after_index =
                Some(rows * super::ordinary::filter_selectivity(ds, &path.index_filters, ctx));
            path.row_estimate = Some(tidb_stats::RowEstimate::new(rows, rows, rows));
            path.keep_index_merge_or_source_filter = keep_source_filter;
            branch.push(vec![path]);
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
    for path in alternatives.iter().flatten().flatten() {
        match path.index_info() {
            Some(index) => {
                possible_ids.insert(index.read().id);
                contain_mv_path |= index.read().mv_index;
            }
            None => {
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
                .map(Vec::as_slice)
                .min_by(|left, right| compare_alternatives(left, right, false))
                .expect("nonempty branch")
        })
        .collect::<Vec<_>>();
    let total_rows = estimate_union_access(ds, &selected, ctx);

    Ok(Some(AccessPath {
        partial_alternative_index_paths: alternatives,
        row_estimate: Some(tidb_stats::RowEstimate::new(total_rows, total_rows, total_rows)),
        index_merge_or_source_filter: Some(ds.all_conds[or_position].clone()),
        table_filters,
        ..AccessPath::default()
    }))
}

pub(crate) fn estimate_union_access(
    ds: &DataSource,
    selected: &[&[AccessPath]],
    ctx: &AccessPathDerivationContext<'_>,
) -> f64 {
    // Go `estimateCountAfterAccessForIndexMergeOR`: with an MV partial the
    // decided paths unite by `CalcTotalSelectivityForMVIdxPath`.
    let paths = || selected.iter().flat_map(|alternative| alternative.iter());
    if selected.iter().any(|alternative| is_mv_alternative(alternative)) {
        let rows = paths().map(|path| path.count_after_access().unwrap_or(0.0));
        return ds.table_stats.as_ref().map_or(0.0, |stats| stats.row_count())
            * super::mv_index::union_selectivity(ds, rows);
    }
    let mut total_rows = paths()
        .map(|path| path.count_after_access().unwrap_or(0.0))
        .sum::<f64>();
    let access_branches = selected
        .iter()
        .map(|alternative| {
            let first = &alternative[0];
            if is_mv_alternative(alternative) {
                tidb_expr::simple_expr::compose_dnf_condition(
                    alternative
                        .iter()
                        .filter_map(|path| {
                            tidb_expr::simple_expr::compose_cnf_condition(path.access_conds.clone())
                        })
                        .collect(),
                )
            } else if first.is_table_path() {
                tidb_expr::simple_expr::compose_cnf_condition(first.access_conds.clone())
            } else {
                let mut conditions = first.access_conds.clone();
                conditions.extend(first.index_filters.iter().cloned());
                tidb_expr::simple_expr::compose_cnf_condition(conditions)
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
            tidb_expr::infer_pushdown::can_exprs_push_down(
                std::slice::from_ref(condition),
                tidb_kv::store_type::StoreType::TiKv,
                ctx.expr_pushdown_blacklist,
            )
        })
}

/// Go's normal-index unfinished path: retain an immediately usable range, or
/// collect one equality/IN per index column until top-level AND predicates
/// supply a missing leading key. Keep incomplete branches until that stage.
fn collect_unfinished_filters(
    ds: &DataSource,
    index: Option<&tidb_model::GoShared<tidb_model::IndexInfo>>,
    expression: &Expression,
    ctx: &AccessPathDerivationContext<'_>,
) -> Option<(Vec<Expression>, bool)> {
    let conditions = tidb_expr::expr_util::normal_form::split_cnf_items(expression);
    let (pushable, rejected) = partition_partial_filters(&conditions, ctx);
    let ranges = if let Some(index) = index {
        let mut path = AccessPath::index_path(index.clone());
        super::ordinary::fill_index_path(
            ds,
            &mut path,
            &pushable,
            ctx,
            ctx.opt_prefix_index_single_scan,
            false,
        )
        .ok()?;
        path.ranges
    } else {
        let mut path = AccessPath::table_path(tidb_kv::store_type::StoreType::TiKv, false, None);
        super::ordinary::detach_table_path(ds, &mut path, &pushable, ctx, false).ok()?;
        path.ranges
    };
    if !ranges
        .iter()
        .any(|range| range.is_full_range(false))
    {
        return Some((vec![expression.clone()], !rejected.is_empty()));
    }
    let index = index?;
    let mut collected = vec![false; conditions.len()];
    let mut filters = Vec::new();
    for key in index.read().columns.iter_deref() {
        let Some(column) = ds.table_columns.get(key.read().offset as usize) else {
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
/// Go: an OR-branch alternative over a multi-valued index -- one partial
/// path per value (`Index.MVIndex`).
pub(crate) fn is_mv_alternative(alternative: &[AccessPath]) -> bool {
    alternative
        .first()
        .and_then(AccessPath::index_info)
        .is_some_and(|index| index.read().mv_index)
}

pub(crate) fn compare_alternatives(
    left: &[AccessPath],
    right: &[AccessPath],
    physical: bool,
) -> std::cmp::Ordering {
    let attributes = |alternative: &[AccessPath]| {
        let first = &alternative[0];
        let (point, rows, global) = if first.is_table_path() {
            (
                first.ranges.iter().all(|range| range.is_point_nullable()),
                first.count_after_access().unwrap_or(0.0),
                false,
            )
        } else if is_mv_alternative(alternative) {
            // Go cmpAlternatives: every path must be empty or unique-point,
            // and a multi-path alternative compares by its largest count.
            let index = first.index_info().expect("an MV alternative reads its index");
            let point = alternative.iter().all(|path| {
                let ranges = &path.ranges;
                ranges.is_empty()
                    || (index.read().unique
                        && ranges.iter().all(|range| {
                            range.is_point_non_nullable()
                                && range.high_val.len() == index.read().columns.len()
                        }))
            });
            let rows = alternative
                .iter()
                .map(|path| {
                    if path.index_filters.is_empty() {
                        path.count_after_access().unwrap_or(0.0)
                    } else {
                        path.count_after_index.unwrap_or(0.0)
                    }
                })
                .fold(0.0, f64::max);
            (point, rows, index.read().global)
        } else {
            let index = first.index_info().expect("an index alternative reads its index");
            let ranges = &first.ranges;
            let point = ranges.is_empty()
                || (index.read().unique
                    && ranges.iter().all(|range| {
                        range.is_point_non_nullable()
                            && range.high_val.len() == index.read().columns.len()
                    }));
            let rows = first.count_after_access().unwrap_or(0.0);
            (point, first.count_after_index.unwrap_or(rows), index.read().global)
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
    path: &AccessPath,
    ctx: &AccessPathDerivationContext<'_>,
) -> Result<f64, PlanError> {
    let index = path.index.as_ref().expect("a partial index path reads its index");
    estimate_index_ranges(ds, index, &path.ranges, &path.idx_cols, ctx)
        .map(|estimate| estimate.est)
}

/// Go `detachCondAndBuildRangeForPath`'s `GetRowCountByIndexRanges` over
/// `ranges` of `source_index`, whose usable key columns are `columns`.
pub(super) fn estimate_index_ranges(
    ds: &DataSource,
    source_index: &tidb_model::GoShared<tidb_model::IndexInfo>,
    ranges: &tidb_ranger::types::Ranges,
    columns: &[(tidb_expr::column::Column, i64)],
    ctx: &AccessPathDerivationContext<'_>,
) -> Result<tidb_stats::RowEstimate, PlanError> {
    let declared = source_index.read().columns.len();
    // Go `GetRowCountByIndexRanges` records the index it reads before any
    // fast path (`row_count_index.go:48`).
    ds.record_used_item(ctx.used_stats_recorder, source_index.read().id, true);
    let ranges = &super::ordinary::prune_estimate_range(ranges, declared);
    let Some(hist) = ds.table_stats.as_ref().and_then(|stats| stats.hist_coll()) else {
        // Go's table always carries a collection; a profile without one
        // keeps the pseudo rate.
        return Ok(tidb_stats::default_row_est(
            tidb_planner_cardinality::ranger_stats_bridge::pseudo_count_by_index_ranges(
                ranges,
                ds.table_stats
                    .as_ref()
                    .map_or(ranges.len() as f64, |stats| stats.row_count()),
                source_index.read().unique.then_some(source_index.read().columns.len()),
            ),
        ));
    };
    let columns = columns
        .iter()
        .take(declared)
        .map(|(column, _)| column.clone())
        .collect::<Vec<_>>();
    tidb_planner_cardinality::selectivity::get_row_count_by_index_ranges(
        hist,
        source_index.read().id,
        ranges,
        &columns,
        &ctx.estimator_options,
    )
    .map_err(|error| {
        PlanError::from(
            tidb_plannererrors::ERR_INTERNAL
                .gen_with_stack_by_args(&[(error.to_string()).into()]),
        )
    })
}

/// Go `generateNormalIndexPartialPath4And`.
pub(crate) fn generate_normal_index_partial_path_4_and(
    ds: &DataSource,
    ctx: &AccessPathDerivationContext<'_>,
    used_access: &mut std::collections::HashMap<Vec<u8>, Expression>,
) -> Vec<AccessPath> {
    generate_and_index_merge_for_normal_index(ds, ctx, false, used_access)
        .map(|path| path.partial_index_paths)
        .unwrap_or_default()
}

/// Go `generateANDIndexMerge4NormalIndex`: the hinted intersection over
/// ordinary indexes. Composed with MV partials (`used_access` non-empty), a
/// path whose access conditions an MV partial already covers is skipped and
/// one path suffices.
pub(crate) fn generate_and_index_merge_4_normal_index(
    ds: &DataSource,
    ctx: &AccessPathDerivationContext<'_>,
    use_plan_cache: bool,
) -> Result<Option<AccessPath>, PlanError> {
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
) -> Option<AccessPath> {
    use std::collections::HashSet;
    if ds.is_local_temporary()
        || !ds
            .index_merge_hints
            .iter()
            .any(|hint| !hint.index_names.is_empty())
    {
        return None;
    }
    let composed_with_mv_index = !used_access.is_empty();
    let mut partials = Vec::new();
    let mut final_filters = Vec::new();
    let mut partial_filters = Vec::new();
    let mut covered = HashSet::new();
    for index in enumerated_indexes(ds) {
        if index.read().mv_index
            || index.read().is_columnar_index()
            || !index.read().condition_expr_string.is_empty()
            || !is_specified_in_index_merge_hints(ds, index.read().name.original())
        {
            continue;
        }
        let Some(mut path) = ds.path_by_index_id(index.read().id).cloned() else {
            continue;
        };
        if tidb_ranger::types::has_full_range(&path.ranges, false) {
            continue;
        }
        if composed_with_mv_index {
            // For `(1 member of a) and c = 1 and d = 2` over mv(c, a), idx(c)
            // and idx(c, d), idx(c)'s access is already covered by the MV
            // partial; idx(c, d) adds `d = 2`.
            let access_hashes = path
                .access_conds
                .iter()
                .map(Expression::canonical_hash_code)
                .collect::<Vec<_>>();
            if access_hashes.iter().all(|hash| used_access.contains_key(hash)) {
                continue;
            }
            for (hash, access) in access_hashes.into_iter().zip(&path.access_conds) {
                used_access.entry(hash).or_insert_with(|| access.clone());
            }
        }
        // Go clones ordinary paths and retains only pushable index filters.
        // A prefix recheck can occur in access conditions AND table filters;
        // it must never be marked covered merely by the access condition.
        let mut not_covered = path.table_filters.clone();
        path.index_filters.retain(|condition| {
            let pushable =
                tidb_expr::infer_pushdown::can_exprs_push_down(
                    std::slice::from_ref(condition),
                    tidb_kv::store_type::StoreType::TiKv,
                    ctx.expr_pushdown_blacklist,
                );
            if !pushable {
                not_covered.push(condition.clone());
            }
            pushable
        });
        let covered_conditions = path.access_conds.iter().chain(&path.index_filters);
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
        partials.push(path);
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
    Some(AccessPath {
        partial_index_paths: partials,
        row_estimate: Some(tidb_stats::RowEstimate::new(
            count_after_access,
            count_after_access,
            count_after_access,
        )),
        table_filters: final_filters,
        index_merge_is_intersection: true,
        ..AccessPath::default()
    })
}
