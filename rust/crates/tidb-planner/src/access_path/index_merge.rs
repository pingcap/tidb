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
    ds.possible_paths_or_all()
        .into_iter()
        .filter_map(tidb_planner_util::path::AccessPath::index_info)
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
                ctx.can_exprs_push_down_tikv(std::slice::from_ref(condition));
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
