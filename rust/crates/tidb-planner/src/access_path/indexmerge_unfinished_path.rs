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

//! Go `indexmerge_unfinished_path.go`: every (MV and non-MV index) OR type
//! IndexMerge path, with `generateNormalIndexPartialPath` and
//! `accessPathsForConds` of `indexmerge_path.go`.

use tidb_expr::expression::Expression;
use tidb_expr::expr_util::normal_form::{flatten_dnf_conditions, split_cnf_items};
use tidb_planner_core_base::PlanError;
use tidb_planner_core_operator_logicalop::DataSource;
use tidb_planner_util::path::AccessPath;

use super::index_merge::{
    compare_alternatives, estimate_index_ranges, estimate_partial_index_ranges,
    estimate_union_access, is_in_index_merge_hints,
};
use super::mv_index::{
    build_mv_alternative, check_access_filter_4_idx_col, collect_filters_4_mv_index,
    prepare_idx_cols_and_unwrap_array_type, AccessFilterType,
};
use super::AccessPathDerivationContext;

/// Go `generateORIndexMerge`: the OR type IndexMerge paths of `AllConds`,
/// which are implicitly connected by AND.
pub(crate) fn generate_or_index_merge(
    ds: &DataSource,
    ctx: &AccessPathDerivationContext<'_>,
    use_plan_cache: bool,
) -> Result<Vec<AccessPath>, PlanError> {
    let filters = &ds.all_conds;
    let candidate_access_paths = ds.possible_paths_or_all();
    let mut paths = Vec::new();
    // 1. Iterate the input filters and try to find an OR list.
    for (k, cond) in filters.iter().enumerate() {
        let Expression::ScalarFunction(sf) = cond else {
            continue;
        };
        if sf.func_name.lowercase() != "or" {
            continue;
        }
        let dnf_filters = flatten_dnf_conditions(sf);
        // 2. Try to collect usable filters for each candidate access path using the OR list.
        let unfinished_index_merge_path =
            gen_unfinished_path_from_or_list(ds, ctx, &dnf_filters, &candidate_access_paths);
        // 3. Try to collect more usable filters from the top level AND list and build it into a valid AccessPath.
        if let Some(index_merge_path) = handle_top_level_and_list(
            ds,
            ctx,
            filters,
            k,
            &candidate_access_paths,
            unfinished_index_merge_path,
            use_plan_cache,
        )? {
            paths.push(index_merge_path);
        }
    }
    Ok(paths)
}

/// Go `unfinishedAccessPath`: the usable filters collected for one candidate
/// access path, or (with `or_branches`) the container of an OR list's
/// branches.
#[derive(Default)]
struct UnfinishedAccessPath<'a> {
    path: Option<&'a AccessPath>,
    usable_filters: Vec<Expression>,
    /// Set by the column-by-column collection, where `inited_with_valid_range`
    /// is false.
    idx_col_has_usable_filter: Vec<bool>,
    inited_with_valid_range: bool,
    /// The OR list needs to become a filter in the final Selection.
    need_keep_filter: bool,
    /// One element per OR branch.
    or_branches: Vec<UnfinishedAccessPathList<'a>>,
}

/// Go `unfinishedAccessPathList`: one element per candidate access path.
type UnfinishedAccessPathList<'a> = Vec<Option<UnfinishedAccessPath<'a>>>;

/// Go `isMVIndexPath`.
fn is_mv_index_path(path: &AccessPath) -> bool {
    path.index.as_ref().is_some_and(|index| index.read().mv_index)
}

/// Go `genUnfinishedPathFromORList`.
fn gen_unfinished_path_from_or_list<'a>(
    ds: &DataSource,
    ctx: &AccessPathDerivationContext<'_>,
    or_list: &[Expression],
    candidate_access_paths: &[&'a AccessPath],
) -> Option<UnfinishedAccessPath<'a>> {
    if or_list.len() < 2 {
        return None;
    }
    let mut unfinished_partial_paths = Vec::with_capacity(or_list.len());
    for single_filter in or_list {
        unfinished_partial_paths.push(init_unfinished_paths_from_expr(
            ds,
            ctx,
            candidate_access_paths,
            single_filter,
        )?);
    }
    Some(UnfinishedAccessPath {
        or_branches: unfinished_partial_paths,
        ..UnfinishedAccessPath::default()
    })
}

/// Go `initUnfinishedPathsFromExpr`: the access filters `expr` gives each
/// candidate access path; `None` in a candidate's place when it gives none,
/// and `None` altogether when no candidate has any.
fn init_unfinished_paths_from_expr<'a>(
    ds: &DataSource,
    ctx: &AccessPathDerivationContext<'_>,
    candidate_access_paths: &[&'a AccessPath],
    expr: &Expression,
) -> Option<UnfinishedAccessPathList<'a>> {
    let ret = candidate_access_paths
        .iter()
        .map(|path| {
            let unfinished = init_unfinished_path_from_expr(ds, ctx, path, expr);
            // remove useless paths
            (unfinished.inited_with_valid_range
                || unfinished.idx_col_has_usable_filter.contains(&true))
            .then_some(unfinished)
        })
        .collect::<Vec<_>>();
    ret.iter().any(Option::is_some).then_some(ret)
}

/// The body of Go `initUnfinishedPathsFromExpr`'s loop over the candidates.
fn init_unfinished_path_from_expr<'a>(
    ds: &DataSource,
    ctx: &AccessPathDerivationContext<'_>,
    path: &'a AccessPath,
    expr: &Expression,
) -> UnfinishedAccessPath<'a> {
    let mut ret = UnfinishedAccessPath {
        path: Some(path),
        ..UnfinishedAccessPath::default()
    };
    // case 1: try to use the previous logic to handle non-mv index
    if !is_mv_index_path(path) {
        let (partial_path, need_selection) = generate_normal_index_partial_path(ds, ctx, expr, path);
        if partial_path.is_some() {
            ret.inited_with_valid_range = true;
            ret.need_keep_filter = need_selection;
            ret.usable_filters = vec![expr.clone()];
            return ret;
        }
    }
    // Skip table path only if it has no Index (e.g. int handle). For common
    // handle, the primary key has index columns, so the gradual collection
    // below gathers partial filters to merge with top-level AND conditions.
    let Some(index) = path
        .index
        .as_ref()
        .filter(|index| !path.is_table_path() || index.read().primary)
    else {
        return ret;
    };
    let Some(idx_cols) = prepare_idx_cols_and_unwrap_array_type(ds, index, false) else {
        return ret;
    };
    let cnf_items = split_cnf_items(expr);
    for cnf_item in &cnf_items {
        if !ctx.can_exprs_push_down_tikv(std::slice::from_ref(cnf_item)) {
            ret.need_keep_filter = true;
        }
    }

    // case 2: try to use the previous logic to handle mv index
    if is_mv_index_path(path) {
        let (access_filters, remaining_filters, tp) =
            collect_filters_4_mv_index(ctx.expression_evaluator, &cnf_items, &idx_cols);
        if !access_filters.is_empty()
            && matches!(
                tp,
                Some(AccessFilterType::MultiValuesOrOnMvCol | AccessFilterType::SingleValueOnMvCol)
            )
        {
            ret.inited_with_valid_range = true;
            ret.usable_filters = access_filters;
            ret.need_keep_filter = !remaining_filters.is_empty();
            return ret;
        }
    }

    // case 3: collect access filters for each column respectively. If every
    // CNF item of this OR branch is collected, the branch does not need to be
    // rechecked by a Selection.
    ret.idx_col_has_usable_filter = vec![false; idx_cols.len()];
    let mut collected_cnf_items = vec![false; cnf_items.len()];
    for (j, col) in idx_cols.iter().enumerate() {
        for (k, cnf_item) in cnf_items.iter().enumerate() {
            if collected_cnf_items[k] {
                continue;
            }
            // Only the OR list nested in the AND list is handled, and only
            // IndexMerge OR paths are generated, so multiValuesANDOnMVColTp is
            // disabled here.
            if matches!(
                check_access_filter_4_idx_col(ctx.expression_evaluator, cnf_item, col),
                Some(
                    AccessFilterType::EqOrInOnNonMvCol
                        | AccessFilterType::MultiValuesOrOnMvCol
                        | AccessFilterType::SingleValueOnMvCol
                )
            ) {
                ret.usable_filters.push(cnf_item.clone());
                ret.idx_col_has_usable_filter[j] = true;
                collected_cnf_items[k] = true;
                // One valid access filter for this column is enough.
                break;
            }
        }
    }
    ret.need_keep_filter = ret.need_keep_filter || collected_cnf_items.contains(&false);
    ret
}

/// Go `handleTopLevelANDList`: `... AND (... OR ... OR ...) AND ...`. The
/// other items of the top level AND list give the OR list's candidates more
/// access filters, and the whole is built into an access path.
fn handle_top_level_and_list<'a>(
    ds: &DataSource,
    ctx: &AccessPathDerivationContext<'_>,
    all_conds: &[Expression],
    or_list_idx_in_all_conds: usize,
    candidate_access_paths: &[&'a AccessPath],
    mut unfinished_index_merge_path: Option<UnfinishedAccessPath<'a>>,
    use_plan_cache: bool,
) -> Result<Option<AccessPath>, PlanError> {
    for (i, cnf_item) in all_conds.iter().enumerate() {
        // Skip the (... OR ... OR ...) in the list.
        if i == or_list_idx_in_all_conds {
            continue;
        }
        // Collect access filters from one AND item.
        let path_list_from_and_item =
            init_unfinished_paths_from_expr(ds, ctx, candidate_access_paths, cnf_item);
        // Try to merge useful access filters in them into the one from the nested OR list.
        unfinished_index_merge_path = merge_and_item_into_unfinished_index_merge_path(
            unfinished_index_merge_path,
            path_list_from_and_item,
        );
    }
    let Some(unfinished_index_merge_path) = unfinished_index_merge_path else {
        return Ok(None);
    };
    build_into_access_path(
        ds,
        ctx,
        &unfinished_index_merge_path,
        all_conds,
        or_list_idx_in_all_conds,
        use_plan_cache,
    )
}

/// Go `mergeANDItemIntoUnfinishedIndexMergePath`.
fn merge_and_item_into_unfinished_index_merge_path<'a>(
    index_merge_path: Option<UnfinishedAccessPath<'a>>,
    path_list_from_and_item: Option<UnfinishedAccessPathList<'a>>,
) -> Option<UnfinishedAccessPath<'a>> {
    let mut index_merge_path = index_merge_path.filter(|path| !path.or_branches.is_empty())?;
    // No valid access filter in this item of the top level AND list: rely on
    // the nested OR list only.
    let Some(path_list_from_and_item) = path_list_from_and_item else {
        return Some(index_merge_path);
    };
    for path_list_for_single_partial_path in &mut index_merge_path.or_branches {
        if path_list_for_single_partial_path.len() != path_list_from_and_item.len() {
            continue;
        }
        for (path, from_and_item) in path_list_for_single_partial_path
            .iter_mut()
            .zip(&path_list_from_and_item)
        {
            let (Some(path), Some(from_and_item)) = (path, from_and_item) else {
                continue;
            };
            // No precise check: as long as any column has a valid access
            // filter, the item's access filters are collected whole, and
            // `build_into_access_path` handles them.
            if from_and_item.inited_with_valid_range
                || from_and_item.idx_col_has_usable_filter.contains(&true)
            {
                path.usable_filters
                    .extend(from_and_item.usable_filters.iter().cloned());
            }
        }
    }
    Some(index_merge_path)
}

/// Go `buildIntoAccessPath`.
fn build_into_access_path(
    ds: &DataSource,
    ctx: &AccessPathDerivationContext<'_>,
    index_merge_path: &UnfinishedAccessPath<'_>,
    all_conds: &[Expression],
    or_list_idx_in_all_conds: usize,
    use_plan_cache: bool,
) -> Result<Option<AccessPath>, PlanError> {
    if index_merge_path.or_branches.is_empty() {
        return Ok(None);
    }
    // 1. Use the collected usable filters to build partial paths for each alternative of each OR branch.
    let mut all_alternative_paths: Vec<Vec<Vec<AccessPath>>> =
        Vec::with_capacity(index_merge_path.or_branches.len());
    for or_branch in &index_merge_path.or_branches {
        let mut alternatives_for_or_branch = Vec::new();
        for unfinished_path in or_branch.iter().flatten() {
            let path = unfinished_path
                .path
                .expect("a candidate's unfinished path holds the candidate");
            let (mut one_alternative, mut need_selection) = if is_mv_index_path(path) {
                // case 1: mv index
                let index = path.index.as_ref().expect("an MV index path reads its index");
                let Some(alternative) = build_mv_alternative(
                    ds,
                    ctx,
                    index,
                    &unfinished_path.usable_filters,
                    use_plan_cache,
                )?
                else {
                    continue;
                };
                alternative
            } else {
                // case 2: non-mv index
                let Some(item) = tidb_expr::simple_expr::compose_cnf_condition(
                    unfinished_path.usable_filters.clone(),
                ) else {
                    continue;
                };
                let (Some(path), need_selection) =
                    generate_normal_index_partial_path(ds, ctx, &item, path)
                else {
                    continue;
                };
                (vec![path], need_selection)
            };
            need_selection = need_selection || unfinished_path.need_keep_filter;
            if need_selection {
                // only need to set one of the paths to true
                one_alternative[0].keep_index_merge_or_source_filter = true;
            }
            alternatives_for_or_branch.push(one_alternative);
        }
        if alternatives_for_or_branch.is_empty() {
            return Ok(None);
        }
        all_alternative_paths.push(alternatives_for_or_branch);
    }

    // 2. Some extra setup and checks: the partial paths' filters, the index
    // IDs and whether there is any MV index.
    let mut possible_idx_ids = std::collections::BTreeSet::new();
    let mut contain_mv_path = false;
    for p in all_alternative_paths.iter_mut().flatten().flatten() {
        // A partial path can handle TableFilters only if it's a table path,
        // and the filters can be pushed to TiKV. The source filter is kept
        // even then: convertToPartialTableScan removes filters on non-handle
        // columns.
        if !p.table_filters.is_empty() {
            p.keep_index_merge_or_source_filter = true;
            if !ctx.can_exprs_push_down_tikv(&p.table_filters) || !p.is_table_path() {
                p.table_filters.clear();
            }
        }
        // A partial path can handle IndexFilters if the filters can be pushed to TiKV.
        if !p.index_filters.is_empty() && !ctx.can_exprs_push_down_tikv(&p.index_filters) {
            p.keep_index_merge_or_source_filter = true;
            p.index_filters.clear();
        }
        if p.is_table_path() {
            possible_idx_ids.insert(-1);
        } else {
            possible_idx_ids.insert(p.index.as_ref().expect("an index path reads its index").read().id);
        }
        contain_mv_path |= is_mv_index_path(p);
    }
    if !contain_mv_path && possible_idx_ids.len() <= 1 {
        return Ok(None);
    }

    // 3. Build the final access path. For estimation the decided partial
    // paths are needed, chosen here by comparing the row count.
    let paths_for_estimate = all_alternative_paths
        .iter()
        .map(|one_or_branch| {
            one_or_branch
                .iter()
                .map(Vec::as_slice)
                .min_by(|left, right| compare_alternatives(left, right, false))
                .expect("nonempty branch")
        })
        .collect::<Vec<_>>();
    let count_after_access = estimate_union_access(ds, &paths_for_estimate, ctx);
    Ok(Some(AccessPath {
        partial_alternative_index_paths: all_alternative_paths,
        row_estimate: Some(tidb_stats::RowEstimate::new(
            count_after_access,
            count_after_access,
            count_after_access,
        )),
        index_merge_or_source_filter: Some(all_conds[or_list_idx_in_all_conds].clone()),
        table_filters: all_conds
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != or_list_idx_in_all_conds)
            .map(|(_, cond)| cond.clone())
            .collect(),
        ..AccessPath::default()
    }))
}

/// Go `generateNormalIndexPartialPath`: the partial path the pushable CNF
/// items of `item` give `candidate_path`, and whether an item stays behind.
fn generate_normal_index_partial_path(
    ds: &DataSource,
    ctx: &AccessPathDerivationContext<'_>,
    item: &Expression,
    candidate_path: &AccessPath,
) -> (Option<AccessPath>, bool) {
    // Reject partial index first.
    if candidate_path
        .index
        .as_ref()
        .is_some_and(|index| !index.read().condition_expr_string.is_empty())
    {
        return (None, false);
    }
    let mut need_selection = false;
    let mut pushed_down_cnf_items = Vec::new();
    for cnf_item in split_cnf_items(item) {
        if ctx.can_exprs_push_down_tikv(std::slice::from_ref(&cnf_item)) {
            pushed_down_cnf_items.push(cnf_item);
        } else {
            need_selection = true;
        }
    }
    // Without a partial path for this DNF item the entire index merge is not
    // valid anymore.
    match access_paths_for_conds(ds, ctx, &pushed_down_cnf_items, candidate_path) {
        Some(partial_path) => (Some(partial_path), need_selection),
        None => (None, false),
    }
}

/// Go `accessPathsForConds`: an access path for the candidate over
/// `conditions`, unless derivation fails or it reads a full range.
fn access_paths_for_conds(
    ds: &DataSource,
    ctx: &AccessPathDerivationContext<'_>,
    conditions: &[Expression],
    path: &AccessPath,
) -> Option<AccessPath> {
    if path.is_table_path() {
        if !is_in_index_merge_hints(ds, "primary") {
            return None;
        }
        let mut new_path = AccessPath::table_path(
            tidb_kv::store_type::StoreType::TiKv,
            ds.is_common_handle(),
            path.index.clone(),
        );
        super::ordinary::detach_table_path(ds, &mut new_path, conditions, ctx, false).ok()?;
        let rows = match new_path.index.clone() {
            // Go `deriveCommonHandleTablePathStats`: the PRIMARY index's
            // histogram over these ranges.
            Some(index) => {
                estimate_index_ranges(ds, &index, &new_path.ranges, &new_path.idx_cols, ctx)
                    .ok()?
                    .est
            }
            None => super::ordinary::estimate_int_table_path(ds, &new_path, ctx).ok()?,
        };
        new_path.row_estimate = Some(tidb_stats::RowEstimate::new(rows, rows, rows));
        let unsigned_int_handle = new_path.is_int_handle_path
            && ds.pk_is_handle()
            && super::ordinary::table_path_handle(ds).1.is_unsigned();
        // If the newPath contains a full range, ignore it.
        if tidb_ranger::types::has_full_range(&new_path.ranges, unsigned_int_handle) {
            return None;
        }
        Some(new_path)
    } else {
        let index = path.index.clone().expect("an index path reads its index");
        if !is_in_index_merge_hints(ds, index.read().name.original()) {
            return None;
        }
        let mut new_path = AccessPath::index_path(index);
        super::ordinary::fill_index_path(
            ds,
            &mut new_path,
            conditions,
            ctx,
            ctx.opt_prefix_index_single_scan,
            false,
        )
        .ok()?;
        // If the newPath contains a full range, ignore it.
        if tidb_ranger::types::has_full_range(&new_path.ranges, false) {
            return None;
        }
        let rows = estimate_partial_index_ranges(ds, &new_path, ctx).ok()?;
        new_path.count_after_index =
            Some(rows * super::ordinary::filter_selectivity(ds, &new_path.index_filters, ctx));
        new_path.row_estimate = Some(tidb_stats::RowEstimate::new(rows, rows, rows));
        Some(new_path)
    }
}
