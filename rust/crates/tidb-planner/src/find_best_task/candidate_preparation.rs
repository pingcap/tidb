// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Ordinary candidate admission and skyline comparison before physical allocation.

use super::candidate::{CandidateMetrics, HeuristicPath};
use super::dispatch::{
    index_path_matches_order, match_partial_order_property, table_path_matches_order,
    DispatchContext,
};
use crate::access_path::PossiblePath;
use crate::logical::{data_source::index_path_is_single_scan, DataSource};
use crate::physical_property::{PhysicalProperty, TaskType};

pub(super) struct PreparedOrdinaryPaths<'a> {
    pub paths: Vec<&'a PossiblePath>,
    /// Per path, in `paths` order: Go `skylinePruning`'s preferRange inputs,
    /// `(is a preferred range scan, is kept regardless)`.
    pub range_flags: Vec<(bool, bool)>,
    pub idx_missing_stats: bool,
    pub heuristic_selected: bool,
}

/// Unfilled native sources retain the later fallback until logical derivation.
pub(super) fn prepare_ordinary_paths<'a>(
    ds: &DataSource,
    paths: &[&'a PossiblePath],
    prop: &PhysicalProperty,
    ctx: &mut DispatchContext<'_>,
    table_pseudo: bool,
) -> Option<PreparedOrdinaryPaths<'a>> {
    if prop.index_join_prop.is_some() {
        return None;
    }
    let retained = ds.derived_access_paths.as_ref()?;
    let mut candidates = Vec::new();
    for &path in paths {
        if (ds.prefer_store_type & crate::logical::data_source::PREFER_TIFLASH != 0
            && !matches!(path, PossiblePath::TiFlashTable))
            || (ds.prefer_store_type & crate::logical::data_source::PREFER_TIKV != 0
                && matches!(path, PossiblePath::TiFlashTable))
        {
            continue;
        }
        let (metrics, heuristic) = match path {
            PossiblePath::Table { primary_index, .. } => {
                if prop.partial_order_info.is_some() {
                    continue;
                }
                let table = retained.table_path.as_ref()?;
                let columns = crate::column_length::Col2Len::from_pairs(
                    table.detached.access_conds.iter().flat_map(|condition| {
                        tidb_expr::simple_expr::extract_columns(condition)
                            .into_iter()
                            .map(|column| (column.unique_id, -1))
                    }),
                );
                let metrics = table.count_after_access.map(|access| CandidateMetrics {
                    access_columns: columns.clone(),
                    index_columns: Default::default(),
                    table_path: true,
                    single_scan: true,
                    pseudo: table_pseudo,
                    matches_property: table_path_matches_order(ds, prop),
                    eq_or_in_count: super::candidate::equality_facts(&table.detached).0,
                    count_after_access: access,
                    count_after_index: 0.0,
                    min_count_after_access: table.min_count_after_access,
                    max_count_after_access: table.max_count_after_access,
                    ..Default::default()
                });
                let primary = primary_index.and_then(|index| ds.indexes.get(index));
                let heuristic = HeuristicPath {
                    range_count: table.detached.ranges.len(),
                    only_points: table.detached.ranges.iter().all(|range| {
                        primary.map_or_else(
                            || range.is_point_nullable(),
                            |index| {
                                range.is_point_non_nullable()
                                    && range.low_val.len() == index.columns.len()
                            },
                        )
                    }),
                    unique: true,
                    single_scan: true,
                    table_filter_count: table.detached.remained_conds.len(),
                    access_columns: columns,
                };
                (metrics, Some(heuristic))
            }
            PossiblePath::Index { index } => {
                let index = ds.indexes.get(*index)?;
                let state = ds.derived_index_paths.get(&index.id)?;
                let filled = state.filled.as_ref()?;
                let partial = prop
                    .partial_order_info
                    .as_ref()
                    .and_then(|info| match_partial_order_property(ds, index, info));
                if prop.partial_order_info.is_some() && partial.is_none() {
                    continue;
                }
                if ctx.partial_ordered_index_for_topn
                    && partial.is_some()
                    && ds.forced_index_ids.contains(&index.id)
                    && !ds.force_no_keep_order_index_ids.contains(&index.id)
                {
                    ctx.forced_partial_order_paths
                        .insert((std::ptr::from_ref(ds).addr(), index.id));
                }
                let single_scan = state.is_single_scan.unwrap_or_else(|| {
                    index_path_is_single_scan(ds, index, ctx.opt_prefix_index_single_scan)
                });
                // Go `derivePathStatsAndTryHeuristics` selects a path whose
                // ranges came out empty before any skyline pruning: the
                // statement reads nothing, whatever conditions remain.
                let empty_ranges = filled.detached.ranges.is_empty();
                if filled.detached.access_conds.is_empty()
                    && !empty_ranges
                    && prop.is_sort_item_empty()
                    && !ds.forced_index_ids.contains(&index.id)
                    && !single_scan
                    && partial.is_none()
                {
                    continue;
                }
                let metrics = super::candidate::index_candidate_metrics(
                    ds,
                    index,
                    state,
                    single_scan,
                    index_path_matches_order(ds, index, prop),
                );
                let heuristic = if empty_ranges {
                    Some(HeuristicPath::default())
                } else {
                    metrics.as_ref().map(|metrics| HeuristicPath {
                        range_count: filled.detached.ranges.len(),
                        only_points: filled.detached.ranges.iter().all(|range| {
                            range.is_point_non_nullable()
                                && range.low_val.len() == index.columns.len()
                        }),
                        unique: index.unique,
                        single_scan,
                        table_filter_count: filled.table_filters.len(),
                        access_columns: metrics.access_columns.clone(),
                    })
                };
                (metrics, heuristic)
            }
            PossiblePath::TiFlashTable => (None, None),
        };
        let flags = range_scan_flags(ds, path, prop, ctx, metrics.as_ref());
        candidates.push((path, metrics, heuristic, flags));
    }
    // Go `derivePathStatsAndTryHeuristics` prunes `PossibleAccessPaths` in
    // `DataSource.DeriveStats`, before any physical property exists, so the
    // heuristic path serves an ordered or coprocessor request too.
    if let Some(heuristics) = candidates
        .iter()
        .map(|candidate| candidate.2.clone())
        .collect::<Option<Vec<_>>>()
    {
        if let Some(selected) = super::candidate::choose_heuristic_path(&heuristics) {
            return Some(PreparedOrdinaryPaths {
                paths: vec![candidates[selected].0],
                range_flags: vec![candidates[selected].3],
                idx_missing_stats: false,
                heuristic_selected: true,
            });
        }
    }
    let mut skyline = Vec::new();
    let mut idx_missing_stats = false;
    for candidate in candidates {
        let (_, missing, _) = super::candidate::insert_skyline_candidate(
            &mut skyline,
            candidate,
            |candidate| candidate.1.as_ref(),
            table_pseudo,
            prop.expected_cnt,
            ctx.prefer_range_scan,
            ctx.index_join_skyline_threshold,
        );
        idx_missing_stats |= missing;
    }
    Some(PreparedOrdinaryPaths {
        range_flags: skyline.iter().map(|candidate| candidate.3).collect(),
        paths: skyline.into_iter().map(|candidate| candidate.0).collect(),
        idx_missing_stats,
        heuristic_selected: false,
    })
}

/// Go `skylinePruning`'s per-candidate preferRange inputs: whether the path is
/// a range scan worth preferring (`!isFullRange`, and either an equal/IN
/// predicate, more filtering in the index than against the table, or a single
/// scan that serves the required order) and whether it is kept regardless
/// (forced, global or multi-valued). The same rule the conversion loop applies
/// once a task is built; computing it here lets the filter run before any
/// plan id is drawn, as in Go.
fn range_scan_flags(
    ds: &DataSource,
    path: &PossiblePath,
    prop: &PhysicalProperty,
    ctx: &DispatchContext<'_>,
    metrics: Option<&CandidateMetrics>,
) -> (bool, bool) {
    let retained = match ds.derived_access_paths.as_ref() {
        Some(retained) => retained,
        None => return (false, false),
    };
    let prefer_merge = !ds.index_merge_hints.is_empty() || ds.prefer_index_merge_by_fix_control;
    match path {
        PossiblePath::Table { .. } => {
            let Some(table) = retained.table_path.as_ref() else {
                return (false, false);
            };
            // A table path is always a single scan; like an index path it is
            // a preferred range scan only when it serves the required order.
            (
                (prop.is_sort_item_empty() || metrics.is_some_and(|metrics| metrics.matches_property))
                    && !crate::ranger::types::has_full_range(
                        &table.detached.ranges,
                        ds.table_scan_penalty.unsigned_int_handle,
                    ),
                !ds.forced_index_ids.is_empty(),
            )
        }
        PossiblePath::Index { index } => {
            let Some(index) = ds.indexes.get(*index) else {
                return (false, false);
            };
            let Some(filled) = ds
                .derived_index_paths
                .get(&index.id)
                .and_then(|state| state.filled.as_ref())
            else {
                return (false, false);
            };
            let single_scan = metrics.is_some_and(|metrics| metrics.single_scan);
            let matches_property = metrics.is_some_and(|metrics| metrics.matches_property);
            let remained = &filled.detached.remained_conds;
            let index_filter_count = remained
                .iter()
                .filter(|condition| {
                    crate::logical::data_source::index_covers_condition(
                        ds,
                        index,
                        condition,
                        ctx.opt_prefix_index_single_scan,
                    )
                })
                .count();
            let index_filters =
                filled.detached.eq_or_in_count > 0 || remained.len() - index_filter_count < index_filter_count;
            (
                (prefer_merge
                    || ((single_scan || index_filters)
                        && (prop.is_sort_item_empty() || matches_property)))
                    && !crate::ranger::types::has_full_range(&filled.detached.ranges, false),
                ds.forced_index_ids.contains(&index.id) || index.global || index.is_multi_valued,
            )
        }
        PossiblePath::TiFlashTable => (false, true),
    }
}

pub(super) enum PreparedMerge<'a> {
    Union(super::index_merge_union::ConvergedUnionPath<'a>),
    IndexMerge(&'a crate::access_path::index_merge::IndexMergePath),
}

pub(super) struct PreparedDataSourcePaths<'a> {
    pub ordinary: Option<PreparedOrdinaryPaths<'a>>,
    pub merges: Vec<PreparedMerge<'a>>,
}

/// Resolve candidate identities and merge alternatives before converting any path.
pub(super) fn prepare_access_paths<'a>(
    ds: &'a DataSource,
    paths: &[&'a PossiblePath],
    prop: &PhysicalProperty,
    ctx: &mut DispatchContext<'_>,
    table_pseudo: bool,
) -> PreparedDataSourcePaths<'a> {
    let ordinary = prepare_ordinary_paths(ds, paths, prop, ctx, table_pseudo);
    let mut merges = Vec::new();
    // Go admits ordered unions only when all converged partials match.
    // Intersection still cannot provide the required order.
    if matches!(prop.task_tp, TaskType::Root | TaskType::CopMultiRead)
        && prop.index_join_prop.is_none()
        && !ordinary
            .as_ref()
            .is_some_and(|paths| paths.heuristic_selected)
    {
        if let Some(paths) = &ds.derived_access_paths {
            for path in &paths.paths {
                match path {
                    crate::access_path::DerivedAccessPath::Union(path) => {
                        if let Some(path) =
                            super::index_merge_union::converge_union_index_merge_path(
                                ds, path, prop, ctx,
                            )
                        {
                            merges.push(PreparedMerge::Union(path));
                        }
                    }
                    // Go `isMatchPropForIndexMerge` refuses an order to an
                    // intersection; a finished union keeps no order here
                    // either, since its partials scan single element values.
                    crate::access_path::DerivedAccessPath::IndexMerge(path) => {
                        if prop.is_sort_item_empty() {
                            merges.push(PreparedMerge::IndexMerge(path));
                        }
                    }
                    crate::access_path::DerivedAccessPath::Ordinary(_) => {}
                }
            }
        }
    }
    let mut ordinary = ordinary;
    // Go `skylinePruning`'s closing preferRange filter: once several
    // candidates include a preferred range scan, only the preferred ones and
    // the paths kept regardless remain. It runs on the candidate list, before
    // any of them is converted into a physical plan.
    if let Some(prepared) = ordinary.as_mut() {
        let prefer_merge =
            !ds.index_merge_hints.is_empty() || ds.prefer_index_merge_by_fix_control;
        let prefer_range = ctx.prefer_range_scan
            && prop.index_join_prop.is_none()
            && (prefer_merge
                || prepared.idx_missing_stats
                || table_pseudo
                || ds
                    .table_stats
                    .as_ref()
                    .or_else(|| ds.base.base.stats_info())
                    .is_none_or(|stats| stats.row_count() < 1.0));
        let has_range_scan_path = prepared.range_flags.iter().any(|(preferred, _)| *preferred)
            || (prefer_merge && !merges.is_empty());
        if prefer_range
            && prepared.paths.len() + merges.len() > 1
            && has_range_scan_path
            && !prepared.heuristic_selected
        {
            let mut keep = prepared.range_flags.iter().map(|(preferred, kept)| *preferred || *kept);
            prepared.paths.retain(|_| keep.next().unwrap_or(true));
            prepared.range_flags.retain(|(preferred, kept)| *preferred || *kept);
            if !prefer_merge {
                merges.clear();
            }
        }
    }
    PreparedDataSourcePaths { ordinary, merges }
}
