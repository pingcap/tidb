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
                if filled.detached.access_conds.is_empty()
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
                let heuristic = metrics.as_ref().map(|metrics| HeuristicPath {
                    range_count: filled.detached.ranges.len(),
                    only_points: filled.detached.ranges.iter().all(|range| {
                        range.is_point_non_nullable() && range.low_val.len() == index.columns.len()
                    }),
                    unique: index.unique,
                    single_scan,
                    table_filter_count: filled.table_filters.len(),
                    access_columns: metrics.access_columns.clone(),
                });
                (metrics, heuristic)
            }
            PossiblePath::TiFlashTable => (None, None),
        };
        candidates.push((path, metrics, heuristic));
    }
    if prop.is_sort_item_empty() && prop.task_tp == TaskType::Root {
        if let Some(heuristics) = candidates
            .iter()
            .map(|candidate| candidate.2.clone())
            .collect::<Option<Vec<_>>>()
        {
            if let Some(selected) = super::candidate::choose_heuristic_path(&heuristics) {
                return Some(PreparedOrdinaryPaths {
                    paths: vec![candidates[selected].0],
                    idx_missing_stats: false,
                    heuristic_selected: true,
                });
            }
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
        paths: skyline.into_iter().map(|candidate| candidate.0).collect(),
        idx_missing_stats,
        heuristic_selected: false,
    })
}

pub(super) enum PreparedMerge<'a> {
    Union(super::index_merge_union::ConvergedUnionPath<'a>),
    Intersection(&'a crate::access_path::index_merge::IntersectionIndexMergePath),
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
                    crate::access_path::DerivedAccessPath::Intersection(path) => {
                        if prop.is_sort_item_empty() {
                            merges.push(PreparedMerge::Intersection(path));
                        }
                    }
                    crate::access_path::DerivedAccessPath::Ordinary(_) => {}
                }
            }
        }
    }
    PreparedDataSourcePaths { ordinary, merges }
}
