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

//! Go `skylinePruning` over live access paths. Candidate comparison is owned
//! by the planner and shared with native IndexJoin path selection.

use tidb_planner::find_best_task::candidate::{compare_candidates, CandidateMetrics};

/// An enumerated path and the facts Go reads before comparing its cost.
#[derive(Clone, Debug)]
pub(crate) struct Candidate<T> {
    pub(crate) path: T,
    pub(crate) metrics: CandidateMetrics,
    /// Declared key width for Go's OnlyPointRange heuristic.
    pub(crate) index_width: usize,
    pub(crate) full_range: bool,
    pub(crate) empty_range: bool,
    pub(crate) index_filter_count: usize,
    pub(crate) table_filter_count: usize,
    pub(crate) forced: bool,
}

/// Session and table facts used by Go's skyline pruning.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PruningContext {
    pub(crate) table_pseudo: bool,
    pub(crate) row_count: f64,
    pub(crate) prefer_range: bool,
    pub(crate) has_limit: bool,
    pub(crate) has_sort_property: bool,
}

/// Go `skylinePruning`: retain candidates in enumeration order. Metrics and
/// their column-length maps are constructed once, not copied per comparison.
pub(crate) fn skyline_pruning<T>(
    candidates: Vec<Candidate<T>>,
    context: &PruningContext,
) -> Vec<Candidate<T>> {
    let mut survivors: Vec<Candidate<T>> = Vec::with_capacity(candidates.len());
    let mut index_missing_stats = false;
    for candidate in candidates {
        if candidate.empty_range {
            return vec![candidate];
        }
        let mut pruned = false;
        for i in (0..survivors.len()).rev() {
            let comparison = compare_candidates(
                &survivors[i].metrics,
                &candidate.metrics,
                context.table_pseudo,
                if context.has_limit { 0.0 } else { f64::MAX },
                context.prefer_range,
                1000.0,
            );
            index_missing_stats |= comparison.winner_is_pseudo;
            if comparison.ordering == 1 {
                pruned = true;
                break;
            }
            if comparison.ordering == -1 {
                survivors.remove(i);
            }
        }
        if !pruned {
            survivors.push(candidate);
        }
    }

    let prefer_range = context.prefer_range
        && (index_missing_stats || context.table_pseudo || context.row_count < 1.0);
    if prefer_range && survivors.len() > 1 {
        let preferred = |candidate: &Candidate<T>| {
            if candidate.forced || candidate.metrics.global || candidate.metrics.multi_valued {
                return true;
            }
            let index_filters = candidate.metrics.eq_or_in_count > 0
                || candidate.table_filter_count < candidate.index_filter_count;
            (candidate.metrics.single_scan || index_filters)
                && (!context.has_sort_property || candidate.metrics.matches_property)
                && !candidate.full_range
        };
        if survivors.iter().any(preferred) {
            survivors.retain(preferred);
        }
    }
    survivors
}
