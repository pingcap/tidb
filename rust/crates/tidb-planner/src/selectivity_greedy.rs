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

//! Dependency-closed selectivity-statistics greedy selection.
//!
//! `pkg/planner/cardinality/selectivity.go` chooses the statistics nodes that
//! cover the remaining predicates.  The Go owner receives expression masks
//! and real column/index statistics from the planner; those boundaries are
//! not available in this crate yet.  This leaf keeps only the source's
//! deterministic mask traversal and tie-break rules over caller-owned node
//! metadata.  It does not estimate selectivity, inspect expressions, or
//! mutate a catalog.

/// The source statistics-node kinds, ordered for greedy tie breaking.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatsNodeType {
    /// A secondary index statistics node.
    Index,
    /// A primary-key/handle statistics node.
    PrimaryKey,
    /// A single-column statistics node.
    Column,
}

impl StatsNodeType {
    /// Source `compareType` ordering: columns first, then ordinary indexes,
    /// then primary keys.
    const fn sort_rank(self) -> u8 {
        match self {
            Self::Column => 0,
            Self::Index => 1,
            Self::PrimaryKey => 2,
        }
    }
}

/// Caller-owned metadata for one statistics candidate.
///
/// `mask` uses the same signed 64-bit representation as Go's `StatsNode`.
/// Planner expression extraction remains outside this leaf; callers provide
/// the already-computed mask and source tie-break metadata.
#[derive(Clone, Debug, PartialEq)]
pub struct StatsNode {
    /// Whether this candidate is an index, primary-key, or column node.
    pub node_type: StatsNodeType,
    /// Stable source statistics ID used as the deterministic sort key.
    pub id: i64,
    /// Predicate-coverage bit mask from source expression extraction.
    pub mask: i64,
    /// Estimated selectivity used only as the final tie-break rule.
    pub selectivity: f64,
    /// Number of columns represented by this candidate.
    pub num_cols: usize,
    /// Whether the node only partially covers a DNF predicate.
    pub partial_cover: bool,
    /// Minimum number of access conditions among covered DNF branches.
    pub min_access_conditions_for_dnf: i32,
}

impl StatsNode {
    /// Creates the source-shaped baseline used by simple column/index nodes.
    #[must_use]
    pub const fn new(node_type: StatsNodeType, id: i64, mask: i64, num_cols: usize) -> Self {
        Self {
            node_type,
            id,
            mask,
            selectivity: 0.0,
            num_cols,
            partial_cover: false,
            min_access_conditions_for_dnf: 0,
        }
    }
}

/// Selects non-overlapping statistics candidates using Go's greedy ordering.
///
/// The input slice is sorted in source order (`Column`, `PrimaryKey`,
/// `Index`, then ascending ID), matching `slices.SortFunc`.  Returned values
/// borrow the sorted input, just as Go returns pointers to its `StatsNode`
/// entries.  A candidate is considered once its mask is wholly contained in
/// the still-uncovered mask.  The winner is selected by source priority:
/// node kind, number of newly-covered bits, full-versus-partial DNF cover,
/// minimum DNF access-condition count, fewer columns, then lower selectivity.
#[must_use]
pub fn get_usable_sets_by_greedy(nodes: &mut [StatsNode]) -> Vec<&StatsNode> {
    nodes.sort_by(|left, right| {
        left.node_type
            .sort_rank()
            .cmp(&right.node_type.sort_rank())
            .then_with(|| left.id.cmp(&right.id))
    });

    let mut marked = vec![false; nodes.len()];
    let mut remaining_mask = i64::MAX;
    let mut selected_indices = Vec::new();

    loop {
        let mut best_index = None;
        let mut best_mask = 0_i64;
        let mut best_cover_count = 0_u32;

        for (index, node) in nodes.iter().enumerate() {
            if marked[index] {
                continue;
            }

            let current_mask = node.mask & remaining_mask;
            if current_mask != node.mask {
                marked[index] = true;
                continue;
            }

            let cover_count = (current_mask as u64).count_ones();
            if cover_count == 0 {
                marked[index] = true;
                continue;
            }

            let is_better = best_index.is_none_or(|best| {
                is_better_choice(node, cover_count, &nodes[best], best_cover_count)
            });
            if is_better {
                best_index = Some(index);
                best_mask = current_mask;
                best_cover_count = cover_count;
            }
        }

        let Some(index) = best_index else {
            break;
        };

        remaining_mask &= !best_mask;
        marked[index] = true;
        selected_indices.push(index);
    }

    selected_indices
        .into_iter()
        .map(|index| &nodes[index])
        .collect()
}

fn is_better_choice(
    candidate: &StatsNode,
    candidate_cover_count: u32,
    current: &StatsNode,
    current_cover_count: u32,
) -> bool {
    // 1. Prefer primary-key or index statistics over a plain column node.
    // This is the source condition `s.Tp != ColType && other.Tp == ColType`.
    if candidate.node_type != StatsNodeType::Column && current.node_type == StatsNodeType::Column {
        return true;
    }

    // 2. Prefer the candidate covering more remaining expressions.
    if candidate_cover_count != current_cover_count {
        return candidate_cover_count > current_cover_count;
    }

    // 3. A full DNF cover is better than a partial cover.
    if candidate.partial_cover != current.partial_cover {
        return !candidate.partial_cover;
    }

    // 4. Prefer the candidate with more minimum access conditions.
    if candidate.min_access_conditions_for_dnf != current.min_access_conditions_for_dnf {
        return candidate.min_access_conditions_for_dnf > current.min_access_conditions_for_dnf;
    }

    // 5. Fewer represented columns are preferred.
    if candidate.num_cols != current.num_cols {
        return candidate.num_cols < current.num_cols;
    }

    // 6. Finally prefer lower selectivity.  A NaN compares false here, like
    // Go's `<` comparison in the source implementation.
    candidate.selectivity < current.selectivity
}

/// What kind of predicate one condition is, for the leftover-condition tail of
/// Go's `Selectivity`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConditionKind {
    /// A constant that evaluates to NULL or false: it zeroes the result.
    ConstantFalse,
    /// A constant that evaluates to true: it covers itself and changes nothing.
    ConstantTrue,
    /// A disjunction. The source estimates it recursively; this port leaves it
    /// to the default factor and says so at [`combine_selectivity`].
    Disjunction,
    /// `LIKE`/`ILIKE`/`REGEXP`, which carry their own default selectivity.
    StringMatch,
    /// A negated string match, with its own default selectivity again.
    NegatedStringMatch,
    /// Anything else, which falls back to the general selectivity factor.
    Other,
}

/// The session defaults the leftover-condition tail multiplies by.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SelectivityDefaults {
    /// `SelectivityFactor`, Go's 0.8.
    pub selectivity_factor: f64,
    /// `GetStrMatchDefaultSelectivity`.
    pub str_match_default: f64,
    /// `GetNegateStrMatchDefaultSelectivity`.
    pub negate_str_match_default: f64,
}

impl Default for SelectivityDefaults {
    fn default() -> Self {
        Self {
            selectivity_factor: 0.8,
            str_match_default: 0.8,
            negate_str_match_default: 0.8,
        }
    }
}

/// Combines statistics nodes into one selectivity, Go `Selectivity`'s body
/// after the nodes are built.
///
/// `initial` carries the correlated-column product the source accumulates
/// before node selection. `conditions` describes each CNF item in the same
/// order the node masks index. Two source behaviors are *not* reproduced
/// here, because both need an expression evaluator this crate does not have:
/// a [`ConditionKind::Disjunction`] is never estimated recursively, and no
/// TopN-assisted string-match estimation is attempted -- both simply fall
/// through to their default selectivity, which is what the source itself does
/// when those attempts decline.
#[must_use]
pub fn combine_selectivity(
    nodes: &mut [StatsNode],
    conditions: &[ConditionKind],
    initial: f64,
    realtime_row_count: i64,
    defaults: SelectivityDefaults,
) -> f64 {
    if realtime_row_count == 0 || conditions.is_empty() {
        return 1.0;
    }
    let mut ret = initial;
    let mut mask: i64 = if conditions.len() >= 63 {
        i64::MAX
    } else {
        (1_i64 << conditions.len()) - 1
    };

    for set in get_usable_sets_by_greedy(nodes) {
        mask &= !set.mask;
        ret *= set.selectivity;
        // A partial DNF cover leaves residual conditions behind, so the
        // source charges the default factor for them on top.
        if set.partial_cover {
            ret *= defaults.selectivity_factor;
        }
    }

    let (mut has_default, mut has_str_match, mut has_negate_str_match) = (false, false, false);
    for (index, kind) in conditions.iter().enumerate() {
        if mask & (1_i64 << index) == 0 {
            continue;
        }
        match kind {
            ConditionKind::ConstantFalse => {
                ret *= 0.0;
                mask &= !(1_i64 << index);
            }
            ConditionKind::ConstantTrue => {
                mask &= !(1_i64 << index);
            }
            ConditionKind::StringMatch => has_str_match = true,
            ConditionKind::NegatedStringMatch => has_negate_str_match = true,
            ConditionKind::Disjunction | ConditionKind::Other => has_default = true,
        }
    }

    if mask > 0 {
        let mut min_selectivity = 1.0_f64;
        if has_default {
            min_selectivity = min_selectivity.min(defaults.selectivity_factor);
        }
        if has_str_match {
            min_selectivity = min_selectivity.min(defaults.str_match_default);
        }
        if has_negate_str_match {
            min_selectivity = min_selectivity.min(defaults.negate_str_match_default);
        }
        ret *= min_selectivity;
    }

    // The source never lets a selectivity fall below one row.
    ret.max(1.0 / realtime_row_count as f64)
}
