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

//! Source-shaped tests for `GetUsableSetsByGreedy`.

use tidb_planner::selectivity_greedy::{get_usable_sets_by_greedy, StatsNode, StatsNodeType};

fn mock_stats_node(id: i64, mask: i64, num_cols: usize) -> StatsNode {
    StatsNode::new(StatsNodeType::Index, id, mask, num_cols)
}

/// Direct translation of `TestSelectivityGreedyAlgo` from
/// `pkg/planner/cardinality/selectivity_test.go`.
#[test]
fn test_selectivity_greedy_algo() {
    let mut nodes = vec![
        mock_stats_node(1, 3, 2),
        mock_stats_node(2, 5, 2),
        mock_stats_node(3, 9, 2),
    ];

    // Sets should not overlap on mask, so only nodes[0] is chosen.
    let used_sets = get_usable_sets_by_greedy(&mut nodes);
    assert_eq!(used_sets.len(), 1);
    assert_eq!(used_sets[0].id, 1);

    nodes.swap(0, 1);
    // Selection is stable after source-order sorting, so ID 1 still wins.
    let used_sets = get_usable_sets_by_greedy(&mut nodes);
    assert_eq!(used_sets.len(), 1);
    assert_eq!(used_sets[0].id, 1);
}

#[test]
fn type_and_coverage_priority_match_source() {
    let mut nodes = vec![
        StatsNode::new(StatsNodeType::Column, 10, 0b0011, 1),
        StatsNode::new(StatsNodeType::PrimaryKey, 20, 0b0011, 1),
        StatsNode::new(StatsNodeType::Index, 30, 0b1100, 2),
    ];

    // The primary key has fewer represented columns, so it wins after the
    // equal coverage/type tie; the remaining disjoint index follows.
    let selected = get_usable_sets_by_greedy(&mut nodes);
    assert_eq!(
        selected.iter().map(|node| node.id).collect::<Vec<_>>(),
        [20, 30]
    );
}

#[test]
fn source_sort_places_index_before_primary_key() {
    let mut nodes = vec![
        StatsNode::new(StatsNodeType::PrimaryKey, 20, 0b0001, 1),
        StatsNode::new(StatsNodeType::Index, 10, 0b0001, 1),
    ];

    // compareType orders ColType < IndexType < PkType before the greedy
    // tie-breaks are considered, so the index is selected first.
    let selected = get_usable_sets_by_greedy(&mut nodes);
    assert_eq!(
        selected.iter().map(|node| node.id).collect::<Vec<_>>(),
        [10]
    );
}

#[test]
fn source_tie_breakers_are_applied_in_order() {
    let mut nodes = vec![
        StatsNode {
            node_type: StatsNodeType::Index,
            id: 1,
            mask: 0b0011,
            selectivity: 0.8,
            num_cols: 3,
            partial_cover: true,
            min_access_conditions_for_dnf: 2,
        },
        StatsNode {
            node_type: StatsNodeType::Index,
            id: 2,
            mask: 0b0011,
            selectivity: 0.2,
            num_cols: 1,
            partial_cover: false,
            min_access_conditions_for_dnf: 3,
        },
    ];

    // Equal type and coverage: full cover (node 2) wins before the later
    // min-access, number-of-columns, and selectivity rules are considered.
    let selected = get_usable_sets_by_greedy(&mut nodes);
    assert_eq!(selected.iter().map(|node| node.id).collect::<Vec<_>>(), [2]);
}

#[test]
fn lower_selectivity_wins_only_after_structural_ties() {
    let mut nodes = vec![
        StatsNode {
            node_type: StatsNodeType::Index,
            id: 1,
            mask: 1,
            selectivity: 0.1,
            num_cols: 2,
            partial_cover: false,
            min_access_conditions_for_dnf: 1,
        },
        StatsNode {
            node_type: StatsNodeType::Index,
            id: 2,
            mask: 1,
            selectivity: 0.2,
            num_cols: 2,
            partial_cover: false,
            min_access_conditions_for_dnf: 1,
        },
    ];

    let selected = get_usable_sets_by_greedy(&mut nodes);
    assert_eq!(selected.iter().map(|node| node.id).collect::<Vec<_>>(), [1]);
}
