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

#![allow(missing_docs)]

//! GO PORT of `pkg/planner/cardinality/selectivity_test.go:1447
//! TestSelectivityGreedyAlgo` (item 29 of the pkg/planner.part1 slice).
//!
//! Drives `GetUsableSetsByGreedy` (`pkg/planner/cardinality/selectivity.go:652`)
//! through its transcreation
//! [`tidb_planner::selectivity_greedy::get_usable_sets_by_greedy`] with the
//! test-only `MockStatsNode(id, m, num)` helper (`main_test.go:64`). The Go
//! mock leaves `StatsNode.Tp` at its zero value `IndexType`, so every mock
//! node here is [`tidb_planner::selectivity_greedy::StatsNodeType::Index`].

use tidb_planner::selectivity_greedy::{get_usable_sets_by_greedy, StatsNode, StatsNodeType};

/// Go `main_test.go:64 MockStatsNode` -- zero-valued Tp (index), given id,
/// predicate mask, and column count.
fn mock_stats_node(id: i64, mask: i64, num_cols: usize) -> StatsNode {
    // Zero-value source fields stay zero: selectivity 0, no partial cover,
    // minAccessCondsForDNFCond 0.
    let node = StatsNode::new(StatsNodeType::Index, id, mask, num_cols);
    debug_assert_eq!(node.selectivity, 0.0);
    debug_assert!(!node.partial_cover);
    node
}

#[test]
fn selectivity_greedy_algo_prefers_stable_widest_cover() {
    // Three nodes with masks 3, 5, 9 (all covering bit 1): the first sorted
    // candidate wins and everything overlapping is dropped, so exactly one
    // set survives -- node ID 1 -- no matter the input order.
    let mut nodes = vec![mock_stats_node(1, 3, 2), mock_stats_node(2, 5, 2)];
    nodes.push(mock_stats_node(3, 9, 2));

    let used_sets = get_usable_sets_by_greedy(&mut nodes);
    assert_eq!(used_sets.len(), 1, "overlapping masks must keep one set");
    assert_eq!(used_sets[0].id, 1);

    // Swapping nodes[0] and nodes[1] must not change the outcome: the chosen
    // set is stable because sorting normalizes by (type, ID).
    nodes.swap(0, 1);
    let used_sets = get_usable_sets_by_greedy(&mut nodes);
    assert_eq!(used_sets.len(), 1);
    assert_eq!(used_sets[0].id, 1);
}
