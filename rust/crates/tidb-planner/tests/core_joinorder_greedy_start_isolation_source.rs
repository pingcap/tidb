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

//! Port ledger for `pkg/planner/core/joinorder/` (`pkg/planner.part11`, Go
//! items 647–649 on `origin/master`).
//!
//! Family contract: the multi-start greedy join orderer — its conflict-
//! detector set operations, best-greedy-start selection and clone isolation.
//! The `joinorder` package itself (Node, ConflictDetector, chooseBestGreedyStart)
//! is NOT transcreated in this workspace yet, so the unit tests over it are
//! honest gap ports; only `intset.FastIntSet` (Go pkg/util/intset/
//! fast_int_set.go → crates/tidb-util/src/intset.rs) exists, which is exactly
//! one of the two arms benchmarked by item 647 — that arm's operation
//! sequence is ported LIVE below with its derived boolean outcomes (the Go
//! benchmark itself asserts nothing). Nothing was approximated to simulate
//! Go behavior.

/// GO PORT of `pkg/planner/core/joinorder/bitset_bench_test.go:115
/// BenchmarkJoinOrderConflictDetectorOps` — fastintset/conflict arms ONLY
/// (:134-158); live port of their operation sequence as correctness checks.
///
/// Re-derived contract: for each case {n16_e32, n32_e64, n64_e128,
/// n128_e256} the bench synthesizes `edgeCount` edges between singleton sets
/// using fixed index math (buildFastEdges :65-89): `l = i % n`,
/// `r = (i*7+3) % n` bumped off l, `extra = (i*11+1) % n` bumped off both, so
/// `tes = left ∪ right ∪ extra`; rules [{from:right,to:left},{from:left,to:
/// right}]. The hot loop (:138-152) folds one boolean per edge from set ops
/// and DISCARDS it into `sink` (:153-157, anti-dead-code trick) — the bench
/// ASSERTS NOTHING, so this port pins the truthful VALUE each op must return:
/// writing `d := extra ∈ {l, r}` (collision), then per edge — `tes ⊆ s =
/// left ∪ right` is exactly `d` (when extra is distinct it escapes s);
/// `tes ∩ left ≠ ∅` and `tes ∩ right ≠ ∅` are TRUE (left/right ⊆ tes);
/// both intersection-subset checks are TRUE identically; every rule arm has
/// `from = right/left ⊆ s` and `to = left/right ⊆ s`, hence TRUE; |tes| = 2
/// iff d else 3. Consequently the discarded ok is FALSE for every case here
/// (each case contains distinct-extra edges, e.g. i=0). The bitset/conflict
/// arms (:159-184, bits-and-blooms BitSet) and b.N timing harness remain
/// unportable; see the ignored sibling test.
#[test]
fn conflict_detector_edge_ops_match_derived_boolean_truth() {
    use tidb_util::intset::FastIntSet;

    struct Edge {
        tes: FastIntSet,
        left: FastIntSet,
        right: FastIntSet,
        // (from, to) pairs; Go ruleFast{from, to intset.FastIntSet}.
        rules: Vec<(FastIntSet, FastIntSet)>,
        // Whether final extra landed on l or r — decides tes ⊆ left ∪ right.
        extra_collides: bool,
    }

    let build_edges = |node_count: usize, edge_count: usize| -> Vec<Edge> {
        let nodes: Vec<FastIntSet> = (0..node_count)
            .map(|idx| FastIntSet::new(&[idx as i64]))
            .collect();
        let n = node_count;
        let mut edges = Vec::with_capacity(edge_count);
        for i in 0..edge_count {
            let l = i % n;
            let mut r = (i * 7 + 3) % n;
            if r == l {
                r = (r + 1) % n;
            }
            let mut extra = (i * 11 + 1) % n;
            if extra == l || extra == r {
                extra = (extra + 2) % n;
            }
            let tes = nodes[l].union(&nodes[r]).union(&nodes[extra]);
            let left = nodes[l].copy();
            let right = nodes[r].copy();
            let rules = vec![(right.copy(), left.copy()), (left.copy(), right.copy())];
            edges.push(Edge {
                tes,
                left,
                right,
                rules,
                extra_collides: extra == l || extra == r,
            });
        }
        edges
    };

    let cases: [(usize, usize); 4] = [(16, 32), (32, 64), (64, 128), (128, 256)];
    for (node_count, edge_count) in cases {
        let edges = build_edges(node_count, edge_count);
        assert_eq!(edges.len(), edge_count);
        let mut ok = true; // mirrors go ok at bitset_bench_test.go:138
        let mut saw_distinct_extra_edge = false;
        for e in &edges {
            let s = e.left.union(&e.right);
            if !e.tes.subset_of(&s)
                || !e.tes.intersects(&e.left)
                || !e.tes.intersects(&e.right)
            {
                ok = false;
            }
            if !e.left.intersection(&e.tes).subset_of(&e.left)
                || !e.right.intersection(&e.tes).subset_of(&e.right)
            {
                ok = false;
            }
            for (from, to) in &e.rules {
                if from.intersects(&s) && !to.subset_of(&s) {
                    ok = false;
                }
            }
            // Per-clause truths derived above (bitset_bench_test.go:140-151):
            assert_eq!(
                e.tes.subset_of(&s),
                e.extra_collides,
                "tes⊂(left∪right) iff extra collided with l or r"
            );
            assert!(e.tes.intersects(&e.left));
            assert!(e.tes.intersects(&e.right));
            assert!(e.left.intersection(&e.tes).subset_of(&e.left));
            assert!(e.right.intersection(&e.tes).subset_of(&e.right));
            for (from, to) in &e.rules {
                if from.intersects(&s) {
                    assert!(to.subset_of(&s));
                }
            }
            assert_eq!(
                e.tes.len(),
                if e.extra_collides { 2 } else { 3 },
                "|tes| counts distinct vertices among l, r, extra"
            );
            saw_distinct_extra_edge |= !e.extra_collides;
        }
        assert!(saw_distinct_extra_edge, "case n{node_count}_e{edge_count}: every case must contain at least one distinct-extra edge");
        assert!(!ok, "discarded sink ok ends false at n{node_count}_e{edge_count}, matching the folded clause values");
    }
}

/// GO PORT of `pkg/planner/core/joinorder/bitset_bench_test.go:115
/// BenchmarkJoinOrderConflictDetectorOps` — parity-complete bench shape.
///
/// Re-derived contract: besides the fastintset arms above, each case also
/// runs bitset/conflict equivalents over `bits-and-blooms/bitset` instances
/// using IsSuperSet/IntersectionCardinality (:160-184), so the benchmark can
/// TIME both set representations against identical synthetic workloads
/// (b.ResetTimer per arm). No bits-and-blooms counterpart nor timing harness
/// exists in this workspace; the go test tooling itself filters benchmarks
/// out of `go test` runs just like the nextest `-E 'not test(/bench/)'`
/// filter does here.
#[test]
#[ignore = "go-parity-gap: bits-and-blooms BitSet arm + b.N timing harness unported"]
fn benchmark_join_order_conflict_detector_ops_bitset_arm() {}

/// GO PORT of `pkg/planner/core/joinorder/join_order_test.go:26
/// TestChooseBestGreedyStart`.
///
/// Re-derived contract: chooseBestGreedyStart(startCount, runner)
/// (pkg/planner/core/joinorder/join_order.go:642-653) evaluates each start,
/// aborting on error, and keeps the FIRST start whose candidate cumCost is
/// "significantly" less than the current best via cumCostSignificantlyLess
/// (:655-661: cost < best AND best-cost > scale*1e-12 where scale = max(1,
/// |cost|, |bestCost|)). Three subtests pin: lowest cost wins with startIdx
/// 1 / cumCost 10; nil candidates are skipped without poisoning best;
/// floating-point noise is IGNORED — 14166.666666666668 vs …666 differ by
/// ~1e-12·scale ≤ scale*1e-12, so the EARLIER start (index 0) keeps winning
/// with its exact cost preserved. Also startIdx returns -1 when all
/// nil-eligible... not pinned by Go subtests but implied by bestStartIdx init
/// :644.
#[test]
#[ignore = "go-parity-gap: joinorder package (Node/cumCost machinery) not transcreated"]
fn choose_best_greedy_start_keeps_earlier_start_for_fp_noise() {}

/// GO PORT of `pkg/planner/core/joinorder/join_order_test.go:63
/// TestCloneNodesForGreedyStartIsolation`.
///
/// Re-derived contract: cloneNodesForGreedyStart (join_order.go:634-640) via
/// cloneNodeForGreedyStart (:623-632) deep-copies Node identity so greedy
/// starts never mutate shared state: clones are distinct objects
/// (require.NotSame), the usedEdges map is CLONED (maps.Clone :630) —
/// deleting key 1 / inserting key 2 in the clone leaves the original's map
/// with 1 and without 2 — and the plan pointer p is copied SHARED (:627):
/// assigning a fresh LogicalTableDual to the clone's p leaves the original
/// p nil (:76-80).
#[test]
#[ignore = "go-parity-gap: joinorder Node type (bitSet/p/usedEdges) not transcreated"]
fn clone_nodes_for_greedy_start_isolates_used_edges_shares_plan_pointer() {}
