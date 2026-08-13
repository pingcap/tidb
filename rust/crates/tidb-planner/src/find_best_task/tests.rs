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

//! The `(prop, candidate, enforcer)` rules, and the two statements the
//! corpus records that no comparison AT the join reproduces.
//!
//! Every cost asserted here is a line of `EXPLAIN FORMAT='cost_trace'` from a
//! `tidb-server` built from this tree, in a session carrying mysql-tester's
//! DSN variables -- the same oracle [`crate::candidate_cost`]'s tests read,
//! and the same numbers, since the fixtures below are those trees.

use super::*;
use crate::candidate_cost::{ReaderKind, RowSize};
use crate::plan_cost_ver2::{HashJoinInput, IndexJoinInput, TableScanPenaltyInput};

/// `tidb_hash_join_concurrency = 1`, which mysql-tester puts in every DSN.
const RECORDED_HASH_JOIN_CONCURRENCY: f64 = 1.0;

// Column `UniqueID`s. Only identity matters; the values are arbitrary.
const T2_A: i64 = 1;
const T2_B: i64 = 2;
/// The projected expression `plus(mul(t2.b, 2), 10)`, which `EXPLAIN` prints
/// as a bare `Column` because no name reaches it.
const COMPUTED: i64 = 3;
const T1_A: i64 = 4;
const T1_B: i64 = 5;
const T1_C: i64 = 6;
const T3_A: i64 = 7;

fn env() -> CostEnv {
    CostEnv::default()
}

fn asc(col: i64) -> SortItem {
    SortItem::new(col, false)
}

fn ordered(cols: &[i64]) -> PhysicalProperty {
    PhysicalProperty::new(TaskType::Root, cols, false, f64::MAX, false)
}

#[track_caller]
fn assert_prints(cost: f64, expected: &str) {
    assert_eq!(format!("{cost:.2}"), expected);
}

// ---------------------------------------------------------------------------
// The `result:1042` decision site, as the recording built it.
// ---------------------------------------------------------------------------

fn pseudo_full_scan(rows: f64) -> Candidate {
    Candidate::TableScan {
        rows,
        row_size: RowSize::Fixed(72.0),
        is_child_of_inl: None,
        has_full_range_scan: true,
        penalty: TableScanPenaltyInput {
            has_range_info: false,
            allow_prefer_range_scan: true,
            pseudo_stats: true,
            analyze_row_count: 0,
            modify_count: 0,
            has_partition_scan: false,
            has_index_force: false,
        },
        num_ranges: 1,
        desc: false,
    }
}

fn index_full_scan(rows: f64) -> Candidate {
    Candidate::IndexScan {
        rows,
        row_size: RowSize::Fixed(32.0),
        index_id: None,
        num_ranges: 1,
        desc: false,
    }
}

fn projected_t2(scan: Candidate, reader: ReaderKind) -> Candidate {
    Candidate::Projection {
        child: Box::new(Candidate::Reader {
            child: Box::new(Candidate::Selection {
                child: Box::new(scan),
                input_rows: 10000.0,
                conditions: vec![true],
            }),
            rows: 8000.0,
            row_size: RowSize::Fixed(32.0),
            kind: reader,
        }),
        input_rows: 8000.0,
        exprs: vec![false, false, true],
    }
}

/// The OUTER side's two access paths.
///
/// `TableFullScan ... keep order:true` provides `t2.a` and costs
/// `Projection_52 8000.00 517108.73`; the unordered index read of the same
/// projection costs `Projection_61 8000.00 317954.13`. The ordered one is
/// DEARER, which is what makes property propagation observable.
fn t2_side_1042() -> LogicalNode {
    LogicalNode::Leaf(vec![
        LeafAlternative {
            plan: projected_t2(pseudo_full_scan(10000.0), ReaderKind::Table),
            order: vec![asc(T2_A)],
            role: LeafRole::Plain,
        },
        LeafAlternative {
            plan: projected_t2(index_full_scan(10000.0), ReaderKind::Index),
            order: Vec::new(),
            role: LeafRole::Plain,
        },
    ])
}

/// The INNER side: an ordinary index read for a hash join, and the per-outer-
/// row range read `IndexJoinProp` produces for an index join.
fn t1_side_1042() -> LogicalNode {
    LogicalNode::Leaf(vec![
        LeafAlternative {
            // `IndexReader_69(Probe) 9990.00 219926.52`.
            plan: Candidate::Reader {
                child: Box::new(index_full_scan(9990.0)),
                rows: 9990.0,
                row_size: RowSize::Fixed(32.0),
                kind: ReaderKind::Index,
            },
            order: Vec::new(),
            role: LeafRole::Plain,
        },
        LeafAlternative {
            // `IndexReader_58(Probe) 10000.00 31.70`, costed per outer row:
            // `1.25 = 10000/8000`.
            plan: Candidate::Reader {
                child: Box::new(Candidate::Selection {
                    child: Box::new(Candidate::IndexScan {
                        rows: 1.251_251_251_251_251_3,
                        row_size: RowSize::Fixed(32.0),
                        index_id: None,
                        num_ranges: 1,
                        desc: false,
                    }),
                    input_rows: 1.251_251_251_251_251_3,
                    conditions: vec![true],
                }),
                rows: 1.25,
                row_size: RowSize::Fixed(32.0),
                kind: ReaderKind::Index,
            },
            order: Vec::new(),
            role: LeafRole::IndexJoinProbe {
                table_range_scan: false,
            },
        },
    ])
}

fn join_1042() -> LogicalJoin {
    LogicalJoin {
        join_type: LogicalJoinType::Inner,
        left: Box::new(t2_side_1042()),
        right: Box::new(t1_side_1042()),
        left_keys: vec![COMPUTED],
        right_keys: vec![T1_B],
        left_schema: vec![T2_A, T2_B, COMPUTED],
        right_schema: vec![T1_A, T1_B, T1_C],
        // The only order the projection can provide is the one its `t2` scan
        // provides: no index orders `plus(mul(t2.b, 2), 10)`.
        left_properties: vec![vec![T2_A]],
        right_properties: vec![vec![T1_A]],
        force_merge: false,
    }
}

/// The cost model for `result:1042`'s site: every input is a line of the
/// recorded cost trace.
struct Model1042;

impl JoinCostModel for Model1042 {
    fn attach(
        &self,
        _join: &LogicalJoin,
        strategy: &JoinStrategy,
        children: [&Task; 2],
    ) -> Option<Candidate> {
        match strategy {
            JoinStrategy::Index {
                outer_idx: 0,
                table_range_scan: false,
                kind,
                ..
            } => Some(Candidate::IndexJoin {
                build: Box::new(children[0].plan.clone()),
                probe: Box::new(children[1].plan.clone()),
                input: IndexJoinInput {
                    build_rows: 8000.0,
                    build_row_size: 24.0,
                    probe_rows_one: 1.25,
                    probe_row_size: 32.0,
                    num_right_join_keys: 0,
                    num_left_join_keys: 0,
                    num_ranges: 0.0,
                    is_semi_join: false,
                    kind: *kind,
                },
                build_filters: Vec::new(),
                probe_filters: Vec::new(),
            }),
            // `t1.b` is a secondary index, not the clustered handle, so
            // `buildDataSource2TableScanByIndexJoinProp` has no answer and the
            // task is invalid -- which is Go's `base.InvalidTask`.
            JoinStrategy::Index { .. } => None,
            JoinStrategy::Hash(HashJoinShape {
                inner_idx: 0,
                use_outer_to_build: false,
            }) => Some(Candidate::HashJoin {
                build: Box::new(children[0].plan.clone()),
                probe: Box::new(children[1].plan.clone()),
                input: HashJoinInput {
                    build_rows: 8000.0,
                    probe_rows: 9990.0,
                    build_row_size: 24.0,
                    num_build_keys: 1,
                    num_probe_keys: 1,
                    tidb_concurrency: RECORDED_HASH_JOIN_CONCURRENCY,
                },
                build_filters: Vec::new(),
                probe_filters: Vec::new(),
            }),
            // The mirrored build side is a plan the oracle was never asked
            // for, so it is refused rather than priced off invented rows.
            JoinStrategy::Hash(_) | JoinStrategy::Merge { .. } => None,
        }
    }

    fn enforce(&self, prop: &PhysicalProperty, task: &Task) -> Option<Candidate> {
        Some(Candidate::Sort {
            child: Box::new(task.plan.clone()),
            rows: task.costed.rows,
            row_size: RowSize::Fixed(task.costed.row_size),
            by_items: vec![false; prop.sort_items.len()],
        })
    }
}

// ---------------------------------------------------------------------------
// The rule table.
// ---------------------------------------------------------------------------

/// `getHashJoins` opens with "hash join doesn't promise any orders" and
/// returns nothing when the property is non-empty. This is the whole reason a
/// comparison AT the join never reproduced Go's choice: on `result:1042` the
/// hash join is cheaper at the join (`HashJoin_94 2373179.65` against
/// `IndexHashJoin_51 4606578.48`) and it is NOT A CANDIDATE there.
#[test]
fn a_required_order_leaves_a_join_with_no_hash_candidate() {
    let join = join_1042();
    let under_order = exhaust_join(&join, &ordered(&[T2_A]));
    assert!(
        !under_order
            .iter()
            .any(|candidate| matches!(candidate.strategy, JoinStrategy::Hash(_))),
        "a non-empty property must leave no hash candidate: {under_order:#?}"
    );
    let under_nothing = exhaust_join(&join, &PhysicalProperty::default());
    assert_eq!(
        under_nothing
            .iter()
            .filter(|candidate| matches!(candidate.strategy, JoinStrategy::Hash(_)))
            .count(),
        2,
        "an inner join under the empty property gets both build sides"
    );
}

/// `constructIndexJoinStatic` gives the OUTER child `prop.SortItems`
/// unchanged. That single line is what keeps the parent `MergeJoin`s of
/// `result:1042` alive above an index join.
#[test]
fn an_index_join_re_plans_its_outer_side_under_the_same_property() {
    let prop = ordered(&[T2_A]);
    let candidates = exhaust_join(&join_1042(), &prop);
    assert!(!candidates.is_empty());
    for candidate in &candidates {
        let JoinStrategy::Index { outer_idx, .. } = candidate.strategy else {
            panic!("only index joins survive this property: {candidate:#?}");
        };
        assert_eq!(candidate.child_props[outer_idx].sort_items, prop.sort_items);
        assert!(candidate.child_props[1 - outer_idx].is_sort_item_empty());
        assert_eq!(candidate.child_roles[outer_idx], LeafRole::Plain);
        assert!(matches!(
            candidate.child_roles[1 - outer_idx],
            LeafRole::IndexJoinProbe { .. }
        ));
    }
}

/// `enumerateIndexJoinByOuterIdx` requires `prop.AllColsFromSchema(outer)`: an
/// index join reads its inner side once per outer key and can only promise an
/// order the OUTER side already provides.
///
/// So the property does not merely allow or forbid index joins -- it picks
/// WHICH SIDE drives. An order the right child owns leaves only the candidates
/// that drive from the right, and an order spanning both children leaves none.
#[test]
fn the_property_decides_which_side_may_drive_an_index_join() {
    let right_only = exhaust_join(&join_1042(), &ordered(&[T1_B]));
    assert!(!right_only.is_empty());
    for candidate in &right_only {
        assert!(
            matches!(candidate.strategy, JoinStrategy::Index { outer_idx: 1, .. }),
            "only the right side can provide an order over its own column: \
             {candidate:#?}"
        );
    }

    let spanning = exhaust_join(&join_1042(), &ordered(&[T2_A, T1_B]));
    assert!(
        spanning.is_empty(),
        "no single side owns an order over both children: {spanning:#?}"
    );
}

/// Both outer sides for an inner join, one for an outer join.
#[test]
fn only_the_non_preserved_side_may_drive_an_index_join() {
    assert_eq!(LogicalJoinType::Inner.index_join_outer_sides(), &[0, 1]);
    assert_eq!(LogicalJoinType::LeftOuter.index_join_outer_sides(), &[0]);
    assert_eq!(LogicalJoinType::RightOuter.index_join_outer_sides(), &[1]);
    assert_eq!(LogicalJoinType::AntiSemi.index_join_outer_sides(), &[0]);
}

/// `GetMergeJoin` needs BOTH children to provide the keys. `result:1042`'s
/// left key is a projected expression no index orders, so the site has no
/// merge-join candidate under any property -- which is why the index join has
/// no rival there rather than merely a dearer one.
#[test]
fn a_merge_join_needs_both_children_to_provide_their_keys() {
    let join = join_1042();
    for prop in [PhysicalProperty::default(), ordered(&[T2_A])] {
        assert!(
            !exhaust_join(&join, &prop)
                .iter()
                .any(|candidate| matches!(candidate.strategy, JoinStrategy::Merge { .. })),
            "no child order covers `Column`, so no merge join exists"
        );
    }
    // Give the same join keys the children CAN order, and one appears.
    let mut orderable = join;
    orderable.left_keys = vec![T2_A];
    orderable.right_keys = vec![T1_A];
    let candidates = exhaust_join(&orderable, &ordered(&[T2_A]));
    let merges: Vec<&EnumeratedJoin> = candidates
        .iter()
        .filter(|candidate| matches!(candidate.strategy, JoinStrategy::Merge { .. }))
        .collect();
    assert_eq!(merges.len(), 1, "{candidates:#?}");
    assert_eq!(merges[0].child_props[0].sort_items, vec![asc(T2_A)]);
    assert_eq!(merges[0].child_props[1].sort_items, vec![asc(T1_A)]);
}

/// `getEnforcedMergeJoin` is reached only under a `MERGE_JOIN` hint or with
/// hash join disabled, so an unhinted enumeration never offers the
/// `Sort`-under-merge plan. On `result:1169` that plan is CHEAPER than the one
/// Go records (`2987.11` against `4557.50` at the parent `MergeJoin_18`, the
/// `Sort_57 2.00 2535.84` included) and Go still records the index join --
/// reproducing Go's CHOICE and minimising Go's COST are different objectives.
#[test]
fn the_unhinted_enumeration_never_produces_a_sort_enforced_merge_join() {
    let mut join = join_1042();
    join.left_keys = vec![T2_A];
    join.right_keys = vec![T1_A];
    // Take away every child order, so a merge join would have to sort.
    join.left_properties = Vec::new();
    join.right_properties = Vec::new();
    for prop in [PhysicalProperty::default(), ordered(&[T2_A])] {
        assert!(
            !exhaust_join(&join, &prop)
                .iter()
                .any(|candidate| matches!(candidate.strategy, JoinStrategy::Merge { .. })),
            "an unhinted join never enforces its own merge order"
        );
    }
}

#[test]
fn a_forced_merge_join_enables_sort_enforcers_on_both_children() {
    let mut join = join_1042();
    join.left_keys = vec![T2_A, T2_B];
    join.right_keys = vec![T1_A, T1_B];
    join.left_properties.clear();
    join.right_properties.clear();
    join.force_merge = true;

    let candidates = exhaust_join(&join, &ordered(&[T2_B]));
    assert_eq!(candidates.len(), 1, "the hint excludes other join families");
    let candidate = &candidates[0];
    assert!(matches!(candidate.strategy, JoinStrategy::Merge { .. }));
    assert_eq!(
        candidate.child_props[0].sort_items,
        vec![asc(T2_B), asc(T2_A)]
    );
    assert_eq!(
        candidate.child_props[1].sort_items,
        vec![asc(T1_B), asc(T1_A)]
    );
    assert!(candidate
        .child_props
        .iter()
        .all(|property| property.can_add_enforcer));
}

fn join_search_leaf(column: i64, rows: f64, ordered: bool) -> LogicalNode {
    LogicalNode::Leaf(vec![LeafAlternative {
        plan: Candidate::Reader {
            child: Box::new(Candidate::TableScan {
                rows,
                row_size: RowSize::Fixed(16.0),
                is_child_of_inl: None,
                has_full_range_scan: true,
                penalty: TableScanPenaltyInput::default(),
                num_ranges: 1,
                desc: false,
            }),
            rows,
            row_size: RowSize::Fixed(16.0),
            kind: ReaderKind::Table,
        },
        order: ordered.then(|| asc(column)).into_iter().collect(),
        role: LeafRole::Plain,
    }])
}

struct RecursiveMergeHintModel;

impl JoinCostModel for RecursiveMergeHintModel {
    fn attach(
        &self,
        _join: &LogicalJoin,
        strategy: &JoinStrategy,
        children: [&Task; 2],
    ) -> Option<Candidate> {
        match strategy {
            JoinStrategy::Hash(shape) => {
                let build_at = if shape.use_outer_to_build {
                    1 - shape.inner_idx
                } else {
                    shape.inner_idx
                };
                let probe_at = 1 - build_at;
                Some(Candidate::HashJoin {
                    build: Box::new(children[build_at].plan.clone()),
                    probe: Box::new(children[probe_at].plan.clone()),
                    input: HashJoinInput {
                        build_rows: children[build_at].costed.rows,
                        probe_rows: children[probe_at].costed.rows,
                        build_row_size: children[build_at].costed.row_size,
                        num_build_keys: 1,
                        num_probe_keys: 1,
                        tidb_concurrency: 1.0,
                    },
                    build_filters: Vec::new(),
                    probe_filters: Vec::new(),
                })
            }
            JoinStrategy::Merge {
                left_keys,
                right_keys,
                ..
            } => Some(Candidate::MergeJoin {
                left: Box::new(children[0].plan.clone()),
                right: Box::new(children[1].plan.clone()),
                child_rows: (children[0].costed.rows, children[1].costed.rows),
                left_conditions: Vec::new(),
                right_conditions: Vec::new(),
                other_conditions: Vec::new(),
                num_join_keys: (left_keys.len(), right_keys.len()),
            }),
            JoinStrategy::Index { .. } => None,
        }
    }

    fn enforce(&self, prop: &PhysicalProperty, task: &Task) -> Option<Candidate> {
        Some(Candidate::Sort {
            child: Box::new(task.plan.clone()),
            rows: task.costed.rows,
            row_size: RowSize::Fixed(task.costed.row_size),
            by_items: vec![false; prop.sort_items.len()],
        })
    }
}

fn forced_merge_subtree() -> LogicalNode {
    LogicalNode::Join(Box::new(LogicalJoin {
        join_type: LogicalJoinType::Inner,
        left: Box::new(join_search_leaf(T2_A, 8_000.0, false)),
        right: Box::new(join_search_leaf(T1_A, 10_000.0, false)),
        left_keys: vec![T2_B],
        right_keys: vec![T1_A],
        left_schema: vec![T2_A, T2_B],
        right_schema: vec![T1_A],
        left_properties: Vec::new(),
        right_properties: Vec::new(),
        force_merge: true,
    }))
}

#[test]
fn a_merge_hint_retries_below_an_incompatible_property_then_sorts_the_result() {
    let best = find_best_task(
        &forced_merge_subtree(),
        &ordered(&[T3_A]),
        LeafRole::Plain,
        &RecursiveMergeHintModel,
        &env(),
    )
    .expect("the hint works below one result Sort");

    let Candidate::Sort { child, .. } = best.plan else {
        panic!(
            "the incompatible property needs a result Sort: {:#?}",
            best.plan
        );
    };
    assert!(matches!(*child, Candidate::MergeJoin { .. }));
}

#[test]
fn a_child_merge_hint_does_not_force_the_parent_join_method() {
    let root = LogicalNode::Join(Box::new(LogicalJoin {
        join_type: LogicalJoinType::Inner,
        left: Box::new(forced_merge_subtree()),
        right: Box::new(join_search_leaf(T3_A, 10_000.0, true)),
        left_keys: vec![T2_A],
        right_keys: vec![T3_A],
        left_schema: vec![T2_A, T1_A],
        right_schema: vec![T3_A],
        // The hinted child is ordered by its own join key (`t2.b`), not by
        // the different `t2.a` key its parent joins on.
        left_properties: vec![vec![T2_B]],
        right_properties: vec![vec![T3_A]],
        force_merge: false,
    }));

    let best = find_best_task(
        &root,
        &PhysicalProperty::default(),
        LeafRole::Plain,
        &RecursiveMergeHintModel,
        &env(),
    )
    .expect("the recursive tree is buildable");

    let Candidate::HashJoin { build, probe, .. } = best.plan else {
        panic!(
            "the unhinted parent should remain a HashJoin: {:#?}",
            best.plan
        );
    };
    assert!(
        matches!(*build, Candidate::MergeJoin { .. })
            || matches!(*probe, Candidate::MergeJoin { .. }),
        "the child hint must still select its own MergeJoin"
    );
}

/// The enumeration order breaks exact ties, because `compareTaskCost` replaces
/// the incumbent only on a strict `<`.
#[test]
fn the_enumeration_order_is_merge_then_index_then_hash() {
    let mut join = join_1042();
    join.left_keys = vec![T2_A];
    join.right_keys = vec![T1_A];
    let kinds: Vec<&str> = exhaust_join(&join, &PhysicalProperty::default())
        .iter()
        .map(|candidate| match candidate.strategy {
            JoinStrategy::Merge { .. } => "merge",
            JoinStrategy::Index { .. } => "index",
            JoinStrategy::Hash(_) => "hash",
        })
        .collect();
    let first_index = kinds.iter().position(|kind| *kind == "index").unwrap();
    let first_hash = kinds.iter().position(|kind| *kind == "hash").unwrap();
    assert_eq!(kinds[0], "merge");
    assert!(first_index < first_hash, "{kinds:?}");
}

/// `findBestTask`'s enforced branch runs only when `prop.CanAddEnforcer`, and
/// `PhysicalMergeJoin.tryToGetChildReqProp` builds its children's properties
/// with `enforced: false`. A join under a merge-join parent therefore never
/// prices a `Sort` of its own -- correcting the assumption that the enforcer
/// alternative is always in the comparison.
#[test]
fn a_merge_joins_child_property_forbids_an_enforcer() {
    let mut join = join_1042();
    join.left_keys = vec![T2_A];
    join.right_keys = vec![T1_A];
    let candidates = exhaust_join(&join, &ordered(&[T2_A]));
    for candidate in &candidates {
        for prop in &candidate.child_props {
            assert!(
                !prop.can_add_enforcer,
                "no join child property enables an enforcer: {candidate:#?}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Acceptance: `result:1042`.
// ---------------------------------------------------------------------------

/// Go records `IndexHashJoin_51 10000.00 4606578.48` at this site although
/// `HashJoin_94 10000.00 2373179.65` is cheaper AT THE JOIN. The search
/// reproduces the recorded choice, and the reason is the property: under the
/// parent `MergeJoin`'s required order the hash join is not enumerated.
#[test]
fn result_1042_records_the_index_join_because_the_property_bars_the_hash_join() {
    let join = join_1042();
    let prop = ordered(&[T2_A]);
    let best = find_best_task(
        &LogicalNode::Join(Box::new(join)),
        &prop,
        LeafRole::Plain,
        &Model1042,
        &env(),
    )
    .expect("the index join is buildable under this property");

    // └─IndexHashJoin_51(Probe) 10000.00 4606578.48
    assert_prints(best.cost(), "4606578.48");
    assert!(
        matches!(
            best.plan,
            Candidate::IndexJoin {
                input: IndexJoinInput {
                    kind: IndexJoinKind::IndexHashJoin,
                    ..
                },
                ..
            }
        ),
        "the recorded executor is IndexHashJoin: {:#?}",
        best.plan
    );
    // ├─Projection_52(Build) 8000.00 517108.73 -- the ORDERED read, which is
    // dearer than the unordered `Projection_61 8000.00 317954.13` the same
    // side offers. The property, not the cost, chose it.
    assert_prints(best.costed.children[0].est_cost(), "517108.73");
    assert_eq!(best.order, vec![asc(T2_A)]);
}

/// The same site under the EMPTY property, which is what the topmost join of
/// the statement sees. Now the hash join IS enumerated, and it wins -- so the
/// search is not biased toward the index join; the property is the only
/// difference between the two answers.
#[test]
fn result_1042_takes_the_hash_join_once_no_order_is_required() {
    let best = find_best_task(
        &LogicalNode::Join(Box::new(join_1042())),
        &PhysicalProperty::default(),
        LeafRole::Plain,
        &Model1042,
        &env(),
    )
    .expect("both strategies are buildable under the empty property");

    // └─HashJoin_94(Probe) 10000.00 2373179.65, against the index join's
    // 4606578.48 -- "cheaper AT THE JOIN: hash, by 2233398.83".
    assert_prints(best.cost(), "2373179.65");
    assert!(matches!(best.plan, Candidate::HashJoin { .. }));
    // ├─Projection_61(Build) 8000.00 317954.13: the unordered read, which only
    // an unordered parent may take.
    assert_prints(best.costed.children[0].est_cost(), "317954.13");
    assert!(best.order.is_empty());
}

// ---------------------------------------------------------------------------
// Acceptance: `result:1169`.
// ---------------------------------------------------------------------------

fn t2_side_1169() -> LogicalNode {
    let rows = 4.800_000_000_000_001;
    let projection = Candidate::Projection {
        child: Box::new(Candidate::Reader {
            child: Box::new(Candidate::Selection {
                child: Box::new(Candidate::TableScan {
                    rows: 6.0,
                    row_size: RowSize::Fixed(32.0),
                    is_child_of_inl: None,
                    has_full_range_scan: true,
                    penalty: TableScanPenaltyInput {
                        has_range_info: false,
                        allow_prefer_range_scan: true,
                        pseudo_stats: false,
                        analyze_row_count: 6,
                        modify_count: 0,
                        has_partition_scan: false,
                        has_index_force: false,
                    },
                    num_ranges: 1,
                    desc: false,
                }),
                input_rows: 6.0,
                conditions: vec![true],
            }),
            rows,
            row_size: RowSize::Fixed(32.0),
            kind: ReaderKind::Table,
        }),
        input_rows: rows,
        exprs: vec![false, false, true],
    };
    LogicalNode::Leaf(vec![LeafAlternative {
        // `Projection_39(Build) 4.80 190.77` over a `keep order:true` scan.
        plan: projection,
        order: vec![asc(T2_A)],
        role: LeafRole::Plain,
    }])
}

fn t1_side_1169() -> LogicalNode {
    LogicalNode::Leaf(vec![LeafAlternative {
        // `TableReader_38(Probe) 2.00 12.61`, costed per outer row:
        // `0.4166... = 2/4.8`.
        plan: Candidate::Reader {
            child: Box::new(Candidate::TableScan {
                rows: 0.416_666_666_666_666_63,
                row_size: RowSize::Fixed(16.0),
                is_child_of_inl: None,
                has_full_range_scan: false,
                penalty: TableScanPenaltyInput::default(),
                num_ranges: 1,
                desc: false,
            }),
            rows: 0.416_666_666_666_666_63,
            row_size: RowSize::Fixed(16.0),
            kind: ReaderKind::Table,
        },
        order: Vec::new(),
        role: LeafRole::IndexJoinProbe {
            table_range_scan: true,
        },
    }])
}

fn join_1169() -> LogicalJoin {
    LogicalJoin {
        join_type: LogicalJoinType::Inner,
        left: Box::new(t2_side_1169()),
        right: Box::new(t1_side_1169()),
        left_keys: vec![COMPUTED],
        right_keys: vec![T1_A],
        left_schema: vec![T2_A, T2_B, COMPUTED],
        right_schema: vec![T1_A, T1_B],
        left_properties: vec![vec![T2_A]],
        right_properties: vec![vec![T1_A]],
        force_merge: false,
    }
}

struct Model1169;

impl JoinCostModel for Model1169 {
    fn attach(
        &self,
        _join: &LogicalJoin,
        strategy: &JoinStrategy,
        children: [&Task; 2],
    ) -> Option<Candidate> {
        match strategy {
            JoinStrategy::Index {
                outer_idx: 0,
                table_range_scan: true,
                kind,
                ..
            } => Some(Candidate::IndexJoin {
                build: Box::new(children[0].plan.clone()),
                probe: Box::new(children[1].plan.clone()),
                input: IndexJoinInput {
                    build_rows: 4.800_000_000_000_001,
                    build_row_size: 24.0,
                    probe_rows_one: 0.416_666_666_666_666_63,
                    probe_row_size: 16.0,
                    num_right_join_keys: 0,
                    num_left_join_keys: 0,
                    num_ranges: 0.0,
                    is_semi_join: false,
                    kind: *kind,
                },
                build_filters: Vec::new(),
                probe_filters: Vec::new(),
            }),
            _ => None,
        }
    }

    fn enforce(&self, prop: &PhysicalProperty, task: &Task) -> Option<Candidate> {
        Some(Candidate::Sort {
            child: Box::new(task.plan.clone()),
            rows: task.costed.rows,
            row_size: RowSize::Fixed(task.costed.row_size),
            by_items: vec![false; prop.sort_items.len()],
        })
    }
}

/// Go records `IndexJoin_31 2.00 4106.23` although `HashJoin_38 2.00 2423.24`
/// is cheaper at the join AND `Projection_15 2.00 3007.87` is cheaper as a
/// TREE than Go's own `Projection_15 2.00 4578.26`. Not even a whole-task
/// comparison reproduces that; the enumeration scope does. Under the parent
/// `MergeJoin`'s property this site has exactly one strategy family.
#[test]
fn result_1169_records_the_index_join_though_a_cheaper_hash_tree_exists() {
    let join = join_1169();
    let prop = ordered(&[T2_A]);
    let candidates = exhaust_join(&join, &prop);
    assert!(
        candidates
            .iter()
            .all(|candidate| matches!(candidate.strategy, JoinStrategy::Index { .. })),
        "only index joins are enumerated here: {candidates:#?}"
    );

    let best = find_best_task(
        &LogicalNode::Join(Box::new(join)),
        &prop,
        LeafRole::Plain,
        &Model1169,
        &env(),
    )
    .expect("the index join is buildable");

    // └─IndexJoin_31(Probe) 2.00 4106.23
    assert_prints(best.cost(), "4106.23");
    // The recorded executor here is `IndexJoin`, not `IndexHashJoin` as in
    // `result:1042`, and nothing hard-codes that: the two differ only in the
    // hash-table term, and on 4.80 build rows against 2.00 probe rows the
    // `IndexJoin` term is the smaller one.
    assert!(
        matches!(
            best.plan,
            Candidate::IndexJoin {
                input: IndexJoinInput {
                    kind: IndexJoinKind::IndexJoin,
                    ..
                },
                ..
            }
        ),
        "{:#?}",
        best.plan
    );
    // ├─Projection_39(Build) 4.80 190.77
    assert_prints(best.costed.children[0].est_cost(), "190.77");
    // └─TableReader_38(Probe) 2.00 12.61
    assert_prints(best.costed.children[1].est_cost(), "12.61");
}

/// The same site under the empty property does enumerate the hash join, so the
/// bar in the test above is the property and nothing else.
#[test]
fn result_1169_enumerates_the_hash_join_once_no_order_is_required() {
    let candidates = exhaust_join(&join_1169(), &PhysicalProperty::default());
    assert_eq!(
        candidates
            .iter()
            .filter(|candidate| matches!(candidate.strategy, JoinStrategy::Hash(_)))
            .count(),
        2
    );
}

// ---------------------------------------------------------------------------
// Mutation probes.
// ---------------------------------------------------------------------------

/// A model whose enforcer is FREE, which is the mutation "enforcer cost
/// dropped".
struct FreeEnforcer;

impl JoinCostModel for FreeEnforcer {
    fn attach(
        &self,
        _join: &LogicalJoin,
        _strategy: &JoinStrategy,
        _children: [&Task; 2],
    ) -> Option<Candidate> {
        None
    }

    fn enforce(&self, _prop: &PhysicalProperty, task: &Task) -> Option<Candidate> {
        Some(task.plan.clone())
    }
}

/// MUTATION PROBE: drop the enforcer's cost and this fails.
///
/// With `CanAddEnforcer`, a leaf may reach a required order by sorting its
/// cheapest unordered path. That alternative must carry the `Sort`'s own cost;
/// a free enforcer would make the cheap unordered read win every ordered
/// property in the tree.
#[test]
fn the_enforcer_must_cost_its_sort() {
    let mut prop = ordered(&[T2_A]);
    prop.can_add_enforcer = true;
    let side = t2_side_1042();

    let honest = find_best_task(&side, &prop, LeafRole::Plain, &Model1042, &env())
        .expect("the ordered path or the sorted one");
    let free = find_best_task(&side, &prop, LeafRole::Plain, &FreeEnforcer, &env())
        .expect("the free enforcer always applies");

    // The honest search keeps the ordered read at `517108.73`; a free enforcer
    // hands back the unordered `317954.13` pretending it is sorted.
    assert_prints(honest.cost(), "517108.73");
    assert_prints(free.cost(), "317954.13");
    assert!(
        honest.cost() > free.cost(),
        "a free enforcer is strictly cheaper, so only its COST keeps it out"
    );
}

/// MUTATION PROBE: stop propagating the property to the outer child and this
/// fails.
///
/// `constructIndexJoinStatic` hands the OUTER child `prop.SortItems`. Replace
/// that with the empty property and the outer side takes its cheaper
/// unordered path, the index join gets cheaper, and the plan silently stops
/// providing the order its parent `MergeJoin` requires.
#[test]
fn dropping_the_property_on_the_outer_child_changes_both_cost_and_order() {
    let join = join_1042();
    let prop = ordered(&[T2_A]);
    let faithful = find_best_task(
        &LogicalNode::Join(Box::new(join.clone())),
        &prop,
        LeafRole::Plain,
        &Model1042,
        &env(),
    )
    .expect("buildable");

    // The mutation, applied by hand to the enumeration this module produces.
    let mut mutated_candidates = exhaust_join(&join, &prop);
    for candidate in &mut mutated_candidates {
        candidate.child_props[0] = PhysicalProperty::default();
    }
    let mut cheapest: Option<Task> = None;
    for candidate in &mutated_candidates {
        let Some(left) = find_best_task(
            &join.left,
            &candidate.child_props[0],
            candidate.child_roles[0],
            &Model1042,
            &env(),
        ) else {
            continue;
        };
        let Some(right) = find_best_task(
            &join.right,
            &candidate.child_props[1],
            candidate.child_roles[1],
            &Model1042,
            &env(),
        ) else {
            continue;
        };
        let Some(plan) = Model1042.attach(&join, &candidate.strategy, [&left, &right]) else {
            continue;
        };
        let costed = candidate_cost::evaluate(&plan, &env(), CostTaskType::Root);
        let order = left.order.clone();
        let task = Task {
            plan,
            order,
            costed,
        };
        let better = cheapest
            .as_ref()
            .is_none_or(|best| candidate_cost::prefer(&task.costed, &best.costed));
        if better {
            cheapest = Some(task);
        }
    }
    let mutated = cheapest.expect("buildable");

    assert_prints(faithful.cost(), "4606578.48");
    assert!(
        mutated.cost() < faithful.cost(),
        "the mutation is CHEAPER, which is why only the property keeps it out: \
         {} against {}",
        mutated.cost(),
        faithful.cost()
    );
    assert_eq!(faithful.order, vec![asc(T2_A)]);
    assert!(
        mutated.order.is_empty(),
        "the mutation loses the order the parent MergeJoin needs"
    );
}

/// MUTATION PROBE: widen the enumeration past Go's and `result:1169` finds a
/// plan Go never generates.
///
/// The widening is the natural one -- let a hash join answer a required order
/// by sorting afterwards. Go does not: `getHashJoins` returns nothing, and no
/// enforcer runs at this site because the property forbids one.
#[test]
fn widening_the_enumeration_finds_a_plan_go_never_generates() {
    let join = join_1169();
    let prop = ordered(&[T2_A]);
    // Go's enumeration.
    assert!(!exhaust_join(&join, &prop)
        .iter()
        .any(|candidate| matches!(candidate.strategy, JoinStrategy::Hash(_))));
    // The widened one, taken from the EMPTY property as a
    // "sort it afterwards" search would.
    let widened = exhaust_join(&join, &PhysicalProperty::default());
    assert!(
        widened
            .iter()
            .any(|candidate| matches!(candidate.strategy, JoinStrategy::Hash(_))),
        "the widened enumeration reaches the hash join `result:1169` records \
         as HashJoin_38 2.00 2423.24 -- cheaper than Go's IndexJoin_31 \
         2.00 4106.23, and never chosen"
    );
}

/// `Candidate::Sort` is the enforcer, and it is priced by
/// `getPlanCostVer24PhysicalSort` rather than waved through.
#[test]
fn the_sort_enforcer_is_priced_by_the_ver2_formula() {
    let child = Candidate::Reader {
        child: Box::new(index_full_scan(100.0)),
        rows: 100.0,
        row_size: RowSize::Fixed(16.0),
        kind: ReaderKind::Index,
    };
    let bare = candidate_cost::evaluate(&child, &env(), CostTaskType::Root);
    let sorted = candidate_cost::evaluate(
        &Candidate::Sort {
            child: Box::new(child),
            rows: 100.0,
            row_size: RowSize::Fixed(16.0),
            by_items: vec![false],
        },
        &env(),
        CostTaskType::Root,
    );
    assert!(sorted.est_cost() > bare.est_cost());
    // `orderCPU(100*log2(100)*tidb_cpu_factor) + sortMem(100*16*tidb_mem_factor)`
    // on top of the child, and nothing else.
    let added = sorted.est_cost() - bare.est_cost();
    let expected = 100.0 * 100.0_f64.log2() * 49.9 + 100.0 * 16.0 * 0.2;
    assert!(
        (added - expected).abs() < 1e-6,
        "added {added}, expected {expected}"
    );
}
