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

//! Tests for [`crate::index_lookup_hash_join`].
//!
//! WRITTEN, not ported: Go's coverage (`pkg/executor/test/jointest/*`,
//! `TestIndexNestedLoopHashJoin`) runs through `testkit`. What is pinned here
//! is the pair of facts the module header claims -- the `KeepOuterOrder`
//! guarantee and its absence, and the inner-major shape of the unordered loop
//! -- plus the per-join-type emission the joiner supplies.

use super::*;
use crate::index_lookup_join::tests::{drain, fixture, TestCtx};
use crate::joiner::JoinType;

/// The hash variant over the same fixture the plain lookup join's tests use,
/// which is what makes the two files' expectations directly comparable.
fn build(
    join_type: JoinType,
    keep_outer_order: bool,
    outer: Vec<i64>,
    inner: Vec<Vec<i64>>,
) -> IndexNestedLoopHashJoin<TestCtx> {
    IndexNestedLoopHashJoin::new(fixture(join_type, outer, inner).join, keep_outer_order)
}

#[test]
fn keep_outer_order_preserves_the_outer_scan_order() {
    // Go `runInOrder` (:258) / `doJoinInOrder` (:918).
    let mut join = build(
        JoinType::Inner,
        true,
        vec![3, 1, 2, 1],
        vec![vec![1, 10], vec![1, 11], vec![2, 20]],
    );
    let rows = drain(&mut join, 3);
    assert_eq!(
        rows,
        vec![
            vec![Some(1), Some(1), Some(10)],
            vec![Some(1), Some(1), Some(11)],
            vec![Some(2), Some(2), Some(20)],
            vec![Some(1), Some(1), Some(10)],
            vec![Some(1), Some(1), Some(11)],
        ],
        "same rows and same order as the plain IndexLookUpJoin"
    );
}

#[test]
fn unordered_mode_is_inner_major_not_outer_major() {
    // Go `doJoinUnordered` (:780) walks the inner rows, so all outer rows
    // matching inner row 1 come out before any matching inner row 2. This is
    // the observable shape of "output order is not promised".
    let mut join = build(
        JoinType::Inner,
        false,
        vec![1, 2, 1],
        vec![vec![1, 10], vec![2, 20], vec![1, 11]],
    );
    let rows = drain(&mut join, 3);
    assert_eq!(
        rows,
        vec![
            // inner (1,10) matched outer rows 0 and 2
            vec![Some(1), Some(1), Some(10)],
            vec![Some(1), Some(1), Some(10)],
            // inner (2,20) matched outer row 1
            vec![Some(2), Some(2), Some(20)],
            // inner (1,11) matched outer rows 0 and 2
            vec![Some(1), Some(1), Some(11)],
            vec![Some(1), Some(1), Some(11)],
        ]
    );
}

#[test]
fn both_modes_agree_on_the_row_multiset() {
    let make = |ordered| {
        let mut join = build(
            JoinType::Inner,
            ordered,
            vec![4, 1, 2, 1, 9],
            vec![vec![1, 10], vec![2, 20], vec![1, 11], vec![4, 40]],
        );
        let mut rows = drain(&mut join, 3);
        rows.sort();
        rows
    };
    assert_eq!(make(true), make(false));
}

#[test]
fn unordered_left_outer_flushes_misses_after_the_inner_side_is_drained() {
    // Go `doJoinUnordered` (:797): `OnMissMatch` runs only once `innerExec` is
    // nil, so every miss lands at the end, in outer order.
    let mut join = build(JoinType::LeftOuter, false, vec![5, 1, 6], vec![vec![1, 10]]);
    let rows = drain(&mut join, 3);
    assert_eq!(
        rows,
        vec![
            vec![Some(1), Some(1), Some(10)],
            vec![Some(5), None, None],
            vec![Some(6), None, None],
        ]
    );
}

#[test]
fn ordered_left_outer_keeps_the_miss_in_place() {
    let mut join = build(JoinType::LeftOuter, true, vec![5, 1, 6], vec![vec![1, 10]]);
    let rows = drain(&mut join, 3);
    assert_eq!(
        rows,
        vec![
            vec![Some(5), None, None],
            vec![Some(1), Some(1), Some(10)],
            vec![Some(6), None, None],
        ]
    );
}

#[test]
fn semi_join_emits_each_outer_row_at_most_once() {
    // Go `getMatchedOuterRows` (:849) skips an outer row a semi join already
    // settled, which is what stops a second matching inner row re-emitting it.
    for keep_order in [false, true] {
        let mut join = build(
            JoinType::SemiJoin,
            keep_order,
            vec![1, 3, 1],
            vec![vec![1, 10], vec![1, 11]],
        );
        let rows = drain(&mut join, 1);
        assert_eq!(
            rows,
            vec![vec![Some(1)], vec![Some(1)]],
            "keep_outer_order = {keep_order}"
        );
    }
}

#[test]
fn anti_semi_join_emits_only_the_misses() {
    for keep_order in [false, true] {
        let mut join = build(
            JoinType::AntiSemiJoin,
            keep_order,
            vec![1, 3, 4],
            vec![vec![1, 10]],
        );
        let rows = drain(&mut join, 1);
        assert_eq!(
            rows,
            vec![vec![Some(3)], vec![Some(4)]],
            "keep_outer_order = {keep_order}"
        );
    }
}

#[test]
fn incremental_lookup_is_offered_only_to_the_monotone_join_types() {
    // Go `supportIncrementalLookUp` (:476).
    for (join_type, expected) in [
        (JoinType::Inner, true),
        (JoinType::LeftOuter, true),
        (JoinType::RightOuter, true),
        (JoinType::AntiSemiJoin, true),
        (JoinType::SemiJoin, false),
        (JoinType::LeftOuterSemiJoin, false),
        (JoinType::AntiLeftOuterSemiJoin, false),
    ] {
        let join = build(join_type, false, vec![1], vec![vec![1, 10]]);
        assert_eq!(
            join.support_incremental_lookup(),
            expected,
            "{join_type:?} unordered"
        );
        let ordered = build(join_type, true, vec![1], vec![vec![1, 10]]);
        assert!(
            !ordered.support_incremental_lookup(),
            "{join_type:?} keeping outer order can never fetch incrementally"
        );
    }
}

#[test]
fn a_bounded_inner_fetch_still_yields_every_row() {
    // Drive the incremental path: with `max_fetch_size` of 1 the inner reader
    // is drained one row per round and the probe loop repeats (Go
    // `handleTask` :765).
    let mut join = build(
        JoinType::Inner,
        false,
        vec![1, 2],
        vec![vec![1, 10], vec![2, 20], vec![1, 11]],
    );
    join.base.max_fetch_size = 1;
    let mut rows = drain(&mut join, 3);
    rows.sort();
    assert_eq!(
        rows,
        vec![
            vec![Some(1), Some(1), Some(10)],
            vec![Some(1), Some(1), Some(11)],
            vec![Some(2), Some(2), Some(20)],
        ]
    );
}

#[test]
fn empty_outer_side_produces_no_rows() {
    for keep_order in [false, true] {
        let mut join = build(
            JoinType::LeftOuter,
            keep_order,
            Vec::new(),
            vec![vec![1, 10]],
        );
        assert!(drain(&mut join, 3).is_empty());
    }
}
