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

use super::tests::{eq_on, join_of, join_with_memory, run};
use super::{ExecError, JoinKind};
use crate::executor::Executor;
use crate::mem_quota::{OomAction, StatementMemory};
use crate::merge_join_plan::{MergeJoinKey, MergeJoinPlan};
use std::sync::Arc;
use tidb_datatype::Datum;

/// The same fixture shape as the hash differential test -- duplicate keys
/// on both sides, keys present on one side only, and NULL keys -- but
/// SORTED, because that is the promise a merge join is given.
///
/// NULLs sort first, which is where a key-ordered read puts them, so this
/// is a stream a real ordered scan could produce.
fn sorted_fixture(n: i64, modulus: i64, nulls: bool) -> Vec<Vec<Datum>> {
    let mut rows: Vec<Vec<Datum>> = (0..n)
        .map(|i| {
            let key = if nulls && i % 11 == 10 {
                Datum::Null
            } else {
                Datum::Int(i % modulus)
            };
            vec![key, Datum::Int(i)]
        })
        .collect();
    rows.sort_by_key(|row| match row[0] {
        Datum::Null => (0, 0),
        Datum::Int(key) => (1, key),
        _ => unreachable!("the fixture builds only NULLs and ints"),
    });
    rows
}

fn large_duplicate_group_merge(memory: StatementMemory) -> super::JoinExec<tidb_expr::NoColumns> {
    let left = vec![vec![Datum::Int(1), Datum::Int(0)]];
    let right: Vec<Vec<Datum>> = (0..5000)
        .map(|value| vec![Datum::Int(1), Datum::Int(value)])
        .collect();
    let mut join = join_with_memory(
        JoinKind::Inner,
        vec![eq_on(0, 0, 2)],
        left,
        right,
        2,
        memory,
    );
    join.set_merge_plan(MergeJoinPlan {
        keys: vec![MergeJoinKey { left: 0, right: 0 }],
        desc: false,
    });
    join
}

/// A multiset comparison: the merge path emits in KEY order and the hash
/// path in OUTER-ROW order, so the two agree on rows without agreeing on
/// their sequence. That difference is the algorithm, not a bug -- Go's
/// merge join reorders the result the same way, which is why its plans
/// still carry a `Sort` above when the query asked for one.
pub(super) fn as_multiset(mut rows: Vec<Vec<i64>>) -> Vec<Vec<i64>> {
    rows.sort_unstable();
    rows
}

/// The merge path must produce the same ROWS as the hash path for every
/// join kind, over sorted data with duplicate keys on BOTH sides (the
/// group-by-group cross product), keys matched on neither side, and NULL
/// keys (which must match nothing, not even each other).
#[test]
fn merge_path_matches_the_hash_path_row_for_row() {
    for kind in [JoinKind::Inner, JoinKind::Left, JoinKind::Right] {
        let left = sorted_fixture(200, 7, true);
        let right = sorted_fixture(200, 5, true);
        let mut merged = join_of(kind, vec![eq_on(0, 0, 2)], left.clone(), right.clone(), 2);
        merged.set_merge_plan(MergeJoinPlan {
            keys: vec![MergeJoinKey { left: 0, right: 0 }],
            desc: false,
        });
        assert!(merged.is_merge_join());
        let mut hashed = join_of(kind, vec![eq_on(0, 0, 2)], left, right, 2);
        assert!(hashed.is_hash_join());
        assert_eq!(
            as_multiset(run(&mut merged)),
            as_multiset(run(&mut hashed)),
            "{kind:?}"
        );
    }
}

/// A residual conjunct still filters the pairs a matched group produces,
/// and an outer row every pair rejects is still emitted NULL-padded --
/// the rule `emit_outer_row` owns for all three strategies.
#[test]
fn residual_conditions_still_filter_merged_groups() {
    let left = sorted_fixture(150, 7, false);
    let right = sorted_fixture(150, 5, false);
    let conditions = vec![eq_on(0, 0, 2), eq_on(1, 1, 2)];
    let mut merged = join_of(
        JoinKind::Left,
        conditions.clone(),
        left.clone(),
        right.clone(),
        2,
    );
    merged.set_merge_plan(MergeJoinPlan {
        keys: vec![MergeJoinKey { left: 0, right: 0 }],
        desc: false,
    });
    let mut hashed = join_of(JoinKind::Left, conditions, left, right, 2);
    assert_eq!(as_multiset(run(&mut merged)), as_multiset(run(&mut hashed)));
}

/// A DESCENDING merge reads both sides high to low, and must find the
/// same matches: `PhysicalMergeJoin.Desc` reverses the comparison, not
/// the semantics.
#[test]
fn a_descending_merge_finds_the_same_matches() {
    let mut left = sorted_fixture(120, 7, false);
    let mut right = sorted_fixture(120, 5, false);
    left.reverse();
    right.reverse();
    let mut merged = join_of(
        JoinKind::Inner,
        vec![eq_on(0, 0, 2)],
        left.clone(),
        right.clone(),
        2,
    );
    merged.set_merge_plan(MergeJoinPlan {
        keys: vec![MergeJoinKey { left: 0, right: 0 }],
        desc: true,
    });
    let mut hashed = join_of(JoinKind::Inner, vec![eq_on(0, 0, 2)], left, right, 2);
    assert_eq!(as_multiset(run(&mut merged)), as_multiset(run(&mut hashed)));
}

/// One empty side: an inner join produces nothing, and an outer join
/// still emits every preserved row NULL-padded. This is the arm where a
/// merge loop most easily stops early.
#[test]
fn an_empty_side_still_emits_the_preserved_rows() {
    for (kind, expected) in [
        (JoinKind::Inner, 0),
        (JoinKind::Left, 30),
        (JoinKind::Right, 0),
    ] {
        let left = sorted_fixture(30, 7, false);
        let mut merged = join_of(kind, vec![eq_on(0, 0, 2)], left, Vec::new(), 2);
        merged.set_merge_plan(MergeJoinPlan {
            keys: vec![MergeJoinKey { left: 0, right: 0 }],
            desc: false,
        });
        assert_eq!(run(&mut merged).len(), expected, "{kind:?}");
    }
}

/// A group larger than one chunk must still be one group: the merge
/// collects a whole run of equal keys before joining it, so a 3000-row
/// group spanning several source chunks fans out completely.
#[test]
fn a_group_spanning_chunks_is_still_one_group() {
    let left: Vec<Vec<Datum>> = (0..3000)
        .map(|i| vec![Datum::Int(1), Datum::Int(i)])
        .collect();
    let right = vec![vec![Datum::Int(1), Datum::Int(0)]; 3];
    let mut merged = join_of(JoinKind::Inner, vec![eq_on(0, 0, 2)], left, right, 2);
    merged.set_merge_plan(MergeJoinPlan {
        keys: vec![MergeJoinKey { left: 0, right: 0 }],
        desc: false,
    });
    assert_eq!(run(&mut merged).len(), 9000);
}

/// Go keeps a current inner chunk outside the spillable row container and
/// only transfers completed chunks when an equal-key run crosses a chunk
/// boundary. A unique-key inner stream must therefore not perform one
/// RowContainer add/reset cycle per row.
#[test]
fn a_single_row_inner_group_stays_in_the_reusable_staging_chunk() {
    let left: Vec<Vec<Datum>> = (0..3000)
        .map(|i| vec![Datum::Int(i), Datum::Int(i)])
        .collect();
    let right = left.clone();
    let mut merged = join_of(JoinKind::Inner, vec![eq_on(0, 0, 2)], left, right, 2);
    merged.set_merge_plan(MergeJoinPlan {
        keys: vec![MergeJoinKey { left: 0, right: 0 }],
        desc: false,
    });
    merged.open().unwrap();
    let mut req = merged.new_chunk();
    merged.next(&mut req).unwrap();
    assert_eq!(merged.merge_inner_container_chunks(), 0);
    merged.close().unwrap();
}

/// The OUTER side is a stream, not a second materialized equal-key group.
/// A duplicate run larger than the statement quota must reuse the small
/// installed INNER group one row at a time instead of being cancelled or
/// spilled as another build side.
#[test]
fn a_large_outer_duplicate_run_remains_streaming() {
    for kind in [JoinKind::Left, JoinKind::Right] {
        let large: Vec<Vec<Datum>> = (0..5000)
            .map(|value| vec![Datum::Int(1), Datum::Int(value)])
            .collect();
        let small: Vec<Vec<Datum>> = (0..3)
            .map(|value| vec![Datum::Int(1), Datum::Int(value)])
            .collect();
        let (left, right) = if kind == JoinKind::Left {
            (large, small)
        } else {
            (small, large)
        };
        let mut merged = join_with_memory(
            kind,
            vec![eq_on(0, 0, 2)],
            left,
            right,
            2,
            StatementMemory::new(64 * 1024, OomAction::Cancel, 1),
        );
        merged.set_merge_plan(MergeJoinPlan {
            keys: vec![MergeJoinKey { left: 0, right: 0 }],
            desc: false,
        });

        assert_eq!(run(&mut merged).len(), 15_000, "{kind:?}");
    }
}

/// Go `TestMergeJoinInDisk`: one equal-key inner group may span many
/// chunks, so it must move to the row-container spill file instead of
/// cancelling the statement or retaining every datum row in memory.
#[test]
fn a_large_duplicate_group_spills_and_matches_the_unspilled_result() {
    let mut roomy = large_duplicate_group_merge(StatementMemory::default());
    let expected = run(&mut roomy);
    assert!(!roomy.build_side_spilled());
    assert_eq!(expected.len(), 5000);

    let mut tight =
        large_duplicate_group_merge(StatementMemory::new(64 * 1024, OomAction::Cancel, 1));
    let actual = run(&mut tight);
    assert!(
        tight.build_side_spilled(),
        "the merge inner group must actually reach disk"
    );
    assert!(tight.spilled_bytes() > 0);
    assert_eq!(actual, expected);
}

/// A spilled cross product is still a streaming executor result: one
/// `Next` honors the parent's `required_rows` instead of recreating the
/// whole disk-backed group as one oversized output chunk.
#[test]
fn spilled_merge_cross_product_honors_required_rows() {
    let mut join =
        large_duplicate_group_merge(StatementMemory::new(64 * 1024, OomAction::Cancel, 1));
    join.open().unwrap();
    let mut req = join.new_chunk();
    req.set_required_rows(137, 1024);
    let mut rows = 0;
    let mut batches = 0;
    loop {
        join.next(&mut req).unwrap();
        if req.num_rows() == 0 {
            break;
        }
        assert!(req.num_rows() <= 137, "batch had {} rows", req.num_rows());
        rows += req.num_rows();
        batches += 1;
    }
    assert_eq!(rows, 5000);
    assert!(batches > 1);
    join.close().unwrap();
    assert!(join.build_side_spilled());
}

/// A RIGHT join flips the inner authority to the left child. The same
/// spill path must preserve left-then-right output ordering and the full
/// duplicate fanout.
#[test]
fn right_merge_spills_its_left_inner_group() {
    let left: Vec<Vec<Datum>> = (0..5000)
        .map(|value| vec![Datum::Int(1), Datum::Int(value)])
        .collect();
    let right = vec![vec![Datum::Int(1), Datum::Int(0)]];
    let make = |memory| {
        let mut join = join_with_memory(
            JoinKind::Right,
            vec![eq_on(0, 0, 2)],
            left.clone(),
            right.clone(),
            2,
            memory,
        );
        join.set_merge_plan(MergeJoinPlan {
            keys: vec![MergeJoinKey { left: 0, right: 0 }],
            desc: false,
        });
        join
    };
    let mut roomy = make(StatementMemory::default());
    let expected = run(&mut roomy);
    let mut tight = make(StatementMemory::new(64 * 1024, OomAction::Cancel, 1));
    let actual = run(&mut tight);
    assert!(tight.build_side_spilled());
    assert_eq!(actual, expected);
    assert_eq!(actual.len(), 5000);
    assert_eq!(actual[0], vec![1, 0, 1, 0]);
    assert_eq!(actual[4999], vec![1, 4999, 1, 0]);
}

/// `tidb_enable_tmp_storage_on_oom=OFF` keeps the session cancellation at
/// the head of the action chain, so the same group returns errno 8175
/// instead of creating a spill file.
#[test]
fn merge_group_respects_the_tmp_storage_gate() {
    let memory =
        StatementMemory::new(64 * 1024, OomAction::Cancel, 1).with_tmp_storage_on_oom(false);
    let mut join = large_duplicate_group_merge(memory);
    join.open().unwrap();
    let mut req = join.new_chunk();
    let error = join
        .next(&mut req)
        .expect_err("the disabled spill gate must enforce the quota");
    assert!(matches!(error, ExecError::MemoryExceedForQuery { .. }));
    assert!(!join.build_side_spilled());
    assert!(join.registered_spill_action().is_none());
    join.close().unwrap();
}

/// The merge group action is statement-scoped. Closing the join must
/// remove that exact action and restore its cancellation fallback.
#[test]
fn merge_close_unbinds_its_spill_action() {
    let memory = StatementMemory::new(64 * 1024, OomAction::Cancel, 1);
    let mut join = large_duplicate_group_merge(memory.clone());
    join.open().unwrap();
    let mut req = join.new_chunk();
    join.next(&mut req).unwrap();
    let action = join
        .registered_spill_action()
        .expect("merge inner group registers a spill action");
    let head = memory
        .session_tracker()
        .get_fallback_for_test(false)
        .expect("session action chain");
    assert!(Arc::ptr_eq(&head, &action));

    join.close().unwrap();
    assert_eq!(join.tracker.bytes_consumed(), 0);
    assert!(join.merge_state.is_none());

    let mut current = memory.session_tracker().get_fallback_for_test(false);
    while let Some(candidate) = current {
        assert!(!Arc::ptr_eq(&candidate, &action));
        current = candidate.get_fallback();
    }
}
