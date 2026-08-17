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

//! Tests for `pkg/executor/join/joiner.go`.
//!
//! GO PORT: `TestJoinerOtherConditionChunkUsesInitChunkSize`
//! (`joiner_test.go:45`) lands as
//! [`other_condition_chunk_uses_init_chunk_size`]. The rest of Go's coverage
//! for these strategies runs through `testkit` SQL in
//! `pkg/executor/test/jointest` and through the v2 probe tests, neither of
//! which is dependency-closed here.
//!
//! WRITTEN COVERAGE: the tables below pin, per join type, exactly which row
//! reaches the output on a match and on a miss -- the contract Go states in
//! prose at the `Joiner` interface and nowhere asserts directly.

use super::*;

use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_expr::column::Column;
use tidb_expr::Columns;

/// A `Columns` context with no columns and no session state. It must be
/// `Clone` because Go's `baseJoiner.Clone` copies the `sessionctx`.
#[derive(Clone)]
struct TestCtx;

impl Columns for TestCtx {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }
}

const MAX_CHUNK: usize = 32;

fn long() -> FieldType {
    FieldType::new(FieldTypeCode::Long)
}

fn sizes() -> JoinerChunkSizes {
    JoinerChunkSizes {
        init_chunk_size: 8,
        max_chunk_size: MAX_CHUNK,
    }
}

/// A one-column chunk holding `values`; `None` is a NULL cell.
fn chunk_of(values: &[Option<i64>]) -> Chunk {
    let mut chunk = Chunk::new(&[long()], MAX_CHUNK, MAX_CHUNK);
    for value in values {
        match value {
            Some(value) => chunk.append_int64(0, *value),
            None => chunk.append_null(0),
        }
        chunk.set_num_virtual_rows(chunk.num_rows());
    }
    chunk
}

/// The output chunk for a join of `width` columns.
fn output(width: usize) -> Chunk {
    let fields = vec![long(); width];
    Chunk::new(&fields, MAX_CHUNK, MAX_CHUNK)
}

/// Reads a chunk back as `Option<i64>` rows.
fn rows_of(chunk: &Chunk) -> Vec<Vec<Option<i64>>> {
    (0..chunk.num_rows())
        .map(|index| {
            let row = chunk.get_row(index);
            (0..chunk.num_cols())
                .map(|col| {
                    if row.is_null(col) {
                        None
                    } else {
                        Some(row.get_int64(col))
                    }
                })
                .collect()
        })
        .collect()
}

/// A condition that reads the column at `index` of the shallow join row and
/// uses it directly as the boolean. It needs no builtin dispatch, so it is
/// the cheapest way to drive a per-inner-row TRUE/FALSE verdict.
fn column_condition(index: usize) -> Expression {
    let mut column = Column::new(index as i64 + 1, long());
    column.index = index as i64;
    Expression::Column(column)
}

/// The constant `1`, i.e. a condition that is always TRUE.
fn always_true() -> Expression {
    Expression::Constant(tidb_expr::constant::Constant::new(Datum::Int(1), long()))
}

/// The constant `0`, i.e. a condition that is always FALSE.
fn always_false() -> Expression {
    Expression::Constant(tidb_expr::constant::Constant::new(Datum::Int(0), long()))
}

/// Drives Go's documented instruction flow over one outer row against one
/// inner chunk, and returns the resulting output rows.
fn drive(
    joiner: &mut dyn Joiner,
    outer: &Chunk,
    outer_index: usize,
    inners: &Chunk,
    out_width: usize,
    opt: NAAJType,
) -> Vec<Vec<Option<i64>>> {
    let mut chk = output(out_width);
    let mut iter = tidb_chunk::iterator::LendingIterator::chunk(inners);
    iter.begin();
    let outer_row = outer.get_row(outer_index);
    let mut has_match = false;
    let mut has_null = false;
    while iter.current().is_some() {
        let (matched, is_null) = joiner
            .try_to_match_inners(outer_row, &mut iter, &mut chk, opt)
            .expect("condition evaluation succeeds");
        has_match = has_match || matched;
        has_null = has_null || is_null;
    }
    if !has_match {
        joiner.on_miss_match(has_null, outer_row, &mut chk);
    }
    rows_of(&chk)
}

// ---------------------------------------------------------------------------
// Row-producing join types.
// ---------------------------------------------------------------------------

#[test]
fn inner_joiner_emits_every_pair_and_nothing_on_a_miss() {
    let outer = chunk_of(&[Some(7)]);
    let inners = chunk_of(&[Some(1), Some(2)]);
    let mut joiner = new_joiner(
        TestCtx,
        JoinType::Inner,
        false,
        &[],
        Vec::new(),
        &[long()],
        &[long()],
        None,
        false,
        sizes(),
    );
    assert_eq!(
        drive(&mut *joiner, &outer, 0, &inners, 2, NAAJType::Unknown),
        vec![vec![Some(7), Some(1)], vec![Some(7), Some(2)]]
    );

    // An empty inner side is a miss, and an inner join emits nothing for it.
    let empty = chunk_of(&[]);
    assert!(drive(&mut *joiner, &outer, 0, &empty, 2, NAAJType::Unknown).is_empty());
}

#[test]
fn left_outer_joiner_pads_a_missed_outer_row_with_the_default_inner() {
    let outer = chunk_of(&[Some(7)]);
    let mut joiner = new_joiner(
        TestCtx,
        JoinType::LeftOuter,
        false,
        &[Datum::Null],
        Vec::new(),
        &[long()],
        &[long()],
        None,
        false,
        sizes(),
    );
    // Matching side: the pair, left-then-right.
    let inners = chunk_of(&[Some(4)]);
    assert_eq!(
        drive(&mut *joiner, &outer, 0, &inners, 2, NAAJType::Unknown),
        vec![vec![Some(7), Some(4)]]
    );
    // Missing side: the outer row survives, padded on the right.
    let empty = chunk_of(&[]);
    assert_eq!(
        drive(&mut *joiner, &outer, 0, &empty, 2, NAAJType::Unknown),
        vec![vec![Some(7), None]]
    );
}

#[test]
fn right_outer_joiner_pads_a_missed_outer_row_on_the_left() {
    // `outerIsRight` for a right outer join: the padded columns are the LEFT
    // ones and the outer row lands on the right.
    let outer = chunk_of(&[Some(7)]);
    let mut joiner = new_joiner(
        TestCtx,
        JoinType::RightOuter,
        true,
        &[Datum::Null],
        Vec::new(),
        &[long()],
        &[long()],
        None,
        false,
        sizes(),
    );
    let inners = chunk_of(&[Some(4)]);
    assert_eq!(
        drive(&mut *joiner, &outer, 0, &inners, 2, NAAJType::Unknown),
        vec![vec![Some(4), Some(7)]]
    );
    let empty = chunk_of(&[]);
    assert_eq!(
        drive(&mut *joiner, &outer, 0, &empty, 2, NAAJType::Unknown),
        vec![vec![None, Some(7)]]
    );
}

#[test]
fn a_condition_that_rejects_every_pair_makes_the_outer_row_a_miss() {
    // This is the distinction Go's interface comment draws: an outer row whose
    // joined rows all fail the condition is UNMATCHED, so a left outer join
    // still emits it -- padded, not paired.
    let outer = chunk_of(&[Some(7)]);
    let inners = chunk_of(&[Some(1), Some(2)]);
    let mut joiner = new_joiner(
        TestCtx,
        JoinType::LeftOuter,
        false,
        &[Datum::Null],
        vec![always_false()],
        &[long()],
        &[long()],
        None,
        false,
        sizes(),
    );
    assert_eq!(
        drive(&mut *joiner, &outer, 0, &inners, 2, NAAJType::Unknown),
        vec![vec![Some(7), None]]
    );

    let mut joiner = new_joiner(
        TestCtx,
        JoinType::LeftOuter,
        false,
        &[Datum::Null],
        vec![always_true()],
        &[long()],
        &[long()],
        None,
        false,
        sizes(),
    );
    assert_eq!(
        drive(&mut *joiner, &outer, 0, &inners, 2, NAAJType::Unknown),
        vec![vec![Some(7), Some(1)], vec![Some(7), Some(2)]]
    );
}

#[test]
fn the_condition_selects_which_pairs_survive() {
    // The condition reads column 1 of the joined row -- the inner value -- so
    // only the truthy inner rows pair up.
    let outer = chunk_of(&[Some(7)]);
    let inners = chunk_of(&[Some(0), Some(1), Some(0), Some(5)]);
    let mut joiner = new_joiner(
        TestCtx,
        JoinType::Inner,
        false,
        &[],
        vec![column_condition(1)],
        &[long()],
        &[long()],
        None,
        false,
        sizes(),
    );
    assert_eq!(
        drive(&mut *joiner, &outer, 0, &inners, 2, NAAJType::Unknown),
        vec![vec![Some(7), Some(1)], vec![Some(7), Some(5)]]
    );
}

// ---------------------------------------------------------------------------
// Semi family.
// ---------------------------------------------------------------------------

#[test]
fn semi_joiner_emits_the_outer_row_once_and_never_on_a_miss() {
    let outer = chunk_of(&[Some(7)]);
    let inners = chunk_of(&[Some(1), Some(2), Some(3)]);
    let mut joiner = new_joiner(
        TestCtx,
        JoinType::SemiJoin,
        false,
        &[],
        Vec::new(),
        &[long()],
        &[long()],
        None,
        false,
        sizes(),
    );
    assert!(joiner.is_semi_join_without_condition());
    assert_eq!(
        drive(&mut *joiner, &outer, 0, &inners, 1, NAAJType::Unknown),
        vec![vec![Some(7)]]
    );

    let empty = chunk_of(&[]);
    assert!(drive(&mut *joiner, &outer, 0, &empty, 1, NAAJType::Unknown).is_empty());
}

#[test]
fn semi_joiner_with_a_condition_needs_one_passing_inner_row() {
    let outer = chunk_of(&[Some(7)]);
    let mut joiner = new_joiner(
        TestCtx,
        JoinType::SemiJoin,
        false,
        &[],
        vec![column_condition(1)],
        &[long()],
        &[long()],
        None,
        false,
        sizes(),
    );
    assert!(!joiner.is_semi_join_without_condition());
    // Only the third inner row passes; the outer row is still emitted once.
    let inners = chunk_of(&[Some(0), Some(0), Some(9)]);
    assert_eq!(
        drive(&mut *joiner, &outer, 0, &inners, 1, NAAJType::Unknown),
        vec![vec![Some(7)]]
    );
    // No inner row passes: nothing at all.
    let inners = chunk_of(&[Some(0), Some(0)]);
    assert!(drive(&mut *joiner, &outer, 0, &inners, 1, NAAJType::Unknown).is_empty());
}

#[test]
fn anti_semi_joiner_emits_the_outer_row_only_when_nothing_matched() {
    let outer = chunk_of(&[Some(7)]);
    let mut joiner = new_joiner(
        TestCtx,
        JoinType::AntiSemiJoin,
        false,
        &[],
        vec![column_condition(1)],
        &[long()],
        &[long()],
        None,
        false,
        sizes(),
    );
    // Some inner row matches: the outer row is suppressed.
    let inners = chunk_of(&[Some(0), Some(1)]);
    assert!(drive(&mut *joiner, &outer, 0, &inners, 1, NAAJType::Unknown).is_empty());
    // Nothing matches: the outer row survives.
    let inners = chunk_of(&[Some(0), Some(0)]);
    assert_eq!(
        drive(&mut *joiner, &outer, 0, &inners, 1, NAAJType::Unknown),
        vec![vec![Some(7)]]
    );
    // An empty inner side is also a miss.
    let empty = chunk_of(&[]);
    assert_eq!(
        drive(&mut *joiner, &outer, 0, &empty, 1, NAAJType::Unknown),
        vec![vec![Some(7)]]
    );
}

#[test]
fn anti_semi_joiner_suppresses_the_outer_row_when_the_miss_saw_null() {
    // `OnMissMatch(hasNull=true)` is the whole reason the miss carries a
    // reason: `x NOT IN (...)` is UNKNOWN, not TRUE, once a NULL was seen.
    let outer = chunk_of(&[Some(7)]);
    let mut joiner = new_joiner(
        TestCtx,
        JoinType::AntiSemiJoin,
        false,
        &[],
        vec![column_condition(1)],
        &[long()],
        &[long()],
        None,
        false,
        sizes(),
    );
    let mut chk = output(1);
    joiner.on_miss_match(true, outer.get_row(0), &mut chk);
    assert!(rows_of(&chk).is_empty());
    joiner.on_miss_match(false, outer.get_row(0), &mut chk);
    assert_eq!(rows_of(&chk), vec![vec![Some(7)]]);
}

#[test]
fn left_outer_semi_joiner_flags_match_as_one_false_miss_as_zero_and_null_miss_as_null() {
    let outer = chunk_of(&[Some(7)]);
    let mut joiner = new_joiner(
        TestCtx,
        JoinType::LeftOuterSemiJoin,
        false,
        &[],
        vec![column_condition(1)],
        &[long()],
        &[long()],
        None,
        false,
        sizes(),
    );
    let inners = chunk_of(&[Some(0), Some(3)]);
    assert_eq!(
        drive(&mut *joiner, &outer, 0, &inners, 2, NAAJType::Unknown),
        vec![vec![Some(7), Some(1)]]
    );
    let inners = chunk_of(&[Some(0)]);
    assert_eq!(
        drive(&mut *joiner, &outer, 0, &inners, 2, NAAJType::Unknown),
        vec![vec![Some(7), Some(0)]]
    );
    let mut chk = output(2);
    joiner.on_miss_match(true, outer.get_row(0), &mut chk);
    assert_eq!(rows_of(&chk), vec![vec![Some(7), None]]);
}

#[test]
fn anti_left_outer_semi_joiner_flags_are_the_negation() {
    let outer = chunk_of(&[Some(7)]);
    let mut joiner = new_joiner(
        TestCtx,
        JoinType::AntiLeftOuterSemiJoin,
        false,
        &[],
        vec![column_condition(1)],
        &[long()],
        &[long()],
        None,
        false,
        sizes(),
    );
    // Match -> 0 (where the left-outer-semi joiner writes 1).
    let inners = chunk_of(&[Some(3)]);
    assert_eq!(
        drive(&mut *joiner, &outer, 0, &inners, 2, NAAJType::Unknown),
        vec![vec![Some(7), Some(0)]]
    );
    // FALSE miss -> 1.
    let inners = chunk_of(&[Some(0)]);
    assert_eq!(
        drive(&mut *joiner, &outer, 0, &inners, 2, NAAJType::Unknown),
        vec![vec![Some(7), Some(1)]]
    );
    // NULL miss -> NULL, the same as the non-anti form.
    let mut chk = output(2);
    joiner.on_miss_match(true, outer.get_row(0), &mut chk);
    assert_eq!(rows_of(&chk), vec![vec![Some(7), None]]);
}

// ---------------------------------------------------------------------------
// Null-aware variants.
// ---------------------------------------------------------------------------

#[test]
fn null_aware_anti_semi_joiner_refuses_the_probe_row_for_any_bucket_row() {
    // Its conditions are only the inner filters; the NA-EQ nullness was
    // already decided by the bucket the caller drew `inners` from, so ANY
    // surviving inner row refuses the probe row.
    let outer = chunk_of(&[Some(7)]);
    let inners = chunk_of(&[Some(0)]);
    let mut joiner = new_joiner(
        TestCtx,
        JoinType::AntiSemiJoin,
        false,
        &[],
        Vec::new(),
        &[long()],
        &[long()],
        None,
        true,
        sizes(),
    );
    assert_eq!(joiner.join_type(), JoinType::AntiSemiJoin);
    assert!(drive(&mut *joiner, &outer, 0, &inners, 1, NAAJType::Unknown).is_empty());

    // With an inner filter that rejects every row, the bucket contributes
    // nothing and the probe row survives.
    let mut joiner = new_joiner(
        TestCtx,
        JoinType::AntiSemiJoin,
        false,
        &[],
        vec![always_false()],
        &[long()],
        &[long()],
        None,
        true,
        sizes(),
    );
    assert_eq!(
        drive(&mut *joiner, &outer, 0, &inners, 1, NAAJType::Unknown),
        vec![vec![Some(7)]]
    );
}

#[test]
fn null_aware_anti_left_outer_semi_joiner_takes_its_flag_from_the_naaj_type() {
    let outer = chunk_of(&[Some(7)]);
    let inners = chunk_of(&[Some(0)]);
    let flag_for = |opt: NAAJType| {
        let mut joiner = new_joiner(
            TestCtx,
            JoinType::AntiLeftOuterSemiJoin,
            false,
            &[],
            Vec::new(),
            &[long()],
            &[long()],
            None,
            true,
            sizes(),
        );
        drive(&mut *joiner, &outer, 0, &inners, 2, opt)
    };
    // Neither key null: `x NOT IN (x...)` is FALSE.
    assert_eq!(
        flag_for(NAAJType::LeftNotNullRightNotNull),
        vec![vec![Some(7), Some(0)]]
    );
    // A null on either side makes the answer UNKNOWN.
    for opt in [
        NAAJType::LeftNotNullRightHasNull,
        NAAJType::LeftHasNullRightHasNull,
        NAAJType::LeftHasNullRightNotNull,
    ] {
        assert_eq!(flag_for(opt), vec![vec![Some(7), None]]);
    }

    // An empty bucket is a miss, and a miss is TRUE regardless of nullness.
    let mut joiner = new_joiner(
        TestCtx,
        JoinType::AntiLeftOuterSemiJoin,
        false,
        &[],
        Vec::new(),
        &[long()],
        &[long()],
        None,
        true,
        sizes(),
    );
    let empty = chunk_of(&[]);
    assert_eq!(
        drive(
            &mut *joiner,
            &outer,
            0,
            &empty,
            2,
            NAAJType::LeftHasNullRightHasNull
        ),
        vec![vec![Some(7), Some(1)]]
    );
}

// ---------------------------------------------------------------------------
// `TryToMatchOuters`.
// ---------------------------------------------------------------------------

#[test]
fn try_to_match_outers_reports_one_status_per_consumed_outer_row() {
    let outers = chunk_of(&[Some(1), Some(0), Some(4)]);
    let inner = chunk_of(&[Some(9)]);
    let mut joiner = new_joiner(
        TestCtx,
        JoinType::SemiJoin,
        false,
        &[],
        // Reads column 0 of the shallow row, which for a semi join with
        // `outer_is_right == false` is the OUTER value.
        vec![column_condition(0)],
        &[long()],
        &[long()],
        None,
        false,
        sizes(),
    );
    let mut chk = output(1);
    let mut iter = tidb_chunk::iterator::LendingIterator::chunk(&outers);
    iter.begin();
    let mut status = Vec::new();
    joiner
        .try_to_match_outers(&mut iter, inner.get_row(0), &mut chk, &mut status)
        .expect("condition evaluation succeeds");
    assert_eq!(
        status,
        vec![
            OuterRowStatusFlag::Matched,
            OuterRowStatusFlag::Unmatched,
            OuterRowStatusFlag::Matched,
        ]
    );
    assert_eq!(rows_of(&chk), vec![vec![Some(1)], vec![Some(4)]]);
}

#[test]
fn inner_joiner_try_to_match_outers_marks_every_appended_row_matched() {
    let outers = chunk_of(&[Some(1), Some(2)]);
    let inner = chunk_of(&[Some(9)]);
    let mut joiner = new_joiner(
        TestCtx,
        JoinType::Inner,
        false,
        &[],
        Vec::new(),
        &[long()],
        &[long()],
        None,
        false,
        sizes(),
    );
    let mut chk = output(2);
    let mut iter = tidb_chunk::iterator::LendingIterator::chunk(&outers);
    iter.begin();
    let mut status = Vec::new();
    joiner
        .try_to_match_outers(&mut iter, inner.get_row(0), &mut chk, &mut status)
        .expect("no condition to evaluate");
    assert_eq!(
        status,
        vec![OuterRowStatusFlag::Matched, OuterRowStatusFlag::Matched]
    );
    assert_eq!(
        rows_of(&chk),
        vec![vec![Some(1), Some(9)], vec![Some(2), Some(9)]]
    );
}

// ---------------------------------------------------------------------------
// Inline projection, `EvalBool`, and the Go port.
// ---------------------------------------------------------------------------

#[test]
fn the_inline_projection_prunes_the_output_but_not_the_condition() {
    // `rUsed` is empty (non-nil), so no inner column reaches the output --
    // but the condition still reads inner column 1 of the shallow row, which
    // is exactly what Go's `makeShallowJoinRow` comment promises.
    let outer = chunk_of(&[Some(7)]);
    let inners = chunk_of(&[Some(0), Some(5)]);
    let mut joiner = new_joiner(
        TestCtx,
        JoinType::Inner,
        false,
        &[],
        vec![column_condition(1)],
        &[long()],
        &[long()],
        Some((vec![0], Vec::new())),
        false,
        sizes(),
    );
    assert_eq!(
        drive(&mut *joiner, &outer, 0, &inners, 1, NAAJType::Unknown),
        vec![vec![Some(7)]]
    );
}

#[test]
fn eval_bool_treats_a_plain_null_as_false_and_an_eq_from_in_null_as_unknown() {
    // Go `expression.EvalBool`: a NULL short-circuits to (false, false) unless
    // `IsEQCondFromIn`, which is what lets an anti-semi join tell "no match"
    // from "cannot tell".
    let row_chunk = chunk_of(&[None]);
    let row = row_chunk.get_row(0);

    let plain = column_condition(0);
    assert_eq!(
        eval_bool(&TestCtx, std::slice::from_ref(&plain), row).expect("evaluates"),
        (false, false)
    );

    let mut in_column = Column::new(1, long());
    in_column.index = 0;
    in_column.in_operand = true;
    let eq = tidb_expr::new_function::new_function(
        &TestCtx,
        "eq",
        long(),
        vec![Expression::Column(in_column), always_true()],
    )
    .expect("eq is a ported builtin");
    assert!(is_eq_cond_from_in(&eq));
    assert_eq!(
        eval_bool(&TestCtx, std::slice::from_ref(&eq), row).expect("evaluates"),
        (false, true)
    );

    // A later FALSE beats an earlier UNKNOWN, which is the reason Go keeps
    // scanning instead of returning at the NULL.
    assert_eq!(
        eval_bool(&TestCtx, &[eq, always_false()], row).expect("evaluates"),
        (false, false)
    );
}

/// GO PORT of `TestJoinerOtherConditionChunkUsesInitChunkSize`
/// (`joiner_test.go:45`): the scratch chunk an inner/outer joiner builds its
/// candidate rows in is sized by `InitChunkSize`, and a clone keeps that
/// sizing -- while `TryToMatchInners` still emits more rows than that
/// capacity, because the chunk grows.
#[test]
fn other_condition_chunk_uses_init_chunk_size() {
    let init_chunk_size = 8;
    for join_type in [JoinType::Inner, JoinType::LeftOuter, JoinType::RightOuter] {
        let mut joiner = new_joiner(
            TestCtx,
            join_type,
            false,
            &[Datum::Int(0)],
            vec![always_true()],
            &[long()],
            &[long()],
            None,
            false,
            JoinerChunkSizes {
                init_chunk_size,
                max_chunk_size: MAX_CHUNK,
            },
        );
        assert_eq!(joiner.scratch_chunk_capacity(), Some(init_chunk_size));
        let cloned = joiner.clone_joiner();
        assert_eq!(cloned.scratch_chunk_capacity(), Some(init_chunk_size));

        let outer = chunk_of(&[Some(7)]);
        let inner_values: Vec<Option<i64>> = (0..=init_chunk_size as i64).map(Some).collect();
        let inners = chunk_of(&inner_values);
        let emitted = drive(&mut *joiner, &outer, 0, &inners, 2, NAAJType::Unknown);
        assert_eq!(emitted.len(), init_chunk_size + 1);
    }
}

#[test]
fn clone_preserves_the_join_type_and_the_projection() {
    let outer = chunk_of(&[Some(7)]);
    let inners = chunk_of(&[Some(4)]);
    let joiner = new_joiner(
        TestCtx,
        JoinType::LeftOuter,
        false,
        &[Datum::Null],
        Vec::new(),
        &[long()],
        &[long()],
        None,
        false,
        sizes(),
    );
    let mut cloned = joiner.clone_joiner();
    assert_eq!(cloned.join_type(), JoinType::LeftOuter);
    assert_eq!(
        drive(&mut *cloned, &outer, 0, &inners, 2, NAAJType::Unknown),
        vec![vec![Some(7), Some(4)]]
    );
    let empty = chunk_of(&[]);
    assert_eq!(
        drive(&mut *cloned, &outer, 0, &empty, 2, NAAJType::Unknown),
        vec![vec![Some(7), None]]
    );
}
