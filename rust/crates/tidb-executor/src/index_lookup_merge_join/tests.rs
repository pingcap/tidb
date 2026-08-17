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

//! Tests for [`crate::index_lookup_merge_join`].
//!
//! **WRITTEN, not transcreated.** Go's coverage for `IndexLookUpMergeJoin` is
//! `pkg/executor/test/jointest/*` (e.g. `TestIndexNestedLoopMergeJoin`) and
//! runs entirely through `testkit` -- a real session, a real store, real SQL --
//! so none of it is dependency-closed at this layer. What is pinned here is
//! the set of facts the module header claims, chiefly the **ordering
//! contract**: outer-scan order when `need_outer_sort` is false, join-key
//! order *within a batch* when it is true.

use super::*;
use crate::index_lookup_join::tests::{drain, long, schema_of, TestCtx};
use crate::joiner::{new_joiner, JoinType, JoinerChunkSizes};
use std::cell::RefCell;
use std::rc::Rc;

// ---------------------------------------------------------------------------
// scaffolding
// ---------------------------------------------------------------------------

/// An executor over a fixed row list, emitting one chunk of at most
/// `max_chunk_size` rows per `next`. `None` is a NULL cell.
struct RowsExec {
    meta: ExecutorMeta,
    types: Vec<FieldType>,
    rows: Vec<Vec<Option<i64>>>,
    at: usize,
}

impl RowsExec {
    fn new(types: Vec<FieldType>, rows: Vec<Vec<Option<i64>>>, max_chunk_size: usize) -> Self {
        let schema = schema_of(&types);
        let meta = ExecutorMeta::new(schema, 0, max_chunk_size, max_chunk_size);
        RowsExec {
            meta,
            types,
            rows,
            at: 0,
        }
    }
}

impl Executor for RowsExec {
    fn open(&mut self) -> Result<(), ExecError> {
        self.at = 0;
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        while self.at < self.rows.len() && !req.is_full() {
            for (col, value) in self.rows[self.at].iter().enumerate() {
                match value {
                    Some(value) => req.append_int64(col, *value),
                    None => req.append_null(col),
                }
            }
            self.at += 1;
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
        Ok(())
    }

    fn schema(&self) -> &Schema {
        self.meta.schema()
    }

    fn ret_field_types(&self) -> &[FieldType] {
        &self.types
    }

    fn init_cap(&self) -> usize {
        self.meta.init_cap()
    }

    fn max_chunk_size(&self) -> usize {
        self.meta.max_chunk_size()
    }

    fn new_chunk(&self) -> Chunk {
        self.meta.new_chunk()
    }
}

/// Stands in for Go's `buildExecutorForIndexJoinInternal`: answers from a fixed
/// inner table, **in index order**, which is the precondition the merge join
/// runs on.
struct SortedReaderBuilder {
    inner_types: Vec<FieldType>,
    table: Vec<(i64, i64)>,
    desc: bool,
    asked: Rc<RefCell<Vec<Vec<i64>>>>,
    /// Records Go's `canReorderHandles` argument (`handleTask` :490).
    reorder_seen: Rc<RefCell<Vec<bool>>>,
}

impl IndexJoinExecutorBuilder for SortedReaderBuilder {
    fn build_executor_for_index_join(
        &mut self,
        lookup_contents: &[IndexJoinLookUpContent],
        _index_ranges: &[IndexRange],
        _key_off_to_idx_off: &[usize],
        can_reorder_handles: bool,
    ) -> Result<Box<dyn Executor>, ExecError> {
        self.reorder_seen.borrow_mut().push(can_reorder_handles);
        let wanted: Vec<i64> = lookup_contents
            .iter()
            .filter_map(|content| match content.keys.first() {
                Some(Datum::Int(value)) => Some(*value),
                _ => None,
            })
            .collect();
        self.asked.borrow_mut().push(wanted.clone());
        let mut rows: Vec<(i64, i64)> = self
            .table
            .iter()
            .copied()
            .filter(|(key, _)| wanted.contains(key))
            .collect();
        if self.desc {
            rows.sort_by_key(|&(key, _)| std::cmp::Reverse(key));
        } else {
            rows.sort_by_key(|&(key, _)| key);
        }
        Ok(Box::new(RowsExec::new(
            self.inner_types.clone(),
            rows.into_iter()
                .map(|(key, value)| vec![Some(key), Some(value)])
                .collect(),
            32,
        )))
    }
}

struct Fixture {
    join: IndexLookUpMergeJoin<TestCtx>,
    asked: Rc<RefCell<Vec<Vec<i64>>>>,
    reorder_seen: Rc<RefCell<Vec<bool>>>,
}

#[derive(Clone, Copy)]
struct Shape {
    join_type: JoinType,
    need_outer_sort: bool,
    desc: bool,
    /// The outer child's chunk size, which is what decides a batch boundary --
    /// `buildTask` (`:362`) counts whole child chunks.
    outer_chunk_size: usize,
    /// Go `SessionVars.IndexJoinBatchSize`.
    max_batch_size: usize,
    /// The join's own output chunk size, i.e. how often `next` must suspend.
    out_chunk_size: usize,
}

impl Default for Shape {
    fn default() -> Self {
        Shape {
            join_type: JoinType::Inner,
            need_outer_sort: false,
            desc: false,
            outer_chunk_size: 32,
            max_batch_size: 32,
            out_chunk_size: 32,
        }
    }
}

/// Builds a merge join of `outer(k)` against `inner(k, v)` on `k`.
fn fixture(shape: Shape, outer: Vec<Option<i64>>, inner: Vec<(i64, i64)>) -> Fixture {
    let outer_types = vec![long()];
    let inner_types = vec![long(), long()];
    let out_types = vec![long(), long(), long()];

    let asked = Rc::new(RefCell::new(Vec::new()));
    let reorder_seen = Rc::new(RefCell::new(Vec::new()));
    let builder = SortedReaderBuilder {
        inner_types: inner_types.clone(),
        table: inner,
        desc: shape.desc,
        asked: Rc::clone(&asked),
        reorder_seen: Rc::clone(&reorder_seen),
    };

    let joiner = new_joiner(
        TestCtx,
        shape.join_type,
        false,
        &[Datum::Null, Datum::Null],
        Vec::new(),
        &outer_types,
        &inner_types,
        None,
        false,
        JoinerChunkSizes {
            init_chunk_size: 32,
            max_chunk_size: 32,
        },
    );

    let outer_exec = RowsExec::new(
        outer_types.clone(),
        outer.into_iter().map(|k| vec![k]).collect(),
        shape.outer_chunk_size,
    );
    let meta = ExecutorMeta::new(
        schema_of(&out_types),
        1,
        shape.out_chunk_size,
        shape.out_chunk_size,
    );

    let join = IndexLookUpMergeJoin::new(
        meta,
        Box::new(outer_exec),
        OuterMergeCtx {
            row_types: outer_types,
            key_cols: vec![0],
            filter: Vec::new(),
            need_outer_sort: shape.need_outer_sort,
        },
        InnerMergeCtx {
            row_types: inner_types,
            key_cols: vec![0],
            key_col_ids: vec![1],
            key_collators: vec![Collation::Binary],
            col_lens: vec![-1],
            desc: shape.desc,
            key_off_to_key_off_order_by_idx: vec![0],
        },
        Box::new(builder),
        joiner,
        matches!(shape.join_type, JoinType::LeftOuter),
        vec![IndexRange::full()],
        vec![0],
        TestCtx,
    )
    .with_max_batch_size(shape.max_batch_size);

    Fixture {
        join,
        asked,
        reorder_seen,
    }
}

fn keys(rows: &[Vec<Option<i64>>]) -> Vec<Option<i64>> {
    rows.iter().map(|row| row[0]).collect()
}

// ---------------------------------------------------------------------------
// the ordering contract
// ---------------------------------------------------------------------------

#[test]
fn preserves_outer_scan_order_when_no_outer_sort() {
    // `need_outer_sort == false` is the documented contract at
    // `index_lookup_merge_join.go:43`: the outer side is already ordered on
    // the join keys, and `outerOrderIdx` (`:440`) is left in outer-scan order,
    // so `doMergeJoin` (`:555`) emits in exactly that order.
    let mut fixture = fixture(
        Shape::default(),
        vec![Some(1), Some(1), Some(2), Some(4)],
        vec![(1, 10), (1, 11), (2, 20), (3, 30)],
    );
    let rows = drain(&mut fixture.join, 3);
    assert_eq!(
        rows,
        vec![
            // first outer 1: the whole same-key run
            vec![Some(1), Some(1), Some(10)],
            vec![Some(1), Some(1), Some(11)],
            // second outer 1: the run is REWOUND (`sameKeyIter.Begin()` :567),
            // not consumed by the first row
            vec![Some(1), Some(1), Some(10)],
            vec![Some(1), Some(1), Some(11)],
            vec![Some(2), Some(2), Some(20)],
            // outer 4 outruns the inner side entirely
        ]
    );
}

#[test]
fn need_outer_sort_emits_in_join_key_order() {
    // `NeedOuterSort` (`:450`) re-sorts `outerOrderIdx` by join key, so the
    // emission order stops being outer-scan order. This is the OTHER half of
    // the contract and the reason the header states it as conditional.
    let mut fixture = fixture(
        Shape {
            need_outer_sort: true,
            ..Shape::default()
        },
        vec![Some(3), Some(1), Some(2)],
        vec![(1, 10), (2, 20), (3, 30)],
    );
    let rows = drain(&mut fixture.join, 3);
    assert_eq!(keys(&rows), vec![Some(1), Some(2), Some(3)]);
}

#[test]
fn merge_join_sorts_only_within_a_batch() {
    // The sort in `handleTask` is per-task, and a task is one `buildTask`
    // batch (`:362`). With a two-row outer chunk and a one-row batch budget,
    // `[3,1,2,4]` becomes the batches `[3,1]` and `[2,4]`, each sorted on its
    // own -- so the global output is NOT sorted. Anyone reading
    // `NeedOuterSort` as "globally ordered by key" is wrong, and this pins it.
    let mut fixture = fixture(
        Shape {
            need_outer_sort: true,
            outer_chunk_size: 2,
            max_batch_size: 1,
            ..Shape::default()
        },
        vec![Some(3), Some(1), Some(2), Some(4)],
        vec![(1, 10), (2, 20), (3, 30), (4, 40)],
    );
    let rows = drain(&mut fixture.join, 3);
    assert_eq!(
        keys(&rows),
        vec![Some(1), Some(3), Some(2), Some(4)],
        "each batch is sorted, the batches themselves stay in outer-scan order"
    );
}

#[test]
fn order_is_unchanged_by_a_one_row_output_chunk() {
    // Go suspends by pushing a full chunk to `task.results`
    // (`fetchNewChunkWhenFull` :505) and resuming the same loop; here `next`
    // returns and resumes from `RowPhase`. Same rows, same order.
    let batched = {
        let mut fixture = fixture(
            Shape::default(),
            vec![Some(1), Some(1), Some(2)],
            vec![(1, 10), (1, 11), (2, 20), (2, 21)],
        );
        drain(&mut fixture.join, 3)
    };
    let one_at_a_time = {
        let mut fixture = fixture(
            Shape {
                out_chunk_size: 1,
                ..Shape::default()
            },
            vec![Some(1), Some(1), Some(2)],
            vec![(1, 10), (1, 11), (2, 20), (2, 21)],
        );
        drain(&mut fixture.join, 3)
    };
    assert_eq!(batched.len(), 6);
    assert_eq!(batched, one_at_a_time);
}

// ---------------------------------------------------------------------------
// merge semantics
// ---------------------------------------------------------------------------

#[test]
fn left_outer_join_emits_a_miss_in_its_outer_position() {
    // `doMergeJoin`'s `missMatch:` label (`:595`) runs for the outer row
    // itself, so an unmatched outer row keeps its place in the stream rather
    // than being flushed at the end (which is what the *hash* variant's
    // unordered mode does).
    let mut fixture = fixture(
        Shape {
            join_type: JoinType::LeftOuter,
            ..Shape::default()
        },
        vec![Some(1), Some(2), Some(3), Some(5)],
        vec![(1, 10), (3, 30)],
    );
    let rows = drain(&mut fixture.join, 3);
    assert_eq!(
        rows,
        vec![
            vec![Some(1), Some(1), Some(10)],
            vec![Some(2), None, None],
            vec![Some(3), Some(3), Some(30)],
            vec![Some(5), None, None],
        ]
    );
}

#[test]
fn descending_merge_walks_both_sides_backwards() {
    // `InnerMergeCtx.Desc` flips every comparison in `doMergeJoin` (`:550`,
    // `:572`) and `fetchInnerRowsWithSameKey` (`:612`).
    let mut fixture = fixture(
        Shape {
            desc: true,
            ..Shape::default()
        },
        vec![Some(4), Some(3), Some(1)],
        vec![(1, 10), (3, 30), (3, 31), (4, 40)],
    );
    let rows = drain(&mut fixture.join, 3);
    assert_eq!(
        rows,
        vec![
            vec![Some(4), Some(4), Some(40)],
            vec![Some(3), Some(3), Some(30)],
            vec![Some(3), Some(3), Some(31)],
            vec![Some(1), Some(1), Some(10)],
        ]
    );
}

#[test]
fn an_outer_row_before_every_inner_row_matches_nothing() {
    // The inner cursor must not be advanced past a run the outer row has not
    // reached: `fetchInnerRowsWithSameKey` only consumes while
    // `cmp >= 0` (`:612`).
    let mut fixture = fixture(
        Shape {
            join_type: JoinType::LeftOuter,
            ..Shape::default()
        },
        vec![Some(1), Some(7)],
        vec![(7, 70)],
    );
    let rows = drain(&mut fixture.join, 3);
    assert_eq!(
        rows,
        vec![vec![Some(1), None, None], vec![Some(7), Some(7), Some(70)],]
    );
}

#[test]
fn a_null_outer_key_is_never_looked_up_but_still_misses() {
    // `constructDatumLookupKey` (`:671`) returns a nil key for a NULL outer
    // value, so it never reaches the reader; `doMergeJoin` still walks the row
    // and `OnMissMatch` still fires for it.
    let mut fixture = fixture(
        Shape {
            join_type: JoinType::LeftOuter,
            ..Shape::default()
        },
        vec![None, Some(2)],
        vec![(2, 20)],
    );
    let rows = drain(&mut fixture.join, 3);
    assert_eq!(
        rows,
        vec![vec![None, None, None], vec![Some(2), Some(2), Some(20)]]
    );
    assert_eq!(
        *fixture.asked.borrow(),
        vec![vec![2]],
        "the NULL key was not sent to the reader"
    );
}

// ---------------------------------------------------------------------------
// lookup-key construction
// ---------------------------------------------------------------------------

#[test]
fn adjacent_duplicate_keys_are_deduplicated_for_the_reader() {
    // `dedupDatumLookUpKeys` (`:703`) compares ADJACENT entries only, which is
    // sound precisely because the contents are already in key order.
    let mut fixture = fixture(
        Shape::default(),
        vec![Some(1), Some(1), Some(2), Some(2), Some(2)],
        vec![(1, 10), (2, 20)],
    );
    let rows = drain(&mut fixture.join, 3);
    assert_eq!(rows.len(), 5);
    assert_eq!(*fixture.asked.borrow(), vec![vec![1, 2]]);
}

#[test]
fn the_reader_is_built_without_handle_reordering() {
    // `handleTask` (`:490`) passes `canReorderHandles == false`, unlike
    // `index_lookup_join.go`. The whole merge depends on it.
    let mut fixture = fixture(
        Shape::default(),
        vec![Some(1), Some(2)],
        vec![(1, 10), (2, 20)],
    );
    drain(&mut fixture.join, 3);
    assert_eq!(*fixture.reorder_seen.borrow(), vec![false]);
}

#[test]
fn descending_lookup_contents_reach_the_reader_ascending() {
    // `handleTask` (`:484`) reverses the deduped contents when `Desc`, because
    // a range must be built ascending -- while leaving `outerOrderIdx` alone,
    // which `descending_merge_walks_both_sides_backwards` covers.
    let mut fixture = fixture(
        Shape {
            desc: true,
            ..Shape::default()
        },
        vec![Some(3), Some(2), Some(1)],
        vec![(1, 10), (2, 20), (3, 30)],
    );
    drain(&mut fixture.join, 3);
    assert_eq!(*fixture.asked.borrow(), vec![vec![1, 2, 3]]);
}

#[test]
fn each_batch_gets_its_own_reader() {
    // One task, one `BuildExecutorForIndexJoin` (`:490`), one inner scan --
    // that is what "batch lookup" in the type's doc comment (`:44`) means.
    let mut fixture = fixture(
        Shape {
            outer_chunk_size: 2,
            max_batch_size: 1,
            ..Shape::default()
        },
        vec![Some(1), Some(2), Some(3), Some(4)],
        vec![(1, 10), (2, 20), (3, 30), (4, 40)],
    );
    let rows = drain(&mut fixture.join, 3);
    assert_eq!(keys(&rows), vec![Some(1), Some(2), Some(3), Some(4)]);
    assert_eq!(*fixture.asked.borrow(), vec![vec![1, 2], vec![3, 4]]);
}

#[test]
fn an_empty_outer_side_produces_no_task_and_no_lookup() {
    // `buildTask` returns `(nil, nil)` (`:377`) when the outer side is empty,
    // which is the join's only termination condition.
    let mut fixture = fixture(Shape::default(), Vec::new(), vec![(1, 10)]);
    let rows = drain(&mut fixture.join, 3);
    assert!(rows.is_empty());
    assert!(fixture.asked.borrow().is_empty());
}

#[test]
fn a_mismatched_key_column_count_is_refused_at_open() {
    let mut fixture = fixture(Shape::default(), vec![Some(1)], vec![(1, 10)]);
    fixture.join.inner_ctx.key_cols = vec![0, 1];
    let err = fixture.join.open().expect_err("must refuse");
    assert!(format!("{err:?}").contains("key column counts"));
}
