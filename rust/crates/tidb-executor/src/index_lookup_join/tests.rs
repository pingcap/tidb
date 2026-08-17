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

//! Tests for [`crate::index_lookup_join`].
//!
//! WRITTEN, not ported. Go's coverage for `IndexLookUpJoin` lives in
//! `pkg/executor/test/jointest/*` and runs entirely through `testkit` (a real
//! session, a real store, real SQL), so none of it is dependency-closed here.
//! The two facts worth pinning are the ones the module header claims:
//! the per-join-type row emission (delegated to `crate::joiner`) and the
//! outer-order guarantee.

use super::*;
use crate::joiner::{new_joiner, JoinType, JoinerChunkSizes};
use std::cell::RefCell;
use std::rc::Rc;
use tidb_datatype::FieldTypeCode;
use tidb_expr::expression::Column;
use tidb_expr::schema::Schema;

// ---------------------------------------------------------------------------
// scaffolding
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
pub(crate) struct TestCtx;

impl Columns for TestCtx {
    fn get(&self, _path: &[String]) -> Option<Datum> {
        None
    }
}

pub(crate) fn long() -> FieldType {
    FieldType::new(FieldTypeCode::Long)
}

/// A schema of `types.len()` distinct long columns.
pub(crate) fn schema_of(types: &[FieldType]) -> Schema {
    Schema::new(
        types
            .iter()
            .enumerate()
            .map(|(i, ft)| {
                let mut column = Column::new(i64::try_from(i).unwrap_or(0) + 1, ft.clone());
                column.index = i64::try_from(i).unwrap_or(0);
                column
            })
            .collect(),
    )
}

/// An executor over a fixed row list, one chunk per `next`.
pub(crate) struct RowsExec {
    meta: ExecutorMeta,
    types: Vec<FieldType>,
    rows: Vec<Vec<i64>>,
    at: usize,
}

impl RowsExec {
    fn new(types: Vec<FieldType>, rows: Vec<Vec<i64>>) -> Self {
        let schema = schema_of(&types);
        let meta = ExecutorMeta::new(schema, 0, 32, 32);
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
                req.append_int64(col, *value);
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

/// A reader builder that answers from a fixed inner table by key, standing in
/// for Go's `buildExecutorForIndexJoinInternal`.
///
/// It records the lookup keys it was asked for, which is how the tests check
/// batching and dedup.
pub(crate) struct FakeReaderBuilder {
    inner_types: Vec<FieldType>,
    /// key -> the inner rows carrying it.
    table: Vec<Vec<i64>>,
    asked: Rc<RefCell<Vec<Vec<i64>>>>,
}

impl IndexJoinExecutorBuilder for FakeReaderBuilder {
    fn build_executor_for_index_join(
        &mut self,
        lookup_contents: &[IndexJoinLookUpContent],
        _index_ranges: &[IndexRange],
        _key_off_to_idx_off: &[usize],
        _can_reorder_handles: bool,
    ) -> Result<Box<dyn Executor>, ExecError> {
        let mut wanted: Vec<i64> = Vec::new();
        let mut asked_batch: Vec<i64> = Vec::new();
        for content in lookup_contents {
            let key = match content.keys.first() {
                Some(Datum::Int(value)) => *value,
                Some(Datum::UInt(value)) => i64::try_from(*value).unwrap_or(i64::MAX),
                _ => continue,
            };
            wanted.push(key);
            asked_batch.push(key);
        }
        self.asked.borrow_mut().push(asked_batch);
        let rows: Vec<Vec<i64>> = self
            .table
            .iter()
            .filter(|row| wanted.contains(&row[0]))
            .cloned()
            .collect();
        Ok(Box::new(RowsExec::new(self.inner_types.clone(), rows)))
    }
}

pub(crate) struct Fixture {
    pub(crate) join: IndexLookUpJoin<TestCtx>,
    pub(crate) asked: Rc<RefCell<Vec<Vec<i64>>>>,
}

/// Builds a join of `outer(k)` against `inner(k, v)` on `k`.
pub(crate) fn fixture(join_type: JoinType, outer: Vec<i64>, inner: Vec<Vec<i64>>) -> Fixture {
    let outer_types = vec![long()];
    let inner_types = vec![long(), long()];
    let out_types = match join_type {
        JoinType::Inner | JoinType::LeftOuter => vec![long(), long(), long()],
        _ => vec![long()],
    };

    let asked = Rc::new(RefCell::new(Vec::new()));
    let builder = FakeReaderBuilder {
        inner_types: inner_types.clone(),
        table: inner,
        asked: Rc::clone(&asked),
    };

    let joiner = new_joiner(
        TestCtx,
        join_type,
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
    );
    let meta = ExecutorMeta::new(schema_of(&out_types), 1, 32, 32);

    let join = IndexLookUpJoin::new(
        meta,
        Box::new(outer_exec),
        OuterCtx {
            row_types: outer_types.clone(),
            key_cols: vec![0],
            hash_types: outer_types,
            hash_cols: vec![0],
            filter: Vec::new(),
        },
        InnerCtx {
            row_types: inner_types.clone(),
            key_cols: vec![0],
            key_col_ids: vec![1],
            key_collators: vec![Collation::Binary],
            hash_types: inner_types,
            hash_cols: vec![0],
            hash_collators: vec![Collation::Binary],
            hash_is_null_eq: vec![false],
            col_lens: vec![UNSPECIFIED_LENGTH],
            has_prefix_col: false,
        },
        Box::new(builder),
        joiner,
        matches!(join_type, JoinType::LeftOuter),
        vec![IndexRange::full()],
        vec![0],
        TestCtx,
    );
    Fixture { join, asked }
}

/// Drains the join into `(col0, col1, col2)` tuples, NULL as `None`.
pub(crate) fn drain(join: &mut dyn Executor, num_cols: usize) -> Vec<Vec<Option<i64>>> {
    join.open().expect("open");
    let mut out = Vec::new();
    loop {
        let mut chk = join.new_chunk();
        join.next(&mut chk).expect("next");
        if chk.num_rows() == 0 {
            break;
        }
        for i in 0..chk.num_rows() {
            let row = chk.get_row(i);
            out.push(
                (0..num_cols)
                    .map(|c| {
                        if row.is_null(c) {
                            None
                        } else {
                            Some(row.get_int64(c))
                        }
                    })
                    .collect(),
            );
        }
    }
    join.close().expect("close");
    out
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[test]
fn inner_join_emits_every_match_in_outer_order() {
    // The order guarantee from the module header: outer rows come out in
    // outer-scan order even though the keys are looked up sorted+deduped.
    let mut fixture = fixture(
        JoinType::Inner,
        vec![3, 1, 2, 1],
        vec![vec![1, 10], vec![1, 11], vec![2, 20]],
    );
    let rows = drain(&mut fixture.join, 3);
    assert_eq!(
        rows,
        vec![
            vec![Some(1), Some(1), Some(10)],
            vec![Some(1), Some(1), Some(11)],
            vec![Some(2), Some(2), Some(20)],
            vec![Some(1), Some(1), Some(10)],
            vec![Some(1), Some(1), Some(11)],
        ],
        "outer row 3 matches nothing and is dropped; 1, 2, 1 keep their order"
    );
}

#[test]
fn inner_rows_of_one_key_keep_inner_scan_order() {
    // The mvmap's insertion-ordered `get` is what makes this true.
    let mut fixture = fixture(
        JoinType::Inner,
        vec![7],
        vec![vec![7, 1], vec![7, 2], vec![7, 3]],
    );
    let rows = drain(&mut fixture.join, 3);
    let values: Vec<i64> = rows.iter().map(|row| row[2].unwrap()).collect();
    assert_eq!(values, vec![1, 2, 3]);
}

#[test]
fn left_outer_join_keeps_the_unmatched_outer_row() {
    let mut fixture = fixture(
        JoinType::LeftOuter,
        vec![1, 5, 2],
        vec![vec![1, 10], vec![2, 20]],
    );
    let rows = drain(&mut fixture.join, 3);
    assert_eq!(
        rows,
        vec![
            vec![Some(1), Some(1), Some(10)],
            vec![Some(5), None, None],
            vec![Some(2), Some(2), Some(20)],
        ],
        "the miss is padded with the default inner row, in place"
    );
}

#[test]
fn semi_join_emits_the_outer_row_once_per_match() {
    let mut fixture = fixture(
        JoinType::SemiJoin,
        vec![1, 3, 1],
        vec![vec![1, 10], vec![1, 11]],
    );
    let rows = drain(&mut fixture.join, 1);
    assert_eq!(
        rows,
        vec![vec![Some(1)], vec![Some(1)]],
        "two matching inners must still yield one row per outer row"
    );
}

#[test]
fn anti_semi_join_emits_only_the_misses() {
    let mut fixture = fixture(JoinType::AntiSemiJoin, vec![1, 3, 4], vec![vec![1, 10]]);
    let rows = drain(&mut fixture.join, 1);
    assert_eq!(rows, vec![vec![Some(3)], vec![Some(4)]]);
}

#[test]
fn lookup_keys_are_sorted_and_deduplicated_per_batch() {
    // Go `sortAndDedupLookUpContents` (:703): the reader is asked for each
    // distinct key once, in key order, however the outer rows arrived.
    let mut fixture = fixture(
        JoinType::Inner,
        vec![3, 1, 2, 1, 3],
        vec![vec![1, 10], vec![2, 20], vec![3, 30]],
    );
    drain(&mut fixture.join, 3);
    let asked = fixture.asked.borrow();
    assert_eq!(asked.len(), 1, "one batch covers all five outer rows");
    assert_eq!(asked[0], vec![1, 2, 3]);
}

#[test]
fn null_outer_key_is_not_looked_up_but_still_reaches_the_joiner() {
    // Go `constructDatumLookupKey` (:665) returns a nil key for a NULL under a
    // non-null-safe equality; the outer row is a miss, not a dropped row.
    let outer_types = vec![long()];
    let inner_types = vec![long(), long()];
    let asked = Rc::new(RefCell::new(Vec::new()));
    let builder = FakeReaderBuilder {
        inner_types: inner_types.clone(),
        table: vec![vec![1, 10]],
        asked: Rc::clone(&asked),
    };
    let joiner = new_joiner(
        TestCtx,
        JoinType::LeftOuter,
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
    // One NULL outer row, produced by a chunk with a null cell.
    struct NullOuter {
        meta: ExecutorMeta,
        types: Vec<FieldType>,
        done: bool,
    }
    impl Executor for NullOuter {
        fn open(&mut self) -> Result<(), ExecError> {
            self.done = false;
            Ok(())
        }
        fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
            req.reset();
            if !self.done {
                req.append_null(0);
                self.done = true;
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
    let outer_exec = NullOuter {
        meta: ExecutorMeta::new(schema_of(&outer_types), 0, 32, 32),
        types: outer_types.clone(),
        done: false,
    };
    let meta = ExecutorMeta::new(schema_of(&[long(), long(), long()]), 1, 32, 32);
    let mut join = IndexLookUpJoin::new(
        meta,
        Box::new(outer_exec),
        OuterCtx {
            row_types: outer_types.clone(),
            key_cols: vec![0],
            hash_types: outer_types,
            hash_cols: vec![0],
            filter: Vec::new(),
        },
        InnerCtx {
            row_types: inner_types.clone(),
            key_cols: vec![0],
            key_col_ids: vec![1],
            key_collators: vec![Collation::Binary],
            hash_types: inner_types,
            hash_cols: vec![0],
            hash_collators: vec![Collation::Binary],
            hash_is_null_eq: vec![false],
            col_lens: vec![UNSPECIFIED_LENGTH],
            has_prefix_col: false,
        },
        Box::new(builder),
        joiner,
        true,
        vec![IndexRange::full()],
        vec![0],
        TestCtx,
    );
    let rows = drain(&mut join, 3);
    assert_eq!(rows, vec![vec![None, None, None]]);
    assert_eq!(
        asked.borrow()[0],
        Vec::<i64>::new(),
        "the NULL key is never sent to the reader"
    );
}

#[test]
fn open_rejects_a_mismatched_null_eq_flag_list() {
    // Go `Open` (:181).
    let mut fixture = fixture(JoinType::Inner, vec![1], vec![vec![1, 10]]);
    fixture.join.inner_ctx.hash_is_null_eq = Vec::new();
    let err = fixture.join.open().expect_err("length mismatch must fail");
    assert!(matches!(err, ExecError::Internal(_)));
}

#[test]
fn empty_outer_side_produces_no_rows() {
    let mut fixture = fixture(JoinType::LeftOuter, Vec::new(), vec![vec![1, 10]]);
    assert!(drain(&mut fixture.join, 3).is_empty());
}

#[test]
fn batch_size_doubles_up_to_the_cap() {
    // Go `increaseBatchSize` (:512).
    let mut fixture = fixture(JoinType::Inner, vec![1], vec![vec![1, 10]]);
    fixture.join.batch_size = 2;
    fixture.join.max_batch_size = 6;
    fixture.join.increase_batch_size();
    assert_eq!(fixture.join.batch_size, 4);
    fixture.join.increase_batch_size();
    assert_eq!(fixture.join.batch_size, 6, "capped, not 8");
    fixture.join.increase_batch_size();
    assert_eq!(fixture.join.batch_size, 6);
}

#[test]
fn row_ptr_round_trips_through_its_eight_bytes() {
    // Go writes a `chunk.RowPtr` into `valBuf` via `unsafe.Pointer` (:828).
    let ptr = RowPtr {
        chk_idx: 7,
        row_idx: 1234,
    };
    assert_eq!(decode_row_ptr(&encode_row_ptr(ptr)), Some(ptr));
    assert_eq!(decode_row_ptr(&[0u8; 3]), None);
}

#[test]
fn compare_row_is_lexicographic() {
    let collators = vec![Collation::Binary, Collation::Binary];
    let left = vec![Datum::Int(1), Datum::Int(2)];
    let right = vec![Datum::Int(1), Datum::Int(3)];
    assert_eq!(
        compare_row(&left, &right, &collators),
        std::cmp::Ordering::Less
    );
    assert_eq!(
        compare_row(&left, &left, &collators),
        std::cmp::Ordering::Equal
    );
}

#[test]
fn prefix_cut_truncates_only_bytes() {
    // Go `ranger.CutDatumByPrefixLen`, byte half.
    let mut datum = Datum::Bytes(b"alphabet".to_vec());
    cut_datum_by_prefix_len(&mut datum, 4);
    assert_eq!(datum, Datum::Bytes(b"alph".to_vec()));
    let mut short = Datum::Bytes(b"ab".to_vec());
    cut_datum_by_prefix_len(&mut short, 4);
    assert_eq!(short, Datum::Bytes(b"ab".to_vec()));
    let mut int = Datum::Int(9);
    cut_datum_by_prefix_len(&mut int, 4);
    assert_eq!(int, Datum::Int(9));
}
