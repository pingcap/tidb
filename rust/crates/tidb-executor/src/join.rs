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

//! Two-way joins: inner, left outer, and right outer.
//!
//! The output row is the left row's cells followed by the right row's, which
//! is the schema Go's `LogicalJoin` builds and what the column resolver
//! addresses. The `ON` condition filters MATCHES, not rows: an outer row that
//! matches nothing still emits once, padded with NULLs on the other side
//! (Go `HashJoinExec`'s `onMissMatch` path), while a `WHERE` predicate applies
//! above the join and can remove those padded rows -- the distinction that
//! makes `LEFT JOIN ... WHERE right.c IS NULL` an anti-join.
//!
//! A `RIGHT JOIN` is the same algorithm with the sides exchanged: the right
//! table is the outer one, and its unmatched rows are padded on the left. Go's
//! planner rewrites right joins into left joins for the same reason.
//!
//! # Algorithm: hash, with the nested loop as the stated fallback
//!
//! When the `ON` clause carries at least one `col = col` conjunct this module
//! can index (see [`crate::hash_join`] for the encoding and the classes it
//! refuses), the join builds a hash table on the INNER side and streams the
//! OUTER side through it: O(build + probe x fanout) instead of
//! O(build x probe). Everything else -- a cross join, a join whose only
//! conditions are inequalities, a key whose two sides compare in different
//! domains -- keeps the nested loop, which materializes both inputs and
//! compares every pair.
//!
//! The two paths are not two implementations of `ON`. The hash table only
//! CHOOSES the candidate pairs; each candidate is then handed to the same
//! [`JoinExec::matches`] the nested loop calls on every pair. So the hash
//! path can only ever emit a subset of the loop's output, and the key
//! encoding's one obligation -- never separate two rows that satisfy `eq` --
//! is what makes that subset the whole of it.
//!
//! # Build side
//!
//! The build side is the INNER side: the right child for `INNER`/`LEFT`, the
//! left child for `RIGHT`. That is what Go's stats-less enumeration reaches
//! first (`getHashJoins`, `pkg/planner/core/exhaust_physical_plans.go`, which
//! calls `getHashJoin(ge, p, prop, 1, false)` first for `InnerJoin` and
//! `LeftOuterJoin` and uses `innerIdx = 0` for `RightOuterJoin`), and it is
//! what a captured `EXPLAIN` over statistics-free tables actually prints:
//! `hj2` is `(Build)` for `hj1 JOIN hj2` and for `hj1 LEFT JOIN hj2`, `hj1`
//! is `(Build)` for `hj1 RIGHT JOIN hj2`. Go re-picks by row count once
//! statistics exist; this tier has none, so it takes the stats-less choice
//! rather than inventing an estimate.
//!
//! Choosing the inner side is also what keeps the outer semantics
//! single-pass: the side whose unmatched rows must still be emitted is the
//! side being streamed, so a probe row that finds no bucket emits its
//! NULL-padded row immediately. No second sweep over a matched-flag array,
//! and no need to hold the preserved side in memory.
//!
//! # Memory
//!
//! The build side is materialized -- that is inherent to hashing -- as
//! `build rows x row width` datums plus one `u32` per row in its bucket. The
//! probe side is NOT: it is pulled one chunk at a time and each row is
//! dropped as soon as its output is emitted. The nested-loop fallback still
//! materializes both sides.
//!
//! Still deferred relative to Go's `HashJoinExec`: the parallel build/probe
//! worker pipeline, spill-to-disk, and the semi/anti/outer-apply variants.

use crate::executor::{ExecError, Executor, ExecutorMeta};
use crate::hash_join::{row_key, BuildTable, EquiKey, KeyError};
use std::cell::Cell;
use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType};
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_expr::Columns;

/// Which side, if any, keeps rows that match nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinKind {
    /// `JOIN` / `INNER JOIN` / `CROSS JOIN` / comma join: only matches.
    Inner,
    /// `LEFT [OUTER] JOIN`: every left row survives, padded with NULLs.
    Left,
    /// `RIGHT [OUTER] JOIN`: every right row survives, padded with NULLs.
    Right,
}

/// The hash path's live state; absent until the first `next()`, and never
/// created on the nested-loop fallback.
struct HashState {
    /// The materialized, indexed inner side.
    table: BuildTable,
    /// The chunk the outer child streams into, and how far it is consumed.
    probe_chunk: Chunk,
    probe_row: usize,
    probe_done: bool,
}

/// A join of two children, hashing its equal conditions when it can and
/// falling back to a nested loop when it cannot (see the module doc).
pub struct JoinExec<C: Columns> {
    meta: ExecutorMeta,
    kind: JoinKind,
    conditions: Vec<Expression>,
    left: Box<dyn Executor>,
    right: Box<dyn Executor>,
    ctx: C,
    /// The indexable `col = col` conjuncts; empty means the nested loop.
    keys: Vec<EquiKey>,
    /// Nested loop only: whether its single all-at-once batch was emitted.
    emitted: bool,
    hash: Option<HashState>,
    /// How many times the `ON` clause has been evaluated. This is the cost
    /// the hash table exists to remove, so it is the number a scaling test
    /// asserts on directly instead of timing the machine.
    condition_evals: Cell<u64>,
}

impl<C: Columns> JoinExec<C> {
    /// Builds a join of `left` and `right` filtered by `conditions` (the `ON`
    /// clause, empty for a Cartesian product).
    #[must_use]
    pub fn new(
        meta: ExecutorMeta,
        kind: JoinKind,
        conditions: Vec<Expression>,
        left: Box<dyn Executor>,
        right: Box<dyn Executor>,
        ctx: C,
    ) -> Self {
        let keys = crate::hash_join::split_equi(&conditions, left.ret_field_types().len()).keys;
        JoinExec {
            meta,
            kind,
            conditions,
            left,
            right,
            ctx,
            keys,
            emitted: false,
            hash: None,
            condition_evals: Cell::new(0),
        }
    }

    /// Whether this join hashes its equal conditions rather than looping.
    #[must_use]
    pub fn is_hash_join(&self) -> bool {
        !self.keys.is_empty()
    }

    /// How many times the `ON` clause has been evaluated so far.
    #[must_use]
    pub fn condition_evals(&self) -> u64 {
        self.condition_evals.get()
    }

    /// Forces the nested-loop path on a join that would otherwise hash.
    ///
    /// The nested loop is this unit's stated reference: the hash path is
    /// correct exactly insofar as it reproduces the loop's output row for
    /// row. Proving that needs BOTH paths over the SAME data, which is what
    /// this exists for -- nothing outside the differential test uses it.
    #[cfg(test)]
    fn force_nested_loop(&mut self) {
        self.keys.clear();
    }

    /// The outer side is the one whose unmatched rows survive; the inner
    /// side is the other, and is the one the hash path builds its table on.
    /// `true` means the LEFT child is the outer one.
    fn outer_is_left(&self) -> bool {
        self.kind != JoinKind::Right
    }

    /// Drains a child into rows of `Datum`s.
    fn drain(child: &mut dyn Executor) -> Result<Vec<Vec<Datum>>, ExecError> {
        let types: Vec<FieldType> = child.ret_field_types().to_vec();
        let mut chunk = child.new_chunk();
        let mut rows = Vec::new();
        loop {
            child.next(&mut chunk)?;
            let n = chunk.num_rows();
            if n == 0 {
                break;
            }
            for r in 0..n {
                rows.push(datum_row(&chunk, r, &types));
            }
        }
        Ok(rows)
    }

    /// Whether the `ON` conditions all hold for one joined row.
    fn matches(&self, joined: &[Datum]) -> Result<bool, ExecError> {
        if self.conditions.is_empty() {
            return Ok(true);
        }
        self.condition_evals.set(self.condition_evals.get() + 1);
        let types = self.meta.ret_field_types().to_vec();
        let mut chunk = Chunk::new_with_capacity(&types, 1);
        for (i, value) in joined.iter().enumerate() {
            chunk.append_datum(i, value);
        }
        let row = chunk.get_row(0);
        for condition in &self.conditions {
            let value = condition.eval(&self.ctx, row)?;
            if !truthy(&value) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Concatenates an outer and an inner row back into left-then-right
    /// order, which is the join's own schema.
    fn join_rows(&self, outer_row: &[Datum], inner_row: &[Datum]) -> Vec<Datum> {
        if self.outer_is_left() {
            outer_row.iter().chain(inner_row).cloned().collect()
        } else {
            inner_row.iter().chain(outer_row).cloned().collect()
        }
    }

    /// The row an outer row that matched nothing emits: itself, padded with
    /// NULLs on the inner side.
    fn padded_row(&self, outer_row: &[Datum]) -> Vec<Datum> {
        let padding = if self.outer_is_left() {
            self.right.ret_field_types().len()
        } else {
            self.left.ret_field_types().len()
        };
        let nulls = std::iter::repeat_n(Datum::Null, padding);
        if self.outer_is_left() {
            outer_row.iter().cloned().chain(nulls).collect()
        } else {
            nulls.chain(outer_row.iter().cloned()).collect()
        }
    }

    fn append(req: &mut Chunk, joined: &[Datum]) {
        for (c, value) in joined.iter().enumerate() {
            req.append_datum(c, value);
        }
    }

    /// Emits every output row one outer row produces, given the inner rows
    /// it may match. Shared by both paths so the outer-join padding rule and
    /// the output column order have exactly one implementation.
    fn emit_outer_row(
        &self,
        req: &mut Chunk,
        outer_row: &[Datum],
        candidates: impl Iterator<Item = Vec<Datum>>,
    ) -> Result<(), ExecError> {
        let mut matched = false;
        for inner_row in candidates {
            let joined = self.join_rows(outer_row, &inner_row);
            if !self.matches(&joined)? {
                continue;
            }
            matched = true;
            Self::append(req, &joined);
        }
        if !matched && self.kind != JoinKind::Inner {
            Self::append(req, &self.padded_row(outer_row));
        }
        Ok(())
    }

    /// The fallback: materialize both sides and compare every pair.
    fn next_nested(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        if self.emitted {
            return Ok(());
        }
        let left_rows = Self::drain(self.left.as_mut())?;
        let right_rows = Self::drain(self.right.as_mut())?;
        let (outer, inner) = if self.outer_is_left() {
            (&left_rows, &right_rows)
        } else {
            (&right_rows, &left_rows)
        };
        for outer_row in outer {
            self.emit_outer_row(req, outer_row, inner.iter().cloned())?;
        }
        self.emitted = true;
        Ok(())
    }

    /// The hash path: build once on the inner side, then stream the outer.
    ///
    /// One call consumes probe chunks until it has produced at least one
    /// output row, because an empty `req` is the caller's EOF signal and a
    /// probe chunk whose rows all miss (an inner join) produces none.
    fn next_hashed(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        self.build_table()?;
        loop {
            if self.hash.as_ref().is_some_and(|hash| hash.probe_done) {
                return Ok(());
            }
            self.fill_probe_chunk()?;
            self.drain_probe_chunk(req)?;
            if req.num_rows() > 0 {
                return Ok(());
            }
        }
    }

    /// Materializes and indexes the inner side, once per `open()`.
    fn build_table(&mut self) -> Result<(), ExecError> {
        if self.hash.is_some() {
            return Ok(());
        }
        let build_is_left = !self.outer_is_left();
        let build_rows = if build_is_left {
            Self::drain(self.left.as_mut())?
        } else {
            Self::drain(self.right.as_mut())?
        };
        let probe_chunk = if build_is_left {
            self.right.new_chunk()
        } else {
            self.left.new_chunk()
        };
        let table = BuildTable::build(build_rows, &self.keys, build_is_left).map_err(key_error)?;
        self.hash = Some(HashState {
            table,
            probe_chunk,
            probe_row: 0,
            probe_done: false,
        });
        Ok(())
    }

    /// Pulls the next probe chunk when the current one is spent.
    fn fill_probe_chunk(&mut self) -> Result<(), ExecError> {
        let Some(hash) = self.hash.as_mut() else {
            return Ok(());
        };
        if hash.probe_row < hash.probe_chunk.num_rows() {
            return Ok(());
        }
        // Move the chunk out so the probe child can be borrowed mutably
        // alongside it; it goes straight back below.
        let mut chunk = std::mem::replace(&mut hash.probe_chunk, Chunk::new_with_capacity(&[], 0));
        let probe: &mut dyn Executor = if self.outer_is_left() {
            self.left.as_mut()
        } else {
            self.right.as_mut()
        };
        let result = probe.next(&mut chunk);
        let hash = self.hash.as_mut().expect("hash state exists in this arm");
        hash.probe_done = chunk.num_rows() == 0;
        hash.probe_chunk = chunk;
        hash.probe_row = 0;
        result
    }

    /// Emits every output row the current probe chunk produces.
    fn drain_probe_chunk(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        let probe_is_left = self.outer_is_left();
        let probe_types: Vec<FieldType> = if probe_is_left {
            self.left.ret_field_types().to_vec()
        } else {
            self.right.ret_field_types().to_vec()
        };
        let offset = |key: &EquiKey| if probe_is_left { key.left } else { key.right };
        loop {
            let Some(hash) = self.hash.as_ref() else {
                return Ok(());
            };
            if hash.probe_row >= hash.probe_chunk.num_rows() {
                return Ok(());
            }
            let probe_row = datum_row(&hash.probe_chunk, hash.probe_row, &probe_types);
            let key = row_key(&self.keys, &probe_row, offset).map_err(key_error)?;
            // A probe row whose key holds a NULL matches nothing, so it never
            // touches the table -- and, on an outer join, pads immediately.
            let candidates: Vec<Vec<Datum>> = match &key {
                Some(key) => hash
                    .table
                    .probe(key)
                    .iter()
                    .map(|index| hash.table.rows[*index as usize].clone())
                    .collect(),
                None => Vec::new(),
            };
            self.emit_outer_row(req, &probe_row, candidates.into_iter())?;
            self.hash
                .as_mut()
                .expect("hash state exists in this arm")
                .probe_row += 1;
        }
    }
}

/// One chunk row as owned `Datum`s.
fn datum_row(chunk: &Chunk, index: usize, types: &[FieldType]) -> Vec<Datum> {
    let row = chunk.get_row(index);
    types
        .iter()
        .enumerate()
        .map(|(c, ft)| row.get_datum(c, ft))
        .collect()
}

/// A key datum outside its column's statically determined class. The class
/// comes from the key columns' own field types, so this is a type-metadata
/// inconsistency rather than a data condition -- it is reported instead of
/// guessed at, because a guess here silently drops rows.
fn key_error(_: KeyError) -> ExecError {
    ExecError::Unsupported("join key value outside its column's comparison domain")
}

/// Go's condition truth test: NULL and zero are false.
fn truthy(value: &Datum) -> bool {
    match value {
        Datum::Null => false,
        Datum::Int(v) => *v != 0,
        Datum::UInt(v) => *v != 0,
        Datum::Real(v) => *v != 0.0,
        _ => true,
    }
}

impl<C: Columns> Executor for JoinExec<C> {
    fn open(&mut self) -> Result<(), ExecError> {
        self.left.open()?;
        self.right.open()?;
        self.emitted = false;
        self.hash = None;
        self.condition_evals.set(0);
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        if self.keys.is_empty() {
            return self.next_nested(req);
        }
        self.next_hashed(req)
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.left.close()?;
        self.right.close()
    }

    fn schema(&self) -> &Schema {
        self.meta.schema()
    }

    fn ret_field_types(&self) -> &[FieldType] {
        self.meta.ret_field_types()
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

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_ast::CiString;
    use tidb_datatype::FieldTypeCode;
    use tidb_expr::column::Column;
    use tidb_expr::scalar_function::ScalarFunction;
    use tidb_expr::NoColumns;

    const CHUNK: usize = 1024;

    fn long() -> FieldType {
        FieldType::new(FieldTypeCode::Long)
    }

    fn schema_of(width: usize) -> Schema {
        Schema::new(
            (0..width)
                .map(|i| {
                    let mut column = Column::new(i as i64 + 1, long());
                    column.index = i as i64;
                    column
                })
                .collect(),
        )
    }

    /// A source that hands out prebuilt rows in `max_chunk_size` batches, so
    /// the probe side really is pulled incrementally rather than in one go.
    struct RowSource {
        meta: ExecutorMeta,
        rows: Vec<Vec<Datum>>,
        cursor: usize,
    }

    impl RowSource {
        fn new(rows: Vec<Vec<Datum>>, width: usize) -> Self {
            RowSource {
                meta: ExecutorMeta::new(schema_of(width), 0, CHUNK, CHUNK),
                rows,
                cursor: 0,
            }
        }
    }

    impl Executor for RowSource {
        fn open(&mut self) -> Result<(), ExecError> {
            self.cursor = 0;
            Ok(())
        }
        fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
            req.reset();
            let end = (self.cursor + CHUNK).min(self.rows.len());
            for row in &self.rows[self.cursor..end] {
                for (c, value) in row.iter().enumerate() {
                    req.append_datum(c, value);
                }
            }
            self.cursor = end;
            Ok(())
        }
        fn close(&mut self) -> Result<(), ExecError> {
            Ok(())
        }
        fn schema(&self) -> &Schema {
            self.meta.schema()
        }
        fn ret_field_types(&self) -> &[FieldType] {
            self.meta.ret_field_types()
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

    /// `left.<lhs> = right.<rhs>`, addressed against the joined schema.
    fn eq_on(lhs: usize, rhs: usize, left_width: usize) -> Expression {
        let column = |index: usize| {
            let mut column = Column::new(index as i64 + 1, long());
            column.index = index as i64;
            Expression::Column(column)
        };
        Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("eq"),
            long(),
            vec![column(lhs), column(left_width + rhs)],
        ))
    }

    fn join_of(
        kind: JoinKind,
        conditions: Vec<Expression>,
        left: Vec<Vec<Datum>>,
        right: Vec<Vec<Datum>>,
        width: usize,
    ) -> JoinExec<NoColumns> {
        JoinExec::new(
            ExecutorMeta::new(schema_of(2 * width), 1, CHUNK, CHUNK),
            kind,
            conditions,
            Box::new(RowSource::new(left, width)),
            Box::new(RowSource::new(right, width)),
            NoColumns,
        )
    }

    /// Drains a join to completion, exactly as a caller does: repeated
    /// `next()` until an empty chunk.
    fn run(join: &mut JoinExec<NoColumns>) -> Vec<Vec<i64>> {
        join.open().unwrap();
        let types = join.ret_field_types().to_vec();
        let mut out = Vec::new();
        let mut req = join.new_chunk();
        loop {
            join.next(&mut req).unwrap();
            if req.num_rows() == 0 {
                break;
            }
            for r in 0..req.num_rows() {
                let row = req.get_row(r);
                out.push(
                    (0..types.len())
                        .map(|c| match row.get_datum(c, &types[c]) {
                            Datum::Int(value) => value,
                            // NULL padding, distinguishable from any test
                            // value because every fixture value is >= 0.
                            Datum::Null => -1,
                            other => panic!("unexpected datum {other:?}"),
                        })
                        .collect(),
                );
            }
        }
        join.close().unwrap();
        out
    }

    /// Left rows: key `i % 7` (so keys repeat and both sides fan out), value
    /// `i`, with every 11th key NULL. Right rows: key `i % 5`, so some keys
    /// match nothing on either side.
    fn fixture(n: i64, modulus: i64) -> Vec<Vec<Datum>> {
        (0..n)
            .map(|i| {
                let key = if i % 11 == 10 {
                    Datum::Null
                } else {
                    Datum::Int(i % modulus)
                };
                vec![key, Datum::Int(i)]
            })
            .collect()
    }

    /// The hash path must reproduce the nested loop ROW FOR ROW -- same
    /// rows, same order -- for every join kind, over data with duplicate
    /// keys, unmatched keys on both sides, and NULL keys on both sides.
    ///
    /// The NULL rows are the point of the fixture: a NULL key matches
    /// nothing (not even another NULL), so an inner join must drop those
    /// rows and an outer join must still emit them NULL-padded. Getting that
    /// wrong is exactly the failure a bucket-based key can introduce.
    #[test]
    fn hash_path_matches_the_nested_loop_row_for_row() {
        for kind in [JoinKind::Inner, JoinKind::Left, JoinKind::Right] {
            let left = fixture(200, 7);
            let right = fixture(200, 5);
            let mut hashed = join_of(kind, vec![eq_on(0, 0, 2)], left.clone(), right.clone(), 2);
            assert!(hashed.is_hash_join());
            let mut looped = join_of(kind, vec![eq_on(0, 0, 2)], left, right, 2);
            looped.force_nested_loop();
            assert_eq!(run(&mut hashed), run(&mut looped), "{kind:?}");
        }
    }

    /// The same, with a non-equi conjunct riding along: the hash table
    /// selects candidates on the equal condition, and the residue still has
    /// to reject the pairs it rejects.
    #[test]
    fn residual_conditions_still_filter_hashed_candidates() {
        let left = fixture(150, 7);
        let right = fixture(150, 5);
        // `l.key = r.key AND l.value = r.value` -- the second conjunct is
        // also an equal condition, so both become keys; the composite key is
        // what must not let one column borrow the other's bytes.
        let conditions = vec![eq_on(0, 0, 2), eq_on(1, 1, 2)];
        let mut hashed = join_of(
            JoinKind::Left,
            conditions.clone(),
            left.clone(),
            right.clone(),
            2,
        );
        let mut looped = join_of(JoinKind::Left, conditions, left, right, 2);
        looped.force_nested_loop();
        assert_eq!(run(&mut hashed), run(&mut looped));
    }

    /// A join with no equal condition keeps the nested loop, as documented.
    #[test]
    fn cross_join_falls_back_to_the_nested_loop() {
        let mut join = join_of(JoinKind::Inner, Vec::new(), fixture(4, 7), fixture(4, 5), 2);
        assert!(!join.is_hash_join());
        assert_eq!(run(&mut join).len(), 16);
    }

    /// The scaling claim, asserted on the cost the hash table exists to
    /// remove rather than on the wall clock.
    ///
    /// 10k x 10k over 10k distinct keys: the nested loop would evaluate the
    /// `ON` clause 100_000_000 times. The hash join evaluates it once per
    /// candidate pair a bucket produces -- here exactly once per matching
    /// row, because the keys are distinct.
    #[test]
    fn ten_thousand_by_ten_thousand_is_linear_not_quadratic() {
        let rows = 10_000i64;
        let side: Vec<Vec<Datum>> = (0..rows)
            .map(|i| vec![Datum::Int(i), Datum::Int(i * 2)])
            .collect();
        let mut join = join_of(JoinKind::Inner, vec![eq_on(0, 0, 2)], side.clone(), side, 2);
        assert!(join.is_hash_join());
        let out = run(&mut join);
        assert_eq!(out.len(), rows as usize);
        // Every output row is the key joined to itself.
        assert_eq!(out[0], vec![0, 0, 0, 0]);
        assert_eq!(out[9_999], vec![9_999, 19_998, 9_999, 19_998]);

        let evals = join.condition_evals();
        let nested_loop_evals = (rows * rows) as u64;
        assert_eq!(evals, rows as u64, "one candidate pair per probe row");
        // Stated as a ratio so the assertion says what it means: at least
        // four orders of magnitude fewer, not a tuned constant.
        assert!(
            evals * 10_000 <= nested_loop_evals,
            "{evals} evaluations vs the nested loop's {nested_loop_evals}"
        );
    }
}
