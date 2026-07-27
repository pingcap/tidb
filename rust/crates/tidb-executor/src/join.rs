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
//! ALGORITHM (documented): this is a nested-loop join that materializes both
//! inputs, not Go's `HashJoinExec`. The results are identical for every join
//! this supports -- Go builds a hash table on the equal conditions and falls
//! back to a Cartesian product plus filter when there are none, which is what
//! the loop does directly -- but the cost is O(left x right) with no build
//! phase and no spill. Equal-condition hash probing, the parallel worker
//! pipeline, and semi/anti/outer-apply variants are separate units.

use crate::executor::{ExecError, Executor, ExecutorMeta};
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

/// A nested-loop join of two children (Go's `HashJoinExec` position in the
/// plan tree; see the module doc for the algorithm difference).
pub struct JoinExec<C: Columns> {
    meta: ExecutorMeta,
    kind: JoinKind,
    conditions: Vec<Expression>,
    left: Box<dyn Executor>,
    right: Box<dyn Executor>,
    ctx: C,
    emitted: bool,
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
        JoinExec {
            meta,
            kind,
            conditions,
            left,
            right,
            ctx,
            emitted: false,
        }
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
                let row = chunk.get_row(r);
                rows.push(
                    types
                        .iter()
                        .enumerate()
                        .map(|(c, ft)| row.get_datum(c, ft))
                        .collect(),
                );
            }
        }
        Ok(rows)
    }

    /// Whether the `ON` conditions all hold for one joined row.
    fn matches(&self, joined: &[Datum]) -> Result<bool, ExecError> {
        if self.conditions.is_empty() {
            return Ok(true);
        }
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
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        if self.emitted {
            return Ok(());
        }
        let left_rows = Self::drain(self.left.as_mut())?;
        let right_rows = Self::drain(self.right.as_mut())?;
        let left_width = self.left.ret_field_types().len();
        let right_width = self.right.ret_field_types().len();

        // The outer side is the one whose unmatched rows survive. Go rewrites
        // a right join into a left join for exactly this reason.
        let (outer, inner, outer_is_left) = match self.kind {
            JoinKind::Inner | JoinKind::Left => (&left_rows, &right_rows, true),
            JoinKind::Right => (&right_rows, &left_rows, false),
        };

        for outer_row in outer {
            let mut matched = false;
            for inner_row in inner {
                let joined: Vec<Datum> = if outer_is_left {
                    outer_row.iter().chain(inner_row).cloned().collect()
                } else {
                    inner_row.iter().chain(outer_row).cloned().collect()
                };
                if !self.matches(&joined)? {
                    continue;
                }
                matched = true;
                for (c, value) in joined.iter().enumerate() {
                    req.append_datum(c, value);
                }
            }
            // An outer row that matched nothing still emits once, padded with
            // NULLs on the other side.
            if !matched && self.kind != JoinKind::Inner {
                let padding = if outer_is_left {
                    right_width
                } else {
                    left_width
                };
                let nulls = std::iter::repeat_n(Datum::Null, padding);
                let joined: Vec<Datum> = if outer_is_left {
                    outer_row.iter().cloned().chain(nulls).collect()
                } else {
                    nulls.chain(outer_row.iter().cloned()).collect()
                };
                for (c, value) in joined.iter().enumerate() {
                    req.append_datum(c, value);
                }
            }
        }
        self.emitted = true;
        Ok(())
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
