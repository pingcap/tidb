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

//! `pkg/executor/aggregate` `HashAggExec`, serial path (`unparallelExec`):
//! group the child's rows by the group-by expressions and fold each group
//! through the aggregate functions.
//!
//! Aggregates ported (from `pkg/executor/aggfuncs`): `COUNT` (NULL inputs
//! skipped; `COUNT(*)` counts rows), `SUM` (NULL inputs skipped; an all-NULL /
//! empty group sums to NULL), and `FIRST_ROW` (the planner's carrier for
//! group-by columns in the output). Groups emit in first-seen order, matching
//! Go's `groupKeys` insertion order. The no-group-by/no-data case emits ONE
//! empty group (`SELECT COUNT(c) FROM t` on empty `t` is `[0]`), while
//! group-by/no-data emits none -- exactly Go's `unparallelExec` rule.
//!
//! DEFERRED (documented): the parallel partial/final worker pipeline, spill,
//! memory tracking; AVG/MIN/MAX/GROUP_CONCAT and DISTINCT modifiers; and
//! SUM-over-integer's DECIMAL result domain -- this seed accumulates integer
//! sums in `i64` and reports overflow as an error rather than widening to
//! decimal (Go returns DECIMAL; lands with the layout-faithful MyDecimal).

use crate::executor::{ExecError, Executor, ExecutorMeta};
use std::collections::HashMap;
use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType};
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_expr::{Columns, EvalError};

/// The aggregate function kinds this seed supports.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggKind {
    /// `COUNT(expr)` / `COUNT(*)` (no argument).
    Count,
    /// `SUM(expr)`.
    Sum,
    /// `FIRST_ROW(expr)`: the first row's value (the planner's group-column
    /// carrier).
    FirstRow,
}

/// One aggregate: a kind plus its argument (`None` only for `COUNT(*)`).
#[derive(Clone, Debug)]
pub struct AggFunc {
    /// The aggregate kind.
    pub kind: AggKind,
    /// The argument expression; `None` means `COUNT(*)`.
    pub arg: Option<Expression>,
}

/// One group's partial results, in agg-func order.
enum Partial {
    Count(i64),
    /// `None` until the first non-NULL input (an empty sum is NULL).
    SumInt(Option<i64>),
    SumReal(Option<f64>),
    /// `None` until the first row is seen.
    FirstRow(Option<Datum>),
}

impl Partial {
    fn new(kind: AggKind) -> Partial {
        match kind {
            AggKind::Count => Partial::Count(0),
            // The sum's domain is chosen lazily from the first non-NULL input.
            AggKind::Sum => Partial::SumInt(None),
            AggKind::FirstRow => Partial::FirstRow(None),
        }
    }

    fn update(&mut self, value: Option<Datum>) -> Result<(), ExecError> {
        match (self, value) {
            // COUNT(*): every row counts; COUNT(expr): NULL skipped.
            (Partial::Count(n), None) => *n += 1,
            (Partial::Count(_), Some(Datum::Null)) => {}
            (Partial::Count(n), Some(_)) => *n += 1,
            (Partial::SumInt(_) | Partial::SumReal(_), None) => {
                return Err(ExecError::Unsupported("SUM requires an argument"))
            }
            (Partial::SumInt(_) | Partial::SumReal(_), Some(Datum::Null)) => {}
            (this @ Partial::SumInt(_), Some(Datum::Int(v))) => {
                let Partial::SumInt(acc) = this else {
                    unreachable!()
                };
                let base = acc.unwrap_or(0);
                *acc = Some(
                    base.checked_add(v)
                        .ok_or(ExecError::Eval(EvalError::IntOverflow))?,
                );
            }
            (this @ Partial::SumInt(None), Some(Datum::Real(v))) => {
                // First non-NULL input is real: the sum's domain is real.
                *this = Partial::SumReal(Some(v));
            }
            (Partial::SumReal(acc), Some(Datum::Real(v))) => {
                *acc = Some(acc.unwrap_or(0.0) + v);
            }
            (Partial::SumReal(acc), Some(Datum::Int(v))) => {
                *acc = Some(acc.unwrap_or(0.0) + v as f64);
            }
            (Partial::SumInt(_) | Partial::SumReal(_), Some(_)) => {
                return Err(ExecError::Unsupported(
                    "SUM over this datum kind is not yet supported",
                ))
            }
            (Partial::FirstRow(slot), value) => {
                if slot.is_none() {
                    slot.replace(value.unwrap_or(Datum::Null));
                }
            }
        }
        Ok(())
    }

    fn finish(&self) -> Datum {
        match self {
            Partial::Count(n) => Datum::Int(*n),
            Partial::SumInt(None) | Partial::SumReal(None) => Datum::Null,
            Partial::SumInt(Some(v)) => Datum::Int(*v),
            Partial::SumReal(Some(v)) => Datum::Real(*v),
            Partial::FirstRow(v) => v.clone().unwrap_or(Datum::Null),
        }
    }
}

/// Go `HashAggExec` (serial): hash aggregation over the child's rows.
pub struct HashAggExec<C: Columns> {
    meta: ExecutorMeta,
    group_by: Vec<Expression>,
    agg_funcs: Vec<AggFunc>,
    child: Box<dyn Executor>,
    ctx: C,
    child_chunk: Chunk,
    emitted: bool,
}

impl<C: Columns> HashAggExec<C> {
    /// Builds a hash aggregation of `agg_funcs` over `child`, grouped by
    /// `group_by` (empty for a global aggregate).
    #[must_use]
    pub fn new(
        meta: ExecutorMeta,
        group_by: Vec<Expression>,
        agg_funcs: Vec<AggFunc>,
        child: Box<dyn Executor>,
        ctx: C,
    ) -> Self {
        let child_chunk = child.new_chunk();
        HashAggExec {
            meta,
            group_by,
            agg_funcs,
            child,
            ctx,
            child_chunk,
            emitted: false,
        }
    }
}

impl<C: Columns> Executor for HashAggExec<C> {
    fn open(&mut self) -> Result<(), ExecError> {
        self.child.open()?;
        self.child_chunk.reset();
        self.emitted = false;
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        if self.emitted {
            return Ok(());
        }
        // Group key (encoded bytes) -> partials; emission in first-seen order.
        let mut groups: HashMap<Vec<u8>, usize> = HashMap::new();
        let mut ordered: Vec<Vec<Partial>> = Vec::new();
        loop {
            self.child.next(&mut self.child_chunk)?;
            let rows = self.child_chunk.num_rows();
            if rows == 0 {
                break;
            }
            for r in 0..rows {
                let row = self.child_chunk.get_row(r);
                // Encode the group key from the evaluated group-by datums.
                let mut key = Vec::new();
                for expr in &self.group_by {
                    let datum = expr.eval(&self.ctx, row)?;
                    key.extend_from_slice(&tidb_codec::hash_code(&datum));
                    key.push(0xff); // separator, as key parts are length-coded
                }
                let idx = match groups.get(&key) {
                    Some(&idx) => idx,
                    None => {
                        let idx = ordered.len();
                        groups.insert(key, idx);
                        ordered.push(
                            self.agg_funcs
                                .iter()
                                .map(|f| Partial::new(f.kind))
                                .collect(),
                        );
                        idx
                    }
                };
                for (f, partial) in self.agg_funcs.iter().zip(ordered[idx].iter_mut()) {
                    let value = match &f.arg {
                        Some(expr) => Some(expr.eval(&self.ctx, row)?),
                        None => None,
                    };
                    partial.update(value)?;
                }
            }
        }
        // No group-by and no data: one empty group, so a global COUNT is 0.
        if ordered.is_empty() && self.group_by.is_empty() {
            ordered.push(
                self.agg_funcs
                    .iter()
                    .map(|f| Partial::new(f.kind))
                    .collect(),
            );
        }
        for partials in &ordered {
            for (c, partial) in partials.iter().enumerate() {
                req.append_datum(c, &partial.finish());
            }
        }
        self.emitted = true;
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.child.close()
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
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_expr::column::Column;
    use tidb_expr::NoColumns;

    fn long() -> FieldType {
        FieldType::new(FieldTypeCode::LongLong)
    }

    /// A test-only source emitting one prebuilt chunk then EOF.
    struct OneChunkSource {
        meta: ExecutorMeta,
        data: Option<Chunk>,
    }
    impl Executor for OneChunkSource {
        fn open(&mut self) -> Result<(), ExecError> {
            Ok(())
        }
        fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
            req.reset();
            if let Some(data) = self.data.take() {
                for r in 0..data.num_rows() {
                    req.append_row(data.get_row(r));
                }
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

    fn col(index: i64) -> Expression {
        let mut c = Column::new(index + 1, long());
        c.index = index;
        Expression::Column(c)
    }

    fn source(rows: &[(i64, Option<i64>)]) -> Box<dyn Executor> {
        // Two long columns: group key, value (None = NULL).
        let fields = vec![long(), long()];
        let mut data = Chunk::new_with_capacity(&fields, rows.len().max(1));
        for (g, v) in rows {
            data.append_int64(0, *g);
            match v {
                Some(v) => data.append_int64(1, *v),
                None => data.append_null(1),
            }
        }
        let mut cols = Vec::new();
        for i in 0..2 {
            let mut c = Column::new(i + 1, long());
            c.index = i;
            cols.push(c);
        }
        Box::new(OneChunkSource {
            meta: ExecutorMeta::new(Schema::new(cols), 0, rows.len().max(1), 1024),
            data: Some(data),
        })
    }

    fn out_meta(n: usize) -> ExecutorMeta {
        let mut cols = Vec::new();
        for i in 0..n {
            let mut c = Column::new((i + 1) as i64, long());
            c.index = i as i64;
            cols.push(c);
        }
        ExecutorMeta::new(Schema::new(cols), 1, 4, 1024)
    }

    fn run(mut exec: HashAggExec<NoColumns>) -> Vec<Vec<Datum>> {
        exec.open().unwrap();
        let mut req = exec.new_chunk();
        let mut out = Vec::new();
        loop {
            exec.next(&mut req).unwrap();
            if req.num_rows() == 0 {
                break;
            }
            for r in 0..req.num_rows() {
                let row = req.get_row(r);
                out.push(
                    (0..req.num_cols())
                        .map(|c| row.get_datum(c, &long()))
                        .collect(),
                );
            }
        }
        exec.close().unwrap();
        out
    }

    #[test]
    fn global_count_and_sum_with_nulls() {
        // Values: 10, NULL, 30 -> COUNT(v)=2 (NULL skipped), COUNT(*)=3, SUM=40.
        let agg = HashAggExec::new(
            out_meta(3),
            vec![],
            vec![
                AggFunc {
                    kind: AggKind::Count,
                    arg: Some(col(1)),
                },
                AggFunc {
                    kind: AggKind::Count,
                    arg: None,
                },
                AggFunc {
                    kind: AggKind::Sum,
                    arg: Some(col(1)),
                },
            ],
            source(&[(1, Some(10)), (1, None), (1, Some(30))]),
            NoColumns,
        );
        assert_eq!(
            run(agg),
            vec![vec![Datum::Int(2), Datum::Int(3), Datum::Int(40)]]
        );
    }

    #[test]
    fn group_by_emits_first_seen_order() {
        // Groups 2, 1 in first-seen order; FIRST_ROW carries the key.
        let agg = HashAggExec::new(
            out_meta(2),
            vec![col(0)],
            vec![
                AggFunc {
                    kind: AggKind::FirstRow,
                    arg: Some(col(0)),
                },
                AggFunc {
                    kind: AggKind::Sum,
                    arg: Some(col(1)),
                },
            ],
            source(&[(2, Some(5)), (1, Some(7)), (2, Some(6))]),
            NoColumns,
        );
        assert_eq!(
            run(agg),
            vec![
                vec![Datum::Int(2), Datum::Int(11)],
                vec![Datum::Int(1), Datum::Int(7)],
            ]
        );
    }

    #[test]
    fn empty_input_rules() {
        // No group-by + no data: one row, COUNT 0, SUM NULL.
        let agg = HashAggExec::new(
            out_meta(2),
            vec![],
            vec![
                AggFunc {
                    kind: AggKind::Count,
                    arg: Some(col(1)),
                },
                AggFunc {
                    kind: AggKind::Sum,
                    arg: Some(col(1)),
                },
            ],
            source(&[]),
            NoColumns,
        );
        assert_eq!(run(agg), vec![vec![Datum::Int(0), Datum::Null]]);

        // Group-by + no data: empty result.
        let agg = HashAggExec::new(
            out_meta(2),
            vec![col(0)],
            vec![
                AggFunc {
                    kind: AggKind::FirstRow,
                    arg: Some(col(0)),
                },
                AggFunc {
                    kind: AggKind::Count,
                    arg: Some(col(1)),
                },
            ],
            source(&[]),
            NoColumns,
        );
        assert_eq!(run(agg), Vec::<Vec<Datum>>::new());
    }
}
