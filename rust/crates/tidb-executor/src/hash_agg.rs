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
//! `MIN`/`MAX` compare with the shared datum ordering and skip NULL inputs
//! (Go `maxMin4*.UpdatePartialResult`); `AVG` keeps Go's sum/count pair and
//! divides at finalize, returning DECIMAL for integer/decimal inputs and
//! DOUBLE for real ones (Go `typeInfer4Avg` + `baseAvg*.AppendFinalResult2Chunk`),
//! with the same `div_precision_increment` the `/` operator uses. `DISTINCT`
//! de-duplicates a function's inputs per group on the datum hash key, as Go's
//! `*4Distinct*` variants do with their `valueSet`.
//!
//! DEFERRED (documented): the parallel partial/final worker pipeline, spill,
//! memory tracking; GROUP_CONCAT and the bit/variance/percentile families;
//! SUM-over-integer's DECIMAL result domain -- this seed accumulates integer
//! sums in `i64` and reports overflow as an error rather than widening to
//! decimal (Go returns DECIMAL; lands with the layout-faithful MyDecimal);
//! and Go's `Round(retTp.GetDecimal())` display step on the AVG result.

use crate::executor::{ExecError, Executor, ExecutorMeta};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, Decimal, FieldType};
use tidb_expr::compare_datums;
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_expr::Columns;

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
    /// `MIN(expr)`.
    Min,
    /// `MAX(expr)`.
    Max,
    /// `AVG(expr)`.
    Avg,
}

/// One aggregate: a kind plus its argument (`None` only for `COUNT(*)`).
#[derive(Clone, Debug)]
pub struct AggFunc {
    /// The aggregate kind.
    pub kind: AggKind,
    /// The argument expression; `None` means `COUNT(*)`.
    pub arg: Option<Expression>,
    /// Go `AggFuncDesc.HasDistinct`: whether repeated input values are counted
    /// once per group.
    pub distinct: bool,
}

impl AggFunc {
    /// An aggregate without the `DISTINCT` modifier.
    #[must_use]
    pub fn new(kind: AggKind, arg: Option<Expression>) -> Self {
        AggFunc {
            kind,
            arg,
            distinct: false,
        }
    }
}

/// One group's partial results, in agg-func order.
enum Partial {
    Count(i64),
    /// `None` until the first non-NULL input (an empty sum is NULL). Go
    /// sums an integer or decimal argument exactly, in the decimal domain --
    /// `SUM` over a BIGINT column is a DECIMAL in MySQL.
    SumDecimal(Option<Decimal>),
    SumReal(Option<f64>),
    /// `None` until the first row is seen.
    FirstRow(Option<Datum>),
    /// `MIN`/`MAX`: the extreme seen so far, `None` while every input was NULL.
    MaxMin {
        value: Option<Datum>,
        is_max: bool,
    },
    /// `AVG` over integer/decimal inputs: Go's exact decimal sum plus count.
    AvgDecimal {
        sum: Decimal,
        count: i64,
    },
    /// `AVG` over real inputs: Go's `partialResult4AvgFloat64`.
    AvgReal {
        sum: f64,
        count: i64,
    },
}

/// One aggregate's per-group state: its partial result plus, for `DISTINCT`,
/// the input values already folded in (Go's `valueSet`).
struct AggState {
    partial: Partial,
    seen: Option<HashSet<Vec<u8>>>,
}

impl AggState {
    fn new(func: &AggFunc) -> AggState {
        AggState {
            partial: Partial::new(func.kind),
            seen: func.distinct.then(HashSet::new),
        }
    }

    /// Folds one row's input in, skipping values this group has already seen
    /// when the function is `DISTINCT`.
    ///
    /// Go keys its `valueSet` on the codec encoding of the evaluated argument;
    /// the datum hash key is that same encoding. A datum with no hash key
    /// (Go's encode error) fails the statement rather than silently counting
    /// twice.
    fn update(&mut self, value: Option<Datum>) -> Result<(), ExecError> {
        if let Some(seen) = &mut self.seen {
            let datum = value.clone().unwrap_or(Datum::Null);
            if datum != Datum::Null {
                let key = datum
                    .to_hash_key()
                    .map_err(|_| ExecError::Unsupported("DISTINCT over this datum kind"))?;
                if !seen.insert(key) {
                    return Ok(());
                }
            }
        }
        self.partial.update(value)
    }
}

impl Partial {
    fn new(kind: AggKind) -> Partial {
        match kind {
            AggKind::Count => Partial::Count(0),
            // The sum's domain is chosen lazily from the first non-NULL input.
            AggKind::Sum => Partial::SumDecimal(None),
            AggKind::FirstRow => Partial::FirstRow(None),
            AggKind::Min => Partial::MaxMin {
                value: None,
                is_max: false,
            },
            AggKind::Max => Partial::MaxMin {
                value: None,
                is_max: true,
            },
            // As with SUM, the domain is chosen from the first non-NULL input:
            // Go picks it from the inferred return type, which is DECIMAL for
            // integer/decimal arguments and DOUBLE for real ones.
            AggKind::Avg => Partial::AvgDecimal {
                sum: Decimal::from_int(0),
                count: 0,
            },
        }
    }

    fn update(&mut self, value: Option<Datum>) -> Result<(), ExecError> {
        match (self, value) {
            // COUNT(*): every row counts; COUNT(expr): NULL skipped.
            (Partial::Count(n), None) => *n += 1,
            (Partial::Count(_), Some(Datum::Null)) => {}
            (Partial::Count(n), Some(_)) => *n += 1,
            (Partial::SumDecimal(_) | Partial::SumReal(_), None) => {
                return Err(ExecError::Unsupported("SUM requires an argument"))
            }
            (Partial::SumDecimal(_) | Partial::SumReal(_), Some(Datum::Null)) => {}
            (this @ Partial::SumDecimal(None), Some(Datum::Real(v))) => {
                // First non-NULL input is real: the sum's domain is real.
                *this = Partial::SumReal(Some(v));
            }
            (Partial::SumDecimal(acc), Some(input)) => {
                let addend = match input {
                    Datum::Int(v) => Decimal::from_int(v),
                    Datum::UInt(v) => Decimal::from_uint(v),
                    Datum::Decimal(d) => d,
                    _ => {
                        return Err(ExecError::Unsupported(
                            "SUM over this datum kind is not yet supported",
                        ))
                    }
                };
                *acc = Some(match acc.take() {
                    Some(sum) => sum.add(&addend),
                    None => addend,
                });
            }
            (Partial::SumReal(acc), Some(Datum::Real(v))) => {
                *acc = Some(acc.unwrap_or(0.0) + v);
            }
            (Partial::SumReal(acc), Some(Datum::Int(v))) => {
                *acc = Some(acc.unwrap_or(0.0) + v as f64);
            }
            (Partial::SumReal(_), Some(_)) => {
                return Err(ExecError::Unsupported(
                    "SUM over this datum kind is not yet supported",
                ))
            }
            (Partial::FirstRow(slot), value) => {
                if slot.is_none() {
                    slot.replace(value.unwrap_or(Datum::Null));
                }
            }
            (Partial::MaxMin { .. }, None) => {
                return Err(ExecError::Unsupported("MIN/MAX requires an argument"))
            }
            (Partial::MaxMin { .. }, Some(Datum::Null)) => {}
            (Partial::MaxMin { value, is_max }, Some(input)) => match value {
                None => *value = Some(input),
                Some(current) => {
                    let ordering = compare_datums(&input, current)?;
                    if (*is_max && ordering == Ordering::Greater)
                        || (!*is_max && ordering == Ordering::Less)
                    {
                        *value = Some(input);
                    }
                }
            },
            (Partial::AvgDecimal { .. } | Partial::AvgReal { .. }, None) => {
                return Err(ExecError::Unsupported("AVG requires an argument"))
            }
            (Partial::AvgDecimal { .. } | Partial::AvgReal { .. }, Some(Datum::Null)) => {}
            (this @ Partial::AvgDecimal { .. }, Some(Datum::Real(v))) => {
                // First non-NULL input is real: Go's return type is DOUBLE.
                let Partial::AvgDecimal { count, .. } = this else {
                    unreachable!()
                };
                debug_assert_eq!(*count, 0, "the domain is fixed by the first input");
                *this = Partial::AvgReal { sum: v, count: 1 };
            }
            (Partial::AvgDecimal { sum, count }, Some(input)) => {
                let addend = match input {
                    Datum::Int(v) => Decimal::from_int(v),
                    Datum::UInt(v) => Decimal::from_uint(v),
                    Datum::Decimal(d) => d,
                    _ => {
                        return Err(ExecError::Unsupported(
                            "AVG over this datum kind is not yet supported",
                        ))
                    }
                };
                *sum = sum.add(&addend);
                *count += 1;
            }
            (Partial::AvgReal { sum, count }, Some(input)) => {
                let addend = match input {
                    Datum::Real(v) => v,
                    Datum::Int(v) => v as f64,
                    _ => {
                        return Err(ExecError::Unsupported(
                            "AVG over this datum kind is not yet supported",
                        ))
                    }
                };
                *sum += addend;
                *count += 1;
            }
        }
        Ok(())
    }

    fn finish(&self) -> Datum {
        match self {
            Partial::Count(n) => Datum::Int(*n),
            Partial::SumDecimal(None) | Partial::SumReal(None) => Datum::Null,
            Partial::SumDecimal(Some(v)) => Datum::Decimal(v.clone()),
            Partial::SumReal(Some(v)) => Datum::Real(*v),
            Partial::FirstRow(v) => v.clone().unwrap_or(Datum::Null),
            Partial::MaxMin { value, .. } => value.clone().unwrap_or(Datum::Null),
            // Go divides the exact sum by the count with the session's
            // div_precision_increment, the same rule the `/` operator follows.
            Partial::AvgDecimal { count: 0, .. } | Partial::AvgReal { count: 0, .. } => Datum::Null,
            Partial::AvgDecimal { sum, count } => {
                let divisor = Decimal::from_int(*count);
                let target_scale = sum.scale() + DIV_PRECISION_INCREMENT;
                match sum.true_div(&divisor, target_scale) {
                    Some(quotient) => Datum::Decimal(quotient),
                    None => Datum::Null,
                }
            }
            Partial::AvgReal { sum, count } => Datum::Real(sum / *count as f64),
        }
    }
}

/// Go's default `div_precision_increment`, the scale AVG's division adds over
/// its sum (`typeInfer4Avg` sets the result's decimals to it).
const DIV_PRECISION_INCREMENT: u32 = 4;

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
        let mut ordered: Vec<Vec<AggState>> = Vec::new();
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
                        ordered.push(self.agg_funcs.iter().map(AggState::new).collect());
                        idx
                    }
                };
                for (f, state) in self.agg_funcs.iter().zip(ordered[idx].iter_mut()) {
                    let value = match &f.arg {
                        Some(expr) => Some(expr.eval(&self.ctx, row)?),
                        None => None,
                    };
                    state.update(value)?;
                }
            }
        }
        // No group-by and no data: one empty group, so a global COUNT is 0.
        if ordered.is_empty() && self.group_by.is_empty() {
            ordered.push(self.agg_funcs.iter().map(AggState::new).collect());
        }
        for states in &ordered {
            for (c, state) in states.iter().enumerate() {
                req.append_datum(c, &state.partial.finish());
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

    /// An output schema whose column types are given, so a decimal result
    /// lands in a decimal cell.
    fn out_meta_typed(types: &[FieldType]) -> ExecutorMeta {
        let mut cols = Vec::new();
        for (i, t) in types.iter().enumerate() {
            let mut c = Column::new((i + 1) as i64, t.clone());
            c.index = i as i64;
            cols.push(c);
        }
        ExecutorMeta::new(Schema::new(cols), 1, 4, 1024)
    }

    fn decimal() -> FieldType {
        FieldType::new(tidb_datatype::FieldTypeCode::NewDecimal)
    }

    fn run_typed(mut exec: HashAggExec<NoColumns>, types: &[FieldType]) -> Vec<Vec<Datum>> {
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
                        .map(|c| row.get_datum(c, &types[c]))
                        .collect(),
                );
            }
        }
        exec.close().unwrap();
        out
    }

    #[test]
    fn min_max_skip_nulls_and_report_null_for_an_all_null_group() {
        let agg = HashAggExec::new(
            out_meta(2),
            vec![],
            vec![
                AggFunc::new(AggKind::Min, Some(col(1))),
                AggFunc::new(AggKind::Max, Some(col(1))),
            ],
            source(&[(1, Some(30)), (1, None), (1, Some(10)), (1, Some(20))]),
            NoColumns,
        );
        assert_eq!(run(agg), vec![vec![Datum::Int(10), Datum::Int(30)]]);

        let agg = HashAggExec::new(
            out_meta(2),
            vec![],
            vec![
                AggFunc::new(AggKind::Min, Some(col(1))),
                AggFunc::new(AggKind::Max, Some(col(1))),
            ],
            source(&[(1, None), (1, None)]),
            NoColumns,
        );
        assert_eq!(run(agg), vec![vec![Datum::Null, Datum::Null]]);
    }

    /// Go divides AVG's exact sum by the count with div_precision_increment,
    /// so an integer average carries four fraction digits. The expectations
    /// are `types.DecimalDiv(sum, count, _, 4)` output from the Go
    /// implementation in this repository.
    #[test]
    fn avg_over_integers_is_decimal_scaled_by_the_precision_increment() {
        let types = [decimal()];
        for (values, want) in [
            (vec![1i64, 2, 3], "2.0000"),
            (vec![1, 2, 4], "2.3333"),
            (vec![1, 0, 0], "0.3333"),
        ] {
            let rows: Vec<(i64, Option<i64>)> = values.iter().map(|v| (1, Some(*v))).collect();
            let agg = HashAggExec::new(
                out_meta_typed(&types),
                vec![],
                vec![AggFunc::new(AggKind::Avg, Some(col(1)))],
                source(&rows),
                NoColumns,
            );
            assert_eq!(
                run_typed(agg, &types),
                vec![vec![Datum::Decimal(tidb_datatype::Decimal::from_literal(
                    want
                ))]],
                "AVG of {values:?}"
            );
        }
    }

    #[test]
    fn avg_of_an_all_null_group_is_null() {
        let types = [decimal()];
        let agg = HashAggExec::new(
            out_meta_typed(&types),
            vec![],
            vec![AggFunc::new(AggKind::Avg, Some(col(1)))],
            source(&[(1, None)]),
            NoColumns,
        );
        assert_eq!(run_typed(agg, &types), vec![vec![Datum::Null]]);
    }

    /// DISTINCT folds repeated inputs once per group, and the de-duplication
    /// is per group, not global.
    #[test]
    fn distinct_folds_repeats_within_each_group() {
        let mut count = AggFunc::new(AggKind::Count, Some(col(1)));
        count.distinct = true;
        let mut sum = AggFunc::new(AggKind::Sum, Some(col(1)));
        sum.distinct = true;
        // SUM lands in a decimal cell, which is the domain Go sums in.
        let agg = HashAggExec::new(
            out_meta_typed(&[long(), long(), decimal()]),
            vec![col(0)],
            vec![AggFunc::new(AggKind::FirstRow, Some(col(0))), count, sum],
            source(&[
                (1, Some(5)),
                (1, Some(5)),
                (1, Some(7)),
                (1, None),
                (2, Some(5)),
            ]),
            NoColumns,
        );
        assert_eq!(
            run_typed(agg, &[long(), long(), decimal()]),
            vec![
                // Group 1 sees 5,5,7,NULL: two distinct non-NULL values.
                vec![
                    Datum::Int(1),
                    Datum::Int(2),
                    Datum::Decimal(tidb_datatype::Decimal::from_int(12))
                ],
                // Group 2's own 5 is not folded into group 1's.
                vec![
                    Datum::Int(2),
                    Datum::Int(1),
                    Datum::Decimal(tidb_datatype::Decimal::from_int(5))
                ],
            ]
        );
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
            out_meta_typed(&[long(), long(), decimal()]),
            vec![],
            vec![
                AggFunc::new(AggKind::Count, Some(col(1))),
                AggFunc::new(AggKind::Count, None),
                AggFunc::new(AggKind::Sum, Some(col(1))),
            ],
            source(&[(1, Some(10)), (1, None), (1, Some(30))]),
            NoColumns,
        );
        assert_eq!(
            run_typed(agg, &[long(), long(), decimal()]),
            vec![vec![
                Datum::Int(2),
                Datum::Int(3),
                Datum::Decimal(tidb_datatype::Decimal::from_int(40))
            ]]
        );
    }

    #[test]
    fn group_by_emits_first_seen_order() {
        // Groups 2, 1 in first-seen order; FIRST_ROW carries the key.
        let agg = HashAggExec::new(
            out_meta_typed(&[long(), decimal()]),
            vec![col(0)],
            vec![
                AggFunc::new(AggKind::FirstRow, Some(col(0))),
                AggFunc::new(AggKind::Sum, Some(col(1))),
            ],
            source(&[(2, Some(5)), (1, Some(7)), (2, Some(6))]),
            NoColumns,
        );
        assert_eq!(
            run_typed(agg, &[long(), decimal()]),
            vec![
                vec![
                    Datum::Int(2),
                    Datum::Decimal(tidb_datatype::Decimal::from_int(11))
                ],
                vec![
                    Datum::Int(1),
                    Datum::Decimal(tidb_datatype::Decimal::from_int(7))
                ],
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
                AggFunc::new(AggKind::Count, Some(col(1))),
                AggFunc::new(AggKind::Sum, Some(col(1))),
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
                AggFunc::new(AggKind::FirstRow, Some(col(0))),
                AggFunc::new(AggKind::Count, Some(col(1))),
            ],
            source(&[]),
            NoColumns,
        );
        assert_eq!(run(agg), Vec::<Vec<Datum>>::new());
    }
}
