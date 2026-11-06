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

//! Go `pkg/executor/aggfuncs/func_value.go`: the value evaluator
//! `FIRST_VALUE`, `LAST_VALUE`, `NTH_VALUE`, `LEAD` and `LAG` share, and
//! the first three functions.

use tidb_chunk::chunk::Chunk;
use tidb_chunk::row::Row;
use tidb_datatype::{Datum, EvalType, FieldType, FieldTypeCode};
use tidb_driver_error::ExecError;
use tidb_expr::expression::Expression;

use crate::agg_func::{partial, AggFunc, AggFuncUpdateContext, BaseAggFunc, PartialResult};

/// Go `DefPartialResult4FirstValueSize`.
pub const DEF_PARTIAL_RESULT_4_FIRST_VALUE_SIZE: i64 = 24;
/// Go `DefPartialResult4LastValueSize`.
pub const DEF_PARTIAL_RESULT_4_LAST_VALUE_SIZE: i64 = 24;
/// Go `DefPartialResult4NthValueSize`.
pub const DEF_PARTIAL_RESULT_4_NTH_VALUE_SIZE: i64 = 24;

/// The value type each Go `value4X` holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ValueKind {
    /// Go `value4Int`.
    Int,
    /// Go `value4Float32`.
    Float32,
    /// Go `value4Float64`.
    Float64,
    /// Go `value4Decimal`.
    Decimal,
    /// Go `value4Time`.
    Time,
    /// Go `value4Duration`.
    Duration,
    /// Go `value4String`.
    String,
    /// Go `value4JSON`.
    JSON,
    /// Go `value4VectorFloat32`.
    VectorFloat32,
}

impl ValueKind {
    /// Go `DefValue4XSize`.
    const fn size(self) -> i64 {
        match self {
            Self::Int | Self::Float64 | Self::Decimal | Self::Time => 16,
            Self::Float32 => 8,
            Self::Duration | Self::String => 24,
            Self::JSON => 40,
            Self::VectorFloat32 => 32,
        }
    }
}

/// Go's `VectorFloat32{}` (`unsafe.Sizeof`, no data) and
/// `ZeroVectorFloat32` (a zero-length vector's 4 bytes) usages.
const ZERO_VALUE_VECTOR_MEM_USAGE: i64 = 24;
const ZERO_VECTOR_FLOAT32_MEM_USAGE: i64 = 28;

/// Go `valueEvaluator`: the last evaluated value (`None` is Go's
/// `isNull`), and the bytes Go counts for a variable-size one.
#[derive(Clone, Debug)]
pub struct ValueEvaluator {
    kind: ValueKind,
    val: Option<Datum>,
    mem: i64,
}

/// Go `buildValueEvaluator`.
///
/// # Panics
///
/// Go's "unsupported eval type" for a type no evaluator holds.
#[must_use]
pub fn build_value_evaluator(tp: &FieldType) -> (ValueEvaluator, i64) {
    let mut eval_type = tp.eval_type();
    if tp.code() == FieldTypeCode::Bit {
        eval_type = EvalType::String;
    }
    let kind = match eval_type {
        EvalType::Int => ValueKind::Int,
        EvalType::Real => match tp.code() {
            FieldTypeCode::Float => ValueKind::Float32,
            FieldTypeCode::Double => ValueKind::Float64,
            other => panic!("unsupported real type {other:?}"),
        },
        EvalType::Decimal => ValueKind::Decimal,
        EvalType::Datetime | EvalType::Timestamp => ValueKind::Time,
        EvalType::Duration => ValueKind::Duration,
        EvalType::String => ValueKind::String,
        EvalType::Json => ValueKind::JSON,
        EvalType::VectorFloat32 => ValueKind::VectorFloat32,
        #[allow(unreachable_patterns)]
        other => panic!("unsupported eval type {other:?}"),
    };
    let mem = if kind == ValueKind::VectorFloat32 { ZERO_VALUE_VECTOR_MEM_USAGE } else { 0 };
    (ValueEvaluator { kind, val: None, mem }, kind.size())
}

impl ValueEvaluator {
    /// Go `evaluateRow`: evaluates `expr` on `row` as Go's typed `EvalX`
    /// and keeps it; the delta is a string's, a JSON document's or a
    /// vector's change in size.
    pub(crate) fn evaluate_row(&mut self, ctx: &AggFuncUpdateContext, expr: &Expression, row: Row<'_>) -> Result<i64, ExecError> {
        let original = self.mem;
        let value = expr.eval(ctx, row).map_err(ExecError::Eval)?;
        let value = (!value.is_null()).then(|| self.typed(value)).transpose()?;
        self.mem = match (self.kind, &value) {
            (ValueKind::String, Some(Datum::String(text))) => text.bytes().len() as i64,
            (ValueKind::String, Some(Datum::Bytes(bytes))) => bytes.len() as i64,
            (ValueKind::JSON, Some(Datum::Json(json))) => json.value().len() as i64,
            (ValueKind::VectorFloat32, Some(Datum::VectorFloat32(vector))) => vector.estimated_mem_usage() as i64,
            (ValueKind::VectorFloat32, None) => ZERO_VECTOR_FLOAT32_MEM_USAGE,
            _ => 0,
        };
        self.val = value;
        Ok(self.mem - original)
    }

    /// Go's typed `EvalX` of a non-NULL datum.
    fn typed(&self, value: Datum) -> Result<Datum, ExecError> {
        Ok(match (self.kind, value) {
            (ValueKind::Int, value @ (Datum::Int(_) | Datum::UInt(_))) => value,
            (ValueKind::Float32, Datum::Real(v) | Datum::Float32(v)) => Datum::Float32(f64::from(v as f32)),
            (ValueKind::Float64, Datum::Real(v) | Datum::Float32(v)) => Datum::Real(v),
            (ValueKind::Decimal, value @ Datum::Decimal(_)) => value,
            (ValueKind::Time, value @ Datum::Time(_)) => value,
            (ValueKind::Duration, value @ Datum::Duration(_)) => value,
            (ValueKind::String, value @ (Datum::String(_) | Datum::Bytes(_))) => value,
            // Go `Column.EvalString`'s hybrid case.
            (ValueKind::String, hybrid @ (Datum::Enum(..) | Datum::Set(..) | Datum::Bit(_) | Datum::BinaryLiteral(_))) => {
                Datum::Bytes(hybrid.sql_bytes().map_err(|error| ExecError::internal(error.to_string()))?)
            }
            (ValueKind::JSON, value @ Datum::Json(_)) => value,
            (ValueKind::VectorFloat32, value @ Datum::VectorFloat32(_)) => value,
            (kind, value) => return Err(ExecError::internal(format!("a {kind:?} value evaluator read {value:?}"))),
        })
    }

    /// Go `appendResult`.
    pub(crate) fn append_result(&self, chk: &mut Chunk, col_idx: usize) -> Result<(), ExecError> {
        let Some(value) = &self.val else {
            chk.append_null(col_idx);
            return Ok(());
        };
        match (self.kind, value) {
            (ValueKind::Int, Datum::Int(v)) => chk.append_int64(col_idx, *v),
            (ValueKind::Int, Datum::UInt(v)) => chk.append_int64(col_idx, *v as i64),
            (ValueKind::Float32, Datum::Float32(v)) => chk.append_float32(col_idx, *v as f32),
            (ValueKind::Float64, Datum::Real(v)) => chk.append_float64(col_idx, *v),
            (ValueKind::Decimal, Datum::Decimal(v)) => chk.append_my_decimal(col_idx, &crate::func_sum::my_decimal(v)?),
            (ValueKind::Time, Datum::Time(v)) => chk.append_time(col_idx, *v),
            (ValueKind::Duration, Datum::Duration(v)) => chk.append_duration(col_idx, *v),
            (ValueKind::String, Datum::String(text)) => chk.append_bytes(col_idx, text.bytes()),
            (ValueKind::String, Datum::Bytes(bytes)) => chk.append_bytes(col_idx, bytes),
            (ValueKind::JSON, Datum::Json(json)) => chk.append_json(col_idx, json),
            (ValueKind::VectorFloat32, Datum::VectorFloat32(vector)) => chk.append_vector_float32(col_idx, vector),
            (kind, value) => return Err(ExecError::internal(format!("a {kind:?} value evaluator holds {value:?}"))),
        }
        Ok(())
    }
}

/// Go `partialResult4FirstValue` and `partialResult4LastValue`.
#[derive(Clone, Debug)]
pub struct PartialResult4FirstValue {
    got_value: bool,
    evaluator: ValueEvaluator,
}

/// Go `firstValue`.
#[derive(Clone, Debug)]
pub struct FirstValue {
    /// Go's embedded `baseAggFunc`.
    pub base: BaseAggFunc,
    /// Go `tp`.
    pub tp: FieldType,
}

impl AggFunc for FirstValue {
    fn alloc_partial_result(&self) -> (PartialResult, i64) {
        let (evaluator, ve_mem_delta) = build_value_evaluator(&self.tp);
        (Box::new(PartialResult4FirstValue { got_value: false, evaluator }), DEF_PARTIAL_RESULT_4_FIRST_VALUE_SIZE + ve_mem_delta)
    }

    fn reset_partial_result(&self, pr: &mut PartialResult) {
        partial::<PartialResult4FirstValue>(pr).got_value = false;
    }

    fn update_partial_result(&self, sctx: &AggFuncUpdateContext, rows_in_group: &[Row<'_>], pr: &mut PartialResult) -> Result<i64, ExecError> {
        let p = partial::<PartialResult4FirstValue>(pr);
        if p.got_value {
            return Ok(0);
        }
        let Some(row) = rows_in_group.first() else {
            return Ok(0);
        };
        p.got_value = true;
        p.evaluator.evaluate_row(sctx, &self.base.args[0], *row)
    }

    fn append_final_result_2_chunk(&self, _sctx: &AggFuncUpdateContext, pr: &mut PartialResult, chk: &mut Chunk) -> Result<(), ExecError> {
        let p = partial::<PartialResult4FirstValue>(pr);
        if !p.got_value {
            chk.append_null(self.base.ordinal);
            return Ok(());
        }
        p.evaluator.append_result(chk, self.base.ordinal)
    }
}

/// Go `lastValue`.
#[derive(Clone, Debug)]
pub struct LastValue {
    /// Go's embedded `baseAggFunc`.
    pub base: BaseAggFunc,
    /// Go `tp`.
    pub tp: FieldType,
}

impl AggFunc for LastValue {
    fn alloc_partial_result(&self) -> (PartialResult, i64) {
        let (evaluator, ve_mem_delta) = build_value_evaluator(&self.tp);
        (Box::new(PartialResult4FirstValue { got_value: false, evaluator }), DEF_PARTIAL_RESULT_4_LAST_VALUE_SIZE + ve_mem_delta)
    }

    fn reset_partial_result(&self, pr: &mut PartialResult) {
        partial::<PartialResult4FirstValue>(pr).got_value = false;
    }

    fn update_partial_result(&self, sctx: &AggFuncUpdateContext, rows_in_group: &[Row<'_>], pr: &mut PartialResult) -> Result<i64, ExecError> {
        let p = partial::<PartialResult4FirstValue>(pr);
        let Some(row) = rows_in_group.last() else {
            return Ok(0);
        };
        p.got_value = true;
        p.evaluator.evaluate_row(sctx, &self.base.args[0], *row)
    }

    fn append_final_result_2_chunk(&self, _sctx: &AggFuncUpdateContext, pr: &mut PartialResult, chk: &mut Chunk) -> Result<(), ExecError> {
        let p = partial::<PartialResult4FirstValue>(pr);
        if !p.got_value {
            chk.append_null(self.base.ordinal);
            return Ok(());
        }
        p.evaluator.append_result(chk, self.base.ordinal)
    }
}

/// Go `partialResult4NthValue`.
#[derive(Clone, Debug)]
pub struct PartialResult4NthValue {
    seen_rows: u64,
    evaluator: ValueEvaluator,
}

/// Go `nthValue`.
#[derive(Clone, Debug)]
pub struct NthValue {
    /// Go's embedded `baseAggFunc`.
    pub base: BaseAggFunc,
    /// Go `tp`.
    pub tp: FieldType,
    /// Go `nth`.
    pub nth: u64,
}

impl AggFunc for NthValue {
    fn alloc_partial_result(&self) -> (PartialResult, i64) {
        let (evaluator, ve_mem_delta) = build_value_evaluator(&self.tp);
        (Box::new(PartialResult4NthValue { seen_rows: 0, evaluator }), DEF_PARTIAL_RESULT_4_NTH_VALUE_SIZE + ve_mem_delta)
    }

    fn reset_partial_result(&self, pr: &mut PartialResult) {
        partial::<PartialResult4NthValue>(pr).seen_rows = 0;
    }

    fn update_partial_result(&self, sctx: &AggFuncUpdateContext, rows_in_group: &[Row<'_>], pr: &mut PartialResult) -> Result<i64, ExecError> {
        if self.nth == 0 {
            return Ok(0);
        }
        let p = partial::<PartialResult4NthValue>(pr);
        let num_rows = rows_in_group.len() as u64;
        let mut mem_delta = 0;
        if self.nth > p.seen_rows && self.nth - p.seen_rows <= num_rows {
            mem_delta = p.evaluator.evaluate_row(sctx, &self.base.args[0], rows_in_group[(self.nth - p.seen_rows - 1) as usize])?;
        }
        p.seen_rows += num_rows;
        Ok(mem_delta)
    }

    fn append_final_result_2_chunk(&self, _sctx: &AggFuncUpdateContext, pr: &mut PartialResult, chk: &mut Chunk) -> Result<(), ExecError> {
        let p = partial::<PartialResult4NthValue>(pr);
        if self.nth == 0 || p.seen_rows < self.nth {
            chk.append_null(self.base.ordinal);
            return Ok(());
        }
        p.evaluator.append_result(chk, self.base.ordinal)
    }
}
