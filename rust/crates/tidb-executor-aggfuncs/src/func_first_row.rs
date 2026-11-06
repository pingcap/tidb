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

//! Go `pkg/executor/aggfuncs/func_first_row.go`: `FIRSTROW`, the value of a
//! group's first row.
//!
//! Go keeps one struct per value type; here the first value is the `Datum`
//! the argument evaluated to, and each Go function keeps its own struct
//! size, the bytes its stored value adds and its spill encoding.

use tidb_chunk::chunk::Chunk;
use tidb_chunk::row::Row;
use tidb_datatype::{Datum, Decimal, MyDecimal, MAX_DECIMAL_SCALE};
use tidb_driver_error::ExecError;
use tidb_goruntime::GoString;
use tidb_serialization as util;

use crate::agg_func::{deserialize_partial_result_common, partial, AggFunc, AggFuncUpdateContext, BaseAggFunc, PartialResult};
use crate::spill_serialize_helper::SerializeHelper;

/// Go `DefPartialResult4FirstRowIntSize`.
pub const DEF_PARTIAL_RESULT_4_FIRST_ROW_INT_SIZE: i64 = 16;
/// Go `DefPartialResult4FirstRowFloat32Size`.
pub const DEF_PARTIAL_RESULT_4_FIRST_ROW_FLOAT32_SIZE: i64 = 8;
/// Go `DefPartialResult4FirstRowFloat64Size`.
pub const DEF_PARTIAL_RESULT_4_FIRST_ROW_FLOAT64_SIZE: i64 = 16;
/// Go `DefPartialResult4FirstRowStringSize`.
pub const DEF_PARTIAL_RESULT_4_FIRST_ROW_STRING_SIZE: i64 = 24;
/// Go `DefPartialResult4FirstRowTimeSize`.
pub const DEF_PARTIAL_RESULT_4_FIRST_ROW_TIME_SIZE: i64 = 16;
/// Go `DefPartialResult4FirstRowDurationSize`.
pub const DEF_PARTIAL_RESULT_4_FIRST_ROW_DURATION_SIZE: i64 = 24;
/// Go `DefPartialResult4FirstRowJSONSize`.
pub const DEF_PARTIAL_RESULT_4_FIRST_ROW_JSON_SIZE: i64 = 40;
/// Go `DefPartialResult4FirstRowVectorFloat32Size`.
pub const DEF_PARTIAL_RESULT_4_FIRST_ROW_VECTOR_FLOAT32_SIZE: i64 = 32;
/// Go `DefPartialResult4FirstRowDecimalSize`.
pub const DEF_PARTIAL_RESULT_4_FIRST_ROW_DECIMAL_SIZE: i64 = 44;
/// Go `DefPartialResult4FirstRowEnumSize`.
pub const DEF_PARTIAL_RESULT_4_FIRST_ROW_ENUM_SIZE: i64 = 32;
/// Go `DefPartialResult4FirstRowSetSize`.
pub const DEF_PARTIAL_RESULT_4_FIRST_ROW_SET_SIZE: i64 = 32;

/// Go `partialResult4FirstRowX`: its `basePartialResult4FirstRow` and the
/// first value, `None` before a row or for a NULL one.
#[derive(Clone, Debug, Default)]
pub struct PartialResult4FirstRow {
    pub(crate) is_null: bool,
    pub(crate) got_first_row: bool,
    pub(crate) val: Option<Datum>,
}

/// The value type each Go `firstRow4X` holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FirstRowKind {
    Int,
    Float32,
    Float64,
    Decimal,
    String,
    Time,
    Duration,
    JSON,
    VectorFloat32,
    Enum,
    Set,
}

impl FirstRowKind {
    /// Go `DefPartialResult4FirstRowXSize`.
    const fn size(self) -> i64 {
        match self {
            Self::Int => DEF_PARTIAL_RESULT_4_FIRST_ROW_INT_SIZE,
            Self::Float32 => DEF_PARTIAL_RESULT_4_FIRST_ROW_FLOAT32_SIZE,
            Self::Float64 => DEF_PARTIAL_RESULT_4_FIRST_ROW_FLOAT64_SIZE,
            Self::Decimal => DEF_PARTIAL_RESULT_4_FIRST_ROW_DECIMAL_SIZE,
            Self::String => DEF_PARTIAL_RESULT_4_FIRST_ROW_STRING_SIZE,
            Self::Time => DEF_PARTIAL_RESULT_4_FIRST_ROW_TIME_SIZE,
            Self::Duration => DEF_PARTIAL_RESULT_4_FIRST_ROW_DURATION_SIZE,
            Self::JSON => DEF_PARTIAL_RESULT_4_FIRST_ROW_JSON_SIZE,
            Self::VectorFloat32 => DEF_PARTIAL_RESULT_4_FIRST_ROW_VECTOR_FLOAT32_SIZE,
            Self::Enum => DEF_PARTIAL_RESULT_4_FIRST_ROW_ENUM_SIZE,
            Self::Set => DEF_PARTIAL_RESULT_4_FIRST_ROW_SET_SIZE,
        }
    }

    /// The bytes Go's `UpdatePartialResult` adds for a stored value: a
    /// string's length, a JSON's value bytes, a vector's estimate, an enum's
    /// or set's name; nothing for a fixed-size value or a NULL.
    fn mem_delta(self, value: Option<&Datum>) -> i64 {
        let Some(value) = value else {
            return 0;
        };
        match (self, value) {
            (Self::String, Datum::String(text)) => text.bytes().len() as i64,
            (Self::String, Datum::Bytes(bytes)) => bytes.len() as i64,
            (Self::JSON, Datum::Json(json)) => json.value().len() as i64,
            (Self::VectorFloat32, Datum::VectorFloat32(vector)) => vector.estimated_mem_usage() as i64,
            (Self::Enum, Datum::Enum(value, _)) => value.name().len() as i64,
            (Self::Set, Datum::Set(value, _)) => value.name().len() as i64,
            _ => 0,
        }
    }

    /// Go's `EvalX` cast of the first row's value: a `FLOAT` keeps a
    /// `float32`.
    fn store(self, value: Datum) -> Datum {
        match (self, value) {
            (Self::Float32, Datum::Real(value) | Datum::Float32(value)) => Datum::Float32(value as f32 as f64),
            (_, value) => value,
        }
    }
}

/// Go's `firstRow4X`.
#[derive(Clone, Debug)]
pub struct FirstRow {
    /// Go's embedded `baseAggFunc`.
    pub base: BaseAggFunc,
    /// Which Go `firstRow4X` this is.
    pub kind: FirstRowKind,
}

impl FirstRow {
    /// Go `serializePartialResult4FirstRowX`: `isNull`, `gotFirstRow`, then
    /// the value -- Go's zero value of the kind before a row or for NULL.
    fn encode(&self, p: &PartialResult4FirstRow, buf: &mut Vec<u8>) {
        util::serialize_bool(p.is_null, buf);
        util::serialize_bool(p.got_first_row, buf);
        match (self.kind, p.val.as_ref()) {
            (FirstRowKind::Int, Some(Datum::Int(value))) => util::serialize_i64(*value, buf),
            (FirstRowKind::Int, Some(Datum::UInt(value))) => util::serialize_i64(*value as i64, buf),
            (FirstRowKind::Int, _) => util::serialize_i64(0, buf),
            (FirstRowKind::Float32, Some(Datum::Float32(value) | Datum::Real(value))) => util::serialize_f32(*value as f32, buf),
            (FirstRowKind::Float32, _) => util::serialize_f32(0.0, buf),
            (FirstRowKind::Float64, Some(Datum::Real(value) | Datum::Float32(value))) => util::serialize_f64(*value, buf),
            (FirstRowKind::Float64, _) => util::serialize_f64(0.0, buf),
            (FirstRowKind::Decimal, Some(Datum::Decimal(value))) => util::serialize_my_decimal(
                &value.to_my_decimal().expect("a decimal has Go's MyDecimal form"),
                buf,
            ),
            (FirstRowKind::Decimal, _) => util::serialize_my_decimal(&MyDecimal::default(), buf),
            (FirstRowKind::String, Some(Datum::String(text))) => util::serialize_string(&GoString::from(text.bytes().to_vec()), buf),
            (FirstRowKind::String, Some(Datum::Bytes(bytes))) => util::serialize_string(&GoString::from(bytes.clone()), buf),
            (FirstRowKind::String, _) => util::serialize_string(&GoString::default(), buf),
            (FirstRowKind::Time, Some(Datum::Time(value))) => util::serialize_time(*value, buf),
            // Go's zero `types.Time` is the zero packed word.
            (FirstRowKind::Time, _) => util::serialize_u64(0, buf),
            (FirstRowKind::Duration, Some(Datum::Duration(value))) => util::serialize_duration(*value, buf),
            (FirstRowKind::Duration, _) => util::serialize_duration(Default::default(), buf),
            (FirstRowKind::JSON, Some(Datum::Json(value))) => util::serialize_binary_json(value, buf),
            // Go's zero `types.BinaryJSON`: type code 0 and no value bytes.
            (FirstRowKind::JSON, _) => {
                util::serialize_json_type_code(0, buf);
                util::serialize_bytes_buffer(&[], buf);
            }
            (FirstRowKind::Enum, Some(Datum::Enum(value, _))) => util::serialize_enum(value, buf),
            (FirstRowKind::Enum, _) => util::serialize_enum(&Default::default(), buf),
            (FirstRowKind::Set, Some(Datum::Set(value, _))) => util::serialize_set(value, buf),
            (FirstRowKind::Set, _) => util::serialize_set(&Default::default(), buf),
            // Go's `firstRow4VectorFloat32` has no spill encoding.
            (FirstRowKind::VectorFloat32, _) => panic!("Not implemented"),
        }
    }

    /// Go `deserializePartialResult4FirstRowX`.
    fn decode(&self, pab: &mut util::Cursor<'_>) -> PartialResult4FirstRow {
        let is_null = pab.read_bool();
        let got_first_row = pab.read_bool();
        let collation = tidb_charset::Collation::DEFAULT;
        let value = match self.kind {
            FirstRowKind::Int => Datum::Int(pab.read_i64()),
            FirstRowKind::Float32 => Datum::Float32(f64::from(pab.read_f32())),
            FirstRowKind::Float64 => Datum::Real(pab.read_f64()),
            FirstRowKind::Decimal => Datum::Decimal(Decimal::from_my_decimal(&pab.read_my_decimal())),
            FirstRowKind::String => Datum::new_string_datum(pab.read_string().as_bytes().to_vec()),
            FirstRowKind::Time => Datum::Time(pab.read_time()),
            FirstRowKind::Duration => Datum::Duration(pab.read_duration()),
            FirstRowKind::JSON => Datum::Json(pab.read_binary_json()),
            FirstRowKind::Enum => Datum::Enum(pab.read_enum(), collation),
            FirstRowKind::Set => Datum::Set(pab.read_set(), collation),
            FirstRowKind::VectorFloat32 => panic!("Not implemented"),
        };
        PartialResult4FirstRow {
            is_null,
            got_first_row,
            val: (got_first_row && !is_null).then_some(value),
        }
    }
}

impl AggFunc for FirstRow {
    fn alloc_partial_result(&self) -> (PartialResult, i64) {
        (Box::new(PartialResult4FirstRow::default()), self.kind.size())
    }

    fn reset_partial_result(&self, pr: &mut PartialResult) {
        *partial::<PartialResult4FirstRow>(pr) = PartialResult4FirstRow::default();
    }

    fn update_partial_result(&self, sctx: &AggFuncUpdateContext, rows_in_group: &[Row<'_>], pr: &mut PartialResult) -> Result<i64, ExecError> {
        let p = partial::<PartialResult4FirstRow>(pr);
        if p.got_first_row {
            return Ok(0);
        }
        let Some(row) = rows_in_group.first() else {
            return Ok(0);
        };
        let input = self.base.args[0].eval(sctx, *row).map_err(ExecError::Eval)?;
        p.got_first_row = true;
        p.is_null = input.is_null();
        p.val = (!p.is_null).then(|| self.kind.store(input));
        Ok(self.kind.mem_delta(p.val.as_ref()))
    }

    fn merge_partial_result(&self, _sctx: &AggFuncUpdateContext, src: &mut PartialResult, dst: &mut PartialResult) -> Result<i64, ExecError> {
        let p1 = partial::<PartialResult4FirstRow>(src).clone();
        let p2 = partial::<PartialResult4FirstRow>(dst);
        if !p2.got_first_row {
            *p2 = p1;
        }
        Ok(0)
    }

    fn append_final_result_2_chunk(&self, _sctx: &AggFuncUpdateContext, pr: &mut PartialResult, chk: &mut Chunk) -> Result<(), ExecError> {
        let p = partial::<PartialResult4FirstRow>(pr);
        let Some(value) = p.val.as_mut().filter(|_| p.got_first_row && !p.is_null) else {
            chk.append_null(self.base.ordinal);
            return Ok(());
        };
        if let Datum::Decimal(decimal) = value {
            // Go `p.val.Round(&p.val, frac, types.ModeHalfUp)`.
            let mut frac = self.base.ret_tp.decimal();
            if frac == -1 {
                frac = MAX_DECIMAL_SCALE as i64;
            }
            *decimal = decimal.round_to_scale(frac as i32);
        }
        chk.append_datum(self.base.ordinal, value);
        Ok(())
    }

    fn serialize_partial_result(&self, partial_result: &mut PartialResult, chk: &mut Chunk, _spill_helper: &mut SerializeHelper) {
        let p = partial::<PartialResult4FirstRow>(partial_result).clone();
        let mut buf = Vec::with_capacity(64);
        self.encode(&p, &mut buf);
        chk.append_bytes(self.base.ordinal, &buf);
    }

    fn deserialize_partial_result(&self, src: &Chunk) -> (Vec<PartialResult>, i64) {
        deserialize_partial_result_common(src, self.base.ordinal, |helper| {
            let (mut pr, mem_delta) = self.alloc_partial_result();
            *partial::<PartialResult4FirstRow>(&mut pr) = helper.read_row(|pab| self.decode(pab))?;
            Some((pr, mem_delta))
        })
    }
}
