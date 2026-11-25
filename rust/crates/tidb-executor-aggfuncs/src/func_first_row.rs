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
//! Go keeps one struct per value type; here the first value is generally the `Datum`
//! the argument evaluated to, and each Go function keeps its own struct
//! size, the bytes its stored value adds and its spill encoding.

use tidb_chunk::chunk::Chunk;
use tidb_chunk::row::Row;
use tidb_datatype::{Datum, MyDecimal, MAX_DECIMAL_SCALE, MYDECIMAL_STRUCT_SIZE};
use tidb_driver_error::ExecError;
use tidb_serialization as util;

use crate::agg_func::{append_rounded_decimal, deserialize_partial_result_common, deserialize_typed_partial_results, partial, AggFunc, AggFuncUpdateContext, BaseAggFunc, PartialResult, PartialResultString};
use crate::func_sum::eval_my_decimal;
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

/// Go's typed first-row states store the value directly.
#[derive(Clone, Copy, Default)]
struct FirstRowScalar<T> {
    is_null: bool,
    got_first_row: bool,
    val: T,
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
            (FirstRowKind::Int | FirstRowKind::Decimal, _) => {
                unreachable!("integer and decimal first rows store their values directly")
            }
            (FirstRowKind::Float32, Some(Datum::Float32(value) | Datum::Real(value))) => util::serialize_f32(*value as f32, buf),
            (FirstRowKind::Float32, _) => util::serialize_f32(0.0, buf),
            (FirstRowKind::Float64, Some(Datum::Real(value) | Datum::Float32(value))) => util::serialize_f64(*value, buf),
            (FirstRowKind::Float64, _) => util::serialize_f64(0.0, buf),
            (FirstRowKind::String | FirstRowKind::JSON, _) => {
                unreachable!("string and JSON first rows write the payload directly")
            }
            (FirstRowKind::Time, Some(Datum::Time(value))) => util::serialize_time(*value, buf),
            // Go's zero `types.Time` is the zero packed word.
            (FirstRowKind::Time, _) => util::serialize_u64(0, buf),
            (FirstRowKind::Duration, Some(Datum::Duration(value))) => util::serialize_duration(*value, buf),
            (FirstRowKind::Duration, _) => util::serialize_duration(Default::default(), buf),
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
            FirstRowKind::Int | FirstRowKind::Decimal | FirstRowKind::String => {
                unreachable!("typed first rows store their values directly")
            }
            FirstRowKind::Float32 => Datum::Float32(f64::from(pab.read_f32())),
            FirstRowKind::Float64 => Datum::Real(pab.read_f64()),
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
        let state: PartialResult = match self.kind {
            FirstRowKind::Int => Box::new(FirstRowScalar::<i64>::default()),
            FirstRowKind::Decimal => Box::new(FirstRowScalar::<MyDecimal>::default()),
            FirstRowKind::String => Box::new(FirstRowScalar::<PartialResultString>::default()),
            _ => Box::new(PartialResult4FirstRow::default()),
        };
        (state, self.kind.size())
    }

    fn reset_partial_result(&self, pr: &mut PartialResult) {
        match self.kind {
            FirstRowKind::Int => *partial::<FirstRowScalar<i64>>(pr) = FirstRowScalar::default(),
            FirstRowKind::Decimal => {
                let p = partial::<FirstRowScalar<MyDecimal>>(pr);
                p.is_null = false;
                p.got_first_row = false;
            }
            FirstRowKind::String => {
                let p = partial::<FirstRowScalar<PartialResultString>>(pr);
                p.is_null = false;
                p.got_first_row = false;
            }
            _ => *partial::<PartialResult4FirstRow>(pr) = PartialResult4FirstRow::default(),
        }
    }

    fn update_partial_result(&self, sctx: &AggFuncUpdateContext, rows_in_group: &[Row<'_>], pr: &mut PartialResult) -> Result<i64, ExecError> {
        if self.kind == FirstRowKind::Int {
            let p = partial::<FirstRowScalar<i64>>(pr);
            if !p.got_first_row {
                if let Some(&row) = rows_in_group.first() {
                    let (value, is_null) = match self.base.args[0].eval(sctx, row).map_err(ExecError::Eval)? {
                        Datum::Int(value) => (value, false),
                        Datum::UInt(value) => (value as i64, false),
                        Datum::Null => (0, true),
                        _ => return Err(ExecError::internal("FIRST_ROW read a non-integer argument")),
                    };
                    *p = FirstRowScalar {
                        is_null,
                        got_first_row: true,
                        val: value,
                    };
                }
            }
            return Ok(0);
        }
        if self.kind == FirstRowKind::Decimal {
            let p = partial::<FirstRowScalar<MyDecimal>>(pr);
            if p.got_first_row {
                return Ok(0);
            }
            if let Some(&row) = rows_in_group.first() {
                let input = eval_my_decimal(&self.base, sctx, row)?;
                p.is_null = input.is_none();
                p.got_first_row = true;
                if let Some(input) = input {
                    p.val = input;
                }
            }
            return Ok(0);
        }
        if self.kind == FirstRowKind::String {
            let p = partial::<FirstRowScalar<PartialResultString>>(pr);
            if p.got_first_row {
                return Ok(0);
            }
            let Some(&row) = rows_in_group.first() else {
                return Ok(0);
            };
            let input = self.base.args[0].eval(sctx, row).map_err(ExecError::Eval)?;
            p.val = PartialResultString::from_slice(input.go_bytes());
            p.is_null = input.is_null();
            p.got_first_row = true;
            return Ok(p.val.len() as i64);
        }
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
        if self.kind == FirstRowKind::Int {
            let destination = partial::<FirstRowScalar<i64>>(dst);
            if !destination.got_first_row {
                *destination = *partial::<FirstRowScalar<i64>>(src);
            }
            return Ok(0);
        }
        if self.kind == FirstRowKind::Decimal {
            let destination = partial::<FirstRowScalar<MyDecimal>>(dst);
            if !destination.got_first_row {
                *destination = *partial::<FirstRowScalar<MyDecimal>>(src);
            }
            return Ok(0);
        }
        if self.kind == FirstRowKind::String {
            let destination = partial::<FirstRowScalar<PartialResultString>>(dst);
            if !destination.got_first_row {
                *destination = partial::<FirstRowScalar<PartialResultString>>(src).clone();
            }
            return Ok(0);
        }
        let p1 = partial::<PartialResult4FirstRow>(src).clone();
        let p2 = partial::<PartialResult4FirstRow>(dst);
        if !p2.got_first_row {
            *p2 = p1;
        }
        Ok(0)
    }

    fn append_final_result_2_chunk(&self, _sctx: &AggFuncUpdateContext, pr: &mut PartialResult, chk: &mut Chunk) -> Result<(), ExecError> {
        if self.kind == FirstRowKind::Int {
            let p = partial::<FirstRowScalar<i64>>(pr);
            if p.is_null || !p.got_first_row {
                chk.append_null(self.base.ordinal);
            } else {
                chk.append_int64(self.base.ordinal, p.val);
            }
            return Ok(());
        }
        if self.kind == FirstRowKind::Decimal {
            let p = partial::<FirstRowScalar<MyDecimal>>(pr);
            if p.is_null || !p.got_first_row {
                chk.append_null(self.base.ordinal);
                return Ok(());
            }
            return append_rounded_decimal(&self.base, &mut p.val, chk);
        }
        if self.kind == FirstRowKind::String {
            let p = partial::<FirstRowScalar<PartialResultString>>(pr);
            if p.is_null || !p.got_first_row {
                chk.append_null(self.base.ordinal);
            } else {
                chk.append_bytes(self.base.ordinal, &p.val);
            }
            return Ok(());
        }
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

    fn serialize_partial_result(
        &self,
        partial_result: &mut PartialResult,
        chk: &mut Chunk,
        spill_helper: &mut SerializeHelper,
    ) {
        if self.kind == FirstRowKind::Int {
            let p = partial::<FirstRowScalar<i64>>(partial_result);
            let mut bytes = [0; 10];
            bytes[0] = u8::from(p.is_null);
            bytes[1] = u8::from(p.got_first_row);
            bytes[2..].copy_from_slice(&p.val.to_ne_bytes());
            chk.append_byte_array(self.base.ordinal, &bytes);
            return;
        }
        if self.kind == FirstRowKind::Decimal {
            let p = partial::<FirstRowScalar<MyDecimal>>(partial_result);
            let mut bytes = [0; 2 + MYDECIMAL_STRUCT_SIZE];
            bytes[0] = u8::from(p.is_null);
            bytes[1] = u8::from(p.got_first_row);
            bytes[2..].copy_from_slice(&p.val.to_raw_bytes());
            chk.append_byte_array(self.base.ordinal, &bytes);
            return;
        }
        if self.kind == FirstRowKind::String {
            let p = partial::<FirstRowScalar<PartialResultString>>(partial_result);
            let mut header = [0; 2 + size_of::<isize>()];
            header[0] = u8::from(p.is_null);
            header[1] = u8::from(p.got_first_row);
            spill_helper.append_string_partial(chk, self.base.ordinal, header, &p.val);
            return;
        }
        let p = partial::<PartialResult4FirstRow>(partial_result);
        if self.kind == FirstRowKind::JSON {
            let (type_code, value) = match p.val.as_ref() {
                Some(Datum::Json(value)) => (value.type_code(), value.value()),
                _ => (0, &[][..]),
            };
            let mut header = [0; 3 + size_of::<isize>()];
            header[0] = u8::from(p.is_null);
            header[1] = u8::from(p.got_first_row);
            header[2] = type_code;
            spill_helper.append_buffer(chk, self.base.ordinal, header, value);
            return;
        }
        let bytes = spill_helper.encode(|buf| self.encode(p, buf));
        chk.append_bytes(self.base.ordinal, bytes);
    }

    fn deserialize_partial_result(&self, src: &Chunk) -> (Vec<PartialResult>, i64) {
        if self.kind == FirstRowKind::Int {
            return deserialize_partial_result_common(src, self.base.ordinal, |helper| {
                let state = helper.read_row_bytes(|bytes| {
                    let &[is_null, got_first_row, value @ ..] =
                        bytes.first_chunk::<10>().expect("integer first-row spill cell");
                    FirstRowScalar {
                        is_null: is_null & 1 != 0,
                        got_first_row: got_first_row & 1 != 0,
                        val: i64::from_ne_bytes(value),
                    }
                })?;
                Some((Box::new(state), self.kind.size()))
            });
        }
        if self.kind == FirstRowKind::Decimal {
            return deserialize_partial_result_common(src, self.base.ordinal, |helper| {
                let state = helper.read_row(|pab| FirstRowScalar {
                    is_null: pab.read_bool(),
                    got_first_row: pab.read_bool(),
                    val: pab.read_my_decimal(),
                })?;
                Some((Box::new(state), self.kind.size()))
            });
        }
        if self.kind == FirstRowKind::String {
            return deserialize_typed_partial_results(
                src,
                self.base.ordinal,
                DEF_PARTIAL_RESULT_4_FIRST_ROW_STRING_SIZE,
                |pab| FirstRowScalar {
                    is_null: pab.read_bool(),
                    got_first_row: pab.read_bool(),
                    val: PartialResultString::from_slice(pab.read_buffer()),
                },
            );
        }
        deserialize_partial_result_common(src, self.base.ordinal, |helper| {
            let state = helper.read_row(|pab| self.decode(pab))?;
            Some((Box::new(state), self.kind.size()))
        })
    }
}
