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

//! Go `pkg/executor/aggfuncs/func_max_min.go`: `MAX` and `MIN`.
//!
//! Go keeps one struct per value type (`maxMin4Int`, ...); here the
//! extreme so far is the `Datum` it evaluated to, and each Go function keeps
//! its struct size, its comparison (a string under the argument's
//! collation, an enum or set by its name), the bytes a stored variable-size
//! value holds, and its spill encoding.

use std::cmp::Ordering;

use tidb_charset::Collation;
use tidb_chunk::chunk::Chunk;
use tidb_chunk::row::Row;
use tidb_datatype::{Datum, Decimal, MyDecimal, MAX_DECIMAL_SCALE};
use tidb_driver_error::ExecError;
use tidb_goruntime::GoString;
use tidb_serialization as util;

use crate::agg_func::{deserialize_partial_result_common, partial, AggFunc, AggFuncUpdateContext, BaseAggFunc, MaxMinSlidingWindowAggFunc, PartialResult, SlidingWindowAggFunc};
use crate::spill_serialize_helper::SerializeHelper;

/// Go `DefPartialResult4MaxMinIntSize`.
pub const DEF_PARTIAL_RESULT_4_MAX_MIN_INT_SIZE: i64 = 24;
/// Go `DefPartialResult4MaxMinUintSize`.
pub const DEF_PARTIAL_RESULT_4_MAX_MIN_UINT_SIZE: i64 = 24;
/// Go `DefPartialResult4MaxMinDecimalSize`.
pub const DEF_PARTIAL_RESULT_4_MAX_MIN_DECIMAL_SIZE: i64 = 56;
/// Go `DefPartialResult4MaxMinFloat32Size`.
pub const DEF_PARTIAL_RESULT_4_MAX_MIN_FLOAT32_SIZE: i64 = 16;
/// Go `DefPartialResult4MaxMinFloat64Size`.
pub const DEF_PARTIAL_RESULT_4_MAX_MIN_FLOAT64_SIZE: i64 = 24;
/// Go `DefPartialResult4MaxMinTimeSize`.
pub const DEF_PARTIAL_RESULT_4_MAX_MIN_TIME_SIZE: i64 = 24;
/// Go `DefPartialResult4MaxMinDurationSize`.
pub const DEF_PARTIAL_RESULT_4_MAX_MIN_DURATION_SIZE: i64 = 32;
/// Go `DefPartialResult4MaxMinStringSize`.
pub const DEF_PARTIAL_RESULT_4_MAX_MIN_STRING_SIZE: i64 = 32;
/// Go `DefPartialResult4MaxMinJSONSize`.
pub const DEF_PARTIAL_RESULT_4_MAX_MIN_JSON_SIZE: i64 = 40;
/// Go `DefPartialResult4MaxMinVectorFloat32Size`.
pub const DEF_PARTIAL_RESULT_4_MAX_MIN_VECTOR_FLOAT32_SIZE: i64 = 32;
/// Go `DefPartialResult4MaxMinEnumSize`.
pub const DEF_PARTIAL_RESULT_4_MAX_MIN_ENUM_SIZE: i64 = 32;
/// Go `DefPartialResult4MaxMinSetSize`.
pub const DEF_PARTIAL_RESULT_4_MAX_MIN_SET_SIZE: i64 = 32;
/// Go `DefMaxMinDequeSize`.
pub const DEF_MAX_MIN_DEQUE_SIZE: i64 = 40;

/// Go `partialResult4MaxMinX`: the extreme so far, `None` while Go's
/// `isNull` holds, and the sliding variants' deque.
#[derive(Clone, Debug, Default)]
pub struct PartialResult4MaxMin {
    pub(crate) val: Option<Datum>,
    pub(crate) deque: Option<MinMaxDeque>,
}

/// Go `Pair`: a deque item and its absolute row position.
#[derive(Clone, Debug)]
pub struct Pair {
    /// Go `Item`.
    pub item: Datum,
    /// Go `Idx`.
    pub idx: u64,
}

/// Go `MinMaxDeque`: the window's candidates for the extreme, the front
/// the extreme itself -- each newer item drops the older items it beats.
#[derive(Clone, Debug, Default)]
pub struct MinMaxDeque {
    /// Go `Items`.
    pub items: std::collections::VecDeque<Pair>,
    /// Go `IsMax`.
    pub is_max: bool,
}

impl MinMaxDeque {
    /// Go `NewDeque`.
    #[must_use]
    pub fn new(is_max: bool) -> Self {
        Self { items: std::collections::VecDeque::new(), is_max }
    }

    /// Go `Reset`.
    pub fn reset(&mut self) {
        self.items.clear();
    }

    /// Go `PushBack`.
    pub fn push_back(&mut self, idx: u64, item: Datum) {
        self.items.push_back(Pair { item, idx });
    }

    /// Go `PopBack` (`false` for Go's error on an empty deque).
    pub fn pop_back(&mut self) -> bool {
        self.items.pop_back().is_some()
    }

    /// Go `Back`.
    #[must_use]
    pub fn back(&self) -> Option<&Pair> {
        self.items.back()
    }

    /// Go `Front`.
    #[must_use]
    pub fn front(&self) -> Option<&Pair> {
        self.items.front()
    }

    /// Go `Dequeue`: pops the front items at or before `boundary`.
    pub fn dequeue(&mut self, boundary: u64) {
        while self.items.front().is_some_and(|front| front.idx <= boundary) {
            self.items.pop_front();
        }
    }

    /// Go `Enqueue`: pushes `item`, first popping each item at the back it
    /// equals or beats (`cmp_func(item, back)`).
    pub fn enqueue(&mut self, idx: u64, item: Datum, cmp_func: impl Fn(&Datum, &Datum) -> Result<Ordering, ExecError>) -> Result<(), ExecError> {
        while let Some(pair) = self.items.back() {
            let cmp = cmp_func(&item, &pair.item)?;
            if !(cmp.is_ge() && self.is_max || cmp.is_le() && !self.is_max) {
                break;
            }
            self.items.pop_back();
        }
        self.items.push_back(Pair { item, idx });
        Ok(())
    }
}

/// The value type each Go `maxMin4X` holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MaxMinKind {
    /// Go `maxMin4Int`.
    Int,
    /// Go `maxMin4Uint`.
    Uint,
    /// Go `maxMin4Float32`.
    Float32,
    /// Go `maxMin4Float64`.
    Float64,
    /// Go `maxMin4Decimal`.
    Decimal,
    /// Go `maxMin4String`.
    String,
    /// Go `maxMin4Time`.
    Time,
    /// Go `maxMin4Duration`.
    Duration,
    /// Go `maxMin4JSON`.
    JSON,
    /// Go `maxMin4VectorFloat32`.
    VectorFloat32,
    /// Go `maxMin4Enum`.
    Enum,
    /// Go `maxMin4Set`.
    Set,
}

impl MaxMinKind {
    /// Go `DefPartialResult4MaxMinXSize`.
    const fn size(self) -> i64 {
        match self {
            Self::Int => DEF_PARTIAL_RESULT_4_MAX_MIN_INT_SIZE,
            Self::Uint => DEF_PARTIAL_RESULT_4_MAX_MIN_UINT_SIZE,
            Self::Float32 => DEF_PARTIAL_RESULT_4_MAX_MIN_FLOAT32_SIZE,
            Self::Float64 => DEF_PARTIAL_RESULT_4_MAX_MIN_FLOAT64_SIZE,
            Self::Decimal => DEF_PARTIAL_RESULT_4_MAX_MIN_DECIMAL_SIZE,
            Self::String => DEF_PARTIAL_RESULT_4_MAX_MIN_STRING_SIZE,
            Self::Time => DEF_PARTIAL_RESULT_4_MAX_MIN_TIME_SIZE,
            Self::Duration => DEF_PARTIAL_RESULT_4_MAX_MIN_DURATION_SIZE,
            Self::JSON => DEF_PARTIAL_RESULT_4_MAX_MIN_JSON_SIZE,
            Self::VectorFloat32 => DEF_PARTIAL_RESULT_4_MAX_MIN_VECTOR_FLOAT32_SIZE,
            Self::Enum => DEF_PARTIAL_RESULT_4_MAX_MIN_ENUM_SIZE,
            Self::Set => DEF_PARTIAL_RESULT_4_MAX_MIN_SET_SIZE,
        }
    }

    /// The bytes Go counts for a stored variable-size value (`len(val)`,
    /// `len(val.Value)`, `EstimatedMemUsage()`, `len(val.Name)`).
    fn value_bytes(self, value: &Datum) -> i64 {
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

    /// Go's typed value: a `FLOAT` keeps a `float32`, an unsigned integer
    /// its `uint64`.
    fn store(self, value: Datum) -> Datum {
        match (self, value) {
            (Self::Float32, Datum::Real(value) | Datum::Float32(value)) => Datum::Float32(value as f32 as f64),
            (Self::Uint, Datum::Int(value)) => Datum::UInt(value as u64),
            (_, value) => value,
        }
    }
}

/// Go `baseMaxMinAggFunc` and its `maxMin4X`.
#[derive(Clone, Debug)]
pub struct MaxMin {
    /// Go's embedded `baseAggFunc`.
    pub base: BaseAggFunc,
    /// Go `isMax`.
    pub is_max: bool,
    /// Which Go `maxMin4X` this is.
    pub kind: MaxMinKind,
    /// The argument's collation (Go `collator`, and the `CompareString`
    /// collation of `args[0].GetType()`).
    pub collation: Collation,
}

impl MaxMin {
    /// Go's typed comparison of `candidate` with `current`.
    fn compare(&self, candidate: &Datum, current: &Datum) -> Result<Ordering, ExecError> {
        if let (Datum::Duration(left), Datum::Duration(right)) = (candidate, current) {
            // Go `Duration.Compare`: the nanoseconds alone.
            return Ok(left.nanoseconds().cmp(&right.nanoseconds()));
        }
        crate::compare_extreme(candidate, current, self.collation).map_err(ExecError::Eval)
    }

    fn better(&self, candidate: &Datum, current: &Datum) -> Result<bool, ExecError> {
        let cmp = self.compare(candidate, current)?;
        Ok(if self.is_max { cmp == Ordering::Greater } else { cmp == Ordering::Less })
    }

    /// Go `serializePartialResult4MaxMinX`: `isNull`, then the value -- Go's
    /// zero value of the kind while `isNull` holds.
    fn encode(&self, p: &PartialResult4MaxMin, buf: &mut Vec<u8>) {
        util::serialize_bool(p.val.is_none(), buf);
        match (self.kind, p.val.as_ref()) {
            (MaxMinKind::Int, Some(Datum::Int(value))) => util::serialize_i64(*value, buf),
            (MaxMinKind::Int, _) => util::serialize_i64(0, buf),
            (MaxMinKind::Uint, Some(Datum::UInt(value))) => util::serialize_u64(*value, buf),
            (MaxMinKind::Uint, _) => util::serialize_u64(0, buf),
            (MaxMinKind::Float32, Some(Datum::Float32(value) | Datum::Real(value))) => util::serialize_f32(*value as f32, buf),
            (MaxMinKind::Float32, _) => util::serialize_f32(0.0, buf),
            (MaxMinKind::Float64, Some(Datum::Real(value) | Datum::Float32(value))) => util::serialize_f64(*value, buf),
            (MaxMinKind::Float64, _) => util::serialize_f64(0.0, buf),
            (MaxMinKind::Decimal, Some(Datum::Decimal(value))) => util::serialize_my_decimal(
                &value.to_my_decimal().expect("a decimal has Go's MyDecimal form"),
                buf,
            ),
            (MaxMinKind::Decimal, _) => util::serialize_my_decimal(&MyDecimal::default(), buf),
            (MaxMinKind::String, Some(Datum::String(text))) => util::serialize_string(&GoString::from(text.bytes().to_vec()), buf),
            (MaxMinKind::String, Some(Datum::Bytes(bytes))) => util::serialize_string(&GoString::from(bytes.clone()), buf),
            (MaxMinKind::String, _) => util::serialize_string(&GoString::default(), buf),
            (MaxMinKind::Time, Some(Datum::Time(value))) => util::serialize_time(*value, buf),
            (MaxMinKind::Time, _) => util::serialize_u64(0, buf),
            (MaxMinKind::Duration, Some(Datum::Duration(value))) => util::serialize_duration(*value, buf),
            (MaxMinKind::Duration, _) => util::serialize_duration(Default::default(), buf),
            (MaxMinKind::JSON, Some(Datum::Json(value))) => util::serialize_binary_json(value, buf),
            (MaxMinKind::JSON, _) => {
                util::serialize_json_type_code(0, buf);
                util::serialize_bytes_buffer(&[], buf);
            }
            (MaxMinKind::Enum, Some(Datum::Enum(value, _))) => util::serialize_enum(value, buf),
            (MaxMinKind::Enum, _) => util::serialize_enum(&Default::default(), buf),
            (MaxMinKind::Set, Some(Datum::Set(value, _))) => util::serialize_set(value, buf),
            (MaxMinKind::Set, _) => util::serialize_set(&Default::default(), buf),
            // Go's `maxMin4VectorFloat32` has no spill encoding.
            (MaxMinKind::VectorFloat32, _) => panic!("Not implemented"),
        }
    }

    /// Go `deserializePartialResult4MaxMinX`.
    fn decode(&self, pab: &mut util::Cursor<'_>) -> PartialResult4MaxMin {
        let is_null = pab.read_bool();
        let value = match self.kind {
            MaxMinKind::Int => Datum::Int(pab.read_i64()),
            MaxMinKind::Uint => Datum::UInt(pab.read_u64()),
            MaxMinKind::Float32 => Datum::Float32(f64::from(pab.read_f32())),
            MaxMinKind::Float64 => Datum::Real(pab.read_f64()),
            MaxMinKind::Decimal => Datum::Decimal(Decimal::from_my_decimal(&pab.read_my_decimal())),
            MaxMinKind::String => Datum::new_string_datum(pab.read_string().as_bytes().to_vec()),
            MaxMinKind::Time => Datum::Time(pab.read_time()),
            MaxMinKind::Duration => Datum::Duration(pab.read_duration()),
            MaxMinKind::JSON => Datum::Json(pab.read_binary_json()),
            MaxMinKind::Enum => Datum::Enum(pab.read_enum(), self.collation),
            MaxMinKind::Set => Datum::Set(pab.read_set(), self.collation),
            MaxMinKind::VectorFloat32 => panic!("Not implemented"),
        };
        PartialResult4MaxMin {
            val: (!is_null).then_some(value),
            deque: None,
        }
    }
}

impl AggFunc for MaxMin {
    fn alloc_partial_result(&self) -> (PartialResult, i64) {
        (Box::new(PartialResult4MaxMin::default()), self.kind.size())
    }

    fn reset_partial_result(&self, pr: &mut PartialResult) {
        partial::<PartialResult4MaxMin>(pr).val = None;
    }

    fn update_partial_result(&self, sctx: &AggFuncUpdateContext, rows_in_group: &[Row<'_>], pr: &mut PartialResult) -> Result<i64, ExecError> {
        let p = partial::<PartialResult4MaxMin>(pr);
        let mut mem_delta = 0;
        for row in rows_in_group {
            let input = self.base.args[0].eval(sctx, *row).map_err(ExecError::Eval)?;
            if input.is_null() {
                continue;
            }
            let input = self.kind.store(input);
            match p.val.as_ref() {
                None => {
                    mem_delta += self.kind.value_bytes(&input);
                    p.val = Some(input);
                }
                Some(current) => {
                    if self.better(&input, current)? {
                        mem_delta += self.kind.value_bytes(&input) - self.kind.value_bytes(current);
                        p.val = Some(input);
                    }
                }
            }
        }
        Ok(mem_delta)
    }

    fn merge_partial_result(&self, _sctx: &AggFuncUpdateContext, src: &mut PartialResult, dst: &mut PartialResult) -> Result<i64, ExecError> {
        let Some(p1) = partial::<PartialResult4MaxMin>(src).val.clone() else {
            return Ok(0);
        };
        let p2 = partial::<PartialResult4MaxMin>(dst);
        match p2.val.as_ref() {
            None => p2.val = Some(p1),
            Some(current) => {
                if self.better(&p1, current)? {
                    p2.val = Some(p1);
                }
            }
        }
        Ok(0)
    }

    fn append_final_result_2_chunk(&self, _sctx: &AggFuncUpdateContext, pr: &mut PartialResult, chk: &mut Chunk) -> Result<(), ExecError> {
        let p = partial::<PartialResult4MaxMin>(pr);
        let Some(value) = p.val.as_mut() else {
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
        let p = partial::<PartialResult4MaxMin>(partial_result).clone();
        let mut buf = Vec::with_capacity(64);
        self.encode(&p, &mut buf);
        chk.append_bytes(self.base.ordinal, &buf);
    }

    fn deserialize_partial_result(&self, src: &Chunk) -> (Vec<PartialResult>, i64) {
        deserialize_partial_result_common(src, self.base.ordinal, |helper| {
            let (mut pr, mem_delta) = self.alloc_partial_result();
            *partial::<PartialResult4MaxMin>(&mut pr) = helper.read_row(|pab| self.decode(pab))?;
            Some((pr, mem_delta))
        })
    }
}

/// Go `maxMin4XSliding` (and `windowInfo`): the window MAX/MIN, whose
/// partial result keeps a deque of the frame's candidates.
#[derive(Debug)]
pub struct MaxMinSliding {
    /// The `maxMin4X` it embeds.
    pub inner: MaxMin,
    /// Go `windowInfo.start`, which the window executor sets.
    pub start: std::sync::atomic::AtomicU64,
}

impl MaxMinSliding {
    /// Go's sliding types exist for these kinds only.
    #[must_use]
    pub fn slides(kind: MaxMinKind) -> bool {
        matches!(
            kind,
            MaxMinKind::Int
                | MaxMinKind::Uint
                | MaxMinKind::Float32
                | MaxMinKind::Float64
                | MaxMinKind::Decimal
                | MaxMinKind::String
                | MaxMinKind::Time
                | MaxMinKind::Duration
        )
    }

    fn deque(pr: &mut PartialResult) -> &mut MinMaxDeque {
        partial::<PartialResult4MaxMin>(pr).deque.as_mut().expect("a sliding MAX/MIN keeps a deque")
    }

    /// The deque's front, or NULL.
    fn take_front(pr: &mut PartialResult) {
        let front = Self::deque(pr).front().map(|pair| pair.item.clone());
        partial::<PartialResult4MaxMin>(pr).val = front;
    }

    fn enqueue(&self, sctx: &AggFuncUpdateContext, idx: u64, row: Row<'_>, pr: &mut PartialResult) -> Result<(), ExecError> {
        let input = self.inner.base.args[0].eval(sctx, row).map_err(ExecError::Eval)?;
        if input.is_null() {
            return Ok(());
        }
        let input = self.inner.kind.store(input);
        Self::deque(pr).enqueue(idx, input, |a, b| self.inner.compare(a, b))
    }
}

impl AggFunc for MaxMinSliding {
    fn alloc_partial_result(&self) -> (PartialResult, i64) {
        let (mut pr, mem_delta) = self.inner.alloc_partial_result();
        partial::<PartialResult4MaxMin>(&mut pr).deque = Some(MinMaxDeque::new(self.inner.is_max));
        (pr, mem_delta + DEF_MAX_MIN_DEQUE_SIZE)
    }

    fn reset_partial_result(&self, pr: &mut PartialResult) {
        self.inner.reset_partial_result(pr);
        Self::deque(pr).reset();
    }

    fn update_partial_result(&self, sctx: &AggFuncUpdateContext, rows_in_group: &[Row<'_>], pr: &mut PartialResult) -> Result<i64, ExecError> {
        // The deque keys each row by its absolute position: its place in the
        // group from the window's start.
        let start = self.start.load(std::sync::atomic::Ordering::SeqCst);
        for (i, row) in rows_in_group.iter().enumerate() {
            self.enqueue(sctx, i as u64 + start, *row, pr)?;
        }
        Self::take_front(pr);
        Ok(0)
    }

    fn merge_partial_result(&self, sctx: &AggFuncUpdateContext, src: &mut PartialResult, dst: &mut PartialResult) -> Result<i64, ExecError> {
        self.inner.merge_partial_result(sctx, src, dst)
    }

    fn append_final_result_2_chunk(&self, sctx: &AggFuncUpdateContext, pr: &mut PartialResult, chk: &mut Chunk) -> Result<(), ExecError> {
        self.inner.append_final_result_2_chunk(sctx, pr, chk)
    }

    fn serialize_partial_result(&self, partial_result: &mut PartialResult, chk: &mut Chunk, spill_helper: &mut SerializeHelper) {
        self.inner.serialize_partial_result(partial_result, chk, spill_helper);
    }

    fn deserialize_partial_result(&self, src: &Chunk) -> (Vec<PartialResult>, i64) {
        self.inner.deserialize_partial_result(src)
    }

    fn as_sliding_window(&self) -> Option<&dyn SlidingWindowAggFunc> {
        Some(self)
    }

    fn as_max_min_sliding_window(&self) -> Option<&dyn MaxMinSlidingWindowAggFunc> {
        Some(self)
    }
}

impl SlidingWindowAggFunc for MaxMinSliding {
    fn slide<'a>(&self, sctx: &AggFuncUpdateContext, get_row: &dyn Fn(u64) -> Result<Row<'a>, ExecError>, last_start: u64, last_end: u64, shift_start: u64, shift_end: u64, pr: &mut PartialResult) -> Result<(), ExecError> {
        for i in 0..shift_end {
            self.enqueue(sctx, last_end.wrapping_add(i), get_row(last_end.wrapping_add(i))?, pr)?;
        }
        // Check for unsigned underflow.
        if last_start.wrapping_add(shift_start) >= 1 {
            Self::deque(pr).dequeue(last_start.wrapping_add(shift_start) - 1);
        }
        Self::take_front(pr);
        Ok(())
    }
}

impl MaxMinSlidingWindowAggFunc for MaxMinSliding {
    fn set_window_start(&self, start: u64) {
        self.start.store(start, std::sync::atomic::Ordering::SeqCst);
    }
}
