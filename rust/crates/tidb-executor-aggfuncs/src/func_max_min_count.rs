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

//! Go master `pkg/executor/aggfuncs/func_max_min_count.go`: `MAX_COUNT`
//! and `MIN_COUNT`, the count of rows tied at the argument's extreme.
//!
//! As [`MaxMin`] does, one struct over the argument's `Datum` stands for
//! Go's `maxMinCount4X` family, keeping each one's struct size, comparison
//! and spill encoding (`isNull`, `count`, then the value), and
//! [`MaxMinCountSliding`] for Go's `maxMinCount4XSliding` family.

use tidb_chunk::chunk::Chunk;
use tidb_chunk::row::Row;
use tidb_datatype::Datum;
use tidb_driver_error::ExecError;
use tidb_serialization as util;

use crate::agg_func::{
    deserialize_partial_result_common, partial, AggFunc, AggFuncUpdateContext, MaxMinSlidingWindowAggFunc, PartialResult,
    SlidingWindowAggFunc,
};
use crate::func_max_min::{MaxMin, MaxMinKind};
use crate::spill_serialize_helper::SerializeHelper;

/// Go `DefPartialResult4MaxMinCountIntSize`.
pub const DEF_PARTIAL_RESULT_4_MAX_MIN_COUNT_INT_SIZE: i64 = 24;
/// Go `DefPartialResult4MaxMinCountUintSize`.
pub const DEF_PARTIAL_RESULT_4_MAX_MIN_COUNT_UINT_SIZE: i64 = 24;
/// Go `DefPartialResult4MaxMinCountDecimalSize`.
pub const DEF_PARTIAL_RESULT_4_MAX_MIN_COUNT_DECIMAL_SIZE: i64 = 56;
/// Go `DefPartialResult4MaxMinCountFloat32Size`.
pub const DEF_PARTIAL_RESULT_4_MAX_MIN_COUNT_FLOAT32_SIZE: i64 = 24;
/// Go `DefPartialResult4MaxMinCountFloat64Size`.
pub const DEF_PARTIAL_RESULT_4_MAX_MIN_COUNT_FLOAT64_SIZE: i64 = 24;
/// Go `DefPartialResult4MaxMinCountTimeSize`.
pub const DEF_PARTIAL_RESULT_4_MAX_MIN_COUNT_TIME_SIZE: i64 = 24;
/// Go `DefPartialResult4MaxMinCountDurationSize`.
pub const DEF_PARTIAL_RESULT_4_MAX_MIN_COUNT_DURATION_SIZE: i64 = 32;
/// Go `DefPartialResult4MaxMinCountStringSize`.
pub const DEF_PARTIAL_RESULT_4_MAX_MIN_COUNT_STRING_SIZE: i64 = 32;
/// Go `DefPartialResult4MaxMinCountJSONSize`.
pub const DEF_PARTIAL_RESULT_4_MAX_MIN_COUNT_JSON_SIZE: i64 = 48;
/// Go `DefPartialResult4MaxMinCountVectorFloat32Size`.
pub const DEF_PARTIAL_RESULT_4_MAX_MIN_COUNT_VECTOR_FLOAT32_SIZE: i64 = 40;
/// Go `DefPartialResult4MaxMinCountEnumSize`.
pub const DEF_PARTIAL_RESULT_4_MAX_MIN_COUNT_ENUM_SIZE: i64 = 40;
/// Go `DefPartialResult4MaxMinCountSetSize`.
pub const DEF_PARTIAL_RESULT_4_MAX_MIN_COUNT_SET_SIZE: i64 = 40;

/// Go `DefPartialResult4MaxMinCountSlidingSize`.
pub const DEF_PARTIAL_RESULT_4_MAX_MIN_COUNT_SLIDING_SIZE: i64 = 24;
/// Go `DefMaxMinCountDequeSize`.
pub const DEF_MAX_MIN_COUNT_DEQUE_SIZE: i64 = 40;

/// Go `partialResult4MaxMinCountX`: the extreme so far (`None` while Go's
/// `isNull` holds) and how many rows are tied at it.
#[derive(Clone, Debug, Default)]
pub struct PartialResult4MaxMinCount {
    pub(crate) val: Option<Datum>,
    pub(crate) count: i64,
    /// Go `partialResult4MaxMinCountSliding.deque`: the frame's candidates,
    /// kept by the sliding variant only.
    pub(crate) deque: Option<MinMaxCountDeque>,
}

/// Go `maxMinCountDequeItem`.
#[derive(Clone, Debug)]
pub(crate) struct MaxMinCountDequeItem {
    item: Datum,
    idxs: std::collections::VecDeque<u64>,
}

/// Go `minMaxCountDeque`: the frame's candidate extremes, each with the
/// positions of the rows tied at it.
#[derive(Clone, Debug)]
pub(crate) struct MinMaxCountDeque {
    items: std::collections::VecDeque<MaxMinCountDequeItem>,
    is_max: bool,
}

impl MinMaxCountDeque {
    fn new(is_max: bool) -> Self {
        Self { items: std::collections::VecDeque::with_capacity(64), is_max }
    }

    /// Go `enqueue`.
    fn enqueue(
        &mut self,
        idx: u64,
        item: Datum,
        cmp: impl Fn(&Datum, &Datum) -> Result<std::cmp::Ordering, ExecError>,
    ) -> Result<(), ExecError> {
        while let Some(back) = self.items.back_mut() {
            let ordering = cmp(&item, &back.item)?;
            if (self.is_max && ordering.is_gt()) || (!self.is_max && ordering.is_lt()) {
                self.items.pop_back();
                continue;
            }
            if ordering.is_eq() {
                back.idxs.push_back(idx);
                return Ok(());
            }
            break;
        }
        self.items.push_back(MaxMinCountDequeItem { item, idxs: std::collections::VecDeque::from([idx]) });
        Ok(())
    }

    /// Go `dequeue`: drops every position at or before `boundary`.
    fn dequeue(&mut self, boundary: u64) {
        while let Some(front) = self.items.front_mut() {
            let first_alive = front.idxs.iter().take_while(|&&idx| idx <= boundary).count();
            if first_alive == 0 {
                return;
            }
            front.idxs.drain(..first_alive);
            if !front.idxs.is_empty() {
                return;
            }
            self.items.pop_front();
        }
    }
}

/// Go `maxMinCount4X` (`baseMaxMinCountAggFunc`).
#[derive(Clone, Debug)]
pub struct MaxMinCount {
    /// The extreme's value type, comparison and encoding (Go's embedded
    /// `baseMaxMinAggFunc`).
    pub inner: MaxMin,
    /// Go `unsupportedRowBasedFinalMaxMinCount`: the function's name when
    /// this is a Final / Partial2 stage over the two-column partial result
    /// (count, value), which Go builds but refuses to update.
    pub unsupported_row_based_final: Option<&'static str>,
}

impl MaxMinCount {
    /// Go `DefPartialResult4MaxMinCountXSize` of this argument kind.
    const fn size(kind: MaxMinKind) -> i64 {
        match kind {
            MaxMinKind::Int => DEF_PARTIAL_RESULT_4_MAX_MIN_COUNT_INT_SIZE,
            MaxMinKind::Uint => DEF_PARTIAL_RESULT_4_MAX_MIN_COUNT_UINT_SIZE,
            MaxMinKind::Float32 => DEF_PARTIAL_RESULT_4_MAX_MIN_COUNT_FLOAT32_SIZE,
            MaxMinKind::Float64 => DEF_PARTIAL_RESULT_4_MAX_MIN_COUNT_FLOAT64_SIZE,
            MaxMinKind::Time => DEF_PARTIAL_RESULT_4_MAX_MIN_COUNT_TIME_SIZE,
            MaxMinKind::Decimal => DEF_PARTIAL_RESULT_4_MAX_MIN_COUNT_DECIMAL_SIZE,
            MaxMinKind::String => DEF_PARTIAL_RESULT_4_MAX_MIN_COUNT_STRING_SIZE,
            MaxMinKind::Duration => DEF_PARTIAL_RESULT_4_MAX_MIN_COUNT_DURATION_SIZE,
            MaxMinKind::JSON => DEF_PARTIAL_RESULT_4_MAX_MIN_COUNT_JSON_SIZE,
            MaxMinKind::VectorFloat32 => DEF_PARTIAL_RESULT_4_MAX_MIN_COUNT_VECTOR_FLOAT32_SIZE,
            MaxMinKind::Enum => DEF_PARTIAL_RESULT_4_MAX_MIN_COUNT_ENUM_SIZE,
            MaxMinKind::Set => DEF_PARTIAL_RESULT_4_MAX_MIN_COUNT_SET_SIZE,
        }
    }

    /// Go `shouldReplace` / `shouldAccumulate`: `Some(true)` replaces,
    /// `Some(false)` accumulates, `None` keeps.
    fn decide(&self, candidate: &Datum, current: &Datum) -> Result<Option<bool>, ExecError> {
        let cmp = self.inner.compare(candidate, current)?;
        Ok(if (self.inner.is_max && cmp.is_gt()) || (!self.inner.is_max && cmp.is_lt()) {
            Some(true)
        } else if cmp.is_eq() {
            Some(false)
        } else {
            None
        })
    }
}

impl AggFunc for MaxMinCount {
    fn alloc_partial_result(&self) -> (PartialResult, i64) {
        (Box::new(PartialResult4MaxMinCount::default()), Self::size(self.inner.kind))
    }

    fn reset_partial_result(&self, pr: &mut PartialResult) {
        *partial::<PartialResult4MaxMinCount>(pr) = PartialResult4MaxMinCount::default();
    }

    fn update_partial_result(&self, sctx: &AggFuncUpdateContext, rows_in_group: &[Row<'_>], pr: &mut PartialResult) -> Result<i64, ExecError> {
        if let Some(name) = self.unsupported_row_based_final {
            return Err(ExecError::internal(format!("row-based final aggregation for {name} is unsupported")));
        }
        let p = partial::<PartialResult4MaxMinCount>(pr);
        let mut mem_delta = 0;
        for row in rows_in_group {
            let input = self.inner.base.args[0].eval(sctx, *row).map_err(ExecError::Eval)?;
            if input.is_null() {
                continue;
            }
            let input = self.inner.kind.store(input);
            match p.val.as_ref() {
                None => {
                    mem_delta += self.inner.kind.value_bytes(&input);
                    p.val = Some(input);
                    p.count = 1;
                }
                Some(current) => match self.decide(&input, current)? {
                    Some(true) => {
                        mem_delta += self.inner.kind.value_bytes(&input) - self.inner.kind.value_bytes(current);
                        p.val = Some(input);
                        p.count = 1;
                    }
                    Some(false) => p.count += 1,
                    None => {}
                },
            }
        }
        Ok(mem_delta)
    }

    fn merge_partial_result(&self, _sctx: &AggFuncUpdateContext, src: &mut PartialResult, dst: &mut PartialResult) -> Result<i64, ExecError> {
        let p1 = partial::<PartialResult4MaxMinCount>(src).clone();
        let Some(value) = p1.val else {
            return Ok(0);
        };
        let p2 = partial::<PartialResult4MaxMinCount>(dst);
        match p2.val.as_ref() {
            None => {
                p2.val = Some(value);
                p2.count = p1.count;
            }
            Some(current) => match self.decide(&value, current)? {
                Some(true) => {
                    p2.val = Some(value);
                    p2.count = p1.count;
                }
                Some(false) => p2.count += p1.count,
                None => {}
            },
        }
        Ok(0)
    }

    /// Go `appendFinalResult`: the count, 0 when no row had a value.
    fn append_final_result_2_chunk(&self, _sctx: &AggFuncUpdateContext, pr: &mut PartialResult, chk: &mut Chunk) -> Result<(), ExecError> {
        let p = partial::<PartialResult4MaxMinCount>(pr);
        chk.append_int64(self.inner.base.ordinal, if p.val.is_none() { 0 } else { p.count });
        Ok(())
    }

    fn serialize_partial_result(&self, partial_result: &mut PartialResult, chk: &mut Chunk, _spill_helper: &mut SerializeHelper) {
        let p = partial::<PartialResult4MaxMinCount>(partial_result).clone();
        let mut buf = Vec::with_capacity(64);
        util::serialize_bool(p.val.is_none(), &mut buf);
        util::serialize_i64(p.count, &mut buf);
        self.inner.encode_value(p.val.as_ref(), &mut buf);
        chk.append_bytes(self.inner.base.ordinal, &buf);
    }

    fn deserialize_partial_result(&self, src: &Chunk) -> (Vec<PartialResult>, i64) {
        deserialize_partial_result_common(src, self.inner.base.ordinal, |helper| {
            let (mut pr, mem_delta) = self.alloc_partial_result();
            *partial::<PartialResult4MaxMinCount>(&mut pr) = helper.read_row(|pab| {
                let is_null = pab.read_bool();
                let count = pab.read_i64();
                let value = self.inner.decode_value(pab);
                PartialResult4MaxMinCount { val: (!is_null).then_some(value), count, deque: None }
            })?;
            Some((pr, mem_delta))
        })
    }
}

/// Go `maxMinCount4XSliding` (with `windowInfo`): the window MAX_COUNT /
/// MIN_COUNT over a sliding frame, for the kinds Go slides.
#[derive(Debug)]
pub struct MaxMinCountSliding {
    /// The `maxMinCount4X` it embeds.
    pub inner: MaxMinCount,
    /// Go `windowInfo.start`, which the window executor sets.
    pub start: std::sync::atomic::AtomicU64,
}

impl MaxMinCountSliding {
    fn deque(pr: &mut PartialResult) -> &mut MinMaxCountDeque {
        partial::<PartialResult4MaxMinCount>(pr).deque.as_mut().expect("a sliding MAX_COUNT/MIN_COUNT keeps a deque")
    }

    /// Go `refreshCount4MaxMinCountSliding`.
    fn refresh_count(pr: &mut PartialResult) {
        let p = partial::<PartialResult4MaxMinCount>(pr);
        let front = p.deque.as_ref().and_then(|deque| deque.items.front()).map(|front| (front.item.clone(), front.idxs.len()));
        (p.val, p.count) = match front {
            Some((item, count)) => (Some(item), count as i64),
            None => (None, 0),
        };
    }

    fn enqueue(&self, sctx: &AggFuncUpdateContext, idx: u64, row: Row<'_>, pr: &mut PartialResult) -> Result<(), ExecError> {
        let input = self.inner.inner.base.args[0].eval(sctx, row).map_err(ExecError::Eval)?;
        if input.is_null() {
            return Ok(());
        }
        let input = self.inner.inner.kind.store(input);
        Self::deque(pr).enqueue(idx, input, |a, b| self.inner.inner.compare(a, b))
    }
}

impl AggFunc for MaxMinCountSliding {
    /// Go `allocPartialResult4MaxMinCountSliding`.
    fn alloc_partial_result(&self) -> (PartialResult, i64) {
        let p = PartialResult4MaxMinCount { deque: Some(MinMaxCountDeque::new(self.inner.inner.is_max)), ..Default::default() };
        (Box::new(p), DEF_PARTIAL_RESULT_4_MAX_MIN_COUNT_SLIDING_SIZE + DEF_MAX_MIN_COUNT_DEQUE_SIZE)
    }

    /// Go `resetPartialResult4MaxMinCountSliding`.
    fn reset_partial_result(&self, pr: &mut PartialResult) {
        let p = partial::<PartialResult4MaxMinCount>(pr);
        (p.val, p.count) = (None, 0);
        if let Some(deque) = p.deque.as_mut() {
            deque.items.clear();
        }
    }

    fn update_partial_result(&self, sctx: &AggFuncUpdateContext, rows_in_group: &[Row<'_>], pr: &mut PartialResult) -> Result<i64, ExecError> {
        let start = self.start.load(std::sync::atomic::Ordering::SeqCst);
        for (i, row) in rows_in_group.iter().enumerate() {
            self.enqueue(sctx, i as u64 + start, *row, pr)?;
        }
        Self::refresh_count(pr);
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

impl SlidingWindowAggFunc for MaxMinCountSliding {
    fn slide<'a>(&self, sctx: &AggFuncUpdateContext, get_row: &dyn Fn(u64) -> Result<Row<'a>, ExecError>, last_start: u64, last_end: u64, shift_start: u64, shift_end: u64, pr: &mut PartialResult) -> Result<(), ExecError> {
        for i in 0..shift_end {
            self.enqueue(sctx, last_end.wrapping_add(i), get_row(last_end.wrapping_add(i))?, pr)?;
        }
        if last_start.wrapping_add(shift_start) >= 1 {
            Self::deque(pr).dequeue(last_start.wrapping_add(shift_start) - 1);
        }
        Self::refresh_count(pr);
        Ok(())
    }
}

impl MaxMinSlidingWindowAggFunc for MaxMinCountSliding {
    fn set_window_start(&self, start: u64) {
        self.start.store(start, std::sync::atomic::Ordering::SeqCst);
    }
}
