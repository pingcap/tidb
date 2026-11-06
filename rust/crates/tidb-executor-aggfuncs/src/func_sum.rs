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

//! Go `pkg/executor/aggfuncs/func_sum.go`: `SUM` over reals and decimals,
//! with and without `DISTINCT`.

use tidb_chunk::chunk::Chunk;
use tidb_chunk::row::Row;
use tidb_datatype::{Datum, Decimal, MyDecimal, MAX_DECIMAL_SCALE, MYDECIMAL_STRUCT_SIZE};
use tidb_driver_error::ExecError;
use tidb_goruntime::GoString;
use tidb_set::{Float64SetWithMemoryUsage, StringToDecimalMapWithMemoryUsage};

use crate::agg_func::{
    deserialize_partial_result_common, partial, AggFunc, AggFuncUpdateContext, BaseAggFunc,
    PartialResult, SlidingWindowAggFunc,
};
use crate::spill_serialize_helper::SerializeHelper;

/// Go `DefPartialResult4SumFloat64Size`.
pub const DEF_PARTIAL_RESULT_4_SUM_FLOAT64_SIZE: i64 = 16;
/// Go `DefPartialResult4SumDecimalSize`.
pub const DEF_PARTIAL_RESULT_4_SUM_DECIMAL_SIZE: i64 = 48;
/// Go `DefPartialResult4SumDistinctFloat64Size`.
pub const DEF_PARTIAL_RESULT_4_SUM_DISTINCT_FLOAT64_SIZE: i64 = 32;
/// Go `DefPartialResult4SumDistinctDecimalSize`.
pub const DEF_PARTIAL_RESULT_4_SUM_DISTINCT_DECIMAL_SIZE: i64 = 40;

/// Go `partialResult4SumFloat64`.
#[derive(Clone, Debug, Default)]
pub struct PartialResult4SumFloat64 {
    pub(crate) val: f64,
    pub(crate) not_null_row_count: i64,
}

/// Go `partialResult4SumDecimal`. The value is the datum decimal whose
/// `add_mysql` is Go's `DecimalAdd`; spilling writes it as Go's `MyDecimal`.
#[derive(Clone, Debug)]
pub struct PartialResult4SumDecimal {
    pub(crate) val: Decimal,
    pub(crate) not_null_row_count: i64,
}

/// Go's zero `partialResult4SumDecimal`: a zero `MyDecimal`.
impl Default for PartialResult4SumDecimal {
    fn default() -> Self {
        Self {
            val: Decimal::from_int(0),
            not_null_row_count: 0,
        }
    }
}

/// Go `partialResult4SumDistinctFloat64`.
pub struct PartialResult4SumDistinctFloat64 {
    val_set: Float64SetWithMemoryUsage,
}

/// Go `partialResult4SumDistinctDecimal`.
pub struct PartialResult4SumDistinctDecimal {
    val_set: StringToDecimalMapWithMemoryUsage,
}

fn eval(base: &BaseAggFunc, sctx: &AggFuncUpdateContext, row: Row<'_>) -> Result<Datum, ExecError> {
    base.args[0].eval(sctx, row).map_err(ExecError::Eval)
}

/// Go `EvalReal`: `None` for NULL.
pub(crate) fn eval_real(base: &BaseAggFunc, sctx: &AggFuncUpdateContext, row: Row<'_>) -> Result<Option<f64>, ExecError> {
    Ok(match eval(base, sctx, row)? {
        Datum::Null => None,
        Datum::Real(value) | Datum::Float32(value) => Some(value),
        Datum::Int(value) => Some(value as f64),
        Datum::UInt(value) => Some(value as f64),
        _ => return Err(ExecError::internal("a real aggregate read a non-real argument")),
    })
}

/// Go `EvalDecimal`: `None` for NULL.
pub(crate) fn eval_decimal(base: &BaseAggFunc, sctx: &AggFuncUpdateContext, row: Row<'_>) -> Result<Option<Decimal>, ExecError> {
    Ok(match eval(base, sctx, row)? {
        Datum::Null => None,
        Datum::Decimal(value) => Some(value),
        Datum::Int(value) => Some(Decimal::from_int(value)),
        Datum::UInt(value) => Some(Decimal::from_uint(value)),
        _ => return Err(ExecError::internal("a decimal aggregate read a non-decimal argument")),
    })
}

/// Go `types.DecimalAdd`.
pub(crate) fn decimal_add(left: &Decimal, right: &Decimal) -> Decimal {
    left.add_mysql(right).0
}

/// Go `types.DecimalSub`.
pub(crate) fn decimal_sub(left: &Decimal, right: &Decimal) -> Decimal {
    left.sub_mysql(right).0
}

pub(crate) fn my_decimal(value: &Decimal) -> Result<MyDecimal, ExecError> {
    value
        .to_my_decimal()
        .map_err(|error| ExecError::internal(format!("{error:?}")))
}

/// Go `baseSum4Float64`.
#[derive(Clone, Debug)]
pub struct BaseSum4Float64 {
    /// Go's embedded `baseAggFunc`.
    pub base: BaseAggFunc,
}

impl AggFunc for BaseSum4Float64 {
    fn alloc_partial_result(&self) -> (PartialResult, i64) {
        (Box::new(PartialResult4SumFloat64::default()), DEF_PARTIAL_RESULT_4_SUM_FLOAT64_SIZE)
    }

    fn reset_partial_result(&self, pr: &mut PartialResult) {
        *partial::<PartialResult4SumFloat64>(pr) = PartialResult4SumFloat64::default();
    }

    fn append_final_result_2_chunk(&self, _sctx: &AggFuncUpdateContext, pr: &mut PartialResult, chk: &mut Chunk) -> Result<(), ExecError> {
        let p = partial::<PartialResult4SumFloat64>(pr);
        if p.not_null_row_count == 0 {
            chk.append_null(self.base.ordinal);
            return Ok(());
        }
        chk.append_float64(self.base.ordinal, p.val);
        Ok(())
    }

    fn update_partial_result(&self, sctx: &AggFuncUpdateContext, rows_in_group: &[Row<'_>], pr: &mut PartialResult) -> Result<i64, ExecError> {
        let p = partial::<PartialResult4SumFloat64>(pr);
        for row in rows_in_group {
            let Some(input) = eval_real(&self.base, sctx, *row)? else {
                continue;
            };
            p.val += input;
            p.not_null_row_count += 1;
        }
        Ok(0)
    }

    fn merge_partial_result(&self, _sctx: &AggFuncUpdateContext, src: &mut PartialResult, dst: &mut PartialResult) -> Result<i64, ExecError> {
        let p1 = partial::<PartialResult4SumFloat64>(src).clone();
        if p1.not_null_row_count == 0 {
            return Ok(0);
        }
        let p2 = partial::<PartialResult4SumFloat64>(dst);
        p2.val += p1.val;
        p2.not_null_row_count += p1.not_null_row_count;
        Ok(0)
    }

    fn serialize_partial_result(&self, partial_result: &mut PartialResult, chk: &mut Chunk, spill_helper: &mut SerializeHelper) {
        let pr = partial::<PartialResult4SumFloat64>(partial_result).clone();
        let res_buf = spill_helper.serialize_partial_result_4_sum_float64(&pr);
        chk.append_bytes(self.base.ordinal, res_buf);
    }

    fn deserialize_partial_result(&self, src: &Chunk) -> (Vec<PartialResult>, i64) {
        deserialize_partial_result_common(src, self.base.ordinal, |helper| {
            let (mut pr, mem_delta) = self.alloc_partial_result();
            *partial::<PartialResult4SumFloat64>(&mut pr) = helper.deserialize_partial_result_4_sum_float64()?;
            Some((pr, mem_delta))
        })
    }
}

/// Go `sum4Float64`: the sliding-window `SUM` over reals.
#[derive(Clone, Debug)]
pub struct Sum4Float64(pub BaseSum4Float64);

/// Go `sum4Float64HighPrecision`: the same sum, never slid.
#[derive(Clone, Debug)]
pub struct Sum4Float64HighPrecision(pub BaseSum4Float64);

/// Delegates every `AggFunc` method to the wrapped base.
macro_rules! delegate_agg_func {
    ($name:ident) => {
        fn alloc_partial_result(&self) -> (PartialResult, i64) {
            self.0.alloc_partial_result()
        }
        fn reset_partial_result(&self, pr: &mut PartialResult) {
            self.0.reset_partial_result(pr)
        }
        fn update_partial_result(&self, sctx: &AggFuncUpdateContext, rows_in_group: &[Row<'_>], pr: &mut PartialResult) -> Result<i64, ExecError> {
            self.0.update_partial_result(sctx, rows_in_group, pr)
        }
        fn merge_partial_result(&self, sctx: &AggFuncUpdateContext, src: &mut PartialResult, dst: &mut PartialResult) -> Result<i64, ExecError> {
            self.0.merge_partial_result(sctx, src, dst)
        }
        fn append_final_result_2_chunk(&self, sctx: &AggFuncUpdateContext, pr: &mut PartialResult, chk: &mut Chunk) -> Result<(), ExecError> {
            self.0.append_final_result_2_chunk(sctx, pr, chk)
        }
        fn serialize_partial_result(&self, partial_result: &mut PartialResult, chk: &mut Chunk, spill_helper: &mut SerializeHelper) {
            self.0.serialize_partial_result(partial_result, chk, spill_helper)
        }
        fn deserialize_partial_result(&self, src: &Chunk) -> (Vec<PartialResult>, i64) {
            self.0.deserialize_partial_result(src)
        }
    };
}
pub(crate) use delegate_agg_func;

impl AggFunc for Sum4Float64 {
    delegate_agg_func!(Sum4Float64);

    fn as_sliding_window(&self) -> Option<&dyn SlidingWindowAggFunc> {
        Some(self)
    }
}

impl SlidingWindowAggFunc for Sum4Float64 {
    fn slide<'a>(&self, sctx: &AggFuncUpdateContext, get_row: &dyn Fn(u64) -> Row<'a>, last_start: u64, last_end: u64, shift_start: u64, shift_end: u64, pr: &mut PartialResult) -> Result<(), ExecError> {
        let p = partial::<PartialResult4SumFloat64>(pr);
        for i in 0..shift_end {
            let Some(input) = eval_real(&self.0.base, sctx, get_row(last_end + i))? else {
                continue;
            };
            p.val += input;
            p.not_null_row_count += 1;
        }
        for i in 0..shift_start {
            let Some(input) = eval_real(&self.0.base, sctx, get_row(last_start + i))? else {
                continue;
            };
            p.val -= input;
            p.not_null_row_count -= 1;
        }
        Ok(())
    }
}

impl AggFunc for Sum4Float64HighPrecision {
    delegate_agg_func!(Sum4Float64HighPrecision);
}

/// Go `sum4Decimal`.
#[derive(Clone, Debug)]
pub struct Sum4Decimal {
    /// Go's embedded `baseAggFunc`.
    pub base: BaseAggFunc,
}

impl Sum4Decimal {
    fn add_input(p: &mut PartialResult4SumDecimal, input: Decimal) {
        if p.not_null_row_count == 0 {
            p.val = input;
            p.not_null_row_count = 1;
            return;
        }
        p.val = decimal_add(&p.val, &input);
        p.not_null_row_count += 1;
    }
}

impl AggFunc for Sum4Decimal {
    fn alloc_partial_result(&self) -> (PartialResult, i64) {
        (Box::new(PartialResult4SumDecimal::default()), DEF_PARTIAL_RESULT_4_SUM_DECIMAL_SIZE)
    }

    fn reset_partial_result(&self, pr: &mut PartialResult) {
        partial::<PartialResult4SumDecimal>(pr).not_null_row_count = 0;
    }

    fn append_final_result_2_chunk(&self, _sctx: &AggFuncUpdateContext, pr: &mut PartialResult, chk: &mut Chunk) -> Result<(), ExecError> {
        let p = partial::<PartialResult4SumDecimal>(pr);
        if p.not_null_row_count == 0 {
            chk.append_null(self.base.ordinal);
            return Ok(());
        }
        let mut frac = self.base.ret_tp.decimal();
        if frac == -1 {
            frac = MAX_DECIMAL_SCALE as i64;
        }
        // Go `p.val.Round(&p.val, frac, types.ModeHalfUp)`.
        p.val = p.val.round_to_scale(frac as i32);
        chk.append_datum(self.base.ordinal, &Datum::Decimal(p.val.clone()));
        Ok(())
    }

    fn update_partial_result(&self, sctx: &AggFuncUpdateContext, rows_in_group: &[Row<'_>], pr: &mut PartialResult) -> Result<i64, ExecError> {
        let p = partial::<PartialResult4SumDecimal>(pr);
        for row in rows_in_group {
            let Some(input) = eval_decimal(&self.base, sctx, *row)? else {
                continue;
            };
            Self::add_input(p, input);
        }
        Ok(0)
    }

    fn merge_partial_result(&self, _sctx: &AggFuncUpdateContext, src: &mut PartialResult, dst: &mut PartialResult) -> Result<i64, ExecError> {
        let p1 = partial::<PartialResult4SumDecimal>(src).clone();
        if p1.not_null_row_count == 0 {
            return Ok(0);
        }
        let p2 = partial::<PartialResult4SumDecimal>(dst);
        p2.val = decimal_add(&p1.val, &p2.val);
        p2.not_null_row_count += p1.not_null_row_count;
        Ok(0)
    }

    fn serialize_partial_result(&self, partial_result: &mut PartialResult, chk: &mut Chunk, spill_helper: &mut SerializeHelper) {
        let pr = partial::<PartialResult4SumDecimal>(partial_result).clone();
        let value = my_decimal(&pr.val).expect("a summed decimal has Go's MyDecimal form");
        let res_buf = spill_helper.serialize_partial_result_4_sum_decimal(&value, pr.not_null_row_count);
        chk.append_bytes(self.base.ordinal, res_buf);
    }

    fn deserialize_partial_result(&self, src: &Chunk) -> (Vec<PartialResult>, i64) {
        deserialize_partial_result_common(src, self.base.ordinal, |helper| {
            let (mut pr, mem_delta) = self.alloc_partial_result();
            let (value, not_null_row_count) = helper.deserialize_partial_result_4_sum_decimal()?;
            *partial::<PartialResult4SumDecimal>(&mut pr) = PartialResult4SumDecimal {
                val: Decimal::from_my_decimal(&value),
                not_null_row_count,
            };
            Some((pr, mem_delta))
        })
    }

    fn as_sliding_window(&self) -> Option<&dyn SlidingWindowAggFunc> {
        Some(self)
    }
}

impl SlidingWindowAggFunc for Sum4Decimal {
    fn slide<'a>(&self, sctx: &AggFuncUpdateContext, get_row: &dyn Fn(u64) -> Row<'a>, last_start: u64, last_end: u64, shift_start: u64, shift_end: u64, pr: &mut PartialResult) -> Result<(), ExecError> {
        let p = partial::<PartialResult4SumDecimal>(pr);
        for i in 0..shift_end {
            let Some(input) = eval_decimal(&self.base, sctx, get_row(last_end + i))? else {
                continue;
            };
            Self::add_input(p, input);
        }
        for i in 0..shift_start {
            let Some(input) = eval_decimal(&self.base, sctx, get_row(last_start + i))? else {
                continue;
            };
            p.val = decimal_sub(&p.val, &input);
            p.not_null_row_count -= 1;
        }
        Ok(())
    }
}

/// Go `baseSumDistinct4Float64`.
#[derive(Clone, Debug)]
pub struct BaseSumDistinct4Float64 {
    /// Go's embedded `baseAggFunc`.
    pub base: BaseAggFunc,
}

impl BaseSumDistinct4Float64 {
    fn alloc(&self) -> (PartialResult, i64) {
        let (val_set, set_size) = Float64SetWithMemoryUsage::new(std::iter::empty());
        (Box::new(PartialResult4SumDistinctFloat64 { val_set }), DEF_PARTIAL_RESULT_4_SUM_DISTINCT_FLOAT64_SIZE + set_size)
    }

    fn update(&self, sctx: &AggFuncUpdateContext, rows_in_group: &[Row<'_>], pr: &mut PartialResult) -> Result<i64, ExecError> {
        let p = partial::<PartialResult4SumDistinctFloat64>(pr);
        let mut mem_delta = 0;
        for row in rows_in_group {
            let Some(input) = eval_real(&self.base, sctx, *row)? else {
                continue;
            };
            if p.val_set.contains(input) {
                continue;
            }
            mem_delta += p.val_set.insert(input);
        }
        Ok(mem_delta)
    }

    fn append_final(&self, pr: &mut PartialResult, chk: &mut Chunk) {
        let p = partial::<PartialResult4SumDistinctFloat64>(pr);
        if p.val_set.is_empty() {
            chk.append_null(self.base.ordinal);
            return;
        }
        let sum: f64 = p.val_set.iter().sum();
        chk.append_float64(self.base.ordinal, sum);
    }

    fn merge(&self, src: &mut PartialResult, dst: &mut PartialResult) -> i64 {
        let source: Vec<f64> = partial::<PartialResult4SumDistinctFloat64>(src).val_set.iter().collect();
        let d = partial::<PartialResult4SumDistinctFloat64>(dst);
        let mut mem_delta = 0;
        for val in source {
            if d.val_set.contains(val) {
                continue;
            }
            mem_delta += d.val_set.insert(val);
        }
        mem_delta
    }
}

/// Go `baseSumDistinct4Decimal`.
#[derive(Clone, Debug)]
pub struct BaseSumDistinct4Decimal {
    /// Go's embedded `baseAggFunc`.
    pub base: BaseAggFunc,
}

impl BaseSumDistinct4Decimal {
    fn alloc(&self) -> (PartialResult, i64) {
        let (val_set, set_size) = StringToDecimalMapWithMemoryUsage::new();
        (Box::new(PartialResult4SumDistinctDecimal { val_set }), DEF_PARTIAL_RESULT_4_SUM_DISTINCT_DECIMAL_SIZE + set_size)
    }

    fn update(&self, sctx: &AggFuncUpdateContext, rows_in_group: &[Row<'_>], pr: &mut PartialResult) -> Result<i64, ExecError> {
        let p = partial::<PartialResult4SumDistinctDecimal>(pr);
        let mut mem_delta = 0;
        for row in rows_in_group {
            let Some(input) = eval_decimal(&self.base, sctx, *row)? else {
                continue;
            };
            let (hash, _) = input
                .to_hash_key()
                .map_err(|error| ExecError::internal(format!("{error:?}")))?;
            let key_str = GoString::from(hash);
            if p.val_set.get(&key_str).is_some() {
                continue;
            }
            let key_len = key_str.len() as i64;
            mem_delta += p.val_set.insert(key_str, Box::new(my_decimal(&input)?))
                + key_len
                + MYDECIMAL_STRUCT_SIZE as i64;
        }
        Ok(mem_delta)
    }

    fn append_final(&self, pr: &mut PartialResult, chk: &mut Chunk) {
        let p = partial::<PartialResult4SumDistinctDecimal>(pr);
        if p.val_set.is_empty() {
            chk.append_null(self.base.ordinal);
            return;
        }
        let mut sum = Decimal::from_int(0);
        for (_, val) in p.val_set.iter() {
            sum = decimal_add(&sum, &Decimal::from_my_decimal(val));
        }
        chk.append_datum(self.base.ordinal, &Datum::Decimal(sum));
    }

    fn merge(&self, src: &mut PartialResult, dst: &mut PartialResult) -> i64 {
        let source: Vec<(GoString, MyDecimal)> = partial::<PartialResult4SumDistinctDecimal>(src)
            .val_set
            .iter()
            .map(|(key, val)| (key.clone(), (**val).clone()))
            .collect();
        let d = partial::<PartialResult4SumDistinctDecimal>(dst);
        let mut mem_delta = 0;
        for (key, val) in source {
            if d.val_set.get(&key).is_some() {
                continue;
            }
            mem_delta += d.val_set.insert(key, Box::new(val));
        }
        mem_delta
    }
}

/// The `SUM(DISTINCT ...)` wrappers: Go's partial one merges, its original
/// one panics on a merge.
macro_rules! sum_distinct {
    ($partial_name:ident, $original_name:ident, $base:ident) => {
        #[doc = concat!("Go `", stringify!($partial_name), "`.")]
        #[derive(Clone, Debug)]
        pub struct $partial_name(pub $base);

        #[doc = concat!("Go `", stringify!($original_name), "`.")]
        #[derive(Clone, Debug)]
        pub struct $original_name(pub $base);

        impl AggFunc for $partial_name {
            fn alloc_partial_result(&self) -> (PartialResult, i64) {
                self.0.alloc()
            }
            fn reset_partial_result(&self, pr: &mut PartialResult) {
                *pr = self.0.alloc().0;
            }
            fn update_partial_result(&self, sctx: &AggFuncUpdateContext, rows_in_group: &[Row<'_>], pr: &mut PartialResult) -> Result<i64, ExecError> {
                self.0.update(sctx, rows_in_group, pr)
            }
            fn merge_partial_result(&self, _sctx: &AggFuncUpdateContext, src: &mut PartialResult, dst: &mut PartialResult) -> Result<i64, ExecError> {
                Ok(self.0.merge(src, dst))
            }
            fn append_final_result_2_chunk(&self, _sctx: &AggFuncUpdateContext, pr: &mut PartialResult, chk: &mut Chunk) -> Result<(), ExecError> {
                self.0.append_final(pr, chk);
                Ok(())
            }
        }

        impl AggFunc for $original_name {
            fn alloc_partial_result(&self) -> (PartialResult, i64) {
                self.0.alloc()
            }
            fn reset_partial_result(&self, pr: &mut PartialResult) {
                *pr = self.0.alloc().0;
            }
            fn update_partial_result(&self, sctx: &AggFuncUpdateContext, rows_in_group: &[Row<'_>], pr: &mut PartialResult) -> Result<i64, ExecError> {
                self.0.update(sctx, rows_in_group, pr)
            }
            fn append_final_result_2_chunk(&self, _sctx: &AggFuncUpdateContext, pr: &mut PartialResult, chk: &mut Chunk) -> Result<(), ExecError> {
                self.0.append_final(pr, chk);
                Ok(())
            }
        }
    };
}

sum_distinct!(Sum4PartialDistinctFloat64, Sum4OriginalDistinct4Float64, BaseSumDistinct4Float64);
sum_distinct!(Sum4PartialDistinct4Decimal, Sum4OriginalDistinct4Decimal, BaseSumDistinct4Decimal);
