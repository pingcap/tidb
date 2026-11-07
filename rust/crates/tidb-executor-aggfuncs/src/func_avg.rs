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

//! Go `pkg/executor/aggfuncs/func_avg.go`: `AVG` over decimals and reals.
//! A partial average carries `(count, sum)` in its arguments `0` and `1`.

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
use crate::func_sum::{decimal_add, decimal_sub, eval_decimal, eval_real, my_decimal};
use crate::spill_serialize_helper::SerializeHelper;

/// Go `DefPartialResult4AvgDecimalSize`.
pub const DEF_PARTIAL_RESULT_4_AVG_DECIMAL_SIZE: i64 = 48;
/// Go `DefPartialResult4AvgDistinctDecimalSize`.
pub const DEF_PARTIAL_RESULT_4_AVG_DISTINCT_DECIMAL_SIZE: i64 = 40;
/// Go `DefPartialResult4AvgFloat64Size`.
pub const DEF_PARTIAL_RESULT_4_AVG_FLOAT64_SIZE: i64 = 16;
/// Go `DefPartialResult4AvgDistinctFloat64Size`.
pub const DEF_PARTIAL_RESULT_4_AVG_DISTINCT_FLOAT64_SIZE: i64 = 32;

/// Go `partialResult4AvgDecimal`.
#[derive(Clone, Debug)]
pub struct PartialResult4AvgDecimal {
    pub(crate) sum: Decimal,
    pub(crate) count: i64,
}

impl Default for PartialResult4AvgDecimal {
    fn default() -> Self {
        Self {
            sum: Decimal::from_int(0),
            count: 0,
        }
    }
}

/// Go `partialResult4AvgFloat64`.
#[derive(Clone, Copy, Debug, Default)]
pub struct PartialResult4AvgFloat64 {
    pub(crate) sum: f64,
    pub(crate) count: i64,
}

/// Go `partialResult4AvgDistinctDecimal`.
pub struct PartialResult4AvgDistinctDecimal {
    val_set: StringToDecimalMapWithMemoryUsage,
}

/// Go `partialResult4AvgDistinctFloat64`.
pub struct PartialResult4AvgDistinctFloat64 {
    val_set: Float64SetWithMemoryUsage,
}

/// Go `EvalInt` of a partial average's count argument.
fn eval_count(base: &BaseAggFunc, sctx: &AggFuncUpdateContext, row: Row<'_>) -> Result<Option<i64>, ExecError> {
    Ok(match base.args[0].eval(sctx, row).map_err(ExecError::Eval)? {
        Datum::Null => None,
        Datum::Int(value) => Some(value),
        Datum::UInt(value) => Some(value as i64),
        _ => return Err(ExecError::internal("a partial AVG's count is not an integer")),
    })
}

/// Go's final `DecimalDiv(sum, count, result, divPrecisionIncrement)` and
/// `Round(frac, ModeHalfUp)` to the return type's scale.
fn append_decimal_average(base: &BaseAggFunc, sctx: &AggFuncUpdateContext, sum: &Decimal, count: i64, chk: &mut Chunk) -> Result<(), ExecError> {
    // Go returns `DecimalDiv`'s error, its `ErrTruncated` included.
    let (final_result, error) = sum
        .div_mysql_with_warning(&Decimal::from_int(count), sctx.div_precision_increment())
        .ok_or_else(|| ExecError::internal("AVG divided by a zero count"))?;
    if let Some(error) = error {
        return Err(ExecError::Eval(tidb_exprctx::EvalError::Conversion(match error {
            tidb_datatype::DecimalCodecWarning::Overflow => tidb_datatype::types_errors::ERR_OVERFLOW.clone(),
            tidb_datatype::DecimalCodecWarning::Truncated => tidb_datatype::types_errors::ERR_TRUNCATED.clone(),
        })));
    }
    let mut frac = base.ret_tp.decimal();
    if frac == -1 {
        frac = MAX_DECIMAL_SCALE as i64;
    }
    let final_result = final_result.round_to_scale(frac as i32);
    chk.append_datum(base.ordinal, &Datum::Decimal(final_result));
    Ok(())
}

/// Go `baseAvgDecimal`.
#[derive(Clone, Debug)]
pub struct BaseAvgDecimal {
    /// Go's embedded `baseAggFunc`.
    pub base: BaseAggFunc,
}

impl BaseAvgDecimal {
    fn alloc(&self) -> (PartialResult, i64) {
        (Box::new(PartialResult4AvgDecimal::default()), DEF_PARTIAL_RESULT_4_AVG_DECIMAL_SIZE)
    }

    fn reset(&self, pr: &mut PartialResult) {
        *partial::<PartialResult4AvgDecimal>(pr) = PartialResult4AvgDecimal::default();
    }

    fn append_final(&self, sctx: &AggFuncUpdateContext, pr: &mut PartialResult, chk: &mut Chunk) -> Result<(), ExecError> {
        let p = partial::<PartialResult4AvgDecimal>(pr).clone();
        if p.count == 0 {
            chk.append_null(self.base.ordinal);
            return Ok(());
        }
        append_decimal_average(&self.base, sctx, &p.sum, p.count, chk)
    }

    fn serialize(&self, partial_result: &mut PartialResult, chk: &mut Chunk, spill_helper: &mut SerializeHelper) {
        let pr = partial::<PartialResult4AvgDecimal>(partial_result).clone();
        let sum = my_decimal(&pr.sum).expect("an averaged decimal has Go's MyDecimal form");
        let res_buf = spill_helper.serialize_partial_result_4_avg_decimal(&sum, pr.count);
        chk.append_bytes(self.base.ordinal, res_buf);
    }

    fn deserialize(&self, src: &Chunk) -> (Vec<PartialResult>, i64) {
        deserialize_partial_result_common(src, self.base.ordinal, |helper| {
            let (mut pr, mem_delta) = self.alloc();
            let (sum, count) = helper.deserialize_partial_result_4_avg_decimal()?;
            *partial::<PartialResult4AvgDecimal>(&mut pr) = PartialResult4AvgDecimal {
                sum: Decimal::from_my_decimal(&sum),
                count,
            };
            Some((pr, mem_delta))
        })
    }
}

/// Go `avgOriginal4Decimal`.
#[derive(Clone, Debug)]
pub struct AvgOriginal4Decimal(pub BaseAvgDecimal);

/// Go `avgPartial4Decimal`.
#[derive(Clone, Debug)]
pub struct AvgPartial4Decimal(pub BaseAvgDecimal);

/// The `AggFunc` methods a `baseAvgDecimal` wrapper shares.
macro_rules! base_avg_decimal_methods {
    () => {
        fn alloc_partial_result(&self) -> (PartialResult, i64) {
            self.0.alloc()
        }
        fn reset_partial_result(&self, pr: &mut PartialResult) {
            self.0.reset(pr)
        }
        fn append_final_result_2_chunk(&self, sctx: &AggFuncUpdateContext, pr: &mut PartialResult, chk: &mut Chunk) -> Result<(), ExecError> {
            self.0.append_final(sctx, pr, chk)
        }
        fn serialize_partial_result(&self, partial_result: &mut PartialResult, chk: &mut Chunk, spill_helper: &mut SerializeHelper) {
            self.0.serialize(partial_result, chk, spill_helper)
        }
        fn deserialize_partial_result(&self, src: &Chunk) -> (Vec<PartialResult>, i64) {
            self.0.deserialize(src)
        }
    };
}

impl AggFunc for AvgOriginal4Decimal {
    base_avg_decimal_methods!();

    fn update_partial_result(&self, sctx: &AggFuncUpdateContext, rows_in_group: &[Row<'_>], pr: &mut PartialResult) -> Result<i64, ExecError> {
        let p = partial::<PartialResult4AvgDecimal>(pr);
        for row in rows_in_group {
            let Some(input) = eval_decimal(&self.0.base, sctx, *row)? else {
                continue;
            };
            p.sum = decimal_add(&p.sum, &input);
            p.count += 1;
        }
        Ok(0)
    }

    fn as_sliding_window(&self) -> Option<&dyn SlidingWindowAggFunc> {
        Some(self)
    }
}

impl SlidingWindowAggFunc for AvgOriginal4Decimal {
    fn slide<'a>(&self, sctx: &AggFuncUpdateContext, get_row: &dyn Fn(u64) -> Result<Row<'a>, ExecError>, last_start: u64, last_end: u64, shift_start: u64, shift_end: u64, pr: &mut PartialResult) -> Result<(), ExecError> {
        let p = partial::<PartialResult4AvgDecimal>(pr);
        for i in 0..shift_end {
            let Some(input) = eval_decimal(&self.0.base, sctx, get_row(last_end.wrapping_add(i))?)? else {
                continue;
            };
            p.sum = decimal_add(&p.sum, &input);
            p.count += 1;
        }
        for i in 0..shift_start {
            let Some(input) = eval_decimal(&self.0.base, sctx, get_row(last_start.wrapping_add(i))?)? else {
                continue;
            };
            p.sum = decimal_sub(&p.sum, &input);
            p.count -= 1;
        }
        Ok(())
    }
}

impl AggFunc for AvgPartial4Decimal {
    base_avg_decimal_methods!();

    fn update_partial_result(&self, sctx: &AggFuncUpdateContext, rows_in_group: &[Row<'_>], pr: &mut PartialResult) -> Result<i64, ExecError> {
        let p = partial::<PartialResult4AvgDecimal>(pr);
        for row in rows_in_group {
            let input_sum = match self.0.base.args[1].eval(sctx, *row).map_err(ExecError::Eval)? {
                Datum::Null => continue,
                Datum::Decimal(value) => value,
                _ => return Err(ExecError::internal("a partial AVG's sum is not a decimal")),
            };
            let Some(input_count) = eval_count(&self.0.base, sctx, *row)? else {
                continue;
            };
            p.sum = decimal_add(&p.sum, &input_sum);
            p.count += input_count;
        }
        Ok(0)
    }

    fn merge_partial_result(&self, _sctx: &AggFuncUpdateContext, src: &mut PartialResult, dst: &mut PartialResult) -> Result<i64, ExecError> {
        let p1 = partial::<PartialResult4AvgDecimal>(src).clone();
        if p1.count == 0 {
            return Ok(0);
        }
        let p2 = partial::<PartialResult4AvgDecimal>(dst);
        p2.sum = decimal_add(&p1.sum, &p2.sum);
        p2.count += p1.count;
        Ok(0)
    }
}

/// Go `baseAvgDistinct4Decimal`.
#[derive(Clone, Debug)]
pub struct BaseAvgDistinct4Decimal {
    /// Go's embedded `baseAggFunc`.
    pub base: BaseAggFunc,
}

impl BaseAvgDistinct4Decimal {
    fn alloc(&self) -> (PartialResult, i64) {
        let (val_set, set_size) = StringToDecimalMapWithMemoryUsage::new();
        (Box::new(PartialResult4AvgDistinctDecimal { val_set }), DEF_PARTIAL_RESULT_4_AVG_DISTINCT_DECIMAL_SIZE + set_size)
    }

    fn update(&self, sctx: &AggFuncUpdateContext, rows_in_group: &[Row<'_>], pr: &mut PartialResult) -> Result<i64, ExecError> {
        let p = partial::<PartialResult4AvgDistinctDecimal>(pr);
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

    fn append_final(&self, sctx: &AggFuncUpdateContext, pr: &mut PartialResult, chk: &mut Chunk) -> Result<(), ExecError> {
        let p = partial::<PartialResult4AvgDistinctDecimal>(pr);
        let count = p.val_set.len() as i64;
        if count == 0 {
            chk.append_null(self.base.ordinal);
            return Ok(());
        }
        let mut sum = Decimal::from_int(0);
        for (_, val) in p.val_set.iter() {
            sum = decimal_add(&sum, &Decimal::from_my_decimal(val));
        }
        append_decimal_average(&self.base, sctx, &sum, count, chk)
    }

    fn merge(&self, src: &mut PartialResult, dst: &mut PartialResult) -> i64 {
        let source: Vec<(GoString, MyDecimal)> = partial::<PartialResult4AvgDistinctDecimal>(src)
            .val_set
            .iter()
            .map(|(key, val)| (key.clone(), (**val).clone()))
            .collect();
        let d = partial::<PartialResult4AvgDistinctDecimal>(dst);
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

/// Go `baseAvgFloat64`.
#[derive(Clone, Debug)]
pub struct BaseAvgFloat64 {
    /// Go's embedded `baseAggFunc`.
    pub base: BaseAggFunc,
}

impl BaseAvgFloat64 {
    fn alloc(&self) -> (PartialResult, i64) {
        (Box::new(PartialResult4AvgFloat64::default()), DEF_PARTIAL_RESULT_4_AVG_FLOAT64_SIZE)
    }

    fn reset(&self, pr: &mut PartialResult) {
        *partial::<PartialResult4AvgFloat64>(pr) = PartialResult4AvgFloat64::default();
    }

    fn append_final(&self, pr: &mut PartialResult, chk: &mut Chunk) {
        let p = *partial::<PartialResult4AvgFloat64>(pr);
        if p.count == 0 {
            chk.append_null(self.base.ordinal);
        } else {
            chk.append_float64(self.base.ordinal, p.sum / p.count as f64);
        }
    }

    fn update_original(&self, sctx: &AggFuncUpdateContext, rows_in_group: &[Row<'_>], pr: &mut PartialResult) -> Result<i64, ExecError> {
        let p = partial::<PartialResult4AvgFloat64>(pr);
        for row in rows_in_group {
            let Some(input) = eval_real(&self.base, sctx, *row)? else {
                continue;
            };
            p.sum += input;
            p.count += 1;
        }
        Ok(0)
    }

    fn serialize(&self, partial_result: &mut PartialResult, chk: &mut Chunk, spill_helper: &mut SerializeHelper) {
        let pr = *partial::<PartialResult4AvgFloat64>(partial_result);
        let res_buf = spill_helper.serialize_partial_result_4_avg_float64(pr.sum, pr.count);
        chk.append_bytes(self.base.ordinal, res_buf);
    }

    fn deserialize(&self, src: &Chunk) -> (Vec<PartialResult>, i64) {
        deserialize_partial_result_common(src, self.base.ordinal, |helper| {
            let (mut pr, mem_delta) = self.alloc();
            let (sum, count) = helper.deserialize_partial_result_4_avg_float64()?;
            *partial::<PartialResult4AvgFloat64>(&mut pr) = PartialResult4AvgFloat64 { sum, count };
            Some((pr, mem_delta))
        })
    }
}

/// The `AggFunc` methods a `baseAvgFloat64` wrapper shares.
macro_rules! base_avg_float64_methods {
    () => {
        fn alloc_partial_result(&self) -> (PartialResult, i64) {
            self.0.alloc()
        }
        fn reset_partial_result(&self, pr: &mut PartialResult) {
            self.0.reset(pr)
        }
        fn append_final_result_2_chunk(&self, _sctx: &AggFuncUpdateContext, pr: &mut PartialResult, chk: &mut Chunk) -> Result<(), ExecError> {
            self.0.append_final(pr, chk);
            Ok(())
        }
        fn serialize_partial_result(&self, partial_result: &mut PartialResult, chk: &mut Chunk, spill_helper: &mut SerializeHelper) {
            self.0.serialize(partial_result, chk, spill_helper)
        }
        fn deserialize_partial_result(&self, src: &Chunk) -> (Vec<PartialResult>, i64) {
            self.0.deserialize(src)
        }
    };
}

/// Go `avgOriginal4Float64HighPrecision`: the average, never slid.
#[derive(Clone, Debug)]
pub struct AvgOriginal4Float64HighPrecision(pub BaseAvgFloat64);

/// Go `avgOriginal4Float64`: the sliding-window average.
#[derive(Clone, Debug)]
pub struct AvgOriginal4Float64(pub BaseAvgFloat64);

/// Go `avgPartial4Float64`.
#[derive(Clone, Debug)]
pub struct AvgPartial4Float64(pub BaseAvgFloat64);

impl AggFunc for AvgOriginal4Float64HighPrecision {
    base_avg_float64_methods!();

    fn update_partial_result(&self, sctx: &AggFuncUpdateContext, rows_in_group: &[Row<'_>], pr: &mut PartialResult) -> Result<i64, ExecError> {
        self.0.update_original(sctx, rows_in_group, pr)
    }
}

impl AggFunc for AvgOriginal4Float64 {
    base_avg_float64_methods!();

    fn update_partial_result(&self, sctx: &AggFuncUpdateContext, rows_in_group: &[Row<'_>], pr: &mut PartialResult) -> Result<i64, ExecError> {
        self.0.update_original(sctx, rows_in_group, pr)
    }

    fn as_sliding_window(&self) -> Option<&dyn SlidingWindowAggFunc> {
        Some(self)
    }
}

impl SlidingWindowAggFunc for AvgOriginal4Float64 {
    fn slide<'a>(&self, sctx: &AggFuncUpdateContext, get_row: &dyn Fn(u64) -> Result<Row<'a>, ExecError>, last_start: u64, last_end: u64, shift_start: u64, shift_end: u64, pr: &mut PartialResult) -> Result<(), ExecError> {
        let p = partial::<PartialResult4AvgFloat64>(pr);
        for i in 0..shift_end {
            let Some(input) = eval_real(&self.0.base, sctx, get_row(last_end.wrapping_add(i))?)? else {
                continue;
            };
            p.sum += input;
            p.count += 1;
        }
        for i in 0..shift_start {
            let Some(input) = eval_real(&self.0.base, sctx, get_row(last_start.wrapping_add(i))?)? else {
                continue;
            };
            p.sum -= input;
            p.count -= 1;
        }
        Ok(())
    }
}

impl AggFunc for AvgPartial4Float64 {
    base_avg_float64_methods!();

    fn update_partial_result(&self, sctx: &AggFuncUpdateContext, rows_in_group: &[Row<'_>], pr: &mut PartialResult) -> Result<i64, ExecError> {
        let p = partial::<PartialResult4AvgFloat64>(pr);
        for row in rows_in_group {
            let input_sum = match self.0.base.args[1].eval(sctx, *row).map_err(ExecError::Eval)? {
                Datum::Null => continue,
                Datum::Real(value) | Datum::Float32(value) => value,
                _ => return Err(ExecError::internal("a partial AVG's sum is not a real")),
            };
            let Some(input_count) = eval_count(&self.0.base, sctx, *row)? else {
                continue;
            };
            p.sum += input_sum;
            p.count += input_count;
        }
        Ok(0)
    }

    fn merge_partial_result(&self, _sctx: &AggFuncUpdateContext, src: &mut PartialResult, dst: &mut PartialResult) -> Result<i64, ExecError> {
        let p1 = *partial::<PartialResult4AvgFloat64>(src);
        let p2 = partial::<PartialResult4AvgFloat64>(dst);
        p2.sum += p1.sum;
        p2.count += p1.count;
        Ok(0)
    }
}

/// Go `baseAvgDistinct4Float64`.
#[derive(Clone, Debug)]
pub struct BaseAvgDistinct4Float64 {
    /// Go's embedded `baseAggFunc`.
    pub base: BaseAggFunc,
}

impl BaseAvgDistinct4Float64 {
    fn alloc(&self) -> (PartialResult, i64) {
        let (val_set, set_size) = Float64SetWithMemoryUsage::new(std::iter::empty());
        (Box::new(PartialResult4AvgDistinctFloat64 { val_set }), DEF_PARTIAL_RESULT_4_AVG_DISTINCT_FLOAT64_SIZE + set_size)
    }

    fn update(&self, sctx: &AggFuncUpdateContext, rows_in_group: &[Row<'_>], pr: &mut PartialResult) -> Result<i64, ExecError> {
        let p = partial::<PartialResult4AvgDistinctFloat64>(pr);
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

    fn append_final(&self, _sctx: &AggFuncUpdateContext, pr: &mut PartialResult, chk: &mut Chunk) -> Result<(), ExecError> {
        let p = partial::<PartialResult4AvgDistinctFloat64>(pr);
        let count = p.val_set.len();
        if count == 0 {
            chk.append_null(self.base.ordinal);
            return Ok(());
        }
        let sum: f64 = p.val_set.iter().sum();
        chk.append_float64(self.base.ordinal, sum / count as f64);
        Ok(())
    }

    fn merge(&self, src: &mut PartialResult, dst: &mut PartialResult) -> i64 {
        let source: Vec<f64> = partial::<PartialResult4AvgDistinctFloat64>(src).val_set.iter().collect();
        let d = partial::<PartialResult4AvgDistinctFloat64>(dst);
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

/// The `AVG(DISTINCT ...)` wrappers: Go's partial one merges, its original
/// one panics on a merge.
macro_rules! avg_distinct {
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
            fn append_final_result_2_chunk(&self, sctx: &AggFuncUpdateContext, pr: &mut PartialResult, chk: &mut Chunk) -> Result<(), ExecError> {
                self.0.append_final(sctx, pr, chk)
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
            fn append_final_result_2_chunk(&self, sctx: &AggFuncUpdateContext, pr: &mut PartialResult, chk: &mut Chunk) -> Result<(), ExecError> {
                self.0.append_final(sctx, pr, chk)
            }
        }
    };
}

avg_distinct!(AvgPartial4DistinctDecimal, AvgOriginal4DistinctDecimal, BaseAvgDistinct4Decimal);
avg_distinct!(AvgPartial4DistinctFloat64, AvgOriginal4DistinctFloat64, BaseAvgDistinct4Float64);
