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


//! Go `pkg/executor/aggfuncs/func_varpop.go`: `VAR_POP`'s running
//! variance (Welford-style `count`/`sum`/`variance`) and its DISTINCT set,
//! which `VAR_SAMP`, `STDDEV_POP` and `STDDEV_SAMP` reuse with their own
//! final results (`func_varsamp.rs`, `func_stddevpop.rs`,
//! `func_stddevsamp.rs`).

use tidb_chunk::chunk::Chunk;
use tidb_chunk::row::Row;
use tidb_driver_error::ExecError;
use tidb_set::Float64SetWithMemoryUsage;

use crate::agg_func::{partial, AggFuncUpdateContext, BaseAggFunc, PartialResult};
use crate::func_sum::eval_real;

/// Go `DefPartialResult4VarPopFloat64Size`.
pub const DEF_PARTIAL_RESULT_4_VAR_POP_FLOAT64_SIZE: i64 = 24;
/// Go `DefPartialResult4VarPopDistinctFloat64Size`.
pub const DEF_PARTIAL_RESULT_4_VAR_POP_DISTINCT_FLOAT64_SIZE: i64 = 32;

/// Go `baseVarPopAggFunc`.
#[derive(Clone, Debug)]
pub struct BaseVarPopAggFunc {
    /// Go's embedded `baseAggFunc`.
    pub base: BaseAggFunc,
}

/// Go `partialResult4VarPopFloat64`.
#[derive(Clone, Copy, Debug, Default)]
pub struct PartialResult4VarPopFloat64 {
    pub(crate) count: i64,
    pub(crate) sum: f64,
    pub(crate) variance: f64,
}

/// Go `partialResult4VarPopDistinctFloat64`.
pub struct PartialResult4VarPopDistinctFloat64 {
    val_set: Float64SetWithMemoryUsage,
}

/// Go `calculateIntermediate`.
fn calculate_intermediate(count: i64, sum: f64, input: f64, mut variance: f64) -> f64 {
    let t = count as f64 * input - sum;
    variance += (t * t) / ((count * (count - 1)) as f64);
    variance
}

/// Go `calculateMerge`.
fn calculate_merge(src_count: i64, dst_count: i64, src_sum: f64, dst_sum: f64, src_variance: f64, mut dst_variance: f64) -> f64 {
    let src_count_float64 = src_count as f64;
    let dst_count_float64 = dst_count as f64;
    let t = (src_count_float64 / dst_count_float64) * dst_sum - src_sum;
    dst_variance += src_variance + ((dst_count_float64 / src_count_float64) / (dst_count_float64 + src_count_float64)) * t * t;
    dst_variance
}

/// Go `calculateDistinctFloat64Variance`: the set's size and its summed
/// squared deviation from the mean.
fn calculate_distinct_float64_variance(p: &PartialResult4VarPopDistinctFloat64) -> (i64, f64) {
    let count = p.val_set.len() as i64;
    if count == 0 {
        return (0, 0.0);
    }
    let sum: f64 = p.val_set.iter().sum();
    let mean = sum / count as f64;
    let variance = p.val_set.iter().map(|val| (val - mean) * (val - mean)).sum();
    (count, variance)
}

/// The final result each Go type computes from the row count and the summed
/// squared deviation: `variance / count` for a population, `/ (count-1)` for
/// a sample, and the square root of that for a standard deviation.
#[derive(Clone, Copy, Debug)]
pub(crate) struct VarianceFinal {
    /// A sample (`VAR_SAMP`, `STDDEV_SAMP`): NULL below two rows.
    pub(crate) sample: bool,
    /// A standard deviation: `math.Sqrt` of the variance.
    pub(crate) sqrt: bool,
}

impl VarianceFinal {
    fn append(self, count: i64, variance: f64, ordinal: usize, chk: &mut Chunk) {
        let divisor = if self.sample { count - 1 } else { count };
        if divisor <= 0 {
            chk.append_null(ordinal);
            return;
        }
        let variance = variance / divisor as f64;
        chk.append_float64(ordinal, if self.sqrt { variance.sqrt() } else { variance });
    }
}

impl BaseVarPopAggFunc {
    fn update_partial_result(&self, sctx: &AggFuncUpdateContext, rows_in_group: &[Row<'_>], pr: &mut PartialResult) -> Result<i64, ExecError> {
        let p = partial::<PartialResult4VarPopFloat64>(pr);
        for row in rows_in_group {
            let Some(input) = eval_real(&self.base, sctx, *row)? else {
                continue;
            };
            p.count += 1;
            p.sum += input;
            if p.count > 1 {
                p.variance = calculate_intermediate(p.count, p.sum, input, p.variance);
            }
        }
        Ok(0)
    }

    fn merge_partial_result(src: &mut PartialResult, dst: &mut PartialResult) -> i64 {
        let p1 = *partial::<PartialResult4VarPopFloat64>(src);
        let p2 = partial::<PartialResult4VarPopFloat64>(dst);
        if p1.count == 0 {
            return 0;
        }
        if p2.count == 0 {
            *p2 = p1;
            return 0;
        }
        p2.variance = calculate_merge(p1.count, p2.count, p1.sum, p2.sum, p1.variance, p2.variance);
        p2.count += p1.count;
        p2.sum += p1.sum;
        0
    }

    fn update_distinct(&self, sctx: &AggFuncUpdateContext, rows_in_group: &[Row<'_>], pr: &mut PartialResult) -> Result<i64, ExecError> {
        let p = partial::<PartialResult4VarPopDistinctFloat64>(pr);
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

    /// Go `mergePartialResult4VarPopDistinctFloat64`.
    fn merge_distinct(src: &mut PartialResult, dst: &mut PartialResult) -> i64 {
        let source: Vec<f64> = partial::<PartialResult4VarPopDistinctFloat64>(src).val_set.iter().collect();
        let d = partial::<PartialResult4VarPopDistinctFloat64>(dst);
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

/// Go `varPop4Float64` and the types embedding it.
macro_rules! var_pop {
    ($name:ident, $final:expr) => {
        #[doc = concat!("Go `", stringify!($name), "`.")]
        #[derive(Clone, Debug)]
        pub struct $name(pub $crate::func_varpop::BaseVarPopAggFunc);

        impl $crate::agg_func::AggFunc for $name {
            fn alloc_partial_result(&self) -> ($crate::agg_func::PartialResult, i64) {
                $crate::func_varpop::alloc_var_pop()
            }

            fn reset_partial_result(&self, pr: &mut $crate::agg_func::PartialResult) {
                $crate::func_varpop::reset_var_pop(pr);
            }

            fn append_final_result_2_chunk(&self, _sctx: &$crate::agg_func::AggFuncUpdateContext, pr: &mut $crate::agg_func::PartialResult, chk: &mut tidb_chunk::chunk::Chunk) -> Result<(), tidb_driver_error::ExecError> {
                $crate::func_varpop::append_var_pop($final, &self.0, pr, chk);
                Ok(())
            }

            fn update_partial_result(&self, sctx: &$crate::agg_func::AggFuncUpdateContext, rows_in_group: &[tidb_chunk::row::Row<'_>], pr: &mut $crate::agg_func::PartialResult) -> Result<i64, tidb_driver_error::ExecError> {
                $crate::func_varpop::update_var_pop(&self.0, sctx, rows_in_group, pr)
            }

            fn merge_partial_result(&self, _sctx: &$crate::agg_func::AggFuncUpdateContext, src: &mut $crate::agg_func::PartialResult, dst: &mut $crate::agg_func::PartialResult) -> Result<i64, tidb_driver_error::ExecError> {
                Ok($crate::func_varpop::merge_var_pop(src, dst))
            }
        }
    };
}
pub(crate) use var_pop;

/// Go `varPopOriginal4DistinctFloat64` / `varPopPartial4DistinctFloat64`
/// and the types embedding them: only the partial one merges.
macro_rules! var_pop_distinct {
    ($name:ident, $final:expr, merges: $merges:literal) => {
        #[doc = concat!("Go `", stringify!($name), "`.")]
        #[derive(Clone, Debug)]
        pub struct $name(pub $crate::func_varpop::BaseVarPopAggFunc);

        impl $crate::agg_func::AggFunc for $name {
            fn alloc_partial_result(&self) -> ($crate::agg_func::PartialResult, i64) {
                $crate::func_varpop::alloc_var_pop_distinct()
            }

            fn reset_partial_result(&self, pr: &mut $crate::agg_func::PartialResult) {
                $crate::func_varpop::reset_var_pop_distinct(pr);
            }

            fn append_final_result_2_chunk(&self, _sctx: &$crate::agg_func::AggFuncUpdateContext, pr: &mut $crate::agg_func::PartialResult, chk: &mut tidb_chunk::chunk::Chunk) -> Result<(), tidb_driver_error::ExecError> {
                $crate::func_varpop::append_var_pop_distinct($final, &self.0, pr, chk);
                Ok(())
            }

            fn update_partial_result(&self, sctx: &$crate::agg_func::AggFuncUpdateContext, rows_in_group: &[tidb_chunk::row::Row<'_>], pr: &mut $crate::agg_func::PartialResult) -> Result<i64, tidb_driver_error::ExecError> {
                $crate::func_varpop::update_var_pop_distinct(&self.0, sctx, rows_in_group, pr)
            }

            fn merge_partial_result(&self, _sctx: &$crate::agg_func::AggFuncUpdateContext, src: &mut $crate::agg_func::PartialResult, dst: &mut $crate::agg_func::PartialResult) -> Result<i64, tidb_driver_error::ExecError> {
                // Go's `baseAggFunc.MergePartialResult` for an original one.
                assert!($merges, "Not implemented");
                Ok($crate::func_varpop::merge_var_pop_distinct(src, dst))
            }
        }
    };
}
pub(crate) use var_pop_distinct;

pub(crate) fn alloc_var_pop() -> (PartialResult, i64) {
    (Box::new(PartialResult4VarPopFloat64::default()), DEF_PARTIAL_RESULT_4_VAR_POP_FLOAT64_SIZE)
}

pub(crate) fn reset_var_pop(pr: &mut PartialResult) {
    *partial::<PartialResult4VarPopFloat64>(pr) = PartialResult4VarPopFloat64::default();
}

pub(crate) fn append_var_pop(final_result: VarianceFinal, e: &BaseVarPopAggFunc, pr: &mut PartialResult, chk: &mut Chunk) {
    let p = *partial::<PartialResult4VarPopFloat64>(pr);
    final_result.append(p.count, p.variance, e.base.ordinal, chk);
}

pub(crate) fn update_var_pop(e: &BaseVarPopAggFunc, sctx: &AggFuncUpdateContext, rows_in_group: &[Row<'_>], pr: &mut PartialResult) -> Result<i64, ExecError> {
    e.update_partial_result(sctx, rows_in_group, pr)
}

pub(crate) fn merge_var_pop(src: &mut PartialResult, dst: &mut PartialResult) -> i64 {
    BaseVarPopAggFunc::merge_partial_result(src, dst)
}

pub(crate) fn alloc_var_pop_distinct() -> (PartialResult, i64) {
    let (val_set, set_size) = Float64SetWithMemoryUsage::new(std::iter::empty());
    (Box::new(PartialResult4VarPopDistinctFloat64 { val_set }), DEF_PARTIAL_RESULT_4_VAR_POP_DISTINCT_FLOAT64_SIZE + set_size)
}

pub(crate) fn reset_var_pop_distinct(pr: &mut PartialResult) {
    partial::<PartialResult4VarPopDistinctFloat64>(pr).val_set = Float64SetWithMemoryUsage::new(std::iter::empty()).0;
}

pub(crate) fn append_var_pop_distinct(final_result: VarianceFinal, e: &BaseVarPopAggFunc, pr: &mut PartialResult, chk: &mut Chunk) {
    let (count, variance) = calculate_distinct_float64_variance(partial::<PartialResult4VarPopDistinctFloat64>(pr));
    final_result.append(count, variance, e.base.ordinal, chk);
}

pub(crate) fn update_var_pop_distinct(e: &BaseVarPopAggFunc, sctx: &AggFuncUpdateContext, rows_in_group: &[Row<'_>], pr: &mut PartialResult) -> Result<i64, ExecError> {
    e.update_distinct(sctx, rows_in_group, pr)
}

pub(crate) fn merge_var_pop_distinct(src: &mut PartialResult, dst: &mut PartialResult) -> i64 {
    BaseVarPopAggFunc::merge_distinct(src, dst)
}

/// `VAR_POP`'s final result.
pub(crate) const VAR_POP: VarianceFinal = VarianceFinal { sample: false, sqrt: false };

var_pop!(VarPop4Float64, VAR_POP);
var_pop_distinct!(VarPopOriginal4DistinctFloat64, VAR_POP, merges: false);
var_pop_distinct!(VarPopPartial4DistinctFloat64, VAR_POP, merges: true);
