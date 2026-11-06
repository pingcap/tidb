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

//! Go `pkg/executor/aggfuncs/func_count_distinct.go`: `COUNT(DISTINCT ...)`.
//!
//! Each argument eval type keeps its values in Go's memory-aware set of the
//! same key kind (`Int64SetWithMemoryUsage` for integers and durations,
//! `Float64SetWithMemoryUsage` for reals, `StringSetWithMemoryUsage` for
//! decimal hash keys, collation keys and multi-argument encodings), so each
//! update reports the set's bucket growth plus the bytes of a stored string.

use tidb_chunk::chunk::Chunk;
use tidb_chunk::row::Row;
use tidb_datatype::{CollationCollator, Datum};
use tidb_driver_error::ExecError;
use tidb_goruntime::GoString;
use tidb_set::{Float64SetWithMemoryUsage, Int64SetWithMemoryUsage, StringSetWithMemoryUsage};

use crate::agg_func::{partial, AggFunc, AggFuncUpdateContext, BaseAggFunc, PartialResult};

/// Go `DefPartialResult4CountDistinctIntSize`.
pub const DEF_PARTIAL_RESULT_4_COUNT_DISTINCT_INT_SIZE: i64 = 32;
/// Go `DefPartialResult4CountDistinctRealSize`.
pub const DEF_PARTIAL_RESULT_4_COUNT_DISTINCT_REAL_SIZE: i64 = 32;
/// Go `DefPartialResult4CountDistinctDecimalSize`.
pub const DEF_PARTIAL_RESULT_4_COUNT_DISTINCT_DECIMAL_SIZE: i64 = 40;
/// Go `DefPartialResult4CountDistinctDurationSize`.
pub const DEF_PARTIAL_RESULT_4_COUNT_DISTINCT_DURATION_SIZE: i64 = 32;
/// Go `DefPartialResult4CountDistinctStringSize`.
pub const DEF_PARTIAL_RESULT_4_COUNT_DISTINCT_STRING_SIZE: i64 = 40;
/// Go `DefPartialResult4CountWithDistinctSize`.
pub const DEF_PARTIAL_RESULT_4_COUNT_WITH_DISTINCT_SIZE: i64 = 40;

/// Go `baseCountDistinct`.
#[derive(Clone, Debug)]
pub struct BaseCountDistinct {
    /// Go's embedded `baseAggFunc`.
    pub base: BaseAggFunc,
}

impl BaseCountDistinct {
    fn eval(&self, sctx: &AggFuncUpdateContext, index: usize, row: Row<'_>) -> Result<Datum, ExecError> {
        self.base.args[index].eval(sctx, row).map_err(ExecError::Eval)
    }
}

/// Go `partialResult4CountDistinctInt` (also `...Duration`, whose set holds
/// the duration's nanoseconds).
pub struct PartialResult4CountDistinctInt {
    val_set: Int64SetWithMemoryUsage,
}

/// Go `partialResult4CountDistinctReal`.
pub struct PartialResult4CountDistinctReal {
    val_set: Float64SetWithMemoryUsage,
}

/// Go `partialResult4CountDistinctDecimal` / `...String` /
/// `partialResult4CountWithDistinct`: a set of byte keys.
pub struct PartialResult4CountDistinctString {
    val_set: StringSetWithMemoryUsage,
}

/// The calls Go makes on its memory-aware sets (`Exist`, `Insert`, `M`,
/// `Count`), over each set's own key type.
trait DistinctSet {
    type Key: Clone;
    fn exist(&self, key: &Self::Key) -> bool;
    fn insert(&mut self, key: Self::Key) -> i64;
    fn keys(&self) -> Vec<Self::Key>;
    fn count(&self) -> usize;
}

impl DistinctSet for Int64SetWithMemoryUsage {
    type Key = i64;
    fn exist(&self, key: &i64) -> bool {
        self.contains(*key)
    }
    fn insert(&mut self, key: i64) -> i64 {
        Int64SetWithMemoryUsage::insert(self, key)
    }
    fn keys(&self) -> Vec<i64> {
        self.iter().copied().collect()
    }
    fn count(&self) -> usize {
        self.len()
    }
}

impl DistinctSet for Float64SetWithMemoryUsage {
    type Key = f64;
    fn exist(&self, key: &f64) -> bool {
        self.contains(*key)
    }
    fn insert(&mut self, key: f64) -> i64 {
        Float64SetWithMemoryUsage::insert(self, key)
    }
    fn keys(&self) -> Vec<f64> {
        self.iter().collect()
    }
    fn count(&self) -> usize {
        self.len()
    }
}

impl DistinctSet for StringSetWithMemoryUsage {
    type Key = GoString;
    fn exist(&self, key: &GoString) -> bool {
        self.contains(key)
    }
    fn insert(&mut self, key: GoString) -> i64 {
        StringSetWithMemoryUsage::insert(self, key)
    }
    fn keys(&self) -> Vec<GoString> {
        self.iter().cloned().collect()
    }
    fn count(&self) -> usize {
        self.len()
    }
}

fn int_set() -> (Int64SetWithMemoryUsage, i64) {
    Int64SetWithMemoryUsage::new(std::iter::empty())
}

fn real_set() -> (Float64SetWithMemoryUsage, i64) {
    Float64SetWithMemoryUsage::new(std::iter::empty())
}

fn string_set() -> (StringSetWithMemoryUsage, i64) {
    StringSetWithMemoryUsage::new(std::iter::empty())
}

/// One typed `baseCountDistinct4X` and its two wrappers.
///
/// `$key` reads one row's set key (`None` for NULL) and `$extra` is the
/// bytes Go adds beside the set's own growth for a new key.
macro_rules! count_distinct {
    (
        base: $base:ident,
        partial_result: $pr:ident,
        size: $size:expr,
        new_set: $new_set:expr,
        key: |$this:ident, $sctx:ident, $row:ident| $key:expr,
        extra: |$k:ident| $extra:expr,
        merge_extra: $merge_extra:expr,
        partial: $partial_name:ident,
        original: $original_name:ident $(,)?
    ) => {
        #[doc = concat!("Go `", stringify!($base), "`.")]
        #[derive(Clone, Debug)]
        pub struct $base(pub BaseCountDistinct);

        impl $base {
            fn alloc_partial_result(&self) -> (PartialResult, i64) {
                let (val_set, set_size) = $new_set;
                (Box::new($pr { val_set }), $size + set_size)
            }

            fn reset_partial_result(&self, pr: &mut PartialResult) {
                partial::<$pr>(pr).val_set = $new_set.0;
            }

            fn update_partial_result(
                &self,
                sctx: &AggFuncUpdateContext,
                rows_in_group: &[Row<'_>],
                pr: &mut PartialResult,
            ) -> Result<i64, ExecError> {
                let p = partial::<$pr>(pr);
                let mut mem_delta = 0;
                for row in rows_in_group {
                    let ($this, $sctx, $row) = (self, sctx, *row);
                    let Some($k) = $key? else {
                        continue;
                    };
                    if p.val_set.exist(&$k) {
                        continue;
                    }
                    let extra: i64 = $extra;
                    mem_delta += DistinctSet::insert(&mut p.val_set, $k) + extra;
                }
                Ok(mem_delta)
            }

            fn append_final_result_2_chunk(&self, pr: &mut PartialResult, chk: &mut Chunk) {
                let p = partial::<$pr>(pr);
                chk.append_int64(self.0.base.ordinal, p.val_set.count() as i64);
            }

            /// Go `countPartialWithDistinct4X.MergePartialResult`: Go adds no
            /// string bytes for a merged key.
            fn merge_partial_result(&self, src: &mut PartialResult, dst: &mut PartialResult) -> i64 {
                let source = partial::<$pr>(src).val_set.keys();
                let d = partial::<$pr>(dst);
                let mut mem_delta = 0;
                for val in source {
                    if d.val_set.exist(&val) {
                        continue;
                    }
                    let _: bool = $merge_extra;
                    mem_delta += DistinctSet::insert(&mut d.val_set, val);
                }
                mem_delta
            }
        }

        #[doc = concat!("Go `", stringify!($partial_name), "`.")]
        #[derive(Clone, Debug)]
        pub struct $partial_name(pub $base);

        #[doc = concat!("Go `", stringify!($original_name), "`.")]
        #[derive(Clone, Debug)]
        pub struct $original_name(pub $base);

        impl AggFunc for $partial_name {
            fn alloc_partial_result(&self) -> (PartialResult, i64) {
                self.0.alloc_partial_result()
            }

            fn reset_partial_result(&self, pr: &mut PartialResult) {
                self.0.reset_partial_result(pr);
            }

            fn update_partial_result(
                &self,
                sctx: &AggFuncUpdateContext,
                rows_in_group: &[Row<'_>],
                pr: &mut PartialResult,
            ) -> Result<i64, ExecError> {
                self.0.update_partial_result(sctx, rows_in_group, pr)
            }

            fn merge_partial_result(
                &self,
                _sctx: &AggFuncUpdateContext,
                src: &mut PartialResult,
                dst: &mut PartialResult,
            ) -> Result<i64, ExecError> {
                Ok(self.0.merge_partial_result(src, dst))
            }

            fn append_final_result_2_chunk(
                &self,
                _sctx: &AggFuncUpdateContext,
                pr: &mut PartialResult,
                chk: &mut Chunk,
            ) -> Result<(), ExecError> {
                self.0.append_final_result_2_chunk(pr, chk);
                Ok(())
            }
        }

        impl AggFunc for $original_name {
            fn alloc_partial_result(&self) -> (PartialResult, i64) {
                self.0.alloc_partial_result()
            }

            fn reset_partial_result(&self, pr: &mut PartialResult) {
                self.0.reset_partial_result(pr);
            }

            fn update_partial_result(
                &self,
                sctx: &AggFuncUpdateContext,
                rows_in_group: &[Row<'_>],
                pr: &mut PartialResult,
            ) -> Result<i64, ExecError> {
                self.0.update_partial_result(sctx, rows_in_group, pr)
            }

            fn append_final_result_2_chunk(
                &self,
                _sctx: &AggFuncUpdateContext,
                pr: &mut PartialResult,
                chk: &mut Chunk,
            ) -> Result<(), ExecError> {
                self.0.append_final_result_2_chunk(pr, chk);
                Ok(())
            }
        }
    };
}

fn datum_int(datum: Datum) -> Result<Option<i64>, ExecError> {
    Ok(match datum {
        Datum::Null => None,
        Datum::Int(value) => Some(value),
        // Go's `EvalInt` returns an unsigned column's bit pattern.
        Datum::UInt(value) => Some(value as i64),
        _ => return Err(ExecError::internal("COUNT(DISTINCT) over an integer read a non-integer")),
    })
}

count_distinct! {
    base: BaseCountDistinct4Int,
    partial_result: PartialResult4CountDistinctInt,
    size: DEF_PARTIAL_RESULT_4_COUNT_DISTINCT_INT_SIZE,
    new_set: int_set(),
    key: |this, sctx, row| datum_int(this.0.eval(sctx, 0, row)?),
    extra: |key| { let _ = key; 0 },
    merge_extra: true,
    partial: CountPartialWithDistinct4Int,
    original: CountOriginalWithDistinct4Int,
}

count_distinct! {
    base: BaseCountDistinct4Real,
    partial_result: PartialResult4CountDistinctReal,
    size: DEF_PARTIAL_RESULT_4_COUNT_DISTINCT_REAL_SIZE,
    new_set: real_set(),
    key: |this, sctx, row| match this.0.eval(sctx, 0, row)? {
        Datum::Null => Ok::<_, ExecError>(None),
        Datum::Real(value) | Datum::Float32(value) => Ok(Some(value)),
        _ => Err(ExecError::internal("COUNT(DISTINCT) over a real read a non-real")),
    },
    extra: |key| { let _ = key; 0 },
    merge_extra: true,
    partial: CountPartialWithDistinct4Real,
    original: CountOriginalWithDistinct4Real,
}

count_distinct! {
    base: BaseCountDistinct4Decimal,
    partial_result: PartialResult4CountDistinctString,
    size: DEF_PARTIAL_RESULT_4_COUNT_DISTINCT_DECIMAL_SIZE,
    new_set: string_set(),
    key: |this, sctx, row| match this.0.eval(sctx, 0, row)? {
        Datum::Null => Ok::<_, ExecError>(None),
        Datum::Decimal(value) => {
            let (hash, _) = value
                .to_hash_key()
                .map_err(|error| ExecError::internal(format!("{error:?}")))?;
            Ok(Some(GoString::from(hash)))
        }
        _ => Err(ExecError::internal("COUNT(DISTINCT) over a decimal read a non-decimal")),
    },
    extra: |key| key.len() as i64,
    merge_extra: true,
    partial: CountPartialWithDistinct4Decimal,
    original: CountOriginalWithDistinct4Decimal,
}

count_distinct! {
    base: BaseCountDistinct4Duration,
    partial_result: PartialResult4CountDistinctInt,
    size: DEF_PARTIAL_RESULT_4_COUNT_DISTINCT_DURATION_SIZE,
    new_set: int_set(),
    key: |this, sctx, row| match this.0.eval(sctx, 0, row)? {
        Datum::Null => Ok::<_, ExecError>(None),
        Datum::Duration(value) => Ok(Some(value.nanoseconds())),
        _ => Err(ExecError::internal("COUNT(DISTINCT) over a duration read a non-duration")),
    },
    extra: |key| { let _ = key; 0 },
    merge_extra: true,
    partial: CountPartialWithDistinct4Duration,
    original: CountOriginalWithDistinct4Duration,
}

count_distinct! {
    base: BaseCountDistinct4String,
    partial_result: PartialResult4CountDistinctString,
    size: DEF_PARTIAL_RESULT_4_COUNT_DISTINCT_STRING_SIZE,
    new_set: string_set(),
    key: |this, sctx, row| {
        // Go `collator.Key(input)` with the argument's collation.
        let collation = crate::aggfuncs::expr_collation(&this.0.base.args[0]);
        match this.0.eval(sctx, 0, row)? {
            Datum::Null => Ok::<_, ExecError>(None),
            Datum::String(text) => Ok(Some(GoString::from(collation.key(text.bytes())))),
            Datum::Bytes(bytes) => Ok(Some(GoString::from(collation.key(&bytes)))),
            // Go `EvalString` of any other string-class value: its text.
            other => Ok(Some(GoString::from(
                collation.key(&crate::aggfuncs::group_concat_bytes(&other)?),
            ))),
        }
    },
    extra: |key| key.len() as i64,
    merge_extra: true,
    partial: CountPartialWithDistinct4String,
    original: CountOriginalWithDistinct4String,
}

count_distinct! {
    base: BaseCountDistinct4MultiArgs,
    partial_result: PartialResult4CountDistinctString,
    size: DEF_PARTIAL_RESULT_4_COUNT_WITH_DISTINCT_SIZE,
    new_set: string_set(),
    key: |this, sctx, row| {
        // Go `evalAndEncode` of each argument; a NULL argument skips the row.
        let mut encoded_bytes = Vec::new();
        let mut has_null = false;
        for (index, arg) in this.0.base.args.iter().enumerate() {
            let value = this.0.eval(sctx, index, row)?;
            if value.is_null() {
                has_null = true;
                break;
            }
            let collation = crate::aggfuncs::expr_collation(arg);
            encoded_bytes.extend(crate::aggfuncs::approx_count_distinct_encode(&value, collation)?);
        }
        Ok::<_, ExecError>((!has_null).then(|| GoString::from(encoded_bytes)))
    },
    extra: |key| key.len() as i64,
    merge_extra: true,
    partial: CountPartialWithDistinct,
    original: CountOriginalWithDistinct,
}

/// Go `DefPartialResult4ApproxCountDistinctSize`.
pub const DEF_PARTIAL_RESULT_4_APPROX_COUNT_DISTINCT_SIZE: i64 = 32;

use crate::approx_count_distinct::PartialResult4ApproxCountDistinct;

/// Go `baseApproxCountDistinct`.
#[derive(Clone, Debug)]
pub struct BaseApproxCountDistinct {
    /// Go's embedded `baseAggFunc`.
    pub base: BaseAggFunc,
}

impl BaseApproxCountDistinct {
    /// Go `approxCountDistinctOriginal.UpdatePartialResult`: each row's
    /// `evalAndEncode` tuple, hashed into the sketch; a NULL argument skips
    /// the row.
    fn update_original(&self, sctx: &AggFuncUpdateContext, rows_in_group: &[Row<'_>], pr: &mut PartialResult) -> Result<i64, ExecError> {
        let p = partial::<PartialResult4ApproxCountDistinct>(pr);
        let mut mem_delta = 0;
        let collators: Vec<_> = self.base.args.iter().map(crate::aggfuncs::expr_collation).collect();
        let mut encoded_bytes = Vec::new();
        'rows: for row in rows_in_group {
            encoded_bytes.clear();
            for (i, arg) in self.base.args.iter().enumerate() {
                let value = arg.eval(sctx, *row).map_err(ExecError::Eval)?;
                if value.is_null() {
                    continue 'rows;
                }
                encoded_bytes.extend(crate::aggfuncs::approx_count_distinct_encode(&value, collators[i])?);
            }
            let old_mem_usage = p.mem_usage();
            p.insert(&encoded_bytes);
            mem_delta += p.mem_usage() - old_mem_usage;
        }
        Ok(mem_delta)
    }

    /// Go `approxCountDistinctPartial2.UpdatePartialResult`: each row is a
    /// serialized sketch to merge.
    fn update_partial2(&self, sctx: &AggFuncUpdateContext, rows_in_group: &[Row<'_>], pr: &mut PartialResult) -> Result<i64, ExecError> {
        let p = partial::<PartialResult4ApproxCountDistinct>(pr);
        let mut mem_delta = 0;
        for row in rows_in_group {
            let input = match self.base.args[0].eval(sctx, *row).map_err(ExecError::Eval)? {
                Datum::Null => continue,
                Datum::String(text) => text.into_bytes(),
                Datum::Bytes(bytes) => bytes,
                _ => return Err(ExecError::internal("APPROX_COUNT_DISTINCT read a non-string partial result")),
            };
            let old_mem_usage = p.mem_usage();
            p.read_and_merge(&input).map_err(ExecError::internal)?;
            mem_delta += p.mem_usage() - old_mem_usage;
        }
        Ok(mem_delta)
    }
}

/// The Go `approxCountDistinctX` types: which update each runs and whether
/// its final result is the estimate (`BIGINT`) or the serialized sketch.
macro_rules! approx_count_distinct {
    ($name:ident, update: $update:ident, serialized: $serialized:literal) => {
        #[doc = concat!("Go `", stringify!($name), "`.")]
        #[derive(Clone, Debug)]
        pub struct $name(pub BaseApproxCountDistinct);

        impl AggFunc for $name {
            fn alloc_partial_result(&self) -> (PartialResult, i64) {
                (Box::new(PartialResult4ApproxCountDistinct::new()), DEF_PARTIAL_RESULT_4_APPROX_COUNT_DISTINCT_SIZE)
            }

            fn reset_partial_result(&self, pr: &mut PartialResult) {
                *partial::<PartialResult4ApproxCountDistinct>(pr) = PartialResult4ApproxCountDistinct::new();
            }

            fn update_partial_result(&self, sctx: &AggFuncUpdateContext, rows_in_group: &[Row<'_>], pr: &mut PartialResult) -> Result<i64, ExecError> {
                self.0.$update(sctx, rows_in_group, pr)
            }

            fn merge_partial_result(&self, _sctx: &AggFuncUpdateContext, src: &mut PartialResult, dst: &mut PartialResult) -> Result<i64, ExecError> {
                let p1 = partial::<PartialResult4ApproxCountDistinct>(src).clone();
                partial::<PartialResult4ApproxCountDistinct>(dst).merge(&p1);
                Ok(0)
            }

            fn append_final_result_2_chunk(&self, _sctx: &AggFuncUpdateContext, pr: &mut PartialResult, chk: &mut Chunk) -> Result<(), ExecError> {
                let p = partial::<PartialResult4ApproxCountDistinct>(pr);
                if $serialized {
                    chk.append_bytes(self.0.base.ordinal, &p.serialize());
                } else {
                    chk.append_int64(self.0.base.ordinal, p.fixed_size() as i64);
                }
                Ok(())
            }
        }
    };
}

approx_count_distinct!(ApproxCountDistinctOriginal, update: update_original, serialized: false);
approx_count_distinct!(ApproxCountDistinctPartial1, update: update_original, serialized: true);
approx_count_distinct!(ApproxCountDistinctPartial2, update: update_partial2, serialized: true);
approx_count_distinct!(ApproxCountDistinctFinal, update: update_partial2, serialized: false);
