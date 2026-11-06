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

//! Go `pkg/executor/aggfuncs/aggfuncs.go`: the `AggFunc` interface an
//! aggregate executor drives, its partial result and the sizes the
//! functions report as `memDelta`.
//!
//! The sizes are Go's `unsafe.Sizeof` of each partial-result struct on a
//! 64-bit build, because they are what the executors charge their memory
//! trackers: a Rust layout would charge different bytes.

use std::any::Any;

use tidb_chunk::chunk::Chunk;
use tidb_chunk::row::Row;
use tidb_datatype::FieldType;
use tidb_driver_error::ExecError;
use tidb_expr::expression::Expression;
use tidb_exprctx::Columns;

use crate::spill_deserialize_helper::DeserializeHelper;
use crate::spill_serialize_helper::SerializeHelper;

/// Go `DefUint32Size`.
pub const DEF_UINT32_SIZE: i64 = 4;
/// Go `DefUint64Size`.
pub const DEF_UINT64_SIZE: i64 = 8;
/// Go `DefInt64Size`.
pub const DEF_INT64_SIZE: i64 = 8;
/// Go `DefFloat64Size`.
pub const DEF_FLOAT64_SIZE: i64 = 8;
/// Go `DefTimeSize`: `unsafe.Sizeof(types.Time{})`.
pub const DEF_TIME_SIZE: i64 = 8;
/// Go `DefRowSize`: `unsafe.Sizeof(chunk.Row{})`.
pub const DEF_ROW_SIZE: i64 = 16;
/// Go `DefBoolSize`.
pub const DEF_BOOL_SIZE: i64 = 1;
/// Go `DefInterfaceSize`.
pub const DEF_INTERFACE_SIZE: i64 = 16;
/// Go `DefMyDecimalSize`: `unsafe.Sizeof(types.MyDecimal{})`.
pub const DEF_MY_DECIMAL_SIZE: i64 = 40;
/// Go `DefDurationSize`: `unsafe.Sizeof(types.Duration{})`.
pub const DEF_DURATION_SIZE: i64 = 16;

/// Go `PartialResult`: one group's state of one aggregate function, which
/// only the function that allocated it reads (Go casts the
/// `unsafe.Pointer` back to its own struct).
pub type PartialResult = Box<dyn Any + Send>;

/// Go's cast of a `PartialResult` to the struct its function allocated.
pub(crate) fn partial<T: 'static>(pr: &mut PartialResult) -> &mut T {
    pr.downcast_mut::<T>()
        .expect("a partial result is read by the function that allocated it")
}

/// Go `AggFuncUpdateContext` (`exprctx.EvalContext`).
pub type AggFuncUpdateContext = dyn Columns;

/// Go `AggFunc`.
pub trait AggFunc: Send + Sync {
    /// Go `AllocPartialResult`: a new partial result and the bytes it holds.
    fn alloc_partial_result(&self) -> (PartialResult, i64);

    /// Go `ResetPartialResult`.
    fn reset_partial_result(&self, pr: &mut PartialResult);

    /// Go `UpdatePartialResult`: folds `rows_in_group` into `pr`, returning
    /// the bytes the partial result grew by.
    fn update_partial_result(
        &self,
        sctx: &AggFuncUpdateContext,
        rows_in_group: &[Row<'_>],
        pr: &mut PartialResult,
    ) -> Result<i64, ExecError>;

    /// Go `MergePartialResult`: folds `src` into `dst`. Go's `baseAggFunc`
    /// panics for a function that cannot merge.
    fn merge_partial_result(
        &self,
        _sctx: &AggFuncUpdateContext,
        _src: &mut PartialResult,
        _dst: &mut PartialResult,
    ) -> Result<i64, ExecError> {
        panic!("Not implemented")
    }

    /// Go `AppendFinalResult2Chunk`.
    fn append_final_result_2_chunk(
        &self,
        sctx: &AggFuncUpdateContext,
        pr: &mut PartialResult,
        chk: &mut Chunk,
    ) -> Result<(), ExecError>;

    /// Go `serializer.SerializePartialResult`.
    fn serialize_partial_result(
        &self,
        _partial_result: &mut PartialResult,
        _chk: &mut Chunk,
        _spill_helper: &mut SerializeHelper,
    ) {
        panic!("Not implemented")
    }

    /// Go `serializer.DeserializePartialResult`.
    fn deserialize_partial_result(&self, _src: &Chunk) -> (Vec<PartialResult>, i64) {
        panic!("Not implemented")
    }

    /// Go's `aggFunc.(SlidingWindowAggFunc)` assertion.
    fn as_sliding_window(&self) -> Option<&dyn SlidingWindowAggFunc> {
        None
    }
}

/// Go `baseAggFunc`.
#[derive(Clone, Debug)]
pub struct BaseAggFunc {
    /// Go `args`: the input expressions.
    pub args: Vec<Expression>,
    /// Go `ordinal`: the output column of the final result.
    pub ordinal: usize,
    /// Go `retTp`.
    pub ret_tp: FieldType,
}

/// Go `SlidingWindowAggFunc`: a window function that updates its partial
/// result as the frame slides instead of recomputing it.
pub trait SlidingWindowAggFunc {
    /// Go `Slide`: removes `shift_start` rows from `last_start` and adds
    /// `shift_end` rows from `last_end`.
    #[allow(clippy::too_many_arguments)]
    fn slide<'a>(
        &self,
        sctx: &AggFuncUpdateContext,
        get_row: &dyn Fn(u64) -> Row<'a>,
        last_start: u64,
        last_end: u64,
        shift_start: u64,
        shift_end: u64,
        pr: &mut PartialResult,
    ) -> Result<(), ExecError>;
}

/// Go `deserializePartialResultCommon`.
pub(crate) fn deserialize_partial_result_common(
    src: &Chunk,
    ordinal: usize,
    deserialize_func_impl: impl Fn(&mut DeserializeHelper<'_>) -> Option<(PartialResult, i64)>,
) -> (Vec<PartialResult>, i64) {
    let mut total_mem_delta = 0;
    let mut spill_helper = DeserializeHelper::new(src, ordinal, src.num_rows());
    let mut partial_results = Vec::with_capacity(src.num_rows());
    while let Some((pr, mem_delta)) = deserialize_func_impl(&mut spill_helper) {
        partial_results.push(pr);
        total_mem_delta += mem_delta;
    }
    if partial_results.len() != src.num_rows() {
        panic!("Fail to deserialize partial result");
    }
    (partial_results, total_mem_delta)
}
