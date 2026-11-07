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

//! Go `pkg/executor/windows/builder.go`.

use std::collections::VecDeque;
use std::sync::Arc;

use tidb_chunk::chunk::Chunk;
use tidb_driver_error::ExecError;
use tidb_executor_aggfuncs::agg_func::AggFunc;
use tidb_executor_internal_exec::{Executor, ExecutorMeta};
use tidb_executor_internal_vecgroupchecker::VecGroupChecker;
use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
use tidb_expression_aggregation::{AggFuncDesc, WindowFuncDesc};
use tidb_exprctx::Columns;
use tidb_planner_core_operator_logicalop::window::{BoundType, FrameBound, FrameType, WindowFrame};

use crate::pipelined_window::{OrderedWindowExec, PipelinedWindowExec};
use crate::window::{
    AggWindowProcessor, RangeFrameWindowProcessor, RowFrameWindowProcessor, WindowExec, WindowProcessor,
};

/// What Go's `Build` reads of a `PhysicalWindow`, its expressions resolved
/// against the child's schema (Go `PhysicalWindow.ResolveIndices`).
pub struct WindowPlan {
    /// Go `PartitionBy`'s columns.
    pub partition_by: Vec<Expression>,
    /// Go `OrderBy`: each item's column and `Desc`.
    pub order_by: Vec<(Column, bool)>,
    /// Go `WindowFuncDescs`.
    pub window_func_descs: Vec<WindowFuncDesc>,
    /// Go `Frame`.
    pub frame: Option<WindowFrame>,
}

/// Go `BuildOrdered`: the executor for a window plan whose child already
/// provides the required partition/order property.
pub fn build_ordered<C: Columns + Send + 'static>(
    meta: ExecutorMeta,
    ctx: C,
    v: WindowPlan,
    child_exec: Box<dyn Executor>,
) -> Result<OrderedWindowExec<C>, ExecError> {
    match build_window(meta, ctx, v, child_exec, true, false)? {
        Built::Pipelined(pipelined_exec) => Ok(OrderedWindowExec(pipelined_exec)),
        Built::Window(_) => Err(ExecError::internal(
            "ordered window must be built with pipelined window executor",
        )),
    }
}

/// Go `Build`: the concrete executor for a window physical plan.
/// `enable_pipelined_window_exec` is Go's
/// `GetSessionVars().EnablePipelinedWindowExec`.
pub fn build<C: Columns + Send + 'static>(
    meta: ExecutorMeta,
    ctx: C,
    v: WindowPlan,
    child_exec: Box<dyn Executor>,
    force_pipelined: bool,
    enable_pipelined_window_exec: bool,
) -> Result<Box<dyn Executor>, ExecError> {
    Ok(match build_window(meta, ctx, v, child_exec, force_pipelined, enable_pipelined_window_exec)? {
        Built::Window(exec) => Box::new(exec),
        Built::Pipelined(exec) => Box::new(exec),
    })
}

enum Built<C: Columns> {
    Window(WindowExec<C>),
    Pipelined(PipelinedWindowExec<C>),
}

fn build_window<C: Columns + Send + 'static>(
    meta: ExecutorMeta,
    ctx: C,
    v: WindowPlan,
    child_exec: Box<dyn Executor>,
    force_pipelined: bool,
    enable_pipelined_window_exec: bool,
) -> Result<Built<C>, ExecError> {
    let group_by_items = v.partition_by;
    let order_by_cols: Vec<Column> = v.order_by.iter().map(|(col, _)| col.clone()).collect();
    let mut window_funcs: Vec<Box<dyn AggFunc>> = Vec::with_capacity(v.window_func_descs.len());
    let mut partial_results = Vec::with_capacity(v.window_func_descs.len());
    let mut result_col_idx = meta.schema().len() - v.window_func_descs.len();
    for desc in &v.window_func_descs {
        let agg_desc = AggFuncDesc::new_agg_func_desc_for_window_func(desc, false);
        let agg = tidb_executor_aggfuncs::agg_builder::build_window_functions(
            &ctx,
            &agg_desc,
            result_col_idx,
            &order_by_cols,
        )
        .ok_or_else(|| {
            ExecError::internal(format!("window function {} has no aggfuncs implementation", desc.base.name))
        })?;
        let (partial_result, _) = agg.alloc_partial_result();
        window_funcs.push(agg);
        partial_results.push(partial_result);
        result_col_idx += 1;
    }
    let num_window_funcs = v.window_func_descs.len();
    let cmp_result = if v.order_by.first().is_some_and(|(_, desc)| *desc) { 1 } else { -1 };

    if force_pipelined || enable_pipelined_window_exec {
        let (start, end, is_range_frame) = match v.frame {
            None => (unbounded(BoundType::Preceding), unbounded(BoundType::Following), false),
            Some(frame) => {
                let mut start = frame_bound(frame.start, BoundType::Preceding);
                let mut end = frame_bound(frame.end, BoundType::Following);
                let is_range_frame = frame.frame_type == FrameType::Ranges;
                if is_range_frame {
                    start.update_compare_cols(&order_by_cols)?;
                    end.update_compare_cols(&order_by_cols)?;
                }
                (start, end, is_range_frame)
            }
        };
        return Ok(Built::Pipelined(PipelinedWindowExec {
            child_result: Arc::new(child_exec.new_chunk()),
            meta,
            ctx,
            child: child_exec,
            num_window_funcs,
            window_funcs,
            partial_results,
            start,
            end,
            group_checker: VecGroupChecker::new(group_by_items),
            data: VecDeque::new(),
            data_idx: 0,
            done: false,
            accumulated: 0,
            dropped: 0,
            row_to_consume: 0,
            new_partition: false,
            cur_row_idx: 0,
            last_start_row: 0,
            last_end_row: 0,
            staged_start_row: 0,
            staged_end_row: 0,
            row_start: 0,
            order_by_cols: if is_range_frame { order_by_cols } else { Vec::new() },
            expected_cmp_result: if is_range_frame { cmp_result } else { 0 },
            rows: VecDeque::new(),
            row_cnt: 0,
            whole: false,
            is_range_frame,
            empty_frame: false,
            initialized_sliding_window: false,
        }));
    }

    let processor: Box<dyn WindowProcessor> = match v.frame {
        None => Box::new(AggWindowProcessor { window_funcs, partial_results }),
        Some(frame) if frame.frame_type == FrameType::Rows => Box::new(RowFrameWindowProcessor {
            window_funcs,
            partial_results,
            start: frame_bound(frame.start, BoundType::Preceding),
            end: frame_bound(frame.end, BoundType::Following),
            cur_row_idx: 0,
        }),
        Some(frame) => {
            let mut start = frame_bound(frame.start, BoundType::Preceding);
            let mut end = frame_bound(frame.end, BoundType::Following);
            start.update_compare_cols(&order_by_cols)?;
            end.update_compare_cols(&order_by_cols)?;
            Box::new(RangeFrameWindowProcessor {
                window_funcs,
                partial_results,
                start,
                end,
                cur_row_idx: 0,
                last_start_offset: 0,
                last_end_offset: 0,
                order_by_cols,
                expected_cmp_result: cmp_result,
            })
        }
    };
    Ok(Built::Window(WindowExec {
        child_result: Arc::new(Chunk::new(&[], 0, 0)),
        meta,
        ctx,
        child: child_exec,
        group_checker: VecGroupChecker::new(group_by_items),
        executed: false,
        result_chunks: VecDeque::new(),
        remaining_rows_in_chunk: VecDeque::new(),
        num_window_funcs,
        processor,
    }))
}

/// Go `&logicalop.FrameBound{Type: tp, UnBounded: true}`.
fn unbounded(bound_type: BoundType) -> FrameBound {
    FrameBound { bound_type, unbounded: true, ..FrameBound::default() }
}

/// Go's `*FrameBound` of a built frame, which the planner always sets; an
/// absent one is unbounded on its side.
fn frame_bound(bound: Option<FrameBound>, side: BoundType) -> FrameBound {
    bound.unwrap_or_else(|| unbounded(side))
}
