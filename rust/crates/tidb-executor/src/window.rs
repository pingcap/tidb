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

//! Go `pkg/executor/windows/window.go`: physical window execution.
//!
//! The child supplies `PARTITION BY ++ ORDER BY` order. As in Go, result
//! chunks retain the child chunk boundaries and become available only after
//! all their partitions have been processed. Frame rows retain owned input
//! chunk ranges; passthrough output columns alias those same child chunks.
//! COUNT, BIT_XOR, SUM/AVG and the supported MIN/MAX types retain sliding
//! state, with Go's precision policy for floating-point SUM/AVG.
//!
//! RANGE bounds use the planner's typed CalcFuncs/CompareCols and Go's
//! monotonic cursors. Ranking and value functions share partition/peer state.

use std::cmp::Ordering;
use std::collections::VecDeque;
use std::sync::Arc;

mod pipelined;
mod rows;
pub use pipelined::{OrderedWindowExec, PipelinedWindowExec};
pub(crate) use rows::FrameRows;
use rows::WindowRows;

use tidb_chunk::chunk::Chunk;
use tidb_chunk::row::Row;
use tidb_datatype::{Datum, FieldType};
use tidb_expr::Columns;
use tidb_expr::expression::Expression;

use crate::executor::{ExecError, Executor, ExecutorMeta};
use crate::hash_agg::{AggFunc, WindowAggState};
use crate::vec_group_checker::VecGroupChecker;

/// One window function's runtime form.
pub struct WindowFuncSpec {
    /// The concrete aggregate or partition-position function.
    pub func: WindowFunction,
    /// The output column's type (Go `WindowFuncDesc.RetType`).
    pub output_type: FieldType,
}

/// Go aggfuncs' partition-position functions; ordinary aggregates retain
/// the existing frame evaluator.
pub enum WindowFunction {
    /// Aggregate over the frame.
    Aggregate(AggFunc),
    /// One-based position.
    RowNumber,
    /// Peer rank, optionally without gaps.
    Rank { dense: bool },
    /// Relative rank in the partition.
    PercentRank,
    /// Fraction through the last peer.
    CumeDist,
    /// Bucket count (NULL propagates).
    Ntile(Option<u64>),
    /// FIRST/LAST/NTH_VALUE select a row from the current frame.
    Value {
        arg: Expression,
        nth: Option<u64>,
        last: bool,
    },
    /// LEAD/LAG select from the partition, regardless of the frame.
    Relative {
        arg: Expression,
        offset: u64,
        default: Option<Expression>,
        lead: bool,
    },
}

/// One ROWS frame bound.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WindowBound {
    /// Go `ast.CurrentRow`.
    CurrentRow,
    /// Go `FrameBound.UnBounded`.
    Unbounded,
    /// Go `FrameBound.Num`, with its direction.
    Offset {
        /// Go `FrameBound.Num`.
        num: u64,
        /// Go `FrameBound.Type == ast.Preceding`.
        preceding: bool,
    },
}

/// One ROWS frame.
#[derive(Clone, Debug)]
pub struct WindowFrameSpec {
    /// Go `WindowFrame.Start`.
    pub start: WindowBound,
    /// Go `WindowFrame.End`.
    pub end: WindowBound,
    /// Go's RANGE comparison expressions; absent for ROWS.
    pub range: Option<tidb_planner::logical::window::WindowFrame>,
    /// Go rangeFrameWindowProcessor.expectedCmpResult.
    pub range_desc: bool,
}

/// Go `pkg/executor/windows/window.go::WindowExec`, the normal window executor.
pub struct WindowExec<C: Columns> {
    meta: ExecutorMeta,
    funcs: Vec<WindowFuncSpec>,
    partition_by: Vec<Expression>,
    order_by: Vec<Expression>,
    frame: WindowFrameSpec,
    partition_frame: bool,
    range_start: usize,
    range_end: usize,
    child: Box<dyn Executor>,
    ctx: C,
    /// Retained input ranges for the current partition, in child order.
    rows: WindowRows,
    /// Number of passthrough output columns; window results follow them.
    child_width: usize,
    child_result: Arc<Chunk>,
    group_checker: VecGroupChecker,
    results: VecDeque<WindowResult>,
    executed: bool,
}

struct WindowResult {
    /// Exclusive input-row count used by pipelined alias-release checks.
    accumulated: usize,
    chunk: Chunk,
    remaining: usize,
}

impl<C: Columns> WindowExec<C> {
    /// Builds a window over `child`; `child_width` is the number of columns
    /// the child contributes to the output, using the output schema's indexes.
    /// `None` retains Go's frame-less aggregate processor; an explicit frame
    /// uses the row/range processor even when both bounds are unbounded.
    #[must_use]
    pub fn new(
        meta: ExecutorMeta,
        funcs: Vec<WindowFuncSpec>,
        partition_by: Vec<Expression>,
        order_by: Vec<Expression>,
        frame: impl Into<Option<WindowFrameSpec>>,
        child: Box<dyn Executor>,
        ctx: C,
        child_width: usize,
    ) -> Self {
        let child_result = Arc::new(child.new_chunk());
        let frame = frame.into();
        let partition_frame = frame.is_none();
        let frame = frame.unwrap_or(WindowFrameSpec {
            start: WindowBound::Unbounded,
            end: WindowBound::Unbounded,
            range: None,
            range_desc: false,
        });
        Self {
            meta,
            funcs,
            group_checker: VecGroupChecker::new(partition_by.clone()),
            partition_by,
            order_by,
            frame,
            partition_frame,
            range_start: 0,
            range_end: 0,
            child,
            ctx,
            rows: WindowRows::default(),
            child_width,
            child_result,
            results: VecDeque::new(),
            executed: false,
        }
    }

    /// Go fetchChild/copyChk: keep child chunks alive through their output
    /// column aliases. Never reset a chunk whose columns have been queued.
    fn fetch_child(&mut self) -> Result<bool, ExecError> {
        let mut child = self.child.new_chunk();
        self.child.next(&mut child)?;
        if child.num_rows() == 0 {
            return Ok(false);
        }
        let mut result = Chunk::new_with_capacity(self.meta.ret_field_types(), child.num_rows());
        for (destination, column) in self.meta.schema().columns[..self.child_width]
            .iter()
            .enumerate()
        {
            let source = usize::try_from(column.index)
                .ok()
                .filter(|index| *index < child.num_cols())
                .ok_or_else(|| {
                    ExecError::internal("window output column is outside the child schema")
                })?;
            result
                .make_ref_to(destination, &mut child, source)
                .map_err(ExecError::internal)?;
        }
        self.results.push_back(WindowResult {
            accumulated: 0,
            remaining: child.num_rows(),
            chunk: result,
        });
        self.child_result = Arc::new(child);
        Ok(true)
    }

    /// Go consumeOneGroup: collect complete groups from each child chunk.
    /// Cross-chunk continuation uses encoded boundary keys, while groups
    /// inside a chunk use the checker's typed adjacent-row comparison.
    fn consume_group(&mut self) -> Result<(), ExecError> {
        self.rows.reset();
        self.range_start = 0;
        self.range_end = 0;
        if self.group_checker.is_exhausted() {
            if !self.fetch_child()? {
                self.executed = true;
                return Ok(());
            }
            self.group_checker
                .split_into_groups(&self.ctx, &self.child_result)?;
        }
        let (begin, mut end) = self.group_checker.get_next_group();
        self.rows.push_range(&self.child_result, begin..end);
        while end == self.child_result.num_rows() {
            if !self.fetch_child()? {
                self.executed = true;
                break;
            }
            if !self
                .group_checker
                .split_into_groups(&self.ctx, &self.child_result)?
            {
                break;
            }
            let (begin, next_end) = self.group_checker.get_next_group();
            end = next_end;
            self.rows.push_range(&self.child_result, begin..end);
        }
        let end = self.rows.num_rows();
        self.append_partition_results()?;
        // Release frame aliases before the caller can reset an output chunk.
        self.rows.discard_before(end);
        Ok(())
    }

    fn same_row_keys(
        &self,
        keys: &[Expression],
        left: Row<'_>,
        right: Row<'_>,
    ) -> Result<bool, ExecError> {
        for expression in keys {
            let left_value = expression.eval(&self.ctx, left)?;
            let right_value = expression.eval(&self.ctx, right)?;
            if tidb_expr::compare_datums_with_collation(
                &left_value,
                &right_value,
                tidb_expr::collation_derive::collation_of_node(expression),
            )? != Ordering::Equal
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Go `getStartOffset`/`getEndOffset` for the ROWS frame, clamped to the
    /// partition. The end bound is exclusive.
    fn frame_range(
        &mut self,
        index: usize,
        start: usize,
        end: usize,
        pipelined: bool,
    ) -> Result<(usize, usize), ExecError> {
        if let Some(frame) = &self.frame.range {
            self.range_start =
                self.range_bound(frame.start.as_ref(), index, self.range_start, end, false)?;
            self.range_end =
                self.range_bound(frame.end.as_ref(), index, self.range_end, end, true)?;
            return Ok((self.range_start, self.range_end));
        }
        // Go stores ROWS offsets and row cursors as uint64. Its ordinary
        // processor clamps FOLLOWING before adding the exclusive-end one;
        // the pipelined processor adds first and clamps in produce().
        let index = index as u64;
        let start_bound = match self.frame.start {
            WindowBound::CurrentRow => index,
            WindowBound::Unbounded => start as u64,
            WindowBound::Offset {
                num,
                preceding: true,
            } => index.saturating_sub(num),
            WindowBound::Offset {
                num,
                preceding: false,
            } => index.wrapping_add(num),
        }
        .max(start as u64)
        .min(end as u64) as usize;
        let end_bound = match self.frame.end {
            WindowBound::CurrentRow => index.wrapping_add(1),
            WindowBound::Unbounded => end as u64,
            WindowBound::Offset {
                num,
                preceding: true,
            } => index
                .checked_sub(num)
                .map_or(0, |offset| offset.wrapping_add(1)),
            WindowBound::Offset {
                num,
                preceding: false,
            } => {
                let offset = index.wrapping_add(num);
                if !pipelined && offset >= end as u64 {
                    end as u64
                } else {
                    offset.wrapping_add(1)
                }
            }
        }
        .max(start as u64)
        .min(end as u64) as usize;
        // Keep reversed bounds: besides identifying an empty frame, its
        // original end participates in pipelined input-retention accounting.
        Ok((start_bound, end_bound))
    }

    /// Go rangeFrameWindowProcessor advances both offsets monotonically.
    /// CompareCols reads the candidate row; CalcFuncs reads the current row.
    fn range_bound(
        &self,
        bound: Option<&tidb_planner::logical::window::FrameBound>,
        current: usize,
        mut cursor: usize,
        end: usize,
        is_end: bool,
    ) -> Result<usize, ExecError> {
        use tidb_planner::logical::window::BoundType;
        let Some(bound) = bound.filter(|bound| !bound.unbounded) else {
            return Ok(if is_end { end } else { 0 });
        };
        // CURRENT ROW bounds discover peers with monotonic cursors. This also
        // works during pipelined lookahead before the partition is complete.
        let (calculations, comparisons) = if bound.bound_type == BoundType::CurrentRow {
            (&self.order_by, &self.order_by)
        } else {
            (&bound.calc_funcs, &bound.compare_cols)
        };
        if calculations.len() != self.order_by.len() || comparisons.len() != calculations.len() {
            return Err(ExecError::internal(
                "RANGE frame comparison expressions are incomplete",
            ));
        }
        while cursor < end {
            let mut order = Ordering::Equal;
            for (expr, calculation) in comparisons.iter().zip(calculations) {
                // Go evaluates the comparison operands for each candidate,
                // left to right, and stops at the first unequal ordering key.
                // An exhausted cursor must not evaluate an unused boundary:
                // doing so can raise an overflow absent from Go's execution.
                let (value, target) = if is_end {
                    let target = calculation.eval(&self.ctx, self.rows.get_row(current))?;
                    (expr.eval(&self.ctx, self.rows.get_row(cursor))?, target)
                } else {
                    let value = expr.eval(&self.ctx, self.rows.get_row(cursor))?;
                    (
                        value,
                        calculation.eval(&self.ctx, self.rows.get_row(current))?,
                    )
                };
                order = tidb_expr::compare_datums_with_collation(
                    &value,
                    &target,
                    tidb_expr::collation_derive::collation_of_node(expr),
                )?;
                if self.frame.range_desc {
                    order = order.reverse();
                }
                if order != Ordering::Equal {
                    break;
                }
            }
            if if is_end {
                order == Ordering::Greater
            } else {
                order != Ordering::Less
            } {
                break;
            }
            cursor += 1;
        }
        Ok(cursor)
    }

    fn append_partition_results(&mut self) -> Result<(), ExecError> {
        let mut result_index = 0;
        let mut aggregates: Vec<_> = self
            .funcs
            .iter()
            .map(|spec| match &spec.func {
                WindowFunction::Aggregate(func) => Some(WindowAggState::new(func)),
                _ => None,
            })
            .collect();
        let partition_start = 0;
        let partition_end = self.rows.num_rows();
        let mut partition_values: Vec<Option<Datum>> = vec![None; self.funcs.len()];
        let mut rankings = vec![0; self.funcs.len()];
        if self.partition_frame && partition_end != 0 {
            // Go aggWindowProcessor consumes all functions before it appends
            // any result. LEAD/LAG only retain rows during this phase.
            for (position, spec) in self.funcs.iter().enumerate() {
                match &spec.func {
                    WindowFunction::Aggregate(func) => aggregates[position]
                        .as_mut()
                        .expect("aggregate partial result")
                        .update_frame(
                            func,
                            &self.ctx,
                            &self.rows,
                            0,
                            partition_end,
                            &spec.output_type,
                        )?,
                    WindowFunction::Value { .. } => {
                        partition_values[position] = Some(self.window_value(
                            spec,
                            None,
                            &mut rankings[position],
                            0,
                            partition_end,
                            0,
                            partition_end,
                        )?);
                    }
                    _ => {}
                }
            }
        }
        for index in 0..partition_end {
            while self.results[result_index].remaining == 0 {
                result_index += 1;
                // Framed processors restart their sliding state per output
                // chunk. aggWindowProcessor retains the partition's results.
                if !self.partition_frame {
                    for (spec, state) in self.funcs.iter().zip(&mut aggregates) {
                        if let (WindowFunction::Aggregate(func), Some(state)) = (&spec.func, state)
                        {
                            state.reset(func);
                        }
                    }
                }
            }
            let (frame_start, frame_end) =
                self.frame_range(index, partition_start, partition_end, false)?;
            for (position, spec) in self.funcs.iter().enumerate() {
                let value = if self.partition_frame {
                    if let WindowFunction::Aggregate(func) = &spec.func {
                        aggregates[position]
                            .as_mut()
                            .expect("aggregate partial result")
                            .finish(func, &self.ctx, &spec.output_type)?
                    } else if let Some(value) = &partition_values[position] {
                        value.clone()
                    } else {
                        self.window_value(
                            spec,
                            None,
                            &mut rankings[position],
                            index,
                            partition_end,
                            frame_start,
                            frame_end,
                        )?
                    }
                } else {
                    self.window_value(
                        spec,
                        aggregates[position].as_mut(),
                        &mut rankings[position],
                        index,
                        partition_end,
                        frame_start,
                        frame_end,
                    )?
                };
                self.results[result_index]
                    .chunk
                    .append_datum(self.child_width + position, &value);
            }
            self.results[result_index].remaining -= 1;
        }
        Ok(())
    }
    fn window_value(
        &self,
        spec: &WindowFuncSpec,
        aggregate: Option<&mut WindowAggState>,
        ranking: &mut usize,
        index: usize,
        partition_end: usize,
        frame_start: usize,
        frame_end: usize,
    ) -> Result<Datum, ExecError> {
        let partition_start = 0;
        let count = partition_end - partition_start;
        let value = match &spec.func {
            WindowFunction::Aggregate(func) => aggregate.expect("aggregate partial result").value(
                func,
                &self.ctx,
                &self.rows,
                frame_start,
                frame_end,
                &spec.output_type,
            )?,
            WindowFunction::RowNumber => Datum::Int((index - partition_start + 1) as i64),
            WindowFunction::Rank { .. } | WindowFunction::PercentRank => {
                // Go compares adjacent rows when appending each result, after
                // the first row, with independent state for every function.
                if index == 0 {
                    *ranking = 1;
                } else if !self.same_row_keys(
                    &self.order_by,
                    self.rows.get_row(index - 1),
                    self.rows.get_row(index),
                )? {
                    if matches!(spec.func, WindowFunction::Rank { dense: true }) {
                        *ranking += 1;
                    } else {
                        *ranking = index + 1;
                    }
                }
                if matches!(spec.func, WindowFunction::PercentRank) {
                    Datum::Real(if count <= 1 {
                        0.0
                    } else {
                        (*ranking - 1) as f64 / (count - 1) as f64
                    })
                } else {
                    Datum::Int(*ranking as i64)
                }
            }
            WindowFunction::CumeDist => {
                // Unlike RANK, Go's CUME_DIST compares the current row against
                // its forward cursor, including the initial self-comparison.
                while *ranking < partition_end
                    && self.same_row_keys(
                        &self.order_by,
                        self.rows.get_row(index),
                        self.rows.get_row(*ranking),
                    )?
                {
                    *ranking += 1;
                }
                Datum::Real(*ranking as f64 / count as f64)
            }
            WindowFunction::Value { arg, nth, last } => {
                let target = if *last {
                    frame_end.checked_sub(1)
                } else {
                    nth.and_then(|n| n.checked_sub(1))
                        .and_then(|n| usize::try_from(n).ok())
                        .and_then(|n| frame_start.checked_add(n))
                };
                match target.filter(|target| *target >= frame_start && *target < frame_end) {
                    Some(target) => arg.eval(&self.ctx, self.rows.get_row(target))?,
                    None => Datum::Null,
                }
            }
            WindowFunction::Relative {
                arg,
                offset,
                default,
                lead,
            } => {
                // Go's curIdx and offset are uint64. LEAD wraps before its
                // bounds check; LAG checks the subtraction first. Convert to
                // a native row index only after doing that source arithmetic.
                let target = if *lead {
                    Some((index as u64).wrapping_add(*offset))
                } else {
                    (index as u64).checked_sub(*offset)
                }
                .and_then(|target| usize::try_from(target).ok());
                match target.filter(|target| *target >= partition_start && *target < partition_end)
                {
                    Some(target) => arg.eval(&self.ctx, self.rows.get_row(target))?,
                    None => match default {
                        Some(default) => default.eval(&self.ctx, self.rows.get_row(index))?,
                        None => Datum::Null,
                    },
                }
            }
            WindowFunction::Ntile(None | Some(0)) => Datum::Null,
            WindowFunction::Ntile(Some(buckets)) => {
                // Go func_ntile.go: the first remainder buckets each
                // have one more row than the quotient.
                let quotient = count as u64 / buckets;
                let remainder = count as u64 % buckets;
                let position = (index - partition_start) as u64;
                let wide_rows = (quotient + 1) * remainder;
                let bucket = if position < wide_rows {
                    position / (quotient + 1) + 1
                } else {
                    remainder + (position - wide_rows) / quotient + 1
                };
                Datum::UInt(bucket)
            }
        };
        // Go's valueEvaluator reads EvalString rather than the enum/
        // set's encoded datum. Keep the label bytes in a string result.
        let value = if matches!(
            spec.func,
            WindowFunction::Value { .. } | WindowFunction::Relative { .. }
        ) && spec.output_type.eval_type() == tidb_datatype::EvalType::String
            && !value.is_null()
        {
            Datum::Bytes(
                value
                    .sql_bytes()
                    .map_err(|_| ExecError::internal("window value cannot be read as a string"))?,
            )
        } else {
            value
        };
        Ok(value)
    }
}

impl<C: Columns + Send> Executor for WindowExec<C> {
    fn open(&mut self) -> Result<(), ExecError> {
        self.executed = false;
        self.group_checker = VecGroupChecker::new(self.partition_by.clone());
        self.child_result = Arc::new(self.child.new_chunk());
        self.results.clear();
        self.rows.reset();
        self.child.open()
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        while !self.executed
            && !self
                .results
                .front()
                .is_some_and(|result| result.remaining == 0)
        {
            if let Err(error) = self.consume_group() {
                self.executed = true;
                return Err(error);
            }
        }
        if let Some(mut result) = self.results.pop_front() {
            req.swap_columns(&mut result.chunk);
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.rows.reset();
        self.results.clear();
        self.child_result = Arc::new(self.child.new_chunk());
        self.child.close()
    }

    fn schema(&self) -> &tidb_expr::schema::Schema {
        self.meta.schema()
    }

    fn ret_field_types(&self) -> &[FieldType] {
        self.meta.ret_field_types()
    }

    fn init_cap(&self) -> usize {
        self.meta.init_cap()
    }

    fn max_chunk_size(&self) -> usize {
        self.meta.max_chunk_size()
    }

    fn new_chunk(&self) -> Chunk {
        self.meta.new_chunk()
    }
}
