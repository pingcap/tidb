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
//! The executor buffers the child (whose physical plan guarantees the
//! `PARTITION BY ++ ORDER BY` order), splits the buffer into contiguous
//! partitions, and for every output row recomputes its frame. Go's
//! `rowFrameWindowProcessor` slides the frame instead; recomputing is
//! equivalent and keeps one implementation for every aggregate.
//!
//! RANGE bounds use the planner's typed CalcFuncs/CompareCols and Go's
//! monotonic cursors. Ranking and value functions share partition/peer state.

use std::cmp::Ordering;

use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType};
use tidb_expr::expression::Expression;
use tidb_expr::Columns;

use crate::executor::{ExecError, Executor, ExecutorMeta};
use crate::hash_agg::AggFunc;

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

/// Go `pkg/executor/windows/window.go::WindowExec` (ROWS slice).
pub struct WindowExec<C: Columns> {
    meta: ExecutorMeta,
    funcs: Vec<WindowFuncSpec>,
    partition_by: Vec<Expression>,
    order_by: Vec<Expression>,
    /// First peer, exclusive last peer, one-based dense rank.
    peers: Vec<(usize, usize, usize)>,
    frame: WindowFrameSpec,
    range_start: usize,
    range_end: usize,
    child: Box<dyn Executor>,
    ctx: C,
    /// Every child row, in child order.
    rows: Chunk,
    /// The child's column count; the window outputs follow it.
    child_width: usize,
    /// Contiguous partition ranges, computed once the child is drained.
    partitions: Vec<(usize, usize)>,
    /// The partition of every buffered row, so emission is O(1) per row.
    partition_of: Vec<(usize, usize)>,
    fetched: bool,
    emitted: usize,
}

impl<C: Columns> WindowExec<C> {
    /// Builds a window over `child`; `child_width` is the number of columns
    /// the child contributes to the output.
    #[must_use]
    pub fn new(
        meta: ExecutorMeta,
        funcs: Vec<WindowFuncSpec>,
        partition_by: Vec<Expression>,
        order_by: Vec<Expression>,
        frame: WindowFrameSpec,
        child: Box<dyn Executor>,
        ctx: C,
        child_width: usize,
    ) -> Self {
        let types = child.ret_field_types().to_vec();
        let capacity = child.init_cap();
        Self {
            meta,
            funcs,
            partition_by,
            order_by,
            peers: Vec::new(),
            frame,
            range_start: 0,
            range_end: 0,
            child,
            ctx,
            rows: Chunk::new_with_capacity(&types, capacity),
            child_width,
            partitions: Vec::new(),
            partition_of: Vec::new(),
            fetched: false,
            emitted: 0,
        }
    }

    /// Drains the child into one buffer and computes the partition ranges.
    fn fetch(&mut self) -> Result<(), ExecError> {
        let types = self.child.ret_field_types().to_vec();
        self.rows = Chunk::new_with_capacity(&types, self.child.init_cap());
        let mut chunk = self.child.new_chunk();
        loop {
            chunk.reset();
            self.child.next(&mut chunk)?;
            if chunk.num_rows() == 0 {
                break;
            }
            for index in 0..chunk.num_rows() {
                self.rows.append_row(chunk.get_row(index));
            }
        }
        self.partitions.clear();
        self.partition_of = vec![(0, 0); self.rows.num_rows()];
        let mut start = 0;
        for index in 1..self.rows.num_rows() {
            if !self.same_partition(index - 1, index)? {
                self.partitions.push((start, index));
                start = index;
            }
        }
        if self.rows.num_rows() > 0 {
            self.partitions.push((start, self.rows.num_rows()));
        }
        for &(start, end) in &self.partitions {
            for slot in &mut self.partition_of[start..end] {
                *slot = (start, end);
            }
        }
        self.peers = vec![(0, 0, 0); self.rows.num_rows()];
        for &(start, end) in &self.partitions {
            let mut peer_start = start;
            let mut rank = 1;
            for peer_end in start + 1..=end {
                if peer_end < end && self.same_keys(&self.order_by, peer_end - 1, peer_end)? {
                    continue;
                }
                self.peers[peer_start..peer_end].fill((peer_start, peer_end, rank));
                peer_start = peer_end;
                rank += 1;
            }
        }
        self.fetched = true;
        Ok(())
    }

    /// Go's partition boundary: consecutive rows belong to one partition
    /// when every `PARTITION BY` key compares equal (NULLs equal).
    fn same_partition(&self, left: usize, right: usize) -> Result<bool, ExecError> {
        self.same_keys(&self.partition_by, left, right)
    }

    fn same_keys(&self, keys: &[Expression], left: usize, right: usize) -> Result<bool, ExecError> {
        let left_row = self.rows.get_row(left);
        let right_row = self.rows.get_row(right);
        for expression in keys {
            let left_value = expression.eval(&self.ctx, left_row)?;
            let right_value = expression.eval(&self.ctx, right_row)?;
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
    ) -> Result<(usize, usize), ExecError> {
        if let Some(frame) = &self.frame.range {
            if index == start {
                self.range_start = start;
                self.range_end = start;
            }
            self.range_start =
                self.range_bound(frame.start.as_ref(), index, self.range_start, end, false)?;
            self.range_end =
                self.range_bound(frame.end.as_ref(), index, self.range_end, end, true)?;
            return Ok((self.range_start, self.range_end.max(self.range_start)));
        }
        let start_bound = match self.frame.start {
            WindowBound::CurrentRow => index,
            WindowBound::Unbounded => start,
            WindowBound::Offset {
                num,
                preceding: true,
            } => index.saturating_sub(usize::try_from(num).unwrap_or(usize::MAX)),
            WindowBound::Offset {
                num,
                preceding: false,
            } => index.saturating_add(usize::try_from(num).unwrap_or(usize::MAX)),
        }
        .max(start)
        .min(end);
        let end_bound = match self.frame.end {
            WindowBound::CurrentRow => index.saturating_add(1),
            WindowBound::Unbounded => end,
            WindowBound::Offset {
                num,
                preceding: true,
            } => index
                .saturating_add(1)
                .saturating_sub(usize::try_from(num).unwrap_or(usize::MAX)),
            WindowBound::Offset {
                num,
                preceding: false,
            } => index
                .saturating_add(1)
                .saturating_add(usize::try_from(num).unwrap_or(usize::MAX)),
        }
        .max(start)
        .min(end);
        Ok((start_bound, end_bound.max(start_bound)))
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
            return Ok(if is_end {
                end
            } else {
                self.partition_of[current].0
            });
        };
        if bound.bound_type == BoundType::CurrentRow {
            return Ok(if is_end {
                self.peers[current].1
            } else {
                self.peers[current].0
            });
        }
        let targets = bound
            .calc_funcs
            .iter()
            .map(|expr| expr.eval(&self.ctx, self.rows.get_row(current)))
            .collect::<Result<Vec<_>, _>>()?;
        if targets.len() != self.order_by.len() || bound.compare_cols.len() != targets.len() {
            return Err(ExecError::internal(
                "RANGE frame comparison expressions are incomplete",
            ));
        }
        while cursor < end {
            let mut order = Ordering::Equal;
            for (expr, target) in bound.compare_cols.iter().zip(&targets) {
                let value = expr.eval(&self.ctx, self.rows.get_row(cursor))?;
                order = tidb_expr::compare_datums_with_collation(
                    &value,
                    target,
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
}

impl<C: Columns> Executor for WindowExec<C> {
    fn open(&mut self) -> Result<(), ExecError> {
        self.fetched = false;
        self.emitted = 0;
        self.partitions.clear();
        self.partition_of.clear();
        self.rows.reset();
        self.child.open()
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        if !self.fetched {
            self.fetch()?;
        }
        let batch = req.required_rows().min(self.meta.max_chunk_size());
        while req.num_rows() < batch && self.emitted < self.rows.num_rows() {
            let index = self.emitted;
            let (partition_start, partition_end) = self.partition_of[index];
            let (frame_start, frame_end) =
                self.frame_range(index, partition_start, partition_end)?;
            req.append_row(self.rows.get_row(index));
            for (position, spec) in self.funcs.iter().enumerate() {
                let (peer_start, peer_end, dense_rank) = self.peers[index];
                let count = partition_end - partition_start;
                let value = match &spec.func {
                    WindowFunction::Aggregate(func) => func.window_frame_value(
                        &self.ctx,
                        &self.rows,
                        frame_start,
                        frame_end,
                        &spec.output_type,
                    )?,
                    WindowFunction::RowNumber => Datum::Int((index - partition_start + 1) as i64),
                    WindowFunction::Rank { dense } => Datum::Int(if *dense {
                        dense_rank
                    } else {
                        peer_start - partition_start + 1
                    } as i64),
                    WindowFunction::PercentRank => Datum::Real(if count <= 1 {
                        0.0
                    } else {
                        (peer_start - partition_start) as f64 / (count - 1) as f64
                    }),
                    WindowFunction::CumeDist => {
                        Datum::Real((peer_end - partition_start) as f64 / count as f64)
                    }
                    WindowFunction::Value { arg, nth, last } => {
                        let target = if *last {
                            frame_end.checked_sub(1)
                        } else {
                            nth.and_then(|n| n.checked_sub(1))
                                .and_then(|n| usize::try_from(n).ok())
                                .and_then(|n| frame_start.checked_add(n))
                        };
                        match target.filter(|target| *target >= frame_start && *target < frame_end)
                        {
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
                        let target = usize::try_from(*offset).ok().and_then(|offset| {
                            if *lead {
                                index.checked_add(offset)
                            } else {
                                index.checked_sub(offset)
                            }
                        });
                        match target
                            .filter(|target| *target >= partition_start && *target < partition_end)
                        {
                            Some(target) => arg.eval(&self.ctx, self.rows.get_row(target))?,
                            None => match default {
                                Some(default) => {
                                    default.eval(&self.ctx, self.rows.get_row(index))?
                                }
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
                req.append_datum(self.child_width + position, &value);
            }
            self.emitted += 1;
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.rows.reset();
        self.partitions.clear();
        self.partition_of.clear();
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
