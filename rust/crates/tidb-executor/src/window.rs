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

//! Go `pkg/executor/windows/window.go`: `WindowExec` over a ROWS frame.
//!
//! The executor buffers the child (whose physical plan guarantees the
//! `PARTITION BY ++ ORDER BY` order), splits the buffer into contiguous
//! partitions, and for every output row recomputes its frame. Go's
//! `rowFrameWindowProcessor` slides the frame instead; recomputing is
//! equivalent and keeps one implementation for every aggregate.
//!
//! Narrowings, by name: RANGE and GROUPS frames are refused at build time,
//! and the pipelined/shuffle window variants are absent with their tiers.

use std::cmp::Ordering;

use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType};
use tidb_expr::expression::Expression;
use tidb_expr::Columns;

use crate::executor::{ExecError, Executor, ExecutorMeta};
use crate::hash_agg::AggFunc;

/// One window function's runtime form.
pub struct WindowFuncSpec {
    /// The aggregate folded over each frame. `None` for `row_number`, whose
    /// value is the row's 1-based position in its partition.
    pub func: Option<AggFunc>,
    /// The output column's type (Go `WindowFuncDesc.RetType`).
    pub output_type: FieldType,
    /// Whether this descriptor is `row_number()`.
    pub is_row_number: bool,
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
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WindowFrameSpec {
    /// Go `WindowFrame.Start`.
    pub start: WindowBound,
    /// Go `WindowFrame.End`.
    pub end: WindowBound,
}

/// Go `pkg/executor/windows/window.go::WindowExec` (ROWS slice).
pub struct WindowExec<C: Columns> {
    meta: ExecutorMeta,
    funcs: Vec<WindowFuncSpec>,
    partition_by: Vec<Expression>,
    frame: WindowFrameSpec,
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
            frame,
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
        self.fetched = true;
        Ok(())
    }

    /// Go's partition boundary: consecutive rows belong to one partition
    /// when every `PARTITION BY` key compares equal (NULLs equal).
    fn same_partition(&self, left: usize, right: usize) -> Result<bool, ExecError> {
        if self.partition_by.is_empty() {
            return Ok(true);
        }
        let left_row = self.rows.get_row(left);
        let right_row = self.rows.get_row(right);
        for expression in &self.partition_by {
            let left_value = expression.eval(&self.ctx, left_row)?;
            let right_value = expression.eval(&self.ctx, right_row)?;
            if tidb_expr::compare_datums(&left_value, &right_value)? != Ordering::Equal {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Go `getStartOffset`/`getEndOffset` for the ROWS frame, clamped to the
    /// partition. The end bound is exclusive.
    fn frame_range(&self, index: usize, start: usize, end: usize) -> (usize, usize) {
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
        (start_bound, end_bound.max(start_bound))
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
            let (frame_start, frame_end) = self.frame_range(index, partition_start, partition_end);
            req.append_row(self.rows.get_row(index));
            for (position, spec) in self.funcs.iter().enumerate() {
                let value = if spec.is_row_number {
                    Datum::Int(i64::try_from(index - partition_start + 1).unwrap_or(i64::MAX))
                } else {
                    spec.func
                        .as_ref()
                        .ok_or_else(|| {
                            ExecError::unsupported(
                                "a window function has neither a row_number flag nor an aggregate",
                            )
                        })?
                        .window_frame_value(
                            &self.ctx,
                            &self.rows,
                            frame_start,
                            frame_end,
                            &spec.output_type,
                        )?
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
