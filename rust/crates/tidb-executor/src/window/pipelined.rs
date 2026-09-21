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

//! Go `pkg/executor/windows/pipelined_window.go` scheduling and row retention.

use super::*;
use std::ops::Range;

struct PendingGroup {
    chunk: Arc<Chunk>,
    range: Range<usize>,
}

/// Pipelined window execution: produce completed frames while preserving the
/// input rows still needed for lookahead and inverse aggregate updates.
pub struct PipelinedWindowExec<C: Columns> {
    base: WindowExec<C>,
    aggregates: Vec<Option<WindowAggState>>,
    cached: Vec<Option<Datum>>,
    rankings: Vec<usize>,
    pending: Option<PendingGroup>,
    new_partition: bool,
    done: bool,
    whole: bool,
    empty_frame: bool,
    current: usize,
    row_count: usize,
    result_index: usize,
    accumulated: usize,
    dropped: usize,
    last_frame: Option<(usize, usize)>,
}

impl<C: Columns> PipelinedWindowExec<C> {
    /// Uses the same built functions, bounds, schema and child as normal Window.
    pub fn new(base: WindowExec<C>) -> Self {
        let aggregates = base
            .funcs
            .iter()
            .map(|spec| match &spec.func {
                WindowFunction::Aggregate(func) => Some(WindowAggState::new(func)),
                _ => None,
            })
            .collect();
        let cached = vec![None; base.funcs.len()];
        let rankings = vec![0; base.funcs.len()];
        Self {
            base,
            aggregates,
            cached,
            rankings,
            pending: None,
            new_partition: false,
            done: false,
            whole: false,
            empty_frame: false,
            current: 0,
            row_count: 0,
            result_index: 0,
            accumulated: 0,
            dropped: 0,
            last_frame: None,
        }
    }

    fn first_result_not_ready(&self) -> bool {
        (!self.done && self.base.results.is_empty())
            || self
                .base
                .results
                .front()
                .is_some_and(|result| result.remaining != 0 || result.accumulated > self.dropped)
    }

    fn enough(&mut self) -> Result<bool, ExecError> {
        if self.current >= self.row_count {
            return Ok(false);
        }
        if self.whole {
            return Ok(true);
        }
        let (start, end) = self
            .base
            .frame_range(self.current, 0, self.row_count, true)?;
        // Go deliberately needs a row beyond both bounds, even for ROWS.
        Ok(start < self.row_count && end < self.row_count)
    }

    fn get_group(&mut self) -> Result<(), ExecError> {
        self.new_partition = self.base.rows.start() != self.base.rows.num_rows();
        if self.base.group_checker.is_exhausted() {
            if !self.base.fetch_child()? {
                self.done = true;
                return Ok(());
            }
            self.accumulated += self.base.child_result.num_rows();
            self.base
                .results
                .back_mut()
                .expect("fetched result")
                .accumulated = self.accumulated;
            if self
                .base
                .group_checker
                .split_into_groups(&self.base.ctx, &self.base.child_result)?
            {
                self.new_partition = false;
            }
        }
        let (begin, end) = self.base.group_checker.get_next_group();
        self.pending = Some(PendingGroup {
            chunk: Arc::clone(&self.base.child_result),
            range: begin..end,
        });
        Ok(())
    }

    fn consume_pending(&mut self) {
        if let Some(group) = self.pending.take() {
            self.base.rows.push_range(&group.chunk, group.range);
            self.row_count = self.base.rows.num_rows();
        }
    }

    fn finish_partition(&mut self) -> Result<(), ExecError> {
        self.whole = true;
        Ok(())
    }

    fn reset_aggregates(&mut self) {
        for (spec, state) in self.base.funcs.iter().zip(&mut self.aggregates) {
            if let (WindowFunction::Aggregate(func), Some(state)) = (&spec.func, state) {
                state.reset(func);
            }
        }
        self.cached.fill(None);
    }

    fn reset_partition(&mut self) {
        self.dropped += self.base.rows.discard_before(self.row_count);
        self.base.rows.reset();
        self.rankings.fill(0);
        self.base.range_start = 0;
        self.base.range_end = 0;
        self.current = 0;
        self.row_count = 0;
        self.last_frame = None;
        self.whole = false;
        self.empty_frame = false;
        self.reset_aggregates();
    }

    fn produce(&mut self) -> Result<(), ExecError> {
        while self.base.results[self.result_index].remaining > 0 && self.enough()? {
            let (start, end) = self
                .base
                .frame_range(self.current, 0, self.row_count, true)?;
            let empty = start >= end;
            if empty && !self.empty_frame {
                self.reset_aggregates();
            }
            let changed = self.last_frame != Some((start, end)) || self.empty_frame != empty;
            for (position, spec) in self.base.funcs.iter().enumerate() {
                // Go preserves a partial result while the bounds are unchanged;
                // ranking/position functions still advance on every output row.
                let cacheable = matches!(
                    spec.func,
                    WindowFunction::Aggregate(_) | WindowFunction::Value { .. }
                );
                let value = if cacheable && !changed && self.cached[position].is_some() {
                    self.cached[position].as_ref().unwrap().clone()
                } else {
                    let retained_start = self.base.rows.start();
                    if !empty && start < retained_start {
                        // Go getRows slices using uint64 offsets and the SQL
                        // layer recovers its bounds panic. Surface the same
                        // error without indexing released Rust input.
                        return Err(ExecError::internal(format!(
                            "runtime error: slice bounds out of range [{}:{}]",
                            (start as u64).wrapping_sub(retained_start as u64),
                            (end as u64).wrapping_sub(retained_start as u64),
                        )));
                    }
                    let value = self.base.window_value(
                        spec,
                        self.aggregates[position].as_mut(),
                        &mut self.rankings[position],
                        self.current,
                        self.row_count,
                        start,
                        end,
                    )?;
                    if cacheable {
                        self.cached[position] = Some(value.clone());
                    }
                    value
                };
                self.base.results[self.result_index]
                    .chunk
                    .append_datum(self.base.child_width + position, &value);
            }
            self.current += 1;
            self.last_frame = Some((start, end));
            self.empty_frame = empty;
            self.base.results[self.result_index].remaining -= 1;
        }
        if let Some((start, end)) = self.last_frame {
            let release = self.current.min(start).min(end);
            self.dropped += self.base.rows.discard_before(release);
        }
        if self.base.results[self.result_index].remaining == 0 {
            self.result_index += 1;
        }
        Ok(())
    }
}

impl<C: Columns + Send> Executor for PipelinedWindowExec<C> {
    fn open(&mut self) -> Result<(), ExecError> {
        self.base.open()?;
        self.rankings.fill(0);
        self.pending = None;
        self.new_partition = false;
        self.done = false;
        self.whole = false;
        self.empty_frame = false;
        self.current = 0;
        self.row_count = 0;
        self.result_index = 0;
        self.accumulated = 0;
        self.dropped = 0;
        self.last_frame = None;
        self.base.range_start = 0;
        self.base.range_end = 0;
        self.reset_aggregates();
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        while self.first_result_not_ready() {
            if !self.enough()? {
                if !self.done && self.pending.is_none() {
                    self.get_group()?;
                }
                if self.done || self.new_partition {
                    self.finish_partition()?;
                    if self.enough()? {
                        continue;
                    }
                    self.new_partition = false;
                    self.reset_partition();
                    if self.pending.is_none() {
                        break;
                    }
                }
                self.consume_pending();
            }
            if self.result_index < self.base.results.len()
                && self.base.results[self.result_index].remaining > 0
            {
                self.produce()?;
            }
        }
        if let Some(mut result) = self.base.results.pop_front() {
            req.swap_columns(&mut result.chunk);
            self.result_index -= 1;
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.pending = None;
        self.reset_aggregates();
        self.base.close()
    }
    fn schema(&self) -> &tidb_expr::schema::Schema {
        self.base.schema()
    }
    fn ret_field_types(&self) -> &[FieldType] {
        self.base.ret_field_types()
    }
    fn init_cap(&self) -> usize {
        self.base.init_cap()
    }
    fn max_chunk_size(&self) -> usize {
        self.base.max_chunk_size()
    }
    fn new_chunk(&self) -> Chunk {
        self.base.new_chunk()
    }
}

/// Go `OrderedWindowExec`: an already-ordered child always uses the pipelined
/// implementation, independently of the session's ordinary Window choice.
pub struct OrderedWindowExec<C: Columns>(PipelinedWindowExec<C>);

impl<C: Columns> OrderedWindowExec<C> {
    /// Go `BuildOrdered`'s forced-pipelined construction.
    pub fn new(base: WindowExec<C>) -> Self {
        Self(PipelinedWindowExec::new(base))
    }
}

impl<C: Columns + Send> Executor for OrderedWindowExec<C> {
    fn open(&mut self) -> Result<(), ExecError> {
        self.0.open()
    }
    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        self.0.next(req)
    }
    fn close(&mut self) -> Result<(), ExecError> {
        self.0.close()
    }
    fn schema(&self) -> &tidb_expr::schema::Schema {
        self.0.schema()
    }
    fn ret_field_types(&self) -> &[FieldType] {
        self.0.ret_field_types()
    }
    fn init_cap(&self) -> usize {
        self.0.init_cap()
    }
    fn max_chunk_size(&self) -> usize {
        self.0.max_chunk_size()
    }
    fn new_chunk(&self) -> Chunk {
        self.0.new_chunk()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash_agg::{AggFunc, AggKind};
    use tidb_datatype::FieldTypeCode;
    use tidb_expr::{NoColumns, column::Column, schema::Schema};

    fn schema(width: usize) -> Schema {
        Schema::new(
            (0..width)
                .map(|i| {
                    let mut column =
                        Column::new(i as i64 + 1, FieldType::new(FieldTypeCode::LongLong));
                    column.index = i as i64;
                    column
                })
                .collect(),
        )
    }

    struct Source {
        meta: ExecutorMeta,
        position: usize,
        total: usize,
    }
    impl Executor for Source {
        fn open(&mut self) -> Result<(), ExecError> {
            self.position = 0;
            Ok(())
        }
        fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
            req.reset();
            let end = self.total.min(self.position + 8);
            for i in self.position..end {
                req.append_int64(0, i as i64);
            }
            self.position = end;
            Ok(())
        }
        fn close(&mut self) -> Result<(), ExecError> {
            Ok(())
        }
        fn schema(&self) -> &Schema {
            self.meta.schema()
        }
        fn ret_field_types(&self) -> &[FieldType] {
            self.meta.ret_field_types()
        }
        fn init_cap(&self) -> usize {
            8
        }
        fn max_chunk_size(&self) -> usize {
            8
        }
        fn new_chunk(&self) -> Chunk {
            self.meta.new_chunk()
        }
    }

    #[test]
    fn bounded_frames_release_inputs_while_partition_is_still_arriving() {
        let source = Source {
            meta: ExecutorMeta::new(schema(1), 1, 8, 8),
            position: 0,
            total: 1000,
        };
        let base = WindowExec::new(
            ExecutorMeta::new(schema(2), 2, 8, 8),
            vec![WindowFuncSpec {
                func: WindowFunction::Aggregate(AggFunc::new(
                    AggKind::Count,
                    Some(Expression::Column(schema(1).columns[0].clone())),
                )),
                output_type: FieldType::new(FieldTypeCode::LongLong),
            }],
            vec![],
            vec![],
            WindowFrameSpec {
                start: WindowBound::Offset {
                    num: 9,
                    preceding: true,
                },
                end: WindowBound::CurrentRow,
                range: None,
                range_desc: false,
            },
            Box::new(source),
            NoColumns,
            1,
        );
        let mut executor = PipelinedWindowExec::new(base);
        for _ in 0..2 {
            executor.open().unwrap();
            let mut output = executor.new_chunk();
            let mut count = 0;
            loop {
                executor.next(&mut output).unwrap();
                if output.num_rows() == 0 {
                    break;
                }
                if count == 0 {
                    assert!(executor.accumulated < 1000);
                }
                // A ten-row frame plus at most two eight-row chunks of lookahead.
                assert!(executor.base.rows.num_rows() - executor.base.rows.start() <= 26);
                assert!(executor.base.results.len() <= 4);
                for row in 0..output.num_rows() {
                    assert_eq!(
                        output
                            .get_row(row)
                            .get_datum(0, &FieldType::new(FieldTypeCode::LongLong)),
                        Datum::Int(count)
                    );
                    assert_eq!(
                        output
                            .get_row(row)
                            .get_datum(1, &FieldType::new(FieldTypeCode::LongLong)),
                        Datum::Int((count + 1).min(10))
                    );
                    count += 1;
                }
                output.reset();
            }
            assert_eq!(count, 1000);
            executor.close().unwrap();
        }
    }
}
