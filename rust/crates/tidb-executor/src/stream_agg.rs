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

//! Streaming aggregation for ordered input.
//!
//! This is the source-shaped serial arm of pkg/executor/aggregate StreamAggExec:
//! one current group survives a child-chunk boundary, is finalized before the
//! next group starts, and therefore keeps bounded aggregate state. The driver
//! must only select it after proving its child preserves the group-key order.

use std::sync::Arc;

use tidb_chunk::chunk::Chunk;
use tidb_datatype::FieldType;
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_expr::Columns;
use tidb_util::memory::Tracker;

use crate::executor::{ExecError, Executor, ExecutorMeta};
use crate::hash_agg::{eval_agg_input, expr_collation, group_key_part, AggFunc, AggState};
use crate::mem_quota::StatementMemory;

/// Ordered-input counterpart to HashAggExec.
///
/// The child must place equal group keys consecutively. This operator does not
/// sort and deliberately exposes no constructor that would imply otherwise.
pub struct StreamAggExec<C: Columns> {
    meta: ExecutorMeta,
    group_by: Vec<Expression>,
    agg_funcs: Vec<AggFunc>,
    child: Box<dyn Executor>,
    ctx: C,
    child_chunk: Chunk,
    row_cursor: usize,
    source_drained: bool,
    executed: bool,
    child_returned_empty: bool,
    default_emitted: bool,
    current_key: Option<Vec<u8>>,
    pending_key: Option<Vec<u8>>,
    states: Vec<AggState>,
    truncated: Vec<bool>,
    memory: StatementMemory,
    tracker: Arc<Tracker>,
}

impl<C: Columns> StreamAggExec<C> {
    /// Creates a streaming aggregation over a child whose order is already
    /// known to match group_by.
    #[must_use]
    pub fn new(
        meta: ExecutorMeta,
        group_by: Vec<Expression>,
        agg_funcs: Vec<AggFunc>,
        child: Box<dyn Executor>,
        ctx: C,
        memory: StatementMemory,
    ) -> Self {
        let child_chunk = child.new_chunk();
        let tracker = memory.operator_tracker(meta.id());
        let truncated = vec![false; agg_funcs.len()];
        Self {
            meta,
            group_by,
            agg_funcs,
            child,
            ctx,
            child_chunk,
            row_cursor: 0,
            source_drained: false,
            executed: false,
            child_returned_empty: true,
            default_emitted: false,
            current_key: None,
            pending_key: None,
            states: Vec::new(),
            truncated,
            memory,
            tracker,
        }
    }

    fn start_group(&mut self, key: Vec<u8>) {
        let bytes = key
            .len()
            .saturating_add(self.agg_funcs.len() * std::mem::size_of::<AggState>());
        self.tracker
            .consume(i64::try_from(bytes).unwrap_or(i64::MAX));
        self.current_key = Some(key);
        self.states = self.agg_funcs.iter().map(AggState::new).collect();
    }

    fn load_child_chunk(&mut self) -> Result<(), ExecError> {
        let before = self.child_chunk.memory_usage();
        self.child.next(&mut self.child_chunk)?;
        self.tracker
            .consume(self.child_chunk.memory_usage().saturating_sub(before));
        self.memory.check()?;
        self.row_cursor = 0;
        if self.child_chunk.num_rows() == 0 {
            self.source_drained = true;
        } else {
            self.child_returned_empty = false;
        }
        Ok(())
    }

    fn group_key(&mut self, chunk: &Chunk) -> Result<Vec<u8>, ExecError> {
        if let Some(key) = self.pending_key.take() {
            return Ok(key);
        }
        let row = chunk.get_row(self.row_cursor);
        let mut key = Vec::new();
        for expr in &self.group_by {
            let datum = expr.eval(&self.ctx, row)?;
            key.extend_from_slice(&group_key_part(&expr_collation(expr), &datum));
            key.push(0xff);
        }
        Ok(key)
    }

    /// Consumes the current input row unless it starts a new group. A new
    /// group is retained as pending work so a full output chunk never causes
    /// the group expression to be evaluated twice.
    fn consume_row(&mut self, chunk: &Chunk) -> Result<bool, ExecError> {
        let key = self.group_key(chunk)?;
        if self
            .current_key
            .as_ref()
            .is_some_and(|current| current != &key)
        {
            self.pending_key = Some(key);
            return Ok(true);
        }
        if self.current_key.is_none() {
            self.start_group(key);
        }
        let row = chunk.get_row(self.row_cursor);
        for c in 0..self.agg_funcs.len() {
            let func = &self.agg_funcs[c];
            let mut extra = Vec::new();
            let value = eval_agg_input(func, &self.ctx, row, &mut extra)?;
            let mut sort_key = Vec::with_capacity(func.order_by.len());
            for (expr, _) in &func.order_by {
                sort_key.push(expr.eval(&self.ctx, row)?);
            }
            self.tracker
                .consume(self.states[c].update(value, &extra, sort_key)?);
        }
        self.row_cursor += 1;
        self.memory.check()?;
        Ok(false)
    }

    fn emit_current_group(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        if self.current_key.is_none() {
            return Ok(());
        }
        if self.agg_funcs.is_empty() {
            req.set_num_virtual_rows(req.num_rows() + 1);
        }
        for c in 0..self.states.len() {
            let func = self.agg_funcs[c].clone();
            let value = self.states[c].finish(
                &func,
                &self.meta.ret_field_types()[c],
                &self.ctx,
                &mut self.truncated[c],
            )?;
            req.append_datum(c, &value);
        }
        self.current_key = None;
        self.states.clear();
        self.tracker
            .replace_bytes_used(self.child_chunk.memory_usage());
        Ok(())
    }

    fn next_stream(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        let batch = self.meta.max_chunk_size();
        while !self.executed && req.num_rows() < batch {
            if self.row_cursor >= self.child_chunk.num_rows() && !self.source_drained {
                self.load_child_chunk()?;
                continue;
            }
            if self.source_drained {
                if self.current_key.is_some() {
                    self.emit_current_group(req)?;
                // A global aggregate produces one synthetic row only when
                // the child was empty. If an actual global group was just
                // emitted above, EOF must terminate rather than append a
                // second all-NULL group.
                } else if !self.default_emitted
                    && self.child_returned_empty
                    && self.group_by.is_empty()
                {
                    self.start_group(Vec::new());
                    self.emit_current_group(req)?;
                    self.default_emitted = true;
                } else {
                    self.executed = true;
                }
                continue;
            }
            let chunk = std::mem::take(&mut self.child_chunk);
            let boundary = self.consume_row(&chunk);
            self.child_chunk = chunk;
            if boundary? {
                self.emit_current_group(req)?;
            }
        }
        Ok(())
    }
}

impl<C: Columns> Executor for StreamAggExec<C> {
    fn agg_tree_input_empty(&self) -> bool {
        self.child_returned_empty
    }

    fn open(&mut self) -> Result<(), ExecError> {
        self.child.open()?;
        self.child_chunk.reset();
        self.row_cursor = 0;
        self.source_drained = false;
        self.executed = false;
        self.child_returned_empty = true;
        self.default_emitted = false;
        self.current_key = None;
        self.pending_key = None;
        self.states.clear();
        self.tracker.replace_bytes_used(0);
        for flag in &mut self.truncated {
            *flag = false;
        }
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        let result = self.next_stream(req);
        if result.is_err() {
            // Go StreamAggExec makes an executor terminal after a child or
            // aggregate error, so a caller cannot observe partial state by
            // calling Next again.
            self.executed = true;
        }
        result
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.states.clear();
        self.current_key = None;
        self.pending_key = None;
        self.tracker.replace_bytes_used(0);
        self.child.close()
    }

    fn schema(&self) -> &Schema {
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

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::{Datum, Decimal, FieldTypeCode};
    use tidb_expr::column::Column;
    use tidb_expr::NoColumns;

    use crate::hash_agg::AggKind;

    struct ChunkSource {
        meta: ExecutorMeta,
        chunks: Vec<Chunk>,
        next: usize,
        failure: Option<ExecError>,
    }

    impl Executor for ChunkSource {
        fn open(&mut self) -> Result<(), ExecError> {
            self.next = 0;
            Ok(())
        }

        fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
            req.reset();
            if let Some(chunk) = self.chunks.get(self.next) {
                for row in 0..chunk.num_rows() {
                    req.append_row(chunk.get_row(row));
                }
                self.next += 1;
            } else if let Some(error) = self.failure.take() {
                return Err(error);
            }
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
            self.meta.init_cap()
        }

        fn max_chunk_size(&self) -> usize {
            self.meta.max_chunk_size()
        }

        fn new_chunk(&self) -> Chunk {
            self.meta.new_chunk()
        }
    }

    fn long() -> FieldType {
        FieldType::new(FieldTypeCode::LongLong)
    }

    fn decimal() -> FieldType {
        FieldType::new(FieldTypeCode::NewDecimal)
    }

    fn column(index: i64, field_type: FieldType) -> Expression {
        let mut column = Column::new(index + 1, field_type);
        column.index = index;
        Expression::Column(column)
    }

    fn source(chunks: &[&[(i64, Option<i64>)]]) -> Box<dyn Executor> {
        let fields = vec![long(), long()];
        let mut source_chunks = Vec::new();
        for rows in chunks {
            let mut chunk = Chunk::new_with_capacity(&fields, rows.len().max(1));
            for (group, value) in *rows {
                chunk.append_int64(0, *group);
                match value {
                    Some(value) => chunk.append_int64(1, *value),
                    None => chunk.append_null(1),
                }
            }
            source_chunks.push(chunk);
        }
        let columns = (0..2)
            .map(|index| {
                let mut column = Column::new((index + 1) as i64, long());
                column.index = index as i64;
                column
            })
            .collect();
        Box::new(ChunkSource {
            meta: ExecutorMeta::new(Schema::new(columns), 0, 2, 2),
            chunks: source_chunks,
            next: 0,
            failure: None,
        })
    }

    fn output_meta(types: &[FieldType], max_chunk_size: usize) -> ExecutorMeta {
        let columns = types
            .iter()
            .enumerate()
            .map(|(index, field_type)| {
                let mut column = Column::new((index + 1) as i64, field_type.clone());
                column.index = index as i64;
                column
            })
            .collect();
        ExecutorMeta::new(Schema::new(columns), 1, 1, max_chunk_size)
    }

    fn drain(
        exec: &mut StreamAggExec<NoColumns>,
        types: &[FieldType],
    ) -> Result<Vec<Vec<Datum>>, ExecError> {
        exec.open()?;
        let mut req = exec.new_chunk();
        let mut rows = Vec::new();
        loop {
            exec.next(&mut req)?;
            if req.num_rows() == 0 {
                break;
            }
            for row in 0..req.num_rows() {
                rows.push(
                    (0..req.num_cols())
                        .map(|column| req.get_row(row).get_datum(column, &types[column]))
                        .collect(),
                );
            }
        }
        exec.close()?;
        Ok(rows)
    }

    #[test]
    fn one_group_crosses_child_chunks_without_changing_output_order() {
        let types = [long(), decimal()];
        let mut exec = StreamAggExec::new(
            output_meta(&types, 1),
            vec![column(0, long())],
            vec![
                AggFunc::new(AggKind::FirstRow, Some(column(0, long()))),
                AggFunc::new(AggKind::Sum, Some(column(1, long()))),
            ],
            source(&[
                &[(1, Some(10)), (1, Some(20))],
                &[(1, Some(30)), (2, Some(5))],
                &[(3, None)],
            ]),
            NoColumns,
            StatementMemory::default(),
        );
        assert_eq!(
            drain(&mut exec, &types).unwrap(),
            vec![
                vec![Datum::Int(1), Datum::Decimal(Decimal::from_int(60))],
                vec![Datum::Int(2), Datum::Decimal(Decimal::from_int(5))],
                vec![Datum::Int(3), Datum::Null],
            ]
        );
        assert!(!exec.agg_tree_input_empty());
    }

    #[test]
    fn empty_input_has_the_same_global_and_grouped_rules_as_hash_aggregation() {
        let types = [long(), decimal()];
        let mut global = StreamAggExec::new(
            output_meta(&types, 8),
            vec![],
            vec![
                AggFunc::new(AggKind::Count, Some(column(1, long()))),
                AggFunc::new(AggKind::Sum, Some(column(1, long()))),
            ],
            source(&[]),
            NoColumns,
            StatementMemory::default(),
        );
        assert_eq!(
            drain(&mut global, &types).unwrap(),
            vec![vec![Datum::Int(0), Datum::Null]]
        );
        assert!(global.agg_tree_input_empty());

        let mut grouped = StreamAggExec::new(
            output_meta(&types, 8),
            vec![column(0, long())],
            vec![
                AggFunc::new(AggKind::FirstRow, Some(column(0, long()))),
                AggFunc::new(AggKind::Sum, Some(column(1, long()))),
            ],
            source(&[]),
            NoColumns,
            StatementMemory::default(),
        );
        assert!(drain(&mut grouped, &types).unwrap().is_empty());
        assert!(grouped.agg_tree_input_empty());
    }

    #[test]
    fn non_empty_global_aggregation_emits_only_its_actual_group() {
        let types = [decimal()];
        let mut exec = StreamAggExec::new(
            output_meta(&types, 8),
            vec![],
            vec![AggFunc::new(AggKind::Sum, Some(column(1, long())))],
            source(&[&[(1, Some(10)), (2, Some(20))]]),
            NoColumns,
            StatementMemory::default(),
        );
        assert_eq!(
            drain(&mut exec, &types).unwrap(),
            vec![vec![Datum::Decimal(Decimal::from_int(30))]]
        );
        assert!(!exec.agg_tree_input_empty());
    }

    #[test]
    fn child_error_makes_the_stream_aggregate_terminal() {
        let fields = vec![long(), long()];
        let mut input = Chunk::new_with_capacity(&fields, 1);
        input.append_int64(0, 1);
        input.append_int64(1, 10);
        let columns = (0..2)
            .map(|index| {
                let mut column = Column::new((index + 1) as i64, long());
                column.index = index as i64;
                column
            })
            .collect();
        let child = Box::new(ChunkSource {
            meta: ExecutorMeta::new(Schema::new(columns), 0, 2, 2),
            chunks: vec![input],
            next: 0,
            failure: Some(ExecError::internal("source failed")),
        });
        let types = [long()];
        let mut exec = StreamAggExec::new(
            output_meta(&types, 8),
            vec![],
            vec![AggFunc::new(AggKind::Count, Some(column(1, long())))],
            child,
            NoColumns,
            StatementMemory::default(),
        );
        exec.open().unwrap();
        let mut req = exec.new_chunk();
        assert!(
            matches!(exec.next(&mut req), Err(ExecError::Internal(message)) if message == "source failed")
        );
        exec.next(&mut req).unwrap();
        assert_eq!(req.num_rows(), 0);
    }
}
