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

//! Serial Go ExpandExec: evaluate every grouping level over one cached chunk.

use crate::executor::{ExecError, Executor, ExecutorMeta};
use crate::StatementMemory;
use std::sync::Arc;
use tidb_chunk::chunk::Chunk;
use tidb_datatype::FieldType;
use tidb_expr::evaluator::{EvaluatorError, EvaluatorSuite};
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_expr::Columns;
use tidb_util::memory::Tracker;

/// Projects a cached input batch once per grouping level before advancing.
pub struct ExpandExec<C: Columns> {
    meta: ExecutorMeta,
    levels: Vec<EvaluatorSuite>,
    child: Box<dyn Executor>,
    ctx: C,
    input: Chunk,
    level: usize,
    done: bool,
    memory: StatementMemory,
    tracker: Arc<Tracker>,
}

impl<C: Columns> ExpandExec<C> {
    /// Builds the level evaluators without swapping columns out of their input.
    pub fn new(
        meta: ExecutorMeta,
        levels: Vec<Vec<Expression>>,
        child: Box<dyn Executor>,
        ctx: C,
        memory: StatementMemory,
    ) -> Self {
        let input = child.new_chunk();
        let tracker = memory.operator_tracker(meta.id());
        Self {
            meta,
            levels: levels
                .into_iter()
                .map(|exprs| EvaluatorSuite::new(exprs, true))
                .collect(),
            child,
            ctx,
            input,
            level: usize::MAX,
            done: false,
            memory,
            tracker,
        }
    }
}

impl<C: Columns> Executor for ExpandExec<C> {
    fn open(&mut self) -> Result<(), ExecError> {
        self.child.open()?;
        self.input.reset();
        self.level = usize::MAX;
        self.done = false;
        self.tracker.replace_bytes_used(self.input.memory_usage());
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.grow_and_reset(self.max_chunk_size());
        if self.done || self.levels.is_empty() {
            return Ok(());
        }
        self.memory.check()?;
        if self.level >= self.levels.len() {
            self.input
                .set_required_rows(req.required_rows() as isize, self.meta.max_chunk_size());
            self.child.next(&mut self.input)?;
            self.tracker.replace_bytes_used(self.input.memory_usage());
            self.memory.check()?;
            if self.input.num_rows() == 0 {
                self.done = true;
                return Ok(());
            }
            self.level = 0;
        }
        if self.meta.schema().is_empty() {
            req.set_num_virtual_rows(self.input.num_rows());
        } else {
            self.levels[self.level]
                .run(&self.ctx, &mut self.input, req)
                .map_err(|error| match error {
                    EvaluatorError::Eval(error) => ExecError::Eval(error),
                    EvaluatorError::Chunk(message) => ExecError::internal(message),
                })?;
        }
        self.level += 1;
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.input = Chunk::new_empty(self.child.ret_field_types());
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

impl<C: Columns> Drop for ExpandExec<C> {
    fn drop(&mut self) {
        self.tracker.replace_bytes_used(0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::{Datum, FieldTypeCode};
    use tidb_expr::{column::Column, constant::Constant};

    #[test]
    fn zero_width_levels_preserve_virtual_rows_and_empty_input() {
        for rows in [0, 1] {
            let ctx = crate::StmtContext::for_query();
            let child = crate::table_dual::TableDualExec::new(
                ExecutorMeta::new(Schema::new(vec![]), 1, 2, 2),
                rows,
            );
            let mut expand = ExpandExec::new(
                ExecutorMeta::new(Schema::new(vec![]), 2, 2, 2),
                vec![vec![], vec![]],
                Box::new(child),
                ctx.clone(),
                ctx.statement_memory(),
            );
            let mut out = expand.new_chunk();
            expand.open().unwrap();
            for expected in [rows, rows, 0, 0] {
                expand.next(&mut out).unwrap();
                assert_eq!(out.num_rows(), expected);
            }
            expand.close().unwrap();
            assert_eq!(expand.tracker.bytes_consumed(), 0);
        }
    }

    #[test]
    fn cached_input_obeys_statement_memory_quota() {
        let ty = FieldType::new(FieldTypeCode::LongLong);
        let schema = Schema::new(vec![Column::new(1, ty.clone())]);
        let ctx = crate::StmtContext::for_query().with_mem_quota(1, crate::OomAction::Cancel);
        let source = Batches {
            meta: ExecutorMeta::new(schema.clone(), 1, 2, 2),
            batch: 0,
        };
        let mut expand = ExpandExec::new(
            ExecutorMeta::new(schema, 2, 2, 2),
            vec![vec![Expression::Constant(Constant::new(Datum::Null, ty))]],
            Box::new(source),
            ctx.clone(),
            ctx.statement_memory(),
        );
        expand.open().unwrap();
        let mut out = expand.new_chunk();
        assert!(matches!(
            expand.next(&mut out),
            Err(ExecError::MemoryExceedForQuery { .. })
        ));
        expand.close().unwrap();
        assert_eq!(expand.tracker.bytes_consumed(), 0);
    }

    struct Batches {
        meta: ExecutorMeta,
        batch: i64,
    }
    impl Executor for Batches {
        fn open(&mut self) -> Result<(), ExecError> {
            self.batch = 0;
            Ok(())
        }
        fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
            req.reset();
            if self.batch < 2 {
                req.append_int64(0, self.batch * 10 + 1);
                req.append_int64(0, self.batch * 10 + 2);
                self.batch += 1;
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
            2
        }
        fn max_chunk_size(&self) -> usize {
            2
        }
        fn new_chunk(&self) -> Chunk {
            self.meta.new_chunk()
        }
    }

    #[test]
    fn every_level_preserves_input_across_batches_and_reopen() {
        let ty = FieldType::new(FieldTypeCode::LongLong);
        let mut column = Column::new(1, ty.clone());
        column.index = 0;
        let schema = Schema::new(vec![column.clone()]);
        let source = Batches {
            meta: ExecutorMeta::new(schema.clone(), 1, 2, 2),
            batch: 0,
        };
        let ctx = crate::StmtContext::for_query();
        let mut expand = ExpandExec::new(
            ExecutorMeta::new(schema, 2, 2, 2),
            vec![
                vec![Expression::Constant(Constant::new(Datum::Null, ty.clone()))],
                vec![Expression::Column(column)],
            ],
            Box::new(source),
            ctx.clone(),
            ctx.statement_memory(),
        );
        let mut out = expand.new_chunk();
        for _ in 0..2 {
            expand.open().unwrap();
            for expected in [
                vec![Datum::Null, Datum::Null],
                vec![Datum::Int(1), Datum::Int(2)],
                vec![Datum::Null, Datum::Null],
                vec![Datum::Int(11), Datum::Int(12)],
            ] {
                expand.next(&mut out).unwrap();
                let actual: Vec<_> = (0..out.num_rows())
                    .map(|i| out.get_row(i).get_datum(0, &ty))
                    .collect();
                assert_eq!(actual, expected);
            }
            expand.next(&mut out).unwrap();
            assert_eq!(out.num_rows(), 0);
            expand.close().unwrap();
        }
    }
}
