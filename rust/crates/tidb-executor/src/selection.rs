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

//! `pkg/executor` `SelectionExec`: keeps the child rows for which every filter
//! evaluates to true -- the `WHERE`/`HAVING` operator.
//!
//! A row passes when every filter is truthy; a filter that is false OR NULL
//! rejects the row (MySQL's three-valued logic, via [`truthy_of`]).
//!
//! This is a simplified serial path: one `Next` call drains the child and
//! materializes all surviving rows. Go's chunk-full batching (`req.IsFull` /
//! `GrowAndReset`) and the vectorized filter are deferred (documented).

use crate::executor::{ExecError, Executor, ExecutorMeta};
use tidb_chunk::chunk::Chunk;
use tidb_datatype::FieldType;
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_expr::{truthy_of, Columns};

/// Go `SelectionExec`: filters its child's rows by a conjunction of predicates.
pub struct SelectionExec<C: Columns> {
    meta: ExecutorMeta,
    filters: Vec<Expression>,
    child: Box<dyn Executor>,
    ctx: C,
    child_chunk: Chunk,
    done: bool,
}

impl<C: Columns> SelectionExec<C> {
    /// Builds a selection of `child`'s rows satisfying every filter in
    /// `filters`, evaluated with `ctx`.
    #[must_use]
    pub fn new(
        meta: ExecutorMeta,
        filters: Vec<Expression>,
        child: Box<dyn Executor>,
        ctx: C,
    ) -> Self {
        let child_chunk = child.new_chunk();
        SelectionExec {
            meta,
            filters,
            child,
            ctx,
            child_chunk,
            done: false,
        }
    }

    /// Whether a row satisfies every filter (all truthy). A false or NULL filter
    /// rejects the row.
    fn row_passes(&self, row: tidb_chunk::row::Row<'_>) -> Result<bool, ExecError> {
        for filter in &self.filters {
            let value = filter.eval(&self.ctx, row)?;
            if truthy_of(&value)? != Some(true) {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

impl<C: Columns> Executor for SelectionExec<C> {
    fn open(&mut self) -> Result<(), ExecError> {
        self.child.open()?;
        self.child_chunk.reset();
        self.done = false;
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        if self.done {
            return Ok(());
        }
        loop {
            self.child.next(&mut self.child_chunk)?;
            let rows = self.child_chunk.num_rows();
            if rows == 0 {
                self.done = true;
                return Ok(());
            }
            for r in 0..rows {
                let row = self.child_chunk.get_row(r);
                if self.row_passes(row)? {
                    req.append_row(row);
                }
            }
        }
    }

    fn close(&mut self) -> Result<(), ExecError> {
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
    use tidb_ast::CiString;
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};
    use tidb_expr::column::Column;
    use tidb_expr::constant::Constant;
    use tidb_expr::expression::ScalarFunction;
    use tidb_expr::NoColumns;

    fn long() -> FieldType {
        FieldType::new(FieldTypeCode::Long)
    }

    /// A test-only source that emits one prebuilt chunk, then EOF.
    struct OneChunkSource {
        meta: ExecutorMeta,
        data: Option<Chunk>,
    }

    impl Executor for OneChunkSource {
        fn open(&mut self) -> Result<(), ExecError> {
            Ok(())
        }
        fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
            req.reset();
            if let Some(data) = self.data.take() {
                for r in 0..data.num_rows() {
                    req.append_row(data.get_row(r));
                }
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

    fn one_long_col_schema() -> Schema {
        let mut c = Column::new(1, long());
        c.index = 0;
        Schema::new(vec![c])
    }

    #[test]
    fn selection_keeps_rows_passing_predicate() {
        // Source rows: col0 in {1, 2, 3}.
        let mut data = Chunk::new_with_capacity(std::slice::from_ref(&long()), 3);
        for v in [1, 2, 3] {
            data.append_int64(0, v);
        }
        let source = OneChunkSource {
            meta: ExecutorMeta::new(one_long_col_schema(), 0, 3, 1024),
            data: Some(data),
        };

        // Filter: col0 > 1 (gt(col0, 1)).
        let mut col = Column::new(1, long());
        col.index = 0;
        let filter = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("gt"),
            long(),
            vec![
                Expression::Column(col),
                Expression::Constant(Constant::new(Datum::Int(1), long())),
            ],
        ));

        let mut sel = SelectionExec::new(
            ExecutorMeta::new(one_long_col_schema(), 1, 3, 1024),
            vec![filter],
            Box::new(source),
            NoColumns,
        );

        sel.open().unwrap();
        let mut req = sel.new_chunk();
        sel.next(&mut req).unwrap();
        assert_eq!(req.num_rows(), 2);
        assert_eq!(req.get_row(0).get_int64(0), 2);
        assert_eq!(req.get_row(1).get_int64(0), 3);

        // Exhausted.
        sel.next(&mut req).unwrap();
        assert_eq!(req.num_rows(), 0);
        sel.close().unwrap();
    }

    #[test]
    fn null_and_false_filters_reject_rows() {
        let mut data = Chunk::new_with_capacity(std::slice::from_ref(&long()), 2);
        data.append_int64(0, 5);
        data.append_null(0); // col0 IS NULL -> gt is NULL -> rejected
        let source = OneChunkSource {
            meta: ExecutorMeta::new(one_long_col_schema(), 0, 2, 1024),
            data: Some(data),
        };
        let mut col = Column::new(1, long());
        col.index = 0;
        // col0 > 10  -> false for 5, NULL for the null row: both rejected.
        let filter = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("gt"),
            long(),
            vec![
                Expression::Column(col),
                Expression::Constant(Constant::new(Datum::Int(10), long())),
            ],
        ));
        let mut sel = SelectionExec::new(
            ExecutorMeta::new(one_long_col_schema(), 1, 2, 1024),
            vec![filter],
            Box::new(source),
            NoColumns,
        );
        sel.open().unwrap();
        let mut req = sel.new_chunk();
        sel.next(&mut req).unwrap();
        assert_eq!(req.num_rows(), 0);
    }
}
