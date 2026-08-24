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
//! The executor retains its position in the current child chunk across calls,
//! stops when the output chunk is full, and returns one row at a time when a
//! filter has order-sensitive side effects. Its cached child chunk is charged
//! to the statement memory budget for its whole open lifetime. Pure filters
//! still use the scalar evaluator; Go's column-vector filter implementation
//! remains outside this seed.

use std::sync::Arc;

use crate::executor::{ExecError, Executor, ExecutorMeta};
use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType};
use tidb_expr::evaluator::vectorizable;
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_expr::{truthy_of, Columns};
use tidb_util::memory::Tracker;

use crate::StatementMemory;

/// Go `SelectionExec`: filters its child's rows by a conjunction of predicates.
pub struct SelectionExec<C: Columns> {
    meta: ExecutorMeta,
    filters: Vec<Expression>,
    fast_filters: Vec<Option<FastSelectionFilter>>,
    child: Box<dyn Executor>,
    ctx: C,
    child_chunk: Option<Chunk>,
    tracker: Arc<Tracker>,
    memory: StatementMemory,
    input_row: usize,
    batched: bool,
    done: bool,
}

/// A row-local filter whose constant collation keys can be prepared once when
/// the Selection is built. This is intentionally narrower than the expression
/// evaluator: only a bare string-column `IN` with strict non-NULL literals is
/// eligible, so casts, warnings, and three-valued cases keep the source path.
#[derive(Clone, Debug)]
enum FastSelectionFilter {
    NullTest {
        column_offset: usize,
        negated: bool,
    },
    StringIn {
        column_offset: usize,
        collator: tidb_datatype::Collator,
        keys: Vec<Vec<u8>>,
    },
    And {
        filters: Vec<Self>,
        complete: bool,
    },
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
        memory: StatementMemory,
    ) -> Self {
        let batched = vectorizable(&filters);
        let fast_filters = filters
            .iter()
            .map(FastSelectionFilter::from_expression)
            .collect();
        let tracker = memory.operator_tracker(meta.id());
        SelectionExec {
            meta,
            filters,
            fast_filters,
            child,
            ctx,
            child_chunk: None,
            tracker,
            memory,
            input_row: 0,
            batched,
            done: false,
        }
    }

    /// Whether a row satisfies every filter (all truthy). A false or NULL filter
    /// rejects the row.
    fn row_passes(&self, row: tidb_chunk::row::Row<'_>) -> Result<bool, ExecError> {
        for (filter, fast_filter) in self.filters.iter().zip(&self.fast_filters) {
            if let Some(fast_filter) = fast_filter {
                if !fast_filter.matches(row) {
                    return Ok(false);
                }
                if fast_filter.is_complete() {
                    continue;
                }
            }
            let value = filter.eval(&self.ctx, row)?;
            if truthy_of(&value)? != Some(true) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn release_child_chunk(&mut self) {
        self.child_chunk = None;
        self.tracker.replace_bytes_used(0);
    }
}

impl FastSelectionFilter {
    fn from_expression(expression: &Expression) -> Option<Self> {
        let Expression::ScalarFunction(function) = expression else {
            return None;
        };
        if function.func_name.lowercase() == "and" {
            let mut filters = Vec::new();
            let mut complete = true;
            for argument in &function.args {
                if let Some(filter) = Self::from_expression(argument) {
                    complete &= filter.is_complete();
                    filters.push(filter);
                } else {
                    complete = false;
                }
            }
            return (!filters.is_empty()).then_some(Self::And { filters, complete });
        }
        let function_name = function.func_name.lowercase();
        if function.args.len() == 1 && (function_name == "isnull" || function_name == "not") {
            let (argument, negated) = if function_name == "isnull" {
                (&function.args[0], false)
            } else {
                let Expression::ScalarFunction(inner) = &function.args[0] else {
                    return None;
                };
                if inner.func_name.lowercase() != "isnull" || inner.args.len() != 1 {
                    return None;
                }
                (&inner.args[0], true)
            };
            let column = argument.as_column()?;
            let column_offset = usize::try_from(column.index).ok()?;
            return Some(Self::NullTest {
                column_offset,
                negated,
            });
        }
        if function.func_name.lowercase() != "in" || function.args.len() < 2 {
            return None;
        }
        let column = function.args.first()?.as_column()?;
        let field_type = column.get_static_type()?;
        if !field_type.is_string() {
            return None;
        }
        let column_offset = usize::try_from(column.index).ok()?;
        // The comparison's own derived collation, not the column's. Go's
        // `deriveCollation` for `ast.In` (`expression/collation.go:290`) runs
        // over ALL the arguments, so an explicit `COLLATE` on any one of them
        // decides it -- and the evaluator this fast path stands in for
        // already keys its hash set that way
        // (`ScalarFunction::prepare_in_string_hash_set`). Re-deriving from the
        // column makes the two disagree on exactly the rows the explicit
        // collation was written to catch.
        let collator = tidb_datatype::get_collator(function.derived_collation().name());
        let mut keys = function
            .args
            .iter()
            .skip(1)
            .map(|argument| match argument {
                Expression::Constant(constant)
                    if constant.deferred_expr.is_none()
                        && matches!(constant.value, Datum::String(_) | Datum::Bytes(_)) =>
                {
                    Some(constant.value.as_raw_bytes()?.to_vec())
                }
                _ => None,
            })
            .collect::<Option<Vec<_>>>()?
            .into_iter()
            .map(|bytes| collator.key(&bytes))
            .collect::<Vec<_>>();
        keys.sort_unstable();
        keys.dedup();
        Some(Self::StringIn {
            column_offset,
            collator,
            keys,
        })
    }

    fn matches(&self, row: tidb_chunk::row::Row<'_>) -> bool {
        match self {
            Self::NullTest {
                column_offset,
                negated,
            } => row.is_null(*column_offset) != *negated,
            Self::StringIn {
                column_offset,
                collator,
                keys,
            } => {
                if row.is_null(*column_offset) {
                    return false;
                }
                let key = collator.key(row.get_string(*column_offset).as_bytes());
                keys.binary_search(&key).is_ok()
            }
            Self::And { filters, .. } => filters.iter().all(|filter| filter.matches(row)),
        }
    }

    fn is_complete(&self) -> bool {
        match self {
            Self::NullTest { .. } | Self::StringIn { .. } => true,
            Self::And { complete, .. } => *complete,
        }
    }
}

impl<C: Columns> Executor for SelectionExec<C> {
    fn open(&mut self) -> Result<(), ExecError> {
        self.release_child_chunk();
        self.child.open()?;
        let child_chunk = self.child.new_chunk();
        self.tracker.replace_bytes_used(child_chunk.memory_usage());
        self.child_chunk = Some(child_chunk);
        self.input_row = 0;
        self.done = false;
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.grow_and_reset(self.max_chunk_size());
        if self.done {
            return Ok(());
        }
        self.memory.check()?;
        loop {
            let child_chunk = self
                .child_chunk
                .as_ref()
                .expect("selection child chunk exists while open");
            let rows = child_chunk.num_rows();
            while self.input_row < rows {
                if req.is_full() {
                    return Ok(());
                }
                let row = child_chunk.get_row(self.input_row);
                let selected = self.row_passes(row)?;
                if selected {
                    req.append_row(row);
                }
                self.input_row += 1;
                if selected && !self.batched {
                    return Ok(());
                }
            }

            let child_chunk = self
                .child_chunk
                .as_mut()
                .expect("selection child chunk exists while open");
            let before = child_chunk.memory_usage();
            let result = self.child.next(child_chunk);
            self.tracker.consume(child_chunk.memory_usage() - before);
            result?;
            self.memory.check()?;
            self.input_row = 0;
            if child_chunk.num_rows() == 0 {
                self.done = true;
                return Ok(());
            }
        }
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.release_child_chunk();
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

    /// A leaf Selection is still a negotiable wrapper around its base table.
    /// Go's predicate pushdown walks through LogicalSelection before building
    /// the physical reader; forwarding this capability lets reordered joins
    /// do the same without removing the Selection that still owns any
    /// residual or sibling-semijoin predicate.
    fn table_access(&mut self) -> Option<&mut dyn crate::table_access::TableAccess> {
        self.child.table_access()
    }
}

impl<C: Columns> Drop for SelectionExec<C> {
    fn drop(&mut self) {
        self.release_child_chunk();
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::HashMap;

    use super::*;
    use tidb_ast::CiString;
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};
    use tidb_expr::column::Column;
    use tidb_expr::constant::Constant;
    use tidb_expr::expression::ScalarFunction;
    use tidb_expr::NoColumns;

    use crate::{OomAction, StatementMemory};

    fn long() -> FieldType {
        FieldType::new(FieldTypeCode::Long)
    }

    fn string() -> FieldType {
        FieldType::new(FieldTypeCode::VarString)
    }

    #[derive(Default)]
    struct UserVariables(RefCell<HashMap<String, Datum>>);

    impl Columns for UserVariables {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn get_uservar(&self, name: &str) -> Option<Datum> {
            self.0.borrow().get(&name.to_ascii_lowercase()).cloned()
        }

        fn set_uservar(&self, name: &str, value: Datum) {
            self.0.borrow_mut().insert(name.to_ascii_lowercase(), value);
        }
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
            StatementMemory::default(),
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
            StatementMemory::default(),
        );
        sel.open().unwrap();
        let mut req = sel.new_chunk();
        sel.next(&mut req).unwrap();
        assert_eq!(req.num_rows(), 0);
    }

    #[test]
    fn selection_preserves_filtered_rows_across_requested_batches() {
        let mut data = Chunk::new_with_capacity(std::slice::from_ref(&long()), 8);
        for value in 1..=8 {
            data.append_int64(0, value);
        }
        let source = OneChunkSource {
            meta: ExecutorMeta::new(one_long_col_schema(), 0, 8, 8),
            data: Some(data),
        };
        let mut column = Column::new(1, long());
        column.index = 0;
        let filter = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("gt"),
            long(),
            vec![
                Expression::Column(column),
                Expression::Constant(Constant::new(Datum::Int(0), long())),
            ],
        ));
        let mut selection = SelectionExec::new(
            ExecutorMeta::new(one_long_col_schema(), 1, 3, 3),
            vec![filter],
            Box::new(source),
            NoColumns,
            StatementMemory::default(),
        );

        selection.open().unwrap();
        let mut req = selection.new_chunk();
        req.set_required_rows(2, 3);
        let mut batches = Vec::new();
        loop {
            selection.next(&mut req).unwrap();
            if req.num_rows() == 0 {
                break;
            }
            batches.push(
                (0..req.num_rows())
                    .map(|row| req.get_row(row).get_int64(0))
                    .collect::<Vec<_>>(),
            );
        }

        assert_eq!(
            batches,
            vec![vec![1, 2], vec![3, 4], vec![5, 6], vec![7, 8]]
        );
    }

    #[test]
    fn selection_accounts_cached_child_chunk_against_query_quota() {
        let mut data = Chunk::new_with_capacity(std::slice::from_ref(&long()), 64);
        for value in 0..64 {
            data.append_int64(0, value);
        }
        let source = OneChunkSource {
            meta: ExecutorMeta::new(one_long_col_schema(), 0, 0, 64),
            data: Some(data),
        };
        let mut column = Column::new(1, long());
        column.index = 0;
        let filter = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("gt"),
            long(),
            vec![
                Expression::Column(column),
                Expression::Constant(Constant::new(Datum::Int(-1), long())),
            ],
        ));
        let memory = StatementMemory::new(200, OomAction::Cancel, 71);
        let mut selection = SelectionExec::new(
            ExecutorMeta::new(one_long_col_schema(), 1, 0, 64),
            vec![filter],
            Box::new(source),
            NoColumns,
            memory.clone(),
        );

        selection.open().unwrap();
        let mut req = selection.new_chunk();
        assert!(matches!(
            selection.next(&mut req),
            Err(ExecError::MemoryExceedForQuery { conn_id: 71 })
        ));
        selection.close().unwrap();
        assert_eq!(memory.bytes_consumed(), 0);
    }

    #[test]
    fn side_effecting_filter_returns_one_row_before_projection_observes_it() {
        let mut data = Chunk::new_with_capacity(std::slice::from_ref(&long()), 3);
        for value in 1..=3 {
            data.append_int64(0, value);
        }
        let source = OneChunkSource {
            meta: ExecutorMeta::new(one_long_col_schema(), 0, 3, 3),
            data: Some(data),
        };
        let mut column = Column::new(1, long());
        column.index = 0;
        let variable_name =
            Expression::Constant(Constant::new(Datum::Bytes(b"v".to_vec()), string()));
        let filter = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("setvar"),
            long(),
            vec![variable_name, Expression::Column(column)],
        ));
        let mut selection = SelectionExec::new(
            ExecutorMeta::new(one_long_col_schema(), 1, 3, 3),
            vec![filter],
            Box::new(source),
            UserVariables::default(),
            StatementMemory::default(),
        );

        selection.open().unwrap();
        let mut req = selection.new_chunk();
        for expected in 1..=3 {
            selection.next(&mut req).unwrap();
            assert_eq!(req.num_rows(), 1);
            assert_eq!(req.get_row(0).get_int64(0), expected);
            assert_eq!(selection.ctx.get_uservar("v"), Some(Datum::Int(expected)));
        }
        selection.next(&mut req).unwrap();
        assert_eq!(req.num_rows(), 0);
    }
}
