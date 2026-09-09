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

//! `pkg/executor` `ProjectionExec`: evaluates a list of expressions over each
//! input row to form the output rows.
//!
//! This is the serial path: one child batch per `Next`, each input row producing
//! one output row. Go's parallel projection (worker pool) is deferred.

use crate::executor::{ExecError, Executor, ExecutorMeta};
use tidb_chunk::chunk::Chunk;
use tidb_datatype::FieldType;
use tidb_expr::evaluator::{EvaluatorError, EvaluatorSuite};
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_expr::Columns;

/// One execution of a physical projection, with its own evaluator and buffer.
pub struct ProjectionExec<C: Columns> {
    meta: ExecutorMeta,
    evaluator_suite: EvaluatorSuite,
    child: Box<dyn Executor>,
    ctx: C,
    child_chunk: Chunk,
}

impl<C: Columns> ProjectionExec<C> {
    /// Builds a projection of `exprs` over `child`, evaluated with `ctx`.
    #[must_use]
    pub fn new(
        meta: ExecutorMeta,
        exprs: Vec<Expression>,
        child: Box<dyn Executor>,
        ctx: C,
    ) -> Self {
        Self::with_column_evaluator(meta, exprs, child, ctx, false)
    }

    /// Go `newProjectionExec`: the physical plan controls column swapping.
    #[must_use]
    pub fn with_column_evaluator(
        meta: ExecutorMeta,
        exprs: Vec<Expression>,
        child: Box<dyn Executor>,
        ctx: C,
        avoid_column_evaluator: bool,
    ) -> Self {
        let child_chunk = child.new_chunk();
        let evaluator_suite = EvaluatorSuite::new(exprs, avoid_column_evaluator);
        ProjectionExec {
            meta,
            evaluator_suite,
            child,
            ctx,
            child_chunk,
        }
    }
}

impl<C: Columns> Executor for ProjectionExec<C> {
    fn open(&mut self) -> Result<(), ExecError> {
        self.child.open()?;
        self.child_chunk.reset();
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        let max_chunk_size = self.max_chunk_size();
        // Go calls GrowAndReset before reading RequiredRows. Growing restores
        // the maximum demand, while an ordinary reset preserves the parent's
        // current request.
        req.grow_and_reset(max_chunk_size);
        let required_rows = isize::try_from(req.required_rows()).unwrap_or(isize::MAX);
        self.child_chunk
            .set_required_rows(required_rows, max_chunk_size);
        self.child.next(&mut self.child_chunk)?;
        if self.child_chunk.num_rows() == 0 {
            return Ok(());
        }
        self.evaluator_suite
            .run(&self.ctx, &mut self.child_chunk, req)
            .map_err(|error| match error {
                EvaluatorError::Eval(error) => ExecError::Eval(error),
                EvaluatorError::Chunk(message) => ExecError::internal(message),
            })
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

    /// Projection preserves one output row per child row, so an exact child
    /// cardinality is also exact for this wrapper.  Expressions are not
    /// evaluated on the count-only path, which matches Go's parent COUNT
    /// shortcut.
    fn row_count(&mut self) -> Result<Option<u64>, ExecError> {
        self.child.row_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table_dual::TableDualExec;
    use std::sync::Arc;
    use tidb_ast::CiString;
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};
    use tidb_expr::column::Column;
    use tidb_expr::constant::Constant;
    use tidb_expr::evaluator::EvaluatorProgram;
    use tidb_expr::expression::ScalarFunction;
    use tidb_expr::NoColumns;

    fn long() -> FieldType {
        FieldType::new(FieldTypeCode::Long)
    }

    fn int_const(v: i64) -> Expression {
        Expression::Constant(Constant::new(Datum::Int(v), long()))
    }

    fn parameter(order: i64) -> Expression {
        let mut constant = Constant::new(Datum::Int(99), long());
        constant.param_marker = Some(tidb_expr::constant::ParamMarker { order });
        Expression::Constant(constant)
    }

    #[test]
    fn prepared_filter_and_projection_use_fresh_execution_state() {
        let predicate = parameter(0);
        let projection = parameter(1);
        for (condition, value, rows) in [
            (Datum::Null, 2, 0),
            (Datum::Int(1), 7, 1),
            (Datum::Int(0), 3, 0),
            (Datum::Int(-1), 9, 1),
        ] {
            let ctx = crate::StmtContext::for_query()
                .with_prepared_params(vec![condition, Datum::Int(value)].into());
            let dual = TableDualExec::new(ExecutorMeta::new(Schema::new(vec![]), 0, 1, 1024), 1);
            let selection = crate::selection::SelectionExec::new(
                ExecutorMeta::new(Schema::new(vec![]), 1, 1, 1024),
                vec![predicate.clone()],
                Box::new(dual),
                ctx.clone(),
                ctx.statement_memory(),
            );
            let mut executor = ProjectionExec::new(
                ExecutorMeta::new(Schema::new(vec![Column::new(1, long())]), 2, 1, 1024),
                vec![projection.clone()],
                Box::new(selection),
                ctx,
            );
            executor.open().unwrap();
            let mut output = executor.new_chunk();
            executor.next(&mut output).unwrap();
            assert_eq!(output.num_rows(), rows);
            if rows != 0 {
                assert_eq!(output.get_row(0).get_int64(0), value);
            }
            executor.next(&mut output).unwrap();
            assert_eq!(output.num_rows(), 0);
            executor.close().unwrap();
        }
    }

    #[test]
    fn prepared_program_is_shared_but_parameter_contexts_are_isolated() {
        let mut deferred = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("plus"),
            long(),
            vec![parameter(0), int_const(0)],
        ));
        let planning =
            crate::StmtContext::for_query().with_prepared_params(vec![Datum::Int(1)].into());
        tidb_expr::fold_constant_in_mode(
            &mut deferred,
            &planning,
            tidb_expr::ConstantFoldMode::Normal,
        );
        assert!(matches!(&deferred, Expression::Constant(c) if c.deferred_expr.is_some()));
        for expression in [parameter(0), deferred] {
            let program = Arc::new(EvaluatorProgram::new(vec![expression], false));
            let executions: Vec<_> = [Datum::Int(7), Datum::Null, Datum::Int(-3)]
                .into_iter()
                .map(|value| {
                    let program = program.clone();
                    std::thread::spawn(move || {
                        let suite = EvaluatorSuite::from_program(program);
                        let ctx = crate::StmtContext::for_query()
                            .with_prepared_params(vec![value.clone()].into());
                        let cloned = ctx.clone();
                        for _ in 0..16 {
                            let mut input = Chunk::new_empty(&[]);
                            input.set_num_virtual_rows(3);
                            let mut output = Chunk::new_with_capacity(&[long()], 3);
                            suite.run(&cloned, &mut input, &mut output).unwrap();
                            for row in 0..3 {
                                assert_eq!(output.get_row(row).get_datum(0, &long()), value);
                            }
                        }
                    })
                })
                .collect();
            for execution in executions {
                execution.join().unwrap();
            }
        }
    }

    #[test]
    fn prepared_deferred_conversion_uses_current_statement_diagnostics() {
        let target = FieldType::new(FieldTypeCode::Tiny);
        let mut constant = Constant::new(Datum::Int(99), target);
        constant.deferred_expr = Some(Box::new(parameter(0)));
        let expression = Expression::Constant(constant);
        for (level, input, expected, warning_count) in [
            (
                tidb_expr::ErrorLevel::Warn,
                "12tail",
                Some(Datum::Int(12)),
                1,
            ),
            (tidb_expr::ErrorLevel::Error, "12tail", None, 0),
            (
                tidb_expr::ErrorLevel::Ignore,
                "12tail",
                Some(Datum::Int(12)),
                0,
            ),
            (tidb_expr::ErrorLevel::Warn, "128tail", None, 1),
            (tidb_expr::ErrorLevel::Warn, "7", Some(Datum::Int(7)), 0),
        ] {
            let ctx = crate::StmtContext::for_query()
                .with_truncate_level(level)
                .with_prepared_params(vec![Datum::new_string(input)].into());
            let result = tidb_expr::eval_expression_once(&expression, &ctx);
            if let Some(expected) = expected {
                assert_eq!(result.unwrap(), expected, "{level:?} / {input}");
            } else {
                let error =
                    crate::DriverError::from(ExecError::Eval(result.unwrap_err())).to_mysql_error();
                let (code, message) = if input == "128tail" {
                    (1690, "constant 128 overflows tinyint")
                } else {
                    (1292, "Truncated incorrect DOUBLE value: '12tail'")
                };
                assert_eq!(error.code, code);
                assert_eq!(error.message, message);
                assert_eq!(
                    error.state,
                    if code == 1690 { *b"22003" } else { *b"22007" }
                );
            }
            let warnings = ctx.take_warnings();
            assert_eq!(warnings.len(), warning_count, "{level:?} / {input}");
            if warning_count != 0 {
                assert_eq!(warnings[0].1, 1292);
                assert_eq!(
                    warnings[0].2,
                    format!("Truncated incorrect DOUBLE value: '{input}'")
                );
            }
        }
        assert!(matches!(expression, Expression::Constant(c) if c.value == Datum::Int(99)));
    }

    #[test]
    fn prepared_lazy_folding_keeps_warnings_execution_local() {
        let division = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("intdiv"),
            long(),
            vec![int_const(1), parameter(1)],
        ));
        let original = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("if"),
            long(),
            vec![parameter(0), int_const(7), division],
        ));
        let planning = crate::StmtContext::for_query()
            .with_prepared_params(vec![Datum::Int(1), Datum::Int(0)].into());
        let mut expression = original.clone();
        tidb_expr::fold_constant_in_mode(
            &mut expression,
            &planning,
            tidb_expr::ConstantFoldMode::Try,
        );
        assert!(planning.take_warnings().is_empty());
        for (condition, divisor, expected, warnings) in [
            (1, 0, Datum::Int(7), 0),
            (0, 0, Datum::Null, 1),
            (0, 1, Datum::Int(1), 0),
        ] {
            let ctx = crate::StmtContext::for_query()
                .with_prepared_params(vec![Datum::Int(condition), Datum::Int(divisor)].into());
            assert_eq!(
                tidb_expr::eval_expression_once(&expression, &ctx).unwrap(),
                expected
            );
            let actual = ctx.take_warnings();
            assert_eq!(actual.len(), warnings);
            if warnings != 0 {
                assert_eq!((actual[0].1, actual[0].2.as_str()), (1365, "Division by 0"));
            }
        }
    }

    /// `SELECT 1 + 1` executes end-to-end: a table-dual source feeds one virtual
    /// row to a projection of `plus(1, 1)`, producing a chunk holding `2`.
    #[test]
    fn select_one_plus_one_executes() {
        // Source: TableDual with an empty schema, one virtual row.
        let dual = TableDualExec::new(ExecutorMeta::new(Schema::new(vec![]), 0, 1, 1024), 1);

        // Projection output schema: a single Long column.
        let out_col = Column::new(1, long());
        let proj_schema = Schema::new(vec![out_col]);
        let plus = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("plus"),
            long(),
            vec![int_const(1), int_const(1)],
        ));
        let mut proj = ProjectionExec::new(
            ExecutorMeta::new(proj_schema, 1, 1, 1024),
            vec![plus],
            Box::new(dual),
            NoColumns,
        );

        proj.open().unwrap();
        let mut req = proj.new_chunk();
        proj.next(&mut req).unwrap();
        assert_eq!(req.num_rows(), 1);
        assert_eq!(req.get_row(0).get_int64(0), 2);

        // Next batch is EOF (dual exhausted).
        proj.next(&mut req).unwrap();
        assert_eq!(req.num_rows(), 0);
        proj.close().unwrap();
    }

    /// The projection's per-row evaluation reads a child column: `col0 + 1` over
    /// an input row whose column 0 is 41 produces 42.
    #[test]
    fn projection_reads_child_column() {
        let mut input = Chunk::new_with_capacity(std::slice::from_ref(&long()), 1);
        input.append_int64(0, 41);

        let mut col = Column::new(7, long());
        col.index = 0;
        let expr = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("plus"),
            long(),
            vec![Expression::Column(col), int_const(1)],
        ));
        // Evaluate directly (the projection's inner loop) to confirm column reads.
        let row = input.get_row(0);
        let out = expr.eval(&NoColumns, row).unwrap();
        assert_eq!(out, Datum::Int(42));
    }
}
