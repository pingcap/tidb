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
use std::collections::{BTreeMap, VecDeque};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Arc;
use tidb_chunk::chunk::Chunk;
use tidb_datatype::FieldType;
use tidb_expr::evaluator::{EvaluatorError, EvaluatorProgram, EvaluatorSuite};
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_expr::Columns;

/// Marks an evaluation context [`ProjectionExec`] accepts.
///
/// Go hands its session context to every projection worker goroutine; a Rust
/// context may cross to the pool's threads only when it is `Send + Sync`. The
/// bridge method carries that proof into the parallel path without imposing
/// the bound on contexts that cannot honor it, which keep the serial path.
pub trait ProjectionContext: Columns {
    /// One parallel `Next`, or `None` when this context cannot share
    /// evaluation across threads.
    fn parallel_next_bridge(
        exec: &mut ProjectionExec<Self>,
        req: &mut Chunk,
    ) -> Option<Result<(), ExecError>>
    where
        Self: Sized,
    {
        let _ = (exec, req);
        None
    }
}

impl ProjectionContext for tidb_expr::NoColumns {
    fn parallel_next_bridge(
        exec: &mut ProjectionExec<Self>,
        req: &mut Chunk,
    ) -> Option<Result<(), ExecError>> {
        Some(exec.parallel_next(req))
    }
}

impl ProjectionContext for crate::StmtContext {
    /// The production statement context shares every interior-mutable handle
    /// through `Arc` + `Mutex`/atomics, so worker threads may evaluate
    /// expressions through `&StmtContext` (the same proof the hash join's
    /// probe workers and the aggregate's partial lanes rely on).
    fn parallel_next_bridge(
        exec: &mut ProjectionExec<Self>,
        req: &mut Chunk,
    ) -> Option<Result<(), ExecError>> {
        Some(exec.parallel_next(req))
    }
}

/// The `'static` snapshot every projection worker task borrows: Go's workers
/// share the executor's `evaluatorSuit` and its evaluation context.
struct ParallelProjectionShared<C> {
    program: Arc<EvaluatorProgram>,
    ctx: C,
}

/// Go `projectionInputFetcher` + `projectionWorker`s, driven from the session
/// thread: the parent's `Next` fetches a child chunk into a free input chunk,
/// hands the pair to a pool worker, and releases finished outputs in fetch
/// order. `numWorkers` input and output chunks circulate, so the fetch runs at
/// most that far ahead of the parent, as Go's `inputCh`/`outputCh` bound it.
struct ParallelProjection<C> {
    shared: Arc<ParallelProjectionShared<C>>,
    result_tx: Sender<(u64, Result<(Chunk, Chunk), ExecError>)>,
    result_rx: Receiver<(u64, Result<(Chunk, Chunk), ExecError>)>,
    /// Finished pairs waiting for their turn (`output.done` in fetch order).
    reorder: BTreeMap<u64, (Chunk, Chunk)>,
    /// Outputs released in order, not yet handed to the parent.
    ready: VecDeque<Chunk>,
    free_inputs: Vec<Chunk>,
    free_outputs: Vec<Chunk>,
    inputs_allocated: usize,
    outputs_allocated: usize,
    next_seq: u64,
    next_release: u64,
    in_flight: usize,
    child_done: bool,
}

/// One execution of a physical projection, with its own evaluator and buffer.
pub struct ProjectionExec<C: Columns> {
    meta: ExecutorMeta,
    evaluator_suite: EvaluatorSuite,
    program: Arc<EvaluatorProgram>,
    child: Box<dyn Executor>,
    ctx: C,
    child_chunk: Chunk,
    /// Go `numWorkers`: the projection workers, zero for the serial path.
    num_workers: usize,
    parallel: Option<ParallelProjection<C>>,
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
        let program = Arc::new(EvaluatorProgram::new(exprs, avoid_column_evaluator));
        let evaluator_suite = EvaluatorSuite::from_program(Arc::clone(&program));
        ProjectionExec {
            meta,
            evaluator_suite,
            program,
            child,
            ctx,
            child_chunk,
            num_workers: 0,
            parallel: None,
        }
    }

    /// Go `newProjectionExec`'s `numWorkers`: the builder passes
    /// `tidb_projection_concurrency` workers, or zero for the serial path.
    #[must_use]
    pub fn with_workers(mut self, num_workers: usize) -> Self {
        self.num_workers = num_workers;
        self
    }

    fn evaluator_error(error: EvaluatorError) -> ExecError {
        match error {
            EvaluatorError::Eval(error) => ExecError::Eval(error),
            EvaluatorError::Chunk(message) => ExecError::internal(message),
        }
    }
}

impl<C: Columns + Clone + Send + Sync + 'static> ProjectionExec<C> {
    /// Go `parallelExecute`: the next finished output in fetch order, fetching
    /// and dispatching further child chunks while a free input and output
    /// chunk exist (`projectionInputFetcher.run`), else waiting on a worker.
    fn parallel_next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        let max_chunk_size = self.meta.max_chunk_size();
        req.grow_and_reset(max_chunk_size);
        if self.parallel.is_none() {
            let (result_tx, result_rx) = std::sync::mpsc::channel();
            self.parallel = Some(ParallelProjection {
                shared: Arc::new(ParallelProjectionShared {
                    program: Arc::clone(&self.program),
                    ctx: self.ctx.clone(),
                }),
                result_tx,
                result_rx,
                reorder: BTreeMap::new(),
                ready: VecDeque::new(),
                free_inputs: Vec::new(),
                free_outputs: Vec::new(),
                inputs_allocated: 0,
                outputs_allocated: 0,
                next_seq: 0,
                next_release: 0,
                in_flight: 0,
                child_done: false,
            });
        }
        loop {
            self.collect_parallel_results(false)?;
            let pipeline = self
                .parallel
                .as_mut()
                .expect("the parallel projection is created above");
            if let Some(mut output) = pipeline.ready.pop_front() {
                // Go `chk.SwapColumns(output.chk); e.fetcher.outputCh <- output`.
                req.swap_columns(&mut output);
                output.reset();
                pipeline.free_outputs.push(output);
                return Ok(());
            }
            if !pipeline.child_done {
                if let Some((input, output)) = self.take_parallel_chunks() {
                    self.fetch_and_dispatch_parallel(input, output, req.required_rows())?;
                    continue;
                }
            }
            let pipeline = self
                .parallel
                .as_ref()
                .expect("the parallel projection is created above");
            if pipeline.in_flight > 0 {
                self.collect_parallel_results(true)?;
                continue;
            }
            // The child is drained and every worker has reported: EOF.
            return Ok(());
        }
    }

    /// A free input and a free output chunk, or `None` when Go's fetcher would
    /// block on `inputCh`/`outputCh`: all `numWorkers` of either are with a
    /// worker or waiting for the parent.
    fn take_parallel_chunks(&mut self) -> Option<(Chunk, Chunk)> {
        let num_workers = self.num_workers.max(1);
        let pipeline = self.parallel.as_mut()?;
        let input = match pipeline.free_inputs.pop() {
            Some(input) => Some(input),
            None if pipeline.inputs_allocated < num_workers => {
                pipeline.inputs_allocated += 1;
                None
            }
            None => return None,
        };
        let output = match pipeline.free_outputs.pop() {
            Some(output) => Some(output),
            None if pipeline.outputs_allocated < num_workers => {
                pipeline.outputs_allocated += 1;
                None
            }
            None => {
                if let Some(input) = input {
                    pipeline.free_inputs.push(input);
                } else {
                    pipeline.inputs_allocated -= 1;
                }
                return None;
            }
        };
        let input = input.unwrap_or_else(|| self.child.new_chunk());
        let output = output.unwrap_or_else(|| self.meta.new_chunk());
        Some((input, output))
    }

    /// Go `projectionInputFetcher.run` for one chunk: pull it from the child
    /// with the parent's required rows, end the fetch on an empty chunk,
    /// otherwise hand the pair to a pool worker (`projectionWorker.run`).
    fn fetch_and_dispatch_parallel(
        &mut self,
        mut input: Chunk,
        output: Chunk,
        required_rows: usize,
    ) -> Result<(), ExecError> {
        let max_chunk_size = self.meta.max_chunk_size();
        input.reset();
        input.set_required_rows(
            isize::try_from(required_rows).unwrap_or(isize::MAX),
            max_chunk_size,
        );
        self.child.next(&mut input)?;
        let pipeline = self
            .parallel
            .as_mut()
            .expect("the parallel projection is created before a fetch");
        if input.num_rows() == 0 {
            pipeline.child_done = true;
            pipeline.free_inputs.push(input);
            pipeline.free_outputs.push(output);
            return Ok(());
        }
        let seq = pipeline.next_seq;
        pipeline.next_seq += 1;
        pipeline.in_flight += 1;
        let shared = Arc::clone(&pipeline.shared);
        let result_tx = pipeline.result_tx.clone();
        crate::worker_pool::enqueue_public(Box::new(move || {
            let mut input = input;
            let mut output = output;
            let suite = EvaluatorSuite::from_program(Arc::clone(&shared.program));
            let result = suite
                .run(&shared.ctx, &mut input, &mut output)
                .map(|()| (input, output))
                .map_err(Self::evaluator_error);
            // A dropped receiver means the projection is already closed.
            let _ = result_tx.send((seq, result));
        }));
        Ok(())
    }

    /// Takes every finished pair (one of them blocking when asked), then
    /// releases outputs in fetch order and returns their inputs to the pool.
    fn collect_parallel_results(&mut self, block: bool) -> Result<(), ExecError> {
        let Some(pipeline) = self.parallel.as_mut() else {
            return Ok(());
        };
        let mut block = block && pipeline.in_flight > 0;
        loop {
            let message = if block {
                block = false;
                pipeline.result_rx.recv().map_err(|_| ())
            } else {
                pipeline.result_rx.try_recv().map_err(|_| ())
            };
            let Ok((seq, result)) = message else {
                break;
            };
            pipeline.in_flight = pipeline.in_flight.saturating_sub(1);
            pipeline.reorder.insert(seq, result?);
        }
        while let Some((mut input, output)) = pipeline.reorder.remove(&pipeline.next_release) {
            pipeline.next_release += 1;
            input.reset();
            pipeline.free_inputs.push(input);
            pipeline.ready.push_back(output);
        }
        Ok(())
    }
}

impl<C: ProjectionContext> Executor for ProjectionExec<C> {
    fn open(&mut self) -> Result<(), ExecError> {
        self.child.open()?;
        self.child_chunk.reset();
        self.parallel = None;
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        // Go `isUnparallelExec`: `numWorkers <= 0` runs on the caller. A
        // zero-column projection carries only a virtual row count, which the
        // serial path below preserves.
        if self.num_workers > 0 && !self.meta.ret_field_types().is_empty() {
            if let Some(result) = C::parallel_next_bridge(self, req) {
                return result;
            }
        }
        let max_chunk_size = self.max_chunk_size();
        // Go calls GrowAndReset before reading RequiredRows. Growing restores
        // the maximum demand, while an ordinary reset preserves the parent's
        // current request.
        req.grow_and_reset(max_chunk_size);
        let required_rows = isize::try_from(req.required_rows()).unwrap_or(isize::MAX);
        self.child_chunk
            .set_required_rows(required_rows, max_chunk_size);
        self.child.next(&mut self.child_chunk)?;
        let child_rows = self.child_chunk.num_rows();
        if child_rows == 0 {
            return Ok(());
        }
        // Go's chunk keeps an explicit virtual-row count when its schema has
        // no columns. Column pruning can legitimately reduce a Projection to
        // zero expressions below COUNT(*); it still preserves one output row
        // per child row. The evaluator has no column on which to record that
        // cardinality, so carry it explicitly instead of turning the batch
        // into an EOF marker.
        if self.meta.ret_field_types().is_empty() {
            req.set_num_virtual_rows(child_rows);
            return Ok(());
        }
        self.evaluator_suite
            .run(&self.ctx, &mut self.child_chunk, req)
            .map_err(Self::evaluator_error)
    }

    fn close(&mut self) -> Result<(), ExecError> {
        // Go closes `finishCh` and waits for the fetcher and workers; a task
        // still running here finds its receiver gone and drops its chunks.
        self.parallel = None;
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

    /// Rows `(i, 2i)` for `i` in `0..total`, `chunk_rows` per chunk, noting
    /// the required rows of every `next` call.
    struct NumberSource {
        meta: ExecutorMeta,
        next: i64,
        total: i64,
        chunk_rows: usize,
        required: std::rc::Rc<std::cell::RefCell<Vec<usize>>>,
    }

    impl NumberSource {
        fn new(
            total: i64,
            chunk_rows: usize,
        ) -> (Box<Self>, std::rc::Rc<std::cell::RefCell<Vec<usize>>>) {
            let required = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
            let mut first = Column::new(1, long());
            first.index = 0;
            let mut second = Column::new(2, long());
            second.index = 1;
            let source = Box::new(NumberSource {
                meta: ExecutorMeta::new(Schema::new(vec![first, second]), 7, chunk_rows, 1024),
                next: 0,
                total,
                chunk_rows,
                required: std::rc::Rc::clone(&required),
            });
            (source, required)
        }
    }

    impl Executor for NumberSource {
        fn open(&mut self) -> Result<(), ExecError> {
            self.next = 0;
            Ok(())
        }
        fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
            req.reset();
            self.required.borrow_mut().push(req.required_rows());
            let end = (self.next + self.chunk_rows as i64).min(self.total);
            while self.next < end {
                req.append_int64(0, self.next);
                req.append_int64(1, self.next * 2);
                self.next += 1;
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

    /// `col0 + col1` and `col1` over the number source: what the projection
    /// answers with `workers` workers, as rows in output order, plus the
    /// required rows its child saw.
    fn project_numbers(
        total: i64,
        chunk_rows: usize,
        workers: usize,
        parent_required_rows: usize,
    ) -> (Vec<(i64, i64)>, Vec<usize>) {
        let (source, required) = NumberSource::new(total, chunk_rows);
        let mut first = Column::new(1, long());
        first.index = 0;
        let mut second = Column::new(2, long());
        second.index = 1;
        let plus = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("plus"),
            long(),
            vec![
                Expression::Column(first),
                Expression::Column(second.clone()),
            ],
        ));
        let mut projection = ProjectionExec::new(
            ExecutorMeta::new(
                Schema::new(vec![Column::new(3, long()), Column::new(4, long())]),
                8,
                chunk_rows,
                1024,
            ),
            vec![plus, Expression::Column(second)],
            source,
            NoColumns,
        )
        .with_workers(workers);
        projection.open().unwrap();
        let mut rows = Vec::new();
        loop {
            let mut chunk = projection.new_chunk();
            chunk.set_required_rows(parent_required_rows as isize, 1024);
            projection.next(&mut chunk).unwrap();
            if chunk.num_rows() == 0 {
                break;
            }
            for row in 0..chunk.num_rows() {
                let row = chunk.get_row(row);
                rows.push((row.get_int64(0), row.get_int64(1)));
            }
        }
        projection.close().unwrap();
        let required = required.borrow().clone();
        (rows, required)
    }

    /// Go `projectionWorker`s evaluate chunks concurrently while
    /// `parallelExecute` hands them back in fetch order: the parallel answer
    /// is the serial answer, row for row.
    #[test]
    fn projection_workers_answer_the_serial_rows_in_order() {
        let expected: Vec<(i64, i64)> = (0..5_003).map(|i| (i * 3, i * 2)).collect();
        let (serial, _) = project_numbers(5_003, 128, 0, 1024);
        assert_eq!(serial, expected);
        for workers in [1, 3, 8] {
            let (parallel, _) = project_numbers(5_003, 128, workers, 1024);
            assert_eq!(parallel, expected, "workers={workers}");
        }
    }

    /// Go `TestProjectionParallelRequiredRows`: the fetcher forwards the
    /// parent's required rows (`parentReqRows`) to the child on every fetch.
    #[test]
    fn projection_workers_forward_the_parents_required_rows() {
        let (rows, required) = project_numbers(1_000, 1024, 4, 37);
        assert_eq!(rows.len(), 1_000);
        assert!(!required.is_empty());
        assert!(
            required.iter().all(|rows| *rows == 37),
            "every child fetch carries the parent's request: {required:?}"
        );
    }

    #[test]
    fn empty_projection_preserves_virtual_row_count() {
        let dual = TableDualExec::new(ExecutorMeta::new(Schema::new(vec![]), 0, 1, 1024), 1);
        let mut projection = ProjectionExec::new(
            ExecutorMeta::new(Schema::new(vec![]), 1, 3, 1024),
            Vec::new(),
            Box::new(dual),
            NoColumns,
        );

        projection.open().unwrap();
        let mut result = projection.new_chunk();
        projection.next(&mut result).unwrap();
        assert_eq!(result.num_rows(), 1);
        projection.close().unwrap();
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
