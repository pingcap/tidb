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
//! Serial evaluation runs on the caller. Parallel evaluation is Go's: one
//! input-fetcher thread pulls the child ahead of the parent and `numWorkers`
//! worker threads evaluate, circulating bounded input/output chunks.

use tidb_executor_internal_exec::{Executor, ExecutorMeta};
use tidb_driver_error::ExecError;
use crossbeam_channel::{bounded, select_biased, Receiver, Sender};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use tidb_chunk::chunk::Chunk;
use tidb_datatype::FieldType;
use tidb_expr::evaluator::{EvaluatorError, EvaluatorProgram, EvaluatorSuite};
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_exprctx::Columns;

/// Marks an evaluation context [`ProjectionExec`] accepts.
///
/// Go hands its session context to every projection worker goroutine; a Rust
/// context and shared worker program must be `Send + Sync`, including when
/// the whole projection moves with a join's build child. The bridge also
/// supplies the clone/lifetime proof needed by persistent projection workers.
pub trait ProjectionContext: Columns + Send + Sync {
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

impl ProjectionContext for tidb_exprctx::NoColumns {
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

/// Go `projectionInput`: a chunk the fetcher fills and the worker it names
/// evaluates.
struct ProjectionInput {
    chk: Chunk,
    target_worker: usize,
}

/// Go `projectionOutput`: the chunk a worker evaluates into, shared with the
/// parent, which takes it once `done` reports.
struct ProjectionOutput {
    chk: Mutex<Chunk>,
    /// Go `done chan error` (capacity one).
    done: (Sender<Result<(), ExecError>>, Receiver<Result<(), ExecError>>),
}

impl ProjectionOutput {
    fn new(chk: Chunk) -> Arc<Self> {
        Arc::new(Self { chk: Mutex::new(chk), done: bounded(1) })
    }

    fn chk(&self) -> MutexGuard<'_, Chunk> {
        self.chk.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Go `prepare`'s channels and goroutines: the parent reads `output_ch`
/// (`e.outputCh`) and gives each output back on `fetcher_output_ch`; Close
/// closes `finish_ch`, joins the threads and drains every resource channel.
struct ParallelProjection {
    /// Go `finishCh`, closed by dropping the sender.
    finish_ch: Option<Sender<()>>,
    /// Go `e.outputCh` (the fetcher's `globalOutputCh`).
    output_ch: Receiver<Arc<ProjectionOutput>>,
    /// Go `e.fetcher.inputCh`.
    fetcher_input_ch: (Sender<ProjectionInput>, Receiver<ProjectionInput>),
    /// Go `e.fetcher.outputCh`.
    fetcher_output_ch: (Sender<Arc<ProjectionOutput>>, Receiver<Arc<ProjectionOutput>>),
    /// Each worker's Go `inputCh` and `outputCh`.
    worker_chs: Vec<(Receiver<ProjectionInput>, Receiver<Arc<ProjectionOutput>>)>,
    /// Go `e.wg`.
    threads: Vec<std::thread::JoinHandle<()>>,
}

/// Go `readProjection`: the next item, or `None` once `finish_ch` closes or
/// the channel does.
fn read_projection<T>(ch: &Receiver<T>, finish_ch: &Receiver<()>) -> Option<T> {
    select_biased! {
        recv(finish_ch) -> _ => None,
        recv(ch) -> item => item.ok(),
    }
}

/// One execution of a physical projection, with its own evaluator and buffer.
pub struct ProjectionExec<C: Columns> {
    meta: ExecutorMeta,
    evaluator_suite: EvaluatorSuite,
    program: Arc<EvaluatorProgram>,
    /// Shared with the input-fetcher thread while the parallel path runs.
    child: Arc<Mutex<Box<dyn Executor>>>,
    ctx: C,
    child_chunk: Chunk,
    /// Go `numWorkers`: the projection workers, zero for the serial path.
    num_workers: usize,
    /// Go `parentReqRows`, read by the fetcher.
    parent_req_rows: Arc<AtomicI64>,
    parallel: Option<ParallelProjection>,
    /// A projection inside a reader's coprocessor plan, which Go runs in
    /// TiKV: an evaluation failure reaches TiDB as a coprocessor error.
    in_coprocessor: bool,
    /// Go `memTracker` (`memory.NewTracker(e.ID(), -1)`): the chunks the
    /// projection holds -- its child result, or its workers' input and
    /// output chunks.
    mem_tracker: Option<Arc<tidb_memory::Tracker>>,
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
        Self::new_projection_exec(meta, exprs, child, ctx, false)
    }

    /// Go `newProjectionExec`: the physical plan controls column swapping.
    #[must_use]
    pub fn new_projection_exec(
        meta: ExecutorMeta,
        exprs: Vec<Expression>,
        child: Box<dyn Executor>,
        ctx: C,
        avoid_column_evaluator: bool,
    ) -> Self {
        // Go's `childResult` comes from the pool in `open`, and only for
        // the serial path; until then nothing of the pool is held.
        let child_chunk = child.new_first_chunk();
        let program = Arc::new(EvaluatorProgram::new(exprs, avoid_column_evaluator));
        let evaluator_suite = EvaluatorSuite::from_program(Arc::clone(&program));
        ProjectionExec {
            meta,
            evaluator_suite,
            program,
            child: Arc::new(Mutex::new(child)),
            ctx,
            child_chunk,
            num_workers: 0,
            parent_req_rows: Arc::new(AtomicI64::new(0)),
            parallel: None,
            in_coprocessor: false,
            mem_tracker: None,
        }
    }

    /// Go `ProjectionExec.memTracker`, attached to the statement tracker.
    #[must_use]
    pub fn with_mem_tracker(mut self, tracker: Arc<tidb_memory::Tracker>) -> Self {
        self.mem_tracker = Some(tracker);
        self
    }

    fn consume(&self, bytes: i64) {
        consume(self.mem_tracker.as_ref(), bytes);
    }

    fn child(&self) -> MutexGuard<'_, Box<dyn Executor>> {
        self.child.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Go `isUnparallelExec`.
    fn is_unparallel_exec(&self) -> bool {
        self.num_workers == 0 || self.meta.ret_field_types().is_empty()
    }

    /// Marks a projection of a reader's coprocessor plan: its evaluation
    /// failures surface as Go's coprocessor errors
    /// ([`crate::driver::coprocessor_evaluation_error`]).
    #[must_use]
    pub fn in_coprocessor(mut self, in_coprocessor: bool) -> Self {
        self.in_coprocessor = in_coprocessor;
        self
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

/// `memTracker.Consume`, when the projection has a tracker.
fn consume(tracker: Option<&Arc<tidb_memory::Tracker>>, bytes: i64) {
    if let Some(tracker) = tracker {
        tracker.consume(bytes);
    }
}

impl<C: Columns> Drop for ProjectionExec<C> {
    fn drop(&mut self) {
        // Rust callers can drop without an explicit Close. Retire evaluation
        // before field destruction can release the child or statement state.
        if let Some(parallel) = self.parallel.take() {
            parallel.close(self.mem_tracker.as_ref());
        }
        if let Some(tracker) = &self.mem_tracker {
            tracker.replace_bytes_used(0);
        }
    }
}

impl ParallelProjection {
    /// Go `Close`'s parallel half: close `finishCh`, wait for the fetcher and
    /// workers, then drain the fetcher's and each worker's resources.
    fn close(mut self, tracker: Option<&Arc<tidb_memory::Tracker>>) {
        drop(self.finish_ch.take());
        for thread in self.threads.drain(..) {
            let _ = thread.join();
        }
        let drain_inputs = |ch: &Receiver<ProjectionInput>| {
            while let Ok(item) = ch.try_recv() {
                consume(tracker, -item.chk.memory_usage());
            }
        };
        let drain_outputs = |ch: &Receiver<Arc<ProjectionOutput>>| {
            while let Ok(item) = ch.try_recv() {
                consume(tracker, -item.chk().memory_usage());
            }
        };
        // clear fetcher
        drain_inputs(&self.fetcher_input_ch.1);
        drain_outputs(&self.fetcher_output_ch.1);
        // clear workers
        for (input_ch, output_ch) in &self.worker_chs {
            drain_inputs(input_ch);
            drain_outputs(output_ch);
        }
    }
}

impl<C: Columns + Clone + Send + Sync + 'static> ProjectionExec<C> {
    /// Go `parallelExecute`: the next output in fetch order once its worker
    /// (or, at the end, the fetcher) reports it done.
    fn parallel_next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.grow_and_reset(self.meta.max_chunk_size());
        self.parent_req_rows
            .store(i64::try_from(req.required_rows()).unwrap_or(i64::MAX), Ordering::SeqCst);
        if self.parallel.is_none() {
            self.prepare()?;
        }
        let parallel = self.parallel.as_ref().expect("the parallel projection is prepared above");
        let Ok(output) = parallel.output_ch.recv() else {
            return Ok(());
        };
        let fetcher_output_ch = parallel.fetcher_output_ch.0.clone();
        if let Ok(err) = output.done.1.recv() {
            err?;
        }
        let m_size = {
            let mut chk = output.chk();
            let m_size = chk.memory_usage();
            req.swap_columns(&mut chk);
            chk.memory_usage() - m_size
        };
        self.consume(m_size);
        let _ = fetcher_output_ch.send(output);
        Ok(())
    }

    /// Go `prepare`: one input and one output chunk per worker on the
    /// fetcher's channels, then the fetcher and worker goroutines.
    fn prepare(&mut self) -> Result<(), ExecError> {
        let num_workers = self.num_workers.max(1);
        let (finish_tx, finish_rx) = bounded::<()>(0);
        let output_ch = bounded::<Arc<ProjectionOutput>>(num_workers);
        let fetcher_input_ch = bounded::<ProjectionInput>(num_workers);
        let fetcher_output_ch = bounded::<Arc<ProjectionOutput>>(num_workers);
        let mut worker_chs = Vec::with_capacity(num_workers);
        let mut worker_txs = Vec::with_capacity(num_workers);
        for i in 0..num_workers {
            let input_ch = bounded::<ProjectionInput>(1);
            let output_ch = bounded::<Arc<ProjectionOutput>>(1);
            worker_txs.push((input_ch.0, output_ch.0));
            worker_chs.push((input_ch.1, output_ch.1));

            let input_chk = tidb_executor_internal_exec::new_first_chunk(self.child().as_ref());
            self.consume(input_chk.memory_usage());
            fetcher_input_ch
                .0
                .send(ProjectionInput { chk: input_chk, target_worker: i })
                .expect("the fetcher's input channel holds every worker's input");

            let output_chk = self.meta.new_first_chunk();
            self.consume(output_chk.memory_usage());
            fetcher_output_ch
                .0
                .send(ProjectionOutput::new(output_chk))
                .expect("the fetcher's output channel holds every worker's output");
        }

        let mut threads = Vec::with_capacity(num_workers + 1);
        let spawn = |threads: &mut Vec<std::thread::JoinHandle<()>>, body: Box<dyn FnOnce() + Send>| {
            std::thread::Builder::new()
                .spawn(tidb_executor_internal_exec::ruv2::inherit_goroutine(body))
                .map(|handle| threads.push(handle))
                .map_err(|error| ExecError::internal(format!("projection thread creation failure: {error}")))
        };

        let fetcher = ProjectionInputFetcher {
            child: Arc::clone(&self.child),
            mem_tracker: self.mem_tracker.clone(),
            parent_req_rows: Arc::clone(&self.parent_req_rows),
            max_chunk_size: self.meta.max_chunk_size(),
            global_finish_ch: finish_rx.clone(),
            global_output_ch: output_ch.0,
            input_ch: fetcher_input_ch.1.clone(),
            output_ch: fetcher_output_ch.1.clone(),
            worker_chs: worker_txs,
        };
        let mut parallel = ParallelProjection {
            finish_ch: Some(finish_tx),
            output_ch: output_ch.1,
            fetcher_input_ch,
            fetcher_output_ch,
            worker_chs,
            threads: Vec::new(),
        };
        let mut result = spawn(&mut threads, Box::new(move || fetcher.run()));
        for (input_ch, output_ch) in parallel.worker_chs.iter().map(|(i, o)| (i.clone(), o.clone())) {
            if result.is_err() {
                break;
            }
            let worker = ProjectionWorker {
                program: Arc::clone(&self.program),
                ctx: self.ctx.clone(),
                in_coprocessor: self.in_coprocessor,
                mem_tracker: self.mem_tracker.clone(),
                global_finish_ch: finish_rx.clone(),
                input_give_back_ch: parallel.fetcher_input_ch.0.clone(),
                input_ch,
                output_ch,
            };
            result = spawn(&mut threads, Box::new(move || worker.run()));
        }
        parallel.threads = threads;
        self.parallel = Some(parallel);
        result
    }
}

/// Go `projectionInputFetcher`.
struct ProjectionInputFetcher {
    child: Arc<Mutex<Box<dyn Executor>>>,
    mem_tracker: Option<Arc<tidb_memory::Tracker>>,
    parent_req_rows: Arc<AtomicI64>,
    max_chunk_size: usize,
    global_finish_ch: Receiver<()>,
    global_output_ch: Sender<Arc<ProjectionOutput>>,
    input_ch: Receiver<ProjectionInput>,
    output_ch: Receiver<Arc<ProjectionOutput>>,
    /// Each worker's Go `inputCh` and `outputCh`, sending ends.
    worker_chs: Vec<(Sender<ProjectionInput>, Sender<Arc<ProjectionOutput>>)>,
}

impl ProjectionInputFetcher {
    /// Go `projectionInputFetcher.run`: takes a free input and output, hands
    /// the output to the parent, fills the input from the child, and
    /// dispatches both to the input's worker -- until the child is drained,
    /// fails, or the projection closes. Ends by closing `globalOutputCh`
    /// (dropping its sender).
    fn run(self) {
        let mut output: Option<Arc<ProjectionOutput>> = None;
        let run = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.fetch(&mut output)));
        if let Err(payload) = run {
            recovery_projection(output.as_deref(), payload.as_ref());
        }
    }

    fn fetch(&self, current_output: &mut Option<Arc<ProjectionOutput>>) {
        loop {
            let Some(mut input) = read_projection(&self.input_ch, &self.global_finish_ch) else {
                return;
            };
            let target_worker = input.target_worker;

            let Some(output) = read_projection(&self.output_ch, &self.global_finish_ch) else {
                consume(self.mem_tracker.as_ref(), -input.chk.memory_usage());
                return;
            };
            *current_output = Some(Arc::clone(&output));

            if self.global_output_ch.send(Arc::clone(&output)).is_err() {
                return;
            }

            let required_rows = self.parent_req_rows.load(Ordering::SeqCst);
            input.chk.reset();
            input
                .chk
                .set_required_rows(isize::try_from(required_rows).unwrap_or(isize::MAX), self.max_chunk_size);
            let m_size = input.chk.memory_usage();
            let err = self.child.lock().unwrap_or_else(PoisonError::into_inner).next(&mut input.chk);
            consume(self.mem_tracker.as_ref(), input.chk.memory_usage() - m_size);
            if err.is_err() || input.chk.num_rows() == 0 {
                let _ = output.done.0.send(err);
                consume(self.mem_tracker.as_ref(), -input.chk.memory_usage());
                return;
            }

            let (input_ch, output_ch) = &self.worker_chs[target_worker];
            if input_ch.send(input).is_err() || output_ch.send(output).is_err() {
                return;
            }
            *current_output = None;
        }
    }
}

/// Go `projectionWorker`.
struct ProjectionWorker<C> {
    program: Arc<EvaluatorProgram>,
    ctx: C,
    in_coprocessor: bool,
    mem_tracker: Option<Arc<tidb_memory::Tracker>>,
    global_finish_ch: Receiver<()>,
    input_give_back_ch: Sender<ProjectionInput>,
    input_ch: Receiver<ProjectionInput>,
    output_ch: Receiver<Arc<ProjectionOutput>>,
}

impl<C: Columns + Send + Sync> ProjectionWorker<C> {
    /// Go `projectionWorker.run`: evaluates each input into its output,
    /// reports the output done, and gives the input back to the fetcher --
    /// until an evaluation fails or the projection closes.
    fn run(self) {
        let suite = EvaluatorSuite::from_program(Arc::clone(&self.program));
        let mut output: Option<Arc<ProjectionOutput>> = None;
        let run = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.work(&suite, &mut output)));
        if let Err(payload) = run {
            recovery_projection(output.as_deref(), payload.as_ref());
        }
    }

    fn work(&self, suite: &EvaluatorSuite, current_output: &mut Option<Arc<ProjectionOutput>>) {
        loop {
            let Some(mut input) = read_projection(&self.input_ch, &self.global_finish_ch) else {
                return;
            };
            let Some(output) = read_projection(&self.output_ch, &self.global_finish_ch) else {
                return;
            };
            *current_output = Some(Arc::clone(&output));

            let err = {
                let mut chk = output.chk();
                let m_size = chk.memory_usage() + input.chk.memory_usage();
                let err = suite.run(&self.ctx, &mut input.chk, &mut chk);
                consume(self.mem_tracker.as_ref(), chk.memory_usage() + input.chk.memory_usage() - m_size);
                err
            };
            let in_coprocessor = self.in_coprocessor;
            let err = err.map_err(|error| match error {
                EvaluatorError::Eval(error) if in_coprocessor => crate::driver::coprocessor_evaluation_error(error),
                EvaluatorError::Eval(error) => ExecError::Eval(error),
                EvaluatorError::Chunk(message) => ExecError::internal(message),
            });
            let failed = err.is_err();
            let _ = output.done.0.send(err);
            *current_output = None;

            if failed {
                return;
            }

            if self.input_give_back_ch.send(input).is_err() {
                return;
            }
        }
    }
}

/// Go `recoveryProjection`: the panicking goroutine's current output reports
/// the recovered error.
fn recovery_projection(output: Option<&ProjectionOutput>, payload: &(dyn std::any::Any + Send)) {
    let message = payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&str>().copied())
        .unwrap_or("projection executor panicked")
        .to_owned();
    if let Some(output) = output {
        let _ = output.done.0.send(Err(ExecError::internal(message.clone())));
    }
    tidb_logutil::bg_logger().warn(
        "projection executor panicked",
        &[tidb_log::Field::new("error", tidb_log::Value::Str(message))],
    );
}

impl<C: ProjectionContext> ProjectionExec<C> {
    /// Go `ProjectionExec.open`: the executor's own state over a child that
    /// is already open (`buildProjectionForIndexJoin`).
    pub(crate) fn open_self(&mut self) -> Result<(), ExecError> {
        if let Some(parallel) = self.parallel.take() {
            parallel.close(self.mem_tracker.as_ref());
        }
        self.child_chunk.reset();
        if self.is_unparallel_exec() {
            let child_chunk = self.child().new_chunk();
            self.child_chunk = child_chunk;
            self.consume(self.child_chunk.memory_usage());
        }
        Ok(())
    }
}

impl<C: ProjectionContext> Executor for ProjectionExec<C> {
    /// Go `skipAdaptiveLimitProjections`: a projection preserves row
    /// cardinality, so the adaptive LIMIT budget passes to its child.
    fn adaptive_limit_node(&mut self) -> Option<&mut dyn std::any::Any> {
        Arc::get_mut(&mut self.child)?.get_mut().ok()?.adaptive_limit_node()
    }

    /// Go `ProjectionExec.Detach`: not when its `evaluatorSuit` requires any
    /// optional evaluation property, then its child.
    fn detach(&mut self) -> bool {
        self.evaluator_suite.required_optional_eval_props().is_empty() && self.child().detach()
    }

    fn open(&mut self) -> Result<(), ExecError> {
        if let Some(parallel) = self.parallel.take() {
            parallel.close(self.mem_tracker.as_ref());
        }
        self.child().open()?;
        self.child_chunk.reset();
        // Go `open`: the serial path takes its child result from the pool
        // (`TryNewCacheChunk`) and charges it as it stands.
        if self.is_unparallel_exec() {
            let child_chunk = self.child().new_chunk();
            self.child_chunk = child_chunk;
            self.consume(self.child_chunk.memory_usage());
        }
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        // Go `isUnparallelExec`: `numWorkers <= 0` runs on the caller. A
        // zero-column projection carries only a virtual row count, which the
        // serial path below preserves.
        if !self.is_unparallel_exec() {
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
        let m_size = self.child_chunk.memory_usage();
        let fetched = self.child.lock().unwrap_or_else(PoisonError::into_inner).next(&mut self.child_chunk);
        self.consume(self.child_chunk.memory_usage() - m_size);
        fetched?;
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
        let in_coprocessor = self.in_coprocessor;
        self.evaluator_suite
            .run(&self.ctx, &mut self.child_chunk, req)
            .map_err(|error| match error {
                EvaluatorError::Eval(error) if in_coprocessor => {
                    crate::driver::coprocessor_evaluation_error(error)
                }
                error => Self::evaluator_error(error),
            })
    }

    fn close(&mut self) -> Result<(), ExecError> {
        // Go closes finishCh, waits for the fetcher and workers, drains their
        // resources, then closes the child. Releasing every chunk still held
        // leaves the tracker at zero.
        if let Some(parallel) = self.parallel.take() {
            parallel.close(self.mem_tracker.as_ref());
        }
        if let Some(tracker) = &self.mem_tracker {
            tracker.replace_bytes_used(0);
        }
        self.child().close()
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
    use tidb_exprctx::NoColumns;

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
        let mut deferred = tidb_expr::new_function_base(
            &tidb_exprctx::NoColumns,
            "plus",
            long(),
            vec![parameter(0), int_const(0)],
        ).unwrap();
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
                            let mut input = Chunk::new_empty_chunk(&[]);
                            input.set_num_virtual_rows(3);
                            let mut output = Chunk::new_chunk_with_capacity(&[long()], 3);
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
                tidb_exprctx::ErrorLevel::Warn,
                "12tail",
                Some(Datum::Int(12)),
                1,
            ),
            (tidb_exprctx::ErrorLevel::Error, "12tail", None, 0),
            (
                tidb_exprctx::ErrorLevel::Ignore,
                "12tail",
                Some(Datum::Int(12)),
                0,
            ),
            (tidb_exprctx::ErrorLevel::Warn, "128tail", None, 1),
            (tidb_exprctx::ErrorLevel::Warn, "7", Some(Datum::Int(7)), 0),
        ] {
            let ctx = crate::StmtContext::for_query()
                .with_truncate_level(level)
                .with_prepared_params(vec![Datum::new_string_datum(input)].into());
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
                assert_eq!(warnings[0].code(), 1292);
                assert_eq!(
                    warnings[0].message(),
                    format!("Truncated incorrect DOUBLE value: '{input}'")
                );
            }
        }
        assert!(matches!(expression, Expression::Constant(c) if c.value == Datum::Int(99)));
    }

    #[test]
    fn prepared_lazy_folding_keeps_warnings_execution_local() {
        let division = tidb_expr::new_function_base(
            &tidb_exprctx::NoColumns,
            "intdiv",
            long(),
            vec![int_const(1), parameter(1)],
        ).unwrap();
        let original = tidb_expr::new_function_base(
            &tidb_exprctx::NoColumns,
            "if",
            long(),
            vec![parameter(0), int_const(7), division],
        ).unwrap();
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
                assert_eq!((actual[0].code(), actual[0].message()), (1365, "Division by 0"));
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
        let plus = tidb_expr::new_function_base(
            &tidb_exprctx::NoColumns,
            "plus",
            long(),
            vec![int_const(1), int_const(1)],
        ).unwrap();
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
        required: std::sync::Arc<std::sync::Mutex<Vec<usize>>>,
    }

    impl NumberSource {
        fn new(
            total: i64,
            chunk_rows: usize,
        ) -> (Box<Self>, std::sync::Arc<std::sync::Mutex<Vec<usize>>>) {
            let required = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
            let mut first = Column::new(1, long());
            first.index = 0;
            let mut second = Column::new(2, long());
            second.index = 1;
            let source = Box::new(NumberSource {
                meta: ExecutorMeta::new(Schema::new(vec![first, second]), 7, chunk_rows, 1024),
                next: 0,
                total,
                chunk_rows,
                required: std::sync::Arc::clone(&required),
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
            self.required.lock().unwrap().push(req.required_rows());
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
        let plus = tidb_expr::new_function_base(
            &tidb_exprctx::NoColumns,
            "plus",
            long(),
            vec![
                Expression::Column(first),
                Expression::Column(second.clone()),
            ],
        ).unwrap();
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
        let required = required.lock().unwrap().clone();
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

    #[derive(Default)]
    struct EvaluationGate {
        state: std::sync::Mutex<(usize, bool)>,
        changed: std::sync::Condvar,
        finished: std::sync::atomic::AtomicBool,
    }

    impl EvaluationGate {
        fn wait_for(&self, ready: impl Fn(&(usize, bool)) -> bool) {
            let (state, timeout) = self
                .changed
                .wait_timeout_while(
                    self.state.lock().unwrap(),
                    std::time::Duration::from_secs(5),
                    |state| !ready(state),
                )
                .unwrap();
            assert!(ready(&state), "evaluation gate timed out: {timeout:?}");
        }

        fn release(&self) {
            self.state.lock().unwrap().1 = true;
            self.changed.notify_all();
        }
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum FirstResult {
        Row,
        Error,
        Panic,
        ChildError,
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Retirement {
        Close,
        Reopen,
        Drop,
    }

    #[derive(Clone)]
    struct GatedProjectionContext {
        gate: Arc<EvaluationGate>,
        first_result: FirstResult,
    }

    impl Columns for GatedProjectionContext {
        fn get(&self, _path: &[String]) -> Option<Datum> {
            None
        }

        fn param_value(&self, _order: usize) -> Result<Datum, tidb_exprctx::EvalError> {
            let call = {
                let mut state = self.gate.state.lock().unwrap();
                state.0 += 1;
                state.0
            };
            self.gate.changed.notify_all();
            if call == 1 {
                // Return the first batch only after its sibling is evaluating.
                self.gate.wait_for(|state| state.0 == 2);
                match self.first_result {
                    FirstResult::Error => {
                        return Err(tidb_exprctx::EvalError::Unsupported("evaluation failed"))
                    }
                    FirstResult::Panic => panic!("projection evaluation panic"),
                    FirstResult::Row | FirstResult::ChildError => {}
                }
            } else {
                self.gate.wait_for(|state| state.1);
                self.gate
                    .finished
                    .store(true, std::sync::atomic::Ordering::Release);
            }
            Ok(Datum::Int(7))
        }
    }

    impl ProjectionContext for GatedProjectionContext {
        fn parallel_next_bridge(
            exec: &mut ProjectionExec<Self>,
            req: &mut Chunk,
        ) -> Option<Result<(), ExecError>> {
            Some(exec.parallel_next(req))
        }
    }

    struct LifetimeSource {
        inner: Box<NumberSource>,
        gate: Arc<EvaluationGate>,
        retired: std::sync::mpsc::Sender<bool>,
        opened: bool,
        fail_fetch: bool,
    }

    impl LifetimeSource {
        fn record_retirement(&self) {
            let _ = self.retired.send(
                self.gate
                    .finished
                    .load(std::sync::atomic::Ordering::Acquire),
            );
        }
    }

    impl Drop for LifetimeSource {
        fn drop(&mut self) {
            self.record_retirement();
        }
    }

    impl Executor for LifetimeSource {
        fn open(&mut self) -> Result<(), ExecError> {
            if self.opened {
                self.record_retirement();
            }
            self.opened = true;
            self.inner.open()
        }
        fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
            if self.fail_fetch && self.inner.next == 2 {
                return Err(ExecError::internal("child fetch error"));
            }
            if self.inner.next == 1 {
                // Establish which batch enters the evaluator first, without
                // depending on the global CPU pool's scheduling order.
                self.gate.wait_for(|state| state.0 >= 1);
            }
            self.inner.next(req)
        }
        fn close(&mut self) -> Result<(), ExecError> {
            self.record_retirement();
            Err(ExecError::internal("child close error"))
        }
        fn schema(&self) -> &Schema {
            self.inner.schema()
        }
        fn ret_field_types(&self) -> &[FieldType] {
            self.inner.ret_field_types()
        }
        fn init_cap(&self) -> usize {
            self.inner.init_cap()
        }
        fn max_chunk_size(&self) -> usize {
            self.inner.max_chunk_size()
        }
        fn new_chunk(&self) -> Chunk {
            self.inner.new_chunk()
        }
    }

    fn projection_lifetime_case(first_result: FirstResult, action: Retirement) {
        // These gates deliberately block CPU workers. Keep the cases serial
        // even on a two-core host so one case cannot starve another's sibling.
        static CASE: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _case = CASE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let gate = Arc::new(EvaluationGate::default());
        let (retired, observed) = std::sync::mpsc::channel();
        let (inner, _) = NumberSource::new(2, 1);
        let source = LifetimeSource {
            inner,
            gate: Arc::clone(&gate),
            retired,
            opened: false,
            fail_fetch: first_result == FirstResult::ChildError,
        };
        let mut projection = ProjectionExec::new(
            ExecutorMeta::new(Schema::new(vec![Column::new(3, long())]), 8, 1, 1024),
            vec![parameter(0)],
            Box::new(source),
            GatedProjectionContext {
                gate: Arc::clone(&gate),
                first_result,
            },
        )
        .with_workers(2);
        projection.open().unwrap();
        let mut chunk = projection.new_chunk();
        let result = projection.next(&mut chunk);
        assert_eq!(
            result.is_err(),
            matches!(first_result, FirstResult::Error | FirstResult::Panic)
        );
        if result.is_ok() {
            assert_eq!(chunk.get_row(0).get_int64(0), 7);
        }
        // With a child error, Go reports outputs in fetch order: the gated
        // second output comes before the error the fetcher hit after it, so
        // the error cannot be read before the gate opens
        // (`projection_reports_a_child_error_after_the_outputs_fetched_before_it`);
        // the close below must still join the gated worker first.
        let (started, starting) = std::sync::mpsc::channel();
        let retirement =
            std::thread::spawn(move || {
                started.send(()).unwrap();
                match action {
                    Retirement::Close => assert!(format!("{:?}", projection.close().unwrap_err())
                        .contains("child close error")),
                    Retirement::Reopen => projection.open().unwrap(),
                    Retirement::Drop => {
                        drop(projection);
                        return None;
                    }
                }
                Some(projection)
            });
        starting.recv().unwrap();
        let early = observed.recv_timeout(std::time::Duration::from_millis(200));
        // Always unblock and join before asserting, including on the old code.
        gate.release();
        let mut projection = retirement.join().unwrap();
        let released = early.unwrap_or_else(|_| observed.recv().unwrap());
        assert!(
            released,
            "child retired while projection evaluation was still running"
        );
        if action == Retirement::Reopen {
            let projection = projection.as_mut().unwrap();
            for expected_rows in [1, 1, 0] {
                projection.next(&mut chunk).unwrap();
                assert_eq!(chunk.num_rows(), expected_rows);
            }
            assert!(projection.close().is_err());
        }
        drop(projection);
    }

    #[test]
    fn projection_lifetime_early_close_joins_before_child_close() {
        projection_lifetime_case(FirstResult::Row, Retirement::Close);
    }

    #[test]
    fn projection_lifetime_error_close_joins_before_child_close() {
        projection_lifetime_case(FirstResult::Error, Retirement::Close);
    }

    #[test]
    fn projection_lifetime_panic_close_joins_before_child_close() {
        projection_lifetime_case(FirstResult::Panic, Retirement::Close);
    }

    #[test]
    fn projection_lifetime_reopen_joins_before_child_open() {
        projection_lifetime_case(FirstResult::Row, Retirement::Reopen);
    }

    #[test]
    fn projection_lifetime_drop_joins_before_child_drop() {
        projection_lifetime_case(FirstResult::Row, Retirement::Drop);
    }

    /// Go `projectionInputFetcher.run` sends a child error on the output it
    /// was fetching for, after the outputs it dispatched before: the parent
    /// reads every earlier row first.
    #[test]
    fn projection_reports_a_child_error_after_the_outputs_fetched_before_it() {
        struct FailingSource(Box<NumberSource>);
        impl Executor for FailingSource {
            fn open(&mut self) -> Result<(), ExecError> {
                self.0.open()
            }
            fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
                if self.0.next == 2 {
                    return Err(ExecError::internal("child fetch error"));
                }
                self.0.next(req)
            }
            fn close(&mut self) -> Result<(), ExecError> {
                self.0.close()
            }
            fn schema(&self) -> &Schema {
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
        let (source, _) = NumberSource::new(3, 1);
        let mut projection = ProjectionExec::new(
            ExecutorMeta::new(Schema::new(vec![Column::new(3, long())]), 8, 1, 1024),
            vec![int_const(7)],
            Box::new(FailingSource(source)),
            NoColumns,
        )
        .with_workers(2);
        projection.open().unwrap();
        let mut chunk = projection.new_chunk();
        for _ in 0..2 {
            projection.next(&mut chunk).unwrap();
            assert_eq!(chunk.num_rows(), 1);
            assert_eq!(chunk.get_row(0).get_int64(0), 7);
        }
        assert!(format!("{:?}", projection.next(&mut chunk).unwrap_err()).contains("child fetch error"));
        projection.close().unwrap();
    }

    #[test]
    fn projection_lifetime_child_error_joins_before_child_close() {
        projection_lifetime_case(FirstResult::ChildError, Retirement::Close);
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
        let mut input = Chunk::new_chunk_with_capacity(std::slice::from_ref(&long()), 1);
        input.append_int64(0, 41);

        let mut col = Column::new(7, long());
        col.index = 0;
        let expr = tidb_expr::new_function_base(
            &tidb_exprctx::NoColumns,
            "plus",
            long(),
            vec![Expression::Column(col), int_const(1)],
        ).unwrap();
        // Evaluate directly (the projection's inner loop) to confirm column reads.
        let row = input.get_row(0);
        let out = expr.eval(&NoColumns, row).unwrap();
        assert_eq!(out, Datum::Int(42));
    }
}
