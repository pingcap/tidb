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

//! Go `pkg/executor/aggregate/agg_hash_executor.go`: `HashAggExec`, over
//! Go's `aggfuncs`.
//!
//! The serial `unparallelExec` (an aggregate-local ORDER BY, or both
//! concurrencies 1) keeps one `AggPartialResultMapper` and spills the rows of
//! groups it does not hold once the spill action raises `inSpillMode`. The
//! parallel path is Go's: a data fetcher, `partialConcurrency` partial
//! workers that each shuffle one mapper to every final worker, and
//! `finalConcurrency` final workers that merge and send -- each a thread, as
//! each is a goroutine in Go, connected by Go's channels.

use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crossbeam_channel::{bounded, select, Receiver, Sender};
use tidb_chunk::chunk::Chunk;
use tidb_chunk::chunk_in_disk::DataInDiskByChunks;
use tidb_datatype::FieldType;
use tidb_driver_error::ExecError;
use tidb_executor_aggfuncs::agg_func::{new_agg_partial_result_mapper, AggFunc, AggPartialResultMapper, PartialResult};
use tidb_executor_internal_exec::{Executor, ExecutorMeta};
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_exprctx::Columns;
use tidb_goruntime::GoString;
use tidb_memory::{ActionOnExceed, ArcAction, Tracker};
use tidb_set::StringSetWithMemoryUsage;
use tidb_variable::mem_quota::StatementMemory;

use crate::agg_hash_base_worker::BaseHashAggWorker;
use crate::agg_hash_final_worker::HashAggFinalWorker;
use crate::agg_hash_partial_worker::{HashAggInput, HashAggPartialWorker, PartialWorkerSpillState};
use crate::agg_spill::{AggSpillDiskAction, ParallelAggSpillDiskAction, ParallelHashAggSpillHelper};
use crate::agg_util::{get_group_key, recover_hash_agg, AfFinalResult, GroupKeys, WaitGroup};
use crate::stats::HashAggRuntimeStats;

/// The construction a `HashAggExec` needs from Go's executor builder.
pub struct HashAggExecBuild {
    /// Go `PartialAggFuncs`: every function in the unparallel path, the
    /// partial halves in the parallel one.
    pub partial_agg_funcs: Vec<Box<dyn AggFunc>>,
    /// Go `FinalAggFuncs`.
    pub final_agg_funcs: Vec<Box<dyn AggFunc>>,
    /// Go `GroupByItems`.
    pub group_by_items: Vec<Expression>,
    /// Go `DefaultVal`.
    pub default_val: Option<Chunk>,
    /// Go `IsUnparallelExec`.
    pub is_unparallel_exec: bool,
    /// Go `HasDistinct`.
    pub has_distinct: bool,
    /// Go `HashAggPartialConcurrency()` and `HashAggFinalConcurrency()`.
    pub partial_concurrency: usize,
    pub final_concurrency: usize,
    /// Go `TrackAggregateMemoryUsage`.
    pub track_aggregate_memory_usage: bool,
    /// Go `EnableParallelHashaggSpill`.
    pub enable_parallel_hashagg_spill: bool,
}

/// The parallel path's channels and workers between `Open` and `Close`.
struct ParallelExec<C: Columns + Send + Sync + 'static> {
    /// Go `finishCh`; dropping it is Go's `close(e.finishCh)`.
    finish_ch: Option<Sender<()>>,
    final_output_ch: Receiver<AfFinalResult>,
    /// The workers and the fetcher's ends, started by the first `Next`.
    pending: Option<PendingWorkers<C>>,
}

struct PendingWorkers<C: Columns + Send + Sync + 'static> {
    input_ch: Receiver<HashAggInput>,
    partial_input_chs: Vec<Sender<Option<Chunk>>>,
    final_output_sender: Sender<AfFinalResult>,
    finish_rx: Receiver<()>,
    partial_workers: Vec<HashAggPartialWorker<C>>,
    final_workers: Vec<HashAggFinalWorker<C>>,
    partial_states: Vec<Arc<Mutex<PartialWorkerSpillState>>>,
    inflight_chunk_sync: Arc<WaitGroup>,
}

/// Go `HashAggExec`.
pub struct HashAggExec<C: Columns + Send + Sync + 'static> {
    meta: ExecutorMeta,
    child: Arc<Mutex<Box<dyn Executor>>>,
    child_schema: Schema,
    child_field_types: Vec<FieldType>,
    ctx: Arc<C>,
    memory: StatementMemory,

    partial_agg_funcs: Arc<Vec<Box<dyn AggFunc>>>,
    final_agg_funcs: Arc<Vec<Box<dyn AggFunc>>>,
    group_by_items: Arc<Vec<Expression>>,
    default_val: Option<Chunk>,
    /// Go `IsChildReturnEmpty`.
    is_child_return_empty: bool,
    is_unparallel_exec: bool,
    has_distinct: bool,
    partial_concurrency: usize,
    final_concurrency: usize,
    track_aggregate_memory_usage: bool,

    // The unparallel path.
    partial_result_map: AggPartialResultMapper,
    group_set: StringSetWithMemoryUsage,
    group_keys: Vec<GoString>,
    cursor_4_group_key: usize,
    group_key_buffer: GroupKeys,
    child_result: Chunk,
    prepared: bool,
    executed: bool,
    data_in_disk: Option<DataInDiskByChunks>,
    num_of_spilled_chks: usize,
    offset_of_spilled_chks: usize,
    /// Go `inSpillMode`, raised by the spill action.
    in_spill_mode: Arc<AtomicBool>,
    is_child_drained: bool,
    tmp_chk_for_spill: Chunk,
    spill_action: Option<Arc<AggSpillDiskAction>>,

    // The parallel path.
    parallel: Option<ParallelExec<C>>,
    parallel_exec_valid: bool,
    enable_parallel_hashagg_spill: bool,
    spill_helper: Option<Arc<ParallelHashAggSpillHelper>>,
    parallel_agg_spill_action: Option<Arc<ParallelAggSpillDiskAction>>,

    mem_tracker: Arc<Tracker>,
    disk_tracker: Arc<tidb_disk::Tracker>,
    stats: Option<Arc<HashAggRuntimeStats>>,
    runtime_stats: Option<(Arc<dyn tidb_execdetails::runtime_stats::ExecutorRuntimeStatsSource>, i64)>,
}

impl<C: Columns + Send + Sync + 'static> HashAggExec<C> {
    /// Go `buildHashAggFromChildExec`'s executor.
    #[must_use]
    pub fn new(meta: ExecutorMeta, child: Box<dyn Executor>, ctx: C, memory: StatementMemory, build: HashAggExecBuild) -> Self {
        let child_schema = child.schema().clone();
        let child_field_types = child.ret_field_types().to_vec();
        let child_result = child.new_chunk();
        let tmp_chk_for_spill = child.new_chunk();
        let mem_tracker = memory.operator_tracker(meta.id());
        let disk_tracker = memory.operator_disk_tracker(meta.id());
        Self {
            meta,
            child: Arc::new(Mutex::new(child)),
            child_schema,
            child_field_types,
            ctx: Arc::new(ctx),
            memory,
            partial_agg_funcs: Arc::new(build.partial_agg_funcs),
            final_agg_funcs: Arc::new(build.final_agg_funcs),
            group_by_items: Arc::new(build.group_by_items),
            default_val: build.default_val,
            is_child_return_empty: true,
            is_unparallel_exec: build.is_unparallel_exec,
            has_distinct: build.has_distinct,
            partial_concurrency: build.partial_concurrency,
            final_concurrency: build.final_concurrency,
            track_aggregate_memory_usage: build.track_aggregate_memory_usage,
            partial_result_map: new_agg_partial_result_mapper(),
            group_set: StringSetWithMemoryUsage::new(std::iter::empty::<GoString>()).0,
            group_keys: Vec::new(),
            cursor_4_group_key: 0,
            group_key_buffer: GroupKeys::new(),
            child_result,
            prepared: false,
            executed: false,
            data_in_disk: None,
            num_of_spilled_chks: 0,
            offset_of_spilled_chks: 0,
            in_spill_mode: Arc::new(AtomicBool::new(false)),
            is_child_drained: false,
            tmp_chk_for_spill,
            spill_action: None,
            parallel: None,
            parallel_exec_valid: false,
            enable_parallel_hashagg_spill: build.enable_parallel_hashagg_spill,
            spill_helper: None,
            parallel_agg_spill_action: None,
            mem_tracker,
            disk_tracker,
            stats: None,
            runtime_stats: None,
        }
    }

    /// Go `StmtCtx.RuntimeStatsColl` and the plan `Close` registers
    /// `HashAggRuntimeStats` on.
    pub fn set_runtime_stats(&mut self, source: Option<Arc<dyn tidb_execdetails::runtime_stats::ExecutorRuntimeStatsSource>>, plan_id: i64) {
        self.runtime_stats = source.map(|source| (source, plan_id));
    }

    fn stmt_child(&self) -> std::sync::MutexGuard<'_, Box<dyn Executor>> {
        self.child.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Go `OpenSelf`.
    fn open_self(&mut self) -> Result<(), ExecError> {
        self.prepared = false;
        self.mem_tracker.reset();
        if self.track_aggregate_memory_usage {
            self.mem_tracker.attach_to(self.memory.stmt_tracker());
        }
        if self.is_unparallel_exec {
            self.init_for_unparallel_exec();
            return Ok(());
        }
        self.init_for_parallel_exec()
    }

    /// Go `initForUnparallelExec`.
    fn init_for_unparallel_exec(&mut self) {
        let (group_set, set_size) = StringSetWithMemoryUsage::new(std::iter::empty::<GoString>());
        self.group_set = group_set;
        self.partial_result_map = new_agg_partial_result_mapper();
        self.mem_tracker.consume(self.partial_result_map.bytes() as i64 + set_size);
        self.group_key_buffer = GroupKeys::new();
        let child_result = self.stmt_child().new_chunk();
        self.child_result = child_result;
        self.mem_tracker.consume(self.child_result.memory_usage());

        self.offset_of_spilled_chks = 0;
        self.num_of_spilled_chks = 0;
        self.executed = false;
        self.is_child_drained = false;
        let mut data_in_disk = DataInDiskByChunks::new(self.child_field_types.clone(), "", self.memory.spill_storage());
        let tmp_chk_for_spill = self.stmt_child().new_chunk();
        self.tmp_chk_for_spill = tmp_chk_for_spill;
        if self.track_aggregate_memory_usage && self.memory.tmp_storage_on_oom() {
            self.disk_tracker.reset();
            self.disk_tracker.attach_to(self.memory.stmt_disk_tracker());
            data_in_disk.disk_tracker().attach_to(&self.disk_tracker);
            let (action, in_spill_mode) = AggSpillDiskAction::new(&self.mem_tracker);
            self.in_spill_mode = in_spill_mode;
            self.memory.session_tracker().fallback_old_and_set_new_action_for_soft_limit(Arc::clone(&action) as ArcAction);
            self.spill_action = Some(action);
        }
        self.data_in_disk = Some(data_in_disk);
    }

    /// Go `initForParallelExec`, `initPartialWorkers` and `initFinalWorkers`.
    fn init_for_parallel_exec(&mut self) -> Result<(), ExecError> {
        let (partial_concurrency, final_concurrency) = (self.partial_concurrency, self.final_concurrency);
        if partial_concurrency == 0 || final_concurrency == 0 {
            return Err(ExecError::internal("partialConcurrency or finalConcurrency is 0"));
        }
        self.is_child_return_empty = true;
        let (final_output_sender, final_output_ch) = bounded(final_concurrency + partial_concurrency + 1);
        let (input_sender, input_ch) = bounded::<HashAggInput>(partial_concurrency);
        let (finish_sender, finish_rx) = bounded::<()>(1);
        let partial_input: Vec<(Sender<Option<Chunk>>, Receiver<Option<Chunk>>)> = (0..partial_concurrency).map(|_| bounded(1)).collect();
        let partial_output: Vec<(Sender<AggPartialResultMapper>, Receiver<AggPartialResultMapper>)> =
            (0..final_concurrency).map(|_| bounded(partial_concurrency)).collect();

        let inflight_chunk_sync = WaitGroup::new(0);
        let is_tracker_enabled = self.track_aggregate_memory_usage && self.memory.tmp_storage_on_oom();
        // The intermediate results spill too: one more column.
        let base_ret_type_num = self.meta.ret_field_types().len();
        let mut spill_chunk_field_types = vec![FieldType::new(tidb_datatype::FieldTypeCode::VarString); base_ret_type_num];
        spill_chunk_field_types.push(FieldType::new(tidb_datatype::FieldTypeCode::String));
        let spill_helper = Arc::new(ParallelHashAggSpillHelper::new(
            &self.mem_tracker,
            Arc::clone(&self.partial_agg_funcs),
            Arc::clone(&self.final_agg_funcs),
            spill_chunk_field_types,
            self.meta.init_cap(),
            self.meta.max_chunk_size(),
            self.memory.spill_storage(),
        )?);
        if is_tracker_enabled && self.enable_parallel_hashagg_spill && !self.has_distinct {
            self.disk_tracker.reset();
            self.disk_tracker.attach_to(self.memory.stmt_disk_tracker());
            *spill_helper.disk_tracker.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::clone(&self.disk_tracker));
            let action = ParallelAggSpillDiskAction::new_for_helper(&self.mem_tracker, Arc::clone(&spill_helper));
            self.memory.session_tracker().fallback_old_and_set_new_action_for_soft_limit(Arc::clone(&action) as ArcAction);
            self.parallel_agg_spill_action = Some(action);
        }
        self.spill_helper = Some(Arc::clone(&spill_helper));

        // Go `initRuntimeStats`.
        self.stats = self.runtime_stats.as_ref().map(|_| Arc::new(HashAggRuntimeStats::new(partial_concurrency, final_concurrency)));

        let max_chunk_size = self.meta.max_chunk_size();
        let mut mem_usage = 0;
        let mut partial_workers = Vec::with_capacity(partial_concurrency);
        let mut partial_states = Vec::with_capacity(partial_concurrency);
        for (sender, receiver) in &partial_input {
            let mut partial_results_map = Vec::with_capacity(final_concurrency);
            let mut sz = 0;
            for _ in 0..final_concurrency {
                let r = new_agg_partial_result_mapper();
                sz += r.bytes() as i64;
                partial_results_map.push(r);
            }
            let mut base = BaseHashAggWorker::new(finish_rx.clone(), Arc::clone(&self.partial_agg_funcs), max_chunk_size, Arc::clone(&self.mem_tracker));
            base.stats = self.stats.as_ref().map(|stats| stats.partial_worker());
            let chk = Chunk::new(&self.child_field_types, 0, max_chunk_size);
            mem_usage += chk.memory_usage();
            let state = Arc::new(Mutex::new(PartialWorkerSpillState::new(partial_results_map)));
            partial_states.push(Arc::clone(&state));
            let mut worker = HashAggPartialWorker {
                partial_result_num_in_row: 0,
                state,
                spill_helper: Arc::clone(&spill_helper),
                inflight_chunk_sync: Arc::clone(&inflight_chunk_sync),
                input_closed: false,
                holding_inflight: false,
                base,
                ctx: Arc::clone(&self.ctx),
                input_ch: receiver.clone(),
                input_ch_sender: sender.clone(),
                output_chs: partial_output.iter().map(|(sender, _)| sender.clone()).collect(),
                global_output_ch: final_output_sender.clone(),
                give_back_ch: input_sender.clone(),
                group_by_items: Arc::clone(&self.group_by_items),
                group_key_buf: GroupKeys::new(),
                chk,
            };
            worker.partial_result_num_in_row = worker.base.get_partial_result_slice_len_consider_byte_align();
            // There is a bucket in each empty mapper.
            self.mem_tracker.consume(sz);
            let input = HashAggInput { chk: Chunk::new(&self.child_field_types, 0, max_chunk_size), give_back_ch: sender.clone() };
            mem_usage += input.chk.memory_usage();
            input_sender.send(input).expect("inputCh holds one input per partial worker");
            partial_workers.push(worker);
        }
        self.mem_tracker.consume(mem_usage);

        let mut final_workers = Vec::with_capacity(final_concurrency);
        for (_, receiver) in &partial_output {
            let mut base = BaseHashAggWorker::new(finish_rx.clone(), Arc::clone(&self.final_agg_funcs), max_chunk_size, Arc::clone(&self.mem_tracker));
            base.stats = self.stats.as_ref().map(|stats| stats.final_worker());
            let partial_result_map = new_agg_partial_result_mapper();
            // There is a bucket in the empty mapper.
            self.mem_tracker.consume(partial_result_map.bytes() as i64);
            let (holder_sender, holder_receiver) = bounded(1);
            holder_sender.send(Chunk::new(self.meta.ret_field_types(), 0, max_chunk_size)).expect("an empty holder channel");
            final_workers.push(HashAggFinalWorker {
                base,
                ctx: Arc::clone(&self.ctx),
                partial_result_map,
                input_ch: receiver.clone(),
                output_ch: final_output_sender.clone(),
                final_result_holder_ch: holder_receiver,
                final_result_holder_sender: holder_sender,
                spill_helper: Arc::clone(&spill_helper),
                restored_agg_result_mapper_mem: 0,
            });
        }
        self.parallel = Some(ParallelExec {
            finish_ch: Some(finish_sender),
            final_output_ch,
            pending: Some(PendingWorkers {
                input_ch,
                partial_input_chs: partial_input.into_iter().map(|(sender, _)| sender).collect(),
                final_output_sender,
                finish_rx,
                partial_workers,
                final_workers,
                partial_states,
                inflight_chunk_sync,
            }),
        });
        self.parallel_exec_valid = true;
        self.executed = false;
        Ok(())
    }

    /// Go `prepare4ParallelExec`: the fetcher, the partial workers and the
    /// final workers, each on its own thread.
    fn prepare_4_parallel_exec(&mut self) {
        let Some(pending) = self.parallel.as_mut().and_then(|parallel| parallel.pending.take()) else {
            return;
        };
        let PendingWorkers { input_ch, partial_input_chs, final_output_sender, finish_rx, partial_workers, final_workers, partial_states, inflight_chunk_sync } = pending;
        let spill_helper = Arc::clone(self.spill_helper.as_ref().expect("the parallel path has its spill helper"));

        // Go `fetchChildData`.
        let fetcher = {
            let child = Arc::clone(&self.child);
            let mem_tracker = Arc::clone(&self.mem_tracker);
            let finish_rx = finish_rx.clone();
            let input_ch = input_ch.clone();
            let final_output_sender = final_output_sender.clone();
            // Go hands the fetcher `ctx`: its child's regions open under
            // this `Next`'s.
            let span = tidb_tracing::current_span();
            let acc = tidb_executor_internal_exec::ruv2::current_next_io_acc();
            let inflight_chunk_sync = Arc::clone(&inflight_chunk_sync);
            let spill_helper = Arc::clone(&spill_helper);
            let agg_funcs = Arc::clone(&self.partial_agg_funcs);
            // Go `spill`: every partial worker's maps to disk, in parallel.
            let spill = {
                let spill_helper = Arc::clone(&spill_helper);
                let mem_tracker = Arc::clone(&mem_tracker);
                let final_output_sender = final_output_sender.clone();
                let partial_states = partial_states.clone();
                move || {
                    spill_helper.set_in_spilling();
                    let handles: Vec<_> = partial_states
                        .iter()
                        .map(|state| {
                            let (state, agg_funcs, spill_helper, mem_tracker, final_output_sender) =
                                (Arc::clone(state), Arc::clone(&agg_funcs), Arc::clone(&spill_helper), Arc::clone(&mem_tracker), final_output_sender.clone());
                            std::thread::spawn(move || {
                                let output = final_output_sender.clone();
                                recover_hash_agg(&output, || {
                                    let mut state = state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                                    if let Err(error) = state.spill_data_to_disk(&agg_funcs, &spill_helper, &mem_tracker) {
                                        let _ = final_output_sender.send(AfFinalResult { chk: None, err: Some(error), give_back_ch: None });
                                        spill_helper.set_error();
                                    }
                                });
                            })
                        })
                        .collect();
                    for handle in handles {
                        let _ = handle.join();
                    }
                    spill_helper.set_spill_triggered();
                }
            };
            move || {
                let _span = tidb_tracing::with_thread_span(span);
                let _acc = tidb_executor_internal_exec::ruv2::with_next_io_acc(acc);
                let output = final_output_sender.clone();
                recover_hash_agg(&output, || loop {
                    let mut input = select! {
                        recv(finish_rx) -> _ => break,
                        recv(input_ch) -> input => match input {
                            Ok(input) => input,
                            Err(_) => break,
                        },
                    };
                    let m_size = input.chk.memory_usage();
                    let result = child.lock().unwrap_or_else(std::sync::PoisonError::into_inner).next(&mut input.chk);
                    if let Err(error) = result {
                        let _ = final_output_sender.send(AfFinalResult { chk: None, err: Some(error), give_back_ch: None });
                        mem_tracker.consume(-m_size);
                        break;
                    }
                    if input.chk.num_rows() == 0 {
                        mem_tracker.consume(-m_size);
                        break;
                    }
                    mem_tracker.consume(input.chk.memory_usage() - m_size);
                    inflight_chunk_sync.add();
                    let _ = input.give_back_ch.send(Some(input.chk));
                    // Go `spillIfNeed`.
                    if spill_helper.check_error() {
                        mem_tracker.consume(-m_size);
                        break;
                    }
                    if spill_helper.check_need_spill() {
                        // Wait for every partial worker to finish its chunk.
                        inflight_chunk_sync.wait();
                        spill();
                    }
                });
                // Wait for every partial worker to finish its chunk.
                inflight_chunk_sync.wait();
                if !spill_helper.is_no_spill() && !spill_helper.check_error() {
                    // Spill the remaining data.
                    spill();
                    for state in &partial_states {
                        let ios = std::mem::take(&mut state.lock().unwrap_or_else(std::sync::PoisonError::into_inner).spilled_chunks_io);
                        spill_helper.add_list_in_disks(ios);
                    }
                }
                // An error may leave disk files open.
                for state in &partial_states {
                    for io in &mut state.lock().unwrap_or_else(std::sync::PoisonError::into_inner).spilled_chunks_io {
                        io.close();
                    }
                }
                // Go `close(e.partialInputChs[i])`.
                for partial_input_ch in partial_input_chs {
                    let _ = partial_input_ch.send(None);
                }
            }
        };
        std::thread::spawn(fetcher);

        let partial_wait_group = WaitGroup::new(partial_workers.len());
        let partial_start = Instant::now();
        for worker in partial_workers {
            let wait_group = Arc::clone(&partial_wait_group);
            std::thread::spawn(move || {
                worker.run();
                wait_group.done();
            });
        }
        // Go `waitPartialWorkerAndCloseOutputChs`: the inputs left in
        // `inputCh` release their chunks.
        {
            let wait_group = Arc::clone(&partial_wait_group);
            let mem_tracker = Arc::clone(&self.mem_tracker);
            let stats = self.stats.clone();
            std::thread::spawn(move || {
                wait_group.wait();
                while let Ok(input) = input_ch.try_recv() {
                    mem_tracker.consume(-input.chk.memory_usage());
                }
                if let Some(stats) = stats {
                    stats.add_partial_wall_time(partial_start);
                }
            });
        }

        let final_wait_group = WaitGroup::new(final_workers.len());
        let final_start = Instant::now();
        for worker in final_workers {
            let wait_group = Arc::clone(&final_wait_group);
            let partial_wait_group = Arc::clone(&partial_wait_group);
            std::thread::spawn(move || {
                // Go: a final worker starts once every partial worker ends.
                partial_wait_group.wait();
                worker.run();
                wait_group.done();
            });
        }
        if let Some(stats) = self.stats.clone() {
            let wait_group = Arc::clone(&final_wait_group);
            std::thread::spawn(move || {
                wait_group.wait();
                stats.add_final_wall_time(final_start);
            });
        }
        drop(final_output_sender);
    }

    /// Go `parallelExec`.
    fn parallel_exec(&mut self, chk: &mut Chunk) -> Result<(), ExecError> {
        if !self.prepared {
            self.prepared = true;
            self.prepare_4_parallel_exec();
        }
        if self.executed {
            return Ok(());
        }
        let final_output_ch = self.parallel.as_ref().expect("the parallel path is open").final_output_ch.clone();
        loop {
            let Ok(result) = final_output_ch.recv() else {
                self.executed = true;
                if self.is_child_return_empty {
                    if let Some(default_val) = &self.default_val {
                        chk.append_row(default_val.get_row(0));
                    }
                }
                return Ok(());
            };
            if let Some(error) = result.err {
                // A worker's kill panic carries the kill error's text; the
                // statement's killer holds the error itself.
                return Err(self.memory.check().err().map_or(error, ExecError::from));
            }
            let mut result_chk = result.chk.expect("a final result carries a chunk");
            chk.swap_columns(&mut result_chk);
            result_chk.reset();
            // So that the chunk is reused.
            if let Some(give_back_ch) = result.give_back_ch {
                let _ = give_back_ch.send(result_chk);
            }
            if chk.num_rows() > 0 {
                self.is_child_return_empty = false;
                return Ok(());
            }
        }
    }

    /// Go `unparallelExec`.
    fn unparallel_exec(&mut self, chk: &mut Chunk) -> Result<(), ExecError> {
        chk.reset();
        loop {
            if self.prepared {
                // Return the groups in first-seen order, not the set's.
                while self.cursor_4_group_key < self.group_keys.len() {
                    let group_key = self.group_keys[self.cursor_4_group_key].clone();
                    self.get_partial_results(&group_key);
                    if self.partial_agg_funcs.is_empty() {
                        chk.set_num_virtual_rows(chk.num_rows() + 1);
                    }
                    let partial_results = self.partial_result_map.get_mut(&group_key).expect("the group's results exist");
                    for (af, partial_result) in self.partial_agg_funcs.iter().zip(partial_results.iter_mut()) {
                        af.append_final_result_2_chunk(&*self.ctx, partial_result, chk)?;
                    }
                    self.cursor_4_group_key += 1;
                    if chk.is_full() {
                        return Ok(());
                    }
                }
                self.reset_spill_mode();
            }
            if self.executed {
                return Ok(());
            }
            self.execute()?;
            if self.group_set.is_empty() && self.group_by_items.is_empty() {
                // No group by and no data: one empty group (`select
                // count(c) from t` answers [0]).
                self.mem_tracker.consume(self.group_set.insert(GoString::default()));
                self.group_keys.push(GoString::default());
            }
            self.prepared = true;
        }
    }

    /// Go `resetSpillMode`.
    fn reset_spill_mode(&mut self) {
        self.cursor_4_group_key = 0;
        self.group_keys.clear();
        let (group_set, set_size) = StringSetWithMemoryUsage::new(std::iter::empty::<GoString>());
        self.group_set = group_set;
        self.partial_result_map = new_agg_partial_result_mapper();
        self.prepared = false;
        let num_chunks = self.data_in_disk.as_ref().map_or(0, DataInDiskByChunks::num_chunks);
        // No data spills again: every row is processed.
        self.executed = self.num_of_spilled_chks == num_chunks;
        self.num_of_spilled_chks = num_chunks;
        self.mem_tracker.replace_bytes_used(set_size);
        self.in_spill_mode.store(false, SeqCst);
    }

    /// Go `execute`: fetches the child's chunks and updates each group's
    /// partial results.
    fn execute(&mut self) -> Result<(), ExecError> {
        let result = self.execute_rounds();
        if result.is_ok() && self.tmp_chk_for_spill.num_rows() > 0 {
            if let Some(data_in_disk) = self.data_in_disk.as_mut() {
                data_in_disk.add(&self.tmp_chk_for_spill).map_err(|error| ExecError::internal(error.to_string()))?;
            }
            self.tmp_chk_for_spill.reset();
        }
        result
    }

    fn execute_rounds(&mut self) -> Result<(), ExecError> {
        loop {
            let m_size = self.child_result.memory_usage();
            self.get_next_chunk()?;
            self.mem_tracker.consume(self.child_result.memory_usage() - m_size);

            // No more data.
            if self.child_result.num_rows() == 0 {
                return Ok(());
            }
            get_group_key(&*self.ctx, &self.child_result, &mut self.group_key_buffer, &self.group_by_items)?;

            let mut all_mem_delta = 0;
            let num_rows = self.child_result.num_rows();
            let mut sel = Vec::with_capacity(num_rows);
            for j in 0..num_rows {
                // A copy: the key buffer is reused.
                let group_key = GoString::from(self.group_key_buffer.key(j).to_vec());
                if !self.group_set.contains(&group_key) {
                    if self.in_spill_mode.load(SeqCst) && !self.group_set.is_empty() {
                        sel.push(j);
                        continue;
                    }
                    all_mem_delta += self.group_set.insert(group_key.clone());
                    self.group_keys.push(group_key.clone());
                }
                self.get_partial_results(&group_key);
                let rows = [self.child_result.get_row(j)];
                let partial_results = self.partial_result_map.get_mut(&group_key).expect("the group's results exist");
                for (af, partial_result) in self.partial_agg_funcs.iter().zip(partial_results.iter_mut()) {
                    all_mem_delta += af.update_partial_result(&*self.ctx, &rows, partial_result)?;
                }
            }

            // Spill the unprocessed rows.
            if !sel.is_empty() {
                let is_full_chk = sel.len() == num_rows;
                self.child_result.set_sel(Some(sel));
                self.spill_unprocessed_data(is_full_chk)?;
            }
            self.mem_tracker.consume(all_mem_delta);
        }
    }

    /// Go `spillUnprocessedData`.
    fn spill_unprocessed_data(&mut self, is_full_chk: bool) -> Result<(), ExecError> {
        let data_in_disk = self.data_in_disk.as_mut().expect("a spilling aggregation has its disk");
        let to_exec = |error: tidb_chunk::chunk_in_disk::DiskError| ExecError::internal(error.to_string());
        if is_full_chk {
            return data_in_disk.add(&self.child_result).map_err(to_exec);
        }
        for i in 0..self.child_result.num_rows() {
            self.tmp_chk_for_spill.append_row(self.child_result.get_row(i));
            if self.tmp_chk_for_spill.is_full() {
                data_in_disk.add(&self.tmp_chk_for_spill).map_err(to_exec)?;
                self.tmp_chk_for_spill.reset();
            }
        }
        Ok(())
    }

    /// Go `getNextChunk`: the child's next chunk, then each spilled one.
    fn get_next_chunk(&mut self) -> Result<(), ExecError> {
        self.child_result.reset();
        if !self.is_child_drained {
            let child = Arc::clone(&self.child);
            child.lock().unwrap_or_else(std::sync::PoisonError::into_inner).next(&mut self.child_result)?;
            if self.child_result.num_rows() != 0 {
                return Ok(());
            }
            self.is_child_drained = true;
        }
        if self.offset_of_spilled_chks < self.num_of_spilled_chks {
            let data_in_disk = self.data_in_disk.as_mut().expect("spilled chunks are on disk");
            self.child_result = data_in_disk.get_chunk(self.offset_of_spilled_chks).map_err(|error| ExecError::internal(error.to_string()))?;
            self.offset_of_spilled_chks += 1;
        }
        Ok(())
    }

    /// Go `getPartialResults`: the group's partial results, allocated the
    /// first time the group appears.
    fn get_partial_results(&mut self, group_key: &GoString) {
        if self.partial_result_map.contains_key(group_key) {
            return;
        }
        let mut all_mem_delta = 0;
        let mut partial_results: Vec<PartialResult> = Vec::with_capacity(self.partial_agg_funcs.len());
        for af in self.partial_agg_funcs.iter() {
            let (partial_result, mem_delta) = af.alloc_partial_result();
            partial_results.push(partial_result);
            all_mem_delta += mem_delta;
        }
        let delta_bytes = self.partial_result_map.set(group_key.clone(), partial_results);
        all_mem_delta += group_key.len() as i64;
        if delta_bytes > 0 {
            self.mem_tracker.consume(delta_bytes);
        }
        self.mem_tracker.consume(all_mem_delta);
    }
}

impl<C: Columns + Send + Sync + 'static> Executor for HashAggExec<C> {
    fn agg_tree_input_empty(&self) -> bool {
        self.is_child_return_empty
    }

    fn open(&mut self) -> Result<(), ExecError> {
        self.stmt_child().open()?;
        self.open_self()
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        if self.is_unparallel_exec {
            return self.unparallel_exec(req);
        }
        self.parallel_exec(req)
    }

    fn close(&mut self) -> Result<(), ExecError> {
        if let (Some((source, plan_id)), Some(stats)) = (&self.runtime_stats, self.stats.take()) {
            source.register_stats(*plan_id, Box::new(stats.snapshot()));
        }
        if self.is_unparallel_exec {
            self.group_set = StringSetWithMemoryUsage::new(std::iter::empty::<GoString>()).0;
            self.partial_result_map = new_agg_partial_result_mapper();
            self.group_keys.clear();
            self.mem_tracker.replace_bytes_used(0);
            if let Some(data_in_disk) = self.data_in_disk.as_mut() {
                data_in_disk.close();
            }
            if let Some(action) = self.spill_action.take() {
                action.set_finished();
            }
            self.data_in_disk = None;
            return self.stmt_child().close();
        }
        if self.parallel_exec_valid {
            if let Some(mut parallel) = self.parallel.take() {
                // Go `close(e.finishCh)`, then the channels drain.
                parallel.pending = None;
                parallel.finish_ch = None;
                while parallel.final_output_ch.try_recv().is_ok() {}
            }
            self.executed = false;
            self.mem_tracker.replace_bytes_used(0);
            self.parallel_exec_valid = false;
            if let Some(action) = self.parallel_agg_spill_action.take() {
                action.set_finished();
                if let Some(spill_helper) = self.spill_helper.take() {
                    spill_helper.close();
                }
            }
        }
        self.stmt_child().close()
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
