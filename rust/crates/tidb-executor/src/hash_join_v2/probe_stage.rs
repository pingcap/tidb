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

//! Go initializeForProbe, probe fetcher, Next, waitJoinWorkers and Close.
//! The initial probe chunk is fetched while the build coordinator runs.
//! The parent owns child Open/Close and the built table until this stage closes.

use std::collections::VecDeque;
use std::sync::mpsc::{Receiver as CompletionReceiver, SyncSender};
use std::sync::{Arc, Mutex};

use crossbeam_channel::{bounded, select_biased, Receiver, Sender, TryRecvError, TrySendError};
use tidb_chunk::chunk::Chunk;

use super::probe_worker::{ProbeWorkerEvent, ProbeWorkerV2};
use super::HashJoinV2Exec;
use crate::joiner::JoinType;
use crate::{ExecError, Executor, ExecutorMeta, StatementMemory};

struct FetchRequest {
    worker_id: usize,
    chunk: Chunk,
    required_rows: usize,
}

struct FetcherHandle {
    scheduler: Arc<ProbeFetcher>,
    requests: Option<Sender<FetchRequest>>,
    completion: CompletionReceiver<Option<Box<dyn Executor>>>,
}

struct FetcherState {
    scheduled: bool,
    closed: bool,
    finished: bool,
}

struct ProbeFetcher {
    requests: Receiver<FetchRequest>,
    source: Mutex<Option<Box<dyn Executor>>>,
    // The fetcher holds its own clones of the workers' input senders for the
    // batches it runs. Go's fetcher goroutine closes every worker's
    // probeResultCh when it exits (hash_join_base.go
    // handleProbeSideFetcherPanic, run on every exit via RunWithRecover); the
    // Mutex lets `complete` drop these senders at that same point so workers
    // see the disconnect even though the stage keeps the fetcher handle alive
    // until the workers report Done.
    inputs: Mutex<Vec<Sender<Chunk>>>,
    events: Sender<ProbeWorkerEvent>,
    close: Receiver<()>,
    killed: Receiver<()>,
    memory: StatementMemory,
    limit_fetch_size: bool,
    max_chunk_size: usize,
    state: Mutex<FetcherState>,
    completion: Mutex<Option<SyncSender<Option<Box<dyn Executor>>>>>,
}

enum FetchResult {
    Chunk { worker_id: usize, chunk: Chunk },
    Eof,
    Closed,
}

enum FetchBatchResult {
    Continue,
    Finished { notify: bool },
}

const FETCH_BATCH_SIZE: usize = 8;

pub struct ProbeStage {
    events: Receiver<ProbeWorkerEvent>,
    close_signal: Option<Sender<()>>,
    scan_ready: Option<Sender<()>>,
    recycles: Vec<Sender<Chunk>>,
    inputs: Vec<Sender<Chunk>>,
    resources: VecDeque<(usize, Chunk)>,
    first_fetched: bool,
    workers: Option<Arc<crate::worker_pool::LanePool>>,
    fetcher: Option<FetcherHandle>,
    source: Option<Box<dyn Executor>>,
    probe_done: Vec<bool>,
    done: Vec<bool>,
    source_eof: bool,
    finished: bool,
    limit_fetch_size: bool,
    memory: StatementMemory,
    collisions: u64,
}

impl ProbeStage {
    /// Starts one blocking worker lane per Go join worker. The factory captures
    /// an Arc-owned build state and constructs a borrowing probe inside its lane;
    /// it must call worker.run exactly once.
    pub fn new<F>(
        source: &mut Option<Box<dyn Executor>>,
        output: &ExecutorMeta,
        build: &HashJoinV2Exec,
        memory: StatementMemory,
        run: F,
    ) -> Result<Self, ExecError>
    where
        F: Fn(usize, ProbeWorkerV2, &StatementMemory) + Send + Sync + 'static,
    {
        Self::new_with_spill(source, output, build, memory, false, false, None, run)
    }

    pub fn new_with_spill<F>(
        source: &mut Option<Box<dyn Executor>>,
        output: &ExecutorMeta,
        build: &HashJoinV2Exec,
        memory: StatementMemory,
        spilled: bool,
        restored: bool,
        mut first_chunk: Option<Chunk>,
        run: F,
    ) -> Result<Self, ExecError>
    where
        F: Fn(usize, ProbeWorkerV2, &StatementMemory) + Send + Sync + 'static,
    {
        let concurrency = build.ctx.concurrency;
        assert!(concurrency > 0);
        // Go canSkipProbeIfHashTableIsEmpty / shouldLimitProbeFetchSize.
        let right_build = build.ctx.right_as_build_side;
        let skip_probe = !spilled
            && build.hash_table_context.hash_table.is_hash_table_empty()
            && match build.ctx.join_type {
                JoinType::Inner => true,
                JoinType::LeftOuter => !right_build,
                JoinType::RightOuter | JoinType::SemiJoin => right_build,
                _ => false,
            };
        let limit_fetch_size = Self::should_limit_fetch_size(build.ctx.join_type, right_build);
        let source_max_chunk_size = source
            .as_ref()
            .ok_or_else(|| ExecError::internal("probe source missing before stage start"))?
            .max_chunk_size();
        let (events_tx, events) = bounded(concurrency + 1);
        let (close_signal, close) = bounded(0);
        let (scan_ready, scan) = bounded(0);
        let mut stage = Self {
            events,
            close_signal: Some(close_signal),
            scan_ready: Some(scan_ready),
            recycles: Vec::with_capacity(concurrency),
            inputs: Vec::with_capacity(concurrency),
            resources: VecDeque::with_capacity(concurrency),
            first_fetched: first_chunk.is_some(),
            workers: Some(Arc::new(crate::worker_pool::LanePool::new(
                "hash-join-probe",
                concurrency,
            ))),
            fetcher: None,
            source: None,
            probe_done: vec![false; concurrency],
            done: vec![false; concurrency],
            source_eof: false,
            finished: false,
            limit_fetch_size,
            memory,
            collisions: 0,
        };
        let run = Arc::new(run);
        for id in 0..concurrency {
            let (worker, ports) = ProbeWorkerV2::new(
                id,
                output.new_chunk(),
                events_tx.clone(),
                close.clone(),
                scan.clone(),
            );
            stage.inputs.push(ports.input);
            stage.recycles.push(ports.recycle);
            if !restored && !skip_probe {
                stage.resources.push_back((
                    id,
                    first_chunk.take().unwrap_or_else(|| {
                        source
                            .as_ref()
                            .expect("probe source missing while allocating resources")
                            .new_chunk()
                    }),
                ));
            }
            let (run, memory, errors, close) = (
                Arc::clone(&run),
                stage.memory.clone(),
                events_tx.clone(),
                close.clone(),
            );
            let worker_pool = stage
                .workers
                .as_ref()
                .expect("probe worker pool installed")
                .clone();
            worker_pool
                .submit(move || {
                    let result = crate::sort_util::recover_worker_panic(|| {
                        run(id, worker, &memory);
                        Ok(())
                    });
                    if let Err(error) = result {
                        select_biased! {
                            recv(close) -> _ => {},
                            send(errors, ProbeWorkerEvent::Error { worker_id: id, error }) -> _ => {},
                        }
                    }
                })
                .map_err(|_| ExecError::internal("hash join probe worker pool stopped"))?;
        }
        if skip_probe || restored {
            stage.finish_fetch();
            return Ok(stage);
        }

        // Go's startProbeFetcher owns a long-lived producer. Schedule its
        // bounded fetch batches on the persistent execution pool rather than
        // creating a native thread for every hash join. The completion channel
        // returns the child after the producer has observed Close or EOF.
        let fetch_source = source
            .take()
            .ok_or_else(|| ExecError::internal("probe source missing for fetcher"))?;
        let (requests_tx, requests_rx) = bounded(concurrency);
        let fetch_inputs = stage.inputs.clone();
        let fetch_events = events_tx.clone();
        let fetch_close = close.clone();
        let fetch_memory = stage.memory.clone();
        let fetch_limit = stage.limit_fetch_size;
        let fetch_max_chunk_size = source_max_chunk_size;
        let (completion_tx, completion_rx) = std::sync::mpsc::sync_channel(1);
        let fetcher = Arc::new(ProbeFetcher {
            requests: requests_rx,
            source: Mutex::new(Some(fetch_source)),
            inputs: Mutex::new(fetch_inputs),
            events: fetch_events,
            close: fetch_close,
            killed: stage.memory.sql_killer().get_kill_event_chan(),
            memory: fetch_memory,
            limit_fetch_size: fetch_limit,
            max_chunk_size: fetch_max_chunk_size,
            state: Mutex::new(FetcherState {
                scheduled: false,
                closed: false,
                finished: false,
            }),
            completion: Mutex::new(Some(completion_tx)),
        });
        stage.fetcher = Some(FetcherHandle {
            scheduler: fetcher,
            requests: Some(requests_tx),
            completion: completion_rx,
        });
        Ok(stage)
    }

    pub(super) fn should_limit_fetch_size(join_type: JoinType, right_build: bool) -> bool {
        matches!(
            (join_type, right_build),
            (JoinType::LeftOuter, true) | (JoinType::RightOuter, false)
        )
    }

    fn finish_fetch(&mut self) {
        self.source_eof = true;
        self.resources.clear();
        self.inputs.clear();
        if let Some(fetcher) = self.fetcher.as_mut() {
            fetcher.requests.take();
            fetcher.scheduler.close();
        }
    }

    fn request_fetch(
        &self,
        request: FetchRequest,
        killed: &Receiver<()>,
    ) -> Result<bool, ExecError> {
        let Some(fetcher) = self.fetcher.as_ref() else {
            return Ok(false);
        };
        if fetcher.scheduler.is_finished() {
            return Ok(false);
        }
        let Some(requests) = fetcher.requests.as_ref() else {
            return Ok(false);
        };
        let sent = select_biased! {
            recv(killed) -> _ => {
                self.memory.check()?;
                Err(ExecError::internal("probe kill event without a kill reason"))
            },
            send(requests, request) -> result => Ok(result.is_ok()),
        }?;
        if !sent {
            return Ok(false);
        }
        fetcher.scheduler.schedule();
        Ok(!fetcher.scheduler.is_finished())
    }

    /// Go Next: reset the caller's chunk, swap a result's columns into it,
    /// and recycle the old columns. RequiredRows is a fetch hint for preserved
    /// probe-side outer joins, as in Go; it does not split join-result chunks.
    pub fn next(&mut self, output: &mut Chunk) -> Result<(), ExecError> {
        output.reset();
        if self.finished {
            return Ok(());
        }
        let result = crate::sort_util::recover_worker_panic(|| self.next_inner(output));
        if result.is_err() {
            // Preserve the original error; Close only drains and joins workers.
            let _ = self.close();
        }
        result
    }

    fn next_inner(&mut self, output: &mut Chunk) -> Result<(), ExecError> {
        let killed = self.memory.sql_killer().get_kill_event_chan();
        loop {
            self.memory.check()?;
            // Admit returned input resources before the next output event, so
            // a prolific worker cannot starve other lanes of source chunks.
            if let Some((id, chunk)) = self.resources.pop_front() {
                if std::mem::take(&mut self.first_fetched) {
                    if chunk.num_rows() == 0 {
                        self.finish_fetch();
                    } else if let Some(input) = self.inputs.get(id) {
                        match input.try_send(chunk) {
                            Ok(()) | Err(TrySendError::Disconnected(_)) => {}
                            Err(TrySendError::Full(_)) => {
                                return Err(ExecError::internal(
                                    "probe resource returned before input was consumed",
                                ))
                            }
                        }
                    }
                } else if !self.request_fetch(
                    FetchRequest {
                        worker_id: id,
                        chunk,
                        required_rows: output.required_rows(),
                    },
                    &killed,
                )? {
                    self.finish_fetch();
                }
            }
            let event = match self.events.try_recv() {
                Ok(event) => event,
                Err(TryRecvError::Empty) if !self.resources.is_empty() => continue,
                Err(TryRecvError::Empty) => select_biased! {
                    recv(killed) -> _ => {
                        self.memory.check()?;
                        return Err(ExecError::internal("probe kill event without a kill reason"));
                    },
                    recv(self.events) -> event => event.map_err(|_| ExecError::internal("probe workers exited without completion"))?,
                },
                Err(TryRecvError::Disconnected) => {
                    return Err(ExecError::internal(
                        "probe workers exited without completion",
                    ))
                }
            };
            match event {
                ProbeWorkerEvent::Input { worker_id, chunk } => {
                    if !self.source_eof {
                        if !self.request_fetch(
                            FetchRequest {
                                worker_id,
                                chunk,
                                required_rows: output.required_rows(),
                            },
                            &killed,
                        )? {
                            self.finish_fetch();
                        }
                    }
                }
                ProbeWorkerEvent::Output {
                    worker_id,
                    mut chunk,
                } => {
                    output.swap_columns(&mut chunk);
                    match self.recycles[worker_id].try_send(chunk) {
                        Ok(()) | Err(TrySendError::Disconnected(_)) => {}
                        Err(TrySendError::Full(_)) => {
                            return Err(ExecError::internal("probe output resource returned twice"))
                        }
                    }
                    // The final partial output may outlive its finished worker.
                    return Ok(());
                }
                ProbeWorkerEvent::ProbeDone { worker_id, .. } => {
                    self.probe_done[worker_id] = true;
                    if self.probe_done.iter().all(|&done| done) {
                        self.scan_ready.take(); // broadcast the global probe barrier
                    }
                }
                ProbeWorkerEvent::Done {
                    worker_id,
                    collisions,
                } => {
                    self.done[worker_id] = true;
                    self.collisions += collisions;
                    if self.done.iter().all(|&done| done) {
                        self.close()?;
                        return Ok(());
                    }
                }
                ProbeWorkerEvent::Error { error, .. }
                | ProbeWorkerEvent::FetcherError { error } => return Err(error),
                ProbeWorkerEvent::FetcherDone => self.finish_fetch(),
            }
        }
    }

    /// Close wakes every queue/barrier wait before joining, then drops buffers.
    /// The parent may release the built table and close children after this.
    pub fn close(&mut self) -> Result<(), ExecError> {
        self.close_signal.take();
        self.scan_ready.take();
        self.finish_fetch();
        let mut error = None;
        // The lane handle owns this stage's worker admissions. Dropping it
        // waits for all queued/running workers, then returns the persistent
        // lane set to the process-wide registry for the next join.
        drop(self.workers.take());
        if let Some(fetcher) = self.fetcher.take() {
            match fetcher.completion.recv() {
                Ok(source) => {
                    self.source = source;
                    if self.source.is_none() {
                        error.get_or_insert_with(|| {
                            ExecError::internal("hash join probe fetcher lost its source")
                        });
                    }
                }
                Err(_) => {
                    error.get_or_insert_with(|| {
                        ExecError::internal("hash join probe fetcher exited without completion")
                    });
                }
            }
        }
        self.recycles.clear();
        while self.events.try_recv().is_ok() {}
        self.finished = true;
        error.map_or(Ok(()), Err)
    }

    pub fn collisions(&self) -> u64 {
        self.collisions
    }

    pub fn is_finished(&self) -> bool {
        self.finished
    }

    /// Returns the probe child after the fetcher has been joined by `close`.
    pub fn take_source(&mut self) -> Option<Box<dyn Executor>> {
        self.source.take()
    }
}

impl Drop for ProbeStage {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

impl ProbeFetcher {
    fn schedule(self: &Arc<Self>) {
        let should_schedule = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.scheduled || state.finished {
                false
            } else {
                state.scheduled = true;
                true
            }
        };
        if should_schedule {
            let fetcher = Arc::clone(self);
            crate::worker_pool::enqueue_public(Box::new(move || fetcher.run_batch()));
        }
    }

    fn close(self: &Arc<Self>) {
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.closed = true;
        }
        self.schedule();
    }

    fn is_finished(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .finished
    }

    fn run_batch(self: Arc<Self>) {
        let Some(mut source) = self
            .source
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        else {
            self.report_error(ExecError::internal("probe fetcher source missing"));
            self.complete();
            return;
        };
        let result = crate::sort_util::recover_worker_panic(|| self.run_batch_inner(&mut *source));
        *self
            .source
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(source);
        match result {
            Ok(FetchBatchResult::Continue) => self.reschedule(),
            Ok(FetchBatchResult::Finished { notify }) => {
                if notify {
                    self.report_done();
                }
                self.complete();
            }
            Err(error) => {
                self.report_error(error);
                self.complete();
            }
        }
    }

    fn run_batch_inner(&self, source: &mut dyn Executor) -> Result<FetchBatchResult, ExecError> {
        for _ in 0..FETCH_BATCH_SIZE {
            if self.is_closed() {
                return Ok(FetchBatchResult::Finished { notify: false });
            }
            let request = match self.requests.try_recv() {
                Ok(request) => request,
                Err(TryRecvError::Empty) => return Ok(FetchBatchResult::Continue),
                Err(TryRecvError::Disconnected) => {
                    return Ok(FetchBatchResult::Finished { notify: false })
                }
            };
            match self.fetch_one(source, request)? {
                FetchResult::Chunk { worker_id, chunk } => {
                    if !self.send_input(worker_id, chunk)? {
                        return Ok(FetchBatchResult::Finished { notify: false });
                    }
                }
                FetchResult::Eof => return Ok(FetchBatchResult::Finished { notify: true }),
                FetchResult::Closed => return Ok(FetchBatchResult::Finished { notify: false }),
            }
        }
        if self.is_closed() {
            return Ok(FetchBatchResult::Finished { notify: false });
        }
        Ok(FetchBatchResult::Continue)
    }

    fn is_closed(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .closed
    }

    fn fetch_one(
        &self,
        source: &mut dyn Executor,
        mut request: FetchRequest,
    ) -> Result<FetchResult, ExecError> {
        if self.is_closed() {
            return Ok(FetchResult::Closed);
        }
        let result = crate::sort_util::recover_worker_panic(|| {
            if self.is_closed() {
                return Ok(FetchResult::Closed);
            }
            self.memory.check()?;
            if self.limit_fetch_size {
                request
                    .chunk
                    .set_required_rows(request.required_rows as isize, self.max_chunk_size);
            }
            source.next(&mut request.chunk)?;
            self.memory.check()?;
            if self.is_closed() {
                Ok(FetchResult::Closed)
            } else if request.chunk.num_rows() == 0 {
                Ok(FetchResult::Eof)
            } else {
                Ok(FetchResult::Chunk {
                    worker_id: request.worker_id,
                    chunk: request.chunk,
                })
            }
        });
        result
    }

    fn report_done(&self) {
        select_biased! {
            recv(self.close) -> _ => {},
            send(self.events, ProbeWorkerEvent::FetcherDone) -> _ => {},
        }
    }

    fn report_error(&self, error: ExecError) {
        select_biased! {
            recv(self.close) -> _ => {},
            send(self.events, ProbeWorkerEvent::FetcherError { error }) -> _ => {},
        }
    }

    fn send_input(&self, worker_id: usize, chunk: Chunk) -> Result<bool, ExecError> {
        let input = {
            let inputs = self
                .inputs
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            inputs.get(worker_id).cloned()
        };
        // `complete` dropped the senders once the source was drained; the
        // worker finishes with its current data, so stop fetching without an
        // error (Go's worker simply stops receiving on the closed channel).
        let Some(input) = input else {
            return Ok(false);
        };
        select_biased! {
            recv(self.close) -> _ => Ok(false),
            recv(self.killed) -> _ => {
                self.memory.check()?;
                Err(ExecError::internal("probe kill event without a kill reason"))
            },
            // A worker drops its input while unwinding, before its recovered
            // error reaches the result channel. Stop fetching and let that
            // worker report the original error instead of racing it with a
            // secondary channel-disconnection error.
            send(input, chunk) -> result => Ok(result.is_ok()),
        }
    }

    fn reschedule(self: &Arc<Self>) {
        let (should_schedule, should_finish) = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.scheduled = false;
            if state.finished || state.closed {
                (false, !state.finished)
            } else if self.requests.is_empty() {
                (false, false)
            } else {
                state.scheduled = true;
                (true, false)
            }
        };
        if should_finish {
            self.complete();
        } else if should_schedule {
            let fetcher = Arc::clone(self);
            crate::worker_pool::enqueue_public(Box::new(move || fetcher.run_batch()));
        }
    }

    fn complete(&self) {
        let source = self
            .source
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.scheduled = false;
            state.finished = true;
        }
        // Go's fetcher goroutine closes every worker's probeResultCh when it
        // exits (hash_join_base.go:88-96, run on every exit via
        // RunWithRecover); without this the workers wait on an open channel
        // and the stage waits for their Done events forever. The batches that
        // race with this clear were spawned under `scheduled = true`, and
        // `complete` only runs at their terminal point, so no later batch can
        // observe the emptied vec.
        self.inputs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
        let completion = self
            .completion
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(completion) = completion {
            let _ = completion.send(source);
        }
    }
}
