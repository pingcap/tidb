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
use std::sync::Arc;
use std::thread::JoinHandle;

use crossbeam_channel::{bounded, select_biased, Receiver, Sender, TryRecvError, TrySendError};
use tidb_chunk::chunk::Chunk;

use super::probe_worker::{ProbeWorkerEvent, ProbeWorkerV2};
use super::HashJoinV2Exec;
use crate::joiner::JoinType;
use crate::{ExecError, Executor, ExecutorMeta, StatementMemory};

pub struct ProbeStage {
    events: Receiver<ProbeWorkerEvent>,
    close_signal: Option<Sender<()>>,
    scan_ready: Option<Sender<()>>,
    recycles: Vec<Sender<Chunk>>,
    inputs: Vec<Sender<Chunk>>,
    resources: VecDeque<(usize, Chunk)>,
    first_fetched: bool,
    workers: Vec<JoinHandle<()>>,
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
        source: &dyn Executor,
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
        source: &dyn Executor,
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
            workers: Vec::with_capacity(concurrency),
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
            if !restored {
                stage
                    .resources
                    .push_back((id, first_chunk.take().unwrap_or_else(|| source.new_chunk())));
            }
            let (run, memory, errors, close) = (
                Arc::clone(&run),
                stage.memory.clone(),
                events_tx.clone(),
                close.clone(),
            );
            let handle = std::thread::Builder::new().name(format!("hash-join-probe-{id}"))
                .spawn(move || {
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
                }).map_err(|error| ExecError::internal(format!("start hash join probe worker: {error}")))?;
            stage.workers.push(handle);
        }
        if skip_probe || restored {
            stage.finish_fetch();
        }
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
    }

    /// Go Next: reset the caller's chunk, swap a result's columns into it,
    /// and recycle the old columns. RequiredRows is a fetch hint for preserved
    /// probe-side outer joins, as in Go; it does not split join-result chunks.
    pub fn next(&mut self, source: &mut dyn Executor, output: &mut Chunk) -> Result<(), ExecError> {
        output.reset();
        if self.finished {
            return Ok(());
        }
        let result = crate::sort_util::recover_worker_panic(|| self.next_inner(source, output));
        if result.is_err() {
            // Preserve the original error; Close only drains and joins workers.
            let _ = self.close();
        }
        result
    }

    fn next_inner(
        &mut self,
        source: &mut dyn Executor,
        output: &mut Chunk,
    ) -> Result<(), ExecError> {
        let killed = self.memory.sql_killer().get_kill_event_chan();
        loop {
            self.memory.check()?;
            // Admit returned input resources before the next output event, so
            // a prolific worker cannot starve other lanes of source chunks.
            if let Some((id, mut chunk)) = self.resources.pop_front() {
                if !std::mem::take(&mut self.first_fetched) {
                    if self.limit_fetch_size {
                        chunk.set_required_rows(
                            output.required_rows() as isize,
                            source.max_chunk_size(),
                        );
                    }
                    source.next(&mut chunk)?;
                }
                self.memory.check()?;
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
                    // A disconnected lane publishes its typed error to events.
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
                        self.resources.push_back((worker_id, chunk));
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
                ProbeWorkerEvent::Error { error, .. } => return Err(error),
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
        for worker in self.workers.drain(..) {
            if let Err(panic) = worker.join() {
                error.get_or_insert_with(|| {
                    ExecError::internal(format!("hash join probe worker panic: {panic:?}"))
                });
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
}

impl Drop for ProbeStage {
    fn drop(&mut self) {
        let _ = self.close();
    }
}
