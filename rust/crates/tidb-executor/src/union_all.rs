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

//! Go `unionexec.UnionExec` (`pkg/executor/unionexec/union.go`).

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::JoinHandle;

use crossbeam_channel::{bounded, select_biased, Receiver, Sender};

use crate::{ExecError, Executor, ExecutorMeta};
use tidb_chunk::chunk::Chunk;
use tidb_datatype::FieldType;
use tidb_expr::schema::Schema;

/// Go `UnionExec`: pulls all its children's output. Its `resultPuller`
/// workers take children from `childIDChan` one at a time, each filling its
/// own result chunk, which `Next` swaps into the parent's chunk; so the rows
/// of different children interleave, and a child never sees the parent's
/// chunk.
pub struct UnionExec {
    meta: ExecutorMeta,
    children: Arc<[Mutex<Box<dyn Executor>>]>,
    concurrency: usize,

    stop_fetch_data: Arc<AtomicBool>,
    /// Go `finished`: closing it (dropping the sender) stops the workers.
    finished: Option<Sender<()>>,
    finished_rx: Option<Receiver<()>>,
    resource_pools: Vec<Sender<Chunk>>,
    result_pool: Option<Receiver<UnionWorkerResult>>,
    workers: Vec<JoinHandle<()>>,
    initialized: bool,
    /// Go `mu.maxOpenedChildID`.
    max_opened_child_id: Arc<Mutex<i64>>,
}

/// Go `unionWorkerResult`.
struct UnionWorkerResult {
    chk: Option<Chunk>,
    err: Option<ExecError>,
    src: usize,
}

impl UnionExec {
    /// Go `buildUnionAll`: `Concurrency` is `UnionConcurrency()`.
    pub fn new(meta: ExecutorMeta, children: Vec<Box<dyn Executor>>, concurrency: usize) -> Self {
        Self {
            meta,
            children: children.into_iter().map(Mutex::new).collect(),
            concurrency,
            stop_fetch_data: Arc::new(AtomicBool::new(false)),
            finished: None,
            finished_rx: None,
            resource_pools: Vec::new(),
            result_pool: None,
            workers: Vec::new(),
            initialized: false,
            max_opened_child_id: Arc::new(Mutex::new(-1)),
        }
    }

    fn initialize(&mut self) -> Result<(), ExecError> {
        if self.concurrency > self.children.len() {
            self.concurrency = self.children.len();
        }
        let Some(finished) = self.finished_rx.clone() else {
            return Err(ExecError::internal("UnionExec.Next before Open"));
        };
        let (result_tx, result_rx) = bounded(self.concurrency);
        self.result_pool = Some(result_rx);
        let (child_id_tx, child_id_rx) = bounded(self.children.len());
        for child_id in 0..self.children.len() {
            child_id_tx.send(child_id).expect("childIDChan holds every child");
        }
        drop(child_id_tx);
        // Go's `NewFirstChunk(e.Children(0))` reads child 0 unlocked; every
        // chunk is made before a worker can hold child 0 and wait on its pool.
        let firsts: Vec<Chunk> = {
            let child = self.children[0].lock().unwrap_or_else(PoisonError::into_inner);
            (0..self.concurrency).map(|_| tidb_executor_internal_exec::new_first_chunk(child.as_ref())).collect()
        };
        for (worker_id, first) in firsts.into_iter().enumerate() {
            let (resource_tx, resource_rx) = bounded(1);
            resource_tx.send(first).expect("a fresh resource pool has room");
            self.resource_pools.push(resource_tx.clone());
            let puller = ResultPuller {
                worker_id,
                children: Arc::clone(&self.children),
                child_ids: child_id_rx.clone(),
                resource_pool: (resource_tx, resource_rx),
                result_pool: result_tx.clone(),
                finished: finished.clone(),
                stop_fetch_data: Arc::clone(&self.stop_fetch_data),
                max_opened_child_id: Arc::clone(&self.max_opened_child_id),
            };
            // Go's `go e.resultPuller(ctx, i)` inherits the statement's
            // profiler labels.
            let spawned = std::thread::Builder::new()
                .spawn(tidb_executor_internal_exec::ruv2::inherit_goroutine(move || puller.run()));
            match spawned {
                Ok(handle) => self.workers.push(handle),
                Err(error) => {
                    self.stop_fetch_data.store(true, Ordering::SeqCst);
                    return Err(ExecError::internal(format!("union thread creation failure: {error}")));
                }
            }
        }
        Ok(())
    }
}

/// Go `resultPuller`'s state: one worker of the union.
struct ResultPuller {
    worker_id: usize,
    children: Arc<[Mutex<Box<dyn Executor>>]>,
    child_ids: Receiver<usize>,
    resource_pool: (Sender<Chunk>, Receiver<Chunk>),
    result_pool: Sender<UnionWorkerResult>,
    finished: Receiver<()>,
    stop_fetch_data: Arc<AtomicBool>,
    max_opened_child_id: Arc<Mutex<i64>>,
}

impl ResultPuller {
    /// Go `resultPuller`, its recovered panic sent as the worker's error.
    fn run(self) {
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| self.pull())) {
            let message = payload
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| payload.downcast_ref::<&str>().copied())
                .unwrap_or("resultPuller panicked");
            self.stop_fetch_data.store(true, Ordering::SeqCst);
            let _ = self.send_result(UnionWorkerResult {
                chk: None,
                err: Some(ExecError::internal(message.to_owned())),
                src: self.worker_id,
            });
        }
    }

    fn pull(&self) {
        for child_id in &self.child_ids {
            {
                let mut max_opened = self.max_opened_child_id.lock().unwrap_or_else(PoisonError::into_inner);
                *max_opened = (*max_opened).max(child_id as i64);
            }
            let mut child = self.children[child_id].lock().unwrap_or_else(PoisonError::into_inner);
            if let Err(err) = child.open() {
                self.stop_fetch_data.store(true, Ordering::SeqCst);
                let result = UnionWorkerResult { chk: None, err: Some(err), src: self.worker_id };
                if !self.send_result(result) {
                    return;
                }
            }
            loop {
                if self.stop_fetch_data.load(Ordering::SeqCst) {
                    return;
                }
                let mut chk = select_biased! {
                    recv(self.finished) -> _ => return,
                    recv(self.resource_pool.1) -> chk => match chk {
                        Ok(chk) => chk,
                        Err(_) => return,
                    },
                };
                let err = child.next(&mut chk).err();
                if err.is_none() && chk.num_rows() == 0 {
                    let _ = self.resource_pool.0.send(chk);
                    break;
                }
                let failed = err.is_some();
                if !self.send_result(UnionWorkerResult { chk: Some(chk), err, src: self.worker_id }) {
                    return;
                }
                if failed {
                    self.stop_fetch_data.store(true, Ordering::SeqCst);
                    return;
                }
            }
        }
    }

    /// Go `sendResult`.
    fn send_result(&self, result: UnionWorkerResult) -> bool {
        select_biased! {
            recv(self.finished) -> _ => false,
            send(self.result_pool, result) -> sent => sent.is_ok(),
        }
    }
}

impl Executor for UnionExec {
    fn open(&mut self) -> Result<(), ExecError> {
        self.stop_fetch_data.store(false, Ordering::SeqCst);
        self.initialized = false;
        let (finished, finished_rx) = bounded(0);
        self.finished = Some(finished);
        self.finished_rx = Some(finished_rx);
        *self.max_opened_child_id.lock().unwrap_or_else(PoisonError::into_inner) = -1;
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.grow_and_reset(self.meta.max_chunk_size());
        if !self.initialized {
            self.initialize()?;
            self.initialized = true;
        }
        // Go's `resultPool` closes once every worker is done
        // (`waitAllFinished`): here, once the workers drop their senders.
        let Some(result) = self.result_pool.as_ref().and_then(|pool| pool.recv().ok()) else {
            return Ok(());
        };
        if let Some(err) = result.err {
            return Err(err);
        }
        let Some(mut chk) = result.chk else {
            return Ok(());
        };
        if chk.num_cols() != req.num_cols() {
            return Err(ExecError::internal(format!(
                "Internal error: UnionExec chunk column count mismatch, req: {}, result: {}",
                req.num_cols(),
                chk.num_cols()
            )));
        }
        req.swap_columns(&mut chk);
        let _ = self.resource_pools[result.src].send(chk);
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
        if let Some(finished) = self.finished.take() {
            self.stop_fetch_data.store(true, Ordering::SeqCst);
            drop(finished);
            for worker in self.workers.drain(..) {
                let _ = worker.join();
            }
        }
        self.finished_rx = None;
        self.result_pool = None;
        self.resource_pools.clear();
        // Every worker has exited, so no child is in use.
        let max_opened = *self.max_opened_child_id.lock().unwrap_or_else(PoisonError::into_inner);
        let mut first_err = None;
        for child in self.children.iter().take(usize::try_from(max_opened + 1).unwrap_or(0)) {
            if let Err(err) = child.lock().unwrap_or_else(PoisonError::into_inner).close() {
                first_err.get_or_insert(err);
            }
        }
        first_err.map_or(Ok(()), Err)
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
