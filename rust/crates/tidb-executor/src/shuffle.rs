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

//! `pkg/executor/shuffle.go`: [`ShuffleExec`], the operator that runs N copies
//! of a child executor over N disjoint partitions of its data sources.
//!
//! Each source and worker owns its executor on a dedicated thread. One recycled
//! input chunk per source/worker pair and one output chunk per worker provide
//! Go's backpressure without aliasing mutable executors. Close broadcasts
//! cancellation, joins every thread, and returns executors to their original
//! owners before closing them. Output ordering between workers is unspecified.
//!
//! Physical-planner integration is present; complete upstream package validation
//! remains pending. Unit tests are focused regression evidence.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use crossbeam_channel::{Receiver, Sender, bounded, select_biased};

use tidb_chunk::chunk::Chunk;
use tidb_datatype::FieldType;
use tidb_expr::Columns;
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;

use crate::executor::{ExecError, Executor, ExecutorMeta};
use crate::vec_group_checker::VecGroupChecker;

enum ShuffleOutput {
    Chunk(Chunk, Sender<Chunk>),
    Err(ExecError),
}

#[derive(Debug)]
struct Inbox {
    input: Option<Sender<Chunk>>,
    incoming: Receiver<Chunk>,
    holder: Sender<Chunk>,
    available: Receiver<Chunk>,
}

impl Default for Inbox {
    fn default() -> Self {
        let (input, incoming) = bounded(1);
        let (holder, available) = bounded(1);
        Self {
            input: Some(input),
            incoming,
            holder,
            available,
        }
    }
}

/// Shared channels for one source/worker pair. Only the source fills buffers;
/// only its receiver returns buffers after swapping columns into its request.
#[derive(Clone, Debug, Default)]
pub struct InboxHandle(Arc<Mutex<Inbox>>);

impl InboxHandle {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn push(&self, chunk: Chunk) {
        let input = self.0.lock().unwrap().input.clone();
        if let Some(input) = input {
            let _ = input.send(chunk);
        }
    }

    fn close(&self) {
        self.0.lock().unwrap().input.take();
    }

    fn clear(&self) {
        let inbox = self.0.lock().unwrap();
        while inbox.incoming.try_recv().is_ok() {}
        while inbox.available.try_recv().is_ok() {}
    }

    fn reopen(&self) {
        *self.0.lock().unwrap() = Inbox::default();
    }

    fn recycle(&self, chunk: Chunk) {
        let holder = self.0.lock().unwrap().holder.clone();
        let _ = holder.send(chunk);
    }

    fn acquire(&self, finish: &FinishFlag) -> Option<Chunk> {
        let available = self.0.lock().unwrap().available.clone();
        let closed = finish.receiver();
        select_biased! {
            recv(closed) -> _ => None,
            recv(available) -> chunk => chunk.ok(),
        }
    }

    fn receive(&self, finish: &FinishFlag) -> Option<Chunk> {
        let incoming = self.0.lock().unwrap().incoming.clone();
        let closed = finish.receiver();
        select_biased! {
            recv(closed) -> _ => None,
            recv(incoming) -> chunk => chunk.ok(),
        }
    }
}

#[derive(Debug)]
struct FinishState {
    sender: Option<Sender<()>>,
    receiver: Receiver<()>,
}

impl Default for FinishState {
    fn default() -> Self {
        let (sender, receiver) = bounded(0);
        Self {
            sender: Some(sender),
            receiver,
        }
    }
}

/// Dropping the sole sender broadcasts cancellation to all blocked receivers.
#[derive(Clone, Debug, Default)]
pub struct FinishFlag(Arc<Mutex<FinishState>>);

impl FinishFlag {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
    fn receiver(&self) -> Receiver<()> {
        self.0.lock().unwrap().receiver.clone()
    }
    fn set(&self) {
        self.0.lock().unwrap().sender.take();
    }
    fn reset(&self) {
        *self.0.lock().unwrap() = FinishState::default();
    }
}

/// Go `shuffleReceiver` (`shuffle.go:350`): the leaf executor a worker's child
/// tree reads, fed by the partition splitter through [`InboxHandle`].
pub struct ShuffleReceiver {
    meta: ExecutorMeta,
    finish: FinishFlag,
    executed: bool,
    inbox: InboxHandle,
}

impl ShuffleReceiver {
    /// Builds the receiver for one (data source, worker) pair. `meta` carries
    /// the *source's* schema, since the receiver only relays that source's
    /// rows.
    #[must_use]
    pub fn new(meta: ExecutorMeta, inbox: InboxHandle, finish: FinishFlag) -> Self {
        ShuffleReceiver {
            meta,
            finish,
            executed: false,
            inbox,
        }
    }
}

impl Executor for ShuffleReceiver {
    /// Go `shuffleReceiver.Open` (`shuffle.go:361`).
    fn open(&mut self) -> Result<(), ExecError> {
        self.executed = false;
        Ok(())
    }

    /// Go `shuffleReceiver.Next` (`shuffle.go:376`).
    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        if self.executed {
            return Ok(());
        }
        match self.inbox.receive(&self.finish) {
            None => self.executed = true,
            Some(mut result) => {
                if result.num_rows() == 0 {
                    self.executed = true;
                    return Ok(());
                }
                req.swap_columns(&mut result);
                self.inbox.recycle(result);
            }
        }
        Ok(())
    }

    /// Go `shuffleReceiver.Close` (`shuffle.go:370`).
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

/// Go `shuffleWorker` (`shuffle.go:397`): one partition's child executor plus
/// the mailboxes feeding its receivers.
pub struct ShuffleWorker {
    child_exec: Box<dyn Executor>,
    /// One per data source, in data-source order, matching Go's
    /// `receivers[dataSourceIndex]` indexing (`shuffle.go:289`).
    ///
    /// Go holds the `*shuffleReceiver`s themselves; this port holds only their
    /// mailboxes, because the receivers live inside `child_exec`'s tree and
    /// Rust will not let the worker alias them.
    inboxes: Vec<InboxHandle>,
    finish: FinishFlag,
}

impl ShuffleWorker {
    /// Builds a worker over `child_exec`, whose leaves must be the
    /// [`ShuffleReceiver`]s holding `inboxes` -- one per data source, in data
    /// source order.
    #[must_use]
    pub fn new(
        child_exec: Box<dyn Executor>,
        inboxes: Vec<InboxHandle>,
        finish: FinishFlag,
    ) -> Self {
        ShuffleWorker {
            child_exec,
            inboxes,
            finish,
        }
    }

    fn run(&mut self, output: &Sender<ShuffleOutput>, first: Chunk) {
        let (holder, available) = bounded(1);
        holder.send(first).unwrap();
        let closed = self.finish.receiver();
        loop {
            let mut chk = select_biased! {
                recv(closed) -> _ => return,
                recv(available) -> chunk => match chunk { Ok(chunk) => chunk, Err(_) => return },
            };
            if let Err(err) = self.child_exec.next(&mut chk) {
                let _ = output.send(ShuffleOutput::Err(err));
                return;
            }
            if chk.num_rows() == 0 {
                return;
            }
            if output
                .send(ShuffleOutput::Chunk(chk, holder.clone()))
                .is_err()
            {
                return;
            }
        }
    }
}

fn report_panic(output: &Sender<ShuffleOutput>, payload: Box<dyn std::any::Any + Send>) {
    let message = payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&str>().copied())
        .unwrap_or("shuffle panicked");
    let _ = output.send(ShuffleOutput::Err(ExecError::internal(message.to_owned())));
}

/// Go `partitionSplitter` (`shuffle.go:440`): assigns each input row a worker.
///
/// Go's `split` returns the (reused) index slice; this port writes into a
/// caller-owned buffer instead, which keeps the reuse without the aliasing.
/// Go leaves the buffer untouched on error (it returns the argument unchanged
/// at `shuffle.go:453` and `shuffle.go:494`) and so does this signature.
pub trait PartitionSplitter<C: Columns>: Send {
    /// Go `split`: fills `worker_indices` with one worker index per input row.
    fn split(
        &mut self,
        ctx: &C,
        input: &Chunk,
        worker_indices: &mut Vec<usize>,
    ) -> Result<(), ExecError>;
}

/// Go `partitionHashSplitter` (`shuffle.go:444`): hashes the `BY` items and
/// takes the hash modulo the worker count.
pub struct PartitionHashSplitter {
    by_items: Vec<Expression>,
    num_workers: usize,
    /// Go's reused `hashKeys [][]byte`, kept for the same reason.
    hash_keys: Vec<Vec<u8>>,
}

impl PartitionHashSplitter {
    /// Go `buildPartitionHashSplitter` (`shuffle.go:464`).
    #[must_use]
    pub fn new(concurrency: usize, by_items: Vec<Expression>) -> Self {
        PartitionHashSplitter {
            by_items,
            num_workers: concurrency,
            hash_keys: Vec::new(),
        }
    }
}

impl<C: Columns> PartitionSplitter<C> for PartitionHashSplitter {
    /// Go `partitionHashSplitter.split` (`shuffle.go:450`).
    fn split(
        &mut self,
        ctx: &C,
        input: &Chunk,
        worker_indices: &mut Vec<usize>,
    ) -> Result<(), ExecError> {
        // Go `aggregate.GetGroupKey` (`aggregate/agg_util.go:106`): one byte
        // string per row, the `BY` items' hash encodings concatenated in item
        // order. Reuses the existing rows' buffers and grows for new ones,
        // exactly as Go's `groupKey[i] = groupKey[i][:0]` prologue does.
        let num_rows = input.num_rows();
        let available = self.hash_keys.len().min(num_rows);
        for key in &mut self.hash_keys[..available] {
            key.clear();
        }
        for _ in available..num_rows {
            self.hash_keys
                .push(Vec::with_capacity(10 * self.by_items.len()));
        }
        let timezone = ctx.time_zone();
        for item in &self.by_items {
            // Go aggregate.GetGroupKey evaluates enums by their numeric value
            // (including invalid zero) and derives decimal precision per value.
            let mut field_type = item
                .static_type()
                .cloned()
                .ok_or_else(|| ExecError::internal("shuffle group expression has no field type"))?;
            match field_type.code() {
                tidb_datatype::FieldTypeCode::Enum => {
                    field_type.add_flags(tidb_datatype::FieldTypeFlags::ENUM_SET_AS_INT);
                }
                tidb_datatype::FieldTypeCode::NewDecimal => field_type.set_flen(0),
                _ => {}
            }
            for row_index in 0..num_rows {
                let datum = item.eval(ctx, input.get_row(row_index))?;
                tidb_codec::append_hash_group_key_in_timezone(
                    &timezone,
                    &datum,
                    &field_type,
                    &mut self.hash_keys[row_index],
                )
                .map_err(|error| ExecError::internal(error.to_string()))?;
            }
        }

        worker_indices.clear();
        for key in &self.hash_keys[..num_rows] {
            // Go: `int(murmur3.Sum32(s.hashKeys[i])) % s.numWorkers`. `Sum32`
            // is unsigned and `int` is 64-bit on every platform TiDB builds
            // for, so the conversion never makes the value negative and the
            // remainder is always a valid worker index.
            worker_indices.push(murmur3_sum32(key) as usize % self.num_workers);
        }
        Ok(())
    }
}

/// Go `partitionRangeSplitter` (`shuffle.go:471`): deals *groups* of a sorted
/// input to workers round-robin.
pub struct PartitionRangeSplitter {
    num_workers: usize,
    group_checker: VecGroupChecker,
    idx: usize,
}

impl PartitionRangeSplitter {
    /// Go `buildPartitionRangeSplitter` (`shuffle.go:478`).
    ///
    /// boundary: Go also stores `byItems` on the splitter, but only the
    /// `VecGroupChecker` built from them is ever read, so the field is folded
    /// into the checker here. The checker reads the expression evaluation
    /// strategy from the statement context supplied to `split`.
    #[must_use]
    pub fn new(concurrency: usize, by_items: Vec<Expression>) -> Self {
        PartitionRangeSplitter {
            num_workers: concurrency,
            group_checker: VecGroupChecker::new(by_items),
            idx: 0,
        }
    }
}

impl<C: Columns> PartitionSplitter<C> for PartitionRangeSplitter {
    /// Go `partitionRangeSplitter.split` (`shuffle.go:490`).
    ///
    /// "This method is supposed to be used for shuffle with sorted
    /// `dataSource`; the caller of this method should guarantee that `input` is
    /// grouped, which means that rows with the same byItems should be
    /// continuous, the order does not matter."
    ///
    /// Note that `idx` persists across chunks, so the round robin continues
    /// where the previous chunk left off -- and a group split across two chunks
    /// is therefore *not* rejoined by this splitter. That is Go's behaviour
    /// (`shuffle.go:490-508` never inspects the checker's "same as previous
    /// chunk" answer, discarding it at `shuffle.go:491`) and it is why the
    /// upstream plan only builds this splitter above a sort.
    fn split(
        &mut self,
        ctx: &C,
        input: &Chunk,
        worker_indices: &mut Vec<usize>,
    ) -> Result<(), ExecError> {
        // Go discards `SplitIntoGroups`' first return value (whether the first
        // group continues the previous chunk's last one).
        self.group_checker.split_into_groups(ctx, input)?;

        worker_indices.clear();
        while !self.group_checker.is_exhausted() {
            let (begin, end) = self.group_checker.get_next_group();
            for _ in begin..end {
                worker_indices.push(self.idx);
            }
            self.idx = (self.idx + 1) % self.num_workers;
        }
        Ok(())
    }
}

/// Go `ShuffleExec` (`shuffle.go:88`): runs `concurrency` copies of a child
/// executor, each over one partition of the data sources.
pub struct ShuffleExec<C: Columns> {
    meta: ExecutorMeta,
    concurrency: usize,
    workers: Vec<ShuffleWorker>,

    prepared: bool,
    executed: bool,

    /// One splitter per data source, same index space as `data_sources`.
    splitters: Vec<Box<dyn PartitionSplitter<C>>>,
    data_sources: Vec<Box<dyn Executor>>,

    finish: FinishFlag,
    output: Option<Receiver<ShuffleOutput>>,
    ctx: Arc<Mutex<C>>,
    source_threads: Vec<JoinHandle<(Box<dyn Executor>, Box<dyn PartitionSplitter<C>>)>>,
    worker_threads: Vec<JoinHandle<ShuffleWorker>>,
    #[cfg(test)]
    fail_spawn_after: Option<usize>,
    #[cfg(test)]
    fail_next: bool,
    #[cfg(test)]
    source_panic_gate: Option<Receiver<()>>,
    #[cfg(test)]
    panic_workers: bool,
}

impl<C: Columns> ShuffleExec<C> {
    /// Builds a shuffle over `data_sources`, splitting each with the splitter
    /// at the same index and feeding `workers`.
    ///
    /// # Panics
    /// If `workers` is empty, if a splitter count does not match the data
    /// source count, or if a worker does not hold exactly one mailbox per data
    /// source. Go derives all three from the plan and would deadlock or index
    /// out of range rather than report them, so they are asserted here.
    #[must_use]
    pub fn new(
        meta: ExecutorMeta,
        ctx: C,
        workers: Vec<ShuffleWorker>,
        splitters: Vec<Box<dyn PartitionSplitter<C>>>,
        data_sources: Vec<Box<dyn Executor>>,
        finish: FinishFlag,
    ) -> Self {
        assert!(!workers.is_empty(), "shuffle needs at least one worker");
        assert_eq!(
            splitters.len(),
            data_sources.len(),
            "shuffle needs one splitter per data source"
        );
        for worker in &workers {
            assert_eq!(
                worker.inboxes.len(),
                data_sources.len(),
                "each shuffle worker needs one receiver per data source"
            );
        }
        let concurrency = workers.len();
        ShuffleExec {
            meta,
            concurrency,
            workers,
            prepared: false,
            executed: false,
            splitters,
            data_sources,
            finish,
            output: None,
            ctx: Arc::new(Mutex::new(ctx)),
            source_threads: Vec::new(),
            worker_threads: Vec::new(),
            #[cfg(test)]
            fail_spawn_after: None,
            #[cfg(test)]
            fail_next: false,
            #[cfg(test)]
            source_panic_gate: None,
            #[cfg(test)]
            panic_workers: false,
        }
    }

    /// Go `ShuffleExec.concurrency`, reported for the runtime-stats line Go
    /// registers in `Close` (`shuffle.go:196`).
    #[must_use]
    pub fn concurrency(&self) -> usize {
        self.concurrency
    }

    fn join_threads(&mut self) {
        // Cancellation must precede joining: a source may be waiting for an
        // input buffer held by an early-stopped worker.
        self.finish.set();
        for handle in self.worker_threads.drain(..) {
            self.workers
                .push(handle.join().expect("shuffle worker recovery failed"));
        }
        for handle in self.source_threads.drain(..) {
            let (source, splitter) = handle.join().expect("shuffle source recovery failed");
            self.data_sources.push(source);
            self.splitters.push(splitter);
        }
        self.output.take();
    }
}

impl<C: Columns + Send + 'static> ShuffleExec<C> {
    #[cfg(test)]
    fn check_thread_spawn(&mut self) -> std::io::Result<()> {
        if let Some(remaining) = &mut self.fail_spawn_after {
            if *remaining == 0 {
                self.fail_spawn_after = None;
                return Err(std::io::Error::other(
                    "injected shuffle thread creation failure",
                ));
            }
            *remaining -= 1;
        }
        Ok(())
    }

    fn spawn_job<T: Send + 'static>(
        &mut self,
        job: T,
        run: impl FnOnce(&mut T) + Send + 'static,
    ) -> Result<JoinHandle<T>, (std::io::Error, T)> {
        #[cfg(test)]
        if let Err(error) = self.check_thread_spawn() {
            return Err((error, job));
        }
        // Keep executor ownership here until the OS accepts the new thread.
        // Builder::spawn drops its closure on failure, so moving the executor
        // directly into that closure would prevent Close from releasing it.
        let (sender, receiver) = bounded(1);
        match std::thread::Builder::new().spawn(move || {
            let mut job = receiver.recv().expect("shuffle startup job");
            run(&mut job);
            job
        }) {
            Ok(handle) => {
                assert!(sender.send(job).is_ok(), "shuffle startup receiver exited");
                Ok(handle)
            }
            Err(error) => Err((error, job)),
        }
    }

    fn prepare4_parallel_exec(&mut self) -> Result<(), ExecError> {
        let (output, receiver) = bounded(self.workers.len() + self.data_sources.len());
        self.output = Some(receiver);
        let inboxes: Vec<Vec<InboxHandle>> = self
            .workers
            .iter()
            .map(|worker| worker.inboxes.clone())
            .collect();
        let mut sources = std::mem::take(&mut self.data_sources)
            .into_iter()
            .zip(std::mem::take(&mut self.splitters))
            .enumerate();
        while let Some((source_index, job)) = sources.next() {
            let inputs: Vec<_> = inboxes
                .iter()
                .map(|row| row[source_index].clone())
                .collect();
            let finish = self.finish.clone();
            let ctx = self.ctx.clone();
            let output = output.clone();
            #[cfg(test)]
            let source_panic_gate = self.source_panic_gate.clone();
            let spawned = self.spawn_job(job, move |(source, splitter)| {
                let result = catch_unwind(AssertUnwindSafe(|| {
                    #[cfg(test)]
                    if let Some(gate) = source_panic_gate {
                        let _ = gate.recv();
                        panic!("shuffleExecFetchDataAndSplitPanic");
                    }
                    fetch_data_and_split(
                        source.as_mut(),
                        splitter.as_mut(),
                        &ctx,
                        &inputs,
                        &finish,
                        &output,
                    );
                }));
                if let Err(payload) = result {
                    report_panic(&output, payload);
                }
                for inbox in &inputs {
                    inbox.close();
                }
            });
            match spawned {
                Ok(handle) => self.source_threads.push(handle),
                Err((error, (source, splitter))) => {
                    self.join_threads();
                    self.data_sources.push(source);
                    self.splitters.push(splitter);
                    for (_, (source, splitter)) in sources {
                        self.data_sources.push(source);
                        self.splitters.push(splitter);
                    }
                    return Err(ExecError::internal(format!(
                        "shuffle thread creation failure: {error}"
                    )));
                }
            }
        }
        let mut workers = std::mem::take(&mut self.workers).into_iter();
        while let Some(worker) = workers.next() {
            let output = output.clone();
            let first = self.meta.new_chunk();
            #[cfg(test)]
            let panic_worker = self.panic_workers;
            let spawned = self.spawn_job(worker, move |worker| {
                let result = catch_unwind(AssertUnwindSafe(|| {
                    #[cfg(test)]
                    if panic_worker {
                        panic!("ShufflePanic");
                    }
                    worker.run(&output, first);
                }));
                if let Err(payload) = result {
                    report_panic(&output, payload);
                }
            });
            match spawned {
                Ok(handle) => self.worker_threads.push(handle),
                Err((error, worker)) => {
                    self.join_threads();
                    self.workers.push(worker);
                    self.workers.extend(workers);
                    return Err(ExecError::internal(format!(
                        "shuffle thread creation failure: {error}"
                    )));
                }
            }
        }
        Ok(())
    }
}

impl<C: Columns> Drop for ShuffleExec<C> {
    fn drop(&mut self) {
        self.join_threads();
    }
}

fn fetch_data_and_split<C: Columns>(
    source: &mut dyn Executor,
    splitter: &mut dyn PartitionSplitter<C>,
    ctx: &Mutex<C>,
    inboxes: &[InboxHandle],
    finish: &FinishFlag,
    output: &Sender<ShuffleOutput>,
) {
    let mut results: Vec<Option<Chunk>> = (0..inboxes.len()).map(|_| None).collect();
    let mut worker_indices = Vec::new();
    let mut chk = source.new_chunk();
    loop {
        if let Err(err) = source.next(&mut chk) {
            let _ = output.send(ShuffleOutput::Err(err));
            return;
        }
        if chk.num_rows() == 0 {
            break;
        }
        // Columns permits non-Sync evaluation contexts. Share the same context
        // and serialize evaluation rather than cloning session warning state.
        let split = splitter.split(
            &*ctx.lock().unwrap_or_else(|e| e.into_inner()),
            &chk,
            &mut worker_indices,
        );
        if let Err(err) = split {
            let _ = output.send(ShuffleOutput::Err(err));
            return;
        }
        assert_eq!(
            worker_indices.len(),
            chk.num_rows(),
            "partition splitter must assign every input row a worker"
        );
        for (row_index, &worker_index) in worker_indices.iter().enumerate() {
            if results[worker_index].is_none() {
                results[worker_index] = inboxes[worker_index].acquire(finish);
                if results[worker_index].is_none() {
                    return;
                }
            }
            let buffer = results[worker_index].as_mut().unwrap();
            buffer.append_row(chk.get_row(row_index));
            if buffer.is_full() {
                inboxes[worker_index].push(results[worker_index].take().unwrap());
            }
        }
    }
    for (inbox, result) in inboxes.iter().zip(results) {
        if let Some(partial) = result {
            inbox.push(partial);
        }
    }
}

impl<C: Columns + Send + 'static> Executor for ShuffleExec<C> {
    /// Go `ShuffleExec.Open` (`shuffle.go:113`).
    fn open(&mut self) -> Result<(), ExecError> {
        for source in &mut self.data_sources {
            source.open()?;
        }

        self.prepared = false;
        self.output = None;
        // Go allocates a *new* `finishCh` here (`shuffle.go:124`), so a
        // shuffle reopened after `Close` is not permanently finished. The Arc
        // is shared with every worker and receiver, which is how Go's
        // `w.finishCh = e.finishCh` fan-out (`shuffle.go:128`) reaches them.
        self.finish.reset();
        for worker in &mut self.workers {
            for inbox in &worker.inboxes {
                inbox.reopen();
            }
            worker.child_exec.open()?;
            for (inbox, source) in worker.inboxes.iter().zip(&self.data_sources) {
                inbox.recycle(source.new_chunk());
            }
        }
        Ok(())
    }

    /// Go `ShuffleExec.Next` (`shuffle.go:241`).
    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        if !self.prepared {
            self.prepared = true;
            if let Err(error) = self.prepare4_parallel_exec() {
                self.executed = true;
                return Err(error);
            }
        }
        // Go shuffleError runs after launching sources/workers, before reading
        // their output. Keep the same injection boundary in unit tests.
        #[cfg(test)]
        if self.fail_next {
            return Err(ExecError::internal("ShuffleExec.Next error"));
        }
        if self.executed {
            return Ok(());
        }
        match self.output.as_ref().expect("shuffle prepared").recv().ok() {
            // Go's closed-and-drained `outputCh`.
            None => {
                self.executed = true;
                Ok(())
            }
            Some(ShuffleOutput::Err(err)) => Err(err),
            Some(ShuffleOutput::Chunk(mut chk, holder)) => {
                req.swap_columns(&mut chk);
                let _ = holder.send(chk);
                Ok(())
            }
        }
    }

    /// Go `ShuffleExec.Close` (`shuffle.go:152`).
    ///
    /// Go closes the channels, drains them, closes each worker's child, then
    /// each data source, keeping the *first* error and returning it. The order
    /// and the first-error rule are both reproduced -- a later failure never
    /// masks an earlier one, and every child is closed even after one fails.
    ///
    /// The instrumented physical builder and EXPLAIN renderer report
    /// Go's `RuntimeStatsWithConcurrencyInfo` using this plan's concurrency.
    fn close(&mut self) -> Result<(), ExecError> {
        let mut first_err: Option<ExecError> = None;

        self.join_threads();

        for worker in &mut self.workers {
            for inbox in &worker.inboxes {
                inbox.close();
                inbox.clear();
            }
            if let Err(err) = worker.child_exec.close() {
                first_err.get_or_insert(err);
            }
        }

        self.executed = false;

        for source in &mut self.data_sources {
            if let Err(err) = source.close() {
                first_err.get_or_insert(err);
            }
        }

        match first_err {
            None => Ok(()),
            Some(err) => Err(err),
        }
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

/// `github.com/twmb/murmur3.Sum32`: MurmurHash3 x86 32-bit with seed 0.
///
/// Ported rather than pulled in as a dependency because it is the only murmur3
/// surface `pkg/executor` uses and the partition assignment must agree with Go
/// bit for bit -- a different hash silently reshuffles which worker a row lands
/// on, which is invisible in the result set but visible in every plan-shaped
/// test.
#[must_use]
pub fn murmur3_sum32(data: &[u8]) -> u32 {
    const C1: u32 = 0xcc9e_2d51;
    const C2: u32 = 0x1b87_3593;

    let mut h1: u32 = 0;
    let mut blocks = data.chunks_exact(4);
    for block in &mut blocks {
        let mut k1 = u32::from_le_bytes([block[0], block[1], block[2], block[3]]);
        k1 = k1.wrapping_mul(C1).rotate_left(15).wrapping_mul(C2);
        h1 ^= k1;
        h1 = h1.rotate_left(13).wrapping_mul(5).wrapping_add(0xe654_6b64);
    }

    let tail = blocks.remainder();
    let mut k1: u32 = 0;
    if tail.len() >= 3 {
        k1 ^= u32::from(tail[2]) << 16;
    }
    if tail.len() >= 2 {
        k1 ^= u32::from(tail[1]) << 8;
    }
    if !tail.is_empty() {
        k1 ^= u32::from(tail[0]);
        k1 = k1.wrapping_mul(C1).rotate_left(15).wrapping_mul(C2);
        h1 ^= k1;
    }

    h1 ^= data.len() as u32;
    // fmix32
    h1 ^= h1 >> 16;
    h1 = h1.wrapping_mul(0x85eb_ca6b);
    h1 ^= h1 >> 13;
    h1 = h1.wrapping_mul(0xc2b2_ae35);
    h1 ^= h1 >> 16;
    h1
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tidb_datatype::FieldTypeCode;
    use tidb_expr::NoColumns;
    use tidb_expr::column::Column;

    const MAX_CHUNK: usize = 4;

    fn long() -> FieldType {
        FieldType::new(FieldTypeCode::LongLong)
    }

    fn one_col_schema() -> Schema {
        let mut column = Column::new(1, long());
        column.index = 0;
        Schema::new(vec![column])
    }

    fn column_expr(index: usize) -> Expression {
        let mut column = Column::new(index as i64 + 1, long());
        column.index = index as i64;
        Expression::Column(column)
    }

    fn chunk_of(values: &[i64]) -> Chunk {
        let mut chunk = Chunk::new(&[long()], MAX_CHUNK, MAX_CHUNK);
        for value in values {
            chunk.append_int64(0, *value);
        }
        chunk
    }

    fn ints(chunk: &Chunk) -> Vec<i64> {
        (0..chunk.num_rows())
            .map(|row| chunk.get_row(row).get_int64(0))
            .collect()
    }

    /// A source replaying prebuilt batches, then EOF. `open` rewinds it.
    struct ReplaySource {
        meta: ExecutorMeta,
        batches: Vec<Vec<i64>>,
        next_batch: usize,
        fail_at: Option<usize>,
        panic_at: Option<usize>,
        close_err: bool,
        next_calls: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl ReplaySource {
        fn new(batches: Vec<Vec<i64>>) -> Self {
            ReplaySource {
                meta: ExecutorMeta::new(one_col_schema(), 0, MAX_CHUNK, MAX_CHUNK),
                batches,
                next_batch: 0,
                fail_at: None,
                panic_at: None,
                close_err: false,
                next_calls: Arc::default(),
            }
        }
    }

    impl Executor for ReplaySource {
        fn open(&mut self) -> Result<(), ExecError> {
            self.next_batch = 0;
            Ok(())
        }

        fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
            req.reset();
            self.next_calls.fetch_add(1, Ordering::Relaxed);
            if self.panic_at == Some(self.next_batch) {
                panic!("shuffle test panic");
            }
            if self.fail_at == Some(self.next_batch) {
                return Err(ExecError::internal("source exploded"));
            }
            let Some(batch) = self.batches.get(self.next_batch) else {
                return Ok(());
            };
            self.next_batch += 1;
            for value in batch {
                req.append_int64(0, *value);
            }
            Ok(())
        }

        fn close(&mut self) -> Result<(), ExecError> {
            if self.close_err {
                return Err(ExecError::internal("source close failed"));
            }
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

    /// A worker child that concatenates its receivers in order -- the smallest
    /// tree that exercises the multi-source `receivers[dataSourceIndex]`
    /// indexing without pulling a real operator in.
    struct ConcatExec {
        meta: ExecutorMeta,
        children: Vec<Box<dyn Executor>>,
        at: usize,
        close_err: Arc<AtomicBool>,
    }

    impl Executor for ConcatExec {
        fn open(&mut self) -> Result<(), ExecError> {
            self.at = 0;
            for child in &mut self.children {
                child.open()?;
            }
            Ok(())
        }

        fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
            req.reset();
            while self.at < self.children.len() {
                self.children[self.at].next(req)?;
                if req.num_rows() != 0 {
                    return Ok(());
                }
                self.at += 1;
            }
            Ok(())
        }

        fn close(&mut self) -> Result<(), ExecError> {
            for child in &mut self.children {
                child.close()?;
            }
            if self.close_err.load(Ordering::Relaxed) {
                return Err(ExecError::internal("worker child close failed"));
            }
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

    /// Builds `concurrency` workers over `sources.len()` receivers each, whose
    /// child is a `ConcatExec` -- i.e. an identity partition-pass-through.
    fn build_workers(
        concurrency: usize,
        num_sources: usize,
        finish: &FinishFlag,
    ) -> Vec<ShuffleWorker> {
        build_workers_with(
            concurrency,
            num_sources,
            finish,
            &Arc::new(AtomicBool::new(false)),
        )
    }

    fn build_workers_with(
        concurrency: usize,
        num_sources: usize,
        finish: &FinishFlag,
        close_err: &Arc<AtomicBool>,
    ) -> Vec<ShuffleWorker> {
        (0..concurrency)
            .map(|_| {
                let inboxes: Vec<InboxHandle> =
                    (0..num_sources).map(|_| InboxHandle::new()).collect();
                let children: Vec<Box<dyn Executor>> = inboxes
                    .iter()
                    .map(|inbox| {
                        Box::new(ShuffleReceiver::new(
                            ExecutorMeta::new(one_col_schema(), 0, MAX_CHUNK, MAX_CHUNK),
                            inbox.clone(),
                            finish.clone(),
                        )) as Box<dyn Executor>
                    })
                    .collect();
                let child = ConcatExec {
                    meta: ExecutorMeta::new(one_col_schema(), 0, MAX_CHUNK, MAX_CHUNK),
                    children,
                    at: 0,
                    close_err: close_err.clone(),
                };
                ShuffleWorker::new(Box::new(child), inboxes, finish.clone())
            })
            .collect()
    }

    fn drain(exec: &mut ShuffleExec<NoColumns>) -> Result<Vec<Vec<i64>>, ExecError> {
        let mut out = Vec::new();
        loop {
            let mut chunk = exec.new_chunk();
            exec.next(&mut chunk)?;
            if chunk.num_rows() == 0 {
                return Ok(out);
            }
            out.push(ints(&chunk));
        }
    }

    // ---- murmur3 -------------------------------------------------------

    /// Published MurmurHash3 x86_32 (seed 0) reference vectors, which is what
    /// `github.com/twmb/murmur3.Sum32` computes.
    #[test]
    fn murmur3_matches_reference_vectors() {
        assert_eq!(murmur3_sum32(b""), 0x0000_0000);
        assert_eq!(murmur3_sum32(b"a"), 0x3c25_69b2);
        assert_eq!(murmur3_sum32(b"abc"), 0xb3dd_93fa);
        assert_eq!(murmur3_sum32(b"abcd"), 0x43ed_676a);
        assert_eq!(murmur3_sum32(b"hello"), 0x248b_fa47);
        assert_eq!(murmur3_sum32(b"hello, world"), 0x149b_bb7f);
        assert_eq!(
            murmur3_sum32(b"The quick brown fox jumps over the lazy dog"),
            0x2e4f_f723
        );
    }

    /// The tail switch has a case per remaining byte; a length sweep proves
    /// none of them was dropped or duplicated.
    #[test]
    fn murmur3_tail_lengths_are_all_distinct() {
        let data = b"0123456789abcdef";
        let hashes: Vec<u32> = (0..=data.len())
            .map(|n| murmur3_sum32(&data[..n]))
            .collect();
        let mut sorted = hashes.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), hashes.len());
    }

    // ---- splitters -----------------------------------------------------

    #[test]
    fn hash_splitter_sends_equal_keys_to_one_worker() {
        let mut splitter = PartitionHashSplitter::new(3, vec![column_expr(0)]);
        let chunk = chunk_of(&[7, 7, 8, 7]);
        let mut indices = Vec::new();
        PartitionSplitter::<NoColumns>::split(&mut splitter, &NoColumns, &chunk, &mut indices)
            .unwrap();
        assert_eq!(indices.len(), 4);
        assert!(indices.iter().all(|&i| i < 3));
        assert_eq!(indices[0], indices[1]);
        assert_eq!(indices[0], indices[3]);
    }

    #[test]
    fn hash_splitter_uses_go_group_keys_for_unsigned_enum_and_decimal() {
        use tidb_datatype::{Collation, Datum, Decimal, FieldTypeFlags, MysqlEnum};
        let mut unsigned = long();
        unsigned.add_flags(FieldTypeFlags::UNSIGNED);
        let mut enumeration = FieldType::new(FieldTypeCode::Enum);
        enumeration.set_elems(vec!["".into(), "a".into()]);
        let mut decimal = FieldType::new(FieldTypeCode::NewDecimal);
        decimal.set_flen(2);
        decimal.set_decimal(2);
        // Captured from Go aggregate.GetGroupKey and murmur3.Sum32 at aba629bb455.
        // The decimal values exceed the declared flen, exercising GetGroupKey's
        // flen=0 adjustment; enum zero and declared empty-string value differ.
        let cases = [
            (
                unsigned,
                vec![Datum::UInt(u64::MAX), Datum::UInt(1)],
                vec![vec![8, 1], vec![8, 2]],
                vec![5, 5],
            ),
            (
                enumeration,
                vec![
                    Datum::new_enum(MysqlEnum::new("", 0), Collation::Utf8Mb4Bin),
                    Datum::new_enum(MysqlEnum::new("", 1), Collation::Utf8Mb4Bin),
                ],
                vec![vec![8, 0], vec![8, 2]],
                vec![6, 5],
            ),
            (
                decimal,
                vec![
                    Datum::new_decimal(Decimal::from_literal("1.20")),
                    Datum::new_decimal(Decimal::from_literal("12.30")),
                ],
                vec![vec![6, 3, 2, 129, 20], vec![6, 4, 2, 140, 30]],
                vec![6, 2],
            ),
        ];
        for (field_type, values, expected_keys, expected_workers) in cases {
            let mut column = Column::new(1, field_type.clone());
            column.index = 0;
            let mut input = Chunk::new(&[field_type], MAX_CHUNK, MAX_CHUNK);
            for value in values {
                input.append_datum(0, &value);
            }
            let mut splitter = PartitionHashSplitter::new(7, vec![Expression::Column(column)]);
            let mut indices = Vec::new();
            splitter.split(&NoColumns, &input, &mut indices).unwrap();
            assert_eq!(splitter.hash_keys, expected_keys);
            assert_eq!(indices, expected_workers);
        }
    }

    /// Go reuses `s.hashKeys` across calls, truncating only the prefix it can
    /// reuse. A shorter chunk after a longer one must not leak the stale tail.
    #[test]
    fn hash_splitter_reuses_key_buffer_across_chunks() {
        let mut splitter = PartitionHashSplitter::new(4, vec![column_expr(0)]);
        let mut indices = Vec::new();
        let big = chunk_of(&[1, 2, 3, 4]);
        PartitionSplitter::<NoColumns>::split(&mut splitter, &NoColumns, &big, &mut indices)
            .unwrap();
        let first = indices.clone();

        let small = chunk_of(&[1]);
        PartitionSplitter::<NoColumns>::split(&mut splitter, &NoColumns, &small, &mut indices)
            .unwrap();
        assert_eq!(indices.len(), 1);
        assert_eq!(indices[0], first[0]);
    }

    #[test]
    fn range_splitter_deals_groups_round_robin() {
        let mut splitter = PartitionRangeSplitter::new(2, vec![column_expr(0)]);
        let mut indices = Vec::new();
        let chunk = chunk_of(&[1, 1, 2, 3]);
        PartitionSplitter::<NoColumns>::split(&mut splitter, &NoColumns, &chunk, &mut indices)
            .unwrap();
        assert_eq!(indices, vec![0, 0, 1, 0]);
    }

    /// Original pkg/executor/shuffle_test.go::TestPartitionRangeSplitter.
    #[test]
    fn range_splitter_matches_original_varchar_fixture() {
        let field_type = FieldType::new(FieldTypeCode::Varchar);
        let mut column = Column::new(1, field_type.clone());
        column.index = 0;
        let mut input = Chunk::new(&[field_type], 1024, 1024);
        for value in [
            "a", "a", "a", "a", "c", "c", "b", "b", "b", "q", "eee", "eee", "ddd",
        ] {
            input.append_string(0, value);
        }
        let mut splitter = PartitionRangeSplitter::new(2, vec![Expression::Column(column)]);
        let mut obtained = Vec::new();
        splitter.split(&NoColumns, &input, &mut obtained).unwrap();
        assert_eq!(obtained, vec![0, 0, 0, 0, 1, 1, 0, 0, 0, 1, 0, 0, 1]);
    }

    /// Go quirk reproduced: `s.idx` persists across chunks, so a group split
    /// over a chunk boundary is dealt to *two different* workers. Upstream
    /// only builds this splitter above a sort, where that is accepted.
    #[test]
    fn range_splitter_does_not_rejoin_a_group_across_chunks() {
        let mut splitter = PartitionRangeSplitter::new(2, vec![column_expr(0)]);
        let mut indices = Vec::new();
        let first = chunk_of(&[5, 5]);
        PartitionSplitter::<NoColumns>::split(&mut splitter, &NoColumns, &first, &mut indices)
            .unwrap();
        assert_eq!(indices, vec![0, 0]);

        let second = chunk_of(&[5, 5]);
        PartitionSplitter::<NoColumns>::split(&mut splitter, &NoColumns, &second, &mut indices)
            .unwrap();
        assert_eq!(indices, vec![1, 1]);
    }

    // ---- receiver ------------------------------------------------------

    #[test]
    fn receiver_relays_then_reports_exhaustion() {
        let inbox = InboxHandle::new();
        inbox.push(chunk_of(&[1, 2]));
        inbox.close();
        let mut receiver = ShuffleReceiver::new(
            ExecutorMeta::new(one_col_schema(), 0, MAX_CHUNK, MAX_CHUNK),
            inbox,
            FinishFlag::new(),
        );
        receiver.open().unwrap();

        let mut chunk = receiver.new_chunk();
        receiver.next(&mut chunk).unwrap();
        assert_eq!(ints(&chunk), vec![1, 2]);
        receiver.next(&mut chunk).unwrap();
        assert_eq!(chunk.num_rows(), 0);
    }

    /// Go quirk reproduced: a zero-row chunk on `inputCh` ends the receiver
    /// exactly like a closed channel, and the queued chunk behind it is never
    /// delivered.
    #[test]
    fn receiver_treats_an_empty_chunk_as_end_of_input() {
        let inbox = InboxHandle::new();
        inbox.push(chunk_of(&[]));
        inbox.close();
        let mut receiver = ShuffleReceiver::new(
            ExecutorMeta::new(one_col_schema(), 0, MAX_CHUNK, MAX_CHUNK),
            inbox,
            FinishFlag::new(),
        );
        receiver.open().unwrap();

        let mut chunk = receiver.new_chunk();
        receiver.next(&mut chunk).unwrap();
        assert_eq!(chunk.num_rows(), 0);
    }

    /// Go's `select` prefers neither branch, but a closed `finishCh` ends the
    /// receiver even with data queued.
    #[test]
    fn receiver_stops_on_finish() {
        let inbox = InboxHandle::new();
        inbox.push(chunk_of(&[1]));
        let finish = FinishFlag::new();
        finish.set();
        let mut receiver = ShuffleReceiver::new(
            ExecutorMeta::new(one_col_schema(), 0, MAX_CHUNK, MAX_CHUNK),
            inbox,
            finish,
        );
        receiver.open().unwrap();
        let mut chunk = receiver.new_chunk();
        receiver.next(&mut chunk).unwrap();
        assert_eq!(chunk.num_rows(), 0);
    }

    #[test]
    fn receiver_waits_for_an_open_mailbox_and_wakes_on_finish() {
        let finish = FinishFlag::new();
        let mut receiver = ShuffleReceiver::new(
            ExecutorMeta::new(one_col_schema(), 0, MAX_CHUNK, MAX_CHUNK),
            InboxHandle::new(),
            finish.clone(),
        );
        receiver.open().unwrap();
        let (done, result) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let mut chunk = receiver.new_chunk();
            done.send(receiver.next(&mut chunk).map(|()| chunk.num_rows()))
                .unwrap();
        });
        assert!(
            result
                .recv_timeout(std::time::Duration::from_millis(20))
                .is_err()
        );
        finish.set();
        assert_eq!(
            result
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap()
                .unwrap(),
            0
        );
        worker.join().unwrap();
    }

    // ---- end to end ----------------------------------------------------

    fn hash_shuffle(
        concurrency: usize,
        batches: Vec<Vec<i64>>,
    ) -> (ShuffleExec<NoColumns>, FinishFlag) {
        let finish = FinishFlag::new();
        let workers = build_workers(concurrency, 1, &finish);
        let splitters: Vec<Box<dyn PartitionSplitter<NoColumns>>> = vec![Box::new(
            PartitionHashSplitter::new(concurrency, vec![column_expr(0)]),
        )];
        let sources: Vec<Box<dyn Executor>> = vec![Box::new(ReplaySource::new(batches))];
        (
            ShuffleExec::new(
                ExecutorMeta::new(one_col_schema(), 1, MAX_CHUNK, MAX_CHUNK),
                NoColumns,
                workers,
                splitters,
                sources,
                finish.clone(),
            ),
            finish,
        )
    }

    #[test]
    fn shuffle_returns_output_before_draining_its_source() {
        let finish = FinishFlag::new();
        let source = ReplaySource::new(vec![vec![42; MAX_CHUNK]; 100]);
        let calls = source.next_calls.clone();
        let mut exec = ShuffleExec::new(
            ExecutorMeta::new(one_col_schema(), 1, MAX_CHUNK, MAX_CHUNK),
            NoColumns,
            build_workers(1, 1, &finish),
            vec![Box::new(PartitionHashSplitter::new(
                1,
                vec![column_expr(0)],
            ))],
            vec![Box::new(source)],
            finish,
        );
        exec.open().unwrap();
        let mut chunk = exec.new_chunk();
        exec.next(&mut chunk).unwrap();
        assert_eq!(ints(&chunk), vec![42; MAX_CHUNK]);
        let fetched = calls.load(Ordering::Relaxed);
        exec.close().unwrap();
        assert!(fetched <= 5, "fetched {fetched} chunks before first output");
    }

    #[test]
    fn shuffle_recovers_source_and_worker_panics_and_closes_blocked_inputs() {
        for panic_in_worker in [false, true] {
            let finish = FinishFlag::new();
            let mut workers = build_workers(1, 1, &finish);
            let mut source = ReplaySource::new(vec![vec![42; MAX_CHUNK]; 100]);
            if panic_in_worker {
                let mut child = ReplaySource::new(vec![]);
                child.panic_at = Some(0);
                workers[0].child_exec = Box::new(child);
            } else {
                source.panic_at = Some(0);
            }
            let mut exec = ShuffleExec::new(
                ExecutorMeta::new(one_col_schema(), 1, MAX_CHUNK, MAX_CHUNK),
                NoColumns,
                workers,
                vec![Box::new(PartitionHashSplitter::new(
                    1,
                    vec![column_expr(0)],
                ))],
                vec![Box::new(source)],
                finish,
            );
            exec.open().unwrap();
            let mut chunk = exec.new_chunk();
            let error = exec.next(&mut chunk).unwrap_err();
            assert!(
                format!("{error:?}").contains("shuffle test panic"),
                "{error:?}"
            );
            exec.close().unwrap();
            assert_eq!(exec.workers.len(), 1);
            assert_eq!(exec.data_sources.len(), 1);
            assert!(exec.worker_threads.is_empty());
            assert!(exec.source_threads.is_empty());
        }
    }

    #[test]
    fn shuffle_thread_start_failure_preserves_executors_for_close_and_reopen() {
        for failed_thread in 0..5 {
            let finish = FinishFlag::new();
            let mut exec = ShuffleExec::new(
                ExecutorMeta::new(one_col_schema(), 1, MAX_CHUNK, MAX_CHUNK),
                NoColumns,
                build_workers(3, 2, &finish),
                (0..2)
                    .map(|_| {
                        Box::new(PartitionHashSplitter::new(3, vec![column_expr(0)]))
                            as Box<dyn PartitionSplitter<NoColumns>>
                    })
                    .collect(),
                vec![
                    Box::new(ReplaySource::new(vec![vec![1, 2, 3]])),
                    Box::new(ReplaySource::new(vec![vec![4, 5, 6]])),
                ],
                finish,
            );
            let source_ids: Vec<_> = exec
                .data_sources
                .iter()
                .map(|e| e.schema() as *const _)
                .collect();
            let worker_ids: Vec<_> = exec
                .workers
                .iter()
                .map(|w| w.child_exec.schema() as *const _)
                .collect();
            exec.open().unwrap();
            exec.fail_spawn_after = Some(failed_thread);
            let mut chunk = exec.new_chunk();
            let error = exec.next(&mut chunk).unwrap_err();
            assert!(format!("{error:?}").contains("shuffle thread creation failure"));
            assert!(exec.source_threads.is_empty());
            assert!(exec.worker_threads.is_empty());
            assert_eq!(
                exec.data_sources
                    .iter()
                    .map(|e| e.schema() as *const _)
                    .collect::<Vec<_>>(),
                source_ids
            );
            assert_eq!(
                exec.workers
                    .iter()
                    .map(|w| w.child_exec.schema() as *const _)
                    .collect::<Vec<_>>(),
                worker_ids
            );
            exec.close().unwrap();
            exec.open().unwrap();
            let mut rows: Vec<_> = drain(&mut exec).unwrap().into_iter().flatten().collect();
            rows.sort_unstable();
            assert_eq!(rows, vec![1, 2, 3, 4, 5, 6]);
            exec.close().unwrap();
        }
    }

    /// Go TestShuffleExit combines an immediate Next error with a delayed
    /// source panic and unconditional worker panics. Use a gate instead of
    /// Go's 100 ms sleep, so cleanup sees the same ordering deterministically.
    #[test]
    fn shuffle_exit_preserves_caller_error_and_joins_panicking_threads() {
        let (mut exec, _) = hash_shuffle(5, vec![vec![1, 2, 3, 4]]);
        let (release, gate) = bounded(0);
        exec.source_panic_gate = Some(gate);
        exec.panic_workers = true;
        exec.fail_next = true;
        exec.open().unwrap();
        let mut chunk = exec.new_chunk();
        let error = exec.next(&mut chunk).unwrap_err();
        assert!(format!("{error:?}").contains("ShuffleExec.Next error"));
        assert_eq!(exec.source_threads.len(), 1);
        assert_eq!(exec.worker_threads.len(), 5);
        // Drop wakes the source after the caller has already received its error.
        drop(release);
        exec.close().unwrap();
        assert!(exec.source_threads.is_empty());
        assert!(exec.worker_threads.is_empty());
        assert_eq!(exec.data_sources.len(), 1);
        assert_eq!(exec.workers.len(), 5);
        exec.fail_next = false;
        exec.panic_workers = false;
        exec.source_panic_gate = None;
        exec.open().unwrap();
        let mut rows: Vec<_> = drain(&mut exec).unwrap().into_iter().flatten().collect();
        rows.sort_unstable();
        assert_eq!(rows, vec![1, 2, 3, 4]);
        exec.close().unwrap();
    }

    #[test]
    fn shuffle_drop_joins_workers_without_explicit_close() {
        let (mut exec, _) = hash_shuffle(2, vec![vec![42; MAX_CHUNK]; 100]);
        exec.open().unwrap();
        let mut chunk = exec.new_chunk();
        exec.next(&mut chunk).unwrap();
        // If Drop failed to cancel a blocked holder receive, this would hang.
        drop(exec);
    }

    /// Every input row comes out exactly once, and equal keys stay together.
    #[test]
    fn shuffle_partitions_every_row_exactly_once() {
        let input: Vec<i64> = (0..17).map(|i| i % 5).collect();
        let (mut exec, _finish) = hash_shuffle(3, vec![input.clone()]);
        exec.open().unwrap();
        let out = drain(&mut exec).unwrap();
        exec.close().unwrap();

        let mut flat: Vec<i64> = out.iter().flatten().copied().collect();
        let mut expected = input;
        flat.sort_unstable();
        expected.sort_unstable();
        assert_eq!(flat, expected);
    }

    /// Equal partition keys stay together; output order between workers is
    /// deliberately unspecified, as in Go.
    #[test]
    fn shuffle_keeps_equal_partition_keys_together() {
        let (mut exec, _finish) = hash_shuffle(2, vec![vec![0, 1, 0, 1, 0, 1]]);
        exec.open().unwrap();
        let out = drain(&mut exec).unwrap();
        exec.close().unwrap();

        // Two keys, two workers: each worker emits one constant-valued run,
        // without requiring either worker to finish first.
        let runs: Vec<Vec<i64>> = out.clone();
        assert!(!runs.is_empty());
        for run in &runs {
            assert!(run.windows(2).all(|w| w[0] == w[1]), "run {run:?} is mixed");
        }
        let flat: Vec<i64> = out.into_iter().flatten().collect();
        let first = flat[0];
        let boundary = flat.iter().position(|&v| v != first).unwrap();
        assert!(flat[..boundary].iter().all(|&v| v == first));
        assert!(flat[boundary..].iter().all(|&v| v != first));
    }

    /// A buffer that fills mid-chunk is pushed immediately and recycled after
    /// consumption, so a partition larger than one chunk arrives as several -- in
    /// order.
    #[test]
    fn shuffle_splits_a_large_partition_into_ordered_chunks() {
        // One key: every row goes to worker 0, MAX_CHUNK rows per buffer.
        let input: Vec<i64> = vec![42; 10];
        let (mut exec, _finish) = hash_shuffle(1, vec![input[..4].to_vec(), input[4..].to_vec()]);
        exec.open().unwrap();
        let out = drain(&mut exec).unwrap();
        exec.close().unwrap();
        assert!(out.len() > 1, "expected several chunks, got {out:?}");
        let flat: Vec<i64> = out.into_iter().flatten().collect();
        assert_eq!(flat, input);
    }

    /// Two data sources, one receiver each: the concatenating child sees
    /// source 0's partition before source 1's, matching Go's
    /// `receivers[dataSourceIndex]` indexing.
    #[test]
    fn shuffle_feeds_one_receiver_per_data_source() {
        let finish = FinishFlag::new();
        let workers = build_workers(1, 2, &finish);
        let splitters: Vec<Box<dyn PartitionSplitter<NoColumns>>> = vec![
            Box::new(PartitionHashSplitter::new(1, vec![column_expr(0)])),
            Box::new(PartitionHashSplitter::new(1, vec![column_expr(0)])),
        ];
        let sources: Vec<Box<dyn Executor>> = vec![
            Box::new(ReplaySource::new(vec![vec![1, 2]])),
            Box::new(ReplaySource::new(vec![vec![3, 4]])),
        ];
        let mut exec = ShuffleExec::new(
            ExecutorMeta::new(one_col_schema(), 1, MAX_CHUNK, MAX_CHUNK),
            NoColumns,
            workers,
            splitters,
            sources,
            finish,
        );
        exec.open().unwrap();
        let out = drain(&mut exec).unwrap();
        exec.close().unwrap();
        let flat: Vec<i64> = out.into_iter().flatten().collect();
        assert_eq!(flat, vec![1, 2, 3, 4]);
    }

    /// A source failure reaches Next. Here neither partial input buffer is
    /// flushed before the error, so no worker can emit a chunk first.
    #[test]
    fn shuffle_surfaces_a_source_error() {
        let finish = FinishFlag::new();
        let workers = build_workers(2, 1, &finish);
        let mut source = ReplaySource::new(vec![vec![1, 2, 3]]);
        source.fail_at = Some(1);
        let splitters: Vec<Box<dyn PartitionSplitter<NoColumns>>> = vec![Box::new(
            PartitionHashSplitter::new(2, vec![column_expr(0)]),
        )];
        let mut exec = ShuffleExec::new(
            ExecutorMeta::new(one_col_schema(), 1, MAX_CHUNK, MAX_CHUNK),
            NoColumns,
            workers,
            splitters,
            vec![Box::new(source)],
            finish,
        );
        exec.open().unwrap();
        let mut chunk = exec.new_chunk();
        assert!(matches!(exec.next(&mut chunk), Err(ExecError::Internal(_))));
        exec.close().unwrap();
    }

    /// Go keeps the *first* error across worker children and data sources
    /// (`shuffle.go:189` then `shuffle.go:204`) and closes every one of them
    /// regardless. Worker children are closed first, so their error wins.
    #[test]
    fn close_keeps_the_first_error_and_closes_everything() {
        let finish = FinishFlag::new();
        let child_close_err = Arc::new(AtomicBool::new(true));
        let workers = build_workers_with(1, 1, &finish, &child_close_err);
        let mut source = ReplaySource::new(vec![vec![1]]);
        source.close_err = true;
        let splitters: Vec<Box<dyn PartitionSplitter<NoColumns>>> = vec![Box::new(
            PartitionHashSplitter::new(1, vec![column_expr(0)]),
        )];
        let mut exec = ShuffleExec::new(
            ExecutorMeta::new(one_col_schema(), 1, MAX_CHUNK, MAX_CHUNK),
            NoColumns,
            workers,
            splitters,
            vec![Box::new(source)],
            finish,
        );
        exec.open().unwrap();
        drain(&mut exec).unwrap();
        match exec.close() {
            Err(ExecError::Internal(message)) => {
                assert_eq!(message, "worker child close failed");
            }
            other => panic!("expected the worker child's error, got {other:?}"),
        }
    }

    /// A `Close`/`Open` cycle rewinds: Go's `Close` closes `finishCh` and
    /// `Open` makes a fresh one (`shuffle.go:124`), so a reopened shuffle
    /// replays instead of seeing a permanently finished flag.
    #[test]
    fn shuffle_is_replayable_after_close_and_reopen() {
        let (mut exec, _finish) = hash_shuffle(2, vec![vec![1, 2, 3, 4]]);
        exec.open().unwrap();
        let mut first: Vec<_> = drain(&mut exec).unwrap().into_iter().flatten().collect();
        exec.close().unwrap();

        exec.open().unwrap();
        let mut second: Vec<_> = drain(&mut exec).unwrap().into_iter().flatten().collect();
        exec.close().unwrap();
        first.sort_unstable();
        second.sort_unstable();
        assert_eq!(first, second);
    }
}
