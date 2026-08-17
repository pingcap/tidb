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
//! Go splits every source chunk row-by-row into per-worker buffers, hands those
//! buffers to N goroutines through channels, and merges whatever each worker
//! emits back onto one output channel. This port keeps the split, the buffers,
//! the receivers and the per-worker child executors exactly as Go has them, and
//! replaces the goroutines with a sequential drive. See `ORDERING` below for
//! what that costs and what it does not.
//!
//! ORDERING. Go's `Next` reads `e.outputCh` (`shuffle.go:253`), a single
//! buffered channel written by *every* worker's `run` (`shuffle.go:432`) and by
//! every source's `fetchDataAndSplit` error path (`shuffle.go:297`,
//! `shuffle.go:303`). Go channels impose no order between distinct senders, so
//! the interleaving of one worker's chunks with another's is genuinely
//! nondeterministic upstream -- `ShuffleExec` is only planned under parents
//! that do not need a global order (each `PARTITION BY` group lands wholly
//! inside one worker). Three orderings *are* structural, and this port
//! preserves all three:
//!
//! 1. Within one worker, the child executor is driven by one goroutine in a
//!    loop (`shuffle.go:414-434`), so that worker's output chunks keep the
//!    order its child produced them.
//! 2. Within one (source, worker) pair, rows are appended in ascending source
//!    row index (`shuffle.go:305-320`: `for i := range numRows` appending
//!    `chk.GetRow(i)` into `results[workerIdx]`), and each filled buffer is
//!    pushed to that receiver's `inputCh` before the next one is started. So a
//!    worker sees its share of a source in the source's own relative order.
//! 3. A source's chunks reach `inputCh` in source order, because one goroutine
//!    per source owns that loop.
//!
//! This port chooses the one legal interleaving that is easiest to reason
//! about: every source is fetched and split to exhaustion, then every worker is
//! drained in ascending worker index. Errors are queued as they occur, so a
//! fetch error precedes any worker output -- also a legal Go interleaving, and
//! the safe one (the caller sees the failure rather than a truncated result).
//!
//! The one behavioural difference this creates is *not* visible in the row
//! stream: Go stops pulling a source once every worker has exited and the
//! finish channel closes, whereas this port always drains each source fully
//! before running any worker. That changes how many times a source's `next` is
//! called under an early-stopping child, never which rows come out.
//!
//! TESTS: WRITTEN, not transcreated. `pkg/executor/shuffle.go` has no
//! `shuffle_test.go`; its upstream coverage lives in
//! `pkg/executor/test/executor/executor_test.go` and
//! `pkg/executor/aggregate/agg_test.go` behind testkit sessions and the
//! `shuffleError` / `shuffleExecFetchDataAndSplit` / `shuffleWorkerRun`
//! failpoints, none of which are reachable from this crate. The murmur3
//! vectors are the published MurmurHash3 x86_32 reference values, which is
//! what `github.com/twmb/murmur3.Sum32` computes.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::rc::Rc;

use tidb_chunk::chunk::Chunk;
use tidb_datatype::FieldType;
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_expr::Columns;

use crate::executor::{ExecError, Executor, ExecutorMeta};
use crate::hash_agg::group_key_part;
use crate::vec_group_checker::VecGroupChecker;

/// Go `shuffleOutput` (`shuffle.go:106`): one item on the shuffle's output
/// channel.
///
/// Go's struct carries `chk`, `err` and `giveBackCh` together and readers
/// inspect `err` first (`shuffle.go:258`), so exactly one of the two is ever
/// meaningful. Making that an enum removes the third state (both set) rather
/// than trusting a convention.
///
/// boundary: Go `shuffleOutput.giveBackCh` is dropped. It returns the emptied
/// chunk to `shuffleWorker.outputHolderCh` so the worker can refill it; it
/// carries no rows and no error, and this port allocates a fresh chunk per
/// worker iteration instead of recycling one.
enum ShuffleOutput {
    /// Go `shuffleOutput{chk: ..., giveBackCh: ...}`. Never empty: the worker
    /// explicitly refuses to send a zero-row chunk (`shuffle.go:428`).
    Chunk(Chunk),
    /// Go `shuffleOutput{err: ...}`.
    Err(ExecError),
}

/// The shared mailbox behind Go's `shuffleReceiver.inputCh`.
///
/// Go pairs `inputCh` with `inputHolderCh`, a one-slot channel that returns the
/// drained chunk to the splitter so it can be refilled. The holder channel is a
/// pure allocation optimisation -- it moves no rows and reports no state -- so
/// only the row-carrying direction is modelled here.
///
/// boundary: Go `shuffleReceiver.inputHolderCh`.
#[derive(Debug, Default)]
struct Inbox {
    /// Chunks queued for the receiver, in the order the splitter filled them.
    queue: RefCell<VecDeque<Chunk>>,
    /// Go's closing of `inputCh` in `fetchDataAndSplit`'s defer
    /// (`shuffle.go:288-290`): no further chunk will arrive for this
    /// (source, worker) pair.
    closed: Cell<bool>,
}

/// A handle on one receiver's mailbox, shared by the splitter that fills it and
/// the [`ShuffleReceiver`] that drains it.
#[derive(Clone, Debug, Default)]
pub struct InboxHandle(Rc<Inbox>);

impl InboxHandle {
    /// A fresh, open mailbox.
    #[must_use]
    pub fn new() -> Self {
        InboxHandle(Rc::new(Inbox::default()))
    }

    fn push(&self, chunk: Chunk) {
        self.0.queue.borrow_mut().push_back(chunk);
    }

    fn pop(&self) -> Option<Chunk> {
        self.0.queue.borrow_mut().pop_front()
    }

    fn close(&self) {
        self.0.closed.set(true);
    }

    /// Go's `channel.Clear(r.inputCh)` in `ShuffleExec.Close`
    /// (`shuffle.go:186`): drop whatever the splitter left queued.
    fn clear(&self) {
        self.0.queue.borrow_mut().clear();
    }

    /// Whether Go's `inputCh` has been closed for this (source, worker) pair.
    fn is_closed(&self) -> bool {
        self.0.closed.get()
    }

    fn reopen(&self) {
        self.0.queue.borrow_mut().clear();
        self.0.closed.set(false);
    }
}

/// A handle on Go's `finishCh`, the broadcast that tells sources and workers to
/// abandon their loops.
///
/// Go closes it only in `ShuffleExec.Close` (`shuffle.go:171`). Because this
/// port runs sources and workers inside `Next` rather than on goroutines,
/// `Close` cannot overlap them and the flag is never observed set from inside a
/// loop -- the checks are kept so the structure matches Go's and so a future
/// concurrent drive inherits the same shape.
#[derive(Clone, Debug, Default)]
pub struct FinishFlag(Rc<Cell<bool>>);

impl FinishFlag {
    /// An unset flag, matching Go's freshly made `finishCh`.
    #[must_use]
    pub fn new() -> Self {
        FinishFlag(Rc::new(Cell::new(false)))
    }

    fn is_set(&self) -> bool {
        self.0.get()
    }

    fn set(&self) {
        self.0.set(true);
    }

    /// Go's `Open` replacing `finishCh` with a fresh, open channel.
    fn reset(&self) {
        self.0.set(false);
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
        // Go's `select` over `finishCh` and `inputCh`.
        if self.finish.is_set() {
            self.executed = true;
            return Ok(());
        }
        match self.inbox.pop() {
            // Go's `!ok` (channel closed and drained) branch.
            None if self.inbox.is_closed() => {
                self.executed = true;
                Ok(())
            }
            // Empty but still open: Go would *block* here waiting for the
            // splitter. The sequential drive exhausts every source (closing
            // every mailbox) before it runs a single worker, so reaching this
            // means that ordering was broken. Reporting it is the safe
            // direction -- silently treating it as exhaustion would drop the
            // rows still to come.
            None => Err(ExecError::internal(
                "shuffle receiver read an open but empty input channel",
            )),
            Some(mut result) => {
                // Go quirk reproduced: a zero-row chunk on `inputCh` ends the
                // receiver just like a closed channel (`shuffle.go:388`),
                // *without* forwarding it. The splitter never sends one -- it
                // only pushes a buffer it has appended at least one row to --
                // so this arm is defensive in Go too, and stays so here.
                if result.num_rows() == 0 {
                    self.executed = true;
                    return Ok(());
                }
                req.swap_columns(&mut result);
                Ok(())
            }
        }
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

    /// Go `shuffleWorker.run` (`shuffle.go:409`).
    ///
    /// boundary: Go's `defer recover() -> recoveryShuffleExec` and the
    /// `shuffleWorkerRun` failpoint are dropped; this port has no panicking
    /// child to catch.
    fn run(&mut self, output: &mut VecDeque<ShuffleOutput>) {
        loop {
            if self.finish.is_set() {
                return;
            }
            let mut chk = self.child_exec.new_chunk();
            if let Err(err) = self.child_exec.next(&mut chk) {
                output.push_back(ShuffleOutput::Err(err));
                return;
            }
            // Go: "Should not send an empty `chk` to `e.outputCh`."
            if chk.num_rows() == 0 {
                return;
            }
            output.push_back(ShuffleOutput::Chunk(chk));
        }
    }
}

/// Go `partitionSplitter` (`shuffle.go:440`): assigns each input row a worker.
///
/// Go's `split` returns the (reused) index slice; this port writes into a
/// caller-owned buffer instead, which keeps the reuse without the aliasing.
/// Go leaves the buffer untouched on error (it returns the argument unchanged
/// at `shuffle.go:453` and `shuffle.go:494`) and so does this signature.
pub trait PartitionSplitter<C: Columns> {
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
        for item in &self.by_items {
            // Same derivation `hash_agg`'s private `expr_collation` performs,
            // so a shuffle partition key and a hash-aggregation group key are
            // encoded identically.
            let collation = tidb_expr::collation_derive::collation_of_node(item);
            for row_index in 0..num_rows {
                let datum = item.eval(ctx, input.get_row(row_index))?;
                // `group_key_part` is this crate's port of Go's
                // `codec.HashGroupKey` element encoding, shared with
                // `hash_agg`/`stream_agg` so a shuffle partition and a hash
                // aggregation group agree on what "same key" means.
                let part = group_key_part(&collation, &datum);
                self.hash_keys[row_index].extend_from_slice(&part);
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
    /// into the checker here. Go's `EnableVectorizedExpression` argument to
    /// `NewVecGroupChecker` selects an evaluation strategy, not a grouping;
    /// this crate's `VecGroupChecker::new` takes no such switch.
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
    /// Go's `outputCh`. Go sizes it `concurrency + len(dataSources)`; a
    /// `VecDeque` needs no bound because nothing here blocks on a full channel.
    output: VecDeque<ShuffleOutput>,

    ctx: C,
    /// Go `allSourceAndWorkerExitForTest` (`shuffle.go:104`).
    all_source_and_worker_exit_for_test: bool,
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
            output: VecDeque::new(),
            ctx,
            all_source_and_worker_exit_for_test: false,
        }
    }

    /// Go `ShuffleExec.concurrency`, reported for the runtime-stats line Go
    /// registers in `Close` (`shuffle.go:196`).
    #[must_use]
    pub fn concurrency(&self) -> usize {
        self.concurrency
    }

    /// Go `ShuffleExec.prepare4ParallelExec` (`shuffle.go:217`), sequentially.
    ///
    /// Go starts one goroutine per data source, one per worker, and one joiner
    /// (`waitWorkerAndCloseOutput`). Here the sources run to exhaustion first
    /// -- which is what closes every mailbox, exactly as Go's per-source defer
    /// does -- and the workers then run in ascending index. See the module
    /// header for why that is a legal interleaving of Go's channel order.
    fn prepare4_parallel_exec(&mut self) {
        self.all_source_and_worker_exit_for_test = false;
        let ShuffleExec {
            workers,
            splitters,
            data_sources,
            finish,
            output,
            ctx,
            ..
        } = self;

        for (source_index, (source, splitter)) in data_sources
            .iter_mut()
            .zip(splitters.iter_mut())
            .enumerate()
        {
            fetch_data_and_split(
                source.as_mut(),
                splitter.as_mut(),
                ctx,
                workers,
                source_index,
                finish,
                output,
            );
        }

        for worker in workers.iter_mut() {
            worker.run(output);
        }

        // Go `waitWorkerAndCloseOutput` (`shuffle.go:234`).
        self.all_source_and_worker_exit_for_test = true;
    }
}

/// Go `ShuffleExec.fetchDataAndSplit` (`shuffle.go:278`).
///
/// Free-standing so the source, its splitter and the workers' mailboxes are
/// borrowed as the disjoint pieces they are.
///
/// boundary: Go's `defer recover() -> recoveryShuffleExec` and the
/// `shuffleExecFetchDataAndSplit` failpoint are dropped -- neither the sources
/// nor the splitters panic in this port; their failures are `ExecError`s.
fn fetch_data_and_split<C: Columns>(
    source: &mut dyn Executor,
    splitter: &mut dyn PartitionSplitter<C>,
    ctx: &C,
    workers: &[ShuffleWorker],
    source_index: usize,
    finish: &FinishFlag,
    output: &mut VecDeque<ShuffleOutput>,
) {
    // Go's `defer` closes this source's `inputCh` on every worker, on every
    // exit path. `close_inboxes` is called at each `return` below for the same
    // reason: a worker's receiver must be able to tell "drained" from
    // "waiting".
    let close_inboxes = |workers: &[ShuffleWorker]| {
        for worker in workers {
            worker.inboxes[source_index].close();
        }
    };

    let mut results: Vec<Option<Chunk>> = (0..workers.len()).map(|_| None).collect();
    let mut worker_indices: Vec<usize> = Vec::new();
    // Go `exec.TryNewCacheChunk(e.dataSources[dataSourceIndex])`.
    let mut chk = source.new_chunk();

    loop {
        if let Err(err) = source.next(&mut chk) {
            output.push_back(ShuffleOutput::Err(err));
            close_inboxes(workers);
            return;
        }
        if chk.num_rows() == 0 {
            break;
        }
        if let Err(err) = splitter.split(ctx, &chk, &mut worker_indices) {
            output.push_back(ShuffleOutput::Err(err));
            close_inboxes(workers);
            return;
        }
        // Go indexes `workerIndices[i]` for `i` in `0..numRows` and would
        // panic on a short slice. Asserting the length up front keeps that
        // loudness while letting the loop walk the indices directly -- walking
        // them without the check would silently *drop* the unassigned tail
        // rows, which is exactly the wrong direction for a narrowing.
        assert_eq!(
            worker_indices.len(),
            chk.num_rows(),
            "partition splitter must assign every input row a worker"
        );
        for (row_index, &worker_index) in worker_indices.iter().enumerate() {
            let worker = &workers[worker_index];
            if results[worker_index].is_none() {
                // Go blocks here on `select { <-finishCh; <-inputHolderCh }`.
                if finish.is_set() {
                    close_inboxes(workers);
                    return;
                }
                // Go `exec.NewFirstChunk(e.dataSources[i])`, pushed onto
                // `inputHolderCh` in `Open` (`shuffle.go:143`): the buffer
                // carries the *source*'s types and sizing, not the shuffle's.
                results[worker_index] = Some(source.new_chunk());
            }
            let buffer = results[worker_index]
                .as_mut()
                .expect("shuffle split buffer was just installed");
            buffer.append_row(chk.get_row(row_index));
            if buffer.is_full() {
                let full = results[worker_index]
                    .take()
                    .expect("shuffle split buffer was just filled");
                worker.inboxes[source_index].push(full);
            }
        }
    }

    // Go flushes the partial buffers before the defer closes the channels.
    for (worker_index, worker) in workers.iter().enumerate() {
        if let Some(partial) = results[worker_index].take() {
            worker.inboxes[source_index].push(partial);
        }
    }
    close_inboxes(workers);
}

impl<C: Columns> Executor for ShuffleExec<C> {
    /// Go `ShuffleExec.Open` (`shuffle.go:113`).
    fn open(&mut self) -> Result<(), ExecError> {
        for source in &mut self.data_sources {
            source.open()?;
        }

        self.prepared = false;
        self.output.clear();
        // Go allocates a *new* `finishCh` here (`shuffle.go:124`), so a
        // shuffle reopened after `Close` is not permanently finished. The Rc
        // is shared with every worker and receiver, which is how Go's
        // `w.finishCh = e.finishCh` fan-out (`shuffle.go:128`) reaches them.
        self.finish.reset();
        for worker in &mut self.workers {
            for inbox in &worker.inboxes {
                inbox.reopen();
            }
            worker.child_exec.open()?;
        }
        Ok(())
    }

    /// Go `ShuffleExec.Next` (`shuffle.go:241`).
    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        if !self.prepared {
            self.prepare4_parallel_exec();
            self.prepared = true;
        }
        // boundary: Go's `shuffleError` failpoint (`shuffle.go:247`) has no
        // Rust counterpart.
        if self.executed {
            return Ok(());
        }
        match self.output.pop_front() {
            // Go's closed-and-drained `outputCh`.
            None => {
                self.executed = true;
                Ok(())
            }
            Some(ShuffleOutput::Err(err)) => Err(err),
            Some(ShuffleOutput::Chunk(mut chk)) => {
                req.swap_columns(&mut chk);
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
    /// boundary: Go's `RuntimeStatsWithConcurrencyInfo` registration
    /// (`shuffle.go:194-198`) is dropped with the rest of this crate's runtime
    /// stats; [`ShuffleExec::concurrency`] exposes the number it would report.
    fn close(&mut self) -> Result<(), ExecError> {
        let mut first_err: Option<ExecError> = None;

        self.finish.set();
        self.output.clear();

        // Go's `intest` assertion: if `Next` ever ran, every source and worker
        // must have finished. The sequential drive makes that structural --
        // `prepare4_parallel_exec` returns only after both loops complete --
        // so this can only fire if that invariant is broken by a later edit.
        debug_assert!(
            !self.prepared || self.all_source_and_worker_exit_for_test,
            "there are still some running sources or workers"
        );

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
    use tidb_datatype::FieldTypeCode;
    use tidb_expr::column::Column;
    use tidb_expr::NoColumns;

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
        close_err: bool,
    }

    impl ReplaySource {
        fn new(batches: Vec<Vec<i64>>) -> Self {
            ReplaySource {
                meta: ExecutorMeta::new(one_col_schema(), 0, MAX_CHUNK, MAX_CHUNK),
                batches,
                next_batch: 0,
                fail_at: None,
                close_err: false,
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
        close_err: Rc<Cell<bool>>,
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
            if self.close_err.get() {
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
        build_workers_with(concurrency, num_sources, finish, &Rc::new(Cell::new(false)))
    }

    fn build_workers_with(
        concurrency: usize,
        num_sources: usize,
        finish: &FinishFlag,
        close_err: &Rc<Cell<bool>>,
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
        inbox.push(chunk_of(&[9]));
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

    /// The fail-loud narrowing: an open-but-empty mailbox is Go's blocking
    /// case, unreachable in the sequential drive, and is reported rather than
    /// silently read as exhaustion.
    #[test]
    fn receiver_reports_an_open_but_empty_mailbox() {
        let mut receiver = ShuffleReceiver::new(
            ExecutorMeta::new(one_col_schema(), 0, MAX_CHUNK, MAX_CHUNK),
            InboxHandle::new(),
            FinishFlag::new(),
        );
        receiver.open().unwrap();
        let mut chunk = receiver.new_chunk();
        assert!(matches!(
            receiver.next(&mut chunk),
            Err(ExecError::Internal(_))
        ));
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

    /// The ordering guarantee this port fixes: workers are drained in
    /// ascending index, and within a worker the source's relative row order
    /// survives.
    #[test]
    fn shuffle_output_is_worker_ordered_and_stable_within_a_worker() {
        let (mut exec, _finish) = hash_shuffle(2, vec![vec![0, 1, 0, 1, 0, 1]]);
        exec.open().unwrap();
        let out = drain(&mut exec).unwrap();
        exec.close().unwrap();

        // Two keys, two workers: each worker emits one constant-valued run,
        // and worker 0's run precedes worker 1's.
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

    /// A buffer that fills mid-chunk is pushed immediately and a fresh one
    /// started, so a partition larger than one chunk arrives as several -- in
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

    /// A source failure reaches the caller through `Next`, ahead of any worker
    /// output -- the safe interleaving of Go's shared output channel.
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
        let child_close_err = Rc::new(Cell::new(true));
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
        let first = drain(&mut exec).unwrap();
        exec.close().unwrap();

        exec.open().unwrap();
        let second = drain(&mut exec).unwrap();
        assert_eq!(first, second);
    }
}
