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

//! `pkg/executor/sortexec/parallel_sort_spill_helper.go`: the spill half of the
//! PARALLEL sort.
//!
//! Go's parallel sort keeps one `parallelSortWorker` per worker goroutine, each
//! holding its own slice of rows. When the memory action fires, this helper
//! makes every worker sort what it holds, merges those per-worker sorted runs
//! with a [`MultiWayMerger`], and streams the merged rows into ONE spill file
//! -- so each spill round produces exactly one new sorted run on disk, and the
//! workers' memory is released. `sort.go` later merges the accumulated runs.
//!
//! COMPLETE for this Go file. Verified symbol by symbol against
//! `parallel_sort_spill_helper.go`: the `parallelSortSpillHelper` struct and
//! all fourteen of its declarations -- `newParallelSortSpillHelper`, `close`,
//! `isNotSpilledNoLock`, `isInSpillingNoLock`, `isSpillNeeded`,
//! `isSpillTriggered`, `setInSpilling`, `setNeedSpillNoLock`, `setNotSpilled`,
//! `spill`, `releaseMemory`, `spillTmpSpillChunk`, `initForSpill`, `spillImpl`
//! -- are present. The file declares nothing else.
//!
//! `parallel_sort_worker.go` and the `parallelSortSpillAction` half of
//! `sort_spill.go` are separate files and are still unported; this module
//! stands in for the worker with the [`LocalSortWorker`] trait and for the
//! action with [`ParallelSortSpillHelper::set_need_spill`].
//!
//! Three members are NOT from this Go file and are named here so the claim
//! above is not read as covering them:
//!
//! * [`ParallelSortSpillHelper::wait_for_spill_finish`] is
//!   `sort_spill.go:113-114`'s `for s.spillHelper.isInSpillingNoLock() {
//!   s.spillHelper.cond.Wait() }`. Go reaches into `p.cond` from the action's
//!   file; a `Condvar` guarded by a private `Mutex` cannot be waited on from
//!   outside, so the loop lives with the lock.
//! * [`ParallelSortSpillHelper::set_bytes_info`] and
//!   [`ParallelSortSpillHelper::err_output_chan`] are accessors for fields this
//!   Go file declares but only `sort_spill.go` writes and reads.
//! * [`ParallelSortSpillHelper::sorted_rows_in_disk`] and
//!   [`ParallelSortSpillHelper::take_sorted_rows_in_disk`] expose
//!   `sortedRowsInDisk`, which Go reads directly as a struct field from
//!   `sort.go`.
//!
//! # Ordering and concurrency
//!
//! Go spills concurrently but the ROW ORDER ON DISK is fully determined, so
//! this port runs the same work sequentially without changing what is
//! observable:
//!
//! * `spill` fans out one goroutine per worker, but each writes its result to
//!   `sortedRowsIters[idx]` -- a slot indexed by the worker's own index, not an
//!   append -- and `workerWaiter.Wait()` joins them all before the merge is
//!   built. The merge input is therefore worker-index-ordered no matter how the
//!   goroutines interleave. This port runs the workers in index order for the
//!   same input.
//! * `spillImpl` runs a SINGLE producer goroutine (`merger.next()` in a loop)
//!   feeding a buffered channel that a SINGLE consumer (the `OuterLoop`) drains
//!   in FIFO order. One producer plus one FIFO channel plus one consumer means
//!   the rows reach `tmpSpillChunk` in exactly merger order, so this port drops
//!   the channel and calls `merger.next()` from the consumer loop directly.
//!
//! The one Go behavior that is genuinely nondeterministic is WHICH error
//! surfaces when several workers fail at once: Go collects them into a buffered
//! `errChannel`, closes it, and returns the first value read
//! (`for err := range errChannel { return err }`), which is arrival order. This
//! port returns the first failure in WORKER INDEX order. Only the identity of
//! one of several concurrent errors differs, and Go's own choice is not
//! reproducible.
//!
//! # Narrowings, by name
//!
//! * `sortExec *SortExec` is not a field here. `SortExec` is not ported at this
//!   tier, and only three things are read through it, so they are held
//!   directly: `sortExec.Parallel.workers` -> `workers`,
//!   `sortExec.memTracker` -> `mem_tracker`, `sortExec.diskTracker` ->
//!   `disk_tracker`.
//! * `finishCh chan struct{}` -> a shared [`AtomicBool`]. Go only ever
//!   *closes* it and *polls* it with a non-blocking `select`, never sends, so a
//!   latch reproduces it exactly.
//! * `errOutputChan chan rowWithError` is carried but never read in this Go
//!   file -- it belongs to `parallelSortSpillAction`. It is kept as a field
//!   with an accessor so the struct stays whole.
//! * `injectErrorForIssue59655`, `injectParallelSortRandomFail` and
//!   `injectPanicForIssue63216` are failpoints, i.e. test-only fault injection;
//!   they are not reproduced.
//! * Go's `recover()` wrappers turn a panic into an error. Rust panics are not
//!   caught here; every fallible step already returns [`ExecError`].
//! * Go's `spill` computes `totalRows` and never uses it. Dead in Go, dropped
//!   here.
//! * `storage: Arc<SpillStorage>` has no Go counterpart field. Go's
//!   `chunk.NewDataInDiskByChunks` finds the temp directory through package
//!   globals in `chunk`; here the directory is an explicit dependency.
//! * Go's `close` returns `tmpSpillChunk` to a chunk pool with
//!   `Destroy(spillChunkSize, fieldTypes)`. There is no chunk pool at this
//!   tier, so the chunk is dropped. Pooling is an allocation strategy, not an
//!   observable.
//!
//! # Tests
//!
//! ALL TWELVE TESTS IN THIS MODULE ARE WRITTEN, NOT TRANSCREATED. Upstream's
//! coverage for this file is `parallel_sort_spill_test.go`, which drives a
//! whole `SortExec` through `testkit` and failpoints; none of that is reachable
//! at this tier, so there is no Go test to transcreate. Each test names the Go
//! lines it pins.
//!
//! # Tie-breaking
//!
//! When two runs' heads compare Equal, which row is emitted first is decided by
//! `container/heap`'s sift order, not by worker index. [`MultiWayMerger`]
//! reproduces Go's heap operations exactly (see `multi_way_merge.rs`), so this
//! port makes the same choice Go does. It is pinned by
//! `equal_keys_break_ties_the_way_gos_heap_does` below rather than left to
//! chance, because a sort that is "stable by worker index" is a claim this code
//! does NOT make.

use std::cmp::Ordering;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering::SeqCst};
use std::sync::mpsc::Sender;
use std::sync::{Arc, Condvar, Mutex};

use tidb_chunk::chunk::Chunk;
use tidb_chunk::chunk_in_disk::DataInDiskByChunks;
use tidb_chunk::row::OwnedRow;
use tidb_datatype::FieldType;
use tidb_util::disk::{self, SpillStorage};
use tidb_util::memory::Tracker;

use crate::executor::ExecError;
use crate::multi_way_merge::{MemorySource, MultiWayMerger};
use crate::sort_util::{RowWithError, SpillStatus, SPILL_CHUNK_SIZE};

/// Go `spillInfo`: the message logged when a spill round starts.
pub const SPILL_INFO: &str = "memory exceeds quota, spill to disk now.";

/// boundary: Go `parallelSortWorker` (`parallel_sort_worker.go`), which is not
/// ported yet. This trait is the part of it this file uses.
///
/// Only the two things `parallel_sort_spill_helper.go` asks of a worker are in
/// the trait.
pub trait LocalSortWorker {
    /// Go `parallelSortWorker.sortLocalRows`: sorts the rows this worker holds
    /// and hands them over as ONE sorted run.
    ///
    /// Go returns `[]chunk.Row` borrowed from the worker's chunks and clears
    /// the worker's own slice; the run is owned here so the merge can outlive
    /// the worker's buffers.
    fn sort_local_rows(&mut self) -> Result<Vec<OwnedRow>, ExecError>;

    /// Go `releaseMemory`'s `worker.totalMemoryUsage` read followed by
    /// `worker.totalMemoryUsage = 0`.
    ///
    /// The two steps are one method so an implementor cannot report memory it
    /// does not then give up -- double-counting would make the helper credit
    /// the tracker twice.
    fn take_total_memory_usage(&mut self) -> i64;
}

/// Go `parallelSortSpillHelper`.
///
/// `W` is the worker (`sortExec.Parallel.workers`' element) and `F` is
/// `lessRowFunc`.
pub struct ParallelSortSpillHelper<W, F> {
    /// Go `cond` + `spillStatus`. Go guards the status with `cond.L` and
    /// broadcasts on it; the same pair, with the status inside the mutex so it
    /// cannot be read unguarded.
    status: Mutex<SpillStatus>,
    /// Go `cond`'s wait/broadcast side.
    cond: Condvar,
    /// Go `sortedRowsInDisk`: one sorted run per completed spill round.
    sorted_rows_in_disk: Vec<DataInDiskByChunks>,
    /// boundary: Go `sortExec.Parallel.workers`.
    workers: Vec<W>,
    /// boundary: Go `sortExec.memTracker`.
    mem_tracker: Arc<Tracker>,
    /// boundary: Go `sortExec.diskTracker`.
    disk_tracker: Arc<disk::Tracker>,
    /// boundary: Go `chunk.NewDataInDiskByChunks`' implicit temp directory,
    /// which it reads from package globals in `chunk`. Made explicit here.
    storage: Arc<SpillStorage>,
    /// Go `lessRowFunc`.
    less: F,
    /// boundary: Go `errOutputChan chan rowWithError`. Declared by this Go file
    /// but written only by `parallel_sort_worker.go` and `sort_util.go`'s
    /// `processPanicAndLog`; carried, never sent on here.
    err_output_chan: Sender<RowWithError<OwnedRow>>,
    /// boundary: Go `finishCh chan struct{}` -- see the module narrowings.
    finish: Arc<AtomicBool>,
    /// Go `fieldTypes`.
    field_types: Vec<FieldType>,
    /// Go `tmpSpillChunk`, allocated lazily by `initForSpill`.
    tmp_spill_chunk: Option<Chunk>,
    /// Go `bytesConsumed`, set by the spill action for the log line.
    bytes_consumed: AtomicI64,
    /// Go `bytesLimit`, likewise.
    bytes_limit: AtomicI64,
    /// Go `fileNamePrefixForTest`.
    file_name_prefix_for_test: String,
}

impl<W, F> ParallelSortSpillHelper<W, F>
where
    W: LocalSortWorker,
    F: FnMut(&OwnedRow, &OwnedRow) -> Result<Ordering, ExecError>,
{
    /// Go `newParallelSortSpillHelper`.
    ///
    /// `workers`, `mem_tracker`, `disk_tracker` and `storage` replace Go's
    /// `sortExec` argument; the rest map one-to-one.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        workers: Vec<W>,
        mem_tracker: Arc<Tracker>,
        disk_tracker: Arc<disk::Tracker>,
        storage: Arc<SpillStorage>,
        field_types: Vec<FieldType>,
        finish: Arc<AtomicBool>,
        less: F,
        err_output_chan: Sender<RowWithError<OwnedRow>>,
        file_name_prefix_for_test: &str,
    ) -> Self {
        ParallelSortSpillHelper {
            status: Mutex::new(SpillStatus::NotSpilled),
            cond: Condvar::new(),
            sorted_rows_in_disk: Vec::new(),
            workers,
            mem_tracker,
            disk_tracker,
            storage,
            less,
            err_output_chan,
            finish,
            field_types,
            tmp_spill_chunk: None,
            bytes_consumed: AtomicI64::new(0),
            bytes_limit: AtomicI64::new(0),
            file_name_prefix_for_test: file_name_prefix_for_test.to_owned(),
        }
    }

    /// Go `close`: removes every spill file and drops the scratch chunk.
    ///
    /// boundary: Go `chunk.Chunk.Destroy(spillChunkSize, fieldTypes)` returns
    /// `tmpSpillChunk` to a chunk pool; there is no pool here, so the chunk is
    /// dropped.
    pub fn close(&mut self) {
        for in_disk in &mut self.sorted_rows_in_disk {
            in_disk.close();
        }
        self.tmp_spill_chunk = None;
    }

    /// Go `errOutputChan`, for the spill action that reports failures.
    #[must_use]
    pub fn err_output_chan(&self) -> &Sender<RowWithError<OwnedRow>> {
        &self.err_output_chan
    }

    /// Go `bytesConsumed.Store`/`bytesLimit.Store`, which the spill action does
    /// before it asks for a spill so the log line can name both numbers.
    pub fn set_bytes_info(&self, consumed: i64, limit: i64) {
        self.bytes_consumed.store(consumed, SeqCst);
        self.bytes_limit.store(limit, SeqCst);
    }

    /// Go `isNotSpilledNoLock`.
    ///
    /// Go's `NoLock` suffix means the caller already holds `cond.L`; Rust holds
    /// the same lock inside, which is why the guard is a parameter-free method
    /// here.
    #[must_use]
    pub fn is_not_spilled(&self) -> bool {
        *self.lock_status() == SpillStatus::NotSpilled
    }

    /// Go `isInSpillingNoLock`.
    #[must_use]
    pub fn is_in_spilling(&self) -> bool {
        *self.lock_status() == SpillStatus::InSpilling
    }

    /// Go `isSpillNeeded`.
    #[must_use]
    pub fn is_spill_needed(&self) -> bool {
        *self.lock_status() == SpillStatus::NeedSpill
    }

    /// Go `isSpillTriggered`: whether any round has already reached disk.
    ///
    /// Note this reads `len(sortedRowsInDisk)`, NOT the status ladder -- a
    /// helper can be back in `notSpilled` and still have runs on disk.
    #[must_use]
    pub fn is_spill_triggered(&self) -> bool {
        let _guard = self.lock_status();
        !self.sorted_rows_in_disk.is_empty()
    }

    /// Go `setInSpilling`.
    pub fn set_in_spilling(&self) {
        *self.lock_status() = SpillStatus::InSpilling;
    }

    /// Go `setNeedSpillNoLock`: what the spill action raises.
    pub fn set_need_spill(&self) {
        *self.lock_status() = SpillStatus::NeedSpill;
    }

    /// Go `setNotSpilled`.
    pub fn set_not_spilled(&self) {
        *self.lock_status() = SpillStatus::NotSpilled;
    }

    /// Go `cond.Wait()`: blocks while a spill round is running.
    ///
    /// Go's spill action loops `for isInSpillingNoLock() { cond.Wait() }`; that
    /// loop is this method, with the spurious-wakeup guard the `Condvar`
    /// contract also requires.
    pub fn wait_for_spill_finish(&self) {
        let mut status = self.lock_status();
        while *status == SpillStatus::InSpilling {
            status = self
                .cond
                .wait(status)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }

    fn lock_status(&self) -> std::sync::MutexGuard<'_, SpillStatus> {
        self.status
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The spilled runs, for the merge `sort.go` does once input is exhausted.
    #[must_use]
    pub fn sorted_rows_in_disk(&self) -> &[DataInDiskByChunks] {
        &self.sorted_rows_in_disk
    }

    /// Takes the spilled runs out of the helper, leaving it with none.
    pub fn take_sorted_rows_in_disk(&mut self) -> Vec<DataInDiskByChunks> {
        std::mem::take(&mut self.sorted_rows_in_disk)
    }

    /// Go `spill`: one whole spill round.
    ///
    /// Every worker sorts its own rows, the runs are merged, and the merged
    /// stream becomes one new file in `sortedRowsInDisk`.
    pub fn spill(&mut self) -> Result<(), ExecError> {
        self.set_in_spilling();
        let result = self.spill_body();
        // Go registers `defer p.cond.Broadcast()` BEFORE `defer
        // p.setNotSpilled()`, so the status is restored first and the waiters
        // are woken second -- otherwise a woken waiter could observe
        // `inSpilling` and go back to sleep.
        self.set_not_spilled();
        self.cond.notify_all();
        result
    }

    fn spill_body(&mut self) -> Result<(), ExecError> {
        // Go: a non-blocking `select` on finishCh -- an already-finished query
        // spills nothing.
        if self.finish.load(SeqCst) {
            return Ok(());
        }

        let mut sorted_runs = Vec::with_capacity(self.workers.len());
        for worker in &mut self.workers {
            // See the module note on error identity: Go returns whichever of
            // several concurrent failures reached its channel first.
            sorted_runs.push(worker.sort_local_rows()?);
        }

        let source = MemorySource::new(sorted_runs);
        let mut merger = MultiWayMerger::new(source, &mut self.less);
        // Go writes `_ = merger.init()`, discarding the error. Kept: a helper
        // whose keys fail to compare must still reach `spillImpl`, where the
        // first `next()` raises the same failure. Silently succeeding here and
        // failing there is what Go's callers see.
        let _ = merger.init();

        let mut spill = SpillRound {
            sorted_rows_in_disk: &mut self.sorted_rows_in_disk,
            disk_tracker: &self.disk_tracker,
            storage: &self.storage,
            field_types: &self.field_types,
            tmp_spill_chunk: &mut self.tmp_spill_chunk,
            finish: &self.finish,
            file_name_prefix_for_test: &self.file_name_prefix_for_test,
            bytes_consumed: self.bytes_consumed.load(SeqCst),
            bytes_limit: self.bytes_limit.load(SeqCst),
        };
        let released = spill.spill_impl(&mut merger)?;
        // Go `releaseMemory`, called from inside `spillImpl` right after the
        // file is appended. Hoisted out only because the workers and the
        // scratch chunk are borrowed disjointly here.
        if released {
            self.release_memory();
        }
        Ok(())
    }

    /// Go `releaseMemory`: give back everything the workers were charged for.
    fn release_memory(&mut self) {
        let mut total_released_memory = 0i64;
        for worker in &mut self.workers {
            total_released_memory += worker.take_total_memory_usage();
        }
        self.mem_tracker.consume(-total_released_memory);
    }
}

/// The borrows `spillImpl` needs, split out so `releaseMemory`'s borrow of the
/// workers does not overlap the merge's borrow of `lessRowFunc`.
struct SpillRound<'a> {
    sorted_rows_in_disk: &'a mut Vec<DataInDiskByChunks>,
    disk_tracker: &'a Arc<disk::Tracker>,
    storage: &'a Arc<SpillStorage>,
    field_types: &'a [FieldType],
    tmp_spill_chunk: &'a mut Option<Chunk>,
    finish: &'a AtomicBool,
    file_name_prefix_for_test: &'a str,
    bytes_consumed: i64,
    bytes_limit: i64,
}

impl SpillRound<'_> {
    /// Go `initForSpill`.
    fn init_for_spill(&mut self) {
        if self.tmp_spill_chunk.is_none() {
            *self.tmp_spill_chunk =
                Some(Chunk::new_with_capacity(self.field_types, SPILL_CHUNK_SIZE));
        }
    }

    /// Go `spillTmpSpillChunk`.
    fn spill_tmp_spill_chunk(&mut self, in_disk: &mut DataInDiskByChunks) -> Result<(), ExecError> {
        let chk = self
            .tmp_spill_chunk
            .as_mut()
            .expect("initForSpill runs before any flush");
        in_disk
            .add(chk)
            .map_err(|e| ExecError::SpillFailed(e.to_string()))?;
        chk.reset();
        Ok(())
    }

    /// Go `spillImpl`: drain the merger into one spill file.
    ///
    /// Returns whether the file was appended to `sortedRowsInDisk`, which is
    /// exactly when Go calls `releaseMemory`.
    fn spill_impl<S, F>(&mut self, merger: &mut MultiWayMerger<S, F>) -> Result<bool, ExecError>
    where
        S: crate::multi_way_merge::MultiWayMergeSource<Item = OwnedRow>,
        F: FnMut(&OwnedRow, &OwnedRow) -> Result<Ordering, ExecError>,
    {
        tracing::info!(
            consumed = self.bytes_consumed,
            quota = self.bytes_limit,
            "{}",
            SPILL_INFO
        );
        self.init_for_spill();
        self.tmp_spill_chunk
            .as_mut()
            .expect("initForSpill just ran")
            .reset();

        let new_in_disk = DataInDiskByChunks::new(
            self.field_types.to_vec(),
            self.file_name_prefix_for_test,
            Arc::clone(self.storage),
        );
        new_in_disk.disk_tracker().attach_to(self.disk_tracker);

        // Go declares ONE `inDisk` pointer (go:201) plus a separate
        // `isInDiskAppended` flag (go:204), and the deferred cleanup (go:216-221)
        // reads `inDisk.NumRows()` on THAT SAME object -- but only when
        // `!isInDiskAppended`, i.e. only when go:283 never handed it to
        // `sortedRowsInDisk`. There is no second, fresh buffer: after the append
        // the deferred `NumRows()` call is dead by Go's own guard, and the file
        // now owned by `sortedRowsInDisk` is never re-inspected here.
        //
        // So `Option` IS `isInDiskAppended`: `Some` means "still ours, still
        // closable", `None` means "handed off at go:283". Encoding the flag as
        // the ownership of the value keeps the cleanup unreachable-by-construction
        // once the run is published, instead of restructuring the loop to satisfy
        // the borrow checker.
        let mut in_disk = Some(new_in_disk);
        // Go's `err` variable: a flush failure leaves `OuterLoop` and is
        // returned after the producer is joined, so the partial file is closed
        // rather than published.
        let mut result: Result<bool, ExecError> = Ok(false);

        loop {
            // Go `case <-p.finishCh: break OuterLoop` -- the round is abandoned
            // and the partial file is discarded by the deferred close.
            if self.finish.load(SeqCst) {
                break;
            }
            let row = match merger.next() {
                Ok(row) => row,
                Err(err) => {
                    // Go: the producer goroutine pushes this onto errChannel
                    // and `spillImpl` returns it after `wg.Wait()`.
                    result = Err(err);
                    break;
                }
            };
            let target = in_disk
                .as_mut()
                .expect("the file is handed off only as the loop's last act");
            let Some(row) = row else {
                // Go's `if !ok` arm (go:272-288): the producer closed the
                // channel, so the merge is drained. Flush the tail and publish.
                let tail = self
                    .tmp_spill_chunk
                    .as_ref()
                    .expect("initForSpill just ran")
                    .num_rows();
                if tail > 0 {
                    if let Err(err) = self.spill_tmp_spill_chunk(target) {
                        result = Err(err);
                        break;
                    }
                }
                // go:282, read BEFORE go:283 hands the file away.
                if target.num_rows() > 0 {
                    self.sorted_rows_in_disk
                        .push(in_disk.take().expect("still owned"));
                    result = Ok(true);
                }
                break;
            };

            let chk = self
                .tmp_spill_chunk
                .as_mut()
                .expect("initForSpill just ran");
            chk.append_row(row.as_row());
            if chk.is_full() {
                if let Err(err) = self.spill_tmp_spill_chunk(target) {
                    // Go `stopProducer(); break` -- the producer is torn down
                    // and the partial file is discarded below.
                    result = Err(err);
                    break;
                }
            }
        }

        // Go's `defer` (go:216-221). `in_disk` is still `Some` exactly when
        // `isInDiskAppended` is false; an unpublished file that holds data is
        // removed. An unpublished EMPTY file is left alone, matching Go -- it
        // creates no backing file until the first `Add`.
        if let Some(mut in_disk) = in_disk {
            if in_disk.num_rows() > 0 {
                in_disk.close();
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc::channel;

    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_util::disk::{SpillEncryptionMethod, SpillStorage, SpillStorageSpec};
    use tidb_util::memory::Tracker;

    use super::*;
    use crate::sort_util::DataCursor;

    // Upstream's coverage for this file lives in
    // `parallel_sort_spill_test.go`, which drives a whole `SortExec` through
    // `testkit` plus failpoints. None of that is reachable at this tier, so
    // every test below is WRITTEN against the Go source rather than
    // transcreated.

    fn int_field_types(n: usize) -> Vec<FieldType> {
        (0..n)
            .map(|_| FieldType::new(FieldTypeCode::Long))
            .collect()
    }

    /// Column 0 is the sort key; column 1 is a tag the comparator ignores, so a
    /// test can tell apart two rows with equal keys.
    fn chunk_of(pairs: &[(i64, i64)]) -> Chunk {
        let fts = int_field_types(2);
        let cap = pairs.len().max(1);
        let mut chk = Chunk::new(&fts, cap, cap);
        for (key, tag) in pairs {
            chk.append_int64(0, *key);
            chk.append_int64(1, *tag);
        }
        chk
    }

    fn spill_dir(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("tidb_rust_parallel_sort_spill_{name}"))
    }

    /// Spill files left in `dir`. `SpillStorage` keeps its own `_dir.lock`
    /// there, which is not a spill file and is not counted.
    fn spill_file_count(dir: &std::path::Path) -> usize {
        std::fs::read_dir(dir)
            .expect("scratch dir")
            .filter_map(Result::ok)
            .filter(|e| !e.file_name().to_string_lossy().starts_with('_'))
            .count()
    }

    fn storage(name: &str) -> Arc<SpillStorage> {
        let dir = spill_dir(name);
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("scratch dir");
        Arc::new(
            SpillStorage::open(SpillStorageSpec {
                path: dir,
                quota_bytes: -1,
                encryption: SpillEncryptionMethod::Plaintext,
            })
            .expect("spill storage"),
        )
    }

    fn rows_of(values: &[i64]) -> Vec<OwnedRow> {
        rows_tagged(values, 0)
    }

    fn rows_tagged(values: &[i64], tag: i64) -> Vec<OwnedRow> {
        let pairs: Vec<(i64, i64)> = values.iter().map(|v| (*v, tag)).collect();
        let chk = chunk_of(&pairs);
        (0..pairs.len())
            .map(|i| chk.get_row(i).copy_construct())
            .collect()
    }

    fn less_int(a: &OwnedRow, b: &OwnedRow) -> Result<Ordering, ExecError> {
        Ok(a.as_row().get_int64(0).cmp(&b.as_row().get_int64(0)))
    }

    /// A worker holding one pre-sorted run.
    struct FakeWorker {
        rows: Vec<OwnedRow>,
        memory: i64,
        fail: bool,
    }

    impl FakeWorker {
        fn new(values: &[i64], memory: i64) -> Self {
            FakeWorker {
                rows: rows_of(values),
                memory,
                fail: false,
            }
        }

        fn failing() -> Self {
            FakeWorker {
                rows: Vec::new(),
                memory: 100,
                fail: true,
            }
        }

        /// One row whose key is 0 for every worker, tagged with `tag` so the
        /// merge's tie order is visible.
        fn tagged(tag: i64) -> Self {
            FakeWorker {
                rows: rows_tagged(&[0], tag),
                memory: 0,
                fail: false,
            }
        }
    }

    impl LocalSortWorker for FakeWorker {
        fn sort_local_rows(&mut self) -> Result<Vec<OwnedRow>, ExecError> {
            if self.fail {
                return Err(ExecError::SpillFailed("worker failed".to_owned()));
            }
            Ok(std::mem::take(&mut self.rows))
        }

        fn take_total_memory_usage(&mut self) -> i64 {
            std::mem::replace(&mut self.memory, 0)
        }
    }

    type Helper = ParallelSortSpillHelper<
        FakeWorker,
        fn(&OwnedRow, &OwnedRow) -> Result<Ordering, ExecError>,
    >;

    fn new_helper(
        name: &str,
        workers: Vec<FakeWorker>,
        mem_tracker: Arc<Tracker>,
    ) -> (Helper, Arc<AtomicBool>) {
        let finish = Arc::new(AtomicBool::new(false));
        let (tx, rx) = channel();
        // The receiver is intentionally leaked: this Go file never sends on
        // `errOutputChan`, and a dropped receiver would turn a future send into
        // a disconnect error.
        std::mem::forget(rx);
        let helper = ParallelSortSpillHelper::new(
            workers,
            mem_tracker,
            disk::new_tracker(1, -1),
            storage(name),
            int_field_types(2),
            Arc::clone(&finish),
            less_int as fn(&OwnedRow, &OwnedRow) -> Result<Ordering, ExecError>,
            tx,
            "",
        );
        (helper, finish)
    }

    fn read_back(in_disk: &mut DataInDiskByChunks) -> Vec<i64> {
        read_back_col(in_disk, 0)
    }

    fn read_back_col(in_disk: &mut DataInDiskByChunks, col: usize) -> Vec<i64> {
        let mut cursor = DataCursor::new();
        let mut out = Vec::new();
        for chk_idx in 0..in_disk.num_chunks() {
            let chk = in_disk.get_chunk(chk_idx).unwrap();
            cursor.set_chunk(chk, chk_idx as i64);
            let mut row = cursor.begin();
            while let Some(r) = row {
                out.push(r.get_int64(col));
                row = cursor.next();
            }
        }
        out
    }

    #[test]
    fn spill_merges_worker_runs_in_sorted_order() {
        let tracker = Tracker::new(1, -1);
        tracker.consume(600);
        let (mut helper, _finish) = new_helper(
            "merge",
            vec![
                FakeWorker::new(&[1, 4, 7], 100),
                FakeWorker::new(&[2, 5, 8], 200),
                FakeWorker::new(&[3, 6, 9], 300),
            ],
            Arc::clone(&tracker),
        );
        helper.spill().unwrap();

        assert!(helper.is_spill_triggered());
        assert!(helper.is_not_spilled(), "status returns to notSpilled");
        let mut runs = helper.take_sorted_rows_in_disk();
        assert_eq!(runs.len(), 1, "one spill round writes exactly one run");
        assert_eq!(read_back(&mut runs[0]), vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
        // `releaseMemory` gave back every worker's charge.
        assert_eq!(tracker.bytes_consumed(), 0);
        for run in &mut runs {
            run.close();
        }
    }

    #[test]
    fn spill_writes_more_rows_than_one_chunk_holds() {
        let tracker = Tracker::new(1, -1);
        let total = SPILL_CHUNK_SIZE * 2 + 7;
        let evens: Vec<i64> = (0..total as i64).map(|i| i * 2).collect();
        let odds: Vec<i64> = (0..total as i64).map(|i| i * 2 + 1).collect();
        let (mut helper, _finish) = new_helper(
            "many_chunks",
            vec![FakeWorker::new(&evens, 0), FakeWorker::new(&odds, 0)],
            Arc::clone(&tracker),
        );
        helper.spill().unwrap();
        let mut runs = helper.take_sorted_rows_in_disk();
        let got = read_back(&mut runs[0]);
        assert_eq!(got.len(), total * 2);
        let want: Vec<i64> = (0..(total * 2) as i64).collect();
        assert_eq!(got, want, "tail chunk is flushed too");
        // A published run is one real file on disk; `close` takes it away
        // again. This also proves `spill_file_count` can see a spill file, so
        // the zero it reports in the failed-merge test means something.
        let dir = spill_dir("many_chunks");
        assert_eq!(spill_file_count(&dir), 1);
        for run in &mut runs {
            run.close();
        }
        assert_eq!(spill_file_count(&dir), 0);
    }

    #[test]
    fn empty_workers_produce_no_run() {
        let tracker = Tracker::new(1, -1);
        tracker.consume(50);
        let (mut helper, _finish) = new_helper(
            "empty",
            vec![FakeWorker::new(&[], 50)],
            Arc::clone(&tracker),
        );
        helper.spill().unwrap();
        assert!(!helper.is_spill_triggered());
        // Nothing reached disk, so Go does not call `releaseMemory` either.
        assert_eq!(tracker.bytes_consumed(), 50);
    }

    #[test]
    fn finished_query_spills_nothing() {
        let tracker = Tracker::new(1, -1);
        let (mut helper, finish) = new_helper(
            "finished",
            vec![FakeWorker::new(&[1, 2, 3], 10)],
            Arc::clone(&tracker),
        );
        finish.store(true, SeqCst);
        helper.spill().unwrap();
        assert!(!helper.is_spill_triggered());
        assert!(helper.is_not_spilled());
    }

    #[test]
    fn worker_failure_aborts_the_round() {
        let tracker = Tracker::new(1, -1);
        let (mut helper, _finish) = new_helper(
            "worker_fail",
            vec![FakeWorker::new(&[1, 2], 10), FakeWorker::failing()],
            Arc::clone(&tracker),
        );
        let err = helper.spill().unwrap_err();
        assert!(matches!(err, ExecError::SpillFailed(_)), "{err:?}");
        // The status ladder is restored even on the failing path, because Go
        // restores it from a `defer`.
        assert!(helper.is_not_spilled());
        assert!(!helper.is_spill_triggered());
    }

    #[test]
    fn two_rounds_accumulate_two_runs() {
        let tracker = Tracker::new(1, -1);
        let (mut helper, _finish) = new_helper(
            "two_rounds",
            vec![FakeWorker::new(&[5, 6], 0)],
            Arc::clone(&tracker),
        );
        helper.spill().unwrap();
        // A second round: the worker is refilled, as `parallelSortWorker` is
        // after `sortLocalRows` cleared it.
        helper.workers[0].rows = rows_of(&[1, 2]);
        helper.spill().unwrap();
        let mut runs = helper.take_sorted_rows_in_disk();
        assert_eq!(runs.len(), 2);
        assert_eq!(read_back(&mut runs[0]), vec![5, 6]);
        assert_eq!(read_back(&mut runs[1]), vec![1, 2]);
        for run in &mut runs {
            run.close();
        }
    }

    #[test]
    fn status_ladder_round_trips() {
        let tracker = Tracker::new(1, -1);
        let (mut helper, _finish) = new_helper("ladder", vec![FakeWorker::new(&[1], 0)], tracker);
        assert!(helper.is_not_spilled());
        assert!(!helper.is_spill_needed());
        helper.set_need_spill();
        assert!(helper.is_spill_needed());
        assert!(!helper.is_not_spilled());
        helper.set_in_spilling();
        assert!(helper.is_in_spilling());
        helper.set_not_spilled();
        assert!(helper.is_not_spilled());
        // `isSpillTriggered` reads the disk runs, not the ladder.
        assert!(!helper.is_spill_triggered());
        helper.spill().unwrap();
        assert!(helper.is_spill_triggered());
        assert!(helper.is_not_spilled());
        helper.close();
    }

    #[test]
    fn wait_for_spill_finish_returns_once_spilling_clears() {
        let tracker = Tracker::new(1, -1);
        let (helper, _finish) = new_helper("wait", vec![FakeWorker::new(&[1], 0)], tracker);
        // Not spilling: the wait is a no-op, which is Go's `for
        // getIsSpillingNoLock()` loop taken zero times.
        helper.wait_for_spill_finish();
        assert!(helper.is_not_spilled());
    }

    #[test]
    fn close_removes_every_spilled_run() {
        let tracker = Tracker::new(1, -1);
        let (mut helper, _finish) =
            new_helper("close", vec![FakeWorker::new(&[1, 2, 3], 0)], tracker);
        helper.spill().unwrap();
        let path = helper.sorted_rows_in_disk()[0].file_path().cloned();
        assert!(path.as_ref().is_some_and(|p| p.exists()));
        helper.close();
        assert!(path.is_some_and(|p| !p.exists()));
    }

    /// WRITTEN. Go's `spillImpl` defer (go:216-221) closes the half-written
    /// file when the round aborts before `sortedRowsInDisk` is appended
    /// (go:283). This is the branch the port's `Option<DataInDiskByChunks>`
    /// encodes, so it is pinned: a comparator that fails midway must leave NO
    /// run published AND no file behind, even though rows already reached disk.
    #[test]
    fn a_failed_merge_publishes_no_run_and_removes_the_partial_file() {
        let dir = spill_dir("merge_fail");
        let tracker = Tracker::new(1, -1);
        tracker.consume(700);
        // Enough rows that `tmpSpillChunk` fills and flushes to disk at least
        // once before the comparator starts failing.
        let poison = (SPILL_CHUNK_SIZE * 2 + 16) as i64;
        let evens: Vec<i64> = (0..poison).map(|i| i * 2).collect();
        let odds: Vec<i64> = (0..poison).map(|i| i * 2 + 1).collect();

        let finish = Arc::new(AtomicBool::new(false));
        let (tx, rx) = channel();
        std::mem::forget(rx);
        let mut helper = ParallelSortSpillHelper::new(
            vec![FakeWorker::new(&evens, 300), FakeWorker::new(&odds, 400)],
            Arc::clone(&tracker),
            disk::new_tracker(1, -1),
            storage("merge_fail"),
            int_field_types(2),
            finish,
            move |a: &OwnedRow, b: &OwnedRow| {
                let (x, y) = (a.as_row().get_int64(0), b.as_row().get_int64(0));
                if x >= poison || y >= poison {
                    return Err(ExecError::SpillFailed("comparator failed".to_owned()));
                }
                Ok(x.cmp(&y))
            },
            tx,
            "",
        );

        let err = helper.spill().unwrap_err();
        assert!(matches!(err, ExecError::SpillFailed(_)), "{err:?}");
        assert!(
            !helper.is_spill_triggered(),
            "a partial run must never be published"
        );
        assert_eq!(
            spill_file_count(&dir),
            0,
            "the half-written file is removed, as Go's defer does"
        );
        // `releaseMemory` runs only after the run reaches disk (go:282-286), so
        // the workers stay charged.
        assert_eq!(tracker.bytes_consumed(), 700);
        assert!(helper.is_not_spilled(), "the status ladder still resets");
    }

    /// WRITTEN. Ties between runs are broken by `container/heap`'s sift order,
    /// NOT by worker index -- see the module's "Tie-breaking" note. Three
    /// single-row workers with an identical key expose the exact order, so a
    /// change in [`MultiWayMerger`]'s heap would be caught here rather than
    /// silently reordering spilled rows.
    #[test]
    fn equal_keys_break_ties_the_way_gos_heap_does() {
        let tracker = Tracker::new(1, -1);
        let (mut helper, _finish) = new_helper(
            "ties",
            vec![
                FakeWorker::tagged(0),
                FakeWorker::tagged(1),
                FakeWorker::tagged(2),
            ],
            tracker,
        );
        helper.spill().unwrap();
        let mut runs = helper.take_sorted_rows_in_disk();
        // Every key is 0, so the values below are purely the heap's tie order:
        // `heap.Remove(0)` swaps the root with the LAST element before popping,
        // which is why worker 2 precedes worker 1.
        assert_eq!(read_back_col(&mut runs[0], 1), vec![0, 2, 1]);
        for run in &mut runs {
            run.close();
        }
    }

    #[test]
    fn bytes_info_is_recorded_for_the_spill_log() {
        let tracker = Tracker::new(1, -1);
        let (helper, _finish) = new_helper("bytes", vec![FakeWorker::new(&[1], 0)], tracker);
        helper.set_bytes_info(1024, 512);
        assert_eq!(helper.bytes_consumed.load(SeqCst), 1024);
        assert_eq!(helper.bytes_limit.load(SeqCst), 512);
    }
}
