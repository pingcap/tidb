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

//! `pkg/executor/aggregate/agg_spill.go`'s `AggSpillDiskAction`: the
//! `ActionOnExceed` the SERIAL hash aggregation registers.
//!
//! # Which of Go's two spill mechanisms this is
//!
//! `agg_spill.go` holds two, and they are not variants of one design:
//!
//! * `parallelHashAggSpillHelper` + `ParallelAggSpillDiskAction` partition the
//!   PARTIAL RESULTS across 256 buckets, write each bucket's serialized
//!   partial results out, and restore them later by MERGING partial results
//!   (`restoreFromOneSpillFile` -> `MergePartialResult`). It needs the merge
//!   because several partial workers can each hold a partial result for the
//!   same group key. DISTINCT states serialize their retained value inputs and
//!   replay only newly admitted values during that merge.
//! * `AggSpillDiskAction` (this one) belongs to `unparallelExec`. It spills
//!   NOTHING of the aggregation state. It sets one flag, `inSpillMode`, and
//!   the serial loop then writes the UNPROCESSED INPUT ROWS of groups it has
//!   not opened yet back to disk (`spillUnprocessedData`), finishes and emits
//!   the groups it does hold, and re-reads the spilled rows as a fresh ROUND
//!   (`resetSpillMode` + `getNextChunk`). No partial result is ever
//!   serialized, and no two partial results for one key ever have to be
//!   merged: a group is opened in exactly one round and completed there.
//!
//! [`crate::hash_agg::HashAggExec`] implements both arms. Serial execution
//! registers [`AggSpillDiskAction`]; parallel execution registers
//! [`ParallelAggSpillDiskAction`] under Go's aggregate-memory, temporary
//! storage, spill-sysvar and aggregate-memory gates. DISTINCT is supported
//! here as well; its value-set payload travels with the serialized state.
//!
//! # The soft limit, not the hard one
//!
//! Go registers this action with `FallbackOldAndSetNewActionForSoftLimit`
//! (`initForUnparallelExec`), so it fires at 80% of `tidb_mem_quota_query`
//! (`memory.SOFT_SCALE`), NOT at the quota itself. The cancellation still sits
//! on the hard-limit slot and still fires at 100% if spilling did not bring
//! the aggregation back under the quota. That is why an over-quota `GROUP BY`
//! can spill and finish while the 8175 path remains reachable.
//!
//! The shared `tidb-chunk` spill container now implements Go's optional
//! `aes128-ctr` file stack. The bounded server still has no top-level config
//! loader to call that container's process-wide config seam, so its startup
//! remains plaintext and rejects unsupported command-line options loudly.

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering::SeqCst};
use std::sync::Arc;

use tidb_memory::{ActionOnExceed, ArcAction, BaseOomAction, Tracker, DEF_SPILL_PRIORITY};

/// Go `maxSpillTimes`: how many times one aggregation may enter spill mode.
/// Past this the action stops spilling and falls through to the cancellation,
/// so a query that keeps overrunning fails rather than writing forever.
pub const MAX_SPILL_TIMES: u32 = 10;

/// Go `hasEnoughDataToSpill`: spill only once the aggregation itself holds at
/// least a FIFTH of the quota, so a query whose overrun comes from some other
/// operator does not make the aggregation open a spill file per chunk.
pub fn has_enough_data_to_spill(agg_tracker: &Tracker, triggered: &Tracker) -> bool {
    agg_tracker.bytes_consumed() >= triggered.get_bytes_limit() / 5
}

/// Go `AggSpillDiskAction`: raises the aggregation's `inSpillMode` flag.
pub struct AggSpillDiskAction {
    base: BaseOomAction,
    /// Go `HashAggExec.inSpillMode`, read by the serial loop at the next row.
    in_spill_mode: Arc<AtomicBool>,
    /// Go `AggSpillDiskAction.spillTimes`.
    spill_times: AtomicU32,
    /// Go `a.e.memTracker`: the aggregation's own tracker.
    agg_tracker: Arc<Tracker>,
}

/// Go `ParallelAggSpillDiskAction`: asks the partial workers to serialize
/// their current result maps. Unlike [`AggSpillDiskAction`], this never
/// changes group admission; the parallel executor drains the in-flight
/// chunks, partitions the partial states, and then resumes with empty maps.
pub struct ParallelAggSpillDiskAction {
    base: BaseOomAction,
    /// Go `spillHelper`, for the Go-shaped executor.
    spill_helper: Option<Arc<ParallelHashAggSpillHelper>>,
    need_spill: Arc<AtomicBool>,
    spill_times: AtomicU32,
    agg_tracker: Arc<Tracker>,
}

impl ParallelAggSpillDiskAction {
    /// Builds the action and the flag polled by the pipeline fetcher.
    #[must_use]
    pub fn new(agg_tracker: &Arc<Tracker>) -> (Arc<ParallelAggSpillDiskAction>, Arc<AtomicBool>) {
        let need_spill = Arc::new(AtomicBool::new(false));
        let action = Arc::new(ParallelAggSpillDiskAction {
            base: BaseOomAction::default(),
            spill_helper: None,
            need_spill: Arc::clone(&need_spill),
            spill_times: AtomicU32::new(0),
            agg_tracker: Arc::clone(agg_tracker),
        });
        (action, need_spill)
    }

    /// Number of spill requests raised for diagnostics. Go applies
    /// `maxSpillTimes` only to the serial action, not this parallel action.
    #[must_use]
    pub fn spill_times(&self) -> u32 {
        self.spill_times.load(SeqCst)
    }
}

impl ParallelAggSpillDiskAction {
    /// Go `actionSpillForParallel`: the action over `spill_helper`.
    pub(crate) fn new_for_helper(agg_tracker: &Arc<Tracker>, spill_helper: Arc<ParallelHashAggSpillHelper>) -> Arc<ParallelAggSpillDiskAction> {
        Arc::new(ParallelAggSpillDiskAction {
            base: BaseOomAction::default(),
            spill_helper: Some(spill_helper),
            need_spill: Arc::new(AtomicBool::new(false)),
            spill_times: AtomicU32::new(0),
            agg_tracker: Arc::clone(agg_tracker),
        })
    }
}

impl ActionOnExceed for ParallelAggSpillDiskAction {
    fn action(&self, triggered: &Arc<Tracker>) {
        if let Some(spill_helper) = &self.spill_helper {
            // Go `actionImpl`.
            spill_helper.wait_for_the_end_of_spill();
            if spill_helper.set_need_spill(&self.agg_tracker, triggered) {
                return;
            }
            if let Some(fallback) = self.get_fallback() {
                fallback.action(triggered);
            }
            return;
        }
        if self.need_spill.load(SeqCst) {
            // Go waits for the active spill to finish before trying to set a
            // new request. The Rust pipeline drains synchronously at the next
            // epoch boundary, so an already-pending request is successful and
            // must not fall through to cancellation.
            return;
        }
        if has_enough_data_to_spill(&self.agg_tracker, triggered) {
            self.spill_times.fetch_add(1, SeqCst);
            self.need_spill.store(true, SeqCst);
            tracing::info!(
                spill_times = self.spill_times.load(SeqCst),
                consumed = triggered.bytes_consumed(),
                quota = triggered.get_bytes_limit(),
                "memory exceeds quota, set parallel aggregate mode to spill-mode"
            );
            return;
        }
        if let Some(fallback) = self.get_fallback() {
            fallback.action(triggered);
        }
    }

    fn set_fallback(&self, action: Option<ArcAction>) {
        self.base.set_fallback(action);
    }

    fn get_fallback(&self) -> Option<ArcAction> {
        self.base.get_fallback()
    }

    fn get_priority(&self) -> i64 {
        DEF_SPILL_PRIORITY
    }

    fn set_finished(&self) {
        self.base.set_finished();
    }

    fn is_finished(&self) -> bool {
        self.base.is_finished()
    }
}

impl AggSpillDiskAction {
    /// Builds the action and the flag the aggregation's loop polls.
    #[must_use]
    pub fn new(agg_tracker: &Arc<Tracker>) -> (Arc<AggSpillDiskAction>, Arc<AtomicBool>) {
        let in_spill_mode = Arc::new(AtomicBool::new(false));
        let action = Arc::new(AggSpillDiskAction {
            base: BaseOomAction::default(),
            in_spill_mode: Arc::clone(&in_spill_mode),
            spill_times: AtomicU32::new(0),
            agg_tracker: Arc::clone(agg_tracker),
        });
        (action, in_spill_mode)
    }

    /// How many times this action has entered spill mode (Go `spillTimes`).
    #[must_use]
    pub fn spill_times(&self) -> u32 {
        self.spill_times.load(SeqCst)
    }
}

impl ActionOnExceed for AggSpillDiskAction {
    /// Go `AggSpillDiskAction.Action`, condition for condition: not already in
    /// spill mode, enough data of our own to be worth a file, and under
    /// `maxSpillTimes`. Otherwise the fallback runs -- which for a `CANCEL`
    /// session ends in the 8175 cancellation.
    fn action(&self, t: &Arc<Tracker>) {
        if !self.in_spill_mode.load(SeqCst)
            && has_enough_data_to_spill(&self.agg_tracker, t)
            && self.spill_times.load(SeqCst) < MAX_SPILL_TIMES
        {
            self.spill_times.fetch_add(1, SeqCst);
            tracing::info!(
                spill_times = self.spill_times.load(SeqCst),
                consumed = t.bytes_consumed(),
                quota = t.get_bytes_limit(),
                "memory exceeds quota, set aggregate mode to spill-mode"
            );
            self.in_spill_mode.store(true, SeqCst);
            return;
        }
        if let Some(fallback) = self.get_fallback() {
            fallback.action(t);
        }
    }

    fn set_fallback(&self, a: Option<ArcAction>) {
        self.base.set_fallback(a);
    }

    fn get_fallback(&self) -> Option<ArcAction> {
        self.base.get_fallback()
    }

    /// Go `AggSpillDiskAction.GetPriority`.
    fn get_priority(&self) -> i64 {
        DEF_SPILL_PRIORITY
    }

    fn set_finished(&self) {
        self.base.set_finished();
    }

    fn is_finished(&self) -> bool {
        self.base.is_finished()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_memory::LABEL_FOR_SESSION;

    /// Go `hasEnoughDataToSpill` is a FIFTH of the limit, and it reads the
    /// aggregation's tracker while the limit comes from the tracker that
    /// overran -- a query can overrun on another operator's bytes.
    #[test]
    fn a_fifth_of_the_quota_is_the_bar() {
        let root = Tracker::new(LABEL_FOR_SESSION, 1000);
        let agg = Tracker::new(1, -1);
        agg.attach_to(&root);
        agg.consume(199);
        assert!(!has_enough_data_to_spill(&agg, &root));
        agg.consume(1);
        assert!(has_enough_data_to_spill(&agg, &root));
    }

    #[test]
    fn the_action_raises_spill_mode_only_once_and_at_most_ten_times() {
        let root = Tracker::new(LABEL_FOR_SESSION, 1000);
        let agg = Tracker::new(1, -1);
        agg.attach_to(&root);
        agg.consume(1000);
        let (action, flag) = AggSpillDiskAction::new(&agg);

        action.action(&root);
        assert!(flag.load(SeqCst));
        assert_eq!(action.spill_times(), 1);
        // Already in spill mode: the action does nothing until the round ends
        // and the aggregation lowers the flag.
        action.action(&root);
        assert_eq!(action.spill_times(), 1);

        for _ in 0..20 {
            flag.store(false, SeqCst);
            action.action(&root);
        }
        assert_eq!(action.spill_times(), MAX_SPILL_TIMES);
        assert!(
            !flag.load(SeqCst),
            "past maxSpillTimes the action must stop spilling"
        );
    }

    /// Too little data of our own: the action must hand over to the fallback
    /// (the cancellation), not swallow the overrun.
    #[test]
    fn too_little_data_falls_through_to_the_fallback() {
        struct Counting {
            base: BaseOomAction,
            hits: AtomicU32,
        }
        impl ActionOnExceed for Counting {
            fn action(&self, _t: &Arc<Tracker>) {
                self.hits.fetch_add(1, SeqCst);
            }
            fn set_fallback(&self, a: Option<ArcAction>) {
                self.base.set_fallback(a);
            }
            fn get_fallback(&self) -> Option<ArcAction> {
                self.base.get_fallback()
            }
            fn get_priority(&self) -> i64 {
                DEF_SPILL_PRIORITY
            }
            fn set_finished(&self) {
                self.base.set_finished();
            }
            fn is_finished(&self) -> bool {
                self.base.is_finished()
            }
        }

        let root = Tracker::new(LABEL_FOR_SESSION, 1000);
        let agg = Tracker::new(1, -1);
        agg.attach_to(&root);
        agg.consume(10);
        let (action, flag) = AggSpillDiskAction::new(&agg);
        let counting = Arc::new(Counting {
            base: BaseOomAction::default(),
            hits: AtomicU32::new(0),
        });
        action.set_fallback(Some(counting.clone() as ArcAction));
        action.action(&root);
        assert!(!flag.load(SeqCst), "10 bytes is not worth a spill file");
        assert_eq!(counting.hits.load(SeqCst), 1);
    }
}

use tidb_chunk::chunk::Chunk;
use tidb_driver_error::ExecError;
use tidb_goruntime::GoString;

/// Go `spillStatus`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SpillStatus {
    NoSpill,
    NeedSpill,
    InSpilling,
    SpillTriggered,
}

/// Go `spilledPartitionNum`.
pub(crate) const SPILLED_PARTITION_NUM: usize = 256;

/// Go `parallelHashAggSpillHelper.lock`'s fields.
struct SpillHelperLock {
    next_partition_idx: isize,
    spilled_chunks_io: Vec<Vec<tidb_chunk::chunk_in_disk::DataInDiskByChunks>>,
    status: SpillStatus,
    memory_consumption: i64,
    memory_quota: i64,
}

/// Go `parallelHashAggSpillHelper`: the parallel HashAgg's spill state the
/// action, the fetcher and the workers share.
pub(crate) struct ParallelHashAggSpillHelper {
    lock: std::sync::Mutex<SpillHelperLock>,
    wait_if_in_spilling: std::sync::Condvar,
    pub(crate) mem_tracker: Arc<Tracker>,
    pub(crate) disk_tracker: std::sync::Mutex<Option<Arc<tidb_disk::Tracker>>>,
    has_error: AtomicBool,
    /// The partial functions, used only to restore what partial workers
    /// spilled.
    agg_funcs_for_restoring: Arc<Vec<Box<dyn tidb_executor_aggfuncs::agg_func::AggFunc>>>,
    final_worker_agg_funcs: Arc<Vec<Box<dyn tidb_executor_aggfuncs::agg_func::AggFunc>>>,
    pub(crate) spill_chunk_field_types: Vec<tidb_datatype::FieldType>,
    init_cap: usize,
    max_chunk_size: usize,
    pub(crate) storage: Arc<tidb_spill_storage::SpillStorage>,
}

impl ParallelHashAggSpillHelper {
    /// Go `newSpillHelper`.
    pub(crate) fn new(
        tracker: &Arc<Tracker>,
        agg_funcs_for_restoring: Arc<Vec<Box<dyn tidb_executor_aggfuncs::agg_func::AggFunc>>>,
        final_worker_agg_funcs: Arc<Vec<Box<dyn tidb_executor_aggfuncs::agg_func::AggFunc>>>,
        spill_chunk_field_types: Vec<tidb_datatype::FieldType>,
        init_cap: usize,
        max_chunk_size: usize,
        storage: Arc<tidb_spill_storage::SpillStorage>,
    ) -> Result<Self, ExecError> {
        if agg_funcs_for_restoring.len() != final_worker_agg_funcs.len() {
            return Err(ExecError::internal("len(aggFuncsForRestoring) != len(finalWorkerAggFuncs)"));
        }
        Ok(Self {
            lock: std::sync::Mutex::new(SpillHelperLock {
                next_partition_idx: SPILLED_PARTITION_NUM as isize - 1,
                spilled_chunks_io: (0..SPILLED_PARTITION_NUM).map(|_| Vec::new()).collect(),
                status: SpillStatus::NoSpill,
                memory_consumption: 0,
                memory_quota: 0,
            }),
            wait_if_in_spilling: std::sync::Condvar::new(),
            mem_tracker: Arc::clone(tracker),
            disk_tracker: std::sync::Mutex::new(None),
            has_error: AtomicBool::new(false),
            agg_funcs_for_restoring,
            final_worker_agg_funcs,
            spill_chunk_field_types,
            init_cap,
            max_chunk_size,
            storage,
        })
    }

    fn locked(&self) -> std::sync::MutexGuard<'_, SpillHelperLock> {
        self.lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Go `getNewSpillChunkFunc`.
    pub(crate) fn get_new_spill_chunk(&self) -> Chunk {
        Chunk::new(&self.spill_chunk_field_types, self.init_cap, self.max_chunk_size)
    }

    /// Go `close`.
    pub(crate) fn close(&self) {
        for ios in &mut self.locked().spilled_chunks_io {
            for io in ios {
                io.close();
            }
        }
    }

    /// Go `isSpilledChunksIOEmpty`.
    pub(crate) fn is_spilled_chunks_io_empty(&self) -> bool {
        self.locked().spilled_chunks_io.iter().all(Vec::is_empty)
    }

    /// Go `getNextPartition`.
    fn get_next_partition(&self) -> Option<usize> {
        let mut lock = self.locked();
        let partition_idx = lock.next_partition_idx;
        if partition_idx < 0 {
            return None;
        }
        lock.next_partition_idx -= 1;
        Some(partition_idx as usize)
    }

    /// Go `addListInDisks`.
    pub(crate) fn add_list_in_disks(&self, data_in_disk: Vec<tidb_chunk::chunk_in_disk::DataInDiskByChunks>) {
        let mut lock = self.locked();
        for (i, data) in data_in_disk.into_iter().enumerate() {
            lock.spilled_chunks_io[i].push(data);
        }
    }

    /// Go `setInSpilling`.
    pub(crate) fn set_in_spilling(&self) {
        let mut lock = self.locked();
        lock.status = SpillStatus::InSpilling;
        tracing::info!(consumed = lock.memory_consumption, quota = lock.memory_quota, "memory exceeds quota, set aggregate mode to spill-mode");
    }

    /// Go `isNoSpill`.
    pub(crate) fn is_no_spill(&self) -> bool {
        self.locked().status == SpillStatus::NoSpill
    }

    /// Go `setSpillTriggered`.
    pub(crate) fn set_spill_triggered(&self) {
        self.locked().status = SpillStatus::SpillTriggered;
        self.wait_if_in_spilling.notify_all();
    }

    /// Go `checkNeedSpill`.
    pub(crate) fn check_need_spill(&self) -> bool {
        self.locked().status == SpillStatus::NeedSpill
    }

    /// Go `setNeedSpill`: whether the flag was set.
    pub(crate) fn set_need_spill(&self, executor_tracker: &Tracker, triggered_tracker: &Tracker) -> bool {
        let mut lock = self.locked();
        if has_enough_data_to_spill(executor_tracker, triggered_tracker) {
            lock.status = SpillStatus::NeedSpill;
            lock.memory_consumption = triggered_tracker.bytes_consumed();
            lock.memory_quota = triggered_tracker.get_bytes_limit();
            return true;
        }
        false
    }

    /// Go `waitForTheEndOfSpill`.
    pub(crate) fn wait_for_the_end_of_spill(&self) {
        let mut lock = self.locked();
        while lock.status == SpillStatus::InSpilling {
            lock = self.wait_if_in_spilling.wait(lock).unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }

    /// Go `checkError`.
    pub(crate) fn check_error(&self) -> bool {
        self.has_error.load(SeqCst)
    }

    /// Go `setError`.
    pub(crate) fn set_error(&self) {
        self.has_error.store(true, SeqCst);
    }

    /// Go `restoreOnePartition`: the next partition's results, merged, or
    /// `None` once every partition is restored.
    pub(crate) fn restore_one_partition(&self, ctx: &tidb_executor_aggfuncs::agg_func::AggFuncUpdateContext) -> Result<(Option<tidb_executor_aggfuncs::agg_func::AggPartialResultMapper>, i64), ExecError> {
        let mut restored_data = tidb_executor_aggfuncs::agg_func::new_agg_partial_result_mapper();
        let mut restored_mem = 0;
        let Some(restored_partition_idx) = self.get_next_partition() else {
            return Ok((None, restored_mem));
        };
        let mut spilled_files_io = std::mem::take(&mut self.locked().spilled_chunks_io[restored_partition_idx]);
        let result = (|| {
            for spilled_file in &mut spilled_files_io {
                let (mem_delta, expand_mem) = self.restore_from_one_spill_file(ctx, &mut restored_data, spilled_file)?;
                self.mem_tracker.consume(mem_delta);
                restored_mem += mem_delta + expand_mem;
            }
            Ok::<_, ExecError>(())
        })();
        // The files stay the helper's until `close` removes them.
        self.locked().spilled_chunks_io[restored_partition_idx] = spilled_files_io;
        result?;
        Ok((Some(restored_data), restored_mem))
    }

    /// Go `restoreFromOneSpillFile`.
    fn restore_from_one_spill_file(
        &self,
        ctx: &tidb_executor_aggfuncs::agg_func::AggFuncUpdateContext,
        restored_data: &mut tidb_executor_aggfuncs::agg_func::AggPartialResultMapper,
        disk_io: &mut tidb_chunk::chunk_in_disk::DataInDiskByChunks,
    ) -> Result<(i64, i64), ExecError> {
        let mut total_mem_delta = 0;
        let total_expand_mem = 0;
        let agg_func_num = self.agg_funcs_for_restoring.len();
        for i in 0..disk_io.num_chunks() {
            let chunk = disk_io.get_chunk(i).map_err(|error| ExecError::internal(error.to_string()))?;
            // Deserialize each function's partial results.
            let mut partial_results_restored = Vec::with_capacity(agg_func_num);
            for agg_func in self.agg_funcs_for_restoring.iter() {
                let (partial_result, mem_delta) = agg_func.deserialize_partial_result(&chunk);
                partial_results_restored.push(partial_result.into_iter().map(Some).collect::<Vec<_>>());
                total_mem_delta += mem_delta;
            }
            // Merge or create the results.
            for row_pos in 0..chunk.num_rows() {
                total_mem_delta += self.process_row(ctx, &chunk, row_pos, agg_func_num, restored_data, &mut partial_results_restored)?;
            }
        }
        Ok((total_mem_delta, total_expand_mem))
    }

    /// Go `processRow`.
    fn process_row(
        &self,
        ctx: &tidb_executor_aggfuncs::agg_func::AggFuncUpdateContext,
        chunk: &Chunk,
        row_pos: usize,
        agg_func_num: usize,
        restored_data: &mut tidb_executor_aggfuncs::agg_func::AggPartialResultMapper,
        partial_results_restored: &mut [Vec<Option<tidb_executor_aggfuncs::agg_func::PartialResult>>],
    ) -> Result<i64, ExecError> {
        let key = GoString::from(chunk.get_row(row_pos).get_bytes(agg_func_num).to_vec());
        let mut total_mem_delta = 0;
        if let Some(prs) = restored_data.get_mut(&key) {
            // The key has appeared before: merge.
            for agg_pos in 0..agg_func_num {
                let mut src = partial_results_restored[agg_pos][row_pos].take().expect("each restored result is merged once");
                total_mem_delta += self.final_worker_agg_funcs[agg_pos].merge_partial_result(ctx, &mut src, &mut prs[agg_pos])?;
            }
        } else {
            total_mem_delta += key.len() as i64;
            let results = (0..agg_func_num)
                .map(|agg_pos| partial_results_restored[agg_pos][row_pos].take().expect("each restored result is used once"))
                .collect();
            let delta = restored_data.set(key, results);
            if delta > 0 {
                self.mem_tracker.consume(delta);
            }
        }
        Ok(total_mem_delta)
    }
}
