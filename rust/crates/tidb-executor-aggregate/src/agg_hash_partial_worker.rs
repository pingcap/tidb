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

//! Go `pkg/executor/aggregate/agg_hash_partial_worker.go`: a partial worker
//! folds its input chunks into one `AggPartialResultMapper` per final
//! worker, keyed by `murmur3(groupKey) % finalConcurrency`, and at the end
//! shuffles mapper `i` to final worker `i` -- or, once the aggregation has
//! spilled, writes its mappers to 256 partitions on disk.

use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;

use crossbeam_channel::{select, Receiver, Sender};
use tidb_chunk::chunk::Chunk;
use tidb_chunk::chunk_in_disk::DataInDiskByChunks;
use tidb_driver_error::ExecError;
use tidb_executor_aggfuncs::agg_func::{new_agg_partial_result_mapper, AggFunc, AggPartialResultMapper, PartialResult};
use tidb_executor_aggfuncs::spill_serialize_helper::SerializeHelper;
use tidb_expr::expression::Expression;
use tidb_exprctx::Columns;
use tidb_goruntime::GoString;
use tidb_memory::Tracker;

use crate::agg_hash_base_worker::BaseHashAggWorker;
use crate::agg_spill::{ParallelHashAggSpillHelper, SPILLED_PARTITION_NUM};
use crate::agg_util::{get_group_key, get_group_key_mem_usage, recover_hash_agg, AfFinalResult, GroupKeys, WaitGroup};

/// Go `HashAggInput`: a chunk for the data fetcher to fill, and the partial
/// worker it is bound to.
pub(crate) struct HashAggInput {
    pub(crate) chk: Chunk,
    pub(crate) give_back_ch: Sender<Option<Chunk>>,
}

/// The partial worker's state the data fetcher spills while the worker
/// waits (Go's fetcher reaches into `HashAggPartialWorker` directly once
/// `inflightChunkSync` is drained).
pub(crate) struct PartialWorkerSpillState {
    /// Go `partialResultsMap`: one mapper per final worker.
    pub(crate) partial_results_map: Vec<AggPartialResultMapper>,
    /// Go `partialResultsMapMem`.
    pub(crate) partial_results_map_mem: i64,
    is_spill_prepared: bool,
    tmp_chks_for_spill: Vec<Chunk>,
    serialize_helpers: SerializeHelper,
    /// Go `spilledChunksIO`.
    pub(crate) spilled_chunks_io: Vec<DataInDiskByChunks>,
}

impl PartialWorkerSpillState {
    pub(crate) fn new(partial_results_map: Vec<AggPartialResultMapper>) -> Self {
        Self {
            partial_results_map,
            partial_results_map_mem: 0,
            is_spill_prepared: false,
            tmp_chks_for_spill: Vec::new(),
            serialize_helpers: SerializeHelper::new(),
            spilled_chunks_io: Vec::new(),
        }
    }

    /// Go `prepareForSpill`.
    fn prepare_for_spill(&mut self, spill_helper: &ParallelHashAggSpillHelper) {
        if self.is_spill_prepared {
            return;
        }
        let disk_tracker = spill_helper.disk_tracker.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone();
        for _ in 0..SPILLED_PARTITION_NUM {
            self.tmp_chks_for_spill.push(spill_helper.get_new_spill_chunk());
            let io = DataInDiskByChunks::new(spill_helper.spill_chunk_field_types.clone(), "", Arc::clone(&spill_helper.storage));
            if let Some(disk_tracker) = &disk_tracker {
                io.disk_tracker().attach_to(disk_tracker);
            }
            self.spilled_chunks_io.push(io);
        }
        self.is_spill_prepared = true;
    }

    /// Go `spillDataToDisk` / `spillDataToDiskImpl`: every group to its
    /// partition (`murmur3(key) % 256`), the mappers then cleared.
    pub(crate) fn spill_data_to_disk(&mut self, agg_funcs: &[Box<dyn AggFunc>], spill_helper: &ParallelHashAggSpillHelper, mem_tracker: &Arc<Tracker>) -> Result<(), ExecError> {
        if self.partial_results_map.is_empty() {
            return Ok(());
        }
        let n = self.partial_results_map.len();
        let result = self.spill_data_to_disk_impl(agg_funcs, spill_helper);
        // Clear the mappers: as many fresh ones as there were.
        self.partial_results_map = (0..n).map(|_| new_agg_partial_result_mapper()).collect();
        mem_tracker.consume(-self.partial_results_map_mem);
        self.partial_results_map_mem = 0;
        result
    }

    fn spill_data_to_disk_impl(&mut self, agg_funcs: &[Box<dyn AggFunc>], spill_helper: &ParallelHashAggSpillHelper) -> Result<(), ExecError> {
        self.prepare_for_spill(spill_helper);
        let to_exec = |error: tidb_chunk::chunk_in_disk::DiskError| ExecError::internal(error.to_string());
        let maps = std::mem::take(&mut self.partial_results_map);
        for map in maps {
            for (key, mut partial_results) in map.into_iter() {
                let partition_num = tidb_murmur3::sum32(key.as_bytes()) as usize % SPILLED_PARTITION_NUM;
                // Spill the tmp chunk when it is full.
                if self.tmp_chks_for_spill[partition_num].is_full() {
                    self.spilled_chunks_io[partition_num].add(&self.tmp_chks_for_spill[partition_num]).map_err(to_exec)?;
                    self.tmp_chks_for_spill[partition_num].reset();
                }
                // Serialize each function's partial result, then the key.
                for (i, agg_func) in agg_funcs.iter().enumerate() {
                    agg_func.serialize_partial_result(&mut partial_results[i], &mut self.tmp_chks_for_spill[partition_num], &mut self.serialize_helpers);
                }
                self.tmp_chks_for_spill[partition_num].append_bytes(agg_funcs.len(), key.as_bytes());
            }
        }
        // Go `spillRemainingDataToDisk`.
        for i in 0..SPILLED_PARTITION_NUM {
            if self.tmp_chks_for_spill[i].num_rows() > 0 {
                self.spilled_chunks_io[i].add(&self.tmp_chks_for_spill[i]).map_err(to_exec)?;
                self.tmp_chks_for_spill[i].reset();
            }
        }
        Ok(())
    }
}

/// Go `HashAggPartialWorker`.
pub(crate) struct HashAggPartialWorker<C: Columns + Send + Sync + 'static> {
    pub(crate) base: BaseHashAggWorker,
    pub(crate) ctx: Arc<C>,
    /// Go `inputCh`; `None` is Go's close.
    pub(crate) input_ch: Receiver<Option<Chunk>>,
    pub(crate) input_ch_sender: Sender<Option<Chunk>>,
    pub(crate) output_chs: Vec<Sender<AggPartialResultMapper>>,
    pub(crate) global_output_ch: Sender<AfFinalResult>,
    /// Go `giveBackCh`: the fetcher's `inputCh`.
    pub(crate) give_back_ch: Sender<HashAggInput>,
    pub(crate) partial_result_num_in_row: usize,
    pub(crate) state: Arc<Mutex<PartialWorkerSpillState>>,
    pub(crate) group_by_items: Arc<Vec<Expression>>,
    pub(crate) group_key_buf: GroupKeys,
    /// Go `chk`: the input this worker swaps with each child chunk.
    pub(crate) chk: Chunk,
    pub(crate) spill_helper: Arc<ParallelHashAggSpillHelper>,
    /// Go `inflightChunkSync`.
    pub(crate) inflight_chunk_sync: Arc<WaitGroup>,
    /// Whether the fetcher's close (Go `close(inputCh)`) was received.
    pub(crate) input_closed: bool,
    /// Whether the chunk being processed still owes Go's deferred
    /// `inflightChunkSync.Done()`.
    pub(crate) holding_inflight: bool,
}

impl<C: Columns + Send + Sync + 'static> HashAggPartialWorker<C> {
    fn state(&self) -> MutexGuard<'_, PartialWorkerSpillState> {
        self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Go `getChildInput`.
    fn get_child_input(&mut self) -> Option<Chunk> {
        select! {
            recv(self.base.finish_ch) -> _ => None,
            recv(self.input_ch) -> chk => match chk {
                Ok(Some(chk)) => Some(chk),
                Ok(None) | Err(_) => {
                    self.input_closed = true;
                    None
                }
            },
        }
    }

    /// Go `fetchChunkAndProcess`.
    fn fetch_chunk_and_process(&mut self, has_error: &mut bool, need_shuffle: &mut bool) -> bool {
        if self.spill_helper.check_error() {
            *has_error = true;
            return false;
        }
        let wait_start = Instant::now();
        let Some(mut chk) = self.get_child_input() else {
            return false;
        };
        self.holding_inflight = true;
        self.base.update_wait_time(wait_start);

        self.chk.swap_columns(&mut chk);
        let _ = self.give_back_ch.send(HashAggInput { chk, give_back_ch: self.input_ch_sender.clone() });

        let exec_start = Instant::now();
        let chk = std::mem::replace(&mut self.chk, Chunk::new_chunk_with_capacity(&[], 0));
        let result = self.update_partial_result(&chk);
        self.chk = chk;
        self.holding_inflight = false;
        self.inflight_chunk_sync.done();
        if let Err(error) = result {
            *has_error = true;
            self.process_error(error);
            return false;
        }
        self.base.update_exec_time(exec_start);

        // The intermediate data is not empty if reaching here.
        *need_shuffle = true;
        true
    }

    /// Go `finalizeWorkerProcess`.
    fn finalize_worker_process(&mut self, need_shuffle: bool, has_error: bool) {
        // Consume every chunk until the fetcher's close, to avoid a hang of
        // the fetcher.
        while !self.input_closed {
            match self.input_ch.recv() {
                Ok(Some(_)) => self.inflight_chunk_sync.done(),
                Ok(None) | Err(_) => self.input_closed = true,
            }
        }
        if self.base.check_finish_ch_closed() || has_error {
            return;
        }
        if need_shuffle && self.spill_helper.is_spilled_chunks_io_empty() {
            self.shuffle_interm_data();
        }
    }

    /// Go `run`.
    pub(crate) fn run(mut self) {
        let start = Instant::now();
        let mut has_error = false;
        let mut need_shuffle = false;
        let global_output_ch = self.global_output_ch.clone();
        recover_hash_agg(&global_output_ch, || while self.fetch_chunk_and_process(&mut has_error, &mut need_shuffle) {});
        if self.holding_inflight {
            self.holding_inflight = false;
            self.inflight_chunk_sync.done();
        }
        recover_hash_agg(&global_output_ch, || self.finalize_worker_process(need_shuffle, has_error));
        self.base.mem_tracker.consume(-self.chk.memory_usage());
        self.base.update_worker_time(start);
    }

    /// Go `getPartialResultsOfEachRow`: each row's group, allocating a new
    /// group's results in its final worker's mapper.
    fn get_partial_results_of_each_row(&mut self, state: &mut PartialWorkerSpillState, final_concurrency: usize) -> Vec<(usize, GoString)> {
        let num_rows = self.group_key_buf.len();
        let mut all_mem_delta = 0;
        let mut located = Vec::with_capacity(num_rows);
        for i in 0..num_rows {
            let key = self.group_key_buf.key(i);
            let final_worker_idx = tidb_murmur3::sum32(key) as usize % final_concurrency;
            let key = GoString::from(key.to_vec());
            let mapper = &mut state.partial_results_map[final_worker_idx];
            if mapper.contains_key(&key) {
                located.push((final_worker_idx, key));
                continue;
            }
            // The first time this group key appears: create its results.
            let mut partial_results: Vec<PartialResult> = Vec::with_capacity(self.partial_result_num_in_row);
            for af in self.base.agg_funcs.iter() {
                let (partial_result, mem_delta) = af.alloc_partial_result();
                partial_results.push(partial_result);
                all_mem_delta += mem_delta;
            }
            all_mem_delta += self.partial_result_num_in_row as i64 * 8;
            let key_len = key.len() as i64;
            let delta = mapper.set(key.clone(), partial_results);
            all_mem_delta += key_len;
            if delta > 0 {
                state.partial_results_map_mem += delta;
                self.base.mem_tracker.consume(delta);
            }
            located.push((final_worker_idx, key));
        }
        state.partial_results_map_mem += all_mem_delta;
        self.base.mem_tracker.consume(all_mem_delta);
        located
    }

    /// Go `updatePartialResult`.
    fn update_partial_result(&mut self, chk: &Chunk) -> Result<(), ExecError> {
        let mem_size = get_group_key_mem_usage(&self.group_key_buf);
        let result = get_group_key(&*self.ctx, chk, &mut self.group_key_buf, &self.group_by_items);
        self.base.mem_tracker.consume(get_group_key_mem_usage(&self.group_key_buf) - mem_size);
        result?;

        let state = Arc::clone(&self.state);
        let mut state = state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let final_concurrency = state.partial_results_map.len();
        let located = self.get_partial_results_of_each_row(&mut state, final_concurrency);
        let mut all_mem_delta = 0;
        for (i, (final_worker_idx, key)) in located.iter().enumerate() {
            let rows = [chk.get_row(i)];
            let partial_results = state.partial_results_map[*final_worker_idx].get_mut(key).expect("the group was just located");
            for (af, partial_result) in self.base.agg_funcs.iter().zip(partial_results.iter_mut()) {
                all_mem_delta += af.update_partial_result(&*self.ctx, &rows, partial_result)?;
            }
        }
        self.base.mem_tracker.consume(all_mem_delta);
        state.partial_results_map_mem += all_mem_delta;
        Ok(())
    }

    /// Go `shuffleIntermData`: mapper `i` to final worker `i`.
    fn shuffle_interm_data(&mut self) {
        let maps = std::mem::take(&mut self.state().partial_results_map);
        for (output_ch, map) in self.output_chs.iter().zip(maps) {
            let _ = output_ch.send(map);
        }
    }

    /// Go `processError`.
    fn process_error(&self, error: ExecError) {
        let _ = self.global_output_ch.send(AfFinalResult { chk: None, err: Some(error), give_back_ch: None });
        self.spill_helper.set_error();
    }
}
