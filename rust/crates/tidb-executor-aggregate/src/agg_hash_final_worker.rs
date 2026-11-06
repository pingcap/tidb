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

//! Go `pkg/executor/aggregate/agg_hash_final_worker.go`: a final worker
//! merges every partial worker's mapper for its keys and sends the final
//! results in chunks it borrows from the main thread.

use std::sync::Arc;
use std::time::Instant;

use crossbeam_channel::{select, Receiver, Sender};
use tidb_chunk::chunk::Chunk;
use tidb_driver_error::ExecError;
use tidb_executor_aggfuncs::agg_func::{new_agg_partial_result_mapper, AggPartialResultMapper};
use tidb_exprctx::Columns;

use crate::agg_hash_base_worker::BaseHashAggWorker;
use crate::agg_spill::ParallelHashAggSpillHelper;
use crate::agg_util::{recover_hash_agg, AfFinalResult};

/// Go `HashAggFinalWorker`.
pub(crate) struct HashAggFinalWorker<C: Columns + Send + Sync + 'static> {
    pub(crate) base: BaseHashAggWorker,
    pub(crate) ctx: Arc<C>,
    pub(crate) partial_result_map: AggPartialResultMapper,
    pub(crate) input_ch: Receiver<AggPartialResultMapper>,
    pub(crate) output_ch: Sender<AfFinalResult>,
    pub(crate) final_result_holder_ch: Receiver<Chunk>,
    pub(crate) final_result_holder_sender: Sender<Chunk>,
    pub(crate) spill_helper: Arc<ParallelHashAggSpillHelper>,
    /// Go `restoredAggResultMapperMem`: the restored partition's bytes,
    /// released before the next partition is restored.
    pub(crate) restored_agg_result_mapper_mem: i64,
}

impl<C: Columns + Send + Sync + 'static> HashAggFinalWorker<C> {
    /// Go `getPartialInput`.
    fn get_partial_input(&self) -> Option<AggPartialResultMapper> {
        let wait_start = Instant::now();
        let input = select! {
            recv(self.base.finish_ch) -> _ => None,
            recv(self.input_ch) -> input => input.ok(),
        };
        self.base.update_wait_time(wait_start);
        input
    }

    /// Go `mergeInputIntoResultMap`.
    fn merge_input_into_result_map(&mut self, mut input: AggPartialResultMapper) -> Result<(), ExecError> {
        // The first input becomes the result map.
        if self.partial_result_map.is_empty() {
            self.partial_result_map = input;
            return Ok(());
        }
        let exec_start = Instant::now();
        let mut all_mem_delta = 0;
        let keys: Vec<_> = input.iter().map(|(key, _)| key.clone()).collect();
        for key in keys {
            let mut value = input.remove(&key).expect("the key is in the input");
            match self.partial_result_map.get_mut(&key) {
                None => {
                    let delta_bytes = self.partial_result_map.set(key, value);
                    if delta_bytes > 0 {
                        self.base.mem_tracker.consume(delta_bytes);
                    }
                }
                Some(dst_val) => {
                    for (j, af) in self.base.agg_funcs.iter().enumerate() {
                        all_mem_delta += af.merge_partial_result(&*self.ctx, &mut value[j], &mut dst_val[j])?;
                    }
                }
            }
        }
        self.base.mem_tracker.consume(all_mem_delta);
        self.base.update_exec_time(exec_start);
        Ok(())
    }

    /// Go `consumeIntermData`.
    fn consume_interm_data(&mut self) -> Result<(), ExecError> {
        while let Some(input) = self.get_partial_input() {
            self.merge_input_into_result_map(input)?;
        }
        Ok(())
    }

    /// Go `generateResultAndSend`.
    fn generate_result_and_send(&mut self, mut result: Chunk) -> Option<Chunk> {
        let map = std::mem::replace(&mut self.partial_result_map, new_agg_partial_result_mapper());
        for (_, mut results) in map.into_iter() {
            for (j, af) in self.base.agg_funcs.iter().enumerate() {
                // Go logs the error and carries on.
                if let Err(error) = af.append_final_result_2_chunk(&*self.ctx, &mut results[j], &mut result) {
                    tracing::error!(?error, "HashAggFinalWorker failed to append final result to Chunk");
                }
            }
            if self.base.agg_funcs.is_empty() {
                result.set_num_virtual_rows(result.num_rows() + 1);
            }
            if result.is_full() {
                let _ = self.output_ch.send(AfFinalResult { chk: Some(result), err: None, give_back_ch: Some(self.final_result_holder_sender.clone()) });
                result = self.receive_final_result_holder()?;
            }
        }
        Some(result)
    }

    /// Go `sendFinalResult`.
    fn send_final_result(&mut self) {
        let wait_start = Instant::now();
        let result = self.receive_final_result_holder();
        self.base.update_wait_time(wait_start);
        let Some(result) = result else {
            return;
        };
        let exec_start = Instant::now();
        self.base.update_exec_time(exec_start);
        let mut result = result;
        if self.spill_helper.is_spilled_chunks_io_empty() {
            let Some(next) = self.generate_result_and_send(result) else {
                return;
            };
            result = next;
        } else {
            loop {
                if self.base.check_finish_ch_closed() {
                    return;
                }
                let (eof, has_error) = self.restore_data_from_disk();
                if has_error {
                    return;
                }
                if eof {
                    break;
                }
                let Some(next) = self.generate_result_and_send(result) else {
                    return;
                };
                result = next;
            }
        }
        let _ = self.output_ch.send(AfFinalResult { chk: Some(result), err: None, give_back_ch: Some(self.final_result_holder_sender.clone()) });
    }

    /// Go `restoreDataFromDisk`: one partition at a time, releasing the
    /// previous one's memory first.
    fn restore_data_from_disk(&mut self) -> (bool, bool) {
        self.spill_helper.mem_tracker.consume(-self.restored_agg_result_mapper_mem);
        match self.spill_helper.restore_one_partition(&*self.ctx) {
            Err(error) => {
                let _ = self.output_ch.send(AfFinalResult { chk: None, err: Some(error), give_back_ch: None });
                (false, true)
            }
            Ok((None, restored_mem)) => {
                // Every partition is restored.
                self.restored_agg_result_mapper_mem = restored_mem;
                (true, false)
            }
            Ok((Some(map), restored_mem)) => {
                self.partial_result_map = map;
                self.restored_agg_result_mapper_mem = restored_mem;
                (false, false)
            }
        }
    }

    /// Go `receiveFinalResultHolder`: `None` once the executor closes.
    fn receive_final_result_holder(&self) -> Option<Chunk> {
        select! {
            recv(self.base.finish_ch) -> _ => None,
            recv(self.final_result_holder_ch) -> result => result.ok(),
        }
    }

    /// Go `run`, after every partial worker has finished; `cleanup`
    /// recovers its panic.
    pub(crate) fn run(mut self) {
        let start = Instant::now();
        let output_ch = self.output_ch.clone();
        recover_hash_agg(&output_ch, || self.run_impl(start));
        self.base.update_worker_time(start);
    }

    fn run_impl(&mut self, start: Instant) {
        let _ = start;
        if self.spill_helper.is_spilled_chunks_io_empty() {
            if let Err(error) = self.consume_interm_data() {
                let _ = self.output_ch.send(AfFinalResult { chk: None, err: Some(error), give_back_ch: None });
                return;
            }
        } else if self.spill_helper.check_error() {
            return;
        }
        self.send_final_result();
    }
}
