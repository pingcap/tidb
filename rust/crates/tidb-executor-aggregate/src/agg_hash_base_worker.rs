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

//! Go `pkg/executor/aggregate/agg_hash_base_worker.go`.

use std::sync::Arc;

use crossbeam_channel::Receiver;
use tidb_executor_aggfuncs::agg_func::AggFunc;
use tidb_memory::Tracker;

use crate::stats::AggWorkerStat;

/// Go `baseHashAggWorker`.
pub(crate) struct BaseHashAggWorker {
    /// Go `finishCh`: closed (every sender dropped) when the executor closes.
    pub(crate) finish_ch: Receiver<()>,
    pub(crate) agg_funcs: Arc<Vec<Box<dyn AggFunc>>>,
    pub(crate) max_chunk_size: usize,
    pub(crate) stats: Option<Arc<AggWorkerStat>>,
    pub(crate) mem_tracker: Arc<Tracker>,
}

impl BaseHashAggWorker {
    /// Go `newBaseHashAggWorker`.
    pub(crate) fn new(finish_ch: Receiver<()>, agg_funcs: Arc<Vec<Box<dyn AggFunc>>>, max_chunk_size: usize, mem_tracker: Arc<Tracker>) -> Self {
        Self { finish_ch, agg_funcs, max_chunk_size, stats: None, mem_tracker }
    }

    /// Go `getPartialResultSliceLenConsiderByteAlign`.
    pub(crate) fn get_partial_result_slice_len_consider_byte_align(&self) -> usize {
        let length = self.agg_funcs.len();
        if length == 1 {
            return 1;
        }
        length + (length & 1)
    }

    /// Go `checkFinishChClosed`.
    pub(crate) fn check_finish_ch_closed(&self) -> bool {
        matches!(self.finish_ch.try_recv(), Err(crossbeam_channel::TryRecvError::Disconnected))
    }

    pub(crate) fn update_wait_time(&self, since: std::time::Instant) {
        if let Some(stats) = &self.stats {
            stats.update_wait_time(since);
        }
    }

    /// Go `updateExecTime`: the time and one task.
    pub(crate) fn update_exec_time(&self, since: std::time::Instant) {
        if let Some(stats) = &self.stats {
            stats.update_exec_time(since);
            stats.add_task();
        }
    }

    pub(crate) fn update_worker_time(&self, since: std::time::Instant) {
        if let Some(stats) = &self.stats {
            stats.update_worker_time(since);
        }
    }
}
