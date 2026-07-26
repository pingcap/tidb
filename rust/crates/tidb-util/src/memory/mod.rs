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

//! Transcreation of Go `pkg/util/memory`'s tracker core (`tracker.go` +
//! `action.go`): the hierarchical memory-consumption tracker and the
//! OOM-action chain executors bind to it.
//!
//! Package coverage: tracker/action core, `pool.go` (`ResourcePool`/
//! `Budget`), `utils.go` (arbitrator support machinery), `arbitrator.go`
//! (the `MemArbitrator`), and the behavioral core of
//! `global_arbitrator.go` (mem-state JSON recorder + mode/soft-limit text
//! parsing). Not ported by scope decision: Prometheus metric reporters and
//! Go-runtime heap sampling (observability with no behavioral contract —
//! the arbitrator ingests stats via `handle_runtime_stats`), the
//! process-global singleton (server-config wiring; lands with the server
//! crate), `memstats.go`
//! (Go-runtime `ReadMemStats` snapshots — no Rust counterpart), and
//! `meminfo.go`, which probes host/cgroup memory via gopsutil +
//! `pkg/util/cgroup` and lands as one unit with the cgroup package port.
//!
//! Faithful adaptations in the core, none changing observable behavior:
//! - Go's `*Tracker` graph becomes `Arc<Tracker>` with a `Weak` parent and
//!   `Arc::ptr_eq` for the identity comparisons `remove`/`ReplaceChild`/
//!   `UnbindActionFromHardLimit` perform.
//! - Prometheus gauges (`metrics.MemoryUsage`) are observability side
//!   effects with no behavioral contract: not ported.
//! - GC-aware release (`EnableGCAwareMemoryTrack` + `runtime.SetFinalizer`)
//!   defers the release decrement until a Go GC cycle — Go-runtime
//!   machinery with no Rust counterpart; `Release` therefore takes the
//!   default (flag-off) path, `Consume(-bytes)`, and the flag is not
//!   surfaced.

mod action;
mod arbitrator;
mod arbitrator_utils;
mod mem_state_recorder;
mod pool;
mod tracker;

pub use action::{
    ActionOnExceed, BaseOomAction, LogOnExceed, PanicOnExceed, DEF_CURSOR_FETCH_SPILL_PRIORITY,
    DEF_LOG_PRIORITY, DEF_PANIC_PRIORITY, DEF_RATE_LIMIT_PRIORITY, DEF_SPILL_PRIORITY,
};
pub use arbitrator::{
    cancel_channel, ArbitrateHelper, ArbitrateResult, ArbitrationContext, ArbitrationPriority,
    ArbitratorStopReason, ArbitratorWorkMode, CancelChannel, CancelHandle, ConcurrentBudget,
    ExecMetricsCounter, LastRisk, MemArbitrator, MemArbitratorActions, MemPoolQuotaUsage, MemStats,
    NumByPattern, NumByPriority, PairSuccessFail, PoolAllocProfile as ArbitratorPoolAllocProfile,
    RecordMemState, RootPoolEntry, RootPoolWrap, RuntimeMemStateV1, SoftLimitMode,
    TrackedConcurrentBudget, ARBITRATION_WAIT_AVERSE, DEF_AWAIT_FREE_POOL_ALLOC_ALIGN_SIZE,
    DEF_AWAIT_FREE_POOL_SHARD_NUM, DEF_POOL_QUOTA_SHARDS, DEF_POOL_STATUS_SHARDS,
    DEF_TASK_TICK_DUR, ERR_ARBITRATE_FAIL, MAX_ARBITRATOR_MODE,
};
pub use arbitrator_utils::{hash_even_num, hash_str};
pub use mem_state_recorder::{
    parse_soft_limit_text, parse_work_mode_text, runtime_mem_state_recorder_file_path,
    RuntimeMemStateRecorder,
};
pub use pool::{
    Budget, OutOfCapacityActionArgs, PoolActions, PoolCallbackCtx, PoolError, ResourcePool,
    ResourcePoolState, DEF_MAX_LIMIT, DEF_MAX_UNUSED_BLOCKS, DEF_POOL_ALLOC_ALIGN_SIZE,
};
pub use tracker::{
    bytes_to_string, format_bytes, Tracker, DEF_MEM_QUOTA_QUERY, LABEL_FOR_GLOBAL_ANALYZE_MEMORY,
    LABEL_FOR_MEM_DB, LABEL_FOR_SQL_TEXT, TRACK_MEM_WHEN_EXCEEDS,
};

#[cfg(test)]
pub(crate) mod arbitrator_test_hooks {
    //! Go `mockWinupCB` (gated by `intest.InTest` in the source).
    use std::sync::Mutex;

    type WindupCb = Box<dyn Fn(&super::arbitrator::RootPoolEntry) + Send>;
    pub(crate) static MOCK_WINDUP_CB: Mutex<Option<WindupCb>> = Mutex::new(None);

    pub(crate) fn fire_windup_cb(e: &super::arbitrator::RootPoolEntry) {
        if let Some(cb) = MOCK_WINDUP_CB.lock().unwrap().as_ref() {
            cb(e);
        }
    }
}
