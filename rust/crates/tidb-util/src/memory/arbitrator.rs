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

//! Transcreation of Go `pkg/util/memory/arbitrator.go`: the `MemArbitrator`
//! that arbitrates memory quota among root resource pools, cancels or kills
//! pools under quota/OOM pressure, and self-tunes via memory profiles.
//!
//! Faithful adaptations, none changing observable behavior:
//! - Go channels map to `crossbeam-channel`: each entry's `resultCh` is a
//!   bounded(1) channel; a session's `cancelCh` (a closed-channel
//!   broadcast) is a zero-capacity channel whose sender side is dropped on
//!   cancel — receiving from it then returns immediately, exactly like a
//!   closed Go channel. `waitAlloc`'s two-way `select` uses crossbeam's
//!   `Select`.
//! - `atomic.Pointer[ArbitrationContext]` becomes a `Mutex<Option<Arc<..>>>`
//!   (loads clone the `Arc`); the "data race is acceptable" hint fields
//!   (`memPriority`, `waitAverse`, `preferPrivilege`) become atomics.
//! - `sync.Map` caches (`contextCache`, digest shards) become
//!   `Mutex<HashMap>`; iteration clones the bucket out first, matching the
//!   snapshot-ish semantics `Range` provides.
//! - Log actions receive a message and a formatted field string instead of
//!   zap fields; counts of Info/Warn/Error calls (the test contract) are
//!   preserved 1:1.
//! - `//go:norace` accessors read under the owning lock or atomically; the
//!   approximation contract (may lag) is kept, data races are not.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicU32, AtomicU64, Ordering::SeqCst};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime};

use crossbeam_channel::{bounded, never, Receiver, Select, Sender};

use super::arbitrator_utils::*;
use super::pool::{OutOfCapacityActionArgs, PoolError, ResourcePool, DEF_MAX_LIMIT};

mod arbitrate;
mod digest_profile;
mod mem_risk;
mod root_pool;
mod runtime_stats;

/// Result of the arbitration process (Go `ArbitrateResult`).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ArbitrateResult {
    /// The arbitration succeeded.
    Ok,
    /// The arbitration failed.
    Fail,
}

/// Soft-limit mode of the mem-arbitrator (Go `SoftLimitMode`).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum SoftLimitMode {
    /// Same as the OOM-risk threshold.
    #[default]
    Disable,
    /// A specified byte count or rate of the limit.
    Specified,
    /// Auto-calculated by the arbitrator.
    Auto,
}

/// Name of the disable soft-limit mode (Go `ArbitratorSoftLimitModDisableName`).
pub const ARBITRATOR_SOFT_LIMIT_MODE_DISABLE_NAME: &str = "0";
/// Name of the auto soft-limit mode.
pub const ARBITRATOR_SOFT_LIMIT_MODE_AUTO_NAME: &str = "auto";
/// Name of the standard work mode.
pub const ARBITRATOR_MODE_STANDARD_NAME: &str = "standard";
/// Name of the priority work mode.
pub const ARBITRATOR_MODE_PRIORITY_NAME: &str = "priority";
/// Name of the disable work mode.
pub const ARBITRATOR_MODE_DISABLE_NAME: &str = "disable";

/// Default task tick duration (Go `defTaskTickDur`; used by the global
/// wiring in the server crate).
pub const DEF_TASK_TICK_DUR: Duration = Duration::from_millis(10);
pub(crate) const DEF_MIN_HEAP_FREE_BPS: i64 = 100 * BYTE_SIZE_MB;
pub(crate) const DEF_HEAP_RECLAIM_CHECK_DURATION: Duration = Duration::from_secs(1);
pub(crate) const DEF_HEAP_RECLAIM_CHECK_MAX_DURATION: Duration = Duration::from_secs(5);
pub(crate) const DEF_OOM_RISK_RATIO: f64 = 0.95;
pub(crate) const DEF_MEM_RISK_RATIO: f64 = 0.9;
pub(crate) const DEF_TICK_DUR_MILLI: i64 = KILO;
pub(crate) const DEF_STORE_POOL_MEDIUM_CAP_DUR_MILLI: i64 = DEF_TICK_DUR_MILLI * 10;
pub(crate) const DEF_TRACK_MEM_STATS_DUR_MILLI: i64 = KILO;
pub(crate) const DEF_MAX: i64 = 9_000_000_000_000_000;
pub(crate) const DEF_SERVERLIMIT_SMALL_LIMIT_NUM: i64 = 1000;
pub(crate) const DEF_SERVERLIMIT_MIN_UNIT_NUM: i64 = 500;
pub(crate) const DEF_SERVERLIMIT_MAX_UNIT_NUM: i64 = 100;
pub(crate) const DEF_UPDATE_MEM_CONSUMED_TIME_ALIGN_SEC: i64 = 30;
pub(crate) const DEF_UPDATE_MEM_MAGNIF_UTIME_ALIGN: i64 = 30;
pub(crate) const DEF_UPDATE_BUFFER_TIME_ALIGN_SEC: i64 = 60;
pub(crate) const DEF_REDUNDANCY: usize = 2;
pub(crate) const DEF_POOL_RESERVED_QUOTA: i64 = BYTE_SIZE_MB;
/// Default await-free pool allocation alignment (Go
/// `defAwaitFreePoolAllocAlignSize`).
pub const DEF_AWAIT_FREE_POOL_ALLOC_ALIGN_SIZE: i64 = DEF_POOL_RESERVED_QUOTA + BYTE_SIZE_MB;
/// Default await-free pool shard count (Go `defAwaitFreePoolShardNum`).
pub const DEF_AWAIT_FREE_POOL_SHARD_NUM: i64 = 256;
pub(crate) const DEF_AWAIT_FREE_POOL_SHRINK_DUR_MILLI: i64 = KILO * 2;
/// Default entry-map shard count (Go `defPoolStatusShards`).
pub const DEF_POOL_STATUS_SHARDS: u64 = 128;
/// Default quota shard count (Go `defPoolQuotaShards`).
pub const DEF_POOL_QUOTA_SHARDS: usize = 27;
pub(crate) const DEF_KILL_CANCEL_CHECK_TIMEOUT: Duration = Duration::from_secs(20);
pub(crate) const DEF_DIGEST_PROFILE_SMALL_MEM_TIMEOUT_SEC: i64 = 60 * 60 * 24;
pub(crate) const DEF_DIGEST_PROFILE_MEM_TIMEOUT_SEC: i64 = 60 * 60 * 24 * 7;
pub(crate) const DEF_MAX_MAGNIF: i64 = KILO * 10;
pub(crate) const DEF_MAX_DIGEST_PROFILE_CACHE_LIMIT: i64 = 40_000;

/// Work mode of the arbitrator (Go `ArbitratorWorkMode`).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ArbitratorWorkMode {
    /// Standard mode.
    Standard = 0,
    /// Priority mode.
    Priority = 1,
    /// Disabled.
    Disable = 2,
}

/// Number of work modes (Go `maxArbitratorMode`; used by the metrics
/// wiring in the server crate).
pub const MAX_ARBITRATOR_MODE: usize = 3;

impl ArbitratorWorkMode {
    fn from_i32(v: i32) -> ArbitratorWorkMode {
        match v {
            0 => ArbitratorWorkMode::Standard,
            1 => ArbitratorWorkMode::Priority,
            _ => ArbitratorWorkMode::Disable,
        }
    }
    /// Go `String`.
    pub fn as_str(&self) -> &'static str {
        match self {
            ArbitratorWorkMode::Standard => ARBITRATOR_MODE_STANDARD_NAME,
            ArbitratorWorkMode::Priority => ARBITRATOR_MODE_PRIORITY_NAME,
            ArbitratorWorkMode::Disable => ARBITRATOR_MODE_DISABLE_NAME,
        }
    }
}

/// Priority of an arbitration task (Go `ArbitrationPriority`).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum ArbitrationPriority {
    /// Low priority.
    Low = 0,
    /// Medium priority.
    Medium = 1,
    /// High priority.
    High = 2,
}

pub(crate) const MAX_ARBITRATION_PRIORITY: usize = 3;
pub(crate) const MAX_ARBITRATE_MODE: usize = MAX_ARBITRATION_PRIORITY + 1;
/// Index of the wait-averse pattern in [`NumByPattern`] (Go
/// `ArbitrationWaitAverse`).
pub const ARBITRATION_WAIT_AVERSE: usize = MAX_ARBITRATION_PRIORITY;

pub(crate) const PRIORITIES: [ArbitrationPriority; MAX_ARBITRATION_PRIORITY] = [
    ArbitrationPriority::Low,
    ArbitrationPriority::Medium,
    ArbitrationPriority::High,
];

impl ArbitrationPriority {
    fn from_i32(v: i32) -> ArbitrationPriority {
        match v {
            0 => ArbitrationPriority::Low,
            1 => ArbitrationPriority::Medium,
            _ => ArbitrationPriority::High,
        }
    }
    /// Go `String`.
    pub fn as_str(&self) -> &'static str {
        match self {
            ArbitrationPriority::Low => "LOW",
            ArbitrationPriority::Medium => "MEDIUM",
            ArbitrationPriority::High => "HIGH",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum EntryExecState {
    Idle = 0,
    Running = 1,
    Privileged = 2,
}

/// The arbitrate-fail error text (Go `errArbitrateFailError`).
pub const ERR_ARBITRATE_FAIL: &str = "failed to allocate resource from arbitrator";

pub(crate) fn err_arbitrate_fail() -> PoolError {
    PoolError(ERR_ARBITRATE_FAIL.to_string())
}

/// Reason why an arbitrate helper is stopped (Go `ArbitratorStopReason`).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ArbitratorStopReason {
    /// Kill because of OOM risk.
    OomRiskKill,
    /// Cancel a wait-averse task out of quota.
    WaitAverseCancel,
    /// Cancel under standard mode out of quota.
    StandardCancel,
    /// Cancel under priority mode out of quota.
    PriorityCancel,
}

impl ArbitratorStopReason {
    /// Go `String`.
    pub fn as_str(&self) -> &'static str {
        match self {
            ArbitratorStopReason::OomRiskKill => "KILL(out-of-memory)",
            ArbitratorStopReason::WaitAverseCancel => "CANCEL(out-of-quota & wait-averse)",
            ArbitratorStopReason::StandardCancel => "CANCEL(out-of-quota & standard-mode)",
            ArbitratorStopReason::PriorityCancel => "CANCEL(out-of-quota & priority-mode)",
        }
    }
}

/// Interface for the arbitrate helper (Go `ArbitrateHelper`).
pub trait ArbitrateHelper: Send + Sync {
    /// Kill (OOM risk) or cancel by the arbitrator.
    fn stop(&self, reason: ArbitratorStopReason) -> bool;
    /// Track heap usage.
    fn heap_inuse(&self) -> i64;
    /// Called when the pool is removed.
    fn finish(&self);
}

/// A cancel signal source mirroring a Go `<-chan struct{}` that is closed
/// to broadcast cancellation.
#[derive(Clone)]
pub struct CancelChannel {
    rx: Receiver<()>,
}

/// The closing half of a [`CancelChannel`]; dropping it (or calling
/// [`CancelHandle::close`]) cancels, exactly like Go's `close(ch)`.
pub struct CancelHandle {
    _tx: Sender<()>,
}

impl CancelHandle {
    /// Close the channel (drop the sender).
    pub fn close(self) {}
}

/// Creates a connected cancel channel/handle pair.
pub fn cancel_channel() -> (CancelHandle, CancelChannel) {
    let (tx, rx) = bounded(0);
    (CancelHandle { _tx: tx }, CancelChannel { rx })
}

impl CancelChannel {
    #[cfg(test)]
    pub(crate) fn is_closed(&self) -> bool {
        matches!(
            self.rx.try_recv(),
            Err(crossbeam_channel::TryRecvError::Disconnected)
        )
    }
}

/// Context & properties of a root pool that the arbitrator can access (Go
/// `ArbitrationContext`).
pub struct ArbitrationContext {
    pub(crate) arbitrate_helper: Mutex<Option<Arc<dyn ArbitrateHelper>>>,
    pub(crate) cancel_ch: Option<CancelChannel>,
    pub(crate) mem_priority: ArbitrationPriority,
    pub(crate) stopped: AtomicBool,
    pub(crate) wait_averse: bool,
    pub(crate) prefer_privilege: bool,
}

impl ArbitrationContext {
    /// Go `NewArbitrationContext`.
    pub fn new(
        cancel_ch: Option<CancelChannel>,
        arbitrate_helper: Option<Arc<dyn ArbitrateHelper>>,
        mem_priority: ArbitrationPriority,
        wait_averse: bool,
        prefer_privilege: bool,
    ) -> Arc<ArbitrationContext> {
        Arc::new(ArbitrationContext {
            arbitrate_helper: Mutex::new(arbitrate_helper),
            cancel_ch,
            mem_priority,
            stopped: AtomicBool::new(false),
            wait_averse,
            prefer_privilege,
        })
    }

    pub(crate) fn helper(&self) -> Option<Arc<dyn ArbitrateHelper>> {
        self.arbitrate_helper.lock().unwrap().clone()
    }

    /// Replace the helper (test surface mirroring Go's direct field write).
    pub fn set_helper(&self, h: Option<Arc<dyn ArbitrateHelper>>) {
        *self.arbitrate_helper.lock().unwrap() = h;
    }

    /// Whether the context can be acted on (Go `available`).
    pub fn available(self: &Arc<Self>) -> bool {
        self.helper().is_some() && !self.stopped.load(SeqCst)
    }

    pub(crate) fn stop(&self, reason: ArbitratorStopReason) {
        if self.stopped.swap(true, SeqCst) {
            return;
        }
        if let Some(h) = self.helper() {
            h.stop(reason);
        }
    }
}

fn ctx_available(ctx: &Option<Arc<ArbitrationContext>>) -> bool {
    match ctx {
        Some(c) => c.available(),
        None => false,
    }
}

pub(crate) struct EntryKillCancelCtx {
    pub(crate) start_time: SystemTime,
    pub(crate) reclaim: i64,
    pub(crate) start: bool,
    pub(crate) fail: bool,
}

impl Default for EntryKillCancelCtx {
    fn default() -> Self {
        EntryKillCancelCtx {
            start_time: SystemTime::UNIX_EPOCH,
            reclaim: 0,
            start: false,
            fail: false,
        }
    }
}
// (kept manual: UNIX_EPOCH is not `SystemTime::default()`)

/// Per-entry state guarded by the arbitrator's `tasks` mutex (Go
/// `rootPoolEntry.taskMu`).
#[derive(Default)]
pub(crate) struct EntryTaskState {
    pub(crate) fifo: WrapListElement,
    pub(crate) fifo_wait_averse: WrapListElement,
    pub(crate) fifo_by_priority: WrapListElement,
    pub(crate) fifo_priority: Option<ArbitrationPriority>,
}

/// State mutated only by the arbitrator (Go `rootPoolEntry.arbitratorMu`).
#[derive(Default)]
pub(crate) struct EntryArbState {
    pub(crate) quota_shard: Option<(ArbitrationPriority, usize)>,
    pub(crate) under_kill: EntryKillCancelCtx,
    pub(crate) under_cancel: EntryKillCancelCtx,
    pub(crate) quota: i64,
    pub(crate) destroyed: bool,
}

/// Root pool entry (Go `rootPoolEntry`).
pub struct RootPoolEntry {
    pub(crate) pool: Arc<ResourcePool>,
    pub(crate) task_mu: Mutex<EntryTaskState>,
    pub(crate) ctx: EntryCtx,
    pub(crate) request: EntryRequest,
    pub(crate) arbitrator_mu: Mutex<EntryArbState>,
    pub(crate) state_mu: EntryStateMu,
}

pub(crate) struct EntryCtx {
    pub(crate) ptr: Mutex<Option<Arc<ArbitrationContext>>>,
    pub(crate) cancel_ch: Mutex<Option<CancelChannel>>,
    pub(crate) canceled: AtomicBool,
    // hint fields; Go tolerates races here, Rust uses atomics.
    pub(crate) mem_priority: AtomicI32,
    pub(crate) wait_averse: AtomicBool,
    pub(crate) prefer_privilege: AtomicBool,
}

pub(crate) struct EntryRequest {
    pub(crate) result_tx: Sender<(ArbitrateResult, i64)>,
    pub(crate) result_rx: Receiver<(ArbitrateResult, i64)>,
    pub(crate) quota: AtomicI64,
    pub(crate) task_mu: Mutex<()>,
}

#[derive(Default)]
pub(crate) struct EntryStateMu {
    pub(crate) quota_to_reclaim: AtomicI64,
    pub(crate) mutex: Mutex<()>,
    pub(crate) stop: AtomicBool,
    pub(crate) exec: AtomicI32,
}

impl RootPoolEntry {
    fn new(pool: Arc<ResourcePool>) -> Arc<RootPoolEntry> {
        let (tx, rx) = bounded(1);
        Arc::new(RootPoolEntry {
            pool,
            task_mu: Mutex::new(EntryTaskState::default()),
            ctx: EntryCtx {
                ptr: Mutex::new(None),
                cancel_ch: Mutex::new(None),
                canceled: AtomicBool::new(false),
                mem_priority: AtomicI32::new(ArbitrationPriority::Medium as i32),
                wait_averse: AtomicBool::new(false),
                prefer_privilege: AtomicBool::new(false),
            },
            request: EntryRequest {
                result_tx: tx,
                result_rx: rx,
                quota: AtomicI64::new(0),
                task_mu: Mutex::new(()),
            },
            arbitrator_mu: Mutex::new(EntryArbState::default()),
            state_mu: EntryStateMu::default(),
        })
    }

    /// The entry's pool (test/introspection surface).
    pub fn pool(&self) -> &Arc<ResourcePool> {
        &self.pool
    }

    pub(crate) fn load_ctx(&self) -> Option<Arc<ArbitrationContext>> {
        self.ctx.ptr.lock().unwrap().clone()
    }

    pub(crate) fn exec_state(&self) -> EntryExecState {
        match self.state_mu.exec.load(SeqCst) {
            0 => EntryExecState::Idle,
            1 => EntryExecState::Running,
            _ => EntryExecState::Privileged,
        }
    }

    pub(crate) fn set_exec_state(&self, s: EntryExecState) {
        self.state_mu.exec.store(s as i32, SeqCst);
    }

    pub(crate) fn enter_exec_privileged(&self) -> bool {
        self.state_mu
            .exec
            .compare_exchange(
                EntryExecState::Running as i32,
                EntryExecState::Privileged as i32,
                SeqCst,
                SeqCst,
            )
            .is_ok()
    }

    pub(crate) fn not_running(&self) -> bool {
        self.state_mu.stop.load(SeqCst)
            || self.exec_state() == EntryExecState::Idle
            || self.state_mu.quota_to_reclaim.load(SeqCst) > 0
    }

    pub(crate) fn mem_priority(&self) -> ArbitrationPriority {
        ArbitrationPriority::from_i32(self.ctx.mem_priority.load(SeqCst))
    }

    /// Wind up a finished task and publish the result (Go `windUp`).
    ///
    /// Go's `windUp` writes the pool capacity (`forceAddCap`) without the
    /// pool lock while the granted waiter may still be blocked inside it —
    /// a race the Go source tolerates. In Rust the capacity delta travels
    /// with the result and the WAITER applies it in its own locking
    /// context, which is the same observable outcome without the race.
    pub(crate) fn wind_up(&self, delta: i64, r: ArbitrateResult) {
        let _ = self.request.result_tx.send((r, delta));
        #[cfg(test)]
        super::arbitrator_test_hooks::fire_windup_cb(self);
    }
}

type MapUidEntry = HashMap<u64, Arc<RootPoolEntry>>;

pub(crate) struct EntryMapShard {
    pub(crate) entries: RwLock<MapUidEntry>,
}

#[derive(Default)]
pub(crate) struct EntryQuotaShard {
    pub(crate) entries: MapUidEntry,
}

/// Sharded status map + priority/quota-ordered shards (Go `entryMap`).
pub(crate) struct EntryMap {
    pub(crate) quota_shards: [Vec<Mutex<EntryQuotaShard>>; MAX_ARBITRATION_PRIORITY],
    pub(crate) context_cache: Mutex<MapUidEntry>,
    pub(crate) context_cache_num: AtomicI64,
    pub(crate) shards: Vec<EntryMapShard>,
    pub(crate) shards_mask: u64,
    pub(crate) max_quota_shard_index: usize,
    pub(crate) min_quota_shard_index_to_check: usize,
}

impl EntryMap {
    pub(crate) fn init(
        shard_num: u64,
        max_quota_shard: usize,
        min_quota_for_reclaim: i64,
    ) -> EntryMap {
        let mut quota_shards: [Vec<Mutex<EntryQuotaShard>>; MAX_ARBITRATION_PRIORITY] =
            [Vec::new(), Vec::new(), Vec::new()];
        for qs in quota_shards.iter_mut() {
            for _ in 0..max_quota_shard {
                qs.push(Mutex::new(EntryQuotaShard::default()));
            }
        }
        let mut shards = Vec::with_capacity(shard_num as usize);
        for _ in 0..shard_num {
            shards.push(EntryMapShard {
                entries: RwLock::new(HashMap::new()),
            });
        }
        EntryMap {
            quota_shards,
            context_cache: Mutex::new(HashMap::new()),
            context_cache_num: AtomicI64::new(0),
            shards,
            shards_mask: shard_num - 1,
            max_quota_shard_index: max_quota_shard,
            min_quota_shard_index_to_check: get_quota_shard(min_quota_for_reclaim, max_quota_shard),
        }
    }

    pub(crate) fn status_shard(&self, key: u64) -> &EntryMapShard {
        &self.shards[shard_index_by_uid(key, self.shards_mask) as usize]
    }

    /// Go `entryMap.delete` (arbitrator-only).
    pub(crate) fn delete(&self, entry: &Arc<RootPoolEntry>) {
        let uid = entry.pool.uid();
        {
            let mut st = entry.arbitrator_mu.lock().unwrap();
            if let Some((prio, pos)) = st.quota_shard.take() {
                self.quota_shards[prio as usize][pos]
                    .lock()
                    .unwrap()
                    .entries
                    .remove(&uid);
                st.quota = 0;
            }
        }
        self.status_shard(uid).entries.write().unwrap().remove(&uid);
        if self.context_cache.lock().unwrap().remove(&uid).is_some() {
            self.context_cache_num.fetch_add(-1, SeqCst);
        }
    }

    /// Go `entryMap.addQuota` (arbitrator-only).
    pub(crate) fn add_quota(&self, entry: &Arc<RootPoolEntry>, delta: i64) {
        if delta == 0 {
            return;
        }
        let uid = entry.pool.uid();
        let mut st = entry.arbitrator_mu.lock().unwrap();
        st.quota += delta;

        if st.quota == 0 {
            if let Some((prio, pos)) = st.quota_shard.take() {
                self.quota_shards[prio as usize][pos]
                    .lock()
                    .unwrap()
                    .entries
                    .remove(&uid);
            }
            return;
        }

        let prio = entry.mem_priority();
        let new_pos = get_quota_shard(st.quota, self.max_quota_shard_index);
        if st.quota_shard != Some((prio, new_pos)) {
            if let Some((op, opos)) = st.quota_shard.take() {
                self.quota_shards[op as usize][opos]
                    .lock()
                    .unwrap()
                    .entries
                    .remove(&uid);
            }
            st.quota_shard = Some((prio, new_pos));
            self.quota_shards[prio as usize][new_pos]
                .lock()
                .unwrap()
                .entries
                .insert(uid, Arc::clone(entry));
        }
    }

    /// Go `entryMap.emplace`: get-or-create; bool = newly created.
    pub(crate) fn emplace(&self, pool: Arc<ResourcePool>) -> (Arc<RootPoolEntry>, bool) {
        let key = pool.uid();
        let shard = self.status_shard(key);
        if let Some(v) = shard.entries.read().unwrap().get(&key) {
            return (Arc::clone(v), false);
        }
        let tar = RootPoolEntry::new(pool);
        let mut entries = shard.entries.write().unwrap();
        if let Some(v) = entries.get(&key) {
            return (Arc::clone(v), false);
        }
        entries.insert(key, Arc::clone(&tar));
        (tar, true)
    }
}

/// Wrapped reference to a root pool entry (Go `rootPoolWrap`).
#[derive(Clone, Default)]
pub struct RootPoolWrap {
    /// The wrapped entry, if found.
    pub entry: Option<Arc<RootPoolEntry>>,
}

/// Profile of root pool allocation (Go `PoolAllocProfile`).
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
pub struct PoolAllocProfile {
    /// `limit / 1000`.
    pub small_pool_limit: i64,
    /// `limit / 500`.
    pub pool_alloc_unit: i64,
    /// `limit / 100`.
    pub max_pool_alloc_unit: i64,
}

/// Success/fail counter pair (Go `pairSuccessFail`).
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
pub struct PairSuccessFail {
    /// Successes.
    pub succ: i64,
    /// Failures.
    pub fail: i64,
}

/// Task counts by priority (Go `NumByPriority`).
pub type NumByPriority = [i64; MAX_ARBITRATION_PRIORITY];
/// Task counts by pattern: 3 priorities then wait-averse (Go `NumByPattern`).
pub type NumByPattern = [i64; MAX_ARBITRATE_MODE];

/// Go `execMetricsAction`.
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
pub struct ExecMetricsAction {
    /// GC action count.
    pub gc: i64,
    /// Runtime-mem-stats refresh count.
    pub update_runtime_mem_stats: i64,
    /// Mem-state record successes/failures.
    pub record_mem_state: PairSuccessFail,
}

/// Go `execMetricsRisk`.
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
pub struct ExecMetricsRisk {
    /// Mem-risk events.
    pub mem: i64,
    /// OOM-risk events.
    pub oom: i64,
    /// OOM kills by priority.
    pub oom_kill: NumByPriority,
}

/// Go `execMetricsCancel`.
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
pub struct ExecMetricsCancel {
    /// Cancels under standard mode.
    pub standard_mode: i64,
    /// Cancels under priority mode by priority.
    pub priority_mode: NumByPriority,
    /// Wait-averse cancels.
    pub wait_averse: i64,
}

/// Go `execMetricsTask`.
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
pub struct ExecMetricsTask {
    /// All-mode success/fail counts.
    pub pair: PairSuccessFail,
    /// Priority-mode successes by priority.
    pub succ_by_priority: NumByPriority,
}

/// Go `awaitFreePoolExecMetrics`.
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
pub struct AwaitFreePoolExecMetrics {
    /// Grow successes/failures.
    pub pair: PairSuccessFail,
    /// Shrink events.
    pub shrink: i64,
    /// Forced shrink events.
    pub force_shrink: i64,
}

/// Execution metrics counter (Go `execMetricsCounter`).
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
pub struct ExecMetricsCounter {
    /// Task metrics.
    pub task: ExecMetricsTask,
    /// Cancel metrics.
    pub cancel: ExecMetricsCancel,
    /// Await-free pool metrics.
    pub await_free: AwaitFreePoolExecMetrics,
    /// Action metrics.
    pub action: ExecMetricsAction,
    /// Risk metrics.
    pub risk: ExecMetricsRisk,
    /// Digest-cache shrink count.
    pub shrink_digest: i64,
}

/// Last risk state (Go `LastRisk`).
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug, serde::Serialize, serde::Deserialize)]
pub struct LastRisk {
    /// Heap alloc at risk time.
    #[serde(rename = "heap")]
    pub heap_alloc: i64,
    /// Quota alloc at risk time.
    #[serde(rename = "quota")]
    pub quota_alloc: i64,
}

/// Runtime memory state (Go `RuntimeMemStateV1`).
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug, serde::Serialize, serde::Deserialize)]
pub struct RuntimeMemStateV1 {
    /// Format version.
    #[serde(rename = "version")]
    pub version: i64,
    /// Last risk state.
    #[serde(rename = "last-risk")]
    pub last_risk: LastRisk,
    /// Magnification ratio of heap-alloc/quota (per-mille).
    #[serde(rename = "magnif")]
    pub magnif: i64,
    /// Medium quota usage of root pools.
    #[serde(rename = "pool-medium-cap")]
    pub pool_medium_cap: i64,
}

/// Interface for recording runtime memory state (Go `RecordMemState`).
pub trait RecordMemState: Send + Sync {
    /// Load the last recorded state.
    fn load(&self) -> Result<Option<RuntimeMemStateV1>, String>;
    /// Store a state.
    fn store(&self, s: &RuntimeMemStateV1) -> Result<(), String>;
}

/// Log/GC hooks of the arbitrator (Go `MemArbitratorActions`). Log hooks
/// receive the message; structured fields are behavioral no-ops here (the
/// call counts are the contract the source tests check).
pub struct MemArbitratorActions {
    /// Info log hook.
    pub info: Box<dyn Fn(&str) + Send + Sync>,
    /// Warn log hook.
    pub warn: Box<dyn Fn(&str) + Send + Sync>,
    /// Error log hook.
    pub error: Box<dyn Fn(&str) + Send + Sync>,
    /// Refresh runtime memory statistics (should call
    /// `set_runtime_mem_stats`).
    pub update_runtime_mem_stats: Option<Box<dyn Fn() + Send + Sync>>,
    /// Garbage collection hook.
    pub gc: Option<Box<dyn Fn() + Send + Sync>>,
}

impl Default for MemArbitratorActions {
    fn default() -> Self {
        MemArbitratorActions {
            info: Box::new(|_| {}),
            warn: Box::new(|_| {}),
            error: Box::new(|_| {}),
            update_runtime_mem_stats: None,
            gc: None,
        }
    }
}

/// Runtime memory stats fed to the arbitrator (Go `memStats`).
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
pub struct MemStats {
    /// Heap objects occupied bytes.
    pub heap_alloc: i64,
    /// Heap in use (alloc + unused).
    pub heap_inuse: i64,
    /// Total freed bytes.
    pub total_free: i64,
    /// Off-heap memory.
    pub mem_off_heap: i64,
    /// End time of last GC (unix nanos).
    pub last_gc: i64,
}

#[derive(Default)]
pub(crate) struct MemProfile {
    pub(crate) start_utime_milli: i64,
    pub(crate) ts_align: i64,
    pub(crate) heap: i64,
    pub(crate) quota: i64,
    pub(crate) ratio: i64,
}

#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
pub(crate) struct BlockedState {
    pub(crate) allocated: i64,
    pub(crate) utime_sec: i64,
}

#[derive(Default)]
pub(crate) struct WrapTimeMaxval {
    pub(crate) ts_align: AtomicI64,
    pub(crate) max_val: AtomicI64,
}

#[derive(Default)]
pub(crate) struct WrapTimeSizeQuota {
    pub(crate) ts: AtomicI64,
    pub(crate) size: AtomicI64,
    pub(crate) quota: AtomicI64,
}

pub(crate) struct StatisticsTimedMapElement {
    pub(crate) ts_align: AtomicI64,
    pub(crate) slot: Vec<AtomicU32>,
    pub(crate) num: AtomicU64,
}

impl Default for StatisticsTimedMapElement {
    fn default() -> Self {
        let mut slot = Vec::with_capacity(DEF_SERVERLIMIT_MIN_UNIT_NUM as usize);
        for _ in 0..DEF_SERVERLIMIT_MIN_UNIT_NUM {
            slot.push(AtomicU32::new(0));
        }
        StatisticsTimedMapElement {
            ts_align: AtomicI64::new(0),
            slot,
            num: AtomicU64::new(0),
        }
    }
}

impl StatisticsTimedMapElement {
    pub(crate) fn reset(&self) {
        self.ts_align.store(0, SeqCst);
        for s in &self.slot {
            s.store(0, SeqCst);
        }
        self.num.store(0, SeqCst);
    }
}

pub(crate) struct DigestProfile {
    pub(crate) max_val: AtomicI64,
    pub(crate) timed_map: [RwLock<WrapTimeMaxval>; 2 + DEF_REDUNDANCY],
    pub(crate) last_fetch_utime_sec: AtomicI64,
}

impl Default for DigestProfile {
    fn default() -> Self {
        DigestProfile {
            max_val: AtomicI64::new(0),
            timed_map: [
                RwLock::new(WrapTimeMaxval::default()),
                RwLock::new(WrapTimeMaxval::default()),
                RwLock::new(WrapTimeMaxval::default()),
                RwLock::new(WrapTimeMaxval::default()),
            ],
            last_fetch_utime_sec: AtomicI64::new(0),
        }
    }
}

pub(crate) struct MapEntryWithMem {
    pub(crate) entries: MapUidEntry,
    pub(crate) num: i64,
}

impl MapEntryWithMem {
    pub(crate) fn new() -> MapEntryWithMem {
        MapEntryWithMem {
            entries: HashMap::new(),
            num: 0,
        }
    }
    pub(crate) fn delete(&mut self, entry: &Arc<RootPoolEntry>) {
        self.entries.remove(&entry.pool.uid());
        self.num -= 1;
    }
    /// Go `approxSize` (metrics surface for the server-crate wiring).
    #[allow(dead_code)]
    pub(crate) fn approx_size(&self) -> i64 {
        self.num
    }
    pub(crate) fn add(&mut self, entry: &Arc<RootPoolEntry>) {
        self.entries.insert(entry.pool.uid(), Arc::clone(entry));
        self.num += 1;
    }
}

/// Quota/tracked-heap usage of the await-free pool (Go `memPoolQuotaUsage`).
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
pub struct MemPoolQuotaUsage {
    /// Tracked heap in use.
    pub tracked_heap: i64,
    /// Quota in use.
    pub quota: i64,
}

/// Concurrent budget over a resource pool (Go `ConcurrentBudget`).
pub struct ConcurrentBudget {
    pub(crate) pool: Arc<ResourcePool>,
    pub(crate) mu: Mutex<()>,
    pub(crate) capacity: AtomicI64,
    pub(crate) last_used_time_sec: AtomicI64,
    /// Used quota.
    pub used: AtomicI64,
}

impl ConcurrentBudget {
    /// Creates a budget over a pool.
    pub fn new(pool: Arc<ResourcePool>) -> ConcurrentBudget {
        ConcurrentBudget {
            pool,
            mu: Mutex::new(()),
            capacity: AtomicI64::new(0),
            last_used_time_sec: AtomicI64::new(0),
            used: AtomicI64::new(0),
        }
    }

    pub(crate) fn set_last_used_time_sec(&self, t: i64) {
        self.last_used_time_sec.store(t, SeqCst);
    }
    pub(crate) fn approx_capacity(&self) -> i64 {
        self.capacity.load(SeqCst)
    }
    pub(crate) fn get_last_used_time_sec(&self) -> i64 {
        self.last_used_time_sec.load(SeqCst)
    }
    /// The budget capacity (test surface).
    pub fn capacity(&self) -> i64 {
        self.approx_capacity()
    }

    /// Go `Stop`: release all capacity and make the pool non-allocatable.
    pub fn stop(&self) -> i64 {
        let _g = self.mu.lock().unwrap();
        self.pool
            .set_out_of_capacity_action(Box::new(|_args| Err(err_arbitrate_fail())));
        let budget_cap = self.capacity.swap(0, SeqCst);
        self.used.store(0, SeqCst);
        if budget_cap > 0 {
            self.pool.release(budget_cap);
        }
        budget_cap
    }

    /// Go `Reserve`.
    pub fn reserve(&self, new_cap: i64) -> Result<(), PoolError> {
        let _g = self.mu.lock().unwrap();
        let cap = self.capacity.load(SeqCst);
        let extra = new_cap.max(self.used.load(SeqCst)).max(cap) - cap;
        self.pool.allocate(extra)?;
        self.capacity.store(cap + extra, SeqCst);
        Ok(())
    }

    /// Go `PullFromUpstream`: non-blocking pull when out of capacity.
    pub fn pull_from_upstream(&self) -> Result<(), PoolError> {
        let _g = self.mu.lock().unwrap();
        let delta = self.used.load(SeqCst) - self.capacity.load(SeqCst);
        if delta > 0 {
            let delta = self.pool.round_size(delta);
            self.pool.allocate(delta)?;
            self.capacity.fetch_add(delta, SeqCst);
        }
        Ok(())
    }

    /// Go `ConsumeQuota`: req > 0 allocs (pulling from upstream when over
    /// capacity), req <= 0 releases.
    pub fn consume_quota(&self, utime_sec: i64, req: i64) -> Result<(), PoolError> {
        if req > 0 {
            if self.get_last_used_time_sec() != utime_sec {
                self.set_last_used_time_sec(utime_sec);
            }
            if self.used.fetch_add(req, SeqCst) + req > self.approx_capacity() {
                self.pull_from_upstream()?;
            }
        } else {
            self.used.fetch_add(req, SeqCst);
        }
        Ok(())
    }
}

/// Concurrent budget with heap tracking (Go `TrackedConcurrentBudget`).
pub struct TrackedConcurrentBudget {
    /// The wrapped budget.
    pub budget: ConcurrentBudget,
    /// Tracked heap in use.
    pub heap_inuse: AtomicI64,
}

impl TrackedConcurrentBudget {
    /// Go `ReportHeapInuse`.
    pub fn report_heap_inuse(&self, req: i64) {
        self.heap_inuse.fetch_add(req, SeqCst);
    }
}

pub(crate) struct ExecMetricsAtomic {
    pub(crate) task_succ: AtomicI64,
    pub(crate) task_fail: AtomicI64,
    pub(crate) task_succ_by_priority: [AtomicI64; MAX_ARBITRATION_PRIORITY],
    pub(crate) cancel_standard: AtomicI64,
    pub(crate) cancel_priority: [AtomicI64; MAX_ARBITRATION_PRIORITY],
    pub(crate) cancel_wait_averse: AtomicI64,
    pub(crate) await_free_succ: AtomicI64,
    pub(crate) await_free_fail: AtomicI64,
    pub(crate) await_free_shrink: AtomicI64,
    pub(crate) await_free_force_shrink: AtomicI64,
    pub(crate) action_gc: AtomicI64,
    pub(crate) action_update_stats: AtomicI64,
    pub(crate) action_record_succ: AtomicI64,
    pub(crate) action_record_fail: AtomicI64,
    pub(crate) risk_mem: AtomicI64,
    pub(crate) risk_oom: AtomicI64,
    pub(crate) risk_oom_kill: [AtomicI64; MAX_ARBITRATION_PRIORITY],
    pub(crate) shrink_digest: AtomicI64,
}

impl Default for ExecMetricsAtomic {
    fn default() -> Self {
        ExecMetricsAtomic {
            task_succ: AtomicI64::new(0),
            task_fail: AtomicI64::new(0),
            task_succ_by_priority: Default::default(),
            cancel_standard: AtomicI64::new(0),
            cancel_priority: Default::default(),
            cancel_wait_averse: AtomicI64::new(0),
            await_free_succ: AtomicI64::new(0),
            await_free_fail: AtomicI64::new(0),
            await_free_shrink: AtomicI64::new(0),
            await_free_force_shrink: AtomicI64::new(0),
            action_gc: AtomicI64::new(0),
            action_update_stats: AtomicI64::new(0),
            action_record_succ: AtomicI64::new(0),
            action_record_fail: AtomicI64::new(0),
            risk_mem: AtomicI64::new(0),
            risk_oom: AtomicI64::new(0),
            risk_oom_kill: Default::default(),
            shrink_digest: AtomicI64::new(0),
        }
    }
}

impl ExecMetricsAtomic {
    pub(crate) fn snapshot(&self) -> ExecMetricsCounter {
        ExecMetricsCounter {
            task: ExecMetricsTask {
                pair: PairSuccessFail {
                    succ: self.task_succ.load(SeqCst),
                    fail: self.task_fail.load(SeqCst),
                },
                succ_by_priority: [
                    self.task_succ_by_priority[0].load(SeqCst),
                    self.task_succ_by_priority[1].load(SeqCst),
                    self.task_succ_by_priority[2].load(SeqCst),
                ],
            },
            cancel: ExecMetricsCancel {
                standard_mode: self.cancel_standard.load(SeqCst),
                priority_mode: [
                    self.cancel_priority[0].load(SeqCst),
                    self.cancel_priority[1].load(SeqCst),
                    self.cancel_priority[2].load(SeqCst),
                ],
                wait_averse: self.cancel_wait_averse.load(SeqCst),
            },
            await_free: AwaitFreePoolExecMetrics {
                pair: PairSuccessFail {
                    succ: self.await_free_succ.load(SeqCst),
                    fail: self.await_free_fail.load(SeqCst),
                },
                shrink: self.await_free_shrink.load(SeqCst),
                force_shrink: self.await_free_force_shrink.load(SeqCst),
            },
            action: ExecMetricsAction {
                gc: self.action_gc.load(SeqCst),
                update_runtime_mem_stats: self.action_update_stats.load(SeqCst),
                record_mem_state: PairSuccessFail {
                    succ: self.action_record_succ.load(SeqCst),
                    fail: self.action_record_fail.load(SeqCst),
                },
            },
            risk: ExecMetricsRisk {
                mem: self.risk_mem.load(SeqCst),
                oom: self.risk_oom.load(SeqCst),
                oom_kill: [
                    self.risk_oom_kill[0].load(SeqCst),
                    self.risk_oom_kill[1].load(SeqCst),
                    self.risk_oom_kill[2].load(SeqCst),
                ],
            },
            shrink_digest: self.shrink_digest.load(SeqCst),
        }
    }

    #[cfg_attr(not(test), allow(dead_code))] // Go tests' resetExecMetricsForTest
    pub(crate) fn reset(&self) {
        let d = ExecMetricsAtomic::default();
        // Overwrite every counter with zero.
        let _ = d; // fields reset individually below
        self.task_succ.store(0, SeqCst);
        self.task_fail.store(0, SeqCst);
        for a in &self.task_succ_by_priority {
            a.store(0, SeqCst);
        }
        self.cancel_standard.store(0, SeqCst);
        for a in &self.cancel_priority {
            a.store(0, SeqCst);
        }
        self.cancel_wait_averse.store(0, SeqCst);
        self.await_free_succ.store(0, SeqCst);
        self.await_free_fail.store(0, SeqCst);
        self.await_free_shrink.store(0, SeqCst);
        self.await_free_force_shrink.store(0, SeqCst);
        self.action_gc.store(0, SeqCst);
        self.action_update_stats.store(0, SeqCst);
        self.action_record_succ.store(0, SeqCst);
        self.action_record_fail.store(0, SeqCst);
        self.risk_mem.store(0, SeqCst);
        self.risk_oom.store(0, SeqCst);
        for a in &self.risk_oom_kill {
            a.store(0, SeqCst);
        }
        self.shrink_digest.store(0, SeqCst);
    }
}

pub(crate) struct Tasks {
    pub(crate) fifo_by_priority: [WrapList<Arc<RootPoolEntry>>; MAX_ARBITRATION_PRIORITY],
    pub(crate) fifo_tasks: WrapList<Arc<RootPoolEntry>>,
    pub(crate) fifo_wait_averse: WrapList<Arc<RootPoolEntry>>,
}

pub(crate) struct DigestShard {
    pub(crate) map: Mutex<HashMap<u64, Arc<DigestProfile>>>,
    pub(crate) num: AtomicI64,
}

pub(crate) struct ExecMuState {
    pub(crate) start_time: SystemTime,
    pub(crate) blocked_state: BlockedState,
    pub(crate) mode: ArbitratorWorkMode,
    pub(crate) await_free_kick_idx: u64,
}

pub(crate) struct MemRiskState {
    pub(crate) start_time: SystemTime,
    pub(crate) last_stats_start_time: SystemTime,
    pub(crate) last_heap_total_free: i64,
    pub(crate) min_heap_free_bps: i64,
    pub(crate) oom_risk: bool,
}

pub(crate) struct AwaitFreeState {
    pub(crate) pool: Arc<ResourcePool>,
    pub(crate) shards: Vec<TrackedConcurrentBudget>,
    pub(crate) size_mask: u64,
}

/// The mem-arbitrator (Go `MemArbitrator`).
pub struct MemArbitrator {
    pub(crate) exec_mu: Mutex<ExecMuState>,
    pub(crate) actions: Mutex<Arc<MemArbitratorActions>>,
    pub(crate) control_running: AtomicBool,
    pub(crate) control_mu: Mutex<Option<Receiver<()>>>,
    pub(crate) privileged_entry: Mutex<Option<Arc<RootPoolEntry>>>,
    pub(crate) under_kill: Mutex<MapEntryWithMem>,
    pub(crate) under_cancel: Mutex<MapEntryWithMem>,
    pub(crate) notifer: Notifer,
    pub(crate) cleanup_fifo: Mutex<WrapList<Arc<RootPoolEntry>>>,
    pub(crate) tasks: Mutex<Tasks>,
    pub(crate) waiting_alloc: AtomicI64,
    pub(crate) digest_shards: Mutex<Vec<Arc<DigestShard>>>,
    pub(crate) digest_shards_mask: AtomicU64,
    pub(crate) digest_num: AtomicI64,
    pub(crate) digest_limit: AtomicI64,
    pub(crate) entry_map: EntryMap,
    pub(crate) await_free: Mutex<Option<Arc<AwaitFreeState>>>,
    pub(crate) await_free_last_usage: Mutex<MemPoolQuotaUsage>,
    pub(crate) await_free_last_shrink_milli: AtomicI64,

    // heapController
    pub(crate) hc_mutex: Mutex<()>,
    pub(crate) hc_recorder_mu: Mutex<Box<dyn RecordMemState>>,
    pub(crate) hc_last_mem_state: Mutex<Option<RuntimeMemStateV1>>,
    pub(crate) hc_mem_risk: Mutex<MemRiskState>,
    pub(crate) hc_mem_risk_start_unix_milli: AtomicI64,
    pub(crate) hc_timed_mem_profile: Mutex<[MemProfile; 2]>,
    pub(crate) hc_last_gc_heap_alloc: AtomicI64,
    pub(crate) hc_last_gc_utime: AtomicI64,
    pub(crate) hc_heap_total_free: AtomicI64,
    pub(crate) hc_heap_alloc: AtomicI64,
    pub(crate) hc_heap_inuse: AtomicI64,
    pub(crate) hc_mem_off_heap: AtomicI64,
    pub(crate) hc_mem_inuse: AtomicI64,

    // poolAllocStats
    pub(crate) pool_alloc_stats: RwLock<PoolAllocProfile>,
    pub(crate) pool_alloc_timed_map: [RwLock<()>; 2 + DEF_REDUNDANCY],
    pub(crate) pool_alloc_timed_elems: [StatisticsTimedMapElement; 2 + DEF_REDUNDANCY],
    pub(crate) pool_alloc_medium_quota: AtomicI64,
    pub(crate) pool_alloc_last_update_milli: AtomicI64,

    // buffer
    pub(crate) buffer_size: AtomicI64,
    pub(crate) buffer_timed_map: [RwLock<()>; 2 + DEF_REDUNDANCY],
    pub(crate) buffer_timed_elems: [WrapTimeSizeQuota; 2 + DEF_REDUNDANCY],

    // mu (quota accounting)
    pub(crate) mu: Mutex<()>,
    pub(crate) mu_allocated: AtomicI64,
    pub(crate) mu_released: AtomicU64,
    pub(crate) mu_last_gc: AtomicU64,
    pub(crate) mu_limit: AtomicI64,
    pub(crate) mu_threshold_risk: AtomicI64,
    pub(crate) mu_threshold_oom_risk: AtomicI64,
    pub(crate) mu_soft_limit_mode: Mutex<SoftLimitMode>,
    pub(crate) mu_soft_limit_size: AtomicI64,
    pub(crate) mu_soft_specified_size: AtomicI64,
    pub(crate) mu_soft_specified_ratio: AtomicI64,

    pub(crate) exec_metrics: ExecMetricsAtomic,

    // avoidance
    pub(crate) avoid_size: AtomicI64,
    pub(crate) heap_tracked: AtomicI64,
    pub(crate) heap_tracked_last_update_milli: AtomicI64,
    pub(crate) mem_magnif_mu: Mutex<()>,
    pub(crate) mem_magnif_ratio: AtomicI64,

    pub(crate) tick_mu: Mutex<()>,
    pub(crate) tick_last_milli: AtomicI64,

    pub(crate) unix_time_sec: AtomicI64,
    pub(crate) root_pool_num: AtomicI64,
    pub(crate) mode: AtomicI32,
}

#[cfg(test)]
pub(crate) mod test_time {
    use std::sync::Mutex;
    use std::time::SystemTime;
    pub(crate) static MOCK_NOW: Mutex<Option<fn() -> SystemTime>> = Mutex::new(None);
    pub(crate) static MOCK_NOW_DYN: Mutex<Option<Box<dyn Fn() -> SystemTime + Send>>> =
        Mutex::new(None);
}

impl MemArbitrator {
    /// Go `NewMemArbitrator`.
    pub fn new(
        limit: i64,
        shard_num: u64,
        max_quota_shard_num: usize,
        min_quota_for_reclaim: i64,
        recorder: Box<dyn RecordMemState>,
    ) -> Arc<MemArbitrator> {
        let limit = if limit <= 0 { DEF_MAX_LIMIT } else { limit };
        let shard_num = next_pow2(shard_num);

        let mut tasks = Tasks {
            fifo_by_priority: [
                WrapList::default(),
                WrapList::default(),
                WrapList::default(),
            ],
            fifo_tasks: WrapList::default(),
            fifo_wait_averse: WrapList::default(),
        };
        tasks.fifo_tasks.init();
        for l in tasks.fifo_by_priority.iter_mut() {
            l.init();
        }
        tasks.fifo_wait_averse.init();

        let mut cleanup = WrapList::default();
        cleanup.init();

        let m = Arc::new(MemArbitrator {
            exec_mu: Mutex::new(ExecMuState {
                start_time: SystemTime::UNIX_EPOCH,
                blocked_state: BlockedState::default(),
                mode: ArbitratorWorkMode::Disable,
                await_free_kick_idx: 0,
            }),
            actions: Mutex::new(Arc::new(MemArbitratorActions::default())),
            control_running: AtomicBool::new(false),
            control_mu: Mutex::new(None),
            privileged_entry: Mutex::new(None),
            under_kill: Mutex::new(MapEntryWithMem::new()),
            under_cancel: Mutex::new(MapEntryWithMem::new()),
            notifer: Notifer::new(),
            cleanup_fifo: Mutex::new(cleanup),
            tasks: Mutex::new(tasks),
            waiting_alloc: AtomicI64::new(0),
            digest_shards: Mutex::new(Vec::new()),
            digest_shards_mask: AtomicU64::new(0),
            digest_num: AtomicI64::new(0),
            digest_limit: AtomicI64::new(0),
            entry_map: EntryMap::init(shard_num, max_quota_shard_num, min_quota_for_reclaim),
            await_free: Mutex::new(None),
            await_free_last_usage: Mutex::new(MemPoolQuotaUsage::default()),
            await_free_last_shrink_milli: AtomicI64::new(0),
            hc_mutex: Mutex::new(()),
            hc_recorder_mu: Mutex::new(recorder),
            hc_last_mem_state: Mutex::new(None),
            hc_mem_risk: Mutex::new(MemRiskState {
                start_time: SystemTime::UNIX_EPOCH,
                last_stats_start_time: SystemTime::UNIX_EPOCH,
                last_heap_total_free: 0,
                min_heap_free_bps: 0,
                oom_risk: false,
            }),
            hc_mem_risk_start_unix_milli: AtomicI64::new(0),
            hc_timed_mem_profile: Mutex::new([MemProfile::default(), MemProfile::default()]),
            hc_last_gc_heap_alloc: AtomicI64::new(0),
            hc_last_gc_utime: AtomicI64::new(0),
            hc_heap_total_free: AtomicI64::new(0),
            hc_heap_alloc: AtomicI64::new(0),
            hc_heap_inuse: AtomicI64::new(0),
            hc_mem_off_heap: AtomicI64::new(0),
            hc_mem_inuse: AtomicI64::new(0),
            pool_alloc_stats: RwLock::new(PoolAllocProfile::default()),
            pool_alloc_timed_map: [
                RwLock::new(()),
                RwLock::new(()),
                RwLock::new(()),
                RwLock::new(()),
            ],
            pool_alloc_timed_elems: [
                StatisticsTimedMapElement::default(),
                StatisticsTimedMapElement::default(),
                StatisticsTimedMapElement::default(),
                StatisticsTimedMapElement::default(),
            ],
            pool_alloc_medium_quota: AtomicI64::new(0),
            pool_alloc_last_update_milli: AtomicI64::new(0),
            buffer_size: AtomicI64::new(0),
            buffer_timed_map: [
                RwLock::new(()),
                RwLock::new(()),
                RwLock::new(()),
                RwLock::new(()),
            ],
            buffer_timed_elems: [
                WrapTimeSizeQuota::default(),
                WrapTimeSizeQuota::default(),
                WrapTimeSizeQuota::default(),
                WrapTimeSizeQuota::default(),
            ],
            mu: Mutex::new(()),
            mu_allocated: AtomicI64::new(0),
            mu_released: AtomicU64::new(0),
            mu_last_gc: AtomicU64::new(0),
            mu_limit: AtomicI64::new(0),
            mu_threshold_risk: AtomicI64::new(0),
            mu_threshold_oom_risk: AtomicI64::new(0),
            mu_soft_limit_mode: Mutex::new(SoftLimitMode::Disable),
            mu_soft_limit_size: AtomicI64::new(0),
            mu_soft_specified_size: AtomicI64::new(0),
            mu_soft_specified_ratio: AtomicI64::new(0),
            exec_metrics: ExecMetricsAtomic::default(),
            avoid_size: AtomicI64::new(0),
            heap_tracked: AtomicI64::new(0),
            heap_tracked_last_update_milli: AtomicI64::new(0),
            mem_magnif_mu: Mutex::new(()),
            mem_magnif_ratio: AtomicI64::new(0),
            tick_mu: Mutex::new(()),
            tick_last_milli: AtomicI64::new(0),
            unix_time_sec: AtomicI64::new(0),
            root_pool_num: AtomicI64::new(0),
            mode: AtomicI32::new(ArbitratorWorkMode::Disable as i32),
        });

        {
            let _g = m.mu.lock().unwrap();
            m.do_set_limit(limit);
        }
        m.reset_statistics();
        m.set_min_heap_free_bps(DEF_MIN_HEAP_FREE_BPS);
        {
            let recorder = m.hc_recorder_mu.lock().unwrap();
            if let Ok(Some(s)) = recorder.load() {
                *m.hc_last_mem_state.lock().unwrap() = Some(s);
                m.do_set_mem_magnif(s.magnif);
                m.pool_alloc_medium_quota.store(s.pool_medium_cap, SeqCst);
            }
        }
        m.reset_digest_profile_cache(shard_num);
        m
    }

    pub(crate) fn now(&self) -> SystemTime {
        #[cfg(test)]
        {
            if let Some(f) = *test_time::MOCK_NOW.lock().unwrap() {
                return f();
            }
        }
        SystemTime::now()
    }

    /// Go `innerTime` (test-mockable clock).
    pub(crate) fn inner_time(&self) -> SystemTime {
        #[cfg(test)]
        {
            if let Some(f) = test_time::MOCK_NOW_DYN.lock().unwrap().as_ref() {
                return f();
            }
            if let Some(f) = *test_time::MOCK_NOW.lock().unwrap() {
                return f();
            }
        }
        SystemTime::now()
    }

    // ----- task queues -----

    pub(crate) fn task_num_by_priority(&self, priority: ArbitrationPriority) -> i64 {
        self.tasks.lock().unwrap().fifo_by_priority[priority as usize].approx_size()
    }

    pub(crate) fn task_num_of_wait_averse(&self) -> i64 {
        self.tasks.lock().unwrap().fifo_wait_averse.approx_size()
    }

    fn remove_task_impl(tasks: &mut Tasks, entry: &Arc<RootPoolEntry>) -> bool {
        let mut st = entry.task_mu.lock().unwrap();
        if st.fifo.valid() {
            tasks.fifo_tasks.remove(st.fifo);
            st.fifo.reset();
            let prio = st.fifo_priority.unwrap();
            tasks.fifo_by_priority[prio as usize].remove(st.fifo_by_priority);
            st.fifo_by_priority.reset();
            if st.fifo_wait_averse.valid() {
                tasks.fifo_wait_averse.remove(st.fifo_wait_averse);
                st.fifo_wait_averse.reset();
            }
            return true;
        }
        false
    }

    /// Go `removeTask`.
    pub(crate) fn remove_task(&self, entry: &Arc<RootPoolEntry>) -> bool {
        let mut tasks = self.tasks.lock().unwrap();
        Self::remove_task_impl(&mut tasks, entry)
    }

    /// Go `addTask`.
    pub(crate) fn add_task(&self, entry: &Arc<RootPoolEntry>) {
        let mut tasks = self.tasks.lock().unwrap();
        let priority = entry.mem_priority();
        let mut st = entry.task_mu.lock().unwrap();
        st.fifo_priority = Some(priority);
        st.fifo_by_priority =
            tasks.fifo_by_priority[priority as usize].push_back(Arc::clone(entry));
        if entry.ctx.wait_averse.load(SeqCst) {
            st.fifo_wait_averse = tasks.fifo_wait_averse.push_back(Arc::clone(entry));
        }
        st.fifo = tasks.fifo_tasks.push_back(Arc::clone(entry));
    }

    /// Go `frontTaskEntry`.
    pub(crate) fn front_task_entry(&self) -> Option<Arc<RootPoolEntry>> {
        self.tasks.lock().unwrap().fifo_tasks.front()
    }

    /// Go `extractFirstTaskEntry`.
    pub(crate) fn extract_first_task_entry(&self) -> Option<Arc<RootPoolEntry>> {
        let tasks = self.tasks.lock().unwrap();
        if let Some(pe) = self.privileged_entry.lock().unwrap().as_ref() {
            if pe.task_mu.lock().unwrap().fifo.valid() {
                let fifo = pe.task_mu.lock().unwrap().fifo;
                let mut tasks = tasks;
                tasks.fifo_tasks.move_to_front(fifo);
                return Some(Arc::clone(pe));
            }
        }
        if self.exec_mu.lock().unwrap().mode == ArbitratorWorkMode::Priority {
            for priority in PRIORITIES.iter().rev() {
                if let Some(e) = tasks.fifo_by_priority[*priority as usize].front() {
                    return Some(e);
                }
            }
            None
        } else {
            tasks.fifo_tasks.front()
        }
    }

    // ----- blocking allocation -----

    /// Go `blockingAllocate` (root pool mutex held by the caller's pool
    /// op); `apply_cap` grants capacity inside the caller's lock.
    pub(crate) fn blocking_allocate(
        &self,
        entry: &Arc<RootPoolEntry>,
        requested_bytes: i64,
        apply_cap: &mut dyn FnMut(i64),
    ) -> ArbitrateResult {
        if entry.exec_state() == EntryExecState::Idle {
            return ArbitrateResult::Fail;
        }
        if entry.ctx.canceled.load(SeqCst) {
            self.exec_metrics.task_fail.fetch_add(1, SeqCst);
            return ArbitrateResult::Fail;
        }
        self.prepare_alloc(entry, requested_bytes);
        self.wait_alloc_with(entry, apply_cap)
    }

    /// Go `prepareAlloc`.
    pub(crate) fn prepare_alloc(&self, entry: &Arc<RootPoolEntry>, requested_bytes: i64) {
        entry.request.quota.store(requested_bytes, SeqCst);
        self.waiting_alloc.fetch_add(requested_bytes, SeqCst);
        self.add_task(entry);
        self.notifer.weak_wake();
    }

    /// Go `waitAlloc` (test/direct form: applies granted capacity to the
    /// entry's pool outside any pool lock).
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn wait_alloc(&self, entry: &Arc<RootPoolEntry>) -> ArbitrateResult {
        let pool = Arc::clone(&entry.pool);
        self.wait_alloc_with(entry, &mut |d| pool.force_add_cap_unlocked(d))
    }

    pub(crate) fn wait_alloc_with(
        &self,
        entry: &Arc<RootPoolEntry>,
        apply_cap: &mut dyn FnMut(i64),
    ) -> ArbitrateResult {
        let cancel_rx = entry.ctx.cancel_ch.lock().unwrap().clone();
        let never_rx = never::<()>();

        let res;
        let result_rx = &entry.request.result_rx;
        let cancel = match &cancel_rx {
            Some(c) => &c.rx,
            None => &never_rx,
        };

        let mut sel = Select::new();
        let op_result = sel.recv(result_rx);
        let _op_cancel = sel.recv(cancel);
        let op = sel.select();
        let idx = op.index();
        if idx == op_result {
            match op.recv(result_rx) {
                Ok((r, delta)) => {
                    apply_cap(delta);
                    res = r;
                    if res == ArbitrateResult::Fail {
                        self.exec_metrics.task_fail.fetch_add(1, SeqCst);
                    } else {
                        self.exec_metrics.task_succ.fetch_add(1, SeqCst);
                    }
                }
                Err(_) => {
                    res = ArbitrateResult::Fail;
                    self.exec_metrics.task_fail.fetch_add(1, SeqCst);
                }
            }
        } else {
            // canceled: 1. by session; 2. by the arbitrate-helper.
            let _ = op.recv(cancel); // consume the select operation
            res = ArbitrateResult::Fail;
            self.exec_metrics.task_fail.fetch_add(1, SeqCst);
            entry.ctx.canceled.store(true, SeqCst);
            {
                let _g = entry.request.task_mu.lock().unwrap();
                if !self.remove_task(entry) {
                    // Wind-up raced ahead: drain and still honor the grant.
                    if let Ok((_r, delta)) = entry.request.result_rx.recv() {
                        apply_cap(delta);
                    }
                }
            }
        }

        self.waiting_alloc
            .fetch_add(-entry.request.quota.load(SeqCst), SeqCst);
        res
    }

    // ----- accounting -----

    pub(crate) fn do_set_limit(&self, limit: i64) {
        self.mu_limit.store(limit, SeqCst);
        self.mu_threshold_oom_risk
            .store((limit as f64 * DEF_OOM_RISK_RATIO) as i64, SeqCst);
        self.mu_threshold_risk
            .store((limit as f64 * DEF_MEM_RISK_RATIO) as i64, SeqCst);
        self.do_adjust_soft_limit();
    }

    pub(crate) fn do_adjust_soft_limit(&self) {
        let limit = self.limit();
        let mode = *self.mu_soft_limit_mode.lock().unwrap();
        let soft_limit = if mode == SoftLimitMode::Specified {
            let size = self.mu_soft_specified_size.load(SeqCst);
            if size > 0 {
                size.min(limit)
            } else {
                multi_ratio(limit, self.mu_soft_specified_ratio.load(SeqCst)).min(limit)
            }
        } else {
            self.oom_risk()
        };
        self.mu_soft_limit_size.store(soft_limit, SeqCst);
    }

    /// Go `SetSoftLimit`.
    pub fn set_soft_limit(&self, soft_limit: i64, soft_limit_ratio: f64, mode: SoftLimitMode) {
        let _g = self.mu.lock().unwrap();
        *self.mu_soft_limit_mode.lock().unwrap() = mode;
        if mode == SoftLimitMode::Specified {
            self.mu_soft_specified_size.store(soft_limit, SeqCst);
            self.mu_soft_specified_ratio
                .store(into_ratio(soft_limit_ratio), SeqCst);
        }
        self.do_adjust_soft_limit();
    }

    pub(crate) fn soft_limit_i(&self) -> i64 {
        self.mu_soft_limit_size.load(SeqCst)
    }

    /// Go `SoftLimit`.
    pub fn soft_limit(&self) -> u64 {
        self.soft_limit_i() as u64
    }

    /// Go `SetLimit`: returns whether the limit changed.
    pub fn set_limit(&self, x: u64) -> bool {
        // Go: `min(int64(x), DefMaxLimit)` — the SIGNED cast happens first,
        // so u64::MAX becomes -1 and is rejected.
        let new_limit = (x as i64).min(DEF_MAX_LIMIT);
        if new_limit <= 0 {
            return false;
        }
        let mut changed = false;
        let mut need_wake = false;
        {
            let _g = self.mu.lock().unwrap();
            let limit = self.limit();
            if new_limit != limit {
                changed = true;
                need_wake = new_limit > limit;
                self.do_set_limit(new_limit);
            }
        }
        if changed {
            self.reset_statistics();
        }
        if need_wake {
            self.weak_wake();
        }
        changed
    }

    /// Go `resetStatistics`.
    pub(crate) fn reset_statistics(&self) {
        let mut stats = self.pool_alloc_stats.write().unwrap();
        *stats = self.pool_alloc_profile();
        for (lock, elem) in self
            .pool_alloc_timed_map
            .iter()
            .zip(self.pool_alloc_timed_elems.iter())
        {
            let _g = lock.write().unwrap();
            elem.reset();
        }
    }

    /// Go `PoolAllocProfile` (the method).
    pub fn pool_alloc_profile(&self) -> PoolAllocProfile {
        let limit = self.limit();
        PoolAllocProfile {
            small_pool_limit: (limit / DEF_SERVERLIMIT_SMALL_LIMIT_NUM).max(1),
            pool_alloc_unit: (limit / DEF_SERVERLIMIT_MIN_UNIT_NUM).max(1),
            max_pool_alloc_unit: (limit / DEF_SERVERLIMIT_MAX_UNIT_NUM).max(1),
        }
    }

    pub(crate) fn alloc(&self, x: i64) {
        let _g = self.mu.lock().unwrap();
        self.do_alloc(x);
    }

    pub(crate) fn do_alloc(&self, x: i64) {
        self.mu_allocated.fetch_add(x, SeqCst);
    }

    pub(crate) fn release(&self, x: i64) {
        if x <= 0 {
            return;
        }
        self.alloc(-x);
    }

    /// Go `allocated` / `Allocated`.
    pub fn allocated(&self) -> i64 {
        self.mu_allocated.load(SeqCst)
    }

    /// Go `OutOfControl`.
    pub fn out_of_control(&self) -> i64 {
        self.avoid_size.load(SeqCst)
    }

    /// Go `WaitingAllocSize`.
    pub fn waiting_alloc_size(&self) -> i64 {
        self.waiting_alloc.load(SeqCst)
    }

    /// Go `TaskNum`.
    pub fn task_num(&self) -> i64 {
        self.tasks.lock().unwrap().fifo_tasks.approx_size()
    }

    /// Go `RootPoolNum`.
    pub fn root_pool_num(&self) -> i64 {
        self.root_pool_num.load(SeqCst)
    }

    pub(crate) fn limit(&self) -> i64 {
        self.mu_limit.load(SeqCst)
    }

    /// Go `Limit`.
    pub fn limit_u64(&self) -> u64 {
        self.limit() as u64
    }

    /// Go `available` (the global metrics reporter reads it).
    pub fn available(&self) -> i64 {
        self.heap_available().min(self.quota_available())
    }

    pub(crate) fn heap_available(&self) -> i64 {
        self.limit() - self.reserved_buffer() - self.hc_heap_alloc.load(SeqCst)
    }

    pub(crate) fn quota_available(&self) -> i64 {
        self.limit() - self.reserved_buffer() - self.out_of_control() - self.allocated()
    }

    pub(crate) fn oom_risk(&self) -> i64 {
        self.mu_threshold_oom_risk.load(SeqCst)
    }

    pub(crate) fn mem_risk(&self) -> i64 {
        self.mu_threshold_risk.load(SeqCst)
    }

    /// Go `reservedBuffer`.
    pub fn reserved_buffer(&self) -> i64 {
        if self.exec_mu.lock().unwrap().mode == ArbitratorWorkMode::Priority {
            self.buffer_size.load(SeqCst)
        } else {
            0
        }
    }

    pub(crate) fn set_buffer_size(&self, v: i64) {
        self.buffer_size.store(v, SeqCst);
    }

    pub(crate) fn last_blocked_at(&self) -> (i64, i64) {
        let b = self.exec_mu.lock().unwrap().blocked_state;
        (b.allocated, b.utime_sec)
    }

    pub(crate) fn update_blocked_at(&self) {
        let b = BlockedState {
            allocated: self.allocated(),
            utime_sec: self.approx_unix_time_sec(),
        };
        self.exec_mu.lock().unwrap().blocked_state = b;
    }

    // ----- work mode -----

    /// Go `SetWorkMode`.
    pub fn set_work_mode(&self, new_mode: ArbitratorWorkMode) -> ArbitratorWorkMode {
        let ori = ArbitratorWorkMode::from_i32(self.mode.swap(new_mode as i32, SeqCst));
        self.wake();
        ori
    }

    /// Go `WorkMode`.
    pub fn work_mode(&self) -> ArbitratorWorkMode {
        ArbitratorWorkMode::from_i32(self.mode.load(SeqCst))
    }

    /// Go `ExecMetrics` (snapshot copy).
    pub fn exec_metrics(&self) -> ExecMetricsCounter {
        self.exec_metrics.snapshot()
    }

    pub(crate) fn weak_wake(&self) {
        self.notifer.weak_wake();
    }

    pub(crate) fn wake(&self) {
        self.notifer.wake();
    }

    pub(crate) fn set_min_heap_free_bps(&self, sz: i64) {
        self.hc_mem_risk.lock().unwrap().min_heap_free_bps = sz;
    }

    pub(crate) fn min_heap_free_bps(&self) -> i64 {
        self.hc_mem_risk.lock().unwrap().min_heap_free_bps
    }

    pub(crate) fn set_unix_time_sec(&self, s: i64) {
        self.unix_time_sec.store(s, SeqCst);
    }

    pub(crate) fn approx_unix_time_sec(&self) -> i64 {
        self.unix_time_sec.load(SeqCst)
    }

    /// Go `AtMemRisk`.
    pub fn at_mem_risk(&self) -> bool {
        self.hc_mem_risk_start_unix_milli.load(SeqCst) != 0
    }

    /// Go `AtOOMRisk`.
    pub fn at_oom_risk(&self) -> bool {
        self.hc_mem_risk.lock().unwrap().oom_risk
    }

    pub(crate) fn enter_oom_risk(&self) {
        self.hc_mem_risk.lock().unwrap().oom_risk = true;
        self.exec_metrics.risk_oom.fetch_add(1, SeqCst);
    }

    pub(crate) fn set_mem_safe(&self) {
        self.hc_mem_risk_start_unix_milli.store(0, SeqCst);
        self.hc_mem_risk.lock().unwrap().oom_risk = false;
    }

    pub(crate) fn do_set_mem_magnif(&self, ratio: i64) {
        self.mem_magnif_ratio.store(ratio, SeqCst);
    }

    pub(crate) fn mem_magnif(&self) -> i64 {
        self.mem_magnif_ratio.load(SeqCst)
    }
}

pub(crate) const DEF_MAX_UNUSED_BLOCKS_LOCAL: i64 = super::pool::DEF_MAX_UNUSED_BLOCKS;

pub(crate) fn mem_hang_risk(
    free_speed_bps: i64,
    min_heap_free_speed_bps: i64,
    now: SystemTime,
    start_time: SystemTime,
) -> bool {
    free_speed_bps < min_heap_free_speed_bps
        || now
            .duration_since(start_time)
            .map(|d| d > DEF_HEAP_RECLAIM_CHECK_MAX_DURATION)
            .unwrap_or(false)
}

#[cfg(test)]
mod tests;
