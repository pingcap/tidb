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

impl MemArbitrator {
    // ----- root pool lifecycle -----

    pub(crate) fn get_root_pool_entry(&self, uid: u64) -> Option<Arc<RootPoolEntry>> {
        self.entry_map
            .status_shard(uid)
            .entries
            .read()
            .unwrap()
            .get(&uid)
            .cloned()
    }

    /// Go `FindRootPool`.
    pub fn find_root_pool(&self, uid: u64) -> RootPoolWrap {
        RootPoolWrap {
            entry: self.get_root_pool_entry(uid),
        }
    }

    /// Go `EmplaceRootPool`.
    pub fn emplace_root_pool(&self, uid: u64) -> Result<RootPoolWrap, PoolError> {
        if let Some(e) = self.get_root_pool_entry(uid) {
            return Ok(RootPoolWrap { entry: Some(e) });
        }
        let pool = ResourcePool::new_raw(
            &format!("root-{uid}"),
            uid,
            DEF_MAX_LIMIT,
            1,
            DEF_MAX_UNUSED_BLOCKS_LOCAL,
        );
        let entry = self.add_root_pool(pool)?;
        Ok(RootPoolWrap { entry: Some(entry) })
    }

    /// Go `addRootPool`.
    pub(crate) fn add_root_pool(
        &self,
        pool: Arc<ResourcePool>,
    ) -> Result<Arc<RootPoolEntry>, PoolError> {
        let b = pool.capacity();
        if b != 0 {
            return Err(PoolError(format!(
                "{}: has {} bytes budget left",
                pool.name(),
                b
            )));
        }
        if let Some(up) = pool.upstream() {
            return Err(PoolError(format!(
                "{}: already started with pool {}",
                pool.name(),
                up.name()
            )));
        }
        if pool.reserved() != 0 {
            return Err(PoolError(format!(
                "{}: has {} reserved budget left",
                pool.name(),
                pool.reserved()
            )));
        }

        let name = pool.name().to_string();
        let (entry, ok) = self.entry_map.emplace(pool);
        if !ok {
            return Err(PoolError(format!("{name}: already exists")));
        }
        self.root_pool_num.fetch_add(1, SeqCst);
        Ok(entry)
    }

    /// Go `RestartEntryByContext`.
    pub fn restart_entry_by_context(
        self: &Arc<Self>,
        p: RootPoolWrap,
        ctx: Option<Arc<ArbitrationContext>>,
    ) -> bool {
        let Some(entry) = p.entry else {
            return false;
        };
        let _sg = entry.state_mu.mutex.lock().unwrap();
        if entry.state_mu.stop.load(SeqCst) || entry.exec_state() != EntryExecState::Idle {
            return false;
        }

        // Go locks the pool mutex across the context install; the Rust pool
        // has interior locking, so equivalent exclusion comes from the
        // state_mu lock held here.
        match &ctx {
            Some(c) => {
                if c.wait_averse {
                    entry.ctx.prefer_privilege.store(false, SeqCst);
                    entry
                        .ctx
                        .mem_priority
                        .store(ArbitrationPriority::High as i32, SeqCst);
                } else {
                    entry.ctx.prefer_privilege.store(c.prefer_privilege, SeqCst);
                    entry.ctx.mem_priority.store(c.mem_priority as i32, SeqCst);
                }
                *entry.ctx.cancel_ch.lock().unwrap() = c.cancel_ch.clone();
                entry.ctx.wait_averse.store(c.wait_averse, SeqCst);
            }
            None => {
                *entry.ctx.cancel_ch.lock().unwrap() = None;
                entry.ctx.wait_averse.store(false, SeqCst);
                entry
                    .ctx
                    .mem_priority
                    .store(ArbitrationPriority::Medium as i32, SeqCst);
                entry.ctx.prefer_privilege.store(false, SeqCst);
            }
        }
        entry.ctx.canceled.store(false, SeqCst);
        *entry.ctx.ptr.lock().unwrap() = ctx;

        {
            let mut cache = self.entry_map.context_cache.lock().unwrap();
            if cache.insert(entry.pool.uid(), Arc::clone(&entry)).is_none() {
                self.entry_map.context_cache_num.fetch_add(1, SeqCst);
            }
        }

        let m = Arc::clone(self);
        let entry_cb = Arc::clone(&entry);
        entry.pool.set_out_of_capacity_action(Box::new(
            move |s: OutOfCapacityActionArgs<'_, '_>| {
                let request = s.request;
                let mut pool_ctx = s.pool;
                if m.blocking_allocate(&entry_cb, request, &mut |d| pool_ctx.force_add_cap(d))
                    != ArbitrateResult::Ok
                {
                    return Err(err_arbitrate_fail());
                }
                Ok(())
            },
        ));
        entry.pool.clear_stopped();
        entry.set_exec_state(EntryExecState::Running);
        true
    }

    /// Go `ResetRootPoolByID`.
    pub fn reset_root_pool_by_id(&self, uid: u64, max_mem_consumed: i64, tune: bool) {
        let Some(entry) = self.get_root_pool_entry(uid) else {
            return;
        };
        self.try_to_update_buffer(max_mem_consumed, self.approx_unix_time_sec());
        if tune {
            let small = self.pool_alloc_stats.read().unwrap().small_pool_limit;
            if max_mem_consumed > small {
                self.record_mem_consumed(max_mem_consumed, self.approx_unix_time_sec());
            }
        }
        self.reset_root_pool_entry(&entry);
        self.wake();
    }

    /// Go `resetRootPoolEntry`.
    pub(crate) fn reset_root_pool_entry(&self, entry: &Arc<RootPoolEntry>) -> bool {
        {
            let _g = entry.state_mu.mutex.lock().unwrap();
            if entry.exec_state() == EntryExecState::Idle {
                return false;
            }
            entry.set_exec_state(EntryExecState::Idle);
        }
        let released = entry.pool.stop();
        if released > 0 {
            entry.state_mu.quota_to_reclaim.fetch_add(released, SeqCst);
        }
        self.cleanup_fifo
            .lock()
            .unwrap()
            .push_back(Arc::clone(entry));
        true
    }

    /// Go `RemoveRootPoolByID`.
    pub fn remove_root_pool_by_id(&self, uid: u64) -> bool {
        let Some(entry) = self.get_root_pool_entry(uid) else {
            return false;
        };
        if self.remove_root_pool_entry(&entry) {
            if let Some(ctx) = entry.load_ctx() {
                if let Some(h) = ctx.helper() {
                    h.finish();
                }
            }
            self.wake();
            return true;
        }
        false
    }

    /// Go `removeRootPoolEntry`.
    pub(crate) fn remove_root_pool_entry(&self, entry: &Arc<RootPoolEntry>) -> bool {
        {
            let _g = entry.state_mu.mutex.lock().unwrap();
            if entry.state_mu.stop.swap(true, SeqCst) {
                return false;
            }
            if entry.exec_state() != EntryExecState::Idle {
                entry.set_exec_state(EntryExecState::Idle);
            }
        }
        self.cleanup_fifo
            .lock()
            .unwrap()
            .push_back(Arc::clone(entry));
        entry.pool.stop();
        true
    }

    fn warn_kill_cancel(&self, entry: &Arc<RootPoolEntry>, ctx_reclaim: i64, reason: &str) {
        let actions = self.actions.lock().unwrap().clone();
        (actions.warn)(&format!(
            "{reason}: uid={} name={} reclaimed={}",
            entry.pool.uid(),
            entry.pool.name(),
            ctx_reclaim,
        ));
    }

    // ----- under kill / cancel bookkeeping (arbitrator-only) -----

    pub(crate) fn add_under_kill(
        &self,
        entry: &Arc<RootPoolEntry>,
        memory_used: i64,
        start_time: SystemTime,
    ) {
        let mut st = entry.arbitrator_mu.lock().unwrap();
        if !st.under_kill.start {
            self.under_kill.lock().unwrap().add(entry);
            st.under_kill = EntryKillCancelCtx {
                start: true,
                start_time,
                reclaim: memory_used,
                fail: false,
            };
        }
    }

    pub(crate) fn add_under_cancel(
        &self,
        entry: &Arc<RootPoolEntry>,
        memory_used: i64,
        start_time: SystemTime,
    ) {
        let mut st = entry.arbitrator_mu.lock().unwrap();
        if !st.under_cancel.start {
            self.under_cancel.lock().unwrap().add(entry);
            st.under_cancel = EntryKillCancelCtx {
                start: true,
                start_time,
                reclaim: memory_used,
                fail: false,
            };
        }
    }

    pub(crate) fn delete_under_kill(&self, entry: &Arc<RootPoolEntry>) {
        let reclaim = {
            let mut st = entry.arbitrator_mu.lock().unwrap();
            if !st.under_kill.start {
                return;
            }
            st.under_kill.start = false;
            st.under_kill.reclaim
        };
        self.under_kill.lock().unwrap().delete(entry);
        self.warn_kill_cancel(entry, reclaim, "Finish to `KILL` root pool");
    }

    pub(crate) fn delete_under_cancel(&self, entry: &Arc<RootPoolEntry>) {
        let mut st = entry.arbitrator_mu.lock().unwrap();
        if st.under_cancel.start {
            st.under_cancel.start = false;
            drop(st);
            self.under_cancel.lock().unwrap().delete(entry);
        }
    }
}

pub(crate) const DEF_MAX_UNUSED_BLOCKS_LOCAL: i64 = super::pool::DEF_MAX_UNUSED_BLOCKS;

impl MemArbitrator {
    // ----- arbitration core -----

    /// Go `allocateFromArbitrator`.
    pub(crate) fn allocate_from_arbitrator(&self, remain_bytes: i64) -> (bool, i64) {
        let mut reclaimed = 0i64;
        let mut ok = false;
        {
            let _g = self.mu.lock().unwrap();
            let available = self.quota_available();
            if remain_bytes <= available {
                self.do_alloc(remain_bytes);
                reclaimed += remain_bytes;
                ok = true;
            } else if available > 0 {
                self.do_alloc(available);
                reclaimed += available;
            }
        }
        (ok, reclaimed)
    }

    /// Go `doReclaimMemByPriority`.
    pub(crate) fn do_reclaim_mem_by_priority(
        &self,
        target: &Arc<RootPoolEntry>,
        remain_bytes: i64,
    ) {
        let mut under_reclaim = 0i64;

        // Check pool entries already under cancel.
        if self.under_cancel.lock().unwrap().num > 0 {
            let now = self.inner_time();
            let entries: Vec<Arc<RootPoolEntry>> = self
                .under_cancel
                .lock()
                .unwrap()
                .entries
                .values()
                .cloned()
                .collect();
            for entry in entries {
                let mut st = entry.arbitrator_mu.lock().unwrap();
                if st.under_cancel.fail {
                    continue;
                }
                let deadline = st.under_cancel.start_time + DEF_KILL_CANCEL_CHECK_TIMEOUT;
                if now >= deadline {
                    let actions = self.actions.lock().unwrap().clone();
                    (actions.warn)(&format!(
                        "Failed to `CANCEL` root pool due to timeout: uid={} name={}",
                        entry.pool.uid(),
                        entry.pool.name()
                    ));
                    st.under_cancel.fail = true;
                    continue;
                }
                under_reclaim += st.under_cancel.reclaim;
            }
        }

        if under_reclaim >= remain_bytes {
            return;
        }

        let target_prio = target.mem_priority();
        for prio in PRIORITIES {
            if prio >= target_prio {
                break;
            }
            let mut pos = self.entry_map.max_quota_shard_index;
            while pos > self.entry_map.min_quota_shard_index_to_check {
                pos -= 1;
                let entries: Vec<(u64, Arc<RootPoolEntry>)> = self.entry_map.quota_shards
                    [prio as usize][pos]
                    .lock()
                    .unwrap()
                    .entries
                    .iter()
                    .map(|(k, v)| (*k, Arc::clone(v)))
                    .collect();
                for (_uid, entry) in entries {
                    if entry.arbitrator_mu.lock().unwrap().under_cancel.start || entry.not_running()
                    {
                        continue;
                    }
                    let ctx = entry.load_ctx();
                    if ctx_available(&ctx) {
                        let ctx = ctx.unwrap();
                        self.exec_metrics.cancel_priority[prio as usize].fetch_add(1, SeqCst);
                        ctx.stop(ArbitratorStopReason::PriorityCancel);
                        if self.remove_task(&entry) {
                            entry.wind_up(0, ArbitrateResult::Fail);
                        }
                        let quota = entry.arbitrator_mu.lock().unwrap().quota;
                        self.add_under_cancel(&entry, quota, self.inner_time());
                        under_reclaim += quota;
                        if under_reclaim >= remain_bytes {
                            return;
                        }
                    }
                }
            }
        }
    }

    /// Go `allocateFromPrivilegedBudget`.
    pub(crate) fn allocate_from_privileged_budget(
        &self,
        target: &Arc<RootPoolEntry>,
        remain_bytes: i64,
    ) -> (bool, i64) {
        let mut ok = false;
        {
            let mut pe = self.privileged_entry.lock().unwrap();
            match pe.as_ref() {
                Some(e) if Arc::ptr_eq(e, target) => ok = true,
                None if target.ctx.prefer_privilege.load(SeqCst)
                    && target.enter_exec_privileged() =>
                {
                    *pe = Some(Arc::clone(target));
                    ok = true;
                }
                _ => {}
            }
        }
        if !ok {
            return (false, 0);
        }
        self.alloc(remain_bytes);
        (true, remain_bytes)
    }

    /// Go `ableToGC`.
    pub(crate) fn able_to_gc(&self) -> bool {
        let small = self.pool_alloc_stats.read().unwrap().small_pool_limit;
        self.mu_released
            .load(SeqCst)
            .wrapping_sub(self.mu_last_gc.load(SeqCst))
            >= small as u64
    }

    pub(crate) fn gc(&self) {
        self.mu_last_gc.store(self.mu_released.load(SeqCst), SeqCst);
        let actions = self.actions.lock().unwrap().clone();
        if let Some(gc) = &actions.gc {
            gc();
        }
        self.exec_metrics.action_gc.fetch_add(1, SeqCst);
    }

    pub(crate) fn reclaim_heap(&self) {
        self.gc();
        self.refresh_runtime_mem_stats();
    }

    /// Go `tryRuntimeGC`.
    pub(crate) fn try_runtime_gc(&self) -> bool {
        if self.able_to_gc() {
            self.update_tracked_heap_stats();
            self.reclaim_heap();
            return true;
        }
        false
    }

    /// Go `arbitrate`.
    pub(crate) fn arbitrate(&self, target: &Arc<RootPoolEntry>) -> (bool, i64) {
        let mut reclaimed_bytes = 0i64;
        let mut remain_bytes = target.request.quota.load(SeqCst);

        let mut only_privileged_budget = false;
        while remain_bytes > self.heap_available() {
            if !self.try_runtime_gc() {
                only_privileged_budget = true;
                break;
            }
        }

        {
            let mut ok = false;
            if self.exec_mu.lock().unwrap().mode == ArbitratorWorkMode::Priority {
                let (o, reclaimed) = self.allocate_from_privileged_budget(target, remain_bytes);
                ok = o;
                reclaimed_bytes += reclaimed;
                remain_bytes -= reclaimed;
            }
            if ok {
                return (true, reclaimed_bytes);
            } else if only_privileged_budget {
                return (false, reclaimed_bytes);
            }
        }

        loop {
            let (ok, reclaimed) = self.allocate_from_arbitrator(remain_bytes);
            reclaimed_bytes += reclaimed;
            remain_bytes -= reclaimed;
            if ok {
                return (true, reclaimed_bytes);
            }
            if !self.try_runtime_gc() {
                break;
            }
        }

        (false, reclaimed_bytes)
    }

    // ----- task execution -----

    /// Go `doCancelPendingTasks`.
    pub(crate) fn do_cancel_pending_tasks(
        &self,
        prio: ArbitrationPriority,
        wait_averse: bool,
    ) -> i64 {
        let mut cnt = 0i64;
        let reason = if wait_averse {
            ArbitratorStopReason::WaitAverseCancel
        } else {
            ArbitratorStopReason::StandardCancel
        };

        loop {
            let mut batch: Vec<Arc<RootPoolEntry>> = Vec::with_capacity(64);
            {
                let mut tasks = self.tasks.lock().unwrap();
                loop {
                    let front = if wait_averse {
                        tasks.fifo_wait_averse.front()
                    } else {
                        tasks.fifo_by_priority[prio as usize].front()
                    };
                    let Some(entry) = front else { break };
                    if Self::remove_task_impl(&mut tasks, &entry) {
                        batch.push(entry);
                    }
                    if batch.len() == 64 {
                        break;
                    }
                }
            }

            let size = batch.len();
            for entry in &batch {
                let ctx = entry.load_ctx();
                if ctx_available(&ctx) {
                    ctx.unwrap().stop(reason);
                }
                entry.wind_up(0, ArbitrateResult::Fail);
            }
            cnt += size as i64;
            if size != 64 {
                break;
            }
        }
        cnt
    }

    /// Go `doExecuteFirstTask`.
    pub(crate) fn do_execute_first_task(&self) -> bool {
        if self.tasks.lock().unwrap().fifo_tasks.approx_empty() {
            return false;
        }
        let Some(entry) = self.extract_first_task_entry() else {
            return false;
        };

        if entry.arbitrator_mu.lock().unwrap().destroyed {
            if self.remove_task(&entry) {
                entry.wind_up(0, ArbitrateResult::Fail);
            }
            return true;
        }

        let mut exec = false;
        {
            let _g = entry.request.task_mu.lock().unwrap();
            let (ok, reclaimed_bytes) = self.arbitrate(&entry);
            if ok {
                exec = true;
                if self.remove_task(&entry) {
                    if self.exec_mu.lock().unwrap().mode == ArbitratorWorkMode::Priority {
                        // Go reads `taskMu.fifoByPriority.priority`, which
                        // survives task removal.
                        let prio = entry
                            .task_mu
                            .lock()
                            .unwrap()
                            .fifo_priority
                            .unwrap_or(ArbitrationPriority::Medium);
                        self.exec_metrics.task_succ_by_priority[prio as usize].fetch_add(1, SeqCst);
                    }
                    self.entry_map.add_quota(&entry, reclaimed_bytes);
                    entry.wind_up(reclaimed_bytes, ArbitrateResult::Ok);
                } else {
                    self.release(reclaimed_bytes);
                }
            } else {
                self.release(reclaimed_bytes);
                self.update_blocked_at();
                self.do_reclaim_by_work_mode(&entry, reclaimed_bytes);
            }
        }
        exec
    }

    /// Go `doReclaimNonBlockingTasks`.
    pub(crate) fn do_reclaim_non_blocking_tasks(&self) {
        if self.exec_mu.lock().unwrap().mode == ArbitratorWorkMode::Standard {
            for prio in PRIORITIES {
                if self.task_num_by_priority(prio) != 0 {
                    let n = self.do_cancel_pending_tasks(prio, false);
                    self.exec_metrics.cancel_standard.fetch_add(n, SeqCst);
                }
            }
        } else if self.task_num_of_wait_averse() != 0 {
            let n = self.do_cancel_pending_tasks(ArbitrationPriority::High, true);
            self.exec_metrics.cancel_wait_averse.fetch_add(n, SeqCst);
        }
    }

    /// Go `doReclaimByWorkMode`.
    pub(crate) fn do_reclaim_by_work_mode(&self, entry: &Arc<RootPoolEntry>, reclaimed: i64) {
        let wait_averse = entry.ctx.wait_averse.load(SeqCst);
        self.do_reclaim_non_blocking_tasks();
        if wait_averse {
            return;
        }
        if self.exec_mu.lock().unwrap().mode == ArbitratorWorkMode::Priority {
            self.do_reclaim_mem_by_priority(entry, entry.request.quota.load(SeqCst) - reclaimed);
        }
    }

    /// Go `doExecuteCleanupTasks`.
    pub(crate) fn do_execute_cleanup_tasks(&self) {
        loop {
            let entry = self.cleanup_fifo.lock().unwrap().pop_front();
            let Some(entry) = entry else { break };

            {
                let mut pe = self.privileged_entry.lock().unwrap();
                if pe.as_ref().is_some_and(|e| Arc::ptr_eq(e, &entry)) {
                    *pe = None;
                }
            }
            self.delete_under_cancel(&entry);
            self.delete_under_kill(&entry);

            if !entry.state_mu.stop.load(SeqCst) {
                let to_release = entry.state_mu.quota_to_reclaim.swap(0, SeqCst);
                if to_release > 0 {
                    self.release(to_release);
                    self.mu_released.fetch_add(to_release as u64, SeqCst);
                    self.entry_map.add_quota(&entry, -to_release);
                }
            } else {
                let destroyed = entry.arbitrator_mu.lock().unwrap().destroyed;
                if !destroyed {
                    let quota = entry.arbitrator_mu.lock().unwrap().quota;
                    if quota > 0 {
                        self.release(quota);
                        self.mu_released.fetch_add(quota as u64, SeqCst);
                    }
                    self.entry_map.delete(&entry);
                    self.root_pool_num.fetch_add(-1, SeqCst);
                    entry.arbitrator_mu.lock().unwrap().destroyed = true;
                }
                if self.remove_task(&entry) {
                    entry.wind_up(0, ArbitrateResult::Fail);
                }
            }
        }
    }

    /// Go `implicitRun` (disable mode: satisfy every subscription).
    pub(crate) fn implicit_run(&self) {
        if self.tasks.lock().unwrap().fifo_tasks.approx_empty() {
            return;
        }
        loop {
            let Some(entry) = self.front_task_entry() else {
                break;
            };
            if entry.arbitrator_mu.lock().unwrap().destroyed {
                if self.remove_task(&entry) {
                    entry.wind_up(0, ArbitrateResult::Fail);
                }
                continue;
            }
            {
                let _g = entry.request.task_mu.lock().unwrap();
                let quota = entry.request.quota.load(SeqCst);
                if self.remove_task(&entry) {
                    self.alloc(quota);
                    self.entry_map.add_quota(&entry, quota);
                    entry.wind_up(quota, ArbitrateResult::Ok);
                }
            }
        }
    }

    /// Go `runOneRound`: -1 disabled, -2 mem unsafe, >= 0 executed tasks.
    pub fn run_one_round(&self) -> i32 {
        {
            let mut exec = self.exec_mu.lock().unwrap();
            exec.start_time = self.now();
            let t = exec
                .start_time
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            if t != self.approx_unix_time_sec() {
                self.set_unix_time_sec(t);
            }
            let mode = self.work_mode();
            if exec.mode != mode {
                exec.mode = mode;
                if mode == ArbitratorWorkMode::Disable {
                    drop(exec);
                    self.set_mem_safe();
                    self.exec_mu.lock().unwrap().blocked_state = BlockedState::default();
                }
            }
        }

        if !self.cleanup_fifo.lock().unwrap().approx_empty() {
            self.do_execute_cleanup_tasks();
        }

        if self.exec_mu.lock().unwrap().mode == ArbitratorWorkMode::Disable {
            self.implicit_run();
            return -1;
        }

        if !self.handle_mem_issues() {
            return -2;
        }

        let mut task_exec_num = 0;
        while self.do_execute_first_task() {
            task_exec_num += 1;
        }
        task_exec_num
    }

    /// Go `asyncRun`.
    pub(crate) fn async_run(self: &Arc<Self>, duration: Duration) -> bool {
        if self.control_running.load(SeqCst) {
            return false;
        }
        self.control_running.store(true, SeqCst);
        let (finish_tx, finish_rx) = bounded::<()>(0);
        *self.control_mu.lock().unwrap() = Some(finish_rx);

        let m = Arc::clone(self);
        std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(duration);
            while m.control_running.load(SeqCst) {
                let mut sel = Select::new();
                let op_tick = sel.recv(&ticker);
                let _op_notif = sel.recv(&m.notifer.rx);
                let op = sel.select();
                if op.index() == op_tick {
                    let _ = op.recv(&ticker);
                    m.weak_wake();
                } else {
                    let _ = op.recv(&m.notifer.rx);
                    m.notifer.clear();
                    m.run_one_round();
                }
            }
            drop(finish_tx); // close(finishCh)
        });
        true
    }

    /// Go `AutoRun`.
    pub fn auto_run(
        self: &Arc<Self>,
        actions: MemArbitratorActions,
        await_free_pool_alloc_align_size: i64,
        await_free_pool_shard_num: i64,
        task_tick_dur: Duration,
    ) -> bool {
        if self.control_running.load(SeqCst) {
            return false;
        }
        *self.actions.lock().unwrap() = Arc::new(actions);
        self.refresh_runtime_mem_stats();
        self.init_await_free_pool(await_free_pool_alloc_align_size, await_free_pool_shard_num);
        self.async_run(task_tick_dur)
    }

    /// Go `stop` (unexported in the source; the global cleanup path needs
    /// it).
    pub fn stop(&self) -> bool {
        if !self.control_running.load(SeqCst) {
            return false;
        }
        let finish = { self.control_mu.lock().unwrap().clone() };
        self.control_running.store(false, SeqCst);
        self.wake();
        if let Some(rx) = finish {
            let _ = rx.recv(); // wait for close
        }
        self.run_one_round();
        true
    }
}

impl MemArbitrator {
    // ----- digest profile cache -----

    pub(crate) fn reset_digest_profile_cache(&self, shard_num: u64) {
        let mut shards = Vec::with_capacity(shard_num as usize);
        for _ in 0..shard_num {
            shards.push(Arc::new(DigestShard {
                map: Mutex::new(HashMap::new()),
                num: AtomicI64::new(0),
            }));
        }
        *self.digest_shards.lock().unwrap() = shards;
        self.digest_shards_mask.store(shard_num - 1, SeqCst);
        self.digest_num.store(0, SeqCst);
        self.digest_limit
            .store(DEF_MAX_DIGEST_PROFILE_CACHE_LIMIT, SeqCst);
    }

    /// Go `SetDigestProfileCacheLimit`.
    pub fn set_digest_profile_cache_limit(&self, limit: i64) {
        self.digest_limit.store(limit.clamp(0, DEF_MAX), SeqCst);
    }

    fn digest_shard(&self, digest_id: u64) -> Arc<DigestShard> {
        let shards = self.digest_shards.lock().unwrap();
        let mask = self.digest_shards_mask.load(SeqCst);
        Arc::clone(&shards[(digest_id & mask) as usize])
    }

    /// Go `GetDigestProfileCache`.
    pub fn get_digest_profile_cache(&self, digest_id: u64, utime_sec: i64) -> Option<i64> {
        let shard = self.digest_shard(digest_id);
        let pf = shard.map.lock().unwrap().get(&digest_id).cloned()?;
        if utime_sec > pf.last_fetch_utime_sec.load(SeqCst) {
            pf.last_fetch_utime_sec.store(utime_sec, SeqCst);
        }
        Some(pf.max_val.load(SeqCst))
    }

    /// Go `UpdateDigestProfileCache`.
    pub fn update_digest_profile_cache(&self, digest_id: u64, mem_consumed: i64, utime_sec: i64) {
        let shard = self.digest_shard(digest_id);
        let pf = {
            let mut map = shard.map.lock().unwrap();
            match map.get(&digest_id) {
                Some(p) => Arc::clone(p),
                None => {
                    let p = Arc::new(DigestProfile::default());
                    map.insert(digest_id, Arc::clone(&p));
                    shard.num.fetch_add(1, SeqCst);
                    self.digest_num.fetch_add(1, SeqCst);
                    p
                }
            }
        };

        const MAX_NUM: i64 = (2 + DEF_REDUNDANCY) as i64;
        const MAX_DUR: i64 = MAX_NUM - DEF_REDUNDANCY as i64;

        let ts_align = utime_sec / DEF_UPDATE_BUFFER_TIME_ALIGN_SEC;
        let tar_idx = (ts_align % MAX_NUM) as usize;

        {
            let ori_ts = pf.timed_map[tar_idx].read().unwrap().ts_align.load(SeqCst);
            if ori_ts < ts_align && ori_ts != 0 {
                // Exclusive lock on purpose (Go `Lock()`): serialize the
                // reset against concurrent RLock readers.
                #[allow(clippy::readonly_write_lock)]
                let tar = pf.timed_map[tar_idx].write().unwrap();
                let ori_ts = tar.ts_align.load(SeqCst);
                if ori_ts < ts_align && ori_ts != 0 {
                    tar.ts_align.store(0, SeqCst);
                    tar.max_val.store(0, SeqCst);
                }
            }
        }

        let mut clean_next = false;
        {
            let tar = pf.timed_map[tar_idx].read().unwrap();
            let mut update_size = false;

            if tar.ts_align.load(SeqCst) == 0
                && tar
                    .ts_align
                    .compare_exchange(0, ts_align, SeqCst, SeqCst)
                    .is_ok()
            {
                clean_next = true;
            }

            loop {
                let old_val = tar.max_val.load(SeqCst);
                if old_val >= mem_consumed {
                    break;
                }
                if tar
                    .max_val
                    .compare_exchange(old_val, mem_consumed, SeqCst, SeqCst)
                    .is_ok()
                {
                    update_size = true;
                    break;
                }
            }

            if update_size {
                let mut maxv = tar.max_val.load(SeqCst);
                for i in 0..MAX_DUR {
                    let d_idx = ((MAX_NUM + ts_align - i) % MAX_NUM) as usize;
                    let d = pf.timed_map[d_idx].read().unwrap();
                    let ts = d.ts_align.load(SeqCst);
                    if ts > ts_align - MAX_DUR && ts <= ts_align {
                        maxv = maxv.max(d.max_val.load(SeqCst));
                    }
                }
                pf.max_val.store(maxv, SeqCst); // force update
            }
        }

        if utime_sec > pf.last_fetch_utime_sec.load(SeqCst) {
            pf.last_fetch_utime_sec.store(utime_sec, SeqCst);
        }

        if clean_next {
            let d_idx = (((ts_align + 1) % MAX_NUM) as usize).min(3);
            // Exclusive on purpose (Go `Lock()`), as above.
            #[allow(clippy::readonly_write_lock)]
            let d = pf.timed_map[d_idx].write().unwrap();
            let ts = d.ts_align.load(SeqCst);
            if ts < ts_align + 1 && ts != 0 {
                d.ts_align.store(0, SeqCst);
                d.max_val.store(0, SeqCst);
            }
        }
    }

    /// Go `shrinkDigestProfile`.
    pub(crate) fn shrink_digest_profile(&self, utime_sec: i64, limit: i64, shrink_to: i64) -> i64 {
        if self.digest_num.load(SeqCst) <= limit {
            return 0;
        }
        self.exec_metrics.shrink_digest.fetch_add(1, SeqCst);

        let mut shrinked = 0i64;
        let mut val_map = [0i64; DEF_POOL_QUOTA_SHARDS];
        let small_pool_limit = self.pool_alloc_stats.read().unwrap().small_pool_limit;

        let shards: Vec<Arc<DigestShard>> = self.digest_shards.lock().unwrap().clone();
        for d in &shards {
            if d.num.load(SeqCst) == 0 {
                continue;
            }
            let mut dn = 0i64;
            let snapshot: Vec<(u64, Arc<DigestProfile>)> = d
                .map
                .lock()
                .unwrap()
                .iter()
                .map(|(k, v)| (*k, Arc::clone(v)))
                .collect();
            for (k, pf) in snapshot {
                let max_val = pf.max_val.load(SeqCst);
                let timeout = if max_val > small_pool_limit {
                    DEF_DIGEST_PROFILE_MEM_TIMEOUT_SEC
                } else {
                    DEF_DIGEST_PROFILE_SMALL_MEM_TIMEOUT_SEC
                };
                if utime_sec - pf.last_fetch_utime_sec.load(SeqCst) > timeout
                    && d.map.lock().unwrap().remove(&k).is_some()
                {
                    d.num.fetch_add(-1, SeqCst);
                    dn += 1;
                    continue;
                }
                let index = get_quota_shard(max_val, DEF_POOL_QUOTA_SHARDS);
                val_map[index] += 1;
            }
            self.digest_num.fetch_add(-dn, SeqCst);
            shrinked += dn;
        }

        let mut to_shrink = self.digest_num.load(SeqCst) - shrink_to;
        if to_shrink <= 0 {
            return shrinked;
        }

        let mut shrink_max_size = DEF_MAX_LIMIT;
        {
            let mut n = 0i64;
            for (i, v) in val_map.iter().enumerate() {
                n += *v;
                if n >= to_shrink {
                    shrink_max_size = BASE_QUOTA_UNIT * (1 << i);
                    break;
                }
            }
        }

        for d in &shards {
            if d.num.load(SeqCst) == 0 {
                continue;
            }
            let mut dn = 0i64;
            let snapshot: Vec<(u64, Arc<DigestProfile>)> = d
                .map
                .lock()
                .unwrap()
                .iter()
                .map(|(k, v)| (*k, Arc::clone(v)))
                .collect();
            for (k, pf) in snapshot {
                if pf.max_val.load(SeqCst) < shrink_max_size
                    && d.map.lock().unwrap().remove(&k).is_some()
                {
                    d.num.fetch_add(-1, SeqCst);
                    to_shrink -= 1;
                    dn += 1;
                }
                if to_shrink <= 0 {
                    break;
                }
            }
            self.digest_num.fetch_add(-dn, SeqCst);
            shrinked += dn;
            if to_shrink <= 0 {
                break;
            }
        }
        shrinked
    }

    // ----- pool alloc statistics & buffer -----

    /// Go `recordMemConsumed`.
    pub(crate) fn record_mem_consumed(&self, mem_consumed: i64, utime_sec: i64) {
        let stats = self.pool_alloc_stats.read().unwrap();
        const MAX_NUM: i64 = (2 + DEF_REDUNDANCY) as i64;
        let ts_align = utime_sec / DEF_UPDATE_MEM_CONSUMED_TIME_ALIGN_SEC;
        let tar_idx = (ts_align % MAX_NUM) as usize;
        let tar = &self.pool_alloc_timed_elems[tar_idx];

        {
            let ori_ts = tar.ts_align.load(SeqCst);
            if ori_ts < ts_align && ori_ts != 0 {
                let _g = self.pool_alloc_timed_map[tar_idx].write().unwrap();
                let ori_ts = tar.ts_align.load(SeqCst);
                if ori_ts < ts_align && ori_ts != 0 {
                    tar.reset();
                }
            }
        }

        let mut clean_next = false;
        {
            let _g = self.pool_alloc_timed_map[tar_idx].read().unwrap();
            if tar.ts_align.load(SeqCst) == 0
                && tar
                    .ts_align
                    .compare_exchange(0, ts_align, SeqCst, SeqCst)
                    .is_ok()
            {
                clean_next = true;
            }
            let pos = (mem_consumed / stats.pool_alloc_unit).min(DEF_SERVERLIMIT_MIN_UNIT_NUM - 1);
            tar.slot[pos as usize].fetch_add(1, SeqCst);
            tar.num.fetch_add(1, SeqCst);
        }

        if clean_next {
            let d_idx = (((ts_align + 1) % MAX_NUM) as usize).min(3);
            let d = &self.pool_alloc_timed_elems[d_idx];
            let _g = self.pool_alloc_timed_map[d_idx].write().unwrap();
            let v = d.ts_align.load(SeqCst);
            if v < ts_align + 1 && v != 0 {
                d.reset();
            }
        }
    }

    /// Go `tryToUpdateBuffer`.
    pub(crate) fn try_to_update_buffer(&self, mem_consumed: i64, utime_sec: i64) {
        const MAX_NUM: i64 = (2 + DEF_REDUNDANCY) as i64;
        const MAX_DUR: i64 = MAX_NUM - DEF_REDUNDANCY as i64;

        let ts_align = utime_sec / DEF_UPDATE_BUFFER_TIME_ALIGN_SEC;
        let tar_idx = (ts_align % MAX_NUM) as usize;
        let tar = &self.buffer_timed_elems[tar_idx];

        {
            let ori_ts = tar.ts.load(SeqCst);
            if ori_ts < ts_align && ori_ts != 0 {
                let _g = self.buffer_timed_map[tar_idx].write().unwrap();
                let ori_ts = tar.ts.load(SeqCst);
                if ori_ts < ts_align && ori_ts != 0 {
                    tar.ts.store(0, SeqCst);
                    tar.size.store(0, SeqCst);
                    tar.quota.store(0, SeqCst);
                }
            }
        }

        let mut clean_next = false;
        {
            let _g = self.buffer_timed_map[tar_idx].read().unwrap();
            let mut update_size = false;
            let mut mem_consumed = mem_consumed;

            if tar.ts.load(SeqCst) == 0
                && tar.ts.compare_exchange(0, ts_align, SeqCst, SeqCst).is_ok()
            {
                clean_next = true;
            }

            loop {
                let old_val = tar.size.load(SeqCst);
                if old_val >= mem_consumed {
                    break;
                }
                if tar
                    .size
                    .compare_exchange(old_val, mem_consumed, SeqCst, SeqCst)
                    .is_ok()
                {
                    update_size = true;
                    break;
                }
            }

            if update_size {
                for i in 0..MAX_DUR {
                    let d_idx = ((MAX_NUM + ts_align - i) % MAX_NUM) as usize;
                    let d = &self.buffer_timed_elems[d_idx];
                    let ts = d.ts.load(SeqCst);
                    if ts > ts_align - MAX_DUR && ts <= ts_align {
                        mem_consumed = mem_consumed.max(d.size.load(SeqCst));
                    }
                }
                if self.buffer_size.load(SeqCst) != mem_consumed {
                    self.set_buffer_size(mem_consumed);
                }
            }
        }

        if clean_next {
            let d_idx = (((ts_align + 1) % MAX_NUM) as usize).min(3);
            let d = &self.buffer_timed_elems[d_idx];
            let _g = self.buffer_timed_map[d_idx].write().unwrap();
            let v = d.ts.load(SeqCst);
            if v < ts_align + 1 && v != 0 {
                d.ts.store(0, SeqCst);
                d.size.store(0, SeqCst);
                d.quota.store(0, SeqCst);
            }
        }
    }
}

impl MemArbitrator {
    // ----- runtime mem stats & avoidance -----

    pub(crate) fn refresh_runtime_mem_stats(&self) {
        let actions = self.actions.lock().unwrap().clone();
        if let Some(f) = &actions.update_runtime_mem_stats {
            f();
        }
        self.exec_metrics.action_update_stats.fetch_add(1, SeqCst);
    }

    pub(crate) fn try_set_runtime_mem_stats(&self, s: MemStats) -> bool {
        if let Ok(g) = self.hc_mutex.try_lock() {
            self.do_set_runtime_mem_stats(s);
            drop(g);
            true
        } else {
            false
        }
    }

    /// Go `setRuntimeMemStats` — feed fresh runtime stats.
    pub fn set_runtime_mem_stats(&self, s: MemStats) {
        let _g = self.hc_mutex.lock().unwrap();
        self.do_set_runtime_mem_stats(s);
    }

    fn do_set_runtime_mem_stats(&self, s: MemStats) {
        self.hc_heap_alloc.store(s.heap_alloc, SeqCst);
        self.hc_heap_inuse.store(s.heap_inuse, SeqCst);
        self.hc_heap_total_free.store(s.total_free, SeqCst);
        self.hc_mem_off_heap.store(s.mem_off_heap, SeqCst);
        self.hc_mem_inuse
            .store(s.mem_off_heap + s.heap_inuse, SeqCst);

        if s.last_gc > self.hc_last_gc_utime.load(SeqCst) {
            self.hc_last_gc_heap_alloc.store(s.heap_alloc, SeqCst);
            self.hc_last_gc_utime.store(s.last_gc, SeqCst);
        }
        self.update_avoid_size();
    }

    /// Go `updateAvoidSize`.
    pub(crate) fn update_avoid_size(&self) {
        let mut capacity = self.soft_limit_i();
        if *self.mu_soft_limit_mode.lock().unwrap() == SoftLimitMode::Auto {
            let ratio = self.mem_magnif();
            if ratio != 0 {
                let new_cap = calc_ratio(self.limit(), ratio);
                capacity = capacity.min(new_cap);
            }
        }
        let avoid_size = 0i64
            .max(
                self.hc_heap_alloc.load(SeqCst) + self.hc_mem_off_heap.load(SeqCst)
                    - self.heap_tracked.load(SeqCst),
            )
            .max(self.limit() - capacity);
        self.avoid_size.store(avoid_size, SeqCst);

        let af = self.await_free.lock().unwrap().clone();
        let Some(af) = af else { return };
        let delta = self.allocated() - self.limit() + avoid_size;
        if delta > 0 && af.pool.allocated() > 0 {
            let mut reclaimed = 0i64;
            let kick_idx = self.exec_mu.lock().unwrap().await_free_kick_idx;
            let mut final_idx = kick_idx;
            for i in 0..af.shards.len() {
                let idx = (kick_idx + i as u64 + 1) & af.size_mask;
                let b = &af.shards[idx as usize];
                if b.budget.approx_capacity() > 0 {
                    if let Ok(_g) = b.budget.mu.try_lock() {
                        let x = (delta - reclaimed).min(b.budget.capacity.load(SeqCst));
                        b.budget.capacity.fetch_add(-x, SeqCst);
                        reclaimed += x;
                    }
                }
                if reclaimed >= delta {
                    final_idx = idx;
                    break;
                }
            }
            if reclaimed >= delta {
                self.exec_mu.lock().unwrap().await_free_kick_idx = final_idx;
            }
            let mut pool_released = 0i64;
            if reclaimed > 0 {
                pool_released = af.pool.release_pop_budget(reclaimed);
            }
            if pool_released > 0 {
                self.release(pool_released);
                self.exec_metrics
                    .await_free_force_shrink
                    .fetch_add(1, SeqCst);
                self.mu_released.fetch_add(pool_released as u64, SeqCst);
            }
        }
    }

    /// Go `HandleRuntimeStats`.
    pub fn handle_runtime_stats(&self, s: MemStats) {
        self.try_shrink_await_free_pool(DEF_POOL_RESERVED_QUOTA, now_unix_milli());
        self.try_update_tracked_mem_stats(now_unix_milli());
        self.try_set_runtime_mem_stats(s);
        self.execute_tick(now_unix_milli());
        self.weak_wake();
    }

    pub(crate) fn try_update_tracked_mem_stats(&self, utime_milli: i64) -> bool {
        if self.heap_tracked_last_update_milli.load(SeqCst) + DEF_TRACK_MEM_STATS_DUR_MILLI
            <= utime_milli
        {
            self.update_tracked_heap_stats();
            return true;
        }
        false
    }

    /// Go `updateTrackedHeapStats`.
    pub(crate) fn update_tracked_heap_stats(&self) {
        let mut total_tracked_heap = 0i64;
        if self.entry_map.context_cache_num.load(SeqCst) != 0 {
            let mut max_mem_used = 0i64;
            let entries: Vec<Arc<RootPoolEntry>> = self
                .entry_map
                .context_cache
                .lock()
                .unwrap()
                .values()
                .cloned()
                .collect();
            for e in entries {
                if e.not_running() {
                    continue;
                }
                let ctx = e.load_ctx();
                if ctx_available(&ctx) {
                    if let Some(h) = ctx.unwrap().helper() {
                        let mem_used = h.heap_inuse();
                        if mem_used > 0 {
                            total_tracked_heap += mem_used;
                            max_mem_used = max_mem_used.max(mem_used);
                        }
                    }
                }
            }
            if self.buffer_size.load(SeqCst) < max_mem_used {
                self.try_to_update_buffer(max_mem_used, self.approx_unix_time_sec());
            }
        }
        total_tracked_heap += self.await_free_pool_used().tracked_heap;
        self.heap_tracked.store(total_tracked_heap, SeqCst);
        self.heap_tracked_last_update_milli
            .store(now_unix_milli(), SeqCst);
    }

    // ----- await-free pool -----

    /// Go `initAwaitFreePool`.
    pub fn init_await_free_pool(self: &Arc<Self>, alloc_align_size: i64, shard_num: i64) {
        let alloc_align_size = if alloc_align_size <= 0 {
            DEF_AWAIT_FREE_POOL_ALLOC_ALIGN_SIZE
        } else {
            alloc_align_size
        };
        let p = ResourcePool::new_raw("awaitfree-pool", 0, DEF_MAX_LIMIT, alloc_align_size, 0);

        let m = Arc::clone(self);
        let pool_cb = Arc::clone(&p);
        p.set_out_of_capacity_action(Box::new(move |mut s: OutOfCapacityActionArgs<'_, '_>| {
            if m.hc_heap_alloc.load(SeqCst) > m.oom_risk() - s.request
                || m.allocated() > m.limit() - m.out_of_control() - s.request
            {
                m.update_blocked_at();
                m.exec_metrics.await_free_fail.fetch_add(1, SeqCst);
                return Err(err_arbitrate_fail());
            }
            m.alloc(s.request);
            s.pool.force_add_cap(s.request);
            let _ = &pool_cb;
            m.exec_metrics.await_free_succ.fetch_add(1, SeqCst);
            Ok(())
        }));

        let cnt = next_pow2(shard_num as u64);
        let mut shards = Vec::with_capacity(cnt as usize);
        for _ in 0..cnt {
            shards.push(TrackedConcurrentBudget {
                budget: ConcurrentBudget::new(Arc::clone(&p)),
                heap_inuse: AtomicI64::new(0),
            });
        }
        *self.await_free.lock().unwrap() = Some(Arc::new(AwaitFreeState {
            pool: p,
            shards,
            size_mask: cnt - 1,
        }));
    }

    pub(crate) fn await_free_state(&self) -> Option<Arc<AwaitFreeState>> {
        self.await_free.lock().unwrap().clone()
    }

    /// Go `GetAwaitFreeBudgets`: runs `f` with the budget shard for `uid`.
    pub fn with_await_free_budget<R>(
        &self,
        uid: u64,
        f: impl FnOnce(&TrackedConcurrentBudget) -> R,
    ) -> Option<R> {
        let af = self.await_free_state()?;
        let index = shard_index_by_uid(uid, af.size_mask);
        Some(f(&af.shards[index as usize]))
    }

    /// Go `ConsumeQuotaFromAwaitFreePool`.
    pub fn consume_quota_from_await_free_pool(&self, uid: u64, req: i64) -> bool {
        let utime = self.approx_unix_time_sec();
        self.with_await_free_budget(uid, |b| b.budget.consume_quota(utime, req).is_ok())
            .unwrap_or(false)
    }

    /// Go `ReportHeapInuseToAwaitFreePool`.
    pub fn report_heap_inuse_to_await_free_pool(&self, uid: u64, req: i64) {
        self.with_await_free_budget(uid, |b| b.report_heap_inuse(req));
    }

    /// Go `awaitFreePoolCap` (metrics surface).
    pub fn await_free_pool_cap(&self) -> i64 {
        match self.await_free_state() {
            Some(af) => af.pool.capacity(),
            None => 0,
        }
    }

    /// Go `awaitFreePoolUsed`.
    pub(crate) fn await_free_pool_used(&self) -> MemPoolQuotaUsage {
        let mut res = MemPoolQuotaUsage::default();
        if let Some(af) = self.await_free_state() {
            for b in &af.shards {
                let d = b.budget.used.load(SeqCst);
                if d > 0 {
                    res.quota += d;
                }
                let d = b.heap_inuse.load(SeqCst);
                if d > 0 {
                    res.tracked_heap += d;
                }
            }
        }
        *self.await_free_last_usage.lock().unwrap() = res;
        res
    }

    /// Go `approxAwaitFreePoolUsed` (metrics surface).
    pub fn approx_await_free_pool_used(&self) -> MemPoolQuotaUsage {
        *self.await_free_last_usage.lock().unwrap()
    }

    pub(crate) fn try_shrink_await_free_pool(&self, min_remain: i64, utime_milli: i64) -> bool {
        if self.await_free_last_shrink_milli.load(SeqCst) + DEF_AWAIT_FREE_POOL_SHRINK_DUR_MILLI
            <= utime_milli
        {
            self.shrink_await_free_pool(min_remain, utime_milli);
            return true;
        }
        false
    }

    /// Go `shrinkAwaitFreePool`.
    pub(crate) fn shrink_await_free_pool(&self, min_remain: i64, utime_milli: i64) {
        let Some(af) = self.await_free_state() else {
            return;
        };
        if af.pool.allocated() <= 0 {
            return;
        }

        let mut reclaimed = 0i64;
        let align = af.pool.alloc_align_size();

        for b in &af.shards {
            let used = b.budget.used.load(SeqCst);
            if used > 0 {
                if b.budget.approx_capacity() - (used + min_remain) >= align {
                    if let Ok(_g) = b.budget.mu.try_lock() {
                        let used = b.budget.used.load(SeqCst);
                        if used > 0 {
                            let to_reclaim = b.budget.capacity.load(SeqCst) - (used + min_remain);
                            if to_reclaim >= align {
                                b.budget.capacity.fetch_add(-to_reclaim, SeqCst);
                                reclaimed += to_reclaim;
                            }
                        }
                    }
                }
            } else if b.budget.approx_capacity() > 0
                && b.budget.get_last_used_time_sec() * KILO + DEF_AWAIT_FREE_POOL_SHRINK_DUR_MILLI
                    <= utime_milli
            {
                if let Ok(_g) = b.budget.mu.try_lock() {
                    let to_reclaim = b.budget.capacity.load(SeqCst);
                    if b.budget.used.load(SeqCst) <= 0 && to_reclaim > 0 {
                        b.budget.capacity.fetch_add(-to_reclaim, SeqCst);
                        reclaimed += to_reclaim;
                    }
                }
            }
        }

        let mut pool_released = 0i64;
        if reclaimed > 0 {
            pool_released = af.pool.release_pop_budget(reclaimed);
        }
        if pool_released > 0 {
            self.release(pool_released);
            self.mu_released.fetch_add(pool_released as u64, SeqCst);
            self.exec_metrics.await_free_shrink.fetch_add(1, SeqCst);
            self.weak_wake();
        }
        self.await_free_last_shrink_milli.store(utime_milli, SeqCst);
    }
}

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

impl MemArbitrator {
    // ----- pool medium capacity & magnification -----

    /// Go `updatePoolMediumCapacity`.
    pub(crate) fn update_pool_medium_capacity(&self, utime_milli: i64) {
        const MAX_NUM: i64 = (2 + DEF_REDUNDANCY) as i64;
        const MAX_DUR: i64 = MAX_NUM - DEF_REDUNDANCY as i64;
        {
            let stats = self.pool_alloc_stats.read().unwrap();
            let ts_align = utime_milli / KILO / DEF_UPDATE_MEM_CONSUMED_TIME_ALIGN_SEC;
            let idx1 = (((MAX_NUM + ts_align - 1) % MAX_NUM) as usize).min(3);
            let idx2 = ((ts_align % MAX_NUM) as usize).min(3);

            let mut tar1 = Some(idx1);
            let mut tar2 = Some(idx2);
            {
                let ts = self.pool_alloc_timed_elems[idx1].ts_align.load(SeqCst);
                if ts <= ts_align - MAX_DUR || ts > ts_align {
                    tar1 = None;
                }
            }
            {
                let ts = self.pool_alloc_timed_elems[idx2].ts_align.load(SeqCst);
                if ts <= ts_align - MAX_DUR || ts > ts_align {
                    tar2 = None;
                }
            }

            let g1 = tar1.map(|i| self.pool_alloc_timed_map[i].read().unwrap());
            let g2 = tar2.map(|i| self.pool_alloc_timed_map[i].read().unwrap());
            let mut total = 0u64;
            if let Some(i) = tar1 {
                total += self.pool_alloc_timed_elems[i].num.load(SeqCst);
            }
            if let Some(i) = tar2 {
                total += self.pool_alloc_timed_elems[i].num.load(SeqCst);
            }

            if total != 0 {
                let expect = 1u64.max(total.div_ceil(2));
                let mut cnt = 0u64;
                let mut index = 0usize;
                for i in 0..DEF_SERVERLIMIT_MIN_UNIT_NUM as usize {
                    if let Some(t1) = tar1 {
                        cnt += self.pool_alloc_timed_elems[t1].slot[i].load(SeqCst) as u64;
                    }
                    if let Some(t2) = tar2 {
                        cnt += self.pool_alloc_timed_elems[t2].slot[i].load(SeqCst) as u64;
                    }
                    if cnt >= expect {
                        index = i;
                        break;
                    }
                }
                let res = stats.pool_alloc_unit * (index as i64 + 1);
                self.pool_alloc_medium_quota.store(res, SeqCst);
            }
            drop(g1);
            drop(g2);
        }
        self.try_store_pool_medium_capacity(utime_milli, self.pool_medium_quota());
    }

    /// Go `tryStorePoolMediumCapacity`.
    pub(crate) fn try_store_pool_medium_capacity(&self, utime_milli: i64, capacity: i64) -> bool {
        if capacity == 0 {
            return false;
        }
        let last_state = self.last_mem_state();
        let should = match last_state {
            None => true,
            Some(s) => {
                self.pool_alloc_last_update_milli.load(SeqCst) + DEF_STORE_POOL_MEDIUM_CAP_DUR_MILLI
                    <= utime_milli
                    && s.pool_medium_cap != capacity
            }
        };
        if should {
            let mem_state = match last_state {
                Some(mut s) => {
                    s.pool_medium_cap = capacity;
                    s
                }
                None => RuntimeMemStateV1 {
                    version: 1,
                    pool_medium_cap: capacity,
                    ..Default::default()
                },
            };
            let _ = self.record_mem_state(&mem_state, "new root pool medium cap");
            self.pool_alloc_last_update_milli.store(utime_milli, SeqCst);
            return true;
        }
        false
    }

    pub(crate) fn pool_medium_quota(&self) -> i64 {
        self.pool_alloc_medium_quota.load(SeqCst)
    }

    /// Go `SuggestPoolInitCap`.
    pub fn suggest_pool_init_cap(&self) -> i64 {
        self.pool_medium_quota()
    }

    /// Go `updateMemMagnification`; returns the updated previous profile.
    pub(crate) fn update_mem_magnification(
        &self,
        utime_milli: i64,
    ) -> Option<(i64, i64, i64, i64)> {
        const MAX_NUM: i64 = 2;
        let cur_ts_align = utime_milli / KILO / DEF_UPDATE_MEM_MAGNIF_UTIME_ALIGN;
        let mut updated_pre: Option<(i64, i64, i64, i64)> = None;

        let mut profs = self.hc_timed_mem_profile.lock().unwrap();
        let cur_idx = (cur_ts_align % MAX_NUM) as usize;
        if profs[cur_idx].ts_align < cur_ts_align {
            {
                let pre_ts = cur_ts_align - 1;
                let pre_idx = (((MAX_NUM + pre_ts) % MAX_NUM) as usize).min(1);
                let pre = &mut profs[pre_idx];
                pre.ratio = 0;
                if pre.ts_align == pre_ts && pre.quota > 0 {
                    if pre.heap > 0 {
                        pre.ratio = calc_ratio(pre.heap, pre.quota);
                    }
                    updated_pre = Some((pre.start_utime_milli, pre.heap, pre.quota, pre.ratio));
                }
            }

            let mut v = 0i64;
            for ts_align in [cur_ts_align - 2, cur_ts_align - 1] {
                let tar = &profs[(((MAX_NUM + ts_align) % MAX_NUM) as usize).min(1)];
                if tar.ts_align != ts_align || tar.heap >= self.oom_risk() {
                    v = 0;
                    break;
                }
                if tar.ratio <= 0 {
                    break;
                }
                v = v.max(tar.ratio);
            }

            let mut updated = false;
            let mut ori_ratio = 0i64;
            let mut new_ratio = 0i64;

            if v != 0 {
                if let Ok(_g) = self.mem_magnif_mu.try_lock() {
                    ori_ratio = self.mem_magnif();
                    if ori_ratio != 0 && v < ori_ratio - 10 {
                        new_ratio = (ori_ratio + v) / 2;
                        if new_ratio <= KILO {
                            new_ratio = 0;
                        }
                        self.do_set_mem_magnif(new_ratio);
                        updated = true;
                    }
                }
            }

            if updated {
                let actions = self.actions.lock().unwrap().clone();
                (actions.info)(&format!(
                    "Update mem quota magnification ratio: ori={ori_ratio} new={new_ratio}"
                ));
                if let Some(last) = self.last_mem_state() {
                    if new_ratio < last.magnif {
                        let mem_state = RuntimeMemStateV1 {
                            version: 1,
                            magnif: new_ratio,
                            pool_medium_cap: self.pool_medium_quota(),
                            ..Default::default()
                        };
                        let _ = self.record_mem_state(&mem_state, "new magnification ratio");
                    }
                }
            }

            profs[cur_idx] = MemProfile {
                ts_align: cur_ts_align,
                start_utime_milli: utime_milli,
                ..Default::default()
            };
        }

        if profs[cur_idx].ts_align == cur_ts_align {
            let ut = self.hc_last_gc_utime.load(SeqCst);
            if cur_ts_align == ut / 1_000_000_000 / DEF_UPDATE_MEM_MAGNIF_UTIME_ALIGN {
                profs[cur_idx].heap = profs[cur_idx]
                    .heap
                    .max(self.hc_last_gc_heap_alloc.load(SeqCst));
            }
            let (blocked_size, utime_sec) = self.last_blocked_at();
            if utime_sec / DEF_UPDATE_MEM_MAGNIF_UTIME_ALIGN == cur_ts_align {
                profs[cur_idx].quota = profs[cur_idx].quota.max(blocked_size);
            }
        }
        updated_pre
    }

    /// Go `executeTick`.
    pub(crate) fn execute_tick(&self, utime_milli: i64) -> bool {
        if self.at_mem_risk() {
            return false;
        }
        if self.tick_last_milli.load(SeqCst) + DEF_TICK_DUR_MILLI > utime_milli {
            return false;
        }
        let _g = self.tick_mu.lock().unwrap();
        self.tick_last_milli.store(utime_milli, SeqCst);

        if let Some((start_ms, heap, quota, ratio)) = self.update_mem_magnification(utime_milli) {
            let actions = self.actions.lock().unwrap().clone();
            (actions.info)(&format!(
                "Mem profile timeline: last-blocked-heap={heap} last-blocked-quota={quota} last-ratio={ratio} start={start_ms}"
            ));
        }
        self.update_pool_medium_capacity(utime_milli);
        let limit = self.digest_limit.load(SeqCst);
        self.shrink_digest_profile(utime_milli / KILO, limit, limit / 2);
        true
    }

    // ----- mem risk -----

    pub(crate) fn is_mem_safe(&self) -> bool {
        self.hc_mem_inuse.load(SeqCst) < self.oom_risk()
    }

    pub(crate) fn is_mem_no_risk(&self) -> bool {
        self.is_mem_safe() && self.hc_heap_alloc.load(SeqCst) < self.mem_risk()
    }

    /// Go `calcMemRisk`.
    pub(crate) fn calc_mem_risk(&self) -> Option<RuntimeMemStateV1> {
        if *self.mu_soft_limit_mode.lock().unwrap() != SoftLimitMode::Auto {
            return None;
        }
        let mut mem_state = RuntimeMemStateV1 {
            version: 1,
            last_risk: LastRisk {
                heap_alloc: self.hc_heap_alloc.load(SeqCst),
                quota_alloc: self.allocated(),
            },
            pool_medium_cap: self.pool_medium_quota(),
            ..Default::default()
        };
        if mem_state.last_risk.quota_alloc == 0
            || mem_state.last_risk.heap_alloc <= mem_state.last_risk.quota_alloc
        {
            return None;
        }
        mem_state.magnif = calc_ratio(
            mem_state.last_risk.heap_alloc,
            mem_state.last_risk.quota_alloc,
        ) + 100;
        if let Some(p) = self.last_mem_state() {
            mem_state.magnif = mem_state.magnif.max(p.magnif);
        }
        Some(mem_state)
    }

    /// Go `handleMemIssues`: returns true if memory state is safe.
    pub(crate) fn handle_mem_issues(&self) -> bool {
        if self.at_mem_risk() {
            let gc_executed = self.try_runtime_gc();
            if !gc_executed {
                self.refresh_runtime_mem_stats();
            }
            if self.is_mem_no_risk() {
                self.update_tracked_heap_stats();
                self.update_avoid_size();
                let actions = self.actions.lock().unwrap().clone();
                (actions.info)("Memory is safe");
                self.set_mem_safe();
                return true;
            }
            self.do_reclaim_non_blocking_tasks();
            self.handle_mem_risk(gc_executed);
            false
        } else if !self.is_mem_safe() {
            self.do_reclaim_non_blocking_tasks();
            self.enter_mem_risk();
            false
        } else {
            true
        }
    }

    /// Go `intoMemRisk`.
    pub(crate) fn enter_mem_risk(&self) {
        let now = self.inner_time();
        {
            let mut risk = self.hc_mem_risk.lock().unwrap();
            risk.start_time = now;
            risk.last_heap_total_free = self.hc_heap_total_free.load(SeqCst);
            risk.last_stats_start_time = now;
        }
        self.hc_mem_risk_start_unix_milli.store(
            now.duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0),
            SeqCst,
        );
        self.exec_metrics.risk_mem.fetch_add(1, SeqCst);

        {
            let actions = self.actions.lock().unwrap().clone();
            (actions.warn)("Memory inuse reach threshold");
        }
        self.reclaim_heap();

        if let Some(mut mem_state) = self.calc_mem_risk() {
            if mem_state.magnif > DEF_MAX_MAGNIF {
                let actions = self.actions.lock().unwrap().clone();
                (actions.warn)("Memory pressure is abnormally high");
                mem_state.magnif = DEF_MAX_MAGNIF;
            }
            {
                let _g = self.mem_magnif_mu.lock().unwrap();
                self.do_set_mem_magnif(mem_state.magnif);
            }
            if let Err(err) = self.record_mem_state(&mem_state, "oom risk") {
                let actions = self.actions.lock().unwrap().clone();
                (actions.error)(&format!("Failed to save mem-risk: {err}"));
            }
        }

        if self.is_mem_no_risk() {
            self.wake();
        }
    }

    /// Go `handleMemRisk`.
    pub(crate) fn handle_mem_risk(&self, gc_executed: bool) {
        let now = self.inner_time();
        let oom_risk = self.hc_mem_inuse.load(SeqCst) > self.limit();
        let (dur, last_free, risk_start) = {
            let risk = self.hc_mem_risk.lock().unwrap();
            (
                now.duration_since(risk.last_stats_start_time)
                    .unwrap_or(Duration::ZERO),
                risk.last_heap_total_free,
                risk.start_time,
            )
        };
        if !oom_risk && dur < DEF_HEAP_RECLAIM_CHECK_DURATION {
            return;
        }
        let mut heap_use_bps = 0i64;
        if dur > Duration::ZERO {
            let heap_frees = self.hc_heap_total_free.load(SeqCst) - last_free;
            heap_use_bps = (heap_frees as f64 / dur.as_secs_f64()) as i64;
        }
        if oom_risk || mem_hang_risk(heap_use_bps, self.min_heap_free_bps(), now, risk_start) {
            self.enter_oom_risk();
            let mem_to_reclaim = self.hc_mem_inuse.load(SeqCst) - self.mem_risk();
            {
                let actions = self.actions.lock().unwrap().clone();
                (actions.warn)("`OOM RISK`: try to `KILL` running root pool");
            }

            let (new_kill_num, _reclaiming) = self.kill_topn_entry(mem_to_reclaim);
            if new_kill_num != 0 {
                let t = self.inner_time();
                {
                    let mut risk = self.hc_mem_risk.lock().unwrap();
                    risk.start_time = t;
                }
                self.hc_mem_risk_start_unix_milli.store(
                    t.duration_since(SystemTime::UNIX_EPOCH)
                        .map(|d| d.as_millis() as i64)
                        .unwrap_or(0),
                    SeqCst,
                );
                let actions = self.actions.lock().unwrap().clone();
                (actions.warn)("Restart runtime memory check");
            } else {
                let under_kill_num = {
                    let uk = self.under_kill.lock().unwrap();
                    uk.entries
                        .values()
                        .filter(|e| !e.arbitrator_mu.lock().unwrap().under_kill.fail)
                        .count()
                };
                if under_kill_num == 0 {
                    let mut force_kill = 0;
                    loop {
                        let Some(entry) = self.front_task_entry() else {
                            break;
                        };
                        let ctx = entry.load_ctx();
                        if ctx_available(&ctx) {
                            let ctx = ctx.unwrap();
                            ctx.stop(ArbitratorStopReason::OomRiskKill);
                            self.exec_metrics.risk_oom_kill[entry.mem_priority() as usize]
                                .fetch_add(1, SeqCst);
                            force_kill += 1;
                            if self.remove_task(&entry) {
                                entry.wind_up(0, ArbitrateResult::Fail);
                            }
                        } else {
                            // Task with unavailable ctx: Go would loop
                            // forever fetching the same front entry; the
                            // source relies on ctx being available here.
                            break;
                        }
                    }
                    let actions = self.actions.lock().unwrap().clone();
                    if force_kill != 0 {
                        (actions.warn)(
                            "No more running root pool can be killed to resolve `OOM RISK`; KILL all awaiting tasks;",
                        );
                    } else {
                        (actions.warn)(
                            "No more running root pool or awaiting task can be terminated to resolve `OOM RISK`",
                        );
                    }
                }
            }
        } else {
            let actions = self.actions.lock().unwrap().clone();
            (actions.warn)("Runtime memory free speed meets require, start re-check");
        }

        if dur >= DEF_HEAP_RECLAIM_CHECK_DURATION {
            let mut risk = self.hc_mem_risk.lock().unwrap();
            risk.last_heap_total_free = self.hc_heap_total_free.load(SeqCst);
            risk.last_stats_start_time = self.inner_time();
        }

        if !gc_executed {
            self.gc();
        }
    }

    /// Go `killTopnEntry`.
    pub(crate) fn kill_topn_entry(&self, required: i64) -> (i32, i64) {
        let mut new_kill_num = 0;
        let mut reclaimed = 0i64;

        if self.under_kill.lock().unwrap().num > 0 {
            let now = self.inner_time();
            let entries: Vec<Arc<RootPoolEntry>> = self
                .under_kill
                .lock()
                .unwrap()
                .entries
                .values()
                .cloned()
                .collect();
            for entry in entries {
                let mut st = entry.arbitrator_mu.lock().unwrap();
                if st.under_kill.fail {
                    continue;
                }
                let deadline = st.under_kill.start_time + DEF_KILL_CANCEL_CHECK_TIMEOUT;
                if now >= deadline {
                    let actions = self.actions.lock().unwrap().clone();
                    (actions.error)(&format!(
                        "Failed to `KILL` root pool due to timeout: uid={} name={}",
                        entry.pool.uid(),
                        entry.pool.name()
                    ));
                    st.under_kill.fail = true;
                    continue;
                }
                reclaimed += st.under_kill.reclaim;
            }
        }

        if reclaimed >= required {
            return (new_kill_num, reclaimed);
        }

        for prio in PRIORITIES {
            let mut pos = self.entry_map.max_quota_shard_index;
            while pos > self.entry_map.min_quota_shard_index_to_check {
                pos -= 1;
                let entries: Vec<(u64, Arc<RootPoolEntry>)> = self.entry_map.quota_shards
                    [prio as usize][pos]
                    .lock()
                    .unwrap()
                    .entries
                    .iter()
                    .map(|(k, v)| (*k, Arc::clone(v)))
                    .collect();
                for (uid, entry) in entries {
                    if entry.arbitrator_mu.lock().unwrap().under_kill.start || entry.not_running() {
                        continue;
                    }
                    let ctx = entry.load_ctx();
                    if ctx_available(&ctx) {
                        let ctx = ctx.unwrap();
                        let memory_used = ctx.helper().map(|h| h.heap_inuse()).unwrap_or(0);
                        if memory_used <= 0 {
                            continue;
                        }
                        self.add_under_kill(&entry, memory_used, self.inner_time());
                        reclaimed += memory_used;
                        ctx.stop(ArbitratorStopReason::OomRiskKill);
                        new_kill_num += 1;
                        self.exec_metrics.risk_oom_kill[prio as usize].fetch_add(1, SeqCst);
                        {
                            let actions = self.actions.lock().unwrap().clone();
                            (actions.warn)(&format!(
                                "Start to `KILL` root pool: uid={uid} mem-used={memory_used}"
                            ));
                        }
                        if self.remove_task(&entry) {
                            let actions = self.actions.lock().unwrap().clone();
                            (actions.warn)(&format!(
                                "Make the mem quota subscription failed: uid={uid}"
                            ));
                            entry.wind_up(0, ArbitrateResult::Fail);
                        }
                        if reclaimed >= required {
                            return (new_kill_num, reclaimed);
                        }
                    }
                }
            }
        }
        (new_kill_num, reclaimed)
    }

    // ----- mem state recording -----

    pub(crate) fn last_mem_state(&self) -> Option<RuntimeMemStateV1> {
        *self.hc_last_mem_state.lock().unwrap()
    }

    /// Go `recordMemState`.
    pub(crate) fn record_mem_state(
        &self,
        s: &RuntimeMemStateV1,
        reason: &str,
    ) -> Result<(), String> {
        let recorder = self.hc_recorder_mu.lock().unwrap();
        *self.hc_last_mem_state.lock().unwrap() = Some(*s);
        if let Err(err) = recorder.store(s) {
            self.exec_metrics.action_record_fail.fetch_add(1, SeqCst);
            return Err(err);
        }
        self.exec_metrics.action_record_succ.fetch_add(1, SeqCst);
        let actions = self.actions.lock().unwrap().clone();
        (actions.info)(&format!("Record mem state: reason={reason} data={s:?}"));
        Ok(())
    }

    /// Go `TaskNumByPattern`.
    pub fn task_num_by_pattern(&self) -> NumByPattern {
        let mut res = NumByPattern::default();
        for p in PRIORITIES {
            res[p as usize] = self.task_num_by_priority(p);
        }
        res[ARBITRATION_WAIT_AVERSE] = self.task_num_of_wait_averse();
        res
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::pool::{new_pool_uid, DEF_MAX_UNUSED_BLOCKS};
    use std::sync::atomic::AtomicI64;

    const NO_WAIT_AVERSE: bool = false;
    const REQUIRE_PRIVILEGE: bool = true;

    type CbMap = Arc<Mutex<HashMap<u64, i32>>>;

    struct HelperForTest {
        cancel: Mutex<Option<CancelHandle>>,
        kill_cb: Mutex<Option<Box<dyn Fn() + Send>>>,
        heap_used_cb: Mutex<Option<Box<dyn Fn() -> i64 + Send>>>,
        cancel_cb: Mutex<Option<Box<dyn Fn() + Send>>>,
    }

    impl HelperForTest {
        fn new_with_channel() -> (Arc<HelperForTest>, CancelChannel) {
            let (handle, ch) = cancel_channel();
            (
                Arc::new(HelperForTest {
                    cancel: Mutex::new(Some(handle)),
                    kill_cb: Mutex::new(None),
                    heap_used_cb: Mutex::new(None),
                    cancel_cb: Mutex::new(None),
                }),
                ch,
            )
        }
        fn cancel_self(&self) {
            let _ = self.cancel.lock().unwrap().take();
        }
        fn set_kill_cb(&self, f: impl Fn() + Send + 'static) {
            *self.kill_cb.lock().unwrap() = Some(Box::new(f));
        }
        fn set_heap_used_cb(&self, f: impl Fn() -> i64 + Send + 'static) {
            *self.heap_used_cb.lock().unwrap() = Some(Box::new(f));
        }
        fn set_cancel_cb(&self, f: impl Fn() + Send + 'static) {
            *self.cancel_cb.lock().unwrap() = Some(Box::new(f));
        }
    }

    impl ArbitrateHelper for HelperForTest {
        fn stop(&self, reason: ArbitratorStopReason) -> bool {
            let _ = self.cancel.lock().unwrap().take();
            if reason == ArbitratorStopReason::OomRiskKill {
                if let Some(f) = self.kill_cb.lock().unwrap().as_ref() {
                    f();
                }
            } else if let Some(f) = self.cancel_cb.lock().unwrap().as_ref() {
                f();
            }
            true
        }
        fn heap_inuse(&self) -> i64 {
            match self.heap_used_cb.lock().unwrap().as_ref() {
                Some(f) => f(),
                None => 0,
            }
        }
        fn finish(&self) {}
    }

    type StoreFn = Box<dyn Fn(&RuntimeMemStateV1) -> Result<(), String> + Send>;
    type LoadFn = Box<dyn Fn() -> Result<Option<RuntimeMemStateV1>, String> + Send>;

    #[derive(Clone)]
    struct RecorderForTest {
        load_fn: Arc<Mutex<LoadFn>>,
        store_fn: Arc<Mutex<StoreFn>>,
    }

    impl RecorderForTest {
        fn set_store(&self, f: impl Fn(&RuntimeMemStateV1) -> Result<(), String> + Send + 'static) {
            *self.store_fn.lock().unwrap() = Box::new(f);
        }
    }

    impl RecordMemState for RecorderForTest {
        fn load(&self) -> Result<Option<RuntimeMemStateV1>, String> {
            (self.load_fn.lock().unwrap())()
        }
        fn store(&self, s: &RuntimeMemStateV1) -> Result<(), String> {
            (self.store_fn.lock().unwrap())(s)
        }
    }

    fn new_arbitrator_for_test(
        shard_count: u64,
        limit: i64,
    ) -> (Arc<MemArbitrator>, RecorderForTest) {
        let load_event = Arc::new(AtomicI64::new(0));
        let le = Arc::clone(&load_event);
        let recorder = RecorderForTest {
            load_fn: Arc::new(Mutex::new(Box::new(move || {
                le.fetch_add(1, SeqCst);
                Ok(None)
            }))),
            store_fn: Arc::new(Mutex::new(Box::new(|_| Ok(())))),
        };
        let m = MemArbitrator::new(limit, shard_count, 3, 0, Box::new(recorder.clone()));
        assert_eq!(load_event.load(SeqCst), 1);
        m.init_await_free_pool(4, 4);
        (m, recorder)
    }

    fn set_actions(
        m: &MemArbitrator,
        info: impl Fn(&str) + Send + Sync + 'static,
        warn: impl Fn(&str) + Send + Sync + 'static,
        error: impl Fn(&str) + Send + Sync + 'static,
        update: Option<Box<dyn Fn() + Send + Sync>>,
        gc: Option<Box<dyn Fn() + Send + Sync>>,
    ) {
        *m.actions.lock().unwrap() = Arc::new(MemArbitratorActions {
            info: Box::new(info),
            warn: Box::new(warn),
            error: Box::new(error),
            update_runtime_mem_stats: update,
            gc,
        });
    }

    fn new_resource_pool_for_test(name: &str, alloc_align_size: i64) -> Arc<ResourcePool> {
        ResourcePool::new(
            new_pool_uid(),
            name,
            0,
            alloc_align_size,
            DEF_MAX_UNUSED_BLOCKS,
            Default::default(),
        )
    }

    fn set_limit_for_test(m: &MemArbitrator, v: i64) {
        let _g = m.mu.lock().unwrap();
        m.mu_limit.store(v, SeqCst);
    }

    fn cleanup_notifer(m: &MemArbitrator) {
        m.notifer.wake();
        m.notifer.wait();
    }

    fn new_ctx_with_helper(
        mem_priority: ArbitrationPriority,
        wait_averse: bool,
        prefer_privilege: bool,
    ) -> (Arc<ArbitrationContext>, Arc<HelperForTest>) {
        let (h, ch) = HelperForTest::new_with_channel();
        let ctx = ArbitrationContext::new(
            Some(ch),
            Some(Arc::clone(&h) as Arc<dyn ArbitrateHelper>),
            mem_priority,
            wait_averse,
            prefer_privilege,
        );
        (ctx, h)
    }

    fn new_def_ctx(mem_priority: ArbitrationPriority) -> Arc<ArbitrationContext> {
        ArbitrationContext::new(None, None, mem_priority, NO_WAIT_AVERSE, false)
    }

    fn add_root_pool_for_test(
        m: &Arc<MemArbitrator>,
        p: Arc<ResourcePool>,
        ctx: Option<Arc<ArbitrationContext>>,
    ) -> Arc<RootPoolEntry> {
        let entry = m.add_root_pool(p).unwrap();
        assert!(m.restart_entry_by_context(
            RootPoolWrap {
                entry: Some(Arc::clone(&entry))
            },
            ctx
        ));
        entry
    }

    fn add_entry_for_test(
        m: &Arc<MemArbitrator>,
        ctx: Option<Arc<ArbitrationContext>>,
    ) -> Arc<RootPoolEntry> {
        let p = new_resource_pool_for_test("test", 1);
        add_root_pool_for_test(m, p, ctx)
    }

    fn new_pool_with_helper(
        m: &Arc<MemArbitrator>,
        prefix: &str,
        mem_priority: ArbitrationPriority,
        wait_averse: bool,
        prefer_privilege: bool,
    ) -> (Arc<RootPoolEntry>, Arc<HelperForTest>) {
        let (ctx, h) = new_ctx_with_helper(mem_priority, wait_averse, prefer_privilege);
        let pool = new_resource_pool_for_test("", 1);
        let pool = ResourcePool::new(
            pool.uid(),
            &format!("{prefix}-{}", pool.uid()),
            0,
            1,
            DEF_MAX_UNUSED_BLOCKS,
            Default::default(),
        );
        let e = add_root_pool_for_test(m, pool, Some(ctx));
        (e, h)
    }

    fn get_all_entries(m: &MemArbitrator) -> HashMap<u64, Arc<RootPoolEntry>> {
        let mut res = HashMap::new();
        let mut empty_cnt = 0i64;
        for shard in &m.entry_map.shards {
            for (uid, v) in shard.entries.read().unwrap().iter() {
                res.insert(*uid, Arc::clone(v));
                let st = v.arbitrator_mu.lock().unwrap();
                if st.quota == 0 {
                    empty_cnt += 1;
                    assert!(st.quota_shard.is_none());
                } else {
                    assert!(st.quota_shard.is_some());
                }
            }
        }
        let mut cnt = res.len() as i64;
        assert_eq!(m.root_pool_num(), cnt);
        assert_eq!(m.entry_map.context_cache_num.load(SeqCst), cnt);

        for prio in PRIORITIES {
            for (pos, shard) in m.entry_map.quota_shards[prio as usize].iter().enumerate() {
                for (uid, v) in shard.lock().unwrap().entries.iter() {
                    let e = res.get(uid).expect("entry in quota shard must exist");
                    assert!(Arc::ptr_eq(e, v));
                    assert_eq!(
                        v.arbitrator_mu.lock().unwrap().quota_shard,
                        Some((prio, pos))
                    );
                    cnt -= 1;
                }
            }
        }
        cnt -= empty_cnt;
        assert_eq!(cnt, 0);
        res
    }

    fn check_entries(m: &MemArbitrator, expect: &[&Arc<RootPoolEntry>]) {
        let s = get_all_entries(m);
        assert_eq!(expect.len(), s.len());
        let mut sum_quota = 0i64;
        for entry in expect {
            let uid = entry.pool.uid();
            let e = s.get(&uid).expect("expected entry present");
            assert!(Arc::ptr_eq(e, entry));
            assert!(!e.arbitrator_mu.lock().unwrap().destroyed);
            assert!(entry.request.result_rx.try_recv().is_err());

            let st = e.arbitrator_mu.lock().unwrap();
            if st.quota == 0 {
                continue;
            }
            sum_quota += st.quota;
            assert_eq!(st.quota, entry.pool.approx_cap());
            let shard = (
                entry.mem_priority(),
                get_quota_shard(st.quota, m.entry_map.max_quota_shard_index),
            );
            assert_eq!(st.quota_shard, Some(shard));
        }
        sum_quota += m.await_free_pool_cap();
        assert_eq!(sum_quota, m.allocated());
    }

    fn check_entry_quota_by_priority(
        m: &MemArbitrator,
        e: &Arc<RootPoolEntry>,
        expected: ArbitrationPriority,
        quota: i64,
    ) {
        assert_eq!(expected, e.mem_priority());
        let st = e.arbitrator_mu.lock().unwrap();
        assert_eq!(quota, st.quota);
        if st.quota != 0 {
            let pos = get_quota_shard(st.quota, m.entry_map.max_quota_shard_index);
            assert_eq!(st.quota_shard, Some((expected, pos)));
            assert!(m.entry_map.quota_shards[expected as usize][pos]
                .lock()
                .unwrap()
                .entries
                .contains_key(&e.pool.uid()));
        }
    }

    fn delete_entries_for_test(m: &Arc<MemArbitrator>, entries: &[&Arc<RootPoolEntry>]) {
        for e in entries {
            m.remove_root_pool_by_id(e.pool.uid());
        }
        m.do_execute_cleanup_tasks();
        cleanup_notifer(m);
    }

    fn reset_entries_for_test(m: &Arc<MemArbitrator>, entries: &[&Arc<RootPoolEntry>]) {
        for e in entries {
            m.reset_root_pool_by_id(e.pool.uid(), 0, false);
        }
        m.do_execute_cleanup_tasks();
        cleanup_notifer(m);
    }

    fn find_task_by_mode(
        m: &MemArbitrator,
        e: &Arc<RootPoolEntry>,
        prio: ArbitrationPriority,
        wait_averse: bool,
    ) -> bool {
        let tasks = m.tasks.lock().unwrap();
        let mut found = false;
        for p in PRIORITIES {
            for (ele, v) in tasks.fifo_by_priority[p as usize].iter_live() {
                if Arc::ptr_eq(&v, e) {
                    assert!(!found);
                    assert_eq!(prio, p);
                    found = true;
                    assert_eq!(e.mem_priority(), p);
                    assert_eq!(e.ctx.wait_averse.load(SeqCst), wait_averse);
                    assert_eq!(e.task_mu.lock().unwrap().fifo_by_priority, ele);

                    match e.load_ctx() {
                        None => {
                            assert_eq!(p, ArbitrationPriority::Medium);
                            assert!(!wait_averse);
                        }
                        Some(ctx) => {
                            assert_eq!(p, ctx.mem_priority);
                            assert_eq!(wait_averse, ctx.wait_averse);
                        }
                    }

                    if wait_averse {
                        assert!(!e.ctx.prefer_privilege.load(SeqCst));
                        let mut found2 = false;
                        for (ele2, v2) in tasks.fifo_wait_averse.iter_live() {
                            if Arc::ptr_eq(&v2, e) {
                                assert!(!found2);
                                assert_eq!(e.task_mu.lock().unwrap().fifo_wait_averse, ele2);
                                found2 = true;
                            }
                        }
                        assert!(found2);
                    }
                    {
                        let mut found3 = false;
                        for (ele3, v3) in tasks.fifo_tasks.iter_live() {
                            if Arc::ptr_eq(&v3, e) {
                                assert!(!found3);
                                assert_eq!(e.task_mu.lock().unwrap().fifo, ele3);
                                found3 = true;
                            }
                        }
                        assert!(found3);
                    }
                }
            }
        }
        found
    }

    fn tasks_count(m: &MemArbitrator) -> i64 {
        let mut sz = 0i64;
        for p in PRIORITIES {
            sz += m.task_num_by_priority(p);
        }
        assert_eq!(sz, m.tasks.lock().unwrap().fifo_tasks.size());
        assert!(!(sz == 0 && m.waiting_alloc_size() != 0));
        sz
    }

    fn front_task_entry_for_test(m: &MemArbitrator) -> Option<Arc<RootPoolEntry>> {
        let entry = m.front_task_entry()?;
        assert!(entry.task_mu.lock().unwrap().fifo.valid());
        Some(entry)
    }

    fn check_task_exec(
        m: &MemArbitrator,
        task: PairSuccessFail,
        cancel_by_standard: i64,
        cancel: NumByPattern,
    ) {
        let em = m.exec_metrics();
        assert_eq!(em.task.pair, task);
        let s: i64 = em.task.succ_by_priority.iter().sum();
        if m.work_mode() == ArbitratorWorkMode::Priority {
            assert_eq!(s, task.succ);
        } else {
            assert_eq!(s, 0);
        }
        assert_eq!(
            em.cancel,
            ExecMetricsCancel {
                standard_mode: cancel_by_standard,
                priority_mode: [cancel[0], cancel[1], cancel[2]],
                wait_averse: cancel[3],
            }
        );
    }

    fn reset_exec_metrics_for_test(m: &MemArbitrator) {
        m.exec_metrics.reset();
        m.exec_mu.lock().unwrap().blocked_state = BlockedState::default();
        m.set_buffer_size(0);
        for (lock, elem) in m.buffer_timed_map.iter().zip(m.buffer_timed_elems.iter()) {
            let _g = lock.write().unwrap();
            elem.ts.store(0, SeqCst);
            elem.size.store(0, SeqCst);
            elem.quota.store(0, SeqCst);
        }
        let n = m.digest_shards.lock().unwrap().len() as u64;
        m.reset_digest_profile_cache(n);
        m.reset_statistics();
        m.mu_last_gc.store(m.mu_released.load(SeqCst), SeqCst);
    }

    fn reset_await_free_for_test(m: &MemArbitrator) {
        let af = m.await_free_state().unwrap();
        for b in &af.shards {
            b.budget.used.store(0, SeqCst);
            b.heap_inuse.store(0, SeqCst);
            b.budget.set_last_used_time_sec(0);
        }
        assert_eq!(m.await_free_pool_used(), MemPoolQuotaUsage::default());
        m.shrink_await_free_pool(0, now_unix_milli());
        assert_eq!(m.await_free_pool_cap(), 0);
    }

    fn check_await_free(m: &MemArbitrator) {
        let af = m.await_free_state().unwrap();
        let s: i64 = af
            .shards
            .iter()
            .map(|b| b.budget.capacity.load(SeqCst))
            .sum();
        assert_eq!(s, af.pool.allocated());
    }

    #[test]
    fn mem_arbitrator_switch_mode() {
        let (m, _recorder) = new_arbitrator_for_test(1, -1);
        assert_eq!(m.work_mode(), ArbitratorWorkMode::Disable);
        reset_exec_metrics_for_test(&m);
        let new_limit = 1000i64;
        set_limit_for_test(&m, new_limit);
        assert_eq!(m.limit(), new_limit);
        let action_cancel: CbMap = Arc::new(Mutex::new(HashMap::new()));

        let gen_test_pool = |mem_priority, wait_averse, prefer_privilege| {
            let (e, h) =
                new_pool_with_helper(&m, "test", mem_priority, wait_averse, prefer_privilege);
            let uid = e.pool.uid();
            let ac = Arc::clone(&action_cancel);
            h.set_cancel_cb(move || {
                *ac.lock().unwrap().entry(uid).or_insert(0) += 1;
            });
            (e, h)
        };

        let gen_test_ctx = |e: &Arc<RootPoolEntry>, mem_priority, wait_averse, prefer_privilege| {
            let (c, h) = new_ctx_with_helper(mem_priority, wait_averse, prefer_privilege);
            let uid = e.pool.uid();
            let ac = Arc::clone(&action_cancel);
            h.set_cancel_cb(move || {
                *ac.lock().unwrap().entry(uid).or_insert(0) += 1;
            });
            (c, h)
        };

        let (entry1, _h1) = gen_test_pool(ArbitrationPriority::Medium, NO_WAIT_AVERSE, false);
        {
            // Disable mode
            entry1.ctx.prefer_privilege.store(true, SeqCst);
            assert_eq!(entry1.exec_state(), EntryExecState::Running);
            assert!(!entry1.state_mu.stop.load(SeqCst));
            let request_size = new_limit * 2;

            m.prepare_alloc(&entry1, request_size);
            m.notifer.wait();
            assert_eq!(tasks_count(&m), 1);
            assert_eq!(m.waiting_alloc_size(), request_size);
            assert!(m.waiting_alloc_size() > m.limit());

            assert!(Arc::ptr_eq(
                &front_task_entry_for_test(&m).unwrap(),
                &entry1
            ));
            assert!(Arc::ptr_eq(
                &m.get_root_pool_entry(entry1.pool.uid()).unwrap(),
                &entry1
            ));
            assert_eq!(m.task_num_by_pattern(), [0, 1, 0, 0]);
            check_entry_quota_by_priority(&m, &entry1, ArbitrationPriority::Medium, 0);
            assert_eq!(m.run_one_round(), -1);
            assert_eq!(m.exec_mu.lock().unwrap().blocked_state.allocated, 0);
            assert_eq!(m.wait_alloc(&entry1), ArbitrateResult::Ok);
            assert_eq!(tasks_count(&m), 0);

            assert!(!find_task_by_mode(
                &m,
                &entry1,
                ArbitrationPriority::Medium,
                NO_WAIT_AVERSE
            ));
            check_entry_quota_by_priority(&m, &entry1, ArbitrationPriority::Medium, request_size);
            check_task_exec(&m, PairSuccessFail { succ: 1, fail: 0 }, 0, [0; 4]);

            assert!(m.privileged_entry.lock().unwrap().is_none());
            assert_eq!(entry1.exec_state(), EntryExecState::Running);
            assert!(!entry1.state_mu.stop.load(SeqCst));

            assert_eq!(m.allocated(), request_size);
            assert!(m.allocated() > m.limit());
            check_entries(&m, &[&entry1]);

            {
                // illegal operation
                let e0 = add_entry_for_test(&m, None);
                m.prepare_alloc(&e0, 1);
                assert_eq!(m.root_pool_num(), 2);
                assert_eq!(m.run_one_round(), -1);
                assert_eq!(m.wait_alloc(&e0), ArbitrateResult::Ok);
                assert_eq!(e0.arbitrator_mu.lock().unwrap().quota, 1);

                m.prepare_alloc(&e0, 1);
                assert_eq!(
                    e0.task_mu.lock().unwrap().fifo_priority,
                    Some(ArbitrationPriority::Medium)
                );
                m.reset_root_pool_entry(&e0);
                let (c, _h) = new_ctx_with_helper(ArbitrationPriority::Low, NO_WAIT_AVERSE, false);
                assert!(m.restart_entry_by_context(
                    RootPoolWrap {
                        entry: Some(Arc::clone(&e0))
                    },
                    Some(c)
                ));
                assert_eq!(e0.mem_priority(), ArbitrationPriority::Low);
                assert_eq!(
                    e0.task_mu.lock().unwrap().fifo_priority,
                    Some(ArbitrationPriority::Medium)
                );
                if m.exec_mu.lock().unwrap().mode == ArbitratorWorkMode::Disable {
                    m.implicit_run();
                }
                assert_eq!(m.wait_alloc(&e0), ArbitrateResult::Ok);
                assert_eq!(e0.arbitrator_mu.lock().unwrap().quota, 2);
                assert_eq!(
                    e0.arbitrator_mu.lock().unwrap().quota,
                    e0.pool.capacity() + e0.state_mu.quota_to_reclaim.load(SeqCst)
                );
                assert_eq!(m.cleanup_fifo.lock().unwrap().size(), 1);

                m.reset_root_pool_entry(&e0);
                assert!(m.remove_root_pool_entry(&e0));
                assert_eq!(m.cleanup_fifo.lock().unwrap().size(), 3);
                m.prepare_alloc(&e0, 1);
                assert_eq!(m.run_one_round(), -1);
                assert_eq!(m.wait_alloc(&e0), ArbitrateResult::Fail);
                assert!(m.get_root_pool_entry(e0.pool.uid()).is_none());
                assert_eq!(m.root_pool_num(), 1);
                m.prepare_alloc(&e0, 1);
                assert_eq!(m.run_one_round(), -1);
                assert_eq!(m.wait_alloc(&e0), ArbitrateResult::Fail);

                let e1 = add_entry_for_test(&m, None);
                assert!(m.remove_root_pool_entry(&e1));
                assert!(!m.reset_root_pool_entry(&e1));
                assert_eq!(m.cleanup_fifo.lock().unwrap().size(), 1);
                m.prepare_alloc(&e1, 1);
                if m.exec_mu.lock().unwrap().mode == ArbitratorWorkMode::Disable {
                    m.implicit_run();
                }
                assert_eq!(m.wait_alloc(&e1), ArbitrateResult::Ok);
                m.do_execute_cleanup_tasks();
            }
        }

        let (entry2, _h2) = gen_test_pool(ArbitrationPriority::Medium, NO_WAIT_AVERSE, false);
        reset_exec_metrics_for_test(&m);
        cleanup_notifer(&m);
        {
            // Disable mode -> Standard mode
            assert_eq!(m.allocated(), 2 * new_limit);
            let request_size = 1i64;

            m.prepare_alloc(&entry2, request_size);
            m.notifer.wait();
            assert_eq!(tasks_count(&m), 1);
            assert_eq!(m.waiting_alloc_size(), request_size);
            assert!(Arc::ptr_eq(
                &front_task_entry_for_test(&m).unwrap(),
                &entry2
            ));
            assert_eq!(entry2.request.quota.load(SeqCst), request_size);
            assert!(find_task_by_mode(
                &m,
                &entry2,
                ArbitrationPriority::Medium,
                NO_WAIT_AVERSE
            ));
            assert_eq!(m.task_num_by_pattern(), [0, 1, 0, 0]);
            check_entry_quota_by_priority(&m, &entry2, ArbitrationPriority::Medium, 0);

            m.set_work_mode(ArbitratorWorkMode::Standard);
            assert_eq!(m.work_mode(), ArbitratorWorkMode::Standard);

            assert_eq!(m.run_one_round(), 0);
            assert_eq!(
                m.exec_mu.lock().unwrap().blocked_state.allocated,
                2 * new_limit
            );

            check_task_exec(&m, PairSuccessFail::default(), 1, [0; 4]);
            assert_eq!(action_cancel.lock().unwrap()[&entry2.pool.uid()], 1);
            assert_eq!(m.wait_alloc(&entry2), ArbitrateResult::Fail);

            assert!(!find_task_by_mode(
                &m,
                &entry2,
                ArbitrationPriority::Medium,
                NO_WAIT_AVERSE
            ));
            check_entry_quota_by_priority(&m, &entry2, ArbitrationPriority::Medium, 0);
            check_entry_quota_by_priority(&m, &entry1, ArbitrationPriority::Medium, new_limit * 2);
            check_task_exec(&m, PairSuccessFail { succ: 0, fail: 1 }, 1, [0; 4]);

            assert_eq!(tasks_count(&m), 0);
            assert_eq!(entry1.exec_state(), EntryExecState::Running);
            assert_eq!(entry2.exec_state(), EntryExecState::Running);
            check_entries(&m, &[&entry1, &entry2]);

            let mut entries: Vec<Arc<RootPoolEntry>> = Vec::new();
            let mut helpers = Vec::new();
            for prio in PRIORITIES {
                for wait_averse in [false, true] {
                    let (e, h) = gen_test_pool(prio, wait_averse, true);
                    m.prepare_alloc(&e, m.limit());
                    entries.push(e);
                    helpers.push(h);
                }
            }
            {
                let mut all: Vec<&Arc<RootPoolEntry>> = entries.iter().collect();
                all.push(&entry1);
                all.push(&entry2);
                check_entries(&m, &all);
            }
            assert!(m.privileged_entry.lock().unwrap().is_none());
            assert_eq!(m.run_one_round(), 0);
            assert_eq!(
                m.exec_mu.lock().unwrap().blocked_state.allocated,
                2 * new_limit
            );
            assert!(m.privileged_entry.lock().unwrap().is_none());

            for e in &entries {
                assert_eq!(m.wait_alloc(e), ArbitrateResult::Fail);
            }
            check_task_exec(&m, PairSuccessFail { succ: 0, fail: 7 }, 7, [0; 4]);
            {
                let refs: Vec<&Arc<RootPoolEntry>> = entries.iter().collect();
                delete_entries_for_test(&m, &refs);
            }
            check_entries(&m, &[&entry1, &entry2]);
        }

        reset_exec_metrics_for_test(&m);
        cleanup_notifer(&m);
        action_cancel.lock().unwrap().clear();
        {
            // Standard mode -> Priority mode: wait until cancel self
            assert_eq!(m.allocated(), 2 * new_limit);
            let request_size = 1i64;
            reset_entries_for_test(&m, &[&entry2]);
            let (c, h2b) =
                gen_test_ctx(&entry2, ArbitrationPriority::Medium, NO_WAIT_AVERSE, false);
            assert!(m.restart_entry_by_context(
                RootPoolWrap {
                    entry: Some(Arc::clone(&entry2))
                },
                Some(c)
            ));
            m.prepare_alloc(&entry2, request_size);
            m.notifer.wait();
            assert_eq!(tasks_count(&m), 1);
            assert_eq!(m.waiting_alloc_size(), request_size);
            let e = front_task_entry_for_test(&m).unwrap();
            assert!(Arc::ptr_eq(&e, &entry2));

            assert!(find_task_by_mode(
                &m,
                &e,
                ArbitrationPriority::Medium,
                NO_WAIT_AVERSE
            ));
            assert_eq!(m.task_num_by_pattern(), [0, 1, 0, 0]);
            check_entry_quota_by_priority(&m, &e, ArbitrationPriority::Medium, 0);
            check_entry_quota_by_priority(&m, &entry1, ArbitrationPriority::Medium, new_limit * 2);

            m.set_work_mode(ArbitratorWorkMode::Priority);
            assert_eq!(m.work_mode(), ArbitratorWorkMode::Priority);

            assert_eq!(m.run_one_round(), 0);
            assert_eq!(tasks_count(&m), 1);
            assert_eq!(
                m.exec_mu.lock().unwrap().blocked_state.allocated,
                2 * new_limit
            );
            assert_eq!(m.waiting_alloc_size(), request_size);
            check_task_exec(&m, PairSuccessFail::default(), 0, [0; 4]);

            assert!(find_task_by_mode(
                &m,
                &e,
                ArbitrationPriority::Medium,
                NO_WAIT_AVERSE
            ));

            h2b.cancel_self();
            assert_eq!(m.wait_alloc(&entry2), ArbitrateResult::Fail);
            check_task_exec(&m, PairSuccessFail { succ: 0, fail: 1 }, 0, [0; 4]);
            assert!(action_cancel.lock().unwrap().is_empty());

            assert_eq!(tasks_count(&m), 0);
            check_entries(&m, &[&entry1, &entry2]);
        }

        reset_exec_metrics_for_test(&m);
        cleanup_notifer(&m);
        {
            // Priority mode: interrupt lower priority tasks
            assert!(action_cancel.lock().unwrap().is_empty());
            assert_eq!(m.allocated(), 2 * new_limit);
            let request_size = 1i64;

            reset_entries_for_test(&m, &[&entry2]);
            assert!(m.restart_entry_by_context(
                RootPoolWrap {
                    entry: Some(Arc::clone(&entry2))
                },
                Some(new_def_ctx(ArbitrationPriority::High))
            ));
            m.prepare_alloc(&entry2, request_size);
            m.notifer.wait();
            assert_eq!(tasks_count(&m), 1);
            assert_eq!(m.waiting_alloc_size(), request_size);
            let e = front_task_entry_for_test(&m).unwrap();
            assert!(Arc::ptr_eq(
                &m.get_root_pool_entry(e.pool.uid()).unwrap(),
                &entry2
            ));
            assert_eq!(entry2.request.quota.load(SeqCst), request_size);
            assert!(find_task_by_mode(
                &m,
                &entry2,
                ArbitrationPriority::High,
                NO_WAIT_AVERSE
            ));
            assert_eq!(m.task_num_by_pattern(), [0, 0, 1, 0]);
            check_entry_quota_by_priority(&m, &entry2, ArbitrationPriority::High, 0);
            check_entry_quota_by_priority(&m, &entry1, ArbitrationPriority::Medium, new_limit * 2);

            assert_eq!(m.run_one_round(), 0);
            assert_eq!(
                m.exec_mu.lock().unwrap().blocked_state.allocated,
                2 * new_limit
            );
            check_task_exec(&m, PairSuccessFail::default(), 0, [0, 1, 0, 0]);

            assert!(find_task_by_mode(
                &m,
                &entry2,
                ArbitrationPriority::High,
                NO_WAIT_AVERSE
            ));
            assert_eq!(m.task_num_by_pattern(), [0, 0, 1, 0]);

            assert_eq!(action_cancel.lock().unwrap()[&entry1.pool.uid()], 1);
            m.reset_root_pool_entry(&entry1);

            assert!(entry1.arbitrator_mu.lock().unwrap().under_cancel.start);
            {
                let st = entry1.arbitrator_mu.lock().unwrap();
                assert_eq!(st.under_cancel.reclaim, st.quota);
            }
            {
                let uc = m.under_cancel.lock().unwrap();
                assert_eq!(uc.num, 1);
                assert!(uc.entries.contains_key(&entry1.pool.uid()));
            }

            assert_eq!(m.run_one_round(), 1);
            assert_eq!(m.wait_alloc(&entry2), ArbitrateResult::Ok);
            check_task_exec(&m, PairSuccessFail { succ: 1, fail: 0 }, 0, [0, 1, 0, 0]);
            {
                let uc = m.under_cancel.lock().unwrap();
                assert_eq!(uc.num, 0);
                assert!(uc.entries.is_empty());
            }
            assert_eq!(tasks_count(&m), 0);

            assert!(!find_task_by_mode(
                &m,
                &entry2,
                ArbitrationPriority::High,
                NO_WAIT_AVERSE
            ));
            check_entry_quota_by_priority(&m, &entry2, ArbitrationPriority::High, request_size);
            check_entry_quota_by_priority(&m, &entry1, ArbitrationPriority::Medium, 0);

            assert_eq!(entry1.exec_state(), EntryExecState::Idle);
            assert_eq!(entry2.exec_state(), EntryExecState::Running);
            check_entries(&m, &[&entry1, &entry2]);
            reset_entries_for_test(&m, &[&entry1, &entry2]);
        }

        reset_exec_metrics_for_test(&m);
        cleanup_notifer(&m);
        {
            // Priority mode -> Disable mode
            assert_eq!(m.allocated(), 0);
            m.consume_quota_from_await_free_pool(0, 99);

            let request_size = new_limit + 1;
            assert!(m.restart_entry_by_context(
                RootPoolWrap {
                    entry: Some(Arc::clone(&entry1))
                },
                Some(new_def_ctx(ArbitrationPriority::Low))
            ));
            m.prepare_alloc(&entry1, request_size);
            assert!(m.restart_entry_by_context(
                RootPoolWrap {
                    entry: Some(Arc::clone(&entry2))
                },
                Some(new_def_ctx(ArbitrationPriority::Low))
            ));
            m.prepare_alloc(&entry2, request_size);

            m.notifer.wait();
            assert_eq!(tasks_count(&m), 2);
            assert_eq!(m.waiting_alloc_size(), request_size * 2);
            assert!(find_task_by_mode(
                &m,
                &entry1,
                ArbitrationPriority::Low,
                NO_WAIT_AVERSE
            ));
            assert!(find_task_by_mode(
                &m,
                &entry2,
                ArbitrationPriority::Low,
                NO_WAIT_AVERSE
            ));
            assert_eq!(m.task_num_by_pattern(), [2, 0, 0, 0]);

            assert_eq!(m.run_one_round(), 0);
            assert_eq!(m.exec_mu.lock().unwrap().blocked_state.allocated, 100);
            check_task_exec(&m, PairSuccessFail::default(), 0, [0; 4]);

            assert_eq!(tasks_count(&m), 2);
            assert_eq!(m.waiting_alloc_size(), request_size * 2);

            m.set_work_mode(ArbitratorWorkMode::Disable);
            assert_eq!(m.run_one_round(), -1);
            assert_eq!(m.exec_mu.lock().unwrap().blocked_state.allocated, 0);

            assert_eq!(m.wait_alloc(&entry1), ArbitrateResult::Ok);
            assert_eq!(m.wait_alloc(&entry2), ArbitrateResult::Ok);
            check_entry_quota_by_priority(&m, &entry1, ArbitrationPriority::Low, request_size);
            check_entry_quota_by_priority(&m, &entry2, ArbitrationPriority::Low, request_size);
            assert_eq!(m.allocated(), request_size * 2 + m.await_free_pool_cap());
            assert_eq!(tasks_count(&m), 0);
            m.consume_quota_from_await_free_pool(0, -99);
            reset_await_free_for_test(&m);
            check_entries(&m, &[&entry1, &entry2]);
            check_task_exec(&m, PairSuccessFail { succ: 2, fail: 0 }, 0, [0; 4]);
            reset_entries_for_test(&m, &[&entry1, &entry2]);
            assert_eq!(m.allocated(), 0);
        }

        m.set_work_mode(ArbitratorWorkMode::Priority);
        reset_exec_metrics_for_test(&m);
        action_cancel.lock().unwrap().clear();
        {
            // Priority mode: mixed task mode with wait-averse
            let alloc_unit = 4000i64;
            let (c1, _h1b) = gen_test_ctx(&entry1, ArbitrationPriority::Low, NO_WAIT_AVERSE, false);
            assert!(m.restart_entry_by_context(
                RootPoolWrap {
                    entry: Some(Arc::clone(&entry1))
                },
                Some(c1)
            ));
            let (c2, _h2c) =
                gen_test_ctx(&entry2, ArbitrationPriority::Medium, NO_WAIT_AVERSE, false);
            assert!(m.restart_entry_by_context(
                RootPoolWrap {
                    entry: Some(Arc::clone(&entry2))
                },
                Some(c2)
            ));
            let (entry3, _h3) = gen_test_pool(ArbitrationPriority::High, NO_WAIT_AVERSE, false);
            let (entry4, _h4) = gen_test_pool(ArbitrationPriority::Low, NO_WAIT_AVERSE, false);
            let (entry5, _h5) = gen_test_pool(ArbitrationPriority::High, true, false);
            check_entries(&m, &[&entry1, &entry2, &entry3, &entry4, &entry5]);

            set_limit_for_test(&m, 8 * alloc_unit);

            m.prepare_alloc(&entry1, alloc_unit);
            m.prepare_alloc(&entry2, alloc_unit);
            m.prepare_alloc(&entry3, alloc_unit);
            m.prepare_alloc(&entry4, alloc_unit * 4);
            m.prepare_alloc(&entry5, alloc_unit);

            assert!(find_task_by_mode(
                &m,
                &entry1,
                ArbitrationPriority::Low,
                NO_WAIT_AVERSE
            ));
            assert!(find_task_by_mode(
                &m,
                &entry2,
                ArbitrationPriority::Medium,
                NO_WAIT_AVERSE
            ));
            assert!(find_task_by_mode(
                &m,
                &entry3,
                ArbitrationPriority::High,
                NO_WAIT_AVERSE
            ));
            assert!(find_task_by_mode(
                &m,
                &entry4,
                ArbitrationPriority::Low,
                NO_WAIT_AVERSE
            ));
            assert!(find_task_by_mode(
                &m,
                &entry5,
                ArbitrationPriority::High,
                true
            ));

            assert_eq!(tasks_count(&m), 5);
            assert_eq!(m.waiting_alloc_size(), 8 * alloc_unit);
            assert_eq!(m.task_num_by_pattern(), [2, 1, 2, 1]);

            assert_eq!(m.run_one_round(), 5);
            assert_eq!(m.exec_mu.lock().unwrap().blocked_state.allocated, 0);
            assert_eq!(m.allocated(), 8 * alloc_unit);

            assert_eq!(m.wait_alloc(&entry1), ArbitrateResult::Ok);
            assert_eq!(m.wait_alloc(&entry2), ArbitrateResult::Ok);
            assert_eq!(m.wait_alloc(&entry3), ArbitrateResult::Ok);
            assert_eq!(m.wait_alloc(&entry4), ArbitrateResult::Ok);
            assert_eq!(m.wait_alloc(&entry5), ArbitrateResult::Ok);

            check_task_exec(&m, PairSuccessFail { succ: 5, fail: 0 }, 0, [0; 4]);
            check_entry_quota_by_priority(&m, &entry1, ArbitrationPriority::Low, alloc_unit);
            check_entry_quota_by_priority(&m, &entry2, ArbitrationPriority::Medium, alloc_unit);
            check_entry_quota_by_priority(&m, &entry3, ArbitrationPriority::High, alloc_unit);
            check_entry_quota_by_priority(&m, &entry4, ArbitrationPriority::Low, alloc_unit * 4);
            check_entry_quota_by_priority(&m, &entry5, ArbitrationPriority::High, alloc_unit);

            {
                let s4 = entry4.arbitrator_mu.lock().unwrap().quota_shard;
                let s1 = entry1.arbitrator_mu.lock().unwrap().quota_shard;
                assert_ne!(s4, s1);
            }

            m.prepare_alloc(&entry1, alloc_unit);
            m.prepare_alloc(&entry2, alloc_unit);
            m.prepare_alloc(&entry3, alloc_unit);
            m.prepare_alloc(&entry4, alloc_unit);
            m.prepare_alloc(&entry5, alloc_unit);

            assert_eq!(tasks_count(&m), 5);
            assert_eq!(m.waiting_alloc_size(), 5 * alloc_unit);
            assert_eq!(m.task_num_by_pattern(), [2, 1, 2, 1]);

            assert_eq!(m.under_cancel.lock().unwrap().num, 0);
            assert_eq!(m.run_one_round(), 0);
            assert_eq!(
                m.exec_mu.lock().unwrap().blocked_state.allocated,
                8 * alloc_unit
            );

            assert_eq!(m.wait_alloc(&entry5), ArbitrateResult::Fail);
            assert_eq!(m.wait_alloc(&entry4), ArbitrateResult::Fail);
            assert_eq!(action_cancel.lock().unwrap()[&entry5.pool.uid()], 1);
            assert!(!entry5.arbitrator_mu.lock().unwrap().under_cancel.start);
            assert_eq!(action_cancel.lock().unwrap()[&entry4.pool.uid()], 1);
            assert!(entry4.arbitrator_mu.lock().unwrap().under_cancel.start);
            assert_eq!(m.under_cancel.lock().unwrap().num, 1);
            assert!(m
                .under_cancel
                .lock()
                .unwrap()
                .entries
                .contains_key(&entry4.pool.uid()));
            check_task_exec(&m, PairSuccessFail { succ: 5, fail: 2 }, 0, [1, 0, 0, 1]);
            assert_eq!(m.task_num_by_pattern(), [1, 1, 1, 0]);
            assert_eq!(tasks_count(&m), 3);
            assert_eq!(m.waiting_alloc_size(), 3 * alloc_unit);

            let ori_metrics = m.exec_metrics();
            assert_eq!(m.run_one_round(), 0);
            assert_eq!(
                m.exec_mu.lock().unwrap().blocked_state.allocated,
                8 * alloc_unit
            );
            assert_eq!(m.exec_metrics(), ori_metrics);

            reset_entries_for_test(&m, &[&entry5]);
            assert_eq!(m.run_one_round(), 1);
            assert_eq!(m.wait_alloc(&entry3), ArbitrateResult::Ok);
            check_task_exec(&m, PairSuccessFail { succ: 6, fail: 2 }, 0, [1, 0, 0, 1]);
            assert_eq!(tasks_count(&m), 2);
            assert_eq!(m.waiting_alloc_size(), 2 * alloc_unit);
            set_limit_for_test(&m, alloc_unit);
            reset_entries_for_test(&m, &[&entry3, &entry4]);
            assert_eq!(m.under_cancel.lock().unwrap().num, 0);

            assert_eq!(m.run_one_round(), 0);
            assert_eq!(
                m.exec_mu.lock().unwrap().blocked_state.allocated,
                2 * alloc_unit
            );

            check_task_exec(&m, PairSuccessFail { succ: 6, fail: 2 }, 0, [2, 0, 0, 1]);
            assert_eq!(m.under_cancel.lock().unwrap().num, 1);
            assert!(m
                .under_cancel
                .lock()
                .unwrap()
                .entries
                .contains_key(&entry1.pool.uid()));
            assert!(entry1.arbitrator_mu.lock().unwrap().under_cancel.start);
            assert_eq!(m.wait_alloc(&entry1), ArbitrateResult::Fail);
            check_task_exec(&m, PairSuccessFail { succ: 6, fail: 3 }, 0, [2, 0, 0, 1]);
            assert_eq!(action_cancel.lock().unwrap()[&entry1.pool.uid()], 1);

            set_limit_for_test(&m, 3 * alloc_unit);
            assert_eq!(m.run_one_round(), 1);
            assert_eq!(m.wait_alloc(&entry2), ArbitrateResult::Ok);
            check_task_exec(&m, PairSuccessFail { succ: 7, fail: 3 }, 0, [2, 0, 0, 1]);
            assert_eq!(tasks_count(&m), 0);

            reset_entries_for_test(&m, &[&entry1, &entry2]);
            reset_exec_metrics_for_test(&m);
            action_cancel.lock().unwrap().clear();

            let restart = |e: &Arc<RootPoolEntry>, prio| {
                let (c, h) = gen_test_ctx(e, prio, NO_WAIT_AVERSE, false);
                assert!(m.restart_entry_by_context(
                    RootPoolWrap {
                        entry: Some(Arc::clone(e))
                    },
                    Some(c)
                ));
                h
            };
            let _ra = restart(&entry1, ArbitrationPriority::Low);
            let _rb = restart(&entry2, ArbitrationPriority::Medium);
            let _rc = restart(&entry3, ArbitrationPriority::High);
            let _rd = restart(&entry4, ArbitrationPriority::Low);
            let _re = restart(&entry5, ArbitrationPriority::Low);

            set_limit_for_test(&m, BASE_QUOTA_UNIT * 8);

            m.prepare_alloc(&entry1, 1);
            m.prepare_alloc(&entry3, BASE_QUOTA_UNIT * 4);
            m.prepare_alloc(&entry4, BASE_QUOTA_UNIT);
            m.prepare_alloc(&entry5, BASE_QUOTA_UNIT * 2);

            assert_eq!(tasks_count(&m), 4);
            assert_eq!(m.task_num_by_pattern(), [3, 0, 1, 0]);
            assert_eq!(m.run_one_round(), 4);

            let alloced = entry1.arbitrator_mu.lock().unwrap().quota
                + entry3.arbitrator_mu.lock().unwrap().quota
                + entry4.arbitrator_mu.lock().unwrap().quota
                + entry5.arbitrator_mu.lock().unwrap().quota;
            assert_eq!(m.allocated(), alloced);
            assert_eq!(m.task_num_by_pattern(), [0, 0, 0, 0]);

            assert_eq!(m.wait_alloc(&entry1), ArbitrateResult::Ok);
            assert_eq!(m.wait_alloc(&entry3), ArbitrateResult::Ok);
            assert_eq!(m.wait_alloc(&entry4), ArbitrateResult::Ok);
            assert_eq!(m.wait_alloc(&entry5), ArbitrateResult::Ok);
            assert_eq!(tasks_count(&m), 0);
            check_task_exec(&m, PairSuccessFail { succ: 4, fail: 0 }, 0, [0; 4]);

            m.prepare_alloc(&entry2, BASE_QUOTA_UNIT * 3);
            assert_eq!(tasks_count(&m), 1);

            assert_eq!(m.run_one_round(), 0);
            assert_eq!(action_cancel.lock().unwrap().len(), 2);
            assert_eq!(action_cancel.lock().unwrap()[&entry4.pool.uid()], 1);
            assert_eq!(action_cancel.lock().unwrap()[&entry5.pool.uid()], 1);
            check_task_exec(&m, PairSuccessFail { succ: 4, fail: 0 }, 0, [2, 0, 0, 0]);
            assert_eq!(m.task_num_by_pattern(), [0, 1, 0, 0]);
            assert_eq!(tasks_count(&m), 1);
            assert_eq!(m.under_cancel.lock().unwrap().num, 2);
            assert!(entry4.arbitrator_mu.lock().unwrap().under_cancel.start);
            assert!(entry5.arbitrator_mu.lock().unwrap().under_cancel.start);

            m.reset_root_pool_by_id(entry4.pool.uid(), 0, false);

            let (self_cancel_handle, self_cancel_ch) = cancel_channel();
            let prio4 = entry4.mem_priority();
            assert!(m.restart_entry_by_context(
                RootPoolWrap {
                    entry: Some(Arc::clone(&entry4))
                },
                Some(ArbitrationContext::new(
                    Some(self_cancel_ch),
                    None,
                    prio4,
                    NO_WAIT_AVERSE,
                    false
                ))
            ));
            m.prepare_alloc(&entry4, BASE_QUOTA_UNIT * 1000);
            assert_eq!(tasks_count(&m), 2);

            assert!(
                !entry4.state_mu.stop.load(SeqCst)
                    && entry4.exec_state() != EntryExecState::Idle
                    && entry4.state_mu.quota_to_reclaim.load(SeqCst) > 0
            );
            assert!(entry4.not_running());

            assert_eq!(m.cleanup_fifo.lock().unwrap().size(), 1);
            assert!(Arc::ptr_eq(
                &m.cleanup_fifo.lock().unwrap().front().unwrap(),
                &entry4
            ));
            assert_eq!(m.run_one_round(), 0);
            assert_eq!(m.cleanup_fifo.lock().unwrap().size(), 0);
            assert_eq!(m.task_num_by_pattern(), [1, 1, 0, 0]);
            assert!(!entry4.arbitrator_mu.lock().unwrap().under_cancel.start);
            assert!(!entry4.not_running());

            check_task_exec(&m, PairSuccessFail { succ: 4, fail: 0 }, 0, [2, 0, 0, 0]);
            {
                let uc = m.under_cancel.lock().unwrap();
                assert_eq!(uc.num, 1);
                assert!(uc.entries.contains_key(&entry5.pool.uid()));
            }

            self_cancel_handle.close();
            assert!(m.remove_task(&entry4));
            assert_eq!(m.task_num_by_pattern(), [0, 1, 0, 0]);

            let ori_fail_cnt = m.exec_metrics.task_fail.load(SeqCst);
            let m2 = Arc::clone(&m);
            let e4 = Arc::clone(&entry4);
            let h = std::thread::spawn(move || {
                while ori_fail_cnt == m2.exec_metrics.task_fail.load(SeqCst) {
                    std::thread::yield_now();
                }
                e4.wind_up(0, ArbitrateResult::Ok);
            });
            assert_eq!(m.wait_alloc(&entry4), ArbitrateResult::Fail);
            h.join().unwrap();
            check_task_exec(&m, PairSuccessFail { succ: 4, fail: 1 }, 0, [2, 0, 0, 0]);
            assert!(m.remove_root_pool_entry(&entry5));

            assert!(entry5.not_running());
            assert!(entry5.state_mu.stop.load(SeqCst));
            assert!(!entry5.arbitrator_mu.lock().unwrap().destroyed);
            assert!(entry5.arbitrator_mu.lock().unwrap().under_cancel.start);
            assert_ne!(entry5.arbitrator_mu.lock().unwrap().quota, 0);

            assert_eq!(m.cleanup_fifo.lock().unwrap().size(), 1);
            assert_eq!(m.run_one_round(), 1);
            assert_eq!(m.under_cancel.lock().unwrap().num, 0);
            assert_eq!(m.cleanup_fifo.lock().unwrap().size(), 0);
            assert_eq!(m.exec_metrics().cancel.priority_mode, [2, 0, 0]);
            assert_eq!(m.exec_metrics().cancel.wait_averse, 0);

            assert!(entry5.arbitrator_mu.lock().unwrap().destroyed);
            assert!(!entry5.arbitrator_mu.lock().unwrap().under_cancel.start);
            assert_eq!(entry5.arbitrator_mu.lock().unwrap().quota, 0);

            assert_eq!(m.allocated(), alloced);
            assert_eq!(m.wait_alloc(&entry2), ArbitrateResult::Ok);
            assert_eq!(
                m.exec_metrics().task.pair,
                PairSuccessFail { succ: 5, fail: 1 }
            );
            cleanup_notifer(&m);
        }

        // drain the result channel state left by the raw wind_up above
        let _ = entry1;
    }

    fn set_mem_stats_for_test(
        m: &MemArbitrator,
        alloc: i64,
        heap_inuse: i64,
        total_alloc: i64,
        mem_off_heap: i64,
    ) {
        let last_gc = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;
        m.set_runtime_mem_stats(MemStats {
            heap_alloc: alloc,
            heap_inuse,
            total_free: total_alloc - alloc,
            mem_off_heap,
            last_gc,
        });
    }

    fn clean_digest_profile_for_test(m: &MemArbitrator) {
        let n = m.digest_num.load(SeqCst);
        assert_eq!(m.shrink_digest_profile(DEF_MAX, 0, 0), n);
        assert_eq!(m.digest_num.load(SeqCst), 0);
    }

    struct MockClock {
        t: Arc<Mutex<SystemTime>>,
    }
    impl MockClock {
        fn install(t0: SystemTime) -> MockClock {
            let t = Arc::new(Mutex::new(t0));
            let t2 = Arc::clone(&t);
            *test_time::MOCK_NOW_DYN.lock().unwrap() = Some(Box::new(move || *t2.lock().unwrap()));
            MockClock { t }
        }
        fn set(&self, v: SystemTime) {
            *self.t.lock().unwrap() = v;
        }
    }
    impl Drop for MockClock {
        fn drop(&mut self) {
            *test_time::MOCK_NOW_DYN.lock().unwrap() = None;
        }
    }

    // Serialize the two clock-mocking tests.
    static CLOCK_TEST_GUARD: Mutex<()> = Mutex::new(());

    #[test]
    #[allow(unused_assignments)] // `tl_pre`/`tl_now` mirror Go's `nextTime()` sequencing
    fn mem_arbitrator() {
        let _guard = CLOCK_TEST_GUARD.lock().unwrap_or_else(|e| e.into_inner());
        let (m, recorder) = new_arbitrator_for_test(3, -1);
        let new_limit = 1_000_000_000i64;
        m.set_work_mode(ArbitratorWorkMode::Standard);
        assert!(m.notifer.is_awake());
        m.notifer.wait();
        assert_eq!(m.work_mode(), ArbitratorWorkMode::Standard);

        // Standard mode
        {
            assert_eq!(m.allocated(), 0);
            assert_eq!(m.limit(), DEF_MAX_LIMIT);
            assert!(!m.notifer.is_awake());
            set_limit_for_test(&m, new_limit);
            assert_eq!(m.limit(), new_limit);
            assert_eq!(tasks_count(&m), 0);

            let expect_shard_count = 4usize; // next pow2 of 3
            assert_eq!(m.entry_map.shards.len(), expect_shard_count);
            assert_eq!(m.entry_map.shards_mask, (expect_shard_count - 1) as u64);
            for p in PRIORITIES {
                assert_eq!(
                    m.entry_map.quota_shards[p as usize].len(),
                    m.entry_map.max_quota_shard_index
                );
            }
            assert_eq!(m.run_one_round(), 0);
            check_entries(&m, &[]);

            let pool1_align = 4i64;
            let pool1 = new_resource_pool_for_test("root1", pool1_align);
            let uid1 = pool1.uid();
            let mut pb1 = pool1.create_budget();
            {
                // no out-of-memory action
                assert_eq!(pool1.limit(), DEF_MAX_LIMIT);
                assert!(pb1.grow(1).is_err());
                pb1.grow(0).unwrap();
                assert!(m.get_root_pool_entry(uid1).is_none());
                assert!(!m.notifer.is_awake());
                check_task_exec(&m, PairSuccessFail::default(), 0, [0; 4]);
            }

            let entry1 = add_root_pool_for_test(&m, Arc::clone(&pool1), None);
            {
                // with out-of-memory action
                check_entries(&m, &[&entry1]);
                assert!(!m.notifer.is_awake());
                assert_eq!(tasks_count(&m), 0);

                let m2 = Arc::clone(&m);
                let e1 = Arc::clone(&entry1);
                let u1 = uid1;
                let h = std::thread::spawn(move || {
                    m2.notifer.wait();
                    assert_eq!(tasks_count(&m2), 1);
                    let e = front_task_entry_for_test(&m2).unwrap();
                    assert!(Arc::ptr_eq(&e, &e1));
                    assert!(Arc::ptr_eq(&m2.get_root_pool_entry(u1).unwrap(), &e1));
                    check_entry_quota_by_priority(&m2, &e, ArbitrationPriority::Medium, 0);
                    assert!(find_task_by_mode(
                        &m2,
                        &e,
                        ArbitrationPriority::Medium,
                        NO_WAIT_AVERSE
                    ));
                    assert_eq!(m2.run_one_round(), 1);
                });

                let grow_size = 1i64;
                pb1.grow(grow_size).unwrap();
                h.join().unwrap();

                assert_eq!(m.allocated(), pool1_align);
                assert_eq!(pb1.used(), grow_size);
                assert_eq!(pb1.capacity(), pool1_align);
                assert_eq!(pool1.capacity(), pool1_align);
                assert_eq!(entry1.arbitrator_mu.lock().unwrap().quota, pool1_align);
                assert!(!m.notifer.is_awake());
                assert_eq!(
                    m.exec_metrics().task.pair,
                    PairSuccessFail { succ: 1, fail: 0 }
                );
                cleanup_notifer(&m);
                check_entries(&m, &[&entry1]);

                let m2 = Arc::clone(&m);
                let e1 = Arc::clone(&entry1);
                let h = std::thread::spawn(move || {
                    m2.notifer.wait();
                    assert_eq!(tasks_count(&m2), 1);
                    let e = front_task_entry_for_test(&m2).unwrap();
                    assert!(Arc::ptr_eq(&e, &e1));
                    assert_eq!(m2.run_one_round(), 1);
                });
                let grow_size = pool1_align - grow_size + 1;
                pb1.grow(grow_size).unwrap();
                h.join().unwrap();

                assert_eq!(m.allocated(), 2 * pool1_align);
                assert_eq!(pb1.used(), pool1_align + 1);
                assert_eq!(pb1.capacity(), pool1_align * 2);
                assert_eq!(pool1.capacity(), pool1_align * 2);
                assert_eq!(entry1.arbitrator_mu.lock().unwrap().quota, pool1_align * 2);
                assert!(!m.notifer.is_awake());
                check_task_exec(&m, PairSuccessFail { succ: 2, fail: 0 }, 0, [0; 4]);
                cleanup_notifer(&m);
                check_entries(&m, &[&entry1]);

                // same quota shard as quota 0
                {
                    let st = entry1.arbitrator_mu.lock().unwrap();
                    assert_eq!(
                        st.quota_shard,
                        Some((
                            ArbitrationPriority::Medium,
                            get_quota_shard(0, m.entry_map.max_quota_shard_index)
                        ))
                    );
                }

                let m2 = Arc::clone(&m);
                let e1 = Arc::clone(&entry1);
                let h = std::thread::spawn(move || {
                    m2.notifer.wait();
                    assert_eq!(tasks_count(&m2), 1);
                    let e = front_task_entry_for_test(&m2).unwrap();
                    assert!(Arc::ptr_eq(&e, &e1));
                    assert_eq!(m2.run_one_round(), 1);
                });
                pb1.grow(BASE_QUOTA_UNIT).unwrap();
                h.join().unwrap();
                {
                    let st = entry1.arbitrator_mu.lock().unwrap();
                    assert_eq!(
                        st.quota_shard,
                        Some((
                            ArbitrationPriority::Medium,
                            get_quota_shard(
                                BASE_QUOTA_UNIT * 2 - 1,
                                m.entry_map.max_quota_shard_index
                            )
                        ))
                    );
                }
                check_task_exec(&m, PairSuccessFail { succ: 3, fail: 0 }, 0, [0; 4]);
                reset_entries_for_test(&m, &[&entry1]);
            }

            {
                // illegal operation (standard mode variant)
                let e0 = add_entry_for_test(&m, None);
                m.prepare_alloc(&e0, 1);
                assert_eq!(m.root_pool_num(), 2);
                assert_eq!(m.run_one_round(), 1);
                assert_eq!(m.wait_alloc(&e0), ArbitrateResult::Ok);
                assert_eq!(e0.arbitrator_mu.lock().unwrap().quota, 1);

                m.prepare_alloc(&e0, 1);
                assert_eq!(
                    e0.task_mu.lock().unwrap().fifo_priority,
                    Some(ArbitrationPriority::Medium)
                );
                m.reset_root_pool_entry(&e0);
                let (c, _h) = new_ctx_with_helper(ArbitrationPriority::Low, NO_WAIT_AVERSE, false);
                assert!(m.restart_entry_by_context(
                    RootPoolWrap {
                        entry: Some(Arc::clone(&e0))
                    },
                    Some(c)
                ));
                assert_eq!(e0.mem_priority(), ArbitrationPriority::Low);
                assert_eq!(
                    e0.task_mu.lock().unwrap().fifo_priority,
                    Some(ArbitrationPriority::Medium)
                );
                m.do_execute_first_task();
                assert_eq!(m.wait_alloc(&e0), ArbitrateResult::Ok);
                assert_eq!(e0.arbitrator_mu.lock().unwrap().quota, 2);
                assert_eq!(
                    e0.arbitrator_mu.lock().unwrap().quota,
                    e0.pool.capacity() + e0.state_mu.quota_to_reclaim.load(SeqCst)
                );
                assert_eq!(m.cleanup_fifo.lock().unwrap().size(), 1);

                m.reset_root_pool_entry(&e0);
                assert!(m.remove_root_pool_entry(&e0));
                assert_eq!(m.cleanup_fifo.lock().unwrap().size(), 3);
                m.prepare_alloc(&e0, 1);
                assert_eq!(m.run_one_round(), 0);
                assert_eq!(m.wait_alloc(&e0), ArbitrateResult::Fail);
                assert!(m.get_root_pool_entry(e0.pool.uid()).is_none());
                assert_eq!(m.root_pool_num(), 1);
                m.prepare_alloc(&e0, 1);
                assert_eq!(m.run_one_round(), 1);
                assert_eq!(m.wait_alloc(&e0), ArbitrateResult::Fail);

                let e1 = add_entry_for_test(&m, None);
                assert!(m.remove_root_pool_entry(&e1));
                assert!(!m.reset_root_pool_entry(&e1));
                assert_eq!(m.cleanup_fifo.lock().unwrap().size(), 1);
                m.prepare_alloc(&e1, 1);
                m.do_execute_first_task();
                assert_eq!(m.wait_alloc(&e1), ArbitrateResult::Ok);
                m.do_execute_cleanup_tasks();
            }

            {
                // async run
                assert!(m.restart_entry_by_context(
                    RootPoolWrap {
                        entry: Some(Arc::clone(&entry1))
                    },
                    entry1.load_ctx()
                ));
                let mut b = pool1.create_budget();
                assert!(m.async_run(Duration::from_secs(3600)));
                assert!(!m.async_run(Duration::from_secs(3600)));
                b.grow(BASE_QUOTA_UNIT).unwrap();
                assert!(m.stop());
                assert!(!m.stop());
                assert!(!m.control_running.load(SeqCst));
                assert_eq!(m.allocated(), b.capacity());
                assert_eq!(m.limit(), new_limit);
                check_entries(&m, &[&entry1]);
                reset_entries_for_test(&m, &[&entry1]);

                reset_exec_metrics_for_test(&m);
                let (cancel_handle, cancel_ch) = cancel_channel();
                assert!(m.restart_entry_by_context(
                    RootPoolWrap {
                        entry: Some(Arc::clone(&entry1))
                    },
                    Some(ArbitrationContext::new(
                        Some(cancel_ch),
                        None,
                        ArbitrationPriority::Medium,
                        NO_WAIT_AVERSE,
                        false
                    ))
                ));
                let m2 = Arc::clone(&m);
                let e1 = Arc::clone(&entry1);
                let cancel_slot = Arc::new(Mutex::new(Some(cancel_handle)));
                let cs = Arc::clone(&cancel_slot);
                let h = std::thread::spawn(move || {
                    m2.notifer.wait();
                    assert_eq!(tasks_count(&m2), 1);
                    assert!(Arc::ptr_eq(&front_task_entry_for_test(&m2).unwrap(), &e1));
                    let _ = cs.lock().unwrap().take(); // close(cancel)
                });
                // blocking grow with cancel
                let err = b.grow(BASE_QUOTA_UNIT).unwrap_err();
                assert_eq!(err.to_string(), ERR_ARBITRATE_FAIL);
                h.join().unwrap();
                assert_eq!(tasks_count(&m), 0);
                check_task_exec(&m, PairSuccessFail { succ: 0, fail: 1 }, 0, [0; 4]);
                reset_entries_for_test(&m, &[&entry1]);

                let (cancel_handle, cancel_ch) = cancel_channel();
                let _keep = cancel_handle;
                assert!(m.restart_entry_by_context(
                    RootPoolWrap {
                        entry: Some(Arc::clone(&entry1))
                    },
                    Some(ArbitrationContext::new(
                        Some(cancel_ch),
                        None,
                        ArbitrationPriority::Medium,
                        NO_WAIT_AVERSE,
                        false
                    ))
                ));
                let m2 = Arc::clone(&m);
                let e1 = Arc::clone(&entry1);
                let h = std::thread::spawn(move || {
                    m2.notifer.wait();
                    assert_eq!(tasks_count(&m2), 1);
                    assert!(Arc::ptr_eq(&front_task_entry_for_test(&m2).unwrap(), &e1));
                    assert_eq!(m2.run_one_round(), 0);
                });
                let err = b.grow(new_limit + 1).unwrap_err();
                assert_eq!(err.to_string(), ERR_ARBITRATE_FAIL);
                h.join().unwrap();
                assert_eq!(tasks_count(&m), 0);
                check_task_exec(&m, PairSuccessFail { succ: 0, fail: 2 }, 1, [0; 4]);
            }
            delete_entries_for_test(&m, &[&entry1]);
        }

        // Priority mode
        m.set_work_mode(ArbitratorWorkMode::Priority);
        assert_eq!(m.work_mode(), ArbitratorWorkMode::Priority);
        {
            // error paths (Go: panics in the test helper)
            {
                let pool = ResourcePool::new_default("?", 1);
                pool.start(None, 1);
                assert_eq!(pool.reserved(), 1);
                assert_eq!(
                    m.add_root_pool(pool).err().unwrap().to_string(),
                    "?: has 1 reserved budget left"
                );
            }
            {
                let pool = ResourcePool::new_default("?", 1);
                pool.force_add_cap_unlocked(1);
                assert_ne!(pool.approx_cap(), 0);
                assert_eq!(
                    m.add_root_pool(pool).err().unwrap().to_string(),
                    "?: has 1 bytes budget left"
                );
            }
            {
                let p1 = ResourcePool::new_default("p1", 1);
                let p2 = ResourcePool::new(
                    p1.uid(),
                    "p2",
                    0,
                    1,
                    DEF_MAX_UNUSED_BLOCKS,
                    Default::default(),
                );
                let p3 = ResourcePool::new_default("p3", 1);
                let e1 = add_root_pool_for_test(&m, Arc::clone(&p1), None);
                check_entries(&m, &[&e1]);
                assert_eq!(
                    m.add_root_pool(Arc::clone(&p2)).err().unwrap().to_string(),
                    "p2: already exists"
                );
                p3.start_no_reserved(Some(&p2));
                assert_eq!(
                    m.add_root_pool(p3).err().unwrap().to_string(),
                    "p3: already started with pool p2"
                );
                check_entries(&m, &[&e1]);
                let e = m.get_root_pool_entry(p1.uid()).unwrap();
                delete_entries_for_test(&m, &[&e]);
            }
            check_entries(&m, &[]);
        }
        {
            // prefer privileged budget under priority mode
            let new_limit = 100i64;
            reset_exec_metrics_for_test(&m);
            set_limit_for_test(&m, new_limit);
            let (e1, _h1) =
                new_pool_with_helper(&m, "e1", ArbitrationPriority::Low, NO_WAIT_AVERSE, true);
            e1.load_ctx().unwrap().set_helper(None);
            assert!(m.privileged_entry.lock().unwrap().is_none());
            assert!(e1.ctx.prefer_privilege.load(SeqCst));

            let req_quota = new_limit + 1;

            m.prepare_alloc(&e1, req_quota);
            assert!(find_task_by_mode(
                &m,
                &e1,
                ArbitrationPriority::Low,
                NO_WAIT_AVERSE
            ));
            assert_eq!(m.run_one_round(), 1);
            assert_eq!(m.wait_alloc(&e1), ArbitrateResult::Ok);
            check_task_exec(&m, PairSuccessFail { succ: 1, fail: 0 }, 0, [0; 4]);
            check_entry_quota_by_priority(&m, &e1, ArbitrationPriority::Low, req_quota);
            assert!(Arc::ptr_eq(
                m.privileged_entry.lock().unwrap().as_ref().unwrap(),
                &e1
            ));

            let (e2, _h2) = new_pool_with_helper(&m, "e2", ArbitrationPriority::High, true, true);
            e2.load_ctx().unwrap().set_helper(None);
            assert!(e2.load_ctx().unwrap().prefer_privilege);
            assert!(!e2.ctx.prefer_privilege.load(SeqCst));
            m.prepare_alloc(&e2, req_quota);
            assert!(find_task_by_mode(&m, &e2, ArbitrationPriority::High, true));
            assert_eq!(m.run_one_round(), 0);
            assert_eq!(m.wait_alloc(&e2), ArbitrateResult::Fail);
            check_task_exec(&m, PairSuccessFail { succ: 1, fail: 1 }, 0, [0, 0, 0, 1]);
            check_entries(&m, &[&e1, &e2]);
            assert!(Arc::ptr_eq(
                m.privileged_entry.lock().unwrap().as_ref().unwrap(),
                &e1
            ));

            let (e3, _h3) =
                new_pool_with_helper(&m, "e3", ArbitrationPriority::Low, NO_WAIT_AVERSE, true);
            e3.load_ctx().unwrap().set_helper(None);
            assert!(e3.ctx.prefer_privilege.load(SeqCst));
            m.prepare_alloc(&e3, req_quota);
            assert_eq!(m.run_one_round(), 0);
            assert_eq!(tasks_count(&m), 1);

            m.prepare_alloc(&e1, req_quota);
            assert_eq!(tasks_count(&m), 2);
            assert_eq!(m.run_one_round(), 1);
            assert_eq!(tasks_count(&m), 1);
            assert_eq!(m.wait_alloc(&e1), ArbitrateResult::Ok);
            check_task_exec(&m, PairSuccessFail { succ: 2, fail: 1 }, 0, [0, 0, 0, 1]);

            m.reset_root_pool_entry(&e1);
            assert!(m.restart_entry_by_context(
                RootPoolWrap {
                    entry: Some(Arc::clone(&e1))
                },
                e1.load_ctx()
            ));
            m.prepare_alloc(&e1, req_quota);

            assert_eq!(tasks_count(&m), 2);
            assert!(Arc::ptr_eq(
                m.privileged_entry.lock().unwrap().as_ref().unwrap(),
                &e1
            ));
            assert_eq!(m.run_one_round(), 1);
            assert_eq!(m.wait_alloc(&e3), ArbitrateResult::Ok);
            check_task_exec(&m, PairSuccessFail { succ: 3, fail: 1 }, 0, [0, 0, 0, 1]);
            assert!(Arc::ptr_eq(
                m.privileged_entry.lock().unwrap().as_ref().unwrap(),
                &e3
            ));
            set_limit_for_test(&m, m.waiting_alloc_size() + m.allocated());
            assert_eq!(m.run_one_round(), 1);
            assert_eq!(m.wait_alloc(&e1), ArbitrateResult::Ok);
            check_task_exec(&m, PairSuccessFail { succ: 4, fail: 1 }, 0, [0, 0, 0, 1]);
            assert!(Arc::ptr_eq(
                m.privileged_entry.lock().unwrap().as_ref().unwrap(),
                &e3
            ));
            check_entries(&m, &[&e1, &e2, &e3]);
            delete_entries_for_test(&m, &[&e1, &e2, &e3]);
        }

        {
            // soft-limit & limit
            let x = 1_000_000_000i64;
            assert!(!m.set_limit(u64::MAX));
            assert!(!m.notifer.is_awake());
            assert!(m.set_limit(x as u64));
            assert!(m.notifer.is_awake());
            assert_eq!(m.mu_limit.load(SeqCst), x);
            assert_eq!(
                m.mu_threshold_oom_risk.load(SeqCst),
                (x as f64 * DEF_OOM_RISK_RATIO) as i64
            );
            assert_eq!(
                m.mu_threshold_risk.load(SeqCst),
                (x as f64 * DEF_MEM_RISK_RATIO) as i64
            );
            assert_eq!(
                m.mu_soft_limit_size.load(SeqCst),
                m.mu_threshold_oom_risk.load(SeqCst)
            );
            cleanup_notifer(&m);
            assert!(m.set_limit(DEF_MAX_LIMIT as u64 + 1));
            assert!(m.notifer.is_awake());
            assert_eq!(m.mu_limit.load(SeqCst), DEF_MAX_LIMIT);
            assert_eq!(
                m.mu_threshold_oom_risk.load(SeqCst),
                (DEF_MAX_LIMIT as f64 * DEF_OOM_RISK_RATIO) as i64
            );
            assert_eq!(m.mu_soft_specified_size.load(SeqCst), 0);
            assert_eq!(
                m.mu_soft_limit_size.load(SeqCst),
                m.mu_threshold_oom_risk.load(SeqCst)
            );
            assert_eq!(
                *m.mu_soft_limit_mode.lock().unwrap(),
                SoftLimitMode::Disable
            );

            m.set_soft_limit(1, 0.0, SoftLimitMode::Specified);
            assert!(m.mu_soft_specified_size.load(SeqCst) > 0);
            assert_eq!(m.mu_soft_limit_size.load(SeqCst), 1);
            assert!(m.set_limit(100));
            assert_eq!(m.mu_soft_limit_size.load(SeqCst), 1);

            let rate = 0.8f64;
            m.set_soft_limit(0, rate, SoftLimitMode::Specified);
            assert_eq!(m.mu_soft_specified_size.load(SeqCst), 0);
            assert_eq!(
                m.mu_soft_specified_ratio.load(SeqCst),
                (rate * 1000.0) as i64
            );
            assert_eq!(
                m.mu_soft_limit_size.load(SeqCst),
                (rate * m.mu_limit.load(SeqCst) as f64) as i64
            );
            assert!(m.set_limit(200));
            assert_eq!(m.mu_soft_limit_size.load(SeqCst), (rate * 200.0) as i64);

            m.set_soft_limit(0, 0.0, SoftLimitMode::Disable);
            assert_eq!(
                m.mu_soft_limit_size.load(SeqCst),
                m.mu_threshold_oom_risk.load(SeqCst)
            );
            assert!(m.set_limit(100));
            assert_eq!(
                m.mu_soft_limit_size.load(SeqCst),
                m.mu_threshold_oom_risk.load(SeqCst)
            );

            m.set_soft_limit(0, 0.0, SoftLimitMode::Auto);
            assert_eq!(
                m.mu_soft_limit_size.load(SeqCst),
                m.mu_threshold_oom_risk.load(SeqCst)
            );
            assert!(m.set_limit(200));
            assert_eq!(
                m.mu_soft_limit_size.load(SeqCst),
                m.mu_threshold_oom_risk.load(SeqCst)
            );
        }

        {
            // out-of-control
            m.set_soft_limit(0, 0.0, SoftLimitMode::Disable);
            assert_eq!(m.await_free_pool_cap(), 0);
            assert_eq!(m.allocated(), 0);

            let af = m.await_free_state().unwrap();
            let ele_size = af.pool.alloc_align_size() + 1;
            let budgets_num = af.shards.len() as i64;
            let mut used_heap = ele_size * budgets_num;
            for b in &af.shards {
                b.budget
                    .consume_quota(m.approx_unix_time_sec(), ele_size)
                    .unwrap();
            }
            assert_eq!(m.await_free_pool_used().tracked_heap, 0);
            for b in &af.shards {
                b.report_heap_inuse(ele_size);
            }

            let mut expect = AwaitFreePoolExecMetrics {
                pair: PairSuccessFail {
                    succ: budgets_num,
                    fail: 0,
                },
                shrink: 0,
                force_shrink: 0,
            };
            assert_eq!(m.exec_metrics().await_free, expect);

            expect.pair.fail += 1;
            assert!(af.shards[0].budget.reserve(m.limit()).is_err());
            check_await_free(&m);
            assert_eq!(m.exec_metrics().await_free, expect);

            assert_eq!(m.await_free_pool_used().tracked_heap, used_heap);
            assert_eq!(
                m.await_free_pool_cap(),
                af.pool.round_size(ele_size) * budgets_num
            );
            {
                let ori = now_unix_milli() - DEF_TRACK_MEM_STATS_DUR_MILLI;
                m.heap_tracked_last_update_milli.store(ori, SeqCst);
                assert!(m.try_update_tracked_mem_stats(now_unix_milli()));
                assert_ne!(m.heap_tracked_last_update_milli.load(SeqCst), ori);
            }

            assert_eq!(m.heap_tracked.load(SeqCst), used_heap);
            assert_eq!(m.buffer_size.load(SeqCst), 0);
            assert_eq!(m.avoid_size.load(SeqCst), 0);

            let e1_mem = Arc::new(AtomicI64::new(13));
            let (e1, h1) =
                new_pool_with_helper(&m, "e1", ArbitrationPriority::Medium, NO_WAIT_AVERSE, false);
            let e1m = Arc::clone(&e1_mem);
            h1.set_heap_used_cb(move || e1m.load(SeqCst));
            let e2 = add_entry_for_test(&m, None);
            let e3 = add_entry_for_test(&m, Some(new_def_ctx(ArbitrationPriority::Medium)));
            m.update_tracked_heap_stats();
            used_heap += 13;
            assert_eq!(m.heap_tracked.load(SeqCst), used_heap);
            assert_eq!(m.buffer_size.load(SeqCst), 13);
            assert_eq!(m.avoid_size.load(SeqCst), 0);

            let free = 1024i64;
            e1_mem.store(17, SeqCst);
            set_mem_stats_for_test(&m, used_heap + 1, used_heap + 100, used_heap + free, 67);
            assert_eq!(m.hc_heap_inuse.load(SeqCst), used_heap + 100);
            assert_eq!(m.hc_heap_alloc.load(SeqCst), used_heap + 1);
            assert_eq!(m.hc_heap_total_free.load(SeqCst), free - 1);
            assert_eq!(m.hc_mem_inuse.load(SeqCst), used_heap + 100 + 67);
            assert_eq!(m.heap_tracked.load(SeqCst), used_heap);
            assert_eq!(m.buffer_size.load(SeqCst), 13); // no update
            assert_eq!(
                m.avoid_size.load(SeqCst),
                m.hc_heap_alloc.load(SeqCst) + m.hc_mem_off_heap.load(SeqCst)
                    - m.heap_tracked.load(SeqCst)
            );
            assert!(
                m.avoid_size.load(SeqCst)
                    > m.mu_limit.load(SeqCst) - m.mu_soft_limit_size.load(SeqCst)
            );

            m.update_tracked_heap_stats();
            assert_eq!(m.buffer_size.load(SeqCst), 17);
            e1_mem.store(3, SeqCst);
            m.update_tracked_heap_stats();
            assert_eq!(m.buffer_size.load(SeqCst), 17); // no update to smaller size

            let now = SystemTime::now();
            let now_unix = now
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            for (i, b) in af.shards.iter().enumerate() {
                if i % 2 == 0 {
                    b.budget.used.store(0, SeqCst);
                    if i == 0 {
                        b.budget.set_last_used_time_sec(
                            now_unix - (DEF_AWAIT_FREE_POOL_SHRINK_DUR_MILLI / KILO - 1),
                        );
                    } else {
                        b.budget.set_last_used_time_sec(0);
                    }
                } else {
                    b.budget.used.store(1, SeqCst);
                }
            }
            cleanup_notifer(&m);
            assert!(!m.notifer.is_awake());
            {
                let now_milli = now
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64;
                let ori = now_milli - DEF_AWAIT_FREE_POOL_SHRINK_DUR_MILLI;
                m.await_free_last_shrink_milli.store(ori, SeqCst);
                assert!(m.try_shrink_await_free_pool(0, now_milli));
                assert!(m.able_to_gc());
                assert_ne!(m.await_free_last_shrink_milli.load(SeqCst), ori);
            }
            assert!(m.notifer.is_awake());
            expect.shrink += 1;
            assert_eq!(m.exec_metrics().await_free, expect);
            check_await_free(&m);

            for (i, b) in af.shards.iter().enumerate() {
                if i % 2 == 0 {
                    assert_eq!(b.budget.used.load(SeqCst), 0);
                    if i == 0 {
                        assert_ne!(b.budget.capacity(), 0);
                    } else {
                        assert_eq!(b.budget.capacity(), 0);
                    }
                } else {
                    assert_eq!(b.budget.used.load(SeqCst), 1);
                }
            }
            assert_eq!(m.await_free_pool_cap(), m.allocated());
            m.set_buffer_size(0);
            check_entries(&m, &[&e1, &e2, &e3]);
            delete_entries_for_test(&m, &[&e1, &e2, &e3]);
            check_entries(&m, &[]);
            reset_await_free_for_test(&m);
            set_mem_stats_for_test(&m, 0, 0, 0, 0);
            assert_eq!(m.allocated(), 0);
        }

        {
            // calc buffer & digest cache
            reset_exec_metrics_for_test(&m);
            m.try_to_update_buffer(2, DEF_UPDATE_BUFFER_TIME_ALIGN_SEC);
            assert_eq!(m.buffer_size.load(SeqCst), 2);
            m.try_to_update_buffer(1, DEF_UPDATE_BUFFER_TIME_ALIGN_SEC);
            assert_eq!(m.buffer_size.load(SeqCst), 2);
            m.try_to_update_buffer(4, DEF_UPDATE_BUFFER_TIME_ALIGN_SEC);
            assert_eq!(m.buffer_size.load(SeqCst), 4);
            m.try_to_update_buffer(1, DEF_UPDATE_BUFFER_TIME_ALIGN_SEC);
            assert_eq!(m.buffer_size.load(SeqCst), 4);
            m.try_to_update_buffer(
                1,
                DEF_UPDATE_BUFFER_TIME_ALIGN_SEC * (DEF_REDUNDANCY as i64),
            );
            assert_eq!(m.buffer_size.load(SeqCst), 4);
            m.try_to_update_buffer(
                3,
                DEF_UPDATE_BUFFER_TIME_ALIGN_SEC * (DEF_REDUNDANCY as i64 + 1),
            );
            assert_eq!(m.buffer_size.load(SeqCst), 3);
            m.try_to_update_buffer(
                1,
                DEF_UPDATE_BUFFER_TIME_ALIGN_SEC * (DEF_REDUNDANCY as i64 + 1),
            );
            assert_eq!(m.buffer_size.load(SeqCst), 3);
            m.set_buffer_size(0);

            m.set_limit(10000);
            assert_eq!(
                *m.pool_alloc_stats.read().unwrap(),
                PoolAllocProfile {
                    small_pool_limit: 10,
                    pool_alloc_unit: 20,
                    max_pool_alloc_unit: 100
                }
            );

            let digest_id1 = hash_str("test");
            let digest_id2 = digest_id1 + 1;
            assert!(m.get_digest_profile_cache(digest_id1, 1).is_none());
            assert_eq!(m.digest_num.load(SeqCst), 0);
            m.update_digest_profile_cache(digest_id1, 1009, 0);
            assert_eq!(m.digest_num.load(SeqCst), 1);
            let shard1 = m.digest_shard(digest_id1);
            {
                assert_eq!(m.get_digest_profile_cache(digest_id1, 2), Some(1009));
                assert_eq!(shard1.num.load(SeqCst), 1);
                let pf = shard1
                    .map
                    .lock()
                    .unwrap()
                    .get(&digest_id1)
                    .cloned()
                    .unwrap();
                assert_eq!(pf.last_fetch_utime_sec.load(SeqCst), 2);
            }
            m.update_digest_profile_cache(digest_id2, 7, 0);
            {
                assert_eq!(m.get_digest_profile_cache(digest_id2, 5), Some(7));
                assert_eq!(m.digest_num.load(SeqCst), 2);
                let shard = m.digest_shard(digest_id2);
                let pf = shard.map.lock().unwrap().get(&digest_id2).cloned().unwrap();
                assert_eq!(pf.last_fetch_utime_sec.load(SeqCst), 5);
            }
            m.update_digest_profile_cache(digest_id1, 107, 0);
            {
                assert_eq!(m.get_digest_profile_cache(digest_id1, 3), Some(1009));
                let pf = shard1
                    .map
                    .lock()
                    .unwrap()
                    .get(&digest_id1)
                    .cloned()
                    .unwrap();
                assert_eq!(pf.last_fetch_utime_sec.load(SeqCst), 3);
            }
            m.update_digest_profile_cache(digest_id1, 107, DEF_UPDATE_BUFFER_TIME_ALIGN_SEC);
            assert_eq!(m.get_digest_profile_cache(digest_id1, 4), Some(1009));
            m.update_digest_profile_cache(digest_id1, 107, DEF_UPDATE_BUFFER_TIME_ALIGN_SEC * 2);
            assert_eq!(m.get_digest_profile_cache(digest_id1, 4), Some(107));
            clean_digest_profile_for_test(&m);

            m.set_limit(5000 * 1000);
            assert_eq!(
                *m.pool_alloc_stats.read().unwrap(),
                PoolAllocProfile {
                    small_pool_limit: 5000,
                    pool_alloc_unit: 10000,
                    max_pool_alloc_unit: 50000
                }
            );

            let test_now = DEF_DIGEST_PROFILE_MEM_TIMEOUT_SEC + 1;
            let mut uid = 107u64;
            let data: [(i64, i64, i64); 6] = [
                (5003, 0, 2),
                (7, 0, 2),
                (4099, test_now - DEF_DIGEST_PROFILE_SMALL_MEM_TIMEOUT_SEC, 2),
                (10003, test_now, 2),
                (5, test_now, 2),
                (4111, test_now, 2),
            ];
            for (v, utime, cnt) in data {
                for _ in 0..cnt {
                    m.update_digest_profile_cache(uid, v, utime);
                    uid += 1;
                }
            }
            assert_eq!(m.digest_num.load(SeqCst), 12);
            assert_eq!(m.shrink_digest_profile(test_now, DEF_MAX, 0), 0);
            assert_eq!(m.shrink_digest_profile(test_now, 11, 9), 4);
            assert_eq!(m.digest_num.load(SeqCst), 8);
            assert_eq!(m.shrink_digest_profile(test_now, 0, 4), 4);
            clean_digest_profile_for_test(&m);
        }

        {
            // gc when quota not available
            let limit = 1000i64;
            let heap = Arc::new(AtomicI64::new(500));
            let buffer = 100i64;
            let alloc = 500i64;

            m.set_limit(limit as u64);
            m.hc_heap_alloc.store(heap.load(SeqCst), SeqCst);
            m.hc_heap_inuse.store(heap.load(SeqCst), SeqCst);
            m.set_buffer_size(buffer);
            m.hc_last_gc_utime.store(0, SeqCst);
            m.hc_last_gc_heap_alloc.store(0, SeqCst);
            assert_eq!(m.exec_metrics().action.gc, 0);
            assert!(m.is_mem_safe());

            let expect_buffer_size = Arc::new(AtomicI64::new(-1));
            let gc_cnt = Arc::new(AtomicI64::new(0));
            let gc_ut = Arc::new(AtomicI64::new(
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as i64,
            ));
            {
                let m2 = Arc::clone(&m);
                let ebs = Arc::clone(&expect_buffer_size);
                let g = Arc::clone(&gc_cnt);
                let m3 = Arc::clone(&m);
                let heap2 = Arc::clone(&heap);
                let gcu = Arc::clone(&gc_ut);
                set_actions(
                    &m,
                    |_| {},
                    |_| {},
                    |_| {},
                    Some(Box::new(move || {
                        m3.set_runtime_mem_stats(MemStats {
                            heap_alloc: heap2.load(SeqCst),
                            heap_inuse: heap2.load(SeqCst) + 100,
                            total_free: 0,
                            mem_off_heap: 3,
                            last_gc: gcu.load(SeqCst),
                        });
                    })),
                    Some(Box::new(move || {
                        assert_eq!(m2.buffer_size.load(SeqCst), ebs.load(SeqCst));
                        g.fetch_add(1, SeqCst);
                    })),
                );
            }
            let make_gc_able = |m: &MemArbitrator| {
                m.mu_last_gc.store(
                    m.mu_released
                        .load(SeqCst)
                        .wrapping_sub(m.pool_alloc_stats.read().unwrap().small_pool_limit as u64),
                    SeqCst,
                );
            };
            make_gc_able(&m);
            let (e1, h1) =
                new_pool_with_helper(&m, "e1", ArbitrationPriority::Medium, NO_WAIT_AVERSE, false);
            let e1_mem_used = Arc::new(AtomicI64::new(0));
            let e1m = Arc::clone(&e1_mem_used);
            h1.set_heap_used_cb(move || e1m.load(SeqCst));
            m.prepare_alloc(&e1, alloc);
            expect_buffer_size.store(100, SeqCst);
            assert_eq!(m.run_one_round(), 0);
            assert_eq!(m.exec_metrics().action.gc, 1);
            assert_eq!(gc_cnt.load(SeqCst), 1);
            assert_eq!(m.hc_heap_alloc.load(SeqCst), heap.load(SeqCst));
            assert_eq!(m.hc_heap_inuse.load(SeqCst), heap.load(SeqCst) + 100);
            assert_eq!(m.hc_last_gc_heap_alloc.load(SeqCst), heap.load(SeqCst));
            assert_eq!(m.hc_last_gc_utime.load(SeqCst), gc_ut.load(SeqCst));

            // unable to trigger runtime GC
            assert_eq!(m.run_one_round(), 0);
            assert_eq!(m.exec_metrics().action.gc, 1);
            assert_eq!(gc_cnt.load(SeqCst), 1);

            heap.store(450, SeqCst); // -50
            let last_alloc = m.hc_last_gc_heap_alloc.load(SeqCst);
            make_gc_able(&m);
            assert_eq!(m.run_one_round(), 0);
            assert_eq!(m.exec_metrics().action.gc, 2);
            assert_eq!(gc_cnt.load(SeqCst), 2);
            assert_eq!(m.hc_heap_alloc.load(SeqCst), 450);
            assert_eq!(m.hc_heap_inuse.load(SeqCst), 550);
            assert_eq!(m.hc_last_gc_heap_alloc.load(SeqCst), last_alloc); // no gc
            assert_eq!(m.hc_last_gc_utime.load(SeqCst), gc_ut.load(SeqCst));
            assert_eq!(m.avoid_size.load(SeqCst), 450 + 3);

            heap.store(390, SeqCst); // -60
            gc_ut.store(
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as i64,
                SeqCst,
            );
            make_gc_able(&m);
            assert_eq!(m.run_one_round(), 1);
            assert_eq!(m.wait_alloc(&e1), ArbitrateResult::Ok);
            assert_eq!(m.exec_metrics().action.gc, 3);
            assert_eq!(gc_cnt.load(SeqCst), 3);
            assert_eq!(m.hc_heap_alloc.load(SeqCst), 390);
            assert_eq!(m.hc_last_gc_heap_alloc.load(SeqCst), 390);
            assert_eq!(m.hc_last_gc_utime.load(SeqCst), gc_ut.load(SeqCst));
            assert_eq!(m.heap_tracked.load(SeqCst), 0);
            assert_eq!(m.avoid_size.load(SeqCst), 390 + 3);

            m.prepare_alloc(&e1, 50);
            assert_eq!(m.run_one_round(), 0);
            assert_eq!(m.exec_metrics().action.gc, 3);
            assert_eq!(gc_cnt.load(SeqCst), 3);
            assert_eq!(m.avoid_size.load(SeqCst), 390 + 3);
            assert_eq!(m.buffer_size.load(SeqCst), 100);

            gc_ut.store(
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as i64,
                SeqCst,
            );
            e1_mem_used.store(150, SeqCst);
            make_gc_able(&m);
            assert!(e1_mem_used.load(SeqCst) > m.buffer_size.load(SeqCst));
            expect_buffer_size.store(150, SeqCst);
            assert_eq!(m.run_one_round(), 1);
            assert_eq!(m.wait_alloc(&e1), ArbitrateResult::Ok);

            assert_eq!(m.exec_metrics().action.gc, 4);
            assert_eq!(gc_cnt.load(SeqCst), 4);
            assert_eq!(m.hc_heap_alloc.load(SeqCst), 390);
            assert_eq!(m.hc_last_gc_heap_alloc.load(SeqCst), 390);
            assert_eq!(m.hc_last_gc_utime.load(SeqCst), gc_ut.load(SeqCst));
            assert_eq!(m.heap_tracked.load(SeqCst), 150);
            assert_eq!(m.avoid_size.load(SeqCst), 390 + 3 - 150);

            delete_entries_for_test(&m, &[&e1]);
            check_entries(&m, &[]);
            m.update_tracked_heap_stats();
            set_mem_stats_for_test(&m, 0, 0, 0, 0);
        }
        let _ = recorder;

        // From here on the test drives the arbitrator's clock explicitly
        // (Go `mockNow`).
        let clock = MockClock::install(SystemTime::UNIX_EPOCH);

        {
            // mem risk
            let new_limit = 100_000i64;
            const HEAP_INUSE_RATE_MILLI: i64 = (DEF_OOM_RISK_RATIO * KILO as f64) as i64;
            let mock_heap = Arc::new(Mutex::new([
                multi_ratio(new_limit, HEAP_INUSE_RATE_MILLI),
                multi_ratio(new_limit, HEAP_INUSE_RATE_MILLI),
                0,
                0,
            ]));
            let t_update = Arc::new(AtomicI64::new(0));
            let t_gc = Arc::new(AtomicI64::new(0));
            let t_record_fail = Arc::new(AtomicI64::new(0));
            let t_record_succ = Arc::new(AtomicI64::new(0));
            let logs_info = Arc::new(AtomicI64::new(0));
            let logs_warn = Arc::new(AtomicI64::new(0));
            let logs_error = Arc::new(AtomicI64::new(0));
            let install_actions = |m: &Arc<MemArbitrator>| {
                let mu = Arc::clone(&t_update);
                let mg = Arc::clone(&t_gc);
                let li = Arc::clone(&logs_info);
                let lw = Arc::clone(&logs_warn);
                let le = Arc::clone(&logs_error);
                let m3 = Arc::clone(m);
                let mh = Arc::clone(&mock_heap);
                set_actions(
                    m,
                    move |_| {
                        li.fetch_add(1, SeqCst);
                    },
                    move |_| {
                        lw.fetch_add(1, SeqCst);
                    },
                    move |_| {
                        le.fetch_add(1, SeqCst);
                    },
                    Some(Box::new(move || {
                        mu.fetch_add(1, SeqCst);
                        let h = *mh.lock().unwrap();
                        set_mem_stats_for_test(&m3, h[0], h[1], h[2], h[3]);
                    })),
                    Some(Box::new(move || {
                        mg.fetch_add(1, SeqCst);
                    })),
                );
            };
            install_actions(&m);
            let check_logs = |i: i64, w: i64, e: i64| {
                assert_eq!(
                    (
                        logs_info.load(SeqCst),
                        logs_warn.load(SeqCst),
                        logs_error.load(SeqCst)
                    ),
                    (i, w, e)
                );
            };

            reset_exec_metrics_for_test(&m);
            assert_eq!(m.hc_heap_alloc.load(SeqCst), 0);
            assert_eq!(m.hc_heap_inuse.load(SeqCst), 0);
            assert_eq!(m.hc_heap_total_free.load(SeqCst), 0);
            assert_eq!(m.heap_tracked.load(SeqCst), 0);
            assert_eq!(m.avoid_size.load(SeqCst), m.limit() - m.soft_limit_i());
            assert_eq!(m.mem_magnif(), 0);
            m.set_limit(new_limit as u64);
            m.set_soft_limit(0, 0.0, SoftLimitMode::Disable);
            assert!(m.is_mem_safe());
            let (e1, h1) =
                new_pool_with_helper(&m, "e1", ArbitrationPriority::Low, NO_WAIT_AVERSE, true);
            let (e2, h2) =
                new_pool_with_helper(&m, "e2", ArbitrationPriority::Low, NO_WAIT_AVERSE, true);
            let (e3, h3) =
                new_pool_with_helper(&m, "e3", ArbitrationPriority::High, NO_WAIT_AVERSE, true);
            let (e4, h4) =
                new_pool_with_helper(&m, "e4", ArbitrationPriority::Low, NO_WAIT_AVERSE, true);
            let (e5, h5) =
                new_pool_with_helper(&m, "e5", ArbitrationPriority::Low, NO_WAIT_AVERSE, true);
            {
                m.set_unix_time_sec(0);
                assert!(m.consume_quota_from_await_free_pool(0, 1000));
                {
                    m.prepare_alloc(&e1, 1000);
                    m.prepare_alloc(&e2, 5000);
                    m.prepare_alloc(&e3, 9000);
                    assert_eq!(m.run_one_round(), 3);
                    assert_eq!(m.wait_alloc(&e1), ArbitrateResult::Ok);
                    assert_eq!(m.wait_alloc(&e2), ArbitrateResult::Ok);
                    assert_eq!(m.wait_alloc(&e3), ArbitrateResult::Ok);
                }
                assert_eq!(m.allocated(), 16000);
                assert_eq!(m.root_pool_num(), 5);
                {
                    let s1 = e1.arbitrator_mu.lock().unwrap().quota_shard;
                    let s2 = e2.arbitrator_mu.lock().unwrap().quota_shard;
                    let s3 = e3.arbitrator_mu.lock().unwrap().quota_shard;
                    assert_ne!(s1, s2);
                    assert_ne!(s1, s3);
                    assert_ne!(s2, s3);
                }
                assert_eq!(
                    m.exec_metrics().task.pair,
                    PairSuccessFail { succ: 3, fail: 0 }
                );
                h1.set_heap_used_cb(|| 1009);
            }
            let ori_released = m.mu_released.load(SeqCst);
            let ori_allocated = m.allocated();
            m.refresh_runtime_mem_stats();
            assert_eq!(m.mu_released.load(SeqCst), ori_released + 1000);
            assert_eq!(m.allocated(), ori_allocated - 1000);
            assert_eq!(
                m.soft_limit_i(),
                multi_ratio(new_limit, HEAP_INUSE_RATE_MILLI)
            );
            assert_eq!(
                m.avoid_size.load(SeqCst),
                multi_ratio(new_limit, HEAP_INUSE_RATE_MILLI)
            );
            assert!(!m.is_mem_safe());

            *mock_heap.lock().unwrap() = [
                multi_ratio(new_limit, 900),
                multi_ratio(new_limit, HEAP_INUSE_RATE_MILLI),
                multi_ratio(new_limit, 900) + 500,
                multi_ratio(new_limit, 50),
            ];
            m.refresh_runtime_mem_stats();
            assert_eq!(m.hc_heap_total_free.load(SeqCst), 500);
            assert!(!m.is_mem_safe());

            assert_eq!(m.exec_metrics().risk, ExecMetricsRisk::default());
            assert!(!m.at_mem_risk());
            assert_eq!(m.min_heap_free_bps(), DEF_MIN_HEAP_FREE_BPS);
            assert_eq!(m.mem_magnif(), 0);
            reset_exec_metrics_for_test(&m);

            t_update.store(0, SeqCst);
            t_gc.store(0, SeqCst);
            logs_info.store(0, SeqCst);
            logs_warn.store(0, SeqCst);
            logs_error.store(0, SeqCst);

            {
                let rf = Arc::clone(&t_record_fail);
                recorder.set_store(move |_| {
                    rf.fetch_add(1, SeqCst);
                    Err("test".to_string())
                });
            }
            *m.hc_last_mem_state.lock().unwrap() = Some(RuntimeMemStateV1 {
                version: 1,
                magnif: 1111,
                ..Default::default()
            });
            m.set_soft_limit(0, 0.0, SoftLimitMode::Auto);
            m.set_min_heap_free_bps(2); // 2 B/s
            let start_time = SystemTime::now();
            clock.set(start_time);
            assert_eq!(m.min_heap_free_bps(), 2);
            {
                assert_eq!(m.run_one_round(), -2);
                assert!(m.at_mem_risk());
                {
                    let risk = m.hc_mem_risk.lock().unwrap();
                    assert_eq!(risk.start_time, start_time);
                    assert_eq!(risk.last_stats_start_time, start_time);
                    assert_eq!(risk.last_heap_total_free, 500);
                }
                let last = m.last_mem_state().unwrap();
                assert_eq!(
                    last,
                    RuntimeMemStateV1 {
                        version: 1,
                        last_risk: LastRisk {
                            heap_alloc: multi_ratio(new_limit, 900),
                            quota_alloc: 15000,
                        },
                        magnif: 6100,
                        pool_medium_cap: 0,
                    }
                );
                assert_eq!(m.mem_magnif(), last.magnif);
                assert_eq!(m.heap_tracked.load(SeqCst), 0);
                assert_eq!(m.avoid_size.load(SeqCst), 95000);
                assert_eq!(
                    (
                        t_gc.load(SeqCst),
                        t_update.load(SeqCst),
                        t_record_fail.load(SeqCst)
                    ),
                    (1, 1, 1)
                );
                check_logs(0, 1, 1);
                let em = m.exec_metrics();
                assert_eq!(
                    em.action,
                    ExecMetricsAction {
                        gc: 1,
                        update_runtime_mem_stats: 1,
                        record_mem_state: PairSuccessFail { succ: 0, fail: 1 }
                    }
                );
                assert_eq!(
                    em.risk,
                    ExecMetricsRisk {
                        mem: 1,
                        oom: 0,
                        oom_kill: [0; 3]
                    }
                );
            }

            {
                // wait to check oom risk
                let expect_action = {
                    let mut a = m.exec_metrics().action;
                    a.update_runtime_mem_stats += 1;
                    a
                };
                let last_risk_state = m.last_mem_state().unwrap();
                {
                    let rs = Arc::clone(&t_record_succ);
                    recorder.set_store(move |_| {
                        rs.fetch_add(1, SeqCst);
                        Ok(())
                    });
                }
                let base = m.hc_mem_risk.lock().unwrap().last_stats_start_time;
                clock.set(base + DEF_HEAP_RECLAIM_CHECK_DURATION - Duration::from_nanos(1));
                assert_eq!(m.run_one_round(), -2);
                assert!(m.at_mem_risk());
                {
                    let risk = m.hc_mem_risk.lock().unwrap();
                    assert_eq!(risk.start_time, start_time);
                    assert_eq!(risk.last_stats_start_time, start_time);
                    assert_eq!(risk.last_heap_total_free, 500);
                }
                assert_eq!(m.mem_magnif(), last_risk_state.magnif);
                assert_eq!(m.last_mem_state().unwrap(), last_risk_state);
                assert_eq!(m.exec_metrics().action, expect_action);
                check_logs(0, 1, 1);
            }
            {
                assert_eq!(m.heap_tracked.load(SeqCst), 0);
                let expect_action = {
                    let mut a = m.exec_metrics().action;
                    a.gc += 1;
                    a.update_runtime_mem_stats += 1;
                    a
                };
                m.mu_last_gc.store(
                    m.mu_released
                        .load(SeqCst)
                        .wrapping_sub(m.pool_alloc_stats.read().unwrap().small_pool_limit as u64),
                    SeqCst,
                );
                assert_eq!(m.run_one_round(), -2);
                assert_eq!(m.exec_metrics().action, expect_action);
                check_logs(0, 1, 1);
                assert_eq!(m.heap_tracked.load(SeqCst), 1009);
            }

            {
                // next round of oom check
                let last_risk_state = m.last_mem_state().unwrap();
                let base = m.hc_mem_risk.lock().unwrap().last_stats_start_time;
                clock.set(base + DEF_HEAP_RECLAIM_CHECK_DURATION);
                let now_t = base + DEF_HEAP_RECLAIM_CHECK_DURATION;
                *mock_heap.lock().unwrap() = [
                    multi_ratio(new_limit, 900),
                    multi_ratio(new_limit, HEAP_INUSE_RATE_MILLI),
                    multi_ratio(new_limit, 900) + 500 + 2,
                    multi_ratio(new_limit, 50),
                ];
                assert_eq!(m.run_one_round(), -2);
                assert!(m.at_mem_risk());
                {
                    let risk = m.hc_mem_risk.lock().unwrap();
                    assert_eq!(risk.start_time, start_time);
                    assert_eq!(risk.last_stats_start_time, now_t);
                    assert_eq!(risk.last_heap_total_free, 500 + 2);
                }
                assert_eq!(m.mem_magnif(), last_risk_state.magnif);
                assert_eq!(m.last_mem_state().unwrap(), last_risk_state);
                assert_eq!(
                    (
                        t_gc.load(SeqCst),
                        t_update.load(SeqCst),
                        t_record_fail.load(SeqCst)
                    ),
                    (3, 4, 1)
                );
                check_logs(0, 2, 1);
                assert_eq!(
                    m.exec_metrics().action,
                    ExecMetricsAction {
                        gc: 3,
                        update_runtime_mem_stats: 4,
                        record_mem_state: PairSuccessFail { succ: 0, fail: 1 }
                    }
                );
                assert_eq!(
                    m.exec_metrics().risk,
                    ExecMetricsRisk {
                        mem: 1,
                        oom: 0,
                        oom_kill: [0; 3]
                    }
                );
            }

            {
                let base = m.hc_mem_risk.lock().unwrap().last_stats_start_time;
                clock.set(base + DEF_HEAP_RECLAIM_CHECK_DURATION);
                let now_t = base + DEF_HEAP_RECLAIM_CHECK_DURATION;
                let last_risk_state = m.last_mem_state().unwrap();
                *mock_heap.lock().unwrap() = [
                    multi_ratio(new_limit, 900),
                    multi_ratio(new_limit, HEAP_INUSE_RATE_MILLI),
                    multi_ratio(new_limit, 900) + 500 + 4,
                    multi_ratio(new_limit, 50),
                ];
                m.mu_last_gc.store(
                    m.mu_released
                        .load(SeqCst)
                        .wrapping_sub(m.pool_alloc_stats.read().unwrap().small_pool_limit as u64),
                    SeqCst,
                );
                assert_eq!(m.run_one_round(), -2);
                {
                    let risk = m.hc_mem_risk.lock().unwrap();
                    assert_eq!(risk.last_stats_start_time, now_t);
                    assert_eq!(risk.last_heap_total_free, 500 + 4);
                }
                assert_eq!(m.mem_magnif(), last_risk_state.magnif);
                assert_eq!(m.last_mem_state().unwrap(), last_risk_state);
                check_logs(0, 3, 1);
                assert_eq!(
                    m.exec_metrics().action,
                    ExecMetricsAction {
                        gc: 4,
                        update_runtime_mem_stats: 5,
                        record_mem_state: PairSuccessFail { succ: 0, fail: 1 }
                    }
                );
                assert_eq!(
                    m.exec_metrics().risk,
                    ExecMetricsRisk {
                        mem: 1,
                        oom: 0,
                        oom_kill: [0; 3]
                    }
                );
            }
            {
                let kill_event: CbMap = Arc::new(Mutex::new(HashMap::new()));
                let last_risk_state = m.last_mem_state().unwrap();
                {
                    let ke = Arc::clone(&kill_event);
                    let u2 = e2.pool.uid();
                    h2.set_kill_cb(move || {
                        *ke.lock().unwrap().entry(u2).or_insert(0) += 1;
                    });
                    let ke = Arc::clone(&kill_event);
                    let (u1, u2b) = (e1.pool.uid(), e2.pool.uid());
                    h1.set_kill_cb(move || {
                        let mut g = ke.lock().unwrap();
                        assert_ne!(*g.get(&u2b).unwrap_or(&0), 0);
                        *g.entry(u1).or_insert(0) += 1;
                    });
                    let ke = Arc::clone(&kill_event);
                    let (u1b, u2c, u3) = (e1.pool.uid(), e2.pool.uid(), e3.pool.uid());
                    h3.set_kill_cb(move || {
                        let mut g = ke.lock().unwrap();
                        assert_ne!(*g.get(&u2c).unwrap_or(&0), 0);
                        assert_ne!(*g.get(&u1b).unwrap_or(&0), 0);
                        *g.entry(u3).or_insert(0) += 1;
                    });
                    let e1c = Arc::clone(&e1);
                    h1.set_heap_used_cb(move || e1c.arbitrator_mu.lock().unwrap().quota);
                    let e2c = Arc::clone(&e2);
                    h2.set_heap_used_cb(move || e2c.arbitrator_mu.lock().unwrap().quota);
                    let e3c = Arc::clone(&e3);
                    h3.set_heap_used_cb(move || e3c.arbitrator_mu.lock().unwrap().quota);
                }
                m.prepare_alloc(&e2, 1000);
                let base = m.hc_mem_risk.lock().unwrap().last_stats_start_time;
                clock.set(base + DEF_HEAP_RECLAIM_CHECK_DURATION);
                let now_t = base + DEF_HEAP_RECLAIM_CHECK_DURATION;
                *mock_heap.lock().unwrap() = [
                    multi_ratio(new_limit, 900),
                    multi_ratio(new_limit, HEAP_INUSE_RATE_MILLI) - 233,
                    multi_ratio(new_limit, 900) + 500 + 4 + 1,
                    233,
                ];
                assert_eq!(m.run_one_round(), -2);
                assert!(!mem_hang_risk(
                    m.min_heap_free_bps(),
                    m.min_heap_free_bps(),
                    SystemTime::UNIX_EPOCH + DEF_HEAP_RECLAIM_CHECK_MAX_DURATION,
                    SystemTime::UNIX_EPOCH
                ));
                assert!(mem_hang_risk(
                    0,
                    0,
                    SystemTime::UNIX_EPOCH
                        + DEF_HEAP_RECLAIM_CHECK_MAX_DURATION
                        + Duration::from_nanos(1),
                    SystemTime::UNIX_EPOCH
                ));
                {
                    let risk = m.hc_mem_risk.lock().unwrap();
                    assert_eq!(risk.last_stats_start_time, now_t);
                    assert_eq!(risk.last_heap_total_free, 500 + 4 + 1);
                }
                assert_eq!(m.mem_magnif(), last_risk_state.magnif);
                assert_eq!(m.last_mem_state().unwrap(), last_risk_state);
                check_logs(0, 7, 1); // OOM RISK; Start to KILL; make task failed; restart check
                assert_eq!(
                    m.exec_metrics().action,
                    ExecMetricsAction {
                        gc: 5,
                        update_runtime_mem_stats: 6,
                        record_mem_state: PairSuccessFail { succ: 0, fail: 1 }
                    }
                );
                assert_eq!(m.wait_alloc(&e2), ArbitrateResult::Fail);
                assert_eq!(
                    m.exec_metrics().task.pair,
                    PairSuccessFail { succ: 0, fail: 1 }
                );
                assert_eq!(
                    m.exec_metrics().risk,
                    ExecMetricsRisk {
                        mem: 1,
                        oom: 1,
                        oom_kill: [1, 0, 0]
                    }
                );
                assert_eq!(kill_event.lock().unwrap().len(), 1);
                assert_eq!(kill_event.lock().unwrap()[&e2.pool.uid()], 1);
                assert_eq!(m.under_kill.lock().unwrap().num, 1);

                let base = m.hc_mem_risk.lock().unwrap().last_stats_start_time;
                clock.set(base + DEF_HEAP_RECLAIM_CHECK_DURATION);
                assert_eq!(m.run_one_round(), -2);
                check_logs(0, 8, 1); // OOM RISK
                assert_eq!(
                    m.exec_metrics().action,
                    ExecMetricsAction {
                        gc: 6,
                        update_runtime_mem_stats: 7,
                        record_mem_state: PairSuccessFail { succ: 0, fail: 1 }
                    }
                );
                assert!(e2.load_ctx().unwrap().stopped.load(SeqCst));
                assert!(e2
                    .ctx
                    .cancel_ch
                    .lock()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .is_closed());
                assert_eq!(
                    m.exec_metrics().risk,
                    ExecMetricsRisk {
                        mem: 1,
                        oom: 2,
                        oom_kill: [1, 0, 0]
                    }
                );

                m.mu_allocated.fetch_add(100_000, SeqCst);
                m.entry_map.add_quota(&e5, 100_000);
                h5.set_heap_used_cb(|| 100_000);
                {
                    let ke = Arc::clone(&kill_event);
                    let u5 = e5.pool.uid();
                    h5.set_kill_cb(move || {
                        *ke.lock().unwrap().entry(u5).or_insert(0) += 1;
                    });
                }

                let base = m.hc_mem_risk.lock().unwrap().last_stats_start_time;
                clock.set(base + DEF_KILL_CANCEL_CHECK_TIMEOUT);
                assert_eq!(m.run_one_round(), -2);
                check_logs(0, 11, 2); // Failed to KILL; Start to KILL
                assert_eq!(
                    m.exec_metrics().action,
                    ExecMetricsAction {
                        gc: 7,
                        update_runtime_mem_stats: 8,
                        record_mem_state: PairSuccessFail { succ: 0, fail: 1 }
                    }
                );
                assert_eq!(m.under_kill.lock().unwrap().num, 2);
                assert!(e2.arbitrator_mu.lock().unwrap().under_kill.fail);
                assert!(e5.arbitrator_mu.lock().unwrap().under_kill.start);
                assert!(!e5.arbitrator_mu.lock().unwrap().under_kill.fail);
                assert_eq!(
                    m.exec_metrics().risk,
                    ExecMetricsRisk {
                        mem: 1,
                        oom: 3,
                        oom_kill: [2, 0, 0]
                    }
                );

                // release quota which makes it able to gc
                assert!(m.remove_root_pool_entry(&e2));
                assert!(m.remove_root_pool_entry(&e5));
                let base = m.hc_mem_risk.lock().unwrap().last_stats_start_time;
                clock.set(base + DEF_HEAP_RECLAIM_CHECK_DURATION);
                assert_eq!(m.run_one_round(), -2);
                assert!(!e5.arbitrator_mu.lock().unwrap().under_kill.start);
                assert!(!e2.arbitrator_mu.lock().unwrap().under_kill.start);
                assert_eq!(m.under_kill.lock().unwrap().num, 2);
                check_logs(0, 17, 2); // Finish KILL x2; OOM RISK; Start KILL x2; Restart check
                assert_eq!(
                    m.exec_metrics().action,
                    ExecMetricsAction {
                        gc: 8,
                        update_runtime_mem_stats: 9,
                        record_mem_state: PairSuccessFail { succ: 0, fail: 1 }
                    }
                );
                {
                    let q1 = e1.arbitrator_mu.lock().unwrap().quota;
                    let q3 = e3.arbitrator_mu.lock().unwrap().quota;
                    assert_eq!(m.heap_tracked.load(SeqCst), q1 + q3);
                }
                assert_eq!(
                    m.exec_metrics().risk,
                    ExecMetricsRisk {
                        mem: 1,
                        oom: 4,
                        oom_kill: [3, 0, 1]
                    }
                );
                {
                    let ke = kill_event.lock().unwrap();
                    assert_eq!(ke.len(), 4);
                    for e in [&e1, &e2, &e3, &e5] {
                        assert_eq!(ke[&e.pool.uid()], 1);
                    }
                }
                assert_eq!(m.buffer_size.load(SeqCst), 9000);

                assert!(m.remove_root_pool_entry(&e1));
                assert!(m.remove_root_pool_entry(&e3));
                *mock_heap.lock().unwrap() = [
                    multi_ratio(new_limit, 900) - 1,
                    multi_ratio(new_limit, 900) - 1,
                    0,
                    0,
                ];
                h4.set_heap_used_cb(|| 1013);
                assert_eq!(m.run_one_round(), 0);
                assert!(!e1.arbitrator_mu.lock().unwrap().under_kill.start);
                assert!(!e3.arbitrator_mu.lock().unwrap().under_kill.start);
                assert_eq!(m.under_kill.lock().unwrap().num, 0);
                assert_eq!(m.heap_tracked.load(SeqCst), 1013);
                check_logs(1, 19, 2); // Finish KILL x2; mem is safe
                assert_eq!(
                    m.exec_metrics().action,
                    ExecMetricsAction {
                        gc: 9,
                        update_runtime_mem_stats: 10,
                        record_mem_state: PairSuccessFail { succ: 0, fail: 1 }
                    }
                );
                assert!(!m.at_mem_risk());
            }
            assert!(m.consume_quota_from_await_free_pool(0, -1000));
            assert_eq!(m.await_free_pool_used(), MemPoolQuotaUsage::default());
            assert_eq!(m.await_free_pool_cap(), 0);
            m.shrink_await_free_pool(0, DEF_AWAIT_FREE_POOL_SHRINK_DUR_MILLI);
            assert_eq!(m.mem_magnif(), 6100);
            assert_eq!(
                m.avoid_size.load(SeqCst),
                mock_heap.lock().unwrap()[0] - 1013
            );
            delete_entries_for_test(&m, &[&e4]);
            check_entries(&m, &[]);
        }
        drop(clock);

        let clock = MockClock::install(SystemTime::UNIX_EPOCH);
        {
            // tick task: reduce mem magnification
            reset_exec_metrics_for_test(&m);
            assert_eq!(m.root_pool_num(), 0);
            assert_eq!(m.allocated(), 0);
            let epoch = SystemTime::UNIX_EPOCH;
            clock.set(epoch + Duration::from_secs(DEF_UPDATE_MEM_MAGNIF_UTIME_ALIGN as u64));
            m.set_unix_time_sec(DEF_UPDATE_MEM_MAGNIF_UTIME_ALIGN);

            m.try_to_update_buffer(23, m.approx_unix_time_sec());
            assert_eq!(m.buffer_size.load(SeqCst), 23);
            let (e1ctx, e1h) = new_ctx_with_helper(
                ArbitrationPriority::Medium,
                NO_WAIT_AVERSE,
                REQUIRE_PRIVILEGE,
            );
            e1h.set_heap_used_cb(|| 31);
            let e1 = add_entry_for_test(&m, Some(e1ctx));
            m.update_tracked_heap_stats();
            assert_eq!(m.buffer_size.load(SeqCst), 31);

            m.reset_root_pool_by_id(e1.pool.uid(), 19, true);
            assert_eq!(m.buffer_size.load(SeqCst), 31);

            m.reset_root_pool_by_id(e1.pool.uid(), 389, true);
            assert_eq!(m.buffer_size.load(SeqCst), 389);

            let logs_info = Arc::new(AtomicI64::new(0));
            {
                let li = Arc::clone(&logs_info);
                set_actions(
                    &m,
                    move |_| {
                        li.fetch_add(1, SeqCst);
                    },
                    |_| {},
                    |_| {},
                    None,
                    None,
                );
            }

            {
                // mock oom-check running blocks the tick
                m.hc_mem_risk_start_unix_milli.store(
                    SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as i64,
                    SeqCst,
                );
                assert!(!m.execute_tick(DEF_MAX));
                m.hc_mem_risk_start_unix_milli.store(0, SeqCst);
            }

            let mut tl_pre = 0i64;
            let mut tl_now = DEF_UPDATE_MEM_MAGNIF_UTIME_ALIGN * KILO;

            let check_prof_impl = |ms: i64, heap: i64, quota: i64, ratio: i64| {
                let sec = ms / KILO;
                let align = sec / DEF_UPDATE_MEM_MAGNIF_UTIME_ALIGN;
                let profs = m.hc_timed_mem_profile.lock().unwrap();
                let p = &profs[(align % 2) as usize];
                assert_eq!(
                    (p.start_utime_milli, p.ts_align, p.heap, p.quota, p.ratio),
                    (ms, align, heap, quota, ratio)
                );
            };
            let check_prof = |ms: i64, heap: i64, quota: i64| check_prof_impl(ms, heap, quota, 0);

            {
                // new suggest pool cap: last mem state is nil
                let ori = m.last_mem_state();
                assert_eq!(
                    ori.unwrap(),
                    RuntimeMemStateV1 {
                        version: 1,
                        last_risk: LastRisk {
                            heap_alloc: 90000,
                            quota_alloc: 15000
                        },
                        magnif: 6100,
                        pool_medium_cap: 0,
                    }
                );
                assert_eq!(m.mem_magnif(), 6100);
                assert_eq!(m.pool_medium_quota(), 0);
                assert_eq!(m.pool_alloc_last_update_milli.load(SeqCst), 0);
                assert_eq!(m.exec_metrics().action.record_mem_state.succ, 0);

                *m.hc_last_mem_state.lock().unwrap() = None;
                assert!(m.execute_tick(tl_now));
                assert_eq!(m.exec_metrics().action.record_mem_state.succ, 1);
                assert_eq!(logs_info.load(SeqCst), 1);
                assert_eq!(m.pool_medium_quota(), 400);
                assert_eq!(m.pool_alloc_last_update_milli.load(SeqCst), tl_now);
                let mut ori = ori.unwrap();
                ori.pool_medium_cap = m.pool_medium_quota();
                *m.hc_last_mem_state.lock().unwrap() = Some(ori);

                // same value
                assert!(
                    !m.try_store_pool_medium_capacity(tl_now + DEF_TICK_DUR_MILLI * 10 + 1, 400)
                );
                // time not satisfied
                assert!(
                    !m.try_store_pool_medium_capacity(tl_now + DEF_TICK_DUR_MILLI * 10 - 1, 399)
                );
                assert_eq!(m.exec_metrics().action.record_mem_state.succ, 1);
                assert_eq!(logs_info.load(SeqCst), 1);
                // last mem state not nil & value differs
                assert!(m.try_store_pool_medium_capacity(tl_now + DEF_TICK_DUR_MILLI * 10, 401));
                assert_eq!(m.exec_metrics().action.record_mem_state.succ, 2);
                assert_eq!(logs_info.load(SeqCst), 2);
                assert_eq!(m.last_mem_state().unwrap().pool_medium_cap, 401);
            }

            {
                m.set_soft_limit(0, 0.0, SoftLimitMode::Disable);
                set_mem_stats_for_test(&m, 0, 0, 0, 0);
                assert_eq!(m.avoid_size.load(SeqCst), 5000);
                m.set_soft_limit(0, 0.0, SoftLimitMode::Auto);
                set_mem_stats_for_test(&m, 0, 0, 0, 0);
                assert_eq!(m.avoid_size.load(SeqCst), 83607);
            }

            {
                // init last gc state
                m.hc_last_gc_heap_alloc.store(DEF_MAX, SeqCst);
                m.hc_last_gc_utime.store(0, SeqCst);
            }

            m.set_limit(15000);
            assert_eq!(m.limit(), 15000);
            m.mu_allocated.store(20000, SeqCst);
            assert_eq!(m.allocated(), 20000);

            tl_pre = tl_now;
            tl_now += DEF_UPDATE_MEM_MAGNIF_UTIME_ALIGN * KILO;
            m.prepare_alloc(&e1, DEF_MAX);
            assert_eq!(
                m.exec_mu.lock().unwrap().blocked_state,
                BlockedState::default()
            );
            m.set_unix_time_sec(tl_now / KILO + DEF_UPDATE_MEM_MAGNIF_UTIME_ALIGN - 1);
            m.hc_last_gc_heap_alloc.store(m.limit(), SeqCst);
            m.hc_last_gc_utime
                .store(m.approx_unix_time_sec() * 1_000_000_000, SeqCst);
            assert!(!m.do_execute_first_task());
            assert_eq!(
                m.exec_mu.lock().unwrap().blocked_state,
                BlockedState {
                    allocated: 20000,
                    utime_sec: m.approx_unix_time_sec()
                }
            );
            // calculate ratio of the previous
            assert!(m.execute_tick(tl_now));
            assert_eq!(m.exec_metrics().action.record_mem_state.succ, 3);
            assert_eq!(logs_info.load(SeqCst), 3);
            assert_eq!(m.last_mem_state().unwrap().pool_medium_cap, 400);
            check_prof(tl_pre, 0, 0);
            check_prof(tl_now, 15000, 20000);

            tl_pre = tl_now;
            tl_now += DEF_UPDATE_MEM_MAGNIF_UTIME_ALIGN * KILO;
            m.set_unix_time_sec(tl_now / KILO + DEF_UPDATE_MEM_MAGNIF_UTIME_ALIGN - 1);
            m.mu_allocated.store(40000, SeqCst);
            m.hc_last_gc_heap_alloc.store(35000, SeqCst);
            m.hc_last_gc_utime
                .store(m.approx_unix_time_sec() * 1_000_000_000, SeqCst);
            assert!(!m.do_execute_first_task());
            assert_eq!(
                m.exec_mu.lock().unwrap().blocked_state,
                BlockedState {
                    allocated: 40000,
                    utime_sec: m.approx_unix_time_sec()
                }
            );
            assert!(m.execute_tick(tl_now));
            assert_eq!(m.exec_metrics().action.record_mem_state.succ, 3);
            assert_eq!(logs_info.load(SeqCst), 4); // mem profile timeline
            check_prof_impl(tl_pre, 15000, 20000, 750);
            check_prof(tl_now, 35000, 40000);

            tl_pre = tl_now;
            tl_now += DEF_UPDATE_MEM_MAGNIF_UTIME_ALIGN * KILO;
            m.set_unix_time_sec(tl_now / KILO + DEF_UPDATE_MEM_MAGNIF_UTIME_ALIGN - 1);
            m.mu_allocated.store(50000, SeqCst);
            m.hc_last_gc_heap_alloc.store(40000, SeqCst);
            m.hc_last_gc_utime
                .store(m.approx_unix_time_sec() * 1_000_000_000, SeqCst);
            assert!(!m.do_execute_first_task());
            assert_eq!(
                m.exec_mu.lock().unwrap().blocked_state,
                BlockedState {
                    allocated: 50000,
                    utime_sec: m.approx_unix_time_sec()
                }
            );
            assert!(m.execute_tick(tl_now));
            // no update because the heap stats are NOT safe
            assert_eq!(m.mem_magnif(), 6100);
            assert_eq!(logs_info.load(SeqCst), 5);
            assert_eq!(m.exec_metrics().action.record_mem_state.succ, 3);
            check_prof_impl(tl_pre, 35000, 40000, 875);
            check_prof(tl_now, 40000, 50000);

            // restore limit to 100000
            m.set_limit(100_000);
            tl_pre = tl_now;
            tl_now += DEF_UPDATE_MEM_MAGNIF_UTIME_ALIGN * KILO;
            m.set_unix_time_sec(tl_now / KILO + 1);
            {
                let mut exec = m.exec_mu.lock().unwrap();
                exec.blocked_state = BlockedState {
                    allocated: 50000,
                    utime_sec: m.unix_time_sec.load(SeqCst),
                };
            }
            m.hc_last_gc_heap_alloc.store(40000, SeqCst);
            m.hc_last_gc_utime
                .store(m.approx_unix_time_sec() * 1_000_000_000, SeqCst);
            assert!(m.execute_tick(tl_now));
            assert_eq!(m.mem_magnif(), (875 + 6100) / 2); // choose 875 over 800
            assert_eq!(logs_info.load(SeqCst), 8);
            assert_eq!(m.exec_metrics().action.record_mem_state.succ, 4);
            check_prof_impl(tl_pre, 40000, 50000, 800);
            check_prof(tl_now, 40000, 50000);

            tl_pre = tl_now;
            tl_now += KILO;
            m.set_unix_time_sec(m.approx_unix_time_sec() + 1);
            m.mu_allocated.store(40000, SeqCst);
            m.hc_last_gc_heap_alloc.store(41000, SeqCst);
            m.hc_last_gc_utime
                .store(m.approx_unix_time_sec() * 1_000_000_000, SeqCst);
            assert!(!m.do_execute_first_task());
            assert_eq!(
                m.exec_mu.lock().unwrap().blocked_state,
                BlockedState {
                    allocated: 40000,
                    utime_sec: m.approx_unix_time_sec()
                }
            );
            assert!(m.execute_tick(tl_now));
            assert_eq!(m.mem_magnif(), (875 + 6100) / 2); // no update
            assert_eq!(logs_info.load(SeqCst), 8);
            assert_eq!(m.exec_metrics().action.record_mem_state.succ, 4);
            check_prof(tl_now - KILO, 41000, 50000);
            tl_now = tl_pre + DEF_UPDATE_MEM_MAGNIF_UTIME_ALIGN * KILO;
            assert!(m.execute_tick(tl_now));
            check_prof_impl(tl_pre, 41000, 50000, 820);
            check_prof(tl_now, 0, 0);
            assert_eq!(m.mem_magnif(), (3487 + 820) / 2);
            assert_eq!(logs_info.load(SeqCst), 11);
            assert_eq!(m.exec_metrics().action.record_mem_state.succ, 5);

            tl_pre = tl_now;
            tl_now += DEF_UPDATE_MEM_MAGNIF_UTIME_ALIGN * KILO;
            // no valid pre profile
            assert!(m.execute_tick(tl_now));
            assert_eq!(logs_info.load(SeqCst), 13);
            assert_eq!(m.exec_metrics().action.record_mem_state.succ, 6);
            assert_eq!(m.mem_magnif(), (2153 + 820) / 2);
            check_prof(tl_now, 0, 0);
            check_prof(tl_pre, 0, 0);
            assert_eq!(logs_info.load(SeqCst), 13);
            assert_eq!(m.exec_metrics().action.record_mem_state.succ, 6);

            tl_pre = tl_now;
            tl_now += DEF_UPDATE_MEM_MAGNIF_UTIME_ALIGN * KILO;
            assert!(m.execute_tick(tl_now));
            assert_eq!(m.mem_magnif(), (2153 + 820) / 2); // no update
            tl_pre = tl_now;
            tl_now += DEF_UPDATE_MEM_MAGNIF_UTIME_ALIGN * KILO;

            // new start
            tl_pre = tl_now;
            tl_now += DEF_UPDATE_MEM_MAGNIF_UTIME_ALIGN * KILO;
            m.set_unix_time_sec(tl_now / KILO);
            {
                let mut exec = m.exec_mu.lock().unwrap();
                exec.blocked_state = BlockedState {
                    allocated: 10000,
                    utime_sec: m.unix_time_sec.load(SeqCst),
                };
            }
            m.hc_last_gc_heap_alloc.store(10000, SeqCst);
            m.hc_last_gc_utime
                .store(m.approx_unix_time_sec() * 1_000_000_000, SeqCst);
            assert!(m.execute_tick(tl_now));
            assert_eq!(m.mem_magnif(), (2153 + 820) / 2); // no update
            assert_eq!(logs_info.load(SeqCst), 13);
            assert_eq!(m.exec_metrics().action.record_mem_state.succ, 6);

            tl_pre = tl_now;
            tl_now += DEF_UPDATE_MEM_MAGNIF_UTIME_ALIGN * KILO;
            m.set_unix_time_sec(tl_now / KILO);
            {
                let mut exec = m.exec_mu.lock().unwrap();
                exec.blocked_state = BlockedState {
                    allocated: 11111,
                    utime_sec: m.unix_time_sec.load(SeqCst),
                };
            }
            m.hc_last_gc_heap_alloc.store(2222, SeqCst);
            m.hc_last_gc_utime
                .store(m.approx_unix_time_sec() * 1_000_000_000, SeqCst);
            assert!(m.execute_tick(tl_now));
            assert_eq!(m.mem_magnif(), (2153 + 820) / 2); // no update
            assert_eq!(logs_info.load(SeqCst), 14); // timed mem profile
            assert_eq!(m.exec_metrics().action.record_mem_state.succ, 6);
            check_prof_impl(tl_pre, 10000, 10000, 1000);
            check_prof(tl_now, 2222, 11111);

            tl_pre = tl_now;
            tl_now += DEF_UPDATE_MEM_MAGNIF_UTIME_ALIGN * KILO;
            m.set_unix_time_sec(tl_now / KILO);
            {
                let mut exec = m.exec_mu.lock().unwrap();
                exec.blocked_state = BlockedState {
                    allocated: 10000,
                    utime_sec: m.unix_time_sec.load(SeqCst),
                };
            }
            m.hc_last_gc_heap_alloc.store(10000, SeqCst);
            m.hc_last_gc_utime
                .store(m.approx_unix_time_sec() * 1_000_000_000, SeqCst);
            assert!(m.execute_tick(tl_now));
            assert_eq!(m.mem_magnif(), (1486 + 1000) / 2);
            assert_eq!(logs_info.load(SeqCst), 17);
            assert_eq!(m.exec_metrics().action.record_mem_state.succ, 7);
            check_prof_impl(tl_pre, 2222, 11111, 199);
            check_prof(tl_now, 10000, 10000);

            tl_pre = tl_now;
            tl_now += DEF_UPDATE_MEM_MAGNIF_UTIME_ALIGN * KILO;
            m.set_unix_time_sec(tl_now / KILO);
            {
                let mut exec = m.exec_mu.lock().unwrap();
                exec.blocked_state = BlockedState {
                    allocated: 10000,
                    utime_sec: m.unix_time_sec.load(SeqCst),
                };
            }
            m.hc_last_gc_heap_alloc.store(5000, SeqCst);
            m.hc_last_gc_utime
                .store(m.approx_unix_time_sec() * 1_000_000_000, SeqCst);
            assert!(m.execute_tick(tl_now));
            check_prof_impl(tl_pre, 10000, 10000, 1000);
            check_prof(tl_now, 5000, 10000);
            // choose 1000 over 199
            assert_eq!(m.mem_magnif(), (1243 + 1000) / 2);
            assert_eq!(logs_info.load(SeqCst), 20);
            assert_eq!(m.exec_metrics().action.record_mem_state.succ, 8);

            tl_pre = tl_now;
            tl_now += DEF_UPDATE_MEM_MAGNIF_UTIME_ALIGN * KILO;
            m.set_unix_time_sec(tl_now / KILO);
            {
                let mut exec = m.exec_mu.lock().unwrap();
                exec.blocked_state = BlockedState {
                    allocated: 10000,
                    utime_sec: m.unix_time_sec.load(SeqCst),
                };
            }
            m.hc_last_gc_heap_alloc.store(5000, SeqCst);
            m.hc_last_gc_utime
                .store(m.approx_unix_time_sec() * 1_000_000_000, SeqCst);
            assert!(m.execute_tick(tl_now));
            assert_eq!(m.mem_magnif(), (1121 + 1000) / 2);
            assert_eq!(logs_info.load(SeqCst), 23);
            assert_eq!(m.exec_metrics().action.record_mem_state.succ, 9);

            tl_pre = tl_now;
            tl_now += DEF_UPDATE_MEM_MAGNIF_UTIME_ALIGN * KILO;
            m.set_unix_time_sec(tl_now / KILO);
            {
                let mut exec = m.exec_mu.lock().unwrap();
                exec.blocked_state = BlockedState {
                    allocated: 10000,
                    utime_sec: m.unix_time_sec.load(SeqCst),
                };
            }
            m.hc_last_gc_heap_alloc.store(20000, SeqCst);
            m.hc_last_gc_utime
                .store(m.approx_unix_time_sec() * 1_000_000_000, SeqCst);
            assert!(m.execute_tick(tl_now));
            assert_eq!(m.mem_magnif(), 0); // smaller than 1000
            assert_eq!(logs_info.load(SeqCst), 26);
            assert_eq!(m.exec_metrics().action.record_mem_state.succ, 10);
            e1h.cancel_self();
            m.wait_alloc(&e1);
            delete_entries_for_test(&m, &[&e1]);
            m.mu_allocated.store(0, SeqCst);
            let _ = tl_pre;
        }
        {
            // no more root pool can be killed
            reset_exec_metrics_for_test(&m);
            assert!(m.is_mem_safe());
            check_entries(&m, &[]);
            m.set_limit(100_000);
            const HEAP_INUSE_RATE_MILLI: i64 = 950;
            let mock_heap = Arc::new(Mutex::new([0i64; 4]));
            {
                let m3 = Arc::clone(&m);
                let mh = Arc::clone(&mock_heap);
                set_actions(
                    &m,
                    |_| {},
                    |_| {},
                    |_| {},
                    Some(Box::new(move || {
                        let h = *mh.lock().unwrap();
                        set_mem_stats_for_test(&m3, h[0], h[1], h[2], h[3]);
                    })),
                    Some(Box::new(|| {})),
                );
            }
            let (c1, h1) = new_ctx_with_helper(
                ArbitrationPriority::Medium,
                NO_WAIT_AVERSE,
                REQUIRE_PRIVILEGE,
            );
            let e1 = add_entry_for_test(&m, Some(c1));
            h1.set_heap_used_cb(|| 10000);
            let kill1 = Arc::new(AtomicI64::new(0));
            {
                let k = Arc::clone(&kill1);
                h1.set_kill_cb(move || {
                    k.fetch_add(1, SeqCst);
                });
            }
            m.prepare_alloc(&e1, 10000);
            assert_eq!(m.run_one_round(), 1);
            assert_eq!(m.wait_alloc(&e1), ArbitrateResult::Ok);
            *mock_heap.lock().unwrap() = [0, multi_ratio(100_000, HEAP_INUSE_RATE_MILLI + 1), 0, 0];
            let (c2, h2) = new_ctx_with_helper(
                ArbitrationPriority::Medium,
                NO_WAIT_AVERSE,
                REQUIRE_PRIVILEGE,
            );
            let e2 = add_entry_for_test(&m, Some(c2));
            let kill2 = Arc::new(AtomicI64::new(0));
            {
                let k = Arc::clone(&kill2);
                h2.set_kill_cb(move || {
                    k.fetch_add(1, SeqCst);
                });
            }
            m.refresh_runtime_mem_stats();
            let debug_now = SystemTime::now();
            clock.set(debug_now);
            assert_eq!(m.run_one_round(), -2);
            clock.set(debug_now + Duration::from_secs(1));
            assert_eq!(m.run_one_round(), -2);
            assert_eq!(m.under_kill.lock().unwrap().num, 1);
            assert!(e1.arbitrator_mu.lock().unwrap().under_kill.start);
            assert!(!e1.arbitrator_mu.lock().unwrap().under_kill.fail);
            clock.set(debug_now + Duration::from_secs(1) + DEF_KILL_CANCEL_CHECK_TIMEOUT);
            m.prepare_alloc(&e2, 10000);
            assert_eq!(m.run_one_round(), -2);
            assert!(e1.arbitrator_mu.lock().unwrap().under_kill.fail);
            assert!(e1.arbitrator_mu.lock().unwrap().under_kill.start);
            assert!(!e2.load_ctx().unwrap().available());
            assert_eq!(m.wait_alloc(&e2), ArbitrateResult::Fail);
            delete_entries_for_test(&m, &[&e1, &e2]);
            check_entries(&m, &[]);
        }
        drop(clock);
    }

    fn bench_round(m: &Arc<MemArbitrator>, n: usize, priority_mode: bool) -> (i64, i64) {
        assert!({
            m.weak_wake();
            m.async_run(DEF_TASK_TICK_DUR)
        });
        let cancel_pool = Arc::new(AtomicI64::new(0));
        let killed_pool = Arc::new(AtomicI64::new(0));
        let start = Arc::new(std::sync::Barrier::new(n));
        let mut handles = Vec::with_capacity(n);
        for i in 0..n {
            let m = Arc::clone(m);
            let cancel_pool = Arc::clone(&cancel_pool);
            let killed_pool = Arc::clone(&killed_pool);
            let start = Arc::clone(&start);
            handles.push(
                std::thread::Builder::new()
                    .stack_size(256 * 1024)
                    .spawn(move || {
                        start.wait();
                        let root = m.emplace_root_pool(i as u64).unwrap();
                        let (h, ch) = HelperForTest::new_with_channel();
                        let cancel_event = Arc::new(AtomicI64::new(0));
                        let killed = Arc::new(AtomicBool::new(false));
                        {
                            let ce = Arc::clone(&cancel_event);
                            let kd = Arc::clone(&killed);
                            h.set_kill_cb(move || {
                                ce.fetch_add(1, SeqCst);
                                kd.store(true, SeqCst);
                            });
                            let ce = Arc::clone(&cancel_event);
                            h.set_cancel_cb(move || {
                                ce.fetch_add(1, SeqCst);
                            });
                            h.set_heap_used_cb(|| 0);
                        }
                        let (prio, wait_averse) = if priority_mode {
                            let p = i % 6;
                            let prio = if p < 2 {
                                ArbitrationPriority::Low
                            } else if p < 4 {
                                ArbitrationPriority::Medium
                            } else {
                                ArbitrationPriority::High
                            };
                            (prio, p % 2 != 0)
                        } else {
                            (ArbitrationPriority::High, false)
                        };
                        let ctx = ArbitrationContext::new(
                            Some(ch),
                            Some(Arc::clone(&h) as Arc<dyn ArbitrateHelper>),
                            prio,
                            wait_averse,
                            true,
                        );
                        assert!(m.restart_entry_by_context(root.clone(), Some(ctx)));

                        let b =
                            ConcurrentBudget::new(Arc::clone(root.entry.as_ref().unwrap().pool()));
                        for _ in 0..200 {
                            if b.used.fetch_add(m.limit() / 150, SeqCst) + m.limit() / 150
                                > b.capacity()
                            {
                                let _ = b.pull_from_upstream();
                            }
                            if cancel_event.load(SeqCst) != 0 {
                                if killed.load(SeqCst) {
                                    killed_pool.fetch_add(1, SeqCst);
                                } else {
                                    cancel_pool.fetch_add(1, SeqCst);
                                }
                                break;
                            }
                        }
                        m.reset_root_pool_by_id(i as u64, b.used.load(SeqCst), true);
                    })
                    .unwrap(),
            );
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(m.root_pool_num.load(SeqCst), n as i64);
        for i in 0..n {
            m.remove_root_pool_by_id(i as u64);
        }
        m.stop();
        check_entries(m, &[]);
        (cancel_pool.load(SeqCst), killed_pool.load(SeqCst))
    }

    #[test]
    fn bench() {
        const N: usize = 3000;
        let m = MemArbitrator::new(
            4 * BYTE_SIZE_GB,
            DEF_POOL_STATUS_SHARDS,
            DEF_POOL_QUOTA_SHARDS,
            64 * BYTE_SIZE_KB,
            Box::new(RecorderForTest {
                load_fn: Arc::new(Mutex::new(Box::new(|| Ok(None)))),
                store_fn: Arc::new(Mutex::new(Box::new(|_| Ok(())))),
            }),
        );
        m.set_work_mode(ArbitratorWorkMode::Standard);
        m.init_await_free_pool(4, 4);
        {
            let (cancel_pool, killed_pool) = bench_round(&m, N, false);
            assert_eq!(killed_pool, 0);
            assert_eq!(cancel_pool, N as i64);
            assert_eq!(m.exec_metrics().task.pair.fail, N as i64);
            assert_eq!(m.exec_metrics().cancel.standard_mode, N as i64);
            reset_exec_metrics_for_test(&m);
        }

        m.set_work_mode(ArbitratorWorkMode::Priority);
        {
            let (cancel_pool, killed_pool) = bench_round(&m, N, true);
            assert_eq!(killed_pool, 0);
            assert_ne!(cancel_pool, 0);
            let em = m.exec_metrics();
            assert!(em.task.pair.fail >= (N / 2) as i64);
            assert_eq!(
                cancel_pool,
                em.cancel.wait_averse
                    + em.cancel.priority_mode[0]
                    + em.cancel.priority_mode[1]
                    + em.cancel.priority_mode[2]
            );
            assert_eq!(em.cancel.wait_averse, (N / 2) as i64);
            // under priority mode, the arbitrator may cancel pools which are
            // not waiting for alloc
            assert!(cancel_pool >= em.task.pair.fail);
            reset_exec_metrics_for_test(&m);
        }
    }
}
