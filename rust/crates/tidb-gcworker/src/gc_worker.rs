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

//! Go `pkg/store/gcworker/gc_worker.go`.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use chrono::{DateTime, FixedOffset, Local};
use tidb_log::{Field, Value};
use tikv_client::pd_keyspace::is_keyspace_using_keyspace_level_gc;
use tikv_client::proto::keyspacepb::{keyspace_meta, KeyspaceMeta, KeyspaceState};
use tikv_client::proto::metapb;

use crate::seams::{
    DelRangeTask, GcError, GcKvClient, GcPdClient, GcResult, GcRuleClient, GcSession,
    GcSessionFactory, GcStore, GcV2Manager,
};

pub(crate) const BOOLEAN_TRUE: &str = "true";
pub(crate) const BOOLEAN_FALSE: &str = "false";

const GC_WORKER_TICK_INTERVAL: Duration = Duration::from_secs(60);
const GC_WORKER_LEASE: Duration = Duration::from_secs(2 * 60);
pub(crate) const GC_LEADER_UUID_KEY: &str = "tikv_gc_leader_uuid";
pub(crate) const GC_LEADER_DESC_KEY: &str = "tikv_gc_leader_desc";
pub(crate) const GC_LEADER_LEASE_KEY: &str = "tikv_gc_leader_lease";

pub(crate) const GC_LAST_RUN_TIME_KEY: &str = "tikv_gc_last_run_time";
pub(crate) const GC_RUN_INTERVAL_KEY: &str = "tikv_gc_run_interval";
pub(crate) const GC_DEFAULT_RUN_INTERVAL: i64 = 10 * 60 * NANOS_PER_SECOND;
const GC_WAIT_TIME: Duration = Duration::from_secs(60);
const GC_REDO_DELETE_RANGE_DELAY: Duration = Duration::from_secs(24 * 60 * 60);

pub(crate) const GC_LIFE_TIME_KEY: &str = "tikv_gc_life_time";
pub(crate) const GC_DEFAULT_LIFE_TIME: i64 = 10 * 60 * NANOS_PER_SECOND;
pub(crate) const GC_MIN_LIFE_TIME: i64 = 10 * 60 * NANOS_PER_SECOND;
pub(crate) const GC_SAFE_POINT_KEY: &str = "tikv_gc_safe_point";
pub(crate) const GC_CONCURRENCY_KEY: &str = "tikv_gc_concurrency";
pub(crate) const GC_DEFAULT_CONCURRENCY: i64 = 2;
pub(crate) const GC_MIN_CONCURRENCY: i64 = 1;
pub(crate) const GC_MAX_CONCURRENCY: i64 = 128;

pub(crate) const GC_ENABLE_KEY: &str = "tikv_gc_enable";
const GC_DEFAULT_ENABLE_VALUE: bool = true;

pub(crate) const GC_MODE_KEY: &str = "tikv_gc_mode";
#[cfg_attr(not(test), allow(dead_code))] // Go's tests only.
pub(crate) const GC_MODE_CENTRAL: &str = "central";
#[cfg_attr(not(test), allow(dead_code))] // Go's tests only.
pub(crate) const GC_MODE_DISTRIBUTED: &str = "distributed";
#[cfg_attr(not(test), allow(dead_code))] // Go's tests only.
const GC_MODE_DEFAULT: &str = GC_MODE_DISTRIBUTED;

const GC_SCAN_LOCK_MODE_KEY: &str = "tikv_gc_scan_lock_mode";

pub(crate) const GC_AUTO_CONCURRENCY_KEY: &str = "tikv_gc_auto_concurrency";
const GC_DEFAULT_AUTO_CONCURRENCY: bool = true;

#[cfg_attr(not(test), allow(dead_code))] // Go's tests only.
const GC_WORKER_SERVICE_SAFE_POINT_ID: &str = "gc_worker";

/// Go `tidbGCLastRunTime`.
pub const TIDB_GC_LAST_RUN_TIME: &str = "tidb_gc_last_run_time";
/// Go `tidbGCLeaderDesc`.
pub const TIDB_GC_LEADER_DESC: &str = "tidb_gc_leader_desc";
/// Go `tidbGCLeaderLease`.
pub const TIDB_GC_LEADER_LEASE: &str = "tidb_gc_leader_lease";
/// Go `tidbGCLeaderUUID`.
pub const TIDB_GC_LEADER_UUID: &str = "tidb_gc_leader_uuid";
/// Go `tidbGCSafePoint`.
pub const TIDB_GC_SAFE_POINT: &str = "tidb_gc_safe_point";

const LOAD_ALL_KEYSPACES_FOR_UNIFIED_GC_BATCH_SIZE: u32 = 50;

/// The default of Go's `txnSafePointSyncWaitTime` (`tikv.GcStateCacheInterval`).
pub const TXN_SAFE_POINT_SYNC_WAIT_TIME: Duration = tikv_client::tikv::GC_STATE_CACHE_INTERVAL;

/// Go's package variable `txnSafePointSyncWaitTime`, in milliseconds: how
/// long a job waits for a new txn safe point to reach every TiDB. Tests set
/// it to 0, as Go's do.
static TXN_SAFE_POINT_SYNC_WAIT_TIME_MS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(TXN_SAFE_POINT_SYNC_WAIT_TIME.as_millis() as u64);

/// Go's `txnSafePointSyncWaitTime = d`.
pub fn set_txn_safe_point_sync_wait_time(wait: Duration) {
    TXN_SAFE_POINT_SYNC_WAIT_TIME_MS.store(
        u64::try_from(wait.as_millis()).unwrap_or(u64::MAX),
        std::sync::atomic::Ordering::Release,
    );
}

const UNSAFE_DESTROY_RANGE_TIMEOUT: Duration = Duration::from_secs(5 * 60);

/// Go `ConcurrencyDivisor`: reduces the input concurrency to avoid
/// overwhelming the system.
pub const CONCURRENCY_DIVISOR: usize = 4;
/// Go `RequestsPerThread`: the number of requests handled by a single thread.
pub const REQUESTS_PER_THREAD: usize = 100_000;

const NANOS_PER_SECOND: i64 = 1_000_000_000;

/// Go `constants.MaxKeyspaceID`.
const MAX_KEYSPACE_ID: u32 = 0x00ff_ffff;
/// Go `constants.NullKeyspaceID`.
pub const NULL_KEYSPACE_ID: u32 = u32::MAX;

/// Go `gcVariableComments`.
pub(crate) fn gc_variable_comment(key: &str) -> &'static str {
    match key {
        GC_LEADER_UUID_KEY => "Current GC worker leader UUID. (DO NOT EDIT)",
        GC_LEADER_DESC_KEY => "Host name and pid of current GC leader. (DO NOT EDIT)",
        GC_LEADER_LEASE_KEY => "Current GC worker leader lease. (DO NOT EDIT)",
        GC_LAST_RUN_TIME_KEY => "The time when last GC starts. (DO NOT EDIT)",
        GC_RUN_INTERVAL_KEY => "GC run interval, at least 10m, in Go format.",
        GC_LIFE_TIME_KEY => {
            "All versions within life time will not be collected by GC, at least 10m, in Go format."
        }
        GC_SAFE_POINT_KEY => "All versions after safe point can be accessed. (DO NOT EDIT)",
        GC_CONCURRENCY_KEY => "How many goroutines used to do GC parallel, [1, 128], default 2",
        GC_ENABLE_KEY => "Current GC enable status",
        GC_MODE_KEY => {
            "Mode of GC, \"central\" or \"distributed\". (Obsolete and no longer effective, the actual GC procedure is always \"distributed\")"
        }
        GC_AUTO_CONCURRENCY_KEY => {
            "Let TiDB pick the concurrency automatically. If set false, tikv_gc_concurrency will be used"
        }
        GC_SCAN_LOCK_MODE_KEY => "Mode of scanning locks, \"physical\" or \"legacy\".(Deprecated)",
        // Go indexes a map; a missing key is the empty string.
        _ => "",
    }
}

fn keyspace_id_of(meta: &KeyspaceMeta) -> u32 {
    match meta.keyspace {
        Some(keyspace_meta::Keyspace::Id(id)) => id,
        _ => 0,
    }
}

/// Go `gcConcurrency`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GcConcurrency {
    /// Go `v`.
    pub v: usize,
    /// Go `isAuto`.
    pub is_auto: bool,
}

/// Go `oracle.GetTimeFromTS`.
pub(crate) fn time_from_ts(ts: u64) -> DateTime<FixedOffset> {
    let physical_ms = (ts >> 18) as i64;
    DateTime::from_timestamp_millis(physical_ms)
        .expect("a TSO's physical part is a valid instant")
        .with_timezone(&Local)
        .fixed_offset()
}

/// Go `oracle.GoTimeToTS`.
pub(crate) fn go_time_to_ts(time: DateTime<FixedOffset>) -> u64 {
    (time.timestamp_millis() as u64) << 18
}

/// Go `oracle.ComposeTS`.
fn compose_ts(physical: i64, logical: i64) -> u64 {
    ((physical << 18) + logical) as u64
}

/// Go `time.Now().String()`: the wall clock and the monotonic reading.
fn go_time_now_string() -> String {
    go_time_string(Local::now()) + &tidb_util::monotime::monotonic_suffix()
}

/// Go `time.Time.String()` of a local wall-clock time.
pub(crate) use tidb_util::go_time_layout::go_time_string;

fn now() -> DateTime<FixedOffset> {
    Local::now().fixed_offset()
}

fn add_nanos(time: DateTime<FixedOffset>, nanos: i64) -> DateTime<FixedOffset> {
    time + chrono::Duration::nanoseconds(nanos)
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn category() -> Field {
    Field::new("category", Value::Str("gc worker".to_owned()))
}

fn str_field(key: &str, value: impl Into<String>) -> Field {
    Field::new(key, Value::Str(value.into()))
}

fn error_field(error: &GcError) -> Field {
    Field::new("error", Value::Str(error.0.clone()))
}

fn logger() -> tidb_util::logutil::Logger {
    tidb_util::logutil::bg_logger()
}

fn gc_worker_counter(label: &str) {
    tidb_txnkv::client_go_metrics::GC_WORKER_ACTIONS_TOTAL
        .with_label_values(&[label])
        .inc();
}

fn gc_job_failure_counter(label: &str) {
    tidb_txnkv::client_go_metrics::GC_FAILURE
        .with_label_values(&[label])
        .inc();
}

fn gc_config_gauge(label: &str, value: f64) {
    tidb_txnkv::client_go_metrics::GC_CONFIG
        .with_label_values(&[label])
        .set(value);
}

fn gc_histogram(stage: &str, seconds: f64) {
    tidb_txnkv::client_go_metrics::GC_SECONDS
        .with_label_values(&[stage])
        .observe(seconds);
}

/// Go `GCUnsafeDestroyRangeFailuresCounterVec`, which is client-go's
/// `TiKVUnsafeDestroyRangeFailuresCounterVec`.
fn unsafe_destroy_range_failures_counter(label: &str) {
    if let Some(counter) = tikv_client::metrics::global_metrics()
        .counter_vec("TiKVUnsafeDestroyRangeFailuresCounterVec")
    {
        counter.with_label_values(&[label]).inc();
    }
}

/// Go `metrics.StageTotal`.
const STAGE_TOTAL: &str = "total";

/// The collaborators a GC worker reaches; see [`crate::seams`].
#[derive(Clone)]
pub struct GcWorkerDeps {
    /// Go `store kv.Storage`.
    pub store: Arc<dyn GcStore>,
    /// Go `pdClient` and `pdGCControllerClient`.
    pub pd: Arc<dyn GcPdClient>,
    /// Go `tikvStore` and `regionLockResolver`.
    pub kv: Arc<dyn GcKvClient>,
    /// Go `createSession(store)`.
    pub sessions: Arc<dyn GcSessionFactory>,
    /// Go `infosync`'s rule calls.
    pub rules: Arc<dyn GcRuleClient>,
    /// Go `extworkload.GetManagerFromStore(store)`; `None` when disabled.
    pub gcv2: Option<Arc<dyn GcV2Manager>>,
}

/// The part of Go's `GCWorker` a running GC job reads: identity and
/// collaborators, never the tick state. Go's job goroutine shares the worker
/// pointer but touches only these fields.
pub(crate) struct GcJobContext {
    pub(crate) uuid: String,
    pub(crate) desc: String,
    pub(crate) keyspace_id: u32,
    pub(crate) deps: GcWorkerDeps,
    /// Go's `overrideLoadKeyspacesBatchSize` failpoint.
    pub(crate) load_keyspaces_batch_size: std::sync::atomic::AtomicU32,
    /// Go's `getLoadKeyspacesBatchCount` failpoint observation.
    pub(crate) load_keyspaces_batch_count: AtomicUsize,
}

/// Go `GCWorker`: periodically triggers the GC process on TiKV.
pub struct GcWorker {
    pub(crate) job: Arc<GcJobContext>,
    pub(crate) gc_is_running: bool,
    pub(crate) last_finish: Instant,
    /// Starter unified GC skips `gcWaitTime` only before the first completed
    /// GC job in production. After one GC job finishes, it falls back to the
    /// normal debounce behavior shared by other deployment modes.
    pub(crate) has_finished_first_gc_job: bool,
    pub(crate) done_tx: Sender<GcResult<()>>,
    pub(crate) done_rx: Receiver<GcResult<()>>,
    /// Go `intest.InTest`, a variable Go's tests flip.
    pub(crate) in_test: bool,
}

impl GcWorker {
    /// Go `NewGCWorker`.
    pub fn new(deps: GcWorkerDeps) -> GcResult<Self> {
        let version = deps.store.current_version()?;
        let host_name =
            tidb_util::sem::operating_system_hostname().unwrap_or_else(|| "unknown".to_owned());
        let uuid = format!("{version:x}");
        let keyspace_id = deps
            .store
            .keyspace_meta()
            .as_ref()
            .map_or(NULL_KEYSPACE_ID, keyspace_id_of);
        let desc = format!(
            "host:{host_name}, pid:{}, start at {}",
            std::process::id(),
            go_time_now_string()
        );
        let worker = Self::with_identity(deps, uuid, desc, keyspace_id);
        tidb_vardef::statusvar::register_statistics(Arc::new(GcStatistics(Arc::clone(
            &worker.job,
        ))));
        Ok(worker)
    }

    pub(crate) fn with_identity(
        deps: GcWorkerDeps,
        uuid: String,
        desc: String,
        keyspace_id: u32,
    ) -> Self {
        let (done_tx, done_rx) = mpsc::channel();
        Self {
            job: Arc::new(GcJobContext {
                uuid,
                desc,
                keyspace_id,
                deps,
                load_keyspaces_batch_size: std::sync::atomic::AtomicU32::new(
                    LOAD_ALL_KEYSPACES_FOR_UNIFIED_GC_BATCH_SIZE,
                ),
                load_keyspaces_batch_count: AtomicUsize::new(0),
            }),
            gc_is_running: false,
            last_finish: Instant::now(),
            has_finished_first_gc_job: false,
            done_tx,
            done_rx,
            in_test: tidb_util::intest::IN_TEST,
        }
    }

    /// Go `GCWorker.uuid`.
    pub fn uuid(&self) -> &str {
        &self.job.uuid
    }

    /// Go `Start`: ticks once to initialize the configs, then once a minute
    /// on a thread of its own until the handle is closed or dropped.
    pub fn start(mut self) -> GcWorkerHandle {
        let cancelled = Arc::new(AtomicBool::new(false));
        let (ready_tx, ready_rx) = mpsc::channel();
        let thread_cancelled = Arc::clone(&cancelled);
        let thread = std::thread::Builder::new()
            .name("gc-worker".to_owned())
            .spawn(move || self.run(&thread_cancelled, ready_tx))
            .expect("spawn the GC worker thread");
        // Go waits for the first tick, which some callers depend on.
        let _ = ready_rx.recv();
        GcWorkerHandle {
            cancelled,
            thread: Mutex::new(Some(thread)),
        }
    }

    /// Go `start`.
    fn run(&mut self, cancelled: &AtomicBool, ready: Sender<()>) {
        logger().info(
            "start",
            &[category(), str_field("uuid", self.job.uuid.clone())],
        );
        self.tick();
        let _ = ready.send(());
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut next_tick = Instant::now() + GC_WORKER_TICK_INTERVAL;
            loop {
                if cancelled.load(Ordering::Acquire) {
                    logger().info(
                        "quit",
                        &[category(), str_field("uuid", self.job.uuid.clone())],
                    );
                    return;
                }
                // Wake at least once a second to notice cancellation, as Go's
                // select notices `ctx.Done()`.
                let wait = next_tick
                    .saturating_duration_since(Instant::now())
                    .min(Duration::from_secs(1));
                match self.done_rx.recv_timeout(wait) {
                    Ok(result) => {
                        self.gc_is_running = false;
                        self.last_finish = Instant::now();
                        self.has_finished_first_gc_job = true;
                        if let Err(error) = result {
                            logger().error("runGCJob", &[category(), error_field(&error)]);
                        }
                    }
                    Err(RecvTimeoutError::Timeout) => {
                        if Instant::now() >= next_tick {
                            self.tick();
                            next_tick = Instant::now() + GC_WORKER_TICK_INTERVAL;
                        }
                    }
                    Err(RecvTimeoutError::Disconnected) => return,
                }
            }
        }));
        if let Err(panic) = outcome {
            let message = panic
                .downcast_ref::<&str>()
                .map(|message| (*message).to_owned())
                .or_else(|| panic.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "non-string panic".to_owned());
            logger().error("gcWorker", &[str_field("r", message)]);
            tidb_util::panic_metrics::PANIC_TOTAL
                .with_label_values(&["gcworker"])
                .inc();
        }
    }

    /// Go `GetScope`.
    pub fn get_scope(&self, _status: &str) -> u8 {
        tidb_vardef::statusvar::DEFAULT_STATUS_VAR_SCOPE_FLAG
    }

    /// Go `Stats`: the GC rows of `mysql.tidb` as status variables.
    pub fn stats(&self) -> HashMap<String, String> {
        self.job.stats()
    }

    /// Go `tick`.
    pub(crate) fn tick(&mut self) {
        match self.job.check_leader() {
            Err(error) => {
                logger().warn("check leader", &[category(), error_field(&error)]);
                gc_job_failure_counter("check_leader");
            }
            Ok(true) => {
                if let Err(error) = self.leader_tick() {
                    logger().warn("leader tick", &[category(), error_field(&error)]);
                }
            }
            Ok(false) => {
                // Config metrics should always be updated by the leader; set
                // them to 0 when this instance is not the leader.
                gc_config_gauge(GC_RUN_INTERVAL_KEY, 0.0);
                gc_config_gauge(GC_LIFE_TIME_KEY, 0.0);
            }
        }
    }

    /// Go `needsToWait`.
    pub(crate) fn needs_to_wait(&self) -> bool {
        // In production Starter mode, unified GC fast-starts on the first
        // tick instead of paying an extra gcWaitTime after bootstrap. The
        // first completion flips hasFinishedFirstGCJob, so later ticks still
        // honor the normal cooldown. Tests keep the historical behavior
        // unless one opts into the production-only path.
        if tidb_config::deploymode::is_starter() && !self.in_test {
            return self.last_finish.elapsed() < GC_WAIT_TIME && self.has_finished_first_gc_job;
        }
        self.last_finish.elapsed() < GC_WAIT_TIME
    }

    /// Go `leaderTick`: checks whether a GC job should start on this tick.
    pub(crate) fn leader_tick(&mut self) -> GcResult<()> {
        if self.gc_is_running {
            logger().info(
                "there's already a gc job running, skipped",
                &[
                    category(),
                    str_field("leaderTick on", self.job.uuid.clone()),
                ],
            );
            return Ok(());
        }

        let concurrency = match self.job.get_gc_concurrency() {
            Ok(concurrency) => concurrency,
            Err(error) => {
                logger().info(
                    "failed to get gc concurrency.",
                    &[
                        category(),
                        str_field("uuid", self.job.uuid.clone()),
                        error_field(&error),
                    ],
                );
                return Err(error);
            }
        };

        // Keyspaces using unified GC only poll the GC safe point of the null
        // keyspace; the null keyspace and keyspace-level GC keyspaces run the
        // whole procedure themselves.
        let keyspace_meta = self.job.deps.store.keyspace_meta();
        if keyspace_meta.is_some() && !is_keyspace_using_keyspace_level_gc(keyspace_meta.as_ref()) {
            return self.run_keyspace_gc_job_in_unified_gc_mode(concurrency);
        }

        let (ok, safe_point) = match self.job.prepare() {
            Ok(prepared) => prepared,
            Err(error) => {
                gc_job_failure_counter("prepare");
                return Err(error);
            }
        };
        if !ok {
            return Ok(());
        }
        // When the worker has just started, or an old GC job has just
        // finished, wait a while before starting a new job.
        if self.needs_to_wait() {
            logger().info(
                "another gc job has just finished, skipped.",
                &[
                    category(),
                    str_field("leaderTick on ", self.job.uuid.clone()),
                ],
            );
            return Ok(());
        }

        self.gc_is_running = true;
        logger().info(
            "starts the whole job",
            &[
                category(),
                str_field("uuid", self.job.uuid.clone()),
                Field::new("safePoint", Value::U64(safe_point)),
                Field::new("concurrency", Value::U64(concurrency.v as u64)),
            ],
        );
        let job = Arc::clone(&self.job);
        let done = self.done_tx.clone();
        std::thread::Builder::new()
            .name("gc-job".to_owned())
            .spawn(move || {
                let _ = done.send(job.run_gc_job(safe_point, concurrency));
            })
            .map_err(|error| GcError::new(error.to_string()))?;
        Ok(())
    }

    /// Go `runKeyspaceGCJobInUnifiedGCMode`.
    fn run_keyspace_gc_job_in_unified_gc_mode(
        &mut self,
        concurrency: GcConcurrency,
    ) -> GcResult<()> {
        // When the worker has just started, or an old GC job has just
        // finished, wait a while before starting a new job.
        if self.needs_to_wait() {
            logger().info(
                "another keyspace gc job has just finished, skipped.",
                &[
                    category(),
                    str_field("leaderTick on ", self.job.uuid.clone()),
                ],
            );
            return Ok(());
        }

        let now = self.job.get_oracle_time()?;
        if !self.job.check_gc_interval(now)? {
            return Ok(());
        }

        let job = Arc::clone(&self.job);
        let done = self.done_tx.clone();
        std::thread::Builder::new()
            .name("gc-keyspace-delete-range".to_owned())
            .spawn(move || {
                let _ = done.send(job.run_keyspace_delete_range(concurrency));
            })
            .map_err(|error| GcError::new(error.to_string()))?;

        self.job.save_time(GC_LAST_RUN_TIME_KEY, now)
    }
}

/// A started GC worker; dropping it stops the worker (Go `Close`) and joins
/// its thread. A GC job already running finishes on its own thread.
pub struct GcWorkerHandle {
    cancelled: Arc<AtomicBool>,
    thread: Mutex<Option<JoinHandle<()>>>,
}

impl GcWorkerHandle {
    /// Go `Close`: stops the background ticking.
    pub fn close(&self) {
        self.cancelled.store(true, Ordering::Release);
    }
}

impl Drop for GcWorkerHandle {
    fn drop(&mut self) {
        self.close();
        let thread = self
            .thread
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(thread) = thread {
            let _ = thread.join();
        }
    }
}

/// The worker as a status-variable provider (Go registers the `GCWorker`
/// itself): the GC rows of `mysql.tidb`.
struct GcStatistics(Arc<GcJobContext>);

impl tidb_vardef::statusvar::Statistics for GcStatistics {
    fn get_scope(&self, _status: &str) -> u8 {
        tidb_vardef::statusvar::DEFAULT_STATUS_VAR_SCOPE_FLAG
    }

    fn stats(&self) -> Result<std::collections::BTreeMap<String, String>, String> {
        Ok(self.0.stats().into_iter().collect())
    }
}

impl GcJobContext {
    /// Go's `txnSafePointSyncWaitTime`.
    pub(crate) fn sync_wait_time(&self) -> Duration {
        Duration::from_millis(
            TXN_SAFE_POINT_SYNC_WAIT_TIME_MS.load(std::sync::atomic::Ordering::Acquire),
        )
    }

    fn session(&self) -> Box<dyn GcSession + '_> {
        self.deps.sessions.create_session()
    }

    /// Go `Stats`.
    pub(crate) fn stats(&self) -> HashMap<String, String> {
        let mut stats = HashMap::new();
        for (key, name) in [
            (GC_LEADER_UUID_KEY, TIDB_GC_LEADER_UUID),
            (GC_LEADER_DESC_KEY, TIDB_GC_LEADER_DESC),
            (GC_LEADER_LEASE_KEY, TIDB_GC_LEADER_LEASE),
            (GC_LAST_RUN_TIME_KEY, TIDB_GC_LAST_RUN_TIME),
            (GC_SAFE_POINT_KEY, TIDB_GC_SAFE_POINT),
        ] {
            if let Ok(value) = self.load_value_from_sys_table(key) {
                stats.insert(name.to_owned(), value);
            }
        }
        stats
    }

    /// Go `getGCSafePoint`: `UpdateGCSafePoint(0)` never lowers the GC safe
    /// point and returns the current one.
    fn get_gc_safe_point(&self) -> GcResult<u64> {
        self.deps.pd.update_gc_safe_point(0)
    }

    /// Go `logIsGCSafePointTooEarly`.
    fn log_is_gc_safe_point_too_early(&self, safe_point: u64) -> GcResult<()> {
        let now = self.get_oracle_time()?;
        let check_ts = go_time_to_ts(add_nanos(now, -GC_DEFAULT_LIFE_TIME * 2));
        if check_ts > safe_point {
            logger().info(
                "gc safepoint is too early. Maybe there is a bit BR/Lightning/CDC task, or a long transaction is running or need a tidb without setting keyspace-name to calculate and update gc safe point.",
                &[category()],
            );
        }
        Ok(())
    }

    /// Go `runKeyspaceDeleteRange`.
    pub(crate) fn run_keyspace_delete_range(&self, concurrency: GcConcurrency) -> GcResult<()> {
        // The GC safe point is updated only after the global GC resolved
        // locks globally, so the ranges below are safe to delete.
        let safe_point = match self.get_gc_safe_point() {
            Ok(safe_point) => safe_point,
            Err(error) => {
                logger().info(
                    "get gc safe point error",
                    &[category(), error_field(&error)],
                );
                return Ok(());
            }
        };
        if safe_point == 0 {
            logger().info(
                "skip keyspace delete range, because gc safe point is 0",
                &[category()],
            );
            return Ok(());
        }
        if let Err(error) = self.log_is_gc_safe_point_too_early(safe_point) {
            logger().info(
                "log is gc safe point is too early error",
                &[category(), error_field(&error)],
            );
            return Ok(());
        }

        let keyspace_id = self
            .deps
            .store
            .keyspace_meta()
            .as_ref()
            .map_or(NULL_KEYSPACE_ID, keyspace_id_of);
        logger().info(
            "start keyspace delete range",
            &[
                category(),
                str_field("uuid", self.uuid.clone()),
                Field::new("concurrency", Value::U64(concurrency.v as u64)),
                Field::new("keyspaceID", Value::U64(u64::from(keyspace_id))),
                Field::new("GCSafepoint", Value::U64(safe_point)),
            ],
        );

        if let Err(error) = self.delete_ranges(safe_point, concurrency) {
            logger().error(
                "delete range returns an error",
                &[
                    category(),
                    str_field("uuid", self.uuid.clone()),
                    error_field(&error),
                ],
            );
            gc_job_failure_counter("delete_range");
            return Err(error);
        }
        if let Err(error) = self.redo_delete_ranges(safe_point, concurrency) {
            logger().error(
                "redo-delete range returns an error",
                &[
                    category(),
                    str_field("uuid", self.uuid.clone()),
                    error_field(&error),
                ],
            );
            gc_job_failure_counter("redo_delete_range");
            return Err(error);
        }
        Ok(())
    }

    /// Go `prepare`: whether a GC job should start, and its safe point.
    ///
    /// Go opens a transaction on a session here so a concurrent `gcEnable`
    /// change cannot interleave with the safe-point update; the loads and
    /// saves below run on sessions of their own, as Go's do.
    pub(crate) fn prepare(&self) -> GcResult<(bool, u64)> {
        let mut session = self.session();
        session.begin()?;
        let checked = self.check_prepare();
        match &checked {
            Ok((true, _)) => session.commit()?,
            _ => session.rollback(),
        }
        checked
    }

    /// Go `checkPrepare`.
    fn check_prepare(&self) -> GcResult<(bool, u64)> {
        if !self.check_gc_enable()? {
            logger().warn("gc status is disabled.", &[category()]);
            return Ok((false, 0));
        }
        let now = self.get_oracle_time()?;
        if !self.check_gc_interval(now)? {
            return Ok((false, 0));
        }
        let Some((new_safe_point, new_safe_point_value)) = self.calc_new_txn_safe_point(now)?
        else {
            return Ok((false, 0));
        };
        self.save_time(GC_LAST_RUN_TIME_KEY, now)?;
        self.save_time(GC_SAFE_POINT_KEY, new_safe_point)?;
        Ok((true, new_safe_point_value))
    }

    /// Go `getOracleTime`.
    pub(crate) fn get_oracle_time(&self) -> GcResult<DateTime<FixedOffset>> {
        Ok(time_from_ts(self.deps.store.current_version()?))
    }

    /// Go `checkGCEnable`.
    pub(crate) fn check_gc_enable(&self) -> GcResult<bool> {
        self.load_boolean_with_default(GC_ENABLE_KEY, GC_DEFAULT_ENABLE_VALUE)
    }

    /// Go `checkUseAutoConcurrency`.
    pub(crate) fn check_use_auto_concurrency(&self) -> GcResult<bool> {
        self.load_boolean_with_default(GC_AUTO_CONCURRENCY_KEY, GC_DEFAULT_AUTO_CONCURRENCY)
    }

    /// Go `loadBooleanWithDefault`.
    fn load_boolean_with_default(&self, key: &str, default_value: bool) -> GcResult<bool> {
        let value = self.load_value_from_sys_table(key)?;
        if value.is_empty() {
            // Save the default value for the key.
            let default_string = if default_value {
                BOOLEAN_TRUE
            } else {
                BOOLEAN_FALSE
            };
            self.save_value_to_sys_table(key, default_string)?;
            return Ok(default_value);
        }
        Ok(value.eq_ignore_ascii_case(BOOLEAN_TRUE))
    }

    /// Go `getGCConcurrency`.
    pub(crate) fn get_gc_concurrency(&self) -> GcResult<GcConcurrency> {
        let use_auto_concurrency = match self.check_use_auto_concurrency() {
            Ok(value) => value,
            Err(error) => {
                logger().error(
                    "failed to load config gc_auto_concurrency. use default value.",
                    &[
                        category(),
                        str_field("uuid", self.uuid.clone()),
                        error_field(&error),
                    ],
                );
                GC_DEFAULT_AUTO_CONCURRENCY
            }
        };
        if !use_auto_concurrency {
            let (value, result) = self.load_gc_concurrency_with_default();
            return result.map(|()| GcConcurrency {
                v: value as usize,
                is_auto: use_auto_concurrency,
            });
        }

        let concurrency = match self.get_stores_for_gc() {
            Ok(stores) => stores.len() as i64,
            Err(error) => {
                logger().error(
                    "failed to get up stores to calculate concurrency. use config.",
                    &[
                        category(),
                        str_field("uuid", self.uuid.clone()),
                        error_field(&error),
                    ],
                );
                let (value, result) = self.load_gc_concurrency_with_default();
                if let Err(error) = result {
                    logger().error(
                        "failed to load gc concurrency from config. use default value.",
                        &[
                            category(),
                            str_field("uuid", self.uuid.clone()),
                            error_field(&error),
                        ],
                    );
                    GC_DEFAULT_CONCURRENCY
                } else {
                    value
                }
            }
        };

        if concurrency == 0 {
            logger().error(
                "no store is up",
                &[category(), str_field("uuid", self.uuid.clone())],
            );
            return Err(GcError::new("[gc worker] no store is up"));
        }
        Ok(GcConcurrency {
            v: concurrency as usize,
            is_auto: use_auto_concurrency,
        })
    }

    /// Go `checkGCInterval`.
    pub(crate) fn check_gc_interval(&self, now: DateTime<FixedOffset>) -> GcResult<bool> {
        let run_interval =
            self.load_duration_with_default(GC_RUN_INTERVAL_KEY, GC_DEFAULT_RUN_INTERVAL)?;
        gc_config_gauge(
            GC_RUN_INTERVAL_KEY,
            run_interval as f64 / NANOS_PER_SECOND as f64,
        );
        let last_run = self.load_time(GC_LAST_RUN_TIME_KEY)?;
        if let Some(last_run) = last_run {
            if add_nanos(last_run, run_interval) > now {
                logger().debug(
                    "skipping garbage collection because gc interval hasn't elapsed since last run",
                    &[
                        category(),
                        str_field("leaderTick on", self.uuid.clone()),
                        Field::new("interval", Value::Duration(run_interval)),
                        str_field("last run", last_run.to_rfc3339()),
                    ],
                );
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Go `validateGCLifeTime`: a life time below the minimum is raised to
    /// it, and the minimum is saved.
    fn validate_gc_life_time(&self, life_time: i64) -> GcResult<i64> {
        if life_time >= GC_MIN_LIFE_TIME {
            return Ok(life_time);
        }
        logger().info(
            "invalid gc life time",
            &[
                category(),
                Field::new("get gc life time", Value::Duration(life_time)),
                Field::new("min gc life time", Value::Duration(GC_MIN_LIFE_TIME)),
            ],
        );
        self.save_duration(GC_LIFE_TIME_KEY, GC_MIN_LIFE_TIME)?;
        Ok(GC_MIN_LIFE_TIME)
    }

    /// Go `calcNewTxnSafePoint`: `None` when the safe point does not move.
    fn calc_new_txn_safe_point(
        &self,
        now: DateTime<FixedOffset>,
    ) -> GcResult<Option<(DateTime<FixedOffset>, u64)>> {
        let life_time = self.load_duration_with_default(GC_LIFE_TIME_KEY, GC_DEFAULT_LIFE_TIME)?;
        let life_time = self.validate_gc_life_time(life_time)?;
        gc_config_gauge(GC_LIFE_TIME_KEY, life_time as f64 / NANOS_PER_SECOND as f64);

        // The target the txn safe point is advanced to.
        let target = go_time_to_ts(add_nanos(now, -life_time));

        let new_txn_safe_point = match self.advance_txn_safe_point(target) {
            Ok(value) => value,
            // Go matches the PD error text until PD client errors are typed.
            Err(error) if error.0.contains("PD:gc:ErrDecreasingTxnSafePoint") => {
                logger().info(
                    "set gc worker service safe point is causing decreasing, the GC will be skipped",
                    &[error_field(&error)],
                );
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        if new_txn_safe_point == 0 {
            return Ok(None);
        }

        // The safe point is saved as a time, which drops the logical part of
        // the timestamp, so compare in that form or an unchanged safe point
        // would keep the worker working.
        Ok(Some((time_from_ts(new_txn_safe_point), new_txn_safe_point)))
    }

    /// Go `advanceTxnSafePoint`: 0 when PD did not advance it.
    pub(crate) fn advance_txn_safe_point(&self, target: u64) -> GcResult<u64> {
        let result = self.deps.pd.advance_txn_safe_point(target)?;
        let fields = |message: &str| {
            logger().info(
                message,
                &[
                    category(),
                    str_field("uuid", self.uuid.clone()),
                    Field::new("target", Value::U64(target)),
                    Field::new("oldTxnSafePoint", Value::U64(result.old_txn_safe_point)),
                    Field::new("newTxnSafePoint", Value::U64(result.new_txn_safe_point)),
                    str_field("blockerDesc", result.blocker_description.clone()),
                ],
            );
        };
        if result.new_txn_safe_point <= result.old_txn_safe_point {
            fields(
                "txn safe point not advanced, GC will be skipped. this may be caused by GC being blocked, or user enlarged GC life time.",
            );
            return Ok(0);
        } else if result.new_txn_safe_point != target {
            fields("txn safe point not advanced to the expected value");
        }
        Ok(result.new_txn_safe_point)
    }

    // Go keeps it for its tests; production never calls it.
    #[cfg_attr(not(test), allow(dead_code))]
    /// Go `setGCWorkerServiceSafePoint`: sets `safe_point` as TiDB's service
    /// safe point (TTL max, permanently valid) and returns the minimal service
    /// safe point among all services.
    pub(crate) fn set_gc_worker_service_safe_point(&self, safe_point: u64) -> GcResult<u64> {
        let min_safe_point = match self.deps.pd.update_service_gc_safe_point(
            GC_WORKER_SERVICE_SAFE_POINT_ID,
            i64::MAX,
            safe_point,
        ) {
            Ok(value) => value,
            Err(error) => {
                logger().error(
                    "failed to update service safe point",
                    &[
                        category(),
                        str_field("uuid", self.uuid.clone()),
                        error_field(&error),
                    ],
                );
                gc_job_failure_counter("update_service_safe_point");
                return Err(error);
            }
        };
        if min_safe_point < safe_point {
            logger().info(
                "there's another service in the cluster requires an earlier safe point. gc will continue with the earlier one",
                &[
                    category(),
                    str_field("uuid", self.uuid.clone()),
                    Field::new("ourSafePoint", Value::U64(safe_point)),
                    Field::new("minSafePoint", Value::U64(min_safe_point)),
                ],
            );
            return Ok(min_safe_point);
        }
        Ok(safe_point)
    }

    /// Go `runGCJob`. `safe_point` serves first as the new txn safe point
    /// (every transaction with `start_ts >= safe_point` stays valid) and
    /// then as the new GC safe point (every snapshot at or above it stays
    /// consistent).
    pub(crate) fn run_gc_job(&self, safe_point: u64, concurrency: GcConcurrency) -> GcResult<()> {
        let start = Instant::now();
        gc_worker_counter("run_job");

        // The txn safe point is synchronized to every component after
        // `txnSafePointSyncWaitTime`; nothing GC does may start before.
        std::thread::sleep(self.sync_wait_time());

        // Resolve locks: settle every transaction that started before the
        // txn safe point, so none of them can proceed.
        let txn_safe_point = safe_point;
        if let Err(error) = self.resolve_locks(txn_safe_point, concurrency.v) {
            logger().warn(
                "resolve locks returns an error",
                &[
                    category(),
                    str_field("uuid", self.uuid.clone()),
                    Field::new("txnSafePoint", Value::U64(txn_safe_point)),
                    error_field(&error),
                ],
            );
            gc_job_failure_counter("resolve_lock");
            return Err(error);
        }

        // From here on the steps clear data, at the GC safe point.
        let gc_safe_point = safe_point;
        if let Err(error) = self.delete_ranges(gc_safe_point, concurrency) {
            logger().error(
                "delete range returns an error",
                &[
                    category(),
                    str_field("uuid", self.uuid.clone()),
                    Field::new("gcSafePoint", Value::U64(gc_safe_point)),
                    error_field(&error),
                ],
            );
            gc_job_failure_counter("delete_range");
            return Err(error);
        }
        if let Err(error) = self.redo_delete_ranges(gc_safe_point, concurrency) {
            logger().error(
                "redo-delete range returns an error",
                &[
                    category(),
                    str_field("uuid", self.uuid.clone()),
                    Field::new("gcSafePoint", Value::U64(gc_safe_point)),
                    error_field(&error),
                ],
            );
            gc_job_failure_counter("redo_delete_range");
            return Err(error);
        }

        // Tell TiKV and TiFlash that the snapshots before the GC safe point
        // may be dropped.
        if let Err(error) = self.broadcast_gc_safe_point(gc_safe_point) {
            logger().error(
                "failed to upload safe point to PD",
                &[
                    category(),
                    str_field("uuid", self.uuid.clone()),
                    error_field(&error),
                ],
            );
            gc_job_failure_counter("upload_safe_point");
            return Err(error);
        }

        self.notify_gcv2_after_gc(gc_safe_point);
        gc_histogram(STAGE_TOTAL, start.elapsed().as_secs_f64());
        Ok(())
    }

    /// Go `notifyGCV2AfterGC`.
    pub(crate) fn notify_gcv2_after_gc(&self, safe_point: u64) {
        let Some(manager) = &self.deps.gcv2 else {
            return;
        };
        if !is_keyspace_using_keyspace_level_gc(manager.meta().as_ref()) {
            return;
        }
        let role = manager.role();
        let role = role.0.as_str();
        use tidb_config::external_workload::{ROLE_GCV2_WORKER, ROLE_MASTER, ROLE_TTL_TASK_WORKER};
        if role == ROLE_MASTER || role == ROLE_TTL_TASK_WORKER || role == ROLE_GCV2_WORKER {
            if let Err(error) = manager.recycle_gcv2(safe_point) {
                logger().warn(
                    "failed to recycle GCV2 task",
                    &[
                        category(),
                        Field::new("safePoint", Value::U64(safe_point)),
                        error_field(&error),
                    ],
                );
            }
        }
        if role == ROLE_MASTER || role == ROLE_TTL_TASK_WORKER {
            match self.load_duration_with_default(GC_LIFE_TIME_KEY, GC_DEFAULT_LIFE_TIME) {
                Err(error) => logger().warn(
                    "failed to load GC life time for external workload",
                    &[category(), error_field(&error)],
                ),
                Ok(life_time) => {
                    let life_time = Duration::from_nanos(life_time.max(0) as u64);
                    if let Err(error) = manager.register_gcv2(safe_point, life_time) {
                        logger().warn(
                            "failed to register GCV2 task",
                            &[
                                category(),
                                Field::new("safePoint", Value::U64(safe_point)),
                                error_field(&error),
                            ],
                        );
                    }
                }
            }
        }
    }

    /// Go `deleteRanges`: every delete range with `ts < safe_point`, on
    /// `calcDeleteRangeConcurrency` workers.
    pub(crate) fn delete_ranges(
        &self,
        safe_point: u64,
        concurrency: GcConcurrency,
    ) -> GcResult<()> {
        gc_worker_counter("delete_range");

        let (ranges, v2) = {
            let mut session = self.session();
            let ranges = session.load_delete_ranges(false, safe_point)?;
            let v2 = session.is_raft_kv2()?;
            (ranges, v2)
        };
        // Table ids whose placement rules are already GC-ed, so one table's
        // rules are not removed once per range.
        let gc_placement_rule_cache = Mutex::new(HashSet::new());
        logger().info(
            "start delete ranges",
            &[
                category(),
                str_field("uuid", self.uuid.clone()),
                Field::new("ranges", Value::I64(ranges.len() as i64)),
            ],
        );
        let start = Instant::now();

        let delete_range_concurrency = calc_delete_range_concurrency(concurrency, ranges.len());
        let delete_one = |range: &DelRangeTask| {
            let mut session = self.session();
            let result = if v2 {
                // raftstore-v2 deletes the range instead, so nothing is
                // omitted.
                self.deps.kv.delete_range_task(
                    &range.start_key,
                    &range.end_key,
                    delete_range_concurrency,
                )
            } else {
                self.do_unsafe_destroy_range_request(&range.start_key, &range.end_key)
            };
            if let Err(error) = result {
                logger().warn(
                    "delete range failed on range",
                    &[
                        category(),
                        str_field("uuid", self.uuid.clone()),
                        str_field("startKey", hex_encode(&range.start_key)),
                        str_field("endKey", hex_encode(&range.end_key)),
                        error_field(&error),
                    ],
                );
                return;
            }
            if let Err(error) = do_gc_placement_rules(
                session.as_mut(),
                self.deps.rules.as_ref(),
                safe_point,
                range,
                &gc_placement_rule_cache,
            ) {
                logger().warn(
                    "gc placement rules failed on range",
                    &[
                        category(),
                        str_field("uuid", self.uuid.clone()),
                        Field::new("jobID", Value::I64(range.job_id)),
                        Field::new("elementID", Value::I64(range.element_id)),
                        error_field(&error),
                    ],
                );
                return;
            }
            // Only rules are deleted, so a concurrent rule update cannot fail
            // this.
            if let Err(error) = self.do_gc_label_rules(range) {
                logger().error(
                    "gc label rules failed on range",
                    &[
                        category(),
                        str_field("uuid", self.uuid.clone()),
                        Field::new("jobID", Value::I64(range.job_id)),
                        Field::new("elementID", Value::I64(range.element_id)),
                        error_field(&error),
                    ],
                );
                return;
            }
            if let Err(error) = session.complete_delete_range(range, !v2) {
                logger().error(
                    "failed to mark delete range task done",
                    &[
                        category(),
                        str_field("uuid", self.uuid.clone()),
                        str_field("startKey", hex_encode(&range.start_key)),
                        str_field("endKey", hex_encode(&range.end_key)),
                        error_field(&error),
                    ],
                );
                unsafe_destroy_range_failures_counter("save");
            }
        };
        run_bounded(&ranges, delete_range_concurrency, delete_one);

        logger().info(
            "finish delete ranges",
            &[
                category(),
                str_field("uuid", self.uuid.clone()),
                Field::new("num of ranges", Value::I64(ranges.len() as i64)),
                Field::new(
                    "cost time",
                    Value::Duration(start.elapsed().as_nanos() as i64),
                ),
            ],
        );
        gc_histogram("delete_ranges", start.elapsed().as_secs_f64());
        Ok(())
    }

    /// Go `redoDeleteRanges`: destroys again every range finished at least
    /// `lifetime + 24h` ago (TiKV RFC #2).
    pub(crate) fn redo_delete_ranges(
        &self,
        safe_point: u64,
        concurrency: GcConcurrency,
    ) -> GcResult<()> {
        gc_worker_counter("redo_delete_range");

        // Check the delete range records deleted about 24 hours ago.
        let redo_delete_ranges_ts = safe_point.wrapping_sub(compose_ts(
            GC_REDO_DELETE_RANGE_DELAY.as_secs() as i64 * 1000,
            0,
        ));
        let ranges = self
            .session()
            .load_delete_ranges(true, redo_delete_ranges_ts)?;

        logger().info(
            "start redo-delete ranges",
            &[
                category(),
                str_field("uuid", self.uuid.clone()),
                Field::new("num of ranges", Value::I64(ranges.len() as i64)),
            ],
        );
        let start = Instant::now();

        let delete_range_concurrency = calc_delete_range_concurrency(concurrency, ranges.len());
        let redo_one = |range: &DelRangeTask| {
            if let Err(error) =
                self.do_unsafe_destroy_range_request(&range.start_key, &range.end_key)
            {
                logger().warn(
                    "redo-delete range failed on range",
                    &[
                        category(),
                        str_field("uuid", self.uuid.clone()),
                        str_field("startKey", hex_encode(&range.start_key)),
                        str_field("endKey", hex_encode(&range.end_key)),
                        error_field(&error),
                    ],
                );
                return;
            }
            if let Err(error) = self.session().delete_done_record(range) {
                logger().error(
                    "failed to remove delete_range_done record",
                    &[
                        category(),
                        str_field("uuid", self.uuid.clone()),
                        str_field("startKey", hex_encode(&range.start_key)),
                        str_field("endKey", hex_encode(&range.end_key)),
                        error_field(&error),
                    ],
                );
                unsafe_destroy_range_failures_counter("save_redo");
            }
        };
        run_bounded(&ranges, delete_range_concurrency, redo_one);

        logger().info(
            "finish redo-delete ranges",
            &[
                category(),
                str_field("uuid", self.uuid.clone()),
                Field::new("num of ranges", Value::I64(ranges.len() as i64)),
                Field::new(
                    "cost time",
                    Value::Duration(start.elapsed().as_nanos() as i64),
                ),
            ],
        );
        gc_histogram("redo_delete_ranges", start.elapsed().as_secs_f64());
        Ok(())
    }

    /// Go `doUnsafeDestroyRangeRequest`: `UnsafeDestroyRange` on every store
    /// GC must reach, each failure reported.
    pub(crate) fn do_unsafe_destroy_range_request(&self, start: &[u8], end: &[u8]) -> GcResult<()> {
        // Every range reads the store list afresh, so it is less likely to be
        // stale.
        let stores = match self.get_stores_for_gc() {
            Ok(stores) => stores,
            Err(error) => {
                logger().error(
                    "delete ranges: got an error while trying to get store list from PD",
                    &[
                        category(),
                        str_field("uuid", self.uuid.clone()),
                        error_field(&error),
                    ],
                );
                unsafe_destroy_range_failures_counter("get_stores");
                return Err(error);
            }
        };

        let errors: Vec<String> = std::thread::scope(|scope| {
            let sends: Vec<_> = stores
                .iter()
                .map(|store| {
                    scope.spawn(move || {
                        let outcome = match self.deps.kv.unsafe_destroy_range(
                            &store.address,
                            start,
                            end,
                            UNSAFE_DESTROY_RANGE_TIMEOUT,
                        ) {
                            Ok(None) => Err(format!(
                                "unsafe destroy range returns nil response from store {}",
                                store.id
                            )),
                            Ok(Some(error)) if !error.is_empty() => Err(format!(
                                "unsafe destroy range failed on store {}: {error}",
                                store.id
                            )),
                            Ok(Some(_)) => Ok(()),
                            Err(error) => Err(error.0),
                        };
                        if outcome.is_err() {
                            unsafe_destroy_range_failures_counter("send");
                        }
                        outcome
                    })
                })
                .collect();
            sends
                .into_iter()
                .filter_map(|send| match send.join() {
                    Ok(Ok(())) => None,
                    Ok(Err(error)) => Some(error),
                    Err(_) => Some("unsafe destroy range panicked".to_owned()),
                })
                .collect()
        });

        if !errors.is_empty() {
            return Err(GcError::new(format!(
                "[gc worker] destroy range finished with errors: [{}]",
                errors.join(" ")
            )));
        }
        Ok(())
    }

    /// Go `getStoresForGC`: the stores the store-level GC requests go to.
    pub(crate) fn get_stores_for_gc(&self) -> GcResult<Vec<metapb::Store>> {
        let stores = self.deps.pd.get_all_stores()?;
        let mut up_stores = Vec::with_capacity(stores.len());
        for store in stores {
            if needs_gc_operation_for_store(&store)? {
                up_stores.push(store);
            }
        }
        Ok(up_stores)
    }

    /// Go `loadGCConcurrencyWithDefault`. Like Go, it returns a value
    /// together with the error, which callers fall back to.
    pub(crate) fn load_gc_concurrency_with_default(&self) -> (i64, GcResult<()>) {
        let value = match self.load_value_from_sys_table(GC_CONCURRENCY_KEY) {
            Ok(value) => value,
            Err(error) => return (GC_DEFAULT_CONCURRENCY, Err(error)),
        };
        if value.is_empty() {
            if let Err(error) = self
                .save_value_to_sys_table(GC_CONCURRENCY_KEY, &GC_DEFAULT_CONCURRENCY.to_string())
            {
                return (GC_DEFAULT_CONCURRENCY, Err(error));
            }
            return (GC_DEFAULT_CONCURRENCY, Ok(()));
        }
        let job_concurrency = match go_atoi(&value) {
            Ok(value) => value,
            Err(error) => return (GC_DEFAULT_CONCURRENCY, Err(error)),
        };
        (
            job_concurrency.clamp(GC_MIN_CONCURRENCY, GC_MAX_CONCURRENCY),
            Ok(()),
        )
    }

    // Go keeps it for its tests; production never calls it.
    #[cfg_attr(not(test), allow(dead_code))]
    /// Go `checkUseDistributedGC`: central mode is gone since v5.0, so this
    /// always answers true, after saving the default mode and warning about
    /// any other.
    pub(crate) fn check_use_distributed_gc(&self) -> bool {
        let mut mode = self.load_value_from_sys_table(GC_MODE_KEY);
        if matches!(&mode, Ok(value) if value.is_empty()) {
            if let Err(error) = self.save_value_to_sys_table(GC_MODE_KEY, GC_MODE_DEFAULT) {
                mode = Err(error);
            }
        }
        match mode {
            Err(error) => {
                logger().error(
                    "failed to load gc mode, fall back to distributed mode",
                    &[category(), str_field("uuid", self.uuid.clone()), error_field(&error)],
                );
                gc_job_failure_counter("check_gc_mode");
            }
            Ok(mode) if mode.eq_ignore_ascii_case(GC_MODE_CENTRAL) => logger().warn(
                "user configured to use central mode GC, which is no longer available. distributed mode will still be used",
                &[category()],
            ),
            // Go compares the value it loaded, which is empty right after it
            // saved the default.
            Ok(mode) if !mode.eq_ignore_ascii_case(GC_MODE_DISTRIBUTED) => logger().warn(
                "distributed mode will be used",
                &[category(), str_field("invalid gc mode", mode)],
            ),
            Ok(_) => {}
        }
        true
    }

    /// Go `resolveLocks`: settles every lock older than `txn_safe_point`,
    /// over the null keyspace and every keyspace using unified GC.
    pub(crate) fn resolve_locks(&self, txn_safe_point: u64, concurrency: usize) -> GcResult<()> {
        gc_worker_counter("resolve_locks");
        logger().info(
            "start resolve locks",
            &[
                category(),
                str_field("uuid", self.uuid.clone()),
                Field::new("txnSafePoint", Value::U64(txn_safe_point)),
                Field::new("concurrency", Value::U64(concurrency as u64)),
            ],
        );
        let start = Instant::now();

        let mut runner_name = "resolve-locks-runner".to_owned();
        let keyspace_meta = self.deps.store.keyspace_meta();
        if self.keyspace_id != NULL_KEYSPACE_ID {
            if let Some(meta) = &keyspace_meta {
                runner_name.push('-');
                runner_name.push_str(&meta.name);
            }
        }
        // `ResolveLocksForRange` takes the inclusive maximum start ts of the
        // locks to resolve, while GC at a txn safe point keeps every
        // transaction with start ts >= the txn safe point valid.
        let runner = self.deps.kv.resolve_locks_runner(
            &runner_name,
            concurrency,
            txn_safe_point.wrapping_sub(1),
            tikv_client::tikv::GC_SCAN_LOCK_LIMIT,
        );

        let is_null_keyspace = keyspace_meta.is_none();
        let batch_size = self.load_keyspaces_batch_size.load(Ordering::Acquire);
        let mut batch_count = 0usize;
        let record_batch_count = |count: usize| {
            self.load_keyspaces_batch_count
                .store(count, Ordering::Release);
        };

        let mut keyspace_batch = Vec::new();
        if is_null_keyspace {
            batch_count += 1;
            match self.deps.pd.get_all_keyspaces(0, batch_size) {
                Ok(batch) => keyspace_batch = batch,
                Err(error) => {
                    record_batch_count(batch_count);
                    return Err(error);
                }
            }
        }

        // A keyspace other than the null keyspace uses keyspace-level GC and
        // resolves its own unbounded range (the codec adds the prefix); with
        // no keyspace in the cluster at all, the whole key space is one range.
        if !is_null_keyspace || keyspace_batch.is_empty() {
            record_batch_count(batch_count);
            if let Err(error) = runner.run_on_range(b"", b"") {
                logger().warn(
                    "resolve locks failed",
                    &[
                        category(),
                        str_field("uuid", self.uuid.clone()),
                        Field::new("txnSafePoint", Value::U64(txn_safe_point)),
                        error_field(&error),
                    ],
                );
                return Err(error);
            }
            logger().info(
                "finish resolve locks",
                &[
                    category(),
                    str_field("uuid", self.uuid.clone()),
                    Field::new("txnSafePoint", Value::U64(txn_safe_point)),
                    Field::new("regions", Value::I64(runner.completed_regions() as i64)),
                ],
            );
            gc_histogram("resolve_locks", start.elapsed().as_secs_f64());
            return Ok(());
        }

        // Otherwise the null keyspace resolves locks for itself and for every
        // keyspace configured with unified GC.
        let mut is_successful = true;

        // First the null keyspace's txn key range, minus the keyspace prefixes.
        {
            let exclude_prefixes = tikv_client::tikv::codec_v1_exclude_prefixes();
            let mut ranges: Vec<(Vec<u8>, Vec<u8>)> =
                Vec::with_capacity(exclude_prefixes.len() + 1);
            let mut next_start_key = Vec::new();
            for prefix in &exclude_prefixes {
                ranges.push((next_start_key, prefix.to_vec()));
                next_start_key = tikv_client::kv::prefix_next_key(prefix);
            }
            // The rest after the last excluded prefix, unless a prefix of only
            // 0xff bytes made the next key the global end.
            if !(!exclude_prefixes.is_empty() && next_start_key.is_empty()) {
                ranges.push((next_start_key, Vec::new()));
            }
            for (range_start, range_end) in &ranges {
                if let Err(error) = runner.run_on_range(range_start, range_end) {
                    logger().warn(
                        "resolve locks for null keyspace sub-range failed",
                        &[
                            category(),
                            str_field("uuid", self.uuid.clone()),
                            Field::new("txnSafePoint", Value::U64(txn_safe_point)),
                            str_field("subRangeStartKey", hex_encode(range_start)),
                            str_field("subRangeEndKey", hex_encode(range_end)),
                            error_field(&error),
                        ],
                    );
                    is_successful = false;
                }
            }
        }

        // Then the keyspaces with unified GC, batch by batch.
        while !keyspace_batch.is_empty() {
            for keyspace in &keyspace_batch {
                if keyspace.state != KeyspaceState::Enabled as i32 {
                    continue;
                }
                if is_keyspace_using_keyspace_level_gc(Some(keyspace)) {
                    continue;
                }
                let codec = match tikv_client::tikv::ApiV2Codec::new(
                    tikv_client::tikv::KeyMode::Txn,
                    keyspace_id_of(keyspace),
                ) {
                    Ok(codec) => codec,
                    Err(error) => {
                        let error = GcError::new(format!(
                            "failed to find codec for keyspace when trying to resolve locks for it, keyspaceID: {}, keyspaceName: {}: {error}",
                            keyspace_id_of(keyspace),
                            keyspace.name
                        ));
                        logger().warn(
                            "resolve locks for unified-GC keyspace failed",
                            &[
                                category(),
                                str_field("uuid", self.uuid.clone()),
                                Field::new("txnSafePoint", Value::U64(txn_safe_point)),
                                error_field(&error),
                            ],
                        );
                        is_successful = false;
                        continue;
                    }
                };
                let (range_start, range_end) = codec.encode_range(b"", b"");
                if let Err(error) = runner.run_on_range(&range_start, &range_end) {
                    logger().warn(
                        "resolve locks for unified-GC keyspace failed",
                        &[
                            category(),
                            str_field("uuid", self.uuid.clone()),
                            Field::new("txnSafePoint", Value::U64(txn_safe_point)),
                            error_field(&error),
                        ],
                    );
                    is_successful = false;
                }
            }

            let next_keyspace_id = keyspace_id_of(
                keyspace_batch
                    .last()
                    .expect("the loop runs only on a non-empty batch"),
            ) + 1;
            if next_keyspace_id > MAX_KEYSPACE_ID {
                break;
            }
            batch_count += 1;
            match self.deps.pd.get_all_keyspaces(next_keyspace_id, batch_size) {
                Ok(batch) => keyspace_batch = batch,
                Err(error) => {
                    record_batch_count(batch_count);
                    return Err(error);
                }
            }
        }
        record_batch_count(batch_count);

        if !is_successful {
            return Err(GcError::new("resolve locks is not completely successful"));
        }
        Ok(())
    }

    /// Go `broadcastGCSafePoint`.
    pub(crate) fn broadcast_gc_safe_point(&self, gc_safe_point: u64) -> GcResult<()> {
        let result = match self.deps.pd.advance_gc_safe_point(gc_safe_point) {
            Ok(result) => result,
            Err(error) => {
                logger().error(
                    "failed to broadcast gc safe point",
                    &[
                        category(),
                        str_field("uuid", self.uuid.clone()),
                        Field::new("gcSafePoint", Value::U64(gc_safe_point)),
                        error_field(&error),
                    ],
                );
                return Err(error);
            }
        };
        if result.new_gc_safe_point != gc_safe_point {
            logger().warn(
                "gc safe point not advanced to the expected value",
                &[
                    category(),
                    str_field("uuid", self.uuid.clone()),
                    Field::new("target", Value::U64(gc_safe_point)),
                    Field::new("oldGCSafePoint", Value::U64(result.old_gc_safe_point)),
                    Field::new("newGCSafePoint", Value::U64(result.new_gc_safe_point)),
                ],
            );
        }
        Ok(())
    }

    // Go keeps it for `TestDoGCForOneRegion`.
    #[cfg_attr(not(test), allow(dead_code))]
    /// Go `doGCForRegion`: `Ok(Some(region error))`, or an error, never both.
    pub(crate) fn do_gc_for_region(
        &self,
        safe_point: u64,
        region: &tikv_client::RegionVerId,
    ) -> GcResult<Option<tikv_client::proto::errorpb::Error>> {
        self.deps.kv.gc_region(region, safe_point)
    }

    /// Go `checkLeader`: renews this worker's lease, or takes over an expired
    /// one.
    pub(crate) fn check_leader(&self) -> GcResult<bool> {
        let label =
            if !is_keyspace_using_keyspace_level_gc(self.deps.store.keyspace_meta().as_ref()) {
                "check_leader"
            } else {
                "check_leader_keyspace"
            };
        gc_worker_counter(label);
        let mut session = self.session();

        session.begin()?;
        let leader = match self.load_value_from_sys_table(GC_LEADER_UUID_KEY) {
            Ok(leader) => leader,
            Err(error) => {
                session.rollback();
                return Err(error);
            }
        };
        logger().debug(
            "got leader",
            &[category(), str_field("uuid", leader.clone())],
        );
        if leader == self.uuid {
            if let Err(error) = self.save_time(GC_LEADER_LEASE_KEY, lease_deadline()) {
                session.rollback();
                return Err(error);
            }
            session.commit()?;
            return Ok(true);
        }
        session.rollback();

        session.begin()?;
        let lease = match self.load_time(GC_LEADER_LEASE_KEY) {
            Ok(lease) => lease,
            Err(error) => {
                session.rollback();
                return Err(error);
            }
        };
        if lease.is_none_or(|lease| lease < now()) {
            logger().debug(
                "register as leader",
                &[category(), str_field("uuid", self.uuid.clone())],
            );
            gc_worker_counter("register_leader");
            let registered = self
                .save_value_to_sys_table(GC_LEADER_UUID_KEY, &self.uuid)
                .and_then(|()| self.save_value_to_sys_table(GC_LEADER_DESC_KEY, &self.desc))
                .and_then(|()| self.save_time(GC_LEADER_LEASE_KEY, lease_deadline()));
            if let Err(error) = registered {
                session.rollback();
                return Err(error);
            }
            session.commit()?;
            return Ok(true);
        }
        session.rollback();
        Ok(false)
    }

    /// Go `saveTime`.
    pub(crate) fn save_time(&self, key: &str, time: DateTime<FixedOffset>) -> GcResult<()> {
        self.save_value_to_sys_table(key, &format_gc_time(time))
    }

    /// Go `loadTime`.
    pub(crate) fn load_time(&self, key: &str) -> GcResult<Option<DateTime<FixedOffset>>> {
        let value = self.load_value_from_sys_table(key)?;
        if value.is_empty() {
            return Ok(None);
        }
        tikv_client::util::compatible_parse_gc_time(&value)
            .map(Some)
            .map_err(|error| GcError::new(error.to_string()))
    }

    /// Go `saveDuration`.
    pub(crate) fn save_duration(&self, key: &str, nanos: i64) -> GcResult<()> {
        self.save_value_to_sys_table(key, &tidb_config::configtypes::format_go_duration(nanos))
    }

    /// Go `loadDuration`.
    pub(crate) fn load_duration(&self, key: &str) -> GcResult<Option<i64>> {
        let value = self.load_value_from_sys_table(key)?;
        if value.is_empty() {
            return Ok(None);
        }
        tidb_config::configtypes::parse_go_duration(&value)
            .map(Some)
            .map_err(GcError::new)
    }

    /// Go `loadDurationWithDefault`.
    pub(crate) fn load_duration_with_default(&self, key: &str, default: i64) -> GcResult<i64> {
        match self.load_duration(key)? {
            Some(value) => Ok(value),
            None => {
                self.save_duration(key, default)?;
                Ok(default)
            }
        }
    }

    /// Go `loadValueFromSysTable`: the empty string when the row is missing.
    pub(crate) fn load_value_from_sys_table(&self, key: &str) -> GcResult<String> {
        let value = self.session().load_value(key)?;
        match &value {
            None => logger().debug("load kv", &[category(), str_field("key", key)]),
            Some(value) => logger().debug(
                "load kv",
                &[
                    category(),
                    str_field("key", key),
                    str_field("value", value.clone()),
                ],
            ),
        }
        Ok(value.unwrap_or_default())
    }

    /// Go `saveValueToSysTable`.
    pub(crate) fn save_value_to_sys_table(&self, key: &str, value: &str) -> GcResult<()> {
        let result = self
            .session()
            .save_value(key, value, gc_variable_comment(key));
        logger().debug(
            "save kv",
            &[
                category(),
                str_field("key", key),
                str_field("value", value),
                str_field(
                    "error",
                    result
                        .as_ref()
                        .err()
                        .map(|error| error.0.clone())
                        .unwrap_or_default(),
                ),
            ],
        );
        result
    }

    /// Go `doGCLabelRules`.
    pub(crate) fn do_gc_label_rules(&self, range: &DelRangeTask) -> GcResult<()> {
        let mut history_job = {
            let mut session = self.session();
            session
                .history_job(range.job_id)?
                .ok_or_else(|| ddl_job_not_found(range.job_id))?
        };
        if history_job.type_ != tidb_model::ActionType::ACTION_DROP_TABLE {
            return Ok(());
        }
        let args = tidb_model::get_finished_drop_table_args(&mut history_job)
            .map_err(|error| GcError::new(error.to_string()))?
            .ok_or_else(|| GcError::new("drop table job carries no arguments"))?;
        let (mut physical_table_ids, rule_ids) = {
            let args = args.read();
            (args.old_partition_ids.clone(), args.old_rule_ids.clone())
        };
        // TODO in Go too: the rules are read from PD and filtered.
        let rules = self.deps.rules.get_label_rules(&rule_ids)?;
        physical_table_ids.push(history_job.table_id);
        let rule_ids = get_gc_rules(&physical_table_ids, &rules);
        self.deps.rules.delete_label_rules(rule_ids)
    }
}

fn lease_deadline() -> DateTime<FixedOffset> {
    now() + chrono::Duration::from_std(GC_WORKER_LEASE).expect("a two-minute lease")
}

/// Go `time.Format(tikvutil.GCTimeFormat)`.
pub(crate) fn format_gc_time(time: DateTime<FixedOffset>) -> String {
    time.format("%Y%m%d-%H:%M:%S%.3f %z").to_string()
}

/// Go `strconv.Atoi`.
fn go_atoi(value: &str) -> GcResult<i64> {
    let digits = value.strip_prefix(['+', '-']).unwrap_or(value);
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(GcError::new(format!(
            "strconv.Atoi: parsing {value:?}: invalid syntax"
        )));
    }
    value.parse::<i64>().map_err(|_| {
        GcError::new(format!(
            "strconv.Atoi: parsing {value:?}: value out of range"
        ))
    })
}

fn ddl_job_not_found(job_id: i64) -> GcError {
    // Go `dbterror.ErrDDLJobNotFound`: 8223, "DDL Job:%v not found".
    GcError::new(
        tidb_util::dbterror::ERR_DDL_JOB_NOT_FOUND
            .generate(format!("DDL Job:{job_id} not found"))
            .to_string(),
    )
}

/// Runs `work` on every item with at most `concurrency` running at once, as
/// Go's `concurrencyLimiter` channel and `WaitGroupWrapper` do.
fn run_bounded<T: Sync>(items: &[T], concurrency: usize, work: impl Fn(&T) + Sync) {
    let next = AtomicUsize::new(0);
    std::thread::scope(|scope| {
        for _ in 0..concurrency.max(1).min(items.len()) {
            scope.spawn(|| loop {
                let index = next.fetch_add(1, Ordering::AcqRel);
                let Some(item) = items.get(index) else {
                    return;
                };
                work(item);
            });
        }
    });
}

/// Go `calcDeleteRangeConcurrency`.
///
/// The concurrency of `deleteRanges` derives from the one `resolveLocks`
/// uses (`tidb_gc_concurrency`), reduced so GC stays gentle on foreground
/// requests: at about 50ms a request, one round of 100,000 requests a thread
/// takes about 5,000s at most.
pub fn calc_delete_range_concurrency(concurrency: GcConcurrency, range_num: usize) -> usize {
    let max_concurrency = (concurrency.v / CONCURRENCY_DIVISOR).max(1);
    let threads_based_on_requests = (range_num / REQUESTS_PER_THREAD).max(1);
    if concurrency.is_auto {
        return max_concurrency.min(threads_based_on_requests);
    }
    max_concurrency
}

/// Go `needsGCOperationForStore`: whether store-level GC requests
/// (`UnsafeDestroyRange`, physical lock scans) go to `store`.
pub fn needs_gc_operation_for_store(store: &metapb::Store) -> GcResult<bool> {
    // A tombstone store has no peer left; an offline one still does until it
    // becomes a tombstone, so it is collected.
    if store.state == metapb::StoreState::Tombstone as i32 {
        return Ok(false);
    }
    let engine_label = store
        .labels
        .iter()
        .find(|label| label.key == tidb_placement::ENGINE_LABEL_KEY)
        .map_or("", |label| label.value.as_str());
    match engine_label {
        // TiFlash deletes dropped tables its own way and holds only learner
        // peers whose data is in TiKV, so it needs neither request.
        tidb_placement::ENGINE_LABEL_TIFLASH => Ok(false),
        tidb_placement::ENGINE_LABEL_TIFLASH_COMPUTE => {
            logger().debug("will ignore gc tiflash_compute node", &[category()]);
            Ok(false)
        }
        // A store without an engine label is a TiKV node.
        tidb_placement::ENGINE_LABEL_TIKV | "" => Ok(true),
        _ => Err(GcError::new(format!(
            "unsupported store engine \"{engine_label}\" with storeID {}, addr {}",
            store.id, store.address
        ))),
    }
}

/// Go `doGCPlacementRules`: removes the placement rules of the physical
/// tables a finished job dropped. They cannot go at DROP / TRUNCATE time,
/// because the table can still be flashed back or recovered.
pub(crate) fn do_gc_placement_rules(
    session: &mut dyn GcSession,
    rules: &dyn GcRuleClient,
    _safe_point: u64,
    range: &DelRangeTask,
    gc_placement_rule_cache: &Mutex<HashSet<i64>>,
) -> GcResult<()> {
    use tidb_model::ActionType;

    let mut history_job = session
        .history_job(range.job_id)?
        .ok_or_else(|| ddl_job_not_found(range.job_id))?;

    // PD drops the rules of these ids, even if some have none.
    let decode_error = |error: serde_json::Error| GcError::new(error.to_string());
    let missing = || GcError::new("finished job carries no arguments");
    let mut physical_table_ids: Vec<i64> = match history_job.type_ {
        ActionType::ACTION_DROP_TABLE => {
            let args = tidb_model::get_finished_drop_table_args(&mut history_job)
                .map_err(decode_error)?
                .ok_or_else(missing)?;
            let mut ids = args.read().old_partition_ids.clone();
            ids.push(history_job.table_id);
            ids
        }
        ActionType::ACTION_TRUNCATE_TABLE | ActionType::ACTION_TRUNCATE_TABLE_PARTITION => {
            let args = tidb_model::get_finished_truncate_table_args(&mut history_job)
                .map_err(decode_error)?
                .ok_or_else(missing)?;
            let mut ids = args.read().old_partition_ids.get().snapshot();
            if history_job.type_ == ActionType::ACTION_TRUNCATE_TABLE {
                ids.push(history_job.table_id);
            }
            ids
        }
        ActionType::ACTION_DROP_TABLE_PARTITION
        | ActionType::ACTION_REORGANIZE_PARTITION
        | ActionType::ACTION_REMOVE_PARTITIONING
        | ActionType::ACTION_ALTER_TABLE_PARTITIONING => {
            let args = tidb_model::get_finished_table_partition_args(&mut history_job)
                .map_err(decode_error)?
                .ok_or_else(missing)?;
            let ids = args.read().old_physical_table_ids.get().snapshot();
            ids
        }
        ActionType::ACTION_DROP_SCHEMA => {
            let args = tidb_model::get_finished_drop_schema_args(&mut history_job)
                .map_err(decode_error)?
                .ok_or_else(missing)?;
            let ids = args.read().all_dropped_table_ids.get().snapshot();
            ids
        }
        _ => Vec::new(),
    };

    // Skip the ids already handled.
    {
        let cache = gc_placement_rule_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        physical_table_ids.retain(|id| !cache.contains(id));
    }
    if physical_table_ids.is_empty() {
        return Ok(());
    }

    if let Err(error) = rules.delete_tiflash_placement_rules(&physical_table_ids) {
        logger().error(
            "delete placement rules failed",
            &[
                error_field(&error),
                str_field("tableIDs", format!("{physical_table_ids:?}")),
            ],
        );
    }
    let bundles = physical_table_ids
        .iter()
        .map(|&id| tidb_placement::new_bundle(id))
        .collect();
    rules.put_rule_bundles(bundles)?;

    // Cache the ids whose rules are deleted.
    gc_placement_rule_cache
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .extend(physical_table_ids);
    Ok(())
}

/// Go `getGCRules`: the ids of the rules whose key ranges are exactly the
/// tables `ids`.
pub fn get_gc_rules(
    ids: &[i64],
    rules: &HashMap<String, tidb_executor::ddl_label::Rule>,
) -> Vec<String> {
    let mut old_ranges = HashSet::new();
    for &id in ids {
        let mut start_key = Vec::new();
        tidb_codec::encode_bytes(&mut start_key, &tidb_codec::table_key::gen_table_prefix(id));
        let mut end_key = Vec::new();
        tidb_codec::encode_bytes(
            &mut end_key,
            &tidb_codec::table_key::gen_table_prefix(id + 1),
        );
        old_ranges.insert(format!(
            "{}{}",
            hex_encode(&start_key),
            hex_encode(&end_key)
        ));
    }

    let mut gc_rules = Vec::new();
    for rule in rules.values() {
        let data = rule.data.as_ref().map(|data| data.read().clone());
        let found = match data {
            Some(serde_json::Value::Array(items)) => items.iter().any(|item| {
                let serde_json::Value::Object(range) = item else {
                    return false;
                };
                // Go formats the map values with `%s`.
                let text = |key: &str| match range.get(key) {
                    Some(serde_json::Value::String(value)) => value.clone(),
                    Some(value) => value.to_string(),
                    None => "%!s(<nil>)".to_owned(),
                };
                old_ranges.contains(&format!("{}{}", text("start_key"), text("end_key")))
            }),
            // Go type-asserts `rule.Data.([]any)` and panics otherwise.
            _ => panic!("label rule {} data is not a list", rule.id),
        };
        if found {
            gc_rules.push(rule.id.clone());
        }
    }
    gc_rules
}

/// Go `RunGCJob`: centralized GC is gone, so it runs the distributed job.
/// Only for the kv API and tests; never alongside a running `GcWorker`.
pub fn run_gc_job(
    deps: GcWorkerDeps,
    safe_point: u64,
    identifier: &str,
    concurrency: usize,
) -> GcResult<()> {
    run_distributed_gc_job(deps, safe_point, identifier, concurrency)
}

/// Go `RunDistributedGCJob`: advances the txn safe point, resolves locks and
/// advances the GC safe point. It may take a while because of
/// `resolveLocks`; `concurrency` is that phase's.
pub fn run_distributed_gc_job(
    deps: GcWorkerDeps,
    safe_point: u64,
    identifier: &str,
    concurrency: usize,
) -> GcResult<()> {
    let worker =
        GcWorker::with_identity(deps, identifier.to_owned(), String::new(), NULL_KEYSPACE_ID);
    let new_txn_safe_point = worker.job.advance_txn_safe_point(safe_point)?;
    // Let the txn safe point synchronize.
    std::thread::sleep(worker.job.sync_wait_time());
    worker.job.resolve_locks(new_txn_safe_point, concurrency)?;
    let gc_safe_point = new_txn_safe_point;
    worker.job.broadcast_gc_safe_point(gc_safe_point)
}

/// Go `RunResolveLocks`: resolves every lock before `safe_point`. Only for
/// tests.
pub fn run_resolve_locks(
    deps: GcWorkerDeps,
    safe_point: u64,
    identifier: &str,
    concurrency: usize,
) -> GcResult<()> {
    let worker =
        GcWorker::with_identity(deps, identifier.to_owned(), String::new(), NULL_KEYSPACE_ID);
    worker.job.resolve_locks(safe_point, concurrency)
}

/// Go `MockGCWorker`: a worker that only deletes ranges, for tests.
pub struct MockGcWorker {
    worker: GcWorker,
}

impl MockGcWorker {
    /// Go `NewMockGCWorker`.
    pub fn new(deps: GcWorkerDeps) -> GcResult<Self> {
        let version = deps.store.current_version()?;
        let host_name =
            tidb_util::sem::operating_system_hostname().unwrap_or_else(|| "unknown".to_owned());
        let desc = format!(
            "host:{host_name}, pid:{}, start at {}",
            std::process::id(),
            go_time_now_string()
        );
        Ok(Self {
            worker: GcWorker::with_identity(deps, format!("{version:x}"), desc, NULL_KEYSPACE_ID),
        })
    }

    /// Go `DeleteRanges`: `deleteRanges` with a concurrency of one.
    pub fn delete_ranges(&self, safe_point: u64) -> GcResult<()> {
        logger().error("deleteRanges is called", &[]);
        self.worker.job.delete_ranges(
            safe_point,
            GcConcurrency {
                v: 1,
                is_auto: false,
            },
        )
    }
}
