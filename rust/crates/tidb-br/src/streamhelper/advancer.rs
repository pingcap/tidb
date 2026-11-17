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


//! Go `streamhelper/advancer.go` and `advancer_daemon.go`: the central node
//! of checkpoint v3. On the owner it collects the region checkpoints of the
//! task's ranges from TiKV (flush events, then `GetLastFlushTSOfRegion` for
//! the lagging ranges), uploads the minimum as the global checkpoint, holds
//! the GC safe point at it, and resolves the locks that keep it from moving.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock};
use std::time::{Duration, Instant, SystemTime};

use tidb_gocontext::Context;
use tidb_log::{Field, Value};
use tidb_objstore::storeapi::Storage;
use tidb_proto::backup::{StorageBackend, StreamBackupTaskInfo};

use super::advancer_cliext::{EventType, TaskEvent};
use super::advancer_env::{Env, LogBackupService, TiKVClusterMeta};
use super::client::{pause_with_error_severity, pause_with_message};
use super::collector::ClusterCollector;
use super::config::{CommandConfig, Config, TiDBConfig, DEFAULT_MAX_CONCURRENCY_ADVANCE};
use super::flush_subscriber::{with_master_context, FlushSubscriber};
use super::models::{Range, CHECKPOINT_TYPE_GLOBAL};
use super::regioniter::{iterate_region, locate_key_of_region};
use crate::errors::{self, Result};
use crate::spans::sorted::{Valued, ValuedFull};
use crate::spans::value_sorted::{sorted, ValueSortedFull};
use crate::utils::worker_pool::{ErrGroup, WorkerPool};

/// Go `streamBackupGlobalCheckpointPrefix`.
const STREAM_BACKUP_GLOBAL_CHECKPOINT_PREFIX: &str = "v1/global_checkpoint";
/// Go `resolveLockMaxVersionMaxRetry`: retries with a lower max version
/// when `ScanLock` meets a newer in-memory lock.
const RESOLVE_LOCK_MAX_VERSION_MAX_RETRY: usize = 2;
/// Go `resolveLockRetryLowerBoundLag`.
const RESOLVE_LOCK_RETRY_LOWER_BOUND_LAG: Duration = Duration::from_secs(10);
/// Go `logBackupConfigRefreshInterval`.
const LOG_BACKUP_CONFIG_REFRESH_INTERVAL: Duration = Duration::from_secs(60);
/// Go `logBackupConfigFetchTimeout`.
const LOG_BACKUP_CONFIG_FETCH_TIMEOUT: Duration = Duration::from_secs(10);
/// Go `ownerPrompt`.
pub const OWNER_PROMPT: &str = "log-backup";
/// Go `ownerPath`.
pub const OWNER_PATH: &str = "/tidb/br-stream/owner";

/// Go `globalCheckpointFileName`.
fn global_checkpoint_file_name() -> String {
    format!("{CHECKPOINT_TYPE_GLOBAL}.ts")
}

/// Go `checkpoint`: a TS over a range, with when locks were last resolved
/// for it (local time; refreshed after a successful round, so `ScanLock`
/// runs at most once per flush interval).
#[derive(Clone, Debug)]
pub struct Checkpoint {
    /// Go `StartKey`.
    pub start_key: Vec<u8>,
    /// Go `EndKey`.
    pub end_key: Vec<u8>,
    /// Go `TS`.
    pub ts: u64,
    resolve_lock_time: Instant,
}

impl Checkpoint {
    /// Go `newCheckpointWithTS`.
    fn with_ts(ts: u64) -> Self {
        Self { start_key: Vec::new(), end_key: Vec::new(), ts, resolve_lock_time: Instant::now() }
    }

    /// Go `newCheckpointWithSpan`.
    fn with_span(s: &Valued) -> Self {
        Self {
            start_key: s.key.start_key.clone(),
            end_key: s.key.end_key.clone(),
            ts: s.value,
            resolve_lock_time: Instant::now(),
        }
    }

    /// Go `safeTS`.
    #[must_use]
    pub fn safe_ts(&self) -> u64 {
        self.ts.saturating_sub(1)
    }

    /// Go `equal`.
    fn equal(&self, o: &Self) -> bool {
        self.start_key == o.start_key && self.end_key == o.end_key && self.ts == o.ts
    }

    /// Go `needResolveLocks`.
    fn need_resolve_locks(&self, interval: Duration) -> bool {
        if let Some(v) = tidb_failpoint::eval("github.com/pingcap/tidb/br/pkg/streamhelper/NeedResolveLocks") {
            return v.bool();
        }
        self.resolve_lock_time.elapsed() > interval
    }
}

/// The task half of Go `CheckpointAdvancer`, guarded by `taskMu`.
#[derive(Default)]
struct TaskState {
    task: Option<StreamBackupTaskInfo>,
    task_range: Vec<Range>,
    checkpoint_storage: Option<Arc<dyn Storage>>,
    last_external_storage_checkpoint: u64,
}

/// Go `createGlobalCheckpointStorage`.
pub type CreateStorage = Arc<dyn Fn(&Context, &StorageBackend) -> Result<Arc<dyn Storage>> + Send + Sync>;

/// The bounds of one resolve-lock round (Go `asyncResolveLocksForRanges`'
/// arguments).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResolveLockPlan {
    /// The locks at or below this version are resolved.
    pub max_version: u64,
    /// The lowest max version a `ScanLock` retry may use.
    pub retry_lower_bound: u64,
    /// Whether `retry_lower_bound` is inside `(checkpoint, max_version)`.
    pub retry_lower_bound_valid: bool,
}

/// Go `CheckpointAdvancer`.
pub struct CheckpointAdvancer {
    env: Arc<dyn Env>,
    task: Mutex<TaskState>,
    cfg: RwLock<Arc<dyn Config>>,
    /// Nanoseconds; zero means "use the config".
    resolve_lock_interval: AtomicU64,
    /// Nanoseconds; zero means "use the config".
    try_advance_threshold: AtomicU64,
    last_checkpoint: Mutex<Option<Checkpoint>>,
    in_resolving_lock: AtomicBool,
    is_paused: AtomicBool,
    checkpoints: Mutex<Option<ValueSortedFull>>,
    subscriber: Mutex<Option<FlushSubscriber>>,
    create_global_checkpoint_storage: RwLock<CreateStorage>,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

fn rfc3339(t: SystemTime) -> String {
    chrono::DateTime::<chrono::Local>::from(t).to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

/// Go `tsoBefore`.
fn tso_before(n: Duration) -> u64 {
    tso_before_from(SystemTime::now(), n)
}

/// Go `tsoBeforeFrom`.
fn tso_before_from(now: SystemTime, n: Duration) -> u64 {
    tikv_client::oracle::go_time_to_ts(now.checked_sub(n).unwrap_or(SystemTime::UNIX_EPOCH))
}

/// Go `tsoBeforeFromTS`.
fn tso_before_from_ts(ts: u64, n: Duration) -> u64 {
    let physical = tikv_client::oracle::extract_physical(ts);
    let before_physical = physical - i64::try_from(n.as_millis()).unwrap_or(i64::MAX);
    if before_physical <= 0 {
        return 0;
    }
    tikv_client::oracle::compose_ts(before_physical, 0)
}

/// Go `tsoAfter`.
fn tso_after(ts: u64, n: Duration) -> u64 {
    tikv_client::oracle::go_time_to_ts(tikv_client::oracle::get_time_from_ts(ts) + n)
}

/// Go `resolveLockTargetUpperBound`.
fn resolve_lock_target_upper_bound(checkpoint_ts: u64, resolve_lock_interval: Duration, current_ts: u64) -> u64 {
    if resolve_lock_interval.is_zero() {
        return tso_after(checkpoint_ts, RESOLVE_LOCK_RETRY_LOWER_BOUND_LAG);
    }
    tso_before_from_ts(current_ts, 2 * resolve_lock_interval)
}

/// Go `resolveLockRetryLowerBound`.
fn resolve_lock_retry_lower_bound(checkpoint_ts: u64, max_version: u64) -> (u64, bool) {
    let lower_bound = tso_after(checkpoint_ts, RESOLVE_LOCK_RETRY_LOWER_BOUND_LAG);
    (lower_bound, lower_bound > checkpoint_ts && lower_bound < max_version)
}

/// Go `isScanLockLockedError`.
#[must_use]
pub fn is_scan_lock_locked_error(err: &str) -> bool {
    err.contains("unexpected scanlock error") && err.contains("locked")
}

/// Go `lowerResolveLockMaxVersion`: halfway into the retry window, so a
/// large lag moves away from newer memory locks quickly.
fn lower_resolve_lock_max_version(max_version: u64, lower_bound: u64) -> Option<u64> {
    if max_version <= lower_bound || max_version - lower_bound <= 1 {
        return None;
    }
    Some(lower_bound + (max_version - lower_bound) / 2)
}

/// Go `resolveLocksForRangeWithMaxVersionRetry`: `resolve(max_version)` is
/// `tikv.ResolveLocksForRange`, retried with a lower max version while it
/// fails on a `ScanLock` locked error.
pub async fn resolve_locks_for_range_with_max_version_retry<T, E, Fut>(
    plan: ResolveLockPlan,
    mut resolve: impl FnMut(u64) -> Fut,
) -> (T, std::result::Result<(), E>)
where
    E: std::fmt::Display,
    Fut: std::future::Future<Output = (T, std::result::Result<(), E>)>,
{
    let mut current_max_version = plan.max_version;
    let mut retry = 0;
    loop {
        let (stat, res) = resolve(current_max_version).await;
        let Err(err) = &res else { return (stat, res) };
        if !is_scan_lock_locked_error(&err.to_string()) || retry >= RESOLVE_LOCK_MAX_VERSION_MAX_RETRY {
            return (stat, res);
        }
        if !plan.retry_lower_bound_valid {
            return (stat, res);
        }
        let Some(next_max_version) = lower_resolve_lock_max_version(current_max_version, plan.retry_lower_bound) else {
            return (stat, res);
        };
        tidb_log::warn(
            "retry resolving locks with lower maxVersion due to ScanLock locked error",
            &[
                Field::new("current-max-version", Value::U64(current_max_version)),
                Field::new("next-max-version", Value::U64(next_max_version)),
                Field::new("error", Value::Str(err.to_string())),
            ],
        );
        current_max_version = next_max_version;
        retry += 1;
    }
}

impl CheckpointAdvancer {
    fn with_config(env: Arc<dyn Env>, cfg: Arc<dyn Config>) -> Self {
        Self {
            env,
            task: Mutex::default(),
            cfg: RwLock::new(cfg),
            resolve_lock_interval: AtomicU64::new(0),
            try_advance_threshold: AtomicU64::new(0),
            last_checkpoint: Mutex::new(None),
            in_resolving_lock: AtomicBool::new(false),
            is_paused: AtomicBool::new(false),
            checkpoints: Mutex::new(None),
            subscriber: Mutex::new(None),
            create_global_checkpoint_storage: RwLock::new(Arc::new(|ctx, backend| {
                tidb_objstore::create(ctx, backend, false).map_err(|e| errors::errorf(e.to_string()))
            })),
        }
    }

    /// Go `NewTiDBCheckpointAdvancer`.
    #[must_use]
    pub fn new_tidb(env: Arc<dyn Env>) -> Arc<Self> {
        Arc::new(Self::with_config(env, Arc::new(TiDBConfig::default())))
    }

    /// Go `NewCommandCheckpointAdvancer`.
    #[must_use]
    pub fn new_command(env: Arc<dyn Env>) -> Arc<Self> {
        Arc::new(Self::with_config(env, Arc::new(CommandConfig::default())))
    }

    /// Go `createGlobalCheckpointStorage`, replaced by tests.
    pub fn set_create_global_checkpoint_storage(&self, f: CreateStorage) {
        *self.create_global_checkpoint_storage.write().unwrap_or_else(PoisonError::into_inner) = f;
    }

    /// Go `HasTask`.
    #[must_use]
    pub fn has_task(&self) -> bool {
        lock(&self.task).task.is_some()
    }

    /// Go `HasSubscriptions`.
    #[must_use]
    pub fn has_subscriptions(&self) -> bool {
        lock(&self.subscriber).as_ref().is_some_and(FlushSubscriber::has_subscriptions)
    }

    /// Go `UpdateConfig`: called before the loop starts.
    pub fn update_config(&self, new_conf: Arc<dyn Config>) {
        *self.cfg.write().unwrap_or_else(PoisonError::into_inner) = new_conf;
    }

    /// Go `Config`.
    #[must_use]
    pub fn config(&self) -> Arc<dyn Config> {
        Arc::clone(&self.cfg.read().unwrap_or_else(PoisonError::into_inner))
    }

    fn get_resolve_lock_interval(&self) -> Duration {
        match self.resolve_lock_interval.load(Ordering::SeqCst) {
            0 => self.config().get_resolve_lock_interval(),
            n => Duration::from_nanos(n),
        }
    }

    fn get_default_start_poll_threshold(&self) -> Duration {
        match self.try_advance_threshold.load(Ordering::SeqCst) {
            0 => self.config().get_default_start_poll_threshold(),
            n => Duration::from_nanos(n),
        }
    }

    fn get_subscriber_error_start_poll_threshold(&self) -> Duration {
        match self.try_advance_threshold.load(Ordering::SeqCst) {
            0 => self.config().get_subscriber_error_start_poll_threshold(),
            n => Duration::from_nanos(n) * 9 / 20,
        }
    }

    /// Go `UpdateLastCheckpoint`.
    pub fn update_last_checkpoint(&self, p: Checkpoint) {
        *lock(&self.last_checkpoint) = Some(p);
    }

    /// The last checkpoint, for tests (Go reads the field).
    #[must_use]
    pub fn last_checkpoint(&self) -> Option<Checkpoint> {
        lock(&self.last_checkpoint).clone()
    }

    /// Go `GetInResolvingLock`.
    #[must_use]
    pub fn get_in_resolving_lock(&self) -> bool {
        self.in_resolving_lock.load(Ordering::SeqCst)
    }

    fn spawn_log_backup_config_updater(self: &Arc<Self>, ctx: &Context) {
        let this = Arc::clone(self);
        let ctx = ctx.clone();
        std::thread::spawn(move || {
            this.refresh_log_backup_flush_interval(&ctx);
            let ticker = crossbeam_channel::tick(LOG_BACKUP_CONFIG_REFRESH_INTERVAL);
            loop {
                crossbeam_channel::select! {
                    recv(ctx.done()) -> _ => return,
                    recv(ticker) -> _ => this.refresh_log_backup_flush_interval(&ctx),
                }
            }
        });
    }

    /// Go `refreshLogBackupFlushInterval`: the resolve-lock interval follows
    /// TiKV's flush interval, the poll threshold 4/3 of it.
    fn refresh_log_backup_flush_interval(&self, ctx: &Context) {
        let timeout = self.config().tick_timeout().max(LOG_BACKUP_CONFIG_FETCH_TIMEOUT);
        let (fetch_ctx, cancel) = Context::with_timeout(ctx, timeout);
        let res = self.env.get_log_backup_flush_interval(&fetch_ctx);
        cancel.cancel();
        let flush_interval = match res {
            Ok(d) => d,
            Err(err) => {
                tidb_log::warn(
                    "failed to refresh TiKV log-backup.max-flush-interval; keep previous advancer intervals",
                    &[
                        Field::new("current-resolve-lock-interval", Value::Str(format!("{:?}", self.get_resolve_lock_interval()))),
                        Field::new("current-try-advance-threshold", Value::Str(format!("{:?}", self.get_default_start_poll_threshold()))),
                        Field::new("error", Value::Str(err.to_string())),
                    ],
                );
                return;
            }
        };
        if flush_interval.is_zero() {
            tidb_log::warn(
                "ignore invalid TiKV log-backup.max-flush-interval; keep previous advancer intervals",
                &[Field::new("flush-interval", Value::Str(format!("{flush_interval:?}")))],
            );
            return;
        }
        let previous = self.get_resolve_lock_interval();
        let previous_try_advance_threshold = self.get_default_start_poll_threshold();
        self.resolve_lock_interval.store(u64::try_from(flush_interval.as_nanos()).unwrap_or(u64::MAX), Ordering::SeqCst);
        let try_advance_threshold = flush_interval * 4 / 3;
        self.try_advance_threshold
            .store(u64::try_from(try_advance_threshold.as_nanos()).unwrap_or(u64::MAX), Ordering::SeqCst);
        if previous != flush_interval || previous_try_advance_threshold != try_advance_threshold {
            tidb_log::info(
                "refreshed TiKV log-backup.max-flush-interval for advancer intervals",
                &[
                    Field::new("previous-resolve-lock-interval", Value::Str(format!("{previous:?}"))),
                    Field::new("resolve-lock-interval", Value::Str(format!("{flush_interval:?}"))),
                    Field::new("previous-try-advance-threshold", Value::Str(format!("{previous_try_advance_threshold:?}"))),
                    Field::new("try-advance-threshold", Value::Str(format!("{try_advance_threshold:?}"))),
                ],
            );
        }
    }

    /// Go `GetCheckpointInRange`: hands the regions of the range to the
    /// collector.
    ///
    /// # Errors
    /// The region scan or the collector failed.
    pub fn get_checkpoint_in_range(&self, ctx: &Context, start: &[u8], end: &[u8], collector: &ClusterCollector) -> Result<()> {
        let meta: &dyn TiKVClusterMeta = self.env.as_ref();
        let mut iter = iterate_region(meta, start, end);
        while !iter.done() {
            for r in iter.next(ctx)? {
                collector.collect_region(r)?;
            }
        }
        Ok(())
    }

    /// Go `tryAdvance`: polls the checkpoints of `ranges` (within the task's
    /// ranges) into the span tree.
    fn try_advance(self: &Arc<Self>, ctx: &Context, ranges: &[Range], task_range: &[Range]) -> Result<()> {
        if let Some(err) = ctx.err() {
            tidb_log::info("tryAdvance aborted due to context cancellation", &[Field::new("error", Value::Str(err.to_string()))]);
            return Err(Arc::new(err));
        }
        let ranges = crate::spans::collapse(ranges);
        let workers = WorkerPool::new(DEFAULT_MAX_CONCURRENCY_ADVANCE * 4, "sub ranges");
        let (eg, cx) = ErrGroup::with_context(ctx);
        let mut collector = ClusterCollector::new(ctx, Arc::clone(&self.env) as Arc<dyn LogBackupService>);
        let this = Arc::clone(self);
        collector.set_on_success_hook(Arc::new(move |u, kr| {
            if let Some(cps) = lock(&this.checkpoints).as_mut() {
                cps.merge(Valued { key: kr, value: u });
            }
        }));
        let collector = Arc::new(collector);
        for r in crate::utils::key::intersect_all(ranges, task_range) {
            let (this, cx, collector) = (Arc::clone(self), cx.clone(), Arc::clone(&collector));
            workers.apply_on_error_group(&eg, move || {
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    this.get_checkpoint_in_range(&cx, &r.start_key, &r.end_key, &collector)
                }))
                .unwrap_or_else(|_| Err(errors::errorf("panicked while getting the checkpoint in range")))
            });
        }
        if let Err(err) = eg.wait() {
            tidb_log::warn("meet error during getting checkpoint", &[Field::new("error", Value::Str(err.to_string()))]);
            return Err(err);
        }
        collector.finish(ctx).map(|_| ())
    }

    /// Go `WithCheckpoints`.
    pub fn with_checkpoints<R>(&self, f: impl FnOnce(Option<&mut ValueSortedFull>) -> R) -> R {
        f(lock(&self.checkpoints).as_mut())
    }

    /// Go `fetchRegionHint`.
    fn fetch_region_hint(&self, ctx: &Context, start_key: &[u8]) -> String {
        let meta: &dyn TiKVClusterMeta = self.env.as_ref();
        let region = match locate_key_of_region(ctx, meta, start_key) {
            Ok(r) => r,
            Err(err) => return errors::annotate(err, "failed to fetch region").to_string(),
        };
        let r = &region.region;
        let prs: Vec<u64> = r.peers.iter().map(|p| p.store_id).collect();
        let epoch = r.region_epoch.unwrap_or_default();
        format!(
            "ID={},Leader={},ConfVer={},Version={},Peers={prs:?},RealRange={}",
            r.id,
            region.leader.store_id,
            epoch.conf_ver,
            epoch.version,
            crate::spans::stringify_range(&Range { start_key: r.start_key.clone(), end_key: r.end_key.clone() })
        )
    }

    /// Go `CalculateGlobalCheckpointLight`: polls the spans older than
    /// `threshold`; returns the minimum before polling.
    ///
    /// # Errors
    /// Polling failed.
    pub fn calculate_global_checkpoint_light(self: &Arc<Self>, ctx: &Context, threshold: Duration, task_range: &[Range]) -> Result<Valued> {
        let threshold_tso = tso_before(threshold);
        let (targets, min_value) = self.with_checkpoints(|vsf| {
            let Some(vsf) = vsf else { return Err(errors::errorf("the checkpoints of the task are not initialized")) };
            let mut targets = Vec::new();
            vsf.traverse_values_less_than(threshold_tso, |v| {
                targets.push(v.clone());
                true
            });
            Ok((targets, vsf.min()))
        })?;
        // A separate context: the hint is wanted even after the parent's
        // deadline.
        let (sctx, cancel) = Context::with_timeout(&Context::background(), Duration::from_secs(1));
        let hint = self.fetch_region_hint(&sctx, &min_value.key.start_key);
        let fields = [
            Field::new("category", Value::Str("log backup advancer hint".into())),
            Field::new("min", Value::Str(min_value.to_string())),
            Field::new("for-polling", Value::U64(targets.len() as u64)),
            Field::new("min-ts", Value::Str(rfc3339(tikv_client::oracle::get_time_from_ts(min_value.value)))),
            Field::new("region-hint", Value::Str(hint)),
        ];
        if min_value.value < threshold_tso {
            tidb_log::info("current last region", &fields);
        } else {
            tidb_log::debug("current last region", &fields);
        }
        cancel.cancel();
        if targets.is_empty() {
            return Ok(min_value);
        }
        let keys: Vec<Range> = targets.into_iter().map(|t| t.key).collect();
        self.try_advance(ctx, &keys, task_range)?;
        Ok(min_value)
    }

    /// Go `consumeAllTask`: the events already queued.
    fn consume_all_task(&self, ctx: &Context, ch: &crossbeam_channel::Receiver<TaskEvent>) -> Result<()> {
        loop {
            let Ok(e) = ch.try_recv() else { return Ok(()) };
            tidb_log::info("meet task event", &[Field::new("event", Value::Str(e.to_string()))]);
            if let Err(err) = self.on_task_event(ctx, &e) {
                if !is_canceled(e.err.as_ref()) {
                    tidb_log::warn("listen task meet error, would reopen.", &[Field::new("error", Value::Str(err.to_string()))]);
                    return Err(err);
                }
                return Ok(());
            }
        }
    }

    /// Go `beginListenTaskChange`: the current tasks applied, and the
    /// channel of their changes.
    fn begin_listen_task_change(&self, ctx: &Context) -> Result<crossbeam_channel::Receiver<TaskEvent>> {
        let (tx, rx) = crossbeam_channel::bounded(1024);
        self.env.begin(ctx, tx)?;
        self.consume_all_task(ctx, &rx)?;
        Ok(rx)
    }

    /// Go `StartTaskListener`: call before the tick loop; without a task
    /// the advancer does nothing.
    pub fn start_task_listener(self: &Arc<Self>, ctx: &Context) {
        let (cx, cancel) = Context::with_cancel(ctx);
        let ch = loop {
            if cx.err().is_some() {
                cancel.cancel();
                return;
            }
            match self.begin_listen_task_change(&cx) {
                Ok(ch) => break ch,
                Err(err) => {
                    tidb_log::warn("failed to begin listening, retrying...", &[Field::new("error", Value::Str(err.to_string()))]);
                    std::thread::sleep(self.config().get_backoff_time());
                }
            }
        };
        let this = Arc::clone(self);
        let ctx = ctx.clone();
        std::thread::spawn(move || {
            let category = || Field::new("category", Value::Str("log backup advancer".into()));
            loop {
                crossbeam_channel::select! {
                    recv(ctx.done()) -> _ => break,
                    recv(ch) -> e => {
                        let Ok(e) = e else {
                            tidb_log::info("Task watcher exits due to stream ends.", &[category()]);
                            break;
                        };
                        tidb_log::info("Meet task event", &[category(), Field::new("event", Value::Str(e.to_string()))]);
                        if let Err(err) = this.on_task_event(&ctx, &e) {
                            if !is_canceled(e.err.as_ref()) {
                                tidb_log::warn("listen task meet error, would reopen.", &[Field::new("error", Value::Str(err.to_string()))]);
                                let (this, ctx) = (Arc::clone(&this), ctx.clone());
                                let backoff = this.config().get_backoff_time();
                                std::thread::spawn(move || {
                                    if !ctx.sleep(backoff) {
                                        this.start_task_listener(&ctx);
                                    }
                                });
                            }
                            tidb_log::info("Task watcher exits due to some error.", &[category(), Field::new("error", Value::Str(err.to_string()))]);
                            break;
                        }
                    }
                }
            }
            cancel.cancel();
        });
    }

    fn set_checkpoints(&self, cps: Option<ValueSortedFull>) {
        *lock(&self.checkpoints) = cps;
    }

    /// Go `onTaskEvent`.
    fn on_task_event(&self, ctx: &Context, e: &TaskEvent) -> Result<()> {
        let mut st = lock(&self.task);
        match e.type_ {
            EventType::Add => {
                crate::utils::log_backup_task_count_inc();
                Self::close_global_checkpoint_storage(&mut st);
                let info = e.info.clone().unwrap_or_default();
                st.task_range = crate::spans::collapse(&e.ranges);
                self.set_checkpoints(Some(sorted(ValuedFull::new_full_with(&e.ranges, 0))));
                let mut global_checkpoint_ts = self.env.get_global_checkpoint_for_task(ctx, &e.name).unwrap_or_else(|err| {
                    // The error is only logged.
                    tidb_log::warn("failed to get global checkpoint, skipping.", &[Field::new("error", Value::Str(err.to_string()))]);
                    0
                });
                global_checkpoint_ts = global_checkpoint_ts.max(info.start_ts);
                tidb_log::info("get global checkpoint", &[Field::new("checkpoint", Value::U64(global_checkpoint_ts))]);
                let cp = Checkpoint::with_ts(global_checkpoint_ts);
                let safe_ts = cp.safe_ts();
                *lock(&self.last_checkpoint) = Some(cp);
                let p = self.env.block_gc_until(ctx, safe_ts).unwrap_or_else(|err| {
                    tidb_log::warn("failed to upload service GC safepoint, skipping.", &[Field::new("error", Value::Str(err.to_string()))]);
                    0
                });
                tidb_log::info(
                    "added event",
                    &[
                        Field::new("task", Value::Str(info.name.clone())),
                        Field::new("ranges", Value::Str(st.task_range.iter().map(crate::spans::stringify_range).collect::<Vec<_>>().join(", "))),
                        Field::new("current-checkpoint", Value::U64(p)),
                    ],
                );
                st.task = Some(info);
            }
            EventType::Del => {
                crate::utils::log_backup_task_count_dec();
                Self::close_global_checkpoint_storage(&mut st);
                st.task = None;
                self.is_paused.store(false, Ordering::SeqCst);
                st.task_range = Vec::new();
                // Synced by the task lock; tests may run without a subscriber.
                if let Some(sub) = lock(&self.subscriber).as_mut() {
                    sub.clear();
                }
                self.set_checkpoints(None);
                if let Err(err) = self.env.clear_v3_global_checkpoint_for_task(ctx, &e.name) {
                    tidb_log::warn("failed to clear global checkpoint", &[Field::new("error", Value::Str(err.to_string()))]);
                }
                if let Err(err) = self.env.unblock_gc(ctx) {
                    tidb_log::warn("failed to remove service GC safepoint", &[Field::new("error", Value::Str(err.to_string()))]);
                }
            }
            EventType::Pause => {
                if st.task.as_ref().is_some_and(|t| t.name == e.name) {
                    self.is_paused.store(true, Ordering::SeqCst);
                }
            }
            EventType::Resume => {
                if st.task.as_ref().is_some_and(|t| t.name == e.name) {
                    self.is_paused.store(false, Ordering::SeqCst);
                }
            }
            EventType::Err => return Err(e.err.clone().unwrap_or_else(|| errors::errorf("unknown task event error"))),
        }
        Ok(())
    }

    /// Go `setCheckpoint`: whether the checkpoint moved (a new range at the
    /// same TS counts, for resolving locks).
    fn set_checkpoint(&self, s: &Valued) -> bool {
        let cp = Checkpoint::with_span(s);
        let mut last = lock(&self.last_checkpoint);
        if let Some(l) = last.as_ref() {
            if cp.ts < l.ts {
                tidb_log::warn(
                    "failed to update global checkpoint: stale",
                    &[Field::new("old", Value::U64(l.ts)), Field::new("new", Value::U64(cp.ts))],
                );
                return false;
            }
            if cp.equal(l) {
                return false;
            }
        }
        *last = Some(cp);
        true
    }

    /// Go `advanceCheckpointBy`.
    fn advance_checkpoint_by(&self, ctx: &Context, task_name: &str, get_checkpoint: impl FnOnce(&Context) -> Result<Valued>) -> Result<()> {
        let start = Instant::now();
        let cp = get_checkpoint(ctx)?;
        if self.set_checkpoint(&cp) {
            tidb_log::info(
                "uploading checkpoint for task",
                &[
                    Field::new("checkpoint", Value::Str(rfc3339(tikv_client::oracle::get_time_from_ts(cp.value)))),
                    Field::new("checkpoint", Value::U64(cp.value)),
                    Field::new("task", Value::Str(task_name.to_owned())),
                    Field::new("take", Value::Str(format!("{:?}", start.elapsed()))),
                ],
            );
        }
        Ok(())
    }

    fn stop_subscriber(&self) {
        if let Some(sub) = lock(&self.subscriber).take() {
            sub.drop_subscriber();
        }
    }

    /// Go `SpawnSubscriptionHandler`: merges the flush events into the span
    /// tree until `ctx` is done.
    pub fn spawn_subscription_handler(self: &Arc<Self>, ctx: &Context) {
        let mut sub_slot = lock(&self.subscriber);
        let sub = FlushSubscriber::new(
            Arc::clone(&self.env) as Arc<dyn LogBackupService>,
            Arc::clone(&self.env) as Arc<dyn TiKVClusterMeta>,
            vec![with_master_context(ctx.clone())],
        );
        let es = sub.events();
        *sub_slot = Some(sub);
        drop(sub_slot);
        tidb_log::info("Subscription handler spawned.", &[Field::new("category", Value::Str("log backup subscription manager".into()))]);
        let this = Arc::clone(self);
        let ctx = ctx.clone();
        std::thread::spawn(move || loop {
            crossbeam_channel::select! {
                recv(ctx.done()) -> _ => return,
                recv(es) -> event => {
                    let Ok(event) = event else { return };
                    tidb_failpoint::inject("github.com/pingcap/tidb/br/pkg/streamhelper/subscription-handler-loop", |_| {});
                    this.with_checkpoints(|vsf| {
                        let Some(vsf) = vsf else {
                            tidb_log::warn(
                                "Span tree not found, perhaps stale event of removed tasks.",
                                &[Field::new("category", Value::Str("log backup subscription manager".into()))],
                            );
                            return;
                        };
                        vsf.merge(event);
                    });
                }
            }
        });
    }

    /// Go `subscribeTick`.
    fn subscribe_tick(&self, ctx: &Context) -> Result<()> {
        let mut sub = lock(&self.subscriber);
        let Some(sub) = sub.as_mut() else { return Ok(()) };
        tidb_failpoint::inject_call("github.com/pingcap/tidb/br/pkg/streamhelper/get_subscriber", &mut ());
        if let Err(err) = sub.update_store_topology(ctx) {
            tidb_log::warn(
                "Error when updating store topology.",
                &[
                    Field::new("category", Value::Str("log backup advancer".into())),
                    Field::new("error", Value::Str(err.to_string())),
                ],
            );
        }
        sub.handle_errors();
        sub.pending_errors()
    }

    /// Go `isCheckpointLagged`.
    fn is_checkpoint_lagged(&self, ctx: &Context, task: &StreamBackupTaskInfo) -> Result<bool> {
        let check_point_lag_limit = self.config().get_check_point_lag_limit();
        if check_point_lag_limit.is_zero() {
            return Ok(false);
        }
        let global_ts = self.env.get_global_checkpoint_for_task(ctx, &task.name)?;
        if global_ts < task.start_ts {
            // Unreachable.
            return Ok(false);
        }
        let now = self.env.fetch_current_ts(ctx)?;
        let lag_duration = tikv_client::oracle::get_time_from_ts(now)
            .duration_since(tikv_client::oracle::get_time_from_ts(global_ts))
            .unwrap_or_default();
        if lag_duration > check_point_lag_limit {
            tidb_log::warn(
                "checkpoint lag is too large",
                &[
                    Field::new("category", Value::Str("log backup advancer".into())),
                    Field::new("lag", Value::Str(format!("{lag_duration:?}"))),
                ],
            );
            return Ok(true);
        }
        Ok(false)
    }

    fn close_global_checkpoint_storage(st: &mut TaskState) {
        if let Some(storage) = st.checkpoint_storage.take() {
            storage.close();
        }
        st.last_external_storage_checkpoint = 0;
    }

    fn get_global_checkpoint_storage(&self, ctx: &Context, st: &mut TaskState) -> Result<Option<Arc<dyn Storage>>> {
        let Some(backend) = st.task.as_ref().and_then(|t| t.storage.clone()) else { return Ok(None) };
        if let Some(storage) = &st.checkpoint_storage {
            return Ok(Some(Arc::clone(storage)));
        }
        let create = Arc::clone(&self.create_global_checkpoint_storage.read().unwrap_or_else(PoisonError::into_inner));
        let storage = create(ctx, &backend)
            .map_err(|e| errors::annotate(e, "failed to create external storage for global checkpoint"))?;
        st.checkpoint_storage = Some(Arc::clone(&storage));
        Ok(Some(storage))
    }

    /// Go `writeGlobalCheckpointToStorage`: little-endian, as `br log`
    /// reads it.
    fn write_global_checkpoint_to_storage(&self, ctx: &Context, st: &mut TaskState, checkpoint: u64) -> Result<()> {
        let Some(storage) = self.get_global_checkpoint_storage(ctx, st)? else { return Ok(()) };
        let file_name = format!("{STREAM_BACKUP_GLOBAL_CHECKPOINT_PREFIX}/{}", global_checkpoint_file_name());
        storage
            .write_file(ctx, &file_name, &checkpoint.to_le_bytes())
            .map_err(|e| errors::annotate(errors::errorf(e.to_string()), "failed to write global checkpoint to external storage"))?;
        st.last_external_storage_checkpoint = checkpoint;
        tidb_log::info(
            "uploaded global checkpoint to external storage",
            &[
                Field::new("category", Value::Str("log backup advancer".into())),
                Field::new("checkpoint", Value::U64(checkpoint)),
                Field::new("file", Value::Str(file_name)),
            ],
        );
        Ok(())
    }

    /// Go `tryWriteGlobalCheckpointToStorage`: failures are logged.
    fn try_write_global_checkpoint_to_storage(&self, ctx: &Context, st: &mut TaskState) {
        let Some(task) = st.task.as_ref().filter(|t| t.storage.is_some()) else { return };
        let task_name = task.name.clone();
        let (write_ctx, cancel) = Context::with_timeout(ctx, self.config().tick_timeout());
        let category = || Field::new("category", Value::Str("log backup advancer".into()));
        match self.env.get_global_checkpoint_for_task(&write_ctx, &task_name) {
            Err(err) => tidb_log::warn(
                "failed to get uploaded global checkpoint, skip uploading to external storage",
                &[category(), Field::new("error", Value::Str(err.to_string()))],
            ),
            Ok(global_checkpoint) if global_checkpoint <= st.last_external_storage_checkpoint => {}
            Ok(global_checkpoint) => {
                if let Err(err) = self.write_global_checkpoint_to_storage(&write_ctx, st, global_checkpoint) {
                    tidb_log::warn(
                        "failed to upload global checkpoint to external storage, skip it",
                        &[category(), Field::new("error", Value::Str(err.to_string()))],
                    );
                }
            }
        }
        cancel.cancel();
    }

    /// Go `importantTick`: uploads the global checkpoint, pauses a task
    /// lagging past the limit, and moves the GC safe point.
    fn important_tick(&self, ctx: &Context, st: &mut TaskState) -> Result<()> {
        let task = st.task.clone().unwrap_or_default();
        let min = lock(&self.checkpoints).as_ref().map(ValueSortedFull::min);
        if let Some(min) = min {
            self.set_checkpoint(&min);
        }
        let last = self.last_checkpoint().unwrap_or_else(|| Checkpoint::with_ts(0));
        self.env
            .upload_v3_global_checkpoint_for_task(ctx, &task.name, last.ts)
            .map_err(|e| errors::annotate(e, "failed to upload global checkpoint"))?;
        let res = (|| {
            let is_lagged = self.is_checkpoint_lagged(ctx, &task).unwrap_or_else(|err| {
                // The error is only logged.
                tidb_log::warn("failed to check timestamp", &[Field::new("error", Value::Str(err.to_string()))]);
                false
            });
            if is_lagged {
                let cp = tikv_client::oracle::get_time_from_ts(last.ts);
                let now = SystemTime::now();
                let msg = format!(
                    "The checkpoint is at {}, now it is {}, the lag is too huge ({:?}) hence pause the task to avoid impaction to the cluster",
                    rfc3339(cp),
                    rfc3339(now),
                    now.duration_since(cp).unwrap_or_default()
                );
                self.env
                    .pause_task(ctx, &task.name, vec![pause_with_message(msg), pause_with_error_severity()])
                    .map_err(|e| errors::annotate(e, "failed to pause task"))?;
                return Err(errors::annotate(errors::errorf("check point lagged too large"), "check point lagged too large"));
            }
            let p = self.env.block_gc_until(ctx, last.safe_ts()).map_err(|e| {
                errors::annotate(
                    e,
                    format!(
                        "failed to update service GC safe point, current checkpoint is {}, target checkpoint is 0",
                        last.safe_ts()
                    ),
                )
            })?;
            if p <= last.safe_ts() {
                tidb_log::info(
                    "updated log backup GC safe point.",
                    &[Field::new("checkpoint", Value::U64(p)), Field::new("target", Value::U64(last.safe_ts()))],
                );
            } else {
                tidb_log::warn(
                    "update log backup GC safe point failed: stale.",
                    &[Field::new("checkpoint", Value::U64(p)), Field::new("target", Value::U64(last.safe_ts()))],
                );
            }
            Ok(())
        })();
        self.try_write_global_checkpoint_to_storage(ctx, st);
        res
    }

    /// Go `optionalTick`: resolves stuck locks, refreshes the
    /// subscriptions and polls the lagging spans.
    fn optional_tick(self: &Arc<Self>, cx: &Context, st: &TaskState) -> Result<()> {
        self.try_resolve_locks_for_checkpoint(cx);
        let mut threshold = self.get_default_start_poll_threshold();
        if let Err(err) = self.subscribe_tick(cx) {
            tidb_log::warn(
                "Subscriber meet error, would polling the checkpoint.",
                &[
                    Field::new("category", Value::Str("log backup advancer".into())),
                    Field::new("error", Value::Str(err.to_string())),
                ],
            );
            threshold = self.get_subscriber_error_start_poll_threshold();
        }
        let task_name = st.task.as_ref().map(|t| t.name.clone()).unwrap_or_default();
        self.advance_checkpoint_by(cx, &task_name, |cx| {
            self.calculate_global_checkpoint_light(cx, threshold, &st.task_range)
        })
    }

    /// Go `tryResolveLocksForCheckpoint`: a checkpoint unchanged for the
    /// resolve-lock interval suggests expired locks.
    fn try_resolve_locks_for_checkpoint(self: &Arc<Self>, ctx: &Context) {
        let resolve_lock_interval = self.get_resolve_lock_interval();
        let Some(checkpoint_to_resolve) = self.checkpoint_to_resolve(resolve_lock_interval) else { return };
        if self.in_resolving_lock.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_err() {
            return;
        }
        let current_ts = match self.env.fetch_current_ts(ctx) {
            Ok(ts) => ts,
            Err(err) => {
                tidb_log::warn(
                    "failed to fetch current timestamp for resolving locks",
                    &[
                        Field::new("resolve-lock-interval", Value::Str(format!("{resolve_lock_interval:?}"))),
                        Field::new("error", Value::Str(err.to_string())),
                    ],
                );
                self.in_resolving_lock.store(false, Ordering::SeqCst);
                return;
            }
        };
        let max_version = resolve_lock_target_upper_bound(checkpoint_to_resolve.ts, resolve_lock_interval, current_ts);
        if max_version <= checkpoint_to_resolve.ts {
            tidb_log::info(
                "skip resolving locks because maxVersion is not greater than checkpoint",
                &[
                    Field::new("checkpoint", Value::U64(checkpoint_to_resolve.ts)),
                    Field::new("current-ts", Value::U64(current_ts)),
                    Field::new("resolve-lock-interval", Value::Str(format!("{resolve_lock_interval:?}"))),
                    Field::new("max-version", Value::U64(max_version)),
                ],
            );
            self.in_resolving_lock.store(false, Ordering::SeqCst);
            return;
        }
        let (retry_lower_bound, retry_lower_bound_valid) = resolve_lock_retry_lower_bound(checkpoint_to_resolve.ts, max_version);
        let targets = self.resolve_lock_targets_for_checkpoint(&checkpoint_to_resolve, max_version);
        if targets.is_empty() {
            // Don't forget to set the state back.
            self.in_resolving_lock.store(false, Ordering::SeqCst);
            return;
        }
        tidb_log::info(
            "Advancer starts to resolve locks",
            &[
                Field::new("category", Value::Str("advancer".into())),
                Field::new("StartKey", Value::Str(tidb_redact::key(&checkpoint_to_resolve.start_key))),
                Field::new("EndKey", Value::Str(tidb_redact::key(&checkpoint_to_resolve.end_key))),
                Field::new("checkpoint", Value::U64(checkpoint_to_resolve.ts)),
                Field::new("current-ts", Value::U64(current_ts)),
                Field::new("max-version", Value::U64(max_version)),
                Field::new("retry-lower-bound", Value::U64(retry_lower_bound)),
                Field::new("retry-lower-bound-valid", Value::Bool(retry_lower_bound_valid)),
                Field::new("targets", Value::U64(targets.len() as u64)),
            ],
        );
        // A new context, so the tick's deadline doesn't cut it short.
        self.async_resolve_locks_for_ranges(
            targets,
            checkpoint_to_resolve,
            ResolveLockPlan { max_version, retry_lower_bound, retry_lower_bound_valid },
        );
    }

    fn checkpoint_to_resolve(&self, resolve_lock_interval: Duration) -> Option<Checkpoint> {
        lock(&self.last_checkpoint).as_ref().filter(|c| c.need_resolve_locks(resolve_lock_interval)).cloned()
    }

    /// Go `resolveLockTargetsForCheckpoint`: the spans below `upper_bound`
    /// while the checkpoint is still the tree's minimum.
    fn resolve_lock_targets_for_checkpoint(&self, checkpoint_to_resolve: &Checkpoint, upper_bound: u64) -> Vec<Valued> {
        self.with_checkpoints(|vsf| {
            let mut targets = Vec::new();
            let Some(vsf) = vsf else { return targets };
            if vsf.min_value() != checkpoint_to_resolve.ts {
                return targets;
            }
            vsf.traverse_values_less_than(upper_bound, |v| {
                targets.push(v.clone());
                true
            });
            targets
        })
    }

    /// Go `asyncResolveLocksForRanges`: off the tick's thread.
    fn async_resolve_locks_for_ranges(self: &Arc<Self>, targets: Vec<Valued>, checkpoint_to_resolve: Checkpoint, plan: ResolveLockPlan) {
        let this = Arc::clone(self);
        std::thread::spawn(move || {
            tidb_failpoint::inject("github.com/pingcap/tidb/br/pkg/streamhelper/AsyncResolveLocks", |_| {});
            let ctx = Context::background();
            let worker_pool = WorkerPool::new(DEFAULT_MAX_CONCURRENCY_ADVANCE, "advancer resolve locks");
            let meet_error = Arc::new(AtomicBool::new(false));
            let (eg, _) = ErrGroup::with_context(&ctx);
            for target in targets {
                let (this, ctx, meet_error) = (Arc::clone(&this), ctx.clone(), Arc::clone(&meet_error));
                worker_pool.apply_on_error_group(&eg, move || {
                    // One runner per target over the whole cluster; it
                    // mostly holds one region, so concurrency 1.
                    if let Err(err) = this.env.resolve_locks_on_range(&ctx, &target.key.start_key, &target.key.end_key, plan) {
                        // Wait for the next tick.
                        meet_error.store(true, Ordering::SeqCst);
                        tidb_log::warn("resolve locks failed, wait for next tick", &[Field::new("error", Value::Str(err.to_string()))]);
                    }
                    Ok(())
                });
            }
            let _ = eg.wait();
            tidb_log::info("finish resolve locks for checkpoint", &[]);
            this.update_resolve_lock_time_after_resolving(&checkpoint_to_resolve, meet_error.load(Ordering::SeqCst));
            this.in_resolving_lock.store(false, Ordering::SeqCst);
        });
    }

    fn update_resolve_lock_time_after_resolving(&self, checkpoint_to_resolve: &Checkpoint, meet_error: bool) {
        if meet_error {
            return;
        }
        if let Some(last) = lock(&self.last_checkpoint).as_mut().filter(|l| l.equal(checkpoint_to_resolve)) {
            last.resolve_lock_time = Instant::now();
        }
    }

    /// Go `tick`.
    fn tick(self: &Arc<Self>, ctx: &Context) -> Result<()> {
        let mut st = lock(&self.task);
        if st.task.is_none() || self.is_paused.load(Ordering::SeqCst) {
            tidb_log::debug("No tasks yet, skipping advancing.", &[]);
            return Ok(());
        }
        let mut errs = Vec::new();
        let (cx, cancel) = Context::with_timeout(ctx, self.config().tick_timeout());
        if let Err(err) = self.optional_tick(&cx, &st) {
            tidb_log::warn(
                "option tick failed.",
                &[Field::new("category", Value::Str("log backup advancer".into())), Field::new("error", Value::Str(err.to_string()))],
            );
            errs.push(err.to_string());
        }
        if let Err(err) = self.important_tick(ctx, &mut st) {
            tidb_log::warn(
                "important tick failed.",
                &[Field::new("category", Value::Str("log backup advancer".into())), Field::new("error", Value::Str(err.to_string()))],
            );
            errs.push(err.to_string());
        }
        cancel.cancel();
        if errs.is_empty() {
            return Ok(());
        }
        Err(errors::errorf(errs.join("; ")))
    }

    /// Go `OnTick`: synchronous; panics become errors.
    ///
    /// # Errors
    /// The tick's errors.
    pub fn on_tick(self: &Arc<Self>, ctx: &Context) -> Result<()> {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.tick(ctx)))
            .unwrap_or_else(|_| Err(errors::errorf("panicked during the advancer tick")))
    }

    /// Go `OnStop`.
    pub fn on_stop(&self) {
        {
            let mut st = lock(&self.task);
            Self::close_global_checkpoint_storage(&mut st);
        }
        self.stop_subscriber();
    }

    /// The subscriber, for tests.
    pub fn with_subscriber<R>(&self, f: impl FnOnce(Option<&mut FlushSubscriber>) -> R) -> R {
        f(lock(&self.subscriber).as_mut())
    }
}

/// Go `errors.Cause(e.Err) == context.Canceled`.
fn is_canceled(err: Option<&errors::Error>) -> bool {
    err.is_some_and(|e| {
        matches!(errors::cause(e.as_ref()).downcast_ref::<tidb_gocontext::CtxError>(), Some(tidb_gocontext::CtxError::Canceled))
    })
}

impl super::daemon::Interface for Arc<CheckpointAdvancer> {
    /// Go `OnStart`.
    fn on_start(&self, ctx: &Context) {
        self.start_task_listener(ctx);
    }

    /// Go `OnBecomeOwner`.
    fn on_become_owner(&self, ctx: &Context) {
        self.spawn_subscription_handler(ctx);
        self.spawn_log_backup_config_updater(ctx);
        let (this, ctx) = (Arc::clone(self), ctx.clone());
        std::thread::spawn(move || {
            let _ = ctx.done().recv();
            this.on_stop();
        });
    }

    fn on_tick(&self, ctx: &Context) -> Result<()> {
        CheckpointAdvancer::on_tick(self, ctx)
    }

    fn name(&self) -> String {
        "LogBackup::Advancer".to_owned()
    }
}

/// Go `OwnerManagerForLogBackup`'s election key and prompt with a fresh id.
#[must_use]
pub fn owner_id_for_log_backup() -> String {
    uuid::Uuid::new_v4().to_string()
}

