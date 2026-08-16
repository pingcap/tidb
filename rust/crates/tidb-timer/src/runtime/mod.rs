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

//! Go `pkg/timer/runtime` lands as a complete package: the background runtime
//! that watches a group of timers, decides which are up, and dispatches their
//! events to per-hook-class workers.
//!
//! File mapping (one Rust module per Go file):
//! - this module <- `runtime.go`
//! - [`cache`] <- `cache.go`
//! - [`worker`] <- `worker.go`
//!
//! The package is dependency-closed in Go apart from metrics, logging and
//! `intest`; the narrowings below name what each became here.
//!
//! Concurrency. Go's runtime is one goroutine per loop plus buffered channels
//! and a `select` over six cases. This workspace has no async runtime and
//! `tidb-timer` depends on no channel crate, so:
//! - goroutines become `std::thread`s tracked by [`WaitGroup`], the stand-in
//!   for `pkg/util.WaitGroupWrapper`;
//! - channels are `std::sync::mpsc::sync_channel`, which gives Go's buffered
//!   channel plus the non-blocking `try_send`/`try_recv` the `select ...
//!   default` arms need;
//! - `select` over a channel and `ctx.Done()` has no std equivalent, so both
//!   loops poll at [`CHANNEL_POLL_INTERVAL`] and check
//!   [`crate::store::Context::is_done`] each turn. The observable behavior —
//!   a request is served as soon as it arrives, cancellation is honoured
//!   promptly — is unchanged; the latency floor is the poll interval;
//! - Go's `time.Timer`/`time.Ticker` cases become deadlines compared against
//!   `Instant::now()` inside the same poll loop, so [`TimerGroupRuntime::timer_loop`]
//!   is one step function that can also be driven directly.
//! - `util.WithRecovery`'s recover-and-continue becomes
//!   [`with_recover_until`], built on `std::panic::catch_unwind`; a panic that
//!   Go recovers is a caught unwind here, and a panic Go propagates is
//!   `resume_unwind`.
//!
//! Other narrowings, each named at its definition site:
//! - `metrics.TimerScopeCounter` / `TimerHookWorkerCounter` become
//!   [`worker::MetricsCounter`], a plain atomic.
//! - `logutil.BgLogger().With(...)` becomes explicit field slices on
//!   `tidb_util::logutil::bg_logger()`.
//! - `intest.InTest`'s `init()` override of `minTriggerEventInterval` and
//!   `batchProcessWatchRespInterval` becomes [`set_in_test_intervals`], which
//!   the ported tests call explicitly.
//! - `uuid.New()` + `hex.EncodeToString` is [`crate::uuid::new_uuid_hex`].
//! - Go's package-level tunables are `AtomicI64`s with getters and setters,
//!   because the upstream tests reassign them.
//! - Go's `idleWatchChan`, a channel that is never ready, is `None`.

pub mod cache;
pub mod worker;

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::mpsc::Receiver;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use tidb_log::{Field, Value};
use tidb_util::logutil::bg_logger;

use crate::client::{new_default_timer_client, TimerClient};
use crate::go_time::{GoTime, MINUTE, SECOND};
use crate::hook::HookFactory;
use crate::store::{
    and, or, Cond, Context, OptionalVal, TimerCond, TimerStore, WatchTimerChan,
    WatchTimerEventType, WatchTimerResponse,
};
use crate::timer::{SchedEventStatus, TimerRecord};
use crate::uuid::new_uuid_hex;

use cache::{RuntimeProcStatus, TimersCache};
use worker::{
    new_hook_worker, HookFn, HookWorker, MetricsCounter, TriggerEventRequest, TriggerEventResponse,
    WORKER_RESP_CHAN_CAP,
};

/// How often the two `select`-replacing loops wake to re-check their channels
/// and their context. See the module header's concurrency note.
pub const CHANNEL_POLL_INTERVAL: Duration = Duration::from_millis(1);

/// Go `func() time.Time`, the injectable clock.
pub type NowFunc = Arc<dyn Fn() -> GoTime + Send + Sync>;

/// Go's default `nowFunc`, `time.Now`.
pub fn default_now_func() -> NowFunc {
    Arc::new(GoTime::now)
}

macro_rules! tunable {
    ($value:ident, $get:ident, $set:ident, $default:expr, $doc:literal) => {
        static $value: AtomicI64 = AtomicI64::new($default);

        #[doc = $doc]
        pub fn $get() -> i64 {
            $value.load(Ordering::SeqCst)
        }

        #[doc = "Overrides the value above; Go's tests assign the package variable directly."]
        pub fn $set(nanoseconds: i64) {
            $value.store(nanoseconds, Ordering::SeqCst);
        }
    };
}

tunable!(
    FULL_REFRESH_TIMERS_INTERVAL,
    full_refresh_timers_interval,
    set_full_refresh_timers_interval,
    MINUTE,
    "Go `fullRefreshTimersInterval`."
);
tunable!(
    MAX_TRIGGER_EVENT_INTERVAL,
    max_trigger_event_interval,
    set_max_trigger_event_interval,
    60 * SECOND,
    "Go `maxTriggerEventInterval`."
);
tunable!(
    MIN_TRIGGER_EVENT_INTERVAL,
    min_trigger_event_interval,
    set_min_trigger_event_interval,
    SECOND,
    "Go `minTriggerEventInterval`."
);
tunable!(
    RE_WATCH_INTERVAL,
    re_watch_interval,
    set_re_watch_interval,
    5 * SECOND,
    "Go `reWatchInterval`."
);
tunable!(
    BATCH_PROCESS_WATCH_RESP_INTERVAL,
    batch_process_watch_resp_interval,
    set_batch_process_watch_resp_interval,
    SECOND,
    "Go `batchProcessWatchRespInterval`."
);
tunable!(
    RETRY_BUSY_WORKER_INTERVAL,
    retry_busy_worker_interval,
    set_retry_busy_worker_interval,
    5 * SECOND,
    "Go `retryBusyWorkerInterval`."
);
tunable!(
    CHECK_WAIT_CLOSE_TIMER_INTERVAL,
    check_wait_close_timer_interval,
    set_check_wait_close_timer_interval,
    10 * SECOND,
    "Go `checkWaitCloseTimerInterval`."
);

/// Go's `init()` under `intest.InTest`, which shrinks the two intervals that
/// exist only to stop the trigger loop from burning CPU. This workspace has no
/// `intest` build tag, so the ported tests call this explicitly.
pub fn set_in_test_intervals() {
    set_min_trigger_event_interval(1_000_000);
    set_batch_process_watch_resp_interval(1_000_000);
}

/// `boundary:` Go `pkg/util.WaitGroupWrapper`, restricted to `Run` and `Wait`.
#[derive(Default)]
pub struct WaitGroup {
    handles: Mutex<Vec<std::thread::JoinHandle<()>>>,
}

impl WaitGroup {
    /// A fresh group.
    pub fn new() -> Self {
        Self::default()
    }

    /// Go `wg.Run(fn)`.
    pub fn run(&self, body: impl FnOnce() + Send + 'static) {
        let handle = std::thread::spawn(body);
        self.handles
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .push(handle);
    }

    /// Go `wg.Wait()`.
    pub fn wait(&self) {
        let handles = {
            let mut guard = self.handles.lock().unwrap_or_else(|err| err.into_inner());
            std::mem::take(&mut *guard)
        };
        for handle in handles {
            let _ = handle.join();
        }
    }
}

/// Go `sleep(ctx, d)`.
pub fn sleep(ctx: &Context, nanoseconds: i64) {
    ctx.wait_done(Duration::from_nanos(nanoseconds.max(0) as u64));
}

/// Go `withRecoverUntil`, whose `util.WithRecovery` recovers a panic and reruns
/// `fn` with an incremented panic count until it completes without panicking
/// or the context is cancelled.
pub fn with_recover_until(ctx: &Context, mut body: impl FnMut(u64)) {
    let mut round = 0u64;
    let mut success = false;
    while !ctx.is_done() && !success {
        // The default panic hook stays installed: it is process-global, other
        // threads are running, and Go's `util.WithRecovery` logs the recovered
        // panic too.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| body(round)));
        if result.is_ok() {
            success = true;
        } else {
            bg_logger().warn("recovered from panic, retrying", &[]);
        }
        round += 1;
    }
}

/// Go `TimerRuntimeBuilder`.
pub struct TimerRuntimeBuilder {
    runtime: TimerGroupRuntime,
}

impl TimerRuntimeBuilder {
    /// Go `NewTimerRuntimeBuilder`.
    pub fn new(group_id: &str, store: TimerStore) -> Self {
        Self {
            runtime: TimerGroupRuntime::new(group_id, store),
        }
    }

    /// Go `(*TimerRuntimeBuilder).SetCond`.
    pub fn set_cond(self, cond: Arc<dyn Cond>) -> Self {
        *self.runtime.shared.cond.lock().unwrap() = cond;
        self
    }

    /// Go `(*TimerRuntimeBuilder).RegisterHookFactory`.
    pub fn register_hook_factory(self, hook_class: &str, factory: HookFactory) -> Self {
        self.runtime
            .shared
            .factories
            .lock()
            .unwrap()
            .insert(hook_class.to_string(), factory);
        self
    }

    /// Go `(*TimerRuntimeBuilder).Build`.
    pub fn build(self) -> TimerGroupRuntime {
        self.runtime
    }
}

struct CtxState {
    ctx: Option<Context>,
    cancel: Option<crate::store::CancelFn>,
}

struct RuntimeShared {
    group_id: String,
    store: TimerStore,
    cli: Arc<dyn TimerClient>,
    cond: Mutex<Arc<dyn Cond>>,
    factories: Mutex<HashMap<String, HookFactory>>,
    cache: Mutex<TimersCache>,
    workers: Mutex<HashMap<String, Arc<HookWorker>>>,
    worker_resp_tx: std::sync::mpsc::SyncSender<TriggerEventResponse>,
    worker_resp_rx: Mutex<Receiver<TriggerEventResponse>>,
    now_func: Mutex<NowFunc>,
    full_refresh_timer_counter: MetricsCounter,
    partial_refresh_timer_counter: MetricsCounter,
    retry_loop_wait: AtomicI64,
    ctx_state: Mutex<CtxState>,
    wait_group: Arc<WaitGroup>,
}

/// Go `TimerGroupRuntime`.
///
/// Go guards only `ctx`/`cancel` with a mutex because every other field is
/// touched exclusively by the loop goroutine. The Rust port is shared with the
/// loop thread through an `Arc`, so the loop-owned fields sit behind their own
/// mutexes; [`TimerGroupRuntime::cache`] and [`TimerGroupRuntime::workers`]
/// hand the guards back for the direct-drive access the upstream tests use.
#[derive(Clone)]
pub struct TimerGroupRuntime {
    shared: Arc<RuntimeShared>,
}

impl TimerGroupRuntime {
    fn new(group_id: &str, store: TimerStore) -> Self {
        let (worker_resp_tx, worker_resp_rx) = std::sync::mpsc::sync_channel(WORKER_RESP_CHAN_CAP);
        Self {
            shared: Arc::new(RuntimeShared {
                group_id: group_id.to_string(),
                store: store.clone(),
                cli: Arc::new(new_default_timer_client(store)),
                cond: Mutex::new(Arc::new(TimerCond::default())),
                factories: Mutex::new(HashMap::new()),
                cache: Mutex::new(TimersCache::new()),
                workers: Mutex::new(HashMap::new()),
                worker_resp_tx,
                worker_resp_rx: Mutex::new(worker_resp_rx),
                now_func: Mutex::new(default_now_func()),
                full_refresh_timer_counter: MetricsCounter::new(),
                partial_refresh_timer_counter: MetricsCounter::new(),
                retry_loop_wait: AtomicI64::new(10 * SECOND),
                ctx_state: Mutex::new(CtxState {
                    ctx: None,
                    cancel: None,
                }),
                wait_group: Arc::new(WaitGroup::new()),
            }),
        }
    }

    /// The group id this runtime was built with.
    pub fn group_id(&self) -> &str {
        &self.shared.group_id
    }

    /// The store this runtime reads and writes.
    pub fn store(&self) -> &TimerStore {
        &self.shared.store
    }

    /// The client the hook factories receive.
    pub fn client(&self) -> Arc<dyn TimerClient> {
        Arc::clone(&self.shared.cli)
    }

    /// Go's `rt.cond`.
    pub fn cond(&self) -> Arc<dyn Cond> {
        Arc::clone(&self.shared.cond.lock().unwrap())
    }

    /// Assigns `rt.cond` after the build, as the upstream tests do.
    pub fn set_cond(&self, cond: Arc<dyn Cond>) {
        *self.shared.cond.lock().unwrap() = cond;
    }

    /// Go's `rt.cache`.
    pub fn cache(&self) -> MutexGuard<'_, TimersCache> {
        self.shared.cache.lock().unwrap()
    }

    /// Go's `rt.workers`.
    pub fn workers(&self) -> MutexGuard<'_, HashMap<String, Arc<HookWorker>>> {
        self.shared.workers.lock().unwrap()
    }

    /// Go's `rt.fullRefreshTimerCounter`.
    pub fn full_refresh_timer_counter(&self) -> &MetricsCounter {
        &self.shared.full_refresh_timer_counter
    }

    /// Go's `rt.partialRefreshTimerCounter`.
    pub fn partial_refresh_timer_counter(&self) -> &MetricsCounter {
        &self.shared.partial_refresh_timer_counter
    }

    /// Go's `rt.retryLoopWait`.
    pub fn set_retry_loop_wait(&self, nanoseconds: i64) {
        self.shared
            .retry_loop_wait
            .store(nanoseconds, Ordering::SeqCst);
    }

    /// Go `(*TimerGroupRuntime).setNowFunc`, "only used by test".
    pub fn set_now_func(&self, now_func: NowFunc) {
        *self.shared.now_func.lock().unwrap() = Arc::clone(&now_func);
        self.shared.cache.lock().unwrap().now_func = now_func;
    }

    fn now(&self) -> GoTime {
        let now_func = Arc::clone(&self.shared.now_func.lock().unwrap());
        now_func()
    }

    fn ctx(&self) -> Context {
        self.shared
            .ctx_state
            .lock()
            .unwrap()
            .ctx
            .clone()
            .unwrap_or_default()
    }

    /// Go `(*TimerGroupRuntime).initCtx`.
    pub fn init_ctx(&self) {
        let (ctx, cancel) = Context::with_cancel();
        let mut state = self.shared.ctx_state.lock().unwrap();
        state.ctx = Some(ctx);
        state.cancel = Some(cancel);
    }

    /// Go `(*TimerGroupRuntime).Start`.
    pub fn start(&self) {
        {
            let state = self.shared.ctx_state.lock().unwrap();
            if state.ctx.is_some() {
                return;
            }
        }

        self.init_ctx();
        let runtime = self.clone();
        let ctx = self.ctx();
        self.shared.wait_group.run(move || {
            with_recover_until(&ctx, |total_panic| runtime.timer_loop(total_panic));
        });
    }

    /// Go `(*TimerGroupRuntime).Running`.
    pub fn running(&self) -> bool {
        let state = self.shared.ctx_state.lock().unwrap();
        state.ctx.is_some() && state.cancel.is_some()
    }

    /// Go `(*TimerGroupRuntime).Stop`.
    pub fn stop(&self) {
        {
            let mut state = self.shared.ctx_state.lock().unwrap();
            if let Some(cancel) = state.cancel.take() {
                cancel.cancel();
            }
        }
        self.shared.wait_group.wait();
    }

    /// Go `(*TimerGroupRuntime).fullRefreshTimers`.
    pub fn full_refresh_timers(&self) {
        self.shared.full_refresh_timer_counter.inc();
        let cond = self.cond();
        let timers = match self.shared.store.list(&self.ctx(), Some(cond.as_ref())) {
            Ok(timers) => timers,
            Err(err) => {
                bg_logger().warn(
                    "error occurs when fullRefreshTimers",
                    &[Field::new("error", Value::Str(err.to_string()))],
                );
                return;
            }
        };
        self.cache().full_update_timers(&timers);
    }

    /// Go `(*TimerGroupRuntime).tryTriggerTimerEvents`.
    pub fn try_trigger_timer_events(&self) {
        let now = self.now();
        let mut ready: Vec<(TimerRecord, Option<GoTime>)> = Vec::new();
        self.cache()
            .iter_try_trigger_timers(|timer, try_trigger_time, next_event_time| {
                if try_trigger_time.after(&now) {
                    return false;
                }

                if timer.event_status.as_str() == SchedEventStatus::IDLE
                    && (!timer.spec.enable || next_event_time.is_none_or(|next| next.after(&now)))
                {
                    return true;
                }

                ready.push((timer.clone_record(), next_event_time.cloned()));
                true
            });

        if ready.is_empty() {
            return;
        }

        // resort timer to make sure the timer has the smallest nextEventTime
        // has a higher priority to trigger
        ready.sort_by(|left, right| match (&left.1, &right.1) {
            (None, None) => std::cmp::Ordering::Equal,
            (Some(_), None) => std::cmp::Ordering::Greater,
            (None, Some(_)) => std::cmp::Ordering::Less,
            (Some(left), Some(right)) => left.compare(right),
        });

        let mut retry_timer_ids: Vec<String> = Vec::new();
        let mut busy_workers: HashSet<String> = HashSet::new();
        for (timer, _) in &ready {
            let Some(worker) = self.ensure_worker(&timer.spec.hook_class) else {
                continue;
            };

            let event_id = if timer.event_id.is_empty() {
                new_uuid_hex()
            } else {
                timer.event_id.clone()
            };

            if self.ctx().is_done() {
                return;
            }

            let request = TriggerEventRequest {
                event_id: event_id.clone(),
                timer: Some(timer.clone_record()),
                store: self.shared.store.clone(),
                resp: Some(self.shared.worker_resp_tx.clone()),
            };

            match worker.ch.try_send(Some(request)) {
                Ok(()) => {
                    self.cache().set_timer_proc_status(
                        &timer.id,
                        RuntimeProcStatus::Triggering,
                        &event_id,
                    );
                }
                Err(_) => {
                    busy_workers.insert(timer.spec.hook_class.clone());
                    retry_timer_ids.push(timer.id.clone());
                }
            }
        }

        if !retry_timer_ids.is_empty() {
            bg_logger().warn(
                "some hook workers are busy, retry triggering after a while",
                &[Field::new(
                    "busyWorkers",
                    Value::Str(busy_workers.into_iter().collect::<Vec<_>>().join(",")),
                )],
            );
            let retry_at = now.add(retry_busy_worker_interval());
            let mut cache = self.cache();
            for timer_id in retry_timer_ids {
                cache.update_next_try_trigger_time(&timer_id, retry_at.clone());
            }
        }
    }

    /// Go `(*TimerGroupRuntime).tryCloseTriggeringTimers`.
    pub fn try_close_triggering_timers(&self) -> bool {
        let ids: HashSet<String> = self.cache().wait_close_timer_ids.clone();
        self.partial_refresh_timers(&ids)
    }

    /// Go `(*TimerGroupRuntime).getNextTryTriggerDuration`.
    pub fn get_next_try_trigger_duration(&self, last_try_trigger_time: &GoTime) -> i64 {
        let now = self.now();
        let since_last_trigger = now.sub(last_try_trigger_time).max(0);

        let max_duration = max_trigger_event_interval() - since_last_trigger;
        if max_duration <= 0 {
            return 0;
        }

        let min_duration = (min_trigger_event_interval() - since_last_trigger).max(0);

        let mut duration = max_duration;
        self.cache()
            .iter_try_trigger_timers(|_, try_trigger_time, _| {
                let interval = try_trigger_time.sub(&now);
                if interval < duration {
                    duration = interval;
                }
                false
            });

        duration.max(min_duration)
    }

    /// Go `(*TimerGroupRuntime).handleWorkerResponse`.
    pub fn handle_worker_response(&self, resp: &TriggerEventResponse) {
        if !self.cache().has_timer(&resp.timer_id) {
            return;
        }

        if let Some(update_timer) = resp.new_timer_record.get() {
            match update_timer {
                None => {
                    self.cache().remove_timer(&resp.timer_id);
                }
                Some(timer) => {
                    self.cache().update_timer(timer);
                }
            }
        }

        if resp.success {
            self.cache().set_timer_proc_status(
                &resp.timer_id,
                RuntimeProcStatus::WaitTriggerClose,
                &resp.event_id,
            );
        } else {
            self.cache()
                .set_timer_proc_status(&resp.timer_id, RuntimeProcStatus::Idle, "");
            if let Some(retry_after) = resp.retry_after.get() {
                let next = self.now().add(*retry_after);
                self.cache()
                    .update_next_try_trigger_time(&resp.timer_id, next);
            }
        }
    }

    /// Go `(*TimerGroupRuntime).partialRefreshTimers`.
    pub fn partial_refresh_timers(&self, timer_ids: &HashSet<String>) -> bool {
        if timer_ids.is_empty() {
            return false;
        }

        self.shared.partial_refresh_timer_counter.inc();
        let cond = self.build_timer_ids_cond(timer_ids);
        let timers = match self.shared.store.list(&self.ctx(), Some(cond.as_ref())) {
            Ok(timers) => timers,
            Err(err) => {
                bg_logger().warn(
                    "error occurs when get timers",
                    &[Field::new("error", Value::Str(err.to_string()))],
                );
                return false;
            }
        };

        if timers.len() != timer_ids.len() {
            let mut missing = timer_ids.clone();
            for timer in &timers {
                missing.remove(&timer.id);
            }
            let mut cache = self.cache();
            for timer_id in missing {
                cache.remove_timer(&timer_id);
            }
        }

        self.cache().partial_batch_update_timers(&timers)
    }

    /// Go `(*TimerGroupRuntime).createWatchTimerChan`.
    ///
    /// `None` is Go's `idleWatchChan`, the package-level channel that is never
    /// ready.
    pub fn create_watch_timer_chan(&self, ctx: &Context) -> Option<WatchTimerChan> {
        let watch_supported = self.shared.store.watch_supported();
        bg_logger().info(
            "create watch chan if possible for timer runtime",
            &[Field::new(
                "storeSupportWatch",
                Value::Str(watch_supported.to_string()),
            )],
        );
        if watch_supported {
            return Some(self.shared.store.watch(ctx));
        }
        None
    }

    /// Go `(*TimerGroupRuntime).batchHandleWatchResponses`.
    pub fn batch_handle_watch_responses(&self, responses: &[WatchTimerResponse]) -> bool {
        if responses.is_empty() {
            return false;
        }

        let mut update_timer_ids: HashSet<String> = HashSet::new();
        let mut del_timer_ids: HashSet<String> = HashSet::new();
        for response in responses {
            for event in &response.events {
                match event.tp {
                    WatchTimerEventType::Create | WatchTimerEventType::Update => {
                        update_timer_ids.insert(event.timer_id.clone());
                    }
                    WatchTimerEventType::Delete => {
                        del_timer_ids.insert(event.timer_id.clone());
                    }
                }
            }
        }

        let mut change = self.partial_refresh_timers(&update_timer_ids);
        let mut cache = self.cache();
        for timer_id in del_timer_ids {
            if cache.remove_timer(&timer_id) {
                change = true;
            }
        }
        change
    }

    /// Go `(*TimerGroupRuntime).ensureWorker`.
    pub fn ensure_worker(&self, hook_class: &str) -> Option<Arc<HookWorker>> {
        if let Some(worker) = self.shared.workers.lock().unwrap().get(hook_class) {
            return Some(Arc::clone(worker));
        }

        {
            let factories = self.shared.factories.lock().unwrap();
            factories.get(hook_class)?;
        }

        let hook_class_owned = hook_class.to_string();
        let shared = Arc::clone(&self.shared);
        let cli = Arc::clone(&self.shared.cli);
        let hook_fn: HookFn = Arc::new(move || {
            let factories = shared.factories.lock().unwrap();
            factories
                .get(&hook_class_owned)
                .map(|factory| factory(&hook_class_owned, Arc::clone(&cli)))
        });

        let now_func = Arc::clone(&self.shared.now_func.lock().unwrap());
        let worker = new_hook_worker(
            &self.ctx(),
            &self.shared.wait_group,
            &self.shared.group_id,
            hook_class,
            Some(hook_fn),
            Some(now_func),
        );
        self.shared
            .workers
            .lock()
            .unwrap()
            .insert(hook_class.to_string(), Arc::clone(&worker));
        Some(worker)
    }

    /// Go `(*TimerGroupRuntime).buildTimerIDsCond`.
    pub fn build_timer_ids_cond(&self, ids: &HashSet<String>) -> Arc<dyn Cond> {
        let children: Vec<Arc<dyn Cond>> = ids
            .iter()
            .map(|timer_id| {
                Arc::new(TimerCond {
                    id: OptionalVal::new(timer_id.clone()),
                    ..Default::default()
                }) as Arc<dyn Cond>
            })
            .collect();
        Arc::new(and(vec![self.cond(), Arc::new(or(children))]))
    }

    /// Go `(*TimerGroupRuntime).loop`, restated as a single poll loop; see the
    /// module header for how each `select` case maps onto a deadline.
    pub fn timer_loop(&self, total_panic: u64) {
        let ctx = self.ctx();
        if total_panic > 0 {
            sleep(&ctx, self.shared.retry_loop_wait.load(Ordering::SeqCst));
            bg_logger().info("TimerGroupRuntime loop resumed from panic", &[]);
        } else {
            bg_logger().info("TimerGroupRuntime loop started", &[]);
        }

        let (watch_ctx, cancel_watch) = Context::with_cancel();
        let _cancel_guard = CancelGuard {
            cancel: cancel_watch,
        };

        let start = Instant::now();
        let mut full_refresh_at = start + nanos(full_refresh_timers_interval());
        let mut check_wait_close_at = start + nanos(check_wait_close_timer_interval());
        let mut try_trigger_at = start + nanos(min_trigger_event_interval());
        let mut re_watch_at: Option<Instant> = None;
        let mut batch_at: Option<Instant> = None;

        let mut watch_ch = self.create_watch_timer_chan(&watch_ctx);
        let mut batch_responses: Vec<WatchTimerResponse> = Vec::with_capacity(1);
        let mut last_try_trigger_time = GoTime::zero();

        self.full_refresh_timers();
        loop {
            if ctx.is_done() {
                return;
            }

            let now = Instant::now();

            if now >= full_refresh_at {
                full_refresh_at = now + nanos(full_refresh_timers_interval());
                self.full_refresh_timers();
                try_trigger_at = self.next_try_trigger_deadline(&last_try_trigger_time);
            }

            if now >= try_trigger_at {
                self.try_trigger_timer_events();
                last_try_trigger_time = self.now();
                try_trigger_at = self.next_try_trigger_deadline(&last_try_trigger_time);
            }

            loop {
                let response = self.shared.worker_resp_rx.lock().unwrap().try_recv();
                match response {
                    Ok(response) => {
                        self.handle_worker_response(&response);
                        try_trigger_at = self.next_try_trigger_deadline(&last_try_trigger_time);
                    }
                    Err(_) => break,
                }
            }

            if now >= check_wait_close_at {
                check_wait_close_at = now + nanos(check_wait_close_timer_interval());
                if self.try_close_triggering_timers() {
                    try_trigger_at = self.next_try_trigger_deadline(&last_try_trigger_time);
                }
            }

            if batch_at.is_some_and(|deadline| now >= deadline) {
                batch_at = None;
                if self.batch_handle_watch_responses(&batch_responses) {
                    try_trigger_at = self.next_try_trigger_deadline(&last_try_trigger_time);
                }
                batch_responses.clear();
            }

            if let Some(receiver) = &watch_ch {
                match receiver.try_recv() {
                    Ok(response) => {
                        if batch_responses.is_empty() {
                            batch_at =
                                Some(Instant::now() + nanos(batch_process_watch_resp_interval()));
                        }
                        batch_responses.push(response);
                    }
                    Err(std::sync::mpsc::TryRecvError::Empty) => {}
                    Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                        bg_logger().warn(
                            "WatchTimerChan closed, retry watch after a while",
                            &[Field::new(
                                "storeSupportWatch",
                                Value::Str(self.shared.store.watch_supported().to_string()),
                            )],
                        );
                        watch_ch = None;
                        re_watch_at = Some(Instant::now() + nanos(re_watch_interval()));
                    }
                }
            }

            if re_watch_at.is_some_and(|deadline| Instant::now() >= deadline) {
                re_watch_at = None;
                if watch_ch.is_none() {
                    watch_ch = self.create_watch_timer_chan(&watch_ctx);
                }
            }

            std::thread::sleep(CHANNEL_POLL_INTERVAL);
        }
    }

    fn next_try_trigger_deadline(&self, last_try_trigger_time: &GoTime) -> Instant {
        Instant::now() + nanos(self.get_next_try_trigger_duration(last_try_trigger_time))
    }
}

fn nanos(value: i64) -> Duration {
    Duration::from_nanos(value.max(0) as u64)
}

/// Go's `defer cancelWatch()`.
struct CancelGuard {
    cancel: crate::store::CancelFn,
}

impl Drop for CancelGuard {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// Go `NewTimerRuntimeBuilder`.
pub fn new_timer_runtime_builder(group_id: &str, store: TimerStore) -> TimerRuntimeBuilder {
    TimerRuntimeBuilder::new(group_id, store)
}
