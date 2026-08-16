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

//! Transcreation of Go `pkg/timer/runtime/worker.go`: the per-hook-class
//! worker that turns a trigger request into a hook invocation and answers the
//! runtime with the outcome.

use std::cell::RefCell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tidb_log::{Field, Value};
use tidb_util::logutil::bg_logger;

use crate::error::TimerError;
use crate::go_time::{GoTime, MINUTE, SECOND};
use crate::hook::{Hook, PreSchedEventResult, TimerShedEvent};
use crate::store::{Context, OptionalVal, TimerStore, TimerUpdate};
use crate::timer::{EventExtra, SchedEventStatus, TimerRecord};

use super::{with_recover_until, NowFunc, WaitGroup, CHANNEL_POLL_INTERVAL};

/// Go `workerRecvChanCap`.
pub const WORKER_RECV_CHAN_CAP: usize = 128;
/// Go `workerRespChanCap`.
pub const WORKER_RESP_CHAN_CAP: usize = 128;
/// Go `workerEventDefaultRetryInterval`.
pub const WORKER_EVENT_DEFAULT_RETRY_INTERVAL: i64 = 10 * SECOND;
/// Go `chanBlockInterval`.
pub const CHAN_BLOCK_INTERVAL: i64 = MINUTE;

/// `boundary:` Go's `metrics.TimerHookWorkerCounter` / `TimerScopeCounter`,
/// which return `prometheus.Counter`s. This workspace has no Prometheus
/// registry, so a counter is a plain atomic — enough for the upstream tests,
/// which replace the real counters with `mockutil.MetricsCounter` and read the
/// accumulated value back.
#[derive(Debug, Default)]
pub struct MetricsCounter {
    value: AtomicU64,
}

impl MetricsCounter {
    /// A fresh zeroed counter.
    pub fn new() -> Self {
        Self::default()
    }

    /// Go `counter.Inc()`.
    pub fn inc(&self) {
        self.value.fetch_add(1, Ordering::SeqCst);
    }

    /// Go `mockutil.MetricsCounter.Val()`.
    pub fn val(&self) -> u64 {
        self.value.load(Ordering::SeqCst)
    }

    /// Go's test helper that swaps a fresh counter in.
    pub fn reset(&self) {
        self.value.store(0, Ordering::SeqCst);
    }
}

/// Go `triggerEventRequest`.
#[derive(Clone)]
pub struct TriggerEventRequest {
    /// Go `eventID`.
    pub event_id: String,
    /// Go `timer`; `None` is Go's nil timer, which the worker discards.
    pub timer: Option<TimerRecord>,
    /// Go `store`.
    pub store: TimerStore,
    /// Go `resp`; `None` is Go's nil response channel.
    pub resp: Option<SyncSender<TriggerEventResponse>>,
}

impl TriggerEventRequest {
    fn timer_id(&self) -> String {
        self.timer
            .as_ref()
            .map(|timer| timer.id.clone())
            .unwrap_or_default()
    }

    /// Go `(*triggerEventRequest).DoneResponse`.
    pub fn done_response(&self) -> TriggerEventResponse {
        TriggerEventResponse {
            success: true,
            timer_id: self.timer_id(),
            event_id: self.event_id.clone(),
            new_timer_record: OptionalVal::default(),
            retry_after: OptionalVal::default(),
        }
    }

    /// Go `(*triggerEventRequest).RetryDefaultResponse`.
    pub fn retry_default_response(&self) -> TriggerEventResponse {
        TriggerEventResponse {
            success: false,
            timer_id: self.timer_id(),
            event_id: self.event_id.clone(),
            new_timer_record: OptionalVal::default(),
            retry_after: OptionalVal::new(WORKER_EVENT_DEFAULT_RETRY_INTERVAL),
        }
    }

    /// Go `(*triggerEventRequest).TimerMetaChangedResponse`.
    pub fn timer_meta_changed_response(&self, timer: Option<TimerRecord>) -> TriggerEventResponse {
        self.retry_default_response()
            .with_new_timer_record(timer)
            .with_retry_immediately()
    }
}

/// Go `triggerEventResponse`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriggerEventResponse {
    /// Go `success`.
    pub success: bool,
    /// Go `timerID`.
    pub timer_id: String,
    /// Go `eventID`.
    pub event_id: String,
    /// Go `newTimerRecord`, whose payload is a pointer and may be nil.
    pub new_timer_record: OptionalVal<Option<TimerRecord>>,
    /// Go `retryAfter`, in nanoseconds.
    pub retry_after: OptionalVal<i64>,
}

impl TriggerEventResponse {
    /// Go `(*triggerEventResponse).WithRetryImmediately`.
    pub fn with_retry_immediately(mut self) -> Self {
        self.retry_after.clear();
        self
    }

    /// Go `(*triggerEventResponse).WithRetryAfter`.
    pub fn with_retry_after(mut self, delay: i64) -> Self {
        self.retry_after.set(delay);
        self
    }

    /// Go `(*triggerEventResponse).WithNewTimerRecord`.
    pub fn with_new_timer_record(mut self, timer: Option<TimerRecord>) -> Self {
        self.new_timer_record.set(timer);
        self
    }
}

/// Go `timerEvent`.
pub struct TimerEvent {
    event_id: String,
    record: TimerRecord,
}

impl TimerShedEvent for TimerEvent {
    fn event_id(&self) -> &str {
        &self.event_id
    }

    fn timer(&self) -> &TimerRecord {
        &self.record
    }
}

/// Go `func() api.Hook`, the per-loop hook constructor.
pub type HookFn = Arc<dyn Fn() -> Option<Arc<dyn Hook>> + Send + Sync>;

/// The six per-hook-class counters `newHookWorker` builds.
#[derive(Debug, Default)]
pub struct WorkerCounters {
    /// Go `triggerRequestCounter`.
    pub trigger_request: MetricsCounter,
    /// Go `onPreSchedEventCounter`.
    pub on_pre_sched_event: MetricsCounter,
    /// Go `onPreSchedEventErrCounter`.
    pub on_pre_sched_event_err: MetricsCounter,
    /// Go `onPreSchedEventDelayCounter`.
    pub on_pre_sched_event_delay: MetricsCounter,
    /// Go `onSchedEventCounter`.
    pub on_sched_event: MetricsCounter,
    /// Go `onSchedEventErrCounter`.
    pub on_sched_event_err: MetricsCounter,
}

/// Go `hookWorker`.
///
/// Narrowing: Go smuggles `retryLoopWait`/`retryRequestWait` in through
/// `context.Context` values (`hookWorkerRetryLoopKey`,
/// `hookWorkerRetryRequestKey`, both documented as "only used for test").
/// Rust's [`Context`] carries no value bag, so the two are plain fields set by
/// [`new_hook_worker_with_retry`].
pub struct HookWorker {
    ctx: Context,
    group_id: String,
    hook_class: String,
    hook_fn: Option<HookFn>,
    /// Go `ch`, the request channel the runtime sends into.
    pub ch: SyncSender<Option<TriggerEventRequest>>,
    receiver: Mutex<Option<Receiver<Option<TriggerEventRequest>>>>,
    now_func: NowFunc,
    /// The counters Go keeps as six separate fields.
    pub counters: WorkerCounters,
    retry_loop_wait: i64,
    retry_request_wait: i64,
}

impl HookWorker {
    /// A worker that owns nothing but its request channel.
    ///
    /// Go's `TestTryTriggerTimer` builds `&hookWorker{ch: ch}` directly, which
    /// Rust cannot do for a struct with private fields; this is that literal.
    pub fn channel_only(ch: SyncSender<Option<TriggerEventRequest>>) -> Self {
        Self {
            ctx: Context::background(),
            group_id: String::new(),
            hook_class: String::new(),
            hook_fn: None,
            ch,
            receiver: Mutex::new(None),
            now_func: super::default_now_func(),
            counters: WorkerCounters::default(),
            retry_loop_wait: 10 * SECOND,
            retry_request_wait: 5 * SECOND,
        }
    }

    fn logger_fields(&self) -> Vec<Field> {
        vec![
            Field::new("groupID", Value::Str(self.group_id.clone())),
            Field::new("hookClass", Value::Str(self.hook_class.clone())),
        ]
    }

    /// Go `(*hookWorker).loop`.
    pub fn worker_loop(&self, total_panic: u64) {
        if total_panic > 0 {
            super::sleep(&self.ctx, self.retry_loop_wait);
            bg_logger().info(
                "timer hookWorker loop resumed from panic",
                &self.logger_fields(),
            );
        } else {
            bg_logger().info("timer hookWorker loop started", &self.logger_fields());
        }

        let hook = self.hook_fn.as_ref().and_then(|hook_fn| hook_fn());

        if let Some(hook) = &hook {
            // Go: `defer hook.Stop()` is registered *before* `hook.Start()`, so
            // a panic out of `Start` still stops the hook.
            let started = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| hook.start()));
            if let Err(payload) = started {
                hook.stop();
                bg_logger().info("timer hookWorker loop exited", &self.logger_fields());
                std::panic::resume_unwind(payload);
            }
        }

        // TODO: we can have multiple `handleRequestLoop` goroutines running
        // concurrently.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.handle_request_loop(hook.clone());
        }));

        if let Some(hook) = &hook {
            hook.stop();
        }
        bg_logger().info("timer hookWorker loop exited", &self.logger_fields());
        if let Err(payload) = result {
            std::panic::resume_unwind(payload);
        }
    }

    /// Go `(*hookWorker).handleRequestLoop`.
    pub fn handle_request_loop(&self, hook: Option<Arc<dyn Hook>>) {
        let unhandled: RefCell<Option<UnhandledRequest>> = RefCell::new(None);
        with_recover_until(&self.ctx, |total_panic| {
            // when retry a request, it will send a response to runtime without
            // calling hook. So we can do the first retry immediately to
            // assumption that it will succeed.
            let retry_zero = unhandled
                .borrow()
                .as_ref()
                .is_some_and(|pending| pending.retry == 0);
            let wait = if total_panic == 0 || retry_zero {
                0
            } else {
                self.retry_request_wait
            };

            if total_panic > 0 {
                std::thread::sleep(Duration::from_nanos(wait.max(0) as u64));
                bg_logger().info(
                    "handleRequestLoop resumed from panic",
                    &self.logger_fields(),
                );
            }

            let pending = unhandled.borrow_mut().take();
            if let Some(mut pending) = pending {
                pending.retry += 1;
                unhandled.replace(Some(pending.clone()));
                self.handle_request_once(hook.as_ref(), &pending);
                unhandled.replace(None);
            }

            let receiver = self.receiver.lock().unwrap_or_else(|err| err.into_inner());
            let Some(receiver) = receiver.as_ref() else {
                return;
            };
            loop {
                if self.ctx.is_done() {
                    return;
                }
                // Go selects on `ctx.Done()` and `w.ch` at once; std's
                // receiver has no such select, so cancellation is observed by
                // polling at `CHANNEL_POLL_INTERVAL`.
                match receiver.recv_timeout(CHANNEL_POLL_INTERVAL) {
                    Ok(request) => {
                        let pending = UnhandledRequest {
                            req: request,
                            retry: 0,
                        };
                        unhandled.replace(Some(pending.clone()));
                        self.handle_request_once(hook.as_ref(), &pending);
                        unhandled.replace(None);
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return,
                }
            }
        });
    }

    /// Go `(*hookWorker).handleRequestOnce`.
    pub fn handle_request_once(&self, hook: Option<&Arc<dyn Hook>>, pending: &UnhandledRequest) {
        let Some(req) = &pending.req else {
            return;
        };

        if req.timer.is_none() {
            bg_logger().warn(
                "invalid triggerEventRequest, timer is nil",
                &self.logger_fields(),
            );
            return;
        }

        let Some(resp_ch) = req.resp.clone() else {
            bg_logger().warn(
                "invalid triggerEventRequest, resp chan is nil",
                &self.logger_fields(),
            );
            return;
        };

        let resp = if pending.retry > 0 {
            bg_logger().info("retry triggerEventRequest", &self.logger_fields());
            req.retry_default_response()
        } else {
            self.trigger_event(hook, req)
        };
        self.response_chan(&resp_ch, resp);
    }

    /// Go `(*hookWorker).triggerEvent`.
    pub fn trigger_event(
        &self,
        hook: Option<&Arc<dyn Hook>>,
        req: &TriggerEventRequest,
    ) -> TriggerEventResponse {
        self.counters.trigger_request.inc();
        let timer = req.timer.clone().expect("caller checked the timer");

        if timer.is_manual_requesting() {
            let timeout = timer
                .manual_request
                .manual_request_time
                .add(timer.manual_request.manual_timeout);
            if (self.now_func)().after(&timeout) {
                bg_logger().warn(
                    "cancel manual trigger for timer is disabled for request timeout",
                    &self.logger_fields(),
                );

                let processed = timer.manual_request.set_processed("");
                // Go reassigns its `timer` local from `GetByID`, so a failed
                // lookup leaves it nil while a failed `Update` leaves the
                // original record in place; both shapes are kept here.
                let mut current = Some(timer.clone_record());
                let mut failure = req
                    .store
                    .update(
                        &self.ctx,
                        &timer.id,
                        &TimerUpdate {
                            manual_request: OptionalVal::new(processed),
                            check_version: OptionalVal::new(timer.version),
                            ..Default::default()
                        },
                    )
                    .err();

                if failure.is_none() {
                    match req.store.get_by_id(&self.ctx, &timer.id) {
                        Ok(record) => current = Some(record),
                        Err(err) => {
                            current = None;
                            failure = Some(err);
                        }
                    }
                }

                return match failure {
                    None | Some(TimerError::TimerNotExist) => {
                        req.timer_meta_changed_response(current)
                    }
                    Some(_) => {
                        bg_logger().error(
                            "error occurs when close manual request",
                            &self.logger_fields(),
                        );
                        req.retry_default_response()
                    }
                };
            }
        }

        if timer.event_status.as_str() == SchedEventStatus::IDLE {
            let mut pre_result = PreSchedEventResult::default();
            if let Some(hook) = hook {
                self.counters.on_pre_sched_event.inc();
                let event = TimerEvent {
                    event_id: req.event_id.clone(),
                    record: timer.clone_record(),
                };
                match hook.on_pre_sched_event(&self.ctx, &event) {
                    Err(_) => {
                        bg_logger().warn(
                            "error occurs when invoking hook.OnPreSchedEvent",
                            &self.logger_fields(),
                        );
                        self.counters.on_pre_sched_event_err.inc();
                        return req.retry_default_response();
                    }
                    Ok(result) => {
                        if result.delay > 0 {
                            self.counters.on_pre_sched_event_delay.inc();
                            return req.retry_default_response().with_retry_after(result.delay);
                        }
                        pre_result = result;
                    }
                }
            }

            let update = build_event_update(req, &pre_result, &self.now_func);
            if let Err(err) = req.store.update(&self.ctx, &timer.id, &update) {
                if err == TimerError::VersionNotMatch {
                    bg_logger().info(
                        "cannot change timer to trigger state, timer version not match",
                        &self.logger_fields(),
                    );
                    if let Ok(new_timer) = req.store.get_by_id(&self.ctx, &timer.id) {
                        return req.timer_meta_changed_response(Some(new_timer));
                    }
                }

                if err == TimerError::TimerNotExist {
                    bg_logger().info(
                        "cannot change timer to trigger state, timer deleted",
                        &self.logger_fields(),
                    );
                    return req.timer_meta_changed_response(None);
                }

                bg_logger().warn(
                    "error occurs to change timer to trigger state,",
                    &self.logger_fields(),
                );
                return req.retry_default_response();
            }
        }

        let timer = match req.store.get_by_id(&self.ctx, &timer.id) {
            Err(TimerError::TimerNotExist) => {
                bg_logger().info(
                    "cannot trigger timer event, timer deleted",
                    &self.logger_fields(),
                );
                return req.timer_meta_changed_response(None);
            }
            Err(_) => {
                bg_logger().warn(
                    "error occurs when getting timer record to trigger timer event",
                    &self.logger_fields(),
                );
                return req.retry_default_response();
            }
            Ok(timer) => timer,
        };

        if timer.event_id != req.event_id {
            bg_logger().info(
                "cannot trigger timer event, timer event closed",
                &self.logger_fields(),
            );
            return req.timer_meta_changed_response(Some(timer));
        }

        if let Some(hook) = hook {
            self.counters.on_sched_event.inc();
            let event = TimerEvent {
                event_id: req.event_id.clone(),
                record: timer.clone_record(),
            };
            if hook.on_sched_event(&self.ctx, &event).is_err() {
                self.counters.on_sched_event_err.inc();
                bg_logger().warn(
                    "error occurs when invoking hook.OnSchedEvent",
                    &self.logger_fields(),
                );
                return req
                    .retry_default_response()
                    .with_new_timer_record(Some(timer));
            }
        }

        req.done_response().with_new_timer_record(Some(timer))
    }

    /// Go `(*hookWorker).responseChan`.
    pub fn response_chan(
        &self,
        ch: &SyncSender<TriggerEventResponse>,
        resp: TriggerEventResponse,
    ) -> bool {
        let mut resp = resp;
        let start = std::time::Instant::now();
        let mut last_warned = start;
        loop {
            if self.ctx.is_done() {
                bg_logger().info(
                    "sending resp to chan aborted for context cancelled",
                    &self.logger_fields(),
                );
                return false;
            }

            match ch.try_send(resp) {
                Ok(()) => return true,
                Err(TrySendError::Disconnected(_)) => return false,
                Err(TrySendError::Full(returned)) => {
                    resp = returned;
                    if last_warned.elapsed() >= Duration::from_nanos(CHAN_BLOCK_INTERVAL as u64) {
                        last_warned = std::time::Instant::now();
                        bg_logger().warn(
                            "sending resp to chan is blocked for a long time",
                            &self.logger_fields(),
                        );
                    }
                    std::thread::sleep(CHANNEL_POLL_INTERVAL);
                }
            }
        }
    }
}

/// Go `unhandledRequest`.
#[derive(Clone)]
pub struct UnhandledRequest {
    /// Go `req`.
    pub req: Option<TriggerEventRequest>,
    /// Go `retry`.
    pub retry: i32,
}

/// Go `newHookWorker`.
pub fn new_hook_worker(
    ctx: &Context,
    wg: &Arc<WaitGroup>,
    group_id: &str,
    hook_class: &str,
    hook_fn: Option<HookFn>,
    now_func: Option<NowFunc>,
) -> Arc<HookWorker> {
    new_hook_worker_with_retry(
        ctx,
        wg,
        group_id,
        hook_class,
        hook_fn,
        now_func,
        10 * SECOND,
        5 * SECOND,
    )
}

/// Go `newHookWorker` with the two retry waits Go injects through context
/// values; see [`HookWorker`]'s narrowing note.
#[allow(clippy::too_many_arguments)]
pub fn new_hook_worker_with_retry(
    ctx: &Context,
    wg: &Arc<WaitGroup>,
    group_id: &str,
    hook_class: &str,
    hook_fn: Option<HookFn>,
    now_func: Option<NowFunc>,
    retry_loop_wait: i64,
    retry_request_wait: i64,
) -> Arc<HookWorker> {
    let (sender, receiver) = sync_channel(WORKER_RECV_CHAN_CAP);
    let worker = Arc::new(HookWorker {
        ctx: ctx.clone(),
        group_id: group_id.to_string(),
        hook_class: hook_class.to_string(),
        hook_fn,
        ch: sender,
        receiver: Mutex::new(Some(receiver)),
        now_func: now_func.unwrap_or_else(super::default_now_func),
        counters: WorkerCounters::default(),
        retry_loop_wait,
        retry_request_wait,
    });

    let spawned = Arc::clone(&worker);
    wg.run(move || {
        with_recover_until(&spawned.ctx.clone(), |total_panic| {
            spawned.worker_loop(total_panic);
        });
    });
    worker
}

/// Go `buildEventUpdate`.
pub fn build_event_update(
    req: &TriggerEventRequest,
    result: &PreSchedEventResult,
    now_func: &NowFunc,
) -> TimerUpdate {
    let timer = req.timer.as_ref().expect("caller checked the timer");
    let mut update = TimerUpdate::default();
    update.event_status.set(SchedEventStatus::trigger());
    update.event_id.set(req.event_id.clone());
    update.event_start.set(now_func());
    update.event_data.set(result.event_data.clone());
    update.check_version.set(timer.version);

    let mut event_extra = EventExtra {
        event_watermark: timer.spec.watermark.clone(),
        ..Default::default()
    };

    let manual = &timer.manual_request;
    if manual.is_manual_requesting() {
        event_extra.event_manual_request_id = manual.manual_request_id.clone();
        update
            .manual_request
            .set(manual.set_processed(&req.event_id));
    }

    update.event_extra.set(event_extra);
    update
}

/// Re-exported so callers can build the response channel Go sizes at
/// `workerRespChanCap`.
pub fn new_worker_resp_channel() -> (
    SyncSender<TriggerEventResponse>,
    Receiver<TriggerEventResponse>,
) {
    sync_channel(WORKER_RESP_CHAN_CAP)
}

/// The zero-capacity response channel the upstream worker tests use.
pub fn new_rendezvous_resp_channel() -> (
    SyncSender<TriggerEventResponse>,
    Receiver<TriggerEventResponse>,
) {
    sync_channel(0)
}

/// Helper mirroring Go's `time.Now()` default for a worker's `nowFunc`.
pub fn now_go_time() -> GoTime {
    GoTime::now()
}
