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

//! Go `pkg/ttl/ttlworker/timer.go`.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Serialize;
use tidb_log::Field;
use tidb_logutil::{bg_logger, Logger};
use tidb_timer::client::{with_set_summary_data, with_set_watermark, TimerClient};
use tidb_timer::error::TimerError;
use tidb_timer::go_time::{GoTime, MINUTE};
use tidb_timer::hook::{Hook, PreSchedEventResult, TimerShedEvent};
use tidb_timer::store::{OptionalVal, TimerCond, TimerStore};
use tidb_timer_runtime::{TimerGroupRuntime, TimerRuntimeBuilder, WaitGroup};
use tidb_ttl_client::context::{CancelFunc, Context};

use crate::config::get_check_job_interval;
use crate::job_manager::TtlSummary;
use crate::timer_sync::{TtlTimerData, TIMER_HOOK_CLASS, TIMER_KEY_PREFIX};
use crate::{field, Result};

/// Go `ttlTimerSummary`.
#[derive(Debug, Clone, Default, Serialize)]
pub struct TtlTimerSummary {
    /// Go `LastJobRequestID`, `omitempty`.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub last_job_request_id: String,
    /// Go `LastJobSummary`, `omitempty`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_job_summary: Option<TtlSummary>,
}

/// Go `TTLJobTrace`: what a submitted job looks like to its timer.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TtlJobTrace {
    /// Go `RequestID`: the request id the job was submitted with.
    pub request_id: String,
    /// Go `Finished`.
    pub finished: bool,
    /// Go `Summary`.
    pub summary: Option<TtlSummary>,
}

/// Go `TTLJobAdapter`: submits TTL jobs and traces them.
pub trait TtlJobAdapter: Send + Sync {
    /// Go `Now`: the current time in the system time zone.
    fn now(&self) -> Result<GoTime>;
    /// Go `CanSubmitJob`.
    fn can_submit_job(&self, table_id: i64, physical_id: i64) -> bool;
    /// Go `SubmitJob`.
    fn submit_job(
        &self,
        ctx: &Context,
        table_id: i64,
        physical_id: i64,
        request_id: &str,
        watermark: &GoTime,
    ) -> Result<TtlJobTrace>;
    /// Go `GetJob`.
    fn get_job(&self, ctx: &Context, table_id: i64, physical_id: i64, request_id: &str)
        -> Result<Option<TtlJobTrace>>;
}

/// Go `timeutil.WithinDayTimePeriod(TTLJobScheduleWindowStartTime,
/// TTLJobScheduleWindowEndTime, now)`.
pub fn within_ttl_job_schedule_window(now: &GoTime) -> bool {
    let read = |time: &std::sync::RwLock<tidb_vardef::DayTime>| {
        *time.read().unwrap_or_else(std::sync::PoisonError::into_inner)
    };
    // Go compares the UTC hour and minute only; any day carries them.
    let day = chrono::DateTime::<chrono::Utc>::from_timestamp(86_400, 0).expect("a valid instant");
    let at = |minutes: i64| day + chrono::Duration::minutes(minutes);
    tidb_timeutil::within_day_time_period(
        at(read(&tidb_vardef::TTL_JOB_SCHEDULE_WINDOW_START_TIME).utc_minutes()),
        at(read(&tidb_vardef::TTL_JOB_SCHEDULE_WINDOW_END_TIME).utc_minutes()),
        now.instant(),
    )
}

/// Go `ttlTimerHook`.
pub struct TtlTimerHook {
    adapter: Arc<dyn TtlJobAdapter>,
    cli: Arc<dyn TimerClient>,
    ctx: Context,
    cancel: CancelFunc,
    wg: Arc<WaitGroup>,
    /// Go `checkTTLJobInterval`.
    pub check_ttl_job_interval: Mutex<Duration>,
    /// Go `waitJobLoopCounter`, only used for test.
    pub wait_job_loop_counter: AtomicI64,
}

/// Go `newTTLTimerHook`.
pub fn new_ttl_timer_hook(adapter: Arc<dyn TtlJobAdapter>, cli: Arc<dyn TimerClient>) -> Arc<TtlTimerHook> {
    let (ctx, cancel) = Context::with_cancel(&Context::background());
    Arc::new(TtlTimerHook {
        adapter,
        cli,
        ctx,
        cancel,
        wg: Arc::new(WaitGroup::new()),
        check_ttl_job_interval: Mutex::new(get_check_job_interval()),
        wait_job_loop_counter: AtomicI64::new(0),
    })
}

fn timer_ctx() -> tidb_timer::store::Context {
    tidb_timer::store::Context::background()
}

impl Hook for TtlTimerHook {
    fn start(&self) {}

    fn stop(&self) {
        self.cancel.cancel();
        self.wg.wait();
    }

    fn on_pre_sched_event(
        &self,
        _ctx: &tidb_timer::store::Context,
        event: &dyn TimerShedEvent,
    ) -> tidb_timer::error::Result<PreSchedEventResult> {
        let mut r = PreSchedEventResult::default();
        if !tidb_vardef::ENABLE_TTL_JOB.load(Ordering::SeqCst) {
            r.delay = MINUTE;
            return Ok(r);
        }

        let now = self.adapter.now().map_err(TimerError::message)?;

        if !within_ttl_job_schedule_window(&now) {
            r.delay = MINUTE;
            return Ok(r);
        }

        let timer = event.timer();
        let data: TtlTimerData = match serde_json::from_slice(&timer.spec.data) {
            Ok(data) => data,
            Err(_) => {
                bg_logger().error(
                    "invalid TTL timer data",
                    &[
                        field("timerID", timer.id.clone()),
                        field("timerKey", timer.spec.key.clone()),
                        Field::new("data", tidb_log::Value::ByteString(timer.spec.data.clone())),
                    ],
                );
                r.delay = MINUTE;
                return Ok(r);
            }
        };

        if !self.adapter.can_submit_job(data.table_id, data.physical_id) {
            r.delay = MINUTE;
            return Ok(r);
        }

        Ok(r)
    }

    fn on_sched_event(&self, _ctx: &tidb_timer::store::Context, event: &dyn TimerShedEvent) -> tidb_timer::error::Result<()> {
        let timer = event.timer();
        let event_id = event.event_id().to_owned();
        let logger = bg_logger().with_fields(&[
            field("key", timer.spec.key.clone()),
            field("eventID", event_id.clone()),
            field("eventStart", timer.event_start.format_date_time()),
            field("tags", timer.spec.tags.join(",")),
        ]);

        logger.info(
            "timer triggered to run TTL job",
            &[field("manualRequest", timer.event_extra.event_manual_request_id.clone())],
        );

        if let Some(err) = self.ctx.err() {
            return Err(TimerError::message(err.to_string()));
        }

        let data: TtlTimerData = match serde_json::from_slice(&timer.spec.data) {
            Ok(data) => data,
            Err(err) => {
                logger.error(
                    "invalid TTL timer data",
                    &[Field::new("data", tidb_log::Value::ByteString(timer.spec.data.clone()))],
                );
                return Err(TimerError::message(err.to_string()));
            }
        };

        let mut job = self
            .adapter
            .get_job(&self.ctx, data.table_id, data.physical_id, &event_id)
            .map_err(TimerError::message)?;

        if job.is_none() {
            let mut cancel = false;
            if !timer.spec.enable || !self.adapter.can_submit_job(data.table_id, data.physical_id) {
                cancel = true;
                logger.warn("cancel current TTL timer event because table's ttl is not enabled", &[]);
            }

            let now = self.adapter.now().map_err(TimerError::message)?;

            if now.sub(&timer.event_start) > 10 * MINUTE {
                cancel = true;
                logger.warn("cancel current TTL timer event because job not submitted for a long time", &[]);
            }

            if cancel {
                return self.cli.close_timer_event(
                    &timer_ctx(),
                    &timer.id,
                    &event_id,
                    &[with_set_watermark(timer.spec.watermark.clone())],
                );
            }

            logger.info("submit TTL job for current timer event", &[]);
            job = Some(
                self.adapter
                    .submit_job(&self.ctx, data.table_id, data.physical_id, &event_id, &timer.event_start)
                    .map_err(TimerError::message)?,
            );
        }
        let job = job.expect("the job was found or submitted");

        let logger = logger.with_fields(&[field("jobRequestID", job.request_id.clone())]);
        logger.info("start to wait TTL job", &[]);
        self.wait_job_loop_counter.fetch_add(1, Ordering::SeqCst);
        let waiter = JobWaiter {
            adapter: Arc::clone(&self.adapter),
            cli: Arc::clone(&self.cli),
            ctx: self.ctx.clone(),
            interval: *self.check_ttl_job_interval.lock().unwrap_or_else(std::sync::PoisonError::into_inner),
        };
        let timer_id = timer.id.clone();
        let event_start = timer.event_start.clone();
        self.wg.run(move || waiter.wait_job_finished(&logger, &data, &timer_id, &event_id, &event_start));
        Ok(())
    }
}

/// What Go's `waitJobFinished` goroutine reads off the hook.
struct JobWaiter {
    adapter: Arc<dyn TtlJobAdapter>,
    cli: Arc<dyn TimerClient>,
    ctx: Context,
    interval: Duration,
}

impl JobWaiter {
    /// Go `waitJobFinished`.
    fn wait_job_finished(&self, logger: &Logger, data: &TtlTimerData, timer_id: &str, event_id: &str, event_start: &GoTime) {
        let ticker = crossbeam_channel::tick(self.interval);
        loop {
            crossbeam_channel::select! {
                recv(self.ctx.done()) -> _ => {
                    logger.info("stop waiting TTL job because of context cancelled", &[]);
                    break;
                }
                recv(ticker) -> _ => {}
            }

            let timer = match self.cli.get_timer_by_id(&timer_ctx(), timer_id) {
                Ok(timer) => timer,
                Err(TimerError::TimerNotExist) => {
                    logger.warn("stop waiting TTL job because of timer is deleted", &[]);
                    break;
                }
                Err(err) => {
                    logger.warn("GetTimerByID failed", &[field("error", err.to_string())]);
                    continue;
                }
            };

            if timer.event_id != event_id {
                logger.warn(
                    "stop waiting TTL job because of current event id changed",
                    &[field("newEventID", timer.event_id.clone())],
                );
                break;
            }

            let job = match self.adapter.get_job(&self.ctx, data.table_id, data.physical_id, event_id) {
                Ok(job) => job,
                Err(err) => {
                    logger.warn("GetJob error", &[field("error", err)]);
                    continue;
                }
            };

            if job.as_ref().is_some_and(|job| !job.finished) {
                continue;
            }

            let mut timer_summary = TtlTimerSummary {
                last_job_request_id: event_id.to_owned(),
                last_job_summary: None,
            };

            match job {
                Some(job) => timer_summary.last_job_summary = job.summary,
                None => logger.warn("job for current TTL timer event not found", &[]),
            }

            logger.info("TTL job is finished, close current timer event", &[]);
            let summary_data = match serde_json::to_vec(&timer_summary) {
                Ok(summary_data) => summary_data,
                Err(err) => {
                    logger.error("marshal summary error", &[field("error", err.to_string())]);
                    continue;
                }
            };

            if let Err(err) = self.cli.close_timer_event(
                &timer_ctx(),
                timer_id,
                event_id,
                &[with_set_watermark(event_start.clone()), with_set_summary_data(summary_data)],
            ) {
                logger.warn("CloseTimerEvent error", &[field("error", err.to_string())]);
                continue;
            }

            break;
        }
        logger.info("stop to wait TTL job", &[]);
    }
}

/// Go `ttlTimerRuntime`.
pub struct TtlTimerRuntime {
    rt: Mutex<Option<TimerGroupRuntime>>,
    store: TimerStore,
    adapter: Arc<dyn TtlJobAdapter>,
}

/// Go `newTTLTimerRuntime`.
pub fn new_ttl_timer_runtime(store: TimerStore, adapter: Arc<dyn TtlJobAdapter>) -> TtlTimerRuntime {
    TtlTimerRuntime {
        rt: Mutex::new(None),
        store,
        adapter,
    }
}

impl TtlTimerRuntime {
    /// Go `Resume`.
    pub fn resume(&self) {
        let mut rt = self.rt.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if rt.is_some() {
            return;
        }

        let adapter = Arc::clone(&self.adapter);
        let runtime = TimerRuntimeBuilder::new("ttl", self.store.clone())
            .set_cond(Arc::new(TimerCond {
                key: OptionalVal::new(TIMER_KEY_PREFIX.to_owned()),
                key_prefix: true,
                ..TimerCond::default()
            }))
            .register_hook_factory(
                TIMER_HOOK_CLASS,
                Box::new(move |_hook_class: &str, cli: Arc<dyn TimerClient>| {
                    new_ttl_timer_hook(Arc::clone(&adapter), cli) as Arc<dyn Hook>
                }),
            )
            .build();
        runtime.start();
        *rt = Some(runtime);
    }

    /// Go `Pause`.
    pub fn pause(&self) {
        let rt = self.rt.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take();
        if let Some(rt) = rt {
            rt.stop();
        }
    }

    /// Whether the runtime is resumed (Go's `r.rt != nil`).
    pub fn is_resumed(&self) -> bool {
        self.rt.lock().unwrap_or_else(std::sync::PoisonError::into_inner).is_some()
    }
}
