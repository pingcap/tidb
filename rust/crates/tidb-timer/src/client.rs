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

//! Transcreation of Go `pkg/timer/api/client.go`: the option builders and the
//! default timer client.

use std::sync::atomic::{AtomicU64, Ordering};

use tidb_log::Field;
use tidb_util::logutil::bg_logger;

use crate::error::{Result, TimerError};
use crate::go_time::{GoTime, MINUTE};
use crate::store::{Context, OptionalVal, TimerCond, TimerStore, TimerUpdate};
use crate::timer::{
    EventExtra, ManualRequest, SchedEventStatus, SchedPolicyType, TimerRecord, TimerSpec,
};
use crate::uuid::new_uuid_hex;

/// Go `clientMaxRetry`.
const CLIENT_MAX_RETRY: u32 = 5;
/// Go `clientRetryBackoff`, in milliseconds.
const CLIENT_RETRY_BACKOFF: u64 = 1000;

/// Go `GetTimerOption`.
pub type GetTimerOption = Box<dyn Fn(&mut TimerCond)>;

/// Go `WithKey`.
pub fn with_key(key: &str) -> GetTimerOption {
    let key = key.to_string();
    Box::new(move |cond| {
        cond.key.set(key.clone());
        cond.key_prefix = false;
    })
}

/// Go `WithKeyPrefix`.
pub fn with_key_prefix(key_prefix: &str) -> GetTimerOption {
    let key_prefix = key_prefix.to_string();
    Box::new(move |cond| {
        cond.key.set(key_prefix.clone());
        cond.key_prefix = true;
    })
}

/// Go `WithID`.
pub fn with_id(id: &str) -> GetTimerOption {
    let id = id.to_string();
    Box::new(move |cond| cond.id.set(id.clone()))
}

/// Go `WithTag`.
pub fn with_tag(tags: &[&str]) -> GetTimerOption {
    let tags: Vec<String> = tags.iter().map(|tag| (*tag).to_string()).collect();
    Box::new(move |cond| cond.tags.set(tags.clone()))
}

/// Go `UpdateTimerOption`.
pub type UpdateTimerOption = Box<dyn Fn(&mut TimerUpdate)>;

/// Go `WithSetEnable`.
pub fn with_set_enable(enable: bool) -> UpdateTimerOption {
    Box::new(move |update| update.enable.set(enable))
}

/// Go `WithSetTimeZone`.
pub fn with_set_time_zone(name: &str) -> UpdateTimerOption {
    let name = name.to_string();
    Box::new(move |update| update.time_zone.set(name.clone()))
}

/// Go `WithSetSchedExpr`.
pub fn with_set_sched_expr(policy_type: SchedPolicyType, expr: &str) -> UpdateTimerOption {
    let expr = expr.to_string();
    Box::new(move |update| {
        update.sched_policy_type.set(policy_type.clone());
        update.sched_policy_expr.set(expr.clone());
    })
}

/// Go `WithSetWatermark`.
pub fn with_set_watermark(watermark: GoTime) -> UpdateTimerOption {
    Box::new(move |update| update.watermark.set(watermark.clone()))
}

/// Go `WithSetSummaryData`.
pub fn with_set_summary_data(summary: Vec<u8>) -> UpdateTimerOption {
    Box::new(move |update| update.summary_data.set(summary.clone()))
}

/// Go `WithSetTags`.
pub fn with_set_tags(tags: Vec<String>) -> UpdateTimerOption {
    Box::new(move |update| update.tags.set(tags.clone()))
}

/// Go `TimerClient`: the interface exposed to users to manage timers.
pub trait TimerClient: Send + Sync {
    /// Go `GetDefaultNamespace`.
    fn get_default_namespace(&self) -> &str;
    /// Go `CreateTimer`.
    fn create_timer(&self, ctx: &Context, spec: TimerSpec) -> Result<TimerRecord>;
    /// Go `GetTimerByID`.
    fn get_timer_by_id(&self, ctx: &Context, timer_id: &str) -> Result<TimerRecord>;
    /// Go `GetTimerByKey`.
    fn get_timer_by_key(&self, ctx: &Context, key: &str) -> Result<TimerRecord>;
    /// Go `GetTimers`.
    fn get_timers(&self, ctx: &Context, opts: &[GetTimerOption]) -> Result<Vec<TimerRecord>>;
    /// Go `UpdateTimer`.
    fn update_timer(&self, ctx: &Context, timer_id: &str, opts: &[UpdateTimerOption])
        -> Result<()>;
    /// Go `ManualTriggerEvent`.
    fn manual_trigger_event(&self, ctx: &Context, timer_id: &str) -> Result<String>;
    /// Go `CloseTimerEvent`.
    fn close_timer_event(
        &self,
        ctx: &Context,
        timer_id: &str,
        event_id: &str,
        opts: &[UpdateTimerOption],
    ) -> Result<()>;
    /// Go `DeleteTimer`.
    fn delete_timer(&self, ctx: &Context, timer_id: &str) -> Result<bool>;
}

/// Go `DefaultStoreNamespace`.
pub const DEFAULT_STORE_NAMESPACE: &str = "default";

/// Go `defaultTimerClient`, the default implementation of `TimerClient`.
///
/// `retry_backoff` is exported through [`DefaultTimerClient::set_retry_backoff`]
/// because Go's own retry test reaches into the unexported field to shrink it.
pub struct DefaultTimerClient {
    namespace: String,
    store: TimerStore,
    retry_backoff: AtomicU64,
}

impl DefaultTimerClient {
    /// Go `NewDefaultTimerClient`.
    pub fn new(store: TimerStore) -> Self {
        Self {
            namespace: DEFAULT_STORE_NAMESPACE.to_string(),
            store,
            retry_backoff: AtomicU64::new(CLIENT_RETRY_BACKOFF),
        }
    }

    /// Sets the per-attempt retry backoff in milliseconds.
    pub fn set_retry_backoff(&self, backoff: u64) {
        self.retry_backoff.store(backoff, Ordering::Relaxed);
    }
}

/// `boundary:` Go `pkg/util.RunWithRetry`, the only symbol `client.go` borrows
/// from `pkg/util`. Reproduced here without its Prometheus
/// `RetryableErrorCount` counter, which this workspace has no registry for.
fn run_with_retry(
    retry_count: u32,
    backoff: u64,
    mut attempt: impl FnMut() -> (bool, Result<()>),
) -> Result<()> {
    let mut last = Ok(());
    for round in 1..=retry_count {
        let (retryable, result) = attempt();
        last = result;
        if last.is_ok() || !retryable {
            return last;
        }
        std::thread::sleep(std::time::Duration::from_millis(backoff * u64::from(round)));
    }
    last
}

impl TimerClient for DefaultTimerClient {
    fn get_default_namespace(&self) -> &str {
        &self.namespace
    }

    fn create_timer(&self, ctx: &Context, mut spec: TimerSpec) -> Result<TimerRecord> {
        if spec.namespace.is_empty() {
            spec.namespace = self.namespace.clone();
        }

        let timer_id = self.store.create(
            ctx,
            &TimerRecord {
                spec,
                ..Default::default()
            },
        )?;
        self.store.get_by_id(ctx, &timer_id)
    }

    fn get_timer_by_id(&self, ctx: &Context, timer_id: &str) -> Result<TimerRecord> {
        self.store.get_by_id(ctx, timer_id)
    }

    fn get_timer_by_key(&self, ctx: &Context, key: &str) -> Result<TimerRecord> {
        self.store.get_by_key(ctx, &self.namespace, key)
    }

    fn get_timers(&self, ctx: &Context, opts: &[GetTimerOption]) -> Result<Vec<TimerRecord>> {
        let mut cond = TimerCond::default();
        for opt in opts {
            opt(&mut cond);
        }
        self.store.list(ctx, Some(&cond))
    }

    fn update_timer(
        &self,
        ctx: &Context,
        timer_id: &str,
        opts: &[UpdateTimerOption],
    ) -> Result<()> {
        let mut update = TimerUpdate::default();
        for opt in opts {
            opt(&mut update);
        }
        self.store.update(ctx, timer_id, &update)
    }

    fn manual_trigger_event(&self, ctx: &Context, timer_id: &str) -> Result<String> {
        let request_id = new_uuid_hex();

        run_with_retry(
            CLIENT_MAX_RETRY,
            self.retry_backoff.load(Ordering::Relaxed),
            || {
                let timer = match self.store.get_by_id(ctx, timer_id) {
                    Ok(timer) => timer,
                    Err(err) => return (false, Err(err)),
                };

                if !timer.event_id.is_empty() {
                    return (
                        false,
                        Err(TimerError::message(
                            "manual trigger is not allowed when event is not closed",
                        )),
                    );
                }

                if !timer.spec.enable {
                    return (
                        false,
                        Err(TimerError::message(
                            "manual trigger is not allowed when timer is disabled",
                        )),
                    );
                }

                let result = self.store.update(
                    ctx,
                    timer_id,
                    &TimerUpdate {
                        manual_request: OptionalVal::new(ManualRequest {
                            manual_request_id: request_id.clone(),
                            manual_request_time: GoTime::now(),
                            manual_timeout: 2 * MINUTE,
                            ..Default::default()
                        }),
                        check_version: OptionalVal::new(timer.version),
                        ..Default::default()
                    },
                );

                if matches!(result, Err(TimerError::VersionNotMatch)) {
                    bg_logger().warn(
                        "failed to update timer for version not match, retry",
                        &[Field::new(
                            "timerID",
                            tidb_log::Value::Str(timer_id.to_string()),
                        )],
                    );
                    return (true, result);
                }

                (false, result)
            },
        )?;

        Ok(request_id)
    }

    fn close_timer_event(
        &self,
        ctx: &Context,
        timer_id: &str,
        event_id: &str,
        opts: &[UpdateTimerOption],
    ) -> Result<()> {
        let mut update = TimerUpdate::default();
        for opt in opts {
            opt(&mut update);
        }

        let fields = update.fields_set(&["Watermark", "SummaryData"]);
        if !fields.is_empty() {
            return Err(TimerError::message(format!(
                "The field(s) [{}] are not allowed to update when close event",
                fields.join(", ")
            )));
        }

        let timer = self.get_timer_by_id(ctx, timer_id)?;

        update.check_event_id.set(event_id.to_string());
        update.event_status.set(SchedEventStatus::idle());
        update.event_id.set(String::new());
        update.event_data.set(Vec::new());
        update.event_start.set(GoTime::zero());
        if !update.watermark.present() {
            update.watermark.set(timer.event_start.clone());
        }
        update.event_extra.set(EventExtra::default());
        self.store.update(ctx, timer_id, &update)
    }

    fn delete_timer(&self, ctx: &Context, timer_id: &str) -> Result<bool> {
        self.store.delete(ctx, timer_id)
    }
}

/// Go `NewDefaultTimerClient`.
pub fn new_default_timer_client(store: TimerStore) -> DefaultTimerClient {
    DefaultTimerClient::new(store)
}
