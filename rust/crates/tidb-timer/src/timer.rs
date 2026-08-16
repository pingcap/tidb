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

//! Transcreation of Go `pkg/timer/api/timer.go`: the timer specification, the
//! record the store persists, and the two schedule policies.

use std::fmt;

use tidb_parser::duration::parse_config_duration;
use tidb_util::timeutil::{parse_time_zone, TimeZone};

use crate::cron;
use crate::error::{Result, TimerError};
use crate::go_time::GoTime;

/// Go `SchedPolicyType`: the type of the event schedule policy.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SchedPolicyType(pub String);

impl SchedPolicyType {
    /// Go `SchedEventInterval`: schedule events every fixed interval.
    pub const INTERVAL: &'static str = "INTERVAL";
    /// Go `SchedEventCron`: schedule events by cron expression.
    pub const CRON: &'static str = "CRON";

    /// Go's `SchedEventInterval` constant as a value.
    pub fn interval() -> Self {
        Self(Self::INTERVAL.to_string())
    }

    /// Go's `SchedEventCron` constant as a value.
    pub fn cron() -> Self {
        Self(Self::CRON.to_string())
    }

    /// The underlying string, as Go's `string(tp)`.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for SchedPolicyType {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl fmt::Display for SchedPolicyType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Go `SchedEventPolicy`: tells the runtime how to schedule a timer's events.
pub trait SchedEventPolicy: fmt::Debug {
    /// Go `NextEventTime`. The second value is false when no event follows
    /// `watermark`.
    fn next_event_time(&self, watermark: &GoTime) -> (GoTime, bool);
}

/// The closed set of implementations Go's `CreateSchedEventPolicy` returns.
///
/// Go's factory returns the `SchedEventPolicy` interface and its tests then
/// assert the dynamic type with `require.IsType`. An enum keeps that check
/// expressible without downcasting, while [`SchedEventPolicy`] stays available
/// for callers that only need the behavior.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchedPolicy {
    /// Go `*SchedIntervalPolicy`.
    Interval(SchedIntervalPolicy),
    /// Go `*CronPolicy`.
    Cron(CronPolicy),
}

impl SchedEventPolicy for SchedPolicy {
    fn next_event_time(&self, watermark: &GoTime) -> (GoTime, bool) {
        match self {
            Self::Interval(policy) => policy.next_event_time(watermark),
            Self::Cron(policy) => policy.next_event_time(watermark),
        }
    }
}

/// Go `SchedIntervalPolicy`, the policy of type `SchedEventInterval`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchedIntervalPolicy {
    expr: String,
    interval: i64,
}

impl SchedIntervalPolicy {
    /// Go `NewSchedIntervalPolicy`.
    ///
    /// Go's `failpoint.Inject("overwrite-ttl-job-interval")` is not carried
    /// over: no failpoint registry exists in this workspace, and the injection
    /// only exists to shrink the interval inside TTL's own integration tests.
    pub fn new(expr: &str) -> Result<Self> {
        let interval = parse_config_duration(expr).map_err(|err| {
            TimerError::message(err.to_string())
                .wrap(format_args!("invalid schedule event expr '{expr}'"))
        })?;
        Ok(Self {
            expr: expr.to_string(),
            interval,
        })
    }

    /// The expression this policy was built from (Go's unexported `expr`).
    pub fn expr(&self) -> &str {
        &self.expr
    }

    /// The parsed interval in nanoseconds (Go's unexported `interval`).
    pub fn interval(&self) -> i64 {
        self.interval
    }
}

impl SchedEventPolicy for SchedIntervalPolicy {
    fn next_event_time(&self, watermark: &GoTime) -> (GoTime, bool) {
        if watermark.is_zero() {
            return (watermark.clone(), true);
        }
        (watermark.add(self.interval), true)
    }
}

/// Go `CronPolicy`, the policy of type `SchedEventCron`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronPolicy {
    cron_schedule: cron::Schedule,
}

impl CronPolicy {
    /// Go `NewCronPolicy`.
    pub fn new(expr: &str) -> Result<Self> {
        let cron_schedule = cron::parse_standard(expr).map_err(|err| {
            TimerError::message(err).wrap(format_args!("invalid cron expr '{expr}'"))
        })?;
        Ok(Self { cron_schedule })
    }
}

impl SchedEventPolicy for CronPolicy {
    fn next_event_time(&self, watermark: &GoTime) -> (GoTime, bool) {
        let next = self.cron_schedule.next(watermark);
        let ok = !next.is_zero();
        (next, ok)
    }
}

/// Go `ManualRequest`: the request info to trigger a timer manually.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ManualRequest {
    /// Go `ManualRequestID`.
    pub manual_request_id: String,
    /// Go `ManualRequestTime`.
    pub manual_request_time: GoTime,
    /// Go `ManualTimeout`, in nanoseconds.
    pub manual_timeout: i64,
    /// Go `ManualProcessed`: the request is processed (triggered or timed out).
    pub manual_processed: bool,
    /// Go `ManualEventID`: the triggered event id for the current request.
    pub manual_event_id: String,
}

impl ManualRequest {
    /// Go `IsManualRequesting`.
    pub fn is_manual_requesting(&self) -> bool {
        !self.manual_request_id.is_empty() && !self.manual_processed
    }

    /// Go `SetProcessed`, which returns a copy rather than mutating.
    pub fn set_processed(&self, event_id: &str) -> Self {
        let mut new_manual = self.clone();
        new_manual.manual_processed = true;
        new_manual.manual_event_id = event_id.to_string();
        new_manual
    }
}

/// Go `EventExtra`: extra attributes for an event.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EventExtra {
    /// Go `EventManualRequestID`; empty when the event was not manual.
    pub event_manual_request_id: String,
    /// Go `EventWatermark`: the watermark when the event triggered.
    pub event_watermark: GoTime,
}

/// Go `SchedEventStatus`: the current schedule status of a timer's event.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SchedEventStatus(pub String);

impl SchedEventStatus {
    /// Go `SchedEventIdle`: the timer is not in the trigger state.
    pub const IDLE: &'static str = "IDLE";
    /// Go `SchedEventTrigger`: the timer is in the trigger state.
    pub const TRIGGER: &'static str = "TRIGGER";

    /// Go's `SchedEventIdle` constant as a value.
    pub fn idle() -> Self {
        Self(Self::IDLE.to_string())
    }

    /// Go's `SchedEventTrigger` constant as a value.
    pub fn trigger() -> Self {
        Self(Self::TRIGGER.to_string())
    }

    /// The underlying string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Go `TimerSpec`: a timer's specification, without runtime status.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TimerSpec {
    /// Go `Namespace`.
    pub namespace: String,
    /// Go `Key`, unique inside each namespace.
    pub key: String,
    /// Go `Tags`.
    pub tags: Vec<String>,
    /// Go `Data`, a user-defined binary.
    pub data: Vec<u8>,
    /// Go `TimeZone`; empty means the cluster's time zone.
    pub time_zone: String,
    /// Go `SchedPolicyType`.
    pub sched_policy_type: SchedPolicyType,
    /// Go `SchedPolicyExpr`.
    pub sched_policy_expr: String,
    /// Go `HookClass`.
    pub hook_class: String,
    /// Go `Watermark`: the progress of the timer's event schedule.
    pub watermark: GoTime,
    /// Go `Enable`.
    pub enable: bool,
}

impl TimerSpec {
    /// Go `(*TimerSpec).Clone`.
    ///
    /// Go copies the struct, so `Tags` and `Data` stay shared with the source;
    /// nothing in this package mutates either slice in place, so the deep Rust
    /// clone is observationally identical.
    pub fn clone_spec(&self) -> Self {
        self.clone()
    }

    /// Go `(*TimerSpec).Validate`.
    pub fn validate(&self) -> Result<()> {
        if self.namespace.is_empty() {
            return Err(TimerError::message("field 'Namespace' should not be empty"));
        }

        if self.key.is_empty() {
            return Err(TimerError::message("field 'Key' should not be empty"));
        }

        validate_time_zone(&self.time_zone)?;

        if self.sched_policy_type.as_str().is_empty() {
            return Err(TimerError::message(
                "field 'SchedPolicyType' should not be empty",
            ));
        }

        self.create_sched_event_policy()
            .map_err(|err| err.wrap("schedule event configuration is not valid"))?;

        Ok(())
    }

    /// Go `(*TimerSpec).CreateSchedEventPolicy`.
    pub fn create_sched_event_policy(&self) -> Result<SchedPolicy> {
        create_sched_event_policy(&self.sched_policy_type, &self.sched_policy_expr)
    }
}

/// Go package-level `CreateSchedEventPolicy`.
pub fn create_sched_event_policy(policy_type: &SchedPolicyType, expr: &str) -> Result<SchedPolicy> {
    match policy_type.as_str() {
        SchedPolicyType::INTERVAL => Ok(SchedPolicy::Interval(SchedIntervalPolicy::new(expr)?)),
        SchedPolicyType::CRON => Ok(SchedPolicy::Cron(CronPolicy::new(expr)?)),
        other => Err(TimerError::message(format!(
            "invalid schedule event type: '{other}'"
        ))),
    }
}

/// Go `TimerRecord`: the timer record saved in the timer store.
///
/// Go embeds `TimerSpec`, `ManualRequest` and `EventExtra` so their fields are
/// promoted onto the record. Rust has no field promotion, so they are named
/// fields here and every access spells the group out.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TimerRecord {
    /// Go's embedded `TimerSpec`.
    pub spec: TimerSpec,
    /// Go `ID`, assigned by the store on create.
    pub id: String,
    /// Go's embedded `ManualRequest`.
    pub manual_request: ManualRequest,
    /// Go `EventStatus`.
    pub event_status: SchedEventStatus,
    /// Go `EventID`; empty while `EventStatus` is `IDLE`.
    pub event_id: String,
    /// Go `EventData`; empty while `EventStatus` is `IDLE`.
    pub event_data: Vec<u8>,
    /// Go `EventStart`; the zero time while `EventStatus` is `IDLE`.
    pub event_start: GoTime,
    /// Go's embedded `EventExtra`.
    pub event_extra: EventExtra,
    /// Go `SummaryData`.
    pub summary_data: Vec<u8>,
    /// Go `CreateTime`.
    pub create_time: GoTime,
    /// Go `Version`, bumped on every update.
    pub version: u64,
    /// Go `Location`, the alias of the TiDB timezone.
    pub location: Option<TimeZone>,
}

impl TimerRecord {
    /// Go `(*TimerRecord).NextEventTime`.
    pub fn next_event_time(&self) -> Result<(GoTime, bool)> {
        if !self.spec.enable {
            return Ok((GoTime::zero(), false));
        }

        let mut watermark = self.spec.watermark.clone();
        if let Some(location) = &self.location {
            watermark = watermark.in_location(location);
        }

        let policy =
            create_sched_event_policy(&self.spec.sched_policy_type, &self.spec.sched_policy_expr)?;
        let (time, ok) = policy.next_event_time(&watermark);
        Ok((time, ok))
    }

    /// Go `(*TimerRecord).Clone`.
    pub fn clone_record(&self) -> Self {
        self.clone()
    }

    /// Go's promoted `(*TimerSpec).Validate` on the record.
    pub fn validate(&self) -> Result<()> {
        self.spec.validate()
    }

    /// Go's promoted `(*ManualRequest).IsManualRequesting` on the record.
    pub fn is_manual_requesting(&self) -> bool {
        self.manual_request.is_manual_requesting()
    }
}

/// Go `ValidateTimeZone`.
pub fn validate_time_zone(time_zone: &str) -> Result<()> {
    if !time_zone.is_empty() {
        parse_time_zone(time_zone).map_err(|err| TimerError::message(err.to_string()))?;
    }
    Ok(())
}
