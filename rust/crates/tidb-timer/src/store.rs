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

//! Transcreation of Go `pkg/timer/api/store.go`: the optional-field carrier,
//! the condition algebra used to filter timer records, the update descriptor,
//! and the store interfaces.

use std::sync::{Arc, Condvar, Mutex};

use tidb_util::timeutil::TimeZone;

use crate::error::{Result, TimerError};
use crate::go_time::GoTime;
use crate::mem_store::get_mem_store_time_zone_loc;
use crate::timer::{EventExtra, ManualRequest, SchedEventStatus, SchedPolicyType, TimerRecord};

/// Go `OptionalVal[T]`: whether a field's condition has been set, or whether
/// the field should be updated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptionalVal<T> {
    value: Option<T>,
}

impl<T> Default for OptionalVal<T> {
    fn default() -> Self {
        Self { value: None }
    }
}

impl<T> OptionalVal<T> {
    /// Go `NewOptionalVal`.
    pub fn new(value: T) -> Self {
        Self { value: Some(value) }
    }

    /// Go `Present`.
    pub fn present(&self) -> bool {
        self.value.is_some()
    }

    /// Go `Get`'s first return value when present.
    pub fn get(&self) -> Option<&T> {
        self.value.as_ref()
    }

    /// Go `Set`.
    pub fn set(&mut self, value: T) {
        self.value = Some(value);
    }

    /// Go `Clear`.
    pub fn clear(&mut self) {
        self.value = None;
    }
}

impl<T: Clone + Default> OptionalVal<T> {
    /// Go `Get`, whose first return value is the type's zero when absent.
    pub fn get_or_zero(&self) -> (T, bool) {
        match &self.value {
            Some(value) => (value.clone(), true),
            None => (T::default(), false),
        }
    }
}

/// Go `Cond`: an interface to match a timer record.
pub trait Cond: Send + Sync {
    /// Go `Match`.
    fn match_record(&self, timer: &TimerRecord) -> bool;

    /// Lets [`not`] reproduce Go's `cond.(*Operator)` type assertion, which
    /// flips an existing operator's `Not` instead of nesting a new one.
    fn as_operator(&self) -> Option<&Operator> {
        None
    }
}

/// Go `TimerCond`: the condition to filter a timer record.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TimerCond {
    /// Go `ID`.
    pub id: OptionalVal<String>,
    /// Go `Namespace`.
    pub namespace: OptionalVal<String>,
    /// Go `Key`; interpreted according to `key_prefix`.
    pub key: OptionalVal<String>,
    /// Go `KeyPrefix`: match `key` as a prefix rather than for equality.
    pub key_prefix: bool,
    /// Go `Tags`: every listed tag must be present on the record.
    pub tags: OptionalVal<Vec<String>>,
}

/// The declaration-ordered field names Go's `iterOptionalFields` walks over
/// `TimerCond` (`KeyPrefix` is a plain `bool`, so reflection skips it).
const TIMER_COND_FIELDS: &[&str] = &["ID", "Namespace", "Key", "Tags"];

impl TimerCond {
    /// Go `(*TimerCond).FieldsSet`.
    ///
    /// Go excludes fields by `unsafe.Pointer` identity; this workspace forbids
    /// `unsafe`, so exclusions are named by the same Go field name that the
    /// result reports.
    pub fn fields_set(&self, excludes: &[&str]) -> Vec<String> {
        let present = [
            self.id.present(),
            self.namespace.present(),
            self.key.present(),
            self.tags.present(),
        ];
        TIMER_COND_FIELDS
            .iter()
            .zip(present)
            .filter(|(name, set)| *set && !excludes.contains(*name))
            .map(|(name, _)| (*name).to_string())
            .collect()
    }

    /// Go `(*TimerCond).Clear`.
    pub fn clear(&mut self) {
        self.id.clear();
        self.namespace.clear();
        self.key.clear();
        self.tags.clear();
        self.key_prefix = false;
    }
}

impl Cond for TimerCond {
    fn match_record(&self, timer: &TimerRecord) -> bool {
        if let Some(value) = self.id.get() {
            if &timer.id != value {
                return false;
            }
        }

        if let Some(value) = self.namespace.get() {
            if &timer.spec.namespace != value {
                return false;
            }
        }

        if let Some(value) = self.key.get() {
            if self.key_prefix && !timer.spec.key.starts_with(value.as_str()) {
                return false;
            }

            if !self.key_prefix && &timer.spec.key != value {
                return false;
            }
        }

        if let Some(values) = self.tags.get() {
            for value in values {
                if !timer.spec.tags.contains(value) {
                    return false;
                }
            }
        }

        true
    }
}

/// Go `TimerUpdate`: how to update a timer.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TimerUpdate {
    /// Go `Tags`.
    pub tags: OptionalVal<Vec<String>>,
    /// Go `Enable`.
    pub enable: OptionalVal<bool>,
    /// Go `TimeZone`.
    pub time_zone: OptionalVal<String>,
    /// Go `SchedPolicyType`.
    pub sched_policy_type: OptionalVal<SchedPolicyType>,
    /// Go `SchedPolicyExpr`.
    pub sched_policy_expr: OptionalVal<String>,
    /// Go `ManualRequest`.
    pub manual_request: OptionalVal<ManualRequest>,
    /// Go `EventStatus`.
    pub event_status: OptionalVal<SchedEventStatus>,
    /// Go `EventID`.
    pub event_id: OptionalVal<String>,
    /// Go `EventData`.
    pub event_data: OptionalVal<Vec<u8>>,
    /// Go `EventStart`.
    pub event_start: OptionalVal<GoTime>,
    /// Go `EventExtra`.
    pub event_extra: OptionalVal<EventExtra>,
    /// Go `Watermark`.
    pub watermark: OptionalVal<GoTime>,
    /// Go `SummaryData`.
    pub summary_data: OptionalVal<Vec<u8>>,
    /// Go `CheckVersion`: check the timer's version before updating.
    pub check_version: OptionalVal<u64>,
    /// Go `CheckEventID`: check the timer's event id before updating.
    pub check_event_id: OptionalVal<String>,
}

/// The declaration-ordered field names Go's `iterOptionalFields` walks over
/// `TimerUpdate`; `FieldsSet`'s result order is asserted by the upstream tests.
const TIMER_UPDATE_FIELDS: &[&str] = &[
    "Tags",
    "Enable",
    "TimeZone",
    "SchedPolicyType",
    "SchedPolicyExpr",
    "ManualRequest",
    "EventStatus",
    "EventID",
    "EventData",
    "EventStart",
    "EventExtra",
    "Watermark",
    "SummaryData",
    "CheckVersion",
    "CheckEventID",
];

impl TimerUpdate {
    /// Go `(*TimerUpdate).apply`, which returns an updated clone and leaves the
    /// argument untouched.
    pub fn apply(&self, record: &TimerRecord) -> Result<TimerRecord> {
        if let Some(version) = self.check_version.get() {
            if record.version != *version {
                return Err(TimerError::VersionNotMatch);
            }
        }

        if let Some(event_id) = self.check_event_id.get() {
            if &record.event_id != event_id {
                return Err(TimerError::EventIDNotMatch);
            }
        }

        let mut record = record.clone_record();
        if let Some(tags) = self.tags.get() {
            // Go normalizes an empty slice to nil so that a cleared tag list
            // compares equal to a record that never had tags.
            record.spec.tags = if tags.is_empty() {
                Vec::new()
            } else {
                tags.clone()
            };
        }

        if let Some(enable) = self.enable.get() {
            record.spec.enable = *enable;
        }

        if let Some(time_zone) = self.time_zone.get() {
            record.spec.time_zone = time_zone.clone();
            record.location = Some(get_mem_store_time_zone_loc(&record.spec.time_zone));
        }

        if let Some(policy_type) = self.sched_policy_type.get() {
            record.spec.sched_policy_type = policy_type.clone();
        }

        if let Some(expr) = self.sched_policy_expr.get() {
            record.spec.sched_policy_expr = expr.clone();
        }

        if let Some(manual) = self.manual_request.get() {
            record.manual_request = manual.clone();
        }

        if let Some(status) = self.event_status.get() {
            record.event_status = status.clone();
        }

        if let Some(event_id) = self.event_id.get() {
            record.event_id = event_id.clone();
        }

        if let Some(data) = self.event_data.get() {
            record.event_data = data.clone();
        }

        if let Some(start) = self.event_start.get() {
            record.event_start = start.clone();
        }

        if let Some(extra) = self.event_extra.get() {
            record.event_extra = extra.clone();
        }

        if let Some(watermark) = self.watermark.get() {
            record.spec.watermark = watermark.clone();
        }

        if let Some(summary) = self.summary_data.get() {
            record.summary_data = summary.clone();
        }

        Ok(record)
    }

    /// Go `(*TimerUpdate).FieldsSet`; see [`TimerCond::fields_set`] for how the
    /// `unsafe.Pointer` exclusions are spelled here.
    pub fn fields_set(&self, excludes: &[&str]) -> Vec<String> {
        let present = [
            self.tags.present(),
            self.enable.present(),
            self.time_zone.present(),
            self.sched_policy_type.present(),
            self.sched_policy_expr.present(),
            self.manual_request.present(),
            self.event_status.present(),
            self.event_id.present(),
            self.event_data.present(),
            self.event_start.present(),
            self.event_extra.present(),
            self.watermark.present(),
            self.summary_data.present(),
            self.check_version.present(),
            self.check_event_id.present(),
        ];
        TIMER_UPDATE_FIELDS
            .iter()
            .zip(present)
            .filter(|(name, set)| *set && !excludes.contains(*name))
            .map(|(name, _)| (*name).to_string())
            .collect()
    }

    /// Go `(*TimerUpdate).Clear`.
    pub fn clear(&mut self) {
        *self = Self::default();
    }
}

/// Go `OperatorTp`: the operator type of the condition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OperatorTp {
    /// Go `OperatorAnd`.
    #[default]
    And,
    /// Go `OperatorOr`.
    Or,
}

/// Go `Operator`, which implements `Cond`.
pub struct Operator {
    /// Go `Op`.
    pub op: OperatorTp,
    /// Go `Not`.
    pub not: bool,
    /// Go `Children`.
    pub children: Vec<Arc<dyn Cond>>,
}

impl std::fmt::Debug for Operator {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Operator")
            .field("op", &self.op)
            .field("not", &self.not)
            .field("children", &self.children.len())
            .finish()
    }
}

/// Go `And(children...)`.
pub fn and(children: Vec<Arc<dyn Cond>>) -> Operator {
    Operator {
        op: OperatorTp::And,
        not: false,
        children,
    }
}

/// Go `Or(children...)`.
pub fn or(children: Vec<Arc<dyn Cond>>) -> Operator {
    Operator {
        op: OperatorTp::Or,
        not: false,
        children,
    }
}

/// Go `Not(cond)`.
pub fn not(cond: Arc<dyn Cond>) -> Operator {
    if let Some(operator) = cond.as_operator() {
        return Operator {
            op: operator.op,
            not: !operator.not,
            children: operator.children.clone(),
        };
    }
    Operator {
        op: OperatorTp::And,
        not: true,
        children: vec![cond],
    }
}

impl Cond for Operator {
    fn match_record(&self, timer: &TimerRecord) -> bool {
        match self.op {
            OperatorTp::And => {
                for child in &self.children {
                    if !child.match_record(timer) {
                        return self.not;
                    }
                }
                !self.not
            }
            OperatorTp::Or => {
                for child in &self.children {
                    if child.match_record(timer) {
                        return !self.not;
                    }
                }
                self.not
            }
        }
    }

    fn as_operator(&self) -> Option<&Operator> {
        Some(self)
    }
}

/// Go `WatchTimerEventType`, a bit flag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchTimerEventType {
    /// Go `WatchTimerEventCreate` (bit 0).
    Create = 1,
    /// Go `WatchTimerEventUpdate` (bit 1).
    Update = 2,
    /// Go `WatchTimerEventDelete` (bit 2).
    Delete = 4,
}

/// Go `WatchTimerEvent`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchTimerEvent {
    /// Go `Tp`.
    pub tp: WatchTimerEventType,
    /// Go `TimerID`.
    pub timer_id: String,
}

/// Go `WatchTimerResponse`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchTimerResponse {
    /// Go `Events`.
    pub events: Vec<WatchTimerEvent>,
}

/// Go `WatchTimerChan`, the receive end of the watch stream.
pub type WatchTimerChan = std::sync::mpsc::Receiver<WatchTimerResponse>;

/// `boundary:` Go's standard-library `context.Context`, which every store
/// method takes. Rust has no stdlib equivalent, so this carries the one
/// capability `pkg/timer/api` actually uses: cancellation, observed by the
/// watch goroutine and ignored by every other method (exactly as in Go, where
/// the memory store discards its `ctx` argument).
#[derive(Debug, Clone, Default)]
pub struct Context {
    cancel: Option<Arc<CancelState>>,
}

#[derive(Debug, Default)]
struct CancelState {
    done: Mutex<bool>,
    changed: Condvar,
}

/// The cancel function Go's `context.WithCancel` returns.
#[derive(Debug, Clone)]
pub struct CancelFn {
    state: Arc<CancelState>,
}

impl CancelFn {
    /// Cancels the context, waking every waiter.
    pub fn cancel(&self) {
        *self.state.done.lock().unwrap() = true;
        self.state.changed.notify_all();
    }
}

impl Context {
    /// Go `context.Background()`: a context that is never cancelled.
    pub fn background() -> Self {
        Self::default()
    }

    /// Go `context.WithCancel(parent)`.
    pub fn with_cancel() -> (Self, CancelFn) {
        let state = Arc::new(CancelState::default());
        (
            Self {
                cancel: Some(Arc::clone(&state)),
            },
            CancelFn { state },
        )
    }

    /// Go `<-ctx.Done()` polled rather than selected on.
    pub fn is_done(&self) -> bool {
        match &self.cancel {
            Some(state) => *state.done.lock().unwrap(),
            None => false,
        }
    }
}

/// Go `TimerStoreCore`: the core methods every store implements.
pub trait TimerStoreCore: Send + Sync {
    /// Go `Create`. When `record.id` is empty an id is assigned; the final id
    /// is returned.
    fn create(&self, ctx: &Context, record: &TimerRecord) -> Result<String>;
    /// Go `List`.
    fn list(&self, ctx: &Context, cond: Option<&dyn Cond>) -> Result<Vec<TimerRecord>>;
    /// Go `Update`.
    fn update(&self, ctx: &Context, timer_id: &str, update: &TimerUpdate) -> Result<()>;
    /// Go `Delete`.
    fn delete(&self, ctx: &Context, timer_id: &str) -> Result<bool>;
    /// Go `WatchSupported`.
    fn watch_supported(&self) -> bool;
    /// Go `Watch`. The returned channel closes when `ctx` is cancelled.
    fn watch(&self, ctx: &Context) -> WatchTimerChan;
    /// Go `Close`.
    fn close(&self);
}

/// Go `TimerStore`, which extends `TimerStoreCore` with record lookups.
///
/// Go embeds the interface; Rust composes it behind an `Arc` so the same core
/// can be shared and wrapped (which is how the upstream retry test injects a
/// hook around `Update`).
#[derive(Clone)]
pub struct TimerStore {
    core: Arc<dyn TimerStoreCore>,
}

impl TimerStore {
    /// Wraps a core implementation, as Go's `TimerStore{TimerStoreCore: core}`.
    pub fn new(core: Arc<dyn TimerStoreCore>) -> Self {
        Self { core }
    }

    /// The wrapped core, for delegating wrappers.
    pub fn core(&self) -> &Arc<dyn TimerStoreCore> {
        &self.core
    }

    /// Go's promoted `Create`.
    pub fn create(&self, ctx: &Context, record: &TimerRecord) -> Result<String> {
        self.core.create(ctx, record)
    }

    /// Go's promoted `List`.
    pub fn list(&self, ctx: &Context, cond: Option<&dyn Cond>) -> Result<Vec<TimerRecord>> {
        self.core.list(ctx, cond)
    }

    /// Go's promoted `Update`.
    pub fn update(&self, ctx: &Context, timer_id: &str, update: &TimerUpdate) -> Result<()> {
        self.core.update(ctx, timer_id, update)
    }

    /// Go's promoted `Delete`.
    pub fn delete(&self, ctx: &Context, timer_id: &str) -> Result<bool> {
        self.core.delete(ctx, timer_id)
    }

    /// Go's promoted `WatchSupported`.
    pub fn watch_supported(&self) -> bool {
        self.core.watch_supported()
    }

    /// Go's promoted `Watch`.
    pub fn watch(&self, ctx: &Context) -> WatchTimerChan {
        self.core.watch(ctx)
    }

    /// Go's promoted `Close`.
    pub fn close(&self) {
        self.core.close();
    }

    /// Go `GetByID`; `ErrTimerNotExist` when the id is unknown.
    pub fn get_by_id(&self, ctx: &Context, timer_id: &str) -> Result<TimerRecord> {
        self.get_one_record(
            ctx,
            &TimerCond {
                id: OptionalVal::new(timer_id.to_string()),
                ..Default::default()
            },
        )
    }

    /// Go `GetByKey`; `ErrTimerNotExist` when the key is unknown.
    pub fn get_by_key(&self, ctx: &Context, namespace: &str, key: &str) -> Result<TimerRecord> {
        self.get_one_record(
            ctx,
            &TimerCond {
                namespace: OptionalVal::new(namespace.to_string()),
                key: OptionalVal::new(key.to_string()),
                ..Default::default()
            },
        )
    }

    fn get_one_record(&self, ctx: &Context, cond: &dyn Cond) -> Result<TimerRecord> {
        let records = self.list(ctx, Some(cond))?;
        records.into_iter().next().ok_or(TimerError::TimerNotExist)
    }
}

/// Go `TimerWatchEventNotifier`.
pub trait TimerWatchEventNotifier: Send + Sync {
    /// Go `Watch`.
    fn watch(&self, ctx: &Context) -> WatchTimerChan;
    /// Go `Notify`.
    fn notify(&self, tp: WatchTimerEventType, timer_id: &str);
    /// Go `Close`.
    fn close(&self);
}

/// Re-exported so `store.rs` callers can name the zone type in signatures.
pub type StoreTimeZone = TimeZone;
