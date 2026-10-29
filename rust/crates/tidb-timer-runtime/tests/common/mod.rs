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

//! Transcreation of Go `pkg/timer/runtime/main_test.go`'s shared fixtures.
//!
//! Go builds its mocks on `stretchr/testify/mock`, whose expectations are
//! queued per method, consumed once each (`.Once()`) or standing (`.Return(..)`
//! with no cardinality), and asserted drained by `AssertExpectations`. The
//! types below reproduce that contract directly: a `VecDeque` of outcomes per
//! method plus an optional standing outcome, and `assert_expectations` checks
//! the queues are empty. `Outcome::Panic` is testify's `.Panic(...)`.
//!
//! `goleak.VerifyTestMain` has no counterpart: every thread this port spawns is
//! owned by a `WaitGroup` that the tests join before returning.

#![allow(dead_code)]

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use tidb_timer::go_time::GoTime;
use tidb_timer::hook::{Hook, PreSchedEventResult, TimerShedEvent};
use tidb_timer::store::{
    Cond, Context, OperatorTp, TimerCond, TimerStore, TimerStoreCore, TimerUpdate, WatchTimerChan,
};
use tidb_timer::timer::{SchedEventStatus, SchedPolicyType, TimerRecord, TimerSpec};
use tidb_timer::{Result, TimerError};

/// Go `newTestTimer` in `cache_test.go`.
pub fn new_test_timer(id: &str, policy_expr: &str, watermark: GoTime) -> TimerRecord {
    TimerRecord {
        id: id.to_string(),
        spec: TimerSpec {
            namespace: "n1".to_string(),
            key: format!("key-{id}"),
            sched_policy_type: SchedPolicyType::interval(),
            sched_policy_expr: policy_expr.to_string(),
            hook_class: "hook1".to_string(),
            watermark: watermark.clone(),
            enable: true,
            ..Default::default()
        },
        location: Some(watermark.location().clone()),
        event_status: SchedEventStatus::idle(),
        version: 1,
        ..Default::default()
    }
}

/// A one-shot signal, standing in for Go's `close(ch)` on a `chan struct{}`.
#[derive(Default)]
pub struct Event {
    state: Mutex<bool>,
    changed: Condvar,
}

impl Event {
    /// Go `close(ch)`.
    pub fn signal(&self) {
        *self.state.lock().unwrap() = true;
        self.changed.notify_all();
    }

    /// Go `waitDone(ch, timeout)`; panics on timeout exactly as Go does.
    pub fn wait_done(&self, timeout: Duration) {
        let done = self.state.lock().unwrap();
        let (done, _) = self
            .changed
            .wait_timeout_while(done, timeout, |done| !*done)
            .unwrap();
        assert!(*done, "wait done timeout");
    }

    /// Go `checkNotDone(ch, after)`.
    pub fn check_not_done(&self, after: Duration) {
        if !after.is_zero() {
            std::thread::sleep(after);
        }
        assert!(
            !*self.state.lock().unwrap(),
            "the channel is expected not done"
        );
    }

    /// Whether the signal has fired.
    pub fn is_done(&self) -> bool {
        *self.state.lock().unwrap()
    }
}

/// One queued mock result.
pub enum Outcome<T> {
    /// testify `.Return(value, nil)`.
    Ok(T),
    /// testify `.Return(zero, errors.New(msg))`.
    Err(&'static str),
    /// testify `.Return(zero, api.ErrXxx)`.
    Sentinel(TimerError),
    /// testify `.Panic(msg)`.
    Panic(&'static str),
}

impl<T> Outcome<T> {
    fn take(self) -> Result<T> {
        match self {
            Self::Ok(value) => Ok(value),
            Self::Err(message) => Err(TimerError::message(message)),
            Self::Sentinel(err) => Err(err),
            Self::Panic(message) => panic!("{message}"),
        }
    }
}

/// A method's queued `.Once()` outcomes plus an optional standing outcome.
pub struct Expectations<T> {
    queue: Mutex<VecDeque<Outcome<T>>>,
    standing: Mutex<Option<Outcome<T>>>,
    calls: AtomicUsize,
}

impl<T> Default for Expectations<T> {
    fn default() -> Self {
        Self {
            queue: Mutex::new(VecDeque::new()),
            standing: Mutex::new(None),
            calls: AtomicUsize::new(0),
        }
    }
}

impl<T: Clone> Expectations<T> {
    /// testify `.Once()`.
    pub fn once(&self, outcome: Outcome<T>) {
        self.queue
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .push_back(outcome);
    }

    /// testify `.Times(n)`.
    pub fn times(&self, count: usize, outcome: Outcome<T>)
    where
        Outcome<T>: CloneOutcome,
    {
        for _ in 0..count {
            self.once(outcome.clone_outcome());
        }
    }

    /// testify's cardinality-free `.Return(...)`.
    pub fn always(&self, outcome: Outcome<T>) {
        *self.standing.lock().unwrap_or_else(|err| err.into_inner()) = Some(outcome);
    }

    fn call(&self, method: &str) -> Result<T>
    where
        Outcome<T>: CloneOutcome,
    {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let queued = self
            .queue
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .pop_front();
        if let Some(outcome) = queued {
            return outcome.take();
        }
        let standing = self
            .standing
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .as_ref()
            .map(CloneOutcome::clone_outcome);
        match standing {
            Some(outcome) => outcome.take(),
            None => panic!("unexpected call to {method}"),
        }
    }

    /// How many times the method was called.
    pub fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    /// testify `AssertExpectations` for this method.
    pub fn assert_drained(&self, method: &str) {
        assert!(
            self.queue
                .lock()
                .unwrap_or_else(|err| err.into_inner())
                .is_empty(),
            "{method} has unfulfilled expectations"
        );
    }
}

/// `Outcome` is not `Clone` in general (it holds `T`); this narrows cloning to
/// the payloads the mocks actually queue.
pub trait CloneOutcome {
    /// A copy of this outcome.
    fn clone_outcome(&self) -> Self;
}

impl<T: Clone> CloneOutcome for Outcome<T> {
    fn clone_outcome(&self) -> Self {
        match self {
            Self::Ok(value) => Self::Ok(value.clone()),
            Self::Err(message) => Self::Err(message),
            Self::Sentinel(err) => Self::Sentinel(err.clone()),
            Self::Panic(message) => Self::Panic(message),
        }
    }
}

/// Go `mockHook`.
#[derive(Default)]
pub struct MockHook {
    /// Go's `started` channel.
    pub started: Event,
    /// Go's `stopped` channel.
    pub stopped: Event,
    /// `Start` outcomes; the default is a plain return.
    pub on_start: Expectations<()>,
    /// `Stop` outcomes.
    pub on_stop: Expectations<()>,
    /// `OnPreSchedEvent` outcomes.
    pub on_pre_sched_event: Expectations<PreSchedEventResult>,
    /// `OnSchedEvent` outcomes.
    pub on_sched_event: Expectations<()>,
    /// The events `OnPreSchedEvent` saw, in order.
    pub pre_sched_events: Mutex<Vec<(String, TimerRecord)>>,
    /// The events `OnSchedEvent` saw, in order.
    pub sched_events: Mutex<Vec<(String, TimerRecord)>>,
    /// Fired every time `OnSchedEvent` returns.
    pub sched_done: Mutex<Option<Arc<Event>>>,
}

impl MockHook {
    /// Go `newMockHook`.
    pub fn new() -> Arc<Self> {
        let hook = Arc::new(Self::default());
        hook.on_start.always(Outcome::Ok(()));
        hook.on_stop.always(Outcome::Ok(()));
        hook
    }

    /// Replaces the "on sched event returned" signal.
    pub fn set_sched_done(&self, event: Arc<Event>) {
        *self.sched_done.lock().unwrap() = Some(event);
    }

    /// testify `AssertExpectations`.
    pub fn assert_expectations(&self) {
        self.on_start.assert_drained("Start");
        self.on_stop.assert_drained("Stop");
        self.on_pre_sched_event.assert_drained("OnPreSchedEvent");
        self.on_sched_event.assert_drained("OnSchedEvent");
    }
}

impl Hook for MockHook {
    fn start(&self) {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = self.on_start.call("Start");
        }));
        self.started.signal();
        if let Err(payload) = result {
            std::panic::resume_unwind(payload);
        }
    }

    fn stop(&self) {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = self.on_stop.call("Stop");
        }));
        self.stopped.signal();
        if let Err(payload) = result {
            std::panic::resume_unwind(payload);
        }
    }

    fn on_pre_sched_event(
        &self,
        _ctx: &Context,
        event: &dyn TimerShedEvent,
    ) -> Result<PreSchedEventResult> {
        self.pre_sched_events
            .lock()
            .unwrap()
            .push((event.event_id().to_string(), event.timer().clone_record()));
        self.on_pre_sched_event.call("OnPreSchedEvent")
    }

    fn on_sched_event(&self, _ctx: &Context, event: &dyn TimerShedEvent) -> Result<()> {
        self.sched_events
            .lock()
            .unwrap()
            .push((event.event_id().to_string(), event.timer().clone_record()));
        let result = self.on_sched_event.call("OnSchedEvent");
        if let Some(done) = self.sched_done.lock().unwrap().as_ref() {
            done.signal();
        }
        result
    }
}

/// The shape of the condition a store method was handed, for the assertions
/// Go performs with type switches inside its `mock.Arguments` `Run` hooks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CondShape {
    /// An `api.TimerCond`.
    Timer(TimerCond),
    /// An `api.Operator`.
    Op {
        /// Go `Op`.
        op: OperatorTp,
        /// Go `Not`.
        not: bool,
        /// Go `Children`.
        children: Vec<CondShape>,
    },
    /// Anything else.
    Other,
}

/// Records a condition tree for later inspection.
pub fn cond_shape(cond: &dyn Cond) -> CondShape {
    if let Some(timer_cond) = cond.as_timer_cond() {
        return CondShape::Timer(timer_cond.clone());
    }
    if let Some(operator) = cond.as_operator() {
        return CondShape::Op {
            op: operator.op,
            not: operator.not,
            children: operator
                .children
                .iter()
                .map(|child| cond_shape(child.as_ref()))
                .collect(),
        };
    }
    CondShape::Other
}

/// Go `mockStoreCore`.
#[derive(Default)]
pub struct MockStoreCore {
    /// `Create` outcomes.
    pub on_create: Expectations<String>,
    /// `List` outcomes.
    pub on_list: Expectations<Vec<TimerRecord>>,
    /// `Update` outcomes.
    pub on_update: Expectations<()>,
    /// `Delete` outcomes.
    pub on_delete: Expectations<bool>,
    /// `WatchSupported` outcomes.
    pub on_watch_supported: Expectations<bool>,
    /// The conditions `List` was called with, in order.
    pub list_conds: Mutex<Vec<Option<CondShape>>>,
    /// The `(timerID, update)` pairs `Update` was called with, in order.
    pub updates: Mutex<Vec<(String, TimerUpdate)>>,
    /// Queued `Watch` return channels.
    pub watch_channels: Mutex<VecDeque<WatchTimerChan>>,
    /// Fires each time `Watch` is called; the payload is the call index.
    pub watch_calls: AtomicUsize,
    /// An optional signal fired on each `Watch` call.
    pub watch_signal: Mutex<Option<Arc<Event>>>,
    /// The `Instant` of each `Watch` call.
    pub watch_times: Mutex<Vec<std::time::Instant>>,
}

impl MockStoreCore {
    /// Go `newMockStore`.
    pub fn new() -> (Arc<Self>, TimerStore) {
        let core = Arc::new(Self::default());
        let store = TimerStore::new(Arc::clone(&core) as Arc<dyn TimerStoreCore>);
        (core, store)
    }

    /// testify `AssertExpectations`.
    pub fn assert_expectations(&self) {
        self.on_create.assert_drained("Create");
        self.on_list.assert_drained("List");
        self.on_update.assert_drained("Update");
        self.on_delete.assert_drained("Delete");
        self.on_watch_supported.assert_drained("WatchSupported");
    }

    /// Queues a channel for the next `Watch` call.
    pub fn queue_watch(&self, channel: WatchTimerChan) {
        self.watch_channels.lock().unwrap().push_back(channel);
    }
}

impl TimerStoreCore for MockStoreCore {
    fn create(&self, _ctx: &Context, _record: &TimerRecord) -> Result<String> {
        self.on_create.call("Create")
    }

    fn list(&self, _ctx: &Context, cond: Option<&dyn Cond>) -> Result<Vec<TimerRecord>> {
        self.list_conds.lock().unwrap().push(cond.map(cond_shape));
        self.on_list.call("List")
    }

    fn update(&self, _ctx: &Context, timer_id: &str, update: &TimerUpdate) -> Result<()> {
        self.updates
            .lock()
            .unwrap()
            .push((timer_id.to_string(), update.clone()));
        self.on_update.call("Update")
    }

    fn delete(&self, _ctx: &Context, _timer_id: &str) -> Result<bool> {
        self.on_delete.call("Delete")
    }

    fn watch_supported(&self) -> bool {
        self.on_watch_supported
            .call("WatchSupported")
            .unwrap_or(false)
    }

    fn watch(&self, _ctx: &Context) -> WatchTimerChan {
        self.watch_calls.fetch_add(1, Ordering::SeqCst);
        self.watch_times
            .lock()
            .unwrap()
            .push(std::time::Instant::now());
        let channel = self
            .watch_channels
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| std::sync::mpsc::sync_channel(1).1);
        if let Some(signal) = self.watch_signal.lock().unwrap().as_ref() {
            signal.signal();
        }
        channel
    }

    fn close(&self) {}
}
