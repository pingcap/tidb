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

//! Transcreation of Go `pkg/timer/api/hook.go`: how a user tells the framework
//! to trigger an event.

use std::sync::Arc;

use crate::client::TimerClient;
use crate::error::Result;
use crate::store::Context;
use crate::timer::TimerRecord;

/// Go `TimerShedEvent`: the current schedule event's information.
pub trait TimerShedEvent: Send + Sync {
    /// Go `EventID`.
    fn event_id(&self) -> &str;
    /// Go `Timer`.
    fn timer(&self) -> &TimerRecord;
}

/// Go `PreSchedEventResult`: the result of `OnPreSchedEvent`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PreSchedEventResult {
    /// Go `Delay`, in nanoseconds. Zero means `OnSchedEvent` runs next;
    /// otherwise `OnPreSchedEvent` is re-consulted after the delay.
    pub delay: i64,
    /// Go `EventData`: data handed to the event that will be triggered.
    pub event_data: Vec<u8>,
}

/// Go `Hook`: implemented by the user to tell the framework how to trigger an
/// event. Several timers sharing a hook class share one hook in a runtime.
pub trait Hook: Send + Sync {
    /// Go `Start`.
    fn start(&self);
    /// Go `Stop`, called when the framework is shutting down.
    fn stop(&self);
    /// Go `OnPreSchedEvent`, called before a new event is triggered; its result
    /// decides the next action. `event.timer().event_id` is empty here because
    /// the event has not actually been triggered — use `event.event_id()`.
    fn on_pre_sched_event(
        &self,
        ctx: &Context,
        event: &dyn TimerShedEvent,
    ) -> Result<PreSchedEventResult>;
    /// Go `OnSchedEvent`, called when a new event is triggered.
    fn on_sched_event(&self, ctx: &Context, event: &dyn TimerShedEvent) -> Result<()>;
}

/// Go `HookFactory`: constructs a new `Hook` for a hook class.
pub type HookFactory = Box<dyn Fn(&str, Arc<dyn TimerClient>) -> Arc<dyn Hook> + Send + Sync>;
