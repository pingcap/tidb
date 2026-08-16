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

//! Transcreation of Go `pkg/timer/runtime/cache.go`: the runtime's view of the
//! timers it manages, kept sorted by the moment each should next be examined.

use std::collections::{HashMap, HashSet};

use tidb_util::timeutil::{zone, TimeZone};

use crate::go_time::GoTime;
use crate::timer::{SchedEventStatus, TimerRecord};

use super::NowFunc;

/// Go `runtimeProcStatus`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RuntimeProcStatus {
    /// Go `procIdle`.
    #[default]
    Idle,
    /// Go `procTriggering`.
    Triggering,
    /// Go `procWaitTriggerClose`.
    WaitTriggerClose,
}

/// The sentinel Go writes as `time.Date(2999, 1, 1, 0, 0, 0, 0, time.UTC)`:
/// "far enough in the future that this timer is never up".
pub fn never_try_trigger_time() -> GoTime {
    GoTime::date(2999, 1, 1, 0, 0, 0, 0, &TimeZone::Named(chrono_tz::Tz::UTC))
}

/// Go `timerCacheItem`.
///
/// Go threads the item into a `container/list` and remembers its element in
/// `sortEle`. The Rust cache keeps the sort order as a `Vec` of timer ids
/// instead (see [`TimersCache::sorted`]), so there is no element handle here.
#[derive(Debug, Clone)]
pub struct TimerCacheItem {
    /// Go `timer`; `None` before the first `update`.
    pub timer: Option<TimerRecord>,
    /// Go `nextEventTime`.
    pub next_event_time: Option<GoTime>,
    /// Go `nextTryTriggerTime`.
    pub next_try_trigger_time: GoTime,
    /// Go `procStatus`.
    pub proc_status: RuntimeProcStatus,
    /// Go `triggerEventID`.
    pub trigger_event_id: String,
}

impl Default for TimerCacheItem {
    fn default() -> Self {
        Self {
            timer: None,
            next_event_time: None,
            next_try_trigger_time: never_try_trigger_time(),
            proc_status: RuntimeProcStatus::Idle,
            trigger_event_id: String::new(),
        }
    }
}

impl TimerCacheItem {
    /// Go `(*timerCacheItem).update`.
    pub fn update(&mut self, timer: &TimerRecord, now_func: &NowFunc) -> bool {
        if let Some(current) = &self.timer {
            if timer.version < current.version {
                return false;
            }

            if timer.version == current.version
                && !location_changed(timer.location.as_ref(), current.location.as_ref())
            {
                return false;
            }
        }

        let timer = timer.clone_record();
        self.next_event_time = None;
        self.next_try_trigger_time = never_try_trigger_time();

        if timer.spec.enable {
            if let Ok((time, true)) = timer.next_event_time() {
                self.next_event_time = Some(time);
            }

            if timer.is_manual_requesting() {
                self.next_event_time = Some(now_func());
            }
        }

        match timer.event_status.as_str() {
            SchedEventStatus::IDLE => {
                if let Some(next) = &self.next_event_time {
                    self.next_try_trigger_time = next.clone();
                }
            }
            SchedEventStatus::TRIGGER => {
                self.next_try_trigger_time = timer.event_start.clone();
            }
            _ => {}
        }

        self.timer = Some(timer);
        true
    }
}

/// Go `timersCache`.
pub struct TimersCache {
    /// Go `items`.
    pub items: HashMap<String, TimerCacheItem>,
    /// Go `sorted`, a `container/list` ordered by `nextTryTriggerTime`.
    ///
    /// Rust has no intrusive list, so the order is a `Vec` of timer ids and
    /// [`TimersCache::resort`] reproduces `MoveBefore`/`MoveAfter` exactly by
    /// removing and re-inserting at the index Go's scan would land on.
    pub sorted: Vec<String>,
    /// Go `waitCloseTimerIDs`.
    pub wait_close_timer_ids: HashSet<String>,
    /// Go `nowFunc`.
    pub now_func: NowFunc,
}

impl TimersCache {
    /// Go `newTimersCache`.
    pub fn new() -> Self {
        Self {
            items: HashMap::new(),
            sorted: Vec::new(),
            wait_close_timer_ids: HashSet::new(),
            now_func: super::default_now_func(),
        }
    }

    /// Go `(*timersCache).updateTimer`.
    pub fn update_timer(&mut self, timer: &TimerRecord) -> bool {
        let now_func = self.now_func.clone();
        let item = self.items.entry(timer.id.clone()).or_default();
        let change = item.update(timer, &now_func);
        if change {
            self.resort(&timer.id);
        }

        let reset = {
            let item = &self.items[&timer.id];
            item.proc_status == RuntimeProcStatus::WaitTriggerClose
                && item.trigger_event_id != timer.event_id
        };
        if reset {
            self.set_timer_proc_status(&timer.id, RuntimeProcStatus::Idle, "");
        }

        change
    }

    /// Go `(*timersCache).removeTimer`.
    pub fn remove_timer(&mut self, timer_id: &str) -> bool {
        if self.items.remove(timer_id).is_none() {
            return false;
        }
        self.sorted.retain(|id| id != timer_id);
        self.wait_close_timer_ids.remove(timer_id);
        true
    }

    /// Go `(*timersCache).hasTimer`.
    pub fn has_timer(&self, timer_id: &str) -> bool {
        self.items.contains_key(timer_id)
    }

    /// Go `(*timersCache).partialBatchUpdateTimers`.
    pub fn partial_batch_update_timers(&mut self, timers: &[TimerRecord]) -> bool {
        let mut change = false;
        for timer in timers {
            if self.update_timer(timer) {
                change = true;
            }
        }
        change
    }

    /// Go `(*timersCache).fullUpdateTimers`.
    pub fn full_update_timers(&mut self, timers: &[TimerRecord]) {
        let present: HashSet<&str> = timers.iter().map(|timer| timer.id.as_str()).collect();
        let stale: Vec<String> = self
            .items
            .keys()
            .filter(|id| !present.contains(id.as_str()))
            .cloned()
            .collect();
        for id in stale {
            self.remove_timer(&id);
        }
        self.partial_batch_update_timers(timers);
    }

    /// Go `(*timersCache).setTimerProcStatus`.
    pub fn set_timer_proc_status(
        &mut self,
        timer_id: &str,
        status: RuntimeProcStatus,
        trigger_event_id: &str,
    ) {
        let Some(item) = self.items.get_mut(timer_id) else {
            return;
        };
        item.proc_status = status;
        item.trigger_event_id = trigger_event_id.to_string();
        if status == RuntimeProcStatus::WaitTriggerClose {
            self.wait_close_timer_ids.insert(timer_id.to_string());
        } else {
            self.wait_close_timer_ids.remove(timer_id);
        }
    }

    /// Go `(*timersCache).updateNextTryTriggerTime`.
    pub fn update_next_try_trigger_time(&mut self, timer_id: &str, time: GoTime) {
        let Some(item) = self.items.get(timer_id) else {
            return;
        };

        // to make sure try trigger time is always after next event time
        let idle = item
            .timer
            .as_ref()
            .is_some_and(|timer| timer.event_status.as_str() == SchedEventStatus::IDLE);
        if idle
            && item
                .next_event_time
                .as_ref()
                .is_none_or(|next| time.before(next))
        {
            return;
        }

        self.items
            .get_mut(timer_id)
            .expect("item was just read")
            .next_try_trigger_time = time;
        self.resort(timer_id);
    }

    /// Go `(*timersCache).iterTryTriggerTimers`.
    pub fn iter_try_trigger_timers(
        &self,
        mut callback: impl FnMut(&TimerRecord, &GoTime, Option<&GoTime>) -> bool,
    ) {
        for id in &self.sorted {
            let Some(item) = self.items.get(id) else {
                continue;
            };
            if item.proc_status != RuntimeProcStatus::Idle {
                continue;
            }
            let Some(timer) = &item.timer else {
                continue;
            };
            if !callback(
                timer,
                &item.next_try_trigger_time,
                item.next_event_time.as_ref(),
            ) {
                break;
            }
        }
    }

    /// Go `(*timersCache).resort`.
    pub fn resort(&mut self, timer_id: &str) {
        let position = match self.sorted.iter().position(|id| id == timer_id) {
            Some(position) => position,
            None => {
                self.sorted.push(timer_id.to_string());
                self.sorted.len() - 1
            }
        };

        let next_trigger = self.items[timer_id].next_try_trigger_time.clone();

        if position > 0 && self.trigger_at(position - 1).after(&next_trigger) {
            let mut cursor = position - 1;
            while cursor > 0 && self.trigger_at(cursor - 1).after(&next_trigger) {
                cursor -= 1;
            }
            let id = self.sorted.remove(position);
            self.sorted.insert(cursor, id);
            return;
        }

        let last = self.sorted.len() - 1;
        if position < last && self.trigger_at(position + 1).before(&next_trigger) {
            let mut cursor = position + 1;
            while cursor < last && self.trigger_at(cursor + 1).before(&next_trigger) {
                cursor += 1;
            }
            let id = self.sorted.remove(position);
            self.sorted.insert(cursor, id);
        }
    }

    fn trigger_at(&self, index: usize) -> GoTime {
        self.items[&self.sorted[index]]
            .next_try_trigger_time
            .clone()
    }
}

impl Default for TimersCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Go `locationChanged`.
///
/// Go short-circuits on `a == b` pointer identity before comparing offsets;
/// `tidb_util::timeutil::TimeZone` is a value, so only the offset comparison
/// remains. The upstream test asserts exactly that behavior: two separately
/// loaded `America/New_York` locations, and two differently named fixed zones
/// with the same offset, both count as unchanged.
pub fn location_changed(a: Option<&TimeZone>, b: Option<&TimeZone>) -> bool {
    match (a, b) {
        (None, None) => false,
        (None, Some(_)) | (Some(_), None) => true,
        (Some(a), Some(b)) => zone(a).1 != zone(b).1,
    }
}
