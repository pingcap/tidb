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

use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use tidb_timeutil::{zone, TimeZone};

use tidb_timer::go_time::GoTime;
use tidb_timer::timer::{SchedEventStatus, TimerRecord};

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
/// Go remembers the list element in `sortEle`. Rust verifies the item's
/// last known index against [`TimersCache::sorted`] before using it.
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
    sort_index: usize,
    present_on_refresh: bool,
}

impl Default for TimerCacheItem {
    fn default() -> Self {
        Self {
            timer: None,
            next_event_time: None,
            next_try_trigger_time: never_try_trigger_time(),
            proc_status: RuntimeProcStatus::Idle,
            trigger_event_id: String::new(),
            sort_index: 0,
            present_on_refresh: false,
        }
    }
}

impl TimerCacheItem {
    fn from_record(timer: TimerRecord, now_func: &NowFunc) -> Self {
        let (next_event_time, next_try_trigger_time) = Self::trigger_times(&timer, now_func);
        Self {
            timer: Some(timer),
            next_event_time,
            next_try_trigger_time,
            proc_status: RuntimeProcStatus::Idle,
            trigger_event_id: String::new(),
            sort_index: 0,
            present_on_refresh: false,
        }
    }

    /// Go `(*timerCacheItem).update`.
    pub fn update(&mut self, timer: &TimerRecord, now_func: &NowFunc) -> bool {
        if !self.should_replace(timer) {
            return false;
        }
        self.replace_record(timer.clone_record(), now_func);
        true
    }

    fn should_replace(&self, timer: &TimerRecord) -> bool {
        self.timer.as_ref().is_none_or(|current| {
            timer.version > current.version
                || (timer.version == current.version
                    && location_changed(timer.location.as_ref(), current.location.as_ref()))
        })
    }

    fn event_changed_while_waiting(&self, timer: &TimerRecord) -> bool {
        self.proc_status == RuntimeProcStatus::WaitTriggerClose
            && self.trigger_event_id != timer.event_id
    }

    fn trigger_times(timer: &TimerRecord, now_func: &NowFunc) -> (Option<GoTime>, GoTime) {
        let mut next_event_time = None;
        if timer.spec.enable {
            if let Ok((time, true)) = timer.next_event_time() {
                next_event_time = Some(time);
            }

            if timer.is_manual_requesting() {
                next_event_time = Some(now_func());
            }
        }

        let next_try = match timer.event_status.as_str() {
            SchedEventStatus::IDLE => next_event_time
                .clone()
                .unwrap_or_else(never_try_trigger_time),
            SchedEventStatus::TRIGGER => timer.event_start.clone(),
            _ => never_try_trigger_time(),
        };
        (next_event_time, next_try)
    }

    fn replace_record(&mut self, timer: TimerRecord, now_func: &NowFunc) -> DateTime<Utc> {
        let (next_event_time, next_try) = Self::trigger_times(&timer, now_func);
        let previous = self.next_try_trigger_time.instant();
        self.next_event_time = next_event_time;
        self.timer = Some(timer);
        self.next_try_trigger_time = next_try;
        previous
    }
}

/// Go `timersCache`.
pub struct TimersCache {
    /// Go `items`.
    pub items: HashMap<String, TimerCacheItem>,
    /// Go `sorted`, a `container/list` ordered by `nextTryTriggerTime`.
    ///
    /// Rust keeps timer IDs in a vector. [`TimersCache::resort`] uses the same
    /// insertion positions as Go's `MoveBefore` and `MoveAfter`.
    pub sorted: Vec<String>,
    /// Go `waitCloseTimerIDs`.
    pub wait_close_timer_ids: HashSet<String>,
    /// Go `nowFunc`.
    pub now_func: NowFunc,
    sort_shift: isize,
}

impl TimersCache {
    /// Go `newTimersCache`.
    pub fn new() -> Self {
        Self {
            items: HashMap::new(),
            sorted: Vec::new(),
            wait_close_timer_ids: HashSet::new(),
            now_func: super::default_now_func(),
            sort_shift: 0,
        }
    }

    /// Go `(*timersCache).updateTimer`.
    pub fn update_timer(&mut self, timer: &TimerRecord) -> bool {
        self.update_record(Cow::Borrowed(timer))
    }

    fn update_record(&mut self, timer: Cow<'_, TimerRecord>) -> bool {
        let Some(item) = self.items.get_mut(&timer.id) else {
            self.insert_record(timer.into_owned());
            return true;
        };
        let reset = item.event_changed_while_waiting(&timer);
        if !item.should_replace(&timer) {
            if reset {
                self.set_timer_proc_status(&timer.id, RuntimeProcStatus::Idle, "");
            }
            return false;
        }

        let id = match &timer {
            Cow::Borrowed(timer) => Cow::Borrowed(timer.id.as_str()),
            Cow::Owned(timer) => Cow::Owned(timer.id.clone()),
        };
        let sort_index = item.sort_index;
        let previous = item.replace_record(timer.into_owned(), &self.now_func);
        let next = item.next_try_trigger_time.instant();
        self.resort_with_index(&id, sort_index, Some((previous, next)));
        if reset {
            self.set_timer_proc_status(&id, RuntimeProcStatus::Idle, "");
        }
        true
    }

    fn insert_record(&mut self, timer: TimerRecord) {
        let id = timer.id.clone();
        let mut item = TimerCacheItem::from_record(timer, &self.now_func);
        let comes_after = |id: &String| {
            self.items[id]
                .next_try_trigger_time
                .after(&item.next_try_trigger_time)
        };
        let sort_index = if self.sorted.last().is_some_and(comes_after) {
            self.sorted.partition_point(|id| !comes_after(id))
        } else {
            self.sorted.len()
        };
        item.sort_index = sort_index;
        self.items.insert(id.clone(), item);
        self.sorted.insert(sort_index, id);
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
        self.update_records(timers.iter().map(Cow::Borrowed))
    }

    pub(crate) fn partial_batch_update_owned_timers(&mut self, timers: Vec<TimerRecord>) -> bool {
        self.update_records(timers.into_iter().map(Cow::Owned))
    }

    fn update_records<'a>(
        &mut self,
        timers: impl ExactSizeIterator<Item = Cow<'a, TimerRecord>>,
    ) -> bool {
        if self.items.is_empty() {
            if self.sorted.is_empty() && timers.len() > 1 {
                return self.load_initial_records(timers);
            }
            self.items.reserve(timers.len());
            self.sorted.reserve(timers.len());
        }
        let mut change = false;
        for timer in timers {
            change |= self.update_record(timer);
        }
        change
    }

    fn load_initial_records<'a>(
        &mut self,
        mut timers: impl ExactSizeIterator<Item = Cow<'a, TimerRecord>>,
    ) -> bool {
        let mut items = std::mem::take(&mut self.items);
        let mut sorted = std::mem::take(&mut self.sorted);
        items.reserve(timers.len());
        sorted.reserve(timers.len());

        // Go inserts new timers after existing timers at the same instant.
        // A repeated ID can move an existing timer in either direction, so
        // publish the ordered prefix before processing that update.
        let mut repeated = None;
        let mut previous = None;
        let mut ordered = true;
        for timer in &mut timers {
            let Entry::Vacant(slot) = items.entry(timer.id.clone()) else {
                repeated = Some(timer);
                break;
            };
            let mut item = TimerCacheItem::from_record(timer.into_owned(), &self.now_func);
            let instant = item.next_try_trigger_time.instant();
            ordered &= previous.is_none_or(|previous| previous <= instant);
            previous = Some(instant);
            item.sort_index = sorted.len();
            sorted.push(slot.key().clone());
            slot.insert(item);
        }

        if !ordered {
            sorted.sort_by_cached_key(|id| items[id].next_try_trigger_time.instant());
            for (index, id) in sorted.iter().enumerate() {
                items
                    .get_mut(id)
                    .expect("sorted timer is in the cache")
                    .sort_index = index;
            }
        }
        self.items = items;
        self.sorted = sorted;
        for timer in repeated.into_iter().chain(timers) {
            self.update_record(timer);
        }
        true
    }

    /// Go `(*timersCache).fullUpdateTimers`.
    pub fn full_update_timers(&mut self, timers: &[TimerRecord]) {
        if self.prepare_refresh(timers) {
            self.partial_batch_update_timers(timers);
        }
    }

    pub(crate) fn full_update_owned_timers(&mut self, timers: Vec<TimerRecord>) {
        if self.prepare_refresh(&timers) {
            self.partial_batch_update_owned_timers(timers);
        }
    }

    /// Remove absent records and report whether the incoming records can
    /// change the cache. When they can, apply them in the source order.
    fn prepare_refresh(&mut self, timers: &[TimerRecord]) -> bool {
        if self.items.is_empty() {
            return !timers.is_empty();
        }
        if timers.is_empty() {
            let orphaned = |id: &String| !self.items.contains_key(id);
            // Verified positions for every map key account for the entire
            // sorted vector when both collections have the same length.
            if self.sorted.len() == self.items.len()
                && self.items.iter().all(|(id, item)| self.sorted.get(item.sort_index) == Some(id))
            {
                self.sorted.clear();
            } else {
                self.sorted.retain(orphaned);
            }
            self.wait_close_timer_ids.retain(orphaned);
            self.items.clear();
            return false;
        }
        let mut present = 0;
        let mut update = false;
        for timer in timers {
            if let Some(item) = self.items.get_mut(timer.id.as_str()) {
                present += usize::from(!std::mem::replace(&mut item.present_on_refresh, true));
                // Equal versions in different locations may need an offset
                // comparison. Leave that decision to the ordinary update.
                update = update
                    || item.timer.as_ref().is_none_or(|current| {
                        timer.version > current.version
                            || (timer.version == current.version
                                && timer.location != current.location)
                    })
                    || item.event_changed_while_waiting(timer);
            } else {
                update = true;
            }
        }
        if present < self.items.len() {
            let keep = |id: &String| self.items.get(id).is_none_or(|item| item.present_on_refresh);
            self.sorted.retain(keep);
            self.wait_close_timer_ids.retain(keep);
            self.items.retain(|_, item| std::mem::take(&mut item.present_on_refresh));
        } else {
            for item in self.items.values_mut() {
                item.present_on_refresh = false;
            }
        }
        update
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
    #[inline]
    pub fn update_next_try_trigger_time(&mut self, timer_id: &str, time: GoTime) {
        let Some(item) = self.items.get_mut(timer_id) else {
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

        let previous = item.next_try_trigger_time.instant();
        let next = time.instant();
        item.next_try_trigger_time = time;
        let sort_index = item.sort_index;
        self.resort_with_index(timer_id, sort_index, Some((previous, next)));
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
        self.resort_with_index(timer_id, self.items[timer_id].sort_index, None);
    }

    #[inline]
    fn resort_with_index(
        &mut self,
        timer_id: &str,
        sort_index: usize,
        change: Option<(DateTime<Utc>, DateTime<Utc>)>,
    ) {
        let matches = |index| self.sorted.get(index).is_some_and(|id| id == timer_id);
        if matches(sort_index) {
            // A verified position at the relevant edge cannot move farther.
            let direction = change.map(|(previous, next)| next.cmp(&previous));
            let must_check_neighbors = match direction {
                Some(Ordering::Equal) => false,
                Some(Ordering::Less) => sort_index != 0,
                Some(Ordering::Greater) => sort_index + 1 != self.sorted.len(),
                None => true,
            };
            if must_check_neighbors {
                let next = change.map_or_else(
                    || self.items[timer_id].next_try_trigger_time.instant(),
                    |(_, next)| next,
                );
                self.resort_at(timer_id, sort_index, sort_index, next, direction);
            }
            return;
        }

        // A move can shift a contiguous range by the same amount. Reuse the
        // last observed shift and check the ID before trusting the position.
        let position = if self.sorted.last().is_some_and(|id| id == timer_id) {
            Some(self.sorted.len() - 1)
        } else if self.sorted.first().is_some_and(|id| id == timer_id) {
            Some(0)
        } else {
            sort_index
                .checked_add_signed(self.sort_shift)
                .and_then(|index| {
                    [index, index.saturating_add(1), index.saturating_sub(1)]
                        .into_iter()
                        .find(|&candidate| matches(candidate))
                })
        }
        .or_else(|| {
            if let Some((previous, _)) = change {
                self.find_previous_position(timer_id, previous)
            } else {
                self.sorted.iter().position(|id| id == timer_id)
            }
        });
        let direction = position.and_then(|_| change.map(|(previous, next)| next.cmp(&previous)));
        let position = if let Some(position) = position {
            self.sort_shift = position as isize - sort_index as isize;
            position
        } else {
            self.sorted.push(timer_id.to_owned());
            self.sorted.len() - 1
        };
        let next = change.map_or_else(
            || self.items[timer_id].next_try_trigger_time.instant(),
            |(_, next)| next,
        );
        self.resort_at(timer_id, position, sort_index, next, direction);
    }

    fn find_previous_position(&self, timer_id: &str, previous: DateTime<Utc>) -> Option<usize> {
        // The changed timer is still in its old position. Use its old time
        // when finding the range of timers that shared that time.
        let start = self.sorted.partition_point(|id| {
            id != timer_id && self.items[id].next_try_trigger_time.instant() < previous
        });
        if self.sorted.get(start).is_some_and(|id| id == timer_id) {
            return Some(start);
        }
        let len = self.sorted[start..].partition_point(|id| {
            id == timer_id || self.items[id].next_try_trigger_time.instant() <= previous
        });
        self.sorted[start..start + len]
            .iter()
            .position(|id| id == timer_id)
            .map(|index| start + index)
    }

    #[inline]
    fn resort_at(
        &mut self,
        timer_id: &str,
        position: usize,
        sort_index: usize,
        next_trigger: DateTime<Utc>,
        direction: Option<Ordering>,
    ) {
        let trigger_at = |id: &String| self.items[id].next_try_trigger_time.instant();
        if position == 0
            && direction == Some(Ordering::Greater)
            && self.sorted.len() > 1
            && trigger_at(&self.sorted[self.sorted.len() - 1]) < next_trigger
        {
            let id = self.sorted.remove(0);
            self.sorted.push(id);
            return;
        }

        // The slices on either side of the changed timer remain sorted.
        let destination = if direction != Some(Ordering::Greater)
            && position > 0
            && trigger_at(&self.sorted[position - 1]) > next_trigger
        {
            let destination = if position == 1 || trigger_at(&self.sorted[0]) > next_trigger {
                0
            } else {
                self.sorted[..position - 1].partition_point(|id| trigger_at(id) <= next_trigger)
            };
            self.sorted[destination..=position].rotate_right(1);
            destination
        } else if direction != Some(Ordering::Less)
            && position + 1 < self.sorted.len()
            && trigger_at(&self.sorted[position + 1]) < next_trigger
        {
            let last = self.sorted.len() - 1;
            let destination = if position + 1 == last || trigger_at(&self.sorted[last]) < next_trigger {
                last
            } else {
                position + 1
                    + self.sorted[position + 2..last]
                        .partition_point(|id| trigger_at(id) < next_trigger)
            };
            self.sorted[position..=destination].rotate_left(1);
            destination
        } else {
            position
        };
        // An endpoint has a direct lookup even when its hint is stale.
        // Avoid rehashing the moved item just to update such a hint.
        if sort_index != destination && destination != 0 && destination + 1 != self.sorted.len() {
            self.items
                .get_mut(timer_id)
                .expect("sorted timer is in the cache")
                .sort_index = destination;
        }
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
/// `tidb_timeutil::TimeZone` is a value, so only the offset comparison
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

#[cfg(test)]
mod tests {
    use super::*;

    fn timer(id: &str, version: u64, trigger: i64) -> TimerRecord {
        TimerRecord {
            id: id.into(),
            version,
            event_status: SchedEventStatus::trigger(),
            event_start: GoTime::from_unix(trigger, 0),
            ..Default::default()
        }
    }

    #[test]
    fn owned_refresh_preserves_equal_time_order_and_stale_event_reset() {
        let mut cache = TimersCache::new();
        let mut first = timer("first", 2, 0);
        first.event_data = vec![42; 1024];
        let data = first.event_data.as_ptr();
        cache.full_update_owned_timers(vec![
            timer("late", 1, 10),
            first,
            timer("second", 1, 0),
            timer("late", 2, 0),
            timer("last", 1, 10),
        ]);
        assert_eq!(cache.sorted, ["first", "second", "late", "last"]);
        let installed = cache.items["first"].timer.as_ref().unwrap();
        assert_eq!(installed.event_data.as_ptr(), data);

        cache.set_timer_proc_status("first", RuntimeProcStatus::WaitTriggerClose, "old");
        assert!(!cache.partial_batch_update_owned_timers(vec![timer("first", 1, 10)]));
        assert_eq!(cache.items["first"].timer.as_ref().unwrap().version, 2);
        assert_eq!(cache.items["first"].proc_status, RuntimeProcStatus::Idle);
        assert!(cache.wait_close_timer_ids.is_empty());
        assert_eq!(cache.sorted, ["first", "second", "late", "last"]);
    }

    #[test]
    fn full_refresh_preserves_orphaned_indexes_and_ignores_duplicate_membership() {
        let mut cache = TimersCache::new();
        let kept = timer("kept", 1, 0);
        let removed = timer("removed", 1, 1);
        cache.full_update_timers(&[kept.clone(), removed]);
        cache.set_timer_proc_status("removed", RuntimeProcStatus::WaitTriggerClose, "event");
        cache.full_update_timers(&[kept.clone(), kept]);
        assert_eq!(cache.sorted, ["kept"]);
        assert_eq!(cache.items.len(), 1);
        assert!(cache.wait_close_timer_ids.is_empty());

        cache.sorted[0] = "orphan".into();
        cache.wait_close_timer_ids.insert("orphan".into());
        cache.full_update_timers(&[]);
        assert!(cache.items.is_empty());
        assert_eq!(cache.sorted, ["orphan"]);
        assert!(cache.wait_close_timer_ids.contains("orphan"));
    }

    #[test]
    fn initial_batch_keeps_trigger_order_and_positions() {
        let mut cache = TimersCache::new();
        let early = GoTime::from_unix(0, 1);
        let middle = GoTime::from_unix(0, 2);
        let mut updated = timer("first", 2, 0);
        updated.event_start = middle.clone();
        let mut tied = timer("tied", 1, 0);
        tied.event_start = middle;
        let mut start = timer("start", 1, 0);
        start.event_start = early;
        let timers = [
            timer("first", 1, 2),
            tied,
            start,
            updated,
            timer("last", 1, 1),
        ];

        assert!(cache.partial_batch_update_timers(&timers));
        assert_eq!(cache.sorted, ["start", "tied", "first", "last"]);
        cache.update_next_try_trigger_time("start", GoTime::from_unix(0, 2));
        assert_eq!(cache.sorted, ["start", "tied", "first", "last"]);
        cache.update_next_try_trigger_time("tied", GoTime::from_unix(3, 0));
        assert_eq!(cache.sorted, ["start", "first", "last", "tied"]);
    }
}
