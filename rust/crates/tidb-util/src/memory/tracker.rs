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

//! Transcreation of Go `pkg/util/memory/tracker.go`'s tracker core. See the
//! module doc for the in-progress package scope and documented deferrals
//! (arbitrator hook, metrics gauges, GC-aware release, the
//! `MemUsageTop1Tracker` global that belongs to the arbitrator tier).

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering::SeqCst};
use std::sync::{Arc, Mutex, Weak};

use super::action::ArcAction;
use crate::sqlkiller::SqlKiller;

/// Consumption is buffered until it exceeds this (Go `TrackMemWhenExceeds`,
/// 100MB).
pub const TRACK_MEM_WHEN_EXCEEDS: i64 = 104_857_600;
/// The default memory quota for a query (Go `DefMemQuotaQuery`, 1GB).
pub const DEF_MEM_QUOTA_QUERY: i64 = 1_073_741_824;

/// `LabelForSQLText`.
pub const LABEL_FOR_SQL_TEXT: i64 = -1;
/// `LabelForMemDB`.
pub const LABEL_FOR_MEM_DB: i64 = -28;
/// `LabelForGlobalAnalyzeMemory`.
pub const LABEL_FOR_GLOBAL_ANALYZE_MEMORY: i64 = -25;

const SOFT_SCALE: f64 = 0.8;

const BYTE_SIZE_GB: i64 = 1 << 30;
const BYTE_SIZE_MB: i64 = 1 << 20;
const BYTE_SIZE_KB: i64 = 1 << 10;
const BYTE_SIZE: i64 = 1;

#[derive(Clone, Copy)]
struct BytesLimits {
    hard: i64,
    soft: i64,
}

const UNLIMITED: BytesLimits = BytesLimits { hard: -1, soft: -1 };

fn limits_for(bytes_limit: i64) -> BytesLimits {
    if bytes_limit <= 0 {
        UNLIMITED
    } else {
        BytesLimits {
            hard: bytes_limit,
            soft: (bytes_limit as f64 * SOFT_SCALE) as i64,
        }
    }
}

#[derive(Default)]
struct ActionSlot {
    action: Mutex<Option<ArcAction>>,
}

impl ActionSlot {
    /// Go's `tryAction`: drop finished links, then fire.
    fn try_action(&self, tracker: &Arc<Tracker>) {
        let action = {
            let mut slot = self.action.lock().unwrap();
            while let Some(a) = slot.clone() {
                if !a.is_finished() {
                    break;
                }
                *slot = a.get_fallback();
            }
            slot.clone()
        };
        if let Some(a) = action {
            a.action(tracker);
        }
    }
}

/// The hierarchical memory tracker (Go `Tracker`). Always used behind an
/// [`Arc`], mirroring Go's `*Tracker`.
pub struct Tracker {
    parent: Mutex<Weak<Tracker>>,
    children: Mutex<HashMap<i64, Vec<Arc<Tracker>>>>,
    action_for_hard_limit: ActionSlot,
    action_for_soft_limit: ActionSlot,
    limits: Mutex<BytesLimits>,
    label: Mutex<i64>,
    bytes_consumed: AtomicI64,
    bytes_released: AtomicI64,
    max_consumed: AtomicI64,
    /// The session this tracker is bound to.
    pub session_id: AtomicU64,
    /// Whether this tracker is the session's root tracker.
    pub is_root_tracker_of_sess: AtomicBool,
    is_global: bool,
    /// The query killer polled during consumption (Go `Killer`).
    pub killer: Arc<SqlKiller>,
}

impl Tracker {
    /// Creates a memory tracker; `bytes_limit <= 0` means no limit (Go
    /// `NewTracker`, whose default hard-limit action is `LogOnExceed`).
    pub fn new(label: i64, bytes_limit: i64) -> Arc<Tracker> {
        Self::new_inner(label, bytes_limit, false)
    }

    /// Creates a global tracker (Go `NewGlobalTracker`): children are not
    /// recorded, avoiding mutex contention on process-wide trackers.
    pub fn new_global(label: i64, bytes_limit: i64) -> Arc<Tracker> {
        Self::new_inner(label, bytes_limit, true)
    }

    fn new_inner(label: i64, bytes_limit: i64, is_global: bool) -> Arc<Tracker> {
        Arc::new(Tracker {
            parent: Mutex::new(Weak::new()),
            children: Mutex::new(HashMap::new()),
            action_for_hard_limit: ActionSlot {
                action: Mutex::new(Some(Arc::new(super::LogOnExceed::default()))),
            },
            action_for_soft_limit: ActionSlot::default(),
            // Go NewTracker stores hard/soft directly even for <=0; SetBytesLimit
            // normalizes. Keep the same: only explicit setters normalize.
            limits: Mutex::new(BytesLimits {
                hard: bytes_limit,
                soft: (bytes_limit as f64 * SOFT_SCALE) as i64,
            }),
            label: Mutex::new(label),
            bytes_consumed: AtomicI64::new(0),
            bytes_released: AtomicI64::new(0),
            max_consumed: AtomicI64::new(0),
            session_id: AtomicU64::new(0),
            is_root_tracker_of_sess: AtomicBool::new(false),
            is_global,
            killer: Arc::new(SqlKiller::default()),
        })
    }

    /// Go `CheckBytesLimit` (test helper).
    pub fn check_bytes_limit(&self, val: i64) -> bool {
        self.limits.lock().unwrap().hard == val
    }

    /// Sets the byte limit; `<= 0` means no limit (Go `SetBytesLimit`).
    pub fn set_bytes_limit(&self, bytes_limit: i64) {
        *self.limits.lock().unwrap() = limits_for(bytes_limit);
    }

    /// The hard byte limit; `<= 0` means no limit (Go `GetBytesLimit`).
    pub fn get_bytes_limit(&self) -> i64 {
        self.limits.lock().unwrap().hard
    }

    /// Whether consumption reached the hard limit (Go `CheckExceed`).
    pub fn check_exceed(&self) -> bool {
        let hard = self.limits.lock().unwrap().hard;
        self.bytes_consumed.load(SeqCst) >= hard && hard > 0
    }

    /// Sets the hard-limit action (Go `SetActionOnExceed`).
    pub fn set_action_on_exceed(&self, a: Option<ArcAction>) {
        *self.action_for_hard_limit.action.lock().unwrap() = a;
    }

    /// Sets a new hard-limit action with the old one as fallback, merged by
    /// priority (Go `FallbackOldAndSetNewAction`).
    pub fn fallback_old_and_set_new_action(&self, a: ArcAction) {
        let mut slot = self.action_for_hard_limit.action.lock().unwrap();
        *slot = rearrange_fallback(Some(a), slot.take());
    }

    /// The soft-limit variant (Go `FallbackOldAndSetNewActionForSoftLimit`).
    pub fn fallback_old_and_set_new_action_for_soft_limit(&self, a: ArcAction) {
        let mut slot = self.action_for_soft_limit.action.lock().unwrap();
        *slot = rearrange_fallback(Some(a), slot.take());
    }

    /// Go `GetFallbackForTest`.
    pub fn get_fallback_for_test(&self, ignore_finished: bool) -> Option<ArcAction> {
        let mut slot = self.action_for_hard_limit.action.lock().unwrap();
        if let Some(a) = slot.clone() {
            if a.is_finished() && ignore_finished {
                *slot = a.get_fallback();
            }
        }
        slot.clone()
    }

    /// Unbinds both actions (Go `UnbindActions`).
    pub fn unbind_actions(&self) {
        *self.action_for_soft_limit.action.lock().unwrap() = None;
        *self.action_for_hard_limit.action.lock().unwrap() = None;
    }

    /// Removes one action from the hard-limit chain by identity (Go
    /// `UnbindActionFromHardLimit`).
    pub fn unbind_action_from_hard_limit(&self, to_unbind: &ArcAction) {
        let mut slot = self.action_for_hard_limit.action.lock().unwrap();
        let mut prev: Option<ArcAction> = None;
        let mut current = slot.clone();
        while let Some(a) = current {
            if Arc::ptr_eq(&a, to_unbind) {
                match &prev {
                    None => *slot = a.get_fallback(),
                    Some(p) => p.set_fallback(a.get_fallback()),
                }
                break;
            }
            prev = Some(a.clone());
            current = a.get_fallback();
        }
    }

    /// The tracker's label (Go `Label`).
    pub fn label(&self) -> i64 {
        *self.label.lock().unwrap()
    }

    /// Re-labels the tracker, re-attaching under its parent (Go `SetLabel`).
    pub fn set_label(self: &Arc<Self>, label: i64) {
        let parent = self.get_parent();
        self.detach();
        *self.label.lock().unwrap() = label;
        if let Some(parent) = parent {
            self.attach_to(&parent);
        }
    }

    /// Attaches this tracker as a child (Go `AttachTo`); consumption bubbles
    /// into the new ancestry, out of the old one.
    pub fn attach_to(self: &Arc<Self>, parent: &Arc<Tracker>) {
        if parent.is_global {
            self.attach_to_global_tracker(parent);
            return;
        }
        if let Some(old) = self.get_parent() {
            old.remove(self);
        }
        parent
            .children
            .lock()
            .unwrap()
            .entry(self.label())
            .or_default()
            .push(Arc::clone(self));
        self.set_parent(Some(parent));
        parent.consume(self.bytes_consumed());
    }

    /// Detaches from the parent (Go `Detach`). A detach from a session root
    /// tracker (except the MemDB child) clears the root's actions and resets
    /// its killer, exactly as the source does at statement end.
    pub fn detach(self: &Arc<Self>) {
        let Some(parent) = self.get_parent() else {
            return;
        };
        if parent.is_global {
            self.detach_from_global_tracker();
            return;
        }
        if parent.is_root_tracker_of_sess.load(SeqCst) && self.label() != LABEL_FOR_MEM_DB {
            parent.unbind_actions();
            parent.killer.reset();
        }
        parent.remove(self);
        self.set_parent(None);
    }

    fn remove(self: &Arc<Self>, old_child: &Arc<Tracker>) {
        let label = old_child.label();
        let mut found = false;
        {
            let mut children = self.children.lock().unwrap();
            if let Some(list) = children.get_mut(&label) {
                if let Some(i) = list.iter().position(|c| Arc::ptr_eq(c, old_child)) {
                    list.remove(i);
                    if list.is_empty() {
                        children.remove(&label);
                    }
                    found = true;
                }
            }
        }
        if found {
            old_child.set_parent(None);
            self.consume(-old_child.bytes_consumed());
        }
    }

    /// Replaces a child in place (Go `ReplaceChild`).
    pub fn replace_child(
        self: &Arc<Self>,
        old_child: &Arc<Tracker>,
        new_child: Option<&Arc<Tracker>>,
    ) {
        let Some(new_child) = new_child else {
            self.remove(old_child);
            return;
        };
        if old_child.label() != new_child.label() {
            self.remove(old_child);
            new_child.attach_to(self);
            return;
        }

        let mut new_consumed = new_child.bytes_consumed();
        new_child.set_parent(Some(self));

        {
            let mut children = self.children.lock().unwrap();
            if let Some(list) = children.get_mut(&old_child.label()) {
                if let Some(i) = list.iter().position(|c| Arc::ptr_eq(c, old_child)) {
                    new_consumed -= old_child.bytes_consumed();
                    old_child.set_parent(None);
                    list[i] = Arc::clone(new_child);
                }
            }
        }
        self.consume(new_consumed);
    }

    /// Consumes (or, negative, releases) memory, bubbling through every
    /// ancestor and firing hard/soft-limit actions on the deepest exceeding
    /// tracker (Go `Consume`). Panics with the kill error when the session
    /// root's killer has a pending signal, as the source does.
    pub fn consume(self: &Arc<Self>, bs: i64) {
        if bs == 0 {
            return;
        }
        let mut root_exceed: Option<Arc<Tracker>> = None;
        let mut root_exceed_soft: Option<Arc<Tracker>> = None;
        let mut session_root: Option<Arc<Tracker>> = None;

        let mut cursor = Some(Arc::clone(self));
        while let Some(tracker) = cursor {
            if tracker.is_root_tracker_of_sess.load(SeqCst) {
                session_root = Some(Arc::clone(&tracker));
            }
            let consumed = tracker.bytes_consumed.fetch_add(bs, SeqCst) + bs;
            let released = tracker.bytes_released.load(SeqCst);
            let limits = *tracker.limits.lock().unwrap();
            if consumed + released >= limits.hard && limits.hard > 0 {
                root_exceed = Some(Arc::clone(&tracker));
            }
            if consumed + released >= limits.soft && limits.soft > 0 {
                root_exceed_soft = Some(Arc::clone(&tracker));
            }
            // maxConsumed CAS loop.
            loop {
                let max_now = tracker.max_consumed.load(SeqCst);
                let consumed_now = tracker.bytes_consumed.load(SeqCst);
                if consumed_now > max_now
                    && tracker
                        .max_consumed
                        .compare_exchange(max_now, consumed_now, SeqCst, SeqCst)
                        .is_err()
                {
                    continue;
                }
                break;
            }
            cursor = tracker.get_parent();
        }

        if bs > 0 {
            if let Some(root) = &session_root {
                if let Some(err) = root.killer.handle_signal() {
                    std::panic::panic_any(err.to_string());
                }
            }
            if let Some(root) = root_exceed {
                root.action_for_hard_limit.try_action(&root);
            }
            if let Some(root) = root_exceed_soft {
                root.action_for_soft_limit.try_action(&root);
            }
        }
    }

    /// Go `HandleKillSignal`.
    pub fn handle_kill_signal(self: &Arc<Self>) {
        let mut session_root: Option<Arc<Tracker>> = None;
        let mut cursor = Some(Arc::clone(self));
        while let Some(tracker) = cursor {
            if tracker.is_root_tracker_of_sess.load(SeqCst) {
                session_root = Some(Arc::clone(&tracker));
            }
            cursor = tracker.get_parent();
        }
        if let Some(root) = session_root {
            if let Some(err) = root.killer.handle_signal() {
                std::panic::panic_any(err.to_string());
            }
        }
    }

    /// Buffers consumption, flushing at `TRACK_MEM_WHEN_EXCEEDS` (Go
    /// `BufferedConsume`; single-threaded by contract).
    pub fn buffered_consume(self: &Arc<Self>, buffered: &mut i64, bytes: i64) {
        *buffered += bytes;
        if *buffered >= TRACK_MEM_WHEN_EXCEEDS {
            self.consume(*buffered);
            *buffered = 0;
        }
    }

    /// Releases tracked memory (Go `Release`). The GC-aware deferred path
    /// (`EnableGCAwareMemoryTrack` + `runtime.SetFinalizer`) is Go-runtime
    /// machinery; this is the default flag-off path.
    pub fn release(self: &Arc<Self>, bytes: i64) {
        if bytes == 0 {
            return;
        }
        self.consume(-bytes);
    }

    /// Go `BufferedRelease`.
    pub fn buffered_release(self: &Arc<Self>, buffered: &mut i64, bytes: i64) {
        *buffered += bytes;
        if *buffered >= TRACK_MEM_WHEN_EXCEEDS {
            self.release(*buffered);
            *buffered = 0;
        }
    }

    /// Consumed bytes (Go `BytesConsumed`).
    pub fn bytes_consumed(&self) -> i64 {
        self.bytes_consumed.load(SeqCst)
    }

    /// Released bytes (Go `BytesReleased`).
    pub fn bytes_released(&self) -> i64 {
        self.bytes_released.load(SeqCst)
    }

    /// Max bytes consumed during execution (Go `MaxConsumed`).
    pub fn max_consumed(&self) -> i64 {
        self.max_consumed.load(SeqCst)
    }

    /// Go `ResetMaxConsumed`.
    pub fn reset_max_consumed(&self) {
        self.max_consumed.store(self.bytes_consumed(), SeqCst);
    }

    /// Go `SearchTrackerWithoutLock` (self or first child with the label).
    pub fn search_tracker(self: &Arc<Self>, label: i64) -> Option<Arc<Tracker>> {
        if self.label() == label {
            return Some(Arc::clone(self));
        }
        self.children
            .lock()
            .unwrap()
            .get(&label)
            .and_then(|list| list.first().cloned())
    }

    /// Go `SearchTrackerConsumedMoreThanNBytes`.
    pub fn search_tracker_consumed_more_than(&self, limit: i64) -> Vec<Arc<Tracker>> {
        let children = self.children.lock().unwrap();
        let mut res = Vec::new();
        for list in children.values() {
            for tracker in list {
                if tracker.bytes_consumed() > limit {
                    res.push(Arc::clone(tracker));
                }
            }
        }
        res
    }

    /// The string representation of the tracker tree (Go `String`).
    pub fn tree_string(&self) -> String {
        let mut buffer = String::from("\n");
        self.to_string_indent("", &mut buffer);
        buffer
    }

    fn to_string_indent(&self, indent: &str, buffer: &mut String) {
        let _ = writeln!(buffer, "{indent}\"{}\"{{", self.label());
        let limit = self.get_bytes_limit();
        if limit > 0 {
            let _ = writeln!(buffer, "{indent}  \"quota\": {}", format_bytes(limit));
        }
        let _ = writeln!(
            buffer,
            "{indent}  \"consumed\": {}",
            format_bytes(self.bytes_consumed())
        );
        let children = self.children.lock().unwrap();
        let mut labels: Vec<i64> = children.keys().copied().collect();
        labels.sort_unstable();
        let deeper = format!("{indent}  ");
        for label in labels {
            for child in &children[&label] {
                child.to_string_indent(&deeper, buffer);
            }
        }
        drop(children);
        buffer.push_str(indent);
        buffer.push_str("}\n");
    }

    /// Whether this tracker consumed less than `other` (Go `LessThan`).
    pub fn less_than(&self, other: &Tracker) -> bool {
        self.bytes_consumed() < other.bytes_consumed()
    }

    /// Go `AttachToGlobalTracker`; panics on a non-global parent like the
    /// source.
    pub fn attach_to_global_tracker(self: &Arc<Self>, global: &Arc<Tracker>) {
        assert!(global.is_global, "Attach to a non-GlobalTracker");
        if let Some(parent) = self.get_parent() {
            if parent.is_global {
                parent.consume(-self.bytes_consumed());
            } else {
                parent.remove(self);
            }
        }
        self.set_parent(Some(global));
        global.consume(self.bytes_consumed());
    }

    /// Go `DetachFromGlobalTracker`; panics on a non-global parent.
    pub fn detach_from_global_tracker(self: &Arc<Self>) {
        let Some(parent) = self.get_parent() else {
            return;
        };
        assert!(parent.is_global, "Detach from a non-GlobalTracker");
        parent.consume(-self.bytes_consumed());
        self.set_parent(None);
    }

    /// Go `ReplaceBytesUsed`.
    pub fn replace_bytes_used(self: &Arc<Self>, bytes: i64) {
        self.consume(bytes - self.bytes_consumed());
    }

    /// Go `Reset`: detach, zero consumption, drop children (label and limit
    /// survive).
    pub fn reset(self: &Arc<Self>) {
        self.detach();
        self.replace_bytes_used(0);
        self.children.lock().unwrap().clear();
    }

    fn get_parent(&self) -> Option<Arc<Tracker>> {
        self.parent.lock().unwrap().upgrade()
    }

    fn set_parent(&self, parent: Option<&Arc<Tracker>>) {
        *self.parent.lock().unwrap() = match parent {
            Some(p) => Arc::downgrade(p),
            None => Weak::new(),
        };
    }

    /// The session ID (Go reads `SessionID` directly).
    pub fn session_id(&self) -> u64 {
        self.session_id.load(SeqCst)
    }

    /// Children snapshot (Go `GetChildrenForTest`).
    pub fn children_for_test(&self) -> Vec<Arc<Tracker>> {
        self.children
            .lock()
            .unwrap()
            .values()
            .flat_map(|list| list.iter().cloned())
            .collect()
    }

    /// Go `CountAllChildrenMemUse`.
    pub fn count_all_children_mem_use(self: &Arc<Self>) -> HashMap<String, i64> {
        let mut map = HashMap::new();
        count_child_mem(self, "", &mut map);
        map
    }
}

fn count_child_mem(t: &Arc<Tracker>, family: &str, map: &mut HashMap<String, i64>) {
    let name = if family.is_empty() {
        format!("[{}]", t.label())
    } else {
        format!("{family} <- [{}]", t.label())
    };
    *map.entry(name.clone()).or_insert(0) += t.bytes_consumed();
    let children = t.children.lock().unwrap();
    for list in children.values() {
        for child in list {
            count_child_mem(child, &name, map);
        }
    }
}

/// Merges two action chains by priority, descending (Go `reArrangeFallback`).
fn rearrange_fallback(a: Option<ArcAction>, b: Option<ArcAction>) -> Option<ArcAction> {
    let (Some(mut a), Some(mut b)) = (a.clone(), b.clone()) else {
        return a.or(b);
    };
    if a.get_priority() < b.get_priority() {
        std::mem::swap(&mut a, &mut b);
    }
    a.set_fallback(rearrange_fallback(a.get_fallback(), Some(b)));
    Some(a)
}

/// Converts a byte count to a readable string (Go `BytesToString`).
pub fn bytes_to_string(num_bytes: i64) -> String {
    let gb = num_bytes as f64 / BYTE_SIZE_GB as f64;
    if gb > 1.0 {
        return format!("{gb} GB");
    }
    let mb = num_bytes as f64 / BYTE_SIZE_MB as f64;
    if mb > 1.0 {
        return format!("{mb} MB");
    }
    let kb = num_bytes as f64 / BYTE_SIZE_KB as f64;
    if kb > 1.0 {
        return format!("{kb} KB");
    }
    format!("{num_bytes} Bytes")
}

/// Formats bytes with pruned precision (Go `FormatBytes`).
pub fn format_bytes(num_bytes: i64) -> String {
    if num_bytes <= BYTE_SIZE_KB {
        return bytes_to_string(num_bytes);
    }
    let (unit, unit_str) = byte_unit(num_bytes);
    if unit == BYTE_SIZE {
        return bytes_to_string(num_bytes);
    }
    let v = num_bytes as f64 / unit as f64;
    let decimal = if num_bytes % unit == 0 {
        0
    } else if v < 10.0 {
        2
    } else {
        1
    };
    format!("{v:.decimal$} {unit_str}")
}

fn byte_unit(b: i64) -> (i64, &'static str) {
    if b > BYTE_SIZE_GB {
        (BYTE_SIZE_GB, "GB")
    } else if b > BYTE_SIZE_MB {
        (BYTE_SIZE_MB, "MB")
    } else if b > BYTE_SIZE_KB {
        (BYTE_SIZE_KB, "KB")
    } else {
        (BYTE_SIZE, "Bytes")
    }
}

#[cfg(test)]
mod tests {
    use super::super::action::{ActionOnExceed, ArcAction, BaseOomAction};
    use super::*;

    /// Go's test `mockAction`: first call marks it, later calls delegate to
    /// the fallback.
    #[derive(Default)]
    struct MockAction {
        base: BaseOomAction,
        called: AtomicBool,
        priority: i64,
    }

    impl MockAction {
        fn with_priority(priority: i64) -> Arc<MockAction> {
            Arc::new(MockAction {
                priority,
                ..Default::default()
            })
        }
        fn called(&self) -> bool {
            self.called.load(SeqCst)
        }
    }

    impl ActionOnExceed for MockAction {
        fn action(&self, t: &Arc<Tracker>) {
            if self.called() {
                if let Some(fallback) = self.base.get_fallback() {
                    fallback.action(t);
                }
                return;
            }
            self.called.store(true, SeqCst);
        }
        fn set_fallback(&self, a: Option<ArcAction>) {
            self.base.set_fallback(a);
        }
        fn get_fallback(&self) -> Option<ArcAction> {
            self.base.get_fallback()
        }
        fn get_priority(&self) -> i64 {
            self.priority
        }
        fn set_finished(&self) {
            self.base.set_finished();
        }
        fn is_finished(&self) -> bool {
            self.base.is_finished()
        }
    }

    // Go `TestSetLabel` + `TestSetLabel2`.
    #[test]
    fn set_label() {
        let tracker = Tracker::new(1, -1);
        assert_eq!(tracker.label(), 1);
        assert_eq!(tracker.bytes_consumed(), 0);
        assert_eq!(tracker.get_bytes_limit(), -1);
        assert!(tracker.get_parent().is_none());
        tracker.set_label(2);
        assert_eq!(tracker.label(), 2);
        assert!(tracker.get_parent().is_none());

        let parent = Tracker::new(1, -1);
        let child = Tracker::new(2, -1);
        child.attach_to(&parent);
        child.consume(10);
        assert_eq!(parent.bytes_consumed(), 10);
        child.set_label(10);
        assert_eq!(parent.bytes_consumed(), 10);
        child.detach();
        assert_eq!(parent.bytes_consumed(), 0);
    }

    // Go `TestConsume` (concurrent).
    #[test]
    fn consume_concurrent() {
        let tracker = Tracker::new(1, -1);
        tracker.consume(100);
        assert_eq!(tracker.bytes_consumed(), 100);

        let mut handles = Vec::new();
        for _ in 0..10 {
            let t = Arc::clone(&tracker);
            handles.push(std::thread::spawn(move || t.consume(10)));
            let t = Arc::clone(&tracker);
            handles.push(std::thread::spawn(move || t.consume(-10)));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(tracker.bytes_consumed(), 100);
    }

    // Go `TestRelease`'s flag-off path (the GC-aware flag-on path is Go
    // finalizer machinery, documented unported).
    #[test]
    fn release_flag_off() {
        let parent = Tracker::new_global(LABEL_FOR_GLOBAL_ANALYZE_MEMORY, -1);
        let tracker = Tracker::new(1, -1);
        tracker.attach_to_global_tracker(&parent);
        tracker.consume(100);
        assert_eq!(tracker.bytes_consumed(), 100);
        assert_eq!(parent.bytes_consumed(), 100);
        tracker.release(100);
        assert_eq!(tracker.bytes_consumed(), 0);
        assert_eq!(parent.bytes_consumed(), 0);
        assert_eq!(tracker.bytes_released(), 0);
        assert_eq!(parent.bytes_released(), 0);
    }

    // Go `TestBufferedConsumeAndRelease`'s buffering thresholds.
    #[test]
    fn buffered_consume_and_release() {
        let parent = Tracker::new_global(LABEL_FOR_GLOBAL_ANALYZE_MEMORY, -1);
        let tracker = Tracker::new(1, -1);
        tracker.attach_to_global_tracker(&parent);

        let mut buffered = 0_i64;
        tracker.buffered_consume(&mut buffered, TRACK_MEM_WHEN_EXCEEDS / 2);
        assert_eq!(tracker.bytes_consumed(), 0);
        tracker.buffered_consume(&mut buffered, TRACK_MEM_WHEN_EXCEEDS / 2);
        assert_eq!(tracker.bytes_consumed(), TRACK_MEM_WHEN_EXCEEDS);

        let mut buffered_release = 0_i64;
        tracker.buffered_release(&mut buffered_release, TRACK_MEM_WHEN_EXCEEDS / 2);
        assert_eq!(parent.bytes_consumed(), TRACK_MEM_WHEN_EXCEEDS);
        tracker.buffered_release(&mut buffered_release, TRACK_MEM_WHEN_EXCEEDS / 2);
        assert_eq!(parent.bytes_consumed(), 0);
    }

    // Go `TestOOMAction`.
    #[test]
    fn oom_action() {
        // No panic without an action able to panic.
        let tracker = Tracker::new(1, 100);
        tracker.consume(10_000);

        let tracker = Tracker::new(1, 100);
        let action = MockAction::with_priority(0);
        tracker.set_action_on_exceed(Some(action.clone()));
        assert!(!action.called());
        tracker.consume(10_000);
        assert!(action.called());

        // Fallback ordering.
        let action1 = MockAction::with_priority(0);
        let action2 = MockAction::with_priority(0);
        tracker.set_action_on_exceed(Some(action1.clone()));
        tracker.fallback_old_and_set_new_action(action2.clone());
        assert!(!action1.called() && !action2.called());
        tracker.consume(10_000);
        assert!(action2.called() && !action1.called());
        tracker.consume(10_000);
        assert!(action1.called() && action2.called());

        // Soft limit chain plus hard limit.
        let tracker = Tracker::new(1, 100);
        let action1 = MockAction::with_priority(0);
        let action2 = MockAction::with_priority(0);
        let action3 = MockAction::with_priority(0);
        tracker.set_action_on_exceed(Some(action1.clone()));
        tracker.fallback_old_and_set_new_action_for_soft_limit(action2.clone());
        tracker.fallback_old_and_set_new_action_for_soft_limit(action3.clone());
        tracker.consume(80);
        assert!(action3.called() && !action2.called() && !action1.called());
        tracker.consume(20);
        assert!(action3.called() && action2.called() && action1.called());

        // Finished links drop out of the chain.
        let a: Vec<Arc<MockAction>> = (0..5).map(|_| MockAction::with_priority(0)).collect();
        tracker.set_action_on_exceed(Some(a[0].clone()));
        for action in &a[1..] {
            tracker.fallback_old_and_set_new_action(action.clone());
        }
        let head = tracker.get_fallback_for_test(false).unwrap();
        assert!(Arc::ptr_eq(&head, &(a[4].clone() as ArcAction)));
        let fb = head.get_fallback().unwrap();
        assert!(Arc::ptr_eq(&fb, &(a[3].clone() as ArcAction)));
        a[3].set_finished();
        let fb = head.get_fallback().unwrap();
        assert!(Arc::ptr_eq(&fb, &(a[2].clone() as ArcAction)));
        a[2].set_finished();
        a[1].set_finished();
        let fb = head.get_fallback().unwrap();
        assert!(Arc::ptr_eq(&fb, &(a[0].clone() as ArcAction)));
    }

    // Go `TestAttachTo`.
    #[test]
    fn attach_to() {
        let old_parent = Tracker::new(1, -1);
        let new_parent = Tracker::new(2, -1);
        let child = Tracker::new(3, -1);
        child.consume(100);
        child.attach_to(&old_parent);
        assert_eq!(child.bytes_consumed(), 100);
        assert_eq!(old_parent.bytes_consumed(), 100);
        assert!(Arc::ptr_eq(&child.get_parent().unwrap(), &old_parent));
        assert_eq!(old_parent.children_for_test().len(), 1);

        child.attach_to(&new_parent);
        assert_eq!(child.bytes_consumed(), 100);
        assert_eq!(old_parent.bytes_consumed(), 0);
        assert_eq!(new_parent.bytes_consumed(), 100);
        assert!(Arc::ptr_eq(&child.get_parent().unwrap(), &new_parent));
        assert_eq!(new_parent.children_for_test().len(), 1);
        assert_eq!(old_parent.children_for_test().len(), 0);
    }

    // Go `TestDetach`.
    #[test]
    fn detach() {
        let parent = Tracker::new(1, -1);
        let child = Tracker::new(2, -1);
        child.consume(100);
        child.attach_to(&parent);
        assert_eq!(parent.bytes_consumed(), 100);
        assert_eq!(parent.children_for_test().len(), 1);

        child.detach();
        assert_eq!(child.bytes_consumed(), 100);
        assert_eq!(parent.bytes_consumed(), 0);
        assert_eq!(parent.children_for_test().len(), 0);
        assert!(child.get_parent().is_none());
    }

    // Go `TestReplaceChild`.
    #[test]
    fn replace_child() {
        let old_child = Tracker::new(1, -1);
        old_child.consume(100);
        let new_child = Tracker::new(2, -1);
        new_child.consume(500);
        let parent = Tracker::new(3, -1);

        old_child.attach_to(&parent);
        assert_eq!(parent.bytes_consumed(), 100);

        parent.replace_child(&old_child, Some(&new_child));
        assert_eq!(parent.bytes_consumed(), 500);
        assert_eq!(parent.children_for_test().len(), 1);
        assert!(Arc::ptr_eq(&new_child.get_parent().unwrap(), &parent));
        assert!(old_child.get_parent().is_none());

        parent.replace_child(&old_child, None);
        assert_eq!(parent.bytes_consumed(), 500);
        assert_eq!(parent.children_for_test().len(), 1);

        parent.replace_child(&new_child, None);
        assert_eq!(parent.bytes_consumed(), 0);
        assert_eq!(parent.children_for_test().len(), 0);
        assert!(new_child.get_parent().is_none());

        let node1 = Tracker::new(1, -1);
        let node2 = Tracker::new(2, -1);
        let node3 = Tracker::new(3, -1);
        node2.attach_to(&node1);
        node3.attach_to(&node2);
        node3.consume(100);
        assert_eq!(node1.bytes_consumed(), 100);
        node2.replace_child(&node3, None);
        assert_eq!(node2.bytes_consumed(), 0);
        assert_eq!(node1.bytes_consumed(), 0);
    }

    // Go `TestToString` (the exact tree rendering).
    #[test]
    fn to_string_tree() {
        let parent = Tracker::new(1, -1);
        let c1 = Tracker::new(2, 1000);
        let c2 = Tracker::new(3, -1);
        let c3 = Tracker::new(4, -1);
        let c4 = Tracker::new(5, -1);
        c1.attach_to(&parent);
        c2.attach_to(&parent);
        c3.attach_to(&parent);
        c4.attach_to(&parent);
        c1.consume(100);
        c2.consume(2 * 1024);
        c3.consume(3 * 1024 * 1024);
        c4.consume(4 * 1024 * 1024 * 1024);

        let expected = "\n\"1\"{\n  \"consumed\": 4.00 GB\n  \"2\"{\n    \"quota\": 1000 Bytes\n    \"consumed\": 100 Bytes\n  }\n  \"3\"{\n    \"consumed\": 2 KB\n  }\n  \"4\"{\n    \"consumed\": 3 MB\n  }\n  \"5\"{\n    \"consumed\": 4 GB\n  }\n}\n";
        assert_eq!(parent.tree_string(), expected);
    }

    // Go `TestMaxConsumed`.
    #[test]
    fn max_consumed() {
        let r = Tracker::new(1, -1);
        let c1 = Tracker::new(2, -1);
        let c2 = Tracker::new(3, -1);
        let cc1 = Tracker::new(4, -1);
        c1.attach_to(&r);
        c2.attach_to(&r);
        cc1.attach_to(&c1);

        let ts = [&r, &c1, &c2, &cc1];
        let (mut consumed, mut max_consumed) = (0_i64, 0_i64);
        for _ in 0..10 {
            let tracker = ts[crate::fastrand::uint32_n(4) as usize];
            let mut b = i64::from(crate::fastrand::uint32_n(1000)) - 500;
            if consumed + b < 0 {
                b = -consumed;
            }
            consumed += b;
            tracker.consume(b);
            max_consumed = max_consumed.max(consumed);
            assert_eq!(r.bytes_consumed(), consumed);
            assert_eq!(r.max_consumed(), max_consumed);
        }
    }

    // Go `TestGlobalTracker` (including both panic contracts).
    #[test]
    fn global_tracker() {
        let r = Tracker::new_global(1, -1);
        let c1 = Tracker::new(2, -1);
        let c2 = Tracker::new(3, -1);
        c1.consume(100);
        c2.consume(200);

        c1.attach_to_global_tracker(&r);
        c2.attach_to_global_tracker(&r);
        assert_eq!(r.bytes_consumed(), 300);
        assert!(Arc::ptr_eq(&c1.get_parent().unwrap(), &r));
        assert_eq!(r.children_for_test().len(), 0);

        c1.detach_from_global_tracker();
        c2.detach_from_global_tracker();
        assert_eq!(r.bytes_consumed(), 0);
        assert!(c1.get_parent().is_none());

        let common = Tracker::new(4, -1);
        let c1_clone = Arc::clone(&c1);
        let common_clone = Arc::clone(&common);
        let panicked = std::panic::catch_unwind(move || {
            c1_clone.attach_to_global_tracker(&common_clone);
        });
        assert!(panicked.is_err(), "attach to non-global must panic");

        c1.attach_to(&common);
        assert_eq!(common.bytes_consumed(), 100);
        c1.attach_to_global_tracker(&r);
        assert_eq!(common.bytes_consumed(), 0);
        assert_eq!(r.bytes_consumed(), 100);

        c2.attach_to(&common);
        let c2_clone = Arc::clone(&c2);
        let panicked = std::panic::catch_unwind(move || {
            c2_clone.detach_from_global_tracker();
        });
        assert!(panicked.is_err(), "detach from non-global must panic");
    }

    // Go `TestFormatBytesWithPrune`, including its parseByte input builder.
    #[test]
    fn format_bytes_with_prune() {
        fn parse_byte(s: &str) -> i64 {
            let (num, unit): (String, String) =
                s.chars().partition(|c| c.is_ascii_digit() || *c == '.');
            let unit = match unit.trim() {
                "GB" => BYTE_SIZE_GB,
                "MB" => BYTE_SIZE_MB,
                "KB" => BYTE_SIZE_KB,
                "Bytes" => BYTE_SIZE,
                other => panic!("invalid byte unit: {other}"),
            };
            (num.parse::<f64>().unwrap() * unit as f64) as i64
        }
        let cases = [
            ("0 Bytes", "0 Bytes"),
            ("1 Bytes", "1 Bytes"),
            ("9 Bytes", "9 Bytes"),
            ("10 Bytes", "10 Bytes"),
            ("999 Bytes", "999 Bytes"),
            ("1 KB", "1024 Bytes"),
            ("1.123 KB", "1.12 KB"),
            ("1.023 KB", "1.02 KB"),
            ("1.003 KB", "1.00 KB"),
            ("10.456 KB", "10.5 KB"),
            ("10.956 KB", "11.0 KB"),
            ("999.056 KB", "999.1 KB"),
            ("999.988 KB", "1000.0 KB"),
            ("1.123 MB", "1.12 MB"),
            ("1.023 MB", "1.02 MB"),
            ("1.003 MB", "1.00 MB"),
            ("10.456 MB", "10.5 MB"),
            ("10.956 MB", "11.0 MB"),
            ("999.056 MB", "999.1 MB"),
            ("999.988 MB", "1000.0 MB"),
            ("1.123 GB", "1.12 GB"),
            ("1.023 GB", "1.02 GB"),
            ("1.003 GB", "1.00 GB"),
            ("10.456 GB", "10.5 GB"),
            ("10.956 GB", "11.0 GB"),
            ("9.412345 MB", "9.41 MB"),
            ("10.412345 MB", "10.4 MB"),
            ("5.999 GB", "6.00 GB"),
            ("100.46 KB", "100.5 KB"),
            ("18.399999618530273 MB", "18.4 MB"),
            ("9.15999984741211 MB", "9.16 MB"),
        ];
        for (input, want) in cases {
            assert_eq!(format_bytes(parse_byte(input)), want, "input: {input}");
        }
    }

    // Go `TestOOMActionPriority`: 100 shuffled actions fire in strict
    // priority order, one per consume.
    #[test]
    fn oom_action_priority() {
        let tracker = Tracker::new(1, 1);
        tracker.set_action_on_exceed(None);
        const N: usize = 100;
        let actions: Vec<Arc<MockAction>> = (0..N)
            .map(|i| MockAction::with_priority(i as i64))
            .collect();

        let mut shuffle: Vec<usize> = Vec::with_capacity(N);
        for i in 0..N {
            shuffle.push(i);
            let pos = crate::fastrand::uint32_n(i as u32 + 1) as usize;
            shuffle.swap(i, pos);
        }
        for &i in &shuffle {
            tracker.fallback_old_and_set_new_action(actions[i].clone());
        }
        for i in (0..N).rev() {
            tracker.consume(100);
            for (j, action) in actions.iter().enumerate() {
                assert_eq!(action.called(), j >= i, "i={i} j={j}");
            }
        }
    }
}
