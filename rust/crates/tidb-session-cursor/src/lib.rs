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

//! Complete transcreation of Go `pkg/session/cursor` (`state.go`,
//! `tracker.go`): the per-session registry of open cursors.
//!
//! Go's `Handle` holds a back-pointer to its tracker so `Close` can remove
//! itself, which needs a reference cycle. Rust models the same contract with
//! a handle that shares ownership of the registry through an `Arc`, so
//! [`CursorHandle::close`] still removes the cursor from the tracker that
//! created it. Go's `Tracker`/`Handle` interfaces each have exactly one
//! implementation, so the concrete types carry their methods directly.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

/// Go `cursor.State`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct State {
    /// Go `State.StartTS`.
    pub start_ts: u64,
}

#[derive(Debug, Default)]
struct TrackerInner {
    /// Go's `sync.Map` of cursors. A `BTreeMap` keeps `range_cursor` in ID
    /// order; Go's map order is unspecified, so ordering here is a
    /// refinement, not a divergence.
    cursors: Mutex<BTreeMap<i64, State>>,
    id_alloc: AtomicI64,
}

/// Go `cursor.Tracker`: the set of cursors open inside one session.
#[derive(Clone, Debug, Default)]
pub struct CursorTracker {
    inner: Arc<TrackerInner>,
}

impl CursorTracker {
    /// Go `NewTracker`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Go `NewCursor`. IDs are handed out by pre-incrementing a counter, so
    /// the first cursor is 1.
    pub fn new_cursor(&self, state: State) -> CursorHandle {
        let id = self.inner.id_alloc.fetch_add(1, Ordering::SeqCst) + 1;
        self.lock().insert(id, state);
        CursorHandle {
            id,
            state,
            tracker: Arc::clone(&self.inner),
        }
    }

    /// Go `GetCursor`: `None` for an unknown or already-closed ID.
    #[must_use]
    pub fn cursor(&self, id: i64) -> Option<CursorHandle> {
        let state = *self.lock().get(&id)?;
        Some(CursorHandle {
            id,
            state,
            tracker: Arc::clone(&self.inner),
        })
    }

    /// Go `RangeCursor`: visits each open cursor until the callback returns
    /// false.
    pub fn range_cursor(&self, mut visit: impl FnMut(&CursorHandle) -> bool) {
        // Snapshot first: Go's sync.Map.Range tolerates mutation during the
        // walk, and a callback that closes a cursor is a normal caller.
        let snapshot: Vec<(i64, State)> = self
            .lock()
            .iter()
            .map(|(id, state)| (*id, *state))
            .collect();
        for (id, state) in snapshot {
            let handle = CursorHandle {
                id,
                state,
                tracker: Arc::clone(&self.inner),
            };
            if !visit(&handle) {
                return;
            }
        }
    }

    /// The number of open cursors.
    #[must_use]
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// Whether no cursor is open.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<i64, State>> {
        self.inner
            .cursors
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Go `cursor.Handle`: a live cursor, able to close itself.
#[derive(Clone, Debug)]
pub struct CursorHandle {
    id: i64,
    state: State,
    tracker: Arc<TrackerInner>,
}

impl CursorHandle {
    /// Go `Handle.ID`.
    #[must_use]
    pub const fn id(&self) -> i64 {
        self.id
    }

    /// Go `Handle.GetState`.
    #[must_use]
    pub const fn state(&self) -> State {
        self.state
    }

    /// Go `Handle.Close`: removes this cursor from its tracker. Closing an
    /// already-closed cursor is a no-op, as deleting a missing key is in Go.
    pub fn close(&self) {
        self.tracker
            .cursors
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&self.id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Go `TestNewCursor`: IDs count up from one and the state round-trips.
    #[test]
    fn new_cursors_get_increasing_ids() {
        let tracker = CursorTracker::new();
        for expected_id in 1..=10 {
            let cursor = tracker.new_cursor(State {
                start_ts: expected_id as u64,
            });
            assert_eq!(cursor.id(), expected_id);
            assert_eq!(cursor.state().start_ts, expected_id as u64);
        }
        assert_eq!(tracker.len(), 10);
    }

    // Go `TestGetCursor`: a stored cursor is retrievable by ID; an unknown or
    // closed ID is not.
    #[test]
    fn cursors_are_retrievable_until_closed() {
        let tracker = CursorTracker::new();
        let cursor = tracker.new_cursor(State { start_ts: 42 });

        let found = tracker.cursor(cursor.id()).expect("just stored");
        assert_eq!(found.id(), cursor.id());
        assert_eq!(found.state().start_ts, 42);

        assert!(tracker.cursor(cursor.id() + 1).is_none());

        cursor.close();
        assert!(tracker.cursor(cursor.id()).is_none());
    }

    // Go `TestRangeCursor`: every open cursor is visited.
    #[test]
    fn range_visits_every_open_cursor() {
        let tracker = CursorTracker::new();
        for start_ts in 0..10 {
            tracker.new_cursor(State { start_ts });
        }

        let mut seen = Vec::new();
        tracker.range_cursor(|cursor| {
            seen.push(cursor.id());
            true
        });
        seen.sort_unstable();
        assert_eq!(seen, (1..=10).collect::<Vec<_>>());
    }

    // A callback returning false stops the walk early.
    #[test]
    fn range_stops_when_the_callback_returns_false() {
        let tracker = CursorTracker::new();
        for start_ts in 0..10 {
            tracker.new_cursor(State { start_ts });
        }

        let mut visited = 0;
        tracker.range_cursor(|_| {
            visited += 1;
            visited < 3
        });
        assert_eq!(visited, 3);
    }

    // Go `TestCursorHandleClose`: closing removes the cursor, and closing
    // twice is harmless.
    #[test]
    fn closing_removes_the_cursor_and_is_idempotent() {
        let tracker = CursorTracker::new();
        let cursor = tracker.new_cursor(State { start_ts: 1 });
        assert_eq!(tracker.len(), 1);

        cursor.close();
        assert!(tracker.is_empty());
        cursor.close();
        assert!(tracker.is_empty());

        // Closing through a second handle for the same cursor also works.
        let cursor = tracker.new_cursor(State { start_ts: 2 });
        let same = tracker.cursor(cursor.id()).expect("just stored");
        same.close();
        assert!(tracker.is_empty());
    }

    // Go `TestCursorTrackerConcurrentCreateDelete`: concurrent creates and
    // closes never lose or duplicate an ID.
    #[test]
    fn concurrent_creates_and_closes_stay_consistent() {
        let tracker = CursorTracker::new();
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let tracker = tracker.clone();
                scope.spawn(move || {
                    for start_ts in 0..200 {
                        let cursor = tracker.new_cursor(State { start_ts });
                        assert_eq!(
                            tracker.cursor(cursor.id()).map(|c| c.id()),
                            Some(cursor.id())
                        );
                        cursor.close();
                    }
                });
            }
        });
        assert!(tracker.is_empty());
    }
}
