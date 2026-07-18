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

//! Source-contract tests for session cursor tracking.

use std::sync::Arc;
use std::thread;

use tidb_exec::cursor_tracker::{CursorState, CursorTracker};

#[test]
fn new_cursor_allocates_monotonic_ids_and_preserves_state() {
    // Source: pkg/session/cursor/state.go:17-20 and
    // pkg/session/cursor/tracker.go:41-45; Go's first Add returns ID 1.
    let tracker = CursorTracker::new();
    let first = tracker.new_cursor(CursorState { start_ts: 41 });
    let second = tracker.new_cursor(CursorState { start_ts: 42 });

    assert_eq!(first.id(), 1);
    assert_eq!(second.id(), 2);
    assert_eq!(first.get_state().start_ts, 41);
    assert_eq!(second.get_state().start_ts, 42);
}

#[test]
fn get_cursor_returns_live_handle_or_none_after_close() {
    // Source: pkg/session/cursor/tracker.go:39-46, 67-70.
    let tracker = CursorTracker::new();
    let cursor = tracker.new_cursor(CursorState::default());
    let id = cursor.id();

    assert_eq!(tracker.get_cursor(id).map(|found| found.id()), Some(id));
    assert!(tracker.get_cursor(999).is_none());
    cursor.close();
    assert!(tracker.get_cursor(id).is_none());
}

#[test]
fn range_cursor_supports_early_stop_and_callback_close() {
    // Source: pkg/session/cursor/tracker.go:48-54 and
    // pkg/session/cursor/tracker_test.go:45-56.
    let tracker = CursorTracker::new();
    let first = tracker.new_cursor(CursorState::default());
    let second = tracker.new_cursor(CursorState::default());
    let mut visited = Vec::new();
    tracker.range_cursor(|found| {
        visited.push(found.id());
        found.close();
        false
    });

    assert_eq!(visited.len(), 1);
    assert!(visited[0] == first.id() || visited[0] == second.id());
    let remaining_id = if visited[0] == first.id() {
        second.id()
    } else {
        first.id()
    };
    assert!(tracker.get_cursor(remaining_id).is_some());
    tracker
        .get_cursor(remaining_id)
        .expect("remaining cursor disappeared")
        .close();
    assert!(tracker.get_cursor(first.id()).is_none());
    assert!(tracker.get_cursor(second.id()).is_none());
}

#[test]
fn concurrent_create_and_delete_are_safe() {
    // Source: pkg/session/cursor/tracker_test.go:69-108. The Go test runs
    // open-ended workers for two seconds; a bounded round count exercises the
    // same concurrent NewCursor/RangeCursor/Close contract deterministically.
    let tracker = Arc::new(CursorTracker::new());
    let mut workers = Vec::new();

    for _ in 0..8 {
        let tracker = Arc::clone(&tracker);
        workers.push(thread::spawn(move || {
            for _ in 0..500 {
                tracker.new_cursor(CursorState::default());
            }
        }));
    }
    for _ in 0..8 {
        let tracker = Arc::clone(&tracker);
        workers.push(thread::spawn(move || {
            for _ in 0..500 {
                tracker.range_cursor(|cursor| {
                    cursor.close();
                    true
                });
            }
        }));
    }

    for worker in workers {
        worker.join().expect("cursor worker panicked");
    }
    tracker.range_cursor(|cursor| {
        cursor.close();
        true
    });
    let mut remaining = false;
    tracker.range_cursor(|_| {
        remaining = true;
        false
    });
    assert!(!remaining);
}
