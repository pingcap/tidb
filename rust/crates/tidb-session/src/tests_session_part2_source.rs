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

//! Behavioral session tests retained from the original Go test inventory.
//! Empty harness entries moved to session-cleanup-obligations.json in
//! rust/docs/parity/current-audit; their Go obligations remain unverified.

#![cfg(test)]

use crate::cursor::{CursorTracker, State};

/// `pkg/session/cursor/tracker_test.go:26::TestNewCursor`.
#[test]
fn test_new_cursor() {
    let tracker = CursorTracker::new();
    let first = tracker.new_cursor(State::default());
    let second = tracker.new_cursor(State::default());
    assert_eq!(first.id(), 1);
    assert_eq!(second.id(), 2);
}

/// `pkg/session/cursor/tracker_test.go:37::TestGetCursor`.
#[test]
fn test_get_cursor() {
    let tracker = CursorTracker::new();
    let cursor = tracker.new_cursor(State { start_ts: 42 });
    let found = tracker.cursor(cursor.id()).expect("cursor was registered");
    assert_eq!(found.id(), cursor.id());
    assert_eq!(found.state(), State { start_ts: 42 });
}

/// `pkg/session/cursor/tracker_test.go:45::TestRangeCursor`.
#[test]
fn test_range_cursor() {
    let tracker = CursorTracker::new();
    tracker.new_cursor(State::default());
    let mut called = false;
    tracker.range_cursor(|cursor| {
        called = true;
        assert_eq!(cursor.id(), 1);
        false
    });
    assert!(called);
}

/// `pkg/session/cursor/tracker_test.go:59::TestCursorHandleClose`.
#[test]
fn test_cursor_handle_close() {
    let tracker = CursorTracker::new();
    let cursor = tracker.new_cursor(State::default());
    let id = cursor.id();
    cursor.close();
    assert!(tracker.cursor(id).is_none());
}

/// `pkg/session/cursor/tracker_test.go:69::TestCursorTrackerConcurrentCreateDelete`.
#[test]
fn test_cursor_tracker_concurrent_create_delete() {
    let tracker = CursorTracker::new();
    std::thread::scope(|scope| {
        for _ in 0..100 {
            let tracker = tracker.clone();
            scope.spawn(move || {
                for _ in 0..100 {
                    let cursor = tracker.new_cursor(State::default());
                    cursor.close();
                }
            });
        }
        for _ in 0..100 {
            let tracker = tracker.clone();
            scope.spawn(move || {
                tracker.range_cursor(|cursor| {
                    cursor.close();
                    true
                });
            });
        }
    });
    assert!(tracker.is_empty());
}
