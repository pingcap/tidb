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

//! Session cursor state and lifecycle tracking.
//!
//! This leaf ports the dependency-closed cursor owner from TiDB's
//! `pkg/session/cursor`. It allocates monotonically increasing cursor IDs,
//! supports lookup and early-stoppable snapshots of the live handles, and
//! removes a handle when it is closed. Query execution, result-set encoding,
//! and session status flags remain outside this owner.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock, Weak};

/// State captured when a cursor is created.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CursorState {
    /// Transaction start timestamp associated with the cursor.
    pub start_ts: u64,
}

#[derive(Debug)]
struct CursorTrackerInner {
    cursors: RwLock<BTreeMap<usize, Arc<CursorHandleInner>>>,
    next_id: AtomicUsize,
}

#[derive(Debug)]
struct CursorHandleInner {
    id: usize,
    state: CursorState,
    tracker: Weak<CursorTrackerInner>,
}

/// A handle used to inspect or close one cursor.
#[derive(Clone, Debug)]
pub struct CursorHandle {
    inner: Arc<CursorHandleInner>,
}

/// Session-owned collection of live cursors.
#[derive(Clone, Debug)]
pub struct CursorTracker {
    inner: Arc<CursorTrackerInner>,
}

impl Default for CursorTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl CursorTracker {
    /// Creates an empty tracker whose first cursor has ID 1.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(CursorTrackerInner {
                cursors: RwLock::new(BTreeMap::new()),
                next_id: AtomicUsize::new(0),
            }),
        }
    }

    /// Allocates, stores, and returns a cursor with a fresh ID.
    pub fn new_cursor(&self, state: CursorState) -> CursorHandle {
        let id = self.inner.next_id.fetch_add(1, Ordering::SeqCst) + 1;
        let handle = CursorHandle {
            inner: Arc::new(CursorHandleInner {
                id,
                state,
                tracker: Arc::downgrade(&self.inner),
            }),
        };
        self.inner
            .cursors
            .write()
            .expect("cursor tracker lock poisoned")
            .insert(id, Arc::clone(&handle.inner));
        handle
    }

    /// Returns a live cursor by ID, or `None` after it has been closed.
    #[must_use]
    pub fn get_cursor(&self, id: usize) -> Option<CursorHandle> {
        self.inner
            .cursors
            .read()
            .expect("cursor tracker lock poisoned")
            .get(&id)
            .map(|inner| CursorHandle {
                inner: Arc::clone(inner),
            })
    }

    /// Visits a snapshot of live cursors until the callback returns `false`.
    ///
    /// The snapshot is taken before callbacks run so a callback may close its
    /// cursor without deadlocking the tracker. A concurrent create or close
    /// may therefore race with the snapshot, matching `sync.Map.Range`'s
    /// weakly consistent traversal contract.
    pub fn range_cursor<F>(&self, mut f: F)
    where
        F: FnMut(&CursorHandle) -> bool,
    {
        let cursors = self
            .inner
            .cursors
            .read()
            .expect("cursor tracker lock poisoned")
            .values()
            .cloned()
            .map(|inner| CursorHandle { inner })
            .collect::<Vec<_>>();
        for cursor in cursors {
            if !f(&cursor) {
                break;
            }
        }
    }

    fn remove(&self, id: usize) {
        self.inner
            .cursors
            .write()
            .expect("cursor tracker lock poisoned")
            .remove(&id);
    }
}

impl CursorHandle {
    /// Returns this cursor's stable ID.
    #[must_use]
    pub fn id(&self) -> usize {
        self.inner.id
    }

    /// Returns the state captured when this cursor was created.
    #[must_use]
    pub fn get_state(&self) -> CursorState {
        self.inner.state
    }

    /// Removes this cursor from its tracker. Repeated calls are harmless.
    pub fn close(&self) {
        if let Some(tracker) = self.inner.tracker.upgrade() {
            CursorTracker { inner: tracker }.remove(self.id());
        }
    }
}
