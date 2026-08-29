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

use std::collections::HashSet;
use std::sync::{Arc, LazyLock, RwLock};

use tidb_sqlexec::{SqlExecError, TrackProcess, TrackSysProc, UntrackSysProc};

/// Go `AutoAnalyzeProcIDGenerator`.
pub trait AutoAnalyzeProcIdGenerator: Send + Sync {
    /// Go `AutoAnalyzeProcID`.
    fn auto_analyze_proc_id(&self) -> u64;
    /// Go `ReleaseAutoAnalyzeProcID`.
    fn release_auto_analyze_proc_id(&self, id: u64);
}

/// Go's private `generator` implementation.
pub struct Generator {
    get: Arc<dyn Fn() -> u64 + Send + Sync>,
    release: Arc<dyn Fn(u64) + Send + Sync>,
}

impl Generator {
    /// Go `NewGenerator`.
    #[must_use]
    pub fn new(
        get: impl Fn() -> u64 + Send + Sync + 'static,
        release: impl Fn(u64) + Send + Sync + 'static,
    ) -> Self {
        Self {
            get: Arc::new(get),
            release: Arc::new(release),
        }
    }
}

impl AutoAnalyzeProcIdGenerator for Generator {
    fn auto_analyze_proc_id(&self) -> u64 {
        (self.get)()
    }

    fn release_auto_analyze_proc_id(&self, id: u64) {
        (self.release)(id);
    }
}

/// Go's private `globalAutoAnalyzeProcessList`, public only because Rust must
/// name the type of [`GLOBAL_AUTO_ANALYZE_PROCESS_LIST`].
#[derive(Debug, Default)]
pub struct AutoAnalyzeProcessList {
    processes: RwLock<HashSet<u64>>,
}

impl AutoAnalyzeProcessList {
    /// Go `Tracker`.
    pub fn tracker(&self, id: u64) {
        self.processes
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(id);
    }

    /// Go `Untracker`.
    pub fn untracker(&self, id: u64) {
        self.processes
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&id);
    }

    /// Go `All`; order is unspecified.
    #[must_use]
    pub fn all(&self) -> Vec<u64> {
        self.processes
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .copied()
            .collect()
    }

    /// Go `Contains`.
    #[must_use]
    pub fn contains(&self, id: u64) -> bool {
        self.processes
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(&id)
    }
}

/// Go `GlobalAutoAnalyzeProcessList`.
pub static GLOBAL_AUTO_ANALYZE_PROCESS_LIST: LazyLock<AutoAnalyzeProcessList> =
    LazyLock::new(AutoAnalyzeProcessList::default);

/// Go `AutoAnalyzeTracker`.
pub struct AutoAnalyzeTracker {
    track: TrackSysProc,
    untrack: UntrackSysProc,
}

impl AutoAnalyzeTracker {
    /// Go `NewAutoAnalyzeTracker`.
    #[must_use]
    pub fn new(track: TrackSysProc, untrack: UntrackSysProc) -> Self {
        Self { track, untrack }
    }

    /// Go `Track`. The global list is updated before the delegated callback,
    /// including when that callback returns an error.
    pub fn track(&self, id: u64, context: Arc<dyn TrackProcess>) -> Result<(), SqlExecError> {
        GLOBAL_AUTO_ANALYZE_PROCESS_LIST.tracker(id);
        (self.track)(id, context)
    }

    /// Go `UnTrack`. The global list is updated before the delegated callback.
    pub fn untrack(&self, id: u64) {
        GLOBAL_AUTO_ANALYZE_PROCESS_LIST.untracker(id);
        (self.untrack)(id);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

    use super::*;

    #[test]
    fn generator_delegates_get_and_release() {
        let released = Arc::new(AtomicU64::new(0));
        let released_by_callback = Arc::clone(&released);
        let generator = Generator::new(
            || 42,
            move |id| released_by_callback.store(id, Ordering::SeqCst),
        );
        assert_eq!(generator.auto_analyze_proc_id(), 42);
        generator.release_auto_analyze_proc_id(42);
        assert_eq!(released.load(Ordering::SeqCst), 42);
    }

    #[test]
    fn process_list_is_an_idempotent_unordered_set() {
        let list = AutoAnalyzeProcessList::default();
        list.tracker(2);
        list.tracker(1);
        list.tracker(2);
        assert!(list.contains(1));
        let mut all = list.all();
        all.sort_unstable();
        assert_eq!(all, [1, 2]);
        list.untracker(1);
        list.untracker(1);
        assert!(!list.contains(1));
    }

    #[test]
    fn tracker_updates_global_state_before_callbacks() {
        const ID: u64 = u64::MAX - 7;
        GLOBAL_AUTO_ANALYZE_PROCESS_LIST.untracker(ID);
        let events = Arc::new(Mutex::new(Vec::new()));
        let track_events = Arc::clone(&events);
        let untrack_events = Arc::clone(&events);
        let tracker = AutoAnalyzeTracker::new(
            Arc::new(move |id, _| {
                assert!(GLOBAL_AUTO_ANALYZE_PROCESS_LIST.contains(id));
                track_events.lock().unwrap().push("track");
                Ok(())
            }),
            Arc::new(move |id| {
                assert!(!GLOBAL_AUTO_ANALYZE_PROCESS_LIST.contains(id));
                untrack_events.lock().unwrap().push("untrack");
            }),
        );
        tracker.track(ID, Arc::new(())).unwrap();
        tracker.untrack(ID);
        assert_eq!(*events.lock().unwrap(), ["track", "untrack"]);
    }
}
