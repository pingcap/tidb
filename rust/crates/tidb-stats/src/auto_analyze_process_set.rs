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

//! Auto-analyze process-ID set from
//! `pkg/statistics/handle/util/auto_analyze_proc_id_generator.go`.
//!
//! TiDB tracks active auto-analyze process IDs in a map protected by an
//! `sync.RWMutex`. This leaf preserves insertion/removal/lookup semantics and
//! snapshot access while leaving the process-ID generator, sysproctrack
//! callbacks, global singleton wiring, and analyze execution external.

use std::collections::HashSet;
use std::sync::RwLock;

/// Thread-safe set of active auto-analyze process IDs.
#[derive(Debug, Default)]
pub struct AutoAnalyzeProcessSet {
    processes: RwLock<HashSet<u64>>,
}

impl AutoAnalyzeProcessSet {
    /// Creates an empty process-ID set.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Tracks an auto-analyze process ID, idempotently.
    pub fn tracker(&self, id: u64) {
        self.processes
            .write()
            .expect("auto-analyze process set lock poisoned")
            .insert(id);
    }

    /// Removes an auto-analyze process ID, idempotently.
    pub fn untracker(&self, id: u64) {
        self.processes
            .write()
            .expect("auto-analyze process set lock poisoned")
            .remove(&id);
    }

    /// Returns a snapshot of all tracked process IDs.
    ///
    /// Like Go's `maps.Keys`, set iteration order is unspecified.
    #[must_use]
    pub fn all(&self) -> Vec<u64> {
        self.processes
            .read()
            .expect("auto-analyze process set lock poisoned")
            .iter()
            .copied()
            .collect()
    }

    /// Reports whether an auto-analyze process ID is tracked.
    #[must_use]
    pub fn contains(&self, id: u64) -> bool {
        self.processes
            .read()
            .expect("auto-analyze process set lock poisoned")
            .contains(&id)
    }
}
