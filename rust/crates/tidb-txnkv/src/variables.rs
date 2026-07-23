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

//! KV client variables re-exported by `pkg/kv/variables.go`.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

/// Default lock-fast backoff in milliseconds.
pub const DEFAULT_BACKOFF_LOCK_FAST: i32 = 10;
/// Default maximum-backoff weight.
pub const DEFAULT_BACKOFF_WEIGHT: i32 = 2;

/// Variables shared with KV storage.
#[derive(Debug, Clone)]
pub struct KvVariables {
    /// Lock-fast backoff base duration in milliseconds.
    pub backoff_lock_fast: i32,
    /// Weight applied to maximum backoff durations.
    pub backoff_weight: i32,
    /// Session kill-reason enum; zero means not killed.
    pub killed: Arc<AtomicU32>,
}

impl KvVariables {
    /// Creates the source default values around the caller-owned kill flag.
    #[must_use]
    pub fn new(killed: Arc<AtomicU32>) -> Self {
        Self {
            backoff_lock_fast: DEFAULT_BACKOFF_LOCK_FAST,
            backoff_weight: DEFAULT_BACKOFF_WEIGHT,
            killed,
        }
    }

    /// Returns the current kill reason.
    #[must_use]
    pub fn kill_reason(&self) -> u32 {
        self.killed.load(Ordering::Acquire)
    }
}

impl Default for KvVariables {
    fn default() -> Self {
        Self::new(Arc::new(AtomicU32::new(0)))
    }
}
