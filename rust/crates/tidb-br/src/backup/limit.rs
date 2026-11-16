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

//! Go `br/pkg/backup/limit.go`.

use std::sync::{Condvar, Mutex};

/// Go `ResourceConcurrentLimiter`: how many sub-ranges are being sent to
/// TiKV at once; an acquire waits while the count is at the threshold.
pub struct ResourceConcurrentLimiter {
    current: Mutex<i64>,
    cond: Condvar,
    threshold: i64,
}

impl ResourceConcurrentLimiter {
    /// Go `NewResourceMemoryLimiter`.
    #[must_use]
    pub fn new(threshold: i64) -> Self {
        Self {
            current: Mutex::new(0),
            cond: Condvar::new(),
            threshold,
        }
    }

    /// Go `Acquire`: the count after adding `resource`.
    pub fn acquire(&self, resource: i64) -> i64 {
        let mut current = self.current.lock().unwrap();
        while *current >= self.threshold {
            current = self.cond.wait(current).unwrap();
        }
        *current += resource;
        *current
    }

    /// Go `Release`.
    pub fn release(&self, resource: i64) {
        *self.current.lock().unwrap() -= resource;
        self.cond.notify_all();
    }
}
