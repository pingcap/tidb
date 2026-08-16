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

//! Complete transcreation of Go `pkg/ttl/cache/base.go`: the refresh clock the
//! info-schema and table-status caches share.
//!
//! Go's zero `time.Time` makes the first `time.Since(updateTime)` enormous, so
//! a never-updated cache always wants refreshing. `Option<Instant>` says the
//! same thing without inventing an epoch, and `None` is the state Go's zero
//! value stands for.

use std::time::{Duration, Instant};

/// Go's unexported `baseCache`.
#[derive(Debug, Clone)]
pub struct BaseCache {
    interval: Duration,
    update_time: Option<Instant>,
}

impl BaseCache {
    /// Go `newBaseCache`.
    #[must_use]
    pub fn new(interval: Duration) -> Self {
        Self {
            interval,
            update_time: None,
        }
    }

    /// Go `(*baseCache).ShouldUpdate`: whether this cache needs update.
    #[must_use]
    pub fn should_update(&self) -> bool {
        match self.update_time {
            None => true,
            Some(update_time) => update_time.elapsed() > self.interval,
        }
    }

    /// Go `(*baseCache).SetInterval`: sets the interval of updating cache.
    pub fn set_interval(&mut self, interval: Duration) {
        self.interval = interval;
    }

    /// Go `(*baseCache).GetInterval`.
    #[must_use]
    pub fn get_interval(&self) -> Duration {
        self.interval
    }

    /// Go's `bc.updateTime = time.Now()`, which the two derived caches perform
    /// directly on the embedded struct at the end of their `Update`.
    pub fn mark_updated(&mut self) {
        self.update_time = Some(Instant::now());
    }

    /// Go's `baseCache.updateTime` field, `None` while it holds the zero time.
    #[must_use]
    pub fn update_time(&self) -> Option<Instant> {
        self.update_time
    }
}
