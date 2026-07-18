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

//! Atomic statistics lease state from
//! `pkg/statistics/handle/util/lease_getter.go`.
//!
//! TiDB stores `time.Duration` as signed nanoseconds in an atomic value. This
//! leaf keeps that caller-visible get/set state independent of the statistics
//! handle, lease scheduling, and time conversion layers.

use std::sync::atomic::{AtomicI64, Ordering};

/// Thread-safe signed nanosecond lease value.
pub struct StatsLease {
    lease_nanos: AtomicI64,
}

impl StatsLease {
    /// Creates a lease initialized to the supplied duration in nanoseconds.
    #[must_use]
    pub const fn new(lease_nanos: i64) -> Self {
        Self {
            lease_nanos: AtomicI64::new(lease_nanos),
        }
    }

    /// Loads the current duration in nanoseconds.
    #[must_use]
    pub fn lease_nanos(&self) -> i64 {
        self.lease_nanos.load(Ordering::SeqCst)
    }

    /// Replaces the current duration in nanoseconds.
    pub fn set_lease_nanos(&self, lease_nanos: i64) {
        self.lease_nanos.store(lease_nanos, Ordering::SeqCst);
    }
}
