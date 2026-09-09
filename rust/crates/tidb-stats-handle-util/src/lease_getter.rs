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

use std::sync::atomic::{AtomicI64, Ordering};

/// Go `LeaseGetter`; values are signed nanoseconds, the representation of
/// Go's `time.Duration`.
pub trait LeaseGetter: Send + Sync {
    /// Go `Lease`.
    fn lease(&self) -> i64;
    /// Go `SetLease`.
    fn set_lease(&self, lease: i64);
}

/// Go's private `leaseGetter`.
pub struct StatsLease {
    lease: AtomicI64,
}

impl StatsLease {
    /// Go `NewLeaseGetter`.
    pub const fn new(lease: i64) -> Self {
        Self {
            lease: AtomicI64::new(lease),
        }
    }
}

impl LeaseGetter for StatsLease {
    fn lease(&self) -> i64 {
        self.lease.load(Ordering::SeqCst)
    }

    fn set_lease(&self, lease: i64) {
        self.lease.store(lease, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::{LeaseGetter, StatsLease};

    #[deny(unused_must_use)]
    #[test]
    fn source_return_values_may_be_ignored_like_go() {
        StatsLease::new(0);
    }

    #[test]
    fn signed_duration_round_trips_atomically() {
        let lease = StatsLease::new(1_000_000_000);
        assert_eq!(lease.lease(), 1_000_000_000);
        lease.set_lease(-1);
        assert_eq!(lease.lease(), -1);
        lease.set_lease(i64::MIN);
        assert_eq!(lease.lease(), i64::MIN);
    }
}
