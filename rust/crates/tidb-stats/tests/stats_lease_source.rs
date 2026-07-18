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

//! Source-backed tests for the atomic statistics lease value.

use tidb_stats::StatsLease;

#[test]
fn source_lease_get_and_set_preserve_duration_value() {
    let lease = StatsLease::new(1_000_000_000);
    assert_eq!(lease.lease_nanos(), 1_000_000_000);

    lease.set_lease_nanos(1_000_000);
    assert_eq!(lease.lease_nanos(), 1_000_000);

    lease.set_lease_nanos(-1);
    assert_eq!(lease.lease_nanos(), -1);
}

#[test]
fn source_lease_keeps_signed_nanosecond_boundaries() {
    let lease = StatsLease::new(i64::MIN);
    assert_eq!(lease.lease_nanos(), i64::MIN);
    lease.set_lease_nanos(i64::MAX);
    assert_eq!(lease.lease_nanos(), i64::MAX);
}
