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

//! Source-backed tests for advisory-lock reference state.

use tidb_exec::advisory_lock_state::AdvisoryLockState;

#[test]
fn advisory_lock_references_match_get_lock_release_lock_contract() {
    // Source: pkg/session/advisory_locks.go:37-58 and
    // pkg/expression/integration_test/integration_test.go:1441-1581
    // (TestGetLock's repeated acquisition/release cases).
    let mut lock = AdvisoryLockState::new(42);
    assert_eq!(lock.owner(), 42);
    assert_eq!(lock.reference_count(), 0);

    // GET_LOCK may be called repeatedly for one lock; release reaches zero
    // only after the matching number of RELEASE_LOCK calls.
    lock.incr_references();
    lock.incr_references();
    assert_eq!(lock.reference_count(), 2);
    lock.decr_references();
    assert_eq!(lock.reference_count(), 1);
    lock.decr_references();
    assert_eq!(lock.reference_count(), 0);
}

#[test]
fn advisory_lock_reference_counter_preserves_source_integer_behavior() {
    // Source: pkg/session/advisory_locks.go:45-57.
    let mut lock = AdvisoryLockState::new(7);
    lock.decr_references();
    assert_eq!(lock.reference_count(), -1);
    lock.incr_references();
    assert_eq!(lock.reference_count(), 0);
}
