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

//! Source-backed tests for worker capacity metadata.

use tidb_stats::{worker_capacity_available, worker_concurrency_changed};

#[test]
fn source_worker_admits_below_capacity_and_rejects_at_capacity() {
    assert!(worker_capacity_available(0, 2));
    assert!(worker_capacity_available(1, 2));
    assert!(!worker_capacity_available(2, 2));
    assert!(!worker_capacity_available(3, 2));
}

#[test]
fn source_worker_handles_zero_and_negative_limits() {
    assert!(!worker_capacity_available(0, 0));
    assert!(!worker_capacity_available(0, -1));
}

#[test]
fn source_worker_concurrency_update_is_noop_only_when_equal() {
    assert!(!worker_concurrency_changed(2, 2));
    assert!(worker_concurrency_changed(2, 3));
    assert!(worker_concurrency_changed(3, 2));
}
