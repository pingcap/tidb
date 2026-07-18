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

//! Source-backed tests for system-session pool capacity policy.

use tidb_exec::session_pool_capacity::{normalize_pool_capacity, POOL_MAX_SIZE};

#[test]
fn session_pool_capacity_matches_source_boundaries() {
    // Source: pkg/session/syssession/pool.go:32-35,65-80 and
    // pkg/session/syssession/pool_test.go:38-75 (TestNewSessionPool).
    assert_eq!(POOL_MAX_SIZE, 1024 * 1024 * 1024);
    assert_eq!(normalize_pool_capacity(128), 128);
    assert_eq!(normalize_pool_capacity(POOL_MAX_SIZE as i64), POOL_MAX_SIZE);
    assert_eq!(normalize_pool_capacity(0), POOL_MAX_SIZE);
    assert_eq!(normalize_pool_capacity(-1), POOL_MAX_SIZE);
    assert_eq!(
        normalize_pool_capacity(POOL_MAX_SIZE as i64 + 1),
        POOL_MAX_SIZE
    );
}

#[test]
fn session_pool_capacity_accepts_every_positive_in_range_value() {
    // Source: pkg/session/syssession/pool.go:65-70.
    for requested in [1_i64, 2, 1024, 1_048_576, POOL_MAX_SIZE as i64 - 1] {
        assert_eq!(normalize_pool_capacity(requested), requested as usize);
    }
}
