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

//! Source-backed contract tests for `StatsPool`.
//!
//! `pkg/statistics/handle/util/util_test.go:75` (`TestCallSCtxFailed`)
//! obtains the statistics handle's session pool before exercising callback
//! cleanup.  Session checkout, callback execution, and internal-session
//! cleanup remain external to this resource-access boundary.

use tidb_stats::StatsPool;

#[derive(Default)]
struct MockPool {
    goroutines: u32,
    sessions: u64,
    closed: bool,
}

impl StatsPool<u32, u64> for MockPool {
    fn gpool(&self) -> &u32 {
        &self.goroutines
    }

    fn spool(&self) -> &u64 {
        &self.sessions
    }

    fn close(&mut self) {
        self.closed = true;
    }
}

#[test]
fn source_stats_pool_exposes_both_resources_and_close() {
    let mut pool = MockPool {
        goroutines: 16,
        sessions: 3,
        closed: false,
    };
    let dynamic: &dyn StatsPool<u32, u64> = &pool;
    assert_eq!(dynamic.gpool(), &16);
    assert_eq!(dynamic.spool(), &3);

    pool.close();
    assert!(pool.closed);
}

#[test]
fn source_stats_pool_allows_distinct_opaque_resource_types() {
    let pool = MockPool {
        goroutines: 1,
        sessions: 2,
        closed: false,
    };
    assert_eq!(*pool.gpool(), 1);
    assert_eq!(*pool.spool(), 2);
}
