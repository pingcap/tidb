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

use crate::StmtContext;

/// Go physicalplantest.TestAllocMPPID, moved to the Rust statement owner.
#[test]
fn alloc_mpp_task_id_increments_one_per_call_from_fresh_context() {
    let context = StmtContext::for_query();
    let info = context.mpp_query_info();
    assert_eq!(info.alloc_task_id(), 1);
    assert_eq!(info.alloc_task_id(), 2);
    assert_eq!(info.alloc_task_id(), 3);
}

#[test]
fn concurrent_gathers_share_query_identity_and_unique_counters() {
    let context = StmtContext::for_query();
    // Configuration detachment must retain the same statement effects.
    let detached = context.clone().with_connection_id(Some(17));
    let results = std::thread::scope(|scope| {
        (0..32)
            .map(|_| {
                let statement = crate::remote_scan::pushdown_statement_context(&detached);
                scope.spawn(move || {
                    let info = statement.mpp_query_info;
                    (
                        info.query_id(42),
                        info.alloc_gather_id(),
                        info.alloc_task_id(),
                    )
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect::<Vec<_>>()
    });
    let id = context.mpp_query_info().query_id(42);
    assert!(results.iter().all(|(query, _, _)| *query == id));
    let mut gathers = results
        .iter()
        .map(|(_, gather, _)| *gather)
        .collect::<Vec<_>>();
    let mut tasks = results.iter().map(|(_, _, task)| *task).collect::<Vec<_>>();
    gathers.sort_unstable();
    tasks.sort_unstable();
    assert_eq!(gathers, (1..=32).collect::<Vec<_>>());
    assert_eq!(tasks, (1..=32).collect::<Vec<_>>());
    assert_eq!(context.mpp_query_info().alloc_gather_id(), 33);
    assert_eq!(context.mpp_query_info().alloc_task_id(), 33);
}
