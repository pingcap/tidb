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

//! Go `StmtCtx.MPPQueryInfo`: identity shared by all gathers of one statement.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use tidb_txnkv::MppQueryId;

// Go physicalop.AllocMPPQueryID starts the process counter at one.
static MPP_QUERY_ID: AtomicU64 = AtomicU64::new(1);

/// Query identity and allocation counters retained through statement retries.
/// The session releases this owner at statement completion; cloned contexts and
/// remote requests share it rather than copying or resetting the counters.
#[derive(Debug, Default)]
pub struct MppQueryInfo {
    query_id: AtomicU64,
    query_ts: AtomicU64,
    allocated_task_id: AtomicI64,
    allocated_gather_id: AtomicU64,
}

impl MppQueryInfo {
    /// Clears completed statement state when no reader still shares it.
    /// The exclusive borrow prevents resetting counters under a running task.
    pub fn reset(&mut self) {
        *self = Self::default();
    }

    /// Go executor.getMPPQueryID/getMPPQueryTS and executorBuilder's server ID.
    #[must_use]
    pub fn query_id(&self, server_id: u64) -> MppQueryId {
        let id = MPP_QUERY_ID.fetch_add(1, Ordering::SeqCst).wrapping_add(1);
        let _ = self
            .query_id
            .compare_exchange(0, id, Ordering::SeqCst, Ordering::SeqCst);
        let now = SystemTime::now().duration_since(UNIX_EPOCH).map_or_else(
            |error| (error.duration().as_nanos() as u64).wrapping_neg(),
            |elapsed| elapsed.as_nanos() as u64,
        );
        let _ = self
            .query_ts
            .compare_exchange(0, now, Ordering::SeqCst, Ordering::SeqCst);
        MppQueryId {
            local_query_id: self.query_id.load(Ordering::SeqCst),
            query_ts: self.query_ts.load(Ordering::SeqCst),
            server_id,
        }
    }

    /// Go physicalop.AllocMPPTaskID, shared across gathers of this statement.
    pub fn alloc_task_id(&self) -> i64 {
        self.allocated_task_id
            .fetch_add(1, Ordering::SeqCst)
            .wrapping_add(1)
    }

    /// Go mpp.allocMPPGatherID: each coordinator attempt gets a new gather.
    pub fn alloc_gather_id(&self) -> u64 {
        self.allocated_gather_id
            .fetch_add(1, Ordering::SeqCst)
            .wrapping_add(1)
    }
}

#[cfg(test)]
mod tests {
    use crate::{remote_scan::PushdownStatementContext, StmtContext};

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
                    let statement = PushdownStatementContext::from_stmt(&detached);
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
}
