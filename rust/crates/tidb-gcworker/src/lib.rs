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

//! Go `pkg/store/gcworker`: the GC worker. One TiDB of a cluster holds the
//! GC leader lease in `mysql.tidb`; on each tick it computes a safe point from
//! `tikv_gc_life_time`, advances PD's txn safe point, resolves every older
//! lock, destroys the key ranges dropped objects left in
//! `mysql.gc_delete_range` on every TiKV store, removes their placement and
//! label rules, and advances PD's GC safe point.
//!
//! The worker reaches the store, PD, TiKV, internal sessions and the rule
//! services through [`seams`]; the server implements them over the cluster.

mod gc_worker;
pub mod seams;

#[cfg(test)]
mod gc_worker_test;

pub use gc_worker::{
    calc_delete_range_concurrency, get_gc_rules, is_keyspace_using_keyspace_level_gc,
    needs_gc_operation_for_store, run_distributed_gc_job, run_gc_job, run_resolve_locks,
    tidb_vardef_scope, GcConcurrency, GcWorker, GcWorkerDeps, GcWorkerHandle, MockGcWorker,
    CONCURRENCY_DIVISOR, KEYSPACE_CONFIG_GC_MANAGEMENT_TYPE,
    KEYSPACE_CONFIG_GC_MANAGEMENT_TYPE_KEYSPACE_LEVEL, KEYSPACE_CONFIG_GC_MANAGEMENT_TYPE_UNIFIED,
    NULL_KEYSPACE_ID, REQUESTS_PER_THREAD, TIDB_GC_LAST_RUN_TIME, TIDB_GC_LEADER_DESC,
    TIDB_GC_LEADER_LEASE, TIDB_GC_LEADER_UUID, TIDB_GC_SAFE_POINT, TXN_SAFE_POINT_SYNC_WAIT_TIME,
};
