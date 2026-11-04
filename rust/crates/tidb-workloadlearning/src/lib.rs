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

//! Go `pkg/workloadlearning`: the Workload-Based Learning Optimizer's table
//! read-cost analysis over the workload repository's statement history
//! (`handle`), its `mysql.tidb_workload_values` cache (`cache`) and the
//! metrics they exchange (`metrics`).

pub mod cache;
pub mod handle;
pub mod metrics;

pub use cache::{TableReadCostCache, WlCacheWorker};
pub use handle::{accumulate_metrics_group_by_table_id, Handle};
pub use metrics::TableReadCostMetrics;

use tidb_datatype::Datum;
use tidb_sqlescape::SqlArg;

/// The operations Go's package performs on a `sysSessionPool` session.
pub trait Session {
    /// Go `GetRestrictedSQLExecutor().ExecRestrictedSQL(ctx, nil, sql,
    /// args...)` under `kv.InternalTxnWorkloadLearning`.
    fn exec_restricted_sql(&self, sql: &str, args: &[SqlArg<'_>]) -> Result<Vec<Vec<Datum>>, String>;
    /// Go `sessiontxn.NewTxn`.
    fn new_txn(&self) -> Result<(), String>;
    /// Go `Txn(true).StartTS()`.
    fn txn_start_ts(&self) -> Result<u64, String>;
    /// Go `txn.Commit`.
    fn commit_txn(&self) -> Result<(), String>;
    /// Go `GetSessionVars().EnableNonPreparedPlanCache = true`.
    fn enable_non_prepared_plan_cache(&self);
}

/// Go `util.DestroyableSessionPool` as the package uses it: `Get`, then `Put`
/// when `f` succeeds or `Destroy` when it fails.
pub trait SessionPool: Send + Sync {
    /// Runs `f` on a pooled session. An `Err` from getting the session is
    /// Go's failed `Get`.
    fn with_session(&self, f: &mut dyn FnMut(&dyn Session) -> Result<(), String>) -> Result<(), String>;
}
