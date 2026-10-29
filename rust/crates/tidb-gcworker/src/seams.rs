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

//! The interfaces Go's `GCWorker` reaches its collaborators through: the
//! store (`kv.Storage`), PD (`pd.Client` and `pdgc.InternalController`), the
//! TiKV client (`tikv.Storage`'s client, the range-task runner and the region
//! lock resolver), internal sessions (`sessionapi.Session` with the
//! `pkg/ddl/util` helpers that take one) and `infosync`'s rule services.
//! Production implements them over the real cluster; tests over doubles, as
//! Go's tests hijack the client and the lock resolver.

use std::fmt;
use std::time::Duration;

use tikv_client::proto::keyspacepb::KeyspaceMeta;
use tikv_client::proto::metapb;

use tidb_ddl::ddl_delete_range::DelRangeTask;

/// An error from the GC worker or one of its collaborators, carrying the
/// message Go's `errors.Trace` would have carried.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GcError(pub String);

impl GcError {
    /// An error with `message`.
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for GcError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for GcError {}

/// The worker's result type.
pub type GcResult<T> = Result<T, GcError>;

/// Go `pdgc.AdvanceTxnSafePointResult`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AdvanceTxnSafePointResult {
    /// Go `OldTxnSafePoint`.
    pub old_txn_safe_point: u64,
    /// Go `Target`.
    pub target: u64,
    /// Go `NewTxnSafePoint`.
    pub new_txn_safe_point: u64,
    /// Go `BlockerDescription`.
    pub blocker_description: String,
}

/// Go `pdgc.AdvanceGCSafePointResult`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AdvanceGcSafePointResult {
    /// Go `OldGCSafePoint`.
    pub old_gc_safe_point: u64,
    /// Go `Target`.
    pub target: u64,
    /// Go `NewGCSafePoint`.
    pub new_gc_safe_point: u64,
}

/// What the worker reads from Go's `kv.Storage`.
pub trait GcStore: Send + Sync {
    /// Go `CurrentVersion(kv.GlobalTxnScope).Ver`: a fresh TSO.
    fn current_version(&self) -> GcResult<u64>;
    /// Go `GetCodec().GetKeyspaceMeta()`; `None` for the null keyspace.
    fn keyspace_meta(&self) -> Option<KeyspaceMeta>;
}

/// The PD calls of Go's `pd.Client` and the keyspace's
/// `pdgc.InternalController` that the worker makes.
pub trait GcPdClient: Send + Sync {
    /// Go `GetAllStores`.
    fn get_all_stores(&self) -> GcResult<Vec<metapb::Store>>;
    /// Go `UpdateGCSafePoint`; with 0 it reads the current GC safe point.
    fn update_gc_safe_point(&self, safe_point: u64) -> GcResult<u64>;
    /// Go `UpdateServiceGCSafePoint`: the minimal service safe point.
    fn update_service_gc_safe_point(
        &self,
        service_id: &str,
        ttl: i64,
        safe_point: u64,
    ) -> GcResult<u64>;
    /// Go `InternalController.AdvanceTxnSafePoint`.
    fn advance_txn_safe_point(&self, target: u64) -> GcResult<AdvanceTxnSafePointResult>;
    /// Go `InternalController.AdvanceGCSafePoint`.
    fn advance_gc_safe_point(&self, target: u64) -> GcResult<AdvanceGcSafePointResult>;
    /// Go `GetAllKeyspaces(ctx, startID, limit)`.
    fn get_all_keyspaces(&self, start_id: u32, limit: u32) -> GcResult<Vec<KeyspaceMeta>>;
}

/// Go `rangetask.TaskStat`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RangeTaskStat {
    /// Go `CompletedRegions`.
    pub completed_regions: usize,
    /// Go `FailedRegions`.
    pub failed_regions: usize,
}

/// Go's `rangetask.RangeTaskRunner` whose handler is
/// `tikv.ResolveLocksForRange` over the worker's region lock resolver.
pub trait ResolveLocksRunner {
    /// Go `RunOnRange`: resolves every lock with `start_ts <= max_version`
    /// in `[start, end)` (an empty `end` is unbounded).
    fn run_on_range(&self, start: &[u8], end: &[u8]) -> GcResult<()>;
    /// Go `CompletedRegions`.
    fn completed_regions(&self) -> usize;
}

/// The TiKV half of Go's `tikv.Storage` the worker uses.
pub trait GcKvClient: Send + Sync {
    /// Go `GetTiKVClient().SendRequest(ctx, address, CmdUnsafeDestroyRange)`:
    /// `Ok(None)` is a nil response, `Ok(Some(error))` the response's
    /// `Error` field (empty on success).
    fn unsafe_destroy_range(
        &self,
        address: &str,
        start: &[u8],
        end: &[u8],
        timeout: Duration,
    ) -> GcResult<Option<String>>;
    /// Go `rangetask.NewDeleteRangeTask(store, start, end, concurrency).Execute`.
    fn delete_range_task(&self, start: &[u8], end: &[u8], concurrency: usize) -> GcResult<()>;
    /// Go `rangetask.NewRangeTaskRunner(name, store, concurrency, handler)`
    /// with the handler of `resolveLocks`: `max_version` is the inclusive
    /// start-ts bound and `scan_limit` the per-request lock limit.
    fn resolve_locks_runner(
        &self,
        name: &str,
        concurrency: usize,
        max_version: u64,
        scan_limit: u32,
    ) -> Box<dyn ResolveLocksRunner + '_>;
    /// Go `doGCForRegion`'s `SendReq(bo, CmdGC, region, gcTimeout)`: the
    /// region error the caller must handle, or `Ok(None)` on success, or an
    /// error, never both.
    fn gc_region(
        &self,
        region: &tikv_client::RegionVerId,
        safe_point: u64,
    ) -> GcResult<Option<tikv_client::proto::errorpb::Error>>;
}

/// The internal session Go's `createSession` returns (`sessionapi.Session`
/// with privilege checks off and `InRestrictedSQL`), together with the
/// `pkg/ddl/util` and `pkg/ddl` helpers the worker calls on it.
pub trait GcSession {
    /// Go `ExecuteInternal(ctx, "BEGIN")`.
    fn begin(&mut self) -> GcResult<()>;
    /// Go `CommitTxn`.
    fn commit(&mut self) -> GcResult<()>;
    /// Go `RollbackTxn`.
    fn rollback(&mut self);
    /// Go `loadValueFromSysTable`'s statement: `SELECT HIGH_PRIORITY
    /// (variable_value) FROM mysql.tidb WHERE variable_name=%? FOR UPDATE`;
    /// `None` when the row is missing.
    fn load_value(&mut self, key: &str) -> GcResult<Option<String>>;
    /// Go `saveValueToSysTable`'s statement: `INSERT HIGH_PRIORITY INTO
    /// mysql.tidb VALUES (%?, %?, %?) ON DUPLICATE KEY UPDATE
    /// variable_value = %?, comment = %?`.
    fn save_value(&mut self, key: &str, value: &str, comment: &str) -> GcResult<()>;
    /// Go `util.LoadDeleteRanges` (`done == false`) or
    /// `util.LoadDoneDeleteRanges` (`done == true`): the ranges with
    /// `ts < safe_point`.
    fn load_delete_ranges(&mut self, done: bool, safe_point: u64) -> GcResult<Vec<DelRangeTask>>;
    /// Go `util.CompleteDeleteRange(se, dr, needToRecordDone)`.
    fn complete_delete_range(&mut self, range: &DelRangeTask, record_done: bool) -> GcResult<()>;
    /// Go `util.DeleteDoneRecord`.
    fn delete_done_record(&mut self, range: &DelRangeTask) -> GcResult<()>;
    /// Go `util.IsRaftKv2`.
    fn is_raft_kv2(&mut self) -> GcResult<bool>;
    /// Go `ddl.GetHistoryJobByID`.
    fn history_job(&mut self, job_id: i64) -> GcResult<Option<tidb_model::Job>>;
}

/// Go `createSession(store)`, which retries until a session opens.
pub trait GcSessionFactory: Send + Sync {
    /// A fresh internal session.
    fn create_session(&self) -> Box<dyn GcSession + '_>;
}

/// The `infosync` calls of `doGCPlacementRules` and `doGCLabelRules`.
pub trait GcRuleClient: Send + Sync {
    /// Go `infosync.DeleteTiFlashPlacementRules`.
    fn delete_tiflash_placement_rules(&self, physical_table_ids: &[i64]) -> GcResult<()>;
    /// Go `infosync.PutRuleBundlesWithDefaultRetry`.
    fn put_rule_bundles(&self, bundles: Vec<tidb_placement::Bundle>) -> GcResult<()>;
    /// Go `infosync.GetLabelRules`, keyed by rule id.
    fn get_label_rules(
        &self,
        rule_ids: &[String],
    ) -> GcResult<std::collections::HashMap<String, tidb_ddl_label::Rule>>;
    /// Go `infosync.UpdateLabelRules(label.NewRulePatch([], rule_ids))`.
    fn delete_label_rules(&self, rule_ids: Vec<String>) -> GcResult<()>;
}

/// Go `extworkload.Manager`, the GC v2 hand-off of `notifyGCV2AfterGC`.
pub trait GcV2Manager: Send + Sync {
    /// Go `Role()`.
    fn role(&self) -> tidb_config::external_workload::ExternalWorkloadRole;
    /// Go `Meta()`.
    fn meta(&self) -> Option<KeyspaceMeta>;
    /// Go `RegisterGCV2`.
    fn register_gcv2(&self, safe_point: u64, gc_life_time: Duration) -> GcResult<()>;
    /// Go `RecycleGCV2`.
    fn recycle_gcv2(&self, safe_point: u64) -> GcResult<()>;
}
