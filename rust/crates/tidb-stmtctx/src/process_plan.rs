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

//! What a running statement publishes about its plan for the process list
//! and the statement summary (Go `StatementContext`'s plan fields and
//! `ProcessInfo.Plan`).

use std::collections::BTreeMap;
use std::sync::Arc;

/// The physical plan a connection last executed, shared with
/// `EXPLAIN FOR CONNECTION` (Go's `ProcessInfo.Plan`, an `any` there too):
/// the planner's plan, which the statement context does not need to know.
#[derive(Clone)]
pub struct StoredPlan(Arc<dyn std::any::Any + Send + Sync>);

impl StoredPlan {
    /// Stores `plan`.
    pub fn new<T: std::any::Any + Send + Sync>(plan: Arc<T>) -> Self {
        Self(plan)
    }

    /// The stored plan as `T`, when it is one.
    #[must_use]
    pub fn get<T: std::any::Any + Send + Sync>(&self) -> Option<Arc<T>> {
        Arc::clone(&self.0).downcast::<T>().ok()
    }
}

impl std::fmt::Debug for StoredPlan {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("StoredPlan")
    }
}

impl PartialEq for StoredPlan {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

/// Plan-derived fields Go publishes on `sessmgr.ProcessInfo` when the
/// ordinary executor is built.
/// Go `ExecStmt`'s `LazyInfo.GetEncodedPlan` / `GetBinaryPlan` for a plan
/// published without its encodings: renders `(encoded, binary)` only when a
/// consumer (the statement summary's sample, the slow log) needs them.
#[derive(Clone)]
pub struct PlanSource(pub Arc<dyn Fn() -> Option<(String, String)> + Send + Sync>);

impl PlanSource {
    /// The `(encoded, binary)` plans, empty when the plan cannot be rendered.
    #[must_use]
    pub fn render(&self) -> (String, String) {
        (self.0)().unwrap_or_default()
    }
}

impl std::fmt::Debug for PlanSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("PlanSource")
    }
}

impl PartialEq for PlanSource {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

/// Go `SetProcessInfo(..., mysql.ComSleep, ...)`'s `GetBriefBinaryPlan(p)`
/// once the statement ended: re-encodes the published plan's brief binary
/// plan with the runtime stats and memory its execution left.
#[derive(Clone)]
pub struct BriefPlanSource(pub Arc<dyn Fn() -> String + Send + Sync>);

impl std::fmt::Debug for BriefPlanSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("BriefPlanSource")
    }
}

impl PartialEq for BriefPlanSource {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

/// The plan facts a statement publishes for its process-list row and its
/// statement-summary entry.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ProcessPlanInfo {
    /// Go `ProcessInfo.BriefBinaryPlan`.
    pub brief_binary_plan: String,
    /// Re-encodes [`Self::brief_binary_plan`] when the statement ends.
    pub brief_plan_source: Option<BriefPlanSource>,
    /// [`Self::brief_binary_plan`] of the previous statement.
    pub previous_brief_binary_plan: String,
    /// Go's `ProcessInfo.Plan` is an `Execute` plan: the statement is a text
    /// `EXECUTE`.
    pub in_execute: bool,
    /// [`Self::in_execute`] of the previous statement.
    pub previous_in_execute: bool,
    /// [`Self::plan`] is the last valid plan a statement without one kept
    /// (Go `SetProcessInfo` while `currentPlan` is nil).
    pub plan_retained: bool,

    /// Go `StmtCtx.GetPlanDigest` normalized text: the select plan with
    /// constants and statistics removed (empty without a physical select plan).
    pub normalized_plan: String,
    /// Go `StmtCtx.GetPlanDigest` digest: the sha256 of [`Self::normalized_plan`]
    /// in lowercase hex, empty when there is no normalized plan.
    pub plan_digest: String,
    /// Go `GenHintsFromFlatPlan` over the retained plan: the hints that
    /// reproduce it, for the statement summary's `PLAN_HINT`.
    pub plan_hints: Vec<tidb_ast::Hint>,
    /// Go executor `IsFastPlan` (one optional projection over PointGet/TableDual).
    pub is_fast_plan: bool,
    /// Go `StmtCtx.TableIDs`.
    pub table_ids: Vec<i64>,
    /// Go `StmtCtx.IndexNames`.
    pub index_names: Vec<String>,
    /// Go `StmtCtx.IsDDLJobInQueue`: the statement's DDL job is queued and
    /// the session waits for it.
    pub is_ddl_job_in_queue: bool,
    /// Go `physicalop.GetStatsInfo(ProcessInfo.Plan)`.
    pub stats_info: std::collections::HashMap<String, u64>,
    /// Go `ProcessInfo.MaxExecutionTime` as `ExecStmt.Exec` publishes it:
    /// the statement's `max_execution_time` in milliseconds, zero outside a
    /// SELECT.
    pub max_execution_time: u64,
    /// Go `ProcessInfo.Time` as `ExecStmt.Exec` republishes it: when
    /// execution began, after planning, which `max_execution_time` counts
    /// from.
    pub execution_started: Option<(std::time::Instant, chrono::DateTime<chrono::Utc>)>,
    /// Go `ProcessInfo.Plan`: the plan, for `EXPLAIN FOR CONNECTION`.
    pub plan: Option<StoredPlan>,
    /// The plan the connection's previous statement published; Go keeps it
    /// on the process info until the next statement replaces it.
    pub previous_plan: Option<StoredPlan>,
    /// Go `ProcessInfo.RuntimeStatsColl`: the statement's runtime stats
    /// collector, which `EXPLAIN FOR CONNECTION` renders [`Self::plan`] with.
    pub runtime_stats_coll: Option<StoredPlan>,
    /// [`Self::runtime_stats_coll`] of the previous statement, kept with
    /// [`Self::previous_plan`].
    pub previous_runtime_stats_coll: Option<StoredPlan>,
    /// The statement's memory, whose trackers Go's plan reads through its
    /// `SCtx()` (`getRuntimeInfo`'s `StmtCtx.MemTracker` / `DiskTracker`).
    pub statement_memory: Option<StoredPlan>,
    /// Go's process plan is one `EXPLAIN FOR CONNECTION` renders as no rows:
    /// an `Explain` plan, or the plan of a statement this port runs without a
    /// physical one (SET, USE, DDL, transaction control).
    pub explain_marker: bool,
    /// [`Self::explain_marker`] of the previous statement.
    pub previous_explain: bool,
    /// Go `StmtCtx.GetUsedStatsInfo(false)`: the statistics planning used,
    /// keyed by physical table id, for the slow log's `Stats`.
    pub used_stats: BTreeMap<i64, crate::used_stats::UsedStatsInfoForTable>,
    /// Go `StmtCtx.IsSyncStatsFailed`.
    pub is_sync_stats_failed: bool,
    /// Go `StmtCtx.IsTiFlash` as the planner set it (`buildDataSource`);
    /// the executor builder's readers add theirs from the plan.
    pub is_tiflash: bool,
    /// Go `getEncodedPlan` and `getBinaryPlan`: the statement's encoded and
    /// binary plans, rendered when the slow log or the statement summary asks,
    /// after the statement ran, so they carry its runtime statistics.
    pub plan_source: Option<PlanSource>,
    /// Go `ProcessInfo.RunawayChecker`'s `CheckRuleKillAction`, set once the
    /// statement's checker is derived.
    pub runaway_rule_kill_action: Option<RunawayRuleKillAction>,
}

/// Go `RunawayChecker.CheckRuleKillAction` as the process list calls it: the
/// exceed cause when the group settings kill the query.
#[derive(Clone)]
pub struct RunawayRuleKillAction(pub Arc<dyn Fn() -> Option<String> + Send + Sync>);

impl std::fmt::Debug for RunawayRuleKillAction {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("RunawayRuleKillAction")
    }
}

impl PartialEq for RunawayRuleKillAction {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
