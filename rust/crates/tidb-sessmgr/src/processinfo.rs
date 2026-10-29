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

//! Go `pkg/session/sessmgr/processinfo.go`: a connection's process entry and
//! the session manager interface the memory alarm, the server memory limit
//! and the expensive-query watchdog read.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use chrono::{DateTime, TimeZone, Utc};
use tidb_memory::Tracker;

/// Go zero `time.Time`: midnight UTC on January 1, year 1.
/// Go's zero `time.Time`, the "never recorded" timestamp.
pub fn zero_time() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(1, 1, 1, 0, 0, 0)
        .single()
        .expect("Go zero time is representable by chrono")
}

// boundary: Go `sessmgr.OOMAlarmVariablesInfo`.
/// OOM-alarm-relevant session variables captured on a process entry.
#[derive(Clone, Copy, Debug, Default)]
pub struct OOMAlarmVariablesInfo {
    /// Go `SessionAnalyzeVersion`.
    pub session_analyze_version: i64,
    /// Go `SessionEnabledRateLimitAction`.
    pub session_enabled_rate_limit_action: bool,
    /// Go `SessionMemQuotaQuery`.
    pub session_mem_quota_query: i64,
}

// boundary: Go `sessmgr.ProcessInfo`, narrowed to the fields
// `pkg/util/memoryusagealarm` and `pkg/util/servermemorylimit` read. Not
// modeled: `StmtCtx` (only its `AffectedRows()` survives as a plain field),
// `RefCountOfStmtCtx`, `SQLCPUUsage`, `Plan`, command/state enums.
/// Snapshot of one live connection, the shared seam over Go
/// `sessmgr.ProcessInfo`.
#[derive(Clone)]
pub struct ProcessInfo {
    /// Go `ID`.
    pub id: u64,
    /// Go `User`.
    pub user: String,
    /// Go `Host`.
    pub host: String,
    /// Go `Port`.
    pub port: String,
    /// Go `DB`.
    pub db: String,
    /// Go `Digest`.
    pub digest: String,
    /// Go `Info` (the SQL text).
    pub info: String,
    /// Go `RedactSQL`.
    pub redact_sql: tidb_parser::RedactMode,
    /// Go `Time` (statement start time).
    pub time: DateTime<Utc>,
    /// The monotonic reading embedded in a live Go `time.Time`. Test fixtures
    /// built with `time.Unix` have no such reading and leave this as `None`.
    pub started_instant: Option<Instant>,
    /// Go `MemTracker`.
    pub mem_tracker: Option<Arc<Tracker>>,
    /// Go `DiskTracker`.
    pub disk_tracker: Option<Arc<Tracker>>,
    /// Go `CurTxnStartTS`.
    pub cur_txn_start_ts: u64,
    /// Go `ResourceGroupName`.
    pub resource_group_name: String,
    /// Go `SessionAlias`.
    pub session_alias: String,
    /// Go `BriefBinaryPlan`.
    pub brief_binary_plan: String,
    /// Go `TableIDs`.
    pub table_ids: Vec<i64>,
    /// Go `IndexNames`.
    pub index_names: Vec<String>,
    // boundary: Go `StatsInfo` is a closure over `Plan`; narrowed to its
    // materialized result.
    /// Materialized Go `StatsInfo(info.Plan)` result.
    pub stats_info: HashMap<String, u64>,
    // boundary: Go reads this through `StmtCtx.AffectedRows()`.
    /// Affected row count of the current statement.
    pub affected_rows: u64,
    /// Go `OOMAlarmVariablesInfo`.
    pub oom_alarm_variables_info: OOMAlarmVariablesInfo,
    // boundary: Go keeps the server-status bits and renders them with
    // `serverStatus2Str`; the process list publishes that text.
    /// Go `State`, as `SHOW PROCESSLIST` renders it.
    pub state: String,
    /// Go `MaxExecutionTime`: the running SELECT's `max_execution_time` in
    /// milliseconds, zero for every other statement.
    pub max_execution_time: u64,
    /// Go `CurTxnCreateTime`, as the monotonic reading of the current
    /// transaction's creation.
    pub cur_txn_create_time: Option<Instant>,
    // boundary: Go reads this through `StmtCtx.InRestrictedSQL`.
    /// Whether the statement is internal (restricted) SQL.
    pub in_restricted_sql: bool,
    // boundary: Go `RunawayChecker`, narrowed to `CheckRuleKillAction`.
    /// The statement's runaway watch rule: a kill cause when it must end.
    pub runaway_rule_kill_action: Option<Arc<dyn Fn() -> Option<String> + Send + Sync>>,
}

impl Default for ProcessInfo {
    fn default() -> Self {
        ProcessInfo {
            id: 0,
            user: String::new(),
            host: String::new(),
            port: String::new(),
            db: String::new(),
            digest: String::new(),
            info: String::new(),
            redact_sql: tidb_parser::RedactMode::Disabled,
            time: zero_time(),
            started_instant: None,
            mem_tracker: None,
            disk_tracker: None,
            cur_txn_start_ts: 0,
            resource_group_name: String::new(),
            session_alias: String::new(),
            brief_binary_plan: String::new(),
            table_ids: Vec::new(),
            index_names: Vec::new(),
            stats_info: HashMap::new(),
            affected_rows: 0,
            oom_alarm_variables_info: OOMAlarmVariablesInfo::default(),
            state: String::new(),
            max_execution_time: 0,
            cur_txn_create_time: None,
            in_restricted_sql: false,
            runaway_rule_kill_action: None,
        }
    }
}

// boundary: Go `sessmgr.Manager`, narrowed to the methods the memory alarm,
// server memory limit and expensive-query packages call;
// `ShowProcessList`'s `map[uint64]*ProcessInfo` flattens to a vec of
// snapshots.
/// The session-manager seam over Go `sessmgr.Manager`.
pub trait SessionManager: Send + Sync {
    /// Go `Manager.ShowProcessList`.
    fn show_process_list(&self) -> Vec<Arc<ProcessInfo>>;
    /// Go `Manager.GetProcessInfo`.
    fn get_process_info(&self, id: u64) -> Option<Arc<ProcessInfo>>;
    /// Go `Manager.Kill`: ends the connection, or only its running statement
    /// when `query`; `max_execution_time` and `runaway` name the watchdog that
    /// asked, which picks the statement's kill signal.
    fn kill(&self, connection_id: u64, query: bool, max_execution_time: bool, runaway: bool);
}
