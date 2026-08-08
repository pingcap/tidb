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

//! `pkg/meta/model/job.go`: DDL job state, lifecycle rules, multi-schema proxy
//! jobs, scheduling involvement, and the persisted JSON envelope. TiDB error
//! payloads use the shared `tidb-error` compatible envelope, while tracing
//! metadata retains its typed string/base64/uint64 source contract.

use std::collections::BTreeMap;
use std::sync::OnceLock;

use serde_json::value::RawValue;
use tidb_error::terror::TerrorError;

use crate::action_type::ActionType;
use crate::db::DBInfo;
use crate::job_enums::{JobState, JobVersion};
use crate::reorg::{DDLReorgMeta, ReorgStage, ReorgType};
use crate::schema_state::SchemaState;
use crate::serde_helpers::GoJsonMerge;
use crate::table_info::TableInfo;

/// Go `AdminCommandOperator` (an `int`): who issued an admin DDL command.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct AdminCommandOperator(
    /// Persisted source ordinal.
    pub i64,
);

impl AdminCommandOperator {
    /// Unknown issuer (Go `AdminCommandByNotKnown`, the zero value).
    pub const BY_NOT_KNOWN: AdminCommandOperator = AdminCommandOperator(0);
    /// Issued by an end user (Go `AdminCommandByEndUser`).
    pub const BY_END_USER: AdminCommandOperator = AdminCommandOperator(1);
    /// Issued by the system (Go `AdminCommandBySystem`).
    pub const BY_SYSTEM: AdminCommandOperator = AdminCommandOperator(2);
}

impl std::fmt::Display for AdminCommandOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match *self {
            AdminCommandOperator::BY_END_USER => "EndUser",
            AdminCommandOperator::BY_SYSTEM => "System",
            _ => "None",
        })
    }
}

/// Go `InvolvingSchemaInfoMode` (an `int`): the lock mode for an involved
/// schema object.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct InvolvingSchemaInfoMode(
    /// Persisted lock-mode ordinal.
    pub i64,
);

impl InvolvingSchemaInfoMode {
    /// Exclusive involvement (Go `ExclusiveInvolving`, the zero value).
    pub const EXCLUSIVE: InvolvingSchemaInfoMode = InvolvingSchemaInfoMode(0);
    /// Shared involvement (Go `SharedInvolving`).
    pub const SHARED: InvolvingSchemaInfoMode = InvolvingSchemaInfoMode(1);
}

/// Go `InvolvingAll` (`"*"`): the wildcard for all databases/tables.
pub const INVOLVING_ALL: &str = "*";
/// Go `InvolvingNone` (`""`): no involvement.
pub const INVOLVING_NONE: &str = "";
/// Go `JobPauseReasonKVDiskFull`.
pub const JOB_PAUSE_REASON_KV_DISK_FULL: &str = "tikv_disk_full";
/// Go `JobResumeReasonKVDiskFull`.
pub const JOB_RESUME_REASON_KV_DISK_FULL: &str = "tikv_disk_full";

/// Owned Go `json.RawMessage` content used by persisted job arguments.
///
/// The exact valid JSON text is retained on decode. Encoding through the
/// package's Go formatter compacts only insignificant whitespace, preserving
/// duplicate keys, member order, and numeric lexical forms like Go Marshal.
#[derive(Clone, Debug)]
pub struct PersistedRawJson(Box<RawValue>);

impl PersistedRawJson {
    /// Validates and owns exact JSON text.
    pub fn from_string(json: String) -> Result<Self, serde_json::Error> {
        RawValue::from_string(json).map(Self)
    }

    /// Returns the exact decoded JSON text before outer-marshaler compaction.
    #[must_use]
    pub fn get(&self) -> &str {
        self.0.get()
    }

    fn from_value(value: &serde_json::Value) -> Result<Self, serde_json::Error> {
        serde_json::value::to_raw_value(value).map(Self)
    }
}

impl PartialEq for PersistedRawJson {
    fn eq(&self, other: &Self) -> bool {
        self.get() == other.get()
    }
}

impl Eq for PersistedRawJson {}

impl serde::Serialize for PersistedRawJson {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serde::Serialize::serialize(&self.0, serializer)
    }
}

impl<'de> serde::Deserialize<'de> for PersistedRawJson {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        serde::Deserialize::deserialize(deserializer).map(Self)
    }
}

/// Go `InvolvingSchemaInfo`: a schema object a DDL job involves (for locking).
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct InvolvingSchemaInfo {
    /// The database name.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub database: String,
    /// The table name.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub table: String,
    /// The placement policy name.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub policy: String,
    /// The resource group name.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub resource_group: String,
    /// The involvement mode.
    #[serde(default, skip_serializing_if = "is_exclusive_mode")]
    pub mode: InvolvingSchemaInfoMode,
}

fn is_exclusive_mode(mode: &InvolvingSchemaInfoMode) -> bool {
    *mode == InvolvingSchemaInfoMode::EXCLUSIVE
}

/// Go `JobPauseReason`: why a DDL job was paused.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct JobPauseReason {
    /// The reason type.
    #[serde(rename = "type", default)]
    pub type_: String,
    /// The reason message.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message: String,
}

/// Go `JobResumeReason`: why a DDL job was resumed.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct JobResumeReason {
    /// The reason type.
    #[serde(rename = "type", default)]
    pub type_: String,
}

/// Go `HistoryInfo`: the schema snapshot recorded when a DDL job finishes.
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct HistoryInfo {
    /// The schema version after the job.
    #[serde(rename = "SchemaVersion", default)]
    pub schema_version: i64,
    /// The affected database, if any.
    #[serde(rename = "DBInfo", default)]
    pub db_info: Option<Box<DBInfo>>,
    /// The affected table, if any.
    #[serde(rename = "TableInfo", default)]
    pub table_info: Option<Box<TableInfo>>,
    /// The finish timestamp (a TSO).
    #[serde(rename = "FinishedTS", default)]
    pub finished_ts: u64,
    /// Multiple affected tables (for multi-table jobs).
    #[serde(rename = "MultipleTableInfos", default)]
    pub multiple_table_infos: Option<Vec<Option<TableInfo>>>,
}

impl HistoryInfo {
    /// Go `HistoryInfo.AddDBInfo`.
    pub fn add_db_info(&mut self, schema_version: i64, db_info: DBInfo) {
        self.schema_version = schema_version;
        self.db_info = Some(Box::new(db_info));
    }

    /// Go `HistoryInfo.AddTableInfo`.
    pub fn add_table_info(&mut self, schema_version: i64, table_info: TableInfo) {
        self.schema_version = schema_version;
        self.table_info = Some(Box::new(table_info));
    }

    /// Go `HistoryInfo.SetTableInfos`.
    pub fn set_table_infos(&mut self, schema_version: i64, table_infos: Vec<TableInfo>) {
        self.schema_version = schema_version;
        self.multiple_table_infos = Some(table_infos.into_iter().map(Some).collect());
    }

    /// Go `HistoryInfo.Clean`. `finished_ts` deliberately survives, as it does
    /// in Go.
    pub fn clean(&mut self) {
        self.schema_version = 0;
        self.db_info = None;
        self.table_info = None;
        self.multiple_table_infos = None;
    }
}

/// Go `JobMeta`: the subset of job metadata embedded by a backfill task.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct JobMeta {
    /// Schema identifier of the DDL job.
    #[serde(rename = "schema_id", default)]
    pub schema_id: i64,
    /// Table identifier of the DDL job.
    #[serde(rename = "table_id", default)]
    pub table_id: i64,
    /// DDL action type.
    #[serde(rename = "job_type", default)]
    pub type_: ActionType,
    /// Original DDL query text.
    #[serde(rename = "query", default)]
    pub query: String,
    /// Operation priority used by index creation.
    #[serde(rename = "priority", default)]
    pub priority: i64,
}

/// A resolved Go `time.Location` shape. Named IANA zones and explicit fixed
/// offsets remain distinct because the latter retains the caller's name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResolvedTimeZone {
    /// A named IANA time zone loaded from the time-zone database.
    Named(chrono_tz::Tz),
    /// A fixed offset retaining the source-provided zone name.
    Fixed {
        /// Caller-provided fixed-zone name.
        name: String,
        /// Offset in seconds east of UTC.
        offset_seconds: i64,
    },
}

impl ResolvedTimeZone {
    /// Returns the stable source-visible location name.
    #[must_use]
    pub fn name(&self) -> &str {
        match self {
            Self::Named(zone) => zone.name(),
            Self::Fixed { name, .. } => name,
        }
    }
}

/// Go `TimeZoneLocation`. The lazily initialized location cache is not
/// serialized and, like Go's mutex-protected pointer, remains stable even if
/// the public name/offset fields are modified after the first lookup.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TimeZoneLocation {
    /// IANA or fixed-zone name persisted by the DDL job.
    #[serde(rename = "name", default)]
    pub name: String,
    /// Fixed offset in seconds east of UTC; zero selects named-zone loading.
    #[serde(rename = "offset", default)]
    pub offset: i64,
    #[serde(skip)]
    location: OnceLock<ResolvedTimeZone>,
}

/// Go `tracing.TraceInfo`: persisted SQL tracing identity carried by a DDL job.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TraceInfo {
    /// Alias of the SQL session that created the job.
    #[serde(rename = "session_alias", default)]
    pub session_alias: String,
    /// Statement trace identifier, preserving nil versus allocated-empty bytes.
    #[serde(rename = "trace_id", default, with = "crate::serde_helpers::go_bytes")]
    pub trace_id: Option<Vec<u8>>,
    /// Connection identifier of the creating SQL session.
    #[serde(rename = "connection_id", default)]
    pub connection_id: u64,
}

/// Go `AddForeignKeyInfo` (runtime-only; no JSON fields in its owner).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AddForeignKeyInfo {
    /// Foreign-key constraint name.
    pub name: tidb_ast::CiString,
    /// Referencing column names.
    pub columns: Vec<tidb_ast::CiString>,
}

/// Go `MultiSchemaInfo`.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct MultiSchemaInfo {
    /// Ordered nullable sub-job pointers, with outer `None` retaining Go's
    /// nil-slice state.
    #[serde(rename = "sub_jobs", default)]
    pub sub_jobs: Option<Vec<Option<SubJob>>>,
    /// Whether every sub-job can still be reverted.
    #[serde(rename = "revertible", default)]
    pub revertible: bool,
    /// Sequence of the currently executing sub-job.
    #[serde(rename = "seq", default)]
    pub seq: i32,
    /// Runtime flag suppressing schema-version generation for a sub-job.
    #[serde(skip)]
    pub skip_version: bool,
    /// Runtime set of columns being added.
    #[serde(skip)]
    pub add_columns: Vec<tidb_ast::CiString>,
    /// Runtime set of columns being dropped.
    #[serde(skip)]
    pub drop_columns: Vec<tidb_ast::CiString>,
    /// Runtime set of columns being modified.
    #[serde(skip)]
    pub modify_columns: Vec<tidb_ast::CiString>,
    /// Runtime set of indexes being added.
    #[serde(skip)]
    pub add_indexes: Vec<tidb_ast::CiString>,
    /// Runtime set of indexes being dropped.
    #[serde(skip)]
    pub drop_indexes: Vec<tidb_ast::CiString>,
    /// Runtime set of indexes being altered.
    #[serde(skip)]
    pub alter_indexes: Vec<tidb_ast::CiString>,
    /// Runtime foreign keys being added.
    #[serde(skip)]
    pub add_foreign_keys: Vec<AddForeignKeyInfo>,
    /// Runtime columns referenced by positional clauses.
    #[serde(skip)]
    pub relative_columns: Vec<tidb_ast::CiString>,
    /// Runtime target columns for positional clauses.
    #[serde(skip)]
    pub position_columns: Vec<tidb_ast::CiString>,
}

impl Default for MultiSchemaInfo {
    fn default() -> Self {
        Self {
            sub_jobs: None,
            revertible: true,
            seq: 0,
            skip_version: false,
            add_columns: Vec::new(),
            drop_columns: Vec::new(),
            modify_columns: Vec::new(),
            add_indexes: Vec::new(),
            drop_indexes: Vec::new(),
            alter_indexes: Vec::new(),
            add_foreign_keys: Vec::new(),
            relative_columns: Vec::new(),
            position_columns: Vec::new(),
        }
    }
}

/// Go `SubJob` persisted fields.
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct SubJob {
    /// DDL action performed by this sub-job.
    #[serde(rename = "type", default)]
    pub type_: ActionType,
    /// Persisted delayed-decode argument envelope.
    #[serde(rename = "raw_args", default)]
    pub raw_args: Option<PersistedRawJson>,
    /// Current schema state reached by the sub-job.
    #[serde(rename = "schema_state", default)]
    pub schema_state: SchemaState,
    /// Snapshot timestamp used by reorganization work.
    #[serde(rename = "snapshot_ver", default)]
    pub snapshot_version: u64,
    /// TSO at which execution actually began.
    #[serde(rename = "real_start_ts", default)]
    pub real_start_ts: u64,
    /// Whether the sub-job can still be reverted.
    #[serde(rename = "revertible", default)]
    pub revertible: bool,
    /// Current sub-job lifecycle state.
    #[serde(rename = "state", default)]
    pub state: JobState,
    /// Rows processed by this sub-job.
    #[serde(rename = "row_count", default)]
    pub row_count: i64,
    /// Persisted warning payload from the sub-job.
    #[serde(rename = "warning", default)]
    pub warning: Option<TerrorError>,
    /// Runtime hint used by modify-column reorganization.
    #[serde(skip)]
    pub need_reorg: bool,
    /// Schema version produced by this sub-job.
    #[serde(rename = "schema_version", default)]
    pub schema_version: i64,
    /// Reorganization strategy selected for the sub-job.
    #[serde(rename = "reorg_tp", default)]
    pub reorg_type: ReorgType,
    /// Reorganization stage reached by the sub-job.
    #[serde(rename = "reorg_stage", default)]
    pub reorg_stage: ReorgStage,
    /// Analyze phase state stored with modify-column work.
    #[serde(rename = "analyze_state", default)]
    pub analyze_state: i8,
    #[serde(skip)]
    args: Option<Vec<serde_json::Value>>,
}

impl SubJob {
    /// Reports whether the sub-job is outside cancellation and rollback states.
    #[must_use]
    pub fn is_normal(&self) -> bool {
        !matches!(
            self.state,
            JobState::CANCELLING
                | JobState::CANCELLED
                | JobState::ROLLINGBACK
                | JobState::ROLLBACK_DONE
        )
    }

    /// Reports whether the sub-job reached a terminal state.
    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.state.is_finished()
    }

    /// Builds the parent-shaped proxy job used to execute this sub-job.
    #[must_use]
    pub fn to_proxy_job(&self, parent: &Job, sequence: i64) -> Job {
        let mut reorg_meta = parent.reorg_meta.clone();
        if let Some(meta) = &mut reorg_meta {
            meta.reorg_type = self.reorg_type;
            meta.stage = self.reorg_stage;
            meta.analyze_state = self.analyze_state;
        }
        Job {
            version: parent.version,
            id: parent.id,
            type_: self.type_,
            schema_id: parent.schema_id,
            table_id: parent.table_id,
            schema_name: parent.schema_name.clone(),
            state: self.state,
            warning: self.warning.clone(),
            row_count: self.row_count,
            need_reorg: self.need_reorg,
            args: self.args.clone(),
            raw_args: self.raw_args.clone(),
            schema_state: self.schema_state,
            snapshot_version: self.snapshot_version,
            real_start_ts: self.real_start_ts,
            start_ts: parent.start_ts,
            dependency_id: parent.dependency_id,
            query: parent.query.clone(),
            binlog_info: parent.binlog_info.clone(),
            reorg_meta,
            multi_schema_info: Some(MultiSchemaInfo {
                revertible: self.revertible,
                seq: sequence as i32,
                ..Default::default()
            }),
            priority: parent.priority,
            sequence_number: parent.sequence_number,
            charset: parent.charset.clone(),
            collate: parent.collate.clone(),
            admin_operator: parent.admin_operator,
            resume_reason: parent.resume_reason.clone(),
            trace_info: parent.trace_info.clone(),
            sql_mode: parent.sql_mode,
            session_vars: parent.session_vars.clone(),
            ..Default::default()
        }
    }

    /// Copies execution results from a proxy job back into this sub-job.
    pub fn from_proxy_job(&mut self, proxy: &Job, schema_version: i64) {
        self.revertible = proxy
            .multi_schema_info
            .as_ref()
            .is_some_and(|info| info.revertible);
        self.schema_state = proxy.schema_state;
        self.snapshot_version = proxy.snapshot_version;
        self.real_start_ts = proxy.real_start_ts;
        self.args = proxy.args.clone();
        self.state = proxy.state;
        self.warning = proxy.warning.clone();
        self.row_count = proxy.row_count;
        self.schema_version = schema_version;
        if let Some(meta) = &proxy.reorg_meta {
            self.reorg_type = meta.reorg_type;
            self.reorg_stage = meta.stage;
            self.analyze_state = meta.analyze_state;
        }
    }

    /// Go `SubJob.Clone`: copies persisted/runtime state but deliberately
    /// clears the private decoded-argument cache.
    #[must_use]
    pub fn clone_without_args(&self) -> Self {
        let mut cloned = self.clone();
        cloned.args = None;
        cloned
    }
}

impl TimeZoneLocation {
    /// Go `GetLocation`.
    pub fn get_location(&self) -> Result<ResolvedTimeZone, String> {
        if let Some(location) = self.location.get() {
            return Ok(location.clone());
        }
        let resolved = if self.offset != 0 {
            Ok(ResolvedTimeZone::Fixed {
                name: self.name.clone(),
                offset_seconds: self.offset,
            })
        } else {
            let canonical = if self.name.is_empty() {
                "UTC"
            } else {
                self.name.as_str()
            };
            canonical
                .parse::<chrono_tz::Tz>()
                .map(ResolvedTimeZone::Named)
                .map_err(|_| format!("unknown time zone {canonical}"))
        }?;
        let _ = self.location.set(resolved.clone());
        Ok(self.location.get().cloned().unwrap_or(resolved))
    }
}

/// Go `Job`: the persisted DDL operation envelope.
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Job {
    /// Persistent job identifier.
    #[serde(rename = "id", default)]
    pub id: i64,
    /// DDL action performed by the job.
    #[serde(rename = "type", default)]
    pub type_: ActionType,
    /// Schema identifier, whose exact role depends on the action type.
    #[serde(rename = "schema_id", default)]
    pub schema_id: i64,
    /// Table identifier, whose exact role depends on the action type.
    #[serde(rename = "table_id", default)]
    pub table_id: i64,
    /// Source schema name used by scheduling involvement fallback.
    #[serde(rename = "schema_name", default)]
    pub schema_name: String,
    /// Source table name used by scheduling involvement fallback.
    #[serde(rename = "table_name", default)]
    pub table_name: String,
    /// Current lifecycle state.
    #[serde(rename = "state", default)]
    pub state: JobState,
    /// Persisted warning payload.
    #[serde(rename = "warning", default)]
    pub warning: Option<TerrorError>,
    /// Persisted execution error payload.
    #[serde(rename = "err", default)]
    pub error: Option<TerrorError>,
    /// Number of execution errors observed.
    #[serde(rename = "err_count", default)]
    pub error_count: i64,
    /// Number of rows processed.
    #[serde(rename = "row_count", default)]
    pub row_count: i64,
    /// Runtime modify-column hint; not a precise persisted reorg decision.
    #[serde(skip)]
    pub need_reorg: bool,
    #[serde(skip)]
    args: Option<Vec<serde_json::Value>>,
    /// Persisted delayed-decode argument envelope.
    #[serde(rename = "raw_args", default)]
    pub raw_args: Option<PersistedRawJson>,
    /// Schema state reached by this job.
    #[serde(rename = "schema_state", default)]
    pub schema_state: SchemaState,
    /// Snapshot timestamp used by reorganization work.
    #[serde(rename = "snapshot_ver", default)]
    pub snapshot_version: u64,
    /// TSO at which execution actually began.
    #[serde(rename = "real_start_ts", default)]
    pub real_start_ts: u64,
    /// TSO allocated when the job entered the job table.
    #[serde(rename = "start_ts", default)]
    pub start_ts: u64,
    /// Largest earlier job identifier this job depends on.
    #[serde(rename = "dependency_id", default)]
    pub dependency_id: i64,
    /// Original DDL query text.
    #[serde(rename = "query", default)]
    pub query: String,
    /// Schema-history snapshot written when the job finishes.
    #[serde(rename = "binlog", default)]
    pub binlog_info: Option<HistoryInfo>,
    /// Persisted job argument encoding version.
    #[serde(rename = "version", default)]
    pub version: JobVersion,
    /// Reorganization execution metadata.
    #[serde(rename = "reorg_meta", default)]
    pub reorg_meta: Option<DDLReorgMeta>,
    /// Multi-schema sub-job state, when present.
    #[serde(rename = "multi_schema_info", default)]
    pub multi_schema_info: Option<MultiSchemaInfo>,
    /// Operation priority used by index creation.
    #[serde(rename = "priority", default)]
    pub priority: i64,
    /// Ordering key used when moving jobs into DDL history.
    #[serde(rename = "seq_num", default)]
    pub sequence_number: u64,
    /// Character set captured when the job was created.
    #[serde(rename = "charset", default)]
    pub charset: String,
    /// Collation captured when the job was created.
    #[serde(rename = "collate", default)]
    pub collate: String,
    #[serde(
        rename = "involving_schema_info",
        default,
        skip_serializing_if = "option_vec_is_none_or_empty"
    )]
    /// Explicit scheduling-lock objects; `None` activates name fallback.
    pub involving_schema_info: Option<Vec<InvolvingSchemaInfo>>,
    /// Origin of an administrative command.
    #[serde(rename = "admin_operator", default)]
    pub admin_operator: AdminCommandOperator,
    #[serde(
        rename = "pause_reason",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    /// Durable reason for a system-initiated pause.
    pub pause_reason: Option<JobPauseReason>,
    #[serde(
        rename = "resume_reason",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    /// Durable reason for explicit resume.
    pub resume_reason: Option<JobResumeReason>,
    /// SQL tracing metadata retained with Go's typed/base64 field contract.
    #[serde(rename = "trace_info", default)]
    pub trace_info: Option<TraceInfo>,
    /// BDR cluster role captured for this DDL.
    #[serde(rename = "bdr_role", default)]
    pub bdr_role: String,
    /// CDC write-source identifier.
    #[serde(rename = "cdc_write_source", default)]
    pub cdc_write_source: u64,
    /// Deprecated flag for execution on the client-connected TiDB.
    #[serde(rename = "local_mode", default)]
    pub local_mode: bool,
    /// SQL mode used to execute the DDL statement.
    #[serde(rename = "sql_mode", default)]
    pub sql_mode: u64,
    #[serde(
        rename = "session_vars",
        default,
        skip_serializing_if = "option_map_is_none_or_empty"
    )]
    /// Session system variables captured for DDL execution.
    pub session_vars: Option<BTreeMap<String, String>>,
    /// Latest schema version returned by the last execution step.
    #[serde(rename = "last_schema_version", default)]
    pub last_schema_version: i64,
}

/// Go `JobW`: a decoded job and the exact binary representation it came with.
#[derive(Clone, Debug)]
pub struct JobW {
    /// Decoded job value.
    pub job: Job,
    /// Exact original binary representation.
    pub bytes: Vec<u8>,
}

impl JobW {
    /// Go `NewJobW`. The byte vector is retained unchanged, including empty
    /// and non-JSON payloads; construction does not decode it.
    #[must_use]
    pub fn new(job: Job, bytes: Vec<u8>) -> Self {
        Self { job, bytes }
    }
}

impl std::ops::Deref for JobW {
    type Target = Job;

    fn deref(&self) -> &Self::Target {
        &self.job
    }
}

impl std::ops::DerefMut for JobW {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.job
    }
}

/// Borrowed warning and warning-count maps returned by [`Job::get_warnings`].
pub type JobWarningsRef<'a> = (
    Option<&'a BTreeMap<String, Option<TerrorError>>>,
    Option<&'a BTreeMap<String, i64>>,
);

impl Job {
    /// Sets the processed row count.
    pub fn set_row_count(&mut self, count: i64) {
        self.row_count = count;
    }

    /// Returns the processed row count.
    #[must_use]
    pub fn get_row_count(&self) -> i64 {
        self.row_count
    }

    /// Replaces the reorganization warning maps.
    pub fn set_warnings(
        &mut self,
        warnings: Option<BTreeMap<String, Option<TerrorError>>>,
        warning_counts: Option<BTreeMap<String, i64>>,
    ) {
        let metadata = self
            .reorg_meta
            .as_mut()
            .expect("Job.ReorgMeta is required by SetWarnings");
        metadata.warnings = warnings;
        metadata.warnings_count = warning_counts;
    }

    /// Borrows the reorganization warning maps.
    #[must_use]
    pub fn get_warnings(&self) -> JobWarningsRef<'_> {
        let metadata = self
            .reorg_meta
            .as_ref()
            .expect("Job.ReorgMeta is required by GetWarnings");
        (metadata.warnings.as_ref(), metadata.warnings_count.as_ref())
    }

    /// Marks a table job finished and records its schema-history snapshot.
    pub fn finish_table_job(
        &mut self,
        state: JobState,
        schema_state: SchemaState,
        version: i64,
        table: TableInfo,
    ) {
        self.state = state;
        self.schema_state = schema_state;
        self.binlog_info
            .as_mut()
            .expect("Job.BinlogInfo is required by FinishTableJob")
            .add_table_info(version, table);
    }

    /// Marks a multi-table job finished and records all affected tables.
    pub fn finish_multiple_table_job(
        &mut self,
        state: JobState,
        schema_state: SchemaState,
        version: i64,
        tables: Vec<TableInfo>,
    ) {
        self.state = state;
        self.schema_state = schema_state;
        let binlog = self
            .binlog_info
            .as_mut()
            .expect("Job.BinlogInfo is required by FinishMultipleTableJob");
        binlog.schema_version = version;
        binlog.table_info = Some(Box::new(
            tables
                .last()
                .expect("FinishMultipleTableJob requires at least one table")
                .clone(),
        ));
        binlog.multiple_table_infos = Some(tables.into_iter().map(Some).collect());
    }

    /// Marks a database job finished and records its database snapshot.
    pub fn finish_db_job(
        &mut self,
        state: JobState,
        schema_state: SchemaState,
        version: i64,
        database: DBInfo,
    ) {
        self.state = state;
        self.schema_state = schema_state;
        self.binlog_info
            .as_mut()
            .expect("Job.BinlogInfo is required by FinishDBJob")
            .add_db_info(version, database);
    }

    /// Makes a multi-schema job permanently non-revertible.
    pub fn mark_non_revertible(&mut self) {
        if let Some(info) = &mut self.multi_schema_info {
            info.revertible = false;
        }
    }

    /// Replaces the decoded generic argument cache used by [`Self::encode`].
    pub fn fill_raw_args(&mut self, args: Vec<serde_json::Value>) {
        self.args = Some(args);
    }

    /// Encodes the job with Go-compatible JSON, optionally refreshing raw arguments.
    pub fn encode(&mut self, update_raw_args: bool) -> Result<Vec<u8>, serde_json::Error> {
        if update_raw_args {
            let raw_args = if self.version.0 <= JobVersion::V1.0 {
                self.args
                    .clone()
                    .map_or(serde_json::Value::Null, serde_json::Value::Array)
            } else {
                debug_assert_eq!(self.version, JobVersion::V2);
                debug_assert!(self.args.as_ref().map_or(0, Vec::len) <= 1);
                self.args
                    .as_ref()
                    .and_then(|args| args.first())
                    .cloned()
                    .unwrap_or(serde_json::Value::Null)
            };
            self.raw_args = Some(PersistedRawJson::from_value(&raw_args)?);
            if let Some(info) = &mut self.multi_schema_info {
                if let Some(sub_jobs) = &mut info.sub_jobs {
                    for sub_job in sub_jobs {
                        let Some(sub_job) = sub_job else {
                            continue;
                        };
                        let Some(args) = &sub_job.args else {
                            continue;
                        };
                        let raw_args = if self.version.0 <= JobVersion::V1.0 {
                            serde_json::Value::Array(args.clone())
                        } else {
                            debug_assert_eq!(self.version, JobVersion::V2);
                            debug_assert!(args.len() <= 1);
                            args.first().cloned().unwrap_or(serde_json::Value::Null)
                        };
                        sub_job.raw_args = Some(PersistedRawJson::from_value(&raw_args)?);
                    }
                }
            }
        }
        crate::serde_helpers::to_go_json(self)
    }

    /// Decodes persisted JSON into this job; JSON `null` leaves it unchanged.
    pub fn decode(&mut self, bytes: &[u8]) -> Result<(), serde_json::Error> {
        // Go's scanner rejects malformed JSON before it starts assigning
        // fields. The raw-member pass is still required: a Value would discard
        // duplicate keys, reject >u64 before field dispatch, and make member
        // errors transactional instead of allowing later fields to mutate.
        let raw: &serde_json::value::RawValue = serde_json::from_slice(bytes)?;
        if raw.get() == "null" {
            return Ok(());
        }
        let mut deserializer = serde_json::Deserializer::from_str(raw.get());
        self.go_json_merge(&mut deserializer)
            .map_err(crate::serde_helpers::normalize_fatal_json_error)?;
        deserializer.end()
    }

    /// Clones through the persisted codec and clears private decoded arguments.
    #[must_use]
    pub fn deep_clone(&mut self) -> Option<Self> {
        let bytes = self.encode(true).ok()?;
        let mut cloned = Self::default();
        cloned.decode(&bytes).ok()?;
        Some(cloned)
    }

    /// Reports whether the job reached any terminal finished state.
    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.state.is_finished()
    }
    /// Reports whether the job was cancelled.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.state.is_cancelled()
    }
    /// Reports whether rollback completed.
    #[must_use]
    pub fn is_rollback_done(&self) -> bool {
        self.state.is_rollback_done()
    }
    /// Reports whether rollback is in progress.
    #[must_use]
    pub fn is_rollingback(&self) -> bool {
        self.state.is_rollingback()
    }
    /// Reports whether cancellation is in progress.
    #[must_use]
    pub fn is_cancelling(&self) -> bool {
        self.state.is_cancelling()
    }
    /// Reports whether the job is paused.
    #[must_use]
    pub fn is_paused(&self) -> bool {
        self.state.is_paused()
    }
    /// Reports whether the job is transitioning to paused.
    #[must_use]
    pub fn is_pausing(&self) -> bool {
        self.state.is_pausing()
    }
    /// Reports whether schema synchronization completed.
    #[must_use]
    pub fn is_synced(&self) -> bool {
        self.state.is_synced()
    }
    /// Reports whether normal execution completed.
    #[must_use]
    pub fn is_done(&self) -> bool {
        self.state.is_done()
    }
    /// Reports whether the job is running.
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.state.is_running()
    }
    /// Reports whether the job is queued.
    #[must_use]
    pub fn is_queueing(&self) -> bool {
        self.state.is_queueing()
    }
    /// Reports whether execution has not started.
    #[must_use]
    pub fn not_started(&self) -> bool {
        self.state.not_started()
    }
    /// Reports whether execution has started.
    #[must_use]
    pub fn started(&self) -> bool {
        !self.not_started()
    }
    /// Reports whether no further lifecycle transition is expected.
    #[must_use]
    pub fn in_final_state(&self) -> bool {
        self.state.in_final_state()
    }

    /// Reports whether TiDB itself placed this job in the paused state.
    #[must_use]
    pub fn is_paused_by_system(&self) -> bool {
        self.is_paused() && self.admin_operator == AdminCommandOperator::BY_SYSTEM
    }

    /// Reports whether the durable pause reason matches `reason`.
    #[must_use]
    pub fn has_pause_reason(&self, reason: &str) -> bool {
        self.pause_reason
            .as_ref()
            .is_some_and(|value| value.type_ == reason)
    }

    /// Records a durable pause reason and message.
    pub fn set_pause_reason(&mut self, type_: impl Into<String>, message: impl Into<String>) {
        self.pause_reason = Some(JobPauseReason {
            type_: type_.into(),
            message: message.into(),
        });
    }

    /// Clears the durable pause reason.
    pub fn clear_pause_reason(&mut self) {
        self.pause_reason = None;
    }

    /// Reports whether the durable resume reason matches `reason`.
    #[must_use]
    pub fn has_resume_reason(&self, reason: &str) -> bool {
        self.resume_reason
            .as_ref()
            .is_some_and(|value| value.type_ == reason)
    }

    /// Records a durable resume reason.
    pub fn set_resume_reason(&mut self, type_: impl Into<String>) {
        self.resume_reason = Some(JobResumeReason {
            type_: type_.into(),
        });
    }

    /// Clears the durable resume reason.
    pub fn clear_resume_reason(&mut self) {
        self.resume_reason = None;
    }

    /// Reports a system pause caused by full TiKV disks.
    #[must_use]
    pub fn is_paused_by_system_for_kv_disk_full(&self) -> bool {
        self.is_paused_by_system() && self.has_pause_reason(JOB_PAUSE_REASON_KV_DISK_FULL)
    }

    /// Reports a pending or completed system pause caused by full TiKV disks.
    #[must_use]
    pub fn is_pausing_or_paused_by_system_for_kv_disk_full(&self) -> bool {
        (self.is_pausing() || self.is_paused())
            && self.admin_operator == AdminCommandOperator::BY_SYSTEM
            && self.has_pause_reason(JOB_PAUSE_REASON_KV_DISK_FULL)
    }

    /// Reports whether this action and state allow a pause request.
    #[must_use]
    pub fn is_pausable(&self) -> bool {
        if self.type_ == ActionType::ACTION_ADD_COLUMNAR_INDEX
            && self.schema_state == SchemaState::WRITE_REORGANIZATION
        {
            return false;
        }
        self.not_started() || (self.is_running() && self.is_rollbackable())
    }

    /// Reports whether this action supports runtime alteration.
    #[must_use]
    pub fn is_alterable(&self) -> bool {
        matches!(
            self.type_,
            ActionType::ACTION_ADD_INDEX
                | ActionType::ACTION_MODIFY_COLUMN
                | ActionType::ACTION_REORGANIZE_PARTITION
        )
    }

    /// Reports whether the paused job can be resumed.
    #[must_use]
    pub fn is_resumable(&self) -> bool {
        self.is_paused()
    }

    /// Inserts one captured session system variable.
    pub fn add_system_var(&mut self, name: impl Into<String>, value: impl Into<String>) {
        self.session_vars
            .as_mut()
            .expect("assignment to entry in nil SessionVars map")
            .insert(name.into(), value.into());
    }

    /// Returns one captured session system variable.
    #[must_use]
    pub fn get_system_var(&self, name: &str) -> Option<&str> {
        self.session_vars
            .as_ref()
            .and_then(|variables| variables.get(name))
            .map(String::as_str)
    }

    /// Reports whether this action may require data reorganization.
    #[must_use]
    pub fn may_need_reorg(&self) -> bool {
        match self.type_ {
            ActionType::ACTION_ADD_INDEX
            | ActionType::ACTION_ADD_PRIMARY_KEY
            | ActionType::ACTION_REORGANIZE_PARTITION
            | ActionType::ACTION_REMOVE_PARTITIONING
            | ActionType::ACTION_ALTER_TABLE_PARTITIONING => true,
            ActionType::ACTION_MODIFY_COLUMN => self.need_reorg,
            ActionType::ACTION_MULTI_SCHEMA_CHANGE => self
                .multi_schema_info
                .as_ref()
                .expect("multi-schema job requires MultiSchemaInfo")
                .sub_jobs
                .as_ref()
                .is_some_and(|sub_jobs| {
                    sub_jobs.iter().any(|sub_job| {
                        let sub_job = sub_job
                            .as_ref()
                            .expect("nil SubJob in MultiSchemaInfo.SubJobs");
                        Job {
                            type_: sub_job.type_,
                            need_reorg: sub_job.need_reorg,
                            ..Default::default()
                        }
                        .may_need_reorg()
                    })
                }),
            _ => false,
        }
    }

    /// Reports whether the current action and schema state can be rolled back.
    #[must_use]
    pub fn is_rollbackable(&self) -> bool {
        match self.type_ {
            ActionType::ACTION_DROP_INDEX | ActionType::ACTION_DROP_PRIMARY_KEY => !matches!(
                self.schema_state,
                SchemaState::DELETE_ONLY
                    | SchemaState::DELETE_REORGANIZATION
                    | SchemaState::WRITE_ONLY
            ),
            ActionType::ACTION_MODIFY_COLUMN => self.schema_state != SchemaState::PUBLIC,
            ActionType::ACTION_ADD_TABLE_PARTITION => matches!(
                self.schema_state,
                SchemaState::NONE | SchemaState::REPLICA_ONLY
            ),
            ActionType::ACTION_DROP_COLUMN
            | ActionType::ACTION_DROP_SCHEMA
            | ActionType::ACTION_DROP_TABLE
            | ActionType::ACTION_DROP_SEQUENCE
            | ActionType::ACTION_DROP_FOREIGN_KEY
            | ActionType::ACTION_DROP_TABLE_PARTITION => self.schema_state == SchemaState::PUBLIC,
            ActionType::ACTION_TRUNCATE_TABLE_PARTITION => matches!(
                self.schema_state,
                SchemaState::PUBLIC | SchemaState::WRITE_ONLY
            ),
            ActionType::ACTION_REBASE_AUTO_ID
            | ActionType::ACTION_SHARD_ROW_ID
            | ActionType::ACTION_TRUNCATE_TABLE
            | ActionType::ACTION_ADD_FOREIGN_KEY
            | ActionType::ACTION_RENAME_TABLE
            | ActionType::ACTION_RENAME_TABLES
            | ActionType::ACTION_MODIFY_TABLE_CHARSET_AND_COLLATE
            | ActionType::ACTION_MODIFY_SCHEMA_CHARSET_AND_COLLATE
            | ActionType::ACTION_REPAIR_TABLE
            | ActionType::ACTION_MODIFY_TABLE_AUTO_IDCACHE
            | ActionType::ACTION_MODIFY_SCHEMA_DEFAULT_PLACEMENT
            | ActionType::ACTION_DROP_CHECK_CONSTRAINT => self.schema_state == SchemaState::NONE,
            ActionType::ACTION_MULTI_SCHEMA_CHANGE => {
                self.multi_schema_info
                    .as_ref()
                    .expect("multi-schema job requires MultiSchemaInfo")
                    .revertible
            }
            ActionType::ACTION_FLASHBACK_CLUSTER => !matches!(
                self.schema_state,
                SchemaState::WRITE_REORGANIZATION | SchemaState::WRITE_ONLY
            ),
            ActionType::ACTION_REORGANIZE_PARTITION
            | ActionType::ACTION_REMOVE_PARTITIONING
            | ActionType::ACTION_ALTER_TABLE_PARTITIONING => {
                self.schema_state != SchemaState::PUBLIC
            }
            _ => true,
        }
    }

    /// Returns explicit scheduling involvement or the schema/table fallback.
    #[must_use]
    pub fn get_involving_schema_info(&self) -> Vec<InvolvingSchemaInfo> {
        if let Some(info) = self
            .involving_schema_info
            .as_ref()
            .filter(|info| !info.is_empty())
        {
            return info.clone();
        }
        let table = if !self.schema_name.is_empty() && self.table_name.is_empty() {
            INVOLVING_ALL.to_owned()
        } else {
            self.table_name.clone()
        };
        vec![InvolvingSchemaInfo {
            database: self.schema_name.clone(),
            table,
            ..Default::default()
        }]
    }

    /// Lowercases scheduling names while preserving `*` and empty sentinels.
    pub fn normalize_involving_schema_info(&mut self) {
        self.schema_name = normalize_involving_name(&self.schema_name);
        self.table_name = normalize_involving_name(&self.table_name);
        if let Some(involving) = &mut self.involving_schema_info {
            for info in involving {
                info.database = normalize_involving_name(&info.database);
                info.table = normalize_involving_name(&info.table);
                info.policy = normalize_involving_name(&info.policy);
                info.resource_group = normalize_involving_name(&info.resource_group);
            }
        }
    }

    /// Validates that each scheduling entry identifies exactly one object kind.
    pub fn check_involving_schema_info(&self) -> Result<(), &'static str> {
        for info in self.get_involving_schema_info() {
            let object_types = usize::from(!info.policy.is_empty())
                + usize::from(!info.resource_group.is_empty())
                + usize::from(!info.database.is_empty() || !info.table.is_empty());
            if object_types != 1 {
                return Err("InvolvingSchemaInfo must involve only one type of object among database/table, placement policy, resource group");
            }
            if info.policy.is_empty() && info.resource_group.is_empty() {
                if info.database.is_empty() || info.table.is_empty() {
                    return Err("DDL job operating on schema or table, must have non-empty name set in InvolvingSchemaInfo");
                }
                if info.database == INVOLVING_ALL && info.table != INVOLVING_ALL {
                    return Err("DDL job operating on all databases, must not set table name in InvolvingSchemaInfo");
                }
            }
        }
        Ok(())
    }

    /// Clears the private decoded-argument cache without changing raw JSON.
    pub fn clear_decoded_args(&mut self) {
        self.args = None;
    }
}

fn normalize_involving_name(name: &str) -> String {
    if matches!(name, INVOLVING_ALL | INVOLVING_NONE) {
        name.to_owned()
    } else {
        tidb_mysql::to_lowercase(name)
    }
}

fn option_vec_is_none_or_empty<T>(values: &Option<Vec<T>>) -> bool {
    values.as_ref().is_none_or(Vec::is_empty)
}

fn option_map_is_none_or_empty<K, V>(values: &Option<BTreeMap<K, V>>) -> bool {
    values.as_ref().is_none_or(BTreeMap::is_empty)
}

impl std::fmt::Display for Job {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let start = crate::bdr::ts_convert_2_time(self.start_ts).format("%Y-%m-%d %H:%M:%S %z UTC");
        let error = self
            .error
            .as_ref()
            .map_or_else(|| "<nil>".to_owned(), ToString::to_string);
        write!(
            formatter,
            "ID:{}, Type:{}, State:{}, SchemaState:{}, SchemaID:{}, TableID:{}, RowCount:{}, ArgLen:{}, start time: {}, Err:{}, ErrCount:{}, SnapshotVersion:{}, Version: {}",
            self.id,
            self.type_,
            self.state,
            self.schema_state,
            self.schema_id,
            self.table_id,
            self.row_count,
            self.args.as_ref().map_or(0, Vec::len),
            start,
            error,
            self.error_count,
            self.snapshot_version,
            self.version,
        )?;
        if let Some(metadata) = &self.reorg_meta {
            if self.type_ == ActionType::ACTION_MODIFY_COLUMN {
                write!(
                    formatter,
                    ", analyze_state:{}, stage:{}",
                    metadata.analyze_state, metadata.stage.0
                )?;
            }
            write!(
                formatter,
                ", UniqueWarnings:{}",
                metadata.warnings.as_ref().map_or(0, BTreeMap::len)
            )?;
        }
        if self.type_ != ActionType::ACTION_MULTI_SCHEMA_CHANGE {
            if let Some(info) = &self.multi_schema_info {
                write!(
                    formatter,
                    ", Multi-Schema Change:true, Revertible:{}",
                    info.revertible
                )?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::placement::PolicyRefInfo;
    use tidb_ast::CiString;

    fn raw_json(json: &str) -> PersistedRawJson {
        PersistedRawJson::from_string(json.to_owned()).unwrap()
    }

    #[test]
    fn enums_and_reasons() {
        assert_eq!(
            AdminCommandOperator::default(),
            AdminCommandOperator::BY_NOT_KNOWN
        );
        assert_ne!(
            AdminCommandOperator::BY_SYSTEM,
            AdminCommandOperator::BY_END_USER
        );
        assert_eq!(
            InvolvingSchemaInfoMode::default(),
            InvolvingSchemaInfoMode::EXCLUSIVE
        );
        assert_eq!(INVOLVING_ALL, "*");
        assert_eq!(INVOLVING_NONE, "");

        let inv = InvolvingSchemaInfo {
            database: INVOLVING_ALL.to_owned(),
            mode: InvolvingSchemaInfoMode::SHARED,
            ..Default::default()
        };
        assert_eq!(inv.clone(), inv);

        let r = JobPauseReason {
            type_: "kv".to_owned(),
            message: "disk full".to_owned(),
        };
        assert_eq!(r.message, "disk full");
        assert_eq!(AdminCommandOperator::BY_END_USER.to_string(), "EndUser");
        assert_eq!(AdminCommandOperator(99).to_string(), "None");
    }

    #[test]
    fn history_info() {
        let h = HistoryInfo {
            schema_version: 42,
            table_info: Some(Box::new(TableInfo {
                name: CiString::new("t"),
                ..Default::default()
            })),
            multiple_table_infos: Some(vec![Some(TableInfo::default())]),
            ..Default::default()
        };
        // Deep clone.
        let c = h.clone();
        assert_eq!(c.schema_version, 42);
        assert_eq!(c.table_info.unwrap().name.original(), "t");
        assert_eq!(c.multiple_table_infos.as_ref().unwrap().len(), 1);

        let nil_json = serde_json::to_value(HistoryInfo::default()).unwrap();
        assert_eq!(nil_json["MultipleTableInfos"], serde_json::Value::Null);
        let mut allocated_empty = HistoryInfo::default();
        allocated_empty.set_table_infos(1, Vec::new());
        let empty_json = serde_json::to_value(&allocated_empty).unwrap();
        assert_eq!(empty_json["MultipleTableInfos"], serde_json::json!([]));

        let mut h = h;
        h.finished_ts = 99;
        h.clean();
        assert_eq!(h.schema_version, 0);
        assert!(h.table_info.is_none());
        assert_eq!(h.finished_ts, 99);
    }

    #[test]
    fn history_decode_merges_existing_nested_objects_and_continues_after_errors() {
        let mut job = Job {
            binlog_info: Some(HistoryInfo {
                schema_version: 10,
                db_info: Some(Box::new(DBInfo {
                    id: 1,
                    name: CiString::new("db-kept"),
                    charset: "old-db-charset".to_owned(),
                    placement_policy_ref: Some(PolicyRefInfo {
                        id: 11,
                        name: CiString::new("db-policy-kept"),
                    }),
                    ..Default::default()
                })),
                table_info: Some(Box::new(TableInfo {
                    id: 2,
                    name: CiString::new("table-kept"),
                    comment: "old-comment".to_owned(),
                    placement_policy_ref: Some(PolicyRefInfo {
                        id: 12,
                        name: CiString::new("table-policy-kept"),
                    }),
                    ..Default::default()
                })),
                finished_ts: 20,
                multiple_table_infos: Some(vec![Some(TableInfo {
                    id: 3,
                    ..Default::default()
                })]),
            }),
            ..Default::default()
        };

        let error = job
            .decode(
                br#"{
                    "binlog": {
                        "DBInfo":{
                            "id":"bad",
                            "charset":null,
                            "policy_ref_info":{"id":"bad","name":{"O":"db-policy-later","L":"db-policy-later"}},
                            "collate":"db-later"
                        },
                        "TableInfo":{
                            "id":"bad",
                            "policy_ref_info":{"id":"bad","name":{"O":"table-policy-later","L":"table-policy-later"}},
                            "comment":"table-later"
                        },
                        "MultipleTableInfos":[
                            null,
                            {"id":"bad","comment":"element-later"},
                            {"id":7,"comment":"last-element"}
                        ],
                        "FinishedTS":30
                    },
                    "row_count":40
                }"#,
            )
            .unwrap_err();
        assert!(error.to_string().contains("invalid type"));

        let history = job.binlog_info.as_ref().unwrap();
        assert_eq!(history.schema_version, 10);
        let database = history.db_info.as_ref().unwrap();
        assert_eq!(database.id, 1);
        assert_eq!(database.name.original(), "db-kept");
        assert_eq!(database.charset, "old-db-charset");
        assert_eq!(database.collate, "db-later");
        let db_policy = database.placement_policy_ref.as_ref().unwrap();
        assert_eq!(db_policy.id, 11);
        assert_eq!(db_policy.name.original(), "db-policy-later");
        let table = history.table_info.as_ref().unwrap();
        assert_eq!(table.id, 2);
        assert_eq!(table.name.original(), "table-kept");
        assert_eq!(table.comment, "table-later");
        let table_policy = table.placement_policy_ref.as_ref().unwrap();
        assert_eq!(table_policy.id, 12);
        assert_eq!(table_policy.name.original(), "table-policy-later");
        assert_eq!(history.finished_ts, 30);
        let tables = history.multiple_table_infos.as_ref().unwrap();
        assert!(tables[0].is_none());
        assert_eq!(tables[1].as_ref().unwrap().id, 0);
        assert_eq!(tables[1].as_ref().unwrap().comment, "element-later");
        assert_eq!(tables[2].as_ref().unwrap().id, 7);
        assert_eq!(tables[2].as_ref().unwrap().comment, "last-element");
        assert_eq!(job.row_count, 40);

        job.decode(br#"{"binlog":{"DBInfo":{"Deprecated":null,"collate":"after-null"}}}"#)
            .unwrap();
        assert_eq!(
            job.binlog_info
                .as_ref()
                .unwrap()
                .db_info
                .as_ref()
                .unwrap()
                .collate,
            "after-null"
        );

        job.decode(br#"{"binlog":{"DBInfo":null,"TableInfo":null,"MultipleTableInfos":null}}"#)
            .unwrap();
        let history = job.binlog_info.as_ref().unwrap();
        assert_eq!(history.schema_version, 10);
        assert!(history.db_info.is_none());
        assert!(history.table_info.is_none());
        assert!(history.multiple_table_infos.is_none());
        assert_eq!(history.finished_ts, 30);
    }

    #[test]
    fn time_zone_location_boundaries() {
        let mut cached = TimeZoneLocation::default();
        let utc = cached.get_location().unwrap();
        assert_eq!(utc.name(), "UTC");
        cached.name = "Asia/Shanghai".to_owned();
        assert_eq!(cached.get_location().unwrap().name(), "UTC");
        let shanghai = TimeZoneLocation {
            name: "Asia/Shanghai".to_owned(),
            ..Default::default()
        };
        assert_eq!(shanghai.get_location().unwrap().name(), "Asia/Shanghai");
        let fixed = TimeZoneLocation {
            name: "UTC".to_owned(),
            offset: 18_000,
            ..Default::default()
        }
        .get_location()
        .unwrap();
        assert_eq!(fixed.name(), "UTC");
        assert!(TimeZoneLocation {
            name: "Not/AZone".to_owned(),
            offset: 0,
            ..Default::default()
        }
        .get_location()
        .is_err());

        let widest_go_int = TimeZoneLocation {
            name: "wide".to_owned(),
            offset: i64::MAX,
            ..Default::default()
        };
        assert_eq!(
            serde_json::to_value(&widest_go_int).unwrap()["offset"],
            serde_json::json!(i64::MAX)
        );
        assert!(matches!(
            widest_go_int.get_location().unwrap(),
            ResolvedTimeZone::Fixed {
                offset_seconds: i64::MAX,
                ..
            }
        ));
    }

    #[test]
    fn job_state_pause_and_reorg_boundaries() {
        let mut job = Job {
            type_: ActionType::ACTION_MODIFY_COLUMN,
            state: JobState::RUNNING,
            schema_state: SchemaState::WRITE_ONLY,
            need_reorg: true,
            admin_operator: AdminCommandOperator::BY_SYSTEM,
            session_vars: Some(BTreeMap::new()),
            ..Default::default()
        };
        assert!(job.is_running());
        assert!(job.started());
        assert!(job.may_need_reorg());
        assert!(job.is_rollbackable());
        assert!(job.is_pausable());
        job.set_pause_reason(JOB_PAUSE_REASON_KV_DISK_FULL, "disk full");
        job.state = JobState::PAUSED;
        assert!(job.is_paused_by_system_for_kv_disk_full());
        assert!(job.is_resumable());
        job.set_resume_reason(JOB_RESUME_REASON_KV_DISK_FULL);
        assert!(job.has_resume_reason(JOB_RESUME_REASON_KV_DISK_FULL));
        job.clear_pause_reason();
        assert!(!job.has_pause_reason(JOB_PAUSE_REASON_KV_DISK_FULL));

        job.schema_state = SchemaState::PUBLIC;
        assert!(!job.is_rollbackable());
        job.add_system_var("sql_mode", "strict");
        assert_eq!(job.get_system_var("sql_mode"), Some("strict"));
    }

    #[test]
    fn job_string_zero_time_boundary() {
        let job = Job {
            version: JobVersion::V1,
            id: 123,
            binlog_info: Some(HistoryInfo::default()),
            ..Default::default()
        };
        assert_eq!(
            job.to_string(),
            "ID:123, Type:none, State:none, SchemaState:none, SchemaID:0, TableID:0, RowCount:0, ArgLen:0, start time: 1970-01-01 00:00:00 +0000 UTC, Err:<nil>, ErrCount:0, SnapshotVersion:0, Version: v1"
        );
    }

    #[test]
    fn rollbackability_action_boundaries() {
        let mut job = Job {
            type_: ActionType::ACTION_DROP_INDEX,
            schema_state: SchemaState::NONE,
            ..Default::default()
        };
        assert!(job.is_rollbackable());
        for state in [
            SchemaState::DELETE_ONLY,
            SchemaState::DELETE_REORGANIZATION,
            SchemaState::WRITE_ONLY,
        ] {
            job.schema_state = state;
            assert!(!job.is_rollbackable());
        }
        job.type_ = ActionType::ACTION_ADD_TABLE_PARTITION;
        job.schema_state = SchemaState::REPLICA_ONLY;
        assert!(job.is_rollbackable());
        job.schema_state = SchemaState::WRITE_ONLY;
        assert!(!job.is_rollbackable());
        job.type_ = ActionType::ACTION_TRUNCATE_TABLE_PARTITION;
        assert!(job.is_rollbackable());
        job.schema_state = SchemaState::DELETE_ONLY;
        assert!(!job.is_rollbackable());
    }

    #[test]
    fn involving_schema_normalization_and_validation_boundaries() {
        let mut job = Job {
            schema_name: "TeSt".to_owned(),
            table_name: "TaBlE".to_owned(),
            ..Default::default()
        };
        job.normalize_involving_schema_info();
        assert_eq!(job.schema_name, "test");
        assert_eq!(job.table_name, "table");
        assert!(job.check_involving_schema_info().is_ok());

        job.schema_name = "\u{130}".to_owned();
        job.table_name = "\u{130}".to_owned();
        job.normalize_involving_schema_info();
        assert_eq!(job.schema_name, "i");
        assert_eq!(job.table_name, "i");

        let mut allocated_empty = job.clone();
        allocated_empty.involving_schema_info = Some(Vec::new());
        assert_eq!(
            allocated_empty.get_involving_schema_info(),
            vec![InvolvingSchemaInfo {
                database: "i".to_owned(),
                table: "i".to_owned(),
                ..Default::default()
            }]
        );
        assert!(serde_json::to_value(&allocated_empty)
            .unwrap()
            .get("involving_schema_info")
            .is_none());

        let mut multi = MultiSchemaInfo::default();
        assert!(multi.add_columns.is_empty());
        assert!(multi.drop_columns.is_empty());
        assert!(multi.modify_columns.is_empty());
        assert!(multi.add_indexes.is_empty());
        assert!(multi.drop_indexes.is_empty());
        assert!(multi.alter_indexes.is_empty());
        assert!(multi.add_foreign_keys.is_empty());
        assert!(multi.relative_columns.is_empty());
        assert!(multi.position_columns.is_empty());
        assert_eq!(
            serde_json::to_value(&multi).unwrap()["sub_jobs"],
            serde_json::Value::Null
        );
        multi.sub_jobs = Some(Vec::new());
        assert_eq!(
            serde_json::to_value(&multi).unwrap()["sub_jobs"],
            serde_json::json!([])
        );

        job.involving_schema_info = Some(vec![InvolvingSchemaInfo {
            database: "db".to_owned(),
            table: "t".to_owned(),
            policy: "p".to_owned(),
            ..Default::default()
        }]);
        assert_eq!(
            job.check_involving_schema_info().unwrap_err(),
            "InvolvingSchemaInfo must involve only one type of object among database/table, placement policy, resource group"
        );
        job.involving_schema_info = Some(vec![InvolvingSchemaInfo {
            database: INVOLVING_ALL.to_owned(),
            table: "t".to_owned(),
            ..Default::default()
        }]);
        assert_eq!(
            job.check_involving_schema_info().unwrap_err(),
            "DDL job operating on all databases, must not set table name in InvolvingSchemaInfo"
        );
    }

    #[test]
    fn job_argument_version_and_proxy_boundaries() {
        let mut nil_v1 = Job {
            version: JobVersion::V1,
            ..Default::default()
        };
        let encoded = nil_v1.encode(true).unwrap();
        assert!(std::str::from_utf8(&encoded)
            .unwrap()
            .contains(r#""raw_args":null"#));

        let mut empty_v1 = Job {
            version: JobVersion::V1,
            ..Default::default()
        };
        empty_v1.fill_raw_args(Vec::new());
        let encoded = empty_v1.encode(true).unwrap();
        assert!(std::str::from_utf8(&encoded)
            .unwrap()
            .contains(r#""raw_args":[]"#));

        let mut v1 = Job {
            version: JobVersion::V1,
            ..Default::default()
        };
        v1.fill_raw_args(vec![serde_json::json!(1), serde_json::json!("x")]);
        let encoded = v1.encode(true).unwrap();
        assert!(std::str::from_utf8(&encoded)
            .unwrap()
            .contains(r#""raw_args":[1,"x"]"#));

        let mut v2 = Job {
            version: JobVersion::V2,
            resume_reason: Some(JobResumeReason {
                type_: JOB_RESUME_REASON_KV_DISK_FULL.to_owned(),
            }),
            ..Default::default()
        };
        v2.fill_raw_args(vec![serde_json::json!({"a": 1})]);
        let encoded = v2.encode(true).unwrap();
        assert!(std::str::from_utf8(&encoded)
            .unwrap()
            .contains(r#""raw_args":{"a":1}"#));

        v2.fill_raw_args(vec![serde_json::json!({
            "text": "<>&\u{2028}\u{2029}",
            "ratio": 1.0
        })]);
        let encoded = v2.encode(true).unwrap();
        let encoded = std::str::from_utf8(&encoded).unwrap();
        assert!(encoded.contains(r#"\u003c\u003e\u0026\u2028\u2029"#));
        assert!(encoded.contains(r#""ratio":1"#));

        let sub_job = SubJob {
            type_: ActionType::ACTION_ADD_INDEX,
            state: JobState::QUEUEING,
            ..Default::default()
        };
        let proxy = sub_job.to_proxy_job(&v2, i64::MAX);
        assert!(proxy.has_resume_reason(JOB_RESUME_REASON_KV_DISK_FULL));
        assert_eq!(proxy.type_, ActionType::ACTION_ADD_INDEX);
        assert_eq!(proxy.multi_schema_info.unwrap().seq, -1);

        let cached = SubJob {
            args: Some(vec![serde_json::json!({"decoded": true})]),
            raw_args: Some(raw_json(r#"{"persisted":true}"#)),
            ..Default::default()
        };
        let clone = cached.clone_without_args();
        assert!(clone.args.is_none());
        assert_eq!(clone.raw_args, cached.raw_args);

        let mut clone_source = Job {
            version: JobVersion::V1,
            ..Default::default()
        };
        clone_source.fill_raw_args(vec![serde_json::json!("persisted by clone")]);
        assert!(clone_source.raw_args.is_none());
        let clone = clone_source.deep_clone().unwrap();
        assert_eq!(
            clone_source.raw_args,
            Some(raw_json(r#"["persisted by clone"]"#))
        );
        assert_eq!(clone.raw_args, clone_source.raw_args);

        let raw = vec![0, 1, 255];
        let mut wrapped = JobW::new(v2, raw.clone());
        assert_eq!(wrapped.bytes, raw);
        assert!(wrapped.has_resume_reason(JOB_RESUME_REASON_KV_DISK_FULL));
        wrapped.id = 88;
        assert_eq!(wrapped.job.id, 88);

        let mut lexical = Job::default();
        lexical
            .decode(br#"{"raw_args":{ "n":18446744073709551616, "n":1.0, "s":"<>&" }}"#)
            .unwrap();
        assert_eq!(
            lexical.raw_args.as_ref().unwrap().get(),
            r#"{ "n":18446744073709551616, "n":1.0, "s":"<>&" }"#
        );
        let encoded = String::from_utf8(lexical.encode(false).unwrap()).unwrap();
        assert!(encoded
            .contains(r#""raw_args":{"n":18446744073709551616,"n":1.0,"s":"\u003c\u003e\u0026"}"#));
        assert!(PersistedRawJson::from_string("{".to_owned()).is_err());

        let mut unchanged = Job {
            id: 42,
            ..Default::default()
        };
        unchanged.decode(b"null").unwrap();
        assert_eq!(unchanged.id, 42);

        unchanged.decode(br#"{"row_count":9}"#).unwrap();
        assert_eq!(unchanged.id, 42);
        assert_eq!(unchanged.row_count, 9);

        let error = unchanged
            .decode(br#"{"err_count":7,"priority":"bad"}"#)
            .unwrap_err();
        assert!(error.to_string().contains("invalid type"));
        assert_eq!(unchanged.error_count, 7);
    }

    #[test]
    fn job_decode_matches_go_object_stream_boundaries() {
        let mut reorg_meta = DDLReorgMeta::default();
        reorg_meta.resource_group_name = "existing".to_owned();
        reorg_meta.warnings = Some(BTreeMap::from([("old".to_owned(), None)]));
        let mut job = Job {
            id: 42,
            row_count: 1,
            raw_args: Some(raw_json(r#"["old"]"#)),
            involving_schema_info: Some(vec![InvolvingSchemaInfo {
                database: "db".to_owned(),
                table: "t".to_owned(),
                ..Default::default()
            }]),
            pause_reason: Some(JobPauseReason {
                type_: "old".to_owned(),
                message: "old message".to_owned(),
            }),
            session_vars: Some(BTreeMap::from([("old".to_owned(), "1".to_owned())])),
            reorg_meta: Some(reorg_meta),
            ..Default::default()
        };

        job.decode(
            br#"{
                "ROW_COUNT":2,
                "row_count":3,
                "ID":null,
                "raw_args":null,
                "warning":{"class":21,"code":2,"message":"warn","rfccode":"global:2"},
                "err":{"class":5,"code":3,"message":"failed","rfccode":"executor:3"},
                "trace_info":{"session_alias":"alias","trace_id":"AAH/","connection_id":9},
                "SESSION_VARS":{"new":"2"},
                "session_vars":{"new":"3"},
                "reorg_meta":{
                    "WARNINGS":{"new":{"class":21,"code":2,"message":"new","rfccode":"global:2"}},
                    "MAX_NODE_COUNT":4,
                    "max_node_count":5
                },
                "pause_reason":null,
                "involving_schema_info":null,
                "unknown":{"ignored":[1,true,null]}
            }"#,
        )
        .unwrap();
        assert_eq!(job.id, 42);
        assert_eq!(job.row_count, 3);
        assert_eq!(job.raw_args, Some(raw_json("null")));
        assert_eq!(job.warning.as_ref().unwrap().message(), "warn");
        assert_eq!(job.error.as_ref().unwrap().message(), "failed");
        assert_eq!(
            job.trace_info,
            Some(TraceInfo {
                session_alias: "alias".to_owned(),
                trace_id: Some(vec![0, 1, 255]),
                connection_id: 9,
            })
        );
        assert!(job.pause_reason.is_none());
        assert!(job.involving_schema_info.is_none());
        assert_eq!(job.get_system_var("old"), Some("1"));
        assert_eq!(job.get_system_var("new"), Some("3"));
        let reorg = job.reorg_meta.as_ref().unwrap();
        assert_eq!(reorg.resource_group_name, "existing");
        assert_eq!(reorg.max_node_count, 5);
        assert!(reorg.warnings.as_ref().unwrap().contains_key("old"));
        assert!(reorg.warnings.as_ref().unwrap().contains_key("new"));
        assert_eq!(
            reorg.warnings.as_ref().unwrap()["new"]
                .as_ref()
                .unwrap()
                .message(),
            "new"
        );

        job.decode(
            "{\"warning\":{\"claſs\":5,\"code\":3,\"message\":\"folded\",\"rfccode\":\"executor:3\"}}"
                .as_bytes(),
        )
        .unwrap();
        assert_eq!(
            job.warning.as_ref().unwrap().class(),
            tidb_error::terror::TerrorClass::from_value(5)
        );

        job.decode(br#"{"session_vars":null,"reorg_meta":{"warnings":null}}"#)
            .unwrap();
        assert!(job.session_vars.is_none());
        assert!(job.reorg_meta.as_ref().unwrap().warnings.is_none());

        let id_before_syntax_error = job.id;
        assert!(job.decode(br#"{"id":99,"row_count":}"#).is_err());
        assert_eq!(job.id, id_before_syntax_error);

        let error = job
            .decode(br#"{"err_count":7,"priority":1.5}"#)
            .unwrap_err();
        assert!(error.to_string().contains("invalid type"));
        assert_eq!(job.error_count, 7);

        job.decode(br#"{"dependency_id":8,"row_count":9223372036854775808}"#)
            .unwrap_err();
        assert_eq!(job.dependency_id, 8);

        let error = job
            .decode(br#"{"warning":7,"id":51,"row_count":9223372036854775808,"table_id":52}"#)
            .unwrap_err();
        assert!(error.to_string().contains("invalid type"));
        assert_eq!(job.id, 42);
        assert_eq!(job.table_id, 0);

        let error = job
            .decode(br#"{"session_vars":{"bad":7,"later":"ok"},"id":53}"#)
            .unwrap_err();
        assert!(error.to_string().contains("invalid type"));
        assert_eq!(job.get_system_var("bad"), Some(""));
        assert_eq!(job.get_system_var("later"), Some("ok"));
        assert_eq!(job.id, 53);

        job.decode(br#"{"session_vars":{"null_value":null},"id":54}"#)
            .unwrap();
        assert_eq!(job.get_system_var("null_value"), Some(""));
        assert_eq!(job.id, 54);

        let reorg_max_nodes = job.reorg_meta.as_ref().unwrap().max_node_count;
        let error = job
            .decode(
                br#"{"reorg_meta":{"warnings":{"bad":7,"later":null},"max_node_count":99},"id":55}"#,
            )
            .unwrap_err();
        assert!(error.to_string().contains("invalid type"));
        let reorg = job.reorg_meta.as_ref().unwrap();
        assert!(!reorg.warnings.as_ref().unwrap().contains_key("bad"));
        assert!(!reorg.warnings.as_ref().unwrap().contains_key("later"));
        assert_eq!(reorg.max_node_count, reorg_max_nodes);
        assert_eq!(job.id, 54);

        job.decode(br#"{"row_count":18446744073709551616,"table_id":54}"#)
            .unwrap_err();
        assert_eq!(job.table_id, 54);

        let error = job
            .decode(br#"{"trace_info":{"session_alias":"changed","trace_id":"AA$=","connection_id":10},"schema_id":53}"#)
            .unwrap_err();
        assert!(error.to_string().contains("illegal base64 data"));
        assert_eq!(job.trace_info.as_ref().unwrap().session_alias, "changed");
        assert_eq!(job.trace_info.as_ref().unwrap().connection_id, 10);
        assert_eq!(job.schema_id, 53);

        let error = job
            .decode(br#"{"reorg_meta":{"version":9,"max_node_count":"bad"}}"#)
            .unwrap_err();
        assert!(error.to_string().contains("invalid type"));
        assert_eq!(job.reorg_meta.as_ref().unwrap().version, 9);
    }

    #[test]
    fn multi_schema_sub_jobs_reuse_nullable_pointers_and_propagate_fatal_errors() {
        let mut job = Job {
            id: 40,
            multi_schema_info: Some(MultiSchemaInfo {
                sub_jobs: Some(vec![Some(SubJob {
                    row_count: 5,
                    schema_version: 6,
                    raw_args: Some(raw_json(r#"{"kept":true}"#)),
                    ..Default::default()
                })]),
                seq: 1,
                ..Default::default()
            }),
            ..Default::default()
        };

        let error = job
            .decode(
                br#"{
                    "multi_schema_info":{
                        "sub_jobs":[
                            {"row_count":"bad","schema_version":7},
                            null,
                            {"row_count":8}
                        ],
                        "seq":2
                    },
                    "id":41
                }"#,
            )
            .unwrap_err();
        assert!(error.to_string().contains("invalid type"));
        let info = job.multi_schema_info.as_ref().unwrap();
        assert_eq!(info.seq, 2);
        let sub_jobs = info.sub_jobs.as_ref().unwrap();
        assert_eq!(sub_jobs.len(), 3);
        let first = sub_jobs[0].as_ref().unwrap();
        assert_eq!(first.row_count, 5);
        assert_eq!(first.schema_version, 7);
        assert_eq!(first.raw_args, Some(raw_json(r#"{"kept":true}"#)));
        assert!(sub_jobs[1].is_none());
        assert_eq!(sub_jobs[2].as_ref().unwrap().row_count, 8);
        assert_eq!(job.id, 41);

        let error = job
            .decode(
                br#"{
                    "multi_schema_info":{
                        "sub_jobs":[{"warning":7,"schema_version":9},null],
                        "seq":3
                    },
                    "id":42
                }"#,
            )
            .unwrap_err();
        assert!(error.to_string().contains("invalid type"));
        let info = job.multi_schema_info.as_ref().unwrap();
        assert_eq!(info.seq, 2);
        let sub_jobs = info.sub_jobs.as_ref().unwrap();
        assert_eq!(sub_jobs[0].as_ref().unwrap().schema_version, 7);
        assert!(sub_jobs[1].is_none());
        assert_eq!(sub_jobs[2].as_ref().unwrap().row_count, 8);
        assert_eq!(job.id, 41);
    }

    #[test]
    fn job_go_int_width_boundaries() {
        let maximum = Job {
            priority: i64::MAX,
            ..Default::default()
        };
        let minimum = JobMeta {
            priority: i64::MIN,
            ..Default::default()
        };
        assert_eq!(
            serde_json::to_value(maximum).unwrap()["priority"],
            serde_json::json!(i64::MAX)
        );
        assert_eq!(
            serde_json::to_value(minimum).unwrap()["priority"],
            serde_json::json!(i64::MIN)
        );
    }

    #[test]
    #[cfg(debug_assertions)]
    fn v2_argument_cardinality_matches_intest_assertions() {
        let mut job = Job {
            version: JobVersion::V2,
            ..Default::default()
        };
        job.fill_raw_args(vec![serde_json::json!(1), serde_json::json!(2)]);
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| job.encode(true))).is_err()
        );

        let sub_job = SubJob {
            args: Some(vec![serde_json::json!(1), serde_json::json!(2)]),
            ..Default::default()
        };
        let mut job = Job {
            version: JobVersion::V2,
            multi_schema_info: Some(MultiSchemaInfo {
                sub_jobs: Some(vec![Some(sub_job)]),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| job.encode(true))).is_err()
        );
    }
}
