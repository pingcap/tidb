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
use std::sync::{Mutex, OnceLock};

use serde_json::value::RawValue;
use tidb_datatype::GoString;
use tidb_error::terror::TerrorError;

use crate::action_type::ActionType;
use crate::db::DBInfo;
use crate::go_any::GoAny;
use crate::go_runtime::{GoShared, GoSharedPointerSlice, GoSharedSlice};
pub use crate::history::HistoryInfo;
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
#[derive(Clone, Debug, Default)]
pub struct PersistedRawJson(GoSharedSlice<u8>);

impl PersistedRawJson {
    /// Validates and owns exact JSON text.
    pub fn from_string(json: String) -> Result<Self, serde_json::Error> {
        let _: &RawValue = serde_json::from_str(&json)?;
        Ok(Self(GoSharedSlice::from_vec(json.into_bytes())))
    }

    /// Constructs the exact Go `json.RawMessage` byte slice without validating
    /// it. Direct Go struct construction permits arbitrary bytes; validation
    /// happens when an outer `encoding/json` marshal consumes the message.
    #[must_use]
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self(GoSharedSlice::from_vec(bytes))
    }

    /// Constructs an allocated raw-message header with an explicit Go slice
    /// capacity. This is primarily useful when a caller already observed the
    /// source header rather than merely its visible bytes.
    #[must_use]
    pub fn from_bytes_with_capacity(bytes: Vec<u8>, capacity: usize) -> Self {
        Self(GoSharedSlice::from_vec_with_capacity(bytes, capacity))
    }

    /// Returns a snapshot of the exact decoded JSON text before
    /// outer-marshaler compaction. A caller that mutates the source byte slice
    /// to invalid UTF-8 must inspect [`Self::bytes`] instead.
    #[must_use]
    pub fn get(&self) -> String {
        String::from_utf8(self.bytes()).expect("RawMessage contains invalid UTF-8")
    }

    /// Returns an exact byte snapshot.
    #[must_use]
    pub fn bytes(&self) -> Vec<u8> {
        self.0.snapshot()
    }

    /// Returns the visible Go slice capacity.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.0.capacity()
    }

    /// Mutates one byte through every shallow copy of this Go slice header.
    pub fn set_byte(&self, index: usize, byte: u8) {
        self.0.set(index, byte);
    }

    /// Reports Go slice backing-array identity.
    #[must_use]
    pub fn backing_ptr_eq(&self, other: &Self) -> bool {
        self.0.backing_ptr_eq(&other.0)
    }

    pub(crate) fn replace_unmarshal_json(&mut self, bytes: Vec<u8>) {
        let capacity = if bytes.len() <= self.0.capacity() {
            self.0.capacity()
        } else {
            crate::go_runtime::go_64_next_slice_capacity_for_element(
                bytes.len(),
                self.0.capacity(),
                1,
                crate::go_runtime::GoSliceElementLayout::NoPointers,
            )
        };
        self.0.replace_decoded(bytes, capacity);
    }

    fn from_marshaled_bytes(bytes: Vec<u8>) -> Self {
        let capacity = crate::go_runtime::go_64_next_slice_capacity_for_element(
            bytes.len(),
            0,
            1,
            crate::go_runtime::GoSliceElementLayout::NoPointers,
        );
        Self(GoSharedSlice::from_vec_with_capacity(bytes, capacity))
    }
}

impl PartialEq for PersistedRawJson {
    fn eq(&self, other: &Self) -> bool {
        self.bytes() == other.bytes()
    }
}

impl Eq for PersistedRawJson {}

impl serde::Serialize for PersistedRawJson {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::Error as _;

        let bytes = self.bytes();
        let raw: &RawValue = serde_json::from_slice(&bytes).map_err(S::Error::custom)?;
        serde::Serialize::serialize(raw, serializer)
    }
}

impl<'de> serde::Deserialize<'de> for PersistedRawJson {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = <Box<RawValue> as serde::Deserialize>::deserialize(deserializer)?;
        Ok(Self(GoSharedSlice::from_vec(raw.get().as_bytes().to_vec())))
    }
}

/// Go `InvolvingSchemaInfo`: a schema object a DDL job involves (for locking).
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct InvolvingSchemaInfo {
    /// The database name.
    #[serde(default, skip_serializing_if = "GoString::is_empty")]
    pub database: GoString,
    /// The table name.
    #[serde(default, skip_serializing_if = "GoString::is_empty")]
    pub table: GoString,
    /// The placement policy name.
    #[serde(default, skip_serializing_if = "GoString::is_empty")]
    pub policy: GoString,
    /// The resource group name.
    #[serde(default, skip_serializing_if = "GoString::is_empty")]
    pub resource_group: GoString,
    /// The involvement mode.
    #[serde(default, skip_serializing_if = "is_exclusive_mode")]
    pub mode: InvolvingSchemaInfoMode,
}

fn is_exclusive_mode(mode: &InvolvingSchemaInfoMode) -> bool {
    *mode == InvolvingSchemaInfoMode::EXCLUSIVE
}

/// Go `JobPauseReason`: why a DDL job was paused.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct JobPauseReason {
    /// The reason type.
    #[serde(rename = "type", default)]
    pub type_: GoString,
    /// The reason message.
    #[serde(default, skip_serializing_if = "GoString::is_empty")]
    pub message: GoString,
}

/// Go `JobResumeReason`: why a DDL job was resumed.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct JobResumeReason {
    /// The reason type.
    #[serde(rename = "type", default)]
    pub type_: GoString,
}

/// Go `JobMeta`: the subset of job metadata embedded by a backfill task.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
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
    pub query: GoString,
    /// Operation priority used by index creation.
    #[serde(rename = "priority", default)]
    pub priority: i64,
}

/// A resolved Go `time.Location` shape. Named IANA zones and explicit fixed
/// offsets remain distinct because the latter retains the caller's name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResolvedTimeZone {
    /// Go's mutable process-local location.
    Local,
    /// A named IANA time zone loaded from the time-zone database.
    Named(chrono_tz::Tz),
    /// A fixed offset retaining the source-provided zone name.
    Fixed {
        /// Caller-provided fixed-zone name.
        name: GoString,
        /// Offset in seconds east of UTC.
        offset_seconds: i64,
    },
}

impl ResolvedTimeZone {
    /// Returns the stable source-visible location name.
    #[must_use]
    pub fn name(&self) -> String {
        match self {
            Self::Local => "Local".to_owned(),
            Self::Named(zone) => zone.name().to_owned(),
            Self::Fixed { name, .. } => name.to_utf8_lossy_go(),
        }
    }

    /// Returns the exact source-visible location-name bytes.
    #[must_use]
    pub fn name_bytes(&self) -> &[u8] {
        match self {
            Self::Local => b"Local",
            Self::Named(zone) => zone.name().as_bytes(),
            Self::Fixed { name, .. } => name.as_bytes(),
        }
    }
}

/// Go `TimeZoneLocation`. The lazily initialized location cache is not
/// serialized and, like Go's mutex-protected pointer, remains stable even if
/// the public name/offset fields are modified after the first lookup.
#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct TimeZoneLocation {
    /// IANA or fixed-zone name persisted by the DDL job.
    #[serde(rename = "name", default)]
    pub name: GoString,
    /// Fixed offset in seconds east of UTC; zero selects named-zone loading.
    #[serde(rename = "offset", default)]
    pub offset: i64,
    #[serde(skip)]
    pub(crate) location: OnceLock<GoShared<ResolvedTimeZone>>,
}

/// Go `tracing.TraceInfo`: persisted SQL tracing identity carried by a DDL job.
#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct TraceInfo {
    /// Alias of the SQL session that created the job.
    #[serde(rename = "session_alias", default)]
    pub session_alias: GoString,
    /// Statement trace identifier, preserving nil versus allocated-empty bytes.
    #[serde(
        rename = "trace_id",
        default,
        with = "crate::serde_helpers::go_shared_bytes"
    )]
    pub trace_id: GoSharedSlice<u8>,
    /// Connection identifier of the creating SQL session.
    #[serde(rename = "connection_id", default)]
    pub connection_id: u64,
}

/// Go `AddForeignKeyInfo` (runtime-only; no JSON fields in its owner).
#[derive(Clone, Debug, Default)]
pub struct AddForeignKeyInfo {
    /// Foreign-key constraint name.
    pub name: tidb_ast::CiString,
    /// Referencing column names.
    pub columns: GoSharedSlice<tidb_ast::CiString>,
}

/// Go `MultiSchemaInfo`.
#[derive(Clone, Debug, serde::Serialize)]
pub struct MultiSchemaInfo {
    /// Ordered nullable sub-job pointers, retaining nil/empty/capacity and
    /// outer-backing identity.
    #[serde(rename = "sub_jobs", default)]
    pub sub_jobs: GoSharedPointerSlice<SubJob>,
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
    pub add_columns: GoSharedSlice<tidb_ast::CiString>,
    /// Runtime set of columns being dropped.
    #[serde(skip)]
    pub drop_columns: GoSharedSlice<tidb_ast::CiString>,
    /// Runtime set of columns being modified.
    #[serde(skip)]
    pub modify_columns: GoSharedSlice<tidb_ast::CiString>,
    /// Runtime set of indexes being added.
    #[serde(skip)]
    pub add_indexes: GoSharedSlice<tidb_ast::CiString>,
    /// Runtime set of indexes being dropped.
    #[serde(skip)]
    pub drop_indexes: GoSharedSlice<tidb_ast::CiString>,
    /// Runtime set of indexes being altered.
    #[serde(skip)]
    pub alter_indexes: GoSharedSlice<tidb_ast::CiString>,
    /// Runtime foreign keys being added.
    #[serde(skip)]
    pub add_foreign_keys: GoSharedSlice<AddForeignKeyInfo>,
    /// Runtime columns referenced by positional clauses.
    #[serde(skip)]
    pub relative_columns: GoSharedSlice<tidb_ast::CiString>,
    /// Runtime target columns for positional clauses.
    #[serde(skip)]
    pub position_columns: GoSharedSlice<tidb_ast::CiString>,
}

impl Default for MultiSchemaInfo {
    fn default() -> Self {
        Self {
            sub_jobs: GoSharedPointerSlice::default(),
            revertible: true,
            seq: 0,
            skip_version: false,
            add_columns: GoSharedSlice::default(),
            drop_columns: GoSharedSlice::default(),
            modify_columns: GoSharedSlice::default(),
            add_indexes: GoSharedSlice::default(),
            drop_indexes: GoSharedSlice::default(),
            alter_indexes: GoSharedSlice::default(),
            add_foreign_keys: GoSharedSlice::default(),
            relative_columns: GoSharedSlice::default(),
            position_columns: GoSharedSlice::default(),
        }
    }
}

/// Go `SubJob` persisted fields.
#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct SubJob {
    /// DDL action performed by this sub-job.
    #[serde(rename = "type", default)]
    pub type_: ActionType,
    /// Runtime typed argument interface. The nil interface remains distinct
    /// from typed-nil and arbitrary dynamic values.
    #[serde(skip)]
    pub job_args: GoAny,
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
    pub warning: Option<GoShared<TerrorError>>,
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
    pub(crate) args: GoSharedSlice<GoAny>,
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
        let reorg_meta = parent.reorg_meta.as_ref().map(|parent_meta| {
            let meta = parent_meta.read().shallow_copy();
            let mut meta_value = meta.write();
            meta_value.reorg_type = self.reorg_type;
            meta_value.stage = self.reorg_stage;
            meta_value.analyze_state = self.analyze_state;
            drop(meta_value);
            meta
        });
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
            multi_schema_info: Some(GoShared::new(MultiSchemaInfo {
                revertible: self.revertible,
                seq: sequence as i32,
                ..Default::default()
            })),
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
            .expect("proxy Job.MultiSchemaInfo is required by FromProxyJob")
            .read()
            .revertible;
        self.schema_state = proxy.schema_state;
        self.snapshot_version = proxy.snapshot_version;
        self.real_start_ts = proxy.real_start_ts;
        self.args = proxy.args.clone();
        self.state = proxy.state;
        self.warning = proxy.warning.clone();
        self.row_count = proxy.row_count;
        self.schema_version = schema_version;
        if let Some(meta) = &proxy.reorg_meta {
            let meta = meta.read();
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
        cloned.args = GoSharedSlice::default();
        cloned
    }

    /// Installs the exact private `[]any` header produced by a version-1
    /// `JobArgs.getArgsV1` hook. The hook itself belongs to `job_args.go`.
    pub fn set_v1_decoded_args(&mut self, args: GoSharedSlice<GoAny>) {
        self.args = args;
    }

    /// Compatibility adapter for callers that have already erased argument
    /// dynamic types into JSON values. Source-shaped callers should install a
    /// Go interface slice with [`Self::set_v1_decoded_args`] instead.
    pub fn fill_raw_args(&mut self, args: Vec<serde_json::Value>) {
        self.args = GoSharedSlice::from_vec(
            args.into_iter()
                .map(|value| {
                    serde_json::from_value(value)
                        .expect("a serde_json::Value is always valid interface JSON")
                })
                .collect(),
        );
    }

    /// Go version-2 `SubJob.FillArgs`: the typed JobArgs interface is the one
    /// private argument element.
    pub fn fill_v2_args(&mut self) {
        self.args = GoSharedSlice::from_vec(vec![self.job_args.clone()]);
    }

    /// Returns a copied Go slice header for the private argument cache.
    #[must_use]
    pub fn decoded_args(&self) -> GoSharedSlice<GoAny> {
        self.args.clone()
    }
}

impl TimeZoneLocation {
    /// Go `GetLocation`.
    pub fn get_location(&self) -> Result<GoShared<ResolvedTimeZone>, String> {
        if let Some(location) = self.location.get() {
            return Ok(location.clone());
        }
        let resolved = if self.offset != 0 {
            Ok(ResolvedTimeZone::Fixed {
                name: self.name.clone(),
                offset_seconds: self.offset,
            })
        } else {
            let canonical = self
                .name
                .as_utf8()
                .map_err(|_| format!("unknown time zone {}", self.name))?;
            if canonical == "Local" {
                Ok(ResolvedTimeZone::Local)
            } else {
                let canonical = if canonical.is_empty() {
                    "UTC"
                } else {
                    canonical
                };
                canonical
                    .parse::<chrono_tz::Tz>()
                    .map(ResolvedTimeZone::Named)
                    .map_err(|_| format!("unknown time zone {canonical}"))
            }
        }?;
        let resolved = GoShared::new(resolved);
        let _ = self.location.set(resolved.clone());
        Ok(self.location.get().cloned().unwrap_or(resolved))
    }
}

#[derive(Default)]
pub(crate) struct JobMutex(Mutex<()>);

impl Clone for JobMutex {
    fn clone(&self) -> Self {
        Self::default()
    }
}

impl std::fmt::Debug for JobMutex {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("JobMutex")
    }
}

impl JobMutex {
    fn lock(&self) -> std::sync::MutexGuard<'_, ()> {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Go `Job`: the persisted DDL operation envelope.
#[derive(Clone, Debug, Default, serde::Serialize)]
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
    pub schema_name: GoString,
    /// Source table name used by scheduling involvement fallback.
    #[serde(rename = "table_name", default)]
    pub table_name: GoString,
    /// Current lifecycle state.
    #[serde(rename = "state", default)]
    pub state: JobState,
    /// Persisted warning payload.
    #[serde(rename = "warning", default)]
    pub warning: Option<GoShared<TerrorError>>,
    /// Persisted execution error payload.
    #[serde(rename = "err", default)]
    pub error: Option<GoShared<TerrorError>>,
    /// Number of execution errors observed.
    #[serde(rename = "err_count", default)]
    pub error_count: i64,
    /// Number of rows processed.
    #[serde(rename = "row_count", default)]
    pub row_count: i64,
    #[serde(skip)]
    pub(crate) mu: JobMutex,
    /// Runtime modify-column hint; not a precise persisted reorg decision.
    #[serde(skip)]
    pub need_reorg: bool,
    #[serde(skip)]
    pub(crate) args: GoSharedSlice<GoAny>,
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
    pub query: GoString,
    /// Schema-history snapshot written when the job finishes.
    #[serde(rename = "binlog", default)]
    pub binlog_info: Option<GoShared<HistoryInfo>>,
    /// Persisted job argument encoding version.
    #[serde(rename = "version", default)]
    pub version: JobVersion,
    /// Reorganization execution metadata.
    #[serde(rename = "reorg_meta", default)]
    pub reorg_meta: Option<GoShared<DDLReorgMeta>>,
    /// Multi-schema sub-job state, when present.
    #[serde(rename = "multi_schema_info", default)]
    pub multi_schema_info: Option<GoShared<MultiSchemaInfo>>,
    /// Operation priority used by index creation.
    #[serde(rename = "priority", default)]
    pub priority: i64,
    /// Ordering key used when moving jobs into DDL history.
    #[serde(rename = "seq_num", default)]
    pub sequence_number: u64,
    /// Character set captured when the job was created.
    #[serde(rename = "charset", default)]
    pub charset: GoString,
    /// Collation captured when the job was created.
    #[serde(rename = "collate", default)]
    pub collate: GoString,
    #[serde(
        rename = "involving_schema_info",
        default,
        skip_serializing_if = "shared_slice_is_empty"
    )]
    /// Explicit scheduling-lock objects; `None` activates name fallback.
    pub involving_schema_info: GoSharedSlice<InvolvingSchemaInfo>,
    /// Origin of an administrative command.
    #[serde(rename = "admin_operator", default)]
    pub admin_operator: AdminCommandOperator,
    #[serde(
        rename = "pause_reason",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    /// Durable reason for a system-initiated pause.
    pub pause_reason: Option<GoShared<JobPauseReason>>,
    #[serde(
        rename = "resume_reason",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    /// Durable reason for explicit resume.
    pub resume_reason: Option<GoShared<JobResumeReason>>,
    /// SQL tracing metadata retained with Go's typed/base64 field contract.
    #[serde(rename = "trace_info", default)]
    pub trace_info: Option<GoShared<TraceInfo>>,
    /// BDR cluster role captured for this DDL.
    #[serde(rename = "bdr_role", default)]
    pub bdr_role: GoString,
    /// CDC write-source identifier.
    #[serde(rename = "cdc_write_source", default)]
    pub cdc_write_source: u64,
    /// Deprecated flag for execution on the client-connected TiDB.
    #[serde(rename = "local_mode", default)]
    pub local_mode: bool,
    /// SQL mode used to execute the DDL statement.
    #[serde(rename = "sql_mode", default)]
    pub sql_mode: i64,
    #[serde(
        rename = "session_vars",
        default,
        skip_serializing_if = "shared_map_is_none_or_empty"
    )]
    /// Session system variables captured for DDL execution.
    pub session_vars: Option<GoShared<BTreeMap<GoString, GoString>>>,
    /// Latest schema version returned by the last execution step.
    #[serde(rename = "last_schema_version", default)]
    pub last_schema_version: i64,
}

/// Go `JobW`: a decoded job and the exact binary representation it came with.
#[derive(Clone, Debug)]
pub struct JobW {
    /// Decoded nullable job pointer.
    pub job: Option<GoShared<Job>>,
    /// Exact original binary representation.
    pub bytes: GoSharedSlice<u8>,
}

impl JobW {
    /// Go `NewJobW`. The byte vector is retained unchanged, including empty
    /// and non-JSON payloads; construction does not decode it.
    #[must_use]
    pub fn new(job: Option<GoShared<Job>>, bytes: GoSharedSlice<u8>) -> Self {
        Self { job, bytes }
    }
}

/// Shared warning and warning-count map handles returned by
/// [`Job::get_warnings`].
pub type JobWarnings = (
    Option<GoShared<crate::reorg::DDLWarningMap>>,
    Option<GoShared<crate::reorg::DDLWarningCountMap>>,
);

impl Job {
    /// Sets the processed row count.
    pub fn set_row_count(&mut self, count: i64) {
        let _guard = self.mu.lock();
        self.row_count = count;
    }

    /// Returns the processed row count.
    #[must_use]
    pub fn get_row_count(&self) -> i64 {
        let _guard = self.mu.lock();
        self.row_count
    }

    /// Replaces the reorganization warning maps.
    pub fn set_warnings(
        &mut self,
        warnings: Option<GoShared<crate::reorg::DDLWarningMap>>,
        warning_counts: Option<GoShared<crate::reorg::DDLWarningCountMap>>,
    ) {
        let _guard = self.mu.lock();
        let metadata = self
            .reorg_meta
            .as_ref()
            .expect("Job.ReorgMeta is required by SetWarnings");
        let mut metadata = metadata.write();
        metadata.warnings = warnings;
        metadata.warnings_count = warning_counts;
    }

    /// Returns aliases of the reorganization warning maps.
    #[must_use]
    pub fn get_warnings(&self) -> JobWarnings {
        let _guard = self.mu.lock();
        let metadata = self
            .reorg_meta
            .as_ref()
            .expect("Job.ReorgMeta is required by GetWarnings");
        let metadata = metadata.read();
        (metadata.warnings.clone(), metadata.warnings_count.clone())
    }

    /// Marks a table job finished and records its schema-history snapshot.
    pub fn finish_table_job(
        &mut self,
        state: JobState,
        schema_state: SchemaState,
        version: i64,
        table: Option<GoShared<TableInfo>>,
    ) {
        self.state = state;
        self.schema_state = schema_state;
        self.binlog_info
            .as_ref()
            .expect("Job.BinlogInfo is required by FinishTableJob")
            .write()
            .add_table_info(version, table);
    }

    /// Marks a multi-table job finished and records all affected tables.
    pub fn finish_multiple_table_job(
        &mut self,
        state: JobState,
        schema_state: SchemaState,
        version: i64,
        tables: &GoSharedPointerSlice<TableInfo>,
    ) {
        self.state = state;
        self.schema_state = schema_state;
        let binlog = self
            .binlog_info
            .as_ref()
            .expect("Job.BinlogInfo is required by FinishMultipleTableJob");
        let mut binlog = binlog.write();
        binlog.schema_version = version;
        // Go assigns the outer slice header before indexing its final element;
        // the empty-input panic therefore leaves the caller's nil or
        // allocated-empty header installed in history.
        binlog.multiple_table_infos = tables.clone();
        binlog.table_info = tables.get(
            tables
                .len()
                .checked_sub(1)
                .expect("FinishMultipleTableJob requires at least one table"),
        );
    }

    /// Marks a database job finished and records its database snapshot.
    pub fn finish_db_job(
        &mut self,
        state: JobState,
        schema_state: SchemaState,
        version: i64,
        database: Option<GoShared<DBInfo>>,
    ) {
        self.state = state;
        self.schema_state = schema_state;
        self.binlog_info
            .as_ref()
            .expect("Job.BinlogInfo is required by FinishDBJob")
            .write()
            .add_db_info(version, database);
    }

    /// Makes a multi-schema job permanently non-revertible.
    pub fn mark_non_revertible(&mut self) {
        if let Some(info) = &self.multi_schema_info {
            info.write().revertible = false;
        }
    }

    /// Installs the exact private `[]any` header produced by a version-1
    /// `JobArgs` or `FinishedJobArgs` hook.
    pub fn set_v1_decoded_args(&mut self, args: GoSharedSlice<GoAny>) {
        self.args = args;
    }

    /// Go version-2 `FillArgs`/`FillFinishedArgs`: one dynamic typed argument
    /// is stored in an allocated one-element `[]any`.
    pub fn fill_v2_arg(&mut self, argument: GoAny) {
        self.args = GoSharedSlice::from_vec(vec![argument]);
    }

    /// Returns a copied Go slice header for the private decoded argument cache.
    #[must_use]
    pub fn decoded_args(&self) -> GoSharedSlice<GoAny> {
        self.args.clone()
    }

    /// Encodes the job with Go-compatible JSON, optionally refreshing raw arguments.
    pub fn encode(&mut self, update_raw_args: bool) -> Result<Vec<u8>, serde_json::Error> {
        if update_raw_args {
            match marshal_args(self.version, &self.args) {
                Ok(raw_args) => self.raw_args = Some(raw_args),
                Err(error) => {
                    self.raw_args = None;
                    return Err(error);
                }
            }
            if let Some(info) = &self.multi_schema_info {
                let sub_jobs = info.read().sub_jobs.clone();
                for sub_job in sub_jobs.iter_handles() {
                    let sub_job = sub_job.expect("nil SubJob in MultiSchemaInfo.SubJobs");
                    let mut sub_job = sub_job.write();
                    if !sub_job.args.is_allocated() {
                        continue;
                    }
                    match marshal_args(self.version, &sub_job.args) {
                        Ok(raw_args) => sub_job.raw_args = Some(raw_args),
                        Err(error) => {
                            sub_job.raw_args = None;
                            return Err(error);
                        }
                    }
                }
            }
        }
        let _guard = self.mu.lock();
        crate::serde_helpers::to_go_json(&*self)
    }

    /// Explicit nullable receiver boundary for Go `(*Job).Encode`. The source
    /// dereferences a nil receiver while updating or marshaling it.
    pub fn encode_pointer(
        receiver: Option<&mut Self>,
        update_raw_args: bool,
    ) -> Result<Vec<u8>, serde_json::Error> {
        receiver.expect("nil *Job receiver").encode(update_raw_args)
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

    /// Explicit nullable receiver boundary for Go `(*Job).Decode`.
    pub fn decode_pointer(
        receiver: Option<&mut Self>,
        bytes: &[u8],
    ) -> Result<(), serde_json::Error> {
        // `json.Unmarshal` validates the document before checking whether its
        // destination pointer is usable.
        let _: &serde_json::value::RawValue = serde_json::from_slice(bytes)?;
        let Some(receiver) = receiver else {
            return Err(<serde_json::Error as serde::de::Error>::custom(
                "json: Unmarshal(nil *model.Job)",
            ));
        };
        receiver.decode(bytes)
    }

    /// Go `Job.Clone`: clones through the persisted codec, clears private
    /// decoded argument slices, and restores only each SubJob JobArgs
    /// interface value.
    #[must_use]
    pub fn deep_clone(&mut self) -> Option<Self> {
        let bytes = self.encode(true).ok()?;
        let mut cloned = Self::default();
        cloned.decode(&bytes).ok()?;
        if let Some(source_info) = &self.multi_schema_info {
            let source_sub_jobs = source_info.read().sub_jobs.clone();
            let cloned_sub_jobs = cloned
                .multi_schema_info
                .as_ref()
                .expect("encoded MultiSchemaInfo must decode")
                .read()
                .sub_jobs
                .clone();
            for index in 0..source_sub_jobs.len() {
                let source = source_sub_jobs
                    .get(index)
                    .expect("nil SubJob in source Job.Clone");
                let destination = cloned_sub_jobs
                    .get(index)
                    .expect("nil SubJob in cloned Job.Clone");
                destination.write().job_args = source.read().job_args.clone();
            }
        }
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
            .is_some_and(|value| value.read().type_ == reason)
    }

    /// Records a durable pause reason and message.
    pub fn set_pause_reason(&mut self, type_: impl Into<GoString>, message: impl Into<GoString>) {
        self.pause_reason = Some(GoShared::new(JobPauseReason {
            type_: type_.into(),
            message: message.into(),
        }));
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
            .is_some_and(|value| value.read().type_ == reason)
    }

    /// Records a durable resume reason.
    pub fn set_resume_reason(&mut self, type_: impl Into<GoString>) {
        self.resume_reason = Some(GoShared::new(JobResumeReason {
            type_: type_.into(),
        }));
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
    pub fn add_system_var(&mut self, name: impl Into<GoString>, value: impl Into<GoString>) {
        self.session_vars
            .as_ref()
            .expect("assignment to entry in nil SessionVars map")
            .write()
            .insert(name.into(), value.into());
    }

    /// Returns one captured session system variable.
    #[must_use]
    pub fn get_system_var(&self, name: &str) -> Option<GoString> {
        self.session_vars
            .as_ref()
            .and_then(|variables| variables.read().get(&GoString::from(name)).cloned())
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
            ActionType::ACTION_MULTI_SCHEMA_CHANGE => {
                let info = self
                    .multi_schema_info
                    .as_ref()
                    .expect("multi-schema job requires MultiSchemaInfo")
                    .read();
                info.sub_jobs.iter_deref().any(|sub_job| {
                    let sub_job = sub_job.read();
                    Job {
                        type_: sub_job.type_,
                        need_reorg: sub_job.need_reorg,
                        ..Default::default()
                    }
                    .may_need_reorg()
                })
            }
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
                    .read()
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
    pub fn get_involving_schema_info(&self) -> GoSharedSlice<InvolvingSchemaInfo> {
        if !self.involving_schema_info.is_empty() {
            return self.involving_schema_info.clone();
        }
        let table = if !self.schema_name.is_empty() && self.table_name.is_empty() {
            GoString::from(INVOLVING_ALL)
        } else {
            self.table_name.clone()
        };
        GoSharedSlice::from_vec(vec![InvolvingSchemaInfo {
            database: self.schema_name.clone(),
            table,
            ..Default::default()
        }])
    }

    /// Lowercases scheduling names while preserving `*` and empty sentinels.
    pub fn normalize_involving_schema_info(&mut self) {
        self.schema_name = normalize_involving_name(&self.schema_name);
        self.table_name = normalize_involving_name(&self.table_name);
        for index in 0..self.involving_schema_info.len() {
            self.involving_schema_info.update(index, |info| {
                info.database = normalize_involving_name(&info.database);
                info.table = normalize_involving_name(&info.table);
                info.policy = normalize_involving_name(&info.policy);
                info.resource_group = normalize_involving_name(&info.resource_group);
            });
        }
    }

    /// Validates that each scheduling entry identifies exactly one object kind.
    pub fn check_involving_schema_info(&self) -> Result<(), &'static str> {
        for info in self.get_involving_schema_info().snapshot() {
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
        self.args = GoSharedSlice::default();
    }
}

fn marshal_args(
    version: JobVersion,
    args: &GoSharedSlice<GoAny>,
) -> Result<PersistedRawJson, serde_json::Error> {
    let bytes = if version.0 <= JobVersion::V1.0 {
        crate::serde_helpers::to_go_json(args)?
    } else {
        let argument = if args.is_empty() {
            GoAny::nil()
        } else {
            args.get(0)
        };
        crate::serde_helpers::to_go_json(&argument)?
    };
    Ok(PersistedRawJson::from_marshaled_bytes(bytes))
}

fn normalize_involving_name(name: &GoString) -> GoString {
    if name == INVOLVING_ALL || name == INVOLVING_NONE {
        name.clone()
    } else {
        GoString::from(tidb_mysql::to_lowercase(&name.to_utf8_lossy_go()))
    }
}

fn shared_slice_is_empty<T>(values: &GoSharedSlice<T>) -> bool {
    values.is_empty()
}

fn shared_map_is_none_or_empty<K, V>(values: &Option<GoShared<BTreeMap<K, V>>>) -> bool {
    values
        .as_ref()
        .is_none_or(|values| values.read().is_empty())
}

impl std::fmt::Display for Job {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let start = format_tso_in_process_location(self.start_ts);
        let error = self
            .error
            .as_ref()
            .map_or_else(|| "<nil>".to_owned(), |error| error.read().to_string());
        write!(
            formatter,
            "ID:{}, Type:{}, State:{}, SchemaState:{}, SchemaID:{}, TableID:{}, RowCount:{}, ArgLen:{}, start time: {}, Err:{}, ErrCount:{}, SnapshotVersion:{}, Version: {}",
            self.id,
            self.type_,
            self.state,
            self.schema_state,
            self.schema_id,
            self.table_id,
            self.get_row_count(),
            self.args.len(),
            start,
            error,
            self.error_count,
            self.snapshot_version,
            self.version,
        )?;
        if let Some(metadata) = &self.reorg_meta {
            let metadata = metadata.read();
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
                metadata
                    .warnings
                    .as_ref()
                    .map_or(0, |warnings| warnings.read().len())
            )?;
        }
        if self.type_ != ActionType::ACTION_MULTI_SCHEMA_CHANGE {
            if let Some(info) = &self.multi_schema_info {
                let info = info.read();
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

fn format_tso_in_process_location(tso: u64) -> String {
    use chrono::TimeZone as _;

    let millis = crate::bdr::ts_convert_2_time(tso).unix_millis();
    let value = chrono::Local
        .timestamp_millis_opt(millis)
        .single()
        .expect("TSO physical milliseconds fit Chrono");
    let mut output = value.format("%Y-%m-%d %H:%M:%S").to_string();
    let fractional = millis.rem_euclid(1_000);
    if fractional != 0 {
        let fraction = format!("{fractional:03}");
        output.push('.');
        output.push_str(fraction.trim_end_matches('0'));
    }
    output.push(' ');
    output.push_str(&value.format("%z %Z").to_string());
    output
}

#[cfg(test)]
#[path = "job_tests.rs"]
mod tests;
