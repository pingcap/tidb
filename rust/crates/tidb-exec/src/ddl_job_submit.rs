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

//! Pinned Go `pkg/ddl/jobsubmit`: job construction, admission, ID allocation,
//! active-table insertion, and retry-attempt cleanup.

use std::collections::BTreeSet;
use std::fmt;

use tidb_meta::{key, value};
use tidb_metadef::MAX_USER_GLOBAL_ID;
use tidb_model::{
    ActionType, AdminCommandOperator, AlterTableModeArgs, AlterTableModeTarget, GoField, GoShared,
    GoSharedSlice, HistoryInfo, InvolvingSchemaInfo, Job, JobArgsValue, JobState, JobVersion,
    PartitionInfo, TableInfo, TraceInfo,
};
use tidb_txnkv::transaction::OptimisticMutation;

use crate::cluster_catalog::{ClusterCatalog, MetaSnapshot};
use crate::cluster_ddl::{DdlAdmissionError, DdlPlanError};
use crate::ddl_job_table::DdlJobTable;

/// Go `JobSpec`: the durable envelope plus its source-typed private arguments.
#[derive(Clone, Debug)]
pub struct JobSpec {
    /// Job envelope mutated by submission.
    pub job: Job,
    /// Typed arguments filled only after submission IDs are assigned.
    pub args: JobArgsValue,
    /// Whether the caller already assigned all action-owned object IDs.
    pub id_allocated: bool,
}

/// Go `GenGlobalIDs`: transaction-local allocation over the locked global-ID
/// key. The caller commits [`Self::mutation`] together with every inserted job
/// row, preserving Go's allocation/insertion atomicity.
pub(crate) struct GlobalIdAllocator {
    original: i64,
    current: i64,
}

impl GlobalIdAllocator {
    pub(crate) fn load<S: MetaSnapshot>(snapshot: &mut S) -> Result<Self, DdlPlanError> {
        let current = match snapshot.get(&key::next_global_id_kv_key())? {
            Some(stored) => value::parse_int_value(&stored)
                .map_err(|error| DdlPlanError::Encode(format!("NextGlobalID: {error}")))?,
            None => 0,
        };
        Ok(Self {
            original: current,
            current,
        })
    }

    pub(crate) fn allocate(&mut self, count: i64) -> Result<Vec<i64>, DdlPlanError> {
        let first = self
            .current
            .checked_add(1)
            .ok_or(DdlPlanError::GlobalIdExhausted { wanted: i64::MAX })?;
        let new_max = self
            .current
            .checked_add(count)
            .ok_or(DdlPlanError::GlobalIdExhausted { wanted: i64::MAX })?;
        if new_max > MAX_USER_GLOBAL_ID {
            return Err(DdlPlanError::GlobalIdExhausted { wanted: new_max });
        }
        self.current = new_max;
        Ok((first..=new_max).collect())
    }

    pub(crate) fn mutation(&self) -> Result<Option<OptimisticMutation>, DdlPlanError> {
        (self.current != self.original)
            .then(|| {
                OptimisticMutation::meta_put(
                    key::next_global_id_kv_key(),
                    value::encode_int_value(self.current),
                )
            })
            .transpose()
            .map_err(Into::into)
    }
}

fn create_table_args(spec: &JobSpec) -> GoShared<tidb_model::CreateTableArgs> {
    match &spec.args {
        JobArgsValue::CreateTable(Some(args)) => args.clone(),
        _ => panic!("Go JobSpec.Args is not *model.CreateTableArgs"),
    }
}

fn batch_create_table_args(spec: &JobSpec) -> GoShared<tidb_model::BatchCreateTableArgs> {
    match &spec.args {
        JobArgsValue::BatchCreateTable(Some(args)) => args.clone(),
        _ => panic!("Go JobSpec.Args is not *model.BatchCreateTableArgs"),
    }
}

fn create_schema_args(spec: &JobSpec) -> GoShared<tidb_model::CreateSchemaArgs> {
    match &spec.args {
        JobArgsValue::CreateSchema(Some(args)) => args.clone(),
        _ => panic!("Go JobSpec.Args is not *model.CreateSchemaArgs"),
    }
}

fn resource_group_args(spec: &JobSpec) -> GoShared<tidb_model::ResourceGroupArgs> {
    match &spec.args {
        JobArgsValue::ResourceGroup(Some(args)) => args.clone(),
        _ => panic!("Go JobSpec.Args is not *model.ResourceGroupArgs"),
    }
}

fn table_partition_args(spec: &JobSpec) -> GoShared<tidb_model::TablePartitionArgs> {
    match &spec.args {
        JobArgsValue::TablePartition(Some(args)) => args.clone(),
        _ => panic!("Go JobSpec.Args is not *model.TablePartitionArgs"),
    }
}

fn truncate_table_args(spec: &JobSpec) -> GoShared<tidb_model::TruncateTableArgs> {
    match &spec.args {
        JobArgsValue::TruncateTable(Some(args)) => args.clone(),
        _ => panic!("Go JobSpec.Args is not *model.TruncateTableArgs"),
    }
}

fn table_id_count(table: &TableInfo) -> usize {
    1 + table
        .get_partition_info()
        .map_or(0, |partition| partition.read().definitions.len())
}

/// Pinned Go `getRequiredGIDCount`.
#[must_use]
pub fn required_global_id_count(specs: &[JobSpec]) -> usize {
    let mut count = specs.len();
    for spec in specs {
        if spec.id_allocated {
            continue;
        }
        match spec.job.type_ {
            ActionType::ACTION_CREATE_VIEW
            | ActionType::ACTION_CREATE_SEQUENCE
            | ActionType::ACTION_CREATE_TABLE => {
                let args = create_table_args(spec);
                let table = args
                    .read()
                    .table_info
                    .get()
                    .expect("nil CreateTableArgs.TableInfo");
                count += table_id_count(&table.read());
            }
            ActionType::ACTION_CREATE_TABLES => {
                let args = batch_create_table_args(spec);
                for table_args in args.read().tables.get().iter_deref() {
                    let table = table_args
                        .read()
                        .table_info
                        .get()
                        .expect("nil CreateTableArgs.TableInfo");
                    count += table_id_count(&table.read());
                }
            }
            ActionType::ACTION_CREATE_SCHEMA | ActionType::ACTION_CREATE_RESOURCE_GROUP => {
                count += 1;
            }
            ActionType::ACTION_ALTER_TABLE_PARTITIONING => {
                let args = table_partition_args(spec);
                let partition = args
                    .read()
                    .part_info
                    .get()
                    .expect("nil TablePartitionArgs.PartInfo");
                count += 1 + partition.read().definitions.len();
            }
            ActionType::ACTION_TRUNCATE_TABLE_PARTITION => {
                count += truncate_table_args(spec)
                    .read()
                    .old_partition_ids
                    .read()
                    .len();
            }
            ActionType::ACTION_ADD_TABLE_PARTITION
            | ActionType::ACTION_REORGANIZE_PARTITION
            | ActionType::ACTION_REMOVE_PARTITIONING => {
                let args = table_partition_args(spec);
                let partition = args
                    .read()
                    .part_info
                    .get()
                    .expect("nil TablePartitionArgs.PartInfo");
                count += partition.read().definitions.len();
            }
            ActionType::ACTION_TRUNCATE_TABLE => {
                count += 1 + truncate_table_args(spec)
                    .read()
                    .old_partition_ids
                    .read()
                    .len();
            }
            _ => {}
        }
    }
    count
}

struct GlobalIdAssigner<'a> {
    ids: &'a [i64],
    next: usize,
}

impl GlobalIdAssigner<'_> {
    fn next(&mut self) -> i64 {
        let id = self.ids[self.next];
        self.next += 1;
        id
    }

    fn assign_partition(&mut self, partition: &GoShared<PartitionInfo>) {
        let definitions = partition.read().definitions.clone();
        for index in 0..definitions.len() {
            definitions.update(index, |definition| definition.id = self.next());
        }
    }

    fn assign_table(&mut self, table: &GoShared<TableInfo>) {
        table.write().id = self.next();
        if let Some(partition) = table.read().get_partition_info() {
            self.assign_partition(&partition);
        }
    }
}

/// Pinned Go `assignGIDsForJobs`.
///
/// This intentionally retains Go's panic boundaries for mismatched typed
/// arguments or an ID slice whose length disagrees with
/// [`required_global_id_count`].
pub fn assign_global_ids(specs: &mut [JobSpec], ids: &[i64]) {
    assert_eq!(ids.len(), required_global_id_count(specs));
    let mut allocator = GlobalIdAssigner { ids, next: 0 };
    for spec in specs {
        match spec.job.type_ {
            ActionType::ACTION_CREATE_VIEW
            | ActionType::ACTION_CREATE_SEQUENCE
            | ActionType::ACTION_CREATE_TABLE => {
                let args = create_table_args(spec);
                let table = args
                    .read()
                    .table_info
                    .get()
                    .expect("nil CreateTableArgs.TableInfo");
                if !spec.id_allocated {
                    allocator.assign_table(&table);
                }
                spec.job.table_id = table.read().id;
            }
            ActionType::ACTION_CREATE_TABLES => {
                if !spec.id_allocated {
                    for table_args in batch_create_table_args(spec)
                        .read()
                        .tables
                        .get()
                        .iter_deref()
                    {
                        let table = table_args
                            .read()
                            .table_info
                            .get()
                            .expect("nil CreateTableArgs.TableInfo");
                        allocator.assign_table(&table);
                    }
                }
            }
            ActionType::ACTION_CREATE_SCHEMA => {
                let database = create_schema_args(spec)
                    .read()
                    .db_info
                    .get()
                    .expect("nil CreateSchemaArgs.DBInfo");
                if !spec.id_allocated {
                    database.write().id = allocator.next();
                }
                spec.job.schema_id = database.read().id;
            }
            ActionType::ACTION_CREATE_RESOURCE_GROUP => {
                if !spec.id_allocated {
                    resource_group_args(spec)
                        .read()
                        .resource_group_info
                        .get()
                        .expect("nil ResourceGroupArgs.RGInfo")
                        .write()
                        .id = allocator.next();
                }
            }
            ActionType::ACTION_ALTER_TABLE_PARTITIONING => {
                if !spec.id_allocated {
                    let partition = table_partition_args(spec)
                        .read()
                        .part_info
                        .get()
                        .expect("nil TablePartitionArgs.PartInfo");
                    allocator.assign_partition(&partition);
                    partition.write().new_table_id = allocator.next();
                }
            }
            ActionType::ACTION_ADD_TABLE_PARTITION | ActionType::ACTION_REORGANIZE_PARTITION => {
                if !spec.id_allocated {
                    let partition = table_partition_args(spec)
                        .read()
                        .part_info
                        .get()
                        .expect("nil TablePartitionArgs.PartInfo");
                    allocator.assign_partition(&partition);
                }
            }
            ActionType::ACTION_REMOVE_PARTITIONING => {
                let partition = table_partition_args(spec)
                    .read()
                    .part_info
                    .get()
                    .expect("nil TablePartitionArgs.PartInfo");
                if !spec.id_allocated {
                    allocator.assign_partition(&partition);
                }
                let first_id = partition.read().definitions.get(0).id;
                partition.write().new_table_id = first_id;
            }
            ActionType::ACTION_TRUNCATE_TABLE | ActionType::ACTION_TRUNCATE_TABLE_PARTITION => {
                if !spec.id_allocated {
                    let args = truncate_table_args(spec);
                    let args = args.read();
                    if spec.job.type_ == ActionType::ACTION_TRUNCATE_TABLE {
                        args.new_table_id.set(allocator.next());
                    }
                    let partition_count = args.old_partition_ids.read().len();
                    args.new_partition_ids.set(GoSharedSlice::from_vec(
                        (0..partition_count).map(|_| allocator.next()).collect(),
                    ));
                }
            }
            _ => {}
        }
        spec.job.id = allocator.next();
    }
    assert_eq!(allocator.next, ids.len());
}

/// A refusal raised by pinned Go's `jobsubmit` package before insertion.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JobSubmitError {
    /// `infoschema.ErrInvalidTableModeSet` (8259).
    InvalidTableModeSet {
        /// Current table mode.
        current: tidb_model::TableMode,
        /// Requested table mode.
        target: tidb_model::TableMode,
        /// Target table name.
        table: String,
    },
    /// `Job.CheckInvolvingSchemaInfo` refused malformed scheduler ownership.
    InvalidInvolvingSchemaInfo(&'static str),
    /// `dbterror.ErrBDRRestrictedDDL` (8263).
    BdrRestricted(String),
    /// An active flashback-cluster job excludes every new DDL submission.
    FlashbackClusterJob,
}

impl JobSubmitError {
    /// MySQL/TiDB error code when Go exposes a named SQL error.
    #[must_use]
    pub const fn code(&self) -> u16 {
        match self {
            Self::InvalidTableModeSet { .. } => tidb_error::tidb::errcode::ErrInvalidTableModeSet,
            Self::InvalidInvolvingSchemaInfo(_) => tidb_error::mysql::errcode::ErrUnknown,
            Self::BdrRestricted(_) => tidb_error::tidb::errcode::ErrBDRRestrictedDDL,
            Self::FlashbackClusterJob => tidb_error::mysql::errcode::ErrUnknown,
        }
    }
}

impl fmt::Display for JobSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTableModeSet {
                current,
                target,
                table,
            } => write!(
                formatter,
                "Invalid mode set from (or by default) {current} to {target} for table {table}"
            ),
            Self::InvalidInvolvingSchemaInfo(message) => formatter.write_str(message),
            Self::BdrRestricted(role) => write!(
                formatter,
                "The operation is not allowed while the bdr role of this cluster is set to {role}."
            ),
            Self::FlashbackClusterJob => {
                formatter.write_str("Can't add ddl job, have flashback cluster job")
            }
        }
    }
}

impl std::error::Error for JobSubmitError {}

/// Go `BuildAlterTableModeJob`.
///
/// `Ok(None)` is Go's `noop=true`: the table is already in the requested mode.
pub fn build_alter_table_mode_job(
    context: &tidb_executor::StmtContext,
    target: &AlterTableModeTarget,
) -> Result<Option<(Job, GoShared<AlterTableModeArgs>)>, JobSubmitError> {
    if !target.current_mode.can_transition_to(target.target_mode) {
        return Err(JobSubmitError::InvalidTableModeSet {
            current: target.current_mode,
            target: target.target_mode,
            table: target.table_name.original().to_owned(),
        });
    }
    if target.current_mode == target.target_mode {
        return Ok(None);
    }

    let args = GoShared::new(AlterTableModeArgs {
        table_mode: GoField::new(target.target_mode),
        schema_id: GoField::new(target.schema_id),
        table_id: GoField::new(target.table_id),
    });
    let mut job = Job::default();
    job.version = JobVersion::V2;
    job.schema_id = target.schema_id;
    job.table_id = target.table_id;
    job.schema_name = target.schema_name.lowercase().into();
    job.table_name = target.table_name.lowercase().to_owned().into();
    job.type_ = ActionType::ACTION_ALTER_TABLE_MODE;
    job.query = "skip".into();
    job.binlog_info = Some(GoShared::new(HistoryInfo::default()));
    job.cdc_write_source = context.ddl_cdc_write_source();
    job.sql_mode = context.ddl_sql_mode();
    job.involving_schema_info = GoSharedSlice::from_vec(vec![InvolvingSchemaInfo {
        database: target.schema_name.lowercase().into(),
        table: target.table_name.lowercase().to_owned().into(),
        ..Default::default()
    }]);
    Ok(Some((job, args)))
}

/// Go `SubmitBatch`'s action-independent mutation of one job envelope.
///
/// BDR admission, flashback exclusion, and ID assignment require the submit
/// transaction's metadata snapshot and are performed by the transaction-level
/// planner. This function is deliberately limited to the exact per-job common
/// state Go applies before allocating IDs.
pub fn prepare_spec_for_submit(
    spec: &mut JobSpec,
    start_ts: u64,
    bdr_role: &[u8],
    upgrading: bool,
) -> Result<(), JobSubmitError> {
    let job = &mut spec.job;
    job.normalize_involving_schema_info();
    job.check_involving_schema_info()
        .map_err(JobSubmitError::InvalidInvolvingSchemaInfo)?;
    if job.trace_info.is_none() {
        job.trace_info = Some(GoShared::new(TraceInfo::default()));
    }
    job.start_ts = start_ts;
    job.bdr_role = bdr_role.to_vec().into();
    let role = match bdr_role {
        b"primary" => Some(tidb_ast::BdrRole::Primary),
        b"secondary" => Some(tidb_ast::BdrRole::Secondary),
        _ => None,
    };
    if job.cdc_write_source == 0
        && role.is_some()
        && !tidb_util::filter::is_system_schema(&job.schema_name.to_utf8_lossy_go())
    {
        let denied = if job.type_ == ActionType::ACTION_MULTI_SCHEMA_CHANGE {
            job.multi_schema_info.as_ref().is_some_and(|info| {
                info.read().sub_jobs.iter_deref().any(|sub_job| {
                    let sub_job = sub_job.read();
                    tidb_model::ddl_bdr::is_denied(role, sub_job.type_, sub_job.job_args_value())
                })
            })
        } else {
            tidb_model::ddl_bdr::is_denied(role, job.type_, Some(&spec.args))
        };
        if denied {
            return Err(JobSubmitError::BdrRestricted(
                String::from_utf8_lossy(bdr_role).into_owned(),
            ));
        }
    }
    set_job_state_to_queueing(job);

    let has_system_database = job
        .get_involving_schema_info()
        .snapshot()
        .iter()
        .any(|info| tidb_metadef::is_system_related_db(&info.database.to_utf8_lossy_go()));
    if upgrading && !has_system_database {
        job.state = JobState::PAUSING;
        job.admin_operator = AdminCommandOperator::BY_SYSTEM;
    }
    Ok(())
}

fn submit_error(error: JobSubmitError) -> DdlPlanError {
    DdlPlanError::Admission(DdlAdmissionError::with_code(
        error.code(),
        error.to_string(),
    ))
}

fn string_for_ids(ids: impl IntoIterator<Item = i64>) -> String {
    ids.into_iter()
        .map(|id| id.to_string())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>()
        .join(",")
}

/// Pinned Go `job2SchemaIDs` for every currently source-typed job argument.
#[must_use]
pub fn job_schema_ids(spec: &JobSpec) -> String {
    match (spec.job.type_, &spec.args) {
        (ActionType::ACTION_RENAME_TABLES, JobArgsValue::RenameTables(Some(args))) => {
            string_for_ids(
                args.read()
                    .rename_table_infos
                    .get()
                    .iter_deref()
                    .flat_map(|info| {
                        let info = info.read();
                        [info.old_schema_id, info.new_schema_id]
                    }),
            )
        }
        (ActionType::ACTION_RENAME_TABLE, JobArgsValue::RenameTable(Some(args))) => {
            string_for_ids([args.read().old_schema_id, spec.job.schema_id])
        }
        (
            ActionType::ACTION_EXCHANGE_TABLE_PARTITION,
            JobArgsValue::ExchangeTablePartition(Some(args)),
        ) => string_for_ids([
            spec.job.schema_id,
            args.read().partitioned_table_schema_id.get(),
        ]),
        _ => spec.job.schema_id.to_string(),
    }
}

/// Pinned Go `job2TableIDs` for every currently source-typed job argument.
#[must_use]
pub fn job_table_ids(spec: &JobSpec) -> String {
    match (spec.job.type_, &spec.args) {
        (ActionType::ACTION_RENAME_TABLES, JobArgsValue::RenameTables(Some(args))) => {
            string_for_ids(
                args.read()
                    .rename_table_infos
                    .get()
                    .iter_deref()
                    .map(|info| info.read().table_id),
            )
        }
        (
            ActionType::ACTION_EXCHANGE_TABLE_PARTITION,
            JobArgsValue::ExchangeTablePartition(Some(args)),
        ) => string_for_ids([spec.job.table_id, args.read().partitioned_table_id.get()]),
        (ActionType::ACTION_TRUNCATE_TABLE, JobArgsValue::TruncateTable(Some(args))) => {
            format!("{},{}", spec.job.table_id, args.read().new_table_id.get())
        }
        // Go `job2TableIDs` (master `94a9cbedab`): a materialized view
        // reports the view id plus every created log id; a log reports its
        // id plus the base table id when one is recorded.
        (
            ActionType::ACTION_CREATE_MATERIALIZED_VIEW,
            JobArgsValue::CreateMaterializedView(Some(args)),
        ) => {
            let args = args.read();
            let mlog_ids: Vec<i64> = args.mlog_table_ids.get().snapshot();
            if !mlog_ids.is_empty() {
                let mut ids = Vec::with_capacity(mlog_ids.len() + 1);
                ids.push(spec.job.table_id);
                ids.extend(mlog_ids);
                string_for_ids(ids)
            } else {
                spec.job.table_id.to_string()
            }
        }
        (
            ActionType::ACTION_CREATE_MATERIALIZED_VIEW_LOG,
            JobArgsValue::CreateMaterializedViewLog(Some(args)),
        ) => {
            let args = args.read();
            if let Some(table_info) = args.table_info.get() {
                let base_table_id = table_info
                    .read()
                    .materialized_view_log
                    .as_ref()
                    .map(|log| log.read().base_table_id)
                    .unwrap_or(0);
                if base_table_id > 0 {
                    return string_for_ids([spec.job.table_id, base_table_id]);
                }
            }
            spec.job.table_id.to_string()
        }
        _ => spec.job.table_id.to_string(),
    }
}

/// Applies pinned Go `SubmitBatch`'s one-time admission and envelope mutation.
/// This phase is deliberately outside the ID-allocation retry loop.
pub fn prepare_submit_batch<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    specs: &mut [JobSpec],
    start_ts: u64,
    upgrading: bool,
    min_job_id: i64,
) -> Result<(), DdlPlanError> {
    if specs.is_empty() {
        return Ok(());
    }
    let system_tables = crate::ddl_systable::SystemTableManager::new(catalog);
    if system_tables
        .has_flashback_cluster_job(snapshot, min_job_id)
        .map_err(|error| DdlPlanError::Encode(error.to_string()))?
    {
        return Err(submit_error(JobSubmitError::FlashbackClusterJob));
    }
    let bdr_role = snapshot.get(&key::bdr_role_kv_key())?.unwrap_or_default();
    for spec in specs.iter_mut() {
        prepare_spec_for_submit(spec, start_ts, &bdr_role, upgrading).map_err(submit_error)?;
    }
    Ok(())
}

/// Plans pinned Go's locked `GenGlobalIDs` and `assignGIDsForJobs` portion of
/// one insertion attempt.
pub fn plan_assign_global_ids<S: MetaSnapshot>(
    snapshot: &mut S,
    specs: &mut [JobSpec],
) -> Result<Vec<OptimisticMutation>, DdlPlanError> {
    if specs.is_empty() {
        return Ok(Vec::new());
    }
    let count = i64::try_from(required_global_id_count(specs))
        .map_err(|_| DdlPlanError::GlobalIdExhausted { wanted: i64::MAX })?;
    let mut allocator = GlobalIdAllocator::load(snapshot)?;
    let ids = allocator.allocate(count)?;
    assign_global_ids(specs, &ids);

    let mut mutations = Vec::new();
    if let Some(global_id) = allocator.mutation()? {
        mutations.push(global_id);
    }
    Ok(mutations)
}

/// Plans pinned Go `insertDDLJobs2Table` after IDs and any caller callback
/// have been applied.
pub fn plan_insert_job_rows(
    catalog: &ClusterCatalog,
    specs: &mut [JobSpec],
    mutations: &mut Vec<OptimisticMutation>,
) -> Result<(), DdlPlanError> {
    if specs.is_empty() {
        return Ok(());
    }
    let job_table =
        DdlJobTable::locate(catalog).map_err(|error| DdlPlanError::Encode(error.to_string()))?;
    for spec in specs {
        if spec.job.type_ == ActionType::ACTION_MULTI_SCHEMA_CHANGE {
            if let Some(info) = &spec.job.multi_schema_info {
                for sub_job in info.read().sub_jobs.iter_deref() {
                    sub_job.write().fill_v2_args();
                }
            }
        } else {
            spec.args.fill_job(&mut spec.job);
        }
        let reorg = spec.job.may_need_reorg();
        let schema_ids = job_schema_ids(spec);
        let table_ids = job_table_ids(spec);
        job_table
            .append_insert(
                &mut spec.job,
                reorg,
                &schema_ids,
                &table_ids,
                false,
                mutations,
            )
            .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
    }
    Ok(())
}

/// Plans the ID-allocation and row-encoding portion of one pinned Go
/// `GenGIDAndInsertJobsWithRetry` transaction attempt.
///
/// The callback runs at the same source boundary as Go: every ID is already
/// assigned, but no job row has been encoded. Its cleanup runs immediately if
/// row planning fails; callers pass it to [`finish_insert_attempt`] so a
/// failed commit performs the same cleanup while a successful commit disarms
/// it.
pub fn plan_insert_attempt<S, F, Cleanup>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    specs: &mut [JobSpec],
    before_insert_with_assigned_ids: &mut F,
) -> Result<(Vec<OptimisticMutation>, Option<Cleanup>), DdlPlanError>
where
    S: MetaSnapshot,
    F: FnMut(&[JobSpec]) -> Option<Cleanup>,
    Cleanup: FnOnce(),
{
    let mut mutations = plan_assign_global_ids(snapshot, specs)?;
    let cleanup = before_insert_with_assigned_ids(specs);
    if let Err(error) = plan_insert_job_rows(catalog, specs, &mut mutations) {
        if let Some(cleanup) = cleanup {
            cleanup();
        }
        return Err(error);
    }
    Ok((mutations, cleanup))
}

/// Completes one pinned Go insertion attempt's deferred cleanup contract.
/// Failed attempts clean up their assigned-ID registrations; successful
/// attempts retain them for the scheduler/waiter lifecycle.
pub fn finish_insert_attempt<T, E, Cleanup>(
    result: Result<T, E>,
    cleanup: Option<Cleanup>,
) -> Result<T, E>
where
    Cleanup: FnOnce(),
{
    if result.is_err() {
        if let Some(cleanup) = cleanup {
            cleanup();
        }
    }
    result
}

/// Go `setJobStateToQueueing`.
pub fn set_job_state_to_queueing(job: &mut Job) {
    if job.type_ == ActionType::ACTION_MULTI_SCHEMA_CHANGE {
        if let Some(info) = &job.multi_schema_info {
            for sub_job in info.read().sub_jobs.iter_deref() {
                sub_job.write().state = JobState::QUEUEING;
            }
        }
    }
    job.state = JobState::QUEUEING;
}

/// Go `HasFlashbackClusterJob` admission in `SubmitBatch`.
pub fn ensure_no_flashback_cluster_job<'a>(
    jobs: impl IntoIterator<Item = &'a Job>,
) -> Result<(), JobSubmitError> {
    if jobs
        .into_iter()
        .any(|job| job.type_ == ActionType::ACTION_FLASHBACK_CLUSTER)
    {
        return Err(JobSubmitError::FlashbackClusterJob);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_model::{
        BatchCreateTableArgs, CreateTableArgs, GoAny, GoSharedPointerSlice, IndexArg, JobArgs,
        ModifyIndexArgs, MultiSchemaInfo, PartitionDefinition, RenameTableArgs, RenameTablesArgs,
        ResourceGroupArgs, ResourceGroupInfo, SubJob, TableMode, TablePartitionArgs,
        TruncateTableArgs,
    };

    fn job(action: ActionType) -> Job {
        let mut job = Job::default();
        job.type_ = action;
        job
    }

    fn spec(job: Job) -> JobSpec {
        JobSpec {
            job,
            args: tidb_model::EmptyArgs::into_job_args_value(Some(GoShared::new(
                tidb_model::EmptyArgs::default(),
            ))),
            id_allocated: true,
        }
    }

    fn create_table_spec(partitions: usize, id_allocated: bool) -> JobSpec {
        let table = GoShared::new(TableInfo {
            id: id_allocated.then_some(501).unwrap_or_default(),
            partition: (partitions != 0).then(|| {
                GoShared::new(PartitionInfo {
                    enable: true,
                    definitions: GoSharedSlice::from_vec(
                        (0..partitions)
                            .map(|_| PartitionDefinition::default())
                            .collect(),
                    ),
                    ..Default::default()
                })
            }),
            ..Default::default()
        });
        JobSpec {
            job: job(ActionType::ACTION_CREATE_TABLE),
            args: CreateTableArgs::into_job_args_value(Some(GoShared::new(CreateTableArgs {
                table_info: GoField::new(Some(table)),
                ..Default::default()
            }))),
            id_allocated,
        }
    }

    fn partition_spec(action: ActionType, partitions: usize, id_allocated: bool) -> JobSpec {
        JobSpec {
            job: job(action),
            args: TablePartitionArgs::into_job_args_value(Some(GoShared::new(
                TablePartitionArgs {
                    part_info: GoField::new(Some(GoShared::new(PartitionInfo {
                        definitions: GoSharedSlice::from_vec(
                            (0..partitions)
                                .map(|_| PartitionDefinition::default())
                                .collect(),
                        ),
                        ..Default::default()
                    }))),
                    ..Default::default()
                },
            ))),
            id_allocated,
        }
    }

    fn truncate_spec(action: ActionType, partitions: usize, id_allocated: bool) -> JobSpec {
        JobSpec {
            job: job(action),
            args: TruncateTableArgs::into_job_args_value(Some(GoShared::new(TruncateTableArgs {
                old_partition_ids: GoField::new(GoSharedSlice::from_vec(
                    (0..partitions).map(|index| index as i64 + 100).collect(),
                )),
                ..Default::default()
            }))),
            id_allocated,
        }
    }

    fn target(current_mode: TableMode, target_mode: TableMode) -> AlterTableModeTarget {
        AlterTableModeTarget {
            schema_id: 101,
            schema_name: tidb_ast::CiString::new("TestDB"),
            table_id: 202,
            table_name: tidb_ast::CiString::new("T1"),
            current_mode,
            target_mode,
        }
    }

    #[test]
    fn alter_table_mode_job_matches_go() {
        let context = tidb_executor::StmtContext::for_query()
            .with_ddl_sql_mode(4)
            .with_ddl_job_context(7, 1, "", Vec::new());
        let (job, args) =
            build_alter_table_mode_job(&context, &target(TableMode::NORMAL, TableMode::IMPORT))
                .unwrap()
                .unwrap();
        assert_eq!(job.version, tidb_model::JobVersion::V2);
        assert_eq!(job.schema_id, 101);
        assert_eq!(job.table_id, 202);
        assert_eq!(job.schema_name, "testdb");
        assert_eq!(job.table_name, "t1");
        assert_eq!(job.type_, ActionType::ACTION_ALTER_TABLE_MODE);
        assert_eq!(job.query, "skip");
        assert!(job.binlog_info.is_some());
        assert_eq!(job.cdc_write_source, 7);
        assert_eq!(job.sql_mode, 4);
        assert_eq!(job.involving_schema_info.len(), 1);
        assert_eq!(job.involving_schema_info.get(0).database, "testdb");
        assert_eq!(job.involving_schema_info.get(0).table, "t1");
        assert_eq!(*args.read().table_mode.read(), TableMode::IMPORT);
        assert_eq!(*args.read().schema_id.read(), 101);
        assert_eq!(*args.read().table_id.read(), 202);
    }

    #[test]
    fn alter_table_mode_noop_and_invalid_match_go() {
        let context = tidb_executor::StmtContext::for_query();
        assert!(build_alter_table_mode_job(
            &context,
            &target(TableMode::IMPORT, TableMode::IMPORT)
        )
        .unwrap()
        .is_none());
        let error =
            build_alter_table_mode_job(&context, &target(TableMode::IMPORT, TableMode::RESTORE))
                .unwrap_err();
        assert_eq!(error.code(), 8259);
        assert_eq!(
            error.to_string(),
            "Invalid mode set from (or by default) Import to Restore for table T1"
        );
    }

    #[test]
    fn common_submit_preflight_normalizes_traces_and_pauses_like_go() {
        let mut job = Job::default();
        job.type_ = ActionType::ACTION_ALTER_TABLE_MODE;
        job.schema_name = "TestDB".into();
        job.table_name = "T1".into();
        job.cdc_write_source = 7;
        job.involving_schema_info = GoSharedSlice::from_vec(vec![InvolvingSchemaInfo {
            database: "AnotherDB".into(),
            table: "T2".into(),
            ..Default::default()
        }]);
        let mut table_mode = spec(job);
        prepare_spec_for_submit(&mut table_mode, 99, b"primary", true).unwrap();
        assert_eq!(table_mode.job.schema_name, "testdb");
        assert_eq!(table_mode.job.table_name, "t1");
        assert_eq!(
            table_mode.job.involving_schema_info.get(0).database,
            "anotherdb"
        );
        assert_eq!(table_mode.job.involving_schema_info.get(0).table, "t2");
        assert!(table_mode.job.trace_info.is_some());
        assert_eq!(table_mode.job.start_ts, 99);
        assert_eq!(table_mode.job.bdr_role.as_bytes(), b"primary");
        assert_eq!(table_mode.job.state, JobState::PAUSING);
        assert_eq!(
            table_mode.job.admin_operator,
            AdminCommandOperator::BY_SYSTEM
        );

        let sub_jobs = GoSharedPointerSlice::from_nullable(vec![Some(SubJob::default())]);
        let mut multi = Job::default();
        multi.type_ = ActionType::ACTION_MULTI_SCHEMA_CHANGE;
        multi.schema_name = "mysql".into();
        multi.table_name = "t".into();
        multi.multi_schema_info = Some(GoShared::new(MultiSchemaInfo {
            sub_jobs: sub_jobs.clone(),
            ..Default::default()
        }));
        let mut multi = spec(multi);
        prepare_spec_for_submit(&mut multi, 100, b"", true).unwrap();
        assert_eq!(multi.job.state, JobState::QUEUEING);
        assert_eq!(sub_jobs.get(0).unwrap().read().state, JobState::QUEUEING);
    }

    #[test]
    fn common_submit_preflight_rejects_invalid_ownership_and_bdr_like_go() {
        let mut invalid = Job::default();
        invalid.type_ = ActionType::ACTION_ALTER_TABLE_MODE;
        invalid.schema_name = "test".into();
        invalid.table_name = "t".into();
        invalid.involving_schema_info =
            GoSharedSlice::from_vec(vec![InvolvingSchemaInfo::default()]);
        let error = prepare_spec_for_submit(&mut spec(invalid), 1, b"", false).unwrap_err();
        assert!(matches!(
            error,
            JobSubmitError::InvalidInvolvingSchemaInfo(_)
        ));

        let mut restricted = Job::default();
        restricted.type_ = ActionType::ACTION_ALTER_TABLE_MODE;
        restricted.schema_name = "test".into();
        restricted.table_name = "t".into();
        let error =
            prepare_spec_for_submit(&mut spec(restricted), 2, b"primary", false).unwrap_err();
        assert_eq!(error.code(), 8263);
        assert_eq!(
            error.to_string(),
            "The operation is not allowed while the bdr role of this cluster is set to primary."
        );
    }

    #[test]
    fn bdr_admission_reads_modify_index_args_for_jobs_and_subjobs() {
        let modify_index = |unique| {
            JobArgsValue::ModifyIndex(Some(GoShared::new(ModifyIndexArgs {
                index_args: vec![IndexArg {
                    unique,
                    ..Default::default()
                }]
                .into(),
                ..Default::default()
            })))
        };

        for action in [
            ActionType::ACTION_ADD_INDEX,
            ActionType::ACTION_ADD_PRIMARY_KEY,
        ] {
            let mut unique_job = job(action);
            unique_job.schema_name = "test".into();
            let mut unique = JobSpec {
                job: unique_job,
                args: modify_index(true),
                id_allocated: true,
            };
            let error = prepare_spec_for_submit(&mut unique, 1, b"primary", false).unwrap_err();
            assert_eq!(error.code(), 8263);

            let mut ordinary_job = job(action);
            ordinary_job.schema_name = "test".into();
            let mut ordinary = JobSpec {
                job: ordinary_job,
                args: modify_index(false),
                id_allocated: true,
            };
            if action == ActionType::ACTION_ADD_INDEX {
                prepare_spec_for_submit(&mut ordinary, 1, b"primary", false).unwrap();
                assert_eq!(ordinary.job.state, JobState::QUEUEING);
            } else {
                let error =
                    prepare_spec_for_submit(&mut ordinary, 1, b"primary", false).unwrap_err();
                assert_eq!(error.code(), 8263);
            }
        }

        let mut unique_subjob = SubJob::default();
        unique_subjob.type_ = ActionType::ACTION_ADD_INDEX;
        unique_subjob.job_args = GoAny::new(modify_index(true));
        let mut parent = job(ActionType::ACTION_MULTI_SCHEMA_CHANGE);
        parent.schema_name = "test".into();
        parent.multi_schema_info = Some(GoShared::new(MultiSchemaInfo {
            sub_jobs: GoSharedPointerSlice::from_handles(vec![Some(GoShared::new(unique_subjob))]),
            ..Default::default()
        }));
        let mut parent = spec(parent);
        let error = prepare_spec_for_submit(&mut parent, 1, b"primary", false).unwrap_err();
        assert_eq!(error.code(), 8263);
    }

    #[test]
    fn global_id_count_and_assignment_match_go_create_actions() {
        let mut table = create_table_spec(2, false);
        assert_eq!(required_global_id_count(std::slice::from_ref(&table)), 4);
        assign_global_ids(std::slice::from_mut(&mut table), &[11, 12, 13, 14]);
        let args = create_table_args(&table);
        let table_info = args.read().table_info.get().unwrap();
        assert_eq!(table_info.read().id, 11);
        assert_eq!(
            table_info
                .read()
                .partition
                .as_ref()
                .unwrap()
                .read()
                .definitions
                .get(0)
                .id,
            12
        );
        assert_eq!(
            table_info
                .read()
                .partition
                .as_ref()
                .unwrap()
                .read()
                .definitions
                .get(1)
                .id,
            13
        );
        assert_eq!(table.job.table_id, 11);
        assert_eq!(table.job.id, 14);

        let mut allocated = create_table_spec(2, true);
        assert_eq!(
            required_global_id_count(std::slice::from_ref(&allocated)),
            1
        );
        assign_global_ids(std::slice::from_mut(&mut allocated), &[15]);
        assert_eq!(allocated.job.table_id, 501);
        assert_eq!(allocated.job.id, 15);

        let first = create_table_args(&create_table_spec(1, false));
        let second = create_table_args(&create_table_spec(0, false));
        let mut batch = JobSpec {
            job: job(ActionType::ACTION_CREATE_TABLES),
            args: BatchCreateTableArgs::into_job_args_value(Some(GoShared::new(
                BatchCreateTableArgs {
                    tables: GoField::new(GoSharedPointerSlice::from_handles(vec![
                        Some(first),
                        Some(second),
                    ])),
                },
            ))),
            id_allocated: false,
        };
        assert_eq!(required_global_id_count(std::slice::from_ref(&batch)), 4);
        assign_global_ids(std::slice::from_mut(&mut batch), &[21, 22, 23, 24]);
        let tables = batch_create_table_args(&batch).read().tables.get();
        assert_eq!(
            tables
                .get(0)
                .unwrap()
                .read()
                .table_info
                .get()
                .unwrap()
                .read()
                .id,
            21
        );
        assert_eq!(
            tables
                .get(0)
                .unwrap()
                .read()
                .table_info
                .get()
                .unwrap()
                .read()
                .partition
                .as_ref()
                .unwrap()
                .read()
                .definitions
                .get(0)
                .id,
            22
        );
        assert_eq!(
            tables
                .get(1)
                .unwrap()
                .read()
                .table_info
                .get()
                .unwrap()
                .read()
                .id,
            23
        );
        assert_eq!(batch.job.id, 24);
    }

    #[test]
    fn global_id_count_and_assignment_match_go_schema_resource_and_partition_actions() {
        let database = GoShared::new(tidb_model::DBInfo::default());
        let mut schema = JobSpec {
            job: job(ActionType::ACTION_CREATE_SCHEMA),
            args: tidb_model::CreateSchemaArgs::into_job_args_value(Some(GoShared::new(
                tidb_model::CreateSchemaArgs {
                    db_info: GoField::new(Some(database.clone())),
                },
            ))),
            id_allocated: false,
        };
        assign_global_ids(std::slice::from_mut(&mut schema), &[31, 32]);
        assert_eq!(database.read().id, 31);
        assert_eq!(schema.job.schema_id, 31);
        assert_eq!(schema.job.id, 32);

        let resource = GoShared::new(ResourceGroupInfo::default());
        let mut group = JobSpec {
            job: job(ActionType::ACTION_CREATE_RESOURCE_GROUP),
            args: ResourceGroupArgs::into_job_args_value(Some(GoShared::new(ResourceGroupArgs {
                resource_group_info: GoField::new(Some(resource.clone())),
            }))),
            id_allocated: false,
        };
        assign_global_ids(std::slice::from_mut(&mut group), &[33, 34]);
        assert_eq!(resource.read().id, 33);
        assert_eq!(group.job.id, 34);

        let mut alter = partition_spec(ActionType::ACTION_ALTER_TABLE_PARTITIONING, 2, false);
        assign_global_ids(std::slice::from_mut(&mut alter), &[41, 42, 43, 44]);
        let partition = table_partition_args(&alter).read().part_info.get().unwrap();
        assert_eq!(partition.read().definitions.get(0).id, 41);
        assert_eq!(partition.read().definitions.get(1).id, 42);
        assert_eq!(partition.read().new_table_id, 43);
        assert_eq!(alter.job.id, 44);

        for action in [
            ActionType::ACTION_ADD_TABLE_PARTITION,
            ActionType::ACTION_REORGANIZE_PARTITION,
        ] {
            let mut spec = partition_spec(action, 2, false);
            assign_global_ids(std::slice::from_mut(&mut spec), &[51, 52, 53]);
            let partition = table_partition_args(&spec).read().part_info.get().unwrap();
            assert_eq!(partition.read().definitions.get(0).id, 51);
            assert_eq!(partition.read().definitions.get(1).id, 52);
            assert_eq!(spec.job.id, 53);
        }

        let mut remove = partition_spec(ActionType::ACTION_REMOVE_PARTITIONING, 1, false);
        assign_global_ids(std::slice::from_mut(&mut remove), &[61, 62]);
        let partition = table_partition_args(&remove)
            .read()
            .part_info
            .get()
            .unwrap();
        assert_eq!(partition.read().definitions.get(0).id, 61);
        assert_eq!(partition.read().new_table_id, 61);
        assert_eq!(remove.job.id, 62);
    }

    #[test]
    fn global_id_count_and_assignment_match_go_truncate_actions() {
        let mut table = truncate_spec(ActionType::ACTION_TRUNCATE_TABLE, 2, false);
        assert_eq!(required_global_id_count(std::slice::from_ref(&table)), 4);
        assign_global_ids(std::slice::from_mut(&mut table), &[71, 72, 73, 74]);
        let args = truncate_table_args(&table);
        assert_eq!(args.read().new_table_id.get(), 71);
        assert_eq!(args.read().new_partition_ids.get().snapshot(), vec![72, 73]);
        assert_eq!(table.job.id, 74);

        let mut partitions = truncate_spec(ActionType::ACTION_TRUNCATE_TABLE_PARTITION, 2, false);
        assert_eq!(
            required_global_id_count(std::slice::from_ref(&partitions)),
            3
        );
        assign_global_ids(std::slice::from_mut(&mut partitions), &[81, 82, 83]);
        let args = truncate_table_args(&partitions);
        assert_eq!(args.read().new_table_id.get(), 0);
        assert_eq!(args.read().new_partition_ids.get().snapshot(), vec![81, 82]);
        assert_eq!(partitions.job.id, 83);
    }

    #[test]
    fn scheduling_row_id_strings_match_go() {
        let infos = GoSharedPointerSlice::from_handles(vec![
            Some(GoShared::new(RenameTableArgs {
                old_schema_id: 2,
                new_schema_id: 10,
                table_id: 20,
                ..Default::default()
            })),
            Some(GoShared::new(RenameTableArgs {
                old_schema_id: 3,
                new_schema_id: 2,
                table_id: 100,
                ..Default::default()
            })),
        ]);
        let rename_tables = JobSpec {
            job: job(ActionType::ACTION_RENAME_TABLES),
            args: RenameTablesArgs::into_job_args_value(Some(GoShared::new(RenameTablesArgs {
                rename_table_infos: GoField::new(infos),
            }))),
            id_allocated: true,
        };
        // Go converts to decimal strings before sorting, so this is lexical.
        assert_eq!(job_schema_ids(&rename_tables), "10,2,3");
        assert_eq!(job_table_ids(&rename_tables), "100,20");

        let mut rename_job = job(ActionType::ACTION_RENAME_TABLE);
        rename_job.schema_id = 10;
        let rename = JobSpec {
            job: rename_job,
            args: RenameTableArgs::into_job_args_value(Some(GoShared::new(RenameTableArgs {
                old_schema_id: 2,
                ..Default::default()
            }))),
            id_allocated: true,
        };
        assert_eq!(job_schema_ids(&rename), "10,2");

        let mut exchange_job = job(ActionType::ACTION_EXCHANGE_TABLE_PARTITION);
        exchange_job.schema_id = 2;
        exchange_job.table_id = 20;
        let exchange = JobSpec {
            job: exchange_job,
            args: tidb_model::ExchangeTablePartitionArgs::into_job_args_value(Some(GoShared::new(
                tidb_model::ExchangeTablePartitionArgs {
                    partitioned_table_schema_id: GoField::new(10),
                    partitioned_table_id: GoField::new(100),
                    ..Default::default()
                },
            ))),
            id_allocated: true,
        };
        assert_eq!(job_schema_ids(&exchange), "10,2");
        assert_eq!(job_table_ids(&exchange), "100,20");

        let mut truncate = truncate_spec(ActionType::ACTION_TRUNCATE_TABLE, 0, true);
        truncate.job.table_id = 7;
        truncate_table_args(&truncate).read().new_table_id.set(9);
        assert_eq!(job_table_ids(&truncate), "7,9");
    }

    /// Go `job2TableIDs` (master `94a9cbedab`) for the materialized-view
    /// creates: the view reports the view id plus every log id; the log
    /// reports its id plus the recorded base table id.
    #[test]
    fn job_table_ids_cover_materialized_view_creates() {
        let mut view_job = job(ActionType::ACTION_CREATE_MATERIALIZED_VIEW);
        view_job.table_id = 50;
        let mut view_table = tidb_model::TableInfo::default();
        view_table.id = 50;
        let view = JobSpec {
            job: view_job,
            args: tidb_model::CreateMaterializedViewArgs::into_job_args_value(Some(GoShared::new(
                tidb_model::CreateMaterializedViewArgs {
                    table_info: GoField::new(Some(GoShared::new(view_table))),
                    mlog_table_ids: GoField::new(GoSharedSlice::from_vec(vec![99, 100])),
                },
            ))),
            id_allocated: true,
        };
        // Go `makeStringForIDs` dedupes into a set and sorts the decimal
        // strings lexicographically: "100" < "50" < "99".
        assert_eq!(job_table_ids(&view), "100,50,99");

        // No log ids: only the view's own id.
        let mut bare_view_table = tidb_model::TableInfo::default();
        bare_view_table.id = 50;
        let bare_view = JobSpec {
            job: {
                let mut job = job(ActionType::ACTION_CREATE_MATERIALIZED_VIEW);
                job.table_id = 50;
                job
            },
            args: tidb_model::CreateMaterializedViewArgs::into_job_args_value(Some(GoShared::new(
                tidb_model::CreateMaterializedViewArgs {
                    table_info: GoField::new(Some(GoShared::new(bare_view_table))),
                    mlog_table_ids: GoField::new(GoSharedSlice::default()),
                },
            ))),
            id_allocated: true,
        };
        assert_eq!(job_table_ids(&bare_view), "50");

        let mut log_job = job(ActionType::ACTION_CREATE_MATERIALIZED_VIEW_LOG);
        log_job.table_id = 51;
        let mut log_table = tidb_model::TableInfo::default();
        log_table.id = 51;
        let mut log_meta = tidb_model::MaterializedViewLogInfo::default();
        log_meta.base_table_id = 88;
        log_table.materialized_view_log = Some(GoShared::new(log_meta));
        let log = JobSpec {
            job: log_job,
            args: tidb_model::CreateMaterializedViewLogArgs::into_job_args_value(Some(
                GoShared::new(tidb_model::CreateMaterializedViewLogArgs {
                    table_info: GoField::new(Some(GoShared::new(log_table))),
                }),
            )),
            id_allocated: true,
        };
        assert_eq!(job_table_ids(&log), "51,88");

        // A log without recorded base metadata reports only its own id.
        let mut bare_log_table = tidb_model::TableInfo::default();
        bare_log_table.id = 51;
        let bare_log = JobSpec {
            job: {
                let mut job = job(ActionType::ACTION_CREATE_MATERIALIZED_VIEW_LOG);
                job.table_id = 51;
                job
            },
            args: tidb_model::CreateMaterializedViewLogArgs::into_job_args_value(Some(
                GoShared::new(tidb_model::CreateMaterializedViewLogArgs {
                    table_info: GoField::new(Some(GoShared::new(bare_log_table))),
                }),
            )),
            id_allocated: true,
        };
        assert_eq!(job_table_ids(&bare_log), "51");
    }
}
