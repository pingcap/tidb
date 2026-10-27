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

//! ADD INDEX / ADD PRIMARY KEY as Go runs them: a persisted job
//! (`executor.createIndex`), stepped by the owner through
//! `onCreateIndex`'s schema states with a transactional reorganization in
//! `StateWriteReorganization`, and rolled back through `onDropIndex`'s states
//! when the reorganization finds a duplicate (`convertAddIdxJob2RollbackJob`).
//!
//! The reorganization itself is not planned here: a step that needs one
//! answers an [`AddIndexReorg`] request, the owner runs it in its own
//! transactions, and the next plan reads its [`ReorgOutcome`] -- Go's
//! `reorgCtx.doneCh` -- from the job's in-memory schema state.

use super::*;

/// Go `meta.IndexElementKey`.
pub const INDEX_ELEMENT_KEY: &[u8] = b"_idx_";

/// Go `meta.ColumnElementKey`.
pub const COLUMN_ELEMENT_KEY: &[u8] = b"_col_";

/// Go `ingest.LitInitialized`: this node has no Lightning ingest backend,
/// so a fast reorganization falls back to the transactional merge.
const LIGHTNING_INITIALIZED: bool = false;

/// Go `tablecodec.TempIndexPrefix`.
pub const TEMP_INDEX_PREFIX: i64 = 0x7fff_0000_0000_0000;

/// What the owner must reorganize before an ADD INDEX job moves on.
#[derive(Clone, Debug)]
pub struct AddIndexReorg {
    /// The job.
    pub job_id: i64,
    /// What to do.
    pub stage: ReorgStageRequest,
    /// The table as the job sees it, the indexes being added among its own.
    pub table: Box<TableInfo>,
    /// The indexes being added (Go's reorg elements).
    pub index_ids: Vec<i64>,
    /// Whether this is the temporary-index merge (`mergingTmpIdx`).
    pub merging: bool,
    /// Every physical table in reorganization order.
    pub physical_ids: Vec<i64>,
    /// Go `ReorgMeta.GetBatchSize`.
    pub batch_size: usize,
    /// Go `ReorgMeta`'s SQL mode and time zone, for decoding rows and
    /// evaluating index expressions.
    pub session_vars: ReorgSessionVars,
    /// The job's row count, where the reorganization's count starts.
    pub row_count: i64,
    /// For a MODIFY COLUMN's row-rewrite stage, the column it rewrites (the
    /// reorganization's element is then the changing column, not an index).
    pub column: Option<ColumnReorgSpec>,
    /// The multi-schema sub-job that asked for it (its proxy's
    /// `MultiSchemaInfo.Seq`), `None` for a job of its own.
    pub sub_job: Option<i32>,
    /// Go `typeCleanUpIndexWorker`: the rows of the dropped partitions in
    /// `physical_ids` have their entries in the global indexes `index_ids`
    /// deleted, instead of an index being added.
    pub clean_up_global_index: bool,
    /// The phase of a partition reorganization this runs (Go
    /// `reorgPartitionDataAndIndex`), `None` for any other job.
    pub partition_phase: Option<PartitionReorgPhase>,
    /// The table's database, whose auto-ID counters a partition copy may
    /// draw a fresh `_tidb_rowid` from.
    pub schema_id: i64,
}

/// Go `reorgPartitionDataAndIndex`'s phases, in order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PartitionReorgPhase {
    /// The dropping partitions' rows copied into the adding ones (the
    /// reorganization's column element).
    CopyRows,
    /// Every index built on the adding partitions.
    AddingIndexes,
    /// The new global indexes filled from the partitions the job leaves
    /// untouched.
    UntouchedGlobalIndexes,
}

/// What a MODIFY COLUMN's row rewrite reorganizes: Go's column element.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnReorgSpec {
    /// The column being changed.
    pub old_column_id: i64,
    /// The temporary column the rows get, the reorganization's element.
    pub changing_column_id: i64,
}

/// A data check a step asks the owner to run outside the job's
/// transaction, as Go's owner runs `checkModifyColumnData` through its
/// session (Go `ExecRestrictedSQL`).
#[derive(Clone, Debug)]
pub struct ColumnDataCheck {
    /// The database's name.
    pub schema: String,
    /// The table as the job sees it.
    pub table: Box<TableInfo>,
    /// The column whose rows are checked.
    pub old_column_id: i64,
    /// The type the rows must fit.
    pub new_column: Box<tidb_model::ColumnInfo>,
    /// Go `checkValueRange`: whether the values must fit the new type, or
    /// only be non-NULL.
    pub check_value_range: bool,
}

/// The phase of the reorganization the owner runs.
#[derive(Clone, Debug)]
pub enum ReorgStageRequest {
    /// Go `getReorgInfo` with `SnapshotVer == 0`: take a fresh version,
    /// find the first physical table's range (or check the work can be
    /// skipped) and persist the reorg handle.
    Init,
    /// Go `runReorgJob`: resume from the persisted handle.
    Run(ReorgHandle),
    /// Go `checkModifyColumnData`.
    Check(ColumnDataCheck),
    /// Go `checkForeignKeyConstrain`: the child's existing rows against the
    /// parent, answered as a check.
    CheckForeignKey(Box<super::ForeignKeyCheck>),
    /// Go `checkExchangePartitionRecordValidation`: the standalone table's
    /// rows against the exchanged partition, answered as a check.
    CheckExchangePartition(Box<super::ExchangePartitionValidation>),
}

/// One `mysql.tidb_ddl_reorg` row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReorgHandle {
    /// `ele_id`.
    pub element_id: i64,
    /// `ele_type`.
    pub element_type: Vec<u8>,
    /// `start_key`.
    pub start_key: Vec<u8>,
    /// `end_key`.
    pub end_key: Vec<u8>,
    /// `physical_id`.
    pub physical_id: i64,
}

/// What the owner's reorganization came to.
#[derive(Clone, Debug)]
pub struct ReorgOutcome {
    /// The job it ran for.
    pub job_id: i64,
    /// Whether it was the temporary-index merge.
    pub merging: bool,
    /// The column it rewrote, for a MODIFY COLUMN's row rewrite.
    pub column: Option<ColumnReorgSpec>,
    /// The multi-schema sub-job it ran for (see [`AddIndexReorg::sub_job`]).
    pub sub_job: Option<i32>,
    /// The partition reorganization phase it ran (see
    /// [`AddIndexReorg::partition_phase`]).
    pub partition_phase: Option<PartitionReorgPhase>,
    /// The job's row count the request started from, which a phase that
    /// only initialized or skipped carries on to the next.
    pub row_count: i64,
    /// A row rewrite's warnings (Go `backfillTaskContext.warnings`): each
    /// code's first warning and its count.
    pub warnings: Vec<(u16, String, i64)>,
    /// The result.
    pub result: ReorgResult,
}

/// See [`ReorgOutcome`].
#[derive(Clone, Debug)]
pub enum ReorgResult {
    /// The handle was persisted at this snapshot version.
    Initialized(u64),
    /// The table (or the temporary index) holds nothing: Go's
    /// `checkIfTableReorgWorkCanSkip`.
    Skipped,
    /// The reorganization finished; the job's row count is now this (Go's
    /// `reorgCtx` count, which starts from the job's).
    Done(i64),
    /// Go `ErrWaitReorgTimeout`: still running after the owner's wait; the
    /// job records this row count and resumes from the handle.
    Progress(i64),
    /// It failed with this error, having counted this many rows.
    Failed {
        /// The error code.
        code: u16,
        /// The error message.
        message: String,
        /// The job's row count when it failed.
        rows: i64,
    },
    /// It stopped between batches because the job is no longer runnable
    /// (Go `isReorgRunnable`: cancelling or pausing); the handle holds where
    /// it got to.
    Interrupted,
    /// A column check ran: `None` when every row fits, else the error Go's
    /// `checkModifyColumnData` answers (`checked == true`).
    Checked(Option<(u16, String)>),
}

/// The ADD INDEX submission's answer.
pub enum AddIndexSubmission {
    /// The job to queue, and the warnings Go's executor appended to the
    /// statement before submitting it.
    Job(
        Box<crate::ddl_job_submit::JobSpec>,
        Vec<tidb_util::context::SqlWarn>,
    ),
    /// `IF NOT EXISTS` over an existing index: a Note, no job.
    AlreadySatisfied {
        /// What was already true.
        detail: String,
        /// The Note.
        warnings: Vec<tidb_util::context::SqlWarn>,
    },
}

/// Go `executor.createIndex` up to `doDDLJob2`: the admission checks, the
/// job's arguments (`ModifyIndexArgs` with the hidden columns built and every
/// key part naming a column) and its reorganization settings
/// (`initJobReorgMetaFromVariables`). `None` when the statement is not one
/// ADD INDEX. `store_count` is PD's store count, for Go's automatic
/// `max_node_count`.
pub fn prepare_add_index_job_submission<S: MetaSnapshot>(
    snapshot: &mut S,
    statement: &DdlStatement,
    start_ts: u64,
    upgrading: bool,
    min_job_id: i64,
    store_count: usize,
    has_null: &mut dyn FnMut(&TableInfo, i64) -> Result<bool, DdlPlanError>,
    job_context: &dyn DdlSessionContext,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    let catalog = load_cluster_catalog(snapshot)?;
    let mut built = build_add_index_job(snapshot, &catalog, statement, store_count, job_context)?;
    // Go `Job.Query` is the submitting statement's text, which the job
    // context carries (the lowered statement's context predates it).
    if let Some(AddIndexSubmission::Job(spec, _)) = built.as_mut() {
        if spec.job.query.is_empty() {
            spec.job.query = job_context.ddl_query().into();
        }
    }
    if matches!(built, Some(AddIndexSubmission::Job(..))) {
        check_added_primary_key_holds_no_null(&catalog, statement, has_null)?;
    }
    submit_built_job(snapshot, &catalog, built, start_ts, upgrading, min_job_id)
}

/// Go `checkPrimaryKeyNotNull` -> `modifyColsFromNull2NotNull` ->
/// `checkForNullValue`: a primary key over a nullable column that holds a
/// NULL is refused with 1138. Go's owner asks during the job; this node asks
/// in the submitting transaction, as it does for MODIFY COLUMN.
pub(super) fn check_added_primary_key_holds_no_null(
    catalog: &ClusterCatalog,
    statement: &DdlStatement,
    has_null: &mut dyn FnMut(&TableInfo, i64) -> Result<bool, DdlPlanError>,
) -> Result<(), DdlPlanError> {
    let DdlStatement::CreateIndex {
        schema,
        table,
        index,
        ..
    } = statement
    else {
        return Ok(());
    };
    if !index.primary {
        return Ok(());
    }
    let (_, stored) = locate_table(catalog, schema, table)?;
    for part in index.columns.iter_deref() {
        let name = part.read().name.lowercase().to_owned();
        let Some(column) = stored
            .columns
            .iter_deref()
            .find(|column| column.read().name.lowercase() == name)
        else {
            continue;
        };
        let (id, nullable) = {
            let column = column.read();
            (
                column.id,
                !column
                    .field_type
                    .has_flag(tidb_datatype::FieldTypeFlags::NOT_NULL),
            )
        };
        if nullable && has_null(stored, id)? {
            return Err(coded_refusal(
                &tidb_util::dbterror::ERR_INVALID_USE_OF_NULL,
                "Invalid use of NULL value",
            ));
        }
    }
    Ok(())
}

/// The executor half without `doDDLJob2`: the job unsubmitted, which a
/// multi-schema change appends as a sub-job (Go `appendToSubJobs`).
pub(super) fn build_add_index_job<S: MetaSnapshot>(
    _snapshot: &mut S,
    catalog: &ClusterCatalog,
    statement: &DdlStatement,
    store_count: usize,
    job_context: &dyn DdlSessionContext,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    let DdlStatement::CreateIndex {
        schema,
        table,
        if_not_exists,
        index,
        expressions,
        condition,
        context,
    } = statement
    else {
        return Ok(None);
    };
    let (schema_id, stored) = locate_table(&catalog, schema, table)?;
    if index.primary && (find_index(stored, "PRIMARY").is_some() || stored.pk_is_handle) {
        return Err(DdlPlanError::Admission(
            tidb_ddl_build::table_info_build::DdlAdmissionError::with_proto(
                &tidb_error::catalog::infoschema::ERR_MULTIPLE_PRI_KEY,
                "Multiple primary key defined",
            ),
        ));
    }
    if index.primary && expressions.iter().any(Option::is_some) {
        return Err(coded_refusal(
            &tidb_util::dbterror::ERR_FUNCTIONAL_INDEX_PRIMARY_KEY,
            "The primary key cannot be an expression index",
        ));
    }
    let name = settle_added_index_name(stored, index, expressions);
    if let Some(existing) = find_index(stored, &name) {
        let existing = existing.read();
        if *if_not_exists {
            return Ok(Some(AddIndexSubmission::AlreadySatisfied {
                detail: format!(
                    "index `{}` already exists on `{schema}`.`{table}`",
                    existing.name.original()
                ),
                warnings: vec![crate::real_tikv_ddl::ddl_note(
                    &tidb_util::dbterror::ERR_DUP_KEY_NAME,
                    format!("Duplicate key name '{name}'"),
                )],
            }));
        }
        return Err(DdlPlanError::from(
            tidb_util::dbterror::ERR_DUP_KEY_NAME.generate_with_stack_by_args(&[name.into()]),
        ));
    }
    let hypo = index.tp == tidb_ast::IndexType::HYPO && !index.primary;
    // Go builds a hypothetical expression index over `finalColumns`, a COPY
    // of the table's columns with the hidden ones appended, and keeps only
    // the `IndexInfo`, whose parts then name columns the table does not
    // hold; the planner here resolves a key part against the table.
    if hypo && expressions.iter().any(Option::is_some) {
        return Err(DdlPlanError::Admission(
            tidb_ddl_build::table_info_build::DdlAdmissionError::unsupported(
                "a hypothetical expression index is not supported",
            ),
        ));
    }
    let mut candidate = stored.clone_like_go();
    let added = tidb_ddl_build::index::build_index_info(
        &mut candidate,
        index,
        expressions,
        condition.as_ref(),
        &name,
        &context.0,
    )
    .map_err(DdlPlanError::Admission)?;
    // Go `createIndex`'s hypo branch: `BuildIndexInfo` and
    // `addHypoIndexIntoCtx`, no job.
    if hypo {
        job_context
            .add_hypo_index(&schema.go_to_lower(), stored.name.lowercase(), added)
            .map_err(DdlPlanError::from_mysql_error)?;
        return Ok(Some(AddIndexSubmission::AlreadySatisfied {
            detail: format!("hypothetical index `{name}` added to the session"),
            warnings: Vec::new(),
        }));
    }
    warn_missing_region_split_policy(stored, &name, &context.0);

    let reorg_meta = new_add_index_reorg_meta(&context.0, schema, store_count);
    // Go `createIndex`: a partial index needs a fast reorganization.
    if !added.condition_expr_string.is_empty() && !reorg_meta.is_fast_reorg {
        return Err(unsupported_partial_index());
    }
    // The hidden columns the job carries are the executor's: no id, no
    // offset, state none (Go `buildHiddenColumnInfoWithCheck`); the owner's
    // `checkAndBuildIndexInfo` adds them to the table.
    let hidden_cols: Vec<tidb_model::ColumnInfo> = candidate
        .columns
        .iter_deref()
        .skip(stored.columns.len())
        .map(|column| {
            let mut column = column.read().clone_like_go();
            column.id = 0;
            column.offset = 0;
            column.state = SchemaState::NONE;
            column
        })
        .collect();
    // Go's key parts name the hidden column of an expression part.
    let parts: Vec<tidb_ast::IndexPartSpecification> = added
        .columns
        .iter_deref()
        .zip(index.columns.iter_deref())
        .map(|(planned, written)| {
            let planned = planned.read();
            let written = written.read();
            tidb_ast::IndexPartSpecification {
                column: Some(Box::new(tidb_ast::ColumnName {
                    schema: CiString::default(),
                    table: CiString::default(),
                    name: planned.name.clone(),
                })),
                length: if written.length > 0 {
                    written.length
                } else {
                    -1
                },
                desc: false,
                expr: None,
            }
        })
        .collect();
    let option = tidb_ast::IndexOption {
        tp: index.tp.0,
        // Go `validateCommentLength` truncated the option's comment in place.
        comment: added.comment.clone(),
        visibility: if index.invisible { 2 } else { 0 },
        global: index.global,
        ..tidb_ast::IndexOption::default()
    };
    let arg = tidb_model::IndexArg {
        unique: index.unique,
        index_name: CiString::new(name.clone()),
        index_part_specifications: parts.into(),
        index_option: Some(GoShared::new(option)),
        hidden_cols: hidden_cols.into(),
        is_pk: index.primary,
        sql_mode: context.0.ddl_sql_mode() as u64,
        condition_string: added.condition_expr_string.clone(),
        ..tidb_model::IndexArg::default()
    };
    let args = GoShared::new(tidb_model::ModifyIndexArgs {
        index_args: vec![arg].into(),
        op_type: tidb_model::IndexOp::ADD_INDEX,
        ..tidb_model::ModifyIndexArgs::default()
    });

    // Go `buildAddIndexJobWithoutTypeAndArgs`.
    let mut job = Job::default();
    job.version = get_job_ver_in_use();
    job.schema_id = schema_id;
    job.table_id = stored.id;
    job.schema_name = schema.go_to_lower().into();
    job.table_name = stored.name.lowercase().to_owned().into();
    job.type_ = if index.primary {
        ActionType::ACTION_ADD_PRIMARY_KEY
    } else {
        ActionType::ACTION_ADD_INDEX
    };
    job.binlog_info = Some(GoShared::new(HistoryInfo::default()));
    job.priority = context.0.ddl_reorg_priority();
    let (charset, collate) = context.0.connection_charset_info();
    job.charset = charset.to_owned().into();
    job.collate = collate.to_owned().into();
    job.sql_mode = context.0.ddl_sql_mode();
    job.query = context.0.ddl_query().into();
    job.cdc_write_source = context.0.ddl_cdc_write_source();
    job.trace_info = Some(GoShared::new(TraceInfo {
        session_alias: context.0.ddl_session_alias().into(),
        trace_id: context.0.ddl_trace_id().to_vec().into(),
        connection_id: context.0.ddl_connection_id(),
    }));
    let mut session_vars = std::collections::BTreeMap::new();
    let vars = context.0.ddl_reorg_vars();
    session_vars.insert(
        tidb_vardef::tidb_vars::TIDB_ENABLE_DDL_ANALYZE.to_owned(),
        vars.enable_ddl_analyze.clone().into(),
    );
    session_vars.insert(
        tidb_vardef::tidb_vars::TIDB_ANALYZE_VERSION.to_owned(),
        vars.analyze_version.clone().into(),
    );
    job.session_vars = Some(GoShared::new(session_vars));
    job.reorg_meta = Some(GoShared::new(reorg_meta));

    Ok(Some(AddIndexSubmission::Job(
        Box::new(crate::ddl_job_submit::JobSpec {
            job,
            args: JobArgsValue::ModifyIndex(Some(args)),
            id_allocated: true,
        }),
        Vec::new(),
    )))
}

pub(super) fn unsupported_partial_index() -> DdlPlanError {
    coded_refusal(
        &tidb_util::dbterror::ERR_UNSUPPORTED_ADD_PARTIAL_INDEX,
        "Unsupported add partial index: add partial index without fast reorg is not supported",
    )
}

/// Go `NewDDLReorgMeta` + `initJobReorgMetaFromVariables` for ADD INDEX on
/// the classic kernel.
pub(super) fn new_add_index_reorg_meta(
    context: &dyn DdlSessionContext,
    schema: &str,
    store_count: usize,
) -> tidb_model::DDLReorgMeta {
    init_job_reorg_meta_from_variables(context, schema, store_count, true, true)
}

/// Go `initJobReorgMetaFromVariables`: the session's reorganization
/// settings when the job may reorganize (`setReorgParam`), and its
/// distributed-task settings when it may run as one (`setDistTaskParam`).
pub(super) fn init_job_reorg_meta_from_variables(
    context: &dyn DdlSessionContext,
    schema: &str,
    store_count: usize,
    set_reorg_param: bool,
    set_dist_task_param: bool,
) -> tidb_model::DDLReorgMeta {
    let vars = context.ddl_reorg_vars();
    let mut meta = tidb_model::DDLReorgMeta::new(
        context.ddl_sql_mode() as u64,
        tidb_model::TimeZoneLocation::new(vars.time_zone_name.clone(), vars.time_zone_offset),
        vars.resource_group_name.clone(),
    );
    if set_reorg_param {
        meta.set_concurrency(vars.worker_count);
        meta.set_batch_size(vars.batch_size);
        meta.set_max_write_speed(vars.max_write_speed);
    }
    if set_dist_task_param {
        meta.is_dist_reorg = vars.enable_dist_task;
        meta.is_fast_reorg = vars.enable_fast_reorg;
        meta.target_scope = vars.target_scope.clone().into();
        meta.max_node_count = if vars.max_dist_task_nodes == -1 {
            // Go `scheduler.CalcMaxNodeCountByStoresNum`.
            (store_count / 3).max(3) as i64
        } else {
            vars.max_dist_task_nodes
        };
        if tidb_metadef::is_system_related_db(&schema.go_to_lower()) {
            meta.is_dist_reorg = false;
            meta.is_fast_reorg = false;
        }
    }
    meta
}

/// The multi-schema sub-job a reorganization request comes from: its proxy
/// job's sequence.
pub(super) fn reorg_sub_job(job: &Job) -> Option<i32> {
    job.multi_schema_info.as_ref().map(|info| info.read().seq)
}

pub use tidb_ddl_build::session_pool::ReorgSessionVars;


pub(super) fn job_batch_size(job: &Job) -> usize {
    let defaults = tidb_model::DDLReorgProcessDefaults::new(
        || tidb_vardef::defaults::DEF_TIDB_DDL_REORG_WORKER_COUNT,
        || tidb_vardef::defaults::DEF_TIDB_DDL_REORG_BATCH_SIZE,
    );
    job.reorg_meta
        .as_ref()
        .map_or(
            tidb_vardef::defaults::DEF_TIDB_DDL_REORG_BATCH_SIZE,
            |meta| meta.read().get_batch_size(defaults),
        )
        .max(1) as usize
}

/// Go `getPartitionIDs`.
pub(super) fn partition_ids(table: &TableInfo) -> Vec<i64> {
    table
        .get_partition_info()
        .map(|info| {
            info.read()
                .definitions
                .snapshot()
                .iter()
                .map(|definition| definition.id)
                .collect()
        })
        .unwrap_or_default()
}

/// The physical tables a non-global index's entries live under.
pub(super) fn physical_ids(table: &TableInfo) -> Vec<i64> {
    let partitions = partition_ids(table);
    if partitions.is_empty() {
        vec![table.id]
    } else {
        partitions
    }
}

/// Go `pickBackfillType`.
pub(super) fn pick_backfill_type(job: &Job) -> tidb_model::ReorgType {
    let Some(meta) = job.reorg_meta.as_ref() else {
        return tidb_model::ReorgType::TXN;
    };
    let mut meta = meta.write();
    if meta.reorg_type != tidb_model::ReorgType::NONE {
        return meta.reorg_type;
    }
    meta.reorg_type = if !meta.is_fast_reorg {
        tidb_model::ReorgType::TXN
    } else if LIGHTNING_INITIALIZED {
        tidb_model::ReorgType::INGEST
    } else {
        tidb_model::ReorgType::TXN_MERGE
    };
    meta.reorg_type
}

/// Go `updateVersionAndTableInfo`: the table write and, when the state moved,
/// the version bump with its diff.
pub(super) fn table_step(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    database_id: i64,
    info: &TableInfo,
    should_update_version: bool,
    start_ts: u64,
) -> Result<DdlWrite, DdlPlanError> {
    table_step_with_diff(
        catalog,
        active,
        database_id,
        info,
        should_update_version,
        start_ts,
        |_| {},
    )
}

/// [`table_step`] whose diff `fill` completes, as Go's `updateSchemaVersion`
/// does per action (the affected physical tables of a partition job).
pub(super) fn table_step_with_diff(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    database_id: i64,
    info: &TableInfo,
    should_update_version: bool,
    start_ts: u64,
    fill: impl FnOnce(&mut SchemaDiff),
) -> Result<DdlWrite, DdlPlanError> {
    let mut info = info.clone_like_go();
    let mut mutations = Vec::new();
    info.update_ts = start_ts;
    mutations.push(BufferMutation::set(
        key::table_kv_key(database_id, info.id),
        value::serialize_table_info(&info)
            .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
    )?);
    // Go `updateVersionAndTableInfo`: one multi-schema step publishes one
    // version, the sub-jobs after the first skipping theirs.
    let should_update_version = should_update_version && !skips_version(&active.job);
    let (schema_version, diff) = if should_update_version {
        let schema_version = catalog.schema_version + 1;
        let mut diff = SchemaDiff {
            version: schema_version,
            action_type: active.job.type_,
            schema_id: database_id,
            table_id: info.id,
            ..SchemaDiff::default()
        };
        fill(&mut diff);
        mutations.push(BufferMutation::set(
            key::schema_version_kv_key(),
            value::encode_int_value(schema_version),
        )?);
        mutations.push(BufferMutation::set(
            key::schema_diff_kv_key(schema_version),
            value::serialize_schema_diff(&diff)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
        )?);
        active.job.last_schema_version = schema_version;
        (schema_version, diff)
    } else {
        (0, SchemaDiff::default())
    };
    let mut write = job_only_write(active.job.id, mutations);
    write.schema_version = schema_version;
    write.diff = diff;
    Ok(write)
}

pub(super) fn action_step(write: DdlWrite) -> PersistedDdlActionStep {
    PersistedDdlActionStep {
        write,
        update_raw_args: true,
        run_error: None,
        reorg: None,
    }
}

pub(super) fn reorg_step(job: &Job, reorg: AddIndexReorg) -> PersistedDdlActionStep {
    PersistedDdlActionStep {
        write: job_only_write(job.id, Vec::new()),
        update_raw_args: false,
        run_error: None,
        reorg: Some(reorg),
    }
}

/// Go `checkAndBuildIndexInfo`: the index the job adds, built against the
/// table from the job's arguments (and appended with its hidden columns) the
/// first time, found by name afterwards.
fn check_and_build_index_info(
    info: &mut TableInfo,
    arg: &tidb_model::IndexArg,
    is_pk: bool,
) -> Result<GoShared<IndexInfo>, DdlPlanError> {
    if let Some(existing) = find_index(info, arg.index_name.original()) {
        if existing.read().state == SchemaState::PUBLIC {
            if is_pk {
                return Err(DdlPlanError::Admission(
                    tidb_ddl_build::table_info_build::DdlAdmissionError::with_proto(
                        &tidb_error::catalog::infoschema::ERR_MULTIPLE_PRI_KEY,
                        "Multiple primary key defined",
                    ),
                ));
            }
            return Err(DdlPlanError::from(
                tidb_util::dbterror::ERR_DUP_KEY_NAME.generate_with_stack_by_args(&[(arg
                    .index_name
                    .original()
                    .to_owned())
                .into()]),
            ));
        }
        return Ok(existing);
    }
    for hidden in arg.hidden_cols.iter_deref() {
        let hidden = hidden.read();
        if info.columns.iter_deref().any(|column| {
            let column = column.read();
            column.state == SchemaState::PUBLIC
                && column.name.lowercase() == hidden.name.lowercase()
        }) {
            return Err(coded_refusal(
                &tidb_error::catalog::infoschema::ERR_COLUMN_EXISTS,
                format!("Duplicate column name '{}'", hidden.name.original()),
            ));
        }
    }
    // Go `InitAndAddColumnToTable`.
    for hidden in arg.hidden_cols.iter_deref() {
        let mut column = hidden.read().clone_like_go();
        info.max_column_id += 1;
        column.id = info.max_column_id;
        column.offset = info.columns.len() as i64;
        // Appended at the end in StateNone; `moveAndUpdateHiddenColumnsToPublic`
        // below publishes and places them once the index names their offsets.
        column.state = SchemaState::NONE;
        info.columns.push_go(column);
    }
    // Go `checkAddColumnTooManyColumns(len(tblInfo.Columns))`
    // (`pkg/ddl/index.go:852`), the hidden columns included.
    check_add_column_too_many_columns(info.columns.len())?;
    // Go `BuildIndexInfo` from the key parts, every one naming a column.
    let template = IndexInfo {
        name: arg.index_name.clone(),
        unique: arg.unique || is_pk,
        primary: is_pk,
        columns: arg
            .index_part_specifications
            .iter_deref()
            .map(|part| {
                let part = part.read();
                IndexColumn {
                    name: part
                        .column
                        .as_ref()
                        .map(|column| column.name.clone())
                        .unwrap_or_default(),
                    offset: 0,
                    length: if part.length > 0 { part.length } else { -1 },
                    ..IndexColumn::default()
                }
            })
            .collect::<Vec<_>>()
            .into(),
        comment: arg
            .index_option
            .as_ref()
            .map(|option| option.read().comment.clone())
            .unwrap_or_default(),
        tp: tidb_ast::IndexType(
            arg.index_option
                .as_ref()
                .map_or(0, |option| option.read().tp),
        ),
        invisible: arg
            .index_option
            .as_ref()
            .is_some_and(|option| option.read().visibility == 2),
        global: arg
            .index_option
            .as_ref()
            .is_some_and(|option| option.read().global),
        state: SchemaState::NONE,
        ..IndexInfo::default()
    };
    let condition = if arg.condition_string.is_empty() {
        None
    } else {
        Some(
            tidb_model::generated_expr::parse_expression(&arg.condition_string)
                .map_err(|error| DdlPlanError::Encode(error.message))?,
        )
    };
    let mut added = build_index_info(info, &template, condition.as_ref(), arg.index_name.original())?;
    // Go `checkTooManyIndexes(tblInfo.Indices)` once the new index is
    // appended, counted before the table is touched.
    tidb_ddl_build::table_info_build::check_too_many_indexes(info.indices.len() + 1).map_err(|error| {
        DdlPlanError::Admission(DdlAdmissionError::from_mysql_error(error.into()))
    })?;
    added.state = SchemaState::NONE;
    allocate_added_index(info, &mut added);
    let added = GoShared::new(added);
    info.indices.push_handle_go(Some(added.clone()));
    move_and_update_hidden_columns_to_public(info, &added);
    check_table_info_valid(info)?;
    Ok(added)
}

/// Go `moveAndUpdateHiddenColumnsToPublic`: the index's hidden columns become
/// public and move to the first non-public position, so `Columns` keeps the
/// `[public..., non-public...]` order every `Cols()` reader relies on -- a
/// column a sibling sub-job is still adding otherwise leaves a hole.
fn move_and_update_hidden_columns_to_public(info: &mut TableInfo, index: &GoShared<IndexInfo>) {
    let offsets: Vec<i64> = index
        .read()
        .columns
        .iter_deref()
        .map(|part| part.read().offset)
        .collect();
    let hidden: std::collections::HashSet<i64> = offsets
        .iter()
        .copied()
        .filter(|offset| {
            info.columns
                .get(*offset as usize)
                .is_some_and(|column| column.read().hidden)
        })
        .collect();
    if hidden.is_empty() {
        return;
    }
    let first_non_public = info
        .columns
        .iter_deref()
        .position(|column| column.read().state != SchemaState::PUBLIC)
        .unwrap_or(info.columns.len() - 1);
    // Go re-reads each part's offset as it goes: an earlier move shifts the
    // later parts' offsets through `MoveColumnInfo`.
    for position in 0..offsets.len() {
        let offset = index
            .read()
            .columns
            .get(position)
            .expect("index part")
            .read()
            .offset;
        if let Some(column) = info.columns.get(offset as usize) {
            column.write().state = SchemaState::PUBLIC;
        }
        // Go tests membership with the part's CURRENT offset, as here.
        if hidden.contains(&offset) {
            info.move_column_info(offset as isize, first_non_public as isize);
        }
    }
}

/// Plans one step of a persisted ADD INDEX / ADD PRIMARY KEY job: Go
/// `onCreateIndex`, and `onDropIndex` once it is rolling back. `outcome` is
/// the reorganization the owner ran since the last step, if any.
pub(super) fn plan_persisted_add_index_job_step<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
    outcome: Option<&ReorgOutcome>,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    if active.job.is_rollingback() {
        return plan_rollback_add_index(catalog, active, start_ts);
    }
    let is_pk = active.job.type_ == ActionType::ACTION_ADD_PRIMARY_KEY;
    let stored = checked_job_table(catalog, active, true)?;
    let database_id = active.job.schema_id;
    if stored.table_cache_status_type != tidb_model::TableCacheStatusType::DISABLE {
        return Err(cancel_ddl_job(
            active,
            coded_refusal(
                &tidb_util::dbterror::ERR_OPT_ON_CACHE_TABLE,
                "'Create Index' is unsupported on cache tables.",
            ),
        ));
    }
    let args = tidb_model::get_modify_index_args(&mut active.job)
        .map_err(|error| cancel_ddl_job(active, DdlPlanError::Encode(error.to_string())))?
        .ok_or_else(|| DdlPlanError::Encode("ADD INDEX job has nil args".to_owned()))?;
    let index_args: Vec<tidb_model::IndexArg> = args
        .read()
        .index_args
        .iter_deref()
        .map(|arg| arg.read().clone())
        .collect();
    let mut info = stored.clone_like_go();
    let mut indexes = Vec::with_capacity(index_args.len());
    for arg in &index_args {
        match check_and_build_index_info(&mut info, arg, is_pk) {
            Ok(index) => indexes.push(index),
            Err(error) => return Err(cancel_ddl_job(active, error)),
        }
    }
    let original_state = indexes[0].read().state;
    match original_state {
        SchemaState::NONE => {
            // Go `initForReorgIndexes`.
            let reorg_type = pick_backfill_type(&active.job);
            for index in &indexes {
                if matches!(
                    reorg_type,
                    tidb_model::ReorgType::TXN | tidb_model::ReorgType::TXN_MERGE
                ) && index.read().has_condition()
                {
                    return Err(cancel_ddl_job(active, unsupported_partial_index()));
                }
            }
            for index in &indexes {
                let mut index = index.write();
                if reorg_type.need_merge_process() {
                    index.backfill_state = tidb_model::BackfillState::RUNNING;
                }
                index.state = SchemaState::DELETE_ONLY;
            }
            active.job.schema_state = SchemaState::DELETE_ONLY;
            let write = table_step(catalog, active, database_id, &info, true, start_ts)?;
            Ok(action_step(write))
        }
        SchemaState::DELETE_ONLY | SchemaState::WRITE_ONLY => {
            let next = if original_state == SchemaState::DELETE_ONLY {
                SchemaState::WRITE_ONLY
            } else {
                SchemaState::WRITE_REORGANIZATION
            };
            for index in &indexes {
                index.write().state = next;
            }
            if is_pk {
                let index = indexes[0].read().clone_like_go();
                check_primary_key_not_null(&mut info, &index);
            }
            if next == SchemaState::WRITE_REORGANIZATION {
                active.job.snapshot_version = 0;
            }
            active.job.schema_state = next;
            let write = table_step(catalog, active, database_id, &info, true, start_ts)?;
            Ok(action_step(write))
        }
        SchemaState::WRITE_REORGANIZATION => plan_write_reorganization(
            snapshot, catalog, active, info, indexes, outcome, start_ts, is_pk,
        ),
        state => Err(DdlPlanError::from(
            tidb_util::dbterror::ERR_INVALID_DDL_STATE.generate_with_stack_by_args(&[
                FormatArg::from("index"),
                FormatArg::new(state.to_string(), state.0.to_string(), "model.SchemaState"),
            ]),
        )),
    }
}

/// Go `checkPrimaryKeyNotNull`: a nullable key column gets
/// `PreventNullInsertFlag`; one holding a NULL refuses the key.
fn check_primary_key_not_null(info: &mut TableInfo, index: &IndexInfo) {
    for part in index.columns.iter_deref() {
        let offset = part.read().offset as usize;
        if let Some(column) = info.columns.get(offset) {
            let mut column = column.write();
            if !column
                .field_type
                .has_flag(tidb_datatype::FieldTypeFlags::NOT_NULL)
            {
                column
                    .field_type
                    .add_flags(tidb_datatype::FieldTypeFlags::PREVENT_NULL_INSERT);
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn plan_write_reorganization<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    mut info: TableInfo,
    indexes: Vec<GoShared<IndexInfo>>,
    outcome: Option<&ReorgOutcome>,
    start_ts: u64,
    is_pk: bool,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let database_id = active.job.schema_id;
    let analyze_state = active
        .job
        .reorg_meta
        .as_ref()
        .map_or(tidb_model::reorg::analyze_state::NONE, |meta| {
            meta.read().analyze_state
        });
    if analyze_state == tidb_model::reorg::analyze_state::NONE {
        match do_reorg_work_for_create_index(
            snapshot, catalog, active, &info, &indexes, outcome, start_ts,
        )? {
            CreateIndexReorg::Step(step) => return Ok(step),
            CreateIndexReorg::Failed(error) => {
                // Go `runReorgJobAndHandleErr` turns a non-retryable failure
                // into a rollback.
                let (write, error) =
                    convert_add_index_to_rollback(active, &mut info, &indexes, error)?;
                return Ok(PersistedDdlActionStep {
                    write,
                    update_raw_args: true,
                    run_error: Some(error),
                    reorg: None,
                });
            }
            CreateIndexReorg::Done => {}
        }
        // Go: no analyze for a job whose `tidb_enable_ddl_analyze` is off.
        if let Some(meta) = &active.job.reorg_meta {
            meta.write().analyze_state = tidb_model::reorg::analyze_state::SKIPPED;
        }
        check_and_mark_non_revertible(&mut active.job);
        let write = table_step(catalog, active, database_id, &info, true, start_ts)?;
        return Ok(action_step(write));
    }
    // Analyze done or skipped: public.
    for index in &indexes {
        let mut index = index.write();
        index.state = SchemaState::PUBLIC;
    }
    if is_pk {
        let index = indexes[0].read().clone_like_go();
        for part in index.columns.iter_deref() {
            let offset = part.read().offset as usize;
            if let Some(column) = info.columns.get(offset) {
                let mut column = column.write();
                column
                    .field_type
                    .add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
            }
        }
    }
    let mut write = table_step(catalog, active, database_id, &info, true, start_ts)?;
    // Go `asyncNotifyEvent(NewAddIndexEvent(...))`, skipped for the memory
    // and system databases.
    if !tidb_metadef::is_mem_or_sys_db(&active.job.schema_name.to_string().go_to_lower()) {
        let analyzed = active.job.reorg_meta.as_ref().is_some_and(|meta| {
            meta.read().analyze_state == tidb_model::reorg::analyze_state::DONE
        });
        let added = indexes
            .iter()
            .map(|index| index.read().clone_like_go())
            .collect();
        append_schema_change_mutations(
            snapshot,
            catalog,
            active.job.id,
            &[(
                notifier_sub_job_id(&active.job),
                SchemaChangeEvent::add_indexes(info.clone_like_go(), added, analyzed),
            )],
            &mut write.mutations,
        )?;
    }
    let finished_args = GoShared::new(tidb_model::ModifyIndexArgs {
        partition_ids: partition_ids(&info).into(),
        op_type: tidb_model::IndexOp::ADD_INDEX,
        index_args: indexes
            .iter()
            .map(|index| {
                let index = index.read();
                tidb_model::IndexArg {
                    index_id: index.id,
                    if_exist: false,
                    is_global: index.global,
                    ..tidb_model::IndexArg::default()
                }
            })
            .collect::<Vec<_>>()
            .into(),
    });
    active.job.fill_finished_args(Some(finished_args));
    active.job.finish_table_job(
        JobState::DONE,
        SchemaState::PUBLIC,
        write.schema_version,
        Some(GoShared::new(info.clone_like_go())),
    );
    Ok(PersistedDdlActionStep {
        write,
        // Go `needUpdateRawArgs`: the finished arguments are what history
        // and the delete range read.
        update_raw_args: true,
        run_error: None,
        reorg: None,
    })
}

/// What one visit of Go `doReorgWorkForCreateIndex` came to.
pub(super) enum CreateIndexReorg {
    /// The step to take: a reorganization request, a recorded count, or a
    /// backfill-state change.
    Step(PersistedDdlActionStep),
    /// The indexes are backfilled (and merged); the caller moves on in this
    /// same visit.
    Done,
    /// The reorganization failed with this error; the job's row count is
    /// recorded.
    Failed(DdlPlanError),
}

/// Go `doReorgWorkForCreateIndex` over `indexes` of `info`: the
/// transactional backfill, and for a merge reorganization the
/// running -> ready-to-merge -> merging -> inapplicable backfill states,
/// each persisted by its own step. The owner runs a requested
/// reorganization and hands its [`ReorgOutcome`] to the next visit.
pub(super) fn do_reorg_work_for_create_index<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    info: &TableInfo,
    indexes: &[GoShared<IndexInfo>],
    outcome: Option<&ReorgOutcome>,
    start_ts: u64,
) -> Result<CreateIndexReorg, DdlPlanError> {
    let database_id = active.job.schema_id;
    let reorg_type = pick_backfill_type(&active.job);
    let backfill_state = indexes[0].read().backfill_state;
    let merging =
        reorg_type.need_merge_process() && backfill_state == tidb_model::BackfillState::MERGING;
    // A merge's reorg phases are its own: an outcome from the backfill phase
    // is not the merge's.
    let outcome = outcome.filter(|outcome| {
        outcome.job_id == active.job.id && outcome.merging == merging && outcome.column.is_none()
    });
    if reorg_type.need_merge_process()
        && backfill_state == tidb_model::BackfillState::READY_TO_MERGE
    {
        // Go: ReadyToMerge -> Merging, with a fresh snapshot for the merge.
        for index in indexes {
            index.write().backfill_state = tidb_model::BackfillState::MERGING;
        }
        active.job.snapshot_version = 0;
        let write = table_step(catalog, active, database_id, info, true, start_ts)?;
        return Ok(CreateIndexReorg::Step(action_step(write)));
    }
    let request =
        |active: &crate::ddl_job_table::ActiveDdlJob, stage: ReorgStageRequest| AddIndexReorg {
            job_id: active.job.id,
            stage,
            table: Box::new(info.clone_like_go()),
            index_ids: indexes.iter().map(|index| index.read().id).collect(),
            merging,
            physical_ids: physical_ids(info),
            batch_size: job_batch_size(&active.job),
            session_vars: ReorgSessionVars::of_job(&active.job),
            row_count: active.job.row_count,
            column: None,
            sub_job: reorg_sub_job(&active.job),
            clean_up_global_index: false,
            partition_phase: None,
            schema_id: active.job.schema_id,
        };
    let finished = match outcome.map(|outcome| &outcome.result) {
        // Go `getReorgInfo`: the first visit persists the snapshot.
        None if active.job.snapshot_version == 0 => {
            let reorg = request(active, ReorgStageRequest::Init);
            return Ok(CreateIndexReorg::Step(reorg_step(&active.job, reorg)));
        }
        Some(ReorgResult::Initialized(version)) => {
            active.job.snapshot_version = *version;
            let write = job_only_write(active.job.id, Vec::new());
            return Ok(CreateIndexReorg::Step(action_step(write)));
        }
        None | Some(ReorgResult::Interrupted) => {
            let handle = load_reorg_handle(snapshot, catalog, active)?;
            let reorg = request(active, ReorgStageRequest::Run(handle));
            return Ok(CreateIndexReorg::Step(reorg_step(&active.job, reorg)));
        }
        Some(ReorgResult::Progress(rows)) => {
            // Go `onCreateIndex` on `ErrWaitReorgTimeout`: the job keeps
            // its state and records the count so far.
            active.job.row_count = *rows;
            return Ok(CreateIndexReorg::Step(action_step(job_only_write(
                active.job.id,
                Vec::new(),
            ))));
        }
        Some(ReorgResult::Checked(_)) => {
            return Err(DdlPlanError::Encode(
                "an index reorganization got a column check's outcome".to_owned(),
            ))
        }
        Some(ReorgResult::Skipped) => active.job.row_count,
        Some(ReorgResult::Done(rows)) => *rows,
        Some(ReorgResult::Failed {
            code,
            message,
            rows,
        }) => {
            // Go `runReorgJob` records the count first.
            active.job.row_count = *rows;
            return Ok(CreateIndexReorg::Failed(
                crate::cluster_ddl::checked_refusal(*code, message.clone()),
            ));
        }
    };
    active.job.row_count = finished;
    if reorg_type.need_merge_process() {
        if backfill_state == tidb_model::BackfillState::RUNNING {
            for index in indexes {
                index.write().backfill_state = tidb_model::BackfillState::READY_TO_MERGE;
            }
            let write = table_step(catalog, active, database_id, info, true, start_ts)?;
            return Ok(CreateIndexReorg::Step(action_step(write)));
        }
        // The merge finished: no more double writes.
        for index in indexes {
            index.write().backfill_state = tidb_model::BackfillState::INAPPLICABLE;
        }
    }
    Ok(CreateIndexReorg::Done)
}

/// Go `convertAddIdxJob2RollbackJob`: the indexes back to delete-only, the
/// job rolling back with `error`.
fn convert_add_index_to_rollback(
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    info: &mut TableInfo,
    indexes: &[GoShared<IndexInfo>],
    error: DdlPlanError,
) -> Result<(DdlWrite, DdlPlanError), DdlPlanError> {
    let mut drop_args = tidb_model::ModifyIndexArgs {
        partition_ids: partition_ids(info).into(),
        op_type: tidb_model::IndexOp::ROLLBACK_ADD_INDEX,
        ..tidb_model::ModifyIndexArgs::default()
    };
    let mut index_args = Vec::new();
    for index in indexes {
        let mut index = index.write();
        if index.primary {
            for part in index.columns.iter_deref() {
                let offset = part.read().offset as usize;
                if let Some(column) = info.columns.get(offset) {
                    column
                        .write()
                        .field_type
                        .del_flags(tidb_datatype::FieldTypeFlags::PREVENT_NULL_INSERT);
                }
            }
        }
        index.state = SchemaState::DELETE_ONLY;
        index_args.push(tidb_model::IndexArg {
            index_name: index.name.clone(),
            if_exist: false,
            ..tidb_model::IndexArg::default()
        });
    }
    drop_args.index_args = index_args.into();
    active
        .job
        .fill_finished_args(Some(GoShared::new(drop_args)));
    active.job.schema_state = SchemaState::DELETE_ONLY;
    active.job.state = JobState::ROLLINGBACK;
    let database_id = active.job.schema_id;
    let mut info = info.clone_like_go();
    info.update_ts = 0;
    let mutations = vec![BufferMutation::set(
        key::table_kv_key(database_id, info.id),
        value::serialize_table_info(&info)
            .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
    )
    .map_err(|error| DdlPlanError::Encode(error.to_string()))?];
    Ok((job_only_write(active.job.id, mutations), error))
}

/// Go `onDropIndex` for a rolled-back ADD INDEX: delete-only ->
/// delete-reorganization -> gone, then ROLLBACK DONE.
fn plan_rollback_add_index(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let stored = checked_job_table(catalog, active, true)?;
    let database_id = active.job.schema_id;
    let mut info = stored.clone_like_go();
    let args = tidb_model::get_finished_modify_index_args(&mut active.job)
        .map_err(|error| DdlPlanError::Encode(error.to_string()))?
        .ok_or_else(|| DdlPlanError::Encode("rolling back ADD INDEX has nil args".to_owned()))?;
    let names: Vec<String> = args
        .read()
        .index_args
        .iter_deref()
        .map(|arg| arg.read().index_name.original().to_owned())
        .collect();
    let indexes: Vec<GoShared<IndexInfo>> = names
        .iter()
        .filter_map(|name| find_index(&info, name))
        .collect();
    let Some(first) = indexes.first() else {
        // Nothing left to remove (the index was never added).
        active.job.state = JobState::ROLLBACK_DONE;
        return Ok(PersistedDdlActionStep {
            write: job_only_write(active.job.id, Vec::new()),
            update_raw_args: false,
            run_error: None,
            reorg: None,
        });
    };
    let original_state = first.read().state;
    match original_state {
        SchemaState::PUBLIC | SchemaState::WRITE_ONLY | SchemaState::DELETE_ONLY => {
            let next = match original_state {
                SchemaState::PUBLIC => SchemaState::WRITE_ONLY,
                SchemaState::WRITE_ONLY => SchemaState::DELETE_ONLY,
                _ => SchemaState::DELETE_REORGANIZATION,
            };
            for index in &indexes {
                index.write().state = next;
            }
            active.job.schema_state = next;
            let write = table_step(catalog, active, database_id, &info, true, start_ts)?;
            Ok(action_step(write))
        }
        SchemaState::DELETE_REORGANIZATION => {
            let mut index_ids = Vec::new();
            for index in &indexes {
                let removed = index.read().clone_like_go();
                index_ids.push(removed.id);
                remove_index_and_hidden_columns(&mut info, &removed);
            }
            let write = table_step(catalog, active, database_id, &info, true, start_ts)?;
            let finished = GoShared::new(tidb_model::ModifyIndexArgs {
                partition_ids: args.read().partition_ids.clone(),
                op_type: tidb_model::IndexOp::ADD_INDEX,
                index_args: index_ids
                    .iter()
                    .map(|id| tidb_model::IndexArg {
                        index_id: *id,
                        if_exist: false,
                        ..tidb_model::IndexArg::default()
                    })
                    .collect::<Vec<_>>()
                    .into(),
            });
            active.job.finish_table_job(
                JobState::ROLLBACK_DONE,
                SchemaState::NONE,
                write.schema_version,
                Some(GoShared::new(info.clone_like_go())),
            );
            active.job.fill_finished_args(Some(finished));
            Ok(PersistedDdlActionStep {
                write,
                // Go `needUpdateRawArgs`: the delete range reads these.
                update_raw_args: true,
                run_error: None,
                reorg: None,
            })
        }
        state => Err(DdlPlanError::from(
            tidb_util::dbterror::ERR_INVALID_DDL_STATE.generate_with_stack_by_args(&[
                FormatArg::from("index"),
                FormatArg::new(state.to_string(), state.0.to_string(), "model.SchemaState"),
            ]),
        )),
    }
}

/// Go `DropIndexColumnFlag`, `RemoveDependentHiddenColumns` and
/// `removeIndexInfo`.
pub(super) fn remove_index_and_hidden_columns(info: &mut TableInfo, index: &IndexInfo) {
    drop_index_column_flag(info, index);
    let hidden: Vec<String> = index
        .columns
        .iter_deref()
        .filter_map(|part| {
            let offset = part.read().offset as usize;
            info.columns.get(offset).and_then(|column| {
                let column = column.read();
                column.hidden.then(|| column.name.lowercase().to_owned())
            })
        })
        .collect();
    let kept_indexes: Vec<IndexInfo> = info
        .indices
        .iter_deref()
        .filter(|existing| existing.read().id != index.id)
        .map(|existing| existing.read().clone_like_go())
        .collect();
    info.indices = kept_indexes.into();
    if hidden.is_empty() {
        return;
    }
    let kept_columns: Vec<tidb_model::ColumnInfo> = info
        .columns
        .iter_deref()
        .filter(|column| !hidden.contains(&column.read().name.lowercase().to_owned()))
        .map(|column| column.read().clone_like_go())
        .collect();
    let mut kept_columns = kept_columns;
    for (offset, column) in kept_columns.iter_mut().enumerate() {
        column.offset = offset as i64;
    }
    info.columns = kept_columns.into();
    // Index parts address columns by offset.
    let names: Vec<String> = info
        .columns
        .iter_deref()
        .map(|column| column.read().name.lowercase().to_owned())
        .collect();
    for existing in info.indices.iter_deref() {
        for part in existing.read().columns.iter_deref() {
            let mut part = part.write();
            if let Some(offset) = names.iter().position(|name| *name == part.name.lowercase()) {
                part.offset = offset as i64;
            }
        }
    }
}

/// Go `DropIndexColumnFlag`: the flag the index gave its columns goes, and
/// another index led by the same first column puts its own flag back.
fn drop_index_column_flag(info: &TableInfo, index: &IndexInfo) {
    use tidb_datatype::FieldTypeFlags;
    let unflag = |offset: i64, flag| {
        if let Some(column) = usize::try_from(offset)
            .ok()
            .and_then(|offset| info.columns.get(offset))
        {
            column.write().field_type.del_flags(flag);
        }
    };
    let Some(first) = index.columns.get(0) else {
        return;
    };
    if index.primary {
        for part in index.columns.iter_deref() {
            unflag(part.read().offset, FieldTypeFlags::PRI_KEY);
        }
    } else if index.unique && index.columns.len() == 1 {
        unflag(first.read().offset, FieldTypeFlags::UNIQUE_KEY);
    } else {
        unflag(first.read().offset, FieldTypeFlags::MULTIPLE_KEY);
    }
    let first_name = first.read().name.lowercase().to_owned();
    for other in info.indices.iter_deref() {
        let other = other.read();
        if other.name.lowercase() == index.name.lowercase() {
            continue;
        }
        if other
            .columns
            .get(0)
            .is_none_or(|part| part.read().name.lowercase() != first_name)
        {
            continue;
        }
        super::add_index_column_flag(info, &other);
    }
}

/// Go `rollingbackAddIndex` for `ADMIN CANCEL DDL JOB`: the job turns into
/// its rollback, from wherever it stood.
pub(super) fn plan_cancel_add_index(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let _ = start_ts;
    let stored = checked_job_table(catalog, active, true)?;
    let mut info = stored.clone_like_go();
    let args = tidb_model::get_modify_index_args(&mut active.job)
        .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
    let names: Vec<String> = args
        .map(|args| {
            args.read()
                .index_args
                .iter_deref()
                .map(|arg| arg.read().index_name.original().to_owned())
                .collect()
        })
        .unwrap_or_default();
    let indexes: Vec<GoShared<IndexInfo>> = names
        .iter()
        .filter_map(|name| find_index(&info, name))
        .collect();
    if indexes.is_empty() {
        // Go `convertNotReorgAddIdxJob2RollbackJob` before the index exists.
        return Err(cancelled_ddl_job(active));
    }
    let error = DdlPlanError::from(
        tidb_util::dbterror::ERR_CANCELLED_DDL_JOB
            .generate(tidb_error::tidb::errname::ErrCancelledDDLJob.raw),
    );
    let (write, error) = convert_add_index_to_rollback(active, &mut info, &indexes, error)?;
    Ok(PersistedDdlActionStep {
        write,
        update_raw_args: true,
        run_error: Some(error),
        reorg: None,
    })
}

/// Go `getDDLReorgHandle`: the job's `mysql.tidb_ddl_reorg` row.
pub(super) fn load_reorg_handle<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    active: &crate::ddl_job_table::ActiveDdlJob,
) -> Result<ReorgHandle, DdlPlanError> {
    let table =
        crate::ddl_reorg_table::DdlReorgTable::locate(catalog).map_err(DdlPlanError::Encode)?;
    table
        .load(snapshot, active.job.id)
        .map_err(DdlPlanError::Encode)?
        .into_iter()
        .next()
        .map(|row| row.handle)
        .ok_or_else(|| {
            // Go `meta.ErrDDLReorgElementNotExist`.
            DdlPlanError::Encode(format!("DDL job {} has no reorg handle", active.job.id))
        })
}
