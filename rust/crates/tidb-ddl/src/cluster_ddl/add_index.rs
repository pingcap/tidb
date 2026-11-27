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

/// Go `tablecodec.TempIndexPrefix`.
pub const TEMP_INDEX_PREFIX: i64 = 0x7fff_0000_0000_0000;

/// What the owner must reorganize before an ADD INDEX job moves on.
#[derive(Clone, Debug)]
pub struct AddIndexReorg {
    /// The job.
    pub job_id: i64,
    /// Go `job.Query`, which the reorganization's workers attach to Top SQL.
    pub query: tidb_goruntime::GoString,
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
    /// Go `ReorgMeta.GetConcurrency`: the backfill workers to run.
    pub concurrency: usize,
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
    /// Go master `buildCreateMaterializedViewData`: a new materialized
    /// view's initial rows, answered with the build's read TS.
    BuildMaterializedView(Box<super::materialized_view::MViewBuild>),
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
    /// Go `executeDistTask`: DXF paused for storage disk full; persist the
    /// system pause before the owning multi-schema job promotes its proxy.
    AutoPaused { task_id:i64, error:Option<std::sync::Arc<dyn std::error::Error+Send+Sync>> },
    /// It failed with this error, having counted this many rows.
    Failed {
        /// The error code.
        code: u16,
        /// The error message.
        message: String,
        /// The job's row count when it failed.
        rows: i64,
        /// The error's Go identity, which the job error keeps (`[kv:8025]`).
        identity: Option<tidb_terror::TerrorIdentity>,
    },
    /// It stopped between batches because the job is no longer runnable
    /// (Go `isReorgRunnable`: cancelling or pausing); the handle holds where
    /// it got to.
    Interrupted,
    /// A column check ran: `None` when every row fits, else the error Go's
    /// `checkModifyColumnData` answers (`checked == true`).
    Checked(Option<(u16, String)>),
    /// An index reorganization (Go `addTableIndex`) returned this error,
    /// which the next step handles as Go's `runReorgJobAndHandleErr`.
    Error(DdlPlanError),
    /// A materialized view's initial build finished at this read TS (Go
    /// master `setCreateMaterializedViewBuildReadTSInReorgCtx`).
    Built(u64),
}

/// The ADD INDEX submission's answer.
pub enum AddIndexSubmission {
    /// The job to queue, and the warnings Go's executor appended to the
    /// statement before submitting it.
    Job(
        Box<tidb_ddl_jobsubmit::JobSpec>,
        Vec<tidb_context::SqlWarn>,
    ),
    /// `IF NOT EXISTS` over an existing index: a Note, no job.
    AlreadySatisfied {
        /// What was already true.
        detail: String,
        /// The Note.
        warnings: Vec<tidb_context::SqlWarn>,
    },
}

/// Go `checkPrimaryKeyNotNull` with the index's resolved definition.
pub(crate) fn check_primary_key_null_values(
    catalog: &SessionExtendedInfoSchema,
    schema: &str,
    table: &str,
    index: &IndexInfo,
    has_null: &mut dyn FnMut(&str, &TableInfo, i64) -> Result<bool, DdlPlanError>,
) -> Result<(), DdlPlanError> {
    if !index.primary {
        return Ok(());
    }
    let (_, stored) = locate_table(catalog, schema, table)?;
    let stored = stored.read();
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
        if nullable && has_null(schema, &stored, id)? {
            return Err(coded_refusal(
                &tidb_dbterror::ERR_INVALID_USE_OF_NULL,
                "Invalid use of NULL value",
            ));
        }
    }
    Ok(())
}

/// Go `checkIndexNameAndColumns` for a columnar index: the name the job
/// adds, and the note of an `IF NOT EXISTS` over an existing name -- after
/// which Go goes on with the EMPTY name.
fn check_index_name_and_columns(
    stored: &TableInfo,
    spec: &ColumnarIndexSpec,
    columnar_index_type: tidb_model::ColumnarIndexType,
    if_not_exists: bool,
) -> Result<(String, Vec<tidb_context::SqlWarn>), DdlPlanError> {
    let mut index_name = spec.index_name.clone();
    // Deal with anonymous index.
    if index_name.is_empty() {
        // Go `getAnonymousIndexPrefix`.
        let prefix = match spec.parts.first() {
            Some(tidb_ast::IndexPart::Column { name, .. }) => name.as_str(),
            _ if columnar_index_type == tidb_model::ColumnarIndexType::VECTOR => "vector_index",
            _ => "expression_index",
        };
        let existing = stored
            .indices
            .iter_deref()
            .map(|index| index.read().name.original().to_owned())
            .collect::<Vec<_>>();
        let existing = existing.iter().map(String::as_str).collect::<Vec<_>>();
        index_name = tidb_ddl_build::index::get_name_for_anonymous_index(&existing, prefix, "");
    }
    let mut warnings = Vec::new();
    if let Some(existing) = find_index(stored, &index_name) {
        let error = if existing.read().state != SchemaState::PUBLIC {
            // NOTE: explicit error message. See issue #18363.
            tidb_dbterror::ERR_DUP_KEY_NAME.gen_with_stack(format!(
                "index already exist {index_name}; a background job is trying to add the same index, please check by `ADMIN SHOW DDL JOBS`"
            ))
        } else {
            tidb_dbterror::ERR_DUP_KEY_NAME.gen_with_stack_by_args(&[index_name.clone().into()])
        };
        if !if_not_exists {
            return Err(DdlPlanError::from(error));
        }
        warnings.push(crate::real_tikv_ddl::ddl_note(error));
        index_name = String::new();
    }
    // Go `checkTooLongIndex`.
    tidb_ddl_build::check_too_long_identifier(&index_name)
        .map_err(|error| DdlPlanError::Admission(DdlAdmissionError::from_mysql_error(error)))?;
    // Go `checkAddColumnTooManyColumns(len(t.Cols()))`, the public columns.
    let public_columns = stored
        .columns
        .iter_deref()
        .filter(|column| column.read().state == SchemaState::PUBLIC)
        .count();
    check_add_column_too_many_columns(public_columns)?;
    Ok((index_name, warnings))
}

/// The key parts a columnar index job carries: Go's
/// `IndexPartSpecification`s with a vector index's `Expr` cleared, since
/// `FuncExpr` carries it as text.
fn columnar_job_parts(parts: &[tidb_ast::IndexPart]) -> Vec<tidb_ast::IndexPartSpecification> {
    parts
        .iter()
        .map(|part| match part {
            tidb_ast::IndexPart::Column { name, prefix_len, desc } => tidb_ast::IndexPartSpecification {
                column: Some(Box::new(tidb_ast::ColumnName {
                    name: CiString::new(name.clone()),
                    ..tidb_ast::ColumnName::default()
                })),
                length: prefix_len.unwrap_or(-1),
                desc: *desc,
                expr: None,
            },
            tidb_ast::IndexPart::Expr { desc, .. } => tidb_ast::IndexPartSpecification {
                column: None,
                length: -1,
                desc: *desc,
                expr: None,
            },
        })
        .collect()
}

/// The key parts `BuildIndexInfo` reads back from a columnar index job: a
/// vector index's part is `FuncExpr` parsed again (Go `onCreateColumnarIndex`).
fn columnar_parts_of_job(arg: &tidb_model::IndexArg) -> Result<Vec<tidb_ast::IndexPart>, DdlPlanError> {
    arg.index_part_specifications
        .iter_deref()
        .map(|part| {
            let part = part.read();
            Ok(match &part.column {
                Some(column) => tidb_ast::IndexPart::Column {
                    name: column.name.original().to_owned(),
                    prefix_len: (part.length != -1).then_some(part.length),
                    desc: part.desc,
                },
                None => tidb_ast::IndexPart::Expr {
                    expr: tidb_model::generated_expr::parse_expression(&arg.func_expr)
                        .map_err(|error| DdlPlanError::Encode(error.message))?,
                    desc: part.desc,
                },
            })
        })
        .collect()
}

/// Go `executor.createColumnarIndex` after `getSchemaAndTableByIdent`: the
/// `ActionAddColumnarIndex` job of a vector, inverted or full-text index.
fn create_columnar_index(
    schema_id: i64,
    schema: &str,
    stored: &TableInfo,
    spec: &ColumnarIndexSpec,
    if_not_exists: bool,
    context: &DdlStatementContext,
) -> Result<AddIndexSubmission, DdlPlanError> {
    let tp = tidb_ast::IndexType(spec.option.tp);
    let columnar_index_type = if tp == tidb_ast::IndexType::INVERTED {
        tidb_model::ColumnarIndexType::INVERTED
    } else if tp == tidb_ast::IndexType::VECTOR {
        tidb_model::ColumnarIndexType::VECTOR
    } else if tp == tidb_ast::IndexType::FULLTEXT {
        tidb_model::ColumnarIndexType::FULLTEXT
    } else {
        return Err(DdlPlanError::from(
            tidb_dbterror::ERR_UNSUPPORTED_INDEX_TYPE.gen_with_stack_by_args(&[tp.sql().into()]),
        ));
    };
    if columnar_index_type == tidb_model::ColumnarIndexType::FULLTEXT {
        tidb_ddl_build::table_info_build::check_full_text_supported_in_starter()
            .map_err(DdlPlanError::Admission)?;
    }
    check_table_type_for_columnar_index(stored)?;
    let (index_name, warnings) =
        check_index_name_and_columns(stored, spec, columnar_index_type, if_not_exists)?;
    let index_name = CiString::new(index_name);
    let metabuild = context.0.metabuild_context();
    // Go `build{Inverted,Vector,FullText}InfoWithCheck` and
    // `buildIndexColumns`, whose results the executor drops but for the
    // vector function's text.
    let (_, columnar) = tidb_ddl_build::table_info_build::build_columnar_index_info(
        Some(&metabuild),
        stored,
        &index_name,
        columnar_index_type,
        &spec.parts,
        &spec.option,
        SchemaState::NONE,
    )
    .map_err(DdlPlanError::Admission)?;
    // May be truncate comment here, when index comment too long and
    // sql_mode is't strict.
    let mut option = spec.option.clone();
    option.comment = tidb_ddl_build::validate_comment_length(
        &option.comment,
        index_name.original(),
        tidb_ddl_build::CommentOwner::Table,
        &metabuild,
    )
    .map_err(|error| DdlPlanError::Admission(DdlAdmissionError::from_mysql_error(error)))?;

    // Go `buildAddIndexJobWithoutTypeAndArgs`.
    let mut job = Job::default();
    job.version = get_job_ver_in_use();
    job.schema_id = schema_id;
    job.table_id = stored.id;
    job.schema_name = schema.go_to_lower().into();
    job.table_name = stored.name.lowercase().to_owned().into();
    job.type_ = ActionType::ACTION_ADD_COLUMNAR_INDEX;
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
    job.session_vars = Some(GoShared::new(std::collections::BTreeMap::new()));

    let arg = tidb_model::IndexArg {
        index_name,
        index_part_specifications: columnar_job_parts(&spec.parts).into(),
        index_option: Some(GoShared::new(option)),
        func_expr: columnar.func_expr,
        is_columnar: true,
        columnar_index_type,
        ..tidb_model::IndexArg::default()
    };
    let args = GoShared::new(tidb_model::ModifyIndexArgs {
        index_args: vec![arg].into(),
        op_type: tidb_model::IndexOp::ADD_INDEX,
        ..tidb_model::ModifyIndexArgs::default()
    });
    Ok(AddIndexSubmission::Job(
        Box::new(tidb_ddl_jobsubmit::JobSpec {
            job,
            args: JobArgsValue::ModifyIndex(Some(args)),
            id_allocated: true,
        }),
        warnings,
    ))
}

/// Go `buildIndexPresplitOpt`: the index option's `PRE_SPLIT_REGIONS`, its
/// expressions kept as their restored SQL.
fn build_index_presplit_opt(
    opt: Option<&tidb_ast::IndexPreSplitRegions>,
) -> Result<Option<tidb_model::IndexArgSplitOpt>, DdlPlanError> {
    let restore = |exprs: &[tidb_ast::Expr]| -> Vec<String> {
        exprs.iter().map(|expr| expr.restore_with_flags(tidb_parser_format::RestoreFlags::DEFAULT)).collect()
    };
    // Go `ast.SplitOption`: `PRE_SPLIT_REGIONS = n` is `{Num: n}`.
    let (lower, upper, num, value_lists) = match opt {
        None => return Ok(None),
        Some(tidb_ast::IndexPreSplitRegions::Count(num)) => (Vec::new(), Vec::new(), *num, Vec::new()),
        Some(tidb_ast::IndexPreSplitRegions::Boundaries(tidb_ast::SplitOption::By(lists))) => {
            (Vec::new(), Vec::new(), 0, lists.iter().map(|list| restore(list)).collect::<Vec<_>>())
        }
        Some(tidb_ast::IndexPreSplitRegions::Boundaries(tidb_ast::SplitOption::Between { lower, upper, regions })) => {
            (restore(lower), restore(upper), *regions, Vec::new())
        }
    };
    if !value_lists.is_empty() {
        return Ok(Some(tidb_model::IndexArgSplitOpt {
            num,
            value_lists: value_lists.into_iter().map(Into::into).collect::<Vec<_>>().into(),
            ..tidb_model::IndexArgSplitOpt::default()
        }));
    }
    let max_split_region_num = tidb_config::config_tree::config::get_global_config().split_region_max_num as i64;
    if num > max_split_region_num {
        return Err(DdlPlanError::from_mysql_error(tidb_terror::MysqlError::unknown(format!(
            "Split index region num exceeded the limit {max_split_region_num}"
        ))));
    } else if num < 1 {
        return Err(DdlPlanError::from_mysql_error(tidb_terror::MysqlError::unknown(
            "Split index region num should be greater than 0",
        )));
    }
    Ok(Some(tidb_model::IndexArgSplitOpt {
        lower: lower.into(),
        upper: upper.into(),
        num,
        ..tidb_model::IndexArgSplitOpt::default()
    }))
}

/// Go `checkTableTypeForColumnarIndex`.
fn check_table_type_for_columnar_index(stored: &TableInfo) -> Result<(), DdlPlanError> {
    tidb_ddl_build::table_info_build::check_table_type_for_columnar_index(stored)
        .map_err(DdlPlanError::Admission)
}

/// Go `executor.CreateIndex` with the prepared column/index definition.
#[allow(clippy::too_many_arguments)]
pub(crate) fn create_index_with_info<S: MetaSnapshot>(
    _snapshot: &mut S,
    catalog: &SessionExtendedInfoSchema,
    schema: &String,
    table: &String,
    if_not_exists: &bool,
    index: &Box<IndexInfo>,
    expressions: &Vec<Option<tidb_ast::Expr>>,
    condition: &Option<tidb_ast::Expr>,
    context: &DdlStatementContext,
    columnar: &Option<Box<ColumnarIndexSpec>>,
    pre_split_regions: Option<&tidb_ast::IndexPreSplitRegions>,
    store_count: usize,
    job_context: &dyn DdlSessionContext,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    let (schema_id, stored) = locate_table(&catalog, schema, table)?;
    let stored = stored.read();
    // Go master `CheckIndexOperationMaterializedViewConstraints` in
    // `CreatePrimaryKey`, `createColumnarIndex` and `createIndex`.
    let (op, unique) = if index.primary {
        ("ALTER TABLE ADD PRIMARY KEY", true)
    } else if columnar.is_some() {
        ("CREATE INDEX", false)
    } else if index.unique {
        ("CREATE UNIQUE INDEX", true)
    } else {
        ("CREATE INDEX", false)
    };
    super::materialized_view::check_index_operation_materialized_view_constraints(&stored, op, unique)
        .map_err(DdlPlanError::from_mysql_error)?;
    if let Some(spec) = columnar {
        return create_columnar_index(schema_id, schema, &stored, spec, *if_not_exists, context)
            .map(Some);
    }
    // Go `executor.CreatePrimaryKey`: check the written name, then use
    // mysql.PrimaryKeyName for every lookup and for the submitted job.
    let name = if index.primary {
        tidb_ddl_build::check_too_long_identifier(index.name.original()).map_err(|_| {
            DdlPlanError::from(
                tidb_dbterror::ERR_TOO_LONG_IDENT
                    .gen_with_stack_by_args(&[tidb_mysql::consts::PrimaryKeyName.into()]),
            )
        })?;
        tidb_mysql::consts::PrimaryKeyName.to_owned()
    } else {
        settle_added_index_name(&stored, index, expressions)
    };
    if index.primary && (find_index(&stored, "PRIMARY").is_some() || stored.pk_is_handle) {
        return Err(DdlPlanError::Admission(
            tidb_ddl_build::table_info_build::DdlAdmissionError::from_terror(
                &tidb_infoschema::infoschema_errors::ERR_MULTIPLE_PRI_KEY
                    .gen_with_stack_by_args(&[]),
            ),
        ));
    }
    if index.primary && expressions.iter().any(Option::is_some) {
        return Err(coded_refusal(
            &tidb_dbterror::ERR_FUNCTIONAL_INDEX_PRIMARY_KEY,
            "The primary key cannot be an expression index",
        ));
    }
    if let Some(existing) = find_index(&stored, &name) {
        let existing = existing.read();
        if *if_not_exists {
            return Ok(Some(AddIndexSubmission::AlreadySatisfied {
                detail: format!(
                    "index `{}` already exists on `{schema}`.`{table}`",
                    existing.name.original()
                ),
                warnings: vec![crate::real_tikv_ddl::ddl_note(
                    tidb_dbterror::ERR_DUP_KEY_NAME.gen_with_stack_by_args(&[name.into()]),
                )],
            }));
        }
        return Err(DdlPlanError::from(
            tidb_dbterror::ERR_DUP_KEY_NAME.gen_with_stack_by_args(&[name.into()]),
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
    warn_missing_region_split_policy(&stored, &name, &context.0);

    let reorg_meta = new_ddl_reorg_meta(&context.0, schema, store_count)?;
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
    let split_opt = build_index_presplit_opt(pre_split_regions)?;
    let arg = tidb_model::IndexArg {
        unique: index.unique,
        index_name: CiString::new(name.clone()),
        split_opt: split_opt.map(GoShared::new),
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
        Box::new(tidb_ddl_jobsubmit::JobSpec {
            job,
            args: JobArgsValue::ModifyIndex(Some(args)),
            id_allocated: true,
        }),
        Vec::new(),
    )))
}

pub(super) fn unsupported_partial_index() -> DdlPlanError {
    coded_refusal(
        &tidb_dbterror::ERR_UNSUPPORTED_ADD_PARTIAL_INDEX,
        "Unsupported add partial index: add partial index without fast reorg is not supported",
    )
}

/// Go `NewDDLReorgMeta` + `initJobReorgMetaFromVariables` for ADD INDEX on
/// the classic kernel.
pub(super) fn new_ddl_reorg_meta(
    context: &dyn DdlSessionContext,
    schema: &str,
    store_count: usize,
) -> Result<tidb_model::DDLReorgMeta, DdlPlanError> {
    init_job_reorg_meta_from_variables(context, schema, store_count, true, true, true)
}

/// Go `initJobReorgMetaFromVariables`: the session's reorganization
/// settings when the job may reorganize (`setReorgParam`), and its
/// distributed-task settings when it may run as one (`setDistTaskParam`).
/// `validate_dist_task_at_submit` refuses a distributed reorg without fast
/// reorg now (ADD INDEX / ADD PRIMARY KEY); MODIFY COLUMN checks it when its
/// index reorg starts (`initForReorgIndexes`).
pub(super) fn init_job_reorg_meta_from_variables(
    context: &dyn DdlSessionContext,
    schema: &str,
    store_count: usize,
    set_reorg_param: bool,
    set_dist_task_param: bool,
    validate_dist_task_at_submit: bool,
) -> Result<tidb_model::DDLReorgMeta, DdlPlanError> {
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
        // MODIFY/CHANGE COLUMN reserves NeedReorg=true before execution
        // classifies the conversion; defer its dist-reorg check until index
        // reorg starts.
        if validate_dist_task_at_submit && meta.is_dist_reorg && !meta.is_fast_reorg {
            return Err(DdlPlanError::Admission(
                tidb_ddl_build::table_info_build::DdlAdmissionError::from_terror(
                    &tidb_dbterror::ERR_UNSUPPORTED_DIST_TASK.gen_with_stack_by_args(&[]),
                ),
            ));
        }
    }
    Ok(meta)
}

/// The multi-schema sub-job a reorganization request comes from: its proxy
/// job's sequence.
pub(super) fn reorg_sub_job(job: &Job) -> Option<i32> {
    job.multi_schema_info.as_ref().map(|info| info.read().seq)
}

use tidb_ddl_build::session_pool::ReorgSessionVars;


pub(crate) fn job_batch_size(job: &Job) -> usize {
    let defaults = crate::index::reorg_defaults();
    job.reorg_meta
        .as_ref()
        .map_or(
            tidb_vardef::defaults::DEF_TIDB_DDL_REORG_BATCH_SIZE,
            |meta| meta.read().get_batch_size(defaults),
        )
        .max(1) as usize
}

/// Go `ReorgMeta.GetConcurrency`.
pub(crate) fn job_concurrency(job: &Job) -> usize {
    let defaults = crate::index::reorg_defaults();
    job.reorg_meta
        .as_ref()
        .map_or(
            tidb_vardef::defaults::DEF_TIDB_DDL_REORG_WORKER_COUNT,
            |meta| meta.read().get_concurrency(defaults),
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

/// The physical IDs a temporary-index merge walks (Go `getReorgInfo` with
/// `mergingTmpIdx` and `getNextPartitionInfo`): a global index's temporary
/// entries are prefixed with the table ID, so any global index starts the
/// merge there; the partitions follow unless every index is global.
fn merge_physical_ids(table: &TableInfo, indexes: &[GoShared<IndexInfo>]) -> Vec<i64> {
    let partitions = partition_ids(table);
    if partitions.is_empty() || !indexes.iter().any(|index| index.read().global) {
        return physical_ids(table);
    }
    let mut ids = vec![table.id];
    if !indexes.iter().all(|index| index.read().global) {
        ids.extend(partitions);
    }
    ids
}

/// Go `loadCloudStorageURI`: a distributed reorganization sorts in cloud
/// storage when `tidb_cloud_storage_uri` is set; the owner's job context
/// keeps the URI for the task (`resolve_cloud_storage_uri`).
pub(super) fn load_cloud_storage_uri(job: &Job) {
    let Some(meta) = job.reorg_meta.as_ref() else {
        return;
    };
    let cloud_storage_uri = tidb_dxf_handle::get_cloud_storage_uri(tidb_dxf_handle::cluster_id());
    crate::job_ctx::with_job_ctx(job.id, |jc| jc.cloud_storage_uri = cloud_storage_uri.clone());
    let mut meta = meta.write();
    meta.use_cloud_storage = !cloud_storage_uri.is_empty() && meta.is_dist_reorg;
    drop(meta);
    let mut job = job.clone();
    tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/ddl/afterLoadCloudStorageURI", &mut job);
}

/// Go `initForReorgIndexes`' first check: a distributed reorg needs fast
/// reorg (`ErrUnsupportedDistTask`).
pub(super) fn check_dist_reorg_is_fast_reorg(job: &Job) -> Result<(), DdlPlanError> {
    if job.reorg_meta.as_ref().is_some_and(|meta| {
        let meta = meta.read();
        meta.is_dist_reorg && !meta.is_fast_reorg
    }) {
        return Err(DdlPlanError::Admission(
            tidb_ddl_build::table_info_build::DdlAdmissionError::from_terror(
                &tidb_dbterror::ERR_UNSUPPORTED_DIST_TASK.gen_with_stack_by_args(&[]),
            ),
        ));
    }
    Ok(())
}

/// Go `initForReorgIndexes`' NextGen guard: global sort is the product
/// design for ADD INDEX / MODIFY COLUMN there, and local sort is not
/// supported. `LitInitialized` tells whether the ingest environment exists
/// (without it `pickBackfillType` falls back to a txn-merge backfill, which
/// does not sort locally), and a job that is not a fast reorganization (a
/// system-schema job during upgrade) backfills by txn and does not sort.
pub(super) fn check_next_gen_local_sort(job: &Job) -> Result<(), DdlPlanError> {
    let Some(meta) = job.reorg_meta.as_ref() else {
        return Ok(());
    };
    let (fast_reorg, cloud_storage) = {
        let meta = meta.read();
        (meta.is_fast_reorg, meta.use_cloud_storage)
    };
    if tidb_config_kerneltype::is_next_gen()
        && tidb_ddl_ingest::env::LIT_INITIALIZED.load(std::sync::atomic::Ordering::Acquire)
        && fast_reorg
        && !cloud_storage
    {
        return Err(DdlPlanError::from(
            tidb_dbterror::ERR_NOT_SUPPORTED_YET
                .gen_with_stack_by_args(&["add index or modify column with local sort".into()]),
        ));
    }
    Ok(())
}

/// Go `pickBackfillType`.
pub(super) fn pick_backfill_type(job: &Job) -> Result<tidb_model::ReorgType, DdlPlanError> {
    let Some(meta) = job.reorg_meta.as_ref() else {
        return Ok(tidb_model::ReorgType::TXN);
    };
    let mut meta = meta.write();
    if meta.reorg_type != tidb_model::ReorgType::NONE {
        // The backfill task has been started.
        // Don't change the backfill type.
        return Ok(meta.reorg_type);
    }
    if !meta.is_fast_reorg {
        meta.reorg_type = tidb_model::ReorgType::TXN;
        return Ok(meta.reorg_type);
    }
    if tidb_ddl_ingest::env::LIT_INITIALIZED.load(std::sync::atomic::Ordering::SeqCst) {
        if !meta.use_cloud_storage {
            let checked = tidb_ddl_ingest::env::lightning_env()
                .map_or(Ok(()), |env| env.disk_root.pre_check_usage());
            if let Err(error) = checked {
                tidb_ddl_logutil::ddl_ingest_logger().info(
                    "ingest backfill is not available",
                    &[tidb_log::Field::new("error", tidb_log::Value::Str(error.to_string()))],
                );
                return Err(ingest_error(error));
            }
        }
        meta.reorg_type = tidb_model::ReorgType::INGEST;
        return Ok(meta.reorg_type);
    }
    // The lightning environment is unavailable, but we can still use the txn-merge backfill.
    tidb_ddl_logutil::ddl_logger().info(
        "fallback to txn-merge backfill process",
        &[tidb_log::Field::new("lightning env initialized", tidb_log::Value::Bool(false))],
    );
    meta.reorg_type = tidb_model::ReorgType::TXN_MERGE;
    Ok(meta.reorg_type)
}

/// An ingest error as the job's error: its MySQL code and text.
fn ingest_error(error: tidb_ddl_ingest::Error) -> DdlPlanError {
    match error {
        tidb_ddl_ingest::Error::Database(error) => {
            DdlPlanError::from_mysql_error(tidb_terror::MysqlError::from_terror(&error))
        }
        error => DdlPlanError::from_mysql_error(tidb_terror::MysqlError::unknown(error.to_string())),
    }
}

/// Go `job.ReorgMeta.ReorgTp`, as the backfill picked it.
pub(super) fn job_reorg_type(job: &Job) -> tidb_model::ReorgType {
    job.reorg_meta.as_ref().map_or(tidb_model::ReorgType::NONE, |meta| meta.read().reorg_type)
}

/// Go `updateVersionAndTableInfo`: when the state moved, the version bump
/// with its diff (`updateSchemaVersion`), then the table write
/// (`updateTable` -> `Mutator.UpdateTable`), both through the step's
/// transaction.
pub(super) fn update_version_and_table_info(
    txn: &mut JobTxn<'_>,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    database_id: i64,
    info: &TableInfo,
    should_update_version: bool,
    start_ts: u64,
) -> Result<DdlWrite, DdlPlanError> {
    table_step_with_diff(
        txn,
        active,
        database_id,
        info,
        should_update_version,
        start_ts,
        |_| {},
    )
}

/// [`update_version_and_table_info`] whose diff `fill` completes, as Go's `updateSchemaVersion`
/// does per action (the affected physical tables of a partition job).
pub(super) fn table_step_with_diff(
    txn: &mut JobTxn<'_>,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    database_id: i64,
    info: &TableInfo,
    should_update_version: bool,
    start_ts: u64,
    fill: impl FnOnce(&mut SchemaDiff),
) -> Result<DdlWrite, DdlPlanError> {
    if let Some(value) = tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/mockUpdateVersionAndTableInfoErr") {
        match value.int() {
            1 => {
                return Err(DdlPlanError::from_mysql_error(tidb_terror::MysqlError::unknown(
                    "mock update version and tableInfo error",
                )))
            }
            2 => {
                // "We change it cancelled directly here, because we want to
                // get the original error with the job id appended."
                active.job.state = JobState::CANCELLED;
                return Err(DdlPlanError::from_mysql_error(tidb_terror::MysqlError::unknown(format!(
                    "mock update version and tableInfo error, jobID={}",
                    active.job.id
                ))));
            }
            _ => {}
        }
    }
    let meta = txn.meta();
    // Go: one multi-schema step publishes one version, the sub-jobs after the
    // first skipping theirs.
    let should_update_version = should_update_version && !skips_version(&active.job);
    let (schema_version, diff) = if should_update_version {
        let mut diff = SchemaDiff {
            action_type: active.job.type_,
            schema_id: database_id,
            table_id: info.id,
            ..SchemaDiff::default()
        };
        fill(&mut diff);
        update_schema_version(&meta, active, diff)?
    } else {
        (0, SchemaDiff::default())
    };
    // Go `updateTable`'s `needUpdateTs`: a public table's `UpdateTS` moves,
    // except under the jobs that keep the table's identity in place.
    let need_update_ts = info.state == SchemaState::PUBLIC
        && !matches!(
            active.job.type_,
            ActionType::ACTION_TRUNCATE_TABLE
                | ActionType::ACTION_TRUNCATE_TABLE_PARTITION
                | ActionType::ACTION_RENAME_TABLE
                | ActionType::ACTION_RENAME_TABLES
                | ActionType::ACTION_EXCHANGE_TABLE_PARTITION
        );
    // Go `updateTable` stores the handler's table itself; a Go `Clone` here
    // would persist its nil definition lists as empty ones. Only the
    // top-level `UpdateTS` and `Revision` change, so a shallow copy keeps the
    // caller's table as it was.
    let mut info = info.clone();
    if need_update_ts {
        info.update_ts = start_ts;
    }
    meta.update_table(database_id, &mut info)
        .map_err(|error| job_meta_error(&meta, error))?;
    // The step's meta writes are its transaction's.
    let mut write = job_only_write(active.job.id, Vec::new());
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
                    tidb_ddl_build::table_info_build::DdlAdmissionError::from_terror(
                        &tidb_infoschema::infoschema_errors::ERR_MULTIPLE_PRI_KEY
                            .gen_with_stack_by_args(&[]),
                    ),
                ));
            }
            return Err(DdlPlanError::from(
                tidb_dbterror::ERR_DUP_KEY_NAME.gen_with_stack_by_args(&[(arg
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
                &tidb_infoschema::infoschema_errors::ERR_COLUMN_EXISTS,
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
    let mut added = if arg.columnar_index_type != tidb_model::ColumnarIndexType::NA {
        // Go `BuildIndexInfo` of a columnar index, its checks run again
        // against the table the job finds.
        let option = arg
            .index_option
            .as_ref()
            .map(|option| option.read().clone())
            .unwrap_or_default();
        let (added, _) = tidb_ddl_build::table_info_build::build_columnar_index_info(
            None,
            info,
            &arg.index_name,
            arg.columnar_index_type,
            &columnar_parts_of_job(arg)?,
            &option,
            SchemaState::NONE,
        )
        .map_err(DdlPlanError::Admission)?;
        added
    } else {
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
        build_index_info(info, &template, condition.as_ref(), arg.index_name.original())?
    };
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
pub(super) fn plan_persisted_add_index_job_step(
    txn: &mut JobTxn<'_>,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
    outcome: Option<&ReorgOutcome>,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    if active.job.is_rollingback() {
        return plan_rollback_add_index(txn, active, start_ts);
    }
    let is_pk = active.job.type_ == ActionType::ACTION_ADD_PRIMARY_KEY;
    let stored = checked_job_table(txn, active, true)?;
    let stored = stored.read();
    let database_id = active.job.schema_id;
    if stored.table_cache_status_type != tidb_model::TableCacheStatusType::DISABLE {
        return Err(cancel_ddl_job(
            active,
            coded_refusal(
                &tidb_dbterror::ERR_OPT_ON_CACHE_TABLE,
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
            if let Err(error) = check_dist_reorg_is_fast_reorg(&active.job) {
                return Err(cancel_ddl_job(active, error));
            }
            load_cloud_storage_uri(&active.job);
            if let Err(error) = check_next_gen_local_sort(&active.job) {
                return Err(cancel_ddl_job(active, error));
            }
            let reorg_type = match pick_backfill_type(&active.job) {
                Ok(reorg_type) => reorg_type,
                Err(error) => return Err(cancel_ddl_job(active, error)),
            };
            for index in &indexes {
                if matches!(
                    reorg_type,
                    tidb_model::ReorgType::TXN | tidb_model::ReorgType::TXN_MERGE
                ) && index.read().has_condition()
                {
                    return Err(cancel_ddl_job(active, unsupported_partial_index()));
                }
            }
            if let Err(error) = crate::index_presplit::pre_split_index_regions(
                txn.region_store().as_deref(),
                &info,
                &indexes,
                reorg_type,
                &args.read(),
                &tidb_ddl_build::session_pool::ReorgSessionVars::of_job(&active.job),
            ) {
                if !crate::real_tikv_ddl::is_retryable_job_error(
                    &error,
                    active.job.error_count,
                    tidb_vardef::ddl_error_count_limit(),
                ) {
                    return Err(cancel_ddl_job(active, error));
                }
                return Err(error);
            }
            for index in &indexes {
                let mut index = index.write();
                if reorg_type.need_merge_process() {
                    index.backfill_state = tidb_model::BackfillState::RUNNING;
                }
                index.state = SchemaState::DELETE_ONLY;
            }
            active.job.schema_state = SchemaState::DELETE_ONLY;
            let write = update_version_and_table_info(txn, active, database_id, &info, true, start_ts)?;
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
            let write = update_version_and_table_info(txn, active, database_id, &info, true, start_ts)?;
            Ok(action_step(write))
        }
        SchemaState::WRITE_REORGANIZATION => plan_write_reorganization(
            txn,
            active,
            info,
            indexes,
            outcome,
            start_ts,
            is_pk,
        ),
        state => Err(DdlPlanError::from(
            tidb_dbterror::ERR_INVALID_DDL_STATE.gen_with_stack_by_args(&[
                FormatArg::from("index"),
                FormatArg::new(state.to_string(), state.0.to_string(), "model.SchemaState"),
            ]),
        )),
    }
}

/// Go `onCreateColumnarIndex`: one step of an `ActionAddColumnarIndex` job.
/// The columnar replica builds the index; the job walks the schema states and
/// waits on the replica's progress.
pub(super) fn on_create_columnar_index(
    txn: &mut JobTxn<'_>,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    // Handle the rolling back job.
    if active.job.is_rollingback() {
        return plan_rollback_add_index(txn, active, start_ts);
    }
    // Handle normal job.
    let stored = checked_job_table(txn, active, true)?;
    let stored = stored.read();
    let database_id = active.job.schema_id;
    check_table_type_for_columnar_index(&stored)?;
    let args = tidb_model::get_modify_index_args(&mut active.job)
        .map_err(|error| cancel_ddl_job(active, DdlPlanError::Encode(error.to_string())))?
        .ok_or_else(|| DdlPlanError::Encode("ADD COLUMNAR INDEX job has nil args".to_owned()))?;
    let arg = args
        .read()
        .index_args
        .iter_deref()
        .next()
        .map(|arg| arg.read().clone())
        .ok_or_else(|| DdlPlanError::Encode("ADD COLUMNAR INDEX job has no index argument".to_owned()))?;
    let mut info = stored.clone_like_go();
    let index = match check_and_build_index_info(&mut info, &arg, false) {
        Ok(index) => index,
        Err(error) => return Err(cancel_ddl_job(active, error)),
    };
    let original_state = index.read().state;
    match original_state {
        SchemaState::NONE | SchemaState::DELETE_ONLY | SchemaState::WRITE_ONLY => {
            // none -> delete only -> write only -> reorganization
            let next = match original_state {
                SchemaState::NONE => SchemaState::DELETE_ONLY,
                SchemaState::DELETE_ONLY => SchemaState::WRITE_ONLY,
                _ => SchemaState::WRITE_REORGANIZATION,
            };
            index.write().state = next;
            if next == SchemaState::WRITE_REORGANIZATION {
                // Initialize SnapshotVer to 0 for later reorganization check.
                active.job.snapshot_version = 0;
            }
            active.job.schema_state = next;
            let write = update_version_and_table_info(txn, active, database_id, &info, true, start_ts)?;
            Ok(action_step(write))
        }
        SchemaState::WRITE_REORGANIZATION => {
            // reorganization -> public
            let indexes = [index];
            if active.job.is_cancelling() {
                let error = DdlPlanError::from(
                    tidb_dbterror::ERR_CANCELLED_DDL_JOB
                        .generate(tidb_errno::errname::ErrCancelledDDLJob.raw),
                );
                return columnar_index_rollback_step(txn, active, &mut info, &indexes, error, start_ts);
            }
            // Send sync schema notification to TiFlash.
            if active.job.snapshot_version == 0 {
                tidb_domain_infosync::tiflash_manager::sync_tiflash_table_schema(info.id)
                    .map_err(DdlPlanError::Encode)?;
                // Go `getValidCurrentVersion`; the step's start timestamp
                // is a version the store has reached.
                active.job.snapshot_version = start_ts;
                return Ok(action_step(super::job_only_write(active.job.id, Vec::new())));
            }
            // Check the progress of the TiFlash backfill index.
            let index_id = indexes[0].read().id;
            match check_columnar_index_process(txn, &mut active.job, &info, index_id) {
                Ok(true) => {}
                // Go `ErrWaitReorgTimeout`: the job runs this state again.
                Ok(false) => return Ok(action_step(super::job_only_write(active.job.id, Vec::new()))),
                Err(error) => {
                    if !crate::real_tikv_ddl::is_retryable_job_error(
                        &error,
                        active.job.error_count,
                        tidb_vardef::ddl_error_count_limit(),
                    ) {
                        return columnar_index_rollback_step(txn, active, &mut info, &indexes, error, start_ts);
                    }
                    return Err(error);
                }
            }
            indexes[0].write().state = SchemaState::PUBLIC;
            let write = update_version_and_table_info(txn, active, database_id, &info, true, start_ts)?;
            let finished_args = GoShared::new(tidb_model::ModifyIndexArgs {
                index_args: vec![tidb_model::IndexArg {
                    index_id,
                    ..tidb_model::IndexArg::default()
                }]
                .into(),
                partition_ids: partition_ids(&info).into(),
                op_type: tidb_model::IndexOp::ADD_INDEX,
            });
            active.job.fill_finished_args(Some(finished_args));
            // Finish this job.
            active.job.finish_table_job(
                JobState::DONE,
                SchemaState::PUBLIC,
                write.schema_version,
                Some(GoShared::new(info.clone_like_go())),
            );
            Ok(PersistedDdlActionStep {
                write,
                update_raw_args: true,
                run_error: None,
                reorg: None,
            })
        }
        state => Err(DdlPlanError::from(
            tidb_dbterror::ERR_INVALID_DDL_STATE.gen_with_stack_by_args(&[
                FormatArg::from("index"),
                FormatArg::new(state.to_string(), state.0.to_string(), "model.SchemaState"),
            ]),
        )),
    }
}

/// Go `convertAddIdxJob2RollbackJob` as a columnar index job's step.
fn columnar_index_rollback_step(
    txn: &mut JobTxn<'_>,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    info: &mut TableInfo,
    indexes: &[GoShared<IndexInfo>],
    error: DdlPlanError,
    start_ts: u64,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let (write, error) = convert_add_index_to_rollback(txn, active, info, indexes, error, start_ts)?;
    Ok(PersistedDdlActionStep {
        write,
        update_raw_args: true,
        run_error: Some(error),
        reorg: None,
    })
}

/// Go `checkColumnarIndexProcess`: polls the replica's build for one
/// 500 ms tick. `Ok(false)` is Go's `ErrWaitReorgTimeout`, the tick spent
/// with the build unfinished; the job's `RowCount` follows the rows indexed.
fn check_columnar_index_process(
    txn: &JobTxn<'_>,
    job: &mut Job,
    table: &TableInfo,
    index_id: i64,
) -> Result<bool, DdlPlanError> {
    let wait_timeout = std::time::Duration::from_millis(500);
    let started = std::time::Instant::now();
    while started.elapsed() < wait_timeout {
        let (is_done, _, added_index_cnt) = check_columnar_index_process_once(txn, table, index_id)?;
        job.row_count = added_index_cnt;
        if is_done {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Go `checkColumnarIndexProcessFromTiKV` checks the backfill process of a
/// columnar index from TiKV.
pub(crate) fn check_columnar_index_process_from_tikv(table: &TableInfo, index_id: i64) -> Result<bool, DdlPlanError> {
    use tidb_domain_infosync::tiflash_manager;
    let Some(index) = table.find_index_by_id(index_id) else {
        return Err(DdlPlanError::Encode(format!("could not find indexInfo by {index_id}")));
    };
    let columnar_index_type = index.read().get_columnar_index_type();
    if columnar_index_type == tidb_model::ColumnarIndexType::NA {
        return Err(DdlPlanError::from(
            tidb_dbterror::ERR_UNSUPPORTED_ADD_COLUMNAR_INDEX
                .gen_with_stack_by_args(&["Columnar does not support index types.".into()]),
        ));
    }
    let tikv_stats = tiflash_manager::get_tiflash_stores_stat().map_err(DdlPlanError::Encode)?;
    let tikv_stores: Vec<_> = tikv_stats
        .stores
        .into_iter()
        .filter(|store| !tiflash_manager::is_tiflash_http_resp(&store.store))
        .collect();
    let progress =
        tiflash_manager::calculate_columnar_index_progress(table.id, index_id, columnar_index_type, &tikv_stores)
            .map_err(DdlPlanError::Encode)?;
    Ok(progress >= 1.0)
}

/// Go `checkColumnarIndexProcessOnce`: whether the build is done, and the
/// rows not yet and already indexed.
fn check_columnar_index_process_once(
    txn: &JobTxn<'_>,
    table: &TableInfo,
    index_id: i64,
) -> Result<(bool, i64, i64), DdlPlanError> {
    let table_id = table.id;
    #[cfg(feature = "failpoints")]
    if let Some(val) = tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/MockCheckColumnarIndexProcess").map(|val| val.int()) {
        return match val {
            val if val < 0 => Err(DdlPlanError::from(
                tidb_dbterror::ERR_TI_FLASH_BACKFILL_INDEX.fast_gen_by_args(&["mock a check error".into()]),
            )),
            0 => Ok((false, 0, 0)),
            val => Ok((true, 0, val)),
        };
    }

    let cse = &tidb_config::config_tree::config::get_global_config().cse;
    // If columnar store is enabled, Go checks the columnar index process
    // from TiKV (`CalculateColumnarIndexProgress`).
    let done = !cse.is_columnar_store_enabled() || check_columnar_index_process_from_tikv(table, index_id)?;
    // If TiFlash is not enabled, we return the result directly.
    if !cse.is_tiflash_enabled() {
        return Ok((done, 0, 0));
    }

    // TODO: Support partition table
    let sql = format!(
        "select rows_stable_not_indexed, rows_stable_indexed, error_message from information_schema.tiflash_indexes where table_id = {table_id} and index_id = {index_id};"
    );
    let rows = match txn.execute(&sql, "add_vector_index_check_result", &[]) {
        Ok(rows) if !rows.is_empty() => rows,
        Err(error) => return Err(DdlPlanError::from_mysql_error(error)),
        Ok(_) => return Ok((false, 0, 0)),
    };
    // Get and process info from multiple TiFlash nodes.
    let int = |datum: &tidb_datatype::Datum| match datum {
        tidb_datatype::Datum::Int(value) => *value,
        tidb_datatype::Datum::UInt(value) => *value as i64,
        _ => 0,
    };
    let mut not_added_index_cnt = 0;
    let mut added_index_cnt = 0;
    for row in &rows {
        not_added_index_cnt += int(&row[0]);
        added_index_cnt += int(&row[1]);
        let err_msg = String::from_utf8_lossy(row[2].go_bytes()).into_owned();
        if !err_msg.is_empty() {
            return Err(DdlPlanError::from(
                tidb_dbterror::ERR_TI_FLASH_BACKFILL_INDEX.fast_gen_by_args(&[err_msg.into()]),
            ));
        }
    }
    if not_added_index_cnt != 0 {
        return Ok((false, 0, 0));
    }
    Ok((true, not_added_index_cnt, added_index_cnt))
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
fn plan_write_reorganization(
    txn: &mut JobTxn<'_>,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
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
            txn,
            active,
            &info,
            &indexes,
            outcome,
            start_ts,
        )? {
            CreateIndexReorg::Step(step) => return Ok(step),
            CreateIndexReorg::Failed(error) => {
                // Go `runReorgJobAndHandleErr` turns a non-retryable failure
                // into a rollback.
                let (write, error) =
                    convert_add_index_to_rollback(txn, active, &mut info, &indexes, error, start_ts)?;
                return Ok(PersistedDdlActionStep {
                    write,
                    update_raw_args: true,
                    run_error: Some(error),
                    reorg: None,
                });
            }
            CreateIndexReorg::Done => {}
        }
        // For multi-schema change, analyze is done by parent job.
        if active.job.multi_schema_info.is_none() && super::analyze::check_need_analyze(&active.job, &info) {
            if let Some(meta) = &active.job.reorg_meta {
                meta.write().analyze_state = tidb_model::reorg::analyze_state::RUNNING;
            }
        } else {
            if let Some(meta) = &active.job.reorg_meta {
                meta.write().analyze_state = tidb_model::reorg::analyze_state::SKIPPED;
            }
            check_and_mark_non_revertible(&mut active.job);
        }
        let write = update_version_and_table_info(txn, active, database_id, &info, true, start_ts)?;
        return Ok(action_step(write));
    }
    if analyze_state == tidb_model::reorg::analyze_state::RUNNING {
        super::analyze::start_analyze_and_wait(txn, &active.job, &info);
        return Ok(action_step(super::job_only_write(active.job.id, Vec::new())));
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
    let mut write = update_version_and_table_info(txn, active, database_id, &info, true, start_ts)?;
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
        append_schema_change_mutations(txn, active.job.id, &[(
                notifier_sub_job_id(&active.job),
                SchemaChangeEvent::add_indexes(info.clone_like_go(), added, analyzed),
            )])?;
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
pub(super) fn do_reorg_work_for_create_index(
    txn: &mut JobTxn<'_>,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    info: &TableInfo,
    indexes: &[GoShared<IndexInfo>],
    outcome: Option<&ReorgOutcome>,
    start_ts: u64,
) -> Result<CreateIndexReorg, DdlPlanError> {
    let database_id = active.job.schema_id;
    let reorg_type = pick_backfill_type(&active.job)?;
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
        tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/ddl/beforeBackfillMerge", &mut ());
        // Go: ReadyToMerge -> Merging, with a fresh txn for the merge.
        for index in indexes {
            index.write().backfill_state = tidb_model::BackfillState::MERGING;
        }
        active.job.snapshot_version = 0;
        let write = update_version_and_table_info(txn, active, database_id, info, true, start_ts)?;
        return Ok(CreateIndexReorg::Step(action_step(write)));
    }
    let request =
        |active: &tidb_ddl_systable::ddl_job_table::ActiveDdlJob, stage: ReorgStageRequest| AddIndexReorg {
            job_id: active.job.id,
            query: active.job.query.clone(),
            stage,
            table: Box::new(info.clone_like_go()),
            index_ids: indexes.iter().map(|index| index.read().id).collect(),
            merging,
            physical_ids: if merging { merge_physical_ids(info, indexes) } else { physical_ids(info) },
            batch_size: job_batch_size(&active.job),
            concurrency: job_concurrency(&active.job),
            session_vars: ReorgSessionVars::of_job(&active.job),
            row_count: active.job.row_count,
            column: None,
            sub_job: reorg_sub_job(&active.job),
            clean_up_global_index: false,
            partition_phase: None,
            schema_id: active.job.schema_id,
        };
    let finished = match outcome.map(|outcome| &outcome.result) {
        // Go `getReorgInfo`: the first visit persists the txn.
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
            let handle = get_ddl_reorg_handle(txn, active)?;
            let reorg = request(active, ReorgStageRequest::Run(handle));
            return Ok(CreateIndexReorg::Step(reorg_step(&active.job, reorg)));
        }
        Some(ReorgResult::Progress(rows)) => {
            // Go `onCreateIndex` on `ErrWaitReorgTimeout`: the job keeps
            // its state and records the count so far.
            active.job.row_count = *rows;
            if backfill_state == tidb_model::BackfillState::RUNNING {
                after_run_ingest_reorg_job(&active.job, reorg_type, false);
            }
            return Ok(CreateIndexReorg::Step(action_step(job_only_write(
                active.job.id,
                Vec::new(),
            ))));
        }
        Some(ReorgResult::AutoPaused{task_id,error}) => {
            crate::index::auto_pause_add_index_job_on_kv_disk_full(&mut active.job,*task_id,error.as_ref().map(|e|e.as_ref() as &(dyn std::error::Error+'static)));
            return Ok(CreateIndexReorg::Step(action_step(job_only_write(active.job.id,Vec::new()))));
        }
        Some(ReorgResult::Checked(_) | ReorgResult::Built(_)) => {
            return Err(DdlPlanError::Encode(
                "an index reorganization got a column check's outcome".to_owned(),
            ))
        }
        Some(ReorgResult::Skipped) => {
            // Go `doReorgWorkForCreateIndex`: the merging state skips an
            // empty temporary index, the running state an empty table; the
            // txn backfill's skip has no failpoint.
            if merging {
                tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/ddl/afterCheckTempIndexReorgCanSkip", &mut ());
            } else if reorg_type.need_merge_process() {
                tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/ddl/afterCheckTableReorgCanSkip", &mut ());
            }
            active.job.row_count
        }
        Some(ReorgResult::Done(rows)) => {
            // Go `runReorgJobAndHandleErr`, done.
            tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/ddl/afterRunReorgJobAndHandleErr", &mut ());
            *rows
        }
        Some(ReorgResult::Failed {
            code,
            message,
            rows,
            identity,
        }) => {
            // Go `runReorgJob` records the count first.
            active.job.row_count = *rows;
            return run_reorg_job_error(active, crate::cluster_ddl::reorg_failure(*code, message, *identity));
        }
        Some(ReorgResult::Error(error)) => return run_reorg_job_error(active, error.clone()),
    };
    active.job.row_count = finished;
    if active.job.reorg_meta.as_ref().is_some_and(|m|m.read().is_dist_reorg) { active.job.clear_resume_reason(); }
    if reorg_type.need_merge_process() {
        if backfill_state == tidb_model::BackfillState::RUNNING {
            after_run_ingest_reorg_job(&active.job, reorg_type, true);
            for index in indexes {
                index.write().backfill_state = tidb_model::BackfillState::READY_TO_MERGE;
            }
            let write = update_version_and_table_info(txn, active, database_id, info, true, start_ts);
            tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/ddl/afterBackfillStateRunningDone", &mut active.job);
            return Ok(CreateIndexReorg::Step(action_step(write?)));
        }
        // The merge finished: no more double writes.
        for index in indexes {
            index.write().backfill_state = tidb_model::BackfillState::INAPPLICABLE;
        }
    }
    Ok(CreateIndexReorg::Done)
}

/// Go `runReorgJobAndHandleErr` on the reorganization's error: one that is
/// not retryable (or past `tidb_ddl_error_count_limit`) rolls the job back;
/// any other is the step's error, which the job counts and retries.
fn run_reorg_job_error(
    active: &tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    error: DdlPlanError,
) -> Result<CreateIndexReorg, DdlPlanError> {
    if !crate::real_tikv_ddl::is_retryable_job_error(&error, active.job.error_count, tidb_vardef::ddl_error_count_limit()) {
        tidb_ddl_logutil::ddl_logger().warn(
            "run add index job failed, convert job to rollback",
            &[
                tidb_log::Field::new("job", tidb_log::Value::Str(active.job.to_string())),
                tidb_log::Field::new("error", tidb_log::Value::Str(error.to_string())),
            ],
        );
        return Ok(CreateIndexReorg::Failed(error));
    }
    Err(error)
}

/// Go `runIngestReorgJob` (the local ingest, not DXF) ends a run without
/// error with `InjectCall("afterRunIngestReorgJob", job, done)`.
fn after_run_ingest_reorg_job(job: &Job, reorg_type: tidb_model::ReorgType, done: bool) {
    if reorg_type == tidb_model::ReorgType::INGEST
        && !job.reorg_meta.as_ref().is_some_and(|m| m.read().is_dist_reorg)
    {
        tidb_failpoint::inject_call(
            "github.com/pingcap/tidb/pkg/ddl/afterRunIngestReorgJob",
            &mut (job.clone(), done),
        );
    }
}

/// Go `convertAddIdxJob2RollbackJob`: the indexes back to delete-only, the
/// job rolling back with `error`.
fn convert_add_index_to_rollback(
    txn: &mut JobTxn<'_>,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    info: &mut TableInfo,
    indexes: &[GoShared<IndexInfo>],
    error: DdlPlanError,
    start_ts: u64,
) -> Result<(DdlWrite, DdlPlanError), DdlPlanError> {
    let original_state = indexes[0].read().state;
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
                        .del_flag(tidb_datatype::FieldTypeFlags::PREVENT_NULL_INSERT);
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
    // Go `updateVersionAndTableInfo(..., originalState != StateDeleteOnly)`,
    // then the job rolls back.
    let database_id = active.job.schema_id;
    let write = update_version_and_table_info(
        txn,
        active,
        database_id,
        info,
        original_state != SchemaState::DELETE_ONLY,
        start_ts,
    )?;
    active.job.state = JobState::ROLLINGBACK;
    // TODO(tangenta): get duplicate column and match index.
    Ok((write, complete_err(error, &indexes[0].read())))
}

/// Go `completeErr`: an expression index's invalid-JSON CAST error names
/// the index.
fn complete_err(error: DdlPlanError, index: &IndexInfo) -> DdlPlanError {
    let proto = &tidb_expr::expression_errors::ERR_INVALID_JSON_FOR_FUNC_INDEX;
    if error.identity() == Some(proto.identity()) {
        return DdlPlanError::Source(proto.gen_with_stack_by_args(&[index.name.original().into()]));
    }
    error
}

/// Go `onDropIndex` for a rolled-back ADD INDEX: delete-only ->
/// delete-reorganization -> gone, then ROLLBACK DONE.
fn plan_rollback_add_index(
    txn: &mut JobTxn<'_>,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let stored = checked_job_table(txn, active, true)?;
    let stored = stored.read();
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
            let write = update_version_and_table_info(txn, active, database_id, &info, true, start_ts)?;
            Ok(action_step(write))
        }
        SchemaState::DELETE_REORGANIZATION => {
            let mut index_ids = Vec::new();
            for index in &indexes {
                let removed = index.read().clone_like_go();
                index_ids.push(removed.id);
                remove_index_and_hidden_columns(&mut info, &removed);
            }
            let write = update_version_and_table_info(txn, active, database_id, &info, true, start_ts)?;
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
            tidb_dbterror::ERR_INVALID_DDL_STATE.gen_with_stack_by_args(&[
                FormatArg::from("index"),
                FormatArg::new(state.to_string(), state.0.to_string(), "model.SchemaState"),
            ]),
        )),
    }
}

/// Go `DropIndexColumnFlag`, `RemoveDependentHiddenColumns` and
/// `removeIndexInfo`. A hidden column another index still uses (a copy of the
/// index a partition reorganization made) stays.
pub(super) fn remove_index_and_hidden_columns(info: &mut TableInfo, index: &IndexInfo) {
    drop_index_column_flag(info, index);
    let hidden: Vec<String> = index
        .columns
        .iter_deref()
        .filter_map(|part| {
            let offset = part.read().offset;
            info.columns.get(offset as usize).and_then(|column| {
                let column = column.read();
                (column.hidden && !is_column_used_by_other_index(info, index.id, offset))
                    .then(|| column.name.lowercase().to_owned())
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

/// Go `isColumnUsedByOtherIndex`: whether an index other than `index_id`
/// uses the column at `offset`, e.g. a copy of the index a partition
/// reorganization made.
fn is_column_used_by_other_index(info: &TableInfo, index_id: i64, offset: i64) -> bool {
    info.indices.iter_deref().any(|other| {
        let other = other.read();
        other.id != index_id && other.columns.iter_deref().any(|part| part.read().offset == offset)
    })
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
            column.write().field_type.del_flag(flag);
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
        // By id: a partition reorganization keeps index copies with the same
        // name.
        if other.id == index.id {
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
pub(super) fn rollingback_add_index(
    txn: &mut JobTxn<'_>,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let _ = start_ts;
    // Go `convertNotReorgAddIdxJob2RollbackJob`: a job that has not
    // published any index metadata is cancelled as it is. A same-named index
    // may belong to an earlier job and must not be rolled back here.
    if active.job.schema_state == SchemaState::NONE {
        return Err(cancelled_ddl_job(active));
    }
    let stored = checked_job_table(txn, active, true)?;
    let stored = stored.read();
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
        tidb_dbterror::ERR_CANCELLED_DDL_JOB
            .generate(tidb_errno::errname::ErrCancelledDDLJob.raw),
    );
    let (write, error) = convert_add_index_to_rollback(txn, active, &mut info, &indexes, error, start_ts)?;
    Ok(PersistedDdlActionStep {
        write,
        update_raw_args: true,
        run_error: Some(error),
        reorg: None,
    })
}

/// Go `getDDLReorgHandle`: the job's `mysql.tidb_ddl_reorg` row, selected on
/// the step's session.
pub(super) fn get_ddl_reorg_handle(
    txn: &mut JobTxn<'_>,
    active: &tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
) -> Result<ReorgHandle, DdlPlanError> {
    let sql = format!(
        "select ele_id, ele_type, start_key, end_key, physical_id, reorg_meta from mysql.tidb_ddl_reorg where job_id = {}",
        active.job.id
    );
    let handle = txn
        .execute(&sql, "get_handle", &[])
        .map_err(DdlPlanError::from_mysql_error)?
        .first()
        .map(|row| ReorgHandle {
            element_id: row[0].get_int64(),
            element_type: row[1].go_bytes().to_vec(),
            start_key: row[2].go_bytes().to_vec(),
            end_key: row[3].go_bytes().to_vec(),
            physical_id: row[4].get_int64(),
        });
    handle
        .ok_or_else(|| {
            // Go `meta.ErrDDLReorgElementNotExist`.
            DdlPlanError::Encode(format!("DDL job {} has no reorg handle", active.job.id))
        })
}

#[cfg(test)]
mod next_gen_local_sort_tests {
    use super::*;

    /// Go `TestInitForReorgIndexesRequiresGlobalSort` (NextGen only): with
    /// the ingest environment, a fast reorganization that would sort locally
    /// is refused; without it, without fast reorg, or with cloud storage the
    /// job proceeds.
    #[test]
    fn test_init_for_reorg_indexes_requires_global_sort() {
        if !tidb_config_kerneltype::is_next_gen() {
            return; // Go: "global sort is only enforced on the NextGen kernel".
        }
        use std::sync::atomic::Ordering;
        let lit = &tidb_ddl_ingest::env::LIT_INITIALIZED;
        let old = lit.load(Ordering::Acquire);
        let new_job = |fast: bool, cloud: bool| {
            let mut meta = tidb_model::reorg::DDLReorgMeta::default();
            meta.is_fast_reorg = fast;
            meta.is_dist_reorg = fast;
            meta.use_cloud_storage = cloud;
            let mut job = Job::default();
            job.id = 1;
            job.reorg_meta = Some(GoShared::new(meta));
            job
        };
        // Without the ingest environment the backfill falls back to
        // txn-merge, which does not sort locally.
        lit.store(false, Ordering::Release);
        assert!(check_next_gen_local_sort(&new_job(true, false)).is_ok());
        // Local sort is refused when the ingest environment is available.
        lit.store(true, Ordering::Release);
        let error = check_next_gen_local_sort(&new_job(true, false)).unwrap_err();
        assert!(error.equal(&tidb_dbterror::ERR_NOT_SUPPORTED_YET), "{error}");
        assert!(error.to_string().contains("add index or modify column with local sort"), "{error}");
        // Jobs without fast reorg don't sort locally.
        assert!(check_next_gen_local_sort(&new_job(false, false)).is_ok());
        // Global sort is allowed.
        assert!(check_next_gen_local_sort(&new_job(true, true)).is_ok());
        lit.store(old, Ordering::Release);
    }
}

#[cfg(test)]
mod rollingback_tests {
    use super::*;

    struct EmptySnapshot;

    impl tidb_infoschema::cluster_catalog::MetaSnapshot for EmptySnapshot {
        fn get(&mut self, _: &[u8]) -> Result<Option<Vec<u8>>, tidb_infoschema::cluster_catalog::ClusterCatalogError> {
            Ok(None)
        }

        fn scan_prefix(
            &mut self,
            _: &[u8],
        ) -> Result<tidb_infoschema::cluster_catalog::MetaPairs, tidb_infoschema::cluster_catalog::ClusterCatalogError> {
            Ok(Vec::new())
        }
    }

    /// Go `TestCancelQueuedAddIndexKeepsExistingIndex` (8523c574c48), its
    /// queued half: a cancelled add-index job that has published no index
    /// metadata is cancelled as it is, and a same-named public index from an
    /// earlier job stays.
    #[test]
    fn test_cancel_queued_add_index_keeps_existing_index() {
        for version in [tidb_model::JobVersion::V1, tidb_model::JobVersion::V2] {
            let mut store = EmptySnapshot;
            let mut txn = JobTxn::new(&mut store, 1, crate::job_txn::unused_step_session(crate::StartMode::Normal));
            let mut job = Job::default();
            job.id = 10;
            job.version = version;
            job.type_ = ActionType::ACTION_ADD_INDEX;
            job.schema_id = 1;
            job.table_id = 2;
            job.state = tidb_model::JobState::CANCELLING;
            let index = IndexInfo {
                id: 3,
                name: tidb_ast::CiString::new("idx"),
                state: SchemaState::PUBLIC,
                ..IndexInfo::default()
            };
            job.fill_args(Some(GoShared::new(tidb_model::ModifyIndexArgs {
                index_args: vec![tidb_model::IndexArg {
                    index_name: index.name.clone(),
                    ..tidb_model::IndexArg::default()
                }]
                .into(),
                ..tidb_model::ModifyIndexArgs::default()
            })));
            job.encode(true).unwrap();
            let db = tidb_model::db::DBInfo { id: 1, name: tidb_ast::CiString::new("test"), ..Default::default() };
            txn.meta().create_database(&db).unwrap();
            let table = TableInfo {
                id: 2,
                name: tidb_ast::CiString::new("t"),
                state: SchemaState::PUBLIC,
                indices: vec![index].into(),
                ..TableInfo::default()
            };
            txn.meta().create_table_or_view(1, &table).unwrap();
            let mut active = tidb_ddl_systable::ddl_job_table::ActiveDdlJob {
                job,
                reorg: true,
                schema_ids: "1".to_owned(),
                table_ids: "2".to_owned(),
                type_: i64::from(ActionType::ACTION_ADD_INDEX.0),
                processing: true,
            };
            let Err(error) = rollingback_add_index(&mut txn, &mut active, 1) else {
                panic!("{version:?}: the queued job was rolled back instead of cancelled");
            };
            assert!(error.to_string().contains("Cancelled DDL job"), "{version:?}: {error}");
            let updated = txn.meta().table(1, 2).unwrap().unwrap();
            assert_eq!(updated.indices.len(), 1);
            let updated_index = updated.indices.iter_deref().next().unwrap();
            assert_eq!(updated_index.read().id, 3);
            // A queued job has not created an index. The public index belongs
            // to an earlier job and must not become a rollback target.
            assert_eq!(updated_index.read().state, SchemaState::PUBLIC, "{version:?}");
            assert_eq!(active.job.state, tidb_model::JobState::CANCELLED, "{version:?}");
            assert_eq!(active.job.schema_state, SchemaState::NONE, "{version:?}");
        }
    }
}
