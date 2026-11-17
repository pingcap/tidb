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

//! Go master `pkg/ddl/materialized_view.go`'s executor half: the jobs the
//! materialized view statements submit.

use tidb_ast::CreateMaterializedViewLogStmt;
use tidb_ddl_build::session_context::DdlSessionContext;
use tidb_goruntime::go_runtime::GoSharedSlice;
use tidb_infoschema::SessionExtendedInfoSchema;
use tidb_model::{ActionType, GoShared, JobArgsValue};
use tidb_terror::MysqlError;

/// Go `checkMaterializedViewEnabled`.
///
/// # Errors
///
/// `tidb_mview_enable` is off.
pub(crate) fn check_materialized_view_enabled(context: &dyn DdlSessionContext) -> Result<(), MysqlError> {
    if !context.enable_mview() {
        return Err(MysqlError::from_terror(&tidb_dbterror::ERR_GENERAL_UNSUPPORTED_DDL.gen_with_stack(
            "Materialized View is disabled, please set `tidb_mview_enable` to `ON` to enable it",
        )));
    }
    Ok(())
}

/// The expression context Go's DDL builds a schedule expression in: no
/// columns, the session's time zone.
struct SessionExprContext<'a>(&'a dyn DdlSessionContext);

impl tidb_expr::rewriter::ColumnResolver for SessionExprContext<'_> {
    fn resolve(&self, _path: &[String]) -> Option<(usize, tidb_datatype::FieldType, i64)> {
        None
    }
    fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
        self.0.session_zone()
    }
}

/// Go `executor.CreateMaterializedViewLog` up to `DoDDLJobWrapper`: the job,
/// and the schema it creates the log in.
///
/// # Errors
///
/// Go's refusals, in Go's order.
pub(crate) fn create_materialized_view_log_job(
    catalog: &SessionExtendedInfoSchema,
    stmt: &CreateMaterializedViewLogStmt,
    current_db: &str,
    context: &dyn DdlSessionContext,
) -> Result<(crate::JobWrapper, String), MysqlError> {
    check_materialized_view_enabled(context)?;
    let (schema_name, table_name) = match stmt.table.as_slice() {
        [schema, table] => (schema.clone(), table.clone()),
        [table] => {
            if current_db.is_empty() {
                return Err(MysqlError::from_terror(&tidb_plannererrors::ERR_NO_DB.gen_with_stack_by_args(&[])));
            }
            (current_db.to_owned(), table.clone())
        }
        _ => return Err(MysqlError::unknown("invalid materialized view log table name".to_owned())),
    };
    let Some(schema) = catalog.schema_by_name(&schema_name) else {
        return Err(MysqlError::from_terror(
            &tidb_infoschema::infoschema_errors::ERR_DATABASE_NOT_EXISTS.gen_with_stack_by_args(&[schema_name.into()]),
        ));
    };
    let schema = schema.read().clone();
    let Some((_, base)) = catalog.table_info_by_name(&schema_name, &table_name) else {
        return Err(MysqlError::from_terror(
            &tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                .gen_with_stack_by_args(&[schema_name.clone().into(), table_name.clone().into()]),
        ));
    };
    let base = base.read().clone();
    if !crate::cluster_ddl::materialized_view::is_valid_materialized_view_log_base_table(
        &tidb_hack::go_to_lower(&schema_name),
        &base,
    ) {
        return Err(MysqlError::from_terror(&tidb_dbterror::ERR_WRONG_OBJECT.gen_with_stack_by_args(&[
            schema_name.clone().into(),
            table_name.clone().into(),
            "BASE TABLE".into(),
        ])));
    }
    if base.get_partition_info().is_some() {
        return Err(MysqlError::from_terror(&tidb_dbterror::ERR_GENERAL_UNSUPPORTED_DDL.gen_with_stack_by_args(&[
            "CREATE MATERIALIZED VIEW LOG on partition table".into(),
        ])));
    }
    let mlog_name = tidb_model::table::materialized_view_log_table_name(&base.name);
    if catalog.table_info_by_name(&schema_name, mlog_name.original()).is_some() {
        return Err(MysqlError::from_terror(&tidb_infoschema::infoschema_errors::ERR_TABLE_EXISTS.gen_with_stack_by_args(
            &[format!("{schema_name}.{}", mlog_name.original()).into()],
        )));
    }
    let mlog = tidb_ddl_build::materialized_view::build_materialized_view_log_table_info(
        &context.metabuild_context(),
        &SessionExprContext(context),
        &schema_name,
        &schema.charset,
        &schema.collate,
        &base,
        stmt,
        context.enable_check_constraint(),
    )
    .map_err(|error| error.to_mysql_error())?;

    let mut job = crate::cluster_ddl::session_job(context, ActionType::ACTION_CREATE_MATERIALIZED_VIEW_LOG);
    job.schema_id = schema.id;
    job.schema_name = schema.name.lowercase().to_owned().into();
    job.table_name = mlog.name.lowercase().to_owned().into();
    job.involving_schema_info = GoSharedSlice::from_vec(vec![
        tidb_model::InvolvingSchemaInfo {
            database: schema.name.lowercase().to_owned().into(),
            table: mlog.name.lowercase().to_owned().into(),
            ..tidb_model::InvolvingSchemaInfo::default()
        },
        tidb_model::InvolvingSchemaInfo {
            database: schema.name.lowercase().to_owned().into(),
            table: base.name.lowercase().to_owned().into(),
            ..tidb_model::InvolvingSchemaInfo::default()
        },
    ]);
    job.session_vars = Some(GoShared::new(std::collections::BTreeMap::from([(
        tidb_vardef::tidb_vars::TIDB_SCATTER_REGION.to_owned(),
        context.ddl_reorg_vars().scatter_region.clone().into(),
    )])));
    let args = tidb_model::CreateMaterializedViewLogArgs { table_info: Some(GoShared::new(mlog)) };
    Ok((
        crate::JobWrapper {
            job,
            args: JobArgsValue::CreateMaterializedViewLog(Some(GoShared::new(args))),
            id_allocated: false,
        },
        schema_name,
    ))
}

/// Go master `getMaterializedViewSchema`: the statement's schema, the
/// current database for an unqualified name.
fn materialized_view_schema(
    catalog: &SessionExtendedInfoSchema,
    path: &[String],
    current_db: &str,
) -> Result<(String, String), MysqlError> {
    let (schema, table) = match path {
        [schema, table] => (schema.clone(), table.clone()),
        [table] => {
            if current_db.is_empty() {
                return Err(MysqlError::from_terror(&tidb_plannererrors::ERR_NO_DB.gen_with_stack_by_args(&[])));
            }
            (current_db.to_owned(), table.clone())
        }
        _ => return Err(MysqlError::unknown("invalid materialized view table name".to_owned())),
    };
    if catalog.schema_by_name(&schema).is_none() {
        return Err(MysqlError::from_terror(
            &tidb_infoschema::infoschema_errors::ERR_DATABASE_NOT_EXISTS.gen_with_stack_by_args(&[schema.into()]),
        ));
    }
    Ok((schema, table))
}

/// What Go master's `executor.DropMaterializedViewLog` does before
/// `dropTableObject`: the log table to drop, or `None` for a missing log
/// under IF EXISTS, after its note.
pub(crate) fn drop_materialized_view_log_target(
    catalog: &SessionExtendedInfoSchema,
    stmt: &tidb_ast::DropMaterializedViewLogStmt,
    current_db: &str,
    context: &dyn DdlSessionContext,
) -> Result<Option<(String, String)>, MysqlError> {
    let (schema, table) = materialized_view_schema(catalog, &stmt.table, current_db)?;
    let Some((_, base)) = catalog.table_info_by_name(&schema, &table) else {
        return Err(MysqlError::from_terror(
            &tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                .gen_with_stack_by_args(&[schema.clone().into(), table.clone().into()]),
        ));
    };
    let base = base.read().clone();
    if !crate::cluster_ddl::materialized_view::is_valid_materialized_view_log_base_table(
        &tidb_hack::go_to_lower(&schema),
        &base,
    ) {
        return Err(MysqlError::from_terror(&tidb_dbterror::ERR_WRONG_OBJECT.gen_with_stack_by_args(&[
            schema.clone().into(),
            table.clone().into(),
            "BASE TABLE".into(),
        ])));
    }
    let mlog_name = tidb_model::table::materialized_view_log_table_name(&base.name).original().to_owned();
    let Some((_, mlog)) = catalog.table_info_by_name(&schema, &mlog_name) else {
        if stmt.if_exists {
            let note = tidb_infoschema::infoschema_errors::ERR_TABLE_DROP_EXISTS
                .gen_with_stack_by_args(&[format!("{schema}.{table}").into()]);
            let note = MysqlError::from_terror(&note);
            context.append_note_parts(note.code, &note.message);
            return Ok(None);
        }
        return Err(MysqlError::from_terror(
            &tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                .gen_with_stack_by_args(&[schema.clone().into(), mlog_name.into()]),
        ));
    };
    let mlog = mlog.read().clone();
    let log = mlog.materialized_view_log.as_ref().map(|log| log.read().clone());
    if log.as_ref().is_none_or(|log| log.base_table_id != base.id) {
        return Err(MysqlError::from_terror(&tidb_dbterror::ERR_WRONG_OBJECT.gen_with_stack_by_args(&[
            schema.clone().into(),
            mlog_name.into(),
            "MATERIALIZED VIEW LOG".into(),
        ])));
    }
    if log.is_some_and(|log| !log.dependent_mview_ids.is_empty()) {
        return Err(MysqlError::unknown(format!(
            "cannot drop materialized view log on {schema}.{}: dependent materialized views exist",
            table
        )));
    }
    Ok(Some((schema, mlog_name)))
}

/// One planned action of Go master's `executor.AlterMaterializedViewLog`.
pub(crate) enum AlterMaterializedViewLogStep {
    /// `alterMaterializedViewLogPurge`: the job, and the schedule the
    /// executor stores after it.
    Purge { job: crate::JobWrapper, start_with: String, next: String, sql_mode: i64, mlog_id: i64 },
    /// `addMaterializedViewLogColumns`: the base columns, added to the log
    /// `schema`.`mlog` after `after` (or first).
    AddColumns { schema: String, mlog: String, base_columns: Vec<tidb_model::ColumnInfo>, after: Option<String> },
}

/// Go master `executor.AlterMaterializedViewLog`'s checks and the jobs of its
/// actions, in statement order.
pub(crate) fn alter_materialized_view_log_steps(
    catalog: &SessionExtendedInfoSchema,
    stmt: &tidb_ast::AlterMaterializedViewLogStmt,
    current_db: &str,
    context: &dyn DdlSessionContext,
) -> Result<Vec<AlterMaterializedViewLogStep>, MysqlError> {
    let (schema_name, table) = materialized_view_schema(catalog, &stmt.table, current_db)?;
    let schema = catalog.schema_by_name(&schema_name).expect("checked above").read().clone();
    let Some((_, base)) = catalog.table_info_by_name(&schema_name, &table) else {
        return Err(MysqlError::from_terror(
            &tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                .gen_with_stack_by_args(&[schema_name.clone().into(), table.clone().into()]),
        ));
    };
    let base = base.read().clone();
    if base.is_view() || base.is_sequence() || base.temp_table_type != tidb_model::TempTableType::NONE {
        return Err(MysqlError::from_terror(&tidb_dbterror::ERR_WRONG_OBJECT.gen_with_stack_by_args(&[
            schema_name.clone().into(),
            table.clone().into(),
            "BASE TABLE".into(),
        ])));
    }
    let mlog_name = tidb_model::table::materialized_view_log_table_name(&base.name).original().to_owned();
    let Some((_, mlog)) = catalog.table_info_by_name(&schema_name, &mlog_name) else {
        return Err(MysqlError::from_terror(
            &tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                .gen_with_stack_by_args(&[schema_name.clone().into(), mlog_name.into()]),
        ));
    };
    let mlog = mlog.read().clone();
    if mlog.materialized_view_log.as_ref().is_none_or(|log| log.read().base_table_id != base.id) {
        return Err(MysqlError::from_terror(&tidb_dbterror::ERR_WRONG_OBJECT.gen_with_stack_by_args(&[
            schema_name.clone().into(),
            mlog_name.into(),
            "MATERIALIZED VIEW LOG".into(),
        ])));
    }
    let add_column_actions = stmt
        .actions
        .iter()
        .filter(|action| matches!(action, tidb_ast::AlterMaterializedViewLogAction::AddColumn(_)))
        .count();
    if add_column_actions > 1 {
        return Err(MysqlError::from_terror(&tidb_dbterror::ERR_GENERAL_UNSUPPORTED_DDL.gen_with_stack_by_args(&[
            "ALTER MATERIALIZED VIEW LOG with multiple ADD COLUMN actions".into(),
        ])));
    }
    let base_columns: Vec<tidb_model::ColumnInfo> = base
        .columns
        .iter_deref()
        .map(|column| column.read().clone())
        .filter(|column| column.state == tidb_model::SchemaState::PUBLIC)
        .collect();
    let mut mlog_columns: Vec<String> =
        mlog.columns.iter_deref().map(|column| column.read().name.lowercase().to_owned()).collect();
    let expr_ctx = SessionExprContext(context);
    // Go's first pass validates every action before any job runs.
    for action in &stmt.actions {
        match action {
            tidb_ast::AlterMaterializedViewLogAction::Purge(purge) => {
                tidb_ddl_build::materialized_view::build_mlog_purge_meta(&expr_ctx, Some(purge))?;
            }
            tidb_ast::AlterMaterializedViewLogAction::AddColumn(columns) => {
                for column in columns {
                    let lower = tidb_hack::GoToLower::go_to_lower(column);
                    if mlog_columns.contains(&lower) {
                        return Err(MysqlError::from_terror(
                            &tidb_infoschema::infoschema_errors::ERR_COLUMN_EXISTS
                                .gen_with_stack_by_args(&[column.clone().into()]),
                        ));
                    }
                    let Some(base_column) = base_columns.iter().find(|base| base.name.lowercase() == lower) else {
                        return Err(MysqlError::from_terror(
                            &tidb_infoschema::infoschema_errors::ERR_COLUMN_NOT_EXISTS
                                .gen_with_stack_by_args(&[column.clone().into(), table.clone().into()]),
                        ));
                    };
                    tidb_ddl_build::materialized_view::check_materialized_view_log_column_supported_for_op(
                        "ALTER MATERIALIZED VIEW LOG",
                        base_column,
                    )?;
                    mlog_columns.push(lower);
                }
            }
        }
    }
    let mut last_tracked = log_info_last_column(&mlog);
    let mut steps = Vec::new();
    for action in &stmt.actions {
        let purge = match action {
            tidb_ast::AlterMaterializedViewLogAction::Purge(purge) => purge,
            tidb_ast::AlterMaterializedViewLogAction::AddColumn(columns) => {
                let base_columns = columns
                    .iter()
                    .map(|column| {
                        let lower = tidb_hack::GoToLower::go_to_lower(column);
                        base_columns.iter().find(|base| base.name.lowercase() == lower).expect("checked above").clone()
                    })
                    .collect();
                steps.push(AlterMaterializedViewLogStep::AddColumns {
                    schema: schema_name.clone(),
                    mlog: mlog.name.original().to_owned(),
                    base_columns,
                    after: last_tracked.clone(),
                });
                if let Some(last) = columns.last() {
                    last_tracked = Some(last.clone());
                }
                continue;
            }
        };
        let (purge_method, purge_start_with, purge_next) =
            tidb_ddl_build::materialized_view::build_mlog_purge_meta(&expr_ctx, Some(purge))?;
        let update_purge_schedule = purge.start_with.is_some() || purge.next.is_some();
        let sql_mode = context.ddl_sql_mode();
        let mut job = crate::cluster_ddl::session_job(context, ActionType::ACTION_ALTER_MATERIALIZED_VIEW_LOG_PURGE);
        job.schema_id = schema.id;
        job.table_id = mlog.id;
        job.schema_name = schema.name.lowercase().to_owned().into();
        job.table_name = mlog.name.lowercase().to_owned().into();
        let args = tidb_model::AlterMaterializedViewLogPurgeArgs {
            purge_method,
            purge_start_with: purge_start_with.clone(),
            purge_next: purge_next.clone(),
            purge_schedule_sql_mode: sql_mode,
            update_purge_schedule,
        };
        steps.push(AlterMaterializedViewLogStep::Purge {
            job: crate::JobWrapper {
                job,
                args: JobArgsValue::AlterMaterializedViewLogPurge(Some(GoShared::new(args))),
                id_allocated: true,
            },
            start_with: purge_start_with,
            next: purge_next,
            sql_mode,
            mlog_id: mlog.id,
        });
    }
    Ok(steps)
}

/// Go `lastTrackedCol`: the log's last tracked column, if it tracks any.
fn log_info_last_column(mlog: &tidb_model::TableInfo) -> Option<String> {
    let log = mlog.materialized_view_log.as_ref()?.read();
    log.columns.iter().last().map(|column| column.original().to_owned())
}

/// Go `alterMaterializedScheduleInfoUnixSecondsUpdateSpec`.
pub(crate) struct ScheduleInfoUpdateSpec {
    pub operation: &'static str,
    pub info_table: &'static str,
    pub id_column: &'static str,
    pub schedule_column: &'static str,
    pub row_missing_err: &'static str,
    pub table_not_exists_err: &'static str,
}

/// Go `updateMaterializedViewLogPurgeInfoNextUnixSeconds`'s spec.
pub(crate) const MLOG_PURGE_INFO_UPDATE_SPEC: ScheduleInfoUpdateSpec = ScheduleInfoUpdateSpec {
    operation: "alter materialized view log purge",
    info_table: "mysql.tidb_mlog_purge_info",
    id_column: "MLOG_ID",
    schedule_column: "NEXT_PURGE_UNIX_SECONDS",
    row_missing_err: "alter materialized view log purge: purge info row missing in mysql.tidb_mlog_purge_info",
    table_not_exists_err:
        "alter materialized view log purge: required system table mysql.tidb_mlog_purge_info does not exist",
};

/// Go `alterMaterializedScheduleInfoUpdateLockWaitTimeoutSec`.
const SCHEDULE_INFO_UPDATE_LOCK_WAIT_TIMEOUT_SEC: i64 = 10;

/// Go's `error` value as `ErrX.Equal(err)` reads it: the cause's class and
/// code when it has them, else a plain error with its message.
fn cause_mysql_error(error: &(dyn std::error::Error + 'static)) -> MysqlError {
    let mut cause = error;
    while let Some(source) = cause.source() {
        cause = source;
    }
    if let Some(error) = cause.downcast_ref::<tidb_store_driver_error::sql_query_error::SqlQueryError>() {
        return MysqlError::from(error.clone());
    }
    if let Some(error) = cause.downcast_ref::<MysqlError>() {
        return error.clone();
    }
    if let Some(error) = cause.downcast_ref::<tidb_terror::TerrorError>() {
        return MysqlError::from_terror(error);
    }
    MysqlError::unknown(error.to_string())
}

/// Go `isAlterMaterializedScheduleInfoUpdateLockContentionErr`.
fn is_schedule_info_update_lock_contention_err(error: &MysqlError) -> bool {
    error.equal(&tidb_txnkv::tikv_errors::ERR_LOCK_WAIT_TIMEOUT)
        || error.equal(&tidb_exeerrors::ERR_DEADLOCK)
        || error.equal(&tidb_kv::kv_errors::ERR_WRITE_CONFLICT)
}

/// Go `appendAlterMaterializedScheduleInfoUpdateWarning`.
fn schedule_info_update_warning(spec: &ScheduleInfoUpdateSpec) -> tidb_context::SqlWarn {
    tidb_context::SqlWarn::new(
        tidb_context::WarningLevel::Warning,
        tidb_context::WarnErr::Message(format!(
            "{}: metadata updated but failed to update {}.{} within {}s due to row lock contention; please retry later if immediate reschedule is needed",
            spec.operation, spec.info_table, spec.schedule_column, SCHEDULE_INFO_UPDATE_LOCK_WAIT_TIMEOUT_SEC
        )),
    )
}

/// Go `executor.bestEffortUpdateMaterializedScheduleInfoNextUnixSeconds` on
/// a session of the DDL session pool: lock the info row (waiting at most
/// 10 s), then store the next time. Lock contention leaves the row alone
/// and raises a warning; it answers whether the row was updated.
pub(crate) fn best_effort_update_materialized_schedule_info_next_unix_seconds(
    session_pool: &dyn tidb_ddl_build::session_pool::DdlSessionPool,
    spec: &ScheduleInfoUpdateSpec,
    object_id: i64,
    next_unix_seconds: Option<i64>,
    warnings: &mut Vec<tidb_context::SqlWarn>,
) -> Result<bool, MysqlError> {
    let pool = session_pool
        .sess_pool()
        .ok_or_else(|| MysqlError::unknown("the DDL session pool has no sessions"))?;
    let sess_ctx = pool.get().map_err(|error| MysqlError::unknown(error.to_string()))?;
    let session = tidb_ddl_session::Session::new(std::sync::Arc::clone(&sess_ctx));
    let result = update_schedule_info_row(&session, spec, object_id, next_unix_seconds, warnings);
    pool.put(sess_ctx);
    result
}

fn update_schedule_info_row<C: tidb_ddl_session::SessionContext + ?Sized>(
    session: &tidb_ddl_session::Session<C>,
    spec: &ScheduleInfoUpdateSpec,
    object_id: i64,
    next_unix_seconds: Option<i64>,
    warnings: &mut Vec<tidb_context::SqlWarn>,
) -> Result<bool, MysqlError> {
    let context = tidb_sqlexec::BackgroundContext;
    session.begin_pessimistic(&context).map_err(|error| cause_mysql_error(&error))?;
    let table_not_exists = |error: MysqlError| {
        if error.equal(&tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS) {
            MysqlError::unknown(spec.table_not_exists_err)
        } else {
            error
        }
    };
    let mut committed = false;
    let result = (|| {
        let lock_sql = format!(
            "SELECT 1 FROM {} WHERE {} = %? LIMIT 1 FOR UPDATE WAIT {}",
            spec.info_table, spec.id_column, SCHEDULE_INFO_UPDATE_LOCK_WAIT_TIMEOUT_SEC
        );
        let rows = match session.execute(
            &context,
            &lock_sql,
            &format!("{}-lock-info-row", spec.operation),
            &[tidb_sqlescape::SqlArg::Signed(object_id)],
        ) {
            Ok(rows) => rows.unwrap_or_default(),
            Err(error) => {
                let error = cause_mysql_error(error.as_ref());
                if is_schedule_info_update_lock_contention_err(&error) {
                    warnings.push(schedule_info_update_warning(spec));
                    return Ok(false);
                }
                return Err(table_not_exists(error));
            }
        };
        if rows.is_empty() {
            return Err(MysqlError::unknown(spec.row_missing_err));
        }
        let next_arg = next_unix_seconds.map_or(tidb_sqlescape::SqlArg::Null, tidb_sqlescape::SqlArg::Signed);
        let update_sql =
            format!("UPDATE {} SET {} = %? WHERE {} = %?", spec.info_table, spec.schedule_column, spec.id_column);
        if let Err(error) = session.execute(
            &context,
            &update_sql,
            &format!("{}-update-schedule-unix-seconds", spec.operation),
            &[next_arg, tidb_sqlescape::SqlArg::Signed(object_id)],
        ) {
            let error = cause_mysql_error(error.as_ref());
            if is_schedule_info_update_lock_contention_err(&error) {
                warnings.push(schedule_info_update_warning(spec));
                return Ok(false);
            }
            return Err(table_not_exists(error));
        }
        if let Err(error) = session.commit(&context) {
            let error = cause_mysql_error(&error);
            if is_schedule_info_update_lock_contention_err(&error) {
                warnings.push(schedule_info_update_warning(spec));
                return Ok(false);
            }
            return Err(error);
        }
        committed = true;
        Ok(true)
    })();
    if !committed {
        session.rollback();
    }
    result
}

/// Go master `mviewGroupByInfo`.
pub(crate) struct MViewGroupByInfo {
    /// The GROUP BY column's offset in the SELECT list.
    pub select_idx: usize,
    /// Whether the base column is NOT NULL.
    pub not_null: bool,
}

/// Go master `mviewQueryAnalysis`.
pub(crate) struct MViewQueryAnalysis {
    pub group_by_infos: Vec<MViewGroupByInfo>,
    pub group_by_cols: Vec<String>,
    pub has_min_or_max: bool,
}

fn general_unsupported(message: impl Into<String>) -> MysqlError {
    MysqlError::from_terror(&tidb_dbterror::ERR_GENERAL_UNSUPPORTED_DDL.gen_with_stack(message.into()))
}

/// Go master `extractSingleTableNameAndAliasFromSelect`: the table path and
/// its alias.
fn extract_single_table_name_and_alias_from_select(
    select: &tidb_ast::SelectStmt,
) -> Result<(Vec<String>, Option<String>), MysqlError> {
    let single = || general_unsupported("CREATE MATERIALIZED VIEW only supports a single base table");
    let Some(from) = &select.from else {
        return Err(single());
    };
    if from.right.is_some() {
        return Err(single());
    }
    let tidb_ast::JoinNode::Table(table) = &from.left else {
        return Err(single());
    };
    Ok((table.name.clone(), table.alias.clone()))
}

/// The statement's base table as Go's `ast.TableName` holds it: schema and
/// name, the schema empty when not written.
fn split_table_path(path: &[String]) -> (String, String) {
    match path {
        [schema, table] => (schema.clone(), table.clone()),
        [table] => (String::new(), table.clone()),
        _ => (String::new(), String::new()),
    }
}

/// Go master `resolveMViewColumnName`: the base column a reference names.
fn resolve_mview_column_name(
    path: &[String],
    base_schema: &str,
    base_table: &str,
    from_alias: Option<&str>,
    base_columns: &[tidb_model::ColumnInfo],
) -> Result<String, MysqlError> {
    let (schema, table, column) = match path {
        [column] => ("", "", column.as_str()),
        [table, column] => ("", table.as_str(), column.as_str()),
        [schema, table, column] => (schema.as_str(), table.as_str(), column.as_str()),
        _ => return Err(general_unsupported("column reference is nil in CREATE MATERIALIZED VIEW")),
    };
    let not_exists = || {
        MysqlError::from_terror(
            &tidb_infoschema::infoschema_errors::ERR_COLUMN_NOT_EXISTS
                .gen_with_stack_by_args(&[column.to_owned().into(), base_table.to_owned().into()]),
        )
    };
    let lower = |name: &str| tidb_hack::go_to_lower(name);
    if !schema.is_empty() && lower(schema) != lower(base_schema) {
        return Err(not_exists());
    }
    if !table.is_empty()
        && lower(table) != lower(base_table)
        && from_alias.is_none_or(|alias| alias.is_empty() || lower(table) != lower(alias))
    {
        return Err(not_exists());
    }
    let column = lower(column);
    if !base_columns.iter().any(|base| base.name.lowercase() == column) {
        return Err(not_exists());
    }
    Ok(column)
}

/// Go master `collectColumnNamesInExpr`: the column references in `expr`
/// (not under a subquery).
fn collect_column_names_in_expr(expr: &tidb_ast::Expr) -> Vec<Vec<String>> {
    struct ColumnNameCollector(Vec<Vec<String>>);
    impl tidb_ast::Visitor for ColumnNameCollector {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if let Some(tidb_ast::Expr::Column(path)) = node.downcast_ref::<tidb_ast::Expr>() {
                self.0.push(path.clone());
            }
            false
        }
        fn leave(&mut self, _: &mut dyn std::any::Any) -> bool {
            true
        }
    }
    let mut collector = ColumnNameCollector(Vec::new());
    let mut expr = expr.clone();
    tidb_ast::Visitable::accept(&mut expr, &mut collector);
    collector.0
}

/// Go master `isCountStarOrOne`: the integer literal 1 (`COUNT(*)` is
/// parsed as `COUNT(1)`).
fn is_count_star_or_one(arg: &tidb_ast::Expr) -> bool {
    matches!(arg, tidb_ast::Expr::Int(text) if text.parse::<i64>() == Ok(1))
}

/// Go master `buildMViewSingleTableExpr`'s column scope: the base table's
/// columns, named by the alias when there is one.
struct MViewBaseColumns<'a> {
    context: &'a dyn DdlSessionContext,
    schema: String,
    table: String,
    columns: &'a [tidb_model::ColumnInfo],
}

impl tidb_expr::rewriter::ColumnResolver for MViewBaseColumns<'_> {
    fn resolve(&self, path: &[String]) -> Option<(usize, tidb_datatype::FieldType, i64)> {
        let lower = |name: &str| tidb_hack::go_to_lower(name);
        let (schema, table, column) = match path {
            [column] => (None, None, column),
            [table, column] => (None, Some(table), column),
            [schema, table, column] => (Some(schema), Some(table), column),
            _ => return None,
        };
        if schema.is_some_and(|schema| lower(schema) != lower(&self.schema))
            || table.is_some_and(|table| lower(table) != lower(&self.table))
        {
            return None;
        }
        let column = lower(column);
        self.columns
            .iter()
            .enumerate()
            .find(|(_, base)| base.name.lowercase() == column)
            .map(|(offset, base)| (offset, base.field_type.clone(), base.id))
    }
    fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
        self.context.session_zone()
    }
}

/// Go master `validateCreateMaterializedViewQuery`: the single-table
/// aggregation a materialized view keeps, its GROUP BY keys and whether it
/// needs a MIN/MAX index; every column it reads must be in the log.
pub(crate) fn validate_create_materialized_view_query(
    context: &dyn DdlSessionContext,
    base_schema: &str,
    base_table: &str,
    base: &tidb_model::TableInfo,
    mlog_columns: &[String],
    select: &tidb_ast::QueryStmt,
) -> Result<MViewQueryAnalysis, MysqlError> {
    let tidb_ast::QueryStmt::Select(select) = select else {
        return Err(general_unsupported("CREATE MATERIALIZED VIEW only supports SELECT statement"));
    };
    let (from_path, from_alias) = extract_single_table_name_and_alias_from_select(select)?;
    let (mut from_schema, from_table) = split_table_path(&from_path);
    if from_schema.is_empty() {
        from_schema = base_schema.to_owned();
    }
    let lower = |name: &str| tidb_hack::go_to_lower(name);
    if lower(&from_schema) != lower(base_schema) || lower(&from_table) != lower(base_table) {
        return Err(general_unsupported("CREATE MATERIALIZED VIEW only supports a single base table"));
    }
    tidb_mviewutil::check_materialized_view_select(&tidb_ast::QueryStmt::Select(select.clone()))?;
    if select.group_by.is_empty() {
        return Err(general_unsupported("CREATE MATERIALIZED VIEW requires GROUP BY clause"));
    }
    if select.rollup {
        return Err(general_unsupported("CREATE MATERIALIZED VIEW does not support GROUP BY WITH ROLLUP"));
    }
    if select.having.is_some() {
        return Err(general_unsupported("CREATE MATERIALIZED VIEW does not support HAVING clause"));
    }
    if !select.order_by.is_empty() {
        return Err(general_unsupported("CREATE MATERIALIZED VIEW does not support ORDER BY clause"));
    }
    if select.limit.is_some() {
        return Err(general_unsupported("CREATE MATERIALIZED VIEW does not support LIMIT clause"));
    }
    if select.distinct {
        return Err(general_unsupported("CREATE MATERIALIZED VIEW does not support SELECT DISTINCT"));
    }
    let base_columns: Vec<tidb_model::ColumnInfo> =
        base.columns.iter_deref().map(|column| column.read().clone_like_go()).collect();
    let alias = from_alias.as_deref();
    let resolve = |path: &[String]| resolve_mview_column_name(path, base_schema, base_table, alias, &base_columns);
    let base_column = |name: &str| base_columns.iter().find(|base| base.name.lowercase() == name).expect("resolved");

    let mut group_by_cols: Vec<String> = Vec::new();
    let mut group_by_not_null: Vec<bool> = Vec::new();
    let mut count_expr_cols: Vec<String> = Vec::new();
    let mut nullable_sum_cols: Vec<String> = Vec::new();
    let mut used_cols: Vec<String> = Vec::new();
    let mut use_col = |used: &mut Vec<String>, name: &str| {
        if !used.iter().any(|seen| seen == name) {
            used.push(name.to_owned());
        }
    };
    for item in &select.group_by {
        let tidb_ast::Expr::Column(path) = &item.expr else {
            return Err(general_unsupported("GROUP BY expression is not supported in CREATE MATERIALIZED VIEW"));
        };
        let name = resolve(path)?;
        if group_by_cols.contains(&name) {
            return Err(general_unsupported("duplicate GROUP BY column is not supported in CREATE MATERIALIZED VIEW"));
        }
        group_by_not_null.push(base_column(&name).field_type.has_flag(tidb_datatype::FieldTypeFlags::NOT_NULL));
        use_col(&mut used_cols, &name);
        group_by_cols.push(name);
    }

    if let Some(condition) = &select.where_clause {
        let scope = MViewBaseColumns {
            context,
            schema: base_schema.to_owned(),
            table: alias.filter(|alias| !alias.is_empty()).unwrap_or(base_table).to_owned(),
            columns: &base_columns,
        };
        let built = tidb_expr::rewriter::rewrite_expr_resolved(condition, &scope)
            .map_err(|_| general_unsupported("CREATE MATERIALIZED VIEW WHERE clause is not supported"))?;
        if tidb_expr::expr_util::predicates::check_non_deterministic(&built) {
            return Err(general_unsupported("CREATE MATERIALIZED VIEW WHERE clause must be deterministic"));
        }
        for path in collect_column_names_in_expr(condition) {
            let name = resolve(&path)?;
            use_col(&mut used_cols, &name);
        }
    }

    let mut select_col_idx: Vec<(String, usize)> = Vec::new();
    let mut has_count_star_or_one = false;
    let mut has_min_or_max = false;
    for (offset, field) in select.fields.fields().iter().enumerate() {
        let expr = match field {
            tidb_ast::SelectField::Wildcard(_) => {
                return Err(general_unsupported("CREATE MATERIALIZED VIEW does not support wildcard select field"));
            }
            tidb_ast::SelectField::Expr { expr, .. } => expr,
        };
        match expr {
            tidb_ast::Expr::Column(path) => {
                let name = resolve(path)?;
                if !group_by_cols.contains(&name) {
                    return Err(general_unsupported("non-aggregated column must appear in GROUP BY clause"));
                }
                if select_col_idx.iter().any(|(seen, _)| *seen == name) {
                    return Err(general_unsupported(
                        "duplicate GROUP BY column in SELECT list is not supported in CREATE MATERIALIZED VIEW",
                    ));
                }
                use_col(&mut used_cols, &name);
                select_col_idx.push((name, offset));
            }
            tidb_ast::Expr::Aggregate { name, written, distinct, args } => {
                if *distinct {
                    return Err(general_unsupported("CREATE MATERIALIZED VIEW does not support DISTINCT aggregate function"));
                }
                let agg = name.to_ascii_lowercase();
                if !matches!(agg.as_str(), "count" | "sum" | "min" | "max") {
                    let written = written.as_deref().unwrap_or(name);
                    return Err(general_unsupported(format!(
                        "unsupported aggregate function in CREATE MATERIALIZED VIEW: agg {written}"
                    )));
                }
                if agg == "count" {
                    let [arg] = args.as_slice() else {
                        return Err(general_unsupported(
                            "count(*)/count(1) must have exactly one argument in CREATE MATERIALIZED VIEW",
                        ));
                    };
                    if let tidb_ast::Expr::Column(path) = arg {
                        let name = resolve(path)?;
                        use_col(&mut count_expr_cols, &name);
                        use_col(&mut used_cols, &name);
                        continue;
                    }
                    if !is_count_star_or_one(arg) {
                        return Err(general_unsupported("CREATE MATERIALIZED VIEW only supports count(*)/count(1)"));
                    }
                    has_count_star_or_one = true;
                    continue;
                }
                let [arg] = args.as_slice() else {
                    return Err(general_unsupported(
                        "aggregate function must have exactly one argument in CREATE MATERIALIZED VIEW",
                    ));
                };
                let tidb_ast::Expr::Column(path) = arg else {
                    return Err(general_unsupported(
                        "aggregate function only supports column argument in CREATE MATERIALIZED VIEW",
                    ));
                };
                let name = resolve(path)?;
                if agg == "sum" {
                    let column = base_column(&name);
                    let code = column.field_type.code();
                    if code.is_type_time() || code == tidb_datatype::FieldTypeCode::Duration {
                        return Err(general_unsupported(
                            "CREATE MATERIALIZED VIEW does not support SUM on DATE/DATETIME/TIMESTAMP/TIME column",
                        ));
                    }
                    if !column.field_type.has_flag(tidb_datatype::FieldTypeFlags::NOT_NULL) {
                        use_col(&mut nullable_sum_cols, &name);
                    }
                } else {
                    has_min_or_max = true;
                }
                use_col(&mut used_cols, &name);
            }
            _ => return Err(general_unsupported("unsupported SELECT expression in CREATE MATERIALIZED VIEW")),
        }
    }
    if !has_count_star_or_one {
        return Err(general_unsupported("CREATE MATERIALIZED VIEW must contain count(*)/count(1)"));
    }
    // Go walks a map here, so with several such columns which one it names
    // is unspecified; this names the first in SELECT order.
    if let Some(column) = nullable_sum_cols.iter().find(|column| !count_expr_cols.contains(column)) {
        return Err(general_unsupported(format!(
            "CREATE MATERIALIZED VIEW SUM on nullable column {column} requires matching COUNT({column}) in SELECT list"
        )));
    }
    let mut group_by_infos = Vec::with_capacity(select.group_by.len());
    for (item, (name, not_null)) in select.group_by.iter().zip(group_by_cols.iter().zip(&group_by_not_null)) {
        let Some((_, select_idx)) = select_col_idx.iter().find(|(seen, _)| seen == name) else {
            let written = match &item.expr {
                tidb_ast::Expr::Column(path) => path.last().cloned().unwrap_or_default(),
                _ => String::new(),
            };
            return Err(MysqlError::unknown(format!("GROUP BY column {written} must appear in SELECT list")));
        };
        group_by_infos.push(MViewGroupByInfo { select_idx: *select_idx, not_null: *not_null });
    }
    if has_min_or_max && !tidb_mviewutil::has_index_with_prefix_covering_columns(base, &group_by_cols, "", true) {
        return Err(general_unsupported(
            "CREATE MATERIALIZED VIEW with MIN/MAX requires base table index whose leading columns cover all GROUP BY columns",
        ));
    }
    // Go walks a map here too; this names the first in reference order.
    let mlog_columns: Vec<String> = mlog_columns.iter().map(|column| tidb_hack::go_to_lower(column)).collect();
    if let Some(column) = used_cols.iter().find(|column| !mlog_columns.contains(column)) {
        return Err(general_unsupported(format!("materialized view log does not contain column {column}")));
    }
    Ok(MViewQueryAnalysis { group_by_infos, group_by_cols, has_min_or_max })
}

/// Go master `executor.CreateMaterializedView` through
/// `validateCreateMaterializedViewQuery`: the statement's checks in Go's
/// order. The job that follows (the SELECT's output types, the table and its
/// initial build) is not ported yet, so a statement that passes them is
/// refused.
pub(crate) fn create_materialized_view_job(
    catalog: &SessionExtendedInfoSchema,
    stmt: &tidb_ast::CreateMaterializedViewStmt,
    current_db: &str,
    context: &dyn DdlSessionContext,
) -> Result<crate::JobWrapper, MysqlError> {
    check_materialized_view_enabled(context)?;
    let (schema_name, view_name) = match stmt.view_name.as_slice() {
        [schema, view] => (schema.clone(), view.clone()),
        [view] => {
            if current_db.is_empty() {
                return Err(MysqlError::from_terror(&tidb_plannererrors::ERR_NO_DB.gen_with_stack_by_args(&[])));
            }
            (current_db.to_owned(), view.clone())
        }
        _ => return Err(MysqlError::unknown("invalid materialized view name".to_owned())),
    };
    if catalog.schema_by_name(&schema_name).is_none() {
        return Err(MysqlError::from_terror(
            &tidb_infoschema::infoschema_errors::ERR_DATABASE_NOT_EXISTS.gen_with_stack_by_args(&[schema_name.into()]),
        ));
    }
    tidb_ddl_build::validate_comment_length(
        &stmt.comment,
        &tidb_hack::GoToLower::go_to_lower(&view_name),
        tidb_ddl_build::CommentOwner::Table,
        &context.metabuild_context(),
    )?;
    let tidb_ast::QueryStmt::Select(select) = &*stmt.select else {
        return Err(general_unsupported("CREATE MATERIALIZED VIEW only supports SELECT statement"));
    };
    let (base_path, _) = extract_single_table_name_and_alias_from_select(select)?;
    let (mut base_schema, base_table) = split_table_path(&base_path);
    if base_schema.is_empty() {
        base_schema = schema_name.clone();
    }
    let lower = |name: &str| tidb_hack::go_to_lower(name);
    if lower(&base_schema) != lower(&schema_name) {
        return Err(general_unsupported("CREATE MATERIALIZED VIEW only supports base table in the same schema"));
    }
    let Some((_, base)) = catalog.table_info_by_name(&base_schema, &base_table) else {
        return Err(MysqlError::from_terror(
            &tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                .gen_with_stack_by_args(&[base_schema.clone().into(), base_table.clone().into()]),
        ));
    };
    let base = base.read().clone_like_go();
    if base.is_view()
        || base.is_sequence()
        || base.temp_table_type != tidb_model::TempTableType::NONE
        || base.materialized_view.is_some()
    {
        return Err(MysqlError::from_terror(&tidb_dbterror::ERR_WRONG_OBJECT.gen_with_stack_by_args(&[
            schema_name.clone().into(),
            base_table.clone().into(),
            "BASE TABLE".into(),
        ])));
    }
    if base.get_partition_info().is_some() {
        return Err(general_unsupported("CREATE MATERIALIZED VIEW on partition table"));
    }
    let mlog_name = tidb_model::table::materialized_view_log_table_name(&base.name).original().to_owned();
    let Some((_, mlog)) = catalog.table_info_by_name(&base_schema, &mlog_name) else {
        return Err(MysqlError::unknown(format!(
            "materialized view log does not exist for base table {base_schema}.{base_table}"
        )));
    };
    let mlog = mlog.read().clone_like_go();
    let Some(log) = mlog.materialized_view_log.as_ref().filter(|log| log.read().base_table_id == base.id) else {
        return Err(MysqlError::unknown(format!(
            "table {base_schema}.{mlog_name} is not a materialized view log for base table {base_schema}.{base_table}"
        )));
    };
    let mlog_columns: Vec<String> = log.read().columns.iter().map(|column| column.original().to_owned()).collect();
    let _analysis = validate_create_materialized_view_query(
        context,
        &base_schema,
        &base_table,
        &base,
        &mlog_columns,
        &stmt.select,
    )?;
    Err(general_unsupported("CREATE MATERIALIZED VIEW on this node"))
}
