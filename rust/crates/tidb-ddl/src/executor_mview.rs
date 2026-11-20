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
        schema.placement_policy_ref.as_ref(),
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
) -> Result<Result<(String, String), String>, MysqlError> {
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
            return Ok(Err(format!("{schema}.{table}")));
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
    tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/ddl/afterCheckDropMaterializedViewLog", &mut ());
    tidb_failpoint::inject("github.com/pingcap/tidb/pkg/ddl/pauseDropMaterializedViewLogAfterCheck", |_| {});
    Ok(Ok((schema, mlog_name)))
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

/// Go master `updateMaterializedViewRefreshInfoNextUnixSeconds`'s spec.
pub(crate) const MVIEW_REFRESH_INFO_UPDATE_SPEC: ScheduleInfoUpdateSpec = ScheduleInfoUpdateSpec {
    operation: "alter materialized view refresh",
    info_table: "mysql.tidb_mview_refresh_info",
    id_column: "MVIEW_ID",
    schedule_column: "NEXT_REFRESH_UNIX_SECONDS",
    row_missing_err: "alter materialized view refresh: refresh info row missing in mysql.tidb_mview_refresh_info",
    table_not_exists_err: "alter materialized view refresh: required system table mysql.tidb_mview_refresh_info does not exist",
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

/// Go master `analyzeStoredMaterializedViewQuery`: a stored definition
/// parsed with the session's connection charset (the DDL session's when
/// `context` is `None`) and validated as CREATE MATERIALIZED VIEW does.
pub(crate) fn analyze_stored_materialized_view_query(
    context: Option<&dyn DdlSessionContext>,
    base_schema: &str,
    base: &tidb_model::TableInfo,
    mlog_columns: &[String],
    sql: &str,
) -> Result<MViewQueryAnalysis, MysqlError> {
    let (charset, collation, time_zone) = match context {
        Some(context) => {
            let (charset, collation) = context.connection_charset_info();
            (charset.to_owned(), collation.to_owned(), context.session_zone())
        }
        None => (
            tidb_mysql::DefaultCharset.to_owned(),
            tidb_mysql::DefaultCollationName.to_owned(),
            tidb_datatype::SessionTimeZone::default(),
        ),
    };
    let stmt = tidb_parser::parse_with_connection(sql, &charset, &collation)
        .map_err(|error| MysqlError::unknown(error.compatibility_message(sql)))?;
    let tidb_ast::Stmt::Query(query) = stmt else {
        return Err(MysqlError::unknown("expected select statement"));
    };
    validate_create_materialized_view_query(time_zone, base_schema, base.name.original(), base, mlog_columns, &query)
}

/// Go master `checkBaseTableDependentMViewMinMaxIndexConstraintsWithEffectiveTable`:
/// every MIN/MAX view on `base` must keep a visible public index (of
/// `effective`, the table as the DDL leaves it, without
/// `excluded_index_name`) whose leading columns cover its GROUP BY.
#[allow(clippy::too_many_arguments)]
pub(crate) fn check_base_table_dependent_mview_min_max_index_constraints_with_effective_table(
    context: Option<&dyn DdlSessionContext>,
    table_by_id: &dyn Fn(i64) -> Option<tidb_model::TableInfo>,
    schema_name: &str,
    base: &tidb_model::TableInfo,
    effective: &tidb_model::TableInfo,
    excluded_index_name: &str,
    index_name_for_err: &str,
    op: &str,
) -> Result<(), MysqlError> {
    let Some(base_info) = base.materialized_view_base.as_ref().map(|info| info.read().clone()) else {
        return Ok(());
    };
    if base_info.mview_ids.is_empty() {
        return Ok(());
    }
    let Some(mlog) = table_by_id(base_info.mlog_id).filter(|mlog| mlog.materialized_view_log.is_some()) else {
        return Err(general_unsupported_by_args(format!("{op} on base table with invalid materialized view log metadata")));
    };
    let mlog_columns: Vec<String> = mlog
        .materialized_view_log
        .as_ref()
        .expect("checked above")
        .read()
        .columns
        .iter()
        .map(|column| column.original().to_owned())
        .collect();
    for &mview_id in base_info.mview_ids.iter() {
        let Some(mview) = table_by_id(mview_id).filter(|mview| mview.materialized_view.is_some()) else {
            return Err(general_unsupported_by_args(format!(
                "{op} on base table with invalid dependent materialized view metadata"
            )));
        };
        let sql = mview.materialized_view.as_ref().expect("checked above").read().sql_content.clone();
        let Ok(analysis) = analyze_stored_materialized_view_query(context, schema_name, base, &mlog_columns, &sql) else {
            return Err(general_unsupported_by_args(format!(
                "{op} on base table with invalid dependent materialized view {} metadata",
                mview.name.original()
            )));
        };
        if !analysis.has_min_or_max {
            continue;
        }
        if tidb_mviewutil::has_index_with_prefix_covering_columns(
            effective,
            &analysis.group_by_cols,
            &tidb_hack::go_to_lower(excluded_index_name),
            true,
        ) {
            continue;
        }
        return Err(general_unsupported_by_args(format!(
            "{op} on base table index {index_name_for_err} required by materialized view {} for MIN/MAX fast refresh",
            mview.name.original()
        )));
    }
    Ok(())
}

/// Go `dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs(message)`:
/// `Unsupported <message>`.
fn general_unsupported_by_args(message: String) -> MysqlError {
    MysqlError::from_terror(&tidb_dbterror::ERR_GENERAL_UNSUPPORTED_DDL.gen_with_stack_by_args(&[message.into()]))
}

/// Go `dbterror.ErrGeneralUnsupportedDDL.GenWithStack(message)`: `message`
/// in place of the template.
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
    time_zone: tidb_datatype::SessionTimeZone,
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
        self.time_zone.clone()
    }
}

/// Go master `validateCreateMaterializedViewQuery`: the single-table
/// aggregation a materialized view keeps, its GROUP BY keys and whether it
/// needs a MIN/MAX index; every column it reads must be in the log.
pub(crate) fn validate_create_materialized_view_query(
    time_zone: tidb_datatype::SessionTimeZone,
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
    let use_col = |used: &mut Vec<String>, name: &str| {
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
            time_zone: time_zone.clone(),
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

/// What Go master's `executor.CreateMaterializedView` has settled before it
/// runs the SELECT for its output types.
pub(crate) struct CreateMaterializedViewPlan {
    pub schema: tidb_model::db::DBInfo,
    pub view_name: String,
    pub comment: String,
    pub base: tidb_model::TableInfo,
    pub mlog: tidb_model::TableInfo,
    pub analysis: MViewQueryAnalysis,
    /// Go `restoreNodeToCanonicalSQL(s.Select)` once the base table carries
    /// its schema and hints their database.
    pub select_sql: String,
}

/// Go master `normalizeMVDefinitionHintDBNames`: a hint's table without a
/// database names `default_db`'s.
fn normalize_mv_definition_hint_db_names(query: &mut tidb_ast::QueryStmt, default_db: &str) {
    struct Normalizer<'a>(&'a str);
    impl tidb_ast::Visitor for Normalizer<'_> {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if let Some(hint) = node.downcast_mut::<tidb_ast::Hint>() {
                let fill = |table: &mut tidb_ast::HintTable| {
                    if table.db_name.as_deref().is_none_or(str::is_empty) {
                        table.db_name = Some(self.0.to_owned());
                    }
                };
                match &mut hint.kind {
                    tidb_ast::HintKind::Tables { tables, .. } => tables.iter_mut().for_each(fill),
                    tidb_ast::HintKind::Index { table, .. } => fill(table),
                    _ => {}
                }
            }
            false
        }
        fn leave(&mut self, _: &mut dyn std::any::Any) -> bool {
            true
        }
    }
    if default_db.is_empty() {
        return;
    }
    tidb_ast::Visitable::accept(query, &mut Normalizer(default_db));
}

/// Go master `executor.CreateMaterializedView` up to the SELECT it runs for
/// the output types: the checks in Go's order, then the definition SQL.
pub(crate) fn create_materialized_view_plan(
    catalog: &SessionExtendedInfoSchema,
    stmt: &tidb_ast::CreateMaterializedViewStmt,
    current_db: &str,
    context: &dyn DdlSessionContext,
) -> Result<CreateMaterializedViewPlan, MysqlError> {
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
    let Some(schema) = catalog.schema_by_name(&schema_name) else {
        return Err(MysqlError::from_terror(
            &tidb_infoschema::infoschema_errors::ERR_DATABASE_NOT_EXISTS.gen_with_stack_by_args(&[schema_name.into()]),
        ));
    };
    let schema = schema.read().clone();
    // Go master: `if s.IfNotExists && is.TableExists(schemaName,
    // s.ViewName.Name)` is a note (the caller's `if_not_exists_note`).
    if stmt.if_not_exists && catalog.table_exists(&schema_name, &view_name) {
        return Err(MysqlError::from_terror(
            &tidb_infoschema::infoschema_errors::ERR_TABLE_EXISTS.fast_gen_by_args(&[format!("{schema_name}.{view_name}").into()]),
        ));
    }
    let comment = tidb_ddl_build::validate_comment_length(
        &stmt.comment,
        &tidb_hack::go_to_lower(&view_name),
        tidb_ddl_build::CommentOwner::Table,
        &context.metabuild_context(),
    )?;
    let mut select = (*stmt.select).clone();
    let tidb_ast::QueryStmt::Select(select_stmt) = &mut select else {
        return Err(general_unsupported("CREATE MATERIALIZED VIEW only supports SELECT statement"));
    };
    let (base_path, _) = extract_single_table_name_and_alias_from_select(select_stmt)?;
    let (mut base_schema, base_table) = split_table_path(&base_path);
    if base_schema.is_empty() {
        // Go sets the TableName's schema in the statement, so the restored
        // definition names it.
        base_schema = schema_name.clone();
        if let Some(from) = &mut select_stmt.from {
            if let tidb_ast::JoinNode::Table(table) = &mut from.left {
                table.name = vec![schema_name.clone(), base_table.clone()];
            }
        }
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
    crate::cluster_ddl::materialized_view::validate_materialized_view_source_state(&schema_name, &base)?;
    crate::cluster_ddl::materialized_view::validate_materialized_view_nesting_depth(
        &base,
        &mut |table_id| {
            catalog.table_info_by_id(table_id).map(|table| table.read().clone_like_go()).ok_or_else(|| {
                MysqlError::from_terror(&tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS.gen_with_stack_by_args(&[
                    schema_name.clone().into(),
                    format!("(Table ID {table_id})").into(),
                ]))
            })
        },
        &crate::cluster_ddl::materialized_view::materialized_view_nesting_depth_exceeded,
    )?;
    let mlog_id = base.materialized_view_base.as_ref().map_or(0, |info| info.read().mlog_id);
    if mlog_id == 0 {
        return Err(MysqlError::unknown(format!(
            "materialized view log does not exist for source object {base_schema}.{base_table}"
        )));
    }
    let Some(mlog) = catalog.table_info_by_id(mlog_id) else {
        return Err(MysqlError::unknown(format!(
            "materialized view log id {mlog_id} does not exist for source object {base_schema}.{base_table}"
        )));
    };
    let mlog = mlog.read().clone_like_go();
    crate::cluster_ddl::materialized_view::validate_materialized_view_source_mlog(&schema_name, &base, &mlog)?;
    let log = mlog.materialized_view_log.as_ref().expect("validate_materialized_view_source_mlog checked the log");
    let mlog_columns: Vec<String> = log.read().columns.iter().map(|column| column.original().to_owned()).collect();
    let analysis =
        validate_create_materialized_view_query(context.session_zone(), &base_schema, &base_table, &base, &mlog_columns, &select)?;
    normalize_mv_definition_hint_db_names(&mut select, &schema_name);
    let select_sql = select.restore_with_flags(
        tidb_parser_format::RestoreFlags::DEFAULT | tidb_parser_format::RestoreFlags::STRING_WITHOUT_CHARSET,
    );
    Ok(CreateMaterializedViewPlan { schema, view_name, comment, base, mlog, analysis, select_sql })
}

/// Go master `executor.CreateMaterializedView` after the SELECT's output
/// types (`field_types`, one per output column) are known: the table with
/// its group-key PRIMARY / UNIQUE key and `MaterializedViewInfo`, and the job.
pub(crate) fn create_materialized_view_job(
    plan: CreateMaterializedViewPlan,
    stmt: &tidb_ast::CreateMaterializedViewStmt,
    field_types: Vec<tidb_datatype::FieldType>,
    context: &dyn DdlSessionContext,
) -> Result<crate::JobWrapper, MysqlError> {
    if field_types.len() != stmt.cols.len() {
        return Err(MysqlError::unknown(format!(
            "materialized view column count {} does not match query output {}",
            stmt.cols.len(),
            field_types.len()
        )));
    }
    let field_types: Vec<tidb_datatype::FieldType> = field_types
        .into_iter()
        .map(|mut field_type| {
            field_type.del_flag(
                tidb_datatype::FieldTypeFlags::PRI_KEY
                    | tidb_datatype::FieldTypeFlags::UNIQUE_KEY
                    | tidb_datatype::FieldTypeFlags::MULTIPLE_KEY
                    | tidb_datatype::FieldTypeFlags::AUTO_INCREMENT
                    | tidb_datatype::FieldTypeFlags::ON_UPDATE_NOW,
            );
            field_type
        })
        .collect();
    let columns: Vec<tidb_ast::ColumnDef> =
        stmt.cols.iter().map(|name| tidb_ddl_build::materialized_view::built_type_column_def(name)).collect();
    // Go: one row per group, a PRIMARY KEY when every group key is NOT NULL,
    // else UNIQUE.
    let all_not_null = plan.analysis.group_by_infos.iter().all(|info| info.not_null);
    let parts = plan
        .analysis
        .group_by_infos
        .iter()
        .map(|info| tidb_ast::IndexPart::Column { name: stmt.cols[info.select_idx].clone(), prefix_len: None, desc: false })
        .collect();
    let key = tidb_ast::IndexConstraintDefinition {
        kind: if all_not_null { tidb_ast::IndexConstraintKind::PrimaryKey } else { tidb_ast::IndexConstraintKind::Unique },
        if_not_exists: false,
        name: None,
        is_empty_index: false,
        parts,
        options: tidb_ast::IndexOptions::default(),
    };
    let create = tidb_ast::CreateTableStmt {
        temporary: tidb_ast::CreateTableTemporary::None,
        on_commit_delete: false,
        if_not_exists: false,
        name: vec![plan.schema.name.original().to_owned(), plan.view_name.clone()],
        like_table: None,
        columns,
        table_constraints: vec![tidb_ast::TableConstraint::Index(key)],
        table_options: stmt.options.clone(),
        partitioning: None,
        splits: Vec::new(),
        ctas: None,
    };
    let mut info = tidb_ddl_build::table_info_build::build_table_info_with_field_types(
        &create,
        &field_types,
        plan.schema.name.original(),
        &plan.schema.charset,
        &plan.schema.collate,
        &context.metabuild_context(),
        context.enable_check_constraint(),
    )
    .map_err(|error| error.to_mysql_error())?;
    tidb_ddl_build::table_info_build::inherit_database_placement(&mut info, plan.schema.placement_policy_ref.as_ref());
    info.comment = plan.comment.clone();
    let expr_ctx = SessionExprContext(context);
    let (refresh_method, refresh_start_with, refresh_next) =
        tidb_ddl_build::materialized_view::build_mview_refresh_meta(&expr_ctx, stmt.refresh.as_ref())?;
    let (alert_warning_sec, alert_overdue_sec, alert_refresh_failed) =
        tidb_ddl_build::materialized_view::parse_mview_attributes(&stmt.attributes)?;
    let vars = context.ddl_reorg_vars();
    let sql_mode = context.ddl_sql_mode();
    info.materialized_view = Some(GoShared::new(tidb_model::table::MaterializedViewInfo {
        base_table_ids: vec![plan.base.id].into(),
        init_build_state: tidb_model::table::MViewInitBuildState::BUILDING,
        sql_content: plan.select_sql.clone(),
        refresh_method,
        refresh_start_with,
        refresh_next,
        alert_warning_sec,
        alert_overdue_sec,
        alert_refresh_failed,
        definition_sql_mode: sql_mode,
        refresh_schedule_sql_mode: sql_mode,
        definition_div_precision_increment: vars.div_precision_increment,
        definition_time_zone: tidb_model::TimeZoneLocation::new(vars.time_zone_name.clone(), vars.time_zone_offset),
        ..tidb_model::table::MaterializedViewInfo::default()
    }));
    let schema_l = plan.schema.name.lowercase().to_owned();
    let mut job = crate::cluster_ddl::session_job(context, ActionType::ACTION_CREATE_MATERIALIZED_VIEW);
    job.schema_id = plan.schema.id;
    job.schema_name = schema_l.clone().into();
    job.table_name = info.name.lowercase().to_owned().into();
    let involving = |table: &str| tidb_model::InvolvingSchemaInfo {
        database: schema_l.clone().into(),
        table: table.to_owned().into(),
        ..tidb_model::InvolvingSchemaInfo::default()
    };
    job.involving_schema_info = tidb_goruntime::go_runtime::GoSharedSlice::from_vec(vec![
        involving(info.name.lowercase()),
        involving(plan.base.name.lowercase()),
        involving(plan.mlog.name.lowercase()),
    ]);
    // Go `initMaterializedViewReorgMetaFromVariables`.
    let mut reorg = tidb_model::DDLReorgMeta::new(
        sql_mode as u64,
        tidb_model::TimeZoneLocation::new(vars.time_zone_name.clone(), vars.time_zone_offset),
        vars.resource_group_name.clone(),
    );
    reorg.set_concurrency(vars.worker_count);
    reorg.set_batch_size(vars.batch_size);
    reorg.set_max_write_speed(vars.max_write_speed);
    job.reorg_meta = Some(GoShared::new(reorg));
    let mut session_vars = std::collections::BTreeMap::new();
    session_vars.insert(tidb_vardef::tidb_vars::TIDB_SCATTER_REGION.to_owned(), vars.scatter_region.clone().into());
    for (name, value) in vars.mview_execution.job_system_vars() {
        session_vars.insert(name.to_owned(), value.into());
    }
    job.session_vars = Some(GoShared::new(session_vars));
    let args = tidb_model::CreateMaterializedViewArgs {
        table_info: Some(GoShared::new(info)),
        mlog_table_ids: vec![plan.mlog.id],
    };
    Ok(crate::JobWrapper {
        job,
        args: JobArgsValue::CreateMaterializedView(Some(GoShared::new(args))),
        id_allocated: false,
    })
}

/// Go master `executor.CreateMaterializedView`'s
/// `ExecRestrictedSQL("SELECT * FROM (<sql>) AS tidb_mv_query LIMIT 0")`
/// under the definition's sql_mode, div_precision_increment and time zone:
/// the output column types, on a session of the DDL session pool (Go's
/// restricted executor borrows one too).
pub(crate) fn materialized_view_query_output_types(
    session_pool: &dyn tidb_ddl_build::session_pool::DdlSessionPool,
    select_sql: &str,
    context: &dyn DdlSessionContext,
) -> Result<Vec<tidb_datatype::FieldType>, MysqlError> {
    let pool = session_pool
        .sess_pool()
        .ok_or_else(|| MysqlError::unknown("the DDL session pool has no sessions"))?;
    let sess_ctx = pool.get().map_err(|error| MysqlError::unknown(error.to_string()))?;
    let result = query_output_types_on(&*sess_ctx, select_sql, context);
    pool.put(sess_ctx);
    result
}

fn query_output_types_on(
    session: &(impl tidb_ddl_session::SessionContext + ?Sized),
    select_sql: &str,
    context: &dyn DdlSessionContext,
) -> Result<Vec<tidb_datatype::FieldType>, MysqlError> {
    let exec_context = tidb_sqlexec::BackgroundContext;
    let source = tidb_ddl_session::INTERNAL_TXN_DDL;
    let run = |sql: &str, args: &[tidb_sqlescape::SqlArg<'_>]| {
        session.execute_internal(&exec_context, source, sql, args).map_err(|error| cause_mysql_error(error.as_ref()))
    };
    let read_vars = |record_set: Option<Box<dyn tidb_sqlexec::RecordSet>>| -> Result<Vec<String>, MysqlError> {
        let Some(mut record_set) = record_set else {
            return Ok(Vec::new());
        };
        let rows = tidb_sqlexec::drain_record_set(&exec_context, record_set.as_mut(), 1)
            .map_err(|error| cause_mysql_error(error.as_ref()))?;
        Ok(rows.first().map(|row| row.iter().map(datum_text).collect()).unwrap_or_default())
    };
    // Go's `ExecOptionWithSessionVarsSetup`: the definition's settings for
    // this statement, the session's own restored afterwards.
    let original = read_vars(run("SELECT @@SESSION.sql_mode, @@SESSION.time_zone, @@SESSION.div_precision_increment", &[])?)?;
    let vars = context.ddl_reorg_vars();
    let div_precision_increment = vars.div_precision_increment.to_string();
    run(
        "SET @@SESSION.sql_mode = %?, @@SESSION.time_zone = %?, @@SESSION.div_precision_increment = %?",
        &[
            tidb_sqlescape::SqlArg::String(vars.sql_mode_var.as_bytes()),
            tidb_sqlescape::SqlArg::String(vars.time_zone_var.as_bytes()),
            tidb_sqlescape::SqlArg::String(div_precision_increment.as_bytes()),
        ],
    )?;
    let query = format!("SELECT * FROM ({select_sql}) AS `tidb_mv_query` LIMIT 0");
    let result = run(&query, &[]).and_then(|record_set| {
        let Some(record_set) = record_set else {
            return Ok(Vec::new());
        };
        record_set
            .fields()
            .iter()
            .map(|field| {
                field
                    .read()
                    .column
                    .as_ref()
                    .map(|column| column.read().field_type.clone())
                    .ok_or_else(|| MysqlError::unknown("a result field of the definition has no column"))
            })
            .collect::<Result<Vec<_>, _>>()
    });
    if let [sql_mode, time_zone, div] = original.as_slice() {
        let _ = run(
            "SET @@SESSION.sql_mode = %?, @@SESSION.time_zone = %?, @@SESSION.div_precision_increment = %?",
            &[
                tidb_sqlescape::SqlArg::String(sql_mode.as_bytes()),
                tidb_sqlescape::SqlArg::String(time_zone.as_bytes()),
                tidb_sqlescape::SqlArg::String(div.as_bytes()),
            ],
        );
    }
    result
}

/// A system variable's value as `SELECT @@var` answers it.
fn datum_text(datum: &tidb_datatype::Datum) -> String {
    match datum {
        tidb_datatype::Datum::Int(value) => value.to_string(),
        tidb_datatype::Datum::UInt(value) => value.to_string(),
        tidb_datatype::Datum::String(value) => String::from_utf8_lossy(value.bytes()).into_owned(),
        tidb_datatype::Datum::Bytes(value) => String::from_utf8_lossy(value).into_owned(),
        _ => String::new(),
    }
}

/// Go master `executor.DropMaterializedView` up to `dropTableObject`: the
/// view's schema and name, or under IF EXISTS the missing `schema.view`
/// Go's note names (`appendDropMaterializedViewNotExistsNote`).
pub(crate) fn drop_materialized_view_target(
    catalog: &SessionExtendedInfoSchema,
    stmt: &tidb_ast::DropMaterializedViewStmt,
    current_db: &str,
) -> Result<Result<(String, String), String>, MysqlError> {
    let (schema, view) = match stmt.view_name.as_slice() {
        [schema, view] => (schema.clone(), view.clone()),
        [view] => {
            if current_db.is_empty() {
                return Err(MysqlError::from_terror(&tidb_plannererrors::ERR_NO_DB.gen_with_stack_by_args(&[])));
            }
            (current_db.to_owned(), view.clone())
        }
        _ => return Err(MysqlError::unknown("invalid materialized view name".to_owned())),
    };
    if catalog.schema_by_name(&schema).is_none() {
        if stmt.if_exists {
            return Ok(Err(format!("{schema}.{view}")));
        }
        return Err(MysqlError::from_terror(
            &tidb_infoschema::infoschema_errors::ERR_DATABASE_NOT_EXISTS.gen_with_stack_by_args(&[schema.into()]),
        ));
    }
    let Some((_, table)) = catalog.table_info_by_name(&schema, &view) else {
        if stmt.if_exists {
            return Ok(Err(format!("{schema}.{view}")));
        }
        return Err(MysqlError::from_terror(
            &tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                .gen_with_stack_by_args(&[schema.into(), view.into()]),
        ));
    };
    let table = table.read().clone_like_go();
    if table.materialized_view.is_none() {
        return Err(MysqlError::from_terror(&tidb_dbterror::ERR_WRONG_OBJECT.gen_with_stack_by_args(&[
            schema.clone().into(),
            view.clone().into(),
            "MATERIALIZED VIEW".into(),
        ])));
    }
    crate::cluster_ddl::materialized_view::check_base_table_materialized_view_dependency_constraints(
        &table,
        "DROP MATERIALIZED VIEW",
    )?;
    Ok(Ok((schema, view)))
}

/// Go master `executor.deleteMaterializedViewRefreshAlert`: a view whose
/// schedule was turned off has no refresh-overdue alert left.
pub(crate) fn delete_materialized_view_refresh_alert(
    session_pool: &dyn tidb_ddl_build::session_pool::DdlSessionPool,
    mview_id: i64,
) -> Result<(), MysqlError> {
    let pool = session_pool
        .sess_pool()
        .ok_or_else(|| MysqlError::unknown("the DDL session pool has no sessions"))?;
    let sess_ctx = pool.get().map_err(|error| MysqlError::unknown(error.to_string()))?;
    let session = tidb_ddl_session::Session::new(std::sync::Arc::clone(&sess_ctx));
    let context = tidb_sqlexec::BackgroundContext;
    let id = [tidb_sqlescape::SqlArg::Signed(mview_id)];
    let mut result = session
        .execute(
            &context,
            "UPDATE mysql.tidb_mview_refresh_alert SET ALERT_LEVEL = NULL, UPDATE_TIME = NOW(6) WHERE MVIEW_ID = %? AND ALERT_LEVEL IS NOT NULL",
            "alter-materialized-view-refresh-clear-alert-level",
            &id,
        )
        .map(|_| ())
        .map_err(|error| cause_mysql_error(error.as_ref()));
    if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/mockDeleteMaterializedViewRefreshAlertTableNotExists")
        .is_some_and(|value| value.bool())
    {
        result = Err(MysqlError::from_terror(
            &tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                .gen_with_stack_by_args(&["mysql".to_owned().into(), "tidb_mview_refresh_alert".to_owned().into()]),
        ));
    }
    let result = result
        .and_then(|_| {
            session
                .execute(
                    &context,
                    "DELETE FROM mysql.tidb_mview_refresh_alert WHERE MVIEW_ID = %? AND REFRESH_FAILED IS NULL",
                    "alter-materialized-view-refresh-delete-alert",
                    &id,
                )
                .map(|_| ())
                .map_err(|error| cause_mysql_error(error.as_ref()))
        })
        .map_err(|error| {
            if error.equal(&tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS) {
                MysqlError::unknown(
                    "alter materialized view refresh: required system table mysql.tidb_mview_refresh_alert does not exist",
                )
            } else {
                error
            }
        });
    pool.put(sess_ctx);
    result
}

/// One planned action of Go master's `executor.AlterMaterializedView`.
pub(crate) enum AlterMaterializedViewStep {
    /// COMMENT: the view's own ALTER TABLE (`alterTable(..., true)`).
    Comment(tidb_ast::AlterTableStmt),
    /// `alterMaterializedViewRefresh`: the job, then the schedule row.
    Refresh { job: crate::JobWrapper, start_with: String, next: String, sql_mode: i64, mview_id: i64 },
    /// `alterMaterializedViewAttributes`.
    Attributes(crate::JobWrapper),
}

/// Go master `executor.AlterMaterializedView`'s checks and the work of its
/// actions, in statement order.
pub(crate) fn alter_materialized_view_steps(
    catalog: &SessionExtendedInfoSchema,
    stmt: &tidb_ast::AlterMaterializedViewStmt,
    current_db: &str,
    context: &dyn DdlSessionContext,
) -> Result<Vec<AlterMaterializedViewStep>, MysqlError> {
    let (schema_name, view) = match stmt.view_name.as_slice() {
        [schema, view] => (schema.clone(), view.clone()),
        [view] => {
            if current_db.is_empty() {
                return Err(MysqlError::from_terror(&tidb_plannererrors::ERR_NO_DB.gen_with_stack_by_args(&[])));
            }
            (current_db.to_owned(), view.clone())
        }
        _ => return Err(MysqlError::unknown("invalid materialized view name".to_owned())),
    };
    let Some(schema) = catalog.schema_by_name(&schema_name) else {
        return Err(MysqlError::from_terror(
            &tidb_infoschema::infoschema_errors::ERR_DATABASE_NOT_EXISTS.gen_with_stack_by_args(&[schema_name.into()]),
        ));
    };
    let schema = schema.read().clone();
    let Some((_, table)) = catalog.table_info_by_name(&schema_name, &view) else {
        return Err(MysqlError::from_terror(
            &tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                .gen_with_stack_by_args(&[schema_name.into(), view.into()]),
        ));
    };
    let table = table.read().clone_like_go();
    if table.materialized_view.is_none() {
        return Err(MysqlError::from_terror(&tidb_dbterror::ERR_WRONG_OBJECT.gen_with_stack_by_args(&[
            schema_name.clone().into(),
            view.clone().into(),
            "MATERIALIZED VIEW".into(),
        ])));
    }
    let expr_ctx = SessionExprContext(context);
    // Go's `validateCommentLength(..., &action.Comment, ...)` truncates a
    // too-long comment in place outside strict mode.
    let mut validated_comments = Vec::new();
    for action in &stmt.actions {
        match action {
            tidb_ast::AlterMaterializedViewAction::Comment(comment) => {
                validated_comments.push(tidb_ddl_build::validate_comment_length(
                    comment,
                    &tidb_hack::go_to_lower(&view),
                    tidb_ddl_build::CommentOwner::Table,
                    &context.metabuild_context(),
                )?);
            }
            tidb_ast::AlterMaterializedViewAction::Refresh(refresh) => {
                tidb_ddl_build::materialized_view::build_mview_refresh_meta(&expr_ctx, Some(refresh))?;
            }
            tidb_ast::AlterMaterializedViewAction::Attributes(attributes) => {
                tidb_ddl_build::materialized_view::parse_mview_attributes(attributes)?;
            }
        }
    }
    let job_for = |action: ActionType| {
        let mut job = crate::cluster_ddl::session_job(context, action);
        job.schema_id = schema.id;
        job.table_id = table.id;
        job.schema_name = schema.name.lowercase().to_owned().into();
        job.table_name = tidb_hack::go_to_lower(&view).into();
        job
    };
    let mut steps = Vec::new();
    let mut validated_comments = validated_comments.into_iter();
    for action in &stmt.actions {
        match action {
            // Go hands the checked (possibly truncated) comment to
            // `alterTable`, which checks its length again.
            tidb_ast::AlterMaterializedViewAction::Comment(_) => {
                let comment = validated_comments.next().expect("one checked comment per COMMENT action");
                steps.push(AlterMaterializedViewStep::Comment(tidb_ast::AlterTableStmt {
                    name: vec![schema_name.clone(), view.clone()],
                    actions: vec![tidb_ast::AlterTableAction::SetTableOptions {
                        options: vec![tidb_ast::TableOption::Comment(comment)],
                    }],
                }));
            }
            tidb_ast::AlterMaterializedViewAction::Refresh(refresh) => {
                let (refresh_method, refresh_start_with, refresh_next) =
                    tidb_ddl_build::materialized_view::build_mview_refresh_meta(&expr_ctx, Some(refresh))?;
                let sql_mode = context.ddl_sql_mode();
                let args = tidb_model::AlterMaterializedViewRefreshArgs {
                    refresh_method,
                    refresh_start_with: refresh_start_with.clone(),
                    refresh_next: refresh_next.clone(),
                    refresh_schedule_sql_mode: sql_mode,
                    update_refresh_schedule: refresh.start_with.is_some() || refresh.next.is_some(),
                };
                steps.push(AlterMaterializedViewStep::Refresh {
                    job: crate::JobWrapper {
                        job: job_for(ActionType::ACTION_ALTER_MATERIALIZED_VIEW_REFRESH),
                        args: JobArgsValue::AlterMaterializedViewRefresh(Some(GoShared::new(args))),
                        id_allocated: true,
                    },
                    start_with: refresh_start_with,
                    next: refresh_next,
                    sql_mode,
                    mview_id: table.id,
                });
            }
            tidb_ast::AlterMaterializedViewAction::Attributes(attributes) => {
                let (alert_warning_sec, alert_overdue_sec, alert_refresh_failed) =
                    tidb_ddl_build::materialized_view::parse_mview_attributes(attributes)?;
                let args = tidb_model::AlterMaterializedViewAttributesArgs {
                    alert_warning_sec,
                    alert_overdue_sec,
                    alert_refresh_failed,
                };
                steps.push(AlterMaterializedViewStep::Attributes(crate::JobWrapper {
                    job: job_for(ActionType::ACTION_ALTER_MATERIALIZED_VIEW_ATTRIBUTES),
                    args: JobArgsValue::AlterMaterializedViewAttributes(Some(GoShared::new(args))),
                    id_allocated: true,
                }));
            }
        }
    }
    Ok(steps)
}

/// Go master `executor.CreateMaterializedViewShadowTable` up to
/// `DoDDLJobWrapper`: the shadow table's create job, named for the refresh
/// it serves (`REFRESH MATERIALIZED VIEW <schema>.<view> COMPLETE OUT OF
/// PLACE` as its query) and sharing the source view.
pub(crate) fn create_materialized_view_shadow_table_job(
    catalog: &SessionExtendedInfoSchema,
    schema_id: i64,
    schema_name: &str,
    shadow: tidb_model::TableInfo,
    context: &dyn DdlSessionContext,
) -> Result<crate::JobWrapper, MysqlError> {
    let invalid = |message: &str| {
        MysqlError::from_terror(&tidb_dbterror::ERR_INVALID_DDL_JOB.gen_with_stack_by_args(&[message.to_owned().into()]))
    };
    let Some(source_mview_id) = shadow.materialized_view_shadow.as_ref().map(|info| info.read().source_mview_id) else {
        return Err(invalid("create materialized view shadow table: invalid shadow metadata"));
    };
    if source_mview_id == 0 {
        return Err(invalid("create materialized view shadow table: invalid source materialized view id"));
    }
    let schema_l = tidb_hack::go_to_lower(schema_name);
    let involving = |table: &str, mode: tidb_model::InvolvingSchemaInfoMode| tidb_model::InvolvingSchemaInfo {
        database: schema_l.clone().into(),
        table: table.to_owned().into(),
        mode,
        ..tidb_model::InvolvingSchemaInfo::default()
    };
    let mut involving_schemas = vec![involving(shadow.name.lowercase(), tidb_model::InvolvingSchemaInfoMode::EXCLUSIVE)];
    let mut refresh_target = shadow.name.original().to_owned();
    if let Some(source) = catalog.table_info_by_id(source_mview_id) {
        let source = source.read();
        refresh_target = source.name.original().to_owned();
        involving_schemas.push(involving(source.name.lowercase(), tidb_model::InvolvingSchemaInfoMode::SHARED));
    }
    let mut job = crate::cluster_ddl::session_job(context, ActionType::ACTION_CREATE_MATERIALIZED_VIEW_SHADOW);
    job.query = tidb_sqlescape::escape_sql_text(
        "REFRESH MATERIALIZED VIEW %n.%n COMPLETE OUT OF PLACE",
        &[
            tidb_sqlescape::SqlArg::String(schema_name.as_bytes()),
            tidb_sqlescape::SqlArg::String(refresh_target.as_bytes()),
        ],
    )
    .map_err(|error| MysqlError::unknown(error.to_string()))?
    .into();
    job.schema_id = schema_id;
    job.schema_name = schema_l.clone().into();
    job.table_name = shadow.name.lowercase().to_owned().into();
    job.involving_schema_info = GoSharedSlice::from_vec(involving_schemas);
    job.session_vars = Some(GoShared::new(std::collections::BTreeMap::from([(
        tidb_vardef::tidb_vars::TIDB_SCATTER_REGION.to_owned(),
        context.ddl_reorg_vars().scatter_region.clone().into(),
    )])));
    let args = tidb_model::CreateTableArgs {
        table_info: tidb_model::GoField::new(Some(GoShared::new(shadow))),
        fk_check: tidb_model::GoField::new(context.foreign_key_checks()),
        ..Default::default()
    };
    Ok(crate::JobWrapper { job, args: JobArgsValue::CreateTable(Some(GoShared::new(args))), id_allocated: false })
}

/// Go master `buildMViewRefreshOutOfPlaceCutoverInvolvingSchemaInfo`: every
/// table the cutover rewrites, exclusive -- the view, its sources, the shadow
/// table, the source logs that list the view as a dependent, the view's own
/// log and its dependent views.
fn build_mview_refresh_out_of_place_cutover_involving_schema_info(
    catalog: &SessionExtendedInfoSchema,
    schema_name: &str,
    old_mview_id: i64,
    shadow_table_id: i64,
) -> Result<Vec<tidb_model::InvolvingSchemaInfo>, MysqlError> {
    let invalid = |message: String| {
        MysqlError::from_terror(&tidb_dbterror::ERR_INVALID_DDL_JOB.gen_with_stack_by_args(&[message.into()]))
    };
    let mut involving: Vec<tidb_model::InvolvingSchemaInfo> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    let mut add_table = |id: i64, involving: &mut Vec<tidb_model::InvolvingSchemaInfo>| -> Result<tidb_model::TableInfo, MysqlError> {
        let info = catalog.table_info_by_id(id).map(|table| table.read().clone()).ok_or_else(|| {
            invalid(format!("refresh materialized view complete OUT OF PLACE cutover: table {id} not found"))
        })?;
        if seen.insert(id) {
            let Some(db) = catalog.schema_name_by_table_id(id) else {
                return Err(MysqlError::from_terror(
                    &tidb_infoschema::infoschema_errors::ERR_DATABASE_NOT_EXISTS
                        .gen_with_stack_by_args(&[schema_name.to_owned().into()]),
                ));
            };
            involving.push(tidb_model::InvolvingSchemaInfo {
                database: tidb_hack::go_to_lower(&db).into(),
                table: info.name.lowercase().to_owned().into(),
                ..tidb_model::InvolvingSchemaInfo::default()
            });
        }
        Ok(info)
    };
    let old = add_table(old_mview_id, &mut involving)?;
    let Some(old_mview) = old.materialized_view.as_ref().map(|info| info.read().clone()) else {
        return Err(invalid(
            "refresh materialized view complete OUT OF PLACE cutover: old materialized view metadata missing".to_owned(),
        ));
    };
    let mut input_mlog_ids = Vec::new();
    for &id in old_mview.base_table_ids.iter() {
        let base = add_table(id, &mut involving)?;
        let mlog_id = base.materialized_view_base.as_ref().map_or(0, |info| info.read().mlog_id);
        if mlog_id != 0 {
            input_mlog_ids.push(mlog_id);
        }
    }
    add_table(shadow_table_id, &mut involving)?;
    for id in input_mlog_ids {
        let log = catalog.table_info_by_id(id).map(|table| table.read().clone()).ok_or_else(|| {
            invalid(format!("refresh materialized view complete OUT OF PLACE cutover: table {id} not found"))
        })?;
        // "Cutover only rewrites source logs that list the old MV as a
        // dependent."
        let feeds = log
            .materialized_view_log
            .as_ref()
            .is_some_and(|info| info.read().dependent_mview_ids.iter().any(|&mview| mview == old_mview_id));
        if !feeds {
            continue;
        }
        add_table(id, &mut involving)?;
    }
    if let Some(base) = old.materialized_view_base.as_ref().map(|info| info.read().clone()) {
        if base.mlog_id != 0 {
            add_table(base.mlog_id, &mut involving)?;
        }
        for &id in base.mview_ids.iter() {
            add_table(id, &mut involving)?;
        }
    }
    Ok(involving)
}

/// Go master `executor.RefreshMaterializedViewCompleteOutOfPlaceCutover` up
/// to `doDDLJob2`: the job that swaps the built shadow table in under the
/// view's name and moves its refresh metadata.
pub(crate) fn refresh_materialized_view_complete_out_of_place_cutover_job(
    catalog: &SessionExtendedInfoSchema,
    schema_id: i64,
    schema_name: &str,
    view_name: &str,
    args: tidb_model::RefreshMaterializedViewCompleteOutOfPlaceCutoverArgs,
    context: &dyn DdlSessionContext,
) -> Result<crate::JobWrapper, MysqlError> {
    let involving =
        build_mview_refresh_out_of_place_cutover_involving_schema_info(catalog, schema_name, args.old_mview_id, args.shadow_table_id)?;
    // Go: the view's own log, which the cutover replaces (its new ID is
    // allocated at submit, `assignGIDsForJobs`).
    let mut args = args;
    args.old_mlog_id = catalog
        .table_info_by_id(args.old_mview_id)
        .and_then(|table| table.read().materialized_view_base.as_ref().map(|base| base.read().mlog_id))
        .unwrap_or(0);
    let mut job = crate::cluster_ddl::session_job(context, ActionType::ACTION_MVIEW_REFRESH_OUT_OF_PLACE_CUTOVER);
    job.schema_id = schema_id;
    job.table_id = args.old_mview_id;
    job.schema_name = tidb_hack::go_to_lower(schema_name).into();
    job.table_name = tidb_hack::go_to_lower(view_name).into();
    job.involving_schema_info = GoSharedSlice::from_vec(involving);
    Ok(crate::JobWrapper {
        job,
        args: JobArgsValue::RefreshMaterializedViewCompleteOutOfPlaceCutover(Some(GoShared::new(args))),
        id_allocated: false,
    })
}

/// Go master `pkg/ddl/mview_worker_test.go`'s cutover involving-schema test.
#[cfg(test)]
mod mview_worker_tests {
    use tidb_model::table::{MaterializedViewBaseInfo, MaterializedViewInfo, MaterializedViewLogInfo, MaterializedViewShadowInfo};
    use tidb_model::{GoShared, SchemaState, TableInfo};

    /// Go `infoschema.MockInfoSchema(tables)`: the tables, public, in `test`.
    fn mock_info_schema(tables: Vec<TableInfo>) -> super::SessionExtendedInfoSchema {
        let mut db = tidb_model::db::DBInfo { id: 1, name: tidb_ast::CiString::new("test"), state: SchemaState::PUBLIC, ..Default::default() };
        for mut table in tables {
            table.state = SchemaState::PUBLIC;
            table.db_id = 1;
            db.deprecated_tables.push_go(table);
        }
        super::SessionExtendedInfoSchema::init_with_db_infos(vec![GoShared::new(db)], Vec::new(), Vec::new(), 1)
    }

    fn table(id: i64, name: &str) -> TableInfo {
        TableInfo { id, name: tidb_ast::CiString::new(name), ..Default::default() }
    }

    fn tables_of(involving: &[tidb_model::InvolvingSchemaInfo]) -> Vec<String> {
        involving
            .iter()
            .map(|info| {
                assert_eq!(info.database.to_string(), "test");
                assert_eq!(info.mode, tidb_model::InvolvingSchemaInfo::default().mode);
                info.table.to_string()
            })
            .collect()
    }

    #[test]
    fn test_build_m_view_refresh_out_of_place_cutover_involving_schema_info() {
        let mut base = table(1, "base");
        base.materialized_view_base = Some(GoShared::new(MaterializedViewBaseInfo { mlog_id: 2, mview_ids: vec![3].into(), ..Default::default() }));
        let mut mlog = table(2, "$mlog$base");
        mlog.materialized_view_log =
            Some(GoShared::new(MaterializedViewLogInfo { base_table_id: 1, dependent_mview_ids: vec![3].into(), ..Default::default() }));
        let mut old_mview = table(3, "mv");
        old_mview.materialized_view = Some(GoShared::new(MaterializedViewInfo { base_table_ids: vec![1].into(), ..Default::default() }));
        let mut shadow = table(4, "__mv_shadow");
        shadow.materialized_view_shadow = Some(GoShared::new(MaterializedViewShadowInfo { source_mview_id: 3, ..Default::default() }));
        let build = |tables: Vec<TableInfo>| {
            tables_of(&super::build_mview_refresh_out_of_place_cutover_involving_schema_info(&mock_info_schema(tables), "test", 3, 4).unwrap())
        };
        assert_eq!(build(vec![base.clone(), mlog.clone(), old_mview.clone(), shadow.clone()]), vec!["mv", "base", "__mv_shadow", "$mlog$base"]);

        // "A MLog that does not list the old MV is unrelated to this cutover
        // and is intentionally omitted from the involving-schema lock set."
        let mut unrelated = mlog.clone();
        unrelated.materialized_view_log =
            Some(GoShared::new(MaterializedViewLogInfo { base_table_id: 1, ..Default::default() }));
        let involving = build(vec![base.clone(), unrelated, old_mview.clone(), shadow.clone()]);
        assert_eq!(involving.len(), 3);
        assert!(!involving.contains(&"$mlog$base".to_owned()));

        // "A shared child can have another source. The cutover claim includes
        // the child once and every upstream source/log that the worker can
        // rewrite."
        let mut parent = old_mview.clone();
        parent.materialized_view = Some(GoShared::new(MaterializedViewInfo { base_table_ids: vec![1, 107].into(), ..Default::default() }));
        parent.materialized_view_base = Some(GoShared::new(MaterializedViewBaseInfo { mlog_id: 105, mview_ids: vec![106].into(), ..Default::default() }));
        let mut base_info = base.clone();
        base_info.materialized_view_base = Some(GoShared::new(MaterializedViewBaseInfo { mlog_id: 104, mview_ids: vec![3].into(), ..Default::default() }));
        let mut input_log = table(104, "input_log");
        input_log.materialized_view_log =
            Some(GoShared::new(MaterializedViewLogInfo { base_table_id: 1, dependent_mview_ids: vec![3].into(), ..Default::default() }));
        let mut shared_child = table(106, "shared_child");
        shared_child.materialized_view = Some(GoShared::new(MaterializedViewInfo { base_table_ids: vec![3, 1].into(), ..Default::default() }));
        let mut second_source = table(107, "second_source");
        second_source.materialized_view_base = Some(GoShared::new(MaterializedViewBaseInfo { mlog_id: 108, ..Default::default() }));
        let mut second_log = table(108, "second_log");
        second_log.materialized_view_log =
            Some(GoShared::new(MaterializedViewLogInfo { base_table_id: 107, dependent_mview_ids: vec![106].into(), ..Default::default() }));
        let mut involving = build(vec![parent, base_info, shadow, input_log, table(105, "output_log"), shared_child, second_source, second_log]);
        involving.sort();
        // "second_log serves another MV, so cutover does not rewrite or claim it."
        let mut expected = vec!["mv", "base", "__mv_shadow", "input_log", "output_log", "shared_child", "second_source"];
        expected.sort_unstable();
        assert_eq!(involving, expected);
    }

    #[test]
    fn test_build_drop_table_involving_schema_info() {
        use crate::cluster_ddl::drop_object::build_drop_table_involving_schema_info;
        let mut base1 = table(1, "base1");
        let base2 = table(2, "base2");
        let mut mv = table(3, "mv");
        mv.materialized_view = Some(GoShared::new(MaterializedViewInfo { base_table_ids: vec![1, 2].into(), ..Default::default() }));
        let mut mlog = table(4, "$mlog$base1");
        mlog.materialized_view_log = Some(GoShared::new(MaterializedViewLogInfo { base_table_id: 1, ..Default::default() }));
        base1.materialized_view_base = Some(GoShared::new(MaterializedViewBaseInfo { mlog_id: 4, ..Default::default() }));
        mlog.materialized_view_log.as_ref().unwrap().write().dependent_mview_ids = vec![3].into();
        // Go's MockInfoSchema shares the table pointers its test mutates;
        // here each state is a fresh schema of the same tables.
        let is = |tables: &[&TableInfo]| mock_info_schema(tables.iter().map(|t| (*t).clone_like_go()).collect());
        let build = |is: &super::SessionExtendedInfoSchema, info: &TableInfo| build_drop_table_involving_schema_info(is, "test", info);

        let involving = build(&is(&[&base1, &base2, &mv, &mlog]), &mv).unwrap();
        assert_eq!(tables_of(&involving), vec!["mv", "base1", "$mlog$base1", "base2"]);

        mlog.materialized_view_log.as_ref().unwrap().write().dependent_mview_ids = Vec::new().into();
        let involving = build(&is(&[&base1, &base2, &mv, &mlog]), &mv).unwrap();
        assert_eq!(tables_of(&involving), vec!["mv", "base1", "base2"]);

        base1.materialized_view_base.as_ref().unwrap().write().mlog_id = 99;
        let err = build(&is(&[&base1, &base2, &mv, &mlog]), &mv).unwrap_err();
        assert!(err.equal(&tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS), "{err:?}");
        base1.materialized_view_base.as_ref().unwrap().write().mlog_id = 4;

        // "A missing base table must not prevent dropping an orphaned MView."
        let schema = is(&[&base1, &mv, &mlog]);
        let involving = build(&schema, &mv).unwrap();
        assert_eq!(tables_of(&involving), vec!["mv", "base1"]);

        let involving = build(&schema, &mlog).unwrap();
        assert_eq!(tables_of(&involving), vec!["$mlog$base1", "base1"]);
    }
}
