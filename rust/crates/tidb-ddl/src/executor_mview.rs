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
    let add_columns = stmt
        .actions
        .iter()
        .filter(|action| matches!(action, tidb_ast::AlterMaterializedViewLogAction::AddColumn(_)))
        .count();
    if add_columns > 1 {
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
    if add_columns > 0 {
        // Go runs ADD COLUMN as `executor.AddColumn` of the log table with
        // the base column's log type; this node has no such job yet, so the
        // statement is refused before any of its jobs runs.
        return Err(MysqlError::from_terror(&tidb_dbterror::ERR_GENERAL_UNSUPPORTED_DDL.gen_with_stack_by_args(&[
            "ALTER MATERIALIZED VIEW LOG ADD COLUMN on this node".into(),
        ])));
    }
    let mut steps = Vec::new();
    for action in &stmt.actions {
        let tidb_ast::AlterMaterializedViewLogAction::Purge(purge) = action else {
            continue;
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
