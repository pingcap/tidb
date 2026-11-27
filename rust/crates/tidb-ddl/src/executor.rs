// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go `pkg/ddl/executor.go`: statement methods build jobs directly.

use super::*;
use crate::cluster_ddl::add_index::AddIndexSubmission;
use crate::cluster_ddl::{split_name, CreateTableBuild, DdlContext};
use tidb_infoschema::cluster_catalog::MetaSnapshot;
use crate::real_tikv_ddl::{check_flashback_cluster_job, ddl_transaction_options};
use tidb_ddl_build::session_context::DdlSessionContext;
use tidb_infoschema_issyncer::real_tikv_catalog::SnapshotMetaSnapshot;
use tidb_store_driver_txn::cluster_table_storage::SessionTransaction;

/// Go `executor.CreateTable`: build the table (or its LIKE copy), then submit.
pub fn create_table<E: ExecutorStore>(
    executor: &E,
    statement: &tidb_ast::CreateTableStmt,
    current_db: &str,
    context: &DdlContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    if statement.temporary == tidb_ast::CreateTableTemporary::Local {
        return Err(SqlQueryError::unknown(
            "LOCAL temporary-table DDL belongs to the session catalog",
        ));
    }
    let (schema, table) = split_name(&statement.name, current_db, "table")
        .map_err(|e| SqlQueryError::from(e.to_mysql_error()))?;
    let tiflash = || ExecutorStore::tiflash_store_count(executor);
    if let Some(source) = &statement.like_table {
        let (source_schema, source_table) = split_name(source, current_db, "table")
            .map_err(|e| SqlQueryError::from(e.to_mysql_error()))?;
        return executor.with_meta(context.as_ref(), |snapshot, start_ts, upgrading| {
            crate::cluster_ddl::create_object::create_table_like(
                snapshot,
                &schema,
                &table,
                &source_schema,
                &source_table,
                statement.temporary,
                statement.on_commit_delete,
                statement.if_not_exists,
                context.as_ref(),
                &tiflash,
                start_ts,
                upgrading,
            )
        });
    }
    let build = CreateTableBuild::new(statement, &schema, context)
        .map_err(|e| SqlQueryError::from(e.to_mysql_error()))?;
    executor.with_meta(context.as_ref(), |snapshot, start_ts, upgrading| {
        crate::cluster_ddl::create_table::create_table_job(
            snapshot,
            &schema,
            &table,
            statement.if_not_exists,
            &build,
            context.as_ref(),
            &tiflash,
            start_ts,
            upgrading,
        )
    })
}

/// Go `executor.CreateSequence`.
pub fn create_sequence<E: ExecutorStore>(
    executor: &E,
    statement: &tidb_ast::CreateSequenceStmt,
    current_db: &str,
    context: &DdlContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    let (schema, name) = split_name(&statement.name, current_db, "sequence")
        .map_err(|e| SqlQueryError::from(e.to_mysql_error()))?;
    let (sequence, comment) =
        tidb_ddl_build::sequence::build_create_sequence(statement, &schema, &name)
            .map_err(SqlQueryError::from)?;
    let info = crate::cluster_ddl::build_sequence_table_info(&name, sequence, comment);
    let tiflash = || ExecutorStore::tiflash_store_count(executor);
    executor.with_meta(context.as_ref(), |snapshot, start_ts, upgrading| {
        crate::cluster_ddl::create_object::create_sequence(
            snapshot,
            &schema,
            statement.if_not_exists,
            &info,
            context.as_ref(),
            &tiflash,
            start_ts,
            upgrading,
        )
    })
}

/// Go `executor.AlterSequence`.
pub fn alter_sequence<E: ExecutorStore>(
    executor: &E,
    statement: &tidb_ast::AlterSequenceStmt,
    current_db: &str,
    context: &DdlContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    let (schema, name) = split_name(&statement.name, current_db, "sequence")
        .map_err(|e| SqlQueryError::from(e.to_mysql_error()))?;
    executor.with_meta(context.as_ref(), |snapshot, start_ts, upgrading| {
        crate::cluster_ddl::create_object::alter_sequence(
            snapshot,
            &schema,
            &name,
            statement.if_exists,
            &statement.options,
            context.as_ref(),
            start_ts,
            upgrading,
        )
    })
}

/// Go `executor.TruncateTable`.
pub fn truncate_table<E: ExecutorStore>(
    executor: &E,
    name: &[String],
    current_db: &str,
    context: &DdlContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    let (schema, table) = split_name(name, current_db, "table")
        .map_err(|e| SqlQueryError::from(e.to_mysql_error()))?;
    executor.with_meta(context.as_ref(), |snapshot, start_ts, upgrading| {
        crate::cluster_ddl::truncate::truncate_table_by_name(
            snapshot,
            &schema,
            &table,
            context.as_ref(),
            start_ts,
            upgrading,
        )
    })
}

/// Go `executor.RepairTable`.
pub(super) fn repair_table<S: StoreClient>(
    executor: &RealClusterDdl<S>,
    statement: &tidb_ast::RepairTableStmt,
    current_db: &str,
    context: &DdlContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    let (schema, table) = split_name(&statement.table, current_db, "table")
        .map_err(|e| SqlQueryError::from(e.to_mysql_error()))?;
    let (create_schema, _) = split_name(&statement.create.name, current_db, "table")
        .map_err(|e| SqlQueryError::from(e.to_mysql_error()))?;
    let build = CreateTableBuild::new(&statement.create, &create_schema, context)
        .map_err(|e| SqlQueryError::from(e.to_mysql_error()))?;
    let tiflash = || ExecutorStore::tiflash_store_count(executor);
    let report = executor.with_meta(context.as_ref(), |snapshot, start_ts, upgrading| {
        crate::cluster_ddl::repair_table::repair_table_with_info(
            snapshot,
            &schema,
            &table,
            &build,
            context.as_ref(),
            &tiflash,
            start_ts,
            upgrading,
        )
    })?;
    tidb_domainutil::REPAIR_INFO.remove_from_repair_info(
        tidb_ast::CiString::new(schema).lowercase(),
        tidb_ast::CiString::new(table).lowercase(),
    );
    Ok(report)
}

/// Go master `executor.CreateMaterializedViewLog`: the job, then
/// `createTableWithInfoPost` for the log table.
pub(super) fn create_materialized_view_log<S: StoreClient>(
    executor: &RealClusterDdl<S>,
    stmt: &tidb_ast::CreateMaterializedViewLogStmt,
    current_db: &str,
    context: &dyn DdlSessionContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    let (report, schema, name) =
        match if_not_exists_note(stmt.if_not_exists, submit_create_materialized_view_log(executor, stmt, current_db, context)) {
            Ok(Ok(submitted)) => submitted,
            Ok(Err(satisfied)) => return Ok(satisfied),
            Err(error) => return Err(error),
        };
    if matches!(report, ClusterDdlReport::Applied { .. }) {
        after_create_tables(executor, &schema, &[name], context)?;
    }
    Ok(report)
}

/// Go master `executor.DropMaterializedViewLog`: the log's checks, then
/// `dropTableObject` of the log table (`materializedViewLogObject`).
pub fn drop_materialized_view_log<E: ExecutorStore>(
    executor: &E,
    stmt: &tidb_ast::DropMaterializedViewLogStmt,
    current_db: &str,
    context: &DdlContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    let catalog = executor.latest_schema();
    let (schema, mlog) =
        match super::executor_mview::drop_materialized_view_log_target(&catalog, stmt, current_db, context.as_ref())
            .map_err(mysql_query_error)?
        {
            Ok(target) => target,
            Err(missing) => return Ok(drop_missing_note("the materialized view log does not exist", &missing)),
        };
    let result = executor.with_meta(context.as_ref(), |snapshot, start_ts, upgrading| {
        crate::cluster_ddl::drop_object::drop_materialized_view_object(
            snapshot,
            tidb_model::ActionType::ACTION_DROP_MATERIALIZED_VIEW_LOG,
            &schema,
            &mlog,
            context.as_ref(),
            start_ts,
            upgrading,
        )
    });
    match result {
        Err(error)
            if stmt.if_exists
                && error.code
                    == tidb_infoschema::infoschema_errors::ERR_TABLE_DROP_EXISTS.to_sql_error().code =>
        {
            Ok(ClusterDdlReport::AlreadySatisfied {
                detail: "the materialized view log does not exist".into(),
                warnings: vec![crate::real_tikv_ddl::ddl_note(
                    tidb_infoschema::infoschema_errors::ERR_TABLE_DROP_EXISTS
                        .gen_with_stack_by_args(&[format!("{schema}.{mlog}").into()]),
                )],
            })
        }
        other => other,
    }
}

/// Go master `executor.AlterMaterializedViewLog`: the checks, then each
/// action's job; a purge action then stores the log's next purge time
/// (`alterMaterializedViewLogPurge`).
pub(super) fn alter_materialized_view_log<S: StoreClient>(
    executor: &RealClusterDdl<S>,
    stmt: &tidb_ast::AlterMaterializedViewLogStmt,
    current_db: &str,
    context: &DdlContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    let catalog = executor.latest_schema();
    let steps = super::executor_mview::alter_materialized_view_log_steps(&catalog, stmt, current_db, context.as_ref())
        .map_err(mysql_query_error)?;
    let mut last = None;
    let mut warnings = Vec::new();
    for step in steps {
        let report = match step {
            super::executor_mview::AlterMaterializedViewLogStep::Purge { job, start_with, next, sql_mode, mlog_id } => {
                let report = do_ddl_job_wrapper(executor, context.as_ref(), job)?;
                // Go keeps the metadata change and the schedule row
                // decoupled: an error below does not undo the finished job.
                let (next_unix_seconds, should_update) =
                    tidb_ddl_build::materialized_view::derive_materialized_schedule_next_unix_seconds(
                        &start_with,
                        &next,
                        sql_mode,
                        chrono::Utc::now(),
                    )
                    .map_err(mysql_query_error)?;
                if should_update {
                    super::executor_mview::best_effort_update_materialized_schedule_info_next_unix_seconds(
                        executor.scheduler.session_pool.as_ref(),
                        &super::executor_mview::MLOG_PURGE_INFO_UPDATE_SPEC,
                        mlog_id,
                        next_unix_seconds,
                        &mut warnings,
                    )
                    .map_err(mysql_query_error)?;
                }
                report
            }
            super::executor_mview::AlterMaterializedViewLogStep::AddColumns { schema, mlog, base_columns, after } => {
                let store_count = executor.store_count();
                executor.with_meta(context.as_ref(), |snapshot, start_ts, upgrading| {
                    crate::cluster_ddl::add_materialized_view_log_columns(
                        snapshot,
                        &schema,
                        &mlog,
                        &base_columns,
                        after.as_deref(),
                        context,
                        start_ts,
                        upgrading,
                        store_count,
                    )
                })?
            }
        };
        last = Some(report);
    }
    let mut report = last.expect("a statement has at least one action");
    match &mut report {
        ClusterDdlReport::Applied { warnings: report_warnings, .. }
        | ClusterDdlReport::AlreadySatisfied { warnings: report_warnings, .. } => report_warnings.extend(warnings),
    }
    Ok(report)
}

/// Go master `executor.CreateMaterializedView`: the checks, the SELECT's
/// output types, the job, then `createTableWithInfoPost` for the view.
pub(super) fn create_materialized_view<S: StoreClient>(
    executor: &RealClusterDdl<S>,
    stmt: &tidb_ast::CreateMaterializedViewStmt,
    current_db: &str,
    context: &DdlContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    let submitted = (|| {
        let catalog = executor.latest_schema();
        let plan = super::executor_mview::create_materialized_view_plan(&catalog, stmt, current_db, context.as_ref())
            .map_err(mysql_query_error)?;
        let field_types = super::executor_mview::materialized_view_query_output_types(
            executor.scheduler.session_pool.as_ref(),
            &plan.select_sql,
            context.as_ref(),
        )
        .map_err(mysql_query_error)?;
        let schema = plan.schema.name.original().to_owned();
        let job = super::executor_mview::create_materialized_view_job(plan, stmt, field_types, context.as_ref())
            .map_err(mysql_query_error)?;
        let name = job.job.table_name.to_string();
        let report = do_ddl_job_wrapper(executor, context.as_ref(), job)?;
        if matches!(report, ClusterDdlReport::Applied { .. }) {
            after_create_tables(executor, &schema, &[name], context.as_ref())?;
        }
        Ok(report)
    })();
    match if_not_exists_note(stmt.if_not_exists, submitted)? {
        Ok(report) | Err(report) => Ok(report),
    }
}

/// Go master `executor.CreateMaterializedViewShadowTable`: the shadow
/// table's job, then `createTableWithInfoPost` for it.
pub(super) fn create_materialized_view_shadow_table<S: StoreClient>(
    executor: &RealClusterDdl<S>,
    schema_id: i64,
    schema_name: &str,
    shadow: tidb_model::TableInfo,
    context: &dyn DdlSessionContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    let catalog = executor.latest_schema();
    let name = shadow.name.original().to_owned();
    let job = super::executor_mview::create_materialized_view_shadow_table_job(&catalog, schema_id, schema_name, shadow, context)
        .map_err(mysql_query_error)?;
    let report = do_ddl_job_wrapper(executor, context, job)?;
    if matches!(report, ClusterDdlReport::Applied { .. }) {
        after_create_tables(executor, schema_name, &[name], context)?;
    }
    Ok(report)
}

/// Go master `executor.DropMaterializedViewShadowTable`: `dropTableObject`
/// of the shadow table (`materializedViewShadowObject`, IF EXISTS). The
/// caller's context carries Go's query, `DROP TABLE IF EXISTS
/// <schema>.<shadow>`.
pub fn drop_materialized_view_shadow_table<E: ExecutorStore>(
    executor: &E,
    schema_name: &str,
    shadow_name: &str,
    context: &dyn DdlSessionContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    let result = executor.with_meta(context, |snapshot, start_ts, upgrading| {
        crate::cluster_ddl::drop_object::drop_materialized_view_object(
            snapshot,
            tidb_model::ActionType::ACTION_DROP_MATERIALIZED_VIEW_SHADOW,
            schema_name,
            shadow_name,
            context,
            start_ts,
            upgrading,
        )
    });
    match result {
        Err(error)
            if error.code == tidb_infoschema::infoschema_errors::ERR_TABLE_DROP_EXISTS.to_sql_error().code =>
        {
            Ok(drop_missing_note("the shadow table does not exist", &format!("{schema_name}.{shadow_name}")))
        }
        other => other,
    }
}

/// Go master `executor.RefreshMaterializedViewCompleteOutOfPlaceCutover`.
pub(super) fn refresh_materialized_view_complete_out_of_place_cutover<S: StoreClient>(
    executor: &RealClusterDdl<S>,
    schema_id: i64,
    schema_name: &str,
    view_name: &str,
    args: tidb_model::RefreshMaterializedViewCompleteOutOfPlaceCutoverArgs,
    context: &dyn DdlSessionContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    let catalog = executor.latest_schema();
    let job = super::executor_mview::refresh_materialized_view_complete_out_of_place_cutover_job(
        &catalog, schema_id, schema_name, view_name, args, context,
    )
    .map_err(mysql_query_error)?;
    do_ddl_job_wrapper(executor, context, job)
}

/// Go `ErrTableDropExists` as the IF EXISTS note of a missing `qualified`.
/// Go master's IF NOT EXISTS for CREATE MATERIALIZED VIEW [LOG]: an existing
/// table, found by the precheck or by the job ("A concurrent creator may have
/// won after the existence precheck"), is a note.
fn if_not_exists_note<T>(
    if_not_exists: bool,
    result: Result<T, SqlQueryError>,
) -> Result<Result<T, ClusterDdlReport>, SqlQueryError> {
    match result {
        Err(error) if if_not_exists && error.code == tidb_errno::errcode::ErrTableExists => {
            Ok(Err(ClusterDdlReport::AlreadySatisfied {
                detail: "the table already exists".into(),
                warnings: vec![crate::real_tikv_ddl::ddl_note(
                    tidb_infoschema::infoschema_errors::ERR_TABLE_EXISTS.generate(error.message),
                )],
            }))
        }
        other => other.map(Ok),
    }
}

fn drop_missing_note(detail: &str, qualified: &str) -> ClusterDdlReport {
    ClusterDdlReport::AlreadySatisfied {
        detail: detail.into(),
        warnings: vec![crate::real_tikv_ddl::ddl_note(
            tidb_infoschema::infoschema_errors::ERR_TABLE_DROP_EXISTS.gen_with_stack_by_args(&[qualified.to_owned().into()]),
        )],
    }
}

/// Go master `executor.DropMaterializedView`: the view's checks, then
/// `dropTableObject` of the view (`materializedViewObject`).
pub fn drop_materialized_view<E: ExecutorStore>(
    executor: &E,
    stmt: &tidb_ast::DropMaterializedViewStmt,
    current_db: &str,
    context: &DdlContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    let catalog = executor.latest_schema();
    let (schema, mview) = match super::executor_mview::drop_materialized_view_target(&catalog, stmt, current_db)
        .map_err(mysql_query_error)?
    {
        Ok(target) => target,
        Err(missing) => return Ok(drop_missing_note("the materialized view does not exist", &missing)),
    };
    executor.with_meta(context.as_ref(), |snapshot, start_ts, upgrading| {
        crate::cluster_ddl::drop_object::drop_materialized_view_object(
            snapshot,
            tidb_model::ActionType::ACTION_DROP_MATERIALIZED_VIEW,
            &schema,
            &mview,
            context.as_ref(),
            start_ts,
            upgrading,
        )
    })
}

/// Go master `executor.AlterMaterializedView`: the view's checks, every
/// action validated, then each in order (COMMENT as the view's ALTER TABLE,
/// REFRESH and ATTRIBUTES as their jobs, a refresh followed by its schedule
/// row).
pub(super) fn alter_materialized_view<S: StoreClient>(
    executor: &RealClusterDdl<S>,
    stmt: &tidb_ast::AlterMaterializedViewStmt,
    current_db: &str,
    context: &DdlContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    let catalog = executor.latest_schema();
    let steps = super::executor_mview::alter_materialized_view_steps(&catalog, stmt, current_db, context.as_ref())
        .map_err(mysql_query_error)?;
    let mut last = None;
    let mut warnings = Vec::new();
    for step in steps {
        let report = match step {
            super::executor_mview::AlterMaterializedViewStep::Comment(alter) => {
                alter_table_allowing(executor, &alter, current_db, context, true)?
            }
            super::executor_mview::AlterMaterializedViewStep::Attributes(job) => {
                do_ddl_job_wrapper(executor, context.as_ref(), job)?
            }
            super::executor_mview::AlterMaterializedViewStep::Refresh { job, start_with, next, sql_mode, mview_id } => {
                let report = do_ddl_job_wrapper(executor, context.as_ref(), job)?;
                let (next_unix_seconds, should_update) =
                    tidb_ddl_build::materialized_view::derive_materialized_schedule_next_unix_seconds(
                        &start_with,
                        &next,
                        sql_mode,
                        chrono::Utc::now(),
                    )
                    .map_err(mysql_query_error)?;
                if should_update {
                    let updated = super::executor_mview::best_effort_update_materialized_schedule_info_next_unix_seconds(
                        executor.scheduler.session_pool.as_ref(),
                        &super::executor_mview::MVIEW_REFRESH_INFO_UPDATE_SPEC,
                        mview_id,
                        next_unix_seconds,
                        &mut warnings,
                    )
                    .map_err(mysql_query_error)?;
                    if updated && next_unix_seconds.is_none() {
                        // Go logs a failure here and goes on.
                        let _ = super::executor_mview::delete_materialized_view_refresh_alert(
                            executor.scheduler.session_pool.as_ref(),
                            mview_id,
                        );
                    }
                }
                report
            }
        };
        last = Some(report);
    }
    let mut report = last.expect("a statement has at least one action");
    match &mut report {
        ClusterDdlReport::Applied { warnings: report_warnings, .. }
        | ClusterDdlReport::AlreadySatisfied { warnings: report_warnings, .. } => report_warnings.extend(warnings),
    }
    Ok(report)
}

/// Go master `executor.CreateMaterializedViewLog` through `DoDDLJobWrapper`:
/// the report, and the log table's schema and name.
pub fn submit_create_materialized_view_log<E: ExecutorStore>(
    executor: &E,
    stmt: &tidb_ast::CreateMaterializedViewLogStmt,
    current_db: &str,
    context: &dyn DdlSessionContext,
) -> Result<(ClusterDdlReport, String, String), SqlQueryError> {
    let catalog = executor.latest_schema();
    let (job, schema) = super::executor_mview::create_materialized_view_log_job(&catalog, stmt, current_db, context)
        .map_err(mysql_query_error)?;
    let name = job.job.table_name.to_string();
    let report = do_ddl_job_wrapper(executor, context, job)?;
    Ok((report, schema, name))
}

/// Go `executor.CreateSchema` through `CreateSchemaWithInfo`.
pub fn create_schema<E: ExecutorStore>(
    executor: &E,
    name: &str,
    if_not_exists: bool,
    options: &[tidb_ast::DatabaseOption],
    context: &dyn DdlSessionContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    let (charset, collate) = crate::cluster_ddl::database_charset_collation(options)
        .map_err(|error| SqlQueryError::from(error.to_mysql_error()))?;
    let policy = options.iter().rev().find_map(|option| match option {
        tidb_ast::DatabaseOption::PlacementPolicy(policy) => Some(policy.as_str()),
        _ => None,
    });
    let on_exist = if if_not_exists {
        crate::OnExist::Ignore
    } else {
        crate::OnExist::Error
    };
    create_schema_with_info(
        executor,
        crate::cluster_ddl::create_schema::build_db_info(name, &charset, &collate, policy),
        on_exist,
        context,
    )
}

/// Go `executor.CreateTableWithInfo`.
pub(super) fn create_table_with_info<S: StoreClient>(
    executor: &RealClusterDdl<S>,
    db_name: &str,
    info: tidb_model::TableInfo,
    involving_ref: &[tidb_model::InvolvingSchemaInfo],
    config: crate::CreateTableConfig,
    context: &dyn DdlSessionContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    let tiflash = || ExecutorStore::tiflash_store_count(executor);
    let name = info.name.original().to_owned();
    let result = executor.with_meta(context, |snapshot, start_ts, upgrading| {
        crate::cluster_ddl::create_object::create_table_with_info(
            snapshot,
            db_name,
            info,
            involving_ref,
            config,
            context,
            &tiflash,
            start_ts,
            upgrading,
        )
    });
    // Table exists, but the if-not-exists flag is true: the error is ignored.
    let report = ignore_exists(
        result,
        config.on_exist,
        &tidb_infoschema::infoschema_errors::ERR_TABLE_EXISTS,
    )?;
    if matches!(report, ClusterDdlReport::Applied { .. }) {
        after_create_tables(executor, db_name, &[name], context)?;
    }
    Ok(report)
}

/// Go `executor.BatchCreateTableWithInfo`.
pub(super) fn batch_create_table_with_info<S: StoreClient>(
    executor: &RealClusterDdl<S>,
    db_name: &str,
    infos: Vec<tidb_model::TableInfo>,
    config: crate::CreateTableConfig,
    context: &dyn DdlSessionContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    let tiflash = || ExecutorStore::tiflash_store_count(executor);
    let names: Vec<String> = infos
        .iter()
        .map(|info| info.name.original().to_owned())
        .collect();
    let result = executor.with_meta(context, |snapshot, start_ts, upgrading| {
        crate::cluster_ddl::create_object::batch_create_table_with_info(
            snapshot, db_name, infos, config, context, &tiflash, start_ts, upgrading,
        )
    });
    let report = ignore_exists(
        result,
        config.on_exist,
        &tidb_infoschema::infoschema_errors::ERR_TABLE_EXISTS,
    )?;
    if matches!(report, ClusterDdlReport::Applied { .. }) {
        after_create_tables(executor, db_name, &names, context)?;
    }
    Ok(report)
}

/// Go `CreateTableWithInfo` / `BatchCreateTableWithInfo` after a successful
/// job: `preSplitAndScatterTable` (when `split-table` is on and the store
/// splits), and under `StartMode::Br` `handleAutoIncID` in its own
/// transaction.
fn after_create_tables<S: StoreClient>(
    executor: &RealClusterDdl<S>,
    db_name: &str,
    names: &[String],
    context: &dyn DdlSessionContext,
) -> Result<(), SqlQueryError> {
    let catalog = executor.schema_sync.catalog.get_latest();
    let Some(database) = catalog.schema_by_name(db_name) else {
        return Ok(());
    };
    let schema_id = database.read().id;
    // Go `jobW.GetSystemVars(vardef.TiDBScatterRegion)`.
    let scatter_scope = context.ddl_reorg_vars().scatter_region.clone();
    let split = tidb_config::config_tree::config::get_global_config().split_table;
    let regions = executor.scheduler.region_store.get().cloned();
    for name in names {
        let Some((_, table)) = catalog.table_info_by_name(db_name, name) else {
            continue;
        };
        let table = table.read().clone();
        if let (true, Some(regions)) = (split, regions.as_ref()) {
            crate::pre_split::pre_split_and_scatter(regions.as_ref(), &table, &scatter_scope);
        }
        if executor.scheduler.start_mode == crate::StartMode::Br {
            let store = tidb_store_driver_txn::meta_txn::MetaStore::new(
                Arc::clone(&executor.opener),
                executor.timeout,
            );
            tidb_meta::MetaStore::run_in_new_txn(
                &store,
                &tidb_gocontext::Context::background(),
                tidb_txnkv::INTERNAL_TXN_DDL,
                true,
                &mut |txn| {
                    let meta = tidb_meta::transaction::Mutator::new(txn);
                    crate::cluster_ddl::handle_auto_inc_id(&meta, schema_id, &table)
                },
            )
            .map_err(SqlQueryError::unknown)?;
        }
    }
    Ok(())
}

/// Go `executor.CreatePlacementPolicyWithInfo`.
pub fn create_placement_policy_with_info<E: ExecutorStore>(
    executor: &E,
    policy: tidb_model::PolicyInfo,
    on_exist: crate::OnExist,
    context: &dyn DdlSessionContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    // Go `checkIgnorePlacementDDL`.
    if context.placement_mode_ignore() {
        return Ok(ClusterDdlReport::AlreadySatisfied {
            detail: "placement DDL ignored".into(),
            warnings: vec![crate::cluster_ddl::placement_ignored_note()],
        });
    }
    executor.with_meta(context, |snapshot, start_ts, upgrading| {
        crate::cluster_ddl::placement_policy::create_placement_policy_with_info(
            snapshot, policy, on_exist, context, start_ts, upgrading,
        )
    })
}

/// Go `executor.CreateSchemaWithInfo`: a job raced by another creating the
/// database answers its note under `OnExistIgnore`.
pub fn create_schema_with_info<E: ExecutorStore>(
    executor: &E,
    info: tidb_model::DBInfo,
    on_exist: crate::OnExist,
    context: &dyn DdlSessionContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    let result = executor.with_meta(context, |snapshot, start_ts, upgrading| {
        crate::cluster_ddl::create_schema::create_schema_with_info(
            snapshot, info, on_exist, context, start_ts, upgrading,
        )
    });
    ignore_exists(
        result,
        on_exist,
        &tidb_infoschema::infoschema_errors::ERR_DATABASE_EXISTS,
    )
}

/// Go's `if onExist == OnExistIgnore && infoschema.ErrXxxExists.Equal(err)
/// { AppendNote(err); err = nil }` after `DoDDLJobWrapper`.
fn ignore_exists(
    result: Result<ClusterDdlReport, SqlQueryError>,
    on_exist: crate::OnExist,
    exists: &tidb_terror::TerrorError,
) -> Result<ClusterDdlReport, SqlQueryError> {
    match result {
        Err(error)
            if on_exist == crate::OnExist::Ignore && error.identity == Some(exists.identity()) =>
        {
            let note = exists.fast_gen("%s", &[error.message.clone().into()]);
            Ok(ClusterDdlReport::AlreadySatisfied {
                detail: error.message,
                warnings: vec![crate::real_tikv_ddl::ddl_note(note)],
            })
        }
        result => result,
    }
}

/// What Go's `executor` statement methods read and call beyond the
/// statement: a metadata snapshot to check it against followed by the job
/// submission (`DoDDLJobWrapper`), the latest schema (`infoCache`), the
/// store's TiKV and TiFlash store counts, the NOT NULL data check (its
/// `sessPool`) and the DDL's `uuid`. [`RealClusterDdl`] is the server's; a
/// test supplies one over its own meta store.
pub trait ExecutorStore {
    /// The metadata snapshot a statement is checked against.
    type Snapshot: MetaSnapshot;

    /// Go `executor`'s metadata reads followed by `DoDDLJobWrapper`: `build`
    /// checks the statement at one snapshot (given its start timestamp and
    /// whether the cluster is upgrading) and answers the job to submit and
    /// wait for, or why none is needed.
    fn with_meta(
        &self,
        context: &dyn DdlSessionContext,
        build: impl FnOnce(
            &mut Self::Snapshot,
            u64,
            bool,
        ) -> Result<Option<AddIndexSubmission>, DdlPlanError>,
    ) -> Result<ClusterDdlReport, SqlQueryError>;

    /// Go `e.infoCache.GetLatest()`.
    fn latest_schema(&self) -> Arc<tidb_infoschema::SessionExtendedInfoSchema>;

    /// The number of TiKV stores (Go `store.(tikv.Storage).GetRegionCache().GetAllStores()`).
    fn store_count(&self) -> usize;

    /// Go `infoschema.GetTiFlashStoreCount`.
    fn tiflash_store_count(&self) -> Result<u64, String>;

    /// Go `checkForNullValue` over the executor's session pool: whether the
    /// column holds a NULL.
    fn has_null_value(
        &self,
        schema: &str,
        table: &tidb_model::TableInfo,
        column_id: i64,
    ) -> Result<bool, DdlPlanError>;

    /// Go `e.uuid`, the DDL's owner-manager id.
    fn uuid(&self) -> &str;
}

impl<S: StoreClient> ExecutorStore for RealClusterDdl<S> {
    type Snapshot = SnapshotMetaSnapshot;

    fn with_meta(
        &self,
        context: &dyn DdlSessionContext,
        build: impl FnOnce(
            &mut SnapshotMetaSnapshot,
            u64,
            bool,
        ) -> Result<Option<AddIndexSubmission>, DdlPlanError>,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        check_flashback_cluster_job(
            self.scheduler.session_pool.as_ref(),
            self.min_job_id_refresher.get_curr_min_job_id(),
        )
        .map_err(cluster_ddl_error)?;
        let preparation = SessionTransaction::begin(
            Arc::clone(&self.opener),
            self.timeout,
            ddl_transaction_options(),
        )
        .map_err(crate::real_tikv_ddl::ClusterDdlError::from)
        .map_err(cluster_ddl_error)?;
        let prepared = (|| {
            let mut snapshot = SnapshotMetaSnapshot::new(
                preparation
                    .snapshot()
                    .map_err(|error| SqlQueryError::unknown(error.to_string()))?,
            )
            .with_auto_ids_opt(self.auto_ids.as_ref())
            .with_latest_info_schema(self.latest_schema());
            build(
                &mut snapshot,
                preparation.start_ts(),
                self.server_state.is_upgrading_state(),
            )
            .map_err(crate::real_tikv_ddl::ClusterDdlError::from)
            .map_err(cluster_ddl_error)
        })();
        let _ = preparation.rollback();
        match prepared? {
            Some(AddIndexSubmission::AlreadySatisfied { detail, warnings }) => {
                Ok(ClusterDdlReport::AlreadySatisfied { detail, warnings })
            }
            Some(AddIndexSubmission::Job(spec, executor_warnings)) => {
                // Go `DoDDLJobWrapper`: maintenance admission before a
                // persistent job is submitted. Local temporary-table DDL
                // submits no job and remains available.
                check_user_ddl_job_allowed(context, spec.job.type_)?;
                let spec = match capture_job_for_test(spec) {
                    Ok(()) => {
                        return Ok(ClusterDdlReport::AlreadySatisfied {
                            detail: "the job is captured for a test".into(),
                            warnings: executor_warnings,
                        })
                    }
                    Err(spec) => spec,
                };
                crate::real_tikv_ddl::prepare_executor_job(
                    &self.opener,
                    self.timeout,
                    &KvTableIndexBackfiller::new(
                        &self.scheduler.row_ids,
                        &self.scheduler.session_pool,
                    )
                    .with_region_store(self.scheduler.region_store.get().cloned())
                    .with_start_mode(self.scheduler.start_mode),
                    &spec,
                )
                .map_err(cluster_ddl_error)?;
                let mut report = self.deliver_job_and_wait(context, *spec)?;
                let (ClusterDdlReport::Applied { warnings, .. }
                | ClusterDdlReport::AlreadySatisfied { warnings, .. }) = &mut report;
                let job_warnings = std::mem::replace(warnings, executor_warnings);
                warnings.extend(job_warnings);
                Ok(report)
            }
            None => Err(SqlQueryError::unknown("DDL executor built no job")),
        }
    }

    fn latest_schema(&self) -> Arc<tidb_infoschema::SessionExtendedInfoSchema> {
        self.schema_sync.catalog.get_latest()
    }

    fn store_count(&self) -> usize {
        self.opener
            .store()
            .store_addresses()
            .map_or(0, |stores| stores.len())
    }

    fn tiflash_store_count(&self) -> Result<u64, String> {
        self.opener.store().get_tiflash_store_count()
    }

    fn has_null_value(
        &self,
        schema: &str,
        table: &tidb_model::TableInfo,
        column_id: i64,
    ) -> Result<bool, DdlPlanError> {
        KvTableIndexBackfiller::new(&self.scheduler.row_ids, &self.scheduler.session_pool)
            .with_region_store(self.scheduler.region_store.get().cloned())
            .with_start_mode(self.scheduler.start_mode)
            .has_null_value(schema, table, column_id)
            .map_err(|error| {
                DdlPlanError::from_mysql_error(
                    tidb_terror::MysqlError::from_parts(error.code, error.state, error.message)
                        .with_identity_opt(error.identity),
                )
            })
    }

    fn uuid(&self) -> &str {
        self.owner.id()
    }
}

/// Go `objectType` for `executor.dropTableObject`.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ObjectType {
    Table,
    View,
    Sequence,
}

/// Go `executor.DropTable`.
pub fn drop_table<E: ExecutorStore>(
    executor: &E,
    statement: &tidb_ast::DropTableStmt,
    current_db: &str,
    context: &DdlContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    drop_table_object(
        executor,
        &statement.names,
        statement.if_exists,
        ObjectType::Table,
        current_db,
        context,
    )
}

/// Go `executor.DropView`.
pub fn drop_view<E: ExecutorStore>(
    executor: &E,
    names: &[Vec<String>],
    if_exists: bool,
    current_db: &str,
    context: &DdlContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    drop_table_object(
        executor,
        names,
        if_exists,
        ObjectType::View,
        current_db,
        context,
    )
}

/// Go `executor.DropSequence`.
pub fn drop_sequence<E: ExecutorStore>(
    executor: &E,
    statement: &tidb_ast::DropSequenceStmt,
    current_db: &str,
    context: &DdlContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    drop_table_object(
        executor,
        &statement.names,
        statement.if_exists,
        ObjectType::Sequence,
        current_db,
        context,
    )
}

/// Go `executor.dropTableObject`: one job per existing object, then missing names.
fn drop_table_object<E: ExecutorStore>(
    executor: &E,
    paths: &[Vec<String>],
    if_exists: bool,
    object: ObjectType,
    current_db: &str,
    context: &DdlContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    use tidb_ddl_build::table_lifecycle::{drop_objects, DropObjectsError};
    let mut names = paths
        .iter()
        .map(|path| {
            split_name(path, current_db, "table")
                .map_err(|e| SqlQueryError::from(e.to_mysql_error()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    // Go `orderDropTableObjectsByForeignKey`, under foreign_key_checks, for
    // table objects only.
    if object == ObjectType::Table && context.as_ref().foreign_key_checks() {
        names = crate::cluster_ddl::drop_object::order_drop_table_objects_by_foreign_key(
            &executor.latest_schema(),
            &names,
        );
    }
    let mut last = ClusterDdlReport::AlreadySatisfied {
        detail: "no existing DROP targets".into(),
        warnings: Vec::new(),
    };
    let mut notes = Vec::new();
    let missing_error: &tidb_terror::TerrorError = if object == ObjectType::Sequence {
        &tidb_infoschema::infoschema_errors::ERR_SEQUENCE_DROP_EXISTS
    } else {
        &tidb_infoschema::infoschema_errors::ERR_TABLE_DROP_EXISTS
    };
    let missing_raised = |names: &str| missing_error.gen_with_stack_by_args(&[names.into()]);
    let missing = drop_objects(&names, if_exists, |schema, name| {
        let result =
            executor.with_meta(
                context.as_ref(),
                |snapshot, start_ts, upgrading| match object {
                    ObjectType::Table => crate::cluster_ddl::drop_object::drop_table(
                        snapshot,
                        schema,
                        name,
                        false,
                        context.as_ref(),
                        &names,
                        start_ts,
                        upgrading,
                    ),
                    ObjectType::View => crate::cluster_ddl::drop_object::drop_view(
                        snapshot,
                        schema,
                        name,
                        false,
                        context.as_ref(),
                        start_ts,
                        upgrading,
                    ),
                    ObjectType::Sequence => crate::cluster_ddl::drop_object::drop_sequence(
                        snapshot,
                        schema,
                        name,
                        context.as_ref(),
                        start_ts,
                        upgrading,
                    ),
                },
            );
        match result {
            Ok(report) => {
                last = report;
                tidb_failpoint::inject_call(
                    "github.com/pingcap/tidb/pkg/ddl/afterDropTableObject",
                    &mut tidb_hack::go_to_lower(name),
                );
                Ok(true)
            }
            Err(error) if error.code == missing_error.to_sql_error().code => Ok(false),
            Err(error)
                if object == ObjectType::Sequence
                    && if_exists
                    && error.code == tidb_mysql::errors::errcode::ErrWrongObject =>
            {
                notes.push(tidb_context::SqlWarn::from_error(
                    tidb_context::WarningLevel::Note,
                    error.code,
                    error.message,
                    error.identity,
                ));
                Ok(true)
            }
            Err(error) => Err(error),
        }
    })
    .map_err(|error| match error {
        DropObjectsError::Target(error) => error,
        DropObjectsError::Missing(names) => SqlQueryError::from(
            tidb_terror::MysqlError::from_terror(&missing_raised(&names.join(","))),
        ),
    })?;
    let (ClusterDdlReport::Applied { warnings, .. }
    | ClusterDdlReport::AlreadySatisfied { warnings, .. }) = &mut last;
    warnings.extend(notes);
    warnings.extend(
        missing
            .into_iter()
            .map(|name| crate::real_tikv_ddl::ddl_note(missing_raised(&name))),
    );
    Ok(last)
}

/// Go `executor.DropSchema`.
pub fn drop_schema<E: ExecutorStore>(
    executor: &E,
    name: &str,
    if_exists: bool,
    context: &DdlContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    executor.with_meta(context.as_ref(), |snapshot, start_ts, upgrading| {
        crate::cluster_ddl::drop_object::drop_schema(
            snapshot,
            name,
            if_exists,
            context.as_ref(),
            start_ts,
            upgrading,
        )
    })
}

/// Go `executor.RenameTable`, `renameTable`, and `renameTables`.
pub fn rename_table<E: ExecutorStore>(
    executor: &E,
    statement: &tidb_ast::RenameTableStmt,
    current_db: &str,
    context: &DdlContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    let pairs = statement
        .pairs
        .iter()
        .map(|(from, to)| {
            let (from_schema, from_table) = split_name(from, current_db, "renamed table")?;
            let (to_schema, to_table) = split_name(to, current_db, "new table name")?;
            Ok(crate::cluster_ddl::RenameTablePair {
                from_schema,
                from_table,
                to_schema,
                to_table,
            })
        })
        .collect::<Result<Vec<_>, tidb_ddl_build::table_info_build::DdlAdmissionError>>()
        .map_err(|e| SqlQueryError::from(e.to_mysql_error()))?;
    if pairs.is_empty() {
        return Err(SqlQueryError::unknown("RENAME TABLE names no table"));
    }
    executor.with_meta(context.as_ref(), |snapshot, start_ts, upgrading| {
        crate::cluster_ddl::rename_table::rename_tables(
            snapshot,
            &pairs,
            false,
            context.as_ref(),
            start_ts,
            upgrading,
        )
    })
}

/// Go `checkUserDDLJobAllowed`: in NextGen maintenance mode a user
/// statement submits no new DDL job.
pub(crate) fn check_user_ddl_job_allowed(
    context: &dyn DdlSessionContext,
    job_type: tidb_model::ActionType,
) -> Result<(), SqlQueryError> {
    // Releasing existing locks, including session-close cleanup, must remain
    // available during maintenance so other sessions are not left blocked.
    if job_type == tidb_model::ActionType::ACTION_UNLOCK_TABLE {
        return Ok(());
    }
    if tidb_config_kerneltype::is_next_gen()
        && tidb_config::config_tree::config::get_global_config().maintenance_mode
        && !context.in_restricted_sql()
    {
        return Err(mysql_query_error(tidb_terror::MysqlError::from_terror(
            &tidb_exeerrors::ERR_MAINTENANCE_MODE.gen_with_stack_by_args(&["DDL".into()]),
        )));
    }
    Ok(())
}

/// Go `executor.DoDDLJobWrapper`: admit the built job, submit, and wait.
pub fn do_ddl_job_wrapper<E: ExecutorStore>(
    executor: &E,
    context: &dyn DdlSessionContext,
    job: crate::JobWrapper,
) -> Result<ClusterDdlReport, SqlQueryError> {
    executor.with_meta(context, |snapshot, start_ts, upgrading| {
        let mut jobs = [job];
        tidb_ddl_jobsubmit::prepare_submit_batch(snapshot, &mut jobs, start_ts, upgrading)?;
        let [job] = jobs;
        Ok(Some(AddIndexSubmission::Job(Box::new(job), Vec::new())))
    })
}

/// Go `executor.CreateView` after its body has been preprocessed.
pub fn create_view<E: ExecutorStore>(
    executor: &E,
    schema: &str,
    name: &str,
    or_replace: bool,
    info: &tidb_model::TableInfo,
    context: &dyn DdlSessionContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    let tiflash = || ExecutorStore::tiflash_store_count(executor);
    executor.with_meta(context, |snapshot, start_ts, upgrading| {
        crate::cluster_ddl::create_object::create_view(
            snapshot, schema, name, or_replace, info, context, &tiflash, start_ts, upgrading,
        )
    })
}

/// Go `executor.AlterTableMode`.
pub fn alter_table_mode<E: ExecutorStore>(
    executor: &E,
    table_mode: tidb_model::TableMode,
    schema_id: i64,
    table_id: i64,
    context: &dyn DdlSessionContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    executor.with_meta(context, |snapshot, start_ts, upgrading| {
        crate::cluster_ddl::table_mode::alter_table_mode_by_id(
            snapshot, table_mode, schema_id, table_id, context, start_ts, upgrading,
        )
    })
}

/// Go `executor.RefreshMeta`.
pub fn refresh_meta<E: ExecutorStore>(
    executor: &E,
    schema_id: i64,
    table_id: i64,
    involved_db: &str,
    involved_table: &str,
    context: &dyn DdlSessionContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    executor.with_meta(context, |snapshot, start_ts, upgrading| {
        crate::cluster_ddl::table_mode::refresh_meta(
            snapshot,
            schema_id,
            table_id,
            involved_db,
            involved_table,
            context,
            start_ts,
            upgrading,
        )
    })
}

/// Go `executor.CreatePlacementPolicy`.
pub fn create_placement_policy<E: ExecutorStore>(
    executor: &E,
    statement: &tidb_ast::CreatePlacementPolicyStmt,
    context: &DdlContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    if context.placement_mode_ignore() {
        return Ok(ClusterDdlReport::AlreadySatisfied {
            detail: "placement DDL ignored".into(),
            warnings: vec![crate::cluster_ddl::placement_ignored_note()],
        });
    }
    if statement.or_replace && statement.if_not_exists {
        return Err(SqlQueryError::from(
            tidb_dbterror::ERR_WRONG_USAGE
                .gen_with_stack("Incorrect usage of OR REPLACE and IF NOT EXISTS")
                .to_sql_error(),
        ));
    }
    let settings = crate::cluster_ddl::placement_settings_from_options(&statement.options)
        .map_err(|e| SqlQueryError::from(e.to_mysql_error()))?;

    executor.with_meta(context.as_ref(), |snapshot, start_ts, upgrading| {
        crate::cluster_ddl::placement_policy::create_placement_policy(
            snapshot,
            &statement.name,
            &settings,
            statement.if_not_exists,
            statement.or_replace,
            context.as_ref(),
            start_ts,
            upgrading,
        )
    })
}

/// Go `executor.AlterPlacementPolicy`.
pub fn alter_placement_policy<E: ExecutorStore>(
    executor: &E,
    statement: &tidb_ast::AlterPlacementPolicyStmt,
    context: &DdlContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    if context.placement_mode_ignore() {
        return Ok(ClusterDdlReport::AlreadySatisfied {
            detail: "placement DDL ignored".into(),
            warnings: vec![crate::cluster_ddl::placement_ignored_note()],
        });
    }
    let settings = crate::cluster_ddl::placement_settings_from_options(&statement.options)
        .map_err(|e| SqlQueryError::from(e.to_mysql_error()))?;

    executor.with_meta(context.as_ref(), |snapshot, start_ts, upgrading| {
        crate::cluster_ddl::placement_policy::alter_placement_policy(
            snapshot,
            &statement.name,
            &settings,
            context.as_ref(),
            start_ts,
            upgrading,
        )
    })
}

/// Go `executor.DropPlacementPolicy`.
pub fn drop_placement_policy<E: ExecutorStore>(
    executor: &E,
    statement: &tidb_ast::DropPlacementPolicyStmt,
    context: &DdlContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    if context.placement_mode_ignore() {
        return Ok(ClusterDdlReport::AlreadySatisfied {
            detail: "placement DDL ignored".into(),
            warnings: vec![crate::cluster_ddl::placement_ignored_note()],
        });
    }

    executor.with_meta(context.as_ref(), |snapshot, start_ts, upgrading| {
        crate::cluster_ddl::placement_policy::drop_placement_policy(
            snapshot,
            &statement.name,
            statement.if_exists,
            context.as_ref(),
            start_ts,
            upgrading,
        )
    })
}

/// Go `executor.AlterSchema`: charset, then placement; the session runs the TiFlash loop.
pub fn alter_schema<E: ExecutorStore>(
    executor: &E,
    name: &str,
    options: &[tidb_ast::DatabaseOption],
    context: &DdlContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    let options =
        crate::cluster_ddl::alter_schema::resolve_schema_options(options, context.as_ref())
            .map_err(crate::real_tikv_ddl::ClusterDdlError::from)
            .map_err(cluster_ddl_error)?;
    let name = name.to_owned();
    let mut report = ClusterDdlReport::AlreadySatisfied {
        detail: "ALTER DATABASE has no changes".into(),
        warnings: Vec::new(),
    };
    if let Some((charset, collate)) = options.charset_collate {
        report = executor.with_meta(context.as_ref(), |snapshot, start_ts, upgrading| {
            crate::cluster_ddl::alter_schema::modify_schema_charset_and_collate(
                snapshot,
                &name,
                &charset,
                &collate,
                context.as_ref(),
                start_ts,
                upgrading,
            )
        })?;
    }
    if let Some(policy) = options.placement_policy {
        report = executor.with_meta(context.as_ref(), |snapshot, start_ts, upgrading| {
            crate::cluster_ddl::alter_schema::modify_schema_default_placement(
                snapshot,
                &name,
                &policy,
                context.as_ref(),
                start_ts,
                upgrading,
            )
        })?;
    }
    Ok(report)
}

/// Go `executor.AlterTableSetTiFlashReplica`.
pub fn alter_table_set_tiflash_replica<E: ExecutorStore>(
    executor: &E,
    schema: &str,
    table: &str,
    count: u64,
    labels: &[String],
    reset_available: bool,
    hypo: bool,
    context: &DdlContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    let tiflash = || ExecutorStore::tiflash_store_count(executor);
    executor.with_meta(context.as_ref(), |snapshot, start_ts, upgrading| {
        let catalog = tidb_infoschema::cluster_catalog::load_executor_info_schema(snapshot)?;
        let built = crate::cluster_ddl::alter_table::alter_table_set_ti_flash_replica(
            snapshot,
            &catalog,
            &schema.to_owned(),
            &table.to_owned(),
            &count,
            &labels.to_vec(),
            &reset_available,
            &hypo,
            context.as_ref(),
            false,
            &tiflash,
        )?;
        crate::cluster_ddl::submit_built_job(snapshot, built, start_ts, upgrading)
    })
}

/// Go `executor.CreateIndex`.
pub fn create_index<E: ExecutorStore>(
    executor: &E,
    statement: &tidb_ast::CreateIndexStmt,
    current_db: &str,
    context: &DdlContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    let store_count = executor.store_count();
    executor.with_meta(context.as_ref(), |snapshot, start_ts, upgrading| {
        let catalog = tidb_infoschema::cluster_catalog::load_executor_info_schema(snapshot)?;
        let mut has_null = |schema: &str, table: &tidb_model::TableInfo, column_id: i64| {
            executor.has_null_value(schema, table, column_id)
        };
        let built = crate::cluster_ddl::create_index_job(
            snapshot,
            &catalog,
            statement,
            current_db,
            false,
            context,
            store_count,
            &mut has_null,
        )?;
        crate::cluster_ddl::submit_built_job(snapshot, built, start_ts, upgrading)
    })
}

/// Go `executor.DropIndex`.
pub fn drop_index<E: ExecutorStore>(
    executor: &E,
    statement: &tidb_ast::DropIndexStmt,
    current_db: &str,
    context: &DdlContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    let (schema, table) = split_name(&statement.table, current_db, "table")
        .map_err(|e| SqlQueryError::from(e.to_mysql_error()))?;
    executor.with_meta(context.as_ref(), |snapshot, start_ts, upgrading| {
        let catalog = tidb_infoschema::cluster_catalog::load_executor_info_schema(snapshot)?;
        let built = crate::cluster_ddl::drop_index::drop_index_by_name(
            snapshot,
            &catalog,
            &schema,
            &table,
            &statement.name,
            statement.if_exists,
            statement.is_hypo,
            context.as_ref(),
            false,
        )?;
        crate::cluster_ddl::submit_built_job(snapshot, built, start_ts, upgrading)
    })
}

/// Go `executor.RecoverTable` through its snapshot history lookup.
pub(super) fn recover_table<S: StoreClient>(
    executor: &RealClusterDdl<S>,
    table: &Option<Vec<String>>,
    job_id: i64,
    new_name: &str,
    flashback: bool,
    current_db: &str,
    context: &DdlContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    let table = table
        .as_ref()
        .map(|path| split_name(path, current_db, "table"))
        .transpose()
        .map_err(|e| SqlQueryError::from(e.to_mysql_error()))?;
    executor.with_meta(context.as_ref(), |snapshot, _, _| {
        let catalog = tidb_infoschema::cluster_catalog::load_executor_info_schema(snapshot)?;
        let safe_point = crate::cluster_ddl::recover_table::get_gc_safe_point(
            executor.scheduler.session_pool.as_ref(),
        )?;
        let at = crate::real_tikv_ddl::StoreSnapshotMeta {
            opener: Arc::clone(&executor.opener),
            timeout: executor.timeout,
        };
        let job = crate::cluster_ddl::recover_table::recover_table_by_job_or_name(
            snapshot,
            &catalog,
            &table,
            &job_id,
            &new_name.to_owned(),
            &flashback,
            context.as_ref(),
            safe_point,
            &at,
        )?;
        Ok(Some(AddIndexSubmission::Job(Box::new(job), Vec::new())))
    })
}

/// Go `executor.RecoverSchema` through its snapshot history lookup.
pub(super) fn recover_schema<S: StoreClient>(
    executor: &RealClusterDdl<S>,
    name: &str,
    new_name: &str,
    current_db: &str,
    context: &DdlContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    let _ = current_db;
    executor.with_meta(context.as_ref(), |snapshot, _, _| {
        let catalog = tidb_infoschema::cluster_catalog::load_executor_info_schema(snapshot)?;
        let safe_point = crate::cluster_ddl::recover_table::get_gc_safe_point(
            executor.scheduler.session_pool.as_ref(),
        )?;
        let at = crate::real_tikv_ddl::StoreSnapshotMeta {
            opener: Arc::clone(&executor.opener),
            timeout: executor.timeout,
        };
        let job = crate::cluster_ddl::recover_table::recover_schema(
            snapshot,
            &catalog,
            &name.to_owned(),
            &new_name.to_owned(),
            context.as_ref(),
            safe_point,
            &at,
        )?;
        Ok(Some(AddIndexSubmission::Job(Box::new(job), Vec::new())))
    })
}

/// Go `executor.AlterTable`: resolve specifications, then build their jobs against the schema.
pub fn alter_table<E: ExecutorStore>(
    executor: &E,
    statement: &tidb_ast::AlterTableStmt,
    current_db: &str,
    context: &DdlContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    alter_table_allowing(executor, statement, current_db, context, false)
}

/// Go master `executor.alterTable` with its `allowMaterializedViewRelated`.
fn alter_table_allowing<E: ExecutorStore>(
    executor: &E,
    statement: &tidb_ast::AlterTableStmt,
    current_db: &str,
    context: &DdlContext,
    allow_materialized_view_related: bool,
) -> Result<ClusterDdlReport, SqlQueryError> {
    let statement = crate::cluster_ddl::resolve_alter_table_spec(statement, context)
        .map_err(|e| SqlQueryError::from(e.to_mysql_error()))?;
    let catalog = executor.latest_schema();
    // Go `executor.AlterTable`'s masking-policy specs.
    if let [tidb_ast::AlterTableAction::MaskingPolicy(action)] = statement.actions.as_slice() {
        let job = super::executor_masking::alter_table_masking_policy_job(
            &catalog,
            &statement,
            action,
            current_db,
            context.as_ref(),
        )
        .map_err(mysql_query_error)?;
        return match job {
            Some(job) => do_ddl_job_wrapper(executor, context.as_ref(), job),
            None => Ok(ClusterDdlReport::AlreadySatisfied {
                detail: "no masking policy change".into(),
                warnings: Vec::new(),
            }),
        };
    }
    // Go `checkMaskingPolicyOnModifyColumn` (`ModifyColumn` / `ChangeColumn`).
    super::executor_masking::check_masking_policy_on_modify_column(
        &catalog,
        &statement,
        current_db,
        context.as_ref(),
    )
    .map_err(mysql_query_error)?;
    // Go `executor.AlterTable`'s `AlterTableWriteable` case: READ ONLY is a
    // READ ONLY table lock, READ WRITE its cleanup; nothing without the
    // experimental table lock.
    if let [tidb_ast::AlterTableAction::ReadOnly(read_only)] = statement.actions.as_slice() {
        if !tidb_config::config_tree::config::table_lock_enabled() {
            return Ok(ClusterDdlReport::AlreadySatisfied {
                detail: "table lock is disabled".into(),
                warnings: Vec::new(),
            });
        }
        return if *read_only {
            lock_tables(
                executor,
                &[tidb_ast::TableLock {
                    table: statement.name.clone(),
                    lock_type: tidb_ast::TableLockType::READ_ONLY,
                }],
                current_db,
                context.as_ref(),
            )
        } else {
            cleanup_table_lock(
                executor,
                &[statement.name.clone()],
                current_db,
                context.as_ref(),
            )
        };
    }
    let store_count = executor.store_count();
    let tiflash = || ExecutorStore::tiflash_store_count(executor);
    executor.with_meta(context.as_ref(), |snapshot, start_ts, upgrading| {
        let mut has_null = |schema: &str, table: &tidb_model::TableInfo, column_id: i64| {
            executor.has_null_value(schema, table, column_id)
        };
        crate::cluster_ddl::alter_table_job(
            snapshot,
            &statement,
            current_db,
            context,
            start_ts,
            upgrading,
            allow_materialized_view_related,
            store_count,
            &tiflash,
            &mut has_null,
        )
    })
}

/// Go `executor.UpdateTableReplicaInfo`.
pub fn update_table_replica_info<E: ExecutorStore>(
    executor: &E,
    table_id: i64,
    available: bool,
    context: &dyn DdlSessionContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    let tiflash = || ExecutorStore::tiflash_store_count(executor);
    executor.with_meta(context, |snapshot, start_ts, upgrading| {
        let catalog = tidb_infoschema::cluster_catalog::load_executor_info_schema(snapshot)?;
        let built = crate::cluster_ddl::alter_table::update_table_replica_info(
            snapshot, &catalog, &table_id, &available, context, false, &tiflash,
        )?;
        crate::cluster_ddl::submit_built_job(snapshot, built, start_ts, upgrading)
    })
}

/// Go's `SessionInfo{ServerID: e.uuid, SessionID: ConnectionID}`; the DDL's
/// uuid is its owner manager's id.
fn table_lock_session_info(
    server_id: &str,
    context: &dyn DdlSessionContext,
) -> tidb_model::SessionInfo {
    tidb_model::SessionInfo {
        server_id: server_id.to_owned(),
        session_id: context.ddl_connection_id(),
    }
}

fn mysql_query_error(error: tidb_terror::MysqlError) -> SqlQueryError {
    SqlQueryError::from(error)
}

fn plan_query_error(error: DdlPlanError) -> SqlQueryError {
    SqlQueryError::from(tidb_terror::MysqlError::from_sql_error(
        error.to_sql_error(),
    ))
}

/// Go `throwErrIfInMemOrSysDB`.
fn throw_err_if_in_mem_or_sys_db(
    context: &dyn DdlSessionContext,
    db: &str,
) -> Result<(), SqlQueryError> {
    let login = context.ddl_login_user();
    tidb_ddl_build::table_lock::throw_err_if_in_mem_or_sys_db(
        login
            .as_ref()
            .map(|(user, host)| (user.as_str(), host.as_str())),
        db,
    )
    .map_err(mysql_query_error)
}

/// Go `executor.getSchemaAndTableByIdent` over the latest schema, then the
/// lock statements' refusal of a view or a sequence.
fn lockable_table(
    catalog: &tidb_infoschema::SessionExtendedInfoSchema,
    db: &str,
    name: &str,
) -> Result<(i64, tidb_model::GoShared<tidb_model::TableInfo>), SqlQueryError> {
    let Some(database) = catalog.schema_by_name(db) else {
        return Err(mysql_query_error(tidb_terror::MysqlError::from_terror(
            &tidb_infoschema::infoschema_errors::ERR_DATABASE_NOT_EXISTS
                .gen_with_stack_by_args(&[db.to_owned().into()]),
        )));
    };
    let Some((_, table)) = catalog.table_info_by_name(db, name) else {
        return Err(mysql_query_error(tidb_terror::MysqlError::from_terror(
            &tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                .gen_with_stack_by_args(&[db.to_owned().into(), name.to_owned().into()]),
        )));
    };
    if table.read().is_view() || table.read().is_sequence() {
        return Err(mysql_query_error(tidb_terror::MysqlError::from_terror(
            &tidb_table::table_errors::ERR_UNSUPPORTED_OP.gen_with_stack_by_args(&[]),
        )));
    }
    let schema_id = database.read().id;
    Ok((schema_id, table))
}

/// Go `executor.LockTables`.
pub fn lock_tables<E: ExecutorStore>(
    executor: &E,
    locks: &[tidb_ast::TableLock],
    current_db: &str,
    context: &dyn DdlSessionContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    let catalog = executor.latest_schema();
    let catalog = catalog.as_ref();
    let session_info = table_lock_session_info(executor.uuid(), context);
    let mut lock_tables = Vec::with_capacity(locks.len());
    let mut unique_table_id = std::collections::HashSet::new();
    for lock in locks {
        let (db, name) = split_name(&lock.table, current_db, "table")
            .map_err(|e| SqlQueryError::from(e.to_mysql_error()))?;
        throw_err_if_in_mem_or_sys_db(context, &db)?;
        let (schema_id, table) = lockable_table(catalog, &db, &name)?;
        let table = table.read();
        let held = table.lock.as_ref().map(|lock| lock.read().clone());
        tidb_ddl_build::table_lock::check_table_locked(
            table.name.lowercase(),
            held.as_ref(),
            lock.lock_type,
            &session_info,
        )
        .map_err(mysql_query_error)?;
        if !unique_table_id.insert(table.id) {
            return Err(mysql_query_error(tidb_terror::MysqlError::from_terror(
                &tidb_infoschema::infoschema_errors::ERR_NONUNIQ_TABLE
                    .gen_with_stack_by_args(&[table.name.original().to_owned().into()]),
            )));
        }
        lock_tables.push(tidb_model::TableLockTpInfo {
            schema_id,
            table_id: table.id,
            tp: lock.lock_type,
        });
    }
    let session_locks = context.table_lock_context();
    let unlock_tables = session_locks
        .as_ref()
        .map(|locks| locks.get_all_table_locks())
        .unwrap_or_default();
    let job = crate::cluster_ddl::table_lock::lock_tables_job(
        catalog,
        tidb_model::LockTablesArgs {
            lock_tables: lock_tables.clone(),
            unlock_tables: unlock_tables.clone(),
            session_info,
            ..Default::default()
        },
        context,
    )
    .map_err(plan_query_error)?;
    // Go: check admission before recording provisional session locks.
    check_user_ddl_job_allowed(context, tidb_model::ActionType::ACTION_LOCK_TABLE)?;
    // Go: added before the job, so a session killed once the job has run
    // still holds what it locked.
    if let Some(locks) = &session_locks {
        locks.add_table_lock(&lock_tables);
    }
    let report = do_ddl_job_wrapper(executor, context, job)?;
    if let Some(locks) = &session_locks {
        locks.release_table_locks(&unlock_tables);
        locks.add_table_lock(&lock_tables);
    }
    Ok(report)
}

/// Go `executor.UnlockTables`: none to unlock submits no job.
pub fn unlock_tables<E: ExecutorStore>(
    executor: &E,
    unlock_tables: Vec<tidb_model::TableLockTpInfo>,
    context: &dyn DdlSessionContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    let catalog = executor.latest_schema();
    let catalog = catalog.as_ref();
    if unlock_tables.is_empty() {
        return Ok(ClusterDdlReport::AlreadySatisfied {
            detail: "no table is locked".into(),
            warnings: Vec::new(),
        });
    }
    let job = crate::cluster_ddl::table_lock::unlock_tables_job(
        catalog,
        tidb_model::LockTablesArgs {
            unlock_tables,
            session_info: table_lock_session_info(executor.uuid(), context),
            ..Default::default()
        },
        context,
    )
    .map_err(plan_query_error)?;
    let report = do_ddl_job_wrapper(executor, context, job)?;
    if let Some(locks) = context.table_lock_context() {
        locks.release_all_table_locks();
    }
    Ok(report)
}

/// Go `executor.CleanupTableLock`: no job when every named table is
/// unlocked.
pub fn cleanup_table_lock<E: ExecutorStore>(
    executor: &E,
    tables: &[Vec<String>],
    current_db: &str,
    context: &dyn DdlSessionContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    let catalog = executor.latest_schema();
    let catalog = catalog.as_ref();
    let mut unique_table_id = std::collections::HashSet::new();
    let mut cleanup_tables = Vec::with_capacity(tables.len());
    let mut unlocked = 0;
    for path in tables {
        let (db, name) = split_name(path, current_db, "table")
            .map_err(|e| SqlQueryError::from(e.to_mysql_error()))?;
        throw_err_if_in_mem_or_sys_db(context, &db)?;
        let (schema_id, table) = lockable_table(catalog, &db, &name)?;
        let table = table.read();
        if !table.is_locked() {
            unlocked += 1;
        }
        if !unique_table_id.insert(table.id) {
            return Err(mysql_query_error(tidb_terror::MysqlError::from_terror(
                &tidb_infoschema::infoschema_errors::ERR_NONUNIQ_TABLE
                    .gen_with_stack_by_args(&[table.name.original().to_owned().into()]),
            )));
        }
        cleanup_tables.push(tidb_model::TableLockTpInfo {
            schema_id,
            table_id: table.id,
            ..Default::default()
        });
    }
    if cleanup_tables.is_empty() || cleanup_tables.len() == unlocked {
        return Ok(ClusterDdlReport::AlreadySatisfied {
            detail: "no named table is locked".into(),
            warnings: Vec::new(),
        });
    }
    let job = crate::cluster_ddl::table_lock::unlock_tables_job(
        catalog,
        tidb_model::LockTablesArgs {
            unlock_tables: cleanup_tables.clone(),
            is_cleanup: true,
            ..Default::default()
        },
        context,
    )
    .map_err(plan_query_error)?;
    let report = do_ddl_job_wrapper(executor, context, job)?;
    if let Some(locks) = context.table_lock_context() {
        locks.release_table_locks(&cleanup_tables);
    }
    Ok(report)
}

/// Go `executor.CreateMaskingPolicy`. An `IF NOT EXISTS` that loses the race
/// to another creator answers the job's "exists" error as a note.
pub fn create_masking_policy<E: ExecutorStore>(
    executor: &E,
    stmt: &tidb_ast::CreateMaskingPolicyStmt,
    current_db: &str,
    context: &dyn DdlSessionContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    let catalog = executor.latest_schema();
    let Some(job) =
        super::executor_masking::create_masking_policy_job(&catalog, stmt, current_db, context)
            .map_err(mysql_query_error)?
    else {
        return Ok(ClusterDdlReport::AlreadySatisfied {
            detail: "the masking policy exists".into(),
            warnings: Vec::new(),
        });
    };
    match do_ddl_job_wrapper(executor, context, job).map_err(tidb_terror::MysqlError::from) {
        Err(note)
            if stmt.if_not_exists
                && note.equal(&tidb_meta::meta_errors::ERR_MASKING_POLICY_EXISTS) =>
        {
            context.append_note_parts(note.code, &note.message);
            Ok(ClusterDdlReport::AlreadySatisfied {
                detail: "the masking policy exists".into(),
                warnings: Vec::new(),
            })
        }
        other => other.map_err(SqlQueryError::from),
    }
}

thread_local! {
    /// [`capture_ddl_job_for_test`]'s slot: `Some` while capturing, holding
    /// the job once one is built.
    static CAPTURED_JOB: std::cell::RefCell<Option<Option<crate::JobWrapper>>> = const { std::cell::RefCell::new(None) };
}

/// Hands `spec` to an active [`capture_ddl_job_for_test`] on this thread.
fn capture_job_for_test(spec: Box<crate::JobWrapper>) -> Result<(), Box<crate::JobWrapper>> {
    CAPTURED_JOB.with(|slot| match slot.borrow_mut().as_mut() {
        Some(captured) => {
            *captured = Some(*spec);
            Ok(())
        }
        None => Err(spec),
    })
}

/// Test support, Go's `ExecutorForTest` construction: the job the DDL
/// statement `run` executes on this thread builds is kept instead of
/// delivered, for a later `do_ddl_job_wrapper` (Go tests that build a job,
/// change the schema, and then run it).
pub fn capture_ddl_job_for_test<R>(run: impl FnOnce() -> R) -> (R, Option<crate::JobWrapper>) {
    CAPTURED_JOB.with(|slot| *slot.borrow_mut() = Some(None));
    let out = run();
    let job = CAPTURED_JOB.with(|slot| slot.borrow_mut().take().flatten());
    (out, job)
}
