// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go `pkg/ddl/executor.go`: statement methods build jobs directly.

use super::*;
use crate::cluster_ddl::add_index::AddIndexSubmission;
use crate::cluster_ddl::{split_name, CreateTableBuild, DdlContext};
use crate::real_tikv_ddl::{check_flashback_cluster_job, ddl_transaction_options};
use tidb_ddl_build::session_context::DdlSessionContext;
use tidb_infoschema_issyncer::real_tikv_catalog::SnapshotMetaSnapshot;
use tidb_store_driver_txn::cluster_table_storage::SessionTransaction;

/// Go `executor.CreateTable`: build the table (or its LIKE copy), then submit.
pub(super) fn create_table<S: StoreClient>(
    executor: &RealClusterDdl<S>,
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
    let tiflash = || executor.opener.store().get_tiflash_store_count();
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
pub(super) fn create_sequence<S: StoreClient>(
    executor: &RealClusterDdl<S>,
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
    let tiflash = || executor.opener.store().get_tiflash_store_count();
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
pub(super) fn alter_sequence<S: StoreClient>(
    executor: &RealClusterDdl<S>,
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
pub(super) fn truncate_table<S: StoreClient>(
    executor: &RealClusterDdl<S>,
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
    let tiflash = || executor.opener.store().get_tiflash_store_count();
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

/// Go `executor.CreateSchema` through `CreateSchemaWithInfo`.
pub(super) fn create_schema<S: StoreClient>(
    executor: &RealClusterDdl<S>,
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
    executor.with_meta(context, |snapshot, start_ts, upgrading| {
        crate::cluster_ddl::create_schema::create_schema_with_info(
            snapshot,
            name,
            if_not_exists,
            &charset,
            &collate,
            policy,
            context,
            start_ts,
            upgrading,
        )
    })
}

impl<S: StoreClient> RealClusterDdl<S> {
    /// Go `executor`'s metadata reads followed by `DoDDLJobWrapper`.
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
            .with_auto_ids_opt(self.auto_ids.as_ref());
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
}

/// Go `objectType` for `executor.dropTableObject`.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ObjectType {
    Table,
    View,
    Sequence,
}

/// Go `executor.DropTable`.
pub(super) fn drop_table<S: StoreClient>(
    executor: &RealClusterDdl<S>,
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
pub(super) fn drop_view<S: StoreClient>(
    executor: &RealClusterDdl<S>,
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
pub(super) fn drop_sequence<S: StoreClient>(
    executor: &RealClusterDdl<S>,
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
fn drop_table_object<S: StoreClient>(
    executor: &RealClusterDdl<S>,
    paths: &[Vec<String>],
    if_exists: bool,
    object: ObjectType,
    current_db: &str,
    context: &DdlContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    use tidb_ddl_build::table_lifecycle::{drop_objects, DropObjectsError};
    let names = paths
        .iter()
        .map(|path| {
            split_name(path, current_db, "table")
                .map_err(|e| SqlQueryError::from(e.to_mysql_error()))
        })
        .collect::<Result<Vec<_>, _>>()?;
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
pub(super) fn drop_schema<S: StoreClient>(
    executor: &RealClusterDdl<S>,
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
pub(super) fn rename_table<S: StoreClient>(
    executor: &RealClusterDdl<S>,
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

/// Go `executor.DoDDLJobWrapper`: admit the built job, submit, and wait.
pub(super) fn do_ddl_job_wrapper<S: StoreClient>(
    executor: &RealClusterDdl<S>,
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
pub(super) fn create_view<S: StoreClient>(
    executor: &RealClusterDdl<S>,
    schema: &str,
    name: &str,
    or_replace: bool,
    info: &tidb_model::TableInfo,
    context: &dyn DdlSessionContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    let tiflash = || executor.opener.store().get_tiflash_store_count();
    executor.with_meta(context, |snapshot, start_ts, upgrading| {
        crate::cluster_ddl::create_object::create_view(
            snapshot, schema, name, or_replace, info, context, &tiflash, start_ts, upgrading,
        )
    })
}

/// Go `executor.AlterTableMode`.
pub(super) fn alter_table_mode<S: StoreClient>(
    executor: &RealClusterDdl<S>,
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
pub(super) fn refresh_meta<S: StoreClient>(
    executor: &RealClusterDdl<S>,
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
pub(super) fn create_placement_policy<S: StoreClient>(
    executor: &RealClusterDdl<S>,
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
pub(super) fn alter_placement_policy<S: StoreClient>(
    executor: &RealClusterDdl<S>,
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
pub(super) fn drop_placement_policy<S: StoreClient>(
    executor: &RealClusterDdl<S>,
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
pub(super) fn alter_schema<S: StoreClient>(
    executor: &RealClusterDdl<S>,
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
pub(super) fn alter_table_set_tiflash_replica<S: StoreClient>(
    executor: &RealClusterDdl<S>,
    schema: &str,
    table: &str,
    count: u64,
    labels: &[String],
    reset_available: bool,
    hypo: bool,
    context: &DdlContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    let tiflash = || executor.opener.store().get_tiflash_store_count();
    executor.with_meta(context.as_ref(), |snapshot, start_ts, upgrading| {
        let catalog = tidb_infoschema::cluster_catalog::load_info_schema(snapshot)?;
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
pub(super) fn create_index<S: StoreClient>(
    executor: &RealClusterDdl<S>,
    statement: &tidb_ast::CreateIndexStmt,
    current_db: &str,
    context: &DdlContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    let store_count = executor
        .opener
        .store()
        .store_addresses()
        .map_or(0, |stores| stores.len());
    let backfiller = KvTableIndexBackfiller::new(
        &executor.scheduler.row_ids,
        &executor.scheduler.session_pool,
    )
    .with_region_store(executor.scheduler.region_store.get().cloned())
    .with_start_mode(executor.scheduler.start_mode);
    executor.with_meta(context.as_ref(), |snapshot, start_ts, upgrading| {
        let catalog = tidb_infoschema::cluster_catalog::load_info_schema(snapshot)?;
        let mut has_null = |schema: &str, table: &tidb_model::TableInfo, column_id: i64| {
            backfiller
                .has_null_value(schema, table, column_id)
                .map_err(|error| {
                    DdlPlanError::from_mysql_error(
                        tidb_terror::MysqlError::from_parts(error.code, error.state, error.message)
                            .with_identity_opt(error.identity),
                    )
                })
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
pub(super) fn drop_index<S: StoreClient>(
    executor: &RealClusterDdl<S>,
    statement: &tidb_ast::DropIndexStmt,
    current_db: &str,
    context: &DdlContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    let (schema, table) = split_name(&statement.table, current_db, "table")
        .map_err(|e| SqlQueryError::from(e.to_mysql_error()))?;
    executor.with_meta(context.as_ref(), |snapshot, start_ts, upgrading| {
        let catalog = tidb_infoschema::cluster_catalog::load_info_schema(snapshot)?;
        let built = crate::cluster_ddl::drop_index::drop_index_by_name(
            snapshot,
            &catalog,
            &schema,
            &table,
            &statement.name,
            statement.if_exists,
            statement.is_hypo,
            context.as_ref(),
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
        let catalog = tidb_infoschema::cluster_catalog::load_info_schema(snapshot)?;
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
        let catalog = tidb_infoschema::cluster_catalog::load_info_schema(snapshot)?;
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
pub(super) fn alter_table<S: StoreClient>(
    executor: &RealClusterDdl<S>,
    statement: &tidb_ast::AlterTableStmt,
    current_db: &str,
    context: &DdlContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    let statement = crate::cluster_ddl::resolve_alter_table_spec(statement, context)
        .map_err(|e| SqlQueryError::from(e.to_mysql_error()))?;
    let store_count = executor
        .opener
        .store()
        .store_addresses()
        .map_or(0, |stores| stores.len());
    let tiflash = || executor.opener.store().get_tiflash_store_count();
    let backfiller = KvTableIndexBackfiller::new(
        &executor.scheduler.row_ids,
        &executor.scheduler.session_pool,
    )
    .with_region_store(executor.scheduler.region_store.get().cloned())
    .with_start_mode(executor.scheduler.start_mode);
    executor.with_meta(context.as_ref(), |snapshot, start_ts, upgrading| {
        let mut has_null = |schema: &str, table: &tidb_model::TableInfo, column_id: i64| {
            backfiller
                .has_null_value(schema, table, column_id)
                .map_err(|error| {
                    DdlPlanError::from_mysql_error(
                        tidb_terror::MysqlError::from_parts(error.code, error.state, error.message)
                            .with_identity_opt(error.identity),
                    )
                })
        };
        crate::cluster_ddl::alter_table_job(
            snapshot,
            &statement,
            current_db,
            context,
            start_ts,
            upgrading,
            store_count,
            &tiflash,
            &mut has_null,
        )
    })
}

/// Go `executor.UpdateTableReplicaInfo`.
pub(super) fn update_table_replica_info<S: StoreClient>(
    executor: &RealClusterDdl<S>,
    table_id: i64,
    available: bool,
    context: &dyn DdlSessionContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    let tiflash = || executor.opener.store().get_tiflash_store_count();
    executor.with_meta(context, |snapshot, start_ts, upgrading| {
        let catalog = tidb_infoschema::cluster_catalog::load_info_schema(snapshot)?;
        let built = crate::cluster_ddl::alter_table::update_table_replica_info(
            snapshot, &catalog, &table_id, &available, context, false, &tiflash,
        )?;
        crate::cluster_ddl::submit_built_job(snapshot, built, start_ts, upgrading)
    })
}
