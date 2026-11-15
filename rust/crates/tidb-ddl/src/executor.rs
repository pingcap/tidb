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
    let tiflash = || executor.opener.store().get_tiflash_store_count();
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
    let tiflash = || executor.opener.store().get_tiflash_store_count();
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
pub(super) fn create_placement_policy_with_info<S: StoreClient>(
    executor: &RealClusterDdl<S>,
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
pub(super) fn create_schema_with_info<S: StoreClient>(
    executor: &RealClusterDdl<S>,
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

/// Submits one prepared job (Go `doDDLJob2`).
pub type DoDdlJob<'a> = dyn Fn(crate::JobWrapper) -> Result<ClusterDdlReport, SqlQueryError> + 'a;

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
pub(super) fn lock_tables<S: StoreClient>(
    executor: &RealClusterDdl<S>,
    locks: &[tidb_ast::TableLock],
    current_db: &str,
    context: &dyn DdlSessionContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    let catalog = executor.schema_sync.catalog.get_latest();
    lock_tables_with(
        &catalog,
        executor.owner.id(),
        locks,
        current_db,
        context,
        &|job| do_ddl_job_wrapper(executor, context, job),
    )
}

/// Go `executor.LockTables` over `catalog` (the executor's latest schema),
/// the DDL's `server_id` (Go `e.uuid`) and its job submission.
pub fn lock_tables_with(
    catalog: &tidb_infoschema::SessionExtendedInfoSchema,
    server_id: &str,
    locks: &[tidb_ast::TableLock],
    current_db: &str,
    context: &dyn DdlSessionContext,
    do_job: &DoDdlJob<'_>,
) -> Result<ClusterDdlReport, SqlQueryError> {
    let session_info = table_lock_session_info(server_id, context);
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
    // Go: added before the job, so a session killed once the job has run
    // still holds what it locked.
    if let Some(locks) = &session_locks {
        locks.add_table_lock(&lock_tables);
    }
    let report = do_job(job)?;
    if let Some(locks) = &session_locks {
        locks.release_table_locks(&unlock_tables);
        locks.add_table_lock(&lock_tables);
    }
    Ok(report)
}

/// Go `executor.UnlockTables`.
pub(super) fn unlock_tables<S: StoreClient>(
    executor: &RealClusterDdl<S>,
    unlock_tables: Vec<tidb_model::TableLockTpInfo>,
    context: &dyn DdlSessionContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    let catalog = executor.schema_sync.catalog.get_latest();
    unlock_tables_with(
        &catalog,
        executor.owner.id(),
        unlock_tables,
        context,
        &|job| do_ddl_job_wrapper(executor, context, job),
    )
}

/// Go `executor.UnlockTables` (see [`lock_tables_with`]): none to unlock
/// submits no job.
pub fn unlock_tables_with(
    catalog: &tidb_infoschema::SessionExtendedInfoSchema,
    server_id: &str,
    unlock_tables: Vec<tidb_model::TableLockTpInfo>,
    context: &dyn DdlSessionContext,
    do_job: &DoDdlJob<'_>,
) -> Result<ClusterDdlReport, SqlQueryError> {
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
            session_info: table_lock_session_info(server_id, context),
            ..Default::default()
        },
        context,
    )
    .map_err(plan_query_error)?;
    let report = do_job(job)?;
    if let Some(locks) = context.table_lock_context() {
        locks.release_all_table_locks();
    }
    Ok(report)
}

/// Go `executor.CleanupTableLock`.
pub(super) fn cleanup_table_lock<S: StoreClient>(
    executor: &RealClusterDdl<S>,
    tables: &[Vec<String>],
    current_db: &str,
    context: &dyn DdlSessionContext,
) -> Result<ClusterDdlReport, SqlQueryError> {
    let catalog = executor.schema_sync.catalog.get_latest();
    cleanup_table_lock_with(&catalog, tables, current_db, context, &|job| {
        do_ddl_job_wrapper(executor, context, job)
    })
}

/// Go `executor.CleanupTableLock` (see [`lock_tables_with`]): no job when
/// every named table is unlocked.
pub fn cleanup_table_lock_with(
    catalog: &tidb_infoschema::SessionExtendedInfoSchema,
    tables: &[Vec<String>],
    current_db: &str,
    context: &dyn DdlSessionContext,
    do_job: &DoDdlJob<'_>,
) -> Result<ClusterDdlReport, SqlQueryError> {
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
    let report = do_job(job)?;
    if let Some(locks) = context.table_lock_context() {
        locks.release_table_locks(&cleanup_tables);
    }
    Ok(report)
}
