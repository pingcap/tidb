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

//! Go master `pkg/ddl/mview_worker.go`: the owner's materialized view log
//! job steps.

use super::*;

/// Go `isValidMaterializedViewLogBaseTable`.
pub(crate) fn is_valid_materialized_view_log_base_table(schema_lower_name: &str, table: &TableInfo) -> bool {
    if tidb_metadef::is_mem_or_sys_db(schema_lower_name)
        || table.is_view()
        || table.is_sequence()
        || table.temp_table_type != tidb_model::TempTableType::NONE
        || table.materialized_view_shadow.is_some()
        || table.materialized_view_log.is_some()
    {
        return false;
    }
    match &table.materialized_view {
        Some(mview) => table.state == SchemaState::PUBLIC && tidb_model::table::MaterializedViewInfo::get_init_build_state(Some(&mview.read())).is_ready(),
        None => true,
    }
}

/// Go `errUnsupportedMaterializedViewOnPartitionTable`.
pub(crate) fn err_unsupported_materialized_view_on_partition_table(op: &str) -> DdlPlanError {
    DdlPlanError::from(
        tidb_dbterror::ERR_GENERAL_UNSUPPORTED_DDL.gen_with_stack_by_args(&[format!("{op} on partition table").into()]),
    )
}

fn unsupported(message: String) -> tidb_terror::MysqlError {
    tidb_terror::MysqlError::from_terror(&tidb_dbterror::ERR_GENERAL_UNSUPPORTED_DDL.gen_with_stack_by_args(&[message.into()]))
}

/// Go master `checkProtectedMaterializedViewShadowConstraint` for a statement
/// outside materialized view maintenance (`allowInternalMViewShadowBypass`
/// needs `InMViewMaintenance`, which no session of this port sets).
pub(crate) fn check_protected_materialized_view_shadow_constraint(
    table: &TableInfo,
    op: &str,
) -> Result<(), tidb_terror::MysqlError> {
    if table.materialized_view_shadow.is_some() {
        return Err(unsupported(format!("{op} on materialized view shadow table")));
    }
    Ok(())
}

/// Go master `checkTableMaterializedViewConstraints`: DROP / TRUNCATE /
/// RENAME TABLE refuse a log, a materialized view, a shadow table, and a base
/// table with dependents.
pub(crate) fn check_table_materialized_view_constraints(table: &TableInfo, op: &str) -> Result<(), tidb_terror::MysqlError> {
    if table.materialized_view_log.is_some() {
        return Err(unsupported(format!("{op} on materialized view log table")));
    }
    if table.materialized_view.is_some() {
        return Err(unsupported(format!("{op} on materialized view table")));
    }
    check_protected_materialized_view_shadow_constraint(table, op)?;
    if let Some(base) = &table.materialized_view_base {
        let base = base.read();
        if !base.mview_ids.is_empty() {
            return Err(unsupported(format!("{op} on base table with materialized view dependencies")));
        }
        if base.mlog_id != 0 {
            return Err(unsupported(format!("{op} on base table with materialized view log")));
        }
    }
    Ok(())
}

/// Go master `checkDropMaterializedViewLogHasNoDependentMVs`: a log that
/// materialized views read cancels the drop.
pub(crate) fn check_drop_materialized_view_log_has_no_dependent_mvs(
    txn: &mut JobTxn<'_>,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    dropping: &TableInfo,
) -> Result<(), DdlPlanError> {
    let Some(log) = &dropping.materialized_view_log else {
        return Ok(());
    };
    let (base_table_id, has_dependents) = {
        let log = log.read();
        (log.base_table_id, !log.dependent_mview_ids.is_empty())
    };
    if !has_dependents {
        return Ok(());
    }
    let base = match get_table_info(txn, base_table_id, active.job.schema_id) {
        Ok(base) => base,
        Err(error) if is_not_exists(&error) => return Ok(()),
        Err(error) => return Err(error),
    };
    let message = format!(
        "cannot drop materialized view log on {}.{}: dependent materialized views exist",
        active.job.schema_name,
        base.name.original()
    );
    Err(cancel_ddl_job(active, DdlPlanError::from_mysql_error(tidb_terror::MysqlError::unknown(message))))
}

/// Go `getTableInfo` over the job's mutator: the table, or Go's
/// database / table not-exists error.
fn get_table_info(txn: &mut JobTxn<'_>, table_id: i64, schema_id: i64) -> Result<TableInfo, DdlPlanError> {
    let meta = txn.meta();
    match meta.table(schema_id, table_id) {
        Ok(Some(table)) => Ok(table),
        Ok(None) => Err(DdlPlanError::from(
            tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS.gen_with_stack_by_args(&[
                format!("(Schema ID {schema_id})").into(),
                format!("(Table ID {table_id})").into(),
            ]),
        )),
        Err(tidb_meta::MetaError::DatabaseNotExists) => Err(DdlPlanError::from(
            tidb_infoschema::infoschema_errors::ERR_DATABASE_NOT_EXISTS
                .gen_with_stack_by_args(&[format!("(Schema ID {schema_id})").into()]),
        )),
        Err(error) => Err(DdlPlanError::Meta(job_meta_error(&meta, error))),
    }
}

fn is_not_exists(error: &DdlPlanError) -> bool {
    error.equal(&tidb_infoschema::infoschema_errors::ERR_DATABASE_NOT_EXISTS)
        || error.equal(&tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS)
}

/// Go `updateTable(t, schemaID, tblInfo, true)`.
fn update_table(txn: &mut JobTxn<'_>, schema_id: i64, table: &mut TableInfo, start_ts: u64) -> Result<(), DdlPlanError> {
    table.update_ts = start_ts;
    let meta = txn.meta();
    meta.update_table(schema_id, table).map_err(|error| DdlPlanError::Meta(job_meta_error(&meta, error)))
}

/// Go `SetSchemaDiffForMultiInfos`: each extra table not already in the diff
/// becomes an affected option of its own schema.
pub(crate) fn set_schema_diff_for_multi_infos(diff: &mut SchemaDiff, infos: &[(i64, i64)]) {
    if infos.is_empty() {
        return;
    }
    let mut affected: Vec<AffectedOption> = diff.affected_options.iter_deref().map(|a| a.read().clone()).collect();
    let mut seen: Vec<i64> = std::iter::once(diff.table_id).chain(affected.iter().map(|a| a.table_id)).collect();
    for &(schema_id, table_id) in infos {
        if seen.contains(&table_id) {
            continue;
        }
        seen.push(table_id);
        affected.push(AffectedOption {
            schema_id,
            old_schema_id: schema_id,
            table_id,
            old_table_id: table_id,
            ..AffectedOption::default()
        });
    }
    diff.affected_options = GoSharedPointerSlice::from(affected);
}

/// Go `buildCreateMaterializedViewLogPurgeInfoUpsertSQL`.
fn create_materialized_view_log_purge_info_upsert_sql(mlog_id: i64, next: Option<i64>, should_update: bool) -> String {
    let next = next.map_or_else(|| "NULL".to_owned(), |next| next.to_string());
    if should_update {
        format!(
            "INSERT INTO mysql.tidb_mlog_purge_info (MLOG_ID, NEXT_PURGE_UNIX_SECONDS)\nVALUES ({mlog_id}, {next}) ON DUPLICATE KEY UPDATE NEXT_PURGE_UNIX_SECONDS = VALUES(NEXT_PURGE_UNIX_SECONDS)"
        )
    } else {
        format!("INSERT IGNORE INTO mysql.tidb_mlog_purge_info (MLOG_ID) VALUES ({mlog_id})")
    }
}

/// Go `convertCreateMaterializedViewLogPurgeInfoTableNotExistsErr`.
fn convert_purge_info_table_not_exists(error: tidb_terror::MysqlError) -> DdlPlanError {
    if error.equal(&tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS) {
        return DdlPlanError::from(tidb_dbterror::ERR_INVALID_DDL_JOB.gen_with_stack_by_args(&[
            "create materialized view log: mysql.tidb_mlog_purge_info does not exist".into(),
        ]));
    }
    DdlPlanError::from_mysql_error(error)
}

fn execute_job_sql(txn: &JobTxn<'_>, sql: &str) -> Result<(), tidb_terror::MysqlError> {
    match txn.execute(sql, &[]) {
        Some(result) => result.map(|_| ()),
        None => Err(tidb_terror::MysqlError::unknown(
            "a materialized view log job step needs the DDL session".to_owned(),
        )),
    }
}

/// Go `upsertCreateMaterializedViewLogPurgeInfo`: the log's purge schedule
/// row, its next purge derived from the stored START WITH / NEXT.
fn upsert_create_materialized_view_log_purge_info(txn: &JobTxn<'_>, mlog: &TableInfo) -> Result<(), DdlPlanError> {
    let Some(info) = &mlog.materialized_view_log else {
        return Err(DdlPlanError::from(tidb_dbterror::ERR_INVALID_DDL_JOB.gen_with_stack_by_args(&[
            "create materialized view log: invalid materialized view log metadata".into(),
        ])));
    };
    let info = info.read().clone();
    let (next, should_update) = tidb_ddl_build::materialized_view::derive_materialized_schedule_next_unix_seconds(
        &info.purge_start_with,
        &info.purge_next,
        info.purge_schedule_sql_mode,
        chrono::Utc::now(),
    )
    .map_err(DdlPlanError::from_mysql_error)?;
    let sql = create_materialized_view_log_purge_info_upsert_sql(mlog.id, next, should_update);
    execute_job_sql(txn, &sql).map_err(convert_purge_info_table_not_exists)
}

/// Go `deleteMaterializedViewLogPurgeInfos`: a missing table deletes nothing.
pub(crate) fn delete_materialized_view_log_purge_info(txn: &JobTxn<'_>, mlog_id: i64) -> Result<(), DdlPlanError> {
    let sql = format!("DELETE FROM mysql.tidb_mlog_purge_info WHERE MLOG_ID IN ({mlog_id})");
    match execute_job_sql(txn, &sql) {
        Err(error) if error.equal(&tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS) => Ok(()),
        other => other.map_err(DdlPlanError::from_mysql_error),
    }
}

/// Go `updateMaterializedViewBaseInfoOnDrop` for a dropped MV log: its base
/// table forgets it (and its `MaterializedViewBase` once nothing remains).
pub(crate) fn update_materialized_view_base_info_on_drop_log(
    txn: &mut JobTxn<'_>,
    active: &tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    dropping: &TableInfo,
) -> Result<Vec<(i64, TableInfo)>, DdlPlanError> {
    let Some(log) = &dropping.materialized_view_log else {
        return Ok(Vec::new());
    };
    let base_id = log.read().base_table_id;
    let schema_id = active.job.schema_id;
    let meta = txn.meta();
    let base = meta.table(schema_id, base_id).map_err(|error| DdlPlanError::Meta(job_meta_error(&meta, error)))?;
    let Some(mut base) = base else {
        return Ok(Vec::new());
    };
    if let Some(info) = base.materialized_view_base.clone() {
        let (mlog_id, empty) = {
            let mut info = info.write();
            if info.mlog_id == active.job.table_id {
                info.mlog_id = 0;
            }
            (info.mlog_id, info.mview_ids.is_empty())
        };
        if mlog_id == 0 && empty {
            base.materialized_view_base = None;
        }
    }
    Ok(vec![(schema_id, base)])
}

/// Go `(*worker).onCreateMaterializedViewLog`.
pub(super) fn on_create_materialized_view_log(
    txn: &mut JobTxn<'_>,
    catalog: &SessionExtendedInfoSchema,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let invalid_args = |active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob| {
        cancel_ddl_job(
            active,
            DdlPlanError::from(tidb_dbterror::ERR_INVALID_DDL_JOB.gen_with_stack_by_args(&[
                "create materialized view log: invalid job args".into(),
            ])),
        )
    };
    let args = tidb_model::get_create_materialized_view_log_args(&mut active.job)
        .map_err(|error| cancel_ddl_job(active, DdlPlanError::Encode(error.to_string())))?;
    let Some(table_handle) = args.and_then(|args| args.read().table_info.clone()) else {
        return Err(invalid_args(active));
    };
    let mut mlog = table_handle.read().clone();
    let Some(log_info) = mlog.materialized_view_log.as_ref().map(|info| info.read().clone()) else {
        return Err(invalid_args(active));
    };
    if active.job.is_rollingback() {
        return rollback_create_materialized_view_log(txn, active, &mlog, start_ts);
    }
    if log_info.base_table_id == 0 {
        return Err(cancel_ddl_job(
            active,
            DdlPlanError::from(tidb_dbterror::ERR_INVALID_DDL_JOB.gen_with_stack_by_args(&[
                "create materialized view log: invalid base table id".into(),
            ])),
        ));
    }
    let schema_id = active.job.schema_id;
    let mut base = match get_table_info(txn, log_info.base_table_id, schema_id) {
        Ok(base) => base,
        Err(error) if is_not_exists(&error) => return Err(cancel_ddl_job(active, error)),
        Err(error) => return Err(error),
    };
    let schema_name = active.job.schema_name.to_string();
    if !is_valid_materialized_view_log_base_table(&tidb_hack::go_to_lower(&schema_name), &base) {
        return Err(cancel_ddl_job(
            active,
            DdlPlanError::from(tidb_dbterror::ERR_WRONG_OBJECT.gen_with_stack_by_args(&[
                schema_name.clone().into(),
                base.name.original().to_owned().into(),
                "BASE TABLE".into(),
            ])),
        ));
    }
    if base.get_partition_info().is_some() {
        return Err(cancel_ddl_job(
            active,
            err_unsupported_materialized_view_on_partition_table("CREATE MATERIALIZED VIEW LOG"),
        ));
    }
    if base.state != SchemaState::PUBLIC {
        return Err(cancel_ddl_job(
            active,
            DdlPlanError::from(tidb_dbterror::ERR_INVALID_DDL_STATE.generate(format!(
                "table {} is not in public, but {}",
                base.name, base.state
            ))),
        ));
    }
    if base.materialized_view_base.as_ref().is_some_and(|info| info.read().mlog_id != 0) {
        return Err(cancel_ddl_job(
            active,
            DdlPlanError::from(tidb_infoschema::infoschema_errors::ERR_TABLE_EXISTS.gen_with_stack_by_args(&[
                format!("{}.{}", schema_name, mlog.name.original()).into(),
            ])),
        ));
    }
    if base.materialized_view.is_some() {
        // Go takes the MV's refresh advisory lock in the DDL session here
        // (`mviewutil.RefreshAdvisoryLockName`), which the DDL step session
        // of this port does not hold.
        return Err(cancel_ddl_job(
            active,
            DdlPlanError::from(tidb_dbterror::ERR_GENERAL_UNSUPPORTED_DDL.gen_with_stack_by_args(&[
                "CREATE MATERIALIZED VIEW LOG on a materialized view".into(),
            ])),
        ));
    }

    // Go `createTable(w, jobCtx, job, ..., &model.CreateTableArgs{TableInfo:
    // mlogTableInfo, FKCheck: false})`.
    let database = job_schema_with_tables(txn, schema_id)?.ok_or_else(|| {
        cancel_ddl_job(
            active,
            DdlPlanError::from(
                tidb_dbterror::CLASS_SCHEMA
                    .new_std(tidb_errno::errcode::ErrBadDB)
                    .generate(format!("Unknown database '(Schema ID {schema_id})'")),
            ),
        )
    })?;
    let mut placement_bundles = Vec::new();
    let mut affinity_groups = crate::ddl_affinity::AffinityGroupUpdate::default();
    mlog.state = SchemaState::NONE;
    create_table::create_table(
        txn,
        catalog,
        &database,
        active,
        &mut mlog,
        false,
        start_ts,
        &mut placement_bundles,
        &mut affinity_groups,
    )?;
    let base_info = base.materialized_view_base.get_or_insert_with(Default::default);
    base_info.write().mlog_id = mlog.id;
    update_table(txn, schema_id, &mut base, start_ts)?;
    if let Err(error) = upsert_create_materialized_view_log_purge_info(txn, &mlog) {
        if error.equal(&tidb_dbterror::ERR_INVALID_DDL_JOB) {
            active.job.state = JobState::ROLLINGBACK;
        }
        return Err(error);
    }
    let mut diff = SchemaDiff {
        action_type: active.job.type_,
        schema_id,
        table_id: active.job.table_id,
        ..SchemaDiff::default()
    };
    set_schema_diff_for_multi_infos(&mut diff, &[(schema_id, base.id)]);
    let (schema_version, diff) = update_schema_version(&txn.meta(), active, diff)?;
    *table_handle.write() = mlog.clone_like_go();
    let mut mutations = Vec::new();
    append_schema_change_mutations(
        txn,
        catalog,
        active.job.id,
        &[(-1, SchemaChangeEvent::create_table(mlog.clone_like_go()))],
        &mut mutations,
        row_ids,
    )?;
    active.job.finish_multiple_table_job(
        JobState::DONE,
        SchemaState::PUBLIC,
        schema_version,
        &GoSharedPointerSlice::from(vec![base.clone_like_go(), mlog.clone_like_go()]),
    );
    let mut write = job_only_write(active.job.id, mutations);
    write.schema_version = schema_version;
    write.diff = diff;
    write.created_id = Some(mlog.id);
    write.placement_bundles = placement_bundles;
    write.affinity_groups = affinity_groups;
    Ok(PersistedDdlActionStep { write, update_raw_args: true, run_error: None, reorg: None })
}

/// Go `(*worker).rollbackCreateMaterializedViewLog`.
fn rollback_create_materialized_view_log(
    txn: &mut JobTxn<'_>,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    mlog: &TableInfo,
    start_ts: u64,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let schema_id = active.job.schema_id;
    let table_id = active.job.table_id;
    let actual = match get_table_info(txn, table_id, schema_id) {
        Ok(table) => Some(table),
        Err(error) if is_not_exists(&error) => None,
        Err(error) => return Err(error),
    };
    let dropping = actual.clone().unwrap_or_else(|| mlog.clone());
    let extra = update_materialized_view_base_info_on_drop_log(txn, active, &dropping)?;
    for (extra_schema, table) in &extra {
        update_table(txn, *extra_schema, &mut table.clone(), start_ts)?;
    }
    if actual.is_some() {
        let meta = txn.meta();
        meta.drop_table_or_view(schema_id, table_id).map_err(|error| DdlPlanError::Meta(job_meta_error(&meta, error)))?;
        meta.auto_ids(schema_id, table_id).delete().map_err(|error| DdlPlanError::Meta(job_meta_error(&meta, error)))?;
    }
    delete_materialized_view_log_purge_info(txn, table_id)?;
    // Go: the rollback diff needs RollbackDone (`SetSchemaDiffForCreateTable`
    // answers `OldTableID`); a failed update leaves the job retryable.
    let (prev_state, prev_schema_state) = (active.job.state, active.job.schema_state);
    active.job.state = JobState::ROLLBACK_DONE;
    active.job.schema_state = SchemaState::NONE;
    let mut diff = SchemaDiff {
        action_type: active.job.type_,
        schema_id,
        old_table_id: table_id,
        table_id: 0,
        ..SchemaDiff::default()
    };
    let infos: Vec<(i64, i64)> = extra.iter().map(|(schema, table)| (*schema, table.id)).collect();
    set_schema_diff_for_multi_infos(&mut diff, &infos);
    let (schema_version, diff) = match update_schema_version(&txn.meta(), active, diff) {
        Ok(updated) => updated,
        Err(error) => {
            active.job.state = prev_state;
            active.job.schema_state = prev_schema_state;
            return Err(error);
        }
    };
    let mut write = job_only_write(active.job.id, Vec::new());
    write.schema_version = schema_version;
    write.diff = diff;
    Ok(PersistedDdlActionStep { write, update_raw_args: true, run_error: None, reorg: None })
}
