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

//! Go `pkg/ddl/table.go`'s `onRecoverTable` / `recoverTable`,
//! `schema.go`'s `onRecoverSchema`, and `job_worker.go`'s
//! `finishRecoverTable` / `finishRecoverSchema`: RECOVER / FLASHBACK TABLE
//! and FLASHBACK DATABASE.

use super::*;
use std::sync::Arc;

/// Go `recoverCheckFlagEnableGC`: GC was on when the job began.
const RECOVER_CHECK_FLAG_ENABLE_GC: i64 = 1;
/// Go `recoverCheckFlagDisableGC`.
const RECOVER_CHECK_FLAG_DISABLE_GC: i64 = 2;

/// What a step reaches through Go's `*worker` beyond its job transaction:
/// `w.sessPool` (GC, `mysql.gc_delete_range`) and `w.store`'s snapshots
/// (a dropped schema's tables).
#[derive(Clone)]
pub struct DdlStepWorker {
    /// Go `w.sessPool`.
    pub sess_pool: Arc<dyn tidb_ddl_build::session_pool::DdlSessionPool>,
    /// The step's staged writes (Go `w.sess`'s membuffer): the step's
    /// session statements stage into it, and the step commits it.
    pub step_buffer: tidb_store_driver_txn::cluster_storage::MutationBuffer,
    /// Go `meta.NewReader(w.store.GetSnapshot(kv.NewVersion(ts)))`.
    pub store: Option<Arc<dyn SnapshotMetaAt>>,
}

impl std::fmt::Debug for DdlStepWorker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DdlStepWorker")
    }
}

/// Go `gcutil` over one of the worker's sessions.
struct WorkerGcContext<'a>(&'a dyn tidb_ddl_build::session_pool::DdlSessionPool);

fn datum_text(datum: &tidb_datatype::Datum) -> String {
    match datum {
        tidb_datatype::Datum::String(text) => String::from_utf8_lossy(text.bytes()).into_owned(),
        tidb_datatype::Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        tidb_datatype::Datum::Int(value) => value.to_string(),
        tidb_datatype::Datum::UInt(value) => value.to_string(),
        _ => String::new(),
    }
}

impl tidb_gcutil::Context for WorkerGcContext<'_> {
    type Error = tidb_terror::MysqlError;

    fn get_global_sys_var(&mut self, name: &str) -> Result<String, Self::Error> {
        let rows = self.0.exec_restricted_sql(&format!("SELECT @@global.{name}"))?;
        Ok(rows
            .first()
            .and_then(|row| row.first())
            .map(datum_text)
            .unwrap_or_default())
    }

    fn set_global_sys_var(&mut self, name: &str, value: &str) -> Result<(), Self::Error> {
        self.0
            .exec_restricted_sql_with(
                &format!("SET @@global.{name} = %?"),
                &[tidb_sqlescape::SqlArg::String(value.as_bytes())],
            )
            .map(drop)
    }

    fn exec_restricted_sql(
        &mut self,
        sql: &str,
        arguments: &[&str],
        _internal_source_type: &str,
    ) -> Result<Vec<Vec<String>>, Self::Error> {
        let arguments = arguments
            .iter()
            .map(|argument| tidb_sqlescape::SqlArg::String(argument.as_bytes()))
            .collect::<Vec<_>>();
        Ok(self
            .0
            .exec_restricted_sql_with(sql, &arguments)?
            .iter()
            .map(|row| row.iter().map(datum_text).collect())
            .collect())
    }
}

fn gc_error(error: tidb_gcutil::Error<tidb_terror::MysqlError>) -> DdlPlanError {
    match error {
        tidb_gcutil::Error::Context(error) => DdlPlanError::from_mysql_error(error),
        tidb_gcutil::Error::SnapshotTooOld { safe_point_time } => DdlPlanError::from(
            tidb_variable::variable_errors::ERR_SNAPSHOT_TOO_OLD
                .generate_with_stack_by_args(&[safe_point_time.into()]),
        ),
        other => DdlPlanError::from_mysql_error(tidb_terror::MysqlError::unknown(other.to_string())),
    }
}

fn worker_sessions(sessions: Option<&DdlStepWorker>) -> Result<&DdlStepWorker, DdlPlanError> {
    sessions.ok_or_else(|| {
        DdlPlanError::from_mysql_error(tidb_terror::MysqlError::unknown(
            "the DDL worker has no session pool to reach GC through",
        ))
    })
}

/// Go `onRecoverTable`.
pub(super) fn on_recover_table(
    txn: &mut JobTxn<'_>,
    catalog: &SessionExtendedInfoSchema,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
    sessions: Option<&DdlStepWorker>,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let args = match tidb_model::get_recover_args(&mut active.job) {
        Ok(Some(args)) => args,
        // Invalid arguments: cancel the job.
        Ok(None) => {
            return Err(cancel_ddl_job(
                active,
                DdlPlanError::Encode("RECOVER TABLE job has nil args".to_owned()),
            ))
        }
        Err(error) => return Err(cancel_ddl_job(active, DdlPlanError::Encode(error.to_string()))),
    };
    let recover_info = args
        .read()
        .recover_table_infos()
        .into_iter()
        .next()
        .ok_or_else(|| DdlPlanError::Encode("RECOVER TABLE job has no table".to_owned()))?;
    let (schema_id, table, snapshot_ts) = {
        let info = recover_info.read();
        let table = info
            .table_info
            .clone()
            .ok_or_else(|| DdlPlanError::Encode("RECOVER TABLE job has nil table_info".to_owned()))?;
        (info.schema_id, table, info.snapshot_ts)
    };
    // Force-disable the TTL job schedule of the recovered table.
    if let Some(ttl) = table.read().ttl_info.as_ref() {
        ttl.write().enable = false;
    }

    // Check GC and the safe point.
    let sessions = worker_sessions(sessions)?;
    let gc_enable = tidb_gcutil::check_gc_enable(&mut WorkerGcContext(sessions.sess_pool.as_ref()))
        .map_err(|error| cancel_ddl_job(active, gc_error(error)))?;

    // Go `checkTableNotExists` against the info cache, then
    // `checkTableIDNotExists` against meta.
    let name = table.read().name.clone();
    let Some(database) = catalog.schema_by_id(schema_id) else {
        return Err(cancel_ddl_job(
            active,
            DdlPlanError::from(tidb_infoschema::infoschema_errors::ERR_DATABASE_NOT_EXISTS.generate_with_stack_by_args(&[
                format!("(Schema ID {schema_id})").into(),
            ])),
        ));
    };
    let schema_name = database.read().name.original().to_owned();
    if database
        .read()
        .deprecated_tables
        .iter_deref()
        .any(|existing| existing.read().name.lowercase() == name.lowercase())
    {
        return Err(cancel_ddl_job(
            active,
            DdlPlanError::from(tidb_infoschema::infoschema_errors::ERR_TABLE_EXISTS.generate_with_stack_by_args(&[
                format!("{schema_name}.{}", name.original()).into(),
            ])),
        ));
    }
    let table_id = table.read().id;
    let meta = txn.meta();
    let existing = meta
        .table(schema_id, table_id)
        .map_err(|error| DdlPlanError::Meta(job_meta_error(&meta, error)))?;
    if let Some(existing) = existing {
        return Err(cancel_ddl_job(
            active,
            DdlPlanError::from(tidb_infoschema::infoschema_errors::ERR_TABLE_EXISTS.generate_with_stack_by_args(&[
                existing.name.original().into(),
            ])),
        ));
    }

    // Recovering is two steps: the first records whether GC was on (so the
    // job enables it again once done, however many times a step retries);
    // the second disables GC, checks the safe point, and recovers.
    let state = table.read().state;
    match state {
        SchemaState::NONE => {
            // none -> write only
            args.write().check_flag = if gc_enable {
                RECOVER_CHECK_FLAG_ENABLE_GC
            } else {
                RECOVER_CHECK_FLAG_DISABLE_GC
            };
            active.job.schema_state = SchemaState::WRITE_ONLY;
            table.write().state = SchemaState::WRITE_ONLY;
            Ok(PersistedDdlActionStep {
                write: job_only_write(active.job.id, Vec::new()),
                update_raw_args: true,
                run_error: None,
                reorg: None,
            })
        }
        SchemaState::WRITE_ONLY => {
            // write only -> public
            if gc_enable {
                tidb_gcutil::disable_gc(&mut WorkerGcContext(sessions.sess_pool.as_ref())).map_err(|error| {
                    cancel_ddl_job(
                        active,
                        DdlPlanError::from_mysql_error(tidb_terror::MysqlError::unknown(format!(
                            "disable gc failed, try again later. err: {}",
                            gc_error(error)
                        ))),
                    )
                })?;
            }
            tidb_gcutil::validate_snapshot(&mut WorkerGcContext(sessions.sess_pool.as_ref()), snapshot_ts)
                .map_err(|error| cancel_ddl_job(active, gc_error(error)))?;
            let mut placement_bundles = Vec::new();
            let mut label_rules = LabelRuleSteps::new();
            recover_table(
                txn,
                active,
                &recover_info,
                sessions,
                &mut placement_bundles,
                &mut label_rules,
                start_ts,
            )?;
            let mut table_info = table.read().clone_like_go();
            table_info.state = SchemaState::PUBLIC;
            table_info.update_ts = start_ts;
            let mut tids = add_index::partition_ids(&table_info);
            tids.push(table_info.id);
            if table_info.partition.is_none() {
                tids = vec![table_info.id];
            }
            args.write().affected_physical_ids = tids.clone();
            let mut write = add_index::table_step_with_diff(
                txn,
                active,
                schema_id,
                &table_info,
                true,
                start_ts,
                // Go `SetSchemaDiffForRecoverTable`: the placement rule cache
                // is updated for every physical table.
                |diff| diff.affected_options = partition::build_placement_affects(&tids, &tids).into(),
            )?;
            {
                let mut table = table.write();
                table.state = SchemaState::PUBLIC;
                table.update_ts = start_ts;
            }
            active.job.finish_table_job(
                JobState::DONE,
                SchemaState::PUBLIC,
                write.schema_version,
                Some(GoShared::new(table.read().clone_like_go())),
            );
            write.created_id = Some(table_info.id);
            write.placement_bundles = placement_bundles;
            write.label_rules = label_rules.steps;
            Ok(PersistedDdlActionStep {
                write,
                update_raw_args: true,
                run_error: None,
                reorg: None,
            })
        }
        state => Err(DdlPlanError::from(
            tidb_dbterror::ERR_INVALID_DDL_STATE.generate_with_stack_by_args(&[
                FormatArg::from("table"),
                FormatArg::new(state.to_string(), state.0.to_string(), "model.SchemaState"),
            ]),
        )),
    }
}

/// Go `onRecoverSchema`: the same two steps as [`on_recover_table`], over
/// the dropped schema and every table it had at its drop's snapshot.
pub(super) fn on_recover_schema(
    txn: &mut JobTxn<'_>,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
    sessions: Option<&DdlStepWorker>,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let args = match tidb_model::get_recover_args(&mut active.job) {
        Ok(Some(args)) => args,
        // Invalid arguments: cancel the job.
        Ok(None) => {
            return Err(cancel_ddl_job(
                active,
                DdlPlanError::Encode("RECOVER SCHEMA job has nil args".to_owned()),
            ))
        }
        Err(error) => return Err(cancel_ddl_job(active, DdlPlanError::Encode(error.to_string()))),
    };
    let recover_schema_info = args
        .read()
        .recover_info
        .clone()
        .ok_or_else(|| DdlPlanError::Encode("RECOVER SCHEMA job has nil recover_info".to_owned()))?;
    let schema_info = recover_schema_info
        .read()
        .db_info
        .clone()
        .ok_or_else(|| DdlPlanError::Encode("RECOVER SCHEMA job has nil DBInfo".to_owned()))?;
    // Check GC and the safe point.
    let sessions = worker_sessions(sessions)?;
    let gc_enable = tidb_gcutil::check_gc_enable(&mut WorkerGcContext(sessions.sess_pool.as_ref()))
        .map_err(|error| cancel_ddl_job(active, gc_error(error)))?;
    let state = schema_info.read().state;
    match state {
        SchemaState::NONE => {
            // none -> write only: record whether GC was on.
            args.write().check_flag = if gc_enable {
                RECOVER_CHECK_FLAG_ENABLE_GC
            } else {
                RECOVER_CHECK_FLAG_DISABLE_GC
            };
            schema_info.write().state = SchemaState::WRITE_ONLY;
            active.job.schema_state = SchemaState::WRITE_ONLY;
            Ok(PersistedDdlActionStep {
                write: job_only_write(active.job.id, Vec::new()),
                update_raw_args: true,
                run_error: None,
                reorg: None,
            })
        }
        SchemaState::WRITE_ONLY => {
            // write only -> public: recover the schema and its tables.
            if gc_enable {
                tidb_gcutil::disable_gc(&mut WorkerGcContext(sessions.sess_pool.as_ref())).map_err(|error| {
                    cancel_ddl_job(
                        active,
                        DdlPlanError::from_mysql_error(tidb_terror::MysqlError::unknown(format!(
                            "disable gc failed, try again later. err: {}",
                            gc_error(error)
                        ))),
                    )
                })?;
            }
            let (snapshot_ts, load_tables_on_execute, drop_job_id, old_schema_name) = {
                let info = recover_schema_info.read();
                (
                    info.snapshot_ts,
                    info.load_tables_on_execute,
                    info.drop_job_id,
                    info.old_schema_name.lowercase().to_owned(),
                )
            };
            let mut recover_tables = recover_schema_info.read().recover_table_infos.clone().unwrap_or_default();
            if load_tables_on_execute {
                let schema_id = schema_info.read().id;
                let store = sessions.store.as_ref().ok_or_else(|| {
                    DdlPlanError::from_mysql_error(tidb_terror::MysqlError::unknown(
                        "the DDL worker has no store to read the dropped schema's tables from",
                    ))
                })?;
                let tables = store
                    .tables_at(snapshot_ts, schema_id)
                    .map_err(|error| cancel_ddl_job(active, error))?;
                recover_tables = Vec::with_capacity(tables.len());
                for table_info in tables {
                    let auto_ids = store
                        .auto_ids_at(snapshot_ts, schema_id, table_info.id)
                        .map_err(|error| cancel_ddl_job(active, error))?;
                    let old_table_name = table_info.name.lowercase().to_owned();
                    recover_tables.push(GoShared::new(tidb_model::RecoverTableInfo {
                        schema_id,
                        table_info: Some(GoShared::new(table_info)),
                        drop_job_id,
                        snapshot_ts,
                        auto_ids,
                        old_schema_name: old_schema_name.clone(),
                        old_table_name,
                    }));
                }
            }

            let mut db_info = schema_info.read().clone();
            db_info.state = SchemaState::PUBLIC;
            let meta = txn.meta();
            meta.create_database(&db_info)
                .map_err(|error| DdlPlanError::Meta(job_meta_error(&meta, error)))?;
            // Check the GC safe point.
            tidb_gcutil::validate_snapshot(&mut WorkerGcContext(sessions.sess_pool.as_ref()), snapshot_ts)
                .map_err(|error| cancel_ddl_job(active, gc_error(error)))?;

            let mut placement_bundles = Vec::new();
            let mut label_rules = LabelRuleSteps::new();
            for recover_info in &recover_tables {
                // Force-disable the TTL job schedule of each recovered table.
                if let Some(table) = recover_info.read().table_info.as_ref() {
                    if let Some(ttl) = table.read().ttl_info.as_ref() {
                        ttl.write().enable = false;
                    }
                }
                recover_table(
                    txn,
                    active,
                    recover_info,
                    sessions,
                    &mut placement_bundles,
                    &mut label_rules,
                    start_ts,
                )?;
            }
            schema_info.write().state = SchemaState::PUBLIC;
            // Use to update the InfoSchema.
            let schema_id = schema_info.read().id;
            active.job.schema_id = schema_id;
            // Go `SetSchemaDiffForRecoverSchema`: one option per table the
            // job ARGS name (none when the tables were loaded on execute),
            // and the reload reads the tables from meta.
            let affected_options = args
                .read()
                .recover_table_infos()
                .iter()
                .filter_map(|info| info.read().table_info.as_ref().map(|table| table.read().id))
                .map(|table_id| tidb_model::AffectedOption {
                    schema_id,
                    old_schema_id: schema_id,
                    table_id,
                    old_table_id: table_id,
                })
                .collect::<Vec<_>>();
            let meta = txn.meta();
            let action_type = active.job.type_;
            let (schema_version, diff) = update_schema_version(
                &meta,
                active,
                SchemaDiff {
                    action_type,
                    schema_id,
                    affected_options: affected_options.into(),
                    read_table_from_meta: true,
                    ..SchemaDiff::default()
                },
            )?;
            active.job.finish_db_job(
                JobState::DONE,
                SchemaState::PUBLIC,
                schema_version,
                Some(GoShared::new(schema_info.read().clone())),
            );
            let mut write = job_only_write(active.job.id, Vec::new());
            write.schema_version = schema_version;
            write.diff = diff;
            write.created_id = Some(schema_id);
            write.placement_bundles = placement_bundles;
            write.label_rules = label_rules.steps;
            Ok(PersistedDdlActionStep {
                write,
                update_raw_args: true,
                run_error: None,
                reorg: None,
            })
        }
        // We can't enter here.
        state => Err(unknown(format!("invalid db state {state}"))),
    }
}

/// Go `recoverTable`: the table's delete ranges cancelled, its placement
/// cleared, the table and its allocators back in meta, its label rules
/// re-keyed.
fn recover_table(
    txn: &mut JobTxn<'_>,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    recover_info: &GoShared<tidb_model::RecoverTableInfo>,
    sessions: &DdlStepWorker,
    placement_bundles: &mut Vec<tidb_placement::Bundle>,
    label_rules: &mut LabelRuleSteps,
    start_ts: u64,
) -> Result<(), DdlPlanError> {
    let (schema_id, table, drop_job_id, auto_ids, old_schema_name, old_table_name) = {
        let info = recover_info.read();
        (
            info.schema_id,
            info.table_info.clone().expect("checked by onRecoverTable"),
            info.drop_job_id,
            info.auto_ids,
            info.old_schema_name.clone(),
            info.old_table_name.clone(),
        )
    };
    let label_table = LabelRuleTable::from_table_info(&table.read());
    let old_rules = label_rules::get_old_label_rules(
        label_rules.manager(),
        &label_table,
        &old_schema_name,
        &old_table_name,
    )
    .map_err(|error| cancel_ddl_job(active, pd_step_error(Some("failed to get old label rules from PD"), error)))?;
    // Remove the dropped table's job from `mysql.gc_delete_range`.
    sessions
        .sess_pool
        .exec_restricted_sql_with(
            "DELETE FROM mysql.gc_delete_range WHERE job_id = %?",
            &[tidb_sqlescape::SqlArg::Signed(drop_job_id)],
        )
        .map_err(DdlPlanError::from_mysql_error)?;
    // Go `clearTablePlacementAndBundles`.
    {
        let mut table = table.write();
        if table.placement_policy_ref.take().is_some() {
            placement_bundles.push(tidb_placement::new_bundle(table.id));
        }
        if let Some(partition) = table.partition.as_ref() {
            for ordinal in 0..partition.read().definitions.len() {
                let mut cleared = None;
                partition.read().definitions.update(ordinal, |definition| {
                    if definition.placement_policy_ref.take().is_some() {
                        cleared = Some(definition.id);
                    }
                });
                if let Some(id) = cleared {
                    placement_bundles.push(tidb_placement::new_bundle(id));
                }
            }
        }
    }
    let mut table_info = table.read().clone_like_go();
    table_info.state = SchemaState::PUBLIC;
    table_info.update_ts = start_ts;
    let meta = txn.meta();
    meta.create_table_and_set_auto_id(schema_id, &table_info, auto_ids)
        .map_err(|error| DdlPlanError::Meta(job_meta_error(&meta, error)))?;
    label_rules
        .push(
            label_rules::update_label_rules(
                &active.job.schema_name.to_string(),
                &label_table,
                &old_rules,
                old_rules.old_rule_ids(),
                table_info.id,
            ),
            Some("failed to update the label rule to PD"),
        )
        .map_err(|error| cancel_ddl_job(active, error))
}

/// Go `finishRecoverTable` / `finishRecoverSchema`: a recover job that found
/// GC on enables it again, however it ended.
pub(super) fn finish_recover(
    job: &mut Job,
    sessions: Option<&DdlStepWorker>,
) -> Result<(), DdlPlanError> {
    if !matches!(
        job.type_,
        ActionType::ACTION_RECOVER_TABLE | ActionType::ACTION_RECOVER_SCHEMA
    ) {
        return Ok(());
    }
    let check_flag = tidb_model::get_recover_args(job)
        .map_err(|error| DdlPlanError::Encode(error.to_string()))?
        .map_or(0, |args| args.read().check_flag);
    if check_flag == RECOVER_CHECK_FLAG_ENABLE_GC {
        tidb_gcutil::enable_gc(&mut WorkerGcContext(worker_sessions(sessions)?.sess_pool.as_ref())).map_err(gc_error)?;
    }
    Ok(())
}

/// Go `gcutil.GetGCSafePoint` on one of the worker's sessions.
pub(crate) fn gc_safe_point(sessions: &DdlStepWorker) -> Result<u64, DdlPlanError> {
    tidb_gcutil::get_gc_safe_point(&mut WorkerGcContext(sessions.sess_pool.as_ref())).map_err(gc_error)
}

/// Meta reads at a past timestamp (Go `dom.GetSnapshotMeta(ts)` /
/// `meta.NewReader(store.GetSnapshot(kv.NewVersion(ts)))`).
pub trait SnapshotMetaAt {
    /// Go `snapMeta.GetTable(schemaID, tableID)`; `None` also when the
    /// schema does not exist at that timestamp.
    fn table_at(&self, ts: u64, schema_id: i64, table_id: i64) -> Result<Option<TableInfo>, DdlPlanError>;
    /// Go `GetAutoIDAccessors(schemaID, tableID).Get()`.
    fn auto_ids_at(&self, ts: u64, schema_id: i64, table_id: i64) -> Result<tidb_model::AutoIdGroup, DdlPlanError>;
    /// Go `snapMeta.GetDatabase(schemaID)`.
    fn database_at(&self, ts: u64, schema_id: i64) -> Result<Option<tidb_model::db::DBInfo>, DdlPlanError>;
    /// Go `snapMeta.ListTables(ctx, schemaID)`.
    fn tables_at(&self, ts: u64, schema_id: i64) -> Result<Vec<TableInfo>, DdlPlanError>;
}

fn unknown(message: String) -> DdlPlanError {
    DdlPlanError::from_mysql_error(tidb_terror::MysqlError::unknown(message))
}

/// Go `DDLExec.executeRecoverTable` / `executeFlashbackTable`, then
/// `executor.RecoverTable` up to the job: the dropped table found in the DDL
/// history (by name, newest first, or by job id) inside the GC safe point,
/// read with its allocators at the drop's snapshot, and the RECOVER TABLE
/// job over it.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_recover_table_job<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &SessionExtendedInfoSchema,
    statement: &DdlStatement,
    job_context: &dyn DdlSessionContext,
    gc_safe_point: u64,
    at: &dyn SnapshotMetaAt,
) -> Result<tidb_ddl_jobsubmit::JobSpec, DdlPlanError> {
    let DdlStatement::RecoverTable {
        table: by_name,
        job_id,
        new_name,
        flashback,
    } = statement
    else {
        unreachable!("called for RECOVER / FLASHBACK TABLE");
    };
    let (job, mut table_info) = match by_name {
        Some((schema, table)) => recover_table_by_name(snapshot, catalog, schema, table, gc_safe_point, at)?,
        None => {
            // Go `getRecoverTableByJobID`.
            let job = tidb_infoschema::cluster_catalog::with_meta_reader(snapshot, |reader| {
                reader.get_history_ddl_job::<Job>(*job_id)
            })
            .map_err(DdlPlanError::Meta)?
            .ok_or_else(|| {
                DdlPlanError::from(
                    tidb_dbterror::ERR_DDL_JOB_NOT_FOUND.generate_with_stack_by_args(&[(*job_id).into()]),
                )
            })?;
            if !matches!(
                job.type_,
                ActionType::ACTION_DROP_TABLE | ActionType::ACTION_TRUNCATE_TABLE
            ) {
                return Err(unknown(format!(
                    "Job {} type is {}, not dropped/truncated table",
                    job.id, job.type_
                )));
            }
            let snapshot_ts = get_recover_snapshot_ts(&job);
            tidb_gcutil::validate_snapshot_with_gc_safe_point(snapshot_ts, gc_safe_point).map_err(|error| {
                gc_error(match error {
                    tidb_gcutil::Error::SnapshotTooOld { safe_point_time } => {
                        tidb_gcutil::Error::SnapshotTooOld { safe_point_time }
                    }
                    other => tidb_gcutil::Error::Context(tidb_terror::MysqlError::unknown(other.to_string())),
                })
            })?;
            let table = at.table_at(snapshot_ts, job.schema_id, job.table_id)?.ok_or_else(|| {
                DdlPlanError::from(tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS.generate_with_stack_by_args(&[
                    format!("(Schema ID {})", job.schema_id).into(),
                    format!("(Table ID {})", job.table_id).into(),
                ]))
            })?;
            (job, table)
        }
    };
    let old_table_name = match by_name {
        // Go `executeFlashbackTable`: `s.Table.Name.L`.
        Some((_, table)) if *flashback => table.go_to_lower(),
        _ => table_info.name.lowercase().to_owned(),
    };
    if !new_name.is_empty() {
        table_info.name = tidb_ast::CiString::new(new_name);
    }
    // Check that the table ID does not exist (Go `is.TableByID`, over the
    // metadata this catalog image holds).
    let existing = catalog.stored_schemas().into_iter().find_map(|schema| {
        schema
            .read()
            .deprecated_tables
            .iter_deref()
            .find(|table| table.read().id == table_info.id)
            .map(|table| table.read().name.original().to_owned())
    });
    if let Some(existing) = existing {
        let action = if *flashback { "flashback" } else { "recover" };
        let name = match by_name {
            Some((_, table)) if *flashback => table.clone(),
            _ => table_info.name.original().to_owned(),
        };
        return Err(DdlPlanError::from(
            tidb_dbterror::CLASS_SCHEMA
                .new_std(tidb_errno::errcode::ErrTableExists)
                .generate(format!(
                    "Table '{name}' already been {action} to '{existing}', can't be {action} repeatedly"
                )),
        ));
    }
    let snapshot_ts = get_recover_snapshot_ts(&job);
    let auto_ids = at.auto_ids_at(snapshot_ts, job.schema_id, job.table_id)?;

    // Go `executor.RecoverTable`.
    let schema_id = job.schema_id;
    let Some(schema) = catalog.schema_by_id(schema_id) else {
        return Err(DdlPlanError::from(
            tidb_infoschema::infoschema_errors::ERR_DATABASE_NOT_EXISTS
                .generate_with_stack_by_args(&[format!("(Schema ID {schema_id})").into()]),
        ));
    };
    let schema_name = schema.read().name.clone();
    if schema
        .read()
        .deprecated_tables
        .iter_deref()
        .any(|existing| existing.read().name.lowercase() == table_info.name.lowercase())
    {
        return Err(DdlPlanError::from(
            tidb_infoschema::infoschema_errors::ERR_TABLE_EXISTS
                .generate_with_stack_by_args(&[table_info.name.original().into()]),
        ));
    }
    // FLASHBACK TABLE ... TO renames within the same schema.
    let involving_schema_info = if old_table_name != table_info.name.lowercase() {
        vec![
            tidb_model::InvolvingSchemaInfo {
                database: schema_name.lowercase().into(),
                table: old_table_name.as_str().into(),
                ..Default::default()
            },
            tidb_model::InvolvingSchemaInfo {
                database: schema_name.lowercase().into(),
                table: table_info.name.lowercase().into(),
                ..Default::default()
            },
        ]
    } else {
        Vec::new()
    };
    table_info.state = SchemaState::NONE;
    let mut recover_job = session_job(job_context, ActionType::ACTION_RECOVER_TABLE);
    recover_job.schema_id = schema_id;
    recover_job.table_id = table_info.id;
    recover_job.schema_name = schema_name.lowercase().into();
    recover_job.table_name = table_info.name.lowercase().into();
    recover_job.involving_schema_info = involving_schema_info.into();
    let recover_info = tidb_model::RecoverTableInfo {
        schema_id,
        table_info: Some(GoShared::new(table_info)),
        drop_job_id: job.id,
        snapshot_ts,
        auto_ids,
        old_schema_name: job.schema_name.to_string(),
        old_table_name,
    };
    let args = tidb_model::RecoverArgs {
        recover_info: Some(GoShared::new(tidb_model::RecoverSchemaInfo {
            recover_table_infos: Some(vec![GoShared::new(recover_info)]),
            ..Default::default()
        })),
        check_flag: 0,
        affected_physical_ids: Vec::new(),
    };
    Ok(tidb_ddl_jobsubmit::JobSpec {
        job: recover_job,
        args: tidb_model::JobArgsValue::Recover(Some(GoShared::new(args))),
        id_allocated: false,
    })
}

/// Go `GetRecoverSnapshotTS`.
fn get_recover_snapshot_ts(job: &Job) -> u64 {
    if job.real_start_ts != 0 {
        job.real_start_ts
    } else {
        job.start_ts
    }
}

/// Go `getRecoverTableByTableName` over `IterHistoryDDLJobs` and
/// `GetDropOrTruncateTableInfoFromJobsByStore`.
fn recover_table_by_name<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &SessionExtendedInfoSchema,
    schema: &str,
    table: &str,
    gc_safe_point: u64,
    at: &dyn SnapshotMetaAt,
) -> Result<(Job, TableInfo), DdlPlanError> {
    let schema_name = schema.go_to_lower();
    let table_name = table.go_to_lower();
    for job in history_ddl_jobs(snapshot)? {
        if !matches!(
            job.type_,
            ActionType::ACTION_DROP_TABLE | ActionType::ACTION_TRUNCATE_TABLE
        ) {
            continue;
        }
        let snapshot_ts = get_recover_snapshot_ts(&job);
        if let Err(error) = tidb_gcutil::validate_snapshot_with_gc_safe_point(snapshot_ts, gc_safe_point) {
            // Go: `ErrSnapshotTooOld` from the walk becomes this message.
            let tidb_gcutil::Error::SnapshotTooOld { safe_point_time } = error else {
                return Err(unknown(error.to_string()));
            };
            return Err(unknown(format!(
                "Can't find dropped/truncated table '{table}' in GC safe point {safe_point_time}"
            )));
        }
        // A drop whose schema is gone at its snapshot (a failed parallel
        // DDL) is skipped, as Go does.
        let Some(table_info) = at.table_at(snapshot_ts, job.schema_id, job.table_id)? else {
            continue;
        };
        if table_info.name.lowercase() != table_name {
            continue;
        }
        let Some(job_schema) = catalog.schema_by_id(job.schema_id) else {
            continue;
        };
        if job_schema.read().name.lowercase() != schema_name {
            continue;
        }
        // Dropping a local temporary table leaves no DDL job.
        if table_info.temp_table_type == tidb_model::TempTableType::GLOBAL {
            return Err(DdlPlanError::from(
                tidb_exeerrors::ERR_UNSUPPORTED_FLASHBACK_TMP_TABLE.generate_with_stack_by_args(&[]),
            ));
        }
        return Ok((job, table_info));
    }
    Err(unknown(format!(
        "Can't find localTemporary/dropped/truncated table: {table} in DDL history jobs"
    )))
}

/// Go `DDLExec.executeFlashbackDatabase` over `getRecoverDBByName`, then
/// `executor.RecoverSchema` up to the job.
pub(crate) fn build_recover_schema_job<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &SessionExtendedInfoSchema,
    statement: &DdlStatement,
    job_context: &dyn DdlSessionContext,
    gc_safe_point: u64,
    at: &dyn SnapshotMetaAt,
) -> Result<tidb_ddl_jobsubmit::JobSpec, DdlPlanError> {
    let DdlStatement::FlashbackDatabase { name, new_name } = statement else {
        unreachable!("called for FLASHBACK DATABASE");
    };
    let db_name = if new_name.is_empty() { name } else { new_name };
    // Check the schema name does not exist.
    if super::find_database(catalog, db_name).is_some() {
        return Err(DdlPlanError::from(
            tidb_infoschema::infoschema_errors::ERR_DATABASE_EXISTS
                .generate_with_stack_by_args(&[db_name.as_str().into()]),
        ));
    }
    let mut recover_schema_info = recover_schema_by_name(snapshot, name, gc_safe_point, at)?;
    let db_info = recover_schema_info
        .db_info
        .clone()
        .expect("getRecoverDBByName fills DBInfo");
    // Check the schema ID does not exist.
    let schema_id = db_info.read().id;
    if let Some(schema) = catalog.schema_by_id(schema_id) {
        return Err(DdlPlanError::from(
            tidb_infoschema::infoschema_errors::ERR_DATABASE_EXISTS.generate(format!(
                "Schema '{name}' already been recover to '{}', can't be recover repeatedly",
                schema.read().name.original()
            )),
        ));
    }
    db_info.write().name = tidb_ast::CiString::new(db_name);

    // Go `executor.RecoverSchema`.
    let db_lower = db_info.read().name.lowercase().to_owned();
    let mut involving_schema_info = vec![tidb_model::InvolvingSchemaInfo {
        database: db_lower.as_str().into(),
        table: tidb_model::job::INVOLVING_ALL.into(),
        ..Default::default()
    }];
    if recover_schema_info.old_schema_name.lowercase() != db_lower {
        involving_schema_info.push(tidb_model::InvolvingSchemaInfo {
            database: recover_schema_info.old_schema_name.lowercase().into(),
            table: tidb_model::job::INVOLVING_ALL.into(),
            ..Default::default()
        });
    }
    db_info.write().state = SchemaState::NONE;
    recover_schema_info.db_info = Some(db_info);
    let mut recover_job = session_job(job_context, ActionType::ACTION_RECOVER_SCHEMA);
    recover_job.involving_schema_info = involving_schema_info.into();
    let args = tidb_model::RecoverArgs {
        recover_info: Some(GoShared::new(recover_schema_info)),
        check_flag: 0,
        affected_physical_ids: Vec::new(),
    };
    Ok(tidb_ddl_jobsubmit::JobSpec {
        job: recover_job,
        args: tidb_model::JobArgsValue::Recover(Some(GoShared::new(args))),
        id_allocated: false,
    })
}

/// Go `getRecoverDBByName`: the newest `DROP DATABASE` of `schema_name` in
/// the DDL history, inside the GC safe point, read at its snapshot.
fn recover_schema_by_name<S: MetaSnapshot>(
    snapshot: &mut S,
    schema_name: &str,
    gc_safe_point: u64,
    at: &dyn SnapshotMetaAt,
) -> Result<tidb_model::RecoverSchemaInfo, DdlPlanError> {
    let lower = schema_name.go_to_lower();
    for job in history_ddl_jobs(snapshot)? {
        if job.type_ != ActionType::ACTION_DROP_SCHEMA {
            continue;
        }
        let snapshot_ts = get_recover_snapshot_ts(&job);
        // Check the GC safe point for reading the snapshot.
        if let Err(error) = tidb_gcutil::validate_snapshot_with_gc_safe_point(snapshot_ts, gc_safe_point) {
            let tidb_gcutil::Error::SnapshotTooOld { safe_point_time } = error else {
                return Err(unknown(error.to_string()));
            };
            return Err(unknown(format!(
                "Can't find dropped database '{schema_name}' in GC safe point {safe_point_time}"
            )));
        }
        // A drop that failed under a parallel DDL leaves no schema at its
        // snapshot; it is skipped.
        let Some(schema_info) = at.database_at(snapshot_ts, job.schema_id)? else {
            continue;
        };
        if schema_info.name.lowercase() != lower {
            continue;
        }
        return Ok(tidb_model::RecoverSchemaInfo {
            db_info: Some(GoShared::new(schema_info)),
            recover_table_infos: None,
            load_tables_on_execute: true,
            drop_job_id: job.id,
            snapshot_ts,
            old_schema_name: tidb_ast::CiString::new(schema_name),
        });
    }
    Err(unknown(format!(
        "Can't find dropped database: {schema_name} in DDL history jobs"
    )))
}

/// Go `IterHistoryDDLJobs`' order: the history newest first, in batches of
/// `DefNumHistoryJobs`.
fn history_ddl_jobs<S: MetaSnapshot>(snapshot: &mut S) -> Result<Vec<Job>, DdlPlanError> {
    tidb_infoschema::cluster_catalog::with_meta_reader(snapshot, |reader| {
        let mut iterator = reader.get_last_history_ddl_jobs_iterator::<Job>()?;
        let mut jobs = Vec::new();
        loop {
            let mut batch = Vec::new();
            tidb_meta::transaction::LastJobIterator::get_last_jobs(&mut iterator, 10, &mut batch)?;
            if batch.is_empty() {
                return Ok(jobs);
            }
            jobs.extend(batch);
        }
    })
    .map_err(DdlPlanError::Meta)
}
