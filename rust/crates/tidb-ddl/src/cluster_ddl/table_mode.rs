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

//! `pkg/ddl/table_mode.go` and BR's `RefreshMeta`: the two internal jobs
//! IMPORT INTO and BR queue by ID. `executor.AlterTableMode` resolves the
//! table in the latest schema and queues `ActionAlterTableMode`, which the
//! owner's `onAlterTableMode` applies in one step; `executor.RefreshMeta`
//! queues `ActionRefreshMeta`, whose `onRefreshMeta` only publishes a schema
//! version so every node reloads the table from the store.

use super::add_index::{action_step, update_version_and_table_info, AddIndexSubmission};
use super::*;

fn invalid_table_mode_set(
    current: tidb_model::TableMode,
    target: tidb_model::TableMode,
    table: &str,
) -> DdlPlanError {
    DdlPlanError::from(
        tidb_infoschema::infoschema_errors::ERR_INVALID_TABLE_MODE_SET.gen_with_stack_by_args(&[
            current.to_string().into(),
            target.to_string().into(),
            table.into(),
        ]),
    )
}

/// Go `executor.AlterTableMode` and `executor.RefreshMeta` up to
/// `doDDLJob2`. `None` for any other statement.
pub fn alter_table_mode<S: MetaSnapshot>(
    snapshot: &mut S,
    statement: &DdlStatement,
    job_context: &dyn DdlSessionContext,
    start_ts: u64,
    upgrading: bool,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    match statement {
        DdlStatement::AlterTableMode {
            table_mode,
            schema_id,
            table_id,
        } => alter_table_mode_by_id(
            snapshot,
            *table_mode,
            *schema_id,
            *table_id,
            job_context,
            start_ts,
            upgrading,
        ),
        DdlStatement::RefreshMeta {
            schema_id,
            table_id,
            involved_db,
            involved_table,
        } => refresh_meta(
            snapshot,
            *schema_id,
            *table_id,
            involved_db,
            involved_table,
            job_context,
            start_ts,
            upgrading,
        ),
        _ => Ok(None),
    }
}

/// Go `executor.AlterTableMode` with its typed internal arguments.
pub fn alter_table_mode_by_id<S: MetaSnapshot>(
    snapshot: &mut S,
    table_mode: tidb_model::TableMode,
    schema_id: i64,
    table_id: i64,
    job_context: &dyn DdlSessionContext,
    start_ts: u64,
    upgrading: bool,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    let session = |action| {
        let mut job = session_job(job_context, action);
        job.query = "skip".into();
        job
    };
    let (job, args) = {
        let catalog = load_info_schema(snapshot)?;
        let Some(schema) = catalog.schema_by_id(schema_id) else {
            return Err(DdlPlanError::from(
                tidb_infoschema::infoschema_errors::ERR_DATABASE_NOT_EXISTS
                    .gen_with_stack_by_args(&[format!("SchemaID: {schema_id}").into()]),
            ));
        };
        let schema_name = schema.read().name.clone();
        // Go `is.TableByID`: the table by its ID alone.
        let schemas = catalog.stored_schemas();
        let Some(stored) = schemas.iter().find_map(|database| {
            database
                .read()
                .deprecated_tables
                .iter_deref()
                .find(|stored| stored.read().id == table_id)
        }) else {
            return Err(DdlPlanError::from(
                tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS.gen_with_stack_by_args(&[
                    schema_name.original().into(),
                    format!("TableID: {table_id}").into(),
                ]),
            ));
        };
        let stored = stored.read();
        let built = tidb_ddl_jobsubmit::build_alter_table_mode_job(
            job_context.ddl_cdc_write_source(),
            job_context.ddl_sql_mode(),
            &tidb_model::AlterTableModeTarget {
                schema_id: schema_id,
                schema_name,
                table_id: table_id,
                table_name: stored.name.clone(),
                current_mode: stored.mode,
                target_mode: table_mode,
            },
        )?;
        let Some((mut job, args)) = built else {
            return Ok(Some(AddIndexSubmission::AlreadySatisfied {
                detail: "the table is already in the requested mode".to_owned(),
                warnings: Vec::new(),
            }));
        };
        // Go `DoDDLJobWrapper` adds the session's trace info.
        job.trace_info = session(ActionType::ACTION_ALTER_TABLE_MODE).trace_info;
        (job, JobArgsValue::AlterTableMode(Some(args)))
    };
    let built = AddIndexSubmission::Job(
        Box::new(tidb_ddl_jobsubmit::JobSpec {
            job,
            args,
            id_allocated: true,
        }),
        Vec::new(),
    );
    submit_built_job(snapshot, Some(built), start_ts, upgrading)
}

/// Go `executor.RefreshMeta` with its typed internal arguments.
pub fn refresh_meta<S: MetaSnapshot>(
    snapshot: &mut S,
    schema_id: i64,
    table_id: i64,
    involved_db: &str,
    involved_table: &str,
    job_context: &dyn DdlSessionContext,
    start_ts: u64,
    upgrading: bool,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    let session = |action| {
        let mut job = session_job(job_context, action);
        job.query = "skip".into();
        job
    };
    let (job, args) = {
        let mut job = session(ActionType::ACTION_REFRESH_META);
        job.version = tidb_model::JobVersion::V2;
        job.schema_id = schema_id;
        job.table_id = table_id;
        job.schema_name = involved_db.to_owned().into();
        job.involving_schema_info =
            GoSharedSlice::from_vec(vec![tidb_model::InvolvingSchemaInfo {
                database: involved_db.to_owned().into(),
                table: involved_table.to_owned().into(),
                ..tidb_model::InvolvingSchemaInfo::default()
            }]);
        let args = tidb_model::RefreshMetaArgs {
            schema_id: tidb_model::GoField::new(schema_id),
            table_id: tidb_model::GoField::new(table_id),
            involved_database: tidb_model::GoField::new(involved_db.to_owned().into()),
            involved_table: tidb_model::GoField::new(involved_table.to_owned().into()),
        };
        (job, JobArgsValue::RefreshMeta(Some(GoShared::new(args))))
    };
    let built = AddIndexSubmission::Job(
        Box::new(tidb_ddl_jobsubmit::JobSpec {
            job,
            args,
            id_allocated: true,
        }),
        Vec::new(),
    );
    submit_built_job(snapshot, Some(built), start_ts, upgrading)
}

/// Go `onAlterTableMode`.
pub(super) fn on_alter_table_mode(
    txn: &mut JobTxn<'_>,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let args = tidb_model::get_alter_table_mode_args(&mut active.job)
        .map_err(|error| DdlPlanError::Encode(error.to_string()))?
        .ok_or_else(|| DdlPlanError::Encode("alter table mode job has nil args".to_owned()))?;
    let target = args.read().table_mode.get();
    let mut info = checked_job_table(txn, active, false)?.read().clone_like_go();
    let schema_id = active.job.schema_id;
    match info.mode {
        tidb_model::TableMode::NORMAL
        | tidb_model::TableMode::IMPORT
        | tidb_model::TableMode::RESTORE => {
            if info.mode == target {
                active.job.state = JobState::DONE;
                return Ok(action_step(job_only_write(active.job.id, Vec::new())));
            }
            // Go `alterTableMode`: directly to the target mode.
            if !info.mode.can_transition_to(target) {
                return Err(cancel_ddl_job(
                    active,
                    invalid_table_mode_set(info.mode, target, info.name.original()),
                ));
            }
            info.mode = target;
            let write =
                update_version_and_table_info(txn, active, schema_id, &info, true, start_ts)?;
            active.job.finish_table_job(
                JobState::DONE,
                SchemaState::PUBLIC,
                write.schema_version,
                Some(GoShared::new(info.clone_like_go())),
            );
            Ok(action_step(write))
        }
        mode => Err(cancel_ddl_job(
            active,
            invalid_table_mode_set(mode, target, info.name.original()),
        )),
    }
}

/// Go `onRefreshMeta`.
pub(super) fn on_refresh_meta(
    txn: &mut JobTxn<'_>,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    tidb_model::get_refresh_meta_args(&mut active.job)
        .map_err(|error| cancel_ddl_job(active, DdlPlanError::Encode(error.to_string())))?;
    let meta = txn.meta();
    let diff = SchemaDiff {
        action_type: active.job.type_,
        schema_id: active.job.schema_id,
        table_id: active.job.table_id,
        ..SchemaDiff::default()
    };
    let (schema_version, diff) = update_schema_version(&meta, active, diff)?;
    active.job.state = JobState::DONE;
    active.job.schema_state = SchemaState::PUBLIC;
    let mut write = job_only_write(active.job.id, Vec::new());
    write.schema_version = schema_version;
    write.diff = diff;
    Ok(action_step(write))
}
