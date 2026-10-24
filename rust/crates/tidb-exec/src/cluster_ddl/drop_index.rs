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

//! DROP INDEX / DROP PRIMARY KEY as Go runs them: the executor's
//! `dropIndex` builds a persisted job, and the owner steps it through
//! `onDropIndex` -- write-only, delete-only, delete-reorganization, gone --
//! leaving the entries to the delete range the finished job records.

use super::add_index::{
    action_step, partition_ids, remove_index_and_hidden_columns, table_step, AddIndexSubmission,
};
use super::*;

/// Go `executor.dropIndex` up to `doDDLJob2` for `DROP INDEX` and
/// `DROP PRIMARY KEY`. `None` for any other statement.
pub fn prepare_drop_index_job_submission<S: MetaSnapshot>(
    snapshot: &mut S,
    statement: &DdlStatement,
    job_context: &tidb_executor::StmtContext,
    start_ts: u64,
    upgrading: bool,
    min_job_id: i64,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    let catalog = load_cluster_catalog(snapshot)?;
    let built = build_drop_index_job(snapshot, &catalog, statement, job_context)?;
    submit_built_job(snapshot, &catalog, built, start_ts, upgrading, min_job_id)
}

/// The executor half without `doDDLJob2`: the job unsubmitted, which a
/// multi-schema change appends as a sub-job (Go `appendToSubJobs`).
pub(super) fn build_drop_index_job<S: MetaSnapshot>(
    _snapshot: &mut S,
    catalog: &ClusterCatalog,
    statement: &DdlStatement,
    job_context: &tidb_executor::StmtContext,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    let (schema, table, index_name, if_exists) = match statement {
        DdlStatement::DropIndex {
            schema,
            table,
            index,
            if_exists,
        } => (schema, table, index.clone(), *if_exists),
        DdlStatement::DropPrimaryKey { schema, table } => {
            (schema, table, "PRIMARY".to_owned(), false)
        }
        _ => return Ok(None),
    };
    let (schema_id, stored) = locate_table(&catalog, schema, table)?;
    let index = find_index(stored, &index_name);
    // Go `CheckIsDropPrimaryKey`: a general index merely named `primary` is
    // not the primary key (#14243).
    let is_pk = index_name.go_to_lower() == "primary"
        && index.as_ref().is_none_or(|index| index.read().primary);
    if is_pk {
        if index.is_none() && !stored.pk_is_handle {
            return Err(DdlPlanError::UnknownIndex("PRIMARY".to_owned()));
        }
        if stored.is_common_handle || stored.pk_is_handle {
            return Err(DdlPlanError::Admission(
                crate::table_info_build::DdlAdmissionError::with_proto(
                    &tidb_util::dbterror::ERR_RUN_MULTI_SCHEMA_CHANGES,
                    "Unsupported drop primary key when the table is using clustered index"
                        .to_owned(),
                ),
            ));
        }
    }
    let Some(index) = index else {
        // Go `ErrCantDropFieldOrKey.GenWithStack("index %s doesn't exist")`,
        // a note under IF EXISTS.
        if if_exists {
            return Ok(Some(AddIndexSubmission::AlreadySatisfied {
                detail: format!("index `{index_name}` does not exist on `{schema}`.`{table}`"),
                warnings: vec![(
                    DdlWarningLevel::Note,
                    1091,
                    format!("index {index_name} doesn't exist"),
                )],
            }));
        }
        return Err(DdlPlanError::IndexDoesNotExist(index_name));
    };
    let index = index.read().clone_like_go();
    crate::foreign_key_build::check_index_needed_in_foreign_key(&catalog, schema, stored, &index)
        .map_err(DdlPlanError::Admission)?;

    let action = if is_pk {
        ActionType::ACTION_DROP_PRIMARY_KEY
    } else {
        ActionType::ACTION_DROP_INDEX
    };
    let mut job = session_job(job_context, action);
    job.schema_id = schema_id;
    job.table_id = stored.id;
    job.schema_name = schema.go_to_lower().into();
    job.schema_state = index.state;
    job.table_name = stored.name.lowercase().to_owned().into();
    let args = GoShared::new(tidb_model::ModifyIndexArgs {
        index_args: vec![tidb_model::IndexArg {
            index_name: CiString::new(index_name),
            if_exist: if_exists,
            ..tidb_model::IndexArg::default()
        }]
        .into(),
        op_type: tidb_model::IndexOp::DROP_INDEX,
        ..tidb_model::ModifyIndexArgs::default()
    });
    Ok(Some(AddIndexSubmission::Job(
        Box::new(crate::ddl_job_submit::JobSpec {
            job,
            args: JobArgsValue::ModifyIndex(Some(args)),
            id_allocated: true,
        }),
        Vec::new(),
    )))
}

/// Go `checkDropIndex`: the indexes the job names, each still there and not
/// needed by a foreign key. `Ok(Err(if_exists))` when one is missing.
fn check_drop_index(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
) -> Result<
    (
        TableInfo,
        Result<Vec<GoShared<IndexInfo>>, (bool, DdlPlanError)>,
    ),
    DdlPlanError,
> {
    let stored = checked_job_table(catalog, active, false)?.clone_like_go();
    let args = tidb_model::get_drop_index_args(&mut active.job)
        .map_err(|error| cancel_ddl_job(active, DdlPlanError::Encode(error.to_string())))?
        .ok_or_else(|| {
            cancel_ddl_job(
                active,
                DdlPlanError::Encode("DROP INDEX has nil args".to_owned()),
            )
        })?;
    let schema = active.job.schema_name.to_string();
    let mut indexes = Vec::new();
    for arg in args.read().index_args.iter_deref() {
        let arg = arg.read();
        let name = arg.index_name.original().to_owned();
        let Some(index) = find_index(&stored, &name) else {
            active.job.state = JobState::CANCELLED;
            return Ok((
                stored,
                Err((arg.if_exist, DdlPlanError::IndexDoesNotExist(name))),
            ));
        };
        let found = index.read().clone_like_go();
        // Go `checkInvisibleIndexesOnPK`: what is left must not make an
        // invisible unique index the implicit primary key.
        let mut remaining = stored.clone_like_go();
        let kept: Vec<IndexInfo> = remaining
            .indices
            .iter_deref()
            .filter(|other| other.read().name.lowercase() != found.name.lowercase())
            .map(|other| other.read().clone_like_go())
            .collect();
        remaining.indices = kept.into();
        super::check_invisible_index_on_pk(&remaining)
            .map_err(|error| cancel_ddl_job(active, error))?;
        // Go `checkIndexNeededInForeignKeyInOwner`.
        crate::foreign_key_build::check_index_needed_in_foreign_key(
            catalog, &schema, &stored, &found,
        )
        .map_err(|error| cancel_ddl_job(active, DdlPlanError::Admission(error)))?;
        indexes.push(index);
    }
    Ok((stored, Ok(indexes)))
}

/// Go `onDropIndex` for a DROP INDEX / DROP PRIMARY KEY job.
pub(super) fn plan_persisted_drop_index_job_step(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let (stored, checked) = check_drop_index(catalog, active)?;
    let indexes = match checked {
        Ok(indexes) => indexes,
        Err((true, error)) if matches!(error, DdlPlanError::IndexDoesNotExist(_)) => {
            // Go: under IF EXISTS the job finishes with the 1091 as its
            // warning.
            active.job.warning = Some(GoShared::new(
                tidb_util::dbterror::ERR_CANT_DROP_FIELD_OR_KEY.generate(error.to_string()),
            ));
            active.job.state = JobState::DONE;
            return Ok(PersistedDdlActionStep {
                write: job_only_write(active.job.id, Vec::new()),
                update_raw_args: true,
                run_error: None,
                reorg: None,
            });
        }
        Err((_, error)) => return Err(error),
    };
    let database_id = active.job.schema_id;
    let mut info = stored.clone_like_go();
    let indexes: Vec<GoShared<IndexInfo>> = indexes
        .iter()
        .filter_map(|index| find_index(&info, index.read().name.original()))
        .collect();
    let original_state = indexes[0].read().state;
    // Go: a multi-schema sub-job stops at its revertible point first.
    if !active.job.is_rollingback() && is_revertible_sub_job(&active.job) {
        active.job.mark_non_revertible();
        active.job.schema_state = original_state;
        let write = table_step(catalog, active, database_id, &info, false, start_ts)?;
        return Ok(action_step(write));
    }
    let next = match original_state {
        SchemaState::PUBLIC => SchemaState::WRITE_ONLY,
        SchemaState::WRITE_ONLY => SchemaState::DELETE_ONLY,
        SchemaState::DELETE_ONLY => SchemaState::DELETE_REORGANIZATION,
        SchemaState::DELETE_REORGANIZATION => SchemaState::NONE,
        state => {
            return Err(DdlPlanError::from(
                tidb_util::dbterror::ERR_INVALID_DDL_STATE.generate_with_stack_by_args(&[
                    FormatArg::from("index"),
                    FormatArg::new(state.to_string(), state.0.to_string(), "model.SchemaState"),
                ]),
            ))
        }
    };
    if next != SchemaState::NONE {
        for index in &indexes {
            index.write().state = next;
        }
        active.job.schema_state = next;
        let write = table_step(catalog, active, database_id, &info, true, start_ts)?;
        return Ok(action_step(write));
    }
    // Delete-reorganization -> absent.
    let first = indexes[0].read().clone_like_go();
    let mut index_ids = Vec::new();
    for index in &indexes {
        let removed = index.read().clone_like_go();
        index_ids.push(removed.id);
        remove_index_and_hidden_columns(&mut info, &removed);
    }
    let write = table_step(catalog, active, database_id, &info, true, start_ts)?;
    active.job.finish_table_job(
        JobState::DONE,
        SchemaState::NONE,
        write.schema_version,
        Some(GoShared::new(info.clone_like_go())),
    );
    // Go fills the drop args back with the index id, its columnar kind and,
    // for a non-global index, the partitions whose entries the delete range
    // covers.
    let drop_args = tidb_model::get_drop_index_args(&mut active.job)
        .map_err(|error| DdlPlanError::Encode(error.to_string()))?
        .ok_or_else(|| DdlPlanError::Encode("DROP INDEX has nil args".to_owned()))?;
    {
        let mut args = drop_args.write();
        args.op_type = tidb_model::IndexOp::DROP_INDEX;
        if let Some(arg) = args.index_args.get(0) {
            let mut arg = arg.write();
            arg.index_id = index_ids[0];
            arg.is_columnar = first.is_columnar_index();
        }
        if !first.global {
            args.partition_ids = partition_ids(&stored).into();
        }
    }
    active.job.fill_finished_args(Some(drop_args));
    active.job.schema_state = SchemaState::NONE;
    Ok(PersistedDdlActionStep {
        write,
        // Go `needUpdateRawArgs`: history and the delete range read these.
        update_raw_args: true,
        run_error: None,
        reorg: None,
    })
}

/// Go `rollingbackDropIndex`: only a job whose index is still public can be
/// cancelled; past that the drop runs on.
pub(super) fn plan_cancel_drop_index(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let (_, checked) = check_drop_index(catalog, active)?;
    let indexes = checked.map_err(|(_, error)| error)?;
    if indexes[0].read().state == SchemaState::PUBLIC {
        return Err(cancelled_ddl_job(active));
    }
    active.job.state = JobState::RUNNING;
    Ok(job_control_step(&active.job))
}
