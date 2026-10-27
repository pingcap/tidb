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

//! DROP COLUMN as Go runs it: the executor's `DropColumn` queues a job, and
//! the owner steps it through `onDropColumn` -- write-only, delete-only,
//! delete-reorganization, gone -- leaving the entries of the single-column
//! indexes that go with it to the delete range. A rolling-back ADD COLUMN
//! runs the same steps from delete-only.

use super::add_index::{action_step, partition_ids, table_step, AddIndexSubmission};
use super::*;

/// Go `executor.DropColumn` up to `doDDLJob2`. `None` for any other
/// statement.
pub fn prepare_drop_column_job_submission<S: MetaSnapshot>(
    snapshot: &mut S,
    statement: &DdlStatement,
    job_context: &dyn DdlSessionContext,
    start_ts: u64,
    upgrading: bool,
    min_job_id: i64,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    let catalog = load_cluster_catalog(snapshot)?;
    let built = build_drop_column_job(snapshot, &catalog, statement, job_context)?;
    submit_built_job(snapshot, &catalog, built, start_ts, upgrading, min_job_id)
}

/// The executor half without `doDDLJob2`: the job unsubmitted, which a
/// multi-schema change appends as a sub-job (Go `appendToSubJobs`).
pub(super) fn build_drop_column_job<S: MetaSnapshot>(
    _snapshot: &mut S,
    catalog: &ClusterCatalog,
    statement: &DdlStatement,
    job_context: &dyn DdlSessionContext,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    let DdlStatement::DropColumn {
        schema,
        table,
        if_exists,
        column,
        allow_remove_auto_inc,
    } = statement
    else {
        return Ok(None);
    };
    let (schema_id, stored) = locate_table(&catalog, schema, table)?;
    if !check_is_droppable_column(
        &catalog,
        schema,
        stored,
        column,
        *if_exists,
        *allow_remove_auto_inc,
    )? {
        // Go `checkIsDroppableColumn`: IF EXISTS appends the 1091 note.
        return Ok(Some(AddIndexSubmission::AlreadySatisfied {
            detail: format!("column `{column}` does not exist on `{schema}`.`{table}`"),
            warnings: vec![crate::real_tikv_ddl::ddl_note(
                &tidb_util::dbterror::ERR_CANT_DROP_FIELD_OR_KEY,
                format!("Can't DROP '{column}'; check that column/key exists"),
            )],
        }));
    }
    check_visible_column_cnt(stored, 0, 1)?;
    let mut job = session_job(job_context, ActionType::ACTION_DROP_COLUMN);
    job.schema_id = schema_id;
    job.table_id = stored.id;
    job.schema_name = schema.go_to_lower().into();
    job.schema_state = SchemaState::PUBLIC;
    job.table_name = stored.name.lowercase().to_owned().into();
    let args = GoShared::new(tidb_model::TableColumnArgs {
        col: Some(GoShared::new(tidb_model::ColumnInfo {
            name: CiString::new(column.clone()),
            ..tidb_model::ColumnInfo::default()
        })),
        ignore_existence_err: *if_exists,
        ..tidb_model::TableColumnArgs::default()
    });
    Ok(Some(AddIndexSubmission::Job(
        Box::new(crate::ddl_job_submit::JobSpec {
            job,
            args: JobArgsValue::TableColumn(Some(args)),
            id_allocated: true,
        }),
        Vec::new(),
    )))
}

/// What Go `checkDropColumn` found.
struct DropColumnTarget {
    info: TableInfo,
    column: GoShared<tidb_model::ColumnInfo>,
    indexes: Vec<GoShared<IndexInfo>>,
    args: GoShared<tidb_model::TableColumnArgs>,
}

/// Go `checkDropColumn`. `Ok(Err((if_exists, error)))` when the column is
/// not there (the job is cancelled).
fn check_drop_column_in_owner(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
) -> Result<Result<DropColumnTarget, (bool, DdlPlanError)>, DdlPlanError> {
    let stored = checked_job_table(catalog, active, true)?.clone_like_go();
    let args = tidb_model::get_table_column_args(&mut active.job)
        .map_err(|error| cancel_ddl_job(active, DdlPlanError::Encode(error.to_string())))?
        .ok_or_else(|| {
            cancel_ddl_job(
                active,
                DdlPlanError::Encode("DROP COLUMN job has nil args".to_owned()),
            )
        })?;
    let (name, if_exists) = {
        let args = args.read();
        (args.col_name(), args.ignore_existence_err)
    };
    let info = stored;
    let Some(column) = tidb_model::column::find_column_info(&info.columns, name.lowercase())
        .filter(|column| !column.read().hidden)
    else {
        active.job.state = JobState::CANCELLED;
        return Ok(Err((
            if_exists,
            coded_refusal(
                &tidb_util::dbterror::ERR_CANT_DROP_FIELD_OR_KEY,
                format!("column {} doesn't exist", name.original()),
            ),
        )));
    };
    is_droppable_column(&info, name.original(), info.columns.len())
        .map_err(|error| cancel_ddl_job(active, error))?;
    // Go `checkDropColumnWithForeignKeyConstraintInOwner` cancels the job.
    if tidb_vardef::ENABLE_FOREIGN_KEY.load(std::sync::atomic::Ordering::SeqCst) {
        let schema = active.job.schema_name.to_string();
        crate::foreign_key_build::check_drop_column_with_foreign_key_constraint(
            catalog,
            &schema,
            &info,
            name.lowercase(),
        )
        .map_err(|error| cancel_ddl_job(active, DdlPlanError::Admission(error)))?;
    }
    check_drop_column_with_ttl_config(&info, name.lowercase())?;
    // Go `listIndicesWithColumn`: the single-column indexes on the column.
    let indexes = info
        .indices
        .iter_deref()
        .filter(|index| {
            let index = index.read();
            index.columns.len() == 1
                && index
                    .columns
                    .iter_deref()
                    .next()
                    .is_some_and(|part| part.read().name.lowercase() == name.lowercase())
        })
        .collect();
    Ok(Ok(DropColumnTarget {
        info,
        column,
        indexes,
        args,
    }))
}

/// Go `onDropColumn` for a DROP COLUMN job, and for an ADD COLUMN job
/// rolling back.
pub(super) fn plan_persisted_drop_column_job_step(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let DropColumnTarget {
        mut info,
        column,
        indexes,
        args,
    } = match check_drop_column_in_owner(catalog, active)? {
        Ok(target) => target,
        Err((true, error)) => {
            // Go: IF EXISTS turns the error into the job's warning.
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
        Err((false, error)) => return Err(error),
    };
    let database_id = active.job.schema_id;
    let original_state = column.read().state;
    // Go: a multi-schema sub-job stops at its revertible point first.
    if !active.job.is_rollingback() && is_revertible_sub_job(&active.job) {
        active.job.mark_non_revertible();
        active.job.schema_state = original_state;
        check_table_info_valid(&info)?;
        let write = table_step(catalog, active, database_id, &info, false, start_ts)?;
        return Ok(action_step(write));
    }
    let move_to_back = |info: &mut TableInfo| {
        let offset = usize::try_from(column.read().offset).unwrap_or(0);
        let last = info.columns.len() - 1;
        move_column_info(info, offset, last);
    };
    let mut finished = false;
    match original_state {
        SchemaState::PUBLIC => {
            column.write().state = SchemaState::WRITE_ONLY;
            for index in &indexes {
                index.write().state = SchemaState::WRITE_ONLY;
            }
            move_to_back(&mut info);
            tidb_ddl_build::table_info_build::stamp_dropped_column_origin_default(&mut column.write())
                .map_err(DdlPlanError::Admission)?;
            check_table_info_valid(&info)?;
        }
        SchemaState::WRITE_ONLY => {
            column.write().state = SchemaState::DELETE_ONLY;
            move_to_back(&mut info);
            if !indexes.is_empty() {
                let dropped: Vec<i64> = indexes.iter().map(|index| index.read().id).collect();
                let kept: Vec<_> = (0..info.indices.len())
                    .filter_map(|position| {
                        let handle = info.indices.get(position)?;
                        let id = handle.read().id;
                        (!dropped.contains(&id)).then_some(Some(handle))
                    })
                    .collect();
                info.indices = tidb_model::GoSharedPointerSlice::from_handles(kept);
            }
            args.write().index_ids = indexes.iter().map(|index| index.read().id).collect();
            active.job.fill_args(Some(args.clone()));
        }
        SchemaState::DELETE_ONLY => {
            column.write().state = SchemaState::DELETE_REORGANIZATION;
            move_to_back(&mut info);
        }
        SchemaState::DELETE_REORGANIZATION => {
            move_to_back(&mut info);
            let last = info.columns.len() - 1;
            info.columns.delete_go(last, last + 1);
            column.write().state = SchemaState::NONE;
            finished = true;
        }
        _ => {
            return Err(DdlPlanError::from(
                tidb_util::dbterror::ERR_INVALID_DDL_JOB.generate_with_stack_by_args(&[
                    FormatArg::from("table"),
                    FormatArg::new(
                        info.state.to_string(),
                        info.state.0.to_string(),
                        "model.SchemaState",
                    ),
                ]),
            ))
        }
    }
    let new_state = column.read().state;
    let write = table_step(
        catalog,
        active,
        database_id,
        &info,
        original_state != new_state,
        start_ts,
    )?;
    if finished {
        if active.job.is_rollingback() {
            active.job.finish_table_job(
                JobState::ROLLBACK_DONE,
                SchemaState::NONE,
                write.schema_version,
                Some(GoShared::new(info.clone_like_go())),
            );
        } else {
            active.job.finish_table_job(
                JobState::DONE,
                SchemaState::NONE,
                write.schema_version,
                Some(GoShared::new(info.clone_like_go())),
            );
            args.write().partition_ids = partition_ids(&info);
            active.job.fill_args(Some(args.clone()));
        }
    }
    active.job.schema_state = new_state;
    Ok(action_step(write))
}

/// Go `rollingbackDropColumn`: cancellable only before the job has run; past
/// that the drop runs on.
pub(super) fn plan_cancel_drop_column(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let target = check_drop_column_in_owner(catalog, active)?.map_err(|(_, error)| error)?;
    for index in &target.indexes {
        match index.read().state {
            SchemaState::WRITE_ONLY
            | SchemaState::DELETE_ONLY
            | SchemaState::DELETE_REORGANIZATION
            | SchemaState::NONE => {
                active.job.state = JobState::RUNNING;
                return Ok(job_control_step(&active.job));
            }
            SchemaState::PUBLIC => {}
            state => {
                return Err(DdlPlanError::from(
                    tidb_util::dbterror::ERR_INVALID_DDL_STATE.generate_with_stack_by_args(&[
                        FormatArg::from("index"),
                        FormatArg::new(state.to_string(), state.0.to_string(), "model.SchemaState"),
                    ]),
                ))
            }
        }
    }
    if target.column.read().state == SchemaState::PUBLIC {
        return Err(cancelled_ddl_job(active));
    }
    active.job.state = JobState::RUNNING;
    Ok(job_control_step(&active.job))
}
