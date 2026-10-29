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

//! ADD COLUMN as Go runs it: the executor's `AddColumn` builds the column and
//! queues a job, and the owner's `onAddColumn` appends it with a fresh id and
//! steps it through delete-only, write-only and write-reorganization before
//! moving it to its position and publishing it. Cancelled, it rolls back
//! through `onDropColumn` (`rollingbackAddColumn`).

use super::add_index::{action_step, table_step, AddIndexSubmission};
use super::*;

/// Go `executor.AddColumn` up to `doDDLJob2`. `None` for any other
/// statement.
pub fn prepare_add_column_job_submission<S: MetaSnapshot>(
    snapshot: &mut S,
    statement: &DdlStatement,
    job_context: &dyn DdlSessionContext,
    start_ts: u64,
    upgrading: bool,
    min_job_id: i64,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    let catalog = load_cluster_catalog(snapshot)?;
    let built = build_add_column_job(snapshot, &catalog, statement, job_context)?;
    submit_built_job(snapshot, &catalog, built, start_ts, upgrading, min_job_id)
}

/// The executor half without `doDDLJob2`: the job unsubmitted, which a
/// multi-schema change appends as a sub-job (Go `appendToSubJobs`).
pub(super) fn build_add_column_job<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    statement: &DdlStatement,
    job_context: &dyn DdlSessionContext,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    let DdlStatement::AddColumn {
        schema,
        table,
        if_not_exists,
        column,
        position,
        context,
    } = statement
    else {
        return Ok(None);
    };
    let (schema_id, stored) = locate_table(&catalog, schema, table)?;
    let (mut added, _) = match build_add_column(
        stored,
        schema,
        table,
        column,
        position,
        *if_not_exists,
        &context.0,
    )? {
        Ok(built) => built,
        Err(detail) => {
            // Go `checkAndCreateNewColumn`: IF NOT EXISTS appends the 1060
            // note.
            return Ok(Some(AddIndexSubmission::AlreadySatisfied {
                detail,
                warnings: vec![crate::real_tikv_ddl::ddl_note(
                    tidb_error::catalog::infoschema::ERR_COLUMN_EXISTS
                        .generate_with_stack_by_args(&[column.name.clone().into()]),
                )],
            }));
        }
    };
    let bdr_role = snapshot.get(&key::bdr_role_kv_key())?.unwrap_or_default();
    crate::cluster_bdr::admit_add_column(&bdr_role, schema, &column.options)?;
    // Go `columnDefToCol(ctx, len(t.Cols()), ...)`: the owner allocates the
    // id and moves the column.
    added.id = 0;
    added.offset = stored
        .columns
        .iter_deref()
        .filter(|column| column.read().state == SchemaState::PUBLIC)
        .count() as i64;
    added.state = SchemaState::NONE;
    let mut job = session_job(job_context, ActionType::ACTION_ADD_COLUMN);
    job.schema_id = schema_id;
    job.table_id = stored.id;
    job.schema_name = schema.go_to_lower().into();
    job.table_name = stored.name.lowercase().to_owned().into();
    let args = GoShared::new(tidb_model::TableColumnArgs {
        col: Some(GoShared::new(added)),
        pos: Some(GoShared::new(tidb_ast::JobColumnPosition::from_statement(
            position,
        ))),
        ignore_existence_err: *if_not_exists,
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

/// What Go `checkAddColumn` found: the table, the column already appended
/// by an earlier step (if any), the column from the arguments and where it
/// goes.
struct AddColumnTarget {
    info: TableInfo,
    column: Option<GoShared<tidb_model::ColumnInfo>>,
    args: GoShared<tidb_model::TableColumnArgs>,
    position: tidb_ast::ColumnPosition,
}

/// Go `ErrColumnExists`.
fn column_exists(name: &str) -> DdlPlanError {
    DdlPlanError::from(
        tidb_dbterror::CLASS_SCHEMA
            .new_std(tidb_errno::errcode::ErrDupFieldName)
            .generate_with_stack_by_args(&[FormatArg::from(name)]),
    )
}

/// Go `checkAddColumn`. `Ok(Err((if_not_exists, error)))` when a public
/// column of that name already exists (the job is cancelled).
fn check_add_column(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
) -> Result<Result<AddColumnTarget, (bool, DdlPlanError)>, DdlPlanError> {
    let info = checked_job_table(catalog, active, true)?.clone_like_go();
    let args = tidb_model::get_table_column_args(&mut active.job)
        .map_err(|error| cancel_ddl_job(active, DdlPlanError::Encode(error.to_string())))?
        .ok_or_else(|| {
            cancel_ddl_job(
                active,
                DdlPlanError::Encode("ADD COLUMN job has nil args".to_owned()),
            )
        })?;
    let (name, if_not_exists, position) = {
        let args = args.read();
        (
            args.col_name(),
            args.ignore_existence_err,
            args.pos
                .as_ref()
                .map(|position| position.read().to_statement())
                .unwrap_or(tidb_ast::ColumnPosition::Default),
        )
    };
    let column = tidb_model::column::find_column_info(&info.columns, name.lowercase());
    if column
        .as_ref()
        .is_some_and(|column| column.read().state == SchemaState::PUBLIC)
    {
        active.job.state = JobState::CANCELLED;
        return Ok(Err((if_not_exists, column_exists(name.original()))));
    }
    // Go `CheckAfterPositionExists`, answered as `ErrColumnExists`.
    if let tidb_ast::ColumnPosition::After(anchor) = &position {
        if tidb_model::column::find_column_info(&info.columns, anchor).is_none() {
            active.job.state = JobState::CANCELLED;
            return Err(column_exists(name.original()));
        }
    }
    Ok(Ok(AddColumnTarget {
        info,
        column,
        args,
        position,
    }))
}

/// Go `checkUnsupportedCharsetForTiFlash`.
fn check_unsupported_charset_for_tiflash(
    info: &TableInfo,
    column: &tidb_model::ColumnInfo,
) -> Result<(), DdlPlanError> {
    if info.tiflash_replica.is_none() {
        return Ok(());
    }
    let charset = column.field_type.charset_name();
    if matches!(charset, "utf8" | "utf8mb4" | "ascii" | "latin1" | "binary") {
        return Ok(());
    }
    Err(coded_refusal(
        &tidb_dbterror::ERR_UNSUPPORTED_ADD_COLUMN,
        format!(
            "unsupported add column '{}' when altering '{}' with TiFlash replicas and {} encoding",
            column.name.original(),
            info.name.original(),
            charset
        ),
    ))
}

/// Go `onAddColumn`.
pub(super) fn plan_persisted_add_column_job_step(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    if active.job.is_rollingback() {
        return drop_column::plan_persisted_drop_column_job_step(catalog, active, start_ts);
    }
    let AddColumnTarget {
        mut info,
        column,
        args,
        position,
    } = match check_add_column(catalog, active)? {
        Ok(target) => target,
        Err((true, error)) => {
            // Go: IF NOT EXISTS turns the error into the job's warning.
            active.job.warning = Some(GoShared::new(
                tidb_dbterror::CLASS_SCHEMA
                    .new_std(tidb_errno::errcode::ErrDupFieldName)
                    .generate(error.to_string()),
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
    let column = match column {
        Some(column) => column,
        None => {
            // Go `InitAndAddColumnToTable`, mutating the arguments' column
            // the job re-encodes.
            let added =
                args.read().col.clone().ok_or_else(|| {
                    DdlPlanError::Encode("ADD COLUMN job has no column".to_owned())
                })?;
            {
                let mut added = added.write();
                info.max_column_id += 1;
                added.id = info.max_column_id;
                added.state = SchemaState::NONE;
                added.offset = info.columns.len() as i64;
            }
            // Go appends the arguments' own pointer, so the re-encoded
            // arguments carry the column's id and state too.
            info.columns.push_handle_go(Some(added.clone()));
            check_add_column_too_many_columns(info.columns.len())
                .map_err(|error| cancel_ddl_job(active, error))?;
            check_unsupported_charset_for_tiflash(&info, &added.read())
                .map_err(|error| cancel_ddl_job(active, error))?;
            added
        }
    };
    let database_id = active.job.schema_id;
    let original_state = column.read().state;
    let next = match original_state {
        SchemaState::NONE => SchemaState::DELETE_ONLY,
        SchemaState::DELETE_ONLY => SchemaState::WRITE_ONLY,
        SchemaState::WRITE_ONLY => SchemaState::WRITE_REORGANIZATION,
        SchemaState::WRITE_REORGANIZATION => SchemaState::PUBLIC,
        state => {
            return Err(DdlPlanError::from(
                tidb_dbterror::ERR_INVALID_DDL_STATE.generate_with_stack_by_args(&[
                    FormatArg::from("column"),
                    FormatArg::new(state.to_string(), state.0.to_string(), "model.SchemaState"),
                ]),
            ))
        }
    };
    if next == SchemaState::PUBLIC {
        // Go `LocateOffsetToMove` then `MoveColumnInfo`.
        let offset = usize::try_from(column.read().offset).unwrap_or(0);
        let destination = locate_offset_to_move(offset, &position, &info)?;
        move_column_info(&mut info, offset, destination);
    }
    column.write().state = next;
    // Go `updateVersionAndTableInfoWithCheck` on the first and last steps.
    if matches!(next, SchemaState::DELETE_ONLY | SchemaState::PUBLIC) {
        check_table_info_valid(&info)?;
    }
    let mut write = table_step(catalog, active, database_id, &info, true, start_ts)?;
    if next != SchemaState::PUBLIC {
        active.job.schema_state = next;
        if next == SchemaState::WRITE_REORGANIZATION {
            active.job.mark_non_revertible();
        }
        return Ok(action_step(write));
    }
    if !tidb_metadef::is_mem_or_sys_db(&active.job.schema_name.to_string().go_to_lower()) {
        append_schema_change_mutations(
            catalog,
            active.job.id,
            &[(
                notifier_sub_job_id(&active.job),
                SchemaChangeEvent::add_columns(
                    info.clone_like_go(),
                    vec![column.read().clone_like_go()],
                ),
            )],
            &mut write.mutations,
            row_ids,
        )?;
    }
    active.job.finish_table_job(
        JobState::DONE,
        SchemaState::PUBLIC,
        write.schema_version,
        Some(GoShared::new(info.clone_like_go())),
    );
    Ok(action_step(write))
}

/// Go `rollingbackAddColumn`: before the column exists the job is
/// cancelled; afterwards it turns delete-only and rolls back as a drop.
pub(super) fn plan_cancel_add_column(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let AddColumnTarget { info, column, .. } =
        check_add_column(catalog, active)?.map_err(|(_, error)| error)?;
    let Some(column) = column else {
        return Err(cancelled_ddl_job(active));
    };
    let original_state = column.read().state;
    column.write().state = SchemaState::DELETE_ONLY;
    active.job.schema_state = SchemaState::DELETE_ONLY;
    let name = column.read().name.clone();
    tidb_model::fill_roll_back_args_for_add_column(
        &mut active.job,
        tidb_model::TableColumnArgs {
            col: Some(GoShared::new(tidb_model::ColumnInfo {
                name,
                ..tidb_model::ColumnInfo::default()
            })),
            ..tidb_model::TableColumnArgs::default()
        },
    );
    let database_id = active.job.schema_id;
    let write = table_step(
        catalog,
        active,
        database_id,
        &info,
        original_state != SchemaState::DELETE_ONLY,
        start_ts,
    )?;
    active.job.state = JobState::ROLLINGBACK;
    Ok(PersistedDdlActionStep {
        write,
        update_raw_args: true,
        run_error: Some(DdlPlanError::from(
            tidb_dbterror::ERR_CANCELLED_DDL_JOB
                .generate(tidb_errno::errname::ErrCancelledDDLJob.raw),
        )),
        reorg: None,
    })
}
