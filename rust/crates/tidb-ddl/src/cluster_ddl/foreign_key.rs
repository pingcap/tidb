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

//! ADD FOREIGN KEY as Go runs it: the executor's `CreateForeignKey` checks
//! the key against the original table and queues an `ActionAddForeignKey`
//! job, after a `createIndex` sub-job when no index covers the key's columns
//! (which makes even a single statement a multi-schema change). The owner's
//! `onCreateForeignKey` adds the key write-only, checks the existing rows,
//! and publishes it; rows that break it roll the job back through
//! `dropForeignKey`.

use super::add_index::{
    action_step, table_step, AddIndexReorg, AddIndexSubmission, ReorgOutcome, ReorgResult,
    ReorgStageRequest,
};
use super::*;

/// Go `executor.CreateForeignKey` up to `doDDLJob2`, for an ALTER TABLE
/// with this one specification.
#[allow(clippy::too_many_arguments)]
pub fn prepare_add_foreign_key_job_submission<S: MetaSnapshot>(
    snapshot: &mut S,
    statement: &DdlStatement,
    job_context: &dyn DdlSessionContext,
    start_ts: u64,
    upgrading: bool,
    min_job_id: i64,
    store_count: usize,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    let DdlStatement::AddForeignKey {
        schema,
        table,
        definition,
        context,
    } = statement
    else {
        return Ok(None);
    };
    let catalog = load_cluster_catalog(snapshot)?;
    let (_, stored) = locate_table(&catalog, schema, table)?;
    // Go `AlterTable` names an anonymous key before any spec runs.
    let name = foreign_key_name(definition, stored, 0);
    let mut specs = build_add_foreign_key_jobs(
        snapshot,
        &catalog,
        schema,
        table,
        &name,
        definition,
        context,
        job_context,
        store_count,
    )?;
    let built = if specs.len() == 1 {
        let key = specs.pop().expect("one job");
        AddIndexSubmission::Job(Box::new(key), Vec::new())
    } else {
        // The key's `createIndex` set `StmtCtx.MultiSchemaInfo`, so
        // `AlterTable` ends with `multiSchemaChange`.
        let job = super::multi_schema::build_multi_schema_job(
            &catalog,
            schema,
            table,
            specs,
            job_context,
            store_count,
        )?;
        AddIndexSubmission::Job(Box::new(job), Vec::new())
    };
    submit_built_job(
        snapshot,
        &catalog,
        Some(built),
        start_ts,
        upgrading,
        min_job_id,
    )
}

/// Go `AlterTable`: an anonymous key is named `fk_{MaxForeignKeyID+n}`, `n`
/// counting the anonymous keys of the statement from one.
pub(super) fn foreign_key_name(
    definition: &tidb_ast::ForeignKeyConstraintDefinition,
    stored: &TableInfo,
    anonymous_before: i64,
) -> String {
    match definition.name.as_deref() {
        Some(name) if !name.is_empty() => name.to_owned(),
        _ => format!("fk_{}", stored.max_foreign_key_id + anonymous_before + 1),
    }
}

/// Go `executor.CreateForeignKey`'s jobs: the `createIndex` job when no
/// index covers the key's columns, then the key's own.
#[allow(clippy::too_many_arguments)]
pub(super) fn build_add_foreign_key_jobs<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    schema: &str,
    table: &str,
    name: &str,
    definition: &tidb_ast::ForeignKeyConstraintDefinition,
    context: &DdlStatementContext,
    job_context: &dyn DdlSessionContext,
    store_count: usize,
) -> Result<Vec<tidb_ddl_jobsubmit::JobSpec>, DdlPlanError> {
    let (schema_id, stored) = locate_table(catalog, schema, table)?;
    let created = create_foreign_key(catalog, schema, stored, name, definition, context)?;
    let mut specs = Vec::new();
    if created.needs_index {
        // Go `createIndex(ctx, ti, IndexKeyTypeNone, fkInfo.Name, ...)`, one
        // key part per key column without a prefix.
        let statement = lower_create_index(
            &tidb_ast::CreateIndexStmt {
                kind: tidb_ast::IndexKind::Ordinary,
                if_not_exists: false,
                name: created.fk.name.original().to_owned(),
                table: vec![schema.to_owned(), table.to_owned()],
                parts: created
                    .fk
                    .cols
                    .snapshot()
                    .iter()
                    .map(|column| tidb_ast::IndexPart::Column {
                        name: column.original().to_owned(),
                        prefix_len: None,
                        desc: false,
                    })
                    .collect(),
                options: Default::default(),
                online: Default::default(),
            },
            schema,
            false,
            &context.0,
        )
        .map_err(DdlPlanError::Admission)?;
        match super::add_index::build_add_index_job(
            snapshot,
            catalog,
            &statement,
            store_count,
            &context.0,
        )? {
            Some(AddIndexSubmission::Job(spec, _)) => specs.push(*spec),
            _ => {
                return Err(DdlPlanError::from(
                    tidb_dbterror::ERR_DUP_KEY_NAME.generate_with_stack_by_args(&[(created
                        .fk
                        .name
                        .original()
                        .to_owned())
                    .into()]),
                ))
            }
        }
    }
    let fk = created.fk;
    let mut job = session_job(job_context, ActionType::ACTION_ADD_FOREIGN_KEY);
    job.schema_id = schema_id;
    job.table_id = stored.id;
    job.schema_name = schema.go_to_lower().into();
    job.table_name = stored.name.lowercase().to_owned().into();
    job.involving_schema_info = GoSharedSlice::from_vec(vec![
        tidb_model::InvolvingSchemaInfo {
            database: schema.go_to_lower().into(),
            table: stored.name.lowercase().to_owned().into(),
            ..tidb_model::InvolvingSchemaInfo::default()
        },
        tidb_model::InvolvingSchemaInfo {
            database: fk.ref_schema.lowercase().to_owned().into(),
            table: fk.ref_table.lowercase().to_owned().into(),
            mode: tidb_model::InvolvingSchemaInfoMode::SHARED,
            ..tidb_model::InvolvingSchemaInfo::default()
        },
    ]);
    let args = tidb_model::AddForeignKeyArgs {
        fk_info: Some(GoShared::new(fk)),
        fk_check: context.0.foreign_key_checks(),
    };
    specs.push(tidb_ddl_jobsubmit::JobSpec {
        job,
        args: JobArgsValue::AddForeignKey(Some(GoShared::new(args))),
        id_allocated: true,
    });
    Ok(specs)
}

/// Go `checkAddForeignKeyValidInOwner`.
fn check_add_foreign_key_valid_in_owner(
    catalog: &ClusterCatalog,
    schema: &str,
    info: &TableInfo,
    fk: &tidb_model::table::FKInfo,
    fk_check: bool,
) -> Result<(), DdlPlanError> {
    // Go `checkFKDupName`.
    if info
        .foreign_keys
        .iter_deref()
        .any(|existing| existing.read().name.lowercase() == fk.name.lowercase())
    {
        return Err(coded_refusal(
            &tidb_dbterror::ERR_FK_DUP_NAME,
            format!(
                "Duplicate foreign key constraint name '{}'",
                fk.name.original()
            ),
        ));
    }
    if !tidb_vardef::ENABLE_FOREIGN_KEY.load(std::sync::atomic::Ordering::SeqCst) {
        return Ok(());
    }
    crate::foreign_key_build::check_foreign_key_valid(catalog, schema, info, fk, fk_check, true)
        .map_err(DdlPlanError::Admission)?;
    // The key's columns need an index.
    let cols = fk.cols.snapshot();
    if cols.len() == 1 && info.pk_is_handle {
        if let Some(pk) = info.get_pk_col_info() {
            if pk.read().name.lowercase() == cols[0].lowercase() {
                return Ok(());
            }
        }
    }
    if tidb_model::index::find_index_by_columns_for_foreign_key(info, &info.indices, &cols)
        .is_none()
    {
        return Err(DdlPlanError::plain_refusal(format!(
                "Failed to add the foreign key constraint. Missing index for '{}' foreign key columns in the table '{}'",
                fk.name.original(),
                info.name.original()
            ),
        ));
    }
    Ok(())
}

/// The outcome of this job's existing-row check, if the owner ran one.
fn checked_outcome(
    active: &tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    outcome: Option<&ReorgOutcome>,
) -> Option<Option<(u16, String)>> {
    match outcome {
        Some(ReorgOutcome {
            job_id,
            result: ReorgResult::Checked(found),
            ..
        }) if *job_id == active.job.id => Some(found.clone()),
        _ => None,
    }
}

/// Go `onCreateForeignKey`.
pub(super) fn plan_persisted_add_foreign_key_job_step(
    catalog: &ClusterCatalog,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
    outcome: Option<&ReorgOutcome>,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let database_id = active.job.schema_id;
    let mut info = checked_job_table(catalog, active, true)?.clone_like_go();
    let args = tidb_model::get_add_foreign_key_args(&mut active.job)
        .map_err(|error| cancel_ddl_job(active, DdlPlanError::Encode(error.to_string())))?
        .ok_or_else(|| {
            cancel_ddl_job(
                active,
                DdlPlanError::Encode("ADD FOREIGN KEY job has nil args".to_owned()),
            )
        })?;
    let (fk, fk_check) = {
        let args = args.read();
        let fk = args
            .fk_info
            .clone()
            .ok_or_else(|| DdlPlanError::Encode("ADD FOREIGN KEY job has no key".to_owned()))?;
        (fk, args.fk_check)
    };
    if active.job.is_rollingback() {
        let name = fk.read().name.original().to_owned();
        return drop_foreign_key_step(catalog, active, info, &name, start_ts);
    }
    let schema = active.job.schema_name.to_string();
    let last = |info: &TableInfo| info.foreign_keys.len().checked_sub(1);
    match active.job.schema_state {
        SchemaState::NONE => {
            check_add_foreign_key_valid_in_owner(catalog, &schema, &info, &fk.read(), fk_check)
                .map_err(|error| cancel_ddl_job(active, error))?;
            // Go mutates the arguments' key and appends it, so the
            // re-encoded arguments carry its state and id.
            {
                let mut fk = fk.write();
                fk.state = SchemaState::WRITE_ONLY;
                // Go `allocateFKIndexID`.
                info.max_foreign_key_id += 1;
                fk.id = info.max_foreign_key_id;
            }
            info.foreign_keys.push_go(fk.read().clone_like_go());
            let write = table_step(catalog, active, database_id, &info, true, start_ts)?;
            active.job.schema_state = SchemaState::WRITE_ONLY;
            Ok(action_step(write))
        }
        SchemaState::WRITE_ONLY => {
            if fk_check {
                match checked_outcome(active, outcome) {
                    None => {
                        let check = foreign_key_check(catalog, active, &schema, &info, &fk)?;
                        return Ok(check_request(active, &info, check));
                    }
                    Some(Some((code, message))) => {
                        active.job.state = JobState::ROLLINGBACK;
                        return Err(checked_refusal(code, message));
                    }
                    Some(None) => {}
                }
            }
            if let Some(last) = last(&info) {
                info.foreign_keys.get(last).expect("in range").write().state =
                    SchemaState::WRITE_REORGANIZATION;
            }
            let write = table_step(catalog, active, database_id, &info, true, start_ts)?;
            active.job.schema_state = SchemaState::WRITE_REORGANIZATION;
            Ok(action_step(write))
        }
        SchemaState::WRITE_REORGANIZATION => {
            if let Some(last) = last(&info) {
                info.foreign_keys.get(last).expect("in range").write().state = SchemaState::PUBLIC;
            }
            let write = table_step(catalog, active, database_id, &info, true, start_ts)?;
            active.job.schema_state = SchemaState::PUBLIC;
            active.job.finish_table_job(
                JobState::DONE,
                SchemaState::PUBLIC,
                write.schema_version,
                Some(GoShared::new(info.clone_like_go())),
            );
            Ok(action_step(write))
        }
        state => Err(DdlPlanError::from(
            tidb_dbterror::ERR_INVALID_DDL_STATE.generate_with_stack_by_args(&[
                FormatArg::from("foreign key"),
                FormatArg::new(state.to_string(), state.0.to_string(), "model.SchemaState"),
            ]),
        )),
    }
}

/// Go `checkForeignKeyConstrain`'s question: the child's rows (the key as
/// the table now holds it) against the parent.
fn foreign_key_check(
    catalog: &ClusterCatalog,
    active: &tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    schema: &str,
    info: &TableInfo,
    fk: &GoShared<tidb_model::table::FKInfo>,
) -> Result<ForeignKeyCheck, DdlPlanError> {
    let fk = fk.read();
    let self_reference = fk.ref_schema.lowercase() == schema.go_to_lower()
        && fk.ref_table.lowercase() == info.name.lowercase();
    let parent = if self_reference {
        None
    } else {
        catalog
            .find_table(fk.ref_schema.original(), fk.ref_table.original())
            .map(|(database, parent)| {
                (
                    fk.ref_schema.original().to_owned(),
                    database.id,
                    Box::new(parent.clone_like_go()),
                )
            })
    };
    // Go checks the rows by a query, whatever state the key is in; the
    // check reads the child with the key public.
    let child = info.clone_like_go();
    for key in child.foreign_keys.iter_deref() {
        let mut key = key.write();
        if key.name.lowercase() == fk.name.lowercase() {
            key.state = SchemaState::PUBLIC;
        }
    }
    Ok(ForeignKeyCheck {
        schema: schema.to_owned(),
        schema_id: active.job.schema_id,
        child: Box::new(child),
        parent,
        name: fk.name.original().to_owned(),
        use_new_collation: tidb_datatype::new_collation_enabled(),
        session_vars: tidb_ddl_build::session_pool::ReorgSessionVars::of_job(&active.job),
    })
}

fn check_request(
    active: &tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    info: &TableInfo,
    check: ForeignKeyCheck,
) -> PersistedDdlActionStep {
    PersistedDdlActionStep {
        write: job_only_write(active.job.id, Vec::new()),
        update_raw_args: false,
        run_error: None,
        reorg: Some(AddIndexReorg {
            job_id: active.job.id,
            stage: ReorgStageRequest::CheckForeignKey(Box::new(check)),
            table: Box::new(info.clone_like_go()),
            index_ids: Vec::new(),
            merging: false,
            physical_ids: Vec::new(),
            batch_size: 1,
            session_vars: tidb_ddl_build::session_pool::ReorgSessionVars::of_job(&active.job),
            row_count: active.job.row_count,
            column: None,
            sub_job: add_index::reorg_sub_job(&active.job),
            clean_up_global_index: false,
            partition_phase: None,
            schema_id: active.job.schema_id,
        }),
    }
}

/// Go `dropForeignKey` for an ADD FOREIGN KEY rolling back.
fn drop_foreign_key_step(
    catalog: &ClusterCatalog,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    mut info: TableInfo,
    name: &str,
    start_ts: u64,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    if let Err(error) = drop_foreign_key(&mut info, name) {
        return Err(cancel_ddl_job(active, error));
    }
    let database_id = active.job.schema_id;
    let write = table_step(catalog, active, database_id, &info, true, start_ts)?;
    let state = if active.job.is_rollingback() {
        JobState::ROLLBACK_DONE
    } else {
        JobState::DONE
    };
    active.job.finish_table_job(
        state,
        SchemaState::NONE,
        write.schema_version,
        Some(GoShared::new(info.clone_like_go())),
    );
    active.job.schema_state = SchemaState::NONE;
    Ok(action_step(write))
}

/// Go `rollingbackAddForeignKey`: `cancelOnlyNotHandledJob` at `StateNone`.
pub(super) fn plan_cancel_add_foreign_key(
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    if active.job.schema_state == SchemaState::NONE {
        return Err(cancelled_ddl_job(active));
    }
    active.job.state = JobState::RUNNING;
    Ok(job_control_step(&active.job))
}
