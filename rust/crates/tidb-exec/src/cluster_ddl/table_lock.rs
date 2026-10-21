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

//! LOCK TABLES, UNLOCK TABLES and ADMIN CLEANUP TABLE LOCK as Go runs them:
//! the session's executor (`executor.LockTables`, `UnlockTables`,
//! `CleanupTableLock`) checks the statement and queues one job carrying
//! `LockTablesArgs`; the owner's `onLockTables` / `onUnlockTables` first
//! release the session's earlier locks one table per step, then take each
//! new lock through `none -> pre-lock -> public`, a schema version each step.

use super::add_index::{action_step, table_step, AddIndexSubmission};
use super::*;

/// Go's executor halves up to `doDDLJob2`: the session already checked the
/// tables and holds the lock bookkeeping. `None` for any other statement.
pub fn prepare_table_lock_job_submission<S: MetaSnapshot>(
    snapshot: &mut S,
    statement: &DdlStatement,
    job_context: &tidb_executor::StmtContext,
    start_ts: u64,
    upgrading: bool,
    min_job_id: i64,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    let catalog = load_cluster_catalog(snapshot)?;
    // Go names each table the job touches; an unlock skips a table dropped
    // since it was locked.
    let involving = |tables: &[tidb_model::TableLockTpInfo]| {
        tables
            .iter()
            .filter_map(|table| {
                let database = catalog
                    .databases
                    .iter()
                    .find(|database| database.info.id == table.schema_id)?;
                let stored = database
                    .tables
                    .iter()
                    .find(|stored| stored.id == table.table_id)?;
                Some(tidb_model::InvolvingSchemaInfo {
                    database: database.info.name.lowercase().to_owned().into(),
                    table: stored.name.lowercase().to_owned().into(),
                    ..tidb_model::InvolvingSchemaInfo::default()
                })
            })
            .collect::<Vec<_>>()
    };
    let (job, args) = match statement {
        // Go `executor.LockTables`.
        DdlStatement::LockTables {
            lock_tables,
            unlock_tables,
            session,
        } => {
            let first = lock_tables.first().ok_or_else(|| {
                DdlPlanError::Admission(DdlAdmissionError::plain(
                    "LOCK TABLES names no table".to_owned(),
                ))
            })?;
            let mut job = session_job(job_context, ActionType::ACTION_LOCK_TABLE);
            job.schema_id = first.schema_id;
            job.table_id = first.table_id;
            job.involving_schema_info = GoSharedSlice::from_vec(involving(lock_tables));
            let args = tidb_model::LockTablesArgs {
                lock_tables: lock_tables.clone(),
                unlock_tables: unlock_tables.clone(),
                session_info: session.clone(),
                ..tidb_model::LockTablesArgs::default()
            };
            (job, args)
        }
        // Go `executor.UnlockTables`, `ddl.cleanDeadTableLock` and
        // `executor.CleanupTableLock`.
        DdlStatement::UnlockTables {
            unlock_tables,
            session,
            is_cleanup,
        } => {
            let first = unlock_tables.first().ok_or_else(|| {
                DdlPlanError::Admission(DdlAdmissionError::plain(
                    "UNLOCK TABLES names no table".to_owned(),
                ))
            })?;
            let mut job = session_job(job_context, ActionType::ACTION_UNLOCK_TABLE);
            // Go `setDDLJobQuery`: an unlock job records no query.
            job.query = String::new().into();
            job.schema_id = first.schema_id;
            job.table_id = first.table_id;
            job.involving_schema_info = GoSharedSlice::from_vec(involving(unlock_tables));
            let args = tidb_model::LockTablesArgs {
                unlock_tables: unlock_tables.clone(),
                // A cleanup drops every holder's lock and names no session.
                session_info: if *is_cleanup {
                    tidb_model::SessionInfo::default()
                } else {
                    session.clone()
                },
                is_cleanup: *is_cleanup,
                ..tidb_model::LockTablesArgs::default()
            };
            (job, args)
        }
        _ => return Ok(None),
    };
    let built = AddIndexSubmission::Job(
        Box::new(crate::ddl_job_submit::JobSpec {
            job,
            args: JobArgsValue::LockTables(Some(GoShared::new(args))),
            id_allocated: true,
        }),
        Vec::new(),
    );
    submit_built_job(
        snapshot,
        &catalog,
        Some(built),
        start_ts,
        upgrading,
        min_job_id,
    )
}

fn lock_tables_args(
    active: &mut crate::ddl_job_table::ActiveDdlJob,
) -> Result<tidb_model::LockTablesArgs, DdlPlanError> {
    let args = tidb_model::get_lock_tables_args(&mut active.job)
        .map_err(|error| cancel_ddl_job(active, DdlPlanError::Encode(error.to_string())))?
        .ok_or_else(|| {
            cancel_ddl_job(
                active,
                DdlPlanError::Encode("lock tables job has nil args".to_owned()),
            )
        })?;
    let args = args.read().clone();
    Ok(args)
}

/// A step that changes no table.
fn no_table_write(active: &crate::ddl_job_table::ActiveDdlJob) -> DdlWrite {
    job_only_write(active.job.id, Vec::new())
}

/// Go `ddl.unlockTables`: the next table of `UnlockTables` lets go of the
/// session's lock (every holder's, for a cleanup), a version only when the
/// lock changed; a table dropped since it was locked is skipped.
fn unlock_tables(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    args: &mut tidb_model::LockTablesArgs,
    start_ts: u64,
) -> Result<DdlWrite, DdlPlanError> {
    let Some(target) = usize::try_from(args.index_of_unlock)
        .ok()
        .and_then(|index| args.unlock_tables.get(index))
        .copied()
    else {
        return Ok(no_table_write(active));
    };
    active.job.schema_id = target.schema_id;
    active.job.table_id = target.table_id;
    let write = match stored_table_by_ids(catalog, target.schema_id, target.table_id) {
        None => no_table_write(active),
        Some(stored) => {
            let current = stored.lock.as_ref().map(|lock| lock.read().clone());
            match tidb_executor::ddl::table_lock::unlock_table(
                current.as_ref(),
                &args.session_info,
                args.is_cleanup,
            ) {
                None => no_table_write(active),
                Some(next) => {
                    let mut info = stored.clone_like_go();
                    info.lock = next.map(GoShared::new);
                    table_step(catalog, active, target.schema_id, &info, true, start_ts)?
                }
            }
        }
    };
    args.index_of_unlock += 1;
    active.job.fill_args(Some(GoShared::new(args.clone())));
    Ok(write)
}

/// The job's current table (Go `GetTableInfoAndCancelFaultJob` after the
/// step points the job at it).
fn job_table<'a>(
    catalog: &'a ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    target: &tidb_model::TableLockTpInfo,
) -> Result<&'a TableInfo, DdlPlanError> {
    active.job.schema_id = target.schema_id;
    active.job.table_id = target.table_id;
    checked_job_table(catalog, active, true)
}

/// Go `onLockTables`.
pub(super) fn plan_persisted_lock_tables_job_step(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let mut args = lock_tables_args(active)?;
    // The session's earlier locks go first.
    if (args.index_of_unlock as usize) < args.unlock_tables.len() {
        return unlock_tables(catalog, active, &mut args, start_ts).map(action_step);
    }
    // Another session's lock is checked once, before the first lock.
    if args.index_of_lock == 0 {
        for target in &args.lock_tables {
            let info = job_table(catalog, active, target)?;
            let current = info.lock.as_ref().map(|lock| lock.read().clone());
            tidb_executor::ddl::table_lock::check_table_locked(
                info.name.lowercase(),
                current.as_ref(),
                target.tp,
                &args.session_info,
            )
            .map_err(|error| cancel_ddl_job(active, lock_plan_error(error)))?;
        }
    }
    let Some(target) = usize::try_from(args.index_of_lock)
        .ok()
        .and_then(|index| args.lock_tables.get(index))
        .copied()
    else {
        return Ok(action_step(no_table_write(active)));
    };
    let mut info = job_table(catalog, active, &target)?.clone_like_go();
    let current = info.lock.as_ref().map(|lock| lock.read().clone());
    let mut lock = tidb_executor::ddl::table_lock::lock_table(
        info.name.lowercase(),
        current.as_ref(),
        target.tp,
        &args.session_info,
    )
    .map_err(|error| cancel_ddl_job(active, lock_plan_error(error)))?;
    let finished = match lock.state {
        // none -> pre-lock.
        tidb_model::TableLockState::NONE => {
            lock.state = tidb_model::TableLockState::PRE_LOCK;
            false
        }
        // A public lock is a READ lock another session already holds: this
        // request needs no pre-lock.
        tidb_model::TableLockState::PRE_LOCK | tidb_model::TableLockState::PUBLIC => {
            lock.state = tidb_model::TableLockState::PUBLIC;
            true
        }
        state => {
            return Err(cancel_ddl_job(
                active,
                DdlPlanError::from(
                    tidb_util::dbterror::ERR_INVALID_DDL_STATE.generate_with_stack_by_args(&[
                        FormatArg::from("table lock"),
                        FormatArg::new(
                            state.to_string(),
                            state.0.to_string(),
                            "model.TableLockState",
                        ),
                    ]),
                ),
            ));
        }
    };
    lock.ts = start_ts;
    info.lock = Some(GoShared::new(lock));
    let write = table_step(catalog, active, target.schema_id, &info, true, start_ts)?;
    if finished {
        args.index_of_lock += 1;
        active.job.fill_args(Some(GoShared::new(args.clone())));
        if args.index_of_lock as usize == args.lock_tables.len() {
            active.job.finish_table_job(
                JobState::DONE,
                SchemaState::PUBLIC,
                write.schema_version,
                None,
            );
        }
    }
    Ok(action_step(write))
}

/// Go `onUnlockTables`.
pub(super) fn plan_persisted_unlock_tables_job_step(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let mut args = lock_tables_args(active)?;
    let write = unlock_tables(catalog, active, &mut args, start_ts)?;
    if args.index_of_unlock as usize == args.unlock_tables.len() {
        active.job.finish_table_job(
            JobState::DONE,
            SchemaState::NONE,
            write.schema_version,
            None,
        );
    }
    Ok(action_step(write))
}
