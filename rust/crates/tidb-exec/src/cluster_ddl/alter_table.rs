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

//! The ALTER TABLE jobs Go finishes in one step: the executor checks the
//! statement and queues a job with its arguments; the owner re-reads the
//! table, applies the arguments and publishes one schema version --
//! `onModifyTableComment`, `onAlterIndexVisibility`, `onRenameIndex`,
//! `onDropForeignKey`, `onModifyTableCharsetAndCollate`, `onSetDefaultValue`,
//! `onShardRowID` and `onModifyTableAutoIDCache`.

use super::add_index::{action_step, table_step, AddIndexSubmission};
use super::*;

/// Whether this node submits `statement` as one of these jobs.
pub fn is_alter_table_statement(statement: &DdlStatement) -> bool {
    matches!(
        statement,
        DdlStatement::ModifyTableComment { .. }
            | DdlStatement::AlterIndexVisibility { .. }
            | DdlStatement::RenameIndex { .. }
            | DdlStatement::DropForeignKey { .. }
            | DdlStatement::ModifyTableCharsetAndCollate { .. }
            | DdlStatement::SetColumnDefault { .. }
            | DdlStatement::ShardRowId { .. }
            | DdlStatement::ModifyAutoIdCache { .. }
    )
}

/// The actions [`plan_persisted_alter_table_job_step`] runs.
pub(crate) const ALTER_TABLE_ACTIONS: [ActionType; 8] = [
    ActionType::ACTION_MODIFY_TABLE_COMMENT,
    ActionType::ACTION_ALTER_INDEX_VISIBILITY,
    ActionType::ACTION_RENAME_INDEX,
    ActionType::ACTION_DROP_FOREIGN_KEY,
    ActionType::ACTION_MODIFY_TABLE_CHARSET_AND_COLLATE,
    ActionType::ACTION_SET_DEFAULT_VALUE,
    ActionType::ACTION_SHARD_ROW_ID,
    ActionType::ACTION_MODIFY_TABLE_AUTO_IDCACHE,
];

fn already_satisfied(detail: String) -> Option<AddIndexSubmission> {
    Some(AddIndexSubmission::AlreadySatisfied {
        detail,
        warnings: Vec::new(),
    })
}

/// Go's executor halves up to `doDDLJob2`. `None` for any other statement.
pub fn prepare_alter_table_job_submission<S: MetaSnapshot>(
    snapshot: &mut S,
    statement: &DdlStatement,
    job_context: &tidb_executor::StmtContext,
    start_ts: u64,
    upgrading: bool,
    min_job_id: i64,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    if !is_alter_table_statement(statement) {
        return Ok(None);
    }
    let catalog = load_cluster_catalog(snapshot)?;
    let (action, schema, table, args) = match statement {
        // Go `AlterTableComment`; the length was validated at lowering.
        DdlStatement::ModifyTableComment {
            schema,
            table,
            comment,
        } => (
            ActionType::ACTION_MODIFY_TABLE_COMMENT,
            schema,
            table,
            JobArgsValue::ModifyTableComment(Some(GoShared::new(
                tidb_model::ModifyTableCommentArgs {
                    comment: GoField::new(comment.clone().into()),
                },
            ))),
        ),
        // Go `AlterIndexVisibility` -> `validateAlterIndexVisibility`.
        DdlStatement::AlterIndexVisibility {
            schema,
            table,
            index,
            invisible,
        } => {
            let (_, stored) = locate_table(&catalog, schema, table)?;
            let mut probe = stored.clone_like_go();
            if let AlterColumnOutcome::AlreadySatisfied(detail) =
                apply_index_visibility(&mut probe, schema, table, index, *invisible)?
            {
                return Ok(already_satisfied(detail));
            }
            (
                ActionType::ACTION_ALTER_INDEX_VISIBILITY,
                schema,
                table,
                JobArgsValue::AlterIndexVisibility(Some(GoShared::new(
                    tidb_model::AlterIndexVisibilityArgs {
                        index_name: GoField::new(CiString::new(index.clone())),
                        invisible: GoField::new(*invisible),
                    },
                ))),
            )
        }
        // Go `RenameIndex` -> `ValidateRenameIndex`.
        DdlStatement::RenameIndex {
            schema,
            table,
            from,
            to,
        } => {
            let (_, stored) = locate_table(&catalog, schema, table)?;
            check_rename_index_cache(stored)?;
            let mut probe = stored.clone_like_go();
            if let AlterColumnOutcome::AlreadySatisfied(detail) =
                apply_rename_index(&mut probe, schema, table, from, to)?
            {
                return Ok(already_satisfied(detail));
            }
            (
                ActionType::ACTION_RENAME_INDEX,
                schema,
                table,
                JobArgsValue::ModifyIndex(Some(GoShared::new(tidb_model::ModifyIndexArgs {
                    index_args: vec![
                        tidb_model::IndexArg {
                            index_name: CiString::new(from.clone()),
                            ..tidb_model::IndexArg::default()
                        },
                        tidb_model::IndexArg {
                            index_name: CiString::new(to.clone()),
                            ..tidb_model::IndexArg::default()
                        },
                    ]
                    .into(),
                    ..tidb_model::ModifyIndexArgs::default()
                }))),
            )
        }
        // Go `DropForeignKey`.
        DdlStatement::DropForeignKey {
            schema,
            table,
            name,
        } => {
            let (_, stored) = locate_table(&catalog, schema, table)?;
            if !stored
                .foreign_keys
                .iter_deref()
                .any(|fk| fk.read().name.lowercase() == name.go_to_lower())
            {
                return Err(foreign_key_not_exists(name));
            }
            (
                ActionType::ACTION_DROP_FOREIGN_KEY,
                schema,
                table,
                JobArgsValue::DropForeignKey(Some(GoShared::new(tidb_model::DropForeignKeyArgs {
                    foreign_key_name: GoField::new(CiString::new(name.clone())),
                }))),
            )
        }
        // Go `AlterTableCharsetAndCollate` -> `checkAlterTableCharset`.
        DdlStatement::ModifyTableCharsetAndCollate {
            schema,
            table,
            charset,
            collate,
            overwrite_columns,
        } => {
            check_table_charset_named(charset, collate)?;
            let (_, stored) = locate_table(&catalog, schema, table)?;
            let mut probe = stored.clone_like_go();
            if let AlterColumnOutcome::AlreadySatisfied(detail) = apply_table_charset_and_collate(
                &catalog,
                schema,
                table,
                &mut probe,
                charset,
                collate,
                *overwrite_columns,
            )? {
                return Ok(already_satisfied(detail));
            }
            (
                ActionType::ACTION_MODIFY_TABLE_CHARSET_AND_COLLATE,
                schema,
                table,
                JobArgsValue::ModifyTableCharsetAndCollate(Some(GoShared::new(
                    tidb_model::ModifyTableCharsetAndCollateArgs {
                        to_charset: GoField::new(probe.charset.clone().into()),
                        to_collate: GoField::new(probe.collate.clone().into()),
                        needs_overwrite_columns: GoField::new(*overwrite_columns),
                    },
                ))),
            )
        }
        // Go `AlterColumn`: the column with its new default.
        DdlStatement::SetColumnDefault {
            schema,
            table,
            column,
            default_value,
            context,
        } => {
            let (_, stored) = locate_table(&catalog, schema, table)?;
            let mut probe = stored.clone_like_go();
            apply_set_column_default(
                &mut probe,
                table,
                column,
                default_value.as_deref(),
                &context.0,
            )?;
            let changed = tidb_model::column::find_column_info(&probe.columns, column)
                .expect("the default was set on this column")
                .read()
                .clone_like_go();
            (
                ActionType::ACTION_SET_DEFAULT_VALUE,
                schema,
                table,
                JobArgsValue::SetDefaultValue(Some(GoShared::new(
                    tidb_model::SetDefaultValueArgs {
                        column: GoField::new(Some(GoShared::new(changed))),
                    },
                ))),
            )
        }
        // Go `ShardRowID`, in its order.
        DdlStatement::ShardRowId {
            schema,
            table,
            bits,
        } => {
            let (_, stored) = locate_table(&catalog, schema, table)?;
            if stored.temp_table_type != tidb_model::TempTableType::NONE {
                return Err(coded_refusal(
                    8006,
                    "`shard_row_id_bits` is unsupported on temporary tables.".to_owned(),
                ));
            }
            if *bits == stored.shard_row_id_bits {
                return Ok(already_satisfied(format!(
                    "`{schema}`.`{table}` already has SHARD_ROW_ID_BITS = {bits}"
                )));
            }
            if *bits > 0 && (stored.pk_is_handle || stored.is_common_handle) {
                return Err(DdlPlanError::Admission(DdlAdmissionError::with_code(
                    8200,
                    "Unsupported shard_row_id_bits for table with primary key as row id",
                )));
            }
            (
                ActionType::ACTION_SHARD_ROW_ID,
                schema,
                table,
                JobArgsValue::ShardRowId(Some(GoShared::new(tidb_model::ShardRowIDArgs {
                    shard_row_id_bits: GoField::new(*bits),
                }))),
            )
        }
        // Go `AlterTableAutoIDCache`.
        DdlStatement::ModifyAutoIdCache {
            schema,
            table,
            new_cache,
        } => {
            let (_, stored) = locate_table(&catalog, schema, table)?;
            if (*new_cache == 1) != (stored.auto_id_cache == 1) {
                return Err(DdlPlanError::Unsupported(
                    "Can't Alter AUTO_ID_CACHE between 1 and non-1, the underlying implementation is different"
                        .to_owned(),
                ));
            }
            (
                ActionType::ACTION_MODIFY_TABLE_AUTO_IDCACHE,
                schema,
                table,
                JobArgsValue::ModifyTableAutoIdCache(Some(GoShared::new(
                    tidb_model::ModifyTableAutoIDCacheArgs {
                        new_cache: GoField::new(*new_cache),
                    },
                ))),
            )
        }
        _ => unreachable!("checked by is_alter_table_statement"),
    };
    let (schema_id, stored) = locate_table(&catalog, schema, table)?;
    let mut job = session_job(job_context, action);
    job.schema_id = schema_id;
    job.table_id = stored.id;
    job.schema_name = schema.go_to_lower().into();
    job.table_name = stored.name.lowercase().to_owned().into();
    if action == ActionType::ACTION_DROP_FOREIGN_KEY {
        job.schema_state = SchemaState::PUBLIC;
    }
    let mut specs = [crate::ddl_job_submit::JobSpec {
        job,
        args,
        id_allocated: true,
    }];
    crate::ddl_job_submit::prepare_submit_batch(
        snapshot, &catalog, &mut specs, start_ts, upgrading, min_job_id,
    )?;
    let [spec] = specs;
    Ok(Some(AddIndexSubmission::Job(Box::new(spec), Vec::new())))
}

/// Go `ErrOptOnCacheTable` for `Rename Index`.
fn check_rename_index_cache(info: &TableInfo) -> Result<(), DdlPlanError> {
    if info.table_cache_status_type != tidb_model::TableCacheStatusType::DISABLE {
        return Err(coded_refusal(
            tidb_error::tidb::errcode::ErrOptOnCacheTable,
            "'Rename Index' is unsupported on cache tables.",
        ));
    }
    Ok(())
}

fn args_error(
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    error: serde_json::Error,
) -> DdlPlanError {
    cancel_ddl_job(active, DdlPlanError::Encode(error.to_string()))
}

fn nil_args(active: &mut crate::ddl_job_table::ActiveDdlJob) -> DdlPlanError {
    cancel_ddl_job(
        active,
        DdlPlanError::Encode(format!("{} job has nil args", active.job.type_)),
    )
}

/// The owner halves.
pub(super) fn plan_persisted_alter_table_job_step<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let mut info = checked_job_table(catalog, active, true)?.clone_like_go();
    let schema_id = active.job.schema_id;
    let schema = active.job.schema_name.to_string();
    let table = info.name.original().to_owned();
    let mut with_check = false;
    let mut finished_state = SchemaState::PUBLIC;
    match active.job.type_ {
        ActionType::ACTION_MODIFY_TABLE_COMMENT => {
            let args = tidb_model::get_modify_table_comment_args(&mut active.job)
                .map_err(|error| args_error(active, error))?
                .ok_or_else(|| nil_args(active))?;
            info.comment = args.read().comment.get().to_string();
        }
        ActionType::ACTION_ALTER_INDEX_VISIBILITY => {
            let args = tidb_model::get_alter_index_visibility_args(&mut active.job)
                .map_err(|error| args_error(active, error))?
                .ok_or_else(|| nil_args(active))?;
            let (index, invisible) = {
                let args = args.read();
                (args.index_name.get(), args.invisible.get())
            };
            match apply_index_visibility(&mut info, &schema, &table, index.original(), invisible) {
                Ok(AlterColumnOutcome::Applied) => with_check = true,
                // Go `checkAlterIndexVisibility`'s skip: done, nothing written.
                Ok(AlterColumnOutcome::AlreadySatisfied(_)) => {
                    active.job.state = JobState::DONE;
                    return Ok(job_control_step(&active.job));
                }
                Err(error) => return Err(cancel_ddl_job(active, error)),
            }
        }
        ActionType::ACTION_RENAME_INDEX => {
            let args = tidb_model::get_modify_index_args(&mut active.job)
                .map_err(|error| args_error(active, error))?
                .ok_or_else(|| nil_args(active))?;
            let (from, to) = {
                let args = args.read();
                let name = |position: usize| {
                    args.index_args
                        .get(position)
                        .map(|arg| arg.read().index_name.original().to_owned())
                        .unwrap_or_default()
                };
                (name(0), name(1))
            };
            check_rename_index_cache(&info)?;
            match apply_rename_index(&mut info, &schema, &table, &from, &to) {
                Ok(AlterColumnOutcome::Applied) => {}
                Ok(AlterColumnOutcome::AlreadySatisfied(_)) => {
                    active.job.state = JobState::DONE;
                    return Ok(job_control_step(&active.job));
                }
                Err(error) => return Err(cancel_ddl_job(active, error)),
            }
        }
        ActionType::ACTION_DROP_FOREIGN_KEY => {
            let args = tidb_model::get_drop_foreign_key_args(&mut active.job)
                .map_err(|error| args_error(active, error))?
                .ok_or_else(|| nil_args(active))?;
            let name = args.read().foreign_key_name.get();
            drop_foreign_key(&mut info, name.original())
                .map_err(|error| cancel_ddl_job(active, error))?;
            finished_state = SchemaState::NONE;
        }
        ActionType::ACTION_MODIFY_TABLE_CHARSET_AND_COLLATE => {
            let args = tidb_model::get_modify_table_charset_and_collate_args(&mut active.job)
                .map_err(|error| args_error(active, error))?
                .ok_or_else(|| nil_args(active))?;
            let (charset, collate, overwrite) = {
                let args = args.read();
                (
                    args.to_charset.get().to_string(),
                    args.to_collate.get().to_string(),
                    args.needs_overwrite_columns.get(),
                )
            };
            // Go's double check, which applies even when nothing changes.
            if let AlterColumnOutcome::AlreadySatisfied(_) = apply_table_charset_and_collate(
                catalog, &schema, &table, &mut info, &charset, &collate, overwrite,
            )
            .map_err(|error| cancel_ddl_job(active, error))?
            {
                info.charset = charset;
                info.collate = collate;
            }
        }
        ActionType::ACTION_SET_DEFAULT_VALUE => {
            let args = tidb_model::get_set_default_value_args(&mut active.job)
                .map_err(|error| args_error(active, error))?
                .ok_or_else(|| nil_args(active))?;
            let changed = args
                .read()
                .column
                .get()
                .ok_or_else(|| nil_args(active))?
                .read()
                .clone_like_go();
            // Go `updateColumnDefaultValue`: the default fields onto the
            // column as it stands now.
            let Some(old) =
                tidb_model::column::find_column_info(&info.columns, changed.name.lowercase())
                    .filter(|column| column.read().state == SchemaState::PUBLIC)
            else {
                return Err(cancel_ddl_job(
                    active,
                    DdlPlanError::UnknownColumn {
                        column: changed.name.original().to_owned(),
                        table: table.clone(),
                    },
                ));
            };
            let mut old = old.write();
            old.default_value = changed.default_value.clone();
            old.default_value_bit = changed.default_value_bit.clone();
            old.default_is_expr = changed.default_is_expr;
            let no_default = u64::from(FieldTypeFlags::NO_DEFAULT_VALUE);
            if changed.get_flag() & no_default != 0 {
                old.add_flag(no_default);
            } else {
                old.del_flag(no_default);
            }
        }
        ActionType::ACTION_SHARD_ROW_ID => {
            let args = tidb_model::get_shard_row_id_args(&mut active.job)
                .map_err(|error| args_error(active, error))?
                .ok_or_else(|| nil_args(active))?;
            let bits = args.read().shard_row_id_bits.get();
            if bits < info.shard_row_id_bits {
                info.shard_row_id_bits = bits;
            } else {
                verify_no_overflow_shard_bits(snapshot, schema_id, &info, bits)
                    .map_err(|error| cancel_ddl_job(active, error))?;
                info.shard_row_id_bits = bits;
                info.max_shard_row_id_bits = bits;
            }
        }
        ActionType::ACTION_MODIFY_TABLE_AUTO_IDCACHE => {
            let args = tidb_model::get_modify_table_auto_id_cache_args(&mut active.job)
                .map_err(|error| args_error(active, error))?
                .ok_or_else(|| nil_args(active))?;
            info.auto_id_cache = args.read().new_cache.get();
        }
        action => unreachable!("{action} is not a one-step ALTER TABLE job"),
    }
    if with_check {
        check_table_info_valid(&info)?;
    }
    let write = table_step(catalog, active, schema_id, &info, true, start_ts)?;
    active.job.finish_table_job(
        JobState::DONE,
        finished_state,
        write.schema_version,
        Some(GoShared::new(info.clone_like_go())),
    );
    Ok(action_step(write))
}

/// Go `convertJob2RollbackJob` for these actions.
pub(super) fn plan_cancel_alter_table(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let cancel = match active.job.type_ {
        // `cancelOnlyNotHandledJob(job, model.StateNone)`.
        ActionType::ACTION_ALTER_INDEX_VISIBILITY
        | ActionType::ACTION_MODIFY_TABLE_CHARSET_AND_COLLATE
        | ActionType::ACTION_SHARD_ROW_ID
        | ActionType::ACTION_MODIFY_TABLE_AUTO_IDCACHE => {
            active.job.schema_state == SchemaState::NONE
        }
        // `cancelOnlyNotHandledJob(job, model.StatePublic)`.
        ActionType::ACTION_DROP_FOREIGN_KEY => active.job.schema_state == SchemaState::PUBLIC,
        // `rollingbackRenameIndex`: cancellable while the source is public.
        ActionType::ACTION_RENAME_INDEX => {
            let info = checked_job_table(catalog, active, true)?.clone_like_go();
            let args = tidb_model::get_modify_index_args(&mut active.job)
                .map_err(|error| args_error(active, error))?
                .ok_or_else(|| nil_args(active))?;
            let from = args
                .read()
                .index_args
                .get(0)
                .map(|arg| arg.read().index_name.original().to_owned())
                .unwrap_or_default();
            find_index(&info, &from).is_some_and(|index| index.read().state == SchemaState::PUBLIC)
        }
        // The default arm: always cancelled.
        _ => true,
    };
    if cancel {
        return Err(cancelled_ddl_job(active));
    }
    active.job.state = JobState::RUNNING;
    Ok(job_control_step(&active.job))
}
