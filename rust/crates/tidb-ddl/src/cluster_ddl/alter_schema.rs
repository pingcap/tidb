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

//! ALTER DATABASE as Go runs it: the executor's
//! `ModifySchemaCharsetAndCollate` and `ModifySchemaDefaultPlacement` queue
//! a job over the whole database, and the owner's
//! `onModifySchemaCharsetAndCollate` / `onModifySchemaDefaultPlacement`
//! update the database in one step, finishing without a new version when
//! nothing changes.

use super::add_index::{action_step, AddIndexSubmission};
use super::*;

/// Go `executor.AlterSchema`'s resolved local state, before any job runs.
pub struct AlterSchemaOptions {
    /// Go `isAlterCharsetAndCollate`, `toCharset`, `toCollate`.
    pub charset_collate: Option<(String, String)>,
    /// Go `placementPolicyRef`.
    pub placement_policy: Option<String>,
    /// Go `tiflashReplica`.
    pub tiflash_replica: Option<(u64, Vec<String>)>,
}

/// Go `executor.AlterSchema`: validate every option before the first job.
pub fn resolve_schema_options(
    options: &[tidb_ast::DatabaseOption],
    context: &dyn DdlSessionContext,
) -> Result<AlterSchemaOptions, DdlPlanError> {
    use tidb_ast::DatabaseOption;
    if options
        .iter()
        .filter(|o| matches!(o, DatabaseOption::SetTiFlashReplica { .. }))
        .count()
        > 1
    {
        return Err(DdlPlanError::from(
            tidb_dbterror::ERR_RUN_MULTI_SCHEMA_CHANGES
                .gen_with_stack_by_args(&["set tiflash replica".into()]),
        ));
    }
    let mut charset = String::new();
    let mut collate = String::new();
    let mut alter_charset = false;
    let mut placement_policy = None;
    let mut tiflash_replica = None;
    for option in options {
        let selected = match option {
            DatabaseOption::CharacterSet(value) => {
                alter_charset = true;
                Some(value.clone())
            }
            DatabaseOption::Collate(value) => {
                let info = tidb_ddl_build::column_types::get_collation_by_name(value)
                    .map_err(DdlPlanError::from_mysql_error)?;
                collate = info.name;
                alter_charset = true;
                Some(info.charset_name)
            }
            DatabaseOption::PlacementPolicy(value) => {
                placement_policy = Some(value.clone());
                None
            }
            DatabaseOption::SetTiFlashReplica { count, labels } => {
                tiflash_replica = Some((*count, labels.clone()));
                None
            }
            DatabaseOption::Encryption(_) => None,
        };
        if let Some(selected) = selected {
            if charset.is_empty() {
                charset = selected;
            } else if charset != selected {
                return Err(DdlPlanError::from(
                    tidb_dbterror::ERR_CONFLICTING_DECLARATIONS
                        .gen_with_stack_by_args(&[charset.into(), selected.into()]),
                ));
            }
        }
    }
    let charset_collate = if alter_charset {
        if collate.is_empty() {
            collate = tidb_ddl_build::column_types::get_default_collation(
                &charset,
                context.default_collation_for_utf8mb4(),
            )
            .map_err(DdlPlanError::from_mysql_error)?;
        }
        Some((charset, collate))
    } else {
        None
    };
    Ok(AlterSchemaOptions {
        charset_collate,
        placement_policy,
        tiflash_replica,
    })
}

/// Go `executor.AlterSchema` up to `DoDDLJob`. `None` for any other statement.
pub fn alter_schema<S: MetaSnapshot>(
    snapshot: &mut S,
    statement: &DdlStatement,
    job_context: &dyn DdlSessionContext,
    start_ts: u64,
    upgrading: bool,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    match statement {
        DdlStatement::ModifySchemaCharsetAndCollate {
            name,
            charset,
            collate,
        } => modify_schema_charset_and_collate(
            snapshot,
            name,
            charset,
            collate,
            job_context,
            start_ts,
            upgrading,
        ),
        DdlStatement::ModifySchemaDefaultPlacement {
            name,
            placement_policy,
        } => modify_schema_default_placement(
            snapshot,
            name,
            placement_policy,
            job_context,
            start_ts,
            upgrading,
        ),
        _ => Ok(None),
    }
}

/// Go `executor.ModifySchemaCharsetAndCollate`.
pub fn modify_schema_charset_and_collate<S: MetaSnapshot>(
    snapshot: &mut S,
    name: &String,
    charset: &String,
    collate: &String,
    job_context: &dyn DdlSessionContext,
    start_ts: u64,
    upgrading: bool,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    let catalog = load_info_schema(snapshot)?;
    let (name, action, args, policy) = {
        let Some(database) = find_database(&catalog, name) else {
            return Err(DdlPlanError::from(
                tidb_infoschema::infoschema_errors::ERR_DATABASE_NOT_EXISTS
                    .gen_with_stack_by_args(&[(name.clone()).into()]),
            ));
        };
        if database.read().charset == *charset && database.read().collate == *collate {
            return Ok(Some(AddIndexSubmission::AlreadySatisfied {
                detail: format!("database `{name}` is already {charset}/{collate}"),
                warnings: Vec::new(),
            }));
        }
        (
            name,
            ActionType::ACTION_MODIFY_SCHEMA_CHARSET_AND_COLLATE,
            tidb_model::ModifySchemaArgs {
                to_charset: GoField::new(charset.clone().into()),
                to_collate: GoField::new(collate.clone().into()),
                ..tidb_model::ModifySchemaArgs::default()
            },
            None::<String>,
        )
    };
    let database = find_database(&catalog, name).expect("checked above");
    let mut job = session_job(job_context, action);
    job.schema_id = database.read().id;
    job.schema_name = database.read().name.lowercase().to_owned().into();
    let mut involving = vec![tidb_model::InvolvingSchemaInfo {
        database: job.schema_name.clone(),
        table: tidb_model::job::INVOLVING_ALL.into(),
        ..tidb_model::InvolvingSchemaInfo::default()
    }];
    if let Some(policy) = policy {
        involving.push(tidb_model::InvolvingSchemaInfo {
            policy: policy.into(),
            mode: tidb_model::InvolvingSchemaInfoMode::SHARED,
            ..tidb_model::InvolvingSchemaInfo::default()
        });
    }
    job.involving_schema_info = GoSharedSlice::from_vec(involving);
    let built = AddIndexSubmission::Job(
        Box::new(tidb_ddl_jobsubmit::JobSpec {
            job,
            args: JobArgsValue::ModifySchema(Some(GoShared::new(args))),
            id_allocated: true,
        }),
        Vec::new(),
    );
    submit_built_job(snapshot, Some(built), start_ts, upgrading)
}

/// Go `executor.ModifySchemaDefaultPlacement`.
pub fn modify_schema_default_placement<S: MetaSnapshot>(
    snapshot: &mut S,
    name: &String,
    placement_policy: &String,
    job_context: &dyn DdlSessionContext,
    start_ts: u64,
    upgrading: bool,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    let catalog = load_info_schema(snapshot)?;
    let (name, action, args, policy) = {
        if find_database(&catalog, name).is_none() {
            return Err(DdlPlanError::from(
                tidb_infoschema::infoschema_errors::ERR_DATABASE_NOT_EXISTS
                    .gen_with_stack_by_args(&[(name.clone()).into()]),
            ));
        }
        if let Some(ignored) = check_ignore_placement_ddl(job_context) {
            return Ok(Some(ignored));
        }
        let reference = check_and_normalize_placement_policy(snapshot, placement_policy)?;
        let policy = reference
            .as_ref()
            .map(|reference| reference.read().name.lowercase().to_owned());
        (
            name,
            ActionType::ACTION_MODIFY_SCHEMA_DEFAULT_PLACEMENT,
            tidb_model::ModifySchemaArgs {
                policy_ref: GoField::new(reference),
                ..tidb_model::ModifySchemaArgs::default()
            },
            policy,
        )
    };
    let database = find_database(&catalog, name).expect("checked above");
    let mut job = session_job(job_context, action);
    job.schema_id = database.read().id;
    job.schema_name = database.read().name.lowercase().to_owned().into();
    let mut involving = vec![tidb_model::InvolvingSchemaInfo {
        database: job.schema_name.clone(),
        table: tidb_model::job::INVOLVING_ALL.into(),
        ..tidb_model::InvolvingSchemaInfo::default()
    }];
    if let Some(policy) = policy {
        involving.push(tidb_model::InvolvingSchemaInfo {
            policy: policy.into(),
            mode: tidb_model::InvolvingSchemaInfoMode::SHARED,
            ..tidb_model::InvolvingSchemaInfo::default()
        });
    }
    job.involving_schema_info = GoSharedSlice::from_vec(involving);
    let built = AddIndexSubmission::Job(
        Box::new(tidb_ddl_jobsubmit::JobSpec {
            job,
            args: JobArgsValue::ModifySchema(Some(GoShared::new(args))),
            id_allocated: true,
        }),
        Vec::new(),
    );
    submit_built_job(snapshot, Some(built), start_ts, upgrading)
}

/// Go `onModifySchemaCharsetAndCollate` / `onModifySchemaDefaultPlacement`.
pub(super) fn on_modify_schema_charset_and_collate(
    txn: &mut JobTxn<'_>,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let args = tidb_model::get_modify_schema_args(&mut active.job)
        .map_err(|error| cancel_ddl_job(active, DdlPlanError::Encode(error.to_string())))?
        .ok_or_else(|| {
            cancel_ddl_job(
                active,
                DdlPlanError::Encode("ALTER DATABASE job has nil args".to_owned()),
            )
        })?;
    // Go `checkSchemaExistAndCancelNotExistJob`.
    let Some(database) = job_schema(txn, active.job.schema_id)?
    else {
        return Err(cancel_ddl_job(
            active,
            DdlPlanError::from(
                tidb_dbterror::CLASS_SCHEMA
                    .new_std(tidb_errno::errcode::ErrBadDB)
                    .generate(format!(
                        "Unknown database '(Schema ID {})'",
                        active.job.schema_id
                    )),
            ),
        ));
    };
    let mut info = database.read().clone();
    let unchanged = if active.job.type_ == ActionType::ACTION_MODIFY_SCHEMA_CHARSET_AND_COLLATE {
        let (charset, collate) = {
            let args = args.read();
            (
                args.to_charset.get().to_string(),
                args.to_collate.get().to_string(),
            )
        };
        let unchanged = info.charset == charset && info.collate == collate;
        info.charset = charset;
        info.collate = collate;
        unchanged
    } else {
        let reference = args.read().policy_ref.get();
        // Go `checkPlacementPolicyRefValidAndCanNonValidJob`: the policy
        // must still exist.
        if let Some(reference) = &reference {
            let name = reference.read().name.original().to_owned();
            check_and_normalize_placement_policy(txn, &name)
                .map_err(|error| cancel_ddl_job(active, error))?;
        }
        let wanted = reference.as_ref().map(|reference| reference.read().clone());
        let current = info
            .placement_policy_ref
            .as_ref()
            .map(|reference| reference.read().clone());
        let unchanged = wanted.is_some() && current == wanted;
        info.placement_policy_ref = reference;
        unchanged
    };
    if unchanged {
        active.job.finish_db_job(
            JobState::DONE,
            SchemaState::PUBLIC,
            0,
            Some(GoShared::new(database.read().clone())),
        );
        return Ok(action_step(job_only_write(active.job.id, Vec::new())));
    }
    // Go: `UpdateDatabase`, then `updateSchemaVersion`.
    let meta = txn.meta();
    meta.update_database(&info)
        .map_err(|error| DdlPlanError::Meta(job_meta_error(&meta, error)))?;
    let action_type = active.job.type_;
    let (schema_version, diff) = update_schema_version(
        &meta,
        active,
        SchemaDiff {
            action_type,
            schema_id: info.id,
            ..SchemaDiff::default()
        },
    )?;
    active.job.finish_db_job(
        JobState::DONE,
        SchemaState::PUBLIC,
        schema_version,
        Some(GoShared::new(info)),
    );
    let mut write = job_only_write(active.job.id, Vec::new());
    write.schema_version = schema_version;
    write.diff = diff;
    Ok(action_step(write))
}

/// Go `ModifySchemaSetTiFlashReplica`'s checks before its job loop, over the
/// latest schema: the database exists (1049), is not a memory or system
/// schema (8200), and holds a table (Go `ErrEmptyDatabase`). Answers the
/// database's name as stored and its tables (Go `SchemaTableInfos`).
///
/// # Errors
///
/// Those three refusals.
pub fn schema_tiflash_replica_tables<'catalog>(
    catalog: &'catalog SessionExtendedInfoSchema,
    name: &str,
) -> Result<(String, Vec<GoShared<TableInfo>>), DdlPlanError> {
    let Some(database) = catalog.schema_by_name(name) else {
        return Err(DdlPlanError::Admission(DdlAdmissionError::from_terror(
            &tidb_infoschema::infoschema_errors::ERR_DATABASE_NOT_EXISTS
                .gen_with_stack_by_args(&[name.into()]),
        )));
    };
    if tidb_metadef::is_mem_or_sys_db(database.read().name.lowercase()) {
        return Err(DdlPlanError::Admission(DdlAdmissionError::from_terror(
            &tidb_dbterror::ERR_UNSUPPORTED_TI_FLASH_OPERATION_FOR_SYS_OR_MEM_TABLE
                .gen_with_stack_by_args(&[]),
        )));
    }
    let tables = database.read().deprecated_tables.iter_deref().collect::<Vec<_>>();
    if tables.is_empty() {
        return Err(DdlPlanError::Admission(DdlAdmissionError::from_terror(
            &tidb_infoschema::infoschema_errors::ERR_EMPTY_DATABASE
                .gen_with_stack(format!("Empty database '{name}'")),
        )));
    }
    let name = database.read().name.original().to_owned();
    Ok((name, tables))
}

/// Go `getPendingTiFlashTableCount`'s count: tables outside the memory and
/// system schemas whose TiFlash replica is not available yet.
#[must_use]
pub fn pending_tiflash_table_count(catalog: &SessionExtendedInfoSchema) -> u32 {
    let count = catalog
        .stored_schemas()
        .iter()
        .filter(|database| !tidb_metadef::is_mem_or_sys_db(database.read().name.lowercase()))
        .flat_map(|database| database.read().deprecated_tables.iter_deref())
        .filter(|table| {
            table
                .read()
                .tiflash_replica
                .as_ref()
                .is_some_and(|replica| !replica.read().available)
        })
        .count();
    u32::try_from(count).unwrap_or(u32::MAX)
}
