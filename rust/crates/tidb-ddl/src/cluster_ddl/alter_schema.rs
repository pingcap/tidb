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

/// Go's executor halves up to `doDDLJob2`. `None` for any other statement.
pub fn prepare_alter_schema_job_submission<S: MetaSnapshot>(
    snapshot: &mut S,
    statement: &DdlStatement,
    job_context: &dyn DdlSessionContext,
    start_ts: u64,
    upgrading: bool,
    min_job_id: i64,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    let catalog = load_cluster_catalog(snapshot)?;
    let (name, action, args, policy) = match statement {
        // Go `executor.ModifySchemaCharsetAndCollate`.
        DdlStatement::ModifySchemaCharsetAndCollate {
            name,
            charset,
            collate,
        } => {
            let Some(database) = find_database(&catalog, name) else {
                return Err(DdlPlanError::UnknownDatabase(name.clone()));
            };
            if database.info.charset == *charset && database.info.collate == *collate {
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
                None,
            )
        }
        // Go `executor.ModifySchemaDefaultPlacement`.
        DdlStatement::ModifySchemaDefaultPlacement {
            name,
            placement_policy,
        } => {
            if find_database(&catalog, name).is_none() {
                return Err(DdlPlanError::UnknownDatabase(name.clone()));
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
        }
        _ => return Ok(None),
    };
    let database = find_database(&catalog, name).expect("checked above");
    let mut job = session_job(job_context, action);
    job.schema_id = database.info.id;
    job.schema_name = database.info.name.lowercase().to_owned().into();
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
        Box::new(crate::ddl_job_submit::JobSpec {
            job,
            args: JobArgsValue::ModifySchema(Some(GoShared::new(args))),
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

/// Go `onModifySchemaCharsetAndCollate` / `onModifySchemaDefaultPlacement`.
pub(super) fn plan_persisted_alter_schema_job_step<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
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
    let Some(database) = catalog
        .databases
        .iter()
        .find(|database| database.info.id == active.job.schema_id)
    else {
        return Err(cancel_ddl_job(
            active,
            DdlPlanError::from(
                tidb_util::dbterror::CLASS_SCHEMA
                    .new_std(tidb_error::tidb::errcode::ErrBadDB)
                    .generate(format!(
                        "Unknown database '(Schema ID {})'",
                        active.job.schema_id
                    )),
            ),
        ));
    };
    let mut info = database.info.clone();
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
            check_and_normalize_placement_policy(snapshot, &name)
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
            Some(GoShared::new(database.info.clone())),
        );
        return Ok(action_step(job_only_write(active.job.id, Vec::new())));
    }
    let schema_version = catalog.schema_version + 1;
    let diff = SchemaDiff {
        version: schema_version,
        action_type: active.job.type_,
        schema_id: info.id,
        ..SchemaDiff::default()
    };
    let mutations = vec![
        BufferMutation::set(
            key::database_kv_key(info.id),
            value::serialize_db_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
        )?,
        BufferMutation::set(
            key::schema_version_kv_key(),
            value::encode_int_value(schema_version),
        )?,
        BufferMutation::set(
            key::schema_diff_kv_key(schema_version),
            value::serialize_schema_diff(&diff)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
        )?,
    ];
    active.job.last_schema_version = schema_version;
    active.job.finish_db_job(
        JobState::DONE,
        SchemaState::PUBLIC,
        schema_version,
        Some(GoShared::new(info)),
    );
    let mut write = job_only_write(active.job.id, mutations);
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
    catalog: &'catalog ClusterCatalog,
    name: &str,
) -> Result<(String, Vec<&'catalog TableInfo>), DdlPlanError> {
    let Some((database, tables)) = catalog.schema_by_name(name) else {
        return Err(DdlPlanError::Admission(DdlAdmissionError::with_proto(
            &tidb_error::catalog::infoschema::ERR_DATABASE_NOT_EXISTS,
            format!("Unknown database '{name}'"),
        )));
    };
    if tidb_metadef::is_mem_or_sys_db(database.name.lowercase()) {
        return Err(DdlPlanError::Admission(DdlAdmissionError::with_proto(
            &tidb_util::dbterror::ERR_UNSUPPORTED_TI_FLASH_OPERATION_FOR_SYS_OR_MEM_TABLE,
            "Unsupported `set TiFlash replica` settings for system table and memory table",
        )));
    }
    let tables = tables.iter().collect::<Vec<_>>();
    if tables.is_empty() {
        return Err(DdlPlanError::Admission(DdlAdmissionError::with_proto(
            &tidb_error::catalog::infoschema::ERR_EMPTY_DATABASE,
            format!("Empty database '{name}'"),
        )));
    }
    Ok((database.name.original().to_owned(), tables))
}

/// Go `getPendingTiFlashTableCount`'s count: tables outside the memory and
/// system schemas whose TiFlash replica is not available yet.
#[must_use]
pub fn pending_tiflash_table_count(catalog: &ClusterCatalog) -> u32 {
    let count = catalog
        .databases
        .iter()
        .filter(|database| !tidb_metadef::is_mem_or_sys_db(database.info.name.lowercase()))
        .flat_map(|database| database.tables.iter())
        .filter(|table| {
            table
                .tiflash_replica
                .as_ref()
                .is_some_and(|replica| !replica.read().available)
        })
        .count();
    u32::try_from(count).unwrap_or(u32::MAX)
}
