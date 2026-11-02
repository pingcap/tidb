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

//! CREATE TABLE as Go runs it: `executor.CreateTable` builds the table and
//! queues a job whose ids `assignGIDsForJobs` draws, and the owner's
//! `onCreateTable` publishes it (`createTable`), through delete-only first
//! when it has foreign keys (`createTableWithForeignKeys`).

use super::add_index::AddIndexSubmission;
use super::*;

/// Go `executor.CreateTable` up to `doDDLJob2`. `None` for any other
/// statement.
pub fn prepare_create_table_job_submission<S: MetaSnapshot>(
    snapshot: &mut S,
    statement: &DdlStatement,
    job_context: &dyn DdlSessionContext,
    start_ts: u64,
    upgrading: bool,
    min_job_id: i64,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    let DdlStatement::CreateTable {
        schema,
        table,
        if_not_exists,
        build,
    } = statement
    else {
        return Ok(None);
    };
    let catalog = load_info_schema(snapshot)?;
    let mut warnings = Vec::new();
    let info = match build_create_table_info(
        snapshot,
        &catalog,
        schema,
        table,
        *if_not_exists,
        build,
        &mut warnings,
    )? {
        Ok(info) => info,
        Err(DdlPlan::AlreadySatisfied {
            detail,
            warnings: notes,
        }) => {
            warnings.extend(notes);
            return Ok(Some(AddIndexSubmission::AlreadySatisfied {
                detail,
                warnings,
            }));
        }
        Err(DdlPlan::Write(_)) => unreachable!("the builder answers no write"),
    };
    let database = find_database(&catalog, schema).expect("the builder found the database");
    let mut job = session_job(job_context, ActionType::ACTION_CREATE_TABLE);
    job.schema_id = database.read().id;
    job.schema_name = database.read().name.lowercase().to_owned().into();
    job.table_name = info.name.lowercase().to_owned().into();
    // Go's partition sugar rewrites `QueryString` before the job copies it;
    // a text without the sugar is `rewritePartitionQueryString`'s error.
    if let Some((original, _)) = build.context.ddl_query_rewrite() {
        let query = job_context.ddl_query();
        if !query.is_empty() && !query.contains(original.as_str()) {
            return Err(DdlPlanError::Admission(DdlAdmissionError::plain(
                "Can't find INTERVAL definition in PREPARE STMT",
            )));
        }
    }
    job.query = tidb_ddl_build::partition_interval::rewritten_query(
        job_context.ddl_query(),
        build.context.ddl_query_rewrite().as_ref(),
    )
    .into();
    // Go `getSharedInvolvingSchemaInfo`: the referenced parents and the
    // placement policy, shared, behind the table itself.
    let shared = get_shared_involving_schema_info(&info);
    if !shared.is_empty() {
        let mut involving = vec![tidb_model::InvolvingSchemaInfo {
            database: job.schema_name.clone(),
            table: job.table_name.clone(),
            ..tidb_model::InvolvingSchemaInfo::default()
        }];
        involving.extend(shared);
        job.involving_schema_info = GoSharedSlice::from_vec(involving);
    }
    job.session_vars = Some(GoShared::new(std::collections::BTreeMap::from([(
        tidb_vardef::tidb_vars::TIDB_SCATTER_REGION.to_owned(),
        job_context.ddl_reorg_vars().scatter_region.clone().into(),
    )])));
    let args = GoShared::new(tidb_model::CreateTableArgs {
        table_info: GoField::new(Some(GoShared::new(info))),
        fk_check: GoField::new(build.context.foreign_key_checks()),
        ..tidb_model::CreateTableArgs::default()
    });
    let mut specs = [tidb_ddl_jobsubmit::JobSpec {
        job,
        args: JobArgsValue::CreateTable(Some(args)),
        // `assignGIDsForJobs` draws the table's and its partitions' ids.
        id_allocated: false,
    }];
    tidb_ddl_jobsubmit::prepare_submit_batch(
        snapshot, &catalog, &mut specs, start_ts, upgrading, min_job_id,
    )?;
    let [spec] = specs;
    Ok(Some(AddIndexSubmission::Job(Box::new(spec), warnings)))
}

/// Go `getSharedInvolvingSchemaInfo`.
pub(super) fn get_shared_involving_schema_info(
    info: &TableInfo,
) -> Vec<tidb_model::InvolvingSchemaInfo> {
    let mut involving: Vec<_> = info
        .foreign_keys
        .iter_deref()
        .map(|fk| {
            let fk = fk.read();
            tidb_model::InvolvingSchemaInfo {
                database: fk.ref_schema.lowercase().to_owned().into(),
                table: fk.ref_table.lowercase().to_owned().into(),
                mode: tidb_model::InvolvingSchemaInfoMode::SHARED,
                ..tidb_model::InvolvingSchemaInfo::default()
            }
        })
        .collect();
    if let Some(policy) = &info.placement_policy_ref {
        involving.push(tidb_model::InvolvingSchemaInfo {
            policy: policy.read().name.lowercase().to_owned().into(),
            mode: tidb_model::InvolvingSchemaInfoMode::SHARED,
            ..tidb_model::InvolvingSchemaInfo::default()
        });
    }
    involving
}

/// Go `createTable`, which `onCreateTable` and `onCreateTables` share: the
/// owner's checks, then the table public with its placement bundles,
/// affinity groups and allocator rebases. It neither bumps the schema version
/// nor finishes the job.
#[allow(clippy::too_many_arguments)]
pub(super) fn create_table(
    txn: &mut JobTxn<'_>,
    catalog: &SessionExtendedInfoSchema,
    database: &GoShared<tidb_model::db::DBInfo>,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    info: &mut TableInfo,
    fk_check: bool,
    start_ts: u64,
    placement_bundles: &mut Vec<tidb_placement::Bundle>,
    affinity_groups: &mut crate::ddl_affinity::AffinityGroupUpdate,
) -> Result<(), DdlPlanError> {
    let database_id = database.read().id;
    let schema = database.read().name.original().to_owned();
    // Go: `checkTableNotExists`, then `checkConstraintNamesNotExists`, then
    // `checkTableForeignKeyValidInOwner` and `allocateFKIndexID`.
    info.state = SchemaState::NONE;
    if database
        .read().deprecated_tables
        .iter_deref()
        .any(|table| {
            let table = table.read();
            table.name.lowercase() == info.name.lowercase() || table.id == info.id
        })
    {
        return Err(cancel_ddl_job(
            active,
            DdlPlanError::from(
                tidb_dbterror::CLASS_SCHEMA
                    .new_std(tidb_errno::errcode::ErrTableExists)
                    .generate(format!(
                        "Table '{}.{}' already exists",
                        schema,
                        info.name.original()
                    )),
            ),
        ));
    }
    for constraint in info.constraints.iter_deref() {
        let wanted = constraint.read().name.lowercase().to_owned();
        if database.read().deprecated_tables.iter_deref().any(|table| {
            let table = table.read();
            table
                .constraints
                .iter_deref()
                .any(|existing| existing.read().name.lowercase() == wanted)
        }) {
            return Err(cancel_ddl_job(
                active,
                DdlPlanError::from(
                    tidb_dbterror::CLASS_SCHEMA
                        .new_std(tidb_errno::errcode::ErrCheckConstraintDupName)
                        .generate_with_stack_by_args(&[FormatArg::from(wanted.as_str())]),
                ),
            ));
        }
    }
    crate::foreign_key_build::check_table_foreign_key_valid_in_owner(
        catalog, &schema, info, fk_check,
    )
    .map_err(|error| cancel_ddl_job(active, DdlPlanError::Admission(error)))?;
    info.state = SchemaState::PUBLIC;
    apply_create_table(txn, database_id, info, start_ts, placement_bundles, affinity_groups)
        .map_err(|error| cancel_ddl_job(active, error))
}

/// Go `onCreateTable`.
pub(super) fn on_create_table(
    txn: &mut JobTxn<'_>,
    catalog: &SessionExtendedInfoSchema,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let args = tidb_model::get_create_table_args(&mut active.job)
        .map_err(|error| cancel_ddl_job(active, DdlPlanError::Encode(error.to_string())))?
        .ok_or_else(|| {
            cancel_ddl_job(
                active,
                DdlPlanError::Encode("CREATE TABLE job has nil args".to_owned()),
            )
        })?;
    let (table_handle, fk_check) = {
        let args = args.read();
        (
            args.table_info.get().ok_or_else(|| {
                DdlPlanError::Encode("CREATE TABLE job has nil table_info".to_owned())
            })?,
            args.fk_check.get(),
        )
    };
    let mut info = table_handle.read().clone();
    let database = catalog
        .schema_by_id(active.job.schema_id)
        .ok_or_else(|| {
            cancel_ddl_job(
                active,
                DdlPlanError::from(
                    tidb_dbterror::CLASS_SCHEMA
                        .new_std(tidb_errno::errcode::ErrBadDB)
                        .generate(format!(
                            "Unknown database '(Schema ID {})'",
                            active.job.schema_id
                        )),
                ),
            )
        })?;
    let database_id = database.read().id;
    let schema = database.read().name.original().to_owned();
    let has_foreign_keys = !info.foreign_keys.is_empty();
    let mut mutations = Vec::new();
    let mut placement_bundles = Vec::new();
    let mut affinity_groups = crate::ddl_affinity::AffinityGroupUpdate::default();
    let finished = match info.state {
        SchemaState::NONE | SchemaState::PUBLIC => {
            create_table(
                txn,
                catalog,
                &database,
                active,
                &mut info,
                fk_check,
                start_ts,
                &mut placement_bundles,
                &mut affinity_groups,
            )?;
            // Go `createTableWithForeignKeys`: the table goes delete-only
            // first (`updateVersionAndTableInfo`, whose version is the one
            // below), so a cascading action reaches it before any statement
            // can.
            if has_foreign_keys {
                info.state = SchemaState::DELETE_ONLY;
                let meta = txn.meta();
                meta.update_table(database_id, &mut info)
                    .map_err(|error| DdlPlanError::Meta(job_meta_error(&meta, error)))?;
                active.job.schema_state = SchemaState::DELETE_ONLY;
            }
            !has_foreign_keys
        }
        // `WriteOnly` is the state older versions left mid-job.
        SchemaState::DELETE_ONLY | SchemaState::WRITE_ONLY => {
            info.state = SchemaState::PUBLIC;
            info.update_ts = start_ts;
            let meta = txn.meta();
            meta.update_table(database_id, &mut info)
                .map_err(|error| DdlPlanError::Meta(job_meta_error(&meta, error)))?;
            true
        }
        state => {
            return Err(DdlPlanError::from(
                tidb_dbterror::ERR_INVALID_DDL_JOB.generate_with_stack_by_args(&[
                    FormatArg::from("table"),
                    FormatArg::new(state.to_string(), state.0.to_string(), "model.SchemaState"),
                ]),
            ))
        }
    };
    let action_type = active.job.type_;
    let (schema_version, diff) = update_schema_version(
        &txn.meta(),
        active,
        SchemaDiff {
            action_type,
            schema_id: database_id,
            table_id: info.id,
            ..SchemaDiff::default()
        },
    )?;
    // The args carry the table on to the next step (Go mutates the pointer
    // the job re-encodes).
    *table_handle.write() = info.clone_like_go();
    if finished {
        if !tidb_metadef::is_mem_or_sys_db(&schema.go_to_lower()) {
            append_schema_change_mutations(
                catalog,
                active.job.id,
                &[(-1, SchemaChangeEvent::create_table(info.clone_like_go()))],
                &mut mutations,
                row_ids,
            )?;
        }
        active.job.finish_table_job(
            JobState::DONE,
            SchemaState::PUBLIC,
            schema_version,
            Some(GoShared::new(info.clone_like_go())),
        );
    }
    let mut write = job_only_write(active.job.id, mutations);
    write.schema_version = schema_version;
    write.diff = diff;
    write.created_id = Some(info.id);
    write.placement_bundles = placement_bundles;
    write.affinity_groups = affinity_groups;
    Ok(PersistedDdlActionStep {
        write,
        update_raw_args: true,
        run_error: None,
        reorg: None,
    })
}
