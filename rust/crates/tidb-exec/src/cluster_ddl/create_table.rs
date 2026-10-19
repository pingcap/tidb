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
    job_context: &tidb_executor::StmtContext,
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
    let catalog = load_cluster_catalog(snapshot)?;
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
    let mut job = Job::default();
    job.version = get_job_ver_in_use();
    job.schema_id = database.info.id;
    job.schema_name = database.info.name.lowercase().to_owned().into();
    job.table_name = info.name.lowercase().to_owned().into();
    job.type_ = ActionType::ACTION_CREATE_TABLE;
    job.binlog_info = Some(GoShared::new(HistoryInfo::default()));
    job.cdc_write_source = job_context.ddl_cdc_write_source();
    job.sql_mode = job_context.ddl_sql_mode();
    // Go's partition sugar rewrites `QueryString` before the job copies it.
    job.query = tidb_executor::ddl::job_history::rewritten_query(
        job_context.ddl_query(),
        build.context.ddl_query_rewrite().as_ref(),
    )
    .into();
    let args = GoShared::new(tidb_model::CreateTableArgs {
        table_info: GoField::new(Some(GoShared::new(info))),
        fk_check: GoField::new(build.context.foreign_key_checks()),
        ..tidb_model::CreateTableArgs::default()
    });
    let mut specs = [crate::ddl_job_submit::JobSpec {
        job,
        args: JobArgsValue::CreateTable(Some(args)),
        // `assignGIDsForJobs` draws the table's and its partitions' ids.
        id_allocated: false,
    }];
    crate::ddl_job_submit::prepare_submit_batch(
        snapshot, &catalog, &mut specs, start_ts, upgrading, min_job_id,
    )?;
    let [spec] = specs;
    Ok(Some(AddIndexSubmission::Job(Box::new(spec), warnings)))
}

/// Go `onCreateTable`.
pub(super) fn plan_persisted_create_table_job_step<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
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
        .databases
        .iter()
        .find(|database| database.info.id == active.job.schema_id)
        .ok_or_else(|| {
            cancel_ddl_job(
                active,
                DdlPlanError::from(
                    tidb_util::dbterror::CLASS_SCHEMA
                        .new_std(tidb_error::tidb::errcode::ErrBadDB)
                        .generate(format!(
                            "Unknown database '(Schema ID {})'",
                            active.job.schema_id
                        )),
                ),
            )
        })?;
    let database_id = database.info.id;
    let schema = database.info.name.original().to_owned();
    let has_foreign_keys = !info.foreign_keys.is_empty();
    let schema_version = catalog.schema_version + 1;
    let mut mutations = Vec::new();
    let mut placement_bundles = Vec::new();
    let mut affinity_groups = crate::ddl_affinity::AffinityGroupUpdate::default();
    let finished = match info.state {
        SchemaState::NONE | SchemaState::PUBLIC => {
            // Go `createTable`: `checkTableNotExists`, then
            // `checkConstraintNamesNotExists`, then
            // `checkTableForeignKeyValidInOwner` and `allocateFKIndexID`.
            info.state = SchemaState::NONE;
            if database
                .tables
                .iter()
                .any(|table| table.name.lowercase() == info.name.lowercase() || table.id == info.id)
            {
                return Err(cancel_ddl_job(
                    active,
                    DdlPlanError::from(
                        tidb_util::dbterror::CLASS_SCHEMA
                            .new_std(tidb_error::tidb::errcode::ErrTableExists)
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
                if database.tables.iter().any(|table| {
                    table
                        .constraints
                        .iter_deref()
                        .any(|existing| existing.read().name.lowercase() == wanted)
                }) {
                    return Err(cancel_ddl_job(
                        active,
                        DdlPlanError::from(
                            tidb_util::dbterror::CLASS_SCHEMA
                                .new_std(tidb_error::tidb::errcode::ErrCheckConstraintDupName)
                                .generate_with_stack_by_args(&[FormatArg::from(wanted.as_str())]),
                        ),
                    ));
                }
            }
            crate::foreign_key_build::check_table_foreign_keys_valid_in_owner(
                catalog, &schema, &mut info, fk_check,
            )
            .map_err(|error| cancel_ddl_job(active, DdlPlanError::Admission(error)))?;
            info.state = SchemaState::PUBLIC;
            apply_create_table(
                snapshot,
                database_id,
                &mut info,
                start_ts,
                &mut mutations,
                &mut placement_bundles,
                &mut affinity_groups,
            )
            .map_err(|error| cancel_ddl_job(active, error))?;
            if has_foreign_keys {
                // Go `createTableWithForeignKeys`: delete-only first, so a
                // cascading action reaches it before any statement can.
                info.state = SchemaState::DELETE_ONLY;
                let table_key = key::table_kv_key(database_id, info.id);
                mutations.retain(|mutation| mutation.key() != table_key.as_slice());
                mutations.push(BufferMutation::set(
                    key::table_kv_key(database_id, info.id),
                    value::serialize_table_info(&info)
                        .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
                )?);
                active.job.schema_state = SchemaState::DELETE_ONLY;
                false
            } else {
                true
            }
        }
        // `WriteOnly` is the state older versions left mid-job.
        SchemaState::DELETE_ONLY | SchemaState::WRITE_ONLY => {
            info.state = SchemaState::PUBLIC;
            info.update_ts = start_ts;
            mutations.push(BufferMutation::set(
                key::table_kv_key(database_id, info.id),
                value::serialize_table_info(&info)
                    .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
            )?);
            true
        }
        state => {
            return Err(DdlPlanError::from(
                tidb_util::dbterror::ERR_INVALID_DDL_JOB.generate_with_stack_by_args(&[
                    FormatArg::from("table"),
                    FormatArg::new(state.to_string(), state.0.to_string(), "model.SchemaState"),
                ]),
            ))
        }
    };
    let diff = SchemaDiff {
        version: schema_version,
        action_type: active.job.type_,
        schema_id: database_id,
        table_id: info.id,
        ..SchemaDiff::default()
    };
    mutations.push(BufferMutation::set(
        key::schema_version_kv_key(),
        value::encode_int_value(schema_version),
    )?);
    mutations.push(BufferMutation::set(
        key::schema_diff_kv_key(schema_version),
        value::serialize_schema_diff(&diff)
            .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
    )?);
    active.job.last_schema_version = schema_version;
    // The args carry the table on to the next step (Go mutates the pointer
    // the job re-encodes).
    *table_handle.write() = info.clone_like_go();
    if finished {
        if !tidb_metadef::is_mem_or_sys_db(&schema.go_to_lower()) {
            append_schema_change_mutations(
                snapshot,
                catalog,
                active.job.id,
                &[(-1, SchemaChangeEvent::create_table(info.clone_like_go()))],
                &mut mutations,
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
