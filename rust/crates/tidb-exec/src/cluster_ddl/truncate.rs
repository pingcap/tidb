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

//! TRUNCATE TABLE as Go runs it: `executor.TruncateTable` queues a job whose
//! new table and partition ids `GenGIDAndInsertJobs` assigns, and the owner's
//! `onTruncateTable` swaps the table under the new identity in one step,
//! leaving the old rows to the delete range.

use super::add_index::AddIndexSubmission;
use super::*;

/// Go `executor.TruncateTable` up to `doDDLJob2`. `None` for any other
/// statement.
pub fn prepare_truncate_table_job_submission<S: MetaSnapshot>(
    snapshot: &mut S,
    statement: &DdlStatement,
    job_context: &tidb_executor::StmtContext,
    start_ts: u64,
    upgrading: bool,
    min_job_id: i64,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    let DdlStatement::TruncateTable { schema, table } = statement else {
        return Ok(None);
    };
    let catalog = load_cluster_catalog(snapshot)?;
    let (db_id, stored) = locate_table(&catalog, schema, table)?;
    // A view or a sequence has no rows to truncate and reads as missing.
    if stored.view.is_some() || stored.is_sequence() {
        let database = find_database(&catalog, schema).expect("located above");
        return Err(DdlPlanError::TableNotExists {
            schema: database.info.name.original().to_owned(),
            table: stored.name.original().to_owned(),
        });
    }
    let fk_check = job_context.foreign_key_checks();
    if let Some(referred) = crate::foreign_key_build::check_table_has_foreign_key_referred(
        &catalog,
        schema,
        table,
        &[(schema.clone(), table.clone())],
        fk_check && tidb_vardef::ENABLE_FOREIGN_KEY.load(std::sync::atomic::Ordering::Relaxed),
    ) {
        return Err(truncate_referenced(&referred));
    }
    let old_partition_ids = add_index::partition_ids(stored);
    let mut job = session_job(job_context, ActionType::ACTION_TRUNCATE_TABLE);
    job.schema_id = db_id;
    job.table_id = stored.id;
    job.schema_name = schema.go_to_lower().into();
    job.table_name = stored.name.lowercase().to_owned().into();
    job.session_vars = Some(GoShared::new(std::collections::BTreeMap::from([(
        tidb_vardef::tidb_vars::TIDB_SCATTER_REGION.to_owned(),
        job_context.ddl_reorg_vars().scatter_region.clone().into(),
    )])));
    let args = GoShared::new(tidb_model::TruncateTableArgs {
        fk_check: GoField::new(fk_check),
        old_partition_ids: GoField::new(GoSharedSlice::from_vec(old_partition_ids)),
        ..tidb_model::TruncateTableArgs::default()
    });
    let mut specs = [crate::ddl_job_submit::JobSpec {
        job,
        args: JobArgsValue::TruncateTable(Some(args)),
        // `assignGIDsForJobs` draws the new table and partition ids.
        id_allocated: false,
    }];
    crate::ddl_job_submit::prepare_submit_batch(
        snapshot, &catalog, &mut specs, start_ts, upgrading, min_job_id,
    )?;
    let [spec] = specs;
    Ok(Some(AddIndexSubmission::Job(Box::new(spec), Vec::new())))
}

/// Go `ErrTruncateIllegalForeignKey` with the child named.
fn truncate_referenced(referred: &tidb_model::table::ReferredFKInfo) -> DdlPlanError {
    coded_refusal(
        &tidb_util::dbterror::ERR_TRUNCATE_ILLEGAL_FOREIGN_KEY,
        format!(
            "Cannot truncate a table referenced in a foreign key constraint (`{}`.`{}` CONSTRAINT `{}`)",
            referred.child_schema.original(),
            referred.child_table.original(),
            referred.child_fk_name.original()
        ),
    )
}

/// Go `onTruncateTable`.
pub(super) fn plan_persisted_truncate_table_job_step<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let args = tidb_model::get_truncate_table_args(&mut active.job)
        .map_err(|error| cancel_ddl_job(active, DdlPlanError::Encode(error.to_string())))?
        .ok_or_else(|| {
            cancel_ddl_job(
                active,
                DdlPlanError::Encode("TRUNCATE TABLE has nil args".to_owned()),
            )
        })?;
    let stored = checked_job_table(catalog, active, false)?.clone_like_go();
    let schema = active.job.schema_name.to_string();
    if stored.view.is_some() || stored.is_sequence() {
        return Err(cancel_ddl_job(
            active,
            DdlPlanError::TableNotExists {
                schema: schema.clone(),
                table: stored.name.original().to_owned(),
            },
        ));
    }
    // Go `checkTruncateTableHasForeignKeyReferredInOwner`.
    let fk_check = args.read().fk_check.get();
    if let Some(referred) = crate::foreign_key_build::check_table_has_foreign_key_referred(
        catalog,
        &schema,
        stored.name.original(),
        &[(schema.clone(), stored.name.original().to_owned())],
        fk_check && tidb_vardef::ENABLE_FOREIGN_KEY.load(std::sync::atomic::Ordering::Relaxed),
    ) {
        return Err(cancel_ddl_job(active, truncate_referenced(&referred)));
    }
    let database_id = active.job.schema_id;
    let (new_table_id, new_partition_ids) = {
        let args = args.read();
        (
            args.new_table_id.get(),
            args.new_partition_ids.get().snapshot(),
        )
    };
    let old_partition_ids = add_index::partition_ids(&stored);
    let mut mutations = Vec::new();
    let mut placement_bundles = Vec::new();
    let mut affinity_groups = crate::ddl_affinity::AffinityGroupUpdate::default();
    let mut label_rules = LabelRuleSteps::new();
    let info = apply_truncate_table(
        snapshot,
        &schema,
        database_id,
        &stored,
        new_table_id,
        &new_partition_ids,
        start_ts,
        &mut mutations,
        &mut placement_bundles,
        &mut affinity_groups,
        &mut label_rules,
    )?;
    let schema_version = catalog.schema_version + 1;
    let diff = SchemaDiff {
        version: schema_version,
        action_type: ActionType::ACTION_TRUNCATE_TABLE,
        schema_id: database_id,
        table_id: new_table_id,
        old_table_id: stored.id,
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
    if !tidb_metadef::is_mem_or_sys_db(&schema.go_to_lower()) {
        append_schema_change_mutations(
            snapshot,
            catalog,
            active.job.id,
            &[(
                -1,
                SchemaChangeEvent::truncate_table(info.clone_like_go(), stored.clone_like_go()),
            )],
            &mut mutations,
        )?;
    }
    active.job.last_schema_version = schema_version;
    active.job.finish_table_job(
        JobState::DONE,
        SchemaState::PUBLIC,
        schema_version,
        Some(GoShared::new(info.clone_like_go())),
    );
    {
        let args = args.write();
        args.old_partition_ids
            .set(GoSharedSlice::from_vec(old_partition_ids));
        args.new_partition_ids
            .set(GoSharedSlice::from_vec(new_partition_ids));
    }
    active.job.fill_finished_args(Some(args));
    let mut write = job_only_write(active.job.id, mutations);
    write.schema_version = schema_version;
    write.diff = diff;
    write.created_id = Some(new_table_id);
    write.placement_bundles = placement_bundles;
    write.affinity_groups = affinity_groups;
    write.label_rules = label_rules.steps;
    Ok(PersistedDdlActionStep {
        write,
        // Go `needUpdateRawArgs`: history and the delete range read these.
        update_raw_args: true,
        run_error: None,
        reorg: None,
    })
}
