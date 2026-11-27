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

/// Go `executor.TruncateTable`: typed arguments through `doDDLJob2`.
#[allow(clippy::too_many_arguments)]
pub fn truncate_table_by_name<S: MetaSnapshot>(
    snapshot: &mut S,
    schema: &str,
    table: &str,
    job_context: &dyn DdlSessionContext,
    start_ts: u64,
    upgrading: bool,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    let catalog = tidb_infoschema::cluster_catalog::load_executor_info_schema(snapshot)?;
    let (db_id, stored) = locate_table(&catalog, schema, table)?;
    let stored = stored.read();
    // A view or a sequence has no rows to truncate and reads as missing.
    if stored.view.is_some() || stored.is_sequence() {
        let database = find_database(&catalog, schema).expect("located above");
        return Err(DdlPlanError::from(
            tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS.gen_with_stack_by_args(&[
                (database.read().name.original().to_owned()).into(),
                (stored.name.original().to_owned()).into(),
            ]),
        ));
    }
    // Go master `checkTableMaterializedViewConstraints`.
    super::materialized_view::check_table_materialized_view_constraints(&stored, "TRUNCATE TABLE")
        .map_err(DdlPlanError::from_mysql_error)?;
    tidb_failpoint::inject_call(
        "github.com/pingcap/tidb/pkg/ddl/afterCheckTruncateTableMaterializedViewConstraints",
        &mut stored.id.clone(),
    );
    // Go `TruncateTable` checks under the session's `foreign_key_checks`
    // alone; only the owner's re-check follows `tidb_enable_foreign_key`.
    let fk_check = job_context.foreign_key_checks();
    if let Some(referred) = crate::foreign_key_build::check_table_has_foreign_key_referred(
        &catalog,
        schema,
        table,
        &[(schema.to_owned(), table.to_owned())],
        fk_check,
    ) {
        return Err(truncate_referenced(&referred));
    }
    let old_partition_ids = add_index::partition_ids(&stored);
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
    let mut specs = [tidb_ddl_jobsubmit::JobSpec {
        job,
        args: JobArgsValue::TruncateTable(Some(args)),
        // `assignGIDsForJobs` draws the new table and partition ids.
        id_allocated: false,
    }];
    tidb_ddl_jobsubmit::prepare_submit_batch(snapshot, &mut specs, start_ts, upgrading)?;
    let [spec] = specs;
    Ok(Some(AddIndexSubmission::Job(Box::new(spec), Vec::new())))
}

/// Go `ErrTruncateIllegalForeignKey` with the child named.
fn truncate_referenced(referred: &tidb_model::table::ReferredFKInfo) -> DdlPlanError {
    coded_refusal(
        &tidb_dbterror::ERR_TRUNCATE_ILLEGAL_FOREIGN_KEY,
        format!(
            "Cannot truncate a table referenced in a foreign key constraint (`{}`.`{}` CONSTRAINT `{}`)",
            referred.child_schema.original(),
            referred.child_table.original(),
            referred.child_fk_name.original()
        ),
    )
}

/// Go `onTruncateTable`.
pub(super) fn on_truncate_table(
    txn: &mut JobTxn<'_>,
    catalog: &SessionExtendedInfoSchema,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let args = tidb_model::get_truncate_table_args(&mut active.job)
        .map_err(|error| cancel_ddl_job(active, DdlPlanError::Encode(error.to_string())))?
        .ok_or_else(|| {
            cancel_ddl_job(
                active,
                DdlPlanError::Encode("TRUNCATE TABLE has nil args".to_owned()),
            )
        })?;
    let mut info = checked_job_table(txn, active, false)?.read().clone_like_go();
    let schema = active.job.schema_name.to_string();
    if info.view.is_some() || info.is_sequence() {
        return Err(cancel_ddl_job(
            active,
            DdlPlanError::from(
                tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS.gen_with_stack_by_args(
                    &[
                        (schema.clone()).into(),
                        (info.name.original().to_owned()).into(),
                    ],
                ),
            ),
        ));
    }
    // Go master `onTruncateTable`: the executor's check, again in the owner.
    if let Err(error) = super::materialized_view::check_table_materialized_view_constraints(&info, "TRUNCATE TABLE") {
        return Err(cancel_ddl_job(active, DdlPlanError::from_mysql_error(error)));
    }
    // Copy the old table info for later usage.
    let stored = info.clone_like_go();
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
    let partition_count = info
        .get_partition_info()
        .map_or(0, |partition| partition.read().definitions.len());
    let is_partition_affinity = info
        .affinity
        .as_ref()
        .is_some_and(|affinity| affinity.read().level == "partition");
    if is_partition_affinity && args.read().new_partition_ids.get().len() < partition_count {
        // Reserve additional ids and persist them with the job before
        // creating external groups: a failed truncate must not retain
        // rolled-back ids.
        let missing = partition_count - args.read().new_partition_ids.get().len();
        let meta = txn.meta();
        let more_ids = meta
            .gen_global_ids(missing as i32)
            .map_err(|error| job_meta_error(&meta, error))?;
        {
            let args = args.write();
            let mut ids = args.new_partition_ids.get().snapshot();
            ids.extend(more_ids);
            args.new_partition_ids.set(GoSharedSlice::from_vec(ids));
        }
        active.job.fill_args(Some(args));
        return Ok(PersistedDdlActionStep {
            write: job_only_write(active.job.id, Vec::new()),
            update_raw_args: true,
            run_error: None,
            reorg: None,
        });
    }
    let old_table_id = stored.id;
    let new_table_id = args.read().new_table_id.get();
    // Go: `DropTableOrView` and `GetAutoIDAccessors(...).Del()` of the old
    // id; a failure cancels.
    {
        let meta = txn.meta();
        if let Err(error) = meta.drop_table_or_view(database_id, old_table_id) {
            let error = job_meta_error(&meta, error);
            return Err(cancel_ddl_job(active, error));
        }
        if let Err(error) = meta.auto_ids(database_id, old_table_id).delete() {
            let error = job_meta_error(&meta, error);
            return Err(cancel_ddl_job(active, error));
        }
    }
    if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/truncateTableErr").is_some_and(|val| val.bool()) {
        return Err(cancel_ddl_job(
            active,
            DdlPlanError::Encode("occur an error after dropping table".to_owned()),
        ));
    }
    // Clear the TiFlash replica progress.
    if info.tiflash_replica.is_some() {
        tidb_domain_infosync::tiflash_manager::delete_tiflash_table_sync_progress(&info);
    }
    let mut old_partition_ids = Vec::new();
    let mut new_partition_ids = args.read().new_partition_ids.get().snapshot();
    if info.get_partition_info().is_some() {
        old_partition_ids = add_index::partition_ids(&info);
        // The new partition ids, since all the old data is encoded with the
        // old ones and can no longer be accessed.
        new_partition_ids = truncate_table_by_reassign_partition_ids(txn, &info, new_partition_ids)?;
    }
    // Go `OldPartIDsWithPolicy` / `NewPartIDsWithPolicy`: the schema diff's
    // placement affects for the partitions with a placement policy.
    let (mut old_part_ids_with_policy, mut new_part_ids_with_policy) = (Vec::new(), Vec::new());
    if let Some(partition) = info.get_partition_info() {
        let definitions = partition.read().definitions.snapshot();
        for (old_id, definition) in old_partition_ids.iter().zip(&definitions) {
            if definition.placement_policy_ref.is_some() {
                old_part_ids_with_policy.push(*old_id);
                new_part_ids_with_policy.push(definition.id);
            }
        }
    }
    // Go `getOldLabelRules` and `updateLabelRules`: the rules named for the
    // table and its partitions re-range over the new ids.
    let mut label_rules = LabelRuleSteps::new();
    let label_table = LabelRuleTable::from_table_info(&info);
    let old_rules = label_rules::get_old_label_rules(
        label_rules.manager(),
        &label_table,
        &schema.go_to_lower(),
        &label_table.name,
    )
    .map_err(|error| {
        cancel_ddl_job(active, pd_step_error(Some("failed to get old label rules from PD"), error))
    })?;
    label_rules
        .push(
            label_rules::update_label_rules(
                &schema.go_to_lower(),
                &label_table,
                &old_rules,
                &[],
                new_table_id,
            ),
            Some("failed to update the label rule to PD"),
        )
        .map_err(|error| cancel_ddl_job(active, error))?;
    // Go: the TiFlash replica's PD rules over the new ids; a refusal cancels.
    if let Some((count, labels)) = tiflash_replica_spec(&info) {
        if let Some(partition) = info.get_partition_info() {
            let definitions = partition.read().definitions.snapshot();
            configure_tiflash_pd_for_partitions(true, &definitions, count, &labels, info.id)
                .map_err(|error| cancel_ddl_job(active, error))?;
        } else {
            configure_tiflash_pd_for_table(new_table_id, count, &labels)
                .map_err(|error| cancel_ddl_job(active, error))?;
        }
    }
    if let Some(replica) = &info.tiflash_replica {
        let mut replica = replica.write();
        replica.available_partition_ids = Default::default();
        replica.available = false;
    }
    info.id = new_table_id;
    // Go `updateMaskingPolicyTableIDAfterTruncate`: the policies follow the
    // table to its new id. Column ids stay the same.
    super::masking_policy::update_masking_policy_table_id_after_truncate(txn, old_table_id, info.id)
        .map_err(|error| cancel_ddl_job(active, error))?;
    // Go `placement.NewFullTableBundles`: the bundles for the new identity,
    // sent before the version is published; a failure cancels.
    let mut placement_bundles = Vec::new();
    if info.placement_policy_ref.is_some()
        || info.partition.as_ref().is_some_and(|partition| {
            partition
                .read()
                .definitions
                .snapshot()
                .iter()
                .any(|definition| definition.placement_policy_ref.is_some())
        })
    {
        let policies = load_policies(txn).map_err(|error| cancel_ddl_job(active, error))?;
        placement_bundles = tidb_placement::new_full_table_bundles(&policies, &info).map_err(|error| {
            cancel_ddl_job(
                active,
                DdlPlanError::Admission(DdlAdmissionError::plain(error.to_string())),
            )
        })?;
    }
    {
        let meta = txn.meta();
        if let Err(error) = meta.create_table_or_view(database_id, &info) {
            let error = job_meta_error(&meta, error);
            return Err(cancel_ddl_job(active, error));
        }
    }
    if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/mockTruncateTableUpdateVersionError")
        .is_some_and(|val| val.bool())
    {
        return Err(DdlPlanError::Encode("mock update version error".to_owned()));
    }
    // Create the new affinity groups first (critical: a failure cancels).
    // A cancelled job's `deleteAffinityGroupsOfCancelledJob` deletes the
    // replacement groups, including groups PD created before it failed. The
    // old table's groups stay until this step commits, since a later failure
    // can still cancel the job and restore the old table;
    // `cleanup_truncate_table_affinity_groups_after_commit` deletes them.
    if info.affinity.is_some() {
        crate::ddl_affinity::create_table_affinity_groups_in_pd(&info, &[])
            .map_err(|error| cancel_ddl_job(active, pd_step_error(None, error)))?;
    }
    let (schema_version, diff) = update_schema_version(
        &txn.meta(),
        active,
        SchemaDiff {
            action_type: ActionType::ACTION_TRUNCATE_TABLE,
            schema_id: database_id,
            table_id: new_table_id,
            old_table_id,
            // Go `SetSchemaDiffForTruncateTable`: the affects update the
            // placement rule cache.
            affected_options: if old_part_ids_with_policy.is_empty() {
                Default::default()
            } else {
                super::partition::build_placement_affects(&old_part_ids_with_policy, &new_part_ids_with_policy).into()
            },
            ..SchemaDiff::default()
        },
    )?;
    if !tidb_metadef::is_mem_or_sys_db(&schema.go_to_lower()) {
        append_schema_change_mutations(txn, active.job.id, &[(
                -1,
                SchemaChangeEvent::truncate_table(info.clone_like_go(), stored.clone_like_go()),
            )])?;
    }
    active.job.finish_table_job(
        JobState::DONE,
        SchemaState::PUBLIC,
        schema_version,
        Some(GoShared::new(info.clone_like_go())),
    );
    // See `truncate_table_by_reassign_partition_ids` for why they might
    // change.
    {
        let args = args.write();
        args.old_partition_ids
            .set(GoSharedSlice::from_vec(old_partition_ids));
        args.new_partition_ids
            .set(GoSharedSlice::from_vec(new_partition_ids));
    }
    active.job.fill_finished_args(Some(args));
    let mut write = job_only_write(active.job.id, Vec::new());
    write.schema_version = schema_version;
    write.diff = diff;
    write.created_id = Some(new_table_id);
    write.placement_bundles = placement_bundles;
    write.label_rules = label_rules.steps;
    Ok(PersistedDdlActionStep {
        write,
        // Go `needUpdateRawArgs`: history and the delete range read these.
        update_raw_args: true,
        run_error: None,
        reorg: None,
    })
}

/// Go `truncateTableByReassignPartitionIDs`: the partitions take `pids`,
/// drawing more when the table has gained partitions since the job was
/// queued (a job from a version that gave no ids, or one an ADD or
/// REORGANIZE PARTITION overtook).
fn truncate_table_by_reassign_partition_ids(
    txn: &mut JobTxn<'_>,
    info: &TableInfo,
    mut pids: Vec<i64>,
) -> Result<Vec<i64>, DdlPlanError> {
    let partition = info.partition.as_ref().expect("a partitioned table");
    let count = partition.read().definitions.len();
    if pids.len() < count {
        let meta = txn.meta();
        let more = meta
            .gen_global_ids((count - pids.len()) as i32)
            .map_err(|error| job_meta_error(&meta, error))?;
        pids.extend(more);
    }
    let partition = partition.read();
    for (ordinal, id) in pids.iter().take(count).enumerate() {
        partition
            .definitions
            .update(ordinal, |definition| definition.id = *id);
    }
    Ok(pids)
}

/// Go `cleanupTruncateTableAffinityGroupsAfterCommit`: once a TRUNCATE TABLE
/// job has committed as done, the replaced table's groups go. Best effort:
/// a failure leaves an unused group on the old table id.
pub(super) fn cleanup_truncate_table_affinity_groups_after_commit(job: &mut tidb_model::Job) {
    let Some(new_info) = job
        .binlog_info
        .as_ref()
        .and_then(|binlog| binlog.read().table_info.clone())
        .map(|info| info.read().clone_like_go())
    else {
        return;
    };
    if new_info.affinity.is_none() {
        return;
    }
    let args = match tidb_model::get_finished_truncate_table_args(job) {
        Ok(Some(args)) => args,
        Ok(None) => return,
        Err(error) => {
            tidb_ddl_logutil::ddl_logger().warn(
                "failed to decode truncate table args for post-commit affinity cleanup",
                &[
                    tidb_log::Field::new("jobID", tidb_log::Value::I64(job.id)),
                    tidb_log::Field::new("error", tidb_log::Value::Str(error.to_string())),
                ],
            );
            return;
        }
    };
    // TRUNCATE TABLE keeps the affinity level, and the job's table id is the
    // old table's.
    let old_info = TableInfo {
        id: job.table_id,
        name: new_info.name.clone(),
        affinity: new_info.affinity.clone(),
        ..TableInfo::default()
    };
    let old_definitions =
        crate::ddl_affinity::partition_definitions_from_ids(&args.read().old_partition_ids.get().snapshot());
    if let Err(error) =
        crate::ddl_affinity::delete_table_affinity_groups_with_timeout(&old_info, Some(&old_definitions))
    {
        tidb_ddl_logutil::ddl_logger().warn(
            "failed to delete old affinity groups from PD",
            &[
                tidb_log::Field::new("error", tidb_log::Value::Str(error)),
                tidb_log::Field::new("tableID", tidb_log::Value::I64(old_info.id)),
            ],
        );
    }
}
