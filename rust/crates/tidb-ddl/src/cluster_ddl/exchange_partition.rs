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

//! EXCHANGE PARTITION as Go runs it: the executor's
//! `ExchangeTablePartition` checks the two tables and queues one job on the
//! standalone table; the owner's `onExchangeTablePartition` first marks the
//! standalone table (`ExchangePartitionInfo`, a schema version during which
//! its writes must fit the partition), then, after the optional row
//! validation, swaps the partition's and the table's physical ids.

use super::add_index::{action_step, AddIndexReorg, AddIndexSubmission, ReorgOutcome};
use super::add_index::{ReorgResult, ReorgStageRequest};
use super::*;

/// Go's executor halves up to `doDDLJob2`. `None` for any other statement.
pub fn prepare_exchange_partition_job_submission<S: MetaSnapshot>(
    snapshot: &mut S,
    statement: &DdlStatement,
    job_context: &dyn DdlSessionContext,
    start_ts: u64,
    upgrading: bool,
    min_job_id: i64,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    let DdlStatement::ExchangePartition {
        schema,
        table,
        partition,
        standalone_schema,
        standalone_table,
        with_validation,
        ..
    } = statement
    else {
        return Ok(None);
    };
    let catalog = load_cluster_catalog(snapshot)?;
    let (partitioned_db_id, partitioned) = locate_table(&catalog, schema, table)?;
    let (standalone_db_id, standalone) =
        locate_table(&catalog, standalone_schema, standalone_table)?;
    check_exchange_tables(partitioned, standalone)?;
    let partition_name = partition.go_to_lower();
    // Go `tables.FindPartitionByName`.
    let partition_id = partitioned
        .get_partition_info()
        .and_then(|info| {
            info.read()
                .definitions
                .snapshot()
                .into_iter()
                .find(|definition| definition.name.lowercase() == partition_name)
                .map(|definition| definition.id)
        })
        .ok_or_else(|| {
            exchange_refusal(
                &tidb_error::catalog::table::ERR_UNKNOWN_PARTITION,
                format!(
                    "Unknown partition '{partition_name}' in table '{}'",
                    partitioned.name.original()
                ),
            )
        })?;
    let mut job = session_job(job_context, ActionType::ACTION_EXCHANGE_TABLE_PARTITION);
    job.schema_id = standalone_db_id;
    job.table_id = standalone.id;
    job.schema_name = standalone_schema.go_to_lower().into();
    job.table_name = standalone.name.lowercase().to_owned().into();
    job.involving_schema_info = GoSharedSlice::from_vec(vec![
        tidb_model::InvolvingSchemaInfo {
            database: schema.go_to_lower().into(),
            table: partitioned.name.lowercase().to_owned().into(),
            ..tidb_model::InvolvingSchemaInfo::default()
        },
        tidb_model::InvolvingSchemaInfo {
            database: job.schema_name.clone(),
            table: job.table_name.clone(),
            ..tidb_model::InvolvingSchemaInfo::default()
        },
    ]);
    let args = tidb_model::ExchangeTablePartitionArgs {
        partition_id: GoField::new(partition_id),
        partitioned_table_schema_id: GoField::new(partitioned_db_id),
        partitioned_table_id: GoField::new(partitioned.id),
        partition_name: GoField::new(partition_name.into()),
        with_validation: GoField::new(*with_validation),
    };
    let built = AddIndexSubmission::Job(
        Box::new(crate::ddl_job_submit::JobSpec {
            job,
            args: JobArgsValue::ExchangeTablePartition(Some(GoShared::new(args))),
            id_allocated: true,
        }),
        // Go appends it once the job succeeded.
        vec![tidb_context::SqlWarn::with_code(
            DdlWarningLevel::Warning,
            1105,
            "after the exchange, please analyze related table of the exchange to update statistics"
                .to_owned(),
        )],
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

struct ExchangeArgs {
    partition_id: i64,
    pt_schema_id: i64,
    pt_table_id: i64,
    partition_name: String,
    with_validation: bool,
}

fn exchange_args(
    active: &mut crate::ddl_job_table::ActiveDdlJob,
) -> Result<
    (
        GoShared<tidb_model::ExchangeTablePartitionArgs>,
        ExchangeArgs,
    ),
    DdlPlanError,
> {
    let args = tidb_model::get_exchange_table_partition_args(&mut active.job)
        .map_err(|error| cancel_ddl_job(active, DdlPlanError::Encode(error.to_string())))?
        .ok_or_else(|| {
            cancel_ddl_job(
                active,
                DdlPlanError::Encode("EXCHANGE PARTITION has nil args".to_owned()),
            )
        })?;
    let read = {
        let args = args.read();
        ExchangeArgs {
            partition_id: args.partition_id.get(),
            pt_schema_id: args.partitioned_table_schema_id.get(),
            pt_table_id: args.partitioned_table_id.get(),
            partition_name: args.partition_name.get().to_string(),
            with_validation: args.with_validation.get(),
        }
    };
    Ok((args, read))
}

/// Go `checkExchangePartitionPlacementPolicy`: the standalone table's policy
/// and the partition's effective one (its own, else the table's) must both be
/// unset or name the same policy.
fn check_exchange_partition_placement_policy<S: MetaSnapshot>(
    snapshot: &mut S,
    standalone: Option<&GoShared<tidb_model::PolicyRefInfo>>,
    partitioned: Option<&GoShared<tidb_model::PolicyRefInfo>>,
    partition: Option<&GoShared<tidb_model::PolicyRefInfo>>,
) -> Result<(), DdlPlanError> {
    let different = || {
        exchange_refusal(
            &tidb_dbterror::ERR_TABLES_DIFFERENT_METADATA,
            "Tables have different definitions",
        )
    };
    let partition = partition.or(partitioned);
    let (standalone, partition) = match (standalone, partition) {
        (None, None) => return Ok(()),
        (Some(standalone), Some(partition)) => (standalone, partition),
        _ => return Err(different()),
    };
    let policies = load_policies(snapshot)?;
    let name = |reference: &GoShared<tidb_model::PolicyRefInfo>| {
        let id = reference.read().id;
        policies
            .policies
            .iter()
            .find(|policy| policy.id == id)
            .map(|policy| policy.name.lowercase().to_owned())
    };
    match (name(standalone), name(partition)) {
        (None, None) => Ok(()),
        (Some(left), Some(right)) if left == right => Ok(()),
        _ => Err(different()),
    }
}

/// The schema diff of an exchange step (Go
/// `SetSchemaDiffForExchangeTablePartition`).
fn exchange_diff(
    active: &crate::ddl_job_table::ActiveDdlJob,
    args: &ExchangeArgs,
    partitioned_written: bool,
    diff: &mut SchemaDiff,
) {
    diff.old_table_id = active.job.table_id;
    diff.old_schema_id = active.job.schema_id;
    let mut affected = tidb_model::AffectedOption {
        table_id: args.pt_table_id,
        ..tidb_model::AffectedOption::default()
    };
    if active.job.state == JobState::ROLLBACK_DONE || active.job.schema_state != SchemaState::PUBLIC
    {
        // Only the standalone table's ExchangePartitionInfo changed.
        diff.table_id = active.job.table_id;
        affected.schema_id = active.job.schema_id;
        // The partitioned table is reloaded too when it was written.
        if partitioned_written {
            affected.old_schema_id = args.pt_schema_id;
        }
    } else {
        diff.table_id = args.partition_id;
        affected.schema_id = args.pt_schema_id;
    }
    diff.affected_options = vec![affected].into();
}

/// Go `updateVersionAndTableInfo` over the standalone table and, when
/// given, the partitioned one (`multiInfos`).
fn exchange_step(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    args: &ExchangeArgs,
    standalone: &TableInfo,
    partitioned: Option<&TableInfo>,
    start_ts: u64,
) -> Result<DdlWrite, DdlPlanError> {
    let mut standalone = standalone.clone_like_go();
    standalone.update_ts = start_ts;
    let mut mutations = vec![BufferMutation::set(
        key::table_kv_key(active.job.schema_id, standalone.id),
        value::serialize_table_info(&standalone)
            .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
    )?];
    if let Some(partitioned) = partitioned {
        let mut partitioned = partitioned.clone_like_go();
        partitioned.update_ts = start_ts;
        mutations.push(BufferMutation::set(
            key::table_kv_key(args.pt_schema_id, partitioned.id),
            value::serialize_table_info(&partitioned)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
        )?);
    }
    let schema_version = catalog.schema_version + 1;
    let mut diff = SchemaDiff {
        version: schema_version,
        action_type: active.job.type_,
        schema_id: active.job.schema_id,
        ..SchemaDiff::default()
    };
    exchange_diff(active, args, partitioned.is_some(), &mut diff);
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
    let mut write = job_only_write(active.job.id, mutations);
    write.schema_version = schema_version;
    write.diff = diff;
    Ok(write)
}

/// Go `onExchangeTablePartition`.
pub(super) fn plan_persisted_exchange_partition_job_step<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
    reorg_outcome: Option<&ReorgOutcome>,
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let (raw_args, args) = exchange_args(active)?;
    // `checkSchemaExistAndCancelNotExistJob` and the partitioned table's
    // database.
    if !catalog
        .databases
        .iter()
        .any(|database| database.info.id == args.pt_schema_id)
    {
        return Err(cancel_ddl_job(
            active,
            DdlPlanError::from(
                tidb_dbterror::CLASS_SCHEMA
                    .new_std(tidb_error::tidb::errcode::ErrBadDB)
                    .generate(format!(
                        "Unknown database '(Schema ID {})'",
                        args.pt_schema_id
                    )),
            ),
        ));
    }
    let mut standalone = checked_job_table(catalog, active, true)?.clone_like_go();
    if active.job.is_rollingback() {
        return rollback_exchange_table_partition(catalog, active, &args, standalone, start_ts);
    }
    let Some(partitioned) = stored_table_by_ids(catalog, args.pt_schema_id, args.pt_table_id)
    else {
        return Err(cancel_ddl_job(
            active,
            DdlPlanError::from(
                tidb_dbterror::CLASS_SCHEMA
                    .new_std(tidb_error::tidb::errcode::ErrNoSuchTable)
                    .generate(format!(
                        "Table '(Schema ID {}).(Table ID {})' doesn't exist",
                        args.pt_schema_id, args.pt_table_id
                    )),
            ),
        ));
    };
    let mut partitioned = partitioned.clone_like_go();
    // Go `getPartitionDef`.
    let Some(definition) = partitioned.get_partition_info().and_then(|info| {
        info.read()
            .definitions
            .snapshot()
            .into_iter()
            .find(|definition| definition.name.lowercase() == args.partition_name)
    }) else {
        return Err(exchange_refusal(
            &tidb_error::catalog::table::ERR_UNKNOWN_PARTITION,
            format!(
                "Unknown partition '{}' in table '{}'",
                args.partition_name,
                partitioned.name.original()
            ),
        ));
    };
    if active.job.schema_state == SchemaState::NONE {
        if partitioned.state != SchemaState::PUBLIC {
            return Err(cancel_ddl_job(
                active,
                DdlPlanError::from(tidb_dbterror::ERR_INVALID_DDL_STATE.generate(format!(
                    "table {} is not in public, but {}",
                    partitioned.name, partitioned.state
                ))),
            ));
        }
        check_exchange_tables(&partitioned, &standalone)
            .and_then(|()| {
                check_exchange_partition_placement_policy(
                    snapshot,
                    standalone.placement_policy_ref.as_ref(),
                    partitioned.placement_policy_ref.as_ref(),
                    definition.placement_policy_ref.as_ref(),
                )
            })
            .map_err(|error| cancel_ddl_job(active, error))?;
        let mut args = args;
        if args.partition_id != definition.id {
            args.partition_id = definition.id;
            raw_args.read().partition_id.set(definition.id);
            active.job.fill_args(Some(raw_args));
        }
        // The partitioned table carries the info only when the standalone
        // table has CHECK constraints its rows must meet.
        let constrained = !standalone.constraints.is_empty();
        if constrained {
            partitioned.exchange_partition_info =
                Some(GoShared::new(tidb_model::table::ExchangePartitionInfo {
                    exchange_partition_table_id: standalone.id,
                    exchange_partition_def_id: args.partition_id,
                    ..tidb_model::table::ExchangePartitionInfo::default()
                }));
        }
        standalone.exchange_partition_info =
            Some(GoShared::new(tidb_model::table::ExchangePartitionInfo {
                exchange_partition_table_id: args.pt_table_id,
                exchange_partition_def_id: args.partition_id,
                ..tidb_model::table::ExchangePartitionInfo::default()
            }));
        active.job.schema_state = SchemaState::WRITE_ONLY;
        if let Some(partition) = &partitioned.partition {
            let mut partition = partition.write();
            partition.ddl_state = SchemaState::WRITE_ONLY;
            partition.ddl_action = active.job.type_;
        }
        // `updateVersionAndTableInfoWithCheck`.
        check_table_info_valid(&standalone)?;
        if constrained {
            check_table_info_valid(&partitioned)?;
        }
        let write = exchange_step(
            catalog,
            active,
            &args,
            &standalone,
            constrained.then_some(&partitioned),
            start_ts,
        )?;
        return Ok(action_step(write));
    }
    // The standalone table restricts its writes from here on: a failure
    // rolls that back rather than cancelling.
    let mut args = args;
    if args.partition_id != definition.id {
        args.partition_id = definition.id;
        raw_args.read().partition_id.set(definition.id);
        active.job.fill_args(Some(raw_args));
    }
    if args.with_validation {
        // Go `checkExchangePartitionRecordValidation`, run by the owner
        // between steps.
        let outcome = reorg_outcome.filter(|outcome| outcome.job_id == active.job.id);
        match outcome.map(|outcome| &outcome.result) {
            None => {
                let check = ExchangePartitionValidation {
                    partitioned: Box::new(partitioned.clone_like_go()),
                    standalone: Box::new(standalone.clone_like_go()),
                    partition_id: args.partition_id,
                    check_constraints: tidb_vardef::ENABLE_CHECK_CONSTRAINT
                        .load(std::sync::atomic::Ordering::Relaxed),
                };
                return Ok(PersistedDdlActionStep {
                    write: job_only_write(active.job.id, Vec::new()),
                    update_raw_args: false,
                    run_error: None,
                    reorg: Some(AddIndexReorg {
                        job_id: active.job.id,
                        stage: ReorgStageRequest::CheckExchangePartition(Box::new(check)),
                        table: Box::new(standalone.clone_like_go()),
                        index_ids: Vec::new(),
                        merging: false,
                        physical_ids: Vec::new(),
                        batch_size: 0,
                        session_vars: tidb_ddl_build::session_pool::ReorgSessionVars::of_job(&active.job),
                        row_count: active.job.row_count,
                        column: None,
                        sub_job: None,
                        clean_up_global_index: false,
                        partition_phase: None,
                        schema_id: active.job.schema_id,
                    }),
                });
            }
            Some(ReorgResult::Checked(Some((code, message)))) => {
                active.job.state = JobState::ROLLINGBACK;
                return Ok(PersistedDdlActionStep {
                    write: job_only_write(active.job.id, Vec::new()),
                    update_raw_args: false,
                    run_error: Some(checked_refusal(*code, message.clone())),
                    reorg: None,
                });
            }
            Some(ReorgResult::Checked(None)) => {}
            Some(_) => {
                return Err(DdlPlanError::Encode(
                    "an exchange validation got a reorganization's outcome".to_owned(),
                ))
            }
        }
    }
    let database_id = active.job.schema_id;
    let original_standalone_id = standalone.id;
    let original_partition_id = definition.id;
    // Both tables take the larger of their auto-ID counters.
    let mut mutations = Vec::new();
    for key_for in [
        key::auto_table_id_kv_key as fn(i64, i64) -> Vec<u8>,
        key::auto_increment_id_kv_key,
        key::auto_random_table_id_kv_key,
    ] {
        let partitioned_key = key_for(args.pt_schema_id, partitioned.id);
        let maximum = std::cmp::max(
            read_auto_id(snapshot, &partitioned_key)?,
            read_auto_id(snapshot, &key_for(database_id, original_standalone_id))?,
        );
        mutations.push(BufferMutation::set(
            partitioned_key,
            value::encode_int_value(maximum),
        )?);
        mutations.push(BufferMutation::set(
            key_for(database_id, original_partition_id),
            value::encode_int_value(maximum),
        )?);
    }
    if let Some(replica) = &partitioned.tiflash_replica {
        let mut replica = replica.write();
        let mut ids = replica
            .available_partition_ids
            .iter()
            .copied()
            .collect::<Vec<_>>();
        if let Some(id) = ids.iter_mut().find(|id| **id == original_partition_id) {
            *id = original_standalone_id;
        }
        replica.available_partition_ids = ids.into();
    }
    // `DropTableOrView` of the standalone table under its old id.
    mutations.push(BufferMutation::delete(key::table_kv_key(
        database_id,
        original_standalone_id,
    ))?);
    let original_standalone = standalone.clone_like_go();
    partitioned.exchange_partition_info = None;
    let exchanged_definition = {
        let partition = partitioned
            .partition
            .as_ref()
            .expect("checked partitioned above");
        let mut partition = partition.write();
        let mut definitions = partition.definitions.snapshot();
        let exchanged = definitions
            .iter_mut()
            .find(|definition| definition.id == original_partition_id)
            .expect("the partition was found above");
        exchanged.id = original_standalone_id;
        let exchanged = exchanged.clone_like_go();
        partition.definitions = definitions.into();
        partition.ddl_state = SchemaState::NONE;
        partition.ddl_action = ActionType::ACTION_NONE;
        exchanged
    };
    standalone.id = original_partition_id;
    standalone.exchange_partition_info = None;
    partitioned.update_ts = start_ts;
    mutations.push(BufferMutation::set(
        key::table_kv_key(args.pt_schema_id, partitioned.id),
        value::serialize_table_info(&partitioned)
            .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
    )?);
    // `bundlesForExchangeTablePartition`: the table, the exchanged
    // partition and the standalone table under their new ids; the side that
    // lost a policy is cleared.
    let policies = load_policies(snapshot)?;
    let placement = |error: tidb_placement::PlacementError| DdlPlanError::Encode(error.to_string());
    let mut bundles = Vec::new();
    bundles.extend(tidb_placement::new_table_bundle(&policies, &partitioned).map_err(placement)?);
    let partition_bundle = tidb_placement::new_partition_bundle(&policies, &exchanged_definition)
        .map_err(placement)?;
    let standalone_bundle =
        tidb_placement::new_table_bundle(&policies, &standalone).map_err(placement)?;
    bundles.extend(partition_bundle.clone());
    bundles.extend(standalone_bundle.clone());
    if partition_bundle.is_none() && standalone_bundle.is_some() {
        bundles.push(tidb_placement::new_bundle(exchanged_definition.id));
    }
    if partition_bundle.is_some() && standalone_bundle.is_none() {
        bundles.push(tidb_placement::new_bundle(standalone.id));
    }
    // The standalone table's label rule moves to the partition and the
    // partition's to the table.
    let mut label_rules = LabelRuleSteps::new();
    let partitioned_schema = catalog
        .databases
        .iter()
        .find(|database| database.info.id == args.pt_schema_id)
        .map(|database| database.info.name.lowercase().to_owned())
        .unwrap_or_default();
    let swap = ExchangePartitionLabelSwap {
        partitioned_schema,
        partitioned_table: partitioned.name.lowercase().to_owned(),
        partition: exchanged_definition.name.lowercase().to_owned(),
        standalone_schema: active.job.schema_name.to_string(),
        standalone_table: standalone.name.lowercase().to_owned(),
        partition_id: original_standalone_id,
        standalone_id: original_partition_id,
    };
    let label_write = swap
        .label_write(label_rules.manager())
        .map_err(|error| pd_step_error(Some("failed to get PD the label rules"), error))?;
    label_rules.push(label_write, Some(LABEL_RULES_NOTIFY_ERROR))?;
    active.job.schema_state = SchemaState::PUBLIC;
    check_table_info_valid(&standalone)?;
    let mut write = exchange_step(catalog, active, &args, &standalone, None, start_ts)?;
    mutations.append(&mut write.mutations);
    write.mutations = mutations;
    if !tidb_metadef::is_mem_or_sys_db(&active.job.schema_name.to_string()) {
        append_schema_change_mutations(
            catalog,
            active.job.id,
            &[(
                -1,
                SchemaChangeEvent::exchange_partition(
                    partitioned.clone_like_go(),
                    event_partition_info(vec![definition]),
                    original_standalone,
                ),
            )],
            &mut write.mutations,
            row_ids,
        )?;
    }
    active.job.finish_table_job(
        JobState::DONE,
        SchemaState::NONE,
        write.schema_version,
        Some(GoShared::new(partitioned)),
    );
    write.placement_bundles = bundles;
    write.label_rules = label_rules.steps;
    Ok(action_step(write))
}

/// Go `rollbackExchangeTablePartition`: the standalone table (and the
/// partitioned one, if it was marked) loses its `ExchangePartitionInfo`.
fn rollback_exchange_table_partition(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    args: &ExchangeArgs,
    mut standalone: TableInfo,
    start_ts: u64,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    standalone.exchange_partition_info = None;
    active.job.state = JobState::ROLLBACK_DONE;
    active.job.schema_state = SchemaState::PUBLIC;
    let partitioned = if standalone.constraints.is_empty() {
        None
    } else {
        let mut partitioned = stored_table_by_ids(catalog, args.pt_schema_id, args.pt_table_id)
            .ok_or_else(|| {
                DdlPlanError::from(
                    tidb_dbterror::CLASS_SCHEMA
                        .new_std(tidb_error::tidb::errcode::ErrNoSuchTable)
                        .generate(format!(
                            "Table '(Schema ID {}).(Table ID {})' doesn't exist",
                            args.pt_schema_id, args.pt_table_id
                        )),
                )
            })?
            .clone_like_go();
        partitioned.exchange_partition_info = None;
        Some(partitioned)
    };
    let write = exchange_step(
        catalog,
        active,
        args,
        &standalone,
        partitioned.as_ref(),
        start_ts,
    )?;
    Ok(action_step(write))
}

/// Go `rollingbackExchangeTablePartition`.
pub(super) fn plan_cancel_exchange_partition(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    if active.job.schema_state == SchemaState::NONE {
        return Err(cancelled_ddl_job(active));
    }
    let (_, args) = exchange_args(active)?;
    let standalone = checked_job_table(catalog, active, true)?.clone_like_go();
    rollback_exchange_table_partition(catalog, active, &args, standalone, start_ts)
}
