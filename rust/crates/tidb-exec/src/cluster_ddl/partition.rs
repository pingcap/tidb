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

//! ADD, DROP and TRUNCATE PARTITION as Go runs them: the session's executor
//! (`AddTablePartitions`, `DropTablePartition`, `TruncateTablePartition`)
//! checks the statement and queues one job; the owner's
//! `onAddTablePartition` (none -> replica-only -> public),
//! `onDropTablePartition` and `onTruncateTablePartition` (public ->
//! write-only -> delete-only -> delete-reorganization -> none, the last
//! cleaning the global indexes of the old partitions' rows) step it, and the
//! old partitions' rows are left to the delete range.
//!
//! An ADD on a HASH or KEY table is a reorganization in Go
//! (`hashPartitionManagement`); it is not queued here.

use super::add_index::{
    action_step, job_batch_size, load_reorg_handle, reorg_statement_context, reorg_step,
    reorg_sub_job, table_step, table_step_with_diff, AddIndexReorg, AddIndexSubmission,
    ReorgOutcome, ReorgResult, ReorgStageRequest,
};
use super::*;

/// Go `mysql.PartitionCountLimit`.
const PARTITION_COUNT_LIMIT: usize = 8192;

/// A refusal the shared partition implementation raised.
fn admission(error: tidb_executor::DriverError) -> DdlPlanError {
    let error = error.to_mysql_error();
    DdlPlanError::Admission(DdlAdmissionError::from_mysql_error(error))
}

/// The notes the shared partition implementation left on `context`.
fn partition_notes(context: &tidb_executor::StmtContext) -> Vec<(DdlWarningLevel, u16, String)> {
    context
        .take_warnings()
        .into_iter()
        .map(|(level, code, message)| {
            let level = match level {
                tidb_distsql::WarningLevel::Note => DdlWarningLevel::Note,
                _ => DdlWarningLevel::Warning,
            };
            (level, code, message)
        })
        .collect()
}

/// The job every partition statement starts from, its query rewritten as
/// Go's FIRST / LAST PARTITION sugar rewrites `QueryString`.
fn partition_job(
    job_context: &tidb_executor::StmtContext,
    action: ActionType,
    database_id: i64,
    schema: &str,
    stored: &TableInfo,
    rewrite: Option<&(String, String)>,
) -> Job {
    let mut job = session_job(job_context, action);
    job.schema_id = database_id;
    job.table_id = stored.id;
    job.schema_name = schema.go_to_lower().into();
    job.table_name = stored.name.lowercase().to_owned().into();
    job.query =
        tidb_executor::ddl::job_history::rewritten_query(job_context.ddl_query(), rewrite).into();
    job
}

/// Go's FIRST / LAST PARTITION sugar rewrite of `QueryString`: the clause
/// as the session wrote it (the shared implementation saw the restored
/// statement, whose text the query does not contain) becomes what the shared
/// implementation generated.
fn interval_query_rewrite(
    action: &tidb_ast::AlterPartitionAction,
    context: &tidb_executor::StmtContext,
) -> Option<(String, String)> {
    let bound = match action {
        tidb_ast::AlterPartitionAction::LastPartitionLessThan { expr, .. }
        | tidb_ast::AlterPartitionAction::FirstPartitionLessThan { expr, .. } => expr,
        _ => return None,
    };
    let (_, replacement) = context.ddl_query_rewrite()?;
    Some((
        String::from_utf8_lossy(bound.original_text()).into_owned(),
        replacement,
    ))
}

/// Go's `job.AddSystemVars(vardef.TiDBScatterRegion, ...)`.
fn scatter_region_vars(job: &mut Job, job_context: &tidb_executor::StmtContext) {
    job.session_vars = Some(GoShared::new(std::collections::BTreeMap::from([(
        tidb_vardef::tidb_vars::TIDB_SCATTER_REGION.to_owned(),
        job_context.ddl_reorg_vars().scatter_region.clone().into(),
    )])));
}

/// Go's executor halves up to `doDDLJob2`. `None` for any other statement,
/// and for an ADD on a HASH or KEY table, which Go reorganizes.
pub fn prepare_partition_job_submission<S: MetaSnapshot>(
    snapshot: &mut S,
    statement: &DdlStatement,
    job_context: &tidb_executor::StmtContext,
    start_ts: u64,
    upgrading: bool,
    min_job_id: i64,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    let catalog = load_cluster_catalog(snapshot)?;
    let built = match statement {
        DdlStatement::AddPartitions {
            schema,
            table,
            sql,
            action,
            ..
        } => {
            let (database_id, stored) = locate_table(&catalog, schema, table)?;
            let Some(partitioning) = stored.get_partition_info() else {
                return Err(admission(
                    tidb_executor::DriverError::PartitionManagementOnNonpartitioned,
                ));
            };
            let partitioning = partitioning.read().clone_like_go();
            // Go `AddTablePartitions` refuses a table with AFFINITY before
            // it hands a HASH/KEY table to `hashPartitionManagement`.
            if stored.affinity.is_none()
                && matches!(
                    partitioning.partition_type,
                    tidb_ast::PartitionType::HASH | tidb_ast::PartitionType::KEY
                )
            {
                return Ok(None);
            }
            // `BuildAddedPartitionInfo` and `CheckAndUpdateAddedPartitionDefinitions`.
            let context = tidb_executor::StmtContext::for_query();
            let transformed = apply_partition_change(stored, schema, table, sql, &context)?;
            let notes = partition_notes(&context);
            let old_names = partitioning
                .definitions
                .snapshot()
                .iter()
                .map(|definition| definition.name.lowercase().to_owned())
                .collect::<std::collections::HashSet<_>>();
            let added = transformed
                .definitions
                .into_iter()
                .filter(|definition| !old_names.contains(&definition.name.go_to_lower()))
                .map(|definition| model_partition_definition(definition, 0))
                .collect::<Vec<_>>();
            // `IF NOT EXISTS` over an existing name: Go's note, no job.
            if added.is_empty() {
                return Ok(Some(AddIndexSubmission::AlreadySatisfied {
                    detail: format!("partitions of `{schema}`.`{table}` already exist"),
                    warnings: notes,
                }));
            }
            let part_info = tidb_model::partition::PartitionInfo {
                partition_type: partitioning.partition_type,
                expr: partitioning.expr.clone(),
                columns: partitioning.columns.clone(),
                enable: partitioning.enable,
                num: added.len() as u64,
                definitions: added.into(),
                ..tidb_model::partition::PartitionInfo::default()
            };
            let mut job = partition_job(
                job_context,
                ActionType::ACTION_ADD_TABLE_PARTITION,
                database_id,
                schema,
                stored,
                interval_query_rewrite(action, &context).as_ref(),
            );
            scatter_region_vars(&mut job, job_context);
            let args = tidb_model::TablePartitionArgs {
                part_info: GoField::new(Some(GoShared::new(part_info))),
                ..tidb_model::TablePartitionArgs::default()
            };
            AddIndexSubmission::Job(
                Box::new(crate::ddl_job_submit::JobSpec {
                    job,
                    args: JobArgsValue::TablePartition(Some(GoShared::new(args))),
                    // `assignGIDsForJobs` draws the new partitions' ids.
                    id_allocated: false,
                }),
                notes,
            )
        }
        DdlStatement::DropPartitions {
            schema,
            table,
            sql,
            action,
        } => {
            let (database_id, stored) = locate_table(&catalog, schema, table)?;
            // `CheckDropTablePartition`, and FIRST PARTITION's generated
            // names.
            let context = tidb_executor::StmtContext::for_query();
            let transformed = apply_partition_change(stored, schema, table, sql, &context)?;
            let notes = partition_notes(&context);
            let remaining = transformed
                .definitions
                .iter()
                .map(|definition| definition.name.go_to_lower())
                .collect::<std::collections::HashSet<_>>();
            let dropped = stored
                .get_partition_info()
                .map(|partitioning| partitioning.read().definitions.snapshot())
                .unwrap_or_default()
                .into_iter()
                .map(|definition| definition.name.lowercase().to_owned())
                .filter(|name| !remaining.contains(name))
                .collect::<Vec<_>>();
            // `IF EXISTS` over a missing partition: Go's note, no job.
            if dropped.is_empty() {
                return Ok(Some(AddIndexSubmission::AlreadySatisfied {
                    detail: format!("no partition of `{schema}`.`{table}` to drop"),
                    warnings: notes,
                }));
            }
            // Go's `PartNames`: the names as written, in statement order.
            let part_names = match action.as_ref() {
                tidb_ast::AlterPartitionAction::Drop { names, .. } => {
                    names.iter().map(|name| name.go_to_lower()).collect()
                }
                _ => dropped,
            };
            let mut job = partition_job(
                job_context,
                ActionType::ACTION_DROP_TABLE_PARTITION,
                database_id,
                schema,
                stored,
                interval_query_rewrite(action, &context).as_ref(),
            );
            job.schema_state = SchemaState::PUBLIC;
            let args = tidb_model::TablePartitionArgs {
                part_names: GoField::new(GoSharedSlice::from_vec(
                    part_names.into_iter().map(Into::into).collect(),
                )),
                ..tidb_model::TablePartitionArgs::default()
            };
            AddIndexSubmission::Job(
                Box::new(crate::ddl_job_submit::JobSpec {
                    job,
                    args: JobArgsValue::TablePartition(Some(GoShared::new(args))),
                    id_allocated: true,
                }),
                notes,
            )
        }
        DdlStatement::TruncatePartitions {
            schema,
            table,
            action,
            ..
        } => {
            let (database_id, stored) = locate_table(&catalog, schema, table)?;
            let Some(partitioning) = stored.get_partition_info() else {
                return Err(admission(
                    tidb_executor::DriverError::PartitionManagementOnNonpartitioned,
                ));
            };
            let definitions = partitioning.read().definitions.snapshot();
            // Go's `getTruncatedParts`: ALL, or each name once in statement
            // order (MySQL allows a name twice).
            let old_partition_ids = match action.as_ref() {
                tidb_ast::AlterPartitionAction::Truncate { all: true, .. } => definitions
                    .iter()
                    .map(|definition| definition.id)
                    .collect::<Vec<_>>(),
                tidb_ast::AlterPartitionAction::Truncate { names, .. } => {
                    let mut positions = Vec::new();
                    for name in names {
                        let lower = name.go_to_lower();
                        let Some(position) = definitions
                            .iter()
                            .position(|definition| definition.name.lowercase() == lower)
                        else {
                            return Err(admission(tidb_executor::DriverError::UnknownPartition {
                                partition: lower,
                                table: table.clone(),
                            }));
                        };
                        if !positions.contains(&position) {
                            positions.push(position);
                        }
                    }
                    positions
                        .into_iter()
                        .map(|position| definitions[position].id)
                        .collect()
                }
                other => unreachable!("lowered only for TRUNCATE PARTITION, got {other:?}"),
            };
            let mut job = partition_job(
                job_context,
                ActionType::ACTION_TRUNCATE_TABLE_PARTITION,
                database_id,
                schema,
                stored,
                None,
            );
            job.schema_state = SchemaState::PUBLIC;
            scatter_region_vars(&mut job, job_context);
            let args = tidb_model::TruncateTableArgs {
                old_partition_ids: GoField::new(GoSharedSlice::from_vec(old_partition_ids)),
                ..tidb_model::TruncateTableArgs::default()
            };
            AddIndexSubmission::Job(
                Box::new(crate::ddl_job_submit::JobSpec {
                    job,
                    args: JobArgsValue::TruncateTable(Some(GoShared::new(args))),
                    // `assignGIDsForJobs` draws the new partition ids.
                    id_allocated: false,
                }),
                Vec::new(),
            )
        }
        _ => return Ok(None),
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

fn table_partition_args(
    active: &mut crate::ddl_job_table::ActiveDdlJob,
) -> Result<GoShared<tidb_model::TablePartitionArgs>, DdlPlanError> {
    tidb_model::get_table_partition_args(&mut active.job)
        .map_err(|error| cancel_ddl_job(active, DdlPlanError::Encode(error.to_string())))?
        .ok_or_else(|| {
            cancel_ddl_job(
                active,
                DdlPlanError::Encode("partition job has nil args".to_owned()),
            )
        })
}

/// The table's partitioning, which a partition job's table must have.
fn job_partition(
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    info: &TableInfo,
) -> Result<GoShared<tidb_model::partition::PartitionInfo>, DdlPlanError> {
    info.partition.clone().ok_or_else(|| {
        cancel_ddl_job(
            active,
            admission(tidb_executor::DriverError::PartitionManagementOnNonpartitioned),
        )
    })
}

/// Go `dbterror.ErrInvalidDDLState.GenWithStackByArgs("partition", state)`.
fn invalid_partition_state(state: SchemaState) -> DdlPlanError {
    DdlPlanError::from(
        tidb_util::dbterror::ERR_INVALID_DDL_STATE.generate_with_stack_by_args(&[
            FormatArg::from("partition"),
            FormatArg::new(state.to_string(), state.0.to_string(), "model.SchemaState"),
        ]),
    )
}

/// The placement policies a bundle reads.
fn bundle_error(error: tidb_placement::PlacementError) -> DdlPlanError {
    DdlPlanError::Admission(DdlAdmissionError::plain(format!(
        "building placement rules: {error}"
    )))
}

/// Go `alterTablePartitionBundles`: the table's bundle over its partitions
/// and `add_definitions`, and each of those partitions' own.
pub(super) fn alter_table_partition_bundles<S: MetaSnapshot>(
    snapshot: &mut S,
    info: &TableInfo,
    add_definitions: Vec<tidb_model::partition::PartitionDefinition>,
) -> Result<Vec<tidb_placement::Bundle>, DdlPlanError> {
    let info = info.clone_like_go();
    let mut definitions = add_definitions;
    if let Some(partition) = &info.partition {
        let mut partition = partition.write();
        let existing = partition.definitions.snapshot();
        // A table being partitioned has its own id on its only partition: no
        // rule for that range both as table and partition.
        if existing.first().is_some_and(|first| first.id != info.id) {
            let mut all = existing;
            all.extend(definitions);
            definitions = all;
        }
        partition.definitions = definitions.clone().into();
    }
    let policies = load_policies(snapshot)?;
    let mut bundles = Vec::new();
    if let Some(bundle) =
        tidb_placement::new_table_bundle(&policies, &info).map_err(bundle_error)?
    {
        bundles.push(bundle);
    }
    bundles.extend(
        tidb_placement::new_partition_list_bundles(&policies, &definitions)
            .map_err(bundle_error)?,
    );
    Ok(bundles)
}

/// Go `droppedPartitionBundles`: a dropped partition keeps its placement,
/// its own or the table's, until GC deletes its range.
fn dropped_partition_bundles<S: MetaSnapshot>(
    snapshot: &mut S,
    info: &TableInfo,
    dropped: &[tidb_model::partition::PartitionDefinition],
) -> Result<Vec<tidb_placement::Bundle>, DdlPlanError> {
    let definitions = dropped
        .iter()
        .map(|definition| {
            let mut definition = definition.clone_like_go();
            if definition.placement_policy_ref.is_none() {
                definition.placement_policy_ref = info.placement_policy_ref.clone();
            }
            definition
        })
        .filter(|definition| definition.placement_policy_ref.is_some())
        .collect::<Vec<_>>();
    let policies = load_policies(snapshot)?;
    tidb_placement::new_partition_list_bundles(&policies, &definitions).map_err(bundle_error)
}

/// Go `placement.NewTableBundle` over the table as the step leaves it.
fn table_bundle<S: MetaSnapshot>(
    snapshot: &mut S,
    info: &TableInfo,
) -> Result<Option<tidb_placement::Bundle>, DdlPlanError> {
    let policies = load_policies(snapshot)?;
    tidb_placement::new_table_bundle(&policies, info).map_err(bundle_error)
}

/// Go `buildPlacementAffects`.
fn build_placement_affects(old_ids: &[i64], new_ids: &[i64]) -> Vec<tidb_model::AffectedOption> {
    old_ids
        .iter()
        .zip(new_ids)
        .map(|(old, new)| tidb_model::AffectedOption {
            old_table_id: *old,
            table_id: *new,
            ..tidb_model::AffectedOption::default()
        })
        .collect()
}

/// Go `removeTiFlashAvailablePartitionIDs`.
fn remove_tiflash_available_partition_ids(info: &TableInfo, ids: &[i64]) {
    if let Some(replica) = &info.tiflash_replica {
        let mut replica = replica.write();
        replica.available_partition_ids = replica
            .available_partition_ids
            .iter()
            .copied()
            .filter(|id| !ids.contains(id))
            .collect::<Vec<_>>()
            .into();
    }
}

/// Go `hasGlobalIndex`.
fn has_global_index(info: &TableInfo) -> bool {
    info.indices.iter_deref().any(|index| index.read().global)
}

/// Go `getTableInfoWithDroppingPartitions`: the table with the partitions
/// being dropped among its definitions again.
fn table_info_with_dropping_partitions(info: &TableInfo) -> TableInfo {
    let mut table = info.clone_like_go();
    if let Some(partition) = &table.partition {
        let mut partition = partition.read().clone_like_go();
        let mut definitions = partition.definitions.snapshot();
        definitions.extend(partition.dropping_definitions.snapshot());
        partition.definitions = definitions.into();
        partition.dropping_definitions = Vec::new().into();
        table.partition = Some(GoShared::new(partition));
    }
    table
}

/// Go `cleanGlobalIndexEntriesFromDroppedPartitions`: the global-index
/// entries of the old partitions' rows go, through a reorganization the
/// owner runs between steps. `None` once it is done; otherwise the step that
/// asks for (or records) the next part of it.
fn clean_global_index_entries_from_dropped_partitions<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    old_info: &TableInfo,
    old_ids: &[i64],
    outcome: Option<&ReorgOutcome>,
) -> Result<Option<PersistedDdlActionStep>, DdlPlanError> {
    let index_ids = old_info
        .indices
        .iter_deref()
        .filter_map(|index| {
            let index = index.read();
            index.global.then_some(index.id)
        })
        .collect::<Vec<_>>();
    if index_ids.is_empty() || old_ids.is_empty() {
        return Ok(None);
    }
    let outcome = outcome.filter(|outcome| outcome.job_id == active.job.id);
    let request =
        |active: &crate::ddl_job_table::ActiveDdlJob, stage: ReorgStageRequest| AddIndexReorg {
            job_id: active.job.id,
            stage,
            table: Box::new(old_info.clone_like_go()),
            index_ids: index_ids.clone(),
            merging: false,
            physical_ids: old_ids.to_vec(),
            batch_size: job_batch_size(&active.job),
            context: DdlStatementContext(reorg_statement_context(&active.job)),
            row_count: active.job.row_count,
            column: None,
            sub_job: reorg_sub_job(&active.job),
            clean_up_global_index: true,
            partition_phase: None,
            schema_id: active.job.schema_id,
        };
    match outcome.map(|outcome| &outcome.result) {
        // Go `getReorgInfoFromPartitions`: the first visit persists the
        // snapshot.
        None if active.job.snapshot_version == 0 => {
            let reorg = request(active, ReorgStageRequest::Init);
            Ok(Some(reorg_step(&active.job, reorg)))
        }
        Some(ReorgResult::Initialized(version)) => {
            active.job.snapshot_version = *version;
            Ok(Some(action_step(job_only_write(active.job.id, Vec::new()))))
        }
        None | Some(ReorgResult::Interrupted) => {
            let handle = load_reorg_handle(snapshot, catalog, active)?;
            let reorg = request(active, ReorgStageRequest::Run(handle));
            Ok(Some(reorg_step(&active.job, reorg)))
        }
        // `ErrWaitReorgTimeout`: the job keeps its state.
        Some(ReorgResult::Progress(rows)) => {
            active.job.row_count = *rows;
            Ok(Some(action_step(job_only_write(active.job.id, Vec::new()))))
        }
        Some(ReorgResult::Skipped) => Ok(None),
        // Go `runReorgJob` records the entries cleaned.
        Some(ReorgResult::Done(rows)) => {
            active.job.row_count = *rows;
            Ok(None)
        }
        // The job retries with the error counted.
        Some(ReorgResult::Failed { code, message, .. }) => {
            Err(checked_refusal(*code, message.clone()))
        }
        Some(ReorgResult::Checked(_)) => Err(DdlPlanError::Encode(
            "a global-index cleanup got a check's outcome".to_owned(),
        )),
    }
}

/// Go `checkAddPartitionTooManyPartitions`.
fn check_add_partition_too_many_partitions(count: usize) -> Result<(), DdlPlanError> {
    if count > PARTITION_COUNT_LIMIT {
        return Err(coded_refusal(
            &tidb_util::dbterror::ERR_TOO_MANY_PARTITIONS,
            "Too many partitions (including subpartitions) were defined",
        ));
    }
    Ok(())
}

/// Go `strconv.Atoi` of a stored bound, as `checkAddPartitionValue` reads
/// it.
fn atoi(text: &str) -> Result<i64, DdlPlanError> {
    text.parse::<i64>().map_err(|error| {
        let reason = match error.kind() {
            std::num::IntErrorKind::PosOverflow | std::num::IntErrorKind::NegOverflow => {
                "value out of range"
            }
            _ => "invalid syntax",
        };
        DdlPlanError::Encode(format!("strconv.Atoi: parsing {text:?}: {reason}"))
    })
}

/// Go `checkAddPartitionValue`: a RANGE table's new bounds keep increasing
/// past its last one, and a LIST table with a DEFAULT partition takes none.
fn check_add_partition_value(
    info: &TableInfo,
    part_info: &tidb_model::partition::PartitionInfo,
) -> Result<(), DdlPlanError> {
    let Some(partition) = info.get_partition_info() else {
        return Ok(());
    };
    let partition = partition.read();
    match partition.partition_type {
        tidb_ast::PartitionType::RANGE if partition.columns.len() == 0 => {
            let old = partition.definitions.snapshot();
            let Some(last) = old.last() else {
                return Ok(());
            };
            let range_value = last.less_than.get(0);
            if range_value.eq_ignore_ascii_case("MAXVALUE") {
                return Err(coded_refusal(
                    &tidb_util::dbterror::ERR_PARTITION_MAXVALUE,
                    "MAXVALUE can only be used in last partition definition",
                ));
            }
            let mut current = atoi(&range_value)?;
            let new = part_info.definitions.snapshot();
            for (offset, definition) in new.iter().enumerate() {
                let bound = definition.less_than.get(0);
                let max_value = bound.eq_ignore_ascii_case("MAXVALUE");
                if max_value && offset == new.len() - 1 {
                    return Ok(());
                }
                if max_value {
                    return Err(coded_refusal(
                        &tidb_util::dbterror::ERR_PARTITION_MAXVALUE,
                        "MAXVALUE can only be used in last partition definition",
                    ));
                }
                let next = atoi(&bound)?;
                if next <= current {
                    return Err(coded_refusal(
                        &tidb_util::dbterror::ERR_RANGE_NOT_INCREASING,
                        "VALUES LESS THAN value must be strictly increasing for each partition",
                    ));
                }
                current = next;
            }
            Ok(())
        }
        tidb_ast::PartitionType::LIST if partition.get_default_list_partition() != -1 => {
            Err(coded_refusal(
                &tidb_util::dbterror::ERR_RUN_MULTI_SCHEMA_CHANGES,
                "Unsupported ADD List partition, already contains DEFAULT partition. Please use REORGANIZE PARTITION instead",
            ))
        }
        _ => Ok(()),
    }
}

/// Go `checkAddPartitionNameUnique`.
fn check_add_partition_name_unique(
    info: &TableInfo,
    part_info: &tidb_model::partition::PartitionInfo,
) -> Result<(), DdlPlanError> {
    let mut names = info
        .get_partition_info()
        .map(|partition| partition.read().definitions.snapshot())
        .unwrap_or_default()
        .into_iter()
        .map(|definition| definition.name.lowercase().to_owned())
        .collect::<std::collections::HashSet<_>>();
    for definition in part_info.definitions.snapshot() {
        if !names.insert(definition.name.lowercase().to_owned()) {
            return Err(coded_refusal(
                &tidb_util::dbterror::ERR_SAME_NAME_PARTITION,
                format!("Duplicate partition name {}", definition.name.original()),
            ));
        }
    }
    Ok(())
}

/// Go `onAddTablePartition`.
pub(super) fn plan_persisted_add_table_partition_job_step<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let args = table_partition_args(active)?;
    if active.job.is_rollingback() {
        return rollback_like_drop_partition(snapshot, catalog, active, start_ts);
    }
    let info = checked_job_table(catalog, active, true)?.clone_like_go();
    let partition = job_partition(active, &info)?;
    let part_info = args
        .read()
        .part_info
        .get()
        .map(|info| info.read().clone_like_go())
        .unwrap_or_default();
    let database_id = active.job.schema_id;
    let schema = active.job.schema_name.to_string();
    match active.job.schema_state {
        SchemaState::NONE => {
            if info.affinity.is_some() {
                return Err(cancel_ddl_job(
                    active,
                    coded_refusal(
                        &tidb_util::dbterror::ERR_RUN_MULTI_SCHEMA_CHANGES,
                        "Unsupported ADD PARTITION of a table with AFFINITY option",
                    ),
                ));
            }
            let checked = check_add_partition_too_many_partitions(
                partition.read().definitions.len() + part_info.definitions.len(),
            )
            .and_then(|()| check_add_partition_value(&info, &part_info))
            .and_then(|()| check_add_partition_name_unique(&info, &part_info));
            if let Err(error) = checked {
                return Err(cancel_ddl_job(active, error));
            }
            let before = info.clone_like_go();
            let adding = part_info.definitions.snapshot();
            {
                let mut partition = partition.write();
                partition.adding_definitions = adding.clone().into();
                partition.ddl_state = SchemaState::REPLICA_ONLY;
                partition.ddl_action = active.job.type_;
            }
            // `updateVersionAndTableInfoWithCheck`.
            check_table_info_valid(&info)?;
            let mut write = table_step(catalog, active, database_id, &info, true, start_ts)?;
            for definition in &adding {
                alter_table::check_placement_policy_ref_valid(
                    snapshot,
                    &definition.placement_policy_ref,
                )
                .map_err(|error| cancel_ddl_job(active, error))?;
            }
            // Go configures TiFlash's PD rules for the new partitions here
            // (`ConfigureTiFlashPDForPartitions`); this node configures
            // none for any table yet.
            let bundles = alter_table_partition_bundles(snapshot, &info, adding.clone())
                .map_err(|error| cancel_ddl_job(active, error))?;
            // `alterTableLabelRule` over the table's ids and the adding ones.
            let mut spanning = info.clone_like_go();
            if let Some(partition) = &spanning.partition {
                let mut partition = partition.read().clone_like_go();
                let mut definitions = partition.definitions.snapshot();
                definitions.extend(adding);
                partition.definitions = definitions.into();
                spanning.partition = Some(GoShared::new(partition));
            }
            let mut label_rules = LabelRuleSteps::new();
            label_rules
                .partition(
                    &schema,
                    &before,
                    &spanning,
                    label_rules::PartitionLabelJob::AddOrReorganize,
                )
                .map_err(|error| cancel_ddl_job(active, error))?;
            active.job.schema_state = SchemaState::REPLICA_ONLY;
            write.placement_bundles = bundles;
            write.label_rules = label_rules.steps;
            Ok(action_step(write))
        }
        SchemaState::REPLICA_ONLY => {
            if let Some(replica) = &info.tiflash_replica {
                if replica.read().available {
                    // Go `checkPartitionReplica` waits for each new
                    // partition's regions to get a TiFlash peer; with
                    // TiFlash disabled there is nothing to wait for.
                    if tidb_config::config_tree::config::get_global_config()
                        .cse
                        .is_tiflash_enabled()
                    {
                        return Err(DdlPlanError::Unsupported(
                            "waiting for TiFlash replicas of added partitions is not supported yet"
                                .to_owned(),
                        ));
                    }
                    let mut replica = replica.write();
                    let mut ids = replica
                        .available_partition_ids
                        .iter()
                        .copied()
                        .collect::<Vec<_>>();
                    ids.extend(
                        part_info
                            .definitions
                            .snapshot()
                            .iter()
                            .map(|definition| definition.id),
                    );
                    replica.available_partition_ids = ids.into();
                }
            }
            // `updatePartitionInfo`: the adding definitions join the others.
            {
                let mut partition = partition.write();
                let mut definitions = partition.definitions.snapshot();
                definitions.extend(partition.adding_definitions.snapshot());
                partition.definitions = definitions.into();
                partition.adding_definitions = Vec::new().into();
                partition.ddl_state = SchemaState::NONE;
                partition.ddl_action = ActionType::ACTION_NONE;
            }
            let mut write = table_step(catalog, active, database_id, &info, true, start_ts)?;
            if !tidb_metadef::is_mem_or_sys_db(&schema.go_to_lower()) {
                append_schema_change_mutations(
                    snapshot,
                    catalog,
                    active.job.id,
                    &[(
                        -1,
                        SchemaChangeEvent::add_partitions(
                            info.clone_like_go(),
                            event_partition_info(part_info.definitions.snapshot()),
                        ),
                    )],
                    &mut write.mutations,
                )?;
            }
            active.job.finish_table_job(
                JobState::DONE,
                SchemaState::PUBLIC,
                write.schema_version,
                Some(GoShared::new(info)),
            );
            Ok(action_step(write))
        }
        state => Err(invalid_partition_state(state)),
    }
}

/// Go `rollbackLikeDropPartition`, for an ADD PARTITION and a partition
/// reorganization: the adding definitions (never used) go with their label
/// rules, and the placement bundles are rebuilt without them.
pub(super) fn rollback_like_drop_partition<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let args = table_partition_args(active)?;
    let info = checked_job_table(catalog, active, true)?.clone_like_go();
    let partition = job_partition(active, &info)?;
    let schema = active.job.schema_name.to_string();
    let database_id = active.job.schema_id;
    // The table as the label rules know it: the adding partitions included.
    let before = {
        let mut before = info.clone_like_go();
        if let Some(partition) = &before.partition {
            let mut partition = partition.read().clone_like_go();
            let mut definitions = partition.definitions.snapshot();
            definitions.extend(partition.adding_definitions.snapshot());
            partition.definitions = definitions.into();
            before.partition = Some(GoShared::new(partition));
        }
        before
    };
    // `removePartitionAddingDefinitionsFromTableInfo`.
    let mut physical_table_ids = {
        let mut partition = partition.write();
        partition.dropping_definitions = Vec::new().into();
        let ids = partition
            .adding_definitions
            .snapshot()
            .iter()
            .map(|definition| definition.id)
            .collect::<Vec<_>>();
        partition.adding_definitions = Vec::new().into();
        ids
    };
    let mut label_rules = LabelRuleSteps::new();
    label_rules
        .partition(
            &schema,
            &before,
            &info,
            label_rules::PartitionLabelJob::Drop,
        )
        .map_err(|error| cancel_ddl_job(active, error))?;
    let mut info = info;
    // ALTER TABLE ... PARTITION BY: the new table id goes too, and a table
    // that was not partitioned before is again.
    let part_info = args
        .read()
        .part_info
        .get()
        .map(|info| info.read().clone_like_go())
        .unwrap_or_default();
    if part_info.partition_type != tidb_ast::PartitionType::NONE {
        if part_info.new_table_id != 0 {
            physical_table_ids.push(part_info.new_table_id);
        }
        let reset = {
            let partition = partition.read();
            partition.partition_type == tidb_ast::PartitionType::NONE
                || partition.ddl_type == tidb_ast::PartitionType::NONE
        };
        if reset {
            info.partition = None;
        }
    }
    // The indexes the job created (write-only by now) go with it.
    let mut old_global_indexes = Vec::new();
    let write_only = info
        .indices
        .iter_deref()
        .filter(|index| index.read().state == SchemaState::WRITE_ONLY)
        .map(|index| index.read().clone_like_go())
        .collect::<Vec<_>>();
    for index in write_only {
        super::add_index::remove_index_and_hidden_columns(&mut info, &index);
        if index.global {
            old_global_indexes.push(tidb_model::TableIDIndexID {
                table_id: info.id,
                index_id: index.id,
            });
        }
    }
    if let Some(partition) = &info.partition {
        partition.write().clear_reorg_intermediate_info();
    }
    let bundles = alter_table_partition_bundles(snapshot, &info, Vec::new())
        .map_err(|error| cancel_ddl_job(active, error))?;
    let mut write = table_step(catalog, active, database_id, &info, true, start_ts)?;
    active.job.finish_table_job(
        JobState::ROLLBACK_DONE,
        SchemaState::NONE,
        write.schema_version,
        Some(GoShared::new(info)),
    );
    {
        let args = args.write();
        args.old_physical_table_ids
            .set(GoSharedSlice::from_vec(physical_table_ids));
        args.old_global_indexes
            .set(GoSharedSlice::from_vec(old_global_indexes));
    }
    active.job.fill_finished_args(Some(args));
    write.placement_bundles = bundles;
    write.label_rules = label_rules.steps;
    Ok(action_step(write))
}

/// Go `CheckDropTablePartition`, against the table the job steps.
fn check_drop_table_partition(info: &TableInfo, names: &[String]) -> Result<(), DdlPlanError> {
    let Some(partition) = info.get_partition_info() else {
        return Err(admission(
            tidb_executor::DriverError::PartitionManagementOnNonpartitioned,
        ));
    };
    let partition = partition.read();
    if !matches!(
        partition.partition_type,
        tidb_ast::PartitionType::RANGE | tidb_ast::PartitionType::LIST
    ) {
        return Err(admission(
            tidb_executor::DriverError::PartitionOnlyRangeList("DROP"),
        ));
    }
    if info.affinity.is_some() {
        return Err(coded_refusal(
            &tidb_util::dbterror::ERR_RUN_MULTI_SCHEMA_CHANGES,
            "Unsupported DROP PARTITION of a table with AFFINITY option",
        ));
    }
    let definitions = partition.definitions.snapshot();
    if definitions.len() <= names.len() {
        return Err(admission(tidb_executor::DriverError::PartitionDropLast));
    }
    let mut seen = std::collections::HashSet::new();
    for name in names {
        let found = definitions
            .iter()
            .any(|definition| definition.name.lowercase() == name);
        if !found || !seen.insert(name.clone()) {
            return Err(admission(
                tidb_executor::DriverError::PartitionDropNonexistent,
            ));
        }
    }
    Ok(())
}

/// Go `updateDroppingPartitionInfo`.
fn update_dropping_partition_info(
    partition: &GoShared<tidb_model::partition::PartitionInfo>,
    names: &[String],
) {
    let mut partition = partition.write();
    let (dropping, kept): (Vec<_>, Vec<_>) = partition
        .definitions
        .snapshot()
        .into_iter()
        .partition(|definition| names.iter().any(|name| definition.name.lowercase() == name));
    partition.definitions = kept.into();
    partition.dropping_definitions = dropping.into();
}

/// Go `onDropTablePartition`.
pub(super) fn plan_persisted_drop_table_partition_job_step<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
    reorg_outcome: Option<&ReorgOutcome>,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let args = table_partition_args(active)?;
    let names = args
        .read()
        .part_names
        .get()
        .snapshot()
        .into_iter()
        .map(|name| name.to_string())
        .collect::<Vec<_>>();
    let info = checked_job_table(catalog, active, true)?.clone_like_go();
    let database_id = active.job.schema_id;
    let schema = active.job.schema_name.to_string();
    match active.job.schema_state {
        SchemaState::PUBLIC => {
            check_drop_table_partition(&info, &names)
                .map_err(|error| cancel_ddl_job(active, error))?;
            let partition = job_partition(active, &info)?;
            // Only marked: the partitions are neither read nor written, but
            // stay in the definitions until every node knows it.
            let original = partition.read().definitions.snapshot();
            update_dropping_partition_info(&partition, &names);
            {
                let mut partition = partition.write();
                partition.definitions = original.into();
                partition.ddl_state = SchemaState::WRITE_ONLY;
                partition.ddl_action = active.job.type_;
            }
            active.job.schema_state = SchemaState::WRITE_ONLY;
            let write = table_step(catalog, active, database_id, &info, true, start_ts)?;
            Ok(action_step(write))
        }
        SchemaState::WRITE_ONLY => {
            let partition = job_partition(active, &info)?;
            let before = info.clone_like_go();
            update_dropping_partition_info(&partition, &names);
            let mut label_rules = LabelRuleSteps::new();
            label_rules
                .partition(
                    &schema,
                    &before,
                    &info,
                    label_rules::PartitionLabelJob::Drop,
                )
                .map_err(|error| cancel_ddl_job(active, error))?;
            let dropping = partition.read().dropping_definitions.snapshot();
            let mut bundles = dropped_partition_bundles(snapshot, &info, &dropping)
                .map_err(|error| cancel_ddl_job(active, error))?;
            // The table's bundle loses the dropped partitions' rules.
            bundles.extend(
                table_bundle(snapshot, &info).map_err(|error| cancel_ddl_job(active, error))?,
            );
            partition.write().ddl_state = SchemaState::DELETE_ONLY;
            active.job.schema_state = SchemaState::DELETE_ONLY;
            let mut write = table_step(catalog, active, database_id, &info, true, start_ts)?;
            write.placement_bundles = bundles;
            write.label_rules = label_rules.steps;
            Ok(action_step(write))
        }
        // Not a real delete-only state: it makes sure no node still sees
        // the old partitions when the reorganization runs.
        SchemaState::DELETE_ONLY => {
            let partition = job_partition(active, &info)?;
            partition.write().ddl_state = SchemaState::DELETE_REORGANIZATION;
            active.job.schema_state = SchemaState::DELETE_REORGANIZATION;
            let write = table_step(catalog, active, database_id, &info, true, start_ts)?;
            Ok(action_step(write))
        }
        SchemaState::DELETE_REORGANIZATION => {
            let partition = job_partition(active, &info)?;
            let dropped = partition.read().dropping_definitions.snapshot();
            let physical_table_ids = dropped
                .iter()
                .map(|definition| definition.id)
                .collect::<Vec<_>>();
            if has_global_index(&info) {
                let old_info = table_info_with_dropping_partitions(&info);
                if let Some(step) = clean_global_index_entries_from_dropped_partitions(
                    snapshot,
                    catalog,
                    active,
                    &old_info,
                    &physical_table_ids,
                    reorg_outcome,
                )? {
                    return Ok(step);
                }
            }
            remove_tiflash_available_partition_ids(&info, &physical_table_ids);
            {
                let mut partition = partition.write();
                partition.dropping_definitions = Vec::new().into();
                partition.ddl_state = SchemaState::NONE;
                partition.ddl_action = ActionType::ACTION_NONE;
            }
            active.job.schema_state = SchemaState::NONE;
            // `SetSchemaDiffForDropTablePartition`.
            let affects = build_placement_affects(&physical_table_ids, &physical_table_ids);
            let mut write = table_step_with_diff(
                catalog,
                active,
                database_id,
                &info,
                true,
                start_ts,
                |diff| diff.affected_options = affects.into(),
            )?;
            if !tidb_metadef::is_mem_or_sys_db(&schema.go_to_lower()) {
                append_schema_change_mutations(
                    snapshot,
                    catalog,
                    active.job.id,
                    &[(
                        -1,
                        SchemaChangeEvent::drop_partitions(
                            info.clone_like_go(),
                            event_partition_info(dropped),
                        ),
                    )],
                    &mut write.mutations,
                )?;
            }
            active.job.finish_table_job(
                JobState::DONE,
                SchemaState::NONE,
                write.schema_version,
                Some(GoShared::new(info.clone_like_go())),
            );
            // The delete range reads the old partitions from the finished
            // args.
            args.write()
                .old_physical_table_ids
                .set(GoSharedSlice::from_vec(physical_table_ids));
            active.job.fill_finished_args(Some(args));
            Ok(action_step(write))
        }
        state => Err(invalid_partition_state(state)),
    }
}

/// Go `onTruncateTablePartition`.
pub(super) fn plan_persisted_truncate_table_partition_job_step<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
    reorg_outcome: Option<&ReorgOutcome>,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    // Only the first step may still cancel the job on a fault.
    let can_cancel = active.job.schema_state == SchemaState::PUBLIC;
    let fault = |active: &mut crate::ddl_job_table::ActiveDdlJob, error: DdlPlanError| {
        if can_cancel {
            cancel_ddl_job(active, error)
        } else {
            error
        }
    };
    let args = match tidb_model::get_truncate_table_args(&mut active.job) {
        Ok(Some(args)) => args,
        Ok(None) => {
            return Err(fault(
                active,
                DdlPlanError::Encode("TRUNCATE PARTITION has nil args".to_owned()),
            ))
        }
        Err(error) => return Err(fault(active, DdlPlanError::Encode(error.to_string()))),
    };
    let (old_ids, new_ids) = {
        let args = args.read();
        (
            args.old_partition_ids.get().snapshot(),
            args.new_partition_ids.get().snapshot(),
        )
    };
    if old_ids.len() != new_ids.len() {
        return Err(fault(
            active,
            DdlPlanError::Encode("len(oldIDs) must be the same as len(newIDs)".to_owned()),
        ));
    }
    let info = checked_job_table(catalog, active, true)?.clone_like_go();
    let Some(partition) = info.partition.clone() else {
        return Err(fault(
            active,
            admission(tidb_executor::DriverError::PartitionManagementOnNonpartitioned),
        ));
    };
    let database_id = active.job.schema_id;
    let schema = active.job.schema_name.to_string();
    match active.job.schema_state {
        SchemaState::PUBLIC => {
            // Global-index reads skip entries of the new partitions.
            {
                let mut partition = partition.write();
                partition.new_partition_ids = new_ids.into();
                partition.ddl_action = ActionType::ACTION_TRUNCATE_TABLE_PARTITION;
                partition.ddl_state = SchemaState::WRITE_ONLY;
            }
            active.job.schema_state = SchemaState::WRITE_ONLY;
            let write = table_step(catalog, active, database_id, &info, true, start_ts)?;
            Ok(action_step(write))
        }
        SchemaState::WRITE_ONLY => {
            let before = info.clone_like_go();
            // `replaceTruncatePartitions`: the partitions take their new ids.
            let mut old_definitions = Vec::new();
            let mut new_definitions = Vec::new();
            {
                let partition = partition.read();
                for (old_id, new_id) in old_ids.iter().zip(&new_ids) {
                    if let Some(position) = partition
                        .definitions
                        .snapshot()
                        .iter()
                        .position(|definition| definition.id == *old_id)
                    {
                        let mut new_definition = None;
                        partition.definitions.update(position, |definition| {
                            old_definitions.push(definition.clone_like_go());
                            definition.id = *new_id;
                            new_definition = Some(definition.clone_like_go());
                        });
                        new_definitions.extend(new_definition);
                    }
                }
            }
            // `clearTruncatePartitionTiflashStatus`; Go configures TiFlash's
            // PD rules for the new partitions too, which this node does for
            // no table yet.
            if let Some(replica) = &info.tiflash_replica {
                replica.write().available = false;
                remove_tiflash_available_partition_ids(&info, &old_ids);
            }
            // `updateTruncatePartitionLabelRules`.
            let policies = load_policies(snapshot)?;
            let mut bundles =
                tidb_placement::new_partition_list_bundles(&policies, &new_definitions)
                    .map_err(bundle_error)?;
            bundles.extend(table_bundle(snapshot, &info)?);
            bundles.extend(dropped_partition_bundles(
                snapshot,
                &info,
                &old_definitions,
            )?);
            let mut label_rules = LabelRuleSteps::new();
            label_rules.partition(
                &schema,
                &before,
                &info,
                label_rules::PartitionLabelJob::Truncate,
            )?;
            // Global-index reads skip entries of the old partitions now.
            {
                let mut partition = partition.write();
                partition.dropping_definitions = old_definitions.into();
                partition.ddl_state = SchemaState::DELETE_ONLY;
            }
            active.job.schema_state = SchemaState::DELETE_ONLY;
            let mut write = table_step(catalog, active, database_id, &info, true, start_ts)?;
            write.placement_bundles = bundles;
            write.label_rules = label_rules.steps;
            Ok(action_step(write))
        }
        SchemaState::DELETE_ONLY => {
            partition.write().ddl_state = SchemaState::DELETE_REORGANIZATION;
            active.job.schema_state = SchemaState::DELETE_REORGANIZATION;
            let write = table_step(catalog, active, database_id, &info, true, start_ts)?;
            Ok(action_step(write))
        }
        SchemaState::DELETE_REORGANIZATION => {
            if let Some(step) = clean_global_index_entries_from_dropped_partitions(
                snapshot,
                catalog,
                active,
                &table_info_with_dropping_partitions(&info),
                &old_ids,
                reorg_outcome,
            )? {
                return Ok(step);
            }
            let old_definitions = partition.read().dropping_definitions.snapshot();
            let new_definitions = old_definitions
                .iter()
                .zip(&new_ids)
                .map(|(definition, new_id)| {
                    let mut definition = definition.clone_like_go();
                    definition.id = *new_id;
                    definition
                })
                .collect::<Vec<_>>();
            {
                let mut partition = partition.write();
                partition.dropping_definitions = Vec::new().into();
                partition.new_partition_ids = Vec::new().into();
                partition.ddl_state = SchemaState::NONE;
                partition.ddl_action = ActionType::ACTION_NONE;
            }
            // The new partitions' affinity groups first (critical), then the
            // old ones', best effort.
            let mut affinity_groups = crate::ddl_affinity::AffinityGroupUpdate::default();
            if info.affinity.is_some() {
                affinity_groups
                    .create_for(&info)
                    .map_err(|error| cancel_ddl_job(active, pd_step_error(None, error)))?;
                affinity_groups.delete_for(&info, Some(&old_definitions));
            }
            active.job.schema_state = SchemaState::NONE;
            // `SetSchemaDiffForTruncateTablePartition` with
            // `ShouldUpdateAffectedPartitions`.
            let affects = build_placement_affects(&old_ids, &new_ids);
            let mut write = table_step_with_diff(
                catalog,
                active,
                database_id,
                &info,
                true,
                start_ts,
                |diff| diff.affected_options = affects.into(),
            )?;
            if !tidb_metadef::is_mem_or_sys_db(&schema.go_to_lower()) {
                append_schema_change_mutations(
                    snapshot,
                    catalog,
                    active.job.id,
                    &[(
                        -1,
                        SchemaChangeEvent::truncate_partitions(
                            info.clone_like_go(),
                            event_partition_info(new_definitions),
                            event_partition_info(old_definitions),
                        ),
                    )],
                    &mut write.mutations,
                )?;
            }
            active.job.finish_table_job(
                JobState::DONE,
                SchemaState::NONE,
                write.schema_version,
                Some(GoShared::new(info)),
            );
            // The delete range reads the old partitions from the finished
            // args.
            active
                .job
                .fill_finished_args(Some(GoShared::new(tidb_model::TruncateTableArgs {
                    old_partition_ids: GoField::new(GoSharedSlice::from_vec(old_ids)),
                    ..tidb_model::TruncateTableArgs::default()
                })));
            write.affinity_groups = affinity_groups;
            Ok(action_step(write))
        }
        state => Err(invalid_partition_state(state)),
    }
}

/// Go `rollingbackAddTablePartition`, `rollingbackDropTablePartition` and
/// `rollingbackTruncateTablePartition` (`convertTruncateTablePartitionJob2RollbackJob`).
pub(super) fn plan_cancel_partition(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let info = checked_job_table(catalog, active, true)?.clone_like_go();
    let database_id = active.job.schema_id;
    match active.job.type_ {
        ActionType::ACTION_ADD_TABLE_PARTITION => {
            let adding = info
                .get_partition_info()
                .map(|partition| partition.read().adding_definitions.snapshot())
                .unwrap_or_default();
            // Not yet replica-only: nothing to undo.
            if adding.is_empty() {
                return Err(cancelled_ddl_job(active));
            }
            // `convertAddTablePartitionJob2RollbackJob`.
            let args = GoShared::new(tidb_model::TablePartitionArgs {
                part_names: GoField::new(GoSharedSlice::from_vec(
                    adding
                        .iter()
                        .map(|definition| definition.name.lowercase().to_owned().into())
                        .collect(),
                )),
                ..tidb_model::TablePartitionArgs::default()
            });
            tidb_model::fill_rollback_args_for_add_partition(&mut active.job, Some(&args));
            let write = table_step(catalog, active, database_id, &info, true, start_ts)?;
            active.job.state = JobState::ROLLINGBACK;
            Ok(PersistedDdlActionStep {
                write,
                update_raw_args: true,
                run_error: Some(DdlPlanError::from(
                    tidb_util::dbterror::ERR_CANCELLED_DDL_JOB
                        .generate(tidb_error::tidb::errname::ErrCancelledDDLJob.raw),
                )),
                reorg: None,
            })
        }
        // `cancelOnlyNotHandledJob(job, model.StatePublic)`.
        ActionType::ACTION_DROP_TABLE_PARTITION => {
            if active.job.schema_state == SchemaState::PUBLIC {
                return Err(cancelled_ddl_job(active));
            }
            active.job.state = JobState::RUNNING;
            Ok(job_control_step(&active.job))
        }
        _ => {
            // Past write-only the new partitions may hold rows: roll on.
            if !matches!(
                active.job.schema_state,
                SchemaState::PUBLIC | SchemaState::WRITE_ONLY
            ) {
                active.job.state = JobState::RUNNING;
                return Ok(job_control_step(&active.job));
            }
            let Some(partition) = info.partition.clone() else {
                return Err(cancelled_ddl_job(active));
            };
            let changed = {
                let partition = partition.read();
                partition.new_partition_ids.len() != 0
                    || partition.ddl_action != ActionType::ACTION_NONE
                    || partition.ddl_state != SchemaState::NONE
            };
            if !changed {
                return Err(cancelled_ddl_job(active));
            }
            // Only metadata to undo: no new partition was used yet. The job
            // stays cancelling and is cancelled at its next visit.
            {
                let mut partition = partition.write();
                partition.new_partition_ids = Vec::new().into();
                partition.ddl_action = ActionType::ACTION_NONE;
                partition.ddl_state = SchemaState::NONE;
            }
            let write = table_step(catalog, active, database_id, &info, true, start_ts)?;
            Ok(action_step(write))
        }
    }
}
