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

//! REORGANIZE PARTITION (and COALESCE PARTITION and ADD PARTITION on a HASH
//! or KEY table, Go's `hashPartitionManagement`), ALTER TABLE ... PARTITION
//! BY and REMOVE PARTITIONING as Go runs them: the executor checks the
//! statement and queues one job carrying the partitions it replaces and the
//! new definitions; the owner's `onReorganizePartition` steps it through
//! none -> delete-only -> write-only -> write-reorganization (the rows copied
//! into the new partitions and their indexes built) -> delete-reorganization
//! -> public -> none, DML double writing both partition sets meanwhile.

use super::add_index::{
    action_step, job_batch_size, load_reorg_handle, ReorgSessionVars, reorg_step,
    reorg_sub_job, table_step_with_diff, AddIndexReorg, AddIndexSubmission, PartitionReorgPhase,
    ReorgOutcome, ReorgResult, ReorgStageRequest, COLUMN_ELEMENT_KEY,
};
use super::*;
use tidb_hack::GoCaseCompare;
use tidb_hack::GoToLower;

/// The planner's ids at admission: placeholders the job submitter replaces
/// (Go draws them in `assignGIDsForJobs`, not in the executor).
struct AdmissionReorgHost {
    next: i64,
    policies: Vec<tidb_model::PolicyInfo>,
}

impl tidb_ddl_build::partition_ddl::ReorgHost for AdmissionReorgHost {
    fn allocate_table_id(&mut self) -> i64 {
        self.next -= 1;
        self.next
    }

    fn resolve_placement(
        &self,
        definitions: &mut [tidb_table::partition_routing::PartitionDef],
    ) -> Result<(), tidb_error::mysql::MysqlError> {
        for definition in definitions {
            if let Some(reference) = definition.placement_policy.as_mut() {
                let Some(found) = self
                    .policies
                    .iter()
                    .find(|policy| policy.name.lowercase() == reference.name.lowercase())
                else {
                    return Err(tidb_error::mysql::MysqlError::from_terror(
                        &tidb_error::catalog::infoschema::ERR_PLACEMENT_POLICY_NOT_EXISTS
                            .generate_with_stack_by_args(&[(reference.name.original()).into()]),
                    ));
                };
                reference.id = found.id;
            }
        }
        Ok(())
    }
}

/// Go `getPartitionInfoTypeNone`: the single partition a table that is not
/// partitioned yet is seen as during ALTER TABLE ... PARTITION BY.
fn partition_info_type_none() -> tidb_model::partition::PartitionInfo {
    tidb_model::partition::PartitionInfo {
        partition_type: tidb_ast::PartitionType::NONE,
        enable: true,
        definitions: vec![tidb_model::partition::PartitionDefinition {
            name: CiString::new("pFullTable".to_owned()),
            comment: "Intermediate partition during ALTER TABLE ... PARTITION BY ...".to_owned(),
            ..tidb_model::partition::PartitionDefinition::default()
        }]
        .into(),
        num: 1,
        ..tidb_model::partition::PartitionInfo::default()
    }
}

/// Go's executor halves (`ReorganizePartitions`, `hashPartitionManagement`,
/// `AlterTablePartitioning`, `RemovePartitioning`) up to `doDDLJob2`.
/// `None` for any other statement.
pub fn prepare_reorganize_partition_job_submission<S: MetaSnapshot>(
    snapshot: &mut S,
    statement: &DdlStatement,
    job_context: &dyn DdlSessionContext,
    start_ts: u64,
    upgrading: bool,
    min_job_id: i64,
    store_count: usize,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    let (schema, table, action, context) = match statement {
        DdlStatement::ReorganizePartitions {
            schema,
            table,
            action,
            context,
        }
        | DdlStatement::AddPartitions {
            schema,
            table,
            action,
            context,
            ..
        } => (schema, table, action.as_ref(), &context.0),
        _ => return Ok(None),
    };
    let catalog = load_cluster_catalog(snapshot)?;
    let (database_id, stored) = locate_table(&catalog, schema, table)?;
    let kv = tidb_ddl_build::partition_ddl::PartitionedTable::new(stored)
        .map_err(|error| DdlPlanError::Admission(DdlAdmissionError::from_mysql_error(error)))?;
    let ctx = context.metabuild_context();
    let mut host = AdmissionReorgHost {
        next: 0,
        policies: load_policies(snapshot)?.policies,
    };
    let planned = match action {
        tidb_ast::AlterPartitionAction::Reorganize {
            names, definitions, ..
        } => tidb_ddl_build::partition_ddl::plan_reorganize_partition(
            &kv,
            names,
            definitions,
            &ctx,
            &mut host,
        ),
        tidb_ast::AlterPartitionAction::Repartition(partitioning) => {
            tidb_ddl_build::partition_ddl::plan_alter_table_partitioning(
                &kv,
                partitioning,
                &ctx,
                &mut host,
            )
        }
        tidb_ast::AlterPartitionAction::RemovePartitioning => {
            tidb_ddl_build::partition_ddl::plan_remove_partitioning(&kv)
        }
        tidb_ast::AlterPartitionAction::Coalesce { count, .. } => {
            tidb_ddl_build::partition_ddl::plan_hash_partition_management(
                &kv,
                tidb_ddl_build::partition_ddl::HashManagement::Coalesce(*count),
                &ctx,
                &mut host,
            )
        }
        tidb_ast::AlterPartitionAction::Add { spec, .. } => {
            let change = match spec {
                tidb_ast::AddPartitionSpec::Definitions(definitions) => {
                    tidb_ddl_build::partition_ddl::HashManagement::Add {
                        count: 0,
                        definitions,
                    }
                }
                tidb_ast::AddPartitionSpec::Count(count) => {
                    tidb_ddl_build::partition_ddl::HashManagement::Add {
                        count: *count,
                        definitions: &[],
                    }
                }
            };
            tidb_ddl_build::partition_ddl::plan_hash_partition_management(
                &kv, change, &ctx, &mut host,
            )
        }
        _ => return Ok(None),
    }
    .map_err(DdlPlanError::from_mysql_error)?;
    let job_action = match action {
        tidb_ast::AlterPartitionAction::Repartition(_) => {
            ActionType::ACTION_ALTER_TABLE_PARTITIONING
        }
        tidb_ast::AlterPartitionAction::RemovePartitioning => {
            ActionType::ACTION_REMOVE_PARTITIONING
        }
        _ => ActionType::ACTION_REORGANIZE_PARTITION,
    };
    let old = stored
        .get_partition_info()
        .filter(|partition| partition.read().enable)
        .map(|partition| partition.read().clone_like_go());
    // Go's `PartNames`: the replaced partitions (every one for PARTITION
    // BY and REMOVE PARTITIONING, the intermediate one of a table that is
    // not partitioned yet).
    let part_names = match &old {
        Some(old) => old
            .definitions
            .snapshot()
            .into_iter()
            .filter(|definition| planned.source_ids.contains(&definition.id))
            .map(|definition| definition.name.lowercase().to_owned())
            .collect::<Vec<_>>(),
        None => vec![partition_info_type_none()
            .definitions
            .get(0)
            .name
            .lowercase()
            .to_owned()],
    };
    let old_ids = old
        .as_ref()
        .map(|old| {
            old.definitions
                .snapshot()
                .iter()
                .map(|definition| definition.id)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let mut part_info = match (&planned.stored, job_action) {
        (Some(final_metadata), ActionType::ACTION_ALTER_TABLE_PARTITIONING) => {
            let mut info = tidb_ddl_build::table_info_build::partition_info(final_metadata);
            info.ddl_type = old
                .as_ref()
                .map_or(tidb_ast::PartitionType::NONE, |old| old.partition_type);
            if let tidb_ast::AlterPartitionAction::Repartition(partitioning) = action {
                info.ddl_update_indexes = partitioning
                    .update_indexes
                    .iter()
                    .map(|update| tidb_model::partition::UpdateIndexInfo {
                        index_name: update.name.clone(),
                        global: update.global,
                    })
                    .collect::<Vec<_>>()
                    .into();
            }
            info
        }
        (Some(final_metadata), _) => {
            // Only the definitions the job adds, under the table's scheme.
            let rebuilt = tidb_ddl_build::table_info_build::partition_info(final_metadata);
            let old = old.as_ref().expect("a reorganized table is partitioned");
            let added = rebuilt
                .definitions
                .snapshot()
                .into_iter()
                .filter(|definition| !old_ids.contains(&definition.id))
                .collect::<Vec<_>>();
            tidb_model::partition::PartitionInfo {
                partition_type: old.partition_type,
                expr: old.expr.clone(),
                columns: old.columns.clone(),
                enable: old.enable,
                num: added.len() as u64,
                definitions: added.into(),
                ..tidb_model::partition::PartitionInfo::default()
            }
        }
        // Go `RemovePartitioning`: one `CollapsedPartitions` definition of a
        // NONE-typed partitioning.
        (None, _) => {
            let old = old
                .as_ref()
                .expect("REMOVE PARTITIONING needs partitioning");
            tidb_model::partition::PartitionInfo {
                partition_type: tidb_ast::PartitionType::NONE,
                expr: old.expr.clone(),
                columns: old.columns.clone(),
                enable: old.enable,
                num: 1,
                definitions: vec![tidb_model::partition::PartitionDefinition {
                    name: CiString::new("CollapsedPartitions".to_owned()),
                    ..tidb_model::partition::PartitionDefinition::default()
                }]
                .into(),
                ..tidb_model::partition::PartitionInfo::default()
            }
        }
    };
    // `assignGIDsForJobs` draws every new id.
    let placeholders = part_info
        .definitions
        .snapshot()
        .into_iter()
        .map(|mut definition| {
            definition.id = 0;
            definition
        })
        .collect::<Vec<_>>();
    part_info.definitions = placeholders.into();
    part_info.new_table_id = 0;
    let mut job = session_job(job_context, job_action);
    job.schema_id = database_id;
    job.table_id = stored.id;
    job.schema_name = schema.go_to_lower().into();
    job.table_name = stored.name.lowercase().to_owned().into();
    job.query = tidb_ddl_build::partition_interval::rewritten_query(
        job_context.ddl_query(),
        planned.interval_rewrite.as_ref(),
    )
    .into();
    // Go `initJobReorgMetaFromVariables` with `setReorgParam`.
    job.reorg_meta = Some(GoShared::new(
        super::add_index::init_job_reorg_meta_from_variables(
            job_context,
            &schema.go_to_lower(),
            store_count,
            true,
            false,
        ),
    ));
    let args = tidb_model::TablePartitionArgs {
        part_names: GoField::new(GoSharedSlice::from_vec(
            part_names.into_iter().map(Into::into).collect(),
        )),
        part_info: GoField::new(Some(GoShared::new(part_info))),
        ..tidb_model::TablePartitionArgs::default()
    };
    let warnings = planned
        .warning
        .map(|warning| {
            vec![tidb_util::context::SqlWarn::with_code(
                DdlWarningLevel::Warning,
                1105,
                warning.to_owned(),
            )]
        })
        .unwrap_or_default();
    let built = AddIndexSubmission::Job(
        Box::new(crate::ddl_job_submit::JobSpec {
            job,
            args: JobArgsValue::TablePartition(Some(GoShared::new(args))),
            id_allocated: false,
        }),
        warnings,
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

/// Go `checkReorgPartitionNames`.
fn check_reorg_partition_names(
    partition: &tidb_model::partition::PartitionInfo,
    dropped: &[String],
    part_info: &tidb_model::partition::PartitionInfo,
) -> Result<(), DdlPlanError> {
    let mut names = partition
        .definitions
        .snapshot()
        .into_iter()
        .map(|definition| definition.name.lowercase().to_owned())
        .collect::<std::collections::HashSet<_>>();
    let same_name = |name: &str| {
        coded_refusal(
            &tidb_util::dbterror::ERR_SAME_NAME_PARTITION,
            format!("Duplicate partition name {name}"),
        )
    };
    for name in dropped {
        if !names.remove(&name.go_to_lower()) {
            return Err(same_name(name));
        }
    }
    for definition in part_info.definitions.snapshot() {
        if !names.insert(definition.name.lowercase().to_owned()) {
            return Err(same_name(definition.name.original()));
        }
    }
    Ok(())
}

/// Go `getReplacedPartitionIDs`: the positions of the replaced partitions,
/// and the first and last of them.
fn get_replaced_partition_ids(
    names: &[String],
    partition: &tidb_model::partition::PartitionInfo,
) -> Result<(usize, usize, std::collections::BTreeSet<usize>), DdlPlanError> {
    let definitions = partition.definitions.snapshot();
    let mut positions = std::collections::BTreeSet::new();
    for name in names {
        let lower = name.go_to_lower();
        let Some(position) = definitions
            .iter()
            .position(|definition| definition.name.lowercase() == lower)
        else {
            return Err(coded_refusal(
                &tidb_util::dbterror::ERR_WRONG_PARTITION_NAME,
                "Incorrect partition name",
            ));
        };
        if !positions.insert(position) {
            return Err(coded_refusal(
                &tidb_util::dbterror::ERR_SAME_NAME_PARTITION,
                format!("Duplicate partition name {}", definitions[position].name),
            ));
        }
    }
    let first = positions.iter().next().copied().unwrap_or(0);
    let last = positions.iter().next_back().copied().unwrap_or(0);
    match partition.partition_type {
        tidb_ast::PartitionType::RANGE if positions.len() != last - first + 1 => {
            return Err(coded_refusal(
                &tidb_util::dbterror::ERR_GENERAL_UNSUPPORTED_DDL,
                "Unsupported REORGANIZE PARTITION of RANGE; not adjacent partitions",
            ))
        }
        tidb_ast::PartitionType::HASH | tidb_ast::PartitionType::KEY
            if positions.len() != definitions.len() =>
        {
            return Err(coded_refusal(
                &tidb_util::dbterror::ERR_GENERAL_UNSUPPORTED_DDL,
                "Unsupported REORGANIZE PARTITION of HASH/RANGE; must reorganize all partitions",
            ))
        }
        _ => {}
    }
    Ok((first, last, positions))
}

/// Go `getReorganizedDefinitions`: the definitions once the job is done.
fn get_reorganized_definitions(
    partition: &tidb_model::partition::PartitionInfo,
    first: usize,
    last: usize,
    replaced: &std::collections::BTreeSet<usize>,
) -> Vec<tidb_model::partition::PartitionDefinition> {
    let definitions = partition.definitions.snapshot();
    let adding = partition.adding_definitions.snapshot();
    let mut result = Vec::with_capacity(definitions.len() + adding.len());
    if partition.partition_type == tidb_ast::PartitionType::LIST {
        let mut placed = false;
        for (position, definition) in definitions.into_iter().enumerate() {
            if replaced.contains(&position) {
                if !placed {
                    result.extend(adding.iter().cloned());
                    placed = true;
                }
                continue;
            }
            result.push(definition);
        }
        if !placed {
            result.extend(adding);
        }
        return result;
    }
    result.extend(definitions[..first].iter().cloned());
    result.extend(adding);
    if definitions.len() > last + 1 {
        result.extend(definitions[last + 1..].iter().cloned());
    }
    result
}

/// Go `getNewGlobal`: an index's locality after the job, as `UPDATE
/// INDEXES` names it.
fn get_new_global(part_info: &tidb_model::partition::PartitionInfo, index: &IndexInfo) -> bool {
    part_info
        .ddl_update_indexes
        .snapshot()
        .into_iter()
        .find(|update| update.index_name.go_equal_fold(index.name.lowercase()))
        .map_or(index.global, |update| update.global)
}

fn changed_indexes(partition: &tidb_model::partition::PartitionInfo) -> BTreeMap<i64, bool> {
    partition
        .ddl_changed_index
        .as_ref()
        .map(|changed| changed.read().clone())
        .unwrap_or_default()
}

/// Go `getReorgPartitionInfo`: the table, and at the first step the DDL's
/// target scheme recorded on its partitioning.
fn reorg_partition_info(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    part_info: &tidb_model::partition::PartitionInfo,
) -> Result<TableInfo, DdlPlanError> {
    let mut info = checked_job_table(catalog, active, true)?.clone_like_go();
    if active.job.schema_state == SchemaState::NONE {
        let partition = match &info.partition {
            Some(partition) => partition.clone(),
            None => {
                let none = partition_info_type_none();
                none.definitions
                    .update(0, |definition| definition.id = info.id);
                let partition = GoShared::new(none);
                info.partition = Some(partition.clone());
                partition
            }
        };
        let mut partition = partition.write();
        partition.new_table_id = part_info.new_table_id;
        partition.ddl_type = part_info.partition_type;
        partition.ddl_expr = part_info.expr.clone();
        partition.ddl_columns = part_info.columns.clone();
    }
    Ok(info)
}

/// Go `updateVersionAndTableInfo` for a reorganization step:
/// `SetSchemaDiffForPartitionModify` names the table as its own old table
/// for PARTITION BY and REMOVE PARTITIONING.
fn reorg_table_step(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    database_id: i64,
    info: &TableInfo,
    start_ts: u64,
) -> Result<DdlWrite, DdlPlanError> {
    let modifies = active.job.type_ != ActionType::ACTION_REORGANIZE_PARTITION;
    let job_table_id = active.job.table_id;
    table_step_with_diff(catalog, active, database_id, info, true, start_ts, |diff| {
        if modifies {
            diff.old_table_id = job_table_id;
        }
    })
}

/// Go `onReorganizePartition`.
pub(super) fn plan_persisted_reorganize_partition_job_step<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
    reorg_outcome: Option<&ReorgOutcome>,
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let args = tidb_model::get_table_partition_args(&mut active.job)
        .map_err(|error| cancel_ddl_job(active, DdlPlanError::Encode(error.to_string())))?
        .ok_or_else(|| {
            cancel_ddl_job(
                active,
                DdlPlanError::Encode("partition reorganization has nil args".to_owned()),
            )
        })?;
    if active.job.is_rollingback() {
        return super::partition::rollback_like_drop_partition(snapshot, catalog, active, start_ts);
    }
    let (part_names, part_info) = {
        let args = args.read();
        (
            args.part_names
                .get()
                .snapshot()
                .into_iter()
                .map(|name| name.to_string())
                .collect::<Vec<_>>(),
            args.part_info
                .get()
                .map(|info| info.read().clone_like_go())
                .unwrap_or_default(),
        )
    };
    let mut info = reorg_partition_info(catalog, active, &part_info)?;
    let partition = info
        .partition
        .clone()
        .expect("getReorgPartitionInfo gives the table a partitioning");
    let database_id = active.job.schema_id;
    let schema = active.job.schema_name.to_string();
    match active.job.schema_state {
        SchemaState::NONE => {
            if info.affinity.is_some() {
                return Err(cancel_ddl_job(
                    active,
                    coded_refusal(
                        &tidb_util::dbterror::ERR_GENERAL_UNSUPPORTED_DDL,
                        "Unsupported REORGANIZE PARTITION of a table with AFFINITY option",
                    ),
                ));
            }
            partition.write().ddl_action = active.job.type_;
            let checked = {
                let current = partition.read();
                let count =
                    part_info.definitions.len() + current.definitions.len() - part_names.len();
                (if count > 8192 {
                    Err(coded_refusal(
                        &tidb_util::dbterror::ERR_TOO_MANY_PARTITIONS,
                        "Too many partitions (including subpartitions) were defined",
                    ))
                } else {
                    Ok(())
                })
                .and_then(|()| check_reorg_partition_names(&current, &part_names, &part_info))
                .and_then(|()| get_replaced_partition_ids(&part_names, &current).map(|_| ()))
            };
            // The shared partition implementation checked the new
            // definitions against the table at admission (Go
            // `checkReorgPartitionDefs`, `checkColumnsPartitionType` and
            // `checkPartitionFuncType` repeat that here).
            if let Err(error) = checked {
                return Err(cancel_ddl_job(active, error));
            }
            let before = info.clone_like_go();
            // `updateAddingPartitionInfo`, then the dropping partitions
            // marked while the definitions stay as they are.
            let adding = part_info.definitions.snapshot();
            {
                let mut partition = partition.write();
                partition.adding_definitions = adding.clone().into();
                let (dropping, _): (Vec<_>, Vec<_>) = partition
                    .definitions
                    .snapshot()
                    .into_iter()
                    .partition(|definition| {
                        part_names
                            .iter()
                            .any(|name| definition.name.lowercase() == name.go_to_lower())
                    });
                partition.dropping_definitions = dropping.into();
            }
            for definition in &adding {
                alter_table::check_placement_policy_ref_valid(
                    snapshot,
                    &definition.placement_policy_ref,
                )
                .map_err(|error| cancel_ddl_job(active, error))?;
            }
            // Every global index, and every index the new scheme makes
            // global, is recreated under a new id: both partition id sets
            // must be in it at once.
            let mut changed = BTreeMap::new();
            let mut duplicates = Vec::new();
            for index in info.indices.iter_deref() {
                let index = index.read();
                let new_global = active.job.type_ != ActionType::ACTION_REMOVE_PARTITIONING
                    && get_new_global(&part_info, &index);
                if !index.global && !new_global {
                    continue;
                }
                let mut duplicate = index.clone_like_go();
                duplicate.state = SchemaState::DELETE_ONLY;
                info.max_index_id += 1;
                duplicate.id = info.max_index_id;
                changed.insert(index.id, false);
                changed.insert(duplicate.id, true);
                duplicate.global = new_global;
                duplicates.push(duplicate);
            }
            for mut duplicate in duplicates {
                set_global_index_version(&info, &mut duplicate);
                info.indices.push_go(duplicate);
            }
            if !changed.is_empty() {
                partition.write().ddl_changed_index = Some(GoShared::new(changed));
            }
            // Go configures TiFlash's PD rules for the adding partitions
            // here (`ConfigureTiFlashPDForPartitions`); this node configures
            // none for any table yet.
            let bundles =
                super::partition::alter_table_partition_bundles(snapshot, &info, adding.clone())
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
            if active.job.type_ == ActionType::ACTION_REORGANIZE_PARTITION {
                partition.write().set_original_partition_ids();
            }
            active.job.schema_state = SchemaState::DELETE_ONLY;
            partition.write().ddl_state = SchemaState::DELETE_ONLY;
            check_table_info_valid(&info)?;
            let mut write = reorg_table_step(catalog, active, database_id, &info, start_ts)?;
            write.placement_bundles = bundles;
            write.label_rules = label_rules.steps;
            Ok(action_step(write))
        }
        SchemaState::DELETE_ONLY => {
            if let Some(replica) = &info.tiflash_replica {
                if replica.read().available {
                    // Go `checkPartitionReplica`; with TiFlash disabled there
                    // is nothing to wait for.
                    if tidb_config::config_tree::config::get_global_config()
                        .cse
                        .is_tiflash_enabled()
                    {
                        return Err(DdlPlanError::Unsupported(
                            "waiting for TiFlash replicas of reorganized partitions is not supported yet"
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
                        partition
                            .read()
                            .adding_definitions
                            .snapshot()
                            .iter()
                            .map(|definition| definition.id),
                    );
                    replica.available_partition_ids = ids.into();
                }
            }
            for index in info.indices.iter_deref() {
                let mut index = index.write();
                if index.state == SchemaState::DELETE_ONLY {
                    index.state = SchemaState::WRITE_ONLY;
                }
            }
            active.job.schema_state = SchemaState::WRITE_ONLY;
            partition.write().ddl_state = SchemaState::WRITE_ONLY;
            let write = reorg_table_step(catalog, active, database_id, &info, start_ts)?;
            Ok(action_step(write))
        }
        SchemaState::WRITE_ONLY => {
            active.job.snapshot_version = 0;
            for index in info.indices.iter_deref() {
                let mut index = index.write();
                if index.state == SchemaState::WRITE_ONLY {
                    index.state = SchemaState::WRITE_REORGANIZATION;
                }
            }
            active.job.schema_state = SchemaState::WRITE_REORGANIZATION;
            partition.write().ddl_state = SchemaState::WRITE_REORGANIZATION;
            let write = reorg_table_step(catalog, active, database_id, &info, start_ts)?;
            Ok(action_step(write))
        }
        SchemaState::WRITE_REORGANIZATION => {
            match do_partition_reorg_work(snapshot, catalog, active, &info, reorg_outcome)? {
                PartitionReorgWork::Step(step) => return Ok(step),
                PartitionReorgWork::Failed(error) => {
                    return rollback_reorganize_partition_with_err(
                        catalog, active, &args, start_ts, error,
                    )
                }
                PartitionReorgWork::Done => {}
            }
            let changed = changed_indexes(&partition.read());
            for index in info.indices.iter_deref() {
                let mut index = index.write();
                match changed.get(&index.id) {
                    // The new index replaces the old one ...
                    Some(true) => index.state = SchemaState::PUBLIC,
                    // ... which is kept up to date in case of a rollback.
                    Some(false) => index.state = SchemaState::WRITE_ONLY,
                    None => {}
                }
            }
            let new_definitions = {
                let current = partition.read();
                let (first, last, replaced) = get_replaced_partition_ids(&part_names, &current)?;
                get_reorganized_definitions(&current, first, last, &replaced)
            };
            {
                let mut partition = partition.write();
                partition.num = new_definitions.len() as u64;
                partition.definitions = new_definitions.into();
                if active.job.type_ != ActionType::ACTION_REORGANIZE_PARTITION {
                    let partition = &mut *partition;
                    std::mem::swap(&mut partition.partition_type, &mut partition.ddl_type);
                    std::mem::swap(&mut partition.expr, &mut partition.ddl_expr);
                    std::mem::swap(&mut partition.columns, &mut partition.ddl_columns);
                }
            }
            // The copies are done, but other nodes still see the dropping
            // definitions as ordinary ones: one more version of double
            // writes.
            active.job.schema_state = SchemaState::DELETE_REORGANIZATION;
            partition.write().ddl_state = SchemaState::DELETE_REORGANIZATION;
            let write = reorg_table_step(catalog, active, database_id, &info, start_ts)?;
            Ok(action_step(write))
        }
        SchemaState::DELETE_REORGANIZATION => {
            // The replaced indexes only take deletions from here on.
            let changed = changed_indexes(&partition.read());
            for index in info.indices.iter_deref() {
                let mut index = index.write();
                if changed.get(&index.id) == Some(&false) {
                    index.state = SchemaState::DELETE_ONLY;
                }
            }
            active.job.schema_state = SchemaState::PUBLIC;
            partition.write().ddl_state = SchemaState::PUBLIC;
            let write = reorg_table_step(catalog, active, database_id, &info, start_ts)?;
            Ok(action_step(write))
        }
        SchemaState::PUBLIC => {
            let (dropped, added) = {
                let current = partition.read();
                (
                    current.dropping_definitions.snapshot(),
                    current.adding_definitions.snapshot(),
                )
            };
            let mut physical_table_ids = dropped
                .iter()
                .map(|definition| definition.id)
                .collect::<Vec<_>>();
            let new_ids = part_info
                .definitions
                .snapshot()
                .iter()
                .map(|definition| definition.id)
                .collect::<Vec<_>>();
            {
                let mut partition = partition.write();
                partition.dropping_definitions = Vec::new().into();
                partition.adding_definitions = Vec::new().into();
                partition.ddl_state = SchemaState::NONE;
                partition.ddl_action = ActionType::ACTION_NONE;
                partition.original_partition_ids_order = Vec::new().into();
            }
            // The replaced indexes go (Go `onDropIndex`'s last step); the
            // global ones' entries are left to the delete range.
            let mut old_global_indexes = Vec::new();
            let dropped_indexes = info
                .indices
                .iter_deref()
                .filter(|index| index.read().state == SchemaState::DELETE_ONLY)
                .map(|index| index.read().clone_like_go())
                .collect::<Vec<_>>();
            for index in &dropped_indexes {
                super::add_index::remove_index_and_hidden_columns(&mut info, index);
                if index.global {
                    old_global_indexes.push(tidb_model::TableIDIndexID {
                        table_id: info.id,
                        index_id: index.id,
                    });
                }
            }
            let mut mutations = Vec::new();
            let mut old_table_id = 0;
            if active.job.type_ != ActionType::ACTION_REORGANIZE_PARTITION {
                // PARTITION BY / REMOVE PARTITIONING: the table is recreated
                // under its new id, its auto-ID counters carried across.
                old_table_id = info.id;
                let new_table_id = part_info.new_table_id;
                mutations.push(BufferMutation::delete(key::table_kv_key(
                    database_id,
                    old_table_id,
                ))?);
                for key_for in [
                    key::auto_table_id_kv_key as fn(i64, i64) -> Vec<u8>,
                    key::auto_increment_id_kv_key,
                    key::auto_random_table_id_kv_key,
                ] {
                    let counter = read_auto_id(snapshot, &key_for(database_id, old_table_id))?;
                    mutations.push(BufferMutation::set(
                        key_for(database_id, new_table_id),
                        value::encode_int_value(counter),
                    )?);
                }
                info.id = new_table_id;
                if physical_table_ids.first() != Some(&old_table_id) {
                    physical_table_ids.push(old_table_id);
                }
                if active.job.type_ == ActionType::ACTION_REMOVE_PARTITIONING {
                    info.partition = None;
                } else if let Some(partition) = &info.partition {
                    partition.write().clear_reorg_intermediate_info();
                }
            }
            // The bundles of the final partitions.
            let bundles =
                super::partition::alter_table_partition_bundles(snapshot, &info, Vec::new())?;
            active.job.schema_state = SchemaState::NONE;
            // `SetSchemaDiffForReorganizePartition` /
            // `SetSchemaDiffForPartitionModify`.
            let width = physical_table_ids.len().max(new_ids.len());
            let affects = if new_ids.is_empty() {
                Vec::new()
            } else {
                (0..width)
                    .map(|offset| tidb_model::AffectedOption {
                        old_table_id: physical_table_ids.get(offset).copied().unwrap_or(0),
                        table_id: new_ids.get(offset).copied().unwrap_or(0),
                        ..tidb_model::AffectedOption::default()
                    })
                    .collect::<Vec<_>>()
            };
            let modifies = active.job.type_ != ActionType::ACTION_REORGANIZE_PARTITION;
            let job_table_id = active.job.table_id;
            let mut write = table_step_with_diff(
                catalog,
                active,
                database_id,
                &info,
                true,
                start_ts,
                |diff| {
                    diff.affected_options = affects.into();
                    if modifies {
                        diff.old_table_id = job_table_id;
                    }
                },
            )?;
            mutations.append(&mut write.mutations);
            write.mutations = mutations;
            if !tidb_metadef::is_mem_or_sys_db(&schema.go_to_lower()) {
                let event = match active.job.type_ {
                    ActionType::ACTION_REORGANIZE_PARTITION => {
                        SchemaChangeEvent::reorganize_partitions(
                            info.clone_like_go(),
                            event_partition_info(added),
                            event_partition_info(dropped),
                        )
                    }
                    ActionType::ACTION_ALTER_TABLE_PARTITIONING => {
                        SchemaChangeEvent::add_partitioning(
                            old_table_id,
                            info.clone_like_go(),
                            event_partition_info(added),
                        )
                    }
                    _ => SchemaChangeEvent::remove_partitioning(
                        old_table_id,
                        info.clone_like_go(),
                        event_partition_info(dropped),
                    ),
                };
                append_schema_change_mutations(
                    catalog,
                    active.job.id,
                    &[(-1, event)],
                    &mut write.mutations,
                    row_ids,
                )?;
            }
            active.job.finish_table_job(
                JobState::DONE,
                SchemaState::NONE,
                write.schema_version,
                Some(GoShared::new(info)),
            );
            {
                let args = args.write();
                args.old_physical_table_ids
                    .set(GoSharedSlice::from_vec(physical_table_ids));
                args.new_partition_ids.set(GoSharedSlice::from_vec(new_ids));
                args.old_global_indexes
                    .set(GoSharedSlice::from_vec(old_global_indexes));
            }
            active.job.fill_finished_args(Some(args));
            write.placement_bundles = bundles;
            Ok(action_step(write))
        }
        state => Err(DdlPlanError::from(
            tidb_util::dbterror::ERR_INVALID_DDL_STATE.generate_with_stack_by_args(&[
                FormatArg::from("partition"),
                FormatArg::new(state.to_string(), state.0.to_string(), "model.SchemaState"),
            ]),
        )),
    }
}

enum PartitionReorgWork {
    Step(PersistedDdlActionStep),
    Done,
    Failed(DdlPlanError),
}

/// The partitions and indexes each reorganization phase covers.
fn phase_scope(info: &TableInfo, phase: PartitionReorgPhase) -> (Vec<i64>, Vec<i64>) {
    let partition = info.partition.as_ref().expect("a reorganized table").read();
    let ids = |definitions: &GoSharedSlice<tidb_model::partition::PartitionDefinition>| {
        definitions
            .snapshot()
            .iter()
            .map(|definition| definition.id)
            .collect::<Vec<_>>()
    };
    let changed = changed_indexes(&partition);
    match phase {
        PartitionReorgPhase::CopyRows => (ids(&partition.dropping_definitions), Vec::new()),
        // Every index but the replaced ones (`DDLChangedIndex` false).
        PartitionReorgPhase::AddingIndexes => (
            ids(&partition.adding_definitions),
            info.indices
                .iter_deref()
                .map(|index| index.read().id)
                .filter(|id| changed.get(id) != Some(&false))
                .collect(),
        ),
        // The new global indexes, over the partitions neither added nor
        // dropped.
        PartitionReorgPhase::UntouchedGlobalIndexes => {
            let adding = ids(&partition.adding_definitions);
            let dropping = ids(&partition.dropping_definitions);
            (
                ids(&partition.definitions)
                    .into_iter()
                    .filter(|id| !adding.contains(id) && !dropping.contains(id))
                    .collect(),
                info.indices
                    .iter_deref()
                    .filter(|index| {
                        let index = index.read();
                        index.global && index.state == SchemaState::WRITE_REORGANIZATION
                    })
                    .map(|index| index.read().id)
                    .collect(),
            )
        }
    }
}

/// Go `doPartitionReorgWork` (`reorgPartitionDataAndIndex`): the rows of the
/// dropping partitions copied into the adding ones, then every index built
/// on the adding partitions, then the new global indexes filled from the
/// untouched partitions, each a resumable reorganization the owner runs
/// between steps.
fn do_partition_reorg_work<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    info: &TableInfo,
    outcome: Option<&ReorgOutcome>,
) -> Result<PartitionReorgWork, DdlPlanError> {
    // Go's `job.ReorgMeta.ReorgTp = model.ReorgTypeTxn`.
    if let Some(meta) = &active.job.reorg_meta {
        meta.write().reorg_type = tidb_model::ReorgType::TXN;
    }
    let request = |active: &crate::ddl_job_table::ActiveDdlJob,
                   phase: PartitionReorgPhase,
                   stage: ReorgStageRequest| {
        let (physical_ids, index_ids) = phase_scope(info, phase);
        AddIndexReorg {
            job_id: active.job.id,
            stage,
            table: Box::new(info.clone_like_go()),
            index_ids,
            merging: false,
            physical_ids,
            batch_size: job_batch_size(&active.job),
            session_vars: ReorgSessionVars::of_job(&active.job),
            row_count: active.job.row_count,
            column: None,
            sub_job: reorg_sub_job(&active.job),
            clean_up_global_index: false,
            partition_phase: Some(phase),
            schema_id: active.job.schema_id,
        }
    };
    // A phase with nothing to cover is skipped.
    let has_work = |phase: PartitionReorgPhase| {
        let (physical_ids, index_ids) = phase_scope(info, phase);
        !physical_ids.is_empty()
            && (phase == PartitionReorgPhase::CopyRows || !index_ids.is_empty())
    };
    let next_phase = |phase: PartitionReorgPhase| match phase {
        PartitionReorgPhase::CopyRows => Some(PartitionReorgPhase::AddingIndexes),
        PartitionReorgPhase::AddingIndexes => Some(PartitionReorgPhase::UntouchedGlobalIndexes),
        PartitionReorgPhase::UntouchedGlobalIndexes => None,
    };
    let start_from = |active: &mut crate::ddl_job_table::ActiveDdlJob,
                      mut phase: Option<PartitionReorgPhase>| {
        while let Some(current) = phase {
            if has_work(current) {
                let reorg = request(active, current, ReorgStageRequest::Init);
                return Some(reorg_step(&active.job, reorg));
            }
            phase = next_phase(current);
        }
        None
    };
    let outcome = outcome
        .filter(|outcome| outcome.job_id == active.job.id && outcome.partition_phase.is_some());
    match outcome.map(|outcome| (&outcome.result, outcome.partition_phase)) {
        // Go `getReorgInfoFromPartitions`: the first visit persists the
        // snapshot and the handle.
        None if active.job.snapshot_version == 0 => Ok(
            match start_from(active, Some(PartitionReorgPhase::CopyRows)) {
                Some(step) => PartitionReorgWork::Step(step),
                None => PartitionReorgWork::Done,
            },
        ),
        Some((ReorgResult::Initialized(version), _)) => {
            active.job.snapshot_version = *version;
            // The count of the phases before this one, not yet persisted.
            active.job.row_count =
                outcome.map_or(active.job.row_count, |outcome| outcome.row_count);
            Ok(PartitionReorgWork::Step(action_step(job_only_write(
                active.job.id,
                Vec::new(),
            ))))
        }
        None | Some((ReorgResult::Interrupted, _)) => {
            let handle = load_reorg_handle(snapshot, catalog, active)?;
            // The handle names the phase it is in.
            let adding = phase_scope(info, PartitionReorgPhase::AddingIndexes).0;
            let phase = if handle.element_type == COLUMN_ELEMENT_KEY {
                PartitionReorgPhase::CopyRows
            } else if adding.contains(&handle.physical_id) {
                PartitionReorgPhase::AddingIndexes
            } else {
                PartitionReorgPhase::UntouchedGlobalIndexes
            };
            let reorg = request(active, phase, ReorgStageRequest::Run(handle));
            Ok(PartitionReorgWork::Step(reorg_step(&active.job, reorg)))
        }
        Some((ReorgResult::Progress(rows), _)) => {
            active.job.row_count = *rows;
            Ok(PartitionReorgWork::Step(action_step(job_only_write(
                active.job.id,
                Vec::new(),
            ))))
        }
        Some((ReorgResult::Skipped | ReorgResult::Done(_), phase)) => {
            active.job.row_count = match outcome.map(|outcome| (&outcome.result, outcome.row_count))
            {
                Some((ReorgResult::Done(rows), _)) => *rows,
                Some((_, started)) => started,
                None => active.job.row_count,
            };
            let phase = phase.expect("filtered to partition phases");
            Ok(match start_from(active, next_phase(phase)) {
                Some(step) => PartitionReorgWork::Step(step),
                None => PartitionReorgWork::Done,
            })
        }
        Some((
            ReorgResult::Failed {
                code,
                message,
                rows,
            },
            _,
        )) => {
            active.job.row_count = *rows;
            Ok(PartitionReorgWork::Failed(checked_refusal(
                *code,
                message.clone(),
            )))
        }
        Some((ReorgResult::Checked(_), _)) => Err(DdlPlanError::Encode(
            "a partition reorganization got a check's outcome".to_owned(),
        )),
    }
}

/// Go `rollbackReorganizePartitionWithErr` (`convertReorgPartitionJob2RollbackJob`):
/// the job turns to roll back, the adding partitions to be dropped by
/// `rollbackLikeDropPartition`.
fn rollback_reorganize_partition_with_err(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    args: &GoShared<tidb_model::TablePartitionArgs>,
    start_ts: u64,
    error: DdlPlanError,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    if active.job.schema_state == SchemaState::NONE {
        return Err(cancel_ddl_job(active, error));
    }
    let mut info = checked_job_table(catalog, active, true)?.clone_like_go();
    let partition = info
        .partition
        .clone()
        .expect("a reorganizing table is partitioned");
    let (part_names, changed, ddl_state) = {
        let current = partition.read();
        (
            current
                .adding_definitions
                .snapshot()
                .iter()
                .map(|definition| definition.name.lowercase().to_owned())
                .collect::<Vec<_>>(),
            changed_indexes(&current),
            current.ddl_state,
        )
    };
    let mut dropped_indexes = Vec::new();
    for index in info.indices.iter_deref() {
        let mut index = index.write();
        if !index.unique {
            continue;
        }
        match changed.get(&index.id) {
            None => {}
            Some(false) => {
                if ddl_state == SchemaState::DELETE_REORGANIZATION {
                    index.state = SchemaState::PUBLIC;
                }
            }
            Some(true) => {
                if ddl_state == SchemaState::DELETE_REORGANIZATION {
                    index.state = SchemaState::WRITE_ONLY;
                } else {
                    dropped_indexes.push(index.clone_like_go());
                }
            }
        }
    }
    for index in &dropped_indexes {
        super::add_index::remove_index_and_hidden_columns(&mut info, index);
    }
    if ddl_state == SchemaState::DELETE_REORGANIZATION {
        let mut current = partition.write();
        if active.job.type_ == ActionType::ACTION_REORGANIZE_PARTITION {
            // Back to the original order, which only
            // `OriginalPartitionIDsOrder` records for a LIST table.
            let definitions = current.definitions.snapshot();
            let dropping = current.dropping_definitions.snapshot();
            let order = current.original_partition_ids_order.snapshot();
            let mut restored = Vec::with_capacity(order.len());
            for id in &order {
                if let Some(definition) = definitions
                    .iter()
                    .chain(dropping.iter())
                    .find(|definition| definition.id == *id)
                {
                    restored.push(definition.clone_like_go());
                }
            }
            if restored.len() != order.len() {
                return Err(DdlPlanError::Encode(
                    "Internal error, failed to find original partition definitions".to_owned(),
                ));
            }
            current.definitions = restored.into();
        } else {
            let current = &mut *current;
            std::mem::swap(&mut current.partition_type, &mut current.ddl_type);
            std::mem::swap(&mut current.expr, &mut current.ddl_expr);
            std::mem::swap(&mut current.columns, &mut current.ddl_columns);
            current.definitions = current.dropping_definitions.clone();
        }
        current.num = current.definitions.len() as u64;
        // One state back: other sessions may see the new partitions.
        active.job.schema_state = SchemaState::WRITE_REORGANIZATION;
        current.ddl_state = SchemaState::WRITE_REORGANIZATION;
    }
    args.read().part_names.set(GoSharedSlice::from_vec(
        part_names.into_iter().map(Into::into).collect(),
    ));
    active.job.fill_args(Some(args.clone()));
    let database_id = active.job.schema_id;
    let write = reorg_table_step(catalog, active, database_id, &info, start_ts)?;
    active.job.state = JobState::ROLLINGBACK;
    Ok(PersistedDdlActionStep {
        write,
        update_raw_args: true,
        run_error: Some(error),
        reorg: None,
    })
}

/// Go `onRollbackReorganizePartition`: past public the old indexes are being
/// destroyed, so the job rolls on.
pub(super) fn plan_cancel_reorganize_partition(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let args = tidb_model::get_table_partition_args(&mut active.job)
        .map_err(|error| cancel_ddl_job(active, DdlPlanError::Encode(error.to_string())))?
        .ok_or_else(|| {
            cancel_ddl_job(
                active,
                DdlPlanError::Encode("partition reorganization has nil args".to_owned()),
            )
        })?;
    if active.job.schema_state == SchemaState::PUBLIC {
        active.job.state = JobState::RUNNING;
        return Ok(job_control_step(&active.job));
    }
    rollback_reorganize_partition_with_err(
        catalog,
        active,
        &args,
        start_ts,
        DdlPlanError::from(
            tidb_util::dbterror::ERR_CANCELLED_DDL_JOB
                .generate(tidb_error::tidb::errname::ErrCancelledDDLJob.raw),
        ),
    )
}
