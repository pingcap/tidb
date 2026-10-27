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
use tidb_model::JobArgs;

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
            | DdlStatement::RebaseAutoIncrementId { .. }
            | DdlStatement::RebaseAutoRandom { .. }
            | DdlStatement::AlterTtl { .. }
            | DdlStatement::RemoveTtl { .. }
            | DdlStatement::AlterTablePlacement { .. }
            | DdlStatement::AlterTablePartitionPlacement { .. }
            | DdlStatement::AlterTableAttributes { .. }
            | DdlStatement::AlterTablePartitionAttributes { .. }
            | DdlStatement::AlterTableAffinity { .. }
            | DdlStatement::AlterTableSetRegionSplitPolicy { .. }
            | DdlStatement::SetTiFlashReplica { .. }
            | DdlStatement::UpdateTiFlashReplicaStatus { .. }
            | DdlStatement::AlterTableCache { .. }
            | DdlStatement::ModifyEngineAttribute { .. }
    )
}

/// The actions [`plan_persisted_alter_table_job_step`] runs.
pub(crate) const ALTER_TABLE_ACTIONS: [ActionType; 23] = [
    ActionType::ACTION_MODIFY_TABLE_COMMENT,
    ActionType::ACTION_ALTER_INDEX_VISIBILITY,
    ActionType::ACTION_RENAME_INDEX,
    ActionType::ACTION_DROP_FOREIGN_KEY,
    ActionType::ACTION_MODIFY_TABLE_CHARSET_AND_COLLATE,
    ActionType::ACTION_SET_DEFAULT_VALUE,
    ActionType::ACTION_SHARD_ROW_ID,
    ActionType::ACTION_MODIFY_TABLE_AUTO_IDCACHE,
    ActionType::ACTION_REBASE_AUTO_ID,
    ActionType::ACTION_REBASE_AUTO_RANDOM_BASE,
    ActionType::ACTION_ALTER_TTLINFO,
    ActionType::ACTION_ALTER_TTLREMOVE,
    ActionType::ACTION_ALTER_TABLE_PLACEMENT,
    ActionType::ACTION_ALTER_TABLE_PARTITION_PLACEMENT,
    ActionType::ACTION_ALTER_TABLE_ATTRIBUTES,
    ActionType::ACTION_ALTER_TABLE_PARTITION_ATTRIBUTES,
    ActionType::ACTION_ALTER_TABLE_AFFINITY,
    ActionType::ACTION_ALTER_TABLE_SET_REGION_SPLIT_POLICY,
    ActionType::ACTION_SET_TI_FLASH_REPLICA,
    ActionType::ACTION_UPDATE_TI_FLASH_REPLICA_STATUS,
    ActionType::ACTION_ALTER_CACHE_TABLE,
    ActionType::ACTION_ALTER_NO_CACHE_TABLE,
    ActionType::ACTION_MODIFY_ENGINE_ATTRIBUTE,
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
    job_context: &dyn DdlSessionContext,
    start_ts: u64,
    upgrading: bool,
    min_job_id: i64,
    tiflash_stores: &dyn Fn() -> Result<u64, String>,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    let catalog = load_cluster_catalog(snapshot)?;
    let built = build_alter_table_job(
        snapshot,
        &catalog,
        statement,
        job_context,
        false,
        tiflash_stores,
    )?;
    submit_built_job(snapshot, &catalog, built, start_ts, upgrading, min_job_id)
}

/// The executor half without `doDDLJob2`: the job unsubmitted, which a
/// multi-schema change appends as a sub-job (Go `appendToSubJobs`).
///
/// `multi_schema` is Go's `StmtCtx.MultiSchemaInfo != nil`: the job is one
/// sub-job of a multi-specification `ALTER TABLE`.
pub(super) fn build_alter_table_job<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    statement: &DdlStatement,
    job_context: &dyn DdlSessionContext,
    multi_schema: bool,
    tiflash_stores: &dyn Fn() -> Result<u64, String>,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    if !is_alter_table_statement(statement) {
        return Ok(None);
    }
    let mut warnings = Vec::new();
    // Go's placement jobs lock the policy they name, shared.
    let mut shared_policy: Option<String> = None;
    let (action, schema, table, args) = match statement {
        // Go `AlterTableEngineAttribute`; the attribute was validated at
        // lowering.
        DdlStatement::ModifyEngineAttribute {
            schema,
            table,
            engine_attribute,
        } => (
            ActionType::ACTION_MODIFY_ENGINE_ATTRIBUTE,
            schema,
            table,
            JobArgsValue::ModifyTableEngineAttribute(Some(GoShared::new(
                tidb_model::ModifyTableEngineAttributeArgs {
                    engine_attribute: GoField::new(engine_attribute.clone().into()),
                },
            ))),
        ),
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
            // Go `validateAlterIndexVisibility` early-returns on an unchanged
            // visibility only outside a multi-schema change; inside one the
            // sub-job is always built, so `checkMultiSchemaInfo` sees it.
            if let AlterColumnOutcome::AlreadySatisfied(detail) =
                apply_index_visibility(&mut probe, schema, table, index, *invisible)?
            {
                if !multi_schema {
                    return Ok(already_satisfied(detail));
                }
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
                    &tidb_util::dbterror::ERR_OPT_ON_TEMPORARY_TABLE,
                    "`shard_row_id_bits` is unsupported on temporary tables.".to_owned(),
                ));
            }
            if *bits == stored.shard_row_id_bits {
                return Ok(already_satisfied(format!(
                    "`{schema}`.`{table}` already has SHARD_ROW_ID_BITS = {bits}"
                )));
            }
            if *bits > 0 && (stored.pk_is_handle || stored.is_common_handle) {
                return Err(DdlPlanError::Admission(DdlAdmissionError::with_proto(
                    &tidb_util::dbterror::ERR_UNSUPPORTED_SHARD_ROW_ID_BITS,
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
        // Go `executor.RebaseAutoID` for the auto-increment allocator.
        DdlStatement::RebaseAutoIncrementId {
            schema,
            table,
            new_base,
            force,
        } => {
            let (schema_id, stored) = locate_table(catalog, schema, table)?;
            let mut new_base = *new_base;
            if !*force {
                let adjusted = adjust_new_base_to_next_global_id(
                    snapshot,
                    AutoIdKind::Increment,
                    schema_id,
                    stored,
                    new_base,
                )?;
                if adjusted != new_base {
                    warnings.push(tidb_util::context::SqlWarn::with_code(
                        DdlWarningLevel::Warning,
                        tidb_error::mysql::errcode::ErrUnknown,
                        cant_reset_auto_increment(new_base, adjusted),
                    ));
                }
                new_base = adjusted;
            }
            (
                ActionType::ACTION_REBASE_AUTO_ID,
                schema,
                table,
                JobArgsValue::RebaseAutoId(Some(GoShared::new(tidb_model::RebaseAutoIDArgs {
                    new_base: GoField::new(new_base),
                    force: GoField::new(*force),
                }))),
            )
        }
        // Go `executor.RebaseAutoID` for the auto-random allocator.
        DdlStatement::RebaseAutoRandom {
            schema,
            table,
            next,
            force,
        } => {
            let (schema_id, stored) = locate_table(catalog, schema, table)?;
            if stored.auto_random_bits == 0 || stored.get_pk_col_info().is_none() {
                return Err(DdlPlanError::InvalidAutoRandom(
                    "alter auto_random_base of a non auto_random table".to_owned(),
                ));
            }
            let unsigned = stored.is_auto_random_bit_col_unsigned();
            let range_bits = if stored.auto_random_range_bits == 0 {
                64
            } else {
                stored.auto_random_range_bits
            };
            let incremental_bits = range_bits - stored.auto_random_bits - u64::from(!unsigned);
            let maximum = (1_u64 << incremental_bits) - 1;
            let requested = *next as u64;
            if *next < 0 || requested & maximum != requested {
                return Err(DdlPlanError::InvalidAutoRandom(format!(
                    "alter auto_random_base to {next} overflows the incremental bits, max allowed base is {maximum}"
                )));
            }
            let mut new_base = *next;
            if !*force {
                let adjusted = adjust_new_base_to_next_global_id(
                    snapshot,
                    AutoIdKind::Random,
                    schema_id,
                    stored,
                    new_base,
                )?;
                if adjusted != new_base {
                    warnings.push(tidb_util::context::SqlWarn::with_code(
                        DdlWarningLevel::Warning,
                        tidb_error::mysql::errcode::ErrUnknown,
                        cant_reset_auto_increment(new_base, adjusted),
                    ));
                }
                new_base = adjusted;
            }
            (
                ActionType::ACTION_REBASE_AUTO_RANDOM_BASE,
                schema,
                table,
                JobArgsValue::RebaseAutoId(Some(GoShared::new(tidb_model::RebaseAutoIDArgs {
                    new_base: GoField::new(new_base),
                    force: GoField::new(*force),
                }))),
            )
        }
        // Go `executor.AlterTableTTLInfoOrEnable`.
        DdlStatement::AlterTtl {
            schema,
            table,
            ttl_info,
            enable,
            job_interval,
        } => {
            let (_, stored) = locate_table(catalog, schema, table)?;
            match ttl_info {
                Some(ttl) => {
                    let mut candidate = stored.clone_like_go();
                    candidate.ttl_info = Some(GoShared::new((**ttl).clone()));
                    let referred = !crate::foreign_key_build::get_table_referred_foreign_keys(
                        catalog, schema, table,
                    )
                    .is_empty();
                    tidb_ddl_build::table_info_build::check_ttl_info_valid(&candidate, referred)
                        .map_err(DdlPlanError::from_mysql_error)?;
                }
                None if stored.ttl_info.is_none() => {
                    let option = if enable.is_some() {
                        "TTL_ENABLE"
                    } else {
                        "TTL_JOB_INTERVAL"
                    };
                    let error =
                        tidb_ddl_build::errors::set_ttl_option_for_non_ttl_table(option);
                    return Err(DdlPlanError::from_mysql_error(error));
                }
                None => {}
            }
            (
                ActionType::ACTION_ALTER_TTLINFO,
                schema,
                table,
                JobArgsValue::AlterTtlInfo(Some(GoShared::new(tidb_model::AlterTTLInfoArgs {
                    ttl_info: ttl_info.as_ref().map(|ttl| GoShared::new((**ttl).clone())),
                    ttl_enable: *enable,
                    ttl_cron_job_schedule: job_interval.clone(),
                }))),
            )
        }
        // Go `executor.AlterTableRemoveTTL`: no job for a table without TTL.
        DdlStatement::RemoveTtl { schema, table } => {
            let (_, stored) = locate_table(catalog, schema, table)?;
            if stored.ttl_info.is_none() {
                return Ok(already_satisfied(format!(
                    "`{schema}`.`{table}` has no TTL"
                )));
            }
            (
                ActionType::ACTION_ALTER_TTLREMOVE,
                schema,
                table,
                tidb_model::EmptyArgs::into_job_args_value(Some(GoShared::new(
                    tidb_model::EmptyArgs::default(),
                ))),
            )
        }
        // Go `executor.AlterTablePlacement`.
        DdlStatement::AlterTablePlacement {
            schema,
            table,
            policy,
        } => {
            let (_, stored) = locate_table(catalog, schema, table)?;
            if stored.temp_table_type != tidb_model::TempTableType::NONE {
                return Err(coded_refusal(
                    &tidb_util::dbterror::ERR_OPT_ON_TEMPORARY_TABLE,
                    "`placement` is unsupported on temporary tables.".to_owned(),
                ));
            }
            let reference = check_and_normalize_placement_policy(snapshot, policy)?;
            shared_policy = reference
                .as_ref()
                .map(|reference| reference.read().name.lowercase().to_owned());
            (
                ActionType::ACTION_ALTER_TABLE_PLACEMENT,
                schema,
                table,
                JobArgsValue::AlterTablePlacement(Some(GoShared::new(
                    tidb_model::AlterTablePlacementArgs {
                        placement_policy_ref: reference,
                    },
                ))),
            )
        }
        // Go `executor.AlterTablePartitionPlacement`.
        DdlStatement::AlterTablePartitionPlacement {
            schema,
            table,
            partition,
            policy,
        } => {
            let (_, stored) = locate_table(catalog, schema, table)?;
            let partition_id = find_partition_id(stored, partition)?;
            let reference = check_and_normalize_placement_policy(snapshot, policy)?;
            shared_policy = reference
                .as_ref()
                .map(|reference| reference.read().name.lowercase().to_owned());
            (
                ActionType::ACTION_ALTER_TABLE_PARTITION_PLACEMENT,
                schema,
                table,
                JobArgsValue::AlterTablePartition(Some(GoShared::new(
                    tidb_model::AlterTablePartitionArgs {
                        partition_id: GoField::new(partition_id),
                        policy_ref_info: GoField::new(reference),
                        ..tidb_model::AlterTablePartitionArgs::default()
                    },
                ))),
            )
        }
        // Go `executor.AlterTableAttributes`: the rule over the table and
        // every partition.
        DdlStatement::AlterTableAttributes {
            schema,
            table,
            attributes,
        } => {
            let Some(database) = find_database(catalog, schema) else {
                return Err(DdlPlanError::UnknownDatabase(schema.clone()));
            };
            let (_, stored) = locate_table(catalog, schema, table)?;
            let rule = label_rules::table_attributes_rule(
                attributes,
                database.info.name.lowercase(),
                &LabelRuleTable::from_table_info(stored),
            )
            .map_err(invalid_attributes_spec)?;
            (
                ActionType::ACTION_ALTER_TABLE_ATTRIBUTES,
                schema,
                table,
                JobArgsValue::AlterTableAttributes(Some(GoShared::new(
                    tidb_model::AlterTableAttributesArgs {
                        label_rule: Some(GoShared::new(label_rule_json(&rule)?)),
                    },
                ))),
            )
        }
        // Go `executor.AlterTablePartitionAttributes`.
        DdlStatement::AlterTablePartitionAttributes {
            schema,
            table,
            partition,
            attributes,
        } => {
            let Some(database) = find_database(catalog, schema) else {
                return Err(DdlPlanError::UnknownDatabase(schema.clone()));
            };
            let (_, stored) = locate_table(catalog, schema, table)?;
            let partition_id = find_partition_id(stored, partition)?;
            let rule = label_rules::partition_attributes_rule(
                attributes,
                database.info.name.lowercase(),
                stored.name.lowercase(),
                &partition.go_to_lower(),
                partition_id,
            )
            .map_err(invalid_attributes_spec)?;
            (
                ActionType::ACTION_ALTER_TABLE_PARTITION_ATTRIBUTES,
                schema,
                table,
                JobArgsValue::AlterTablePartition(Some(GoShared::new(
                    tidb_model::AlterTablePartitionArgs {
                        partition_id: GoField::new(partition_id),
                        label_rule: GoField::new(Some(GoShared::new(label_rule_json(&rule)?))),
                        ..tidb_model::AlterTablePartitionArgs::default()
                    },
                ))),
            )
        }
        // Go `executor.AlterTableAffinity`.
        DdlStatement::AlterTableAffinity {
            schema,
            table,
            level,
        } => {
            let (_, stored) = locate_table(catalog, schema, table)?;
            refuse_non_base_table(stored, schema, table)?;
            let affinity = tidb_ddl_build::table_affinity(level).map_err(driver_refusal)?;
            tidb_ddl_build::validate_table_affinity(
                stored.temp_table_type != tidb_model::TempTableType::NONE,
                stored.get_partition_info().is_some(),
                affinity.as_ref(),
            )
            .map_err(driver_refusal)?;
            (
                ActionType::ACTION_ALTER_TABLE_AFFINITY,
                schema,
                table,
                JobArgsValue::AlterTableAffinity(Some(GoShared::new(
                    tidb_model::AlterTableAffinityArgs {
                        affinity: affinity.map(GoShared::new),
                    },
                ))),
            )
        }
        // Go `executor.AlterTableSetRegionSplitPolicy`.
        DdlStatement::AlterTableSetRegionSplitPolicy {
            schema,
            table,
            target,
            option,
        } => {
            let (_, stored) = locate_table(catalog, schema, table)?;
            let target = match target {
                tidb_ast::SplitTarget::Table => tidb_ddl_build::split_region::SplitPolicyTarget::Table,
                tidb_ast::SplitTarget::PrimaryKey => {
                    tidb_ddl_build::split_region::SplitPolicyTarget::PrimaryKey
                }
                tidb_ast::SplitTarget::Index(name) => {
                    tidb_ddl_build::split_region::SplitPolicyTarget::Index(name)
                }
            };
            let (index_name, policy) =
                tidb_ddl_build::split_region::normalize_split_policy(target, option, stored)
                    .map_err(driver_refusal)?;
            (
                ActionType::ACTION_ALTER_TABLE_SET_REGION_SPLIT_POLICY,
                schema,
                table,
                JobArgsValue::AlterTableSetRegionSplitPolicy(Some(GoShared::new(
                    tidb_model::AlterTableSetRegionSplitPolicyArgs {
                        index_name,
                        policy: Some(GoShared::new(policy)),
                    },
                ))),
            )
        }
        // Go `executor.AlterTableSetTiFlashReplica`.
        DdlStatement::SetTiFlashReplica {
            schema,
            table,
            count,
            labels,
            reset_available,
            hypo,
        } => {
            let (_, stored) = locate_table(catalog, schema, table)?;
            tidb_ddl_build::ddl_executor::is_table_tiflash_supported(schema, stored)
                .map_err(DdlPlanError::from_mysql_error)?;
            let current = stored.tiflash_replica.as_ref().map(|replica| replica.read().clone());
            if !tidb_ddl_build::ddl_executor::should_modify_tiflash_replica(
                current.as_ref(),
                *count,
                labels,
            ) {
                return Ok(already_satisfied(format!(
                    "`{schema}`.`{table}` already has this TiFlash replica"
                )));
            }
            // Go `setHypoTiFlashReplica`: session state, no job.
            if *hypo {
                job_context.set_hypo_tiflash_replica(
                    &schema.go_to_lower(),
                    stored.name.lowercase(),
                    *count > 0,
                );
                return Ok(already_satisfied(format!(
                    "hypothetical TiFlash replica of `{schema}`.`{table}` recorded"
                )));
            }
            // Go `checkTiFlashReplicaCount`: with TiFlash enabled, no more
            // replicas than PD's TiFlash stores.
            check_tiflash_replica_count(*count, tiflash_stores)?;
            (
            ActionType::ACTION_SET_TI_FLASH_REPLICA,
            schema,
            table,
            JobArgsValue::SetTiFlashReplica(Some(GoShared::new(
                tidb_model::SetTiFlashReplicaArgs {
                    tiflash_replica: tidb_model::JobTiFlashReplicaSpec {
                        count: *count,
                        labels: Some(labels.clone()),
                        hypo: false,
                    },
                    reset_available: *reset_available,
                    ..tidb_model::SetTiFlashReplicaArgs::default()
                },
            ))),
            )
        }
        // Go `executor.UpdateTableReplicaInfo`: no job once the status
        // already holds.
        DdlStatement::UpdateTiFlashReplicaStatus {
            table_id,
            available,
        } => {
            let Some((database, stored)) = catalog.databases.iter().find_map(|database| {
                database
                    .tables
                    .iter()
                    .find(|stored| stored.id == *table_id || partition_of(stored, *table_id))
                    .map(|stored| (database, stored))
            }) else {
                return Err(DdlPlanError::TableNotExists {
                    schema: String::new(),
                    table: format!("(physical id {table_id})"),
                });
            };
            let unchanged = stored.tiflash_replica.as_ref().is_none_or(|replica| {
                let replica = replica.read();
                if stored.id == *table_id {
                    replica.available == *available
                } else {
                    replica.is_partition_available(*table_id) == *available
                }
            });
            if unchanged {
                return Ok(already_satisfied(
                    "TiFlash replica status already matches".to_owned(),
                ));
            }
            let mut job = session_job(
                job_context,
                ActionType::ACTION_UPDATE_TI_FLASH_REPLICA_STATUS,
            );
            job.schema_id = database.info.id;
            job.table_id = stored.id;
            job.schema_name = database.info.name.lowercase().to_owned().into();
            job.table_name = stored.name.lowercase().to_owned().into();
            return Ok(Some(AddIndexSubmission::Job(
                Box::new(crate::ddl_job_submit::JobSpec {
                    job,
                    args: JobArgsValue::UpdateTiFlashReplicaStatus(Some(GoShared::new(
                        tidb_model::UpdateTiFlashReplicaStatusArgs {
                            available: *available,
                            physical_id: *table_id,
                        },
                    ))),
                    id_allocated: true,
                }),
                warnings,
            )));
        }
        // Go `executor.AlterTableCache` / `AlterTableNoCache`; the size
        // check and the `table_cache_meta` row come at submission.
        DdlStatement::AlterTableCache {
            schema,
            table,
            cache,
        } => {
            let (_, stored) = locate_table(catalog, schema, table)?;
            let wanted = if *cache {
                tidb_model::TableCacheStatusType::ENABLE
            } else {
                tidb_model::TableCacheStatusType::DISABLE
            };
            if stored.table_cache_status_type == wanted {
                return Ok(already_satisfied(format!(
                    "`{schema}`.`{table}` is already {}",
                    if *cache { "cached" } else { "not cached" }
                )));
            }
            if *cache {
                if tidb_metadef::is_mem_or_sys_db(&schema.go_to_lower()) {
                    return Err(coded_refusal(
                        &tidb_util::dbterror::ERR_UNSUPPORTED_ALTER_CACHE_FOR_SYS_TABLE,
                        "ALTER table cache for tables in system database is currently unsupported",
                    ));
                }
                if stored.temp_table_type != tidb_model::TempTableType::NONE {
                    return Err(alter_temporary_table_cache());
                }
                if stored.partition.is_some() {
                    return Err(partition_mode_on_cache_table());
                }
            }
            (
                if *cache {
                    ActionType::ACTION_ALTER_CACHE_TABLE
                } else {
                    ActionType::ACTION_ALTER_NO_CACHE_TABLE
                },
                schema,
                table,
                tidb_model::EmptyArgs::into_job_args_value(Some(GoShared::new(
                    tidb_model::EmptyArgs::default(),
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
    if let Some(policy) = shared_policy {
        job.involving_schema_info = GoSharedSlice::from_vec(vec![
            tidb_model::InvolvingSchemaInfo {
                database: job.schema_name.clone(),
                table: job.table_name.clone(),
                ..tidb_model::InvolvingSchemaInfo::default()
            },
            tidb_model::InvolvingSchemaInfo {
                policy: policy.into(),
                mode: tidb_model::InvolvingSchemaInfoMode::SHARED,
                ..tidb_model::InvolvingSchemaInfo::default()
            },
        ]);
    }
    Ok(Some(AddIndexSubmission::Job(
        Box::new(crate::ddl_job_submit::JobSpec {
            job,
            args,
            id_allocated: true,
        }),
        warnings,
    )))
}

fn driver_refusal(error: impl Into<tidb_error::mysql::MysqlError>) -> DdlPlanError {
    DdlPlanError::from_mysql_error(error.into())
}

fn invalid_attributes_spec(error: tidb_ddl_label::LabelError) -> DdlPlanError {
    coded_refusal(
        &tidb_util::dbterror::ERR_INVALID_ATTRIBUTES_SPEC,
        label_rules::invalid_attributes_spec_message(&error),
    )
}

/// Go's `*pdhttp.LabelRule` as a job persists it.
fn label_rule_json(
    rule: &tidb_ddl_label::Rule,
) -> Result<serde_json::Value, DdlPlanError> {
    serde_json::to_value(rule).map_err(|error| DdlPlanError::Encode(error.to_string()))
}

/// Go `onAlterTableAttributes` / `onAlterTablePartitionAttributes`'s PD
/// call for a persisted rule.
fn label_rule_step(rule: &serde_json::Value) -> Result<LabelRuleStep, DdlPlanError> {
    let rule: tidb_ddl_label::Rule = serde_json::from_value(rule.clone())
        .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
    Ok(LabelRuleStep {
        write: label_rules::attributes_rule_write(rule),
        error_context: Some(LABEL_RULES_NOTIFY_ERROR),
    })
}

/// Go `ErrOptOnTemporaryTable` for ALTER TABLE CACHE.
fn alter_temporary_table_cache() -> DdlPlanError {
    coded_refusal(
        &tidb_util::dbterror::ERR_OPT_ON_TEMPORARY_TABLE,
        "`alter temporary table cache` is unsupported on temporary tables.",
    )
}

/// Go `ErrOptOnCacheTable` for a partitioned table.
fn partition_mode_on_cache_table() -> DdlPlanError {
    coded_refusal(
        &tidb_util::dbterror::ERR_OPT_ON_CACHE_TABLE,
        "'partition mode' is unsupported on cache tables.",
    )
}

/// Go `table.ErrUnknownPartition` as the partition jobs answer it.
fn unknown_partition(info: &TableInfo) -> DdlPlanError {
    coded_refusal(
        &tidb_error::catalog::table::ERR_UNKNOWN_PARTITION,
        format!(
            "Unknown partition 'drop?' in table '{}'",
            info.name.original()
        ),
    )
}

/// Whether `physical_id` is one of `table`'s partitions.
fn partition_of(table: &TableInfo, physical_id: i64) -> bool {
    table.get_partition_info().is_some_and(|partition| {
        partition
            .read()
            .definitions
            .with_visible(|definitions| definitions.iter().any(|p| p.id == physical_id))
    })
}

/// Go `tables.FindPartitionByName` for a partition-management statement.
fn find_partition_id(table: &TableInfo, partition: &str) -> Result<i64, DdlPlanError> {
    let Some(partitioning) = table.get_partition_info() else {
        return Err(driver_refusal(
            tidb_ddl_build::errors::partition_mgmt_on_nonpartitioned(),
        ));
    };
    let name = partition.go_to_lower();
    let found = partitioning
        .read()
        .definitions
        .snapshot()
        .iter()
        .find(|definition| definition.name.lowercase() == name)
        .map(|definition| definition.id);
    found.ok_or_else(|| {
        driver_refusal(tidb_ddl_build::errors::unknown_partition(name, table.name.original().to_owned()))
    })
}

/// Go `checkPlacementPolicyRefValidAndCanNonValidJob`: the policy a job
/// names must still exist.
pub(super) fn check_placement_policy_ref_valid<S: MetaSnapshot>(
    snapshot: &mut S,
    reference: &Option<GoShared<tidb_model::PolicyRefInfo>>,
) -> Result<(), DdlPlanError> {
    let Some(reference) = reference else {
        return Ok(());
    };
    let id = reference.read().id;
    if snapshot.get(&key::policy_kv_key(id))?.is_none() {
        return Err(coded_refusal(
            &tidb_error::catalog::infoschema::ERR_PLACEMENT_POLICY_NOT_EXISTS,
            format!("Unknown placement policy '(Policy ID {id})'"),
        ));
    }
    Ok(())
}

/// Go's "Can't reset AUTO_INCREMENT" message.
fn cant_reset_auto_increment(requested: i64, adjusted: i64) -> String {
    format!(
        "Can't reset AUTO_INCREMENT to {requested} without FORCE option, using {adjusted} instead"
    )
}

/// Go's `autoid.AllocatorType` for the counters a rebase moves.
#[derive(Clone, Copy, PartialEq, Eq)]
enum AutoIdKind {
    /// `AutoIncrementType`.
    Increment,
    /// `AutoRandomType`.
    Random,
}

impl AutoIdKind {
    fn counter_key(self, db_id: i64, info: &TableInfo) -> Vec<u8> {
        match self {
            Self::Increment => tidb_cluster::cluster_auto_id::auto_id_key_for(db_id, info),
            Self::Random => key::auto_random_table_id_kv_key(db_id, info.id),
        }
    }

    fn unsigned(self, info: &TableInfo) -> bool {
        match self {
            Self::Increment => info.is_auto_inc_col_unsigned(),
            Self::Random => info.is_auto_random_bit_col_unsigned(),
        }
    }
}

/// The table's auto-increment allocator when the auto-ID service owns it
/// (`AUTO_ID_CACHE 1`); otherwise the counter lives in the meta keys.
fn auto_increment_service<S: MetaSnapshot>(
    snapshot: &S,
    kind: AutoIdKind,
    db_id: i64,
    info: &TableInfo,
) -> Option<tidb_cluster::cluster_auto_id::AutoIdServiceAllocator<dyn tidb_autoid::service_rpc::AutoIdServiceRpc>> {
    if kind != AutoIdKind::Increment {
        return None;
    }
    let client = snapshot.auto_id_client().filter(|_| info.sep_auto_inc())?;
    Some(tidb_cluster::cluster_auto_id::AutoIdServiceAllocator::new(
        client.clone(),
        if info.auto_id_schema_id == 0 {
            db_id
        } else {
            info.auto_id_schema_id
        },
        info.id,
        info.is_auto_inc_col_unsigned(),
        u32::MAX,
    ))
}

/// The id the counter last handed out (an absent key reads as 0, Go's
/// `GetInt64`).
fn auto_increment_counter<S: MetaSnapshot>(
    snapshot: &mut S,
    kind: AutoIdKind,
    db_id: i64,
    info: &TableInfo,
) -> Result<i64, DdlPlanError> {
    if let Some(service) = auto_increment_service(snapshot, kind, db_id, info) {
        let call = tidb_table::kv_table::AutoIdCall::background();
        return service
            .alloc(&call, 0, 1, 1)
            .map(|(_, end)| end)
            .map_err(|error| DdlPlanError::Encode(error.to_string()));
    }
    let key = kind.counter_key(db_id, info);
    match snapshot.get(&key)? {
        Some(value) => {
            value::parse_int_value(&value).map_err(|error| DdlPlanError::Encode(error.to_string()))
        }
        None => Ok(0),
    }
}

/// Go `adjustNewBaseToNextGlobalID`: never below the next id the counter
/// would hand out (`NextGlobalAutoID`), compared unsigned.
fn adjust_new_base_to_next_global_id<S: MetaSnapshot>(
    snapshot: &mut S,
    kind: AutoIdKind,
    db_id: i64,
    info: &TableInfo,
    new_base: i64,
) -> Result<i64, DdlPlanError> {
    let next_global = (auto_increment_counter(snapshot, kind, db_id, info)? as u64).wrapping_add(1);
    Ok(u64::max(new_base as u64, next_global) as i64)
}

/// Go `onRebaseAutoID`'s `alloc.Rebase(newBase-1)` (only ever grows) or
/// `alloc.ForceRebase(newBase-1)` (sets exactly).
fn rebase_auto_increment_counter<S: MetaSnapshot>(
    snapshot: &mut S,
    kind: AutoIdKind,
    db_id: i64,
    info: &TableInfo,
    new_base: i64,
    force: bool,
    mutations: &mut Vec<BufferMutation>,
) -> Result<(), DdlPlanError> {
    if force && new_base == 0 {
        return Err(DdlPlanError::AutoIdReadFailed);
    }
    let new_end = new_base.wrapping_sub(1);
    if let Some(service) = auto_increment_service(snapshot, kind, db_id, info) {
        let call = if force {
            tidb_table::kv_table::AutoIdCall::with_timeout(std::time::Duration::from_secs(30))
        } else {
            tidb_table::kv_table::AutoIdCall::background()
        };
        return service
            .rebase(&call, new_end, force)
            .map_err(|error| DdlPlanError::Encode(error.to_string()));
    }
    let current = auto_increment_counter(snapshot, kind, db_id, info)?;
    let grows = if kind.unsigned(info) {
        (new_end as u64) > (current as u64)
    } else {
        new_end > current
    };
    if force || grows {
        mutations.push(BufferMutation::set(
            kind.counter_key(db_id, info),
            value::encode_int_value(new_end),
        )?);
    }
    Ok(())
}

/// Go: a multi-schema sub-job stops at its revertible point first,
/// publishing nothing.
fn revertible_point(
    active: &mut crate::ddl_job_table::ActiveDdlJob,
) -> Option<PersistedDdlActionStep> {
    if !is_revertible_sub_job(&active.job) {
        return None;
    }
    active.job.mark_non_revertible();
    Some(action_step(job_only_write(active.job.id, Vec::new())))
}

/// Go: a multi-schema sub-job stops at its revertible point first, storing
/// the table as it stands without a new version.
fn revertible_point_with_table(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    info: &TableInfo,
    with_check: bool,
    start_ts: u64,
) -> Result<Option<PersistedDdlActionStep>, DdlPlanError> {
    if !is_revertible_sub_job(&active.job) {
        return Ok(None);
    }
    active.job.mark_non_revertible();
    if with_check {
        check_table_info_valid(info)?;
    }
    let schema_id = active.job.schema_id;
    let write = table_step(catalog, active, schema_id, info, false, start_ts)?;
    Ok(Some(action_step(write)))
}

/// Go `ErrOptOnCacheTable` for `Rename Index`.
fn check_rename_index_cache(info: &TableInfo) -> Result<(), DdlPlanError> {
    if info.table_cache_status_type != tidb_model::TableCacheStatusType::DISABLE {
        return Err(coded_refusal(
            &tidb_util::dbterror::ERR_OPT_ON_CACHE_TABLE,
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
    let mut counter_writes = Vec::new();
    let mut bundles = Vec::new();
    let mut label_steps = Vec::new();
    let mut affinity_groups = crate::ddl_affinity::AffinityGroupUpdate::default();
    let placement_error = |error: tidb_placement::PlacementError| {
        DdlPlanError::plain_refusal(format!("building placement rules: {error}"))
    };
    match active.job.type_ {
        ActionType::ACTION_MODIFY_TABLE_COMMENT => {
            let args = tidb_model::get_modify_table_comment_args(&mut active.job)
                .map_err(|error| args_error(active, error))?
                .ok_or_else(|| nil_args(active))?;
            if let Some(step) = revertible_point(active) {
                return Ok(step);
            }
            info.comment = args.read().comment.get().to_string();
        }
        // Go `onModifyTableEngineAttribute`: the written attribute is kept
        // for SHOW CREATE TABLE, and `onAlterTableStorageClassSettings`
        // re-resolves the table's and every partition's class when it names
        // one.
        ActionType::ACTION_MODIFY_ENGINE_ATTRIBUTE => {
            let args = tidb_model::get_modify_table_engine_attribute_args(&mut active.job)
                .map_err(|error| args_error(active, error))?
                .ok_or_else(|| nil_args(active))?;
            let engine_attribute = args.read().engine_attribute.get().to_string();
            let settings =
                tidb_ddl_build::storage_class::storage_class_settings_of(&engine_attribute)
                    .map_err(|error| cancel_ddl_job(active, driver_refusal(error)))?;
            if let Some(step) = revertible_point(active) {
                return Ok(step);
            }
            info.engine_attribute = engine_attribute;
            if let Some(settings) = settings {
                let (tier, transitions) =
                    tidb_ddl_build::storage_class::build_storage_class_for_table(&settings);
                info.storage_class_tier = tier;
                info.storage_class_transitions = transitions.into();
                let has_definitions = info
                    .partition
                    .as_ref()
                    .is_some_and(|partition| !partition.read().definitions.is_empty());
                if has_definitions {
                    let spec = stored_partition_spec(&info)
                        .map_err(|error| cancel_ddl_job(active, error))?;
                    let definitions = spec.definitions.iter().collect::<Vec<_>>();
                    let classes =
                        tidb_ddl_build::storage_class::build_storage_class_for_partitions(
                            &settings,
                            &spec.kind,
                            &definitions,
                            &tidb_expr::metabuild::Context::new_non_strict(),
                        )
                        .map_err(|error| cancel_ddl_job(active, driver_refusal(error)))?;
                    let partition = info.partition.as_ref().expect("checked above").read();
                    for (ordinal, (tier, transitions)) in classes.into_iter().enumerate() {
                        partition.definitions.update(ordinal, |definition| {
                            definition.storage_class_tier = tier;
                            definition.storage_class_transitions = transitions.into();
                        });
                    }
                }
            }
        }
        // Go `onRebaseAutoID`.
        ActionType::ACTION_REBASE_AUTO_ID | ActionType::ACTION_REBASE_AUTO_RANDOM_BASE => {
            let kind = if active.job.type_ == ActionType::ACTION_REBASE_AUTO_ID {
                AutoIdKind::Increment
            } else {
                AutoIdKind::Random
            };
            let args = tidb_model::get_rebase_auto_id_args(&mut active.job)
                .map_err(|error| args_error(active, error))?
                .ok_or_else(|| nil_args(active))?;
            let (mut new_base, force) = {
                let args = args.read();
                (args.new_base.get(), args.force.get())
            };
            if let Some(step) = revertible_point(active) {
                return Ok(step);
            }
            if !force {
                let adjusted =
                    adjust_new_base_to_next_global_id(snapshot, kind, schema_id, &info, new_base)?;
                if adjusted != new_base {
                    active.job.warning =
                        Some(GoShared::new(tidb_error::terror::TerrorError::synthesize(
                            tidb_error::terror::TerrorClass::Ddl,
                            tidb_error::terror::TerrorCode::new(isize::from(
                                tidb_error::mysql::errcode::ErrUnknown as i16,
                            )),
                            cant_reset_auto_increment(new_base, adjusted),
                        )));
                }
                new_base = adjusted;
            }
            match kind {
                AutoIdKind::Increment => info.auto_inc_id = new_base,
                AutoIdKind::Random => info.auto_rand_id = new_base,
            }
            rebase_auto_increment_counter(
                snapshot,
                kind,
                schema_id,
                &info,
                new_base,
                force,
                &mut counter_writes,
            )
            .map_err(|error| cancel_ddl_job(active, error))?;
        }
        // Go `onTTLInfoChange`.
        ActionType::ACTION_ALTER_TTLINFO => {
            let args = tidb_model::get_alter_ttl_info_args(&mut active.job)
                .map_err(|error| args_error(active, error))?
                .ok_or_else(|| nil_args(active))?;
            let (ttl_info, ttl_enable, schedule) = {
                let args = args.read();
                (
                    args.ttl_info.clone(),
                    args.ttl_enable,
                    args.ttl_cron_job_schedule.clone(),
                )
            };
            let set_ttl_option = |option: &str| {
                let error =
                    tidb_ddl_build::errors::set_ttl_option_for_non_ttl_table(option);
                DdlPlanError::from_mysql_error(error)
            };
            if let Some(ttl_info) = ttl_info {
                let mut ttl = ttl_info.read().clone();
                if let Some(old) = &info.ttl_info {
                    let old = old.read();
                    if ttl_enable.is_none() {
                        ttl.enable = old.enable;
                    }
                    if schedule.is_none() {
                        ttl.job_interval = old.job_interval.clone();
                    }
                }
                info.ttl_info = Some(GoShared::new(ttl));
            }
            if let Some(enable) = ttl_enable {
                let Some(ttl) = &info.ttl_info else {
                    return Err(set_ttl_option("TTL_ENABLE"));
                };
                ttl.write().enable = enable;
            }
            if let Some(schedule) = schedule {
                let Some(ttl) = &info.ttl_info else {
                    return Err(set_ttl_option("TTL_JOB_INTERVAL"));
                };
                ttl.write().job_interval = schedule;
            }
        }
        // Go `onTTLInfoRemove`.
        ActionType::ACTION_ALTER_TTLREMOVE => info.ttl_info = None,
        ActionType::ACTION_ALTER_INDEX_VISIBILITY => {
            let args = tidb_model::get_alter_index_visibility_args(&mut active.job)
                .map_err(|error| args_error(active, error))?
                .ok_or_else(|| nil_args(active))?;
            let (index, invisible) = {
                let args = args.read();
                (args.index_name.get(), args.invisible.get())
            };
            let mut probe = info.clone_like_go();
            match apply_index_visibility(&mut probe, &schema, &table, index.original(), invisible) {
                Ok(AlterColumnOutcome::Applied) => {
                    if let Some(step) =
                        revertible_point_with_table(catalog, active, &info, false, start_ts)?
                    {
                        return Ok(step);
                    }
                    info = probe;
                    with_check = true;
                }
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
            let mut probe = info.clone_like_go();
            match apply_rename_index(&mut probe, &schema, &table, &from, &to) {
                Ok(AlterColumnOutcome::Applied) => {
                    if let Some(step) =
                        revertible_point_with_table(catalog, active, &info, true, start_ts)?
                    {
                        return Ok(step);
                    }
                    info = probe;
                }
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
            let mut probe = info.clone_like_go();
            let outcome = apply_table_charset_and_collate(
                catalog, &schema, &table, &mut probe, &charset, &collate, overwrite,
            )
            .map_err(|error| cancel_ddl_job(active, error))?;
            if let Some(step) = revertible_point(active) {
                return Ok(step);
            }
            info = probe;
            if let AlterColumnOutcome::AlreadySatisfied(_) = outcome {
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
            if let Some(step) = revertible_point_with_table(catalog, active, &info, true, start_ts)?
            {
                return Ok(step);
            }
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
            tidb_ddl_build::table_info_build::recheck_column_default_in_reorg_context(&old)
                .map_err(|error| cancel_ddl_job(active, DdlPlanError::Admission(error)))?;
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
        // Go `onAlterTablePlacement`.
        ActionType::ACTION_ALTER_TABLE_PLACEMENT => {
            let args = tidb_model::get_alter_table_placement_args(&mut active.job)
                .map_err(|error| args_error(active, error))?
                .ok_or_else(|| nil_args(active))?;
            let reference = args.read().placement_policy_ref.clone();
            check_placement_policy_ref_valid(snapshot, &reference)
                .map_err(|error| cancel_ddl_job(active, error))?;
            let old_enables_placement = info.placement_policy_ref.is_some();
            info.placement_policy_ref = reference;
            let policies = load_policies(snapshot)?;
            match tidb_placement::new_table_bundle(&policies, &info)
                .map_err(|error| cancel_ddl_job(active, placement_error(error)))?
            {
                Some(bundle) => bundles.push(bundle),
                None if old_enables_placement => bundles.push(tidb_placement::new_bundle(info.id)),
                None => {}
            }
        }
        // Go `onAlterTablePartitionPlacement`.
        ActionType::ACTION_ALTER_TABLE_PARTITION_PLACEMENT => {
            let args = tidb_model::get_alter_table_partition_args(&mut active.job)
                .map_err(|error| args_error(active, error))?
                .ok_or_else(|| nil_args(active))?;
            let (partition_id, reference) = {
                let args = args.read();
                (args.partition_id.get(), args.policy_ref_info.get())
            };
            let definitions = info
                .get_partition_info()
                .map(|partition| partition.read().definitions.clone());
            let ordinal = definitions.as_ref().and_then(|definitions| {
                definitions
                    .snapshot()
                    .iter()
                    .position(|definition| definition.id == partition_id)
            });
            let (Some(definitions), Some(ordinal)) = (definitions, ordinal) else {
                return Err(cancel_ddl_job(active, unknown_partition(&info)));
            };
            let mut old_enables_placement = false;
            let mut definition = None;
            definitions.update(ordinal, |def| {
                old_enables_placement = def.placement_policy_ref.is_some();
                def.placement_policy_ref = reference.clone();
                definition = Some(def.clone());
            });
            let definition = definition.expect("the partition was found above");
            check_placement_policy_ref_valid(snapshot, &reference)
                .map_err(|error| cancel_ddl_job(active, error))?;
            let policies = load_policies(snapshot)?;
            match tidb_placement::new_partition_bundle(&policies, &definition)
                .map_err(|error| cancel_ddl_job(active, placement_error(error)))?
            {
                Some(bundle) => bundles.push(bundle),
                None if old_enables_placement => {
                    bundles.push(tidb_placement::new_bundle(definition.id))
                }
                None => {}
            }
        }
        // Go `onAlterTableAttributes`.
        ActionType::ACTION_ALTER_TABLE_ATTRIBUTES => {
            let args = tidb_model::get_alter_table_attributes_args(&mut active.job)
                .map_err(|error| args_error(active, error))?
                .ok_or_else(|| nil_args(active))?;
            let rule = args
                .read()
                .label_rule
                .clone()
                .ok_or_else(|| nil_args(active))?;
            let step = label_rule_step(&rule.read())?;
            label_steps.push(step);
        }
        // Go `onAlterTablePartitionAttributes`.
        ActionType::ACTION_ALTER_TABLE_PARTITION_ATTRIBUTES => {
            let args = tidb_model::get_alter_table_partition_args(&mut active.job)
                .map_err(|error| args_error(active, error))?
                .ok_or_else(|| nil_args(active))?;
            let (partition_id, rule) = {
                let args = args.read();
                (args.partition_id.get(), args.label_rule.get())
            };
            if !partition_of(&info, partition_id) {
                return Err(cancel_ddl_job(active, unknown_partition(&info)));
            }
            let rule = rule.ok_or_else(|| nil_args(active))?;
            let step = label_rule_step(&rule.read())?;
            label_steps.push(step);
        }
        // Go `onAlterTableAffinity`: the new groups first (the job fails when
        // PD refuses), the old ones best-effort unless the level stays.
        ActionType::ACTION_ALTER_TABLE_AFFINITY => {
            let args = tidb_model::get_alter_table_affinity_args(&mut active.job)
                .map_err(|error| args_error(active, error))?
                .ok_or_else(|| nil_args(active))?;
            let affinity = args
                .read()
                .affinity
                .as_ref()
                .map(|affinity| affinity.read().clone());
            let old = info.clone_like_go();
            tidb_ddl_build::validate_table_affinity(
                info.temp_table_type != tidb_model::TempTableType::NONE,
                info.get_partition_info().is_some(),
                affinity.as_ref(),
            )
            .map_err(|error| cancel_ddl_job(active, driver_refusal(error)))?;
            info.affinity = affinity.map(GoShared::new);
            if info.affinity.is_some() {
                affinity_groups
                    .create_for(&info)
                    .map_err(|error| cancel_ddl_job(active, pd_step_error(None, error)))?;
            }
            if let Some(previous) = old.affinity.as_ref() {
                let level_changed = info
                    .affinity
                    .as_ref()
                    .is_none_or(|new| new.read().level != previous.read().level);
                if level_changed {
                    affinity_groups.delete_for(&old, None);
                }
            }
        }
        // Go `onAlterTableSetRegionSplitPolicy`.
        ActionType::ACTION_ALTER_TABLE_SET_REGION_SPLIT_POLICY => {
            let args = tidb_model::get_alter_table_set_region_split_policy_args(&mut active.job)
                .map_err(|error| args_error(active, error))?
                .ok_or_else(|| nil_args(active))?;
            let (index_name, policy) = {
                let args = args.read();
                (
                    args.index_name.clone(),
                    args.policy
                        .as_ref()
                        .map(|policy| policy.read().clone())
                        .unwrap_or_default(),
                )
            };
            if !index_name.is_empty() && find_index(&info, &index_name).is_none() {
                return Err(cancel_ddl_job(
                    active,
                    DdlPlanError::KeyNotExists {
                        index: index_name,
                        table: table.clone(),
                    },
                ));
            }
            tidb_ddl_build::table_info_build::set_region_split_policy(&mut info, &index_name, policy);
        }
        // Go `onSetTableFlashReplica`; PD's rules are the replica manager's.
        ActionType::ACTION_SET_TI_FLASH_REPLICA => {
            let args = tidb_model::get_set_tiflash_replica_args(&mut active.job)
                .map_err(|error| args_error(active, error))?
                .ok_or_else(|| nil_args(active))?;
            let (spec, reset_available) = {
                let args = args.read();
                (args.tiflash_replica.clone(), args.reset_available)
            };
            if tidb_metadef::is_mem_or_sys_db(&active.job.schema_name.to_string().go_to_lower()) {
                return Err(DdlPlanError::from(
                    tidb_util::dbterror::ERR_UNSUPPORTED_TI_FLASH_OPERATION_FOR_SYS_OR_MEM_TABLE
                        .clone(),
                ));
            }
            if spec.count > 0 {
                let available = !reset_available
                    && info
                        .tiflash_replica
                        .as_ref()
                        .is_some_and(|old| old.read().available);
                let mut replica = tidb_model::table::TiFlashReplicaInfo::default();
                replica.count = spec.count;
                replica.location_labels = spec.labels.unwrap_or_default().into();
                replica.available = available;
                info.tiflash_replica = Some(GoShared::new(replica));
            } else {
                info.tiflash_replica = None;
            }
        }
        // Go `onUpdateTiFlashReplicaStatus`.
        ActionType::ACTION_UPDATE_TI_FLASH_REPLICA_STATUS => {
            let args = tidb_model::get_update_tiflash_replica_status_args(&mut active.job)
                .map_err(|error| args_error(active, error))?
                .ok_or_else(|| nil_args(active))?;
            let (available, physical_id) = {
                let args = args.read();
                (args.available, args.physical_id)
            };
            let already = info.tiflash_replica.as_ref().is_none_or(|replica| {
                let replica = replica.read();
                if info.id == physical_id {
                    replica.available == available
                } else {
                    replica.is_partition_available(physical_id) == available
                }
            });
            if already {
                return Err(cancel_ddl_job(
                    active,
                    DdlPlanError::Encode(format!(
                        "the replica available status of table {} is already updated",
                        info.name
                    )),
                ));
            }
            let replica = info.tiflash_replica.clone().expect("checked above");
            if info.id == physical_id {
                replica.write().available = available;
            } else if let Some(partition) = info.get_partition_info() {
                let mut replica = replica.write();
                let mut ids: Vec<i64> = replica.available_partition_ids.iter().copied().collect();
                if available {
                    let definitions = partition.read().definitions.snapshot();
                    if definitions
                        .iter()
                        .any(|definition| definition.id == physical_id)
                    {
                        ids.push(physical_id);
                    }
                    replica.available = definitions
                        .iter()
                        .all(|definition| ids.contains(&definition.id));
                } else if let Some(position) = ids.iter().position(|id| *id == physical_id) {
                    ids.remove(position);
                    replica.available = false;
                }
                replica.available_partition_ids = ids.into();
            } else {
                return Err(cancel_ddl_job(
                    active,
                    DdlPlanError::Encode(format!(
                        "unknown physical ID {physical_id} in table {}",
                        info.name.original()
                    )),
                ));
            }
        }
        // Go `onAlterCacheTable` / `onAlterNoCacheTable`: through
        // `TableCacheStatusSwitching`, one version each way.
        ActionType::ACTION_ALTER_CACHE_TABLE | ActionType::ACTION_ALTER_NO_CACHE_TABLE => {
            let cache = active.job.type_ == ActionType::ACTION_ALTER_CACHE_TABLE;
            let (target, from) = if cache {
                (
                    tidb_model::TableCacheStatusType::ENABLE,
                    tidb_model::TableCacheStatusType::DISABLE,
                )
            } else {
                (
                    tidb_model::TableCacheStatusType::DISABLE,
                    tidb_model::TableCacheStatusType::ENABLE,
                )
            };
            if info.table_cache_status_type == target {
                active.job.finish_table_job(
                    JobState::DONE,
                    SchemaState::PUBLIC,
                    0,
                    Some(GoShared::new(info.clone_like_go())),
                );
                return Ok(action_step(job_only_write(active.job.id, Vec::new())));
            }
            if cache {
                if info.temp_table_type != tidb_model::TempTableType::NONE {
                    return Err(alter_temporary_table_cache());
                }
                if info.partition.is_some() {
                    return Err(partition_mode_on_cache_table());
                }
            }
            if info.table_cache_status_type == from {
                info.table_cache_status_type = tidb_model::TableCacheStatusType::SWITCHING;
                check_table_info_valid(&info)?;
                let write = table_step(catalog, active, schema_id, &info, true, start_ts)?;
                return Ok(action_step(write));
            }
            if info.table_cache_status_type != tidb_model::TableCacheStatusType::SWITCHING {
                let status = format!("{:?}", info.table_cache_status_type);
                return Err(cancel_ddl_job(
                    active,
                    DdlPlanError::from(tidb_util::dbterror::ERR_INVALID_DDL_STATE.generate(
                        format!(
                            "invalid {} state {status}",
                            if cache {
                                "alter table cache"
                            } else {
                                "alter table no cache"
                            }
                        ),
                    )),
                ));
            }
            info.table_cache_status_type = target;
            with_check = true;
        }
        action => unreachable!("{action} is not a one-step ALTER TABLE job"),
    }
    if with_check {
        check_table_info_valid(&info)?;
    }
    let mut write = table_step(catalog, active, schema_id, &info, true, start_ts)?;
    write.mutations.extend(counter_writes);
    write.placement_bundles = bundles;
    write.label_rules = label_steps;
    write.affinity_groups = affinity_groups;
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
        | ActionType::ACTION_MODIFY_TABLE_AUTO_IDCACHE
        | ActionType::ACTION_REBASE_AUTO_ID => active.job.schema_state == SchemaState::NONE,
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

/// Go `checkTiFlashReplicaCount`: with TiFlash enabled, a replica count above
/// PD's TiFlash store count (`GetTiFlashStoreCount`) is refused.
///
/// # Errors
///
/// That refusal (Go `errors.Errorf`, 1105), or the store lookup's failure.
pub fn check_tiflash_replica_count(
    replica_count: u64,
    tiflash_stores: &dyn Fn() -> Result<u64, String>,
) -> Result<(), DdlPlanError> {
    if !tidb_config::config_tree::config::get_global_config()
        .cse
        .is_tiflash_enabled()
    {
        return Ok(());
    }
    let store_count = tiflash_stores().map_err(DdlPlanError::plain_refusal)?;
    if replica_count > store_count {
        return Err(DdlPlanError::plain_refusal(format!(
            "the tiflash replica count: {replica_count} should be less than the total tiflash server count: {store_count}"
        )));
    }
    Ok(())
}
