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

//! MODIFY / CHANGE / RENAME COLUMN as Go runs them: the executor builds the
//! new column (`GetModifiableColumnJob`, `RenameColumn`) and queues an
//! `ActionModifyColumn` job; the owner's `onModifyColumn` decides how the
//! data changes (`getModifyColumnType`) and runs one of Go's five courses:
//!
//! * metadata only (`doModifyColumnNoCheck`);
//! * metadata after a check of the rows, with writes checked meanwhile
//!   (`doModifyColumnWithCheck`, `precheckForVarcharToChar`);
//! * the indexes rebuilt under the new type (`doModifyColumnIndexReorg`);
//! * the rows rewritten into a temporary column and its indexes rebuilt,
//!   then swapped in (`doModifyColumnTypeWithData`).
//!
//! The data work runs outside the job's transaction: a step asks for it and
//! the owner hands the outcome to the next visit.

use tidb_executor::ddl::modify_column_type::{
    get_modify_column_type, no_reorg_data_strict, ModifyColumnFacts, ModifyColumnType,
};
use tidb_model::job_enums::modify_type;

use super::add_index::{
    action_step, do_reorg_work_for_create_index, partition_ids, table_step, AddIndexReorg,
    AddIndexSubmission, ColumnDataCheck, ColumnReorgSpec, CreateIndexReorg, ReorgOutcome,
    ReorgResult, ReorgStageRequest,
};
use super::*;

/// Go `mysql.TypeNull`, the `ModifyColumnType` older versions submitted.
const MYSQL_TYPE_NULL: u8 = 6;

/// Go `mysql.ModeStrictTransTables` and `ModeStrictAllTables`
/// (`SQLMode.HasStrictMode`).
const STRICT_TRANS_TABLES: i64 = 1 << 21;
const STRICT_ALL_TABLES: i64 = 1 << 22;

fn kind_byte(kind: ModifyColumnType) -> u8 {
    match kind {
        ModifyColumnType::NoReorg => modify_type::NO_REORG,
        ModifyColumnType::NoReorgWithCheck => modify_type::NO_REORG_WITH_CHECK,
        ModifyColumnType::Precheck => modify_type::PRECHECK,
        ModifyColumnType::IndexReorg => modify_type::INDEX_REORG,
        ModifyColumnType::Reorg => modify_type::REORG,
    }
}

/// Go's executor halves up to `doDDLJob2`: `executor.ModifyColumn` /
/// `ChangeColumn` (`GetModifiableColumnJob`) and `executor.RenameColumn`.
/// `has_null` answers whether the table holds a NULL in a column (Go
/// `checkForNullValue`'s query, run in the submitting transaction). `None`
/// for any other statement.
#[allow(clippy::too_many_arguments)]
pub fn prepare_modify_column_job_submission<S: MetaSnapshot>(
    snapshot: &mut S,
    statement: &DdlStatement,
    job_context: &tidb_executor::StmtContext,
    start_ts: u64,
    upgrading: bool,
    min_job_id: i64,
    store_count: usize,
    has_null: &mut dyn FnMut(&TableInfo, i64) -> Result<bool, DdlPlanError>,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    let catalog = load_cluster_catalog(snapshot)?;
    let built = build_modify_column_job(
        snapshot,
        &catalog,
        statement,
        job_context,
        store_count,
        has_null,
    )?;
    submit_built_job(snapshot, &catalog, built, start_ts, upgrading, min_job_id)
}

/// The executor half without `doDDLJob2`: the job unsubmitted, which a
/// multi-schema change appends as a sub-job (Go `appendToSubJobs`).
pub(super) fn build_modify_column_job<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    statement: &DdlStatement,
    job_context: &tidb_executor::StmtContext,
    store_count: usize,
    has_null: &mut dyn FnMut(&TableInfo, i64) -> Result<bool, DdlPlanError>,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    let (schema, stored, schema_id, args) = match statement {
        DdlStatement::ModifyColumn {
            schema,
            table,
            column,
            position,
            context,
            rename_from,
            modify_column_type: _,
            if_exists,
        } => {
            let (schema_id, stored) = locate_table(&catalog, schema, table)?;
            // Go `ModifyColumn` / `ChangeColumn`: `ErrColumnNotExists` under
            // IF EXISTS becomes a note and the statement succeeds.
            let source = rename_from.as_deref().unwrap_or(column.name.as_str());
            if *if_exists
                && tidb_model::column::find_column_info(&stored.columns, source).is_none()
            {
                return Ok(Some(AddIndexSubmission::AlreadySatisfied {
                    detail: format!("column `{source}` does not exist on `{schema}`.`{table}`"),
                    warnings: vec![(
                        DdlWarningLevel::Note,
                        1054,
                        format!("Unknown column '{source}' in '{table}'"),
                    )],
                }));
            }
            tidb_executor::ddl::check_column_qualifier(&column.qualifier, schema, table).map_err(
                |error| {
                    let error = error.to_mysql_error();
                    DdlPlanError::Admission(DdlAdmissionError::from_mysql_error(error))
                },
            )?;
            let PreparedModifyColumn {
                column: changed, ..
            } = prepare_modify_column(
                stored,
                schema,
                table,
                column,
                position,
                context,
                rename_from.as_deref(),
            )?;
            let old_name = rename_from.as_deref().unwrap_or(column.name.as_str());
            let old = tidb_model::column::find_column_info(&stored.columns, old_name)
                .expect("prepare_modify_column found the column")
                .read()
                .clone_like_go();
            // Go `IsModifyColumnDenied` over the table with the change.
            let bdr_role = snapshot.get(&key::bdr_role_kv_key())?.unwrap_or_default();
            let mut probe = stored.clone_like_go();
            if let Some(slot) = tidb_model::column::find_column_info_by_id(&probe.columns, old.id) {
                *slot.write() = changed.clone_like_go();
            }
            admit_modified_column_bdr(
                &bdr_role,
                schema,
                stored,
                &probe,
                column,
                rename_from.as_deref(),
            )?;
            let _ = &mut probe;
            // Go `checkForNullValue`: NULL -> NOT NULL fails on a NULL row,
            // except into a TIMESTAMP, which substitutes the current time.
            let old_type = &old.field_type;
            if !old_type.has_flag(FieldTypeFlags::NOT_NULL)
                && changed.field_type.has_flag(FieldTypeFlags::NOT_NULL)
                && !(old_type.code() != FieldTypeCode::Timestamp
                    && changed.field_type.code() == FieldTypeCode::Timestamp)
                && has_null(stored, old.id)?
            {
                return Err(coded_refusal(
                    &tidb_error::catalog::types::ERR_TRUNCATED,
                    format!(
                        "Data truncated for column '{}' at row 1",
                        changed.name.lowercase()
                    ),
                ));
            }
            let new_shard_bits = check_auto_random(stored, &old, &changed, column)?;
            let args = tidb_model::ModifyColumnArgs {
                column: Some(GoShared::new(changed)),
                old_column_name: CiString::new(old_name),
                position: Some(GoShared::new(tidb_ast::JobColumnPosition::from_statement(
                    position,
                ))),
                new_shard_bits,
                ..tidb_model::ModifyColumnArgs::default()
            };
            (schema, stored, schema_id, args)
        }
        DdlStatement::RenameColumn {
            schema,
            table,
            from,
            to,
        } => {
            let (schema_id, stored) = locate_table(&catalog, schema, table)?;
            // Go `RenameColumn`: the column cloned under its new name.
            let Some(renamed) = prepare_rename_column(stored, table, from, to)? else {
                return Ok(Some(AddIndexSubmission::AlreadySatisfied {
                    detail: format!("column `{from}` already has the requested name"),
                    warnings: Vec::new(),
                }));
            };
            let args = tidb_model::ModifyColumnArgs {
                column: Some(GoShared::new(renamed)),
                old_column_name: CiString::new(from.clone()),
                ..tidb_model::ModifyColumnArgs::default()
            };
            (schema, stored, schema_id, args)
        }
        _ => return Ok(None),
    };
    let mut job = session_job(job_context, ActionType::ACTION_MODIFY_COLUMN);
    job.schema_id = schema_id;
    job.table_id = stored.id;
    job.schema_name = schema.go_to_lower().into();
    job.table_name = stored.name.lowercase().to_owned().into();
    // Go `NeedReorg`: `!noReorgDataStrict`.
    if let (Some(new), Some(old)) = (
        args.column.as_ref(),
        tidb_model::column::find_column_info(&stored.columns, args.old_column_name.original()),
    ) {
        job.need_reorg = !no_reorg_data_strict(
            &old.read().field_type,
            &new.read().field_type,
            is_column_with_index(args.old_column_name.original(), stored),
        );
    }
    let vars = job_context.ddl_reorg_vars();
    job.session_vars = Some(GoShared::new(std::collections::BTreeMap::from([
        (
            tidb_vardef::tidb_vars::TIDB_ENABLE_DDL_ANALYZE.to_owned(),
            vars.enable_ddl_analyze.clone().into(),
        ),
        (
            tidb_vardef::tidb_vars::TIDB_ANALYZE_VERSION.to_owned(),
            vars.analyze_version.clone().into(),
        ),
    ])));
    // Go `initJobReorgMetaFromVariables`.
    job.reorg_meta = Some(GoShared::new(add_index::new_add_index_reorg_meta(
        job_context,
        schema,
        store_count,
    )));
    Ok(Some(AddIndexSubmission::Job(
        Box::new(crate::ddl_job_submit::JobSpec {
            job,
            args: JobArgsValue::ModifyColumn(Some(GoShared::new(args))),
            id_allocated: true,
        }),
        Vec::new(),
    )))
}

/// Go `isClusteredPKColumn`.
fn is_clustered_pk_column(column: &tidb_model::ColumnInfo, table: &TableInfo) -> bool {
    if table.pk_is_handle {
        return column.field_type.has_flag(FieldTypeFlags::PRI_KEY);
    }
    if table.is_common_handle {
        return table.indices.iter_deref().any(|index| {
            let index = index.read();
            index.primary
                && index
                    .columns
                    .iter_deref()
                    .any(|part| part.read().offset as i64 == column.offset as i64)
        });
    }
    false
}

/// Go `extractAutoRandomBitsFromColDef` with `AutoRandomShardBitsNormalize`
/// and `AutoRandomRangeBitsNormalize`: (0, 0) without AUTO_RANDOM.
fn extract_auto_random_bits_from_col_def(
    column: &tidb_ast::ColumnDef,
) -> Result<(u64, u64), DdlPlanError> {
    let Some(option) = column.options.iter().find_map(|option| match option {
        tidb_ast::ColumnOption::AutoRandom(option) => Some(option),
        _ => None,
    }) else {
        return Ok((0, 0));
    };
    let shard_bits = option.shard_bits.unwrap_or(5);
    if shard_bits == 0 {
        return Err(DdlPlanError::InvalidAutoRandom(
            "the value of auto_random should be positive".to_owned(),
        ));
    }
    if shard_bits > 15 {
        return Err(DdlPlanError::InvalidAutoRandom(format!(
            "max allowed auto_random shard bits is 15, but got {shard_bits} on column `{}`",
            column.name
        )));
    }
    let range_bits = option.range_bits.unwrap_or(64);
    if !(32..=64).contains(&range_bits) {
        return Err(DdlPlanError::InvalidAutoRandom(format!(
            "auto_random range bits must be between 32 and 64, but got {range_bits}"
        )));
    }
    Ok((shard_bits, range_bits))
}

/// Go `checkAutoRandom`: the shard bits a MODIFY gives an AUTO_RANDOM
/// column (0 when it is not one), which may only grow, and from AUTO_INCREMENT
/// on a clustered primary key only.
fn check_auto_random(
    table: &TableInfo,
    old: &tidb_model::ColumnInfo,
    new: &tidb_model::ColumnInfo,
    column: &tidb_ast::ColumnDef,
) -> Result<u64, DdlPlanError> {
    let (old_shard_bits, old_range_bits) = if is_clustered_pk_column(old, table) {
        (table.auto_random_bits, table.auto_random_range_bits)
    } else {
        (0, 0)
    };
    let (new_shard_bits, new_range_bits) = extract_auto_random_bits_from_col_def(column)?;
    let invalid = |reason: String| DdlPlanError::InvalidAutoRandom(reason);
    if old_shard_bits < new_shard_bits {
        if old_shard_bits == 0 {
            let converting = old.field_type.has_flag(FieldTypeFlags::AUTO_INCREMENT)
                && table.pk_is_handle
                && old.field_type.has_flag(FieldTypeFlags::PRI_KEY);
            if !converting {
                return Err(invalid(
                    "auto_random can only be converted from auto_increment clustered primary key"
                        .to_owned(),
                ));
            }
        }
    } else if old_shard_bits > new_shard_bits {
        if new_shard_bits == 0 {
            return Err(invalid(
                "adding/dropping/modifying auto_random is not supported".to_owned(),
            ));
        }
        return Err(invalid(
            "decreasing auto_random shard bits is not supported".to_owned(),
        ));
    }
    if old_shard_bits > 0 || new_shard_bits > 0 {
        if old.field_type.code() != new.field_type.code() {
            return Err(invalid(
                "modifying the auto_random column type is not supported".to_owned(),
            ));
        }
        if old.field_type.code() != FieldTypeCode::LongLong {
            return Err(invalid(format!(
                "auto_random option must be defined on `bigint` column, but not on `{}` column",
                tidb_datatype::type_str(old.field_type.code())
            )));
        }
        let has = |wanted: fn(&tidb_ast::ColumnOption) -> bool| column.options.iter().any(wanted);
        if has(|option| matches!(option, tidb_ast::ColumnOption::AutoIncrement)) {
            return Err(invalid(
                "auto_random is incompatible with auto_increment".to_owned(),
            ));
        }
        if has(|option| matches!(option, tidb_ast::ColumnOption::Default(_))) {
            return Err(invalid(
                "auto_random is incompatible with default".to_owned(),
            ));
        }
    }
    // Go `rangeBitsIsChanged`: an unset width is the default 64.
    let normalized = |bits: u64| if bits == 0 { 64 } else { bits };
    if normalized(old_range_bits) != normalized(new_range_bits) {
        return Err(invalid(
            "alter the range bits of auto_random column is not supported".to_owned(),
        ));
    }
    Ok(new_shard_bits)
}

/// Go `checkAndApplyAutoRandomBits` (`checkNewAutoRandomBits`,
/// `applyNewAutoRandomBits`): the counter the new layout continues from is
/// bumped and must still fit the incremental bits, then the table takes the
/// new shard bits, and a column converted from AUTO_INCREMENT has its
/// AUTO_RANDOM counter rebased past the row IDs handed out.
fn check_and_apply_auto_random_bits<S: MetaSnapshot>(
    snapshot: &mut S,
    database_id: i64,
    info: &mut TableInfo,
    old: &tidb_model::ColumnInfo,
    new: &tidb_model::ColumnInfo,
    new_shard_bits: u64,
    counter_writes: &mut Vec<BufferMutation>,
) -> Result<(), DdlPlanError> {
    if new_shard_bits == 0 {
        return Ok(());
    }
    // Go `autoid.NewShardIDFormat` over the table's (possibly unset) range
    // bits, in unsigned arithmetic.
    let range_bits = info.auto_random_range_bits;
    let incremental_bits = if new.field_type.is_unsigned() {
        range_bits.wrapping_sub(new_shard_bits)
    } else {
        range_bits.wrapping_sub(new_shard_bits).wrapping_sub(1)
    };
    let converted = old.field_type.has_flag(FieldTypeFlags::AUTO_INCREMENT);
    let counter_key = if !converted {
        key::auto_random_table_id_kv_key(database_id, info.id)
    } else if info.sep_auto_inc() {
        key::auto_increment_id_kv_key(database_id, info.id)
    } else {
        key::auto_table_id_kv_key(database_id, info.id)
    };
    // `idAcc.Inc(1)` first, against concurrent DML, then `Get`.
    let current = read_auto_id(snapshot, &counter_key)?.wrapping_add(1);
    counter_writes.push(BufferMutation::set(
        counter_key.clone(),
        value::encode_int_value(current),
    )?);
    let used_bits = u64::from(64 - (current as u64).leading_zeros());
    if used_bits > incremental_bits {
        let overflow = used_bits - incremental_bits;
        return Err(DdlPlanError::InvalidAutoRandom(format!(
            "max allowed auto_random shard bits is {}, but got {} on column `{}`",
            new_shard_bits.wrapping_sub(overflow),
            new_shard_bits,
            old.name.original()
        )));
    }
    info.auto_random_bits = new_shard_bits;
    if !converted {
        return Ok(());
    }
    // The AUTO_RANDOM allocator's `Rebase(nextAutoIncID)`.
    let row_id_key = key::auto_table_id_kv_key(database_id, info.id);
    let next = if row_id_key == counter_key {
        current
    } else {
        read_auto_id(snapshot, &row_id_key)?
    };
    let random_key = key::auto_random_table_id_kv_key(database_id, info.id);
    if next > read_auto_id(snapshot, &random_key)? {
        counter_writes.push(BufferMutation::set(
            random_key,
            value::encode_int_value(next),
        )?);
    }
    Ok(())
}

/// Go `ErrColumnNotExists` (1054).
fn column_not_exists(column: &str, table: &str) -> DdlPlanError {
    DdlPlanError::UnknownColumn {
        column: column.to_owned(),
        table: table.to_owned(),
    }
}

/// Go `ErrUnsupportedModifyColumn` with its reason.
fn unsupported_modify_column(reason: &str) -> DdlPlanError {
    coded_refusal(
        &tidb_util::dbterror::ERR_RUN_MULTI_SCHEMA_CHANGES,
        format!("Unsupported modify column: {reason}"),
    )
}

/// What one visit works on (Go `getModifyColumnInfo`).
struct ModifyTarget {
    schema: String,
    info: TableInfo,
    old: GoShared<tidb_model::ColumnInfo>,
}

/// Go `getModifyColumnInfo`: the database, the table and the column being
/// changed, found by the id persisted after the first visit.
fn get_modify_column_info(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    args: &mut tidb_model::ModifyColumnArgs,
) -> Result<ModifyTarget, DdlPlanError> {
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
    let schema = database.info.name.original().to_owned();
    let info = checked_job_table(catalog, active, true)?.clone_like_go();
    let old = if args.old_column_id > 0 {
        tidb_model::column::find_column_info_by_id(&info.columns, args.old_column_id)
    } else {
        // An older submitter persisted only the name.
        let found = tidb_model::column::find_column_info(
            &info.columns,
            &tidb_model::column::gen_removing_obj_name(args.old_column_name.lowercase()),
        )
        .or_else(|| {
            tidb_model::column::find_column_info(&info.columns, args.old_column_name.lowercase())
        });
        if let Some(found) = &found {
            args.old_column_id = found.read().id;
        }
        found
    };
    let Some(old) = old else {
        return Err(cancel_ddl_job(
            active,
            column_not_exists(args.old_column_name.original(), info.name.original()),
        ));
    };
    Ok(ModifyTarget { schema, info, old })
}

/// Go `buildRelatedIndexInfos`: the indexes over the column, by id.
fn related_indexes(info: &TableInfo, column_id: i64) -> Vec<GoShared<IndexInfo>> {
    let mut indexes: Vec<_> = info
        .indices
        .iter_deref()
        .filter(|index| index.read().has_column_in_index_columns(info, column_id))
        .collect();
    indexes.sort_by_key(|index| index.read().id);
    indexes
}

/// Go `hasModifyFlag`.
fn has_modify_flag(column: &tidb_model::ColumnInfo) -> bool {
    column
        .field_type
        .has_flag(FieldTypeFlags::PREVENT_NULL_INSERT)
        || column.changing_field_type.is_some()
}

/// Go `isNullToNotNullChange`.
fn is_null_to_not_null_change(old: &tidb_model::ColumnInfo, new: &tidb_model::ColumnInfo) -> bool {
    tidb_executor::ddl::modify_column_type::is_null_to_not_null_change(
        &old.field_type,
        &new.field_type,
    )
}

/// Go `getModifyColumnType`.
fn modify_column_type_of(
    args: &tidb_model::ModifyColumnArgs,
    info: &TableInfo,
    old: &tidb_model::ColumnInfo,
    new: &tidb_model::ColumnInfo,
    sql_mode: i64,
) -> u8 {
    let indexed = info
        .indices
        .iter_deref()
        .any(|index| index.read().has_column_in_index_columns(info, old.id));
    if !no_reorg_data_strict(&old.field_type, &new.field_type, indexed)
        && args.modify_column_type == MYSQL_TYPE_NULL
    {
        // For backward compatibility.
        return modify_type::REORG;
    }
    kind_byte(get_modify_column_type(
        &old.field_type,
        &new.field_type,
        ModifyColumnFacts {
            column_indexed: indexed,
            partitioned: info.partition.is_some(),
            tiflash_replica: info
                .tiflash_replica
                .as_ref()
                .is_some_and(|replica| replica.read().count > 0),
            strict_sql_mode: sql_mode & (STRICT_TRANS_TABLES | STRICT_ALL_TABLES) != 0,
        },
    ))
}

/// Go `checkColumnAlreadyExists`.
fn check_column_already_exists(
    info: &TableInfo,
    args: &tidb_model::ModifyColumnArgs,
    new: &tidb_model::ColumnInfo,
) -> Result<(), DdlPlanError> {
    if new.name.lowercase() != args.old_column_name.lowercase()
        && tidb_model::column::find_column_info(&info.columns, new.name.lowercase()).is_some()
    {
        return Err(DdlPlanError::DuplicateColumnName(
            new.name.original().to_owned(),
        ));
    }
    Ok(())
}

/// The step that records `args` as the job's (Go mutates the arguments the
/// job re-encodes).
fn with_args(
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    args: &tidb_model::ModifyColumnArgs,
    step: PersistedDdlActionStep,
) -> PersistedDdlActionStep {
    active.job.fill_args(Some(GoShared::new(args.clone())));
    step
}

fn reorg_request(job: &Job, reorg: AddIndexReorg) -> PersistedDdlActionStep {
    PersistedDdlActionStep {
        write: job_only_write(job.id, Vec::new()),
        update_raw_args: false,
        run_error: None,
        reorg: Some(reorg),
    }
}

/// A check request (Go `checkModifyColumnData`), run outside the job.
fn check_request(
    active: &crate::ddl_job_table::ActiveDdlJob,
    target: &ModifyTarget,
    new: &tidb_model::ColumnInfo,
    check_value_range: bool,
) -> PersistedDdlActionStep {
    reorg_request(
        &active.job,
        AddIndexReorg {
            job_id: active.job.id,
            stage: ReorgStageRequest::Check(ColumnDataCheck {
                schema: target.schema.clone(),
                table: Box::new(target.info.clone_like_go()),
                old_column_id: target.old.read().id,
                new_column: Box::new(new.clone_like_go()),
                check_value_range,
            }),
            table: Box::new(target.info.clone_like_go()),
            index_ids: Vec::new(),
            merging: false,
            physical_ids: Vec::new(),
            batch_size: 1,
            context: DdlStatementContext(add_index::reorg_statement_context(&active.job)),
            row_count: active.job.row_count,
            column: None,
            sub_job: add_index::reorg_sub_job(&active.job),
            clean_up_global_index: false,
            partition_phase: None,
            schema_id: active.job.schema_id,
        },
    )
}

/// The outcome of this job's column check, if the owner ran one.
fn checked_outcome(
    active: &crate::ddl_job_table::ActiveDdlJob,
    outcome: Option<&ReorgOutcome>,
) -> Option<Option<(u16, String)>> {
    match outcome {
        Some(ReorgOutcome {
            job_id,
            result: ReorgResult::Checked(found),
            ..
        }) if *job_id == active.job.id => Some(found.clone()),
        _ => None,
    }
}

/// Go `onModifyColumn`.
pub(super) fn plan_persisted_modify_column_job_step<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
    outcome: Option<&ReorgOutcome>,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    // The AUTO_RANDOM counters the step bumped commit with it.
    let mut counter_writes = Vec::new();
    let mut step = plan_modify_column_step(
        snapshot,
        catalog,
        active,
        start_ts,
        outcome,
        &mut counter_writes,
    )?;
    step.write.mutations.extend(counter_writes);
    Ok(step)
}

fn plan_modify_column_step<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
    outcome: Option<&ReorgOutcome>,
    counter_writes: &mut Vec<BufferMutation>,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let args = tidb_model::get_modify_column_args(&mut active.job)
        .map_err(|error| cancel_ddl_job(active, DdlPlanError::Encode(error.to_string())))?
        .ok_or_else(|| {
            cancel_ddl_job(
                active,
                DdlPlanError::Encode("MODIFY COLUMN job has nil args".to_owned()),
            )
        })?;
    let mut args = args.read().clone();
    let mut target = get_modify_column_info(catalog, active, &mut args)?;
    let new = args
        .column
        .as_ref()
        .ok_or_else(|| DdlPlanError::Encode("MODIFY COLUMN job has no column".to_owned()))?
        .read()
        .clone_like_go();
    let old = target.old.read().clone_like_go();

    // Go: the index prefix lengths, checked the first time.
    if active.job.schema_state == SchemaState::NONE {
        check_indexes_in_modifiable_columns(&target.info, &old, &new)
            .map_err(|error| cancel_ddl_job(active, error))?;
    }
    if args.modify_column_type == modify_type::NONE || args.modify_column_type == MYSQL_TYPE_NULL {
        // Go re-checks the types: a concurrent no-reorg change may have
        // moved the column.
        check_modify_types_owner(&target.info, &old, &new)
            .map_err(|error| cancel_ddl_job(active, error))?;
        args.modify_column_type =
            modify_column_type_of(&args, &target.info, &old, &new, active.job.sql_mode);
        if old.field_type.code() == FieldTypeCode::Varchar
            && new.field_type.code() == FieldTypeCode::String
            && matches!(
                args.modify_column_type,
                modify_type::NO_REORG_WITH_CHECK | modify_type::INDEX_REORG
            )
        {
            args.modify_column_type = modify_type::PRECHECK;
        }
    }

    if active.job.is_rollingback() {
        let step = match args.modify_column_type {
            modify_type::PRECHECK | modify_type::NO_REORG | modify_type::NO_REORG_WITH_CHECK => {
                rollback_modify_column_job(catalog, active, &mut target, &new, start_ts)?
            }
            modify_type::REORG => rollback_modify_column_job_with_reorg(
                catalog,
                active,
                &mut target,
                &mut args,
                start_ts,
            )?,
            modify_type::INDEX_REORG => rollback_modify_column_job_with_index_reorg(
                catalog,
                active,
                &mut target,
                &mut args,
                start_ts,
            )?,
            _ => {
                return Err(DdlPlanError::Encode(format!(
                    "unexpected modify column type {}",
                    args.modify_column_type
                )))
            }
        };
        return Ok(step);
    }

    // Go `checkAndApplyAutoRandomBits`, for every kind of change.
    let database_id = active.job.schema_id;
    check_and_apply_auto_random_bits(
        snapshot,
        database_id,
        &mut target.info,
        &old,
        &new,
        args.new_shard_bits,
        counter_writes,
    )
    .map_err(|error| cancel_ddl_job(active, error))?;
    check_modify_types_owner(&target.info, &old, &new)
        .map_err(|error| cancel_ddl_job(active, error))?;
    check_column_referenced_by_partial_condition(&target.info, new.name.original())
        .map_err(|error| cancel_ddl_job(active, error))?;
    if args.changing_column.is_none() {
        check_column_already_exists(&target.info, &args, &new)
            .map_err(|error| cancel_ddl_job(active, error))?;
    }

    match args.modify_column_type {
        modify_type::PRECHECK => {
            return precheck_for_varchar_to_char(
                catalog,
                active,
                &mut target,
                &mut args,
                &new,
                outcome,
                start_ts,
            );
        }
        modify_type::NO_REORG_WITH_CHECK => {
            return do_modify_column_with_check(
                snapshot,
                catalog,
                active,
                &mut target,
                &args,
                &new,
                outcome,
                start_ts,
            );
        }
        modify_type::NO_REORG => {
            return do_modify_column_no_check(
                snapshot,
                catalog,
                active,
                &mut target,
                &args,
                &new,
                start_ts,
            );
        }
        _ => {}
    }

    // The reorganizing kinds.
    is_generated_related_column(&target.info, &new, &old)
        .map_err(|error| cancel_ddl_job(active, error))?;
    if related_indexes(&target.info, old.id)
        .iter()
        .any(|index| index.read().is_columnar_index())
    {
        return Err(cancel_ddl_job(
            active,
            unsupported_modify_column("columnar indexes on the column"),
        ));
    }
    if old.field_type.has_flag(FieldTypeFlags::PRI_KEY) {
        return Err(cancel_ddl_job(
            active,
            unsupported_modify_column("can't modify column in primary key"),
        ));
    }
    if args.modify_column_type == modify_type::INDEX_REORG {
        initialize_changing_indexes(&mut args, &mut target, &new)
            .map_err(|error| cancel_ddl_job(active, error))?;
        return do_modify_column_index_reorg(
            snapshot,
            catalog,
            active,
            &mut target,
            &mut args,
            outcome,
            start_ts,
        );
    }
    let changing = get_changing_col(&mut args, &mut target, &new)
        .map_err(|error| cancel_ddl_job(active, error))?;
    do_modify_column_type_with_data(
        snapshot,
        catalog,
        active,
        &mut target,
        &mut args,
        changing,
        outcome,
        start_ts,
    )
}

/// Go `checkModifyTypes(oldCol, args.Column, isColumnWithIndex(...))`.
fn check_modify_types_owner(
    info: &TableInfo,
    old: &tidb_model::ColumnInfo,
    new: &tidb_model::ColumnInfo,
) -> Result<(), DdlPlanError> {
    tidb_executor::ddl::check_modify_types(
        &old.field_type,
        &new.field_type,
        old.name.lowercase(),
        is_column_with_index(old.name.original(), info),
    )
    .map_err(|error| {
        let error = error.to_mysql_error();
        DdlPlanError::Admission(DdlAdmissionError::from_mysql_error(error))
    })
}

/// Go `getReplacedColumns` + `checkIndexInModifiableColumns` over the
/// indexes on the column: each key part's length under the new type.
fn check_indexes_in_modifiable_columns(
    info: &TableInfo,
    old: &tidb_model::ColumnInfo,
    new: &tidb_model::ColumnInfo,
) -> Result<(), DdlPlanError> {
    let replaced = |name: &str| -> Option<tidb_model::ColumnInfo> {
        if name == old.name.lowercase() {
            let mut column = new.clone_like_go();
            column.name = old.name.clone();
            return Some(column);
        }
        tidb_model::column::find_column_info(&info.columns, name)
            .map(|column| column.read().clone_like_go())
    };
    for index in related_indexes(info, old.id) {
        let index = index.read();
        let columnar = index.is_columnar_index();
        for part in index.columns.iter_deref() {
            let part = part.read();
            let Some(column) = replaced(part.name.lowercase()) else {
                return Err(coded_refusal(
                    &tidb_util::dbterror::ERR_KEY_COLUMN_DOES_NOT_EXITS,
                    format!("column does not exist: {}", part.name.original()),
                ));
            };
            if columnar {
                continue;
            }
            let field_type = &column.field_type;
            let surviving = (field_type.code().is_type_prefixable()
                && field_type.flen() > part.length)
                .then_some(part.length);
            tidb_executor::ddl::index_prefix::key_part_length_with_max(
                field_type,
                tidb_executor::ddl::index_prefix::IndexedColumn::Named(column.name.original()),
                surviving,
                true,
                tidb_executor::ddl::index_prefix::MAX_INDEX_LENGTH,
            )
            .map_err(|error| {
                let error = error.to_mysql_error();
                DdlPlanError::Admission(DdlAdmissionError::from_mysql_error(error))
            })?;
        }
    }
    Ok(())
}

/// Clears `hasModifyFlag`'s marks on the old column (the rollbacks' first
/// move).
fn clear_modify_flag(column: &GoShared<tidb_model::ColumnInfo>) {
    let mut column = column.write();
    if has_modify_flag(&column) {
        column.del_flag(u64::from(FieldTypeFlags::PREVENT_NULL_INSERT));
        column.del_flag(u64::from(FieldTypeFlags::NOT_NULL));
        column.changing_field_type = None;
    }
}

/// Go `rollbackModifyColumnJob`: a change that rewrote nothing.
fn rollback_modify_column_job(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    target: &mut ModifyTarget,
    new: &tidb_model::ColumnInfo,
    start_ts: u64,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let database_id = active.job.schema_id;
    let mut version = 0;
    let mut write = job_only_write(active.job.id, Vec::new());
    if target.old.read().id == new.id {
        clear_modify_flag(&target.old);
        write = table_step(catalog, active, database_id, &target.info, true, start_ts)?;
        version = write.schema_version;
    }
    active.job.finish_table_job(
        JobState::ROLLBACK_DONE,
        SchemaState::NONE,
        version,
        Some(GoShared::new(target.info.clone_like_go())),
    );
    // Go mocks empty finished arguments for the delete range.
    active
        .job
        .fill_finished_args(Some(GoShared::new(tidb_model::ModifyColumnArgs::default())));
    Ok(action_step(write))
}

/// Go `rollbackModifyColumnJobWithReorg`.
fn rollback_modify_column_job_with_reorg(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    target: &mut ModifyTarget,
    args: &mut tidb_model::ModifyColumnArgs,
    start_ts: u64,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let database_id = active.job.schema_id;
    clear_modify_flag(&target.old);
    let mut changing_index_ids = Vec::new();
    if let Some(changing) = &args.changing_column {
        let changing_id = changing.read().id;
        let merge = add_index::pick_backfill_type(&active.job).need_merge_process();
        for index in target.info.indices.iter_deref() {
            if index
                .read()
                .has_column_in_index_columns(&target.info, changing_id)
            {
                let id = index.read().id;
                changing_index_ids.push(id);
                if merge {
                    changing_index_ids.push(add_index::TEMP_INDEX_PREFIX | id);
                }
            }
        }
        remove_changing_col_and_idxs(&mut target.info, changing_id);
    }
    check_table_info_valid(&target.info)?;
    let write = table_step(catalog, active, database_id, &target.info, true, start_ts)?;
    active.job.finish_table_job(
        JobState::ROLLBACK_DONE,
        SchemaState::NONE,
        write.schema_version,
        Some(GoShared::new(target.info.clone_like_go())),
    );
    args.index_ids = changing_index_ids;
    args.partition_ids = partition_ids(&target.info);
    active
        .job
        .fill_finished_args(Some(GoShared::new(args.clone())));
    Ok(action_step(write))
}

/// Go `rollbackModifyColumnJobWithIndexReorg`.
fn rollback_modify_column_job_with_index_reorg(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    target: &mut ModifyTarget,
    args: &mut tidb_model::ModifyColumnArgs,
    start_ts: u64,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let database_id = active.job.schema_id;
    clear_modify_flag(&target.old);
    let old_name = target.old.read().name.lowercase().to_owned();
    let changing: Vec<GoShared<IndexInfo>> = related_indexes(&target.info, target.old.read().id)
        .into_iter()
        .filter(|index| {
            index.read().columns.iter_deref().any(|part| {
                let part = part.read();
                part.name.lowercase() == old_name && part.use_changing_type
            })
        })
        .collect();
    for index in &changing {
        args.index_ids.push(index.read().id);
    }
    args.index_ids
        .extend(ingest_temp_index_ids(&active.job, &changing));
    remove_old_indexes(&mut target.info, &changing);
    check_table_info_valid(&target.info)?;
    let write = table_step(catalog, active, database_id, &target.info, true, start_ts)?;
    active.job.finish_table_job(
        JobState::ROLLBACK_DONE,
        SchemaState::NONE,
        write.schema_version,
        Some(GoShared::new(target.info.clone_like_go())),
    );
    args.partition_ids = partition_ids(&target.info);
    active
        .job
        .fill_finished_args(Some(GoShared::new(args.clone())));
    Ok(action_step(write))
}

/// Go `getIngestTempIndexIDs`: the temporary indexes a merge
/// reorganization wrote.
fn ingest_temp_index_ids(job: &Job, indexes: &[GoShared<IndexInfo>]) -> Vec<i64> {
    if !add_index::pick_backfill_type(job).need_merge_process() {
        return Vec::new();
    }
    indexes
        .iter()
        .map(|index| add_index::TEMP_INDEX_PREFIX | index.read().id)
        .collect()
}

/// Go `removeChangingColAndIdxs`.
fn remove_changing_col_and_idxs(info: &mut TableInfo, changing_id: i64) {
    let kept_indexes: Vec<_> = (0..info.indices.len())
        .filter_map(|position| {
            let handle = info.indices.get(position)?;
            let keep = !handle.read().has_column_in_index_columns(info, changing_id);
            keep.then_some(Some(handle))
        })
        .collect();
    info.indices = tidb_model::GoSharedPointerSlice::from_handles(kept_indexes);
    let kept_columns: Vec<_> = (0..info.columns.len())
        .filter_map(|position| {
            let handle = info.columns.get(position)?;
            let keep = handle.read().id != changing_id;
            keep.then_some(Some(handle))
        })
        .collect();
    info.columns = tidb_model::GoSharedPointerSlice::from_handles(kept_columns);
    renumber_offsets(info);
}

/// Go `removeOldIndexes`.
fn remove_old_indexes(info: &mut TableInfo, removed: &[GoShared<IndexInfo>]) {
    let ids: Vec<i64> = removed.iter().map(|index| index.read().id).collect();
    let kept: Vec<_> = (0..info.indices.len())
        .filter_map(|position| {
            let handle = info.indices.get(position)?;
            let keep = !ids.contains(&handle.read().id);
            keep.then_some(Some(handle))
        })
        .collect();
    info.indices = tidb_model::GoSharedPointerSlice::from_handles(kept);
}

/// Every column's offset its position, and every key part's offset its
/// column's (Go's slices carry offsets with the columns).
fn renumber_offsets(info: &mut TableInfo) {
    let names: Vec<String> = info
        .columns
        .iter_deref()
        .enumerate()
        .map(|(offset, column)| {
            let mut column = column.write();
            column.offset = offset as i64;
            column.name.lowercase().to_owned()
        })
        .collect();
    for index in info.indices.iter_deref() {
        for part in index.read().columns.iter_deref() {
            let mut part = part.write();
            if let Some(offset) = names.iter().position(|name| *name == part.name.lowercase()) {
                part.offset = offset as i64;
            }
        }
    }
}

/// Go `adjustTableInfoAfterModifyColumn`: the new column takes the old
/// one's place (and position), and the key parts, foreign keys and TTL
/// naming it follow.
fn adjust_table_info_after_modify_column(
    info: &mut TableInfo,
    new: &tidb_model::ColumnInfo,
    old: &GoShared<tidb_model::ColumnInfo>,
    position: &tidb_ast::ColumnPosition,
) -> Result<(), DdlPlanError> {
    let (old_offset, old_state, old_name) = {
        let old = old.read();
        (old.offset, old.state, old.name.clone())
    };
    let mut new = new.clone_like_go();
    new.offset = old_offset;
    new.state = old_state;
    let table_name = info.name.original().to_owned();
    if let tidb_ast::ColumnPosition::After(anchor) = position {
        if anchor.go_to_lower() == old_name.lowercase() {
            return Err(column_not_exists(old_name.original(), &table_name));
        }
    }
    let offset = usize::try_from(old_offset).unwrap_or(0);
    let destination = locate_offset_to_move(offset, position, info)
        .map_err(|_| column_not_exists(old_name.original(), &table_name))?;
    let new_name = new.name.clone();
    let new_handle = GoShared::new(new);
    let handles: Vec<_> = (0..info.columns.len())
        .map(|position| {
            if position == offset {
                Some(new_handle.clone())
            } else {
                info.columns.get(position)
            }
        })
        .collect();
    info.columns = tidb_model::GoSharedPointerSlice::from_handles(handles);
    move_column_info(info, offset, destination);
    // Go `updateNewIdxColsNameOffset` over every index.
    let changed = new_handle.read().clone_like_go();
    for index in info.indices.iter_deref() {
        for part in index.read().columns.iter_deref() {
            let mut part = part.write();
            if part.name.lowercase() == old_name.lowercase() {
                update_index_col(&mut part, &changed);
            }
        }
    }
    // Go `updateFKInfoWhenModifyColumn` and `updateTTLInfoWhenModifyColumn`.
    if old_name.lowercase() != new_name.lowercase() {
        for fk in info.foreign_keys.iter_deref() {
            let mut fk = fk.write();
            let columns: Vec<CiString> = fk
                .cols
                .snapshot()
                .into_iter()
                .map(|column| {
                    if column.lowercase() == old_name.lowercase() {
                        new_name.clone()
                    } else {
                        column.clone()
                    }
                })
                .collect();
            fk.cols = GoSharedSlice::from_vec(columns);
        }
        if let Some(ttl) = &info.ttl_info {
            let mut ttl = ttl.write();
            if ttl.column_name.lowercase() == old_name.lowercase() {
                ttl.column_name = new_name.clone();
            }
        }
    }
    Ok(())
}

/// Go `UpdateIndexCol`: a key part follows the column, keeping its prefix
/// only while the type stays prefixable and wider than it.
fn update_index_col(part: &mut tidb_model::index::IndexColumn, column: &tidb_model::ColumnInfo) {
    part.name = column.name.clone();
    part.offset = column.offset;
    let prefixable = column.field_type.code().is_type_prefixable();
    if !prefixable || column.field_type.flen() <= part.length {
        part.length = tidb_executor::ddl::index_prefix::UNSPECIFIED_LENGTH;
    }
}

/// Go `adjustForeignKeyChildTableInfoAfterModifyColumn`: a renamed parent
/// column renames the children's referenced column. Returns the children's
/// writes.
fn adjust_foreign_key_children(
    catalog: &ClusterCatalog,
    active: &crate::ddl_job_table::ActiveDdlJob,
    info: &TableInfo,
    new: &tidb_model::ColumnInfo,
    old: &tidb_model::ColumnInfo,
) -> Result<Vec<(i64, TableInfo)>, DdlPlanError> {
    if !tidb_vardef::ENABLE_FOREIGN_KEY.load(std::sync::atomic::Ordering::SeqCst)
        || new.name.lowercase() == old.name.lowercase()
    {
        return Ok(Vec::new());
    }
    let schema = active.job.schema_name.to_string();
    let mut children: Vec<(i64, TableInfo)> = Vec::new();
    for database in &catalog.databases {
        for child in &database.tables {
            if child.id == info.id {
                continue;
            }
            let mut changed = false;
            let child_info = child.clone_like_go();
            for fk in child_info.foreign_keys.iter_deref() {
                let mut fk = fk.write();
                if fk.version < tidb_model::table::FK_VERSION1
                    || fk.ref_schema.lowercase() != schema
                    || fk.ref_table.lowercase() != info.name.lowercase()
                {
                    continue;
                }
                let columns: Vec<CiString> = fk
                    .ref_cols
                    .snapshot()
                    .into_iter()
                    .map(|column| {
                        if column.lowercase() == old.name.lowercase() {
                            changed = true;
                            new.name.clone()
                        } else {
                            column.clone()
                        }
                    })
                    .collect();
                fk.ref_cols = GoSharedSlice::from_vec(columns);
            }
            if changed {
                children.push((database.info.id, child_info));
            }
        }
    }
    Ok(children)
}

/// Go `finishModifyColumnWithoutReorg`.
fn finish_modify_column_without_reorg(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    target: &mut ModifyTarget,
    args: &tidb_model::ModifyColumnArgs,
    new: &tidb_model::ColumnInfo,
    start_ts: u64,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let database_id = active.job.schema_id;
    let old = target.old.read().clone_like_go();
    let position = args
        .position
        .as_ref()
        .map(|position| position.read().to_statement())
        .unwrap_or(tidb_ast::ColumnPosition::Default);
    if let Err(error) =
        adjust_table_info_after_modify_column(&mut target.info, new, &target.old, &position)
    {
        active.job.state = JobState::ROLLINGBACK;
        return Err(error);
    }
    let children = adjust_foreign_key_children(catalog, active, &target.info, new, &old)?;
    if let Err(error) = check_table_info_valid(&target.info) {
        // Go: the NOT NULL change was already in the definition.
        active.job.state = JobState::ROLLINGBACK;
        return Err(error);
    }
    let mut write = table_step(catalog, active, database_id, &target.info, true, start_ts)?;
    // Go `updateVersionAndTableInfoWithCheck(..., childTableInfos...)`.
    let mut affected: Vec<AffectedOption> = Vec::new();
    for (schema_id, child) in &children {
        let mut child = child.clone_like_go();
        child.update_ts = start_ts;
        write.mutations.push(BufferMutation::set(
            key::table_kv_key(*schema_id, child.id),
            value::serialize_table_info(&child)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
        )?);
        affected.push(AffectedOption {
            schema_id: *schema_id,
            old_schema_id: *schema_id,
            table_id: child.id,
            old_table_id: child.id,
            ..AffectedOption::default()
        });
    }
    if !affected.is_empty() {
        write.diff.affected_options = GoSharedPointerSlice::from(affected);
        let version = write.schema_version;
        write
            .mutations
            .retain(|mutation| mutation.key() != key::schema_diff_kv_key(version).as_slice());
        write.mutations.push(BufferMutation::set(
            key::schema_diff_kv_key(version),
            value::serialize_schema_diff(&write.diff)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
        )?);
    }
    active.job.finish_table_job(
        JobState::DONE,
        SchemaState::PUBLIC,
        write.schema_version,
        Some(GoShared::new(target.info.clone_like_go())),
    );
    // Go mocks empty finished arguments for the delete range.
    active
        .job
        .fill_finished_args(Some(GoShared::new(tidb_model::ModifyColumnArgs::default())));
    Ok(action_step(write))
}

/// Go `ErrColumnInChange`.
fn column_in_change(old: &tidb_model::ColumnInfo, new_id: i64) -> DdlPlanError {
    coded_refusal(
        &tidb_util::dbterror::ERR_COLUMN_IN_CHANGE,
        format!(
            "column {}, id {} still in changing",
            old.name.original(),
            new_id
        ),
    )
}

/// Go `doModifyColumnNoCheck`.
fn do_modify_column_no_check<S: MetaSnapshot>(
    _snapshot: &mut S,
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    target: &mut ModifyTarget,
    args: &tidb_model::ModifyColumnArgs,
    new: &tidb_model::ColumnInfo,
    start_ts: u64,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let old = target.old.read().clone_like_go();
    if old.id != new.id {
        active.job.state = JobState::ROLLINGBACK;
        return Err(column_in_change(&old, new.id));
    }
    if let Some(step) = revertible_point(catalog, active, target, args, start_ts)? {
        return Ok(step);
    }
    let step = finish_modify_column_without_reorg(catalog, active, target, args, new, start_ts)?;
    Ok(step)
}

/// Go: a multi-schema sub-job stores its revertible mark, the table as it
/// stands, and finishes in the step that publishes every sub-job.
fn revertible_point(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    target: &ModifyTarget,
    args: &tidb_model::ModifyColumnArgs,
    start_ts: u64,
) -> Result<Option<PersistedDdlActionStep>, DdlPlanError> {
    if !is_revertible_sub_job(&active.job) {
        return Ok(None);
    }
    active.job.mark_non_revertible();
    let database_id = active.job.schema_id;
    check_table_info_valid(&target.info)?;
    let write = table_step(catalog, active, database_id, &target.info, false, start_ts)?;
    Ok(Some(with_args(active, args, action_step(write))))
}

/// The first visit of the checked courses: the old column refuses NULL and
/// checks writes against the new type, under a new version, before the rows
/// are checked.
fn mark_modifying(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    target: &mut ModifyTarget,
    args: &tidb_model::ModifyColumnArgs,
    new: &tidb_model::ColumnInfo,
    start_ts: u64,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let database_id = active.job.schema_id;
    let indexed = is_column_with_index(target.old.read().name.original(), &target.info);
    {
        let mut old = target.old.write();
        if is_null_to_not_null_change(&old, new) {
            old.add_flag(u64::from(FieldTypeFlags::PREVENT_NULL_INSERT));
        }
        if !no_reorg_data_strict(&old.field_type, &new.field_type, indexed) {
            old.changing_field_type = Some(GoShared::new(new.field_type.clone()));
        }
    }
    check_table_info_valid(&target.info)?;
    let write = table_step(catalog, active, database_id, &target.info, true, start_ts)?;
    Ok(with_args(active, args, action_step(write)))
}

/// Go `doModifyColumnWithCheck`.
#[allow(clippy::too_many_arguments)]
fn do_modify_column_with_check<S: MetaSnapshot>(
    _snapshot: &mut S,
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    target: &mut ModifyTarget,
    args: &tidb_model::ModifyColumnArgs,
    new: &tidb_model::ColumnInfo,
    outcome: Option<&ReorgOutcome>,
    start_ts: u64,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let old = target.old.read().clone_like_go();
    if old.id != new.id {
        active.job.state = JobState::ROLLINGBACK;
        return Err(column_in_change(&old, new.id));
    }
    if !has_modify_flag(&old) {
        return mark_modifying(catalog, active, target, args, new, start_ts);
    }
    let indexed = is_column_with_index(old.name.original(), &target.info);
    match checked_outcome(active, outcome) {
        None => {
            let check_value_range =
                !no_reorg_data_strict(&old.field_type, &new.field_type, indexed);
            let step = check_request(active, target, new, check_value_range);
            Ok(with_args(active, args, step))
        }
        Some(Some((code, message))) => {
            // Go: the check found a row that does not fit.
            active.job.state = JobState::ROLLINGBACK;
            active.job.fill_args(Some(GoShared::new(args.clone())));
            Err(checked_refusal(code, message))
        }
        Some(None) => {
            if let Some(step) = revertible_point(catalog, active, target, args, start_ts)? {
                return Ok(step);
            }
            finish_modify_column_without_reorg(catalog, active, target, args, new, start_ts)
        }
    }
}

/// Go `precheckForVarcharToChar`: a passing check settles the kind again;
/// a failing one clears the marks and reorganizes.
#[allow(clippy::too_many_arguments)]
fn precheck_for_varchar_to_char(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    target: &mut ModifyTarget,
    args: &mut tidb_model::ModifyColumnArgs,
    new: &tidb_model::ColumnInfo,
    outcome: Option<&ReorgOutcome>,
    start_ts: u64,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let old = target.old.read().clone_like_go();
    if old.id != new.id {
        active.job.state = JobState::ROLLINGBACK;
        return Err(column_in_change(&old, new.id));
    }
    if !has_modify_flag(&old) {
        return mark_modifying(catalog, active, target, args, new, start_ts);
    }
    match checked_outcome(active, outcome) {
        None => {
            let step = check_request(active, target, new, true);
            Ok(with_args(active, args, step))
        }
        Some(None) => {
            // Go: the rows fit; the kind is decided again, the marks kept.
            args.modify_column_type =
                modify_column_type_of(args, &target.info, &old, new, active.job.sql_mode);
            Ok(with_args(
                active,
                args,
                action_step(job_only_write(active.job.id, Vec::new())),
            ))
        }
        Some(Some(_)) => {
            // Go: fall back to the normal reorganization.
            {
                let mut old = target.old.write();
                old.del_flag(u64::from(FieldTypeFlags::PREVENT_NULL_INSERT));
                old.changing_field_type = None;
            }
            let database_id = active.job.schema_id;
            check_table_info_valid(&target.info)?;
            let write = table_step(catalog, active, database_id, &target.info, true, start_ts)?;
            args.modify_column_type = modify_type::REORG;
            Ok(with_args(active, args, action_step(write)))
        }
    }
}

/// One index Go's `FindRelatedIndexesToChange` returns: the index, the key
/// part over the column, and whether it is a temporary index an earlier
/// change created.
struct ChangingIndex {
    index: GoShared<IndexInfo>,
    offset: usize,
    is_temp: bool,
}

/// Go `isTempIndex`.
fn is_temp_index(index: &IndexInfo, info: &TableInfo) -> bool {
    index.columns.iter_deref().any(|part| {
        let part = part.read();
        part.use_changing_type
            || usize::try_from(part.offset)
                .ok()
                .and_then(|offset| info.columns.get(offset))
                .is_some_and(|column| column.read().change_state_info.is_some())
    })
}

/// Go `FindRelatedIndexesToChange`: the indexes over the column, a
/// temporary index standing in for the index it replaces.
fn find_related_indexes_to_change(info: &TableInfo, column: &CiString) -> Vec<ChangingIndex> {
    let mut normal = Vec::new();
    let mut temp = Vec::new();
    for index in info.indices.iter_deref() {
        let position = index
            .read()
            .columns
            .iter_deref()
            .position(|part| part.read().name.lowercase() == column.lowercase());
        if let Some(offset) = position {
            let is_temp = is_temp_index(&index.read(), info);
            let found = ChangingIndex {
                index,
                offset,
                is_temp,
            };
            if is_temp {
                temp.push(found);
            } else {
                normal.push(found);
            }
        }
    }
    for found in temp {
        let origin = found.index.read().get_changing_origin_name();
        if let Some(slot) = normal
            .iter_mut()
            .find(|candidate| candidate.index.read().name.original() == origin)
        {
            *slot = found;
        }
    }
    normal
}

/// Go `initializeChangingIndexes`: for an index-only rebuild, a temporary
/// copy of every index over the column whose key part casts to the new
/// type (a temporary index an earlier change created is reused under a new
/// id, and the old id is collected for the delete range).
fn initialize_changing_indexes(
    args: &mut tidb_model::ModifyColumnArgs,
    target: &mut ModifyTarget,
    new: &tidb_model::ColumnInfo,
) -> Result<(), DdlPlanError> {
    if args.changing_column.is_some() {
        return Ok(());
    }
    check_column_already_exists(&target.info, args, new)?;
    let mut changing_type = target.old.read().clone_like_go();
    changing_type.field_type = new.field_type.clone();
    let old_name = target.old.read().name.clone();
    let mut redundant = Vec::new();
    for found in find_related_indexes_to_change(&target.info, &old_name) {
        target.info.max_index_id += 1;
        let new_id = target.info.max_index_id;
        if !found.is_temp {
            let mut temp = found.index.read().clone_like_go();
            temp.state = SchemaState::NONE;
            temp.id = new_id;
            temp.name = CiString::new(tidb_model::index::gen_unique_changing_index_name(
                &target.info,
                &found.index.read(),
            ));
            if let Some(part) = temp.columns.get(found.offset) {
                let mut part = part.write();
                part.use_changing_type = true;
                update_index_col(&mut part, &changing_type);
            }
            target
                .info
                .indices
                .push_handle_go(Some(GoShared::new(temp)));
        } else {
            let mut temp = found.index.write();
            temp.state = SchemaState::NONE;
            redundant.push(temp.id);
            temp.id = new_id;
            if let Some(part) = temp.columns.get(found.offset) {
                let mut part = part.write();
                part.use_changing_type = true;
                update_index_col(&mut part, &changing_type);
            }
        }
    }
    args.redundant_idxs = redundant;
    args.changing_column = Some(GoShared::new(new.clone_like_go()));
    Ok(())
}

/// Go `getChangingCol`: the temporary column a row rewrite fills, appended
/// with its temporary indexes on the first visit and found by id later.
fn get_changing_col(
    args: &mut tidb_model::ModifyColumnArgs,
    target: &mut ModifyTarget,
    new: &tidb_model::ColumnInfo,
) -> Result<GoShared<tidb_model::ColumnInfo>, DdlPlanError> {
    if let Some(changing) = &args.changing_column {
        let id = changing.read().id;
        return tidb_model::column::find_column_info_by_id(&target.info.columns, id).ok_or_else(
            || {
                let old = target.old.read();
                column_not_exists(old.name.original(), target.info.name.original())
            },
        );
    }
    let old = target.old.read().clone_like_go();
    let mut changing = new.clone_like_go();
    changing.name = CiString::new(tidb_model::column::gen_unique_changing_column_name(
        &target.info,
        &old,
    ));
    changing.change_state_info = Some(GoShared::new(tidb_model::column::ChangeStateInfo {
        dependency_column_offset: old.offset,
    }));
    // Go `InitAndAddColumnToTable`.
    target.info.max_column_id += 1;
    changing.id = target.info.max_column_id;
    changing.state = SchemaState::NONE;
    changing.offset = target.info.columns.len() as i64;
    let changing = GoShared::new(changing);
    target.info.columns.push_handle_go(Some(changing.clone()));
    let changing_info = changing.read().clone_like_go();
    let mut redundant = Vec::new();
    for found in find_related_indexes_to_change(&target.info, &old.name) {
        target.info.max_index_id += 1;
        let new_id = target.info.max_index_id;
        if !found.is_temp {
            let mut temp = found.index.read().clone_like_go();
            temp.id = new_id;
            temp.name = CiString::new(tidb_model::index::gen_unique_changing_index_name(
                &target.info,
                &found.index.read(),
            ));
            if let Some(part) = temp.columns.get(found.offset) {
                update_index_col(&mut part.write(), &changing_info);
            }
            target
                .info
                .indices
                .push_handle_go(Some(GoShared::new(temp)));
        } else {
            let mut temp = found.index.write();
            redundant.push(temp.id);
            temp.id = new_id;
            if let Some(part) = temp.columns.get(found.offset) {
                update_index_col(&mut part.write(), &changing_info);
            }
        }
    }
    args.redundant_idxs = redundant;
    Ok(changing)
}

/// Go `updateObjectState`.
fn update_object_state(
    column: Option<&GoShared<tidb_model::ColumnInfo>>,
    indexes: &[GoShared<IndexInfo>],
    state: SchemaState,
) {
    if let Some(column) = column {
        column.write().state = state;
    }
    for index in indexes {
        index.write().state = state;
    }
}

/// Go `initForReorgIndexes`: a merge reorganization starts its indexes in
/// the running backfill state; a partial index needs fast reorg.
fn init_for_reorg_indexes(job: &Job, indexes: &[GoShared<IndexInfo>]) -> Result<(), DdlPlanError> {
    if indexes.is_empty() {
        return Ok(());
    }
    let reorg_type = add_index::pick_backfill_type(job);
    if matches!(
        reorg_type,
        tidb_model::ReorgType::TXN | tidb_model::ReorgType::TXN_MERGE
    ) && indexes.iter().any(|index| index.read().has_condition())
    {
        return Err(add_index::unsupported_partial_index());
    }
    if reorg_type.need_merge_process() {
        for index in indexes {
            index.write().backfill_state = tidb_model::BackfillState::RUNNING;
        }
    }
    Ok(())
}

fn reorg_stage(job: &Job) -> tidb_model::reorg::ReorgStage {
    job.reorg_meta
        .as_ref()
        .map_or(tidb_model::reorg::ReorgStage::NONE, |meta| {
            meta.read().stage
        })
}

fn set_reorg_stage(job: &Job, stage: tidb_model::reorg::ReorgStage) {
    if let Some(meta) = &job.reorg_meta {
        meta.write().stage = stage;
    }
}

fn analyze_state(job: &Job) -> i8 {
    job.reorg_meta
        .as_ref()
        .map_or(tidb_model::reorg::analyze_state::NONE, |meta| {
            meta.read().analyze_state
        })
}

fn set_analyze_state(job: &Job, state: i8) {
    if let Some(meta) = &job.reorg_meta {
        meta.write().analyze_state = state;
    }
}

/// Go `reorderChangingIdx`: the changing indexes in the old indexes' order.
fn reorder_changing_idx(old: &[GoShared<IndexInfo>], changing: &mut [GoShared<IndexInfo>]) {
    let by_origin: Vec<(String, GoShared<IndexInfo>)> = changing
        .iter()
        .map(|index| {
            let read = index.read();
            let origin = if read.state != SchemaState::PUBLIC {
                read.get_changing_origin_name()
            } else {
                read.name.original().to_owned()
            };
            (origin, index.clone())
        })
        .collect();
    for (position, old_index) in old.iter().enumerate() {
        let wanted = old_index.read().get_removing_origin_name();
        if let Some((_, found)) = by_origin.iter().find(|(origin, _)| *origin == wanted) {
            changing[position] = found.clone();
        }
    }
}

/// Go `markOldIndexesRemoving`.
fn mark_old_indexes_removing(old: &[GoShared<IndexInfo>], changing: &[GoShared<IndexInfo>]) {
    for (old_index, changing_index) in old.iter().zip(changing) {
        let old_name = old_index.read().name.original().to_owned();
        let public_name = CiString::new(old_index.read().get_removing_origin_name());
        let removing_name = CiString::new(tidb_model::column::gen_removing_obj_name(&old_name));
        changing_index.write().name = public_name;
        old_index.write().name = removing_name;
    }
}

/// Go `renameColumnTo`.
fn rename_column_to(
    column: &GoShared<tidb_model::ColumnInfo>,
    indexes: &[GoShared<IndexInfo>],
    name: CiString,
) {
    let old = column.read().name.lowercase().to_owned();
    for index in indexes {
        for part in index.read().columns.iter_deref() {
            let mut part = part.write();
            if part.name.lowercase() == old {
                part.name = name.clone();
            }
        }
    }
    column.write().name = name;
}

/// Go `markOldObjectRemoving`.
fn mark_old_object_removing(
    old: &GoShared<tidb_model::ColumnInfo>,
    changing: &GoShared<tidb_model::ColumnInfo>,
    old_indexes: &[GoShared<IndexInfo>],
    changing_indexes: &[GoShared<IndexInfo>],
    new_name: &CiString,
) {
    if old.read().id != changing.read().id {
        let removing = CiString::new(tidb_model::column::gen_removing_obj_name(
            old.read().name.original(),
        ));
        rename_column_to(old, old_indexes, removing);
        rename_column_to(changing, changing_indexes, new_name.clone());
    }
    mark_old_indexes_removing(old_indexes, changing_indexes);
}

/// Go `moveChangingColumnToDest`: the changing column swaps places with the
/// old one, then moves where the statement asked.
fn move_changing_column_to_dest(
    info: &mut TableInfo,
    old: &GoShared<tidb_model::ColumnInfo>,
    changing: &GoShared<tidb_model::ColumnInfo>,
    position: &tidb_ast::ColumnPosition,
) -> Result<(), DdlPlanError> {
    let old_offset = usize::try_from(old.read().offset).unwrap_or(0);
    let changing_offset = usize::try_from(changing.read().offset).unwrap_or(0);
    move_column_info(info, old_offset, changing_offset);
    let changing_offset = usize::try_from(changing.read().offset).unwrap_or(0);
    move_column_info(info, changing_offset, old_offset);
    let current = usize::try_from(changing.read().offset).unwrap_or(0);
    let destination = locate_offset_to_move(current, position, info)?;
    move_column_info(info, current, destination);
    Ok(())
}

/// Go `moveOldColumnToBack`: the old column behind every column in a more
/// public state.
fn move_old_column_to_back(info: &mut TableInfo, old: &GoShared<tidb_model::ColumnInfo>) {
    let mut order = vec![
        SchemaState::PUBLIC,
        SchemaState::WRITE_REORGANIZATION,
        SchemaState::WRITE_ONLY,
        SchemaState::DELETE_REORGANIZATION,
        SchemaState::DELETE_ONLY,
        SchemaState::NONE,
    ];
    let old_state = old.read().state;
    while order.last().is_some_and(|state| *state != old_state) {
        order.pop();
    }
    let old_id = old.read().id;
    let mut destination = info.columns.len() - 1;
    for (position, column) in info.columns.iter_deref().enumerate() {
        let column = column.read();
        if column.id == old_id {
            continue;
        }
        if !order.contains(&column.state) {
            destination = position;
            break;
        }
    }
    let offset = usize::try_from(old.read().offset).unwrap_or(0);
    move_column_info(info, offset, destination);
}

/// Go `indexContainsOtherReorg`.
fn index_contains_other_reorg(
    info: &TableInfo,
    index: &IndexInfo,
    changing: &tidb_model::ColumnInfo,
) -> bool {
    index.columns.iter_deref().any(|part| {
        let part = part.read();
        let Some(column) = usize::try_from(part.offset)
            .ok()
            .and_then(|offset| info.columns.get(offset))
        else {
            return false;
        };
        let column = column.read();
        column.id != changing.id && (part.use_changing_type || column.change_state_info.is_some())
    })
}

/// Go `moveIndexInfoToDest` / `swapIndexInfoByID`.
fn move_index_info_to_dest(
    info: &mut TableInfo,
    changing: &tidb_model::ColumnInfo,
    old: &[GoShared<IndexInfo>],
    changing_indexes: &[GoShared<IndexInfo>],
) {
    for (old_index, changing_index) in old.iter().zip(changing_indexes) {
        if index_contains_other_reorg(info, &changing_index.read(), changing) {
            continue;
        }
        let (a, b) = (old_index.read().id, changing_index.read().id);
        let position_of = |id: i64| {
            info.indices
                .iter_deref()
                .position(|index| index.read().id == id)
        };
        if let (Some(pa), Some(pb)) = (position_of(a), position_of(b)) {
            let mut handles: Vec<_> = (0..info.indices.len())
                .map(|position| info.indices.get(position))
                .collect();
            handles.swap(pa, pb);
            info.indices = tidb_model::GoSharedPointerSlice::from_handles(handles);
        }
    }
}

/// Go `updateModifyingCols`.
fn update_modifying_cols(
    old: &GoShared<tidb_model::ColumnInfo>,
    changing: &GoShared<tidb_model::ColumnInfo>,
) {
    let changing_offset = {
        let mut changing = changing.write();
        changing.change_state_info = None;
        changing.origin_default_value = tidb_model::GoAny::nil();
        changing.origin_default_value_bit = Default::default();
        changing.offset
    };
    old.write().change_state_info = Some(GoShared::new(tidb_model::column::ChangeStateInfo {
        dependency_column_offset: changing_offset,
    }));
}

/// Go `removeOldObjects`: the old column (moved last) and its indexes go;
/// the indexes' ids are returned for the delete range.
fn remove_old_objects(
    info: &mut TableInfo,
    old: &GoShared<tidb_model::ColumnInfo>,
    old_indexes: &[GoShared<IndexInfo>],
) -> Vec<i64> {
    let offset = usize::try_from(old.read().offset).unwrap_or(0);
    let last = info.columns.len() - 1;
    move_column_info(info, offset, last);
    info.columns.delete_go(last, last + 1);
    let removed: Vec<i64> = old_indexes.iter().map(|index| index.read().id).collect();
    if !old_indexes.is_empty() {
        remove_old_indexes(info, old_indexes);
    }
    removed
}

/// Go `doModifyColumnIndexReorg`.
fn do_modify_column_index_reorg<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    target: &mut ModifyTarget,
    args: &mut tidb_model::ModifyColumnArgs,
    outcome: Option<&ReorgOutcome>,
    start_ts: u64,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let database_id = active.job.schema_id;
    let new = args
        .column
        .as_ref()
        .expect("checked above")
        .read()
        .clone_like_go();
    let old_id = target.old.read().id;
    let all = related_indexes(&target.info, old_id);
    let (old_indexes, mut changing_indexes): (Vec<_>, Vec<_>) =
        if active.job.schema_state == SchemaState::PUBLIC {
            (
                all.iter()
                    .filter(|index| index.read().is_removing())
                    .cloned()
                    .collect(),
                Vec::new(),
            )
        } else {
            let half = all.len() / 2;
            (all[..half].to_vec(), all[half..].to_vec())
        };
    let finish = |active: &mut crate::ddl_job_table::ActiveDdlJob,
                  target: &mut ModifyTarget,
                  args: &mut tidb_model::ModifyColumnArgs,
                  changing_indexes: &[GoShared<IndexInfo>]|
     -> Result<PersistedDdlActionStep, DdlPlanError> {
        let mut removed: Vec<i64> = old_indexes.iter().map(|index| index.read().id).collect();
        removed.extend(ingest_temp_index_ids(&active.job, changing_indexes));
        remove_old_indexes(&mut target.info, &old_indexes);
        {
            let mut old = target.old.write();
            old.changing_field_type = None;
            old.del_flag(u64::from(FieldTypeFlags::PREVENT_NULL_INSERT));
        }
        let write = table_step(catalog, active, database_id, &target.info, true, start_ts)?;
        active.job.finish_table_job(
            JobState::DONE,
            SchemaState::PUBLIC,
            write.schema_version,
            Some(GoShared::new(target.info.clone_like_go())),
        );
        removed.extend(args.redundant_idxs.iter().copied());
        args.index_ids = removed;
        args.partition_ids = partition_ids(&target.info);
        active
            .job
            .fill_finished_args(Some(GoShared::new(args.clone())));
        Ok(action_step(write))
    };
    match active.job.schema_state {
        SchemaState::NONE => {
            {
                let mut old = target.old.write();
                old.add_flag(u64::from(FieldTypeFlags::PREVENT_NULL_INSERT));
                old.changing_field_type = Some(GoShared::new(new.field_type.clone()));
            }
            update_object_state(None, &changing_indexes, SchemaState::DELETE_ONLY);
            set_reorg_stage(
                &active.job,
                tidb_model::reorg::ReorgStage::MODIFY_COLUMN_UPDATE_COLUMN,
            );
            if let Err(error) = init_for_reorg_indexes(&active.job, &changing_indexes) {
                active.job.state = JobState::ROLLINGBACK;
                return Err(error);
            }
            check_table_info_valid(&target.info)?;
            let write = table_step(catalog, active, database_id, &target.info, true, start_ts)?;
            active.job.schema_state = SchemaState::DELETE_ONLY;
            args.changing_idxs = changing_indexes
                .iter()
                .map(|index| index.read().clone_like_go())
                .collect();
            Ok(with_args(active, args, action_step(write)))
        }
        SchemaState::DELETE_ONLY => match checked_outcome(active, outcome) {
            None => {
                let step = check_request(active, target, &new, true);
                Ok(with_args(active, args, step))
            }
            Some(Some((code, message))) => {
                active.job.state = JobState::ROLLINGBACK;
                active.job.fill_args(Some(GoShared::new(args.clone())));
                Err(checked_refusal(code, message))
            }
            Some(None) => {
                update_object_state(None, &changing_indexes, SchemaState::WRITE_ONLY);
                let write = table_step(catalog, active, database_id, &target.info, true, start_ts)?;
                active.job.schema_state = SchemaState::WRITE_ONLY;
                Ok(with_args(active, args, action_step(write)))
            }
        },
        SchemaState::WRITE_ONLY => {
            update_object_state(None, &changing_indexes, SchemaState::WRITE_REORGANIZATION);
            let write = table_step(catalog, active, database_id, &target.info, true, start_ts)?;
            active.job.snapshot_version = 0;
            active.job.schema_state = SchemaState::WRITE_REORGANIZATION;
            Ok(with_args(active, args, action_step(write)))
        }
        SchemaState::WRITE_REORGANIZATION => {
            if analyze_state(&active.job) == tidb_model::reorg::analyze_state::NONE {
                let stage = reorg_stage(&active.job);
                if stage == tidb_model::reorg::ReorgStage::MODIFY_COLUMN_UPDATE_COLUMN {
                    // No rows to rewrite: on to the indexes.
                    active.job.snapshot_version = 0;
                    set_reorg_stage(
                        &active.job,
                        tidb_model::reorg::ReorgStage::MODIFY_COLUMN_RECREATE_INDEX,
                    );
                    return Ok(with_args(
                        active,
                        args,
                        action_step(job_only_write(active.job.id, Vec::new())),
                    ));
                }
                if stage == tidb_model::reorg::ReorgStage::MODIFY_COLUMN_RECREATE_INDEX {
                    match do_reorg_work_for_create_index(
                        snapshot,
                        catalog,
                        active,
                        &target.info,
                        &changing_indexes,
                        outcome,
                        start_ts,
                    )? {
                        CreateIndexReorg::Step(step) => return Ok(with_args(active, args, step)),
                        CreateIndexReorg::Failed(error) => {
                            active.job.state = JobState::ROLLINGBACK;
                            active.job.fill_args(Some(GoShared::new(args.clone())));
                            return Err(error);
                        }
                        CreateIndexReorg::Done => {
                            set_reorg_stage(
                                &active.job,
                                tidb_model::reorg::ReorgStage::MODIFY_COLUMN_COMPLETED,
                            );
                            let write = table_step(
                                catalog,
                                active,
                                database_id,
                                &target.info,
                                true,
                                start_ts,
                            )?;
                            return Ok(with_args(active, args, action_step(write)));
                        }
                    }
                }
                // Completed: no analyze for a job whose
                // `tidb_enable_ddl_analyze` is off.
                set_analyze_state(&active.job, tidb_model::reorg::analyze_state::SKIPPED);
                check_and_mark_non_revertible(&mut active.job);
                return Ok(with_args(
                    active,
                    args,
                    action_step(job_only_write(active.job.id, Vec::new())),
                ));
            }
            // Analyze done or skipped: the new type takes over.
            reorder_changing_idx(&old_indexes, &mut changing_indexes);
            let (old_type, old_name, old_id, old_offset) = {
                let old = target.old.read();
                (old.field_type.clone(), old.name.clone(), old.id, old.offset)
            };
            let mut replaced = new.clone_like_go();
            replaced.changing_field_type = Some(GoShared::new(old_type));
            replaced.offset = old_offset;
            replaced.id = old_id;
            replaced.state = SchemaState::PUBLIC;
            let replaced = GoShared::new(replaced);
            let slot = usize::try_from(old_offset).unwrap_or(0);
            let handles: Vec<_> = (0..target.info.columns.len())
                .map(|position| {
                    if position == slot {
                        Some(replaced.clone())
                    } else {
                        target.info.columns.get(position)
                    }
                })
                .collect();
            target.info.columns = tidb_model::GoSharedPointerSlice::from_handles(handles);
            target.old = replaced.clone();
            update_object_state(None, &old_indexes, SchemaState::WRITE_ONLY);
            update_object_state(None, &changing_indexes, SchemaState::PUBLIC);
            let position = args
                .position
                .as_ref()
                .map(|position| position.read().to_statement())
                .unwrap_or(tidb_ast::ColumnPosition::Default);
            move_changing_column_to_dest(&mut target.info, &replaced, &replaced, &position)?;
            let replaced_info = replaced.read().clone_like_go();
            move_index_info_to_dest(
                &mut target.info,
                &replaced_info,
                &old_indexes,
                &changing_indexes,
            );
            mark_old_indexes_removing(&old_indexes, &changing_indexes);
            let new_name = new.name.clone();
            let offset = replaced.read().offset;
            for (old_index, changing_index) in old_indexes.iter().zip(&changing_indexes) {
                let parts = changing_index.read().columns.clone();
                for (position, part) in parts.iter_deref().enumerate() {
                    if part.read().name.lowercase() != old_name.lowercase() {
                        continue;
                    }
                    if let Some(old_part) = old_index.read().columns.get(position) {
                        let mut old_part = old_part.write();
                        old_part.name = new_name.clone();
                        old_part.offset = offset;
                        old_part.use_changing_type = true;
                    }
                    let mut part = part.write();
                    part.name = new_name.clone();
                    part.offset = offset;
                    part.use_changing_type = false;
                }
            }
            active.job.schema_state = SchemaState::PUBLIC;
            let write = table_step(catalog, active, database_id, &target.info, true, start_ts)?;
            Ok(with_args(active, args, action_step(write)))
        }
        SchemaState::PUBLIC => {
            if old_indexes.is_empty() {
                return finish(active, target, args, &changing_indexes);
            }
            let old_state = old_indexes[0].read().state;
            match old_state {
                SchemaState::WRITE_ONLY => {
                    update_object_state(None, &old_indexes, SchemaState::DELETE_ONLY);
                    let write =
                        table_step(catalog, active, database_id, &target.info, true, start_ts)?;
                    Ok(with_args(active, args, action_step(write)))
                }
                SchemaState::DELETE_ONLY => finish(active, target, args, &changing_indexes),
                state => Err(DdlPlanError::Encode(format!(
                    "unexpected column state {state} in modify column job"
                ))),
            }
        }
        state => Err(DdlPlanError::from(
            tidb_util::dbterror::ERR_INVALID_DDL_STATE.generate_with_stack_by_args(&[
                FormatArg::from("column"),
                FormatArg::new(state.to_string(), state.0.to_string(), "model.SchemaState"),
            ]),
        )),
    }
}

/// Go `doModifyColumnTypeWithData`.
#[allow(clippy::too_many_arguments)]
fn do_modify_column_type_with_data<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    target: &mut ModifyTarget,
    args: &mut tidb_model::ModifyColumnArgs,
    changing: GoShared<tidb_model::ColumnInfo>,
    outcome: Option<&ReorgOutcome>,
    start_ts: u64,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let database_id = active.job.schema_id;
    let new = args
        .column
        .as_ref()
        .expect("checked above")
        .read()
        .clone_like_go();
    let column_name = new.name.clone();
    let position = args
        .position
        .as_ref()
        .map(|position| position.read().to_statement())
        .unwrap_or(tidb_ast::ColumnPosition::Default);
    let changing_id = changing.read().id;
    let changing_indexes = related_indexes(&target.info, changing_id);
    let original_state = changing.read().state;
    match original_state {
        SchemaState::NONE => {
            // Go `validatePosition`.
            let old = target.old.read().clone_like_go();
            let position_error = match &position {
                tidb_ast::ColumnPosition::After(anchor)
                    if anchor.go_to_lower() == old.name.lowercase() =>
                {
                    Some(column_not_exists(
                        old.name.original(),
                        target.info.name.original(),
                    ))
                }
                _ => locate_offset_to_move(
                    usize::try_from(old.offset).unwrap_or(0),
                    &position,
                    &target.info,
                )
                .err(),
            };
            if let Some(error) = position_error {
                active.job.state = JobState::ROLLINGBACK;
                return Err(error);
            }
            if is_null_to_not_null_change(&old, &changing.read()) {
                target
                    .old
                    .write()
                    .add_flag(u64::from(FieldTypeFlags::PREVENT_NULL_INSERT));
            }
            update_object_state(Some(&changing), &changing_indexes, SchemaState::DELETE_ONLY);
            set_reorg_stage(
                &active.job,
                tidb_model::reorg::ReorgStage::MODIFY_COLUMN_UPDATE_COLUMN,
            );
            if let Err(error) = init_for_reorg_indexes(&active.job, &changing_indexes) {
                active.job.state = JobState::ROLLINGBACK;
                return Err(error);
            }
            check_table_info_valid(&target.info)?;
            let write = table_step(catalog, active, database_id, &target.info, true, start_ts)?;
            active.job.schema_state = SchemaState::DELETE_ONLY;
            args.changing_column = Some(GoShared::new(changing.read().clone_like_go()));
            args.changing_idxs = changing_indexes
                .iter()
                .map(|index| index.read().clone_like_go())
                .collect();
            Ok(with_args(active, args, action_step(write)))
        }
        SchemaState::DELETE_ONLY => {
            // Go: NULL -> NOT NULL checks the rows first.
            let old = target.old.read().clone_like_go();
            if is_null_to_not_null_change(&old, &changing.read()) {
                match checked_outcome(active, outcome) {
                    None => {
                        let step = check_request(active, target, &new, false);
                        return Ok(with_args(active, args, step));
                    }
                    Some(Some((code, message))) => {
                        active.job.state = JobState::ROLLINGBACK;
                        active.job.fill_args(Some(GoShared::new(args.clone())));
                        return Err(checked_refusal(code, message));
                    }
                    Some(None) => {}
                }
            }
            update_object_state(Some(&changing), &changing_indexes, SchemaState::WRITE_ONLY);
            let write = table_step(catalog, active, database_id, &target.info, true, start_ts)?;
            active.job.schema_state = SchemaState::WRITE_ONLY;
            Ok(with_args(active, args, action_step(write)))
        }
        SchemaState::WRITE_ONLY => {
            update_object_state(
                Some(&changing),
                &changing_indexes,
                SchemaState::WRITE_REORGANIZATION,
            );
            let write = table_step(catalog, active, database_id, &target.info, true, start_ts)?;
            active.job.snapshot_version = 0;
            active.job.schema_state = SchemaState::WRITE_REORGANIZATION;
            Ok(with_args(active, args, action_step(write)))
        }
        SchemaState::WRITE_REORGANIZATION => {
            if analyze_state(&active.job) == tidb_model::reorg::analyze_state::NONE {
                let stage = reorg_stage(&active.job);
                if stage == tidb_model::reorg::ReorgStage::MODIFY_COLUMN_UPDATE_COLUMN {
                    match do_reorg_work_for_modify_column(
                        snapshot, catalog, active, target, &changing, outcome,
                    )? {
                        ColumnReorgVisit::Step(step) => return Ok(with_args(active, args, step)),
                        ColumnReorgVisit::Failed(error) => {
                            active.job.state = JobState::ROLLINGBACK;
                            active.job.fill_args(Some(GoShared::new(args.clone())));
                            return Err(error);
                        }
                        ColumnReorgVisit::Done => {
                            if changing_indexes.is_empty() {
                                set_reorg_stage(
                                    &active.job,
                                    tidb_model::reorg::ReorgStage::MODIFY_COLUMN_COMPLETED,
                                );
                            } else {
                                active.job.snapshot_version = 0;
                                set_reorg_stage(
                                    &active.job,
                                    tidb_model::reorg::ReorgStage::MODIFY_COLUMN_RECREATE_INDEX,
                                );
                            }
                            return Ok(with_args(
                                active,
                                args,
                                action_step(job_only_write(active.job.id, Vec::new())),
                            ));
                        }
                    }
                }
                if stage == tidb_model::reorg::ReorgStage::MODIFY_COLUMN_RECREATE_INDEX {
                    match do_reorg_work_for_create_index(
                        snapshot,
                        catalog,
                        active,
                        &target.info,
                        &changing_indexes,
                        outcome,
                        start_ts,
                    )? {
                        CreateIndexReorg::Step(step) => return Ok(with_args(active, args, step)),
                        CreateIndexReorg::Failed(error) => {
                            active.job.state = JobState::ROLLINGBACK;
                            active.job.fill_args(Some(GoShared::new(args.clone())));
                            return Err(error);
                        }
                        CreateIndexReorg::Done => {
                            set_reorg_stage(
                                &active.job,
                                tidb_model::reorg::ReorgStage::MODIFY_COLUMN_COMPLETED,
                            );
                            let write = table_step(
                                catalog,
                                active,
                                database_id,
                                &target.info,
                                true,
                                start_ts,
                            )?;
                            return Ok(with_args(active, args, action_step(write)));
                        }
                    }
                }
                // Completed: no analyze for a job whose
                // `tidb_enable_ddl_analyze` is off.
                set_analyze_state(&active.job, tidb_model::reorg::analyze_state::SKIPPED);
                check_and_mark_non_revertible(&mut active.job);
                return Ok(with_args(
                    active,
                    args,
                    action_step(job_only_write(active.job.id, Vec::new())),
                ));
            }
            // Analyze done or skipped: swap the changing objects in.
            let old_indexes = related_indexes(&target.info, target.old.read().id);
            if let Some(ttl) = &target.info.ttl_info {
                let mut ttl = ttl.write();
                if ttl.column_name.lowercase() == target.old.read().name.lowercase() {
                    ttl.column_name = column_name.clone();
                }
            }
            let mut changing_indexes = related_indexes(&target.info, changing_id);
            reorder_changing_idx(&old_indexes, &mut changing_indexes);
            update_object_state(Some(&target.old), &old_indexes, SchemaState::WRITE_ONLY);
            update_object_state(Some(&changing), &changing_indexes, SchemaState::PUBLIC);
            mark_old_object_removing(
                &target.old,
                &changing,
                &old_indexes,
                &changing_indexes,
                &column_name,
            );
            move_changing_column_to_dest(&mut target.info, &target.old, &changing, &position)?;
            move_old_column_to_back(&mut target.info, &target.old);
            move_index_info_to_dest(
                &mut target.info,
                &changing.read().clone_like_go(),
                &old_indexes,
                &changing_indexes,
            );
            update_modifying_cols(&target.old, &changing);
            active.job.schema_state = SchemaState::PUBLIC;
            let write = table_step(catalog, active, database_id, &target.info, true, start_ts)?;
            Ok(with_args(active, args, action_step(write)))
        }
        SchemaState::PUBLIC => {
            let old_indexes = related_indexes(&target.info, target.old.read().id);
            let old_state = target.old.read().state;
            match old_state {
                SchemaState::WRITE_ONLY => {
                    update_object_state(Some(&target.old), &old_indexes, SchemaState::DELETE_ONLY);
                    move_old_column_to_back(&mut target.info, &target.old);
                    let write =
                        table_step(catalog, active, database_id, &target.info, true, start_ts)?;
                    return Ok(with_args(active, args, action_step(write)));
                }
                SchemaState::DELETE_ONLY => {}
                state => {
                    return Err(DdlPlanError::Encode(format!(
                        "unexpected column state {state} in modify column job"
                    )))
                }
            }
            let mut removed = remove_old_objects(&mut target.info, &target.old, &old_indexes);
            removed.extend(ingest_temp_index_ids(&active.job, &changing_indexes));
            let mut write = table_step(catalog, active, database_id, &target.info, true, start_ts)?;
            // Go `asyncNotifyEvent(NewModifyColumnEvent(...))`.
            if !tidb_metadef::is_mem_or_sys_db(&active.job.schema_name.to_string().go_to_lower()) {
                let analyzed = analyze_state(&active.job) == tidb_model::reorg::analyze_state::DONE;
                append_schema_change_mutations(
                    snapshot,
                    catalog,
                    active.job.id,
                    &[(
                        notifier_sub_job_id(&active.job),
                        SchemaChangeEvent::modify_columns(
                            target.info.clone_like_go(),
                            vec![changing.read().clone_like_go()],
                            analyzed,
                        ),
                    )],
                    &mut write.mutations,
                )?;
            }
            active.job.finish_table_job(
                JobState::DONE,
                SchemaState::PUBLIC,
                write.schema_version,
                Some(GoShared::new(target.info.clone_like_go())),
            );
            removed.extend(args.redundant_idxs.iter().copied());
            args.index_ids = removed;
            args.new_index_ids = changing_indexes
                .iter()
                .map(|index| index.read().id)
                .collect();
            args.partition_ids = partition_ids(&target.info);
            active
                .job
                .fill_finished_args(Some(GoShared::new(args.clone())));
            Ok(action_step(write))
        }
        state => Err(DdlPlanError::from(
            tidb_util::dbterror::ERR_INVALID_DDL_STATE.generate_with_stack_by_args(&[
                FormatArg::from("column"),
                FormatArg::new(state.to_string(), state.0.to_string(), "model.SchemaState"),
            ]),
        )),
    }
}

/// What one visit of the row rewrite came to.
enum ColumnReorgVisit {
    Step(PersistedDdlActionStep),
    Done,
    Failed(DdlPlanError),
}

/// Go `doReorgWorkForModifyColumn`: the row rewrite into the changing
/// column, in the owner's own batched transactions; the warnings it met are
/// merged into the job's `ReorgMeta`.
fn do_reorg_work_for_modify_column<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    target: &ModifyTarget,
    changing: &GoShared<tidb_model::ColumnInfo>,
    outcome: Option<&ReorgOutcome>,
) -> Result<ColumnReorgVisit, DdlPlanError> {
    let spec = ColumnReorgSpec {
        old_column_id: target.old.read().id,
        changing_column_id: changing.read().id,
    };
    let outcome = outcome.filter(|outcome| {
        outcome.job_id == active.job.id && outcome.column.as_ref() == Some(&spec)
    });
    if let Some(outcome) = outcome {
        merge_reorg_warnings(&active.job, &outcome.warnings);
    }
    let request =
        |active: &crate::ddl_job_table::ActiveDdlJob, stage: ReorgStageRequest| AddIndexReorg {
            job_id: active.job.id,
            stage,
            table: Box::new(target.info.clone_like_go()),
            index_ids: Vec::new(),
            merging: false,
            physical_ids: add_index::physical_ids(&target.info),
            batch_size: add_index::job_batch_size(&active.job),
            context: DdlStatementContext(add_index::reorg_statement_context(&active.job)),
            row_count: active.job.row_count,
            column: Some(spec.clone()),
            sub_job: add_index::reorg_sub_job(&active.job),
            clean_up_global_index: false,
            partition_phase: None,
            schema_id: active.job.schema_id,
        };
    Ok(match outcome.map(|outcome| &outcome.result) {
        None if active.job.snapshot_version == 0 => ColumnReorgVisit::Step(reorg_request(
            &active.job,
            request(active, ReorgStageRequest::Init),
        )),
        Some(ReorgResult::Initialized(version)) => {
            active.job.snapshot_version = *version;
            ColumnReorgVisit::Step(action_step(job_only_write(active.job.id, Vec::new())))
        }
        None | Some(ReorgResult::Interrupted) => {
            let handle = add_index::load_reorg_handle(snapshot, catalog, active)?;
            ColumnReorgVisit::Step(reorg_request(
                &active.job,
                request(active, ReorgStageRequest::Run(handle)),
            ))
        }
        Some(ReorgResult::Progress(rows)) => {
            active.job.row_count = *rows;
            ColumnReorgVisit::Step(action_step(job_only_write(active.job.id, Vec::new())))
        }
        Some(ReorgResult::Skipped) => ColumnReorgVisit::Done,
        Some(ReorgResult::Done(rows)) => {
            active.job.row_count = *rows;
            ColumnReorgVisit::Done
        }
        Some(ReorgResult::Failed {
            code,
            message,
            rows,
        }) => {
            active.job.row_count = *rows;
            ColumnReorgVisit::Failed(checked_refusal(*code, message.clone()))
        }
        Some(ReorgResult::Checked(_)) => {
            return Err(DdlPlanError::Encode(
                "a column rewrite got a column check's outcome".to_owned(),
            ))
        }
    })
}

/// Go `reorgCtx.mergeWarnings` into `job.ReorgMeta.Warnings` /
/// `WarningsCount`, keyed by the warning's error id.
fn merge_reorg_warnings(job: &Job, warnings: &[(u16, String, i64)]) {
    if warnings.is_empty() {
        return;
    }
    let Some(meta) = &job.reorg_meta else {
        return;
    };
    let mut meta = meta.write();
    let warnings_map = meta
        .warnings
        .get_or_insert_with(|| GoShared::new(Default::default()))
        .clone();
    let counts = meta
        .warnings_count
        .get_or_insert_with(|| GoShared::new(Default::default()))
        .clone();
    let (mut warnings_map, mut counts) = (warnings_map.write(), counts.write());
    for (code, message, count) in warnings {
        let key: tidb_datatype::GoString = format!("types:{code}").into();
        *counts.entry(key.clone()).or_insert(0) += count;
        warnings_map.entry(key).or_insert_with(|| {
            Some(GoShared::new(
                tidb_util::dbterror::CLASS_TYPES
                    .new_std(*code)
                    .generate(message.clone()),
            ))
        });
    }
}

/// Go `rollingbackModifyColumn`.
pub(super) fn plan_cancel_modify_column(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    // Go `IsRollbackable`: a modify-column job is not once it is public.
    if active.job.schema_state == SchemaState::PUBLIC {
        active.job.state = JobState::RUNNING;
        return Ok(job_control_step(&active.job));
    }
    let args = tidb_model::get_modify_column_args(&mut active.job)
        .map_err(|error| cancel_ddl_job(active, DdlPlanError::Encode(error.to_string())))?
        .ok_or_else(|| {
            cancel_ddl_job(
                active,
                DdlPlanError::Encode("MODIFY COLUMN job has nil args".to_owned()),
            )
        })?;
    let mut args = args.read().clone();
    let target = get_modify_column_info(catalog, active, &mut args)?;
    let cancelled = || {
        DdlPlanError::from(
            tidb_util::dbterror::ERR_CANCELLED_DDL_JOB
                .generate(tidb_error::tidb::errname::ErrCancelledDDLJob.raw),
        )
    };
    match args.modify_column_type {
        modify_type::NONE => Err(cancelled_ddl_job(active)),
        modify_type::NO_REORG | modify_type::NO_REORG_WITH_CHECK | modify_type::PRECHECK => {
            if active.job.schema_state == SchemaState::NONE {
                if has_modify_flag(&target.old.read()) {
                    active.job.state = JobState::ROLLINGBACK;
                    return Ok(PersistedDdlActionStep {
                        write: job_only_write(active.job.id, Vec::new()),
                        update_raw_args: false,
                        run_error: Some(cancelled()),
                        reorg: None,
                    });
                }
                return Err(cancelled_ddl_job(active));
            }
            active.job.state = JobState::RUNNING;
            Ok(job_control_step(&active.job))
        }
        _ => {
            if args.changing_column.is_none() {
                return Err(cancelled_ddl_job(active));
            }
            active.job.state = JobState::ROLLINGBACK;
            Ok(PersistedDdlActionStep {
                write: job_only_write(active.job.id, Vec::new()),
                update_raw_args: false,
                run_error: Some(cancelled()),
                reorg: None,
            })
        }
    }
}
