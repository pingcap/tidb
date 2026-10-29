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

use tidb_ddl_build::modify_column_type::{
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
    job_context: &dyn DdlSessionContext,
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
    job_context: &dyn DdlSessionContext,
    store_count: usize,
    has_null: &mut dyn FnMut(&TableInfo, i64) -> Result<bool, DdlPlanError>,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    let (schema, stored, schema_id, args, need_reorg) = match statement {
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
            // Go `ModifyColumn` / `ChangeColumn`: the qualifiers first.
            tidb_ddl_build::column_changes::check_column_qualifier(&column.qualifier, schema, table)
                .map_err(|error| {
                    DdlPlanError::Admission(DdlAdmissionError::from_mysql_error(error))
                })?;
            let (schema_id, stored) = locate_table(&catalog, schema, table)?;
            let original_col_name = rename_from.as_deref().unwrap_or(column.name.as_str());
            let bdr_role = snapshot.get(&key::bdr_role_kv_key())?.unwrap_or_default();
            let built = match get_modifiable_column_job(
                catalog,
                &bdr_role,
                schema,
                table,
                stored,
                original_col_name,
                column,
                position,
                context,
                has_null,
            ) {
                Ok(built) => built,
                // Go: `ErrColumnNotExists` under IF EXISTS becomes a note
                // and the statement succeeds.
                Err(error)
                    if *if_exists
                        && error.equal(&tidb_error::catalog::infoschema::ERR_COLUMN_NOT_EXISTS) =>
                {
                    return Ok(Some(AddIndexSubmission::AlreadySatisfied {
                        detail: format!(
                            "column `{original_col_name}` does not exist on `{schema}`.`{table}`"
                        ),
                        warnings: vec![crate::real_tikv_ddl::ddl_note(
                            tidb_error::catalog::infoschema::ERR_COLUMN_NOT_EXISTS
                                .generate_with_stack_by_args(&[
                                    original_col_name.into(),
                                    table.into(),
                                ]),
                        )],
                    }));
                }
                Err(error) => return Err(error),
            };
            let args = tidb_model::ModifyColumnArgs {
                column: Some(GoShared::new(built.column)),
                old_column_name: CiString::new(original_col_name),
                position: Some(GoShared::new(tidb_ast::JobColumnPosition::from_statement(
                    position,
                ))),
                new_shard_bits: built.new_shard_bits,
                ..tidb_model::ModifyColumnArgs::default()
            };
            (schema, stored, schema_id, args, built.need_reorg)
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
            // Go `RenameColumn` submits no reorganization.
            (schema, stored, schema_id, args, false)
        }
        _ => return Ok(None),
    };
    let mut job = session_job(job_context, ActionType::ACTION_MODIFY_COLUMN);
    job.schema_id = schema_id;
    job.table_id = stored.id;
    job.schema_name = schema.go_to_lower().into();
    job.table_name = stored.name.lowercase().to_owned().into();
    // Go `NeedReorg: mayNeedChangeColData`.
    job.need_reorg = need_reorg;
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

/// What Go `GetModifiableColumnJob` hands its job: the new column, whether
/// the change may rewrite the data (`NeedReorg`), and the AUTO_RANDOM shard
/// bits (`NewShardBits`).
struct ModifiableColumn {
    column: tidb_model::ColumnInfo,
    need_reorg: bool,
    new_shard_bits: u64,
}

/// A refusal raised as one of the driver's Go errors.
fn driver_refusal(error: impl Into<tidb_error::mysql::MysqlError>) -> DdlPlanError {
    DdlPlanError::Admission(DdlAdmissionError::from_mysql_error(error.into()))
}

/// Go `GetModifiableColumnJob` (`pkg/ddl/modify_column.go:1838`), in Go's
/// order, against the table as the statement found it: `schema` / `table`
/// are the statement's `ident`, `original_col_name` the column a MODIFY
/// names (or a CHANGE's old name), `column` the new definition. `has_null`
/// answers `checkForNullValue`'s query in the submitting transaction.
#[allow(clippy::too_many_arguments)]
fn get_modifiable_column_job(
    catalog: &ClusterCatalog,
    bdr_role: &[u8],
    schema: &str,
    table: &str,
    stored: &TableInfo,
    original_col_name: &str,
    column: &tidb_ast::ColumnDef,
    position: &tidb_ast::ColumnPosition,
    context: &DdlStatementContext,
    has_null: &mut dyn FnMut(&TableInfo, i64) -> Result<bool, DdlPlanError>,
) -> Result<ModifiableColumn, DdlPlanError> {
    let original_l = original_col_name.go_to_lower();
    // Go `table.FindCol(t.Cols(), originalColName.L)`: the public columns.
    let Some(offset) = stored.columns.iter_deref().position(|candidate| {
        let candidate = candidate.read();
        candidate.state == SchemaState::PUBLIC && candidate.name.lowercase() == original_l
    }) else {
        return Err(column_not_exists(original_col_name, table));
    };
    let col = stored
        .columns
        .get(offset)
        .expect("the offset was just found")
        .read()
        .clone_like_go();
    check_column_referenced_by_partial_condition(stored, col.name.original())?;
    let new_col_name = column.name.go_to_lower();
    if new_col_name == "_tidb_rowid" {
        return Err(coded_refusal(
            &tidb_util::dbterror::ERR_WRONG_COLUMN_NAME,
            format!("Incorrect column name '{new_col_name}'"),
        ));
    }
    let err_g =
        check_modify_column_with_generated_columns_constraint(stored, &original_l, original_col_name);

    // Renaming needs the new name free and no generated column reading the
    // old one.
    if new_col_name != original_l {
        if stored.columns.iter_deref().any(|candidate| {
            let candidate = candidate.read();
            candidate.state == SchemaState::PUBLIC && candidate.name.lowercase() == new_col_name
        }) {
            return Err(DdlPlanError::from(
                tidb_error::catalog::infoschema::ERR_COLUMN_EXISTS
                    .generate_with_stack_by_args(&[(column.name.clone()).into()]),
            ));
        }
        if let Some((proto, message)) = &err_g {
            return Err(coded_refusal(proto, message.clone()));
        }
    }

    // Go `checkColumnAttributes`, `table.ToColumn` keeping the old column's
    // identity, then `ProcessColumnCharsetAndCollation`.
    let metabuild = context.0.metabuild_context();
    let mut new_col = tidb_ddl_build::table_info_build::new_modified_column(
        column,
        &stored.charset,
        &stored.collate,
        &metabuild,
    )
    .map_err(DdlPlanError::Admission)?;
    new_col.id = col.id;
    new_col.offset = col.offset;
    new_col.state = col.state;
    new_col.origin_default_value = col.origin_default_value.clone();
    new_col.origin_default_value_bit = col.origin_default_value_bit.clone();
    new_col.version = col.version;

    crate::foreign_key_build::check_modify_column_with_foreign_key_constraint(
        catalog,
        &schema.go_to_lower(),
        stored,
        &col,
        &new_col,
    )
    .map_err(DdlPlanError::Admission)?;

    // Go: "Copy index related options to the new spec", and a primary key
    // column starts NOT NULL.
    let index_flags = col.field_type.flags()
        & (FieldTypeFlags::PRI_KEY | FieldTypeFlags::UNIQUE_KEY | FieldTypeFlags::MULTIPLE_KEY);
    new_col.field_type.add_flags(index_flags);
    if col.field_type.has_flag(FieldTypeFlags::PRI_KEY) {
        new_col.field_type.add_flags(FieldTypeFlags::NOT_NULL);
    }

    tidb_ddl_build::table_info_build::process_modify_column_options(&mut new_col, column, &metabuild)
        .map_err(DdlPlanError::Admission)?;

    tidb_ddl_build::modify_column_type::check_modify_types(
        &col.field_type,
        &new_col.field_type,
        col.name.lowercase(),
        is_column_with_index(col.name.lowercase(), stored),
    )
    .map_err(driver_refusal)?;
    let may_need_change_col_data = !no_reorg_data_strict(
        &col.field_type,
        &new_col.field_type,
        is_column_with_index(col.name.lowercase(), stored),
    );
    if may_need_change_col_data {
        is_generated_related_column(stored, &new_col, &col)?;
        if is_columnar_index_column(stored, &col) {
            return Err(unsupported_modify_column("columnar indexes on the column"));
        }
        // Go: "new col's origin default value be the same as the new default
        // value".
        tidb_ddl_build::table_info_build::stamp_modified_origin_default(&mut new_col, &metabuild)
            .map_err(DdlPlanError::Admission)?;
    }

    check_partition_modifiable_column(stored, &col, &new_col)?;

    // Go: "We don't support modifying column from not_auto_increment to
    // auto_increment", nor an AUTO_INCREMENT column with a default, nor
    // dropping AUTO_INCREMENT unless `@@tidb_allow_remove_auto_inc` allows it.
    let was_auto_increment = col.field_type.has_flag(FieldTypeFlags::AUTO_INCREMENT);
    let is_auto_increment = new_col.field_type.has_flag(FieldTypeFlags::AUTO_INCREMENT);
    if !was_auto_increment && is_auto_increment {
        return Err(unsupported_modify_column("can't set auto_increment"));
    }
    if is_auto_increment && !new_col.default_value.is_nil() {
        return Err(driver_refusal(tidb_error::mysql::MysqlError::from_terror(
            &tidb_util::dbterror::ERR_INVALID_DEFAULT_VALUE.generate_with_stack_by_args(&[
                (new_col.name.original().to_owned()).to_string().into(),
            ]),
        )));
    }
    if !context.0.allow_remove_auto_inc() && was_auto_increment && !is_auto_increment {
        return Err(unsupported_modify_column(
            "can't remove auto_increment without @@tidb_allow_remove_auto_inc enabled",
        ));
    }

    // Go `checkForNullValue`: NULL -> NOT NULL fails on a NULL row, except
    // into a TIMESTAMP, which substitutes the current time.
    if !col.field_type.has_flag(FieldTypeFlags::NOT_NULL)
        && new_col.field_type.has_flag(FieldTypeFlags::NOT_NULL)
        && !(col.field_type.code() != FieldTypeCode::Timestamp
            && new_col.field_type.code() == FieldTypeCode::Timestamp)
        && has_null(stored, col.id)?
    {
        return Err(coded_refusal(
            &tidb_error::catalog::types::ERR_TRUNCATED,
            format!(
                "Data truncated for column '{}' at row 1",
                new_col.name.lowercase()
            ),
        ));
    }

    check_column_with_index_constraint(stored, &col, &new_col)?;

    // Go: "As same with MySQL, we don't support modifying the stored status
    // for generated columns", then the deferred dependency refusal.
    check_modify_generated_column(
        stored,
        schema,
        offset,
        &col,
        &new_col,
        column,
        position,
        context,
    )?;
    if let Some((proto, message)) = err_g {
        // Go wraps `errG.Error()`, the terror text with its class prefix.
        return Err(coded_refusal(
            &tidb_util::dbterror::ERR_UNSUPPORTED_ON_GENERATED_COLUMN,
            format!(
                "'{}' is not supported for generated columns.",
                proto.generate(message)
            ),
        ));
    }

    // Go: "the column referenced by TTL should be a time type".
    if let Some(ttl) = &stored.ttl_info {
        if ttl.read().column_name.lowercase() == original_l
            && !new_col.field_type.code().is_type_time()
        {
            return Err(driver_refusal(tidb_error::mysql::MysqlError::from_terror(
                &tidb_util::dbterror::ERR_UNSUPPORTED_COLUMN_IN_TTL_CONFIG
                    .generate_with_stack_by_args(&[(new_col.name.original().to_owned())
                        .to_string()
                        .into()]),
            )));
        }
    }

    let new_shard_bits = check_auto_random(stored, &col, &new_col, column)?;

    // Go `bdr.IsModifyColumnDenied`, outside the system schemas.
    crate::cluster_bdr::admit_modify_column(
        bdr_role,
        schema,
        &new_col.field_type,
        &col.field_type,
        &column.options,
    )?;

    Ok(ModifiableColumn {
        column: new_col,
        need_reorg: may_need_change_col_data,
        new_shard_bits,
    })
}

/// Go `isColumnarIndexColumn` (`pkg/ddl/index.go:3976`): whether an index
/// over the column (`FindRelatedIndexesToChange`, by name) is columnar.
fn is_columnar_index_column(info: &TableInfo, column: &tidb_model::ColumnInfo) -> bool {
    info.indices.iter_deref().any(|index| {
        let index = index.read();
        index.is_columnar_index()
            && index
                .columns
                .iter_deref()
                .any(|part| part.read().name.lowercase() == column.name.lowercase())
    })
}

/// Go `getReplacedColumns`: the table's columns with the old column replaced
/// by the new one under the old name.
fn get_replaced_columns(
    info: &TableInfo,
    old: &tidb_model::ColumnInfo,
    new: &tidb_model::ColumnInfo,
) -> Vec<tidb_model::ColumnInfo> {
    info.columns
        .iter_deref()
        .map(|column| {
            let column = column.read();
            if column.name.lowercase() == old.name.lowercase() {
                let mut replaced = new.clone_like_go();
                replaced.name = old.name.clone();
                replaced
            } else {
                column.clone_like_go()
            }
        })
        .collect()
}

/// Go `checkIndexInModifiableColumns` (`modify_column.go:2234`): every key
/// part of `index` re-checked against the replaced columns, under the
/// prefix length that survives the change.
fn check_index_in_modifiable_columns(
    columns: &[tidb_model::ColumnInfo],
    index: &IndexInfo,
) -> Result<(), DdlPlanError> {
    let columnar = index.is_columnar_index();
    for part in index.columns.iter_deref() {
        let part = part.read();
        let Some(column) = columns
            .iter()
            .find(|column| column.name.lowercase() == part.name.lowercase())
        else {
            return Err(coded_refusal(
                &tidb_util::dbterror::ERR_KEY_COLUMN_DOES_NOT_EXITS,
                format!("column does not exist: {}", part.name.original()),
            ));
        };
        if columnar {
            continue;
        }
        // When the index column is changed, a prefix length is only valid if
        // the type is still prefixable and larger than the old prefix.
        let field_type = &column.field_type;
        let prefix_length = if field_type.code().is_type_prefixable()
            && field_type.flen() > part.length
        {
            part.length
        } else {
            tidb_datatype::UNSPECIFIED_LENGTH
        };
        tidb_ddl_build::index::check_index_column(column, prefix_length, false)
            .map_err(driver_refusal)?;
    }
    Ok(())
}

/// Go `checkColumnWithIndexConstraint` (`modify_column.go:2185`): the
/// primary key and then every other index over the column, each re-checked
/// key part by key part and then by its total length.
fn check_column_with_index_constraint(
    info: &TableInfo,
    original_col: &tidb_model::ColumnInfo,
    new_col: &tidb_model::ColumnInfo,
) -> Result<(), DdlPlanError> {
    let columns = get_replaced_columns(info, original_col, new_col);
    let check_one_index = |index: &IndexInfo| -> Result<(), DdlPlanError> {
        if !index
            .columns
            .iter_deref()
            .any(|part| part.read().name.lowercase() == original_col.name.lowercase())
        {
            return Ok(());
        }
        check_index_in_modifiable_columns(&columns, index)?;
        let parts = index
            .columns
            .iter_deref()
            .map(|part| part.read().clone())
            .collect::<Vec<_>>();
        tidb_ddl_build::index::check_index_prefix_length(
            &columns,
            &parts,
            index.get_columnar_index_type(),
        )
        .map_err(driver_refusal)
    };
    // Go `tables.FindPrimaryIndex` first, then the secondary indexes.
    if let Some(primary) = info.indices.iter_deref().find(|index| index.read().primary) {
        check_one_index(&primary.read())?;
    }
    for index in info.indices.iter_deref() {
        let index = index.read();
        if index.primary {
            continue;
        }
        check_one_index(&index)?;
    }
    Ok(())
}

/// Go `checkPartitionModifiableColumn` (`modify_column.go:1452`) as
/// `preCheckPartitionModifiableColumn` asks it: a change to a column the
/// partitioning reads keeps the name, an allowed type, the evaluation type,
/// the flags, charset and collation, and only the type changes
/// `checkPartitionColumnTypeChangeAllowlist` lists.
///
/// DEFERRED: Go then rebuilds the partition definitions against the new
/// column (`buildPartitionDefinitionsInfo`) to check every bound still fits;
/// there is no `TableInfo` builder of partition definitions to ask here.
fn check_partition_modifiable_column(
    info: &TableInfo,
    col: &tidb_model::ColumnInfo,
    new_col: &tidb_model::ColumnInfo,
) -> Result<(), DdlPlanError> {
    let Some(partition) = info.partition.as_ref() else {
        return Ok(());
    };
    let (partition_type, columns, expr) = {
        let partition = partition.read();
        (
            partition.partition_type,
            partition.columns.snapshot(),
            partition.expr.clone(),
        )
    };
    let is_partition_column = if columns.is_empty() {
        // Go `extractPartitionColumns(Partition.Expr, tblInfo)`.
        stored_partition_spec(info)?
            .dependencies
            .iter()
            .any(|name| name.go_to_lower() == col.name.lowercase())
    } else {
        columns
            .iter()
            .any(|name| name.lowercase() == col.name.lowercase())
    };
    if !is_partition_column {
        return Ok(());
    }
    // Go `checkPartitionColumnModifiable`.
    if col.name.lowercase() != new_col.name.lowercase() {
        return Err(coded_refusal(
            &tidb_util::dbterror::ERR_DEPENDENT_BY_PARTITION_FUNCTIONAL,
            format!(
                "Column '{}' has a partitioning function dependency and cannot be dropped or \
                 renamed",
                col.name.lowercase()
            ),
        ));
    }
    let (old, new) = (&col.field_type, &new_col.field_type);
    if !is_col_type_allowed_as_partitioning_col(partition_type, new) {
        return Err(coded_refusal(
            &tidb_util::dbterror::ERR_NOT_ALLOWED_TYPE_IN_PARTITION,
            format!(
                "Field '{}' is of a not allowed type for this type of partitioning",
                new_col.name.original()
            ),
        ));
    }
    if new.eval_type() != old.eval_type()
        || !is_allowed_partition_column_flag_change(old.flags(), new.flags())
        || new.collation_name() != old.collation_name()
        || new.charset_name() != old.charset_name()
    {
        return Err(partition_type_change_not_allowed_err());
    }
    check_partition_column_type_change_allowlist(partition_type, !columns.is_empty(), &expr, col, new_col)
}

/// Go `isValidKeyPartitionColType` and `isColTypeAllowedAsPartitioningCol`
/// (`pkg/ddl/partition.go:798-827`).
fn is_col_type_allowed_as_partitioning_col(
    partition_type: tidb_ast::PartitionType,
    field_type: &tidb_datatype::FieldType,
) -> bool {
    if partition_type == tidb_ast::PartitionType::KEY {
        return !matches!(
            field_type.code(),
            FieldTypeCode::Blob
                | FieldTypeCode::MediumBlob
                | FieldTypeCode::LongBlob
                | FieldTypeCode::Json
                | FieldTypeCode::Geometry
                | FieldTypeCode::VectorFloat32
        );
    }
    matches!(
        field_type.code(),
        FieldTypeCode::Tiny
            | FieldTypeCode::Short
            | FieldTypeCode::Int24
            | FieldTypeCode::Long
            | FieldTypeCode::LongLong
            | FieldTypeCode::Date
            | FieldTypeCode::Datetime
            | FieldTypeCode::Duration
            | FieldTypeCode::Varchar
            | FieldTypeCode::String
    )
}

/// Go `isAllowedPartitionColumnFlagChange`: only `NoDefaultValueFlag`, and
/// NOT NULL -> NULL, may change.
fn is_allowed_partition_column_flag_change(old_flag: u32, new_flag: u32) -> bool {
    if old_flag == new_flag {
        return true;
    }
    let mut allowed_changed_flags = FieldTypeFlags::NO_DEFAULT_VALUE;
    if old_flag & FieldTypeFlags::NOT_NULL != 0 && new_flag & FieldTypeFlags::NOT_NULL == 0 {
        allowed_changed_flags |= FieldTypeFlags::NOT_NULL;
    }
    old_flag & !allowed_changed_flags == new_flag & !allowed_changed_flags
}

/// Go `partitionTypeChangeNotAllowedErr`.
fn partition_type_change_not_allowed_err() -> DdlPlanError {
    unsupported_modify_column(
        "can't change the partitioning column, since it would require reorganize all partitions",
    )
}

/// Go `checkPartitionColumnTypeChangeAllowlist` (`modify_column.go:1559`).
fn check_partition_column_type_change_allowlist(
    partition_type: tidb_ast::PartitionType,
    has_columns: bool,
    expr: &str,
    col: &tidb_model::ColumnInfo,
    new_col: &tidb_model::ColumnInfo,
) -> Result<(), DdlPlanError> {
    let (old, new) = (&col.field_type, &new_col.field_type);
    if !is_partition_column_type_changed(old, new) {
        return Ok(());
    }
    match partition_type {
        tidb_ast::PartitionType::KEY => {
            if is_integer_type_widening(old, new)
                || is_string_length_extension(old, new)
                || is_enum_set_tail_append(old, new)
            {
                return Ok(());
            }
        }
        tidb_ast::PartitionType::RANGE
        | tidb_ast::PartitionType::LIST
        | tidb_ast::PartitionType::HASH => {
            if has_columns {
                if partition_type == tidb_ast::PartitionType::HASH {
                    return Err(unsupported_modify_column(
                        "unexpected HASH partition metadata with column list",
                    ));
                }
                // Go `isAllowedTypeChangeForRangeListColumnsPartition`.
                if is_integer_type_widening(old, new)
                    || (is_time_fsp_extended(old, new)
                        && matches!(old.code(), FieldTypeCode::Duration | FieldTypeCode::Datetime))
                    || is_string_length_extension(old, new)
                {
                    return Ok(());
                }
                return Err(partition_type_change_not_allowed_err());
            }
            return check_type_change_for_expr_based_partition(expr, col, new_col);
        }
        _ => {}
    }
    Err(partition_type_change_not_allowed_err())
}

/// Go `partitionExprColumnUsageKind`.
#[derive(Clone, Copy, PartialEq, Eq)]
enum PartitionExprColumnUsageKind {
    NoFunc,
    ToDays,
    Extract,
    Unsupported,
}

/// Go `checkTypeChangeForExprBasedPartition`.
fn check_type_change_for_expr_based_partition(
    part_expr: &str,
    col: &tidb_model::ColumnInfo,
    new_col: &tidb_model::ColumnInfo,
) -> Result<(), DdlPlanError> {
    let (old, new) = (&col.field_type, &new_col.field_type);
    // Go `collectPartitionExprColumnUsageKinds`: nil on a parse failure or
    // when the column is never read.
    let Ok(expression) = tidb_model::generated_expr::parse_expression(part_expr) else {
        return Err(partition_type_change_not_allowed_err());
    };
    let mut usage_kinds = Vec::new();
    collect_partition_expr_column_usage_kinds_from_expr(
        &expression,
        col.name.lowercase(),
        PartitionExprColumnUsageKind::NoFunc,
        &mut usage_kinds,
    );
    if usage_kinds.is_empty() || usage_kinds.contains(&PartitionExprColumnUsageKind::Unsupported) {
        return Err(partition_type_change_not_allowed_err());
    }
    if usage_kinds.contains(&PartitionExprColumnUsageKind::NoFunc)
        && !is_integer_type_widening(old, new)
    {
        return Err(partition_type_change_not_allowed_err());
    }
    if usage_kinds.contains(&PartitionExprColumnUsageKind::ToDays)
        && !(is_time_fsp_extended(old, new) && old.code() == FieldTypeCode::Datetime)
    {
        return Err(partition_type_change_not_allowed_err());
    }
    if usage_kinds.contains(&PartitionExprColumnUsageKind::Extract)
        && !(is_time_fsp_extended(old, new)
            && matches!(old.code(), FieldTypeCode::Duration | FieldTypeCode::Datetime))
    {
        return Err(partition_type_change_not_allowed_err());
    }
    Ok(())
}

/// Go `collectPartitionExprColumnUsageKindsFromExpr`.
fn collect_partition_expr_column_usage_kinds_from_expr(
    expr: &tidb_ast::Expr,
    target_column: &str,
    current_kind: PartitionExprColumnUsageKind,
    usage_kinds: &mut Vec<PartitionExprColumnUsageKind>,
) {
    let mut record = |kind: PartitionExprColumnUsageKind| {
        if !usage_kinds.contains(&kind) {
            usage_kinds.push(kind);
        }
    };
    // Go `mergePartitionExprUsageKind`: unsupported stays unsupported.
    let merge = |func_kind: PartitionExprColumnUsageKind| {
        if current_kind == PartitionExprColumnUsageKind::Unsupported {
            current_kind
        } else {
            func_kind
        }
    };
    match expr {
        tidb_ast::Expr::Column(path) => {
            if path
                .last()
                .is_some_and(|name| name.go_to_lower() == target_column)
            {
                record(current_kind);
            }
        }
        // Go `classifyPartitionExprFuncUsage`.
        tidb_ast::Expr::Func { name, args, .. } => {
            let inner = merge(if name.eq_ignore_ascii_case("to_days") {
                PartitionExprColumnUsageKind::ToDays
            } else {
                PartitionExprColumnUsageKind::Unsupported
            });
            for arg in args {
                collect_partition_expr_column_usage_kinds_from_expr(
                    arg,
                    target_column,
                    inner,
                    usage_kinds,
                );
            }
        }
        // `EXTRACT(unit FROM x)` is Go's `FuncCallExpr` named `extract`
        // whose unit argument is a `TimeUnitExpr`.
        tidb_ast::Expr::Extract { value, .. } => {
            collect_partition_expr_column_usage_kinds_from_expr(
                value,
                target_column,
                merge(PartitionExprColumnUsageKind::Extract),
                usage_kinds,
            );
        }
        tidb_ast::Expr::Paren(inner) | tidb_ast::Expr::Unary(_, inner) => {
            collect_partition_expr_column_usage_kinds_from_expr(
                inner,
                target_column,
                current_kind,
                usage_kinds,
            );
        }
        tidb_ast::Expr::Binary(_, left, right) => {
            collect_partition_expr_column_usage_kinds_from_expr(
                left,
                target_column,
                current_kind,
                usage_kinds,
            );
            collect_partition_expr_column_usage_kinds_from_expr(
                right,
                target_column,
                current_kind,
                usage_kinds,
            );
        }
        // Go `ast.ValueExpr`: literals read no column.
        tidb_ast::Expr::Int(_)
        | tidb_ast::Expr::Decimal(_)
        | tidb_ast::Expr::Float(_)
        | tidb_ast::Expr::Hex(_)
        | tidb_ast::Expr::Bit(_)
        | tidb_ast::Expr::String(_)
        | tidb_ast::Expr::Bool(_)
        | tidb_ast::Expr::Null => {}
        _ => record(PartitionExprColumnUsageKind::Unsupported),
    }
}

/// Go `isPartitionColumnTypeChanged`.
fn is_partition_column_type_changed(
    old: &tidb_datatype::FieldType,
    new: &tidb_datatype::FieldType,
) -> bool {
    old.code() != new.code()
        || (!old.code().is_integer_type() && old.flen() != new.flen())
        || old.decimal() != new.decimal()
        || old.elems_snapshot() != new.elems_snapshot()
}

/// Go `isIntegerTypeWidening`.
fn is_integer_type_widening(old: &tidb_datatype::FieldType, new: &tidb_datatype::FieldType) -> bool {
    old.code().is_integer_type()
        && new.code().is_integer_type()
        && new.code().default_length_and_decimal().0 > old.code().default_length_and_decimal().0
}

/// Go `isStringLengthExtension`.
fn is_string_length_extension(
    old: &tidb_datatype::FieldType,
    new: &tidb_datatype::FieldType,
) -> bool {
    match (old.code(), new.code()) {
        (FieldTypeCode::String, FieldTypeCode::String) => {
            if old.is_binary_string() || new.is_binary_string() {
                return false;
            }
        }
        (FieldTypeCode::Varchar, FieldTypeCode::Varchar)
        | (FieldTypeCode::VarString, FieldTypeCode::VarString) => {}
        _ => return false,
    }
    new.flen() > old.flen()
}

/// Go `isTimeFspExtended`.
fn is_time_fsp_extended(old: &tidb_datatype::FieldType, new: &tidb_datatype::FieldType) -> bool {
    old.code() == new.code()
        && matches!(old.code(), FieldTypeCode::Duration | FieldTypeCode::Datetime)
        && new.decimal() > old.decimal()
}

/// Go `isEnumSetTailAppend`.
fn is_enum_set_tail_append(old: &tidb_datatype::FieldType, new: &tidb_datatype::FieldType) -> bool {
    matches!(old.code(), FieldTypeCode::Enum | FieldTypeCode::Set)
        && old.code() == new.code()
        && !tidb_ddl_build::modify_column_type::is_elems_changed_to_modify_column(
            &old.elems_snapshot(),
            &new.elems_snapshot(),
        )
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
        return Err(DdlPlanError::from(
            tidb_util::dbterror::ERR_INVALID_AUTO_RANDOM.generate_with_stack_by_args(&[
                ("the value of auto_random should be positive".to_owned()).into(),
            ]),
        ));
    }
    if shard_bits > 15 {
        return Err(DdlPlanError::from(
            tidb_util::dbterror::ERR_INVALID_AUTO_RANDOM.generate_with_stack_by_args(&[(format!(
                "max allowed auto_random shard bits is 15, but got {shard_bits} on column `{}`",
                column.name
            ))
            .into()]),
        ));
    }
    let range_bits = option.range_bits.unwrap_or(64);
    if !(32..=64).contains(&range_bits) {
        return Err(DdlPlanError::from(
            tidb_util::dbterror::ERR_INVALID_AUTO_RANDOM.generate_with_stack_by_args(&[(format!(
                "auto_random range bits must be between 32 and 64, but got {range_bits}"
            ))
            .into()]),
        ));
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
    let invalid = |reason: String| {
        DdlPlanError::from(
            tidb_util::dbterror::ERR_INVALID_AUTO_RANDOM
                .generate_with_stack_by_args(&[reason.into()]),
        )
    };
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
        return Err(DdlPlanError::from(
            tidb_util::dbterror::ERR_INVALID_AUTO_RANDOM.generate_with_stack_by_args(&[(format!(
                "max allowed auto_random shard bits is {}, but got {} on column `{}`",
                new_shard_bits.wrapping_sub(overflow),
                new_shard_bits,
                old.name.original()
            ))
            .into()]),
        ));
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
    DdlPlanError::from(
        tidb_error::catalog::infoschema::ERR_COLUMN_NOT_EXISTS
            .generate_with_stack_by_args(&[(column.to_owned()).into(), (table.to_owned()).into()]),
    )
}

/// Go `ErrUnsupportedModifyColumn` with its reason.
fn unsupported_modify_column(reason: &str) -> DdlPlanError {
    coded_refusal(
        &tidb_util::dbterror::ERR_UNSUPPORTED_MODIFY_COLUMN,
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
    tidb_ddl_build::modify_column_type::is_null_to_not_null_change(
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
        return Err(DdlPlanError::from(
            tidb_error::catalog::infoschema::ERR_COLUMN_EXISTS
                .generate_with_stack_by_args(&[(new.name.original().to_owned()).into()]),
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
            session_vars: tidb_ddl_build::session_pool::ReorgSessionVars::of_job(&active.job),
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
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
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
        row_ids,
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
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
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
        row_ids,
    )
}

/// Go `checkModifyTypes(oldCol, args.Column, isColumnWithIndex(...))`.
fn check_modify_types_owner(
    info: &TableInfo,
    old: &tidb_model::ColumnInfo,
    new: &tidb_model::ColumnInfo,
) -> Result<(), DdlPlanError> {
    tidb_ddl_build::modify_column_type::check_modify_types(
        &old.field_type,
        &new.field_type,
        old.name.lowercase(),
        is_column_with_index(old.name.original(), info),
    )
    .map_err(|error| {
        DdlPlanError::Admission(DdlAdmissionError::from_mysql_error(error))
    })
}

/// Go `onModifyColumn`'s first-visit check: `getReplacedColumns`, then
/// `checkIndexInModifiableColumns` over every index on the column
/// (`buildRelatedIndexInfos`).
fn check_indexes_in_modifiable_columns(
    info: &TableInfo,
    old: &tidb_model::ColumnInfo,
    new: &tidb_model::ColumnInfo,
) -> Result<(), DdlPlanError> {
    let columns = get_replaced_columns(info, old, new);
    for index in related_indexes(info, old.id) {
        check_index_in_modifiable_columns(&columns, &index.read())?;
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
        part.length = tidb_datatype::UNSPECIFIED_LENGTH;
    }
}

/// Go `adjustForeignKeyChildTableInfoAfterModifyColumn`: a renamed parent
/// column renames the children's referenced column. Returns the children's
/// writes.
fn adjust_foreign_key_children(
    catalog: &ClusterCatalog,
    active: &crate::ddl_job_table::ActiveDdlJob,
    info: &mut TableInfo,
    new: &tidb_model::ColumnInfo,
    old: &tidb_model::ColumnInfo,
) -> Result<Vec<(i64, TableInfo)>, DdlPlanError> {
    if !tidb_vardef::ENABLE_FOREIGN_KEY.load(std::sync::atomic::Ordering::SeqCst)
        || new.name.lowercase() == old.name.lowercase()
    {
        return Ok(Vec::new());
    }
    let schema = active.job.schema_name.to_string();
    let table_name = info.name.lowercase().to_owned();
    let rename_ref_cols = |fk: &mut tidb_model::table::FKInfo| -> bool {
        if fk.version < tidb_model::table::FK_VERSION1
            || fk.ref_schema.lowercase() != schema
            || fk.ref_table.lowercase() != table_name
        {
            return false;
        }
        let mut changed = false;
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
        changed
    };
    // Go seeds its foreign-key helper with the table being modified
    // (`fkh.addLoadedTable`), so a self-referencing constraint is rewritten
    // in that same `TableInfo` rather than as a separate child write.
    for fk in info.foreign_keys.iter_deref() {
        rename_ref_cols(&mut *fk.write());
    }
    let mut children: Vec<(i64, TableInfo)> = Vec::new();
    for database in &catalog.databases {
        for child in &database.tables {
            if child.id == info.id {
                continue;
            }
            let mut changed = false;
            let child_info = child.clone_like_go();
            for fk in child_info.foreign_keys.iter_deref() {
                changed |= rename_ref_cols(&mut *fk.write());
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
    let children = adjust_foreign_key_children(catalog, active, &mut target.info, new, &old)?;
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
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
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
                    row_ids,
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
            session_vars: tidb_ddl_build::session_pool::ReorgSessionVars::of_job(&active.job),
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
