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

//! Go master `pkg/ddl/mview_worker.go`: the owner's materialized view log
//! job steps.

use super::*;
use tidb_ast::AlterTableAction;

/// Go `isValidMaterializedViewLogBaseTable`.
pub(crate) fn is_valid_materialized_view_log_base_table(schema_lower_name: &str, table: &TableInfo) -> bool {
    if tidb_metadef::is_mem_or_sys_db(schema_lower_name)
        || table.is_view()
        || table.is_sequence()
        || table.temp_table_type != tidb_model::TempTableType::NONE
        || table.materialized_view_shadow.is_some()
        || table.materialized_view_log.is_some()
    {
        return false;
    }
    match &table.materialized_view {
        Some(mview) => table.state == SchemaState::PUBLIC && tidb_model::table::MaterializedViewInfo::get_init_build_state(Some(&mview.read())).is_ready(),
        None => true,
    }
}

/// Go `errUnsupportedMaterializedViewOnPartitionTable`.
pub(crate) fn err_unsupported_materialized_view_on_partition_table(op: &str) -> DdlPlanError {
    DdlPlanError::from(
        tidb_dbterror::ERR_GENERAL_UNSUPPORTED_DDL.gen_with_stack_by_args(&[format!("{op} on partition table").into()]),
    )
}

fn unsupported(message: String) -> tidb_terror::MysqlError {
    tidb_terror::MysqlError::from_terror(&tidb_dbterror::ERR_GENERAL_UNSUPPORTED_DDL.gen_with_stack_by_args(&[message.into()]))
}

/// Go master `checkProtectedMaterializedViewShadowConstraint` for a statement
/// outside materialized view maintenance (`allowInternalMViewShadowBypass`
/// needs `InMViewMaintenance`, which no session of this port sets).
pub(crate) fn check_protected_materialized_view_shadow_constraint(
    table: &TableInfo,
    op: &str,
) -> Result<(), tidb_terror::MysqlError> {
    if table.materialized_view_shadow.is_some() {
        return Err(unsupported(format!("{op} on materialized view shadow table")));
    }
    Ok(())
}

/// Go master `checkTableMaterializedViewConstraints`: DROP / TRUNCATE /
/// RENAME TABLE refuse a log, a materialized view, a shadow table, and a base
/// table with dependents.
pub(crate) fn check_table_materialized_view_constraints(table: &TableInfo, op: &str) -> Result<(), tidb_terror::MysqlError> {
    if table.materialized_view_log.is_some() {
        return Err(unsupported(format!("{op} on materialized view log table")));
    }
    if table.materialized_view.is_some() {
        return Err(unsupported(format!("{op} on materialized view table")));
    }
    check_protected_materialized_view_shadow_constraint(table, op)?;
    if let Some(base) = &table.materialized_view_base {
        let base = base.read();
        if !base.mview_ids.is_empty() {
            return Err(unsupported(format!("{op} on base table with materialized view dependencies")));
        }
        if base.mlog_id != 0 {
            return Err(unsupported(format!("{op} on base table with materialized view log")));
        }
    }
    Ok(())
}

/// Go master `buildMaterializedViewLogBaseInvolvingSchemaInfo`: a log's
/// base table, named in the log's schema.
pub(crate) fn materialized_view_log_base_involving_schema_info(
    catalog: &SessionExtendedInfoSchema,
    schema_name: &str,
    mlog: &TableInfo,
) -> Vec<tidb_model::InvolvingSchemaInfo> {
    let Some(log) = &mlog.materialized_view_log else {
        return Vec::new();
    };
    let base_id = log.read().base_table_id;
    catalog
        .table_info_by_id(base_id)
        .map(|base| {
            vec![tidb_model::InvolvingSchemaInfo {
                database: schema_name.to_owned().into(),
                table: base.read().name.lowercase().to_owned().into(),
                ..tidb_model::InvolvingSchemaInfo::default()
            }]
        })
        .unwrap_or_default()
}

/// Go master `buildMaterializedViewLogInvolvingSchemaInfo`: a log and its
/// base table.
pub(crate) fn materialized_view_log_involving_schema_info(
    catalog: &SessionExtendedInfoSchema,
    schema_name: &str,
    mlog: &TableInfo,
) -> Vec<tidb_model::InvolvingSchemaInfo> {
    if mlog.materialized_view_log.is_none() {
        return Vec::new();
    }
    let mut involving = vec![tidb_model::InvolvingSchemaInfo {
        database: schema_name.to_owned().into(),
        table: mlog.name.lowercase().to_owned().into(),
        ..tidb_model::InvolvingSchemaInfo::default()
    }];
    involving.extend(materialized_view_log_base_involving_schema_info(catalog, schema_name, mlog));
    involving
}

/// Go master `isNullToNotNullChange`.
fn is_null_to_not_null_change(old: &tidb_model::ColumnInfo, new: &tidb_model::ColumnInfo) -> bool {
    !old.field_type.has_flag(FieldTypeFlags::NOT_NULL) && new.field_type.has_flag(FieldTypeFlags::NOT_NULL)
}

/// Go master `isColumnCommentOnlyChange`: `reflect.DeepEqual` of the two
/// columns once the comments agree, compared through their stored form.
fn is_column_comment_only_change(old: &tidb_model::ColumnInfo, new: &tidb_model::ColumnInfo) -> bool {
    let mut new = new.clone_like_go();
    new.comment = old.comment.clone();
    serde_json::to_value(old).ok() == serde_json::to_value(&new).ok()
}

/// Go master `fieldTypeForMVRelatedColumn`: the base column's new type with
/// the related column's own key flags.
fn field_type_for_mv_related_column(
    old_related: &tidb_model::ColumnInfo,
    base_new: &tidb_model::ColumnInfo,
) -> tidb_datatype::FieldType {
    let key_flags = old_related.field_type.flags()
        & (FieldTypeFlags::PRI_KEY | FieldTypeFlags::UNIQUE_KEY | FieldTypeFlags::MULTIPLE_KEY);
    let mut field_type = base_new.field_type.clone();
    field_type.del_flag(
        FieldTypeFlags::PRI_KEY
            | FieldTypeFlags::UNIQUE_KEY
            | FieldTypeFlags::MULTIPLE_KEY
            | FieldTypeFlags::AUTO_INCREMENT
            | FieldTypeFlags::ON_UPDATE_NOW
            | FieldTypeFlags::PREVENT_NULL_INSERT
            | FieldTypeFlags::GENERATED_COLUMN,
    );
    field_type.add_flags(key_flags);
    field_type
}

fn invalid_ddl_job(message: impl Into<String>) -> tidb_terror::MysqlError {
    tidb_terror::MysqlError::from_terror(&tidb_dbterror::ERR_INVALID_DDL_JOB.gen_with_stack_by_args(&[message.into().into()]))
}

/// Go master `validateMaterializedViewLogInfoForModifyColumn`.
fn validate_materialized_view_log_info_for_modify_column(
    base: &TableInfo,
    mlog: &TableInfo,
) -> Result<(), tidb_terror::MysqlError> {
    if mlog.materialized_view_log.as_ref().is_none_or(|log| log.read().base_table_id != base.id) {
        return Err(invalid_ddl_job("modify column: invalid materialized view log metadata"));
    }
    Ok(())
}

/// Go master `trackedMaterializedViewLogColumnForModifyColumn`: the log's
/// column, when the log tracks the base column.
fn tracked_materialized_view_log_column_for_modify_column(
    mlog: &TableInfo,
    column: &str,
) -> Result<Option<GoShared<tidb_model::ColumnInfo>>, tidb_terror::MysqlError> {
    let tracked = mlog
        .materialized_view_log
        .as_ref()
        .is_some_and(|log| log.read().columns.iter().any(|tracked| tracked.lowercase() == column));
    if !tracked {
        return Ok(None);
    }
    tidb_model::column::find_column_info(&mlog.columns, column)
        .map(Some)
        .ok_or_else(|| invalid_ddl_job("modify column: materialized view log column not found"))
}

/// Go master `validateMaterializedViewLogColumnTypeForModifyColumn`.
fn validate_materialized_view_log_column_type_for_modify_column(
    op: &str,
    mlog: &TableInfo,
    mlog_column: &tidb_model::ColumnInfo,
    new: &tidb_model::ColumnInfo,
) -> Result<tidb_datatype::FieldType, tidb_terror::MysqlError> {
    let field_type = field_type_for_mv_related_column(mlog_column, new);
    let indexed = mlog.indices.iter_deref().any(|index| {
        index.read().columns.iter_deref().any(|part| part.read().name.lowercase() == mlog_column.name.lowercase())
    });
    if !tidb_ddl_build::modify_column_type::no_reorg_data_strict(&mlog_column.field_type, &field_type, indexed) {
        return Err(unsupported(format!(
            "{op} on base table with materialized view log only supports no-reorg compatible type changes"
        )));
    }
    Ok(field_type)
}

/// Go master `buildMaterializedViewRelatedTableInfoForModifyColumn`: the
/// tables a base column's change rewrites, each loaded once by `load`
/// (its schema id and a copy of its info): the log, when it tracks the
/// column, retyped from the column's new type.
pub(crate) fn build_materialized_view_related_table_info_for_modify_column(
    load: &mut dyn FnMut(i64, &str) -> Result<(i64, TableInfo), tidb_terror::MysqlError>,
    op: &str,
    base: &TableInfo,
    old: &tidb_model::ColumnInfo,
    new: &tidb_model::ColumnInfo,
) -> Result<Vec<(i64, TableInfo)>, tidb_terror::MysqlError> {
    let Some(base_info) = &base.materialized_view_base else {
        return Ok(Vec::new());
    };
    if is_column_comment_only_change(old, new) {
        return Ok(Vec::new());
    }
    let (mlog_id, has_mviews) = {
        let base_info = base_info.read();
        (base_info.mlog_id, !base_info.mview_ids.is_empty())
    };
    if has_mviews {
        // Go then retypes each dependent materialized view's direct output
        // columns from its definition (`analyzeMVColumnUsage`), which this
        // node does not port yet.
        return Err(unsupported(format!(
            "{op} on base table with materialized view dependencies on this node"
        )));
    }
    let mut related = Vec::new();
    if mlog_id != 0 {
        let (schema_id, mlog) = load(mlog_id, "materialized view log")?;
        validate_materialized_view_log_info_for_modify_column(base, &mlog)?;
        if let Some(mlog_column) = tracked_materialized_view_log_column_for_modify_column(&mlog, old.name.lowercase())? {
            if is_null_to_not_null_change(old, new) {
                return Err(unsupported(format!(
                    "{op} on base table with materialized view log does not support changing tracked columns from NULL to NOT NULL"
                )));
            }
            let field_type =
                validate_materialized_view_log_column_type_for_modify_column(op, &mlog, &mlog_column.read(), new)?;
            if mlog_column.read().field_type != field_type {
                mlog_column.write().field_type = field_type;
                related.push((schema_id, mlog));
            }
        }
    }
    Ok(related)
}

/// The `loadTable` of [`build_materialized_view_related_table_info_for_modify_column`]
/// over the latest catalog (Go's preflight, which must not mutate it).
pub(crate) fn load_related_table_from_catalog(
    catalog: &SessionExtendedInfoSchema,
) -> impl FnMut(i64, &str) -> Result<(i64, TableInfo), tidb_terror::MysqlError> + '_ {
    move |table_id, description| {
        let info = catalog
            .table_info_by_id(table_id)
            .ok_or_else(|| invalid_ddl_job(format!("modify column: {description} table not found")))?;
        let schema_id = catalog
            .schema_name_by_table_id(table_id)
            .and_then(|schema| catalog.schema_by_name(&schema))
            .map(|schema| schema.read().id)
            .ok_or_else(|| invalid_ddl_job(format!("modify column: {description} schema not found")))?;
        let info = info.read().clone_like_go();
        Ok((schema_id, info))
    }
}

/// Go master `validateMaterializedViewBaseModifyColumn`.
pub(crate) fn validate_materialized_view_base_modify_column(
    catalog: &SessionExtendedInfoSchema,
    base: &TableInfo,
    old: &tidb_model::ColumnInfo,
    new: &tidb_model::ColumnInfo,
    op: &str,
    may_need_change_col_data: bool,
) -> Result<(), tidb_terror::MysqlError> {
    let Some(base_info) = &base.materialized_view_base else {
        return Ok(());
    };
    let (mlog_id, has_dependent_mv) = {
        let base_info = base_info.read();
        (base_info.mlog_id, !base_info.mview_ids.is_empty())
    };
    let has_mlog = mlog_id != 0;
    if !has_dependent_mv && !has_mlog || is_column_comment_only_change(old, new) {
        return Ok(());
    }
    let mut tracked = false;
    if has_mlog {
        let mlog = catalog
            .table_info_by_id(mlog_id)
            .ok_or_else(|| invalid_ddl_job("modify column: materialized view log table not found"))?;
        let mlog = mlog.read();
        validate_materialized_view_log_info_for_modify_column(base, &mlog)?;
        tracked = tracked_materialized_view_log_column_for_modify_column(&mlog, old.name.lowercase())?.is_some();
    }
    if old.name.lowercase() != new.name.lowercase() {
        if has_dependent_mv || tracked {
            return Err(unsupported(
                "CHANGE COLUMN on base table with materialized view dependencies does not support renaming".to_owned(),
            ));
        }
        return Ok(());
    }
    if tracked && is_null_to_not_null_change(old, new) {
        return Err(unsupported(format!(
            "{op} on base table with materialized view log does not support changing tracked columns from NULL to NOT NULL"
        )));
    }
    if may_need_change_col_data {
        if has_dependent_mv {
            return Err(unsupported(format!(
                "{op} on base table with materialized view dependencies only supports no-reorg compatible type changes"
            )));
        }
        if tracked {
            return Err(unsupported(format!(
                "{op} on base table with materialized view log only supports no-reorg compatible type changes for tracked columns"
            )));
        }
        return Ok(());
    }
    build_materialized_view_related_table_info_for_modify_column(
        &mut load_related_table_from_catalog(catalog),
        op,
        base,
        old,
        new,
    )
    .map(|_| ())
}

/// Go master `buildInvolvingSchemaInfoForMaterializedViewBaseModifyColumn`:
/// the base table, its log and (recursively) its dependent materialized
/// views with theirs.
pub(crate) fn involving_schema_info_for_materialized_view_base_modify_column(
    catalog: &SessionExtendedInfoSchema,
    schema_name: &str,
    base: &TableInfo,
) -> Vec<tidb_model::InvolvingSchemaInfo> {
    let has_dependents = base
        .materialized_view_base
        .as_ref()
        .is_some_and(|info| info.read().mlog_id != 0 || !info.read().mview_ids.is_empty());
    if !has_dependents {
        return Vec::new();
    }
    let mut involving: Vec<tidb_model::InvolvingSchemaInfo> = Vec::new();
    fn add(involving: &mut Vec<tidb_model::InvolvingSchemaInfo>, database: &str, table: &str) -> bool {
        if database.is_empty()
            || table.is_empty()
            || involving.iter().any(|info| info.database == database && info.table == table)
        {
            return false;
        }
        involving.push(tidb_model::InvolvingSchemaInfo {
            database: database.to_owned().into(),
            table: table.to_owned().into(),
            ..tidb_model::InvolvingSchemaInfo::default()
        });
        true
    }
    fn add_dependencies(
        catalog: &SessionExtendedInfoSchema,
        involving: &mut Vec<tidb_model::InvolvingSchemaInfo>,
        database: &str,
        info: &TableInfo,
    ) {
        if !add(involving, database, info.name.lowercase()) {
            return;
        }
        let Some(base_info) = &info.materialized_view_base else {
            return;
        };
        let (mlog_id, mview_ids) = {
            let base_info = base_info.read();
            (base_info.mlog_id, base_info.mview_ids.iter().copied().collect::<Vec<i64>>())
        };
        let schema_of = |id: i64| catalog.schema_name_by_table_id(id).map(|schema| schema.go_to_lower());
        if mlog_id != 0 {
            if let (Some(mlog), Some(schema)) = (catalog.table_info_by_id(mlog_id), schema_of(mlog_id)) {
                add(involving, &schema, mlog.read().name.lowercase());
            }
        }
        for mview_id in mview_ids {
            if let (Some(mview), Some(schema)) = (catalog.table_info_by_id(mview_id), schema_of(mview_id)) {
                let mview = mview.read().clone_like_go();
                add_dependencies(catalog, involving, &schema, &mview);
            }
        }
    }
    add_dependencies(catalog, &mut involving, schema_name, base);
    involving
}

/// Go master `checkBaseTableMaterializedViewDependencyConstraints`.
pub(crate) fn check_base_table_materialized_view_dependency_constraints(
    table: &TableInfo,
    op: &str,
) -> Result<(), tidb_terror::MysqlError> {
    let Some(base) = &table.materialized_view_base else {
        return Ok(());
    };
    let base = base.read();
    if !base.mview_ids.is_empty() {
        return Err(unsupported(format!("{op} with materialized view dependencies")));
    }
    if base.mlog_id != 0 {
        return Err(unsupported(format!("{op} with materialized view log")));
    }
    Ok(())
}

/// Go master `CheckIndexOperationMaterializedViewConstraints`.
pub(crate) fn check_index_operation_materialized_view_constraints(
    table: &TableInfo,
    op: &str,
    unique: bool,
) -> Result<(), tidb_terror::MysqlError> {
    if table.materialized_view_log.is_some() {
        return Err(unsupported(format!("{op} on materialized view log table")));
    }
    if unique && table.materialized_view.is_some() {
        return Err(unsupported(format!("{op} on materialized view table")));
    }
    check_protected_materialized_view_shadow_constraint(table, op)
}

/// Go master `checkExchangePartitionMaterializedViewConstraints`.
pub(crate) fn check_exchange_partition_materialized_view_constraints(
    table: &TableInfo,
    role: &str,
) -> Result<(), tidb_terror::MysqlError> {
    if table.materialized_view_log.is_some() {
        return Err(unsupported(format!("EXCHANGE PARTITION on {role} with materialized view log")));
    }
    if table.materialized_view.is_some() {
        return Err(unsupported(format!("EXCHANGE PARTITION on {role} materialized view table")));
    }
    if table.materialized_view_shadow.is_some() {
        return Err(unsupported(format!("EXCHANGE PARTITION on {role} materialized view shadow table")));
    }
    check_base_table_materialized_view_dependency_constraints(table, &format!("EXCHANGE PARTITION on {role}"))
}

/// Go master `isAlterTiFlashReplica`.
fn is_alter_tiflash_replica(actions: &[AlterTableAction]) -> bool {
    matches!(actions, [AlterTableAction::SetTiFlashReplica { .. }])
}

/// Go master `isAlterTableOnlyExchangePartition`.
fn is_alter_table_only_exchange_partition(actions: &[AlterTableAction]) -> bool {
    matches!(actions, [AlterTableAction::Partition(tidb_ast::AlterPartitionAction::Exchange { .. })])
}

/// Go master `isAlterTableOnlyAddColumnsAtEnd` over resolved specs
/// (`resolve_alter_table_spec` splits a parenthesized ADD COLUMN).
fn is_alter_table_only_add_columns_at_end(actions: &[AlterTableAction]) -> bool {
    !actions.is_empty()
        && actions.iter().all(|action| {
            matches!(action, AlterTableAction::AddColumn { position: tidb_ast::ColumnPosition::Default, .. })
        })
}

/// Go master `isAlterTableOnlyIndexOperations`.
fn is_alter_table_only_index_operations(actions: &[AlterTableAction]) -> bool {
    use tidb_ast::IndexConstraintKind as Kind;
    !actions.is_empty()
        && actions.iter().all(|action| match action {
            AlterTableAction::DropIndex { .. }
            | AlterTableAction::DropPrimaryKey(_)
            | AlterTableAction::RenameIndex(_)
            | AlterTableAction::AlterIndexVisibility(_) => true,
            AlterTableAction::AddIndexConstraint(constraint) => matches!(
                constraint.kind,
                Kind::Key | Kind::Index | Kind::Unique | Kind::UniqueKey | Kind::UniqueIndex
            ),
            _ => false,
        })
}

/// Go master `hasAlterTableAddUniqueIndexOperation`.
fn has_alter_table_add_unique_index_operation(actions: &[AlterTableAction]) -> Option<&'static str> {
    use tidb_ast::IndexConstraintKind as Kind;
    actions.iter().find_map(|action| match action {
        AlterTableAction::AddIndexConstraint(constraint) => match constraint.kind {
            Kind::Unique | Kind::UniqueKey | Kind::UniqueIndex => Some("ALTER TABLE ADD UNIQUE INDEX"),
            Kind::PrimaryKey => Some("ALTER TABLE ADD PRIMARY KEY"),
            _ => None,
        },
        _ => None,
    })
}

/// Go master `checkAlterTableOnlyNonRenamingModifyOrChangeColumns`.
fn check_alter_table_only_non_renaming_modify_or_change_columns(
    actions: &[AlterTableAction],
    op: &str,
) -> Result<(), tidb_terror::MysqlError> {
    let no_reorg_only =
        || unsupported(format!("{op} on base table with materialized view dependencies only supports no-reorg compatible type changes"));
    if actions.is_empty() {
        return Err(no_reorg_only());
    }
    for action in actions {
        match action {
            AlterTableAction::ModifyColumn { .. } => {}
            AlterTableAction::ChangeColumn { old_name, column, .. } => {
                let old = old_name.last().map(|name| name.go_to_lower()).unwrap_or_default();
                if old != column.name.go_to_lower() {
                    return Err(unsupported(
                        "CHANGE COLUMN on base table with materialized view dependencies does not support renaming".to_owned(),
                    ));
                }
            }
            _ => return Err(no_reorg_only()),
        }
    }
    Ok(())
}

/// Go master `collectAlterTableSpecAffectedColumns`, for the specs that
/// reach it (MODIFY and CHANGE COLUMN are skipped before).
fn collect_alter_table_spec_affected_columns(action: &AlterTableAction) -> Vec<String> {
    match action {
        AlterTableAction::DropColumn { name, .. } => vec![name.go_to_lower()],
        AlterTableAction::RenameColumn(rename) => vec![rename.from.go_to_lower(), rename.to.go_to_lower()],
        AlterTableAction::AlterColumnDefault(alter) => {
            alter.name.last().map(|name| vec![name.go_to_lower()]).unwrap_or_default()
        }
        _ => Vec::new(),
    }
}

/// Go master `checkAlterTableBaseTableMLogColumnConstraints`.
fn check_alter_table_base_table_mlog_column_constraints(
    catalog: &SessionExtendedInfoSchema,
    table: &TableInfo,
    actions: &[AlterTableAction],
    op: &str,
) -> Result<(), tidb_terror::MysqlError> {
    let mlog_id = table.materialized_view_base.as_ref().map_or(0, |base| base.read().mlog_id);
    if mlog_id == 0 {
        return Ok(());
    }
    let mlog = catalog.table_info_by_id(mlog_id);
    let Some(log) = mlog.as_ref().and_then(|mlog| mlog.read().materialized_view_log.clone()) else {
        return Err(unsupported(format!("{op} on base table with invalid materialized view log metadata")));
    };
    let tracked: Vec<String> = log.read().columns.iter().map(|column| column.lowercase().to_owned()).collect();
    for action in actions {
        // Go `NeedToOverwriteColCharset`: CONVERT TO CHARACTER SET.
        if matches!(action, AlterTableAction::ConvertCharacterSet { .. }) && !tracked.is_empty() {
            return Err(unsupported(format!("{op} on base table columns referenced by materialized view log")));
        }
        if matches!(action, AlterTableAction::ModifyColumn { .. } | AlterTableAction::ChangeColumn { .. }) {
            continue;
        }
        if let Some(column) = collect_alter_table_spec_affected_columns(action).into_iter().find(|column| tracked.contains(column)) {
            return Err(unsupported(format!(
                "{op} on base table column {column} referenced by materialized view log"
            )));
        }
    }
    Ok(())
}

/// Go master `checkAlterTableMaterializedViewConstraints` for a statement
/// outside materialized view maintenance.
pub(crate) fn check_alter_table_materialized_view_constraints(
    catalog: &SessionExtendedInfoSchema,
    table: &TableInfo,
    actions: &[AlterTableAction],
    op: &str,
) -> Result<(), tidb_terror::MysqlError> {
    if is_alter_table_only_exchange_partition(actions) {
        return Ok(());
    }
    if table.materialized_view_log.is_some() {
        if is_alter_tiflash_replica(actions) {
            return Ok(());
        }
        return Err(unsupported(format!("{op} on materialized view log table")));
    }
    if table.materialized_view.is_some() {
        if let Some(op) = has_alter_table_add_unique_index_operation(actions) {
            return Err(unsupported(format!("{op} on materialized view table")));
        }
        if is_alter_tiflash_replica(actions) || is_alter_table_only_index_operations(actions) {
            return Ok(());
        }
        return Err(unsupported(format!("{op} on materialized view table")));
    }
    check_protected_materialized_view_shadow_constraint(table, op)?;
    if table.materialized_view_base.as_ref().is_some_and(|base| !base.read().mview_ids.is_empty()) {
        if is_alter_tiflash_replica(actions)
            || is_alter_table_only_index_operations(actions)
            || is_alter_table_only_add_columns_at_end(actions)
        {
            return Ok(());
        }
        check_alter_table_only_non_renaming_modify_or_change_columns(actions, op)?;
    }
    check_alter_table_base_table_mlog_column_constraints(catalog, table, actions, op)
}

/// Go master `checkDropMaterializedViewLogHasNoDependentMVs`: a log that
/// materialized views read cancels the drop.
pub(crate) fn check_drop_materialized_view_log_has_no_dependent_mvs(
    txn: &mut JobTxn<'_>,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    dropping: &TableInfo,
) -> Result<(), DdlPlanError> {
    let Some(log) = &dropping.materialized_view_log else {
        return Ok(());
    };
    let (base_table_id, has_dependents) = {
        let log = log.read();
        (log.base_table_id, !log.dependent_mview_ids.is_empty())
    };
    if !has_dependents {
        return Ok(());
    }
    let base = match get_table_info(txn, base_table_id, active.job.schema_id) {
        Ok(base) => base,
        Err(error) if is_not_exists(&error) => return Ok(()),
        Err(error) => return Err(error),
    };
    let message = format!(
        "cannot drop materialized view log on {}.{}: dependent materialized views exist",
        active.job.schema_name,
        base.name.original()
    );
    Err(cancel_ddl_job(active, DdlPlanError::from_mysql_error(tidb_terror::MysqlError::unknown(message))))
}

/// Go `getTableInfo` over the job's mutator: the table, or Go's
/// database / table not-exists error.
fn get_table_info(txn: &mut JobTxn<'_>, table_id: i64, schema_id: i64) -> Result<TableInfo, DdlPlanError> {
    let meta = txn.meta();
    match meta.table(schema_id, table_id) {
        Ok(Some(table)) => Ok(table),
        Ok(None) => Err(DdlPlanError::from(
            tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS.gen_with_stack_by_args(&[
                format!("(Schema ID {schema_id})").into(),
                format!("(Table ID {table_id})").into(),
            ]),
        )),
        Err(tidb_meta::MetaError::DatabaseNotExists) => Err(DdlPlanError::from(
            tidb_infoschema::infoschema_errors::ERR_DATABASE_NOT_EXISTS
                .gen_with_stack_by_args(&[format!("(Schema ID {schema_id})").into()]),
        )),
        Err(error) => Err(DdlPlanError::Meta(job_meta_error(&meta, error))),
    }
}

fn is_not_exists(error: &DdlPlanError) -> bool {
    error.equal(&tidb_infoschema::infoschema_errors::ERR_DATABASE_NOT_EXISTS)
        || error.equal(&tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS)
}

/// Go `updateTable(t, schemaID, tblInfo, true)`.
fn update_table(txn: &mut JobTxn<'_>, schema_id: i64, table: &mut TableInfo, start_ts: u64) -> Result<(), DdlPlanError> {
    table.update_ts = start_ts;
    let meta = txn.meta();
    meta.update_table(schema_id, table).map_err(|error| DdlPlanError::Meta(job_meta_error(&meta, error)))
}

/// Go `SetSchemaDiffForMultiInfos`: each extra table not already in the diff
/// becomes an affected option of its own schema.
pub(crate) fn set_schema_diff_for_multi_infos(diff: &mut SchemaDiff, infos: &[(i64, i64)]) {
    if infos.is_empty() {
        return;
    }
    let mut affected: Vec<AffectedOption> = diff.affected_options.iter_deref().map(|a| a.read().clone()).collect();
    let mut seen: Vec<i64> = std::iter::once(diff.table_id).chain(affected.iter().map(|a| a.table_id)).collect();
    for &(schema_id, table_id) in infos {
        if seen.contains(&table_id) {
            continue;
        }
        seen.push(table_id);
        affected.push(AffectedOption {
            schema_id,
            old_schema_id: schema_id,
            table_id,
            old_table_id: table_id,
            ..AffectedOption::default()
        });
    }
    diff.affected_options = GoSharedPointerSlice::from(affected);
}

/// Go `buildCreateMaterializedViewLogPurgeInfoUpsertSQL`.
fn create_materialized_view_log_purge_info_upsert_sql(mlog_id: i64, next: Option<i64>, should_update: bool) -> String {
    let next = next.map_or_else(|| "NULL".to_owned(), |next| next.to_string());
    if should_update {
        format!(
            "INSERT INTO mysql.tidb_mlog_purge_info (MLOG_ID, NEXT_PURGE_UNIX_SECONDS)\nVALUES ({mlog_id}, {next}) ON DUPLICATE KEY UPDATE NEXT_PURGE_UNIX_SECONDS = VALUES(NEXT_PURGE_UNIX_SECONDS)"
        )
    } else {
        format!("INSERT IGNORE INTO mysql.tidb_mlog_purge_info (MLOG_ID) VALUES ({mlog_id})")
    }
}

/// Go `convertCreateMaterializedViewLogPurgeInfoTableNotExistsErr`.
fn convert_purge_info_table_not_exists(error: tidb_terror::MysqlError) -> DdlPlanError {
    if error.equal(&tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS) {
        return DdlPlanError::from(tidb_dbterror::ERR_INVALID_DDL_JOB.gen_with_stack_by_args(&[
            "create materialized view log: mysql.tidb_mlog_purge_info does not exist".into(),
        ]));
    }
    DdlPlanError::from_mysql_error(error)
}

fn execute_job_sql(txn: &JobTxn<'_>, sql: &str) -> Result<(), tidb_terror::MysqlError> {
    match txn.execute(sql, &[]) {
        Some(result) => result.map(|_| ()),
        None => Err(tidb_terror::MysqlError::unknown(
            "a materialized view log job step needs the DDL session".to_owned(),
        )),
    }
}

/// Go `upsertCreateMaterializedViewLogPurgeInfo`: the log's purge schedule
/// row, its next purge derived from the stored START WITH / NEXT.
fn upsert_create_materialized_view_log_purge_info(txn: &JobTxn<'_>, mlog: &TableInfo) -> Result<(), DdlPlanError> {
    let Some(info) = &mlog.materialized_view_log else {
        return Err(DdlPlanError::from(tidb_dbterror::ERR_INVALID_DDL_JOB.gen_with_stack_by_args(&[
            "create materialized view log: invalid materialized view log metadata".into(),
        ])));
    };
    let info = info.read().clone();
    let (next, should_update) = tidb_ddl_build::materialized_view::derive_materialized_schedule_next_unix_seconds(
        &info.purge_start_with,
        &info.purge_next,
        info.purge_schedule_sql_mode,
        chrono::Utc::now(),
    )
    .map_err(DdlPlanError::from_mysql_error)?;
    let sql = create_materialized_view_log_purge_info_upsert_sql(mlog.id, next, should_update);
    execute_job_sql(txn, &sql).map_err(convert_purge_info_table_not_exists)
}

/// Go `deleteMaterializedViewLogPurgeInfos`: a missing table deletes nothing.
pub(crate) fn delete_materialized_view_log_purge_info(txn: &JobTxn<'_>, mlog_id: i64) -> Result<(), DdlPlanError> {
    let sql = format!("DELETE FROM mysql.tidb_mlog_purge_info WHERE MLOG_ID IN ({mlog_id})");
    match execute_job_sql(txn, &sql) {
        Err(error) if error.equal(&tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS) => Ok(()),
        other => other.map_err(DdlPlanError::from_mysql_error),
    }
}

/// Go `updateMaterializedViewBaseInfoOnDrop` for a dropped MV log: its base
/// table forgets it (and its `MaterializedViewBase` once nothing remains).
pub(crate) fn update_materialized_view_base_info_on_drop_log(
    txn: &mut JobTxn<'_>,
    active: &tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    dropping: &TableInfo,
) -> Result<Vec<(i64, TableInfo)>, DdlPlanError> {
    let Some(log) = &dropping.materialized_view_log else {
        return Ok(Vec::new());
    };
    let base_id = log.read().base_table_id;
    let schema_id = active.job.schema_id;
    let meta = txn.meta();
    let base = meta.table(schema_id, base_id).map_err(|error| DdlPlanError::Meta(job_meta_error(&meta, error)))?;
    let Some(mut base) = base else {
        return Ok(Vec::new());
    };
    if let Some(info) = base.materialized_view_base.clone() {
        let (mlog_id, empty) = {
            let mut info = info.write();
            if info.mlog_id == active.job.table_id {
                info.mlog_id = 0;
            }
            (info.mlog_id, info.mview_ids.is_empty())
        };
        if mlog_id == 0 && empty {
            base.materialized_view_base = None;
        }
    }
    Ok(vec![(schema_id, base)])
}

/// Go `(*worker).onCreateMaterializedViewLog`.
pub(super) fn on_create_materialized_view_log(
    txn: &mut JobTxn<'_>,
    catalog: &SessionExtendedInfoSchema,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let invalid_args = |active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob| {
        cancel_ddl_job(
            active,
            DdlPlanError::from(tidb_dbterror::ERR_INVALID_DDL_JOB.gen_with_stack_by_args(&[
                "create materialized view log: invalid job args".into(),
            ])),
        )
    };
    let args = tidb_model::get_create_materialized_view_log_args(&mut active.job)
        .map_err(|error| cancel_ddl_job(active, DdlPlanError::Encode(error.to_string())))?;
    let Some(table_handle) = args.and_then(|args| args.read().table_info.clone()) else {
        return Err(invalid_args(active));
    };
    let mut mlog = table_handle.read().clone();
    let Some(log_info) = mlog.materialized_view_log.as_ref().map(|info| info.read().clone()) else {
        return Err(invalid_args(active));
    };
    if active.job.is_rollingback() {
        return rollback_create_materialized_view_log(txn, active, &mlog, start_ts);
    }
    if log_info.base_table_id == 0 {
        return Err(cancel_ddl_job(
            active,
            DdlPlanError::from(tidb_dbterror::ERR_INVALID_DDL_JOB.gen_with_stack_by_args(&[
                "create materialized view log: invalid base table id".into(),
            ])),
        ));
    }
    let schema_id = active.job.schema_id;
    let mut base = match get_table_info(txn, log_info.base_table_id, schema_id) {
        Ok(base) => base,
        Err(error) if is_not_exists(&error) => return Err(cancel_ddl_job(active, error)),
        Err(error) => return Err(error),
    };
    let schema_name = active.job.schema_name.to_string();
    if !is_valid_materialized_view_log_base_table(&tidb_hack::go_to_lower(&schema_name), &base) {
        return Err(cancel_ddl_job(
            active,
            DdlPlanError::from(tidb_dbterror::ERR_WRONG_OBJECT.gen_with_stack_by_args(&[
                schema_name.clone().into(),
                base.name.original().to_owned().into(),
                "BASE TABLE".into(),
            ])),
        ));
    }
    if base.get_partition_info().is_some() {
        return Err(cancel_ddl_job(
            active,
            err_unsupported_materialized_view_on_partition_table("CREATE MATERIALIZED VIEW LOG"),
        ));
    }
    if base.state != SchemaState::PUBLIC {
        return Err(cancel_ddl_job(
            active,
            DdlPlanError::from(tidb_dbterror::ERR_INVALID_DDL_STATE.generate(format!(
                "table {} is not in public, but {}",
                base.name, base.state
            ))),
        ));
    }
    if base.materialized_view_base.as_ref().is_some_and(|info| info.read().mlog_id != 0) {
        return Err(cancel_ddl_job(
            active,
            DdlPlanError::from(tidb_infoschema::infoschema_errors::ERR_TABLE_EXISTS.gen_with_stack_by_args(&[
                format!("{}.{}", schema_name, mlog.name.original()).into(),
            ])),
        ));
    }
    if base.materialized_view.is_some() {
        // Go takes the MV's refresh advisory lock in the DDL session here
        // (`mviewutil.RefreshAdvisoryLockName`), which the DDL step session
        // of this port does not hold.
        return Err(cancel_ddl_job(
            active,
            DdlPlanError::from(tidb_dbterror::ERR_GENERAL_UNSUPPORTED_DDL.gen_with_stack_by_args(&[
                "CREATE MATERIALIZED VIEW LOG on a materialized view".into(),
            ])),
        ));
    }

    // Go `createTable(w, jobCtx, job, ..., &model.CreateTableArgs{TableInfo:
    // mlogTableInfo, FKCheck: false})`.
    let database = job_schema_with_tables(txn, schema_id)?.ok_or_else(|| {
        cancel_ddl_job(
            active,
            DdlPlanError::from(
                tidb_dbterror::CLASS_SCHEMA
                    .new_std(tidb_errno::errcode::ErrBadDB)
                    .generate(format!("Unknown database '(Schema ID {schema_id})'")),
            ),
        )
    })?;
    let mut placement_bundles = Vec::new();
    let mut affinity_groups = crate::ddl_affinity::AffinityGroupUpdate::default();
    mlog.state = SchemaState::NONE;
    create_table::create_table(
        txn,
        catalog,
        &database,
        active,
        &mut mlog,
        false,
        start_ts,
        &mut placement_bundles,
        &mut affinity_groups,
    )?;
    let base_info = base.materialized_view_base.get_or_insert_with(Default::default);
    base_info.write().mlog_id = mlog.id;
    update_table(txn, schema_id, &mut base, start_ts)?;
    if let Err(error) = upsert_create_materialized_view_log_purge_info(txn, &mlog) {
        if error.equal(&tidb_dbterror::ERR_INVALID_DDL_JOB) {
            active.job.state = JobState::ROLLINGBACK;
        }
        return Err(error);
    }
    let mut diff = SchemaDiff {
        action_type: active.job.type_,
        schema_id,
        table_id: active.job.table_id,
        ..SchemaDiff::default()
    };
    set_schema_diff_for_multi_infos(&mut diff, &[(schema_id, base.id)]);
    let (schema_version, diff) = update_schema_version(&txn.meta(), active, diff)?;
    *table_handle.write() = mlog.clone_like_go();
    let mut mutations = Vec::new();
    append_schema_change_mutations(
        txn,
        catalog,
        active.job.id,
        &[(-1, SchemaChangeEvent::create_table(mlog.clone_like_go()))],
        &mut mutations,
        row_ids,
    )?;
    active.job.finish_multiple_table_job(
        JobState::DONE,
        SchemaState::PUBLIC,
        schema_version,
        &GoSharedPointerSlice::from(vec![base.clone_like_go(), mlog.clone_like_go()]),
    );
    let mut write = job_only_write(active.job.id, mutations);
    write.schema_version = schema_version;
    write.diff = diff;
    write.created_id = Some(mlog.id);
    write.placement_bundles = placement_bundles;
    write.affinity_groups = affinity_groups;
    Ok(PersistedDdlActionStep { write, update_raw_args: true, run_error: None, reorg: None })
}

/// Go `(*worker).rollbackCreateMaterializedViewLog`.
fn rollback_create_materialized_view_log(
    txn: &mut JobTxn<'_>,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    mlog: &TableInfo,
    start_ts: u64,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let schema_id = active.job.schema_id;
    let table_id = active.job.table_id;
    let actual = match get_table_info(txn, table_id, schema_id) {
        Ok(table) => Some(table),
        Err(error) if is_not_exists(&error) => None,
        Err(error) => return Err(error),
    };
    let dropping = actual.clone().unwrap_or_else(|| mlog.clone());
    let extra = update_materialized_view_base_info_on_drop_log(txn, active, &dropping)?;
    for (extra_schema, table) in &extra {
        update_table(txn, *extra_schema, &mut table.clone(), start_ts)?;
    }
    if actual.is_some() {
        let meta = txn.meta();
        meta.drop_table_or_view(schema_id, table_id).map_err(|error| DdlPlanError::Meta(job_meta_error(&meta, error)))?;
        meta.auto_ids(schema_id, table_id).delete().map_err(|error| DdlPlanError::Meta(job_meta_error(&meta, error)))?;
    }
    delete_materialized_view_log_purge_info(txn, table_id)?;
    // Go: the rollback diff needs RollbackDone (`SetSchemaDiffForCreateTable`
    // answers `OldTableID`); a failed update leaves the job retryable.
    let (prev_state, prev_schema_state) = (active.job.state, active.job.schema_state);
    active.job.state = JobState::ROLLBACK_DONE;
    active.job.schema_state = SchemaState::NONE;
    let mut diff = SchemaDiff {
        action_type: active.job.type_,
        schema_id,
        old_table_id: table_id,
        table_id: 0,
        ..SchemaDiff::default()
    };
    let infos: Vec<(i64, i64)> = extra.iter().map(|(schema, table)| (*schema, table.id)).collect();
    set_schema_diff_for_multi_infos(&mut diff, &infos);
    let (schema_version, diff) = match update_schema_version(&txn.meta(), active, diff) {
        Ok(updated) => updated,
        Err(error) => {
            active.job.state = prev_state;
            active.job.schema_state = prev_schema_state;
            return Err(error);
        }
    };
    let mut write = job_only_write(active.job.id, Vec::new());
    write.schema_version = schema_version;
    write.diff = diff;
    Ok(PersistedDdlActionStep { write, update_raw_args: true, run_error: None, reorg: None })
}

/// Go master `onAlterMaterializedViewLogPurge`: the log's purge method and
/// schedule, with a notifier event.
pub(super) fn on_alter_materialized_view_log_purge(
    txn: &mut JobTxn<'_>,
    catalog: &SessionExtendedInfoSchema,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let args = tidb_model::get_alter_materialized_view_log_purge_args(&mut active.job)
        .map_err(|error| cancel_ddl_job(active, DdlPlanError::Encode(error.to_string())))?
        .unwrap_or_default();
    let args = args.read().clone();
    // Go `GetTableInfoAndCancelFaultJob`.
    let mut table = checked_job_table(txn, active, true)?.read().clone_like_go();
    let Some(log) = table.materialized_view_log.clone() else {
        return Err(cancel_ddl_job(
            active,
            DdlPlanError::from(tidb_dbterror::ERR_WRONG_OBJECT.gen_with_stack_by_args(&[
                active.job.schema_name.to_string().into(),
                active.job.table_name.to_string().into(),
                "MATERIALIZED VIEW LOG".into(),
            ])),
        ));
    };
    if is_revertible_sub_job(&active.job) {
        active.job.mark_non_revertible();
        return Ok(super::add_index::action_step(job_only_write(active.job.id, Vec::new())));
    }
    let old_table = table.clone_like_go();
    {
        let mut log = log.write();
        log.purge_method = args.purge_method;
        log.purge_start_with = args.purge_start_with;
        log.purge_next = args.purge_next;
        if args.update_purge_schedule {
            log.purge_schedule_sql_mode = args.purge_schedule_sql_mode;
        }
    }
    let schema_id = active.job.schema_id;
    let mut write = super::add_index::update_version_and_table_info(txn, active, schema_id, &table, true, start_ts)?;
    let schema_version = write.schema_version;
    let mut mutations = Vec::new();
    append_schema_change_mutations(
        txn,
        catalog,
        active.job.id,
        &[(
            -1,
            SchemaChangeEvent::alter_materialized_view_log_purge(table.clone_like_go(), old_table),
        )],
        &mut mutations,
        row_ids,
    )?;
    active.job.finish_table_job(JobState::DONE, SchemaState::PUBLIC, schema_version, Some(GoShared::new(table)));
    write.mutations.extend(mutations);
    Ok(super::add_index::action_step(write))
}
