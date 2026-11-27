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
    if table.materialized_view_shadow.is_none() {
        return Ok(());
    }
    if op == "DROP TABLE" {
        // "The DDL owner runs this check again before dropping the table."
        if is_materialized_view_shadow_table_expired(table.name.lowercase(), chrono::Utc::now()) {
            return Ok(());
        }
        return Err(unsupported(
            "DROP TABLE on materialized view shadow table younger than 7 days or with an invalid creation timestamp".to_owned(),
        ));
    }
    Err(unsupported(format!("{op} on materialized view shadow table")))
}

/// Go master `mviewShadowTableRetention`.
const MVIEW_SHADOW_TABLE_RETENTION: chrono::Duration = chrono::Duration::days(7);

/// Go master `isMaterializedViewShadowTableExpired`: "Generated shadow names
/// are __mv_shadow_<mviewID>_<UnixNano>."
fn is_materialized_view_shadow_table_expired(name: &str, now: chrono::DateTime<chrono::Utc>) -> bool {
    let Some(suffix) = name.strip_prefix(tidb_mviewutil::SHADOW_TABLE_PREFIX) else {
        return false;
    };
    let Some((_, timestamp)) = suffix.split_once('_') else {
        return false;
    };
    let Some(cutoff) = (now - MVIEW_SHADOW_TABLE_RETENTION).timestamp_nanos_opt() else {
        return false;
    };
    timestamp.parse::<i64>().is_ok_and(|created| created > 0 && created <= cutoff)
}

/// Go master `checkMaterializedViewGroupKeyIndexConstraints`: "Additional
/// UNIQUE/PRIMARY indexes on MVs are disallowed, so the only such index is
/// the auto-created group key. Integer clustered PKs have no IndexInfo."
pub(crate) fn check_materialized_view_group_key_index_constraints(
    table: &TableInfo,
    index_name: &str,
    op: &str,
) -> Result<(), tidb_terror::MysqlError> {
    if table.materialized_view.is_none() {
        return Ok(());
    }
    let lower = tidb_hack::go_to_lower(index_name);
    let name = if let Some(index) = table.indices.iter_deref().find(|index| index.read().name.lowercase() == lower) {
        let index = index.read();
        if !index.unique && !index.primary {
            return Ok(());
        }
        index.name.original().to_owned()
    } else if table.pk_is_handle && lower == tidb_hack::go_to_lower(tidb_mysql::consts::PrimaryKeyName) {
        tidb_mysql::consts::PrimaryKeyName.to_owned()
    } else {
        return Ok(());
    };
    Err(unsupported(format!(
        "{op} on automatically created index {name} of materialized view {}",
        table.name.original()
    )))
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

/// Go master `maxMaterializedViewNestingDepth`.
const MAX_MATERIALIZED_VIEW_NESTING_DEPTH: usize = 16;

/// Go master `validateMaterializedViewNestingDepth`: the longest source chain,
/// counting the new view as depth 1. "The depth budget also bounds traversal
/// of cyclic metadata."
pub(crate) fn validate_materialized_view_nesting_depth<E>(
    source: &TableInfo,
    get_table: &mut dyn FnMut(i64) -> Result<TableInfo, E>,
    depth_exceeded: &dyn Fn() -> E,
) -> Result<(), E> {
    fn source_depth<E>(
        table: &TableInfo,
        remaining_depth: usize,
        depths: &mut std::collections::HashMap<i64, usize>,
        get_table: &mut dyn FnMut(i64) -> Result<TableInfo, E>,
        depth_exceeded: &dyn Fn() -> E,
    ) -> Result<usize, E> {
        let Some(info) = table.materialized_view.as_ref() else {
            return Ok(0);
        };
        let cached_depth = depths.get(&table.id).copied().unwrap_or(0);
        if remaining_depth == 0 || cached_depth > remaining_depth {
            return Err(depth_exceeded());
        }
        if cached_depth > 0 {
            return Ok(cached_depth);
        }
        let base_table_ids: Vec<i64> = info.read().base_table_ids.iter().copied().collect();
        let mut depth = 1;
        for base_table_id in base_table_ids {
            let base_table = get_table(base_table_id)?;
            let base_depth = source_depth(&base_table, remaining_depth - 1, depths, get_table, depth_exceeded)?;
            depth = depth.max(base_depth + 1);
        }
        depths.insert(table.id, depth);
        Ok(depth)
    }
    source_depth(source, MAX_MATERIALIZED_VIEW_NESTING_DEPTH - 1, &mut std::collections::HashMap::new(), get_table, depth_exceeded)
        .map(|_| ())
}

/// Go master's nesting-depth error.
pub(crate) fn materialized_view_nesting_depth_exceeded() -> tidb_terror::MysqlError {
    tidb_terror::MysqlError::from_terror(&tidb_dbterror::ERR_GENERAL_UNSUPPORTED_DDL.gen_with_stack(format!(
        "materialized view nesting depth exceeds maximum of {MAX_MATERIALIZED_VIEW_NESTING_DEPTH}"
    )))
}

/// Go master `validateMaterializedViewSourceState`: "Public physical tables
/// and ready MVs are valid sources; protected MV auxiliary tables and SQL
/// views are not."
pub(crate) fn validate_materialized_view_source_state(
    schema_name: &str,
    source: &TableInfo,
) -> Result<(), tidb_terror::MysqlError> {
    if source.is_view()
        || source.is_sequence()
        || source.temp_table_type != tidb_model::TempTableType::NONE
        || source.materialized_view_shadow.is_some()
        || source.materialized_view_log.is_some()
    {
        return Err(tidb_terror::MysqlError::from_terror(&tidb_dbterror::ERR_WRONG_OBJECT.gen_with_stack_by_args(&[
            schema_name.to_owned().into(),
            source.name.original().to_owned().into(),
            "BASE TABLE".into(),
        ])));
    }
    if source.state != SchemaState::PUBLIC {
        return Err(tidb_terror::MysqlError::from_terror(&tidb_dbterror::ERR_INVALID_DDL_STATE.gen_with_stack(format!(
            "source object {} is not in public, but {}",
            source.name.original(),
            source.state
        ))));
    }
    if source.get_partition_info().is_some() {
        return Err(unsupported("CREATE MATERIALIZED VIEW on partition table".to_owned()));
    }
    if let Some(info) = &source.materialized_view {
        let state = tidb_model::table::MaterializedViewInfo::get_init_build_state(Some(&info.read()));
        if !state.is_ready() {
            return Err(invalid_ddl_job(
                state.access_error_message(&format!("{schema_name}.{}", source.name.original())),
            ));
        }
    }
    Ok(())
}

/// Go master `validateMaterializedViewSourceMLog`.
pub(crate) fn validate_materialized_view_source_mlog(
    schema_name: &str,
    source: &TableInfo,
    mlog: &TableInfo,
) -> Result<(), tidb_terror::MysqlError> {
    let mlog_id = source.materialized_view_base.as_ref().map_or(0, |base| base.read().mlog_id);
    if mlog_id == 0 {
        return Err(tidb_terror::MysqlError::unknown(format!(
            "materialized view log does not exist for source object {schema_name}.{}",
            source.name.original()
        )));
    }
    if mlog.id != mlog_id || mlog.materialized_view_log.as_ref().is_none_or(|log| log.read().base_table_id != source.id) {
        return Err(tidb_terror::MysqlError::unknown(format!(
            "table {} is not a materialized view log for source object {schema_name}.{}",
            mlog.name.original(),
            source.name.original()
        )));
    }
    if mlog.state != SchemaState::PUBLIC {
        return Err(tidb_terror::MysqlError::from_terror(&tidb_dbterror::ERR_INVALID_DDL_STATE.gen_with_stack(format!(
            "materialized view log {} is not in public, but {}",
            mlog.name.original(),
            mlog.state
        ))));
    }
    Ok(())
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
    let mut mlog_new_column = mlog_column.clone();
    mlog_new_column.field_type = field_type.clone();
    if !tidb_ddl_build::modify_column_change::is_meta_only_change(mlog, mlog_column, &mlog_new_column) {
        return Err(unsupported(format!(
            "{op} on base table with materialized view log only supports no-reorg compatible type changes"
        )));
    }
    Ok(field_type)
}

/// Go master `buildMaterializedViewRelatedTableInfoForModifyColumn`: the
/// tables a base column's change rewrites, each loaded once by `load` (its
/// schema id and a copy of its info): the log, when it tracks the column,
/// and each dependent materialized view's direct output columns, retyped
/// from the column's new type, followed through the view's own log and
/// dependents.
pub(crate) fn build_materialized_view_related_table_info_for_modify_column(
    load: &mut dyn FnMut(i64, &str) -> Result<(i64, TableInfo), tidb_terror::MysqlError>,
    op: &str,
    base: &TableInfo,
    old: &tidb_model::ColumnInfo,
    new: &tidb_model::ColumnInfo,
) -> Result<Vec<(i64, TableInfo)>, tidb_terror::MysqlError> {
    if base.materialized_view_base.is_none() || is_column_comment_only_change(old, new) {
        return Ok(Vec::new());
    }
    // "Keep one pending TableInfo per table so all column changes are
    // persisted together."
    let mut related = RelatedTables { load, infos: Vec::new(), indexes: std::collections::HashMap::new() };
    visit_materialized_view_related_tables(&mut related, op, base, old, new)?;
    Ok(related.infos)
}

/// The pending tables of [`build_materialized_view_related_table_info_for_modify_column`].
struct RelatedTables<'a> {
    load: &'a mut dyn FnMut(i64, &str) -> Result<(i64, TableInfo), tidb_terror::MysqlError>,
    infos: Vec<(i64, TableInfo)>,
    indexes: std::collections::HashMap<i64, usize>,
}

impl RelatedTables<'_> {
    /// Go `loadTable`: a changed table's pending info, else a fresh copy.
    fn load(&mut self, table_id: i64, description: &str) -> Result<(i64, TableInfo), tidb_terror::MysqlError> {
        if let Some(&index) = self.indexes.get(&table_id) {
            return Ok(self.infos[index].clone());
        }
        (self.load)(table_id, description)
    }

    /// Go `addChangedTable` (a pending table's info is Go's same pointer, so
    /// a later change replaces it).
    fn add_changed(&mut self, info: (i64, TableInfo)) {
        match self.indexes.get(&info.1.id) {
            Some(&index) => self.infos[index] = info,
            None => {
                self.indexes.insert(info.1.id, self.infos.len());
                self.infos.push(info);
            }
        }
    }
}

/// Go master `buildMaterializedViewRelatedTableInfoForModifyColumn`'s `visit`.
fn visit_materialized_view_related_tables(
    related: &mut RelatedTables<'_>,
    op: &str,
    base: &TableInfo,
    old: &tidb_model::ColumnInfo,
    new: &tidb_model::ColumnInfo,
) -> Result<(), tidb_terror::MysqlError> {
    let Some(base_info) = &base.materialized_view_base else {
        return Ok(());
    };
    let (mlog_id, mview_ids) = {
        let base_info = base_info.read();
        (base_info.mlog_id, base_info.mview_ids.iter().copied().collect::<Vec<i64>>())
    };
    if mlog_id != 0 {
        let (schema_id, mlog) = related.load(mlog_id, "materialized view log")?;
        validate_materialized_view_log_info_for_modify_column(base, &mlog)?;
        if let Some(mlog_column) = tracked_materialized_view_log_column_for_modify_column(&mlog, old.name.lowercase())? {
            if tidb_ddl_build::modify_column_change::is_null_to_not_null_change(old, new) {
                return Err(unsupported(format!(
                    "{op} on base table with materialized view log does not support changing tracked columns from NULL to NOT NULL"
                )));
            }
            let field_type =
                validate_materialized_view_log_column_type_for_modify_column(op, &mlog, &mlog_column.read(), new)?;
            if mlog_column.read().field_type != field_type {
                mlog_column.write().field_type = field_type;
                related.add_changed((schema_id, mlog));
            }
        }
    }
    for mview_id in mview_ids {
        let (schema_id, mview) = related.load(mview_id, "dependent materialized view")?;
        let usage = validate_dependent_materialized_view_modify_column(op, base, &mview, old, new)?;
        for offset in usage.direct_output_offsets {
            let Some(slot) = mview.columns.get(offset) else {
                return Err(invalid_ddl_job("modify column: materialized view output schema mismatch"));
            };
            let mview_old = slot.read().clone();
            let mut mview_new = mview_old.clone();
            mview_new.field_type = field_type_for_mv_related_column(&mview_old, new);
            if mview_old.field_type == mview_new.field_type {
                continue;
            }
            *slot.write() = mview_new.clone();
            related.add_changed((schema_id, mview.clone()));
            // "Follow the MV's actual output column name, which may differ
            // from the base column."
            visit_materialized_view_related_tables(related, op, &mview, &mview_old, &mview_new)?;
        }
    }
    Ok(())
}

/// Go master `mvColumnUsage`.
#[derive(Default)]
struct MvColumnUsage {
    direct_output_offsets: Vec<usize>,
    is_group_key: bool,
    unsupported_reason: &'static str,
}

/// Go master `validateDependentMaterializedViewModifyColumn`.
fn validate_dependent_materialized_view_modify_column(
    op: &str,
    base: &TableInfo,
    mview: &TableInfo,
    old: &tidb_model::ColumnInfo,
    new: &tidb_model::ColumnInfo,
) -> Result<MvColumnUsage, tidb_terror::MysqlError> {
    let Some(info) = mview.materialized_view.as_ref().map(|info| info.read().clone()).filter(|info| !info.sql_content.is_empty())
    else {
        return Err(invalid_ddl_job("modify column: invalid materialized view metadata"));
    };
    if info.base_table_ids.len() != 1 || info.base_table_ids.iter().next() != Some(&base.id) {
        return Err(invalid_ddl_job("modify column: dependent materialized view base table mismatch"));
    }
    let select = parse_select_from_sql(&info.sql_content)?;
    let usage = analyze_mv_column_usage(&select, old.name.lowercase());
    if !usage.unsupported_reason.is_empty() {
        return Err(unsupported(format!(
            "{op} on base table with materialized view dependencies does not support modifying columns used in {}",
            usage.unsupported_reason
        )));
    }
    if usage.direct_output_offsets.is_empty() {
        if usage.is_group_key {
            return Err(unsupported(format!(
                "{op} on base table with materialized view dependencies does not support modifying columns used only as group keys"
            )));
        }
        return Ok(usage);
    }
    if mview.columns.len() != select.fields.fields().len() {
        return Err(invalid_ddl_job("modify column: materialized view output schema mismatch"));
    }
    let not_null = |column: &tidb_model::ColumnInfo| column.field_type.has_flag(FieldTypeFlags::NOT_NULL);
    if usage.is_group_key
        && (old.field_type.charset() != new.field_type.charset()
            || old.field_type.collation_name() != new.field_type.collation_name()
            || not_null(old) != not_null(new))
    {
        return Err(unsupported(format!(
            "{op} on base table with materialized view dependencies does not support changing charset/collation/nullability of group keys"
        )));
    }
    for &offset in &usage.direct_output_offsets {
        let mview_old = mview.columns.get(offset).expect("checked the output count").read().clone();
        let mut mview_new = mview_old.clone();
        mview_new.field_type = field_type_for_mv_related_column(&mview_old, new);
        if !tidb_ddl_build::modify_column_change::is_meta_only_change(mview, &mview_old, &mview_new) {
            return Err(unsupported(format!(
                "{op} on base table with materialized view dependencies only supports no-reorg compatible type changes for direct output columns"
            )));
        }
    }
    Ok(usage)
}

/// Go master `ddl.parseSelectFromSQL`: `parser.New().ParseSQL(sql)`, one
/// SELECT.
fn parse_select_from_sql(sql: &str) -> Result<tidb_ast::SelectStmt, tidb_terror::MysqlError> {
    let statements = tidb_parser::parse_multi(sql).map_err(|error| tidb_terror::MysqlError::unknown(error.compatibility_message(sql)))?;
    let [statement] = statements.as_slice() else {
        return Err(invalid_ddl_job("modify column: invalid materialized view select sql"));
    };
    match statement {
        tidb_ast::Stmt::Query(query) => match &**query {
            tidb_ast::QueryStmt::Select(select) => Ok((**select).clone()),
            _ => Err(invalid_ddl_job("modify column: materialized view definition is not a SELECT statement")),
        },
        _ => Err(invalid_ddl_job("modify column: materialized view definition is not a SELECT statement")),
    }
}

/// A column reference's name (Go `ColumnNameExpr.Name.Name.L`).
fn column_ref_name(expr: &tidb_ast::Expr) -> Option<String> {
    match expr {
        tidb_ast::Expr::Column(path) => path.last().map(|name| name.go_to_lower()),
        _ => None,
    }
}

/// Go master `analyzeMVColumnUsage`.
fn analyze_mv_column_usage(select: &tidb_ast::SelectStmt, column: &str) -> MvColumnUsage {
    let mut usage = MvColumnUsage::default();
    let mut direct_output = std::collections::HashSet::new();
    let mut alias_to_offset: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for (i, field) in select.fields.fields().iter().enumerate() {
        let expr = match field {
            tidb_ast::SelectField::Wildcard(_) => {
                usage.unsupported_reason = "SELECT *";
                return usage;
            }
            tidb_ast::SelectField::Expr { expr, alias } => {
                if let Some(alias) = alias.as_deref().filter(|alias| !alias.is_empty()) {
                    alias_to_offset.entry(alias.go_to_lower()).or_insert(i);
                }
                expr
            }
        };
        if column_ref_name(expr).as_deref() == Some(column) {
            usage.direct_output_offsets.push(i);
            direct_output.insert(i);
            continue;
        }
        if expr_contains_column_ref(expr, column) {
            usage.unsupported_reason = "non-direct SELECT expressions";
            return usage;
        }
    }
    if select.where_clause.as_ref().is_some_and(|expr| expr_contains_column_ref(expr, column)) {
        usage.unsupported_reason = "WHERE clause";
        return usage;
    }
    if select.having.as_ref().is_some_and(|expr| expr_contains_column_ref(expr, column)) {
        usage.unsupported_reason = "HAVING clause";
        return usage;
    }
    if select.order_by.iter().any(|item| expr_contains_column_ref(&item.expr, column)) {
        usage.unsupported_reason = "ORDER BY clause";
        return usage;
    }
    for item in &select.group_by {
        // Go `*ast.PositionExpr` with `N > 0`.
        if let tidb_ast::Expr::Int(digits) = &item.expr {
            if let Ok(position) = digits.parse::<usize>() {
                if position > 0 && direct_output.contains(&(position - 1)) {
                    usage.is_group_key = true;
                    continue;
                }
            }
        }
        if column_ref_name(&item.expr).as_deref() == Some(column) {
            usage.is_group_key = true;
            continue;
        }
        if let tidb_ast::Expr::Column(path) = &item.expr {
            if let [name] = path.as_slice() {
                if alias_to_offset.get(&name.go_to_lower()).is_some_and(|offset| direct_output.contains(offset)) {
                    usage.is_group_key = true;
                    continue;
                }
            }
        }
        if expr_contains_column_ref(&item.expr, column) {
            usage.unsupported_reason = "GROUP BY expressions";
            return usage;
        }
    }
    usage
}

/// Go master `exprContainsColumnRef` (`columnNameExprFinder`).
fn expr_contains_column_ref(expr: &tidb_ast::Expr, column: &str) -> bool {
    struct Finder<'a> {
        column: &'a str,
        found: bool,
    }
    impl tidb_ast::Visitor for Finder<'_> {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if self.found {
                return true;
            }
            if node.downcast_ref::<tidb_ast::Expr>().and_then(column_ref_name).as_deref() == Some(self.column) {
                self.found = true;
                return true;
            }
            false
        }
        fn leave(&mut self, _: &mut dyn std::any::Any) -> bool {
            !self.found
        }
    }
    let mut finder = Finder { column, found: false };
    tidb_ast::Visitable::accept(&mut expr.clone(), &mut finder);
    finder.found
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
    if tracked && tidb_ddl_build::modify_column_change::is_null_to_not_null_change(old, new) {
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
pub(super) fn get_table_info(txn: &mut JobTxn<'_>, table_id: i64, schema_id: i64) -> Result<TableInfo, DdlPlanError> {
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
        Err(error) => Err(job_meta_error(&meta, error)),
    }
}

/// Go `getTableInfoAndCancelNonExistJob`: the table, or Go's not-exists
/// error with the job cancelled.
pub(super) fn get_table_info_and_cancel_non_exist_job(
    txn: &mut JobTxn<'_>,
    job: &mut Job,
    table_id: i64,
    schema_id: i64,
) -> Result<TableInfo, DdlPlanError> {
    get_table_info(txn, table_id, schema_id).inspect_err(|error| {
        if is_not_exists(error) {
            job.state = JobState::CANCELLED;
        }
    })
}

fn is_not_exists(error: &DdlPlanError) -> bool {
    error.equal(&tidb_infoschema::infoschema_errors::ERR_DATABASE_NOT_EXISTS)
        || error.equal(&tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS)
}

/// Go `updateTable(t, schemaID, tblInfo, true)`.
fn update_table(txn: &mut JobTxn<'_>, schema_id: i64, table: &mut TableInfo, start_ts: u64) -> Result<(), DdlPlanError> {
    table.update_ts = start_ts;
    let meta = txn.meta();
    meta.update_table(schema_id, table).map_err(|error| job_meta_error(&meta, error))
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
            "create materialized view log: required system table mysql.tidb_mlog_purge_info does not exist".into(),
        ]));
    }
    DdlPlanError::from_mysql_error(error)
}

fn execute_job_sql(txn: &JobTxn<'_>, sql: &str, label: &str) -> Result<(), tidb_terror::MysqlError> {
    txn.execute(sql, label, &[]).map(|_| ())
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
    txn.set_must_rollback_txn_on_error();
    let mut result = execute_job_sql(txn, &sql, "mlog-purge-info-upsert");
    if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/mockInsertMLogPurgeTableNotExists").is_some_and(|value| value.bool()) {
        result = Err(system_table_not_exists("tidb_mlog_purge_info"));
    }
    result.map_err(convert_purge_info_table_not_exists)
}

/// Go `deleteMaterializedViewLogPurgeInfos`: a missing table deletes nothing.
pub(crate) fn delete_materialized_view_log_purge_info(txn: &JobTxn<'_>, mlog_id: i64) -> Result<(), DdlPlanError> {
    let sql = format!("DELETE FROM mysql.tidb_mlog_purge_info WHERE MLOG_ID IN ({mlog_id})");
    txn.set_must_rollback_txn_on_error();
    let mut result = execute_job_sql(txn, &sql, "mlog-purge-info-delete");
    if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/mockDeleteMaterializedViewLogPurgeInfoTableNotExists")
        .is_some_and(|value| value.bool())
    {
        result = Err(system_table_not_exists("tidb_mlog_purge_info"));
    }
    if let Some(value) = tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/mockDeleteMaterializedViewLogPurgeInfoErr") {
        result = Err(tidb_terror::MysqlError::unknown(value.string()));
    }
    match result {
        Err(error) if error.equal(&tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS) => Ok(()),
        other => other.map_err(DdlPlanError::from_mysql_error),
    }
}

/// Go `(*worker).onCreateMaterializedViewLog`.
pub(super) fn on_create_materialized_view_log(
    txn: &mut JobTxn<'_>,
    catalog: &SessionExtendedInfoSchema,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
    worker: &DdlStepWorker,
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
        // Go: the DDL session holds the view's refresh advisory lock for the
        // rest of the job's delivery, so no refresh writes the view without
        // the new log; the scheduler releases it after the delivery.
        let lock_name = tidb_mviewutil::refresh_advisory_lock_name(schema_id, log_info.base_table_id);
        match worker.get_advisory_lock(&lock_name, 1) {
            Ok(()) => {}
            Err(tidb_sessionctx::advisory_lock_state::AdvisoryLockError::Timeout) => {
                // Do not retry this job while retaining its DDL scheduling
                // claim: an OOP refresh holds the lock while waiting for its
                // cutover DDL.
                return Err(cancel_ddl_job(
                    active,
                    DdlPlanError::from(tidb_exeerrors::ERR_MV_ADVISORY_LOCK_CONFLICT.fast_gen_by_args(&[format!(
                        "cannot create materialized view log on {schema_name}.{}: materialized view refresh is running, please retry later",
                        base.name.original()
                    )
                    .into()])),
                ));
            }
            Err(error) => return Err(DdlPlanError::plain_refusal(error.to_string())),
        }
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
    append_schema_change_mutations(txn, active.job.id, &[(-1, SchemaChangeEvent::create_table(mlog.clone_like_go()))])?;
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
    Ok(PersistedDdlActionStep { write, update_raw_args: true, run_error: None, reorg: None })
}

/// Go master `(*worker).onCreateMaterializedViewShadow`: the protected
/// shadow table an OUT OF PLACE refresh builds into, created as Go's
/// `createTable` does, with no table-created event.
pub(super) fn on_create_materialized_view_shadow(
    txn: &mut JobTxn<'_>,
    catalog: &SessionExtendedInfoSchema,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let invalid = |active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob, message: &str| {
        cancel_ddl_job(
            active,
            DdlPlanError::from(tidb_dbterror::ERR_INVALID_DDL_JOB.gen_with_stack_by_args(&[message.to_owned().into()])),
        )
    };
    let args = tidb_model::get_create_table_args(&mut active.job)
        .map_err(|error| cancel_ddl_job(active, DdlPlanError::Encode(error.to_string())))?;
    let (table_handle, fk_check) = match args {
        Some(args) => {
            let args = args.read();
            (args.table_info.get(), args.fk_check.get())
        }
        None => (None, false),
    };
    let Some(table_handle) = table_handle else {
        return Err(invalid(active, "create materialized view shadow table: invalid shadow metadata"));
    };
    let mut shadow = table_handle.read().clone();
    let Some(source_mview_id) = shadow.materialized_view_shadow.as_ref().map(|info| info.read().source_mview_id) else {
        return Err(invalid(active, "create materialized view shadow table: invalid shadow metadata"));
    };
    if shadow.materialized_view.is_some()
        || shadow.materialized_view_log.is_some()
        || shadow.view.is_some()
        || shadow.sequence.is_some()
    {
        return Err(invalid(active, "create materialized view shadow table: shadow table must be a protected physical table"));
    }
    if source_mview_id == 0 {
        return Err(invalid(active, "create materialized view shadow table: invalid source materialized view id"));
    }
    let schema_id = active.job.schema_id;
    let source = match get_table_info(txn, source_mview_id, schema_id) {
        Ok(source) => source,
        Err(error) if is_not_exists(&error) => return Err(cancel_ddl_job(active, error)),
        Err(error) => return Err(error),
    };
    if source.materialized_view.is_none() {
        return Err(cancel_ddl_job(
            active,
            DdlPlanError::from(tidb_dbterror::ERR_WRONG_OBJECT.gen_with_stack_by_args(&[
                active.job.schema_name.to_string().into(),
                source.name.original().to_owned().into(),
                "MATERIALIZED VIEW".into(),
            ])),
        ));
    }
    if source.state != SchemaState::PUBLIC {
        let state = source.state;
        return Err(cancel_ddl_job(
            active,
            DdlPlanError::from(tidb_dbterror::ERR_INVALID_DDL_STATE.gen_with_stack_by_args(&[
                FormatArg::from("table"),
                FormatArg::new(state.to_string(), state.0.to_string(), "model.SchemaState"),
            ])),
        ));
    }
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
    create_table::create_table(
        txn,
        catalog,
        &database,
        active,
        &mut shadow,
        fk_check,
        start_ts,
        &mut placement_bundles,
    )?;
    let (schema_version, diff) = update_schema_version(
        &txn.meta(),
        active,
        SchemaDiff { action_type: active.job.type_, schema_id, table_id: shadow.id, ..SchemaDiff::default() },
    )?;
    *table_handle.write() = shadow.clone_like_go();
    active.job.finish_table_job(
        JobState::DONE,
        SchemaState::PUBLIC,
        schema_version,
        Some(GoShared::new(shadow.clone_like_go())),
    );
    let mut write = job_only_write(active.job.id, Vec::new());
    write.schema_version = schema_version;
    write.diff = diff;
    write.created_id = Some(shadow.id);
    write.placement_bundles = placement_bundles;
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
    let extra = update_materialized_view_base_info_on_drop(txn, &active.job, &dropping)?;
    for (extra_schema, table) in &extra {
        update_table(txn, *extra_schema, &mut table.clone(), start_ts)?;
    }
    if actual.is_some() {
        let meta = txn.meta();
        meta.drop_table_or_view(schema_id, table_id).map_err(|error| job_meta_error(&meta, error))?;
        meta.auto_ids(schema_id, table_id).delete().map_err(|error| job_meta_error(&meta, error))?;
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
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
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
    append_schema_change_mutations(txn, active.job.id, &[(
            -1,
            SchemaChangeEvent::alter_materialized_view_log_purge(table.clone_like_go(), old_table),
        )])?;
    active.job.finish_table_job(JobState::DONE, SchemaState::PUBLIC, schema_version, Some(GoShared::new(table)));
    write.mutations.extend(mutations);
    Ok(super::add_index::action_step(write))
}

/// Go master `formatSQLMode`: every mode name the bits touch, sorted.
pub fn format_sql_mode(mode: i64) -> String {
    let mut names: Vec<&str> = tidb_mysql::consts::SQL_MODE_NAMES
        .iter()
        .filter(|(_, bits)| mode & bits.0 != 0)
        .map(|(name, _)| *name)
        .collect();
    names.sort_unstable();
    names.join(",")
}

/// Go master `buildCreateMaterializedViewImportSQL`.
pub(crate) fn build_create_materialized_view_import_sql(
    schema_name: &str,
    mview: &TableInfo,
    thread_cnt: i64,
    disk_quota: &str,
) -> Result<String, tidb_terror::MysqlError> {
    let sql_content = mview_sql_content(mview)?;
    let prefix = tidb_sqlescape::escape_sql_text(
        "IMPORT INTO %n.%n FROM ",
        &[
            tidb_sqlescape::SqlArg::String(schema_name.as_bytes()),
            tidb_sqlescape::SqlArg::String(mview.name.original().as_bytes()),
        ],
    )
    .map_err(|error| tidb_terror::MysqlError::unknown(error.to_string()))?;
    let options = tidb_ddl_build::materialized_view::build_mview_import_into_options(thread_cnt, disk_quota)?;
    Ok(format!("{prefix}({sql_content}) WITH {}", options.join(", ")))
}

/// Go master `buildCreateMaterializedViewInsertSQL`.
pub(crate) fn build_create_materialized_view_insert_sql(
    schema_name: &str,
    mview: &TableInfo,
) -> Result<String, tidb_terror::MysqlError> {
    let sql_content = mview_sql_content(mview)?;
    let prefix = tidb_sqlescape::escape_sql_text(
        "REPLACE INTO %n.%n ",
        &[
            tidb_sqlescape::SqlArg::String(schema_name.as_bytes()),
            tidb_sqlescape::SqlArg::String(mview.name.original().as_bytes()),
        ],
    )
    .map_err(|error| tidb_terror::MysqlError::unknown(error.to_string()))?;
    Ok(format!("{prefix}{sql_content}"))
}

fn mview_sql_content(mview: &TableInfo) -> Result<String, tidb_terror::MysqlError> {
    mview
        .materialized_view
        .as_ref()
        .map(|info| info.read().sql_content.clone())
        .filter(|content| !content.is_empty())
        .ok_or_else(|| invalid_ddl_job("create materialized view: invalid select sql"))
}

/// What a CREATE MATERIALIZED VIEW job's initial build needs (Go master
/// `buildCreateMaterializedViewData`'s arguments).
#[derive(Clone, Debug)]
pub struct MViewBuild {
    /// The job's ID, which the build logs.
    pub job_id: i64,
    /// The job's schema name.
    pub schema_name: String,
    /// The view, as the job created it.
    pub mview: Box<TableInfo>,
    /// The job's session variables (Go `MViewExecutionSessionVarsFromJob`).
    pub job_session_vars: Vec<(String, String)>,
    /// The job's resource group (Go `job.ReorgMeta.ResourceGroupName`).
    pub resource_group: String,
}

fn ddl_session_sql<C: tidb_ddl_session::SessionContext + ?Sized>(
    session: &C,
    sql: &str,
    args: &[tidb_sqlescape::SqlArg<'_>],
) -> Result<Vec<Vec<tidb_datatype::Datum>>, tidb_terror::MysqlError> {
    let context = tidb_sqlexec::BackgroundContext;
    let record_set = session
        .execute_internal(&context, tidb_ddl_session::INTERNAL_TXN_DDL, sql, args)
        .map_err(|error| tidb_terror::MysqlError::unknown(error.to_string()))?;
    match record_set {
        Some(mut record_set) => tidb_sqlexec::drain_record_set(&context, record_set.as_mut(), 8)
            .map_err(|error| tidb_terror::MysqlError::unknown(error.to_string())),
        None => Ok(Vec::new()),
    }
}

/// Go master `maybeMockMViewExecutionSessionVarApplyError`: the injected
/// failure to set `var_name`.
pub fn maybe_mock_mview_execution_session_var_apply_error(var_name: &str) -> Option<String> {
    let value = tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/mockMViewExecutionSessionVarApplyError")?;
    (value.as_string().as_deref() == Some(var_name)).then(|| format!("mock mv execution session var apply error: {var_name}"))
}

/// Go master `ApplyMViewExecutionSessionVars`.
///
/// # Errors
///
/// A variable failed to apply; the session is restored.
pub fn apply_m_view_execution_session_vars(
    vars: &mut tidb_variable::SessionVars,
    target: &tidb_variable::mview_execution::MViewExecutionSessionVars,
) -> Result<tidb_variable::mview_execution::MViewExecutionSessionVarsRestore, tidb_terror::MysqlError> {
    apply_m_view_execution_session_vars_inner(vars, target, false)
}

/// Go master `ApplyMViewExecutionSessionVarsBestEffort`: a setting this
/// server cannot apply keeps the session's value.
///
/// # Errors
///
/// Never in practice: every failure falls back to the session's value.
pub fn apply_m_view_execution_session_vars_best_effort(
    vars: &mut tidb_variable::SessionVars,
    target: &tidb_variable::mview_execution::MViewExecutionSessionVars,
) -> Result<tidb_variable::mview_execution::MViewExecutionSessionVarsRestore, tidb_terror::MysqlError> {
    apply_m_view_execution_session_vars_inner(vars, target, true)
}

/// Go master `applyMViewExecutionSessionVars`.
fn apply_m_view_execution_session_vars_inner(
    vars: &mut tidb_variable::SessionVars,
    target: &tidb_variable::mview_execution::MViewExecutionSessionVars,
    best_effort: bool,
) -> Result<tidb_variable::mview_execution::MViewExecutionSessionVarsRestore, tidb_terror::MysqlError> {
    use tidb_variable::mview_execution as mv;
    mv::apply_m_view_execution_session_vars_with_config(
        vars,
        target,
        mv::MViewExecutionSessionVarsApplyConfig {
            maintain_mem_quota_var_name: tidb_vardef::tidb_vars::TIDB_MEM_QUOTA_QUERY,
            maintain_isolation_read_engines_var_name: tidb_vardef::tidb_vars::TIDB_ISOLATION_READ_ENGINES,
            capture_applied_vars: Some(mv::capture_applied_m_view_execution_session_vars),
            best_effort,
            inject_apply_error: Some(|name| {
                maybe_mock_mview_execution_session_var_apply_error(name).map(tidb_terror::MysqlError::unknown)
            }),
            on_apply_error: Some(|name, value, error| {
                tidb_ddl_logutil::ddl_logger().warn(
                    "mv execution: failed to apply session var, fallback to current session value",
                    &[
                        tidb_log::Field::new("var", tidb_log::Value::Str(name.to_owned())),
                        tidb_log::Field::new("value", tidb_log::Value::Str(value.to_owned())),
                        tidb_log::Field::new("error", tidb_log::Value::Str(error.to_string())),
                    ],
                );
            }),
            on_restore_error: Some(|name, origin, current, error| {
                tidb_ddl_logutil::ddl_logger().warn(
                    "mv execution: failed to restore session var",
                    &[
                        tidb_log::Field::new("var", tidb_log::Value::Str(name.to_owned())),
                        tidb_log::Field::new("origin", tidb_log::Value::Str(origin.to_owned())),
                        tidb_log::Field::new("current", tidb_log::Value::Str(current.to_owned())),
                        tidb_log::Field::new("error", tidb_log::Value::Str(error.to_string())),
                    ],
                );
            }),
        },
    )
}

/// The restore closure Go master `InitMViewSession` answers.
#[must_use = "the session keeps the view's definition settings until it is restored"]
pub struct MViewSessionRestore {
    original_db: String,
    original_resource_group: String,
    orig_enable_mview: bool,
    orig_mview_maintenance: bool,
    original_vars: Vec<(&'static str, String)>,
}

impl MViewSessionRestore {
    /// Runs the restore closure.
    pub fn restore(self, vars: &mut tidb_variable::SessionVars) {
        vars.current_db = self.original_db;
        vars.resource_group = self.original_resource_group;
        let enable = if self.orig_enable_mview { "ON" } else { "OFF" };
        if let Err(error) = vars.set_system_var(tidb_vardef::tidb_vars::TIDB_MVIEW_ENABLE, enable.to_owned()) {
            tidb_ddl_logutil::ddl_logger().warn(
                "materialized view: failed to restore materialized view enablement",
                &[tidb_log::Field::new("error", tidb_log::Value::Str(error.to_string()))],
            );
        }
        for (name, value) in self.original_vars {
            if let Err(error) = vars.set_system_var(name, value) {
                tidb_ddl_logutil::ddl_logger().warn(
                    "materialized view: failed to restore session variable",
                    &[
                        tidb_log::Field::new("name", tidb_log::Value::Str(name.to_owned())),
                        tidb_log::Field::new("error", tidb_log::Value::Str(error.to_string())),
                    ],
                );
            }
        }
        vars.in_mview_maintenance = self.orig_mview_maintenance;
    }
}

/// Go master `InitMViewSession`: materialized views on, the definition's SQL
/// mode, time zone and division precision, the view's schema, the
/// maintenance resource group and maintenance mode, answered with the
/// restore closure. (Go also sets the current statement context's time
/// zone, type flags and error levels; this port derives a statement's
/// context from these variables when the statement starts.)
///
/// # Errors
///
/// The definition time zone is invalid or a variable cannot be set; the
/// session is restored.
pub fn init_m_view_session(
    vars: &mut tidb_variable::SessionVars,
    mview_info: &tidb_model::table::MaterializedViewInfo,
    current_db: &str,
    resource_group: &str,
) -> Result<MViewSessionRestore, tidb_terror::MysqlError> {
    mview_info
        .definition_time_zone
        .get_location()
        .map_err(|error| tidb_terror::MysqlError::unknown(error).annotate("materialized view: invalid definition timezone"))?;
    let mut restore = MViewSessionRestore {
        original_db: vars.current_db.clone(),
        original_resource_group: vars.resource_group.clone(),
        orig_enable_mview: vars.enable_mview(),
        orig_mview_maintenance: vars.in_mview_maintenance,
        original_vars: Vec::with_capacity(3),
    };
    vars.set_system_var(tidb_vardef::tidb_vars::TIDB_MVIEW_ENABLE, "on".to_owned())
        .map_err(|error| error.annotate("materialized view: failed to enable materialized view support"))?;
    for (name, value) in [
        ("sql_mode", format_sql_mode(mview_info.definition_sql_mode)),
        ("time_zone", definition_time_zone_value(&mview_info.definition_time_zone)),
        ("div_precision_increment", mview_info.definition_div_precision_increment.to_string()),
    ] {
        match vars.set_system_var_with_old_state_as_ret(name, value) {
            Ok(old) => restore.original_vars.push((name, old)),
            Err(error) => {
                restore.restore(vars);
                return Err(error.annotate(&format!("materialized view: failed to set {name}")));
            }
        }
    }
    vars.current_db = current_db.to_owned();
    if !resource_group.is_empty() {
        vars.resource_group = resource_group.to_owned();
    }
    vars.in_mview_maintenance = true;
    Ok(restore)
}

/// Go master `MViewExecutionSessionVarsFromJob`: the session's applied
/// execution settings, overridden by those the job carries.
///
/// # Errors
///
/// The job's `tiflash_query_spill_ratio` is not a number.
pub fn m_view_execution_session_vars_from_job(
    job_vars: &[(String, String)],
    default_vars: &tidb_variable::SessionVars,
) -> Result<tidb_variable::mview_execution::MViewExecutionSessionVars, tidb_terror::MysqlError> {
    use tidb_vardef::tidb_vars;
    use tidb_variable::varsutil::tidb_opt_int64;
    let mut target = tidb_variable::mview_execution::capture_applied_m_view_execution_session_vars(default_vars);
    let get = |name: &str| job_vars.iter().find(|(var, _)| var == name).map(|(_, value)| value.as_str());
    if let Some(val) = get(tidb_vars::TIDB_MVIEW_MAINTAIN_MEM_QUOTA) {
        target.maintain_mem_quota = tidb_opt_int64(val, target.maintain_mem_quota);
    }
    if let Some(val) = get(tidb_vars::TIDB_MVIEW_MAINTAIN_ISOLATION_READ_ENGINES) {
        val.clone_into(&mut target.isolation_read_engines);
    }
    if let Some(val) = get(tidb_vars::TIDB_MAX_TIFLASH_THREADS) {
        target.tiflash_max_threads = tidb_opt_int64(val, target.tiflash_max_threads);
    }
    if let Some(val) = get(tidb_vars::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_JOIN) {
        target.tiflash_max_bytes_before_ext_join = tidb_opt_int64(val, target.tiflash_max_bytes_before_ext_join);
    }
    if let Some(val) = get(tidb_vars::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_GROUP_BY) {
        target.tiflash_max_bytes_before_ext_agg = tidb_opt_int64(val, target.tiflash_max_bytes_before_ext_agg);
    }
    if let Some(val) = get(tidb_vars::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_SORT) {
        target.tiflash_max_bytes_before_ext_sort = tidb_opt_int64(val, target.tiflash_max_bytes_before_ext_sort);
    }
    if let Some(val) = get(tidb_vars::TIFLASH_MEM_QUOTA_QUERY_PER_NODE) {
        target.tiflash_mem_quota_query_per_node = tidb_opt_int64(val, target.tiflash_mem_quota_query_per_node);
    }
    if let Some(val) = get(tidb_vars::TIFLASH_QUERY_SPILL_RATIO) {
        target.tiflash_query_spill_ratio = val.parse::<f64>().map_err(|error| {
            tidb_terror::MysqlError::unknown(error.to_string())
                .annotate(&format!("invalid {}", tidb_vars::TIFLASH_QUERY_SPILL_RATIO))
        })?;
    }
    if let Some(val) = get(tidb_vars::TIFLASH_FINE_GRAINED_SHUFFLE_STREAM_COUNT) {
        target.fine_grained_stream_count = tidb_opt_int64(val, target.fine_grained_stream_count);
    }
    if let Some(val) = get(tidb_vars::TIFLASH_FINE_GRAINED_SHUFFLE_BATCH_SIZE) {
        target.fine_grained_batch_size =
            tidb_opt_int64(val, target.fine_grained_batch_size as i64) as u64;
    }
    if let Some(val) = get(tidb_vars::TIDB_MVIEW_MAINTAIN_IMPORT_THREADS) {
        target.import_threads = tidb_variable::varsutil::tidb_opt_int(val, target.import_threads);
    }
    if let Some(val) = get(tidb_vars::TIDB_MVIEW_MAINTAIN_IMPORT_DISK_QUOTA) {
        val.clone_into(&mut target.import_disk_quota);
    }
    if let Some(val) = get(tidb_vars::TIDB_TRACK_AGGREGATE_MEMORY_USAGE) {
        target.track_aggregate_memory_usage = tidb_vardef::option_values::tidb_opt_on(val);
    }
    if let Some(val) = get(tidb_vars::TIDB_ENABLE_PARALLEL_HASHAGG_SPILL) {
        target.enable_parallel_hashagg_spill = tidb_vardef::option_values::tidb_opt_on(val);
    }
    Ok(target)
}

/// The restore closure Go master `initCreateMaterializedViewBuildSession`
/// answers: the view's settings, then the execution ones.
#[must_use = "the build session keeps the view's settings until it is restored"]
pub struct CreateMViewBuildSessionRestore {
    execution: tidb_variable::mview_execution::MViewExecutionSessionVarsRestore,
    mview: MViewSessionRestore,
}

impl CreateMViewBuildSessionRestore {
    /// Runs the restore closure.
    pub fn restore(self, vars: &mut tidb_variable::SessionVars) {
        self.mview.restore(vars);
        self.execution.restore(vars);
    }
}

/// Go master `initCreateMaterializedViewBuildSession`: the job's execution
/// settings (`ApplyMViewExecutionSessionVars`) and the view's definition
/// settings (`InitMViewSession`) on the build session's variables.
///
/// # Errors
///
/// The view has no metadata, a setting failed to apply, or the definition
/// is invalid; the session is restored.
pub fn init_create_materialized_view_build_session(
    vars: &mut tidb_variable::SessionVars,
    build: &MViewBuild,
    current_db: &str,
) -> Result<CreateMViewBuildSessionRestore, tidb_terror::MysqlError> {
    let info = build
        .mview
        .materialized_view
        .as_ref()
        .ok_or_else(|| invalid_ddl_job("create materialized view: invalid materialized view metadata"))?
        .read()
        .clone();
    let target = m_view_execution_session_vars_from_job(&build.job_session_vars, vars)?;
    let execution = apply_m_view_execution_session_vars(vars, &target)?;
    let mview = match init_m_view_session(vars, &info, current_db, &build.resource_group) {
        Ok(restore) => restore,
        Err(error) => {
            execution.restore(vars);
            return Err(error);
        }
    };
    let applied = tidb_variable::mview_execution::capture_applied_m_view_execution_session_vars(vars);
    const DDL_FP: &str = "github.com/pingcap/tidb/pkg/ddl/";
    tidb_failpoint::inject_call(&format!("{DDL_FP}createMaterializedViewBuildMaintainMemQuotaApplied"), &mut applied.maintain_mem_quota.clone());
    tidb_failpoint::inject_call(
        &format!("{DDL_FP}createMaterializedViewBuildTiFlashSessionVarsApplied"),
        &mut (applied.tiflash_max_threads, applied.fine_grained_stream_count, applied.fine_grained_batch_size),
    );
    tidb_failpoint::inject_call(
        &format!("{DDL_FP}createMaterializedViewBuildTiFlashSpillSessionVarsApplied"),
        &mut (
            applied.tiflash_max_bytes_before_ext_join,
            applied.tiflash_max_bytes_before_ext_agg,
            applied.tiflash_max_bytes_before_ext_sort,
            applied.tiflash_mem_quota_query_per_node,
            applied.tiflash_query_spill_ratio,
        ),
    );
    tidb_failpoint::inject_call(
        &format!("{DDL_FP}createMaterializedViewBuildImportSessionVarsApplied"),
        &mut (applied.import_threads, applied.import_disk_quota.clone()),
    );
    Ok(CreateMViewBuildSessionRestore { execution, mview })
}

/// Go `TimeZoneLocation.GetLocation` as a `time_zone` value: a fixed offset,
/// else the zone's name, where an empty name loads as UTC
/// (`time.LoadLocation("")`).
pub fn definition_time_zone_value(zone: &tidb_model::TimeZoneLocation) -> String {
    let name = zone.name.to_string();
    // Go `GetLocation`: `time.LoadLocation(Name)` without an offset (whose
    // `String()` is the name, `UTC` for the empty one), else
    // `time.FixedZone(Name, Offset)`; then `timeutil.ZoneName`.
    if zone.offset == 0 {
        return if name.is_empty() { "UTC".to_owned() } else { name };
    }
    tidb_timeutil::zone_name(&tidb_timeutil::TimeZone::Fixed {
        name,
        offset_secs: i32::try_from(zone.offset).unwrap_or_default(),
    })
}

/// Go master `buildCreateMaterializedViewData` by INSERT (Go's non-TiKV
/// store path; IMPORT INTO is its TiKV path, which this node does not run):
/// `REPLACE INTO schema.view <definition>` on a pooled session set up as the
/// view's maintenance session, answered with the statement's read TS.
///
/// # Errors
///
/// The session could not be set up, the build failed, or its read TS is
/// unknown.
pub fn build_create_materialized_view_data(
    sess_pool: &dyn tidb_ddl_build::session_pool::DdlSessionPool,
    build: &MViewBuild,
) -> Result<u64, tidb_terror::MysqlError> {
    tidb_failpoint::inject("github.com/pingcap/tidb/pkg/ddl/pauseCreateMaterializedViewBuild", |_| {});
    if let Some(value) = tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewBuildErr") {
        // Go returns `context.Canceled` for "context-canceled".
        if value.as_string().as_deref() == Some("context-canceled") {
            return Err(tidb_terror::MysqlError::unknown("context canceled"));
        }
        return Err(tidb_terror::MysqlError::unknown("mock create materialized view build error"));
    }
    build
        .mview
        .materialized_view
        .as_ref()
        .filter(|info| !info.read().sql_content.is_empty())
        .ok_or_else(|| invalid_ddl_job("create materialized view: invalid select sql"))?;
    let pool = sess_pool
        .sess_pool()
        .ok_or_else(|| tidb_terror::MysqlError::unknown("the DDL session pool has no sessions"))?;
    let session = pool.get().map_err(|error| tidb_terror::MysqlError::unknown(error.to_string()))?;
    let result = (|| {
        // Go master `initCreateMaterializedViewBuildSession(sessCtx, job,
        // mviewTableInfo, schemaName)`.
        let mut init = None;
        if !session.with_session_vars(&mut |vars| {
            init = Some(init_create_materialized_view_build_session(vars, build, &build.schema_name));
        }) {
            return Err(tidb_terror::MysqlError::unknown("the DDL session has no session variables"));
        }
        let restore = init.expect("the session ran the initialization")?;
        let built = (|| {
            // Go master `buildCreateMaterializedViewData`: a TiKV store builds
            // by IMPORT INTO (`buildCreateMaterializedViewImportSQL`, with the
            // build session's import options), any other by REPLACE INTO
            // (`buildCreateMaterializedViewInsertSQL`).
            let store_name = sess_pool.store_name();
            let method = if store_name == "TiKV" { "import-into" } else { "insert-into" };
            tidb_ddl_logutil::ddl_logger().info(
                "create materialized view: choose init build method",
                &[
                    tidb_log::Field::new("jobID", tidb_log::Value::I64(build.job_id)),
                    tidb_log::Field::new("schema", tidb_log::Value::Str(build.schema_name.clone())),
                    tidb_log::Field::new("mview", tidb_log::Value::Str(build.mview.name.original().to_owned())),
                    tidb_log::Field::new("storeName", tidb_log::Value::Str(store_name.to_owned())),
                    tidb_log::Field::new("method", tidb_log::Value::Str(method.to_owned())),
                ],
            );
            let build_sql = if store_name == "TiKV" {
                let options = ddl_session_sql(
                    &*session,
                    "SELECT @@SESSION.tidb_mview_maintain_import_threads, @@SESSION.tidb_mview_maintain_import_disk_quota",
                    &[],
                )?;
                let option = |index: usize| {
                    options.first().and_then(|row| row.get(index)).map(datum_as_text).unwrap_or_default()
                };
                build_create_materialized_view_import_sql(
                    &build.schema_name,
                    &build.mview,
                    option(0).parse::<i64>().unwrap_or(0),
                    &option(1),
                )?
            } else {
                build_create_materialized_view_insert_sql(&build.schema_name, &build.mview)?
            };
            ddl_session_sql(&*session, &build_sql, &[])?;
            // Go `getCreateMaterializedViewBuildReadTS`.
            let rows = ddl_session_sql(
                &*session,
                "SELECT COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(@@tidb_last_query_info, '$.start_ts')) AS UNSIGNED), CAST(0 AS UNSIGNED))",
                &[],
            )?;
            let read_ts = match rows.first().and_then(|row| row.first()) {
                Some(tidb_datatype::Datum::UInt(ts)) => *ts,
                Some(tidb_datatype::Datum::Int(ts)) => u64::try_from(*ts).unwrap_or_default(),
                _ => return Err(invalid_ddl_job("create materialized view: cannot fetch build read tso")),
            };
            if read_ts == 0 {
                return Err(invalid_ddl_job("create materialized view: invalid build read tso"));
            }
            Ok(read_ts)
        })();
        let mut restore = Some(restore);
        session.with_session_vars(&mut |vars| {
            if let Some(restore) = restore.take() {
                restore.restore(vars);
            }
        });
        built
    })();
    pool.put(session);
    result
}

fn datum_as_text(datum: &tidb_datatype::Datum) -> String {
    match datum {
        tidb_datatype::Datum::Int(value) => value.to_string(),
        tidb_datatype::Datum::UInt(value) => value.to_string(),
        tidb_datatype::Datum::String(value) => String::from_utf8_lossy(value.bytes()).into_owned(),
        tidb_datatype::Datum::Bytes(value) => String::from_utf8_lossy(value).into_owned(),
        tidb_datatype::Datum::Real(value) => tidb_goruntime::go_strconv::format_float(*value, b'f', -1, 64),
        _ => String::new(),
    }
}

/// Go master `onCreateMaterializedViewBaseCheck`.
fn on_create_materialized_view_base_check(
    txn: &mut JobTxn<'_>,
    schema_id: i64,
    base_id: i64,
    schema_name: &str,
) -> Result<TableInfo, DdlPlanError> {
    let base = get_table_info(txn, base_id, schema_id)?;
    validate_materialized_view_source_state(schema_name, &base).map_err(DdlPlanError::from_mysql_error)?;
    validate_materialized_view_nesting_depth(
        &base,
        &mut |table_id| get_table_info(txn, table_id, schema_id),
        &|| DdlPlanError::from_mysql_error(materialized_view_nesting_depth_exceeded()),
    )?;
    let mlog_id = base.materialized_view_base.as_ref().map_or(0, |info| info.read().mlog_id);
    if mlog_id == 0 {
        return Err(DdlPlanError::from_mysql_error(invalid_ddl_job(
            "create materialized view: base table has no materialized view log",
        )));
    }
    let mlog = get_table_info(txn, mlog_id, schema_id)?;
    if let Err(error) = validate_materialized_view_source_mlog(schema_name, &base, &mlog) {
        return Err(DdlPlanError::from_mysql_error(tidb_terror::MysqlError::from_terror(
            &tidb_dbterror::ERR_INVALID_DDL_JOB.gen_with_stack(format!(
                "create materialized view: invalid materialized view log metadata: {}",
                error.message
            )),
        )));
    }
    Ok(base)
}

/// Go master `isCreateMaterializedViewBaseCheckCancelledErr`.
fn is_create_materialized_view_base_check_cancelled_err(error: &DdlPlanError) -> bool {
    error.equal(&tidb_infoschema::infoschema_errors::ERR_DATABASE_NOT_EXISTS)
        || error.equal(&tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS)
        || error.equal(&tidb_dbterror::ERR_INVALID_DDL_JOB)
        || error.equal(&tidb_dbterror::ERR_WRONG_OBJECT)
        || error.equal(&tidb_dbterror::ERR_INVALID_DDL_STATE)
        || error.equal(&tidb_dbterror::ERR_GENERAL_UNSUPPORTED_DDL)
}

/// Go master `updateMaterializedViewBaseInfoOnCreate` for a materialized
/// view: its base tables list it, its logs read for it. An error cancels the
/// job.
/// Go `job.State = model.JobStateCancelled` before returning `error`.
fn cancel_job(job: &mut Job, error: impl Into<DdlPlanError>) -> DdlPlanError {
    job.state = JobState::CANCELLED;
    error.into()
}

fn update_materialized_view_base_info_on_create(
    txn: &mut JobTxn<'_>,
    job: &mut Job,
    created: &TableInfo,
    mlog_ids: &[i64],
    start_ts: u64,
) -> Result<Vec<(i64, TableInfo)>, DdlPlanError> {
    let schema_id = job.schema_id;
    let base_ids: Vec<i64> = created
        .materialized_view
        .as_ref()
        .map(|info| info.read().base_table_ids.iter().copied().collect())
        .unwrap_or_default();
    if base_ids.is_empty() {
        return Err(cancel_job(
            job,
            DdlPlanError::plain_refusal("materialized view must reference at least one base table"),
        ));
    }
    let mut extra = Vec::new();
    let mut processed: Vec<i64> = Vec::new();
    for &base_id in &base_ids {
        if base_id == 0 {
            return Err(cancel_job(job, DdlPlanError::plain_refusal("materialized view base table id is invalid")));
        }
        if processed.contains(&base_id) {
            continue;
        }
        processed.push(base_id);
        let mut base = get_table_info(txn, base_id, schema_id).map_err(|error| cancel_job(&mut *job, error))?;
        {
            let info = base.materialized_view_base.get_or_insert_with(Default::default);
            let mut info = info.write();
            if !info.mview_ids.iter().any(|&id| id == created.id) {
                let mut ids: Vec<i64> = info.mview_ids.iter().copied().collect();
                ids.push(created.id);
                info.mview_ids = ids.into();
            }
        }
        update_table(txn, schema_id, &mut base, start_ts).map_err(|error| cancel_job(&mut *job, error))?;
        extra.push((schema_id, base));
    }
    let mut processed_mlogs: Vec<i64> = Vec::new();
    for &mlog_id in mlog_ids {
        if mlog_id == 0 {
            return Err(cancel_job(job, DdlPlanError::plain_refusal("materialized view log id is invalid")));
        }
        if processed_mlogs.contains(&mlog_id) {
            continue;
        }
        processed_mlogs.push(mlog_id);
        let mut mlog = get_table_info(txn, mlog_id, schema_id).map_err(|error| cancel_job(&mut *job, error))?;
        let Some(log) = mlog.materialized_view_log.clone() else {
            return Err(cancel_job(
                job,
                DdlPlanError::from_mysql_error(invalid_ddl_job("create materialized view: invalid materialized view log")),
            ));
        };
        if !base_ids.contains(&log.read().base_table_id) {
            return Err(cancel_job(
                job,
                DdlPlanError::from_mysql_error(invalid_ddl_job(
                    "create materialized view: materialized view log does not belong to a base table",
                )),
            ));
        }
        if log.read().dependent_mview_ids.iter().any(|&id| id == created.id) {
            continue;
        }
        let mut ids: Vec<i64> = log.read().dependent_mview_ids.iter().copied().collect();
        ids.push(created.id);
        log.write().dependent_mview_ids = ids.into();
        update_table(txn, schema_id, &mut mlog, start_ts).map_err(|error| cancel_job(&mut *job, error))?;
        extra.push((schema_id, mlog));
    }
    Ok(extra)
}

/// Go master `updateMaterializedViewBaseInfoOnDrop`: the base tables of a
/// dropped materialized view or log forget it, and so do the logs that read
/// for a dropped view.
pub(crate) fn update_materialized_view_base_info_on_drop(
    txn: &mut JobTxn<'_>,
    job: &Job,
    dropping_table: &TableInfo,
) -> Result<Vec<(i64, TableInfo)>, DdlPlanError> {
    let logger = tidb_ddl_logutil::ddl_logger;
    let i64_field = |key: &str, value: i64| tidb_log::Field::new(key, tidb_log::Value::I64(value));
    let base_table_ids: Vec<i64>;
    let apply: &dyn Fn(&mut TableInfo);
    let drop_mview = |base: &mut TableInfo| {
        let Some(info) = base.materialized_view_base.clone() else {
            return;
        };
        let empty = {
            let mut info = info.write();
            let new_ids: Vec<i64> = info.mview_ids.iter().copied().filter(|&id| id != job.table_id).collect();
            info.mview_ids = new_ids.into();
            info.mlog_id == 0 && info.mview_ids.is_empty()
        };
        if empty {
            base.materialized_view_base = None;
        }
    };
    let drop_mlog = |base: &mut TableInfo| {
        let Some(info) = base.materialized_view_base.clone() else {
            return;
        };
        let empty = {
            let mut info = info.write();
            if info.mlog_id == job.table_id {
                info.mlog_id = 0;
            }
            info.mlog_id == 0 && info.mview_ids.is_empty()
        };
        if empty {
            base.materialized_view_base = None;
        }
    };
    if let Some(mview) = &dropping_table.materialized_view {
        if mview.read().base_table_ids.is_empty() {
            logger().warn(
                "materialized view has no base tables in metadata, skip dependency cleanup when dropping",
                &[i64_field("mviewID", dropping_table.id)],
            );
            return Ok(Vec::new());
        }
        base_table_ids = mview.read().base_table_ids.iter().copied().collect();
        apply = &drop_mview;
    } else if let Some(mlog) = &dropping_table.materialized_view_log {
        base_table_ids = vec![mlog.read().base_table_id];
        apply = &drop_mlog;
    } else {
        return Ok(Vec::new());
    }
    let mut extra_infos = Vec::with_capacity(base_table_ids.len());
    let mut processed: Vec<i64> = Vec::with_capacity(base_table_ids.len());
    for base_id in base_table_ids {
        if processed.contains(&base_id) {
            continue;
        }
        processed.push(base_id);
        let meta = txn.meta();
        let base = meta.table(job.schema_id, base_id).map_err(|error| job_meta_error(&meta, error))?;
        let Some(mut base) = base else {
            continue;
        };
        let mlog_id = match (&dropping_table.materialized_view, &base.materialized_view_base) {
            (Some(_), Some(info)) => info.read().mlog_id,
            _ => 0,
        };
        apply(&mut base);
        extra_infos.push((job.schema_id, base));
        if mlog_id == 0 {
            continue;
        }
        let meta = txn.meta();
        let mlog = meta.table(job.schema_id, mlog_id).map_err(|error| job_meta_error(&meta, error))?;
        let Some(log) = mlog.as_ref().and_then(|mlog| mlog.materialized_view_log.clone()) else {
            // "The executor rejects this corrupted metadata before submitting
            // the job. Do not leave an already-started DROP job retrying in
            // delete-only state."
            logger().error(
                "drop materialized view: materialized view log is missing or invalid during dependency cleanup",
                &[i64_field("mviewID", job.table_id), i64_field("baseTableID", base_id), i64_field("mlogID", mlog_id)],
            );
            continue;
        };
        let mlog_base_table_id = log.read().base_table_id;
        if mlog_base_table_id != base_id {
            logger().error(
                "drop materialized view: materialized view log belongs to a different base table during dependency cleanup",
                &[
                    i64_field("mviewID", job.table_id),
                    i64_field("baseTableID", base_id),
                    i64_field("mlogID", mlog_id),
                    i64_field("mlogBaseTableID", mlog_base_table_id),
                ],
            );
            continue;
        }
        if !log.read().dependent_mview_ids.iter().any(|&id| id == job.table_id) {
            continue;
        }
        let kept: Vec<i64> = log.read().dependent_mview_ids.iter().copied().filter(|&id| id != job.table_id).collect();
        log.write().dependent_mview_ids = kept.into();
        extra_infos.push((job.schema_id, mlog.expect("the log was read")));
    }
    Ok(extra_infos)
}

/// Go master `buildCreateMaterializedViewRefreshInfoUpsertSQL`.
fn create_materialized_view_refresh_info_upsert_sql(
    mview_id: i64,
    read_ts: u64,
    last_success: Option<i64>,
    next: Option<i64>,
    should_update: bool,
) -> String {
    let last = last_success.map_or_else(|| "NULL".to_owned(), |last| last.to_string());
    if should_update {
        let next = next.map_or_else(|| "NULL".to_owned(), |next| next.to_string());
        return format!(
            "INSERT INTO mysql.tidb_mview_refresh_info (MVIEW_ID, LAST_SUCCESS_READ_TSO, LAST_SUCCESS_REFRESH_END_UNIX_SECONDS, NEXT_REFRESH_UNIX_SECONDS)\nVALUES ({mview_id}, {read_ts}, {last}, {next}) ON DUPLICATE KEY UPDATE LAST_SUCCESS_READ_TSO = VALUES(LAST_SUCCESS_READ_TSO), LAST_SUCCESS_REFRESH_END_UNIX_SECONDS = VALUES(LAST_SUCCESS_REFRESH_END_UNIX_SECONDS), NEXT_REFRESH_UNIX_SECONDS = VALUES(NEXT_REFRESH_UNIX_SECONDS)"
        );
    }
    format!(
        "INSERT INTO mysql.tidb_mview_refresh_info (MVIEW_ID, LAST_SUCCESS_READ_TSO, LAST_SUCCESS_REFRESH_END_UNIX_SECONDS)\nVALUES ({mview_id}, {read_ts}, {last}) ON DUPLICATE KEY UPDATE LAST_SUCCESS_READ_TSO = VALUES(LAST_SUCCESS_READ_TSO), LAST_SUCCESS_REFRESH_END_UNIX_SECONDS = VALUES(LAST_SUCCESS_REFRESH_END_UNIX_SECONDS)"
    )
}

/// Go master `convertCreateMaterializedViewRefreshInfoTableNotExistsErr`.
fn convert_refresh_info_table_not_exists(error: tidb_terror::MysqlError) -> DdlPlanError {
    if error.equal(&tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS) {
        return DdlPlanError::from_mysql_error(invalid_ddl_job(
            "create materialized view: required system table mysql.tidb_mview_refresh_info does not exist",
        ));
    }
    DdlPlanError::from_mysql_error(error)
}

/// Go master `prewriteCreateMaterializedViewRefreshInfo`: the view's refresh
/// row, read TS its own transaction's start TS, in a transaction of a pooled
/// session.
fn prewrite_create_materialized_view_refresh_info(
    sessions: &DdlStepWorker,
    mview_id: i64,
) -> Result<(), DdlPlanError> {
    let pool = sessions
        .sess_pool
        .sess_pool()
        .ok_or_else(|| DdlPlanError::Encode("the DDL session pool has no sessions".to_owned()))?;
    let session_ctx = pool.get().map_err(|error| DdlPlanError::Encode(error.to_string()))?;
    let context = tidb_sqlexec::BackgroundContext;
    let session = tidb_ddl_session::Session::new(std::sync::Arc::clone(&session_ctx));
    let result = (|| {
        session.begin(&context).map_err(|error| DdlPlanError::Encode(error.to_string()))?;
        let mut committed = false;
        let done = (|| {
            ddl_session_sql(
                &*session_ctx,
                "SELECT 1 FROM mysql.tidb_mview_refresh_info WHERE MVIEW_ID = %? LIMIT 1",
                &[tidb_sqlescape::SqlArg::Signed(mview_id)],
            )
            .map_err(convert_refresh_info_table_not_exists)?;
            let start_ts = session
                .txn()
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?
                .map(|txn| txn.start_ts())
                .filter(|&ts| ts != 0)
                .ok_or_else(|| {
                    DdlPlanError::from_mysql_error(invalid_ddl_job("create materialized view: invalid init refresh tso"))
                })?;
            refresh_info_upsert_result(ddl_session_sql(
                &*session_ctx,
                &create_materialized_view_refresh_info_upsert_sql(mview_id, start_ts, None, None, false),
                &[],
            ))?;
            session.commit(&context).map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            committed = true;
            Ok(())
        })();
        if !committed {
            session.rollback();
        }
        done
    })();
    pool.put(session_ctx);
    result
}

/// Go master `mviewutil.InsertRefreshSourceIDs` on the step's session: the
/// view's whole input log set.
fn insert_mview_refresh_source_info(txn: &JobTxn<'_>, mview_id: i64, mlog_ids: &[i64]) -> Result<(), DdlPlanError> {
    txn.set_must_rollback_txn_on_error();
    tidb_mviewutil::refresh_source::insert_refresh_source_ids(&mut job_sql_exec(txn), mview_id, mlog_ids)
        .map_err(DdlPlanError::from_mysql_error)
}

/// The step session as Go's `w.sess.GetSQLExecutor()` for `mviewutil`.
fn job_sql_exec<'t, 's>(
    txn: &'t JobTxn<'s>,
) -> impl FnMut(&str, &[tidb_sqlescape::SqlArg<'_>]) -> Result<Vec<Vec<tidb_datatype::Datum>>, tidb_terror::MysqlError> + use<'t, 's> {
    move |sql, args| {
        let se = txn.sess()?;
        tidb_sqlexec::execute_sql(&tidb_sqlexec::BackgroundContext, se.session().get_sql_executor(), sql, args)
            .map_err(tidb_store_driver_error::sql_query_error::mysql_error_of)
    }
}

/// Go master `deleteCreateMaterializedViewRefreshInfos` (with
/// `deleteMViewRefreshSourceInfos` first): a missing info table deletes
/// nothing.
pub(crate) fn delete_create_materialized_view_refresh_info(txn: &JobTxn<'_>, mview_id: i64) -> Result<(), DdlPlanError> {
    txn.set_must_rollback_txn_on_error();
    tidb_mviewutil::refresh_source::delete_refresh_source_info(&mut job_sql_exec(txn), &[mview_id])
        .map_err(DdlPlanError::from_mysql_error)?;
    txn.set_must_rollback_txn_on_error();
    let mut result = execute_job_sql(
        txn,
        &format!("DELETE FROM mysql.tidb_mview_refresh_info WHERE MVIEW_ID IN ({mview_id})"),
        "mview-refresh-info-delete",
    )
        .map(|_| ());
    if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/mockDeleteCreateMaterializedViewRefreshInfoTableNotExists")
        .is_some_and(|value| value.bool())
    {
        result = Err(system_table_not_exists("tidb_mview_refresh_info"));
    }
    if let Some(value) = tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/mockDeleteCreateMaterializedViewRefreshInfoErr") {
        result = Err(tidb_terror::MysqlError::unknown(value.string()));
    }
    match result {
        Err(error) if error.equal(&tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS) => Ok(()),
        other => other.map_err(DdlPlanError::from_mysql_error),
    }
}

/// Go `infoschema.ErrTableNotExists.GenWithStackByArgs("mysql", table)`,
/// the maintenance-table failpoints' error.
fn system_table_not_exists(table: &str) -> tidb_terror::MysqlError {
    tidb_terror::MysqlError::from_terror(
        &tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
            .gen_with_stack_by_args(&["mysql".to_owned().into(), table.to_owned().into()]),
    )
}

/// Go master `deleteCreateMaterializedViewRefreshAlerts`: a missing alert
/// table deletes nothing; the callers log any other error.
pub(crate) fn delete_create_materialized_view_refresh_alert(txn: &JobTxn<'_>, mview_id: i64) -> Result<(), tidb_terror::MysqlError> {
    let mut result = Ok(());
    if let Some(value) = tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/mockDeleteCreateMaterializedViewRefreshAlertErr") {
        result = Err(tidb_terror::MysqlError::unknown(value.string()));
    }
    if result.is_ok() {
        txn.set_must_rollback_txn_on_error();
        result = execute_job_sql(
            txn,
            &format!("DELETE FROM mysql.tidb_mview_refresh_alert WHERE MVIEW_ID IN ({mview_id})"),
            "mview-refresh-alert-delete",
        )
            .map(|_| ());
    }
    match result {
        Err(error) if error.equal(&tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS) => Ok(()),
        other => other,
    }
}

/// Go master `execCreateMaterializedViewRefreshInfoUpsert`'s
/// `mockUpsertCreateMaterializedViewRefreshInfoTableNotExists`, then
/// `convertCreateMaterializedViewRefreshInfoTableNotExistsErr`.
fn refresh_info_upsert_result<T>(result: Result<T, tidb_terror::MysqlError>) -> Result<(), DdlPlanError> {
    let mut result = result.map(|_| ());
    if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/mockUpsertCreateMaterializedViewRefreshInfoTableNotExists")
        .is_some_and(|value| value.bool())
    {
        result = Err(system_table_not_exists("tidb_mview_refresh_info"));
    }
    result.map_err(convert_refresh_info_table_not_exists)
}

/// Go master `upsertCreateMaterializedViewRefreshInfo`: the refresh row with
/// the build's read TS, now as the last success, and the next refresh.
fn upsert_create_materialized_view_refresh_info(
    txn: &JobTxn<'_>,
    mview: &TableInfo,
    read_ts: u64,
) -> Result<(), DdlPlanError> {
    let info = mview
        .materialized_view
        .as_ref()
        .ok_or_else(|| {
            DdlPlanError::from_mysql_error(invalid_ddl_job("create materialized view: invalid materialized view metadata"))
        })?
        .read()
        .clone();
    let (next, should_update) = tidb_ddl_build::materialized_view::derive_materialized_schedule_next_unix_seconds(
        &info.refresh_start_with,
        &info.refresh_next,
        info.refresh_schedule_sql_mode,
        chrono::Utc::now(),
    )
    .map_err(DdlPlanError::from_mysql_error)?;
    let last_success = chrono::Utc::now().timestamp();
    let sql = create_materialized_view_refresh_info_upsert_sql(mview.id, read_ts, Some(last_success), next, should_update);
    txn.set_must_rollback_txn_on_error();
    refresh_info_upsert_result(execute_job_sql(txn, &sql, "mview-refresh-info-upsert"))
}

/// Go master `(*worker).onCreateMaterializedView`.
pub(super) fn on_create_materialized_view(
    txn: &mut JobTxn<'_>,
    catalog: &SessionExtendedInfoSchema,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
    reorg_outcome: Option<&super::add_index::ReorgOutcome>,
    sessions: &DdlStepWorker,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let invalid_args = |active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob, message: &str| {
        cancel_ddl_job(active, DdlPlanError::from_mysql_error(invalid_ddl_job(message)))
    };
    let args = tidb_model::get_create_materialized_view_args(&mut active.job)
        .map_err(|error| cancel_ddl_job(active, DdlPlanError::Encode(error.to_string())))?;
    let Some(args) = args else {
        return Err(invalid_args(active, "create materialized view: invalid job args"));
    };
    let (table_handle, mlog_ids) = {
        let args = args.read();
        (args.table_info.clone(), args.mlog_table_ids.clone())
    };
    let Some(table_handle) = table_handle else {
        return Err(invalid_args(active, "create materialized view: invalid job args"));
    };
    let mut mview = table_handle.read().clone_like_go();
    let base_ids: Vec<i64> = match &mview.materialized_view {
        Some(info) if !info.read().base_table_ids.is_empty() => info.read().base_table_ids.iter().copied().collect(),
        _ => return Err(invalid_args(active, "create materialized view: invalid job args")),
    };
    let mut seen: Vec<i64> = Vec::new();
    for &id in &base_ids {
        if id == 0 {
            return Err(invalid_args(active, "create materialized view: invalid base table id"));
        }
        if seen.contains(&id) {
            return Err(invalid_args(active, "create materialized view: duplicate base table id"));
        }
        seen.push(id);
    }
    if active.job.is_rollingback() {
        return rollback_create_materialized_view(txn, active, &mview, &mlog_ids, start_ts);
    }
    let schema_id = active.job.schema_id;
    let schema_name = active.job.schema_name.to_string();
    match active.job.schema_state {
        SchemaState::NONE => {
            for &id in &base_ids {
                if let Err(error) = on_create_materialized_view_base_check(txn, schema_id, id, &schema_name) {
                    if is_create_materialized_view_base_check_cancelled_err(&error) {
                        active.job.state = JobState::CANCELLED;
                    }
                    return Err(error);
                }
            }
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
            mview.state = SchemaState::NONE;
            create_table::create_table(
                txn,
                catalog,
                &database,
                active,
                &mut mview,
                false,
                start_ts,
                &mut placement_bundles,
            )?;
            active.job.table_id = mview.id;
            let extra = update_materialized_view_base_info_on_create(txn, &mut active.job, &mview, &mlog_ids, start_ts)?;
            let mut diff = SchemaDiff {
                action_type: active.job.type_,
                schema_id,
                table_id: mview.id,
                ..SchemaDiff::default()
            };
            let infos: Vec<(i64, i64)> = extra.iter().map(|(schema, table)| (*schema, table.id)).collect();
            set_schema_diff_for_multi_infos(&mut diff, &infos);
            let (schema_version, diff) = update_schema_version(&txn.meta(), active, diff)?;
            *table_handle.write() = mview.clone_like_go();
            let mut mutations = Vec::new();
            append_schema_change_mutations(txn, active.job.id, &[(-1, SchemaChangeEvent::create_table(mview.clone_like_go()))])?;
            if let Err(error) = prewrite_create_materialized_view_refresh_info(sessions, mview.id) {
                active.job.state = JobState::ROLLINGBACK;
                return Err(error);
            }
            active.job.schema_state = SchemaState::WRITE_REORGANIZATION;
            active.job.state = JobState::RUNNING;
            let mut write = job_only_write(active.job.id, mutations);
            write.schema_version = schema_version;
            write.diff = diff;
            write.created_id = Some(mview.id);
            write.placement_bundles = placement_bundles;
            Ok(PersistedDdlActionStep { write, update_raw_args: true, run_error: None, reorg: None })
        }
        SchemaState::WRITE_REORGANIZATION => {
            let Some(outcome) = reorg_outcome else {
                // Go: rows left by a build that did not finish fail the retry.
                let check_args = [
                    tidb_sqlescape::SqlArg::String(schema_name.as_bytes()),
                    tidb_sqlescape::SqlArg::String(mview.name.original().as_bytes()),
                ];
                // Go master `mview_worker.go`: in materialized view maintenance.
                match txn.execute_in_mview_maintenance(
                    "SELECT 1 FROM %n.%n LIMIT 1",
                    "create-materialized-view-check-build-rows",
                    &check_args,
                ) {
                    Ok(rows) if !rows.is_empty() => {
                        active.job.state = JobState::ROLLINGBACK;
                        return Err(DdlPlanError::from_mysql_error(invalid_ddl_job(
                            "create materialized view: detected residual build rows on retry",
                        )));
                    }
                    Err(error) => {
                        active.job.state = JobState::ROLLINGBACK;
                        return Err(DdlPlanError::from_mysql_error(error));
                    }
                    _ => {}
                }
                let build = MViewBuild {
                    job_id: active.job.id,
                    schema_name: schema_name.clone(),
                    mview: Box::new(mview.clone_like_go()),
                    job_session_vars: active
                        .job
                        .session_vars
                        .as_ref()
                        .map(|vars| vars.read().iter().map(|(name, value)| (name.clone(), value.to_string())).collect())
                        .unwrap_or_default(),
                    resource_group: active
                        .job
                        .reorg_meta
                        .as_ref()
                        .map(|meta| meta.read().resource_group_name.to_string())
                        .unwrap_or_default(),
                };
                return Ok(PersistedDdlActionStep {
                    write: job_only_write(active.job.id, Vec::new()),
                    update_raw_args: false,
                    run_error: None,
                    reorg: Some(super::add_index::AddIndexReorg {
                        job_id: active.job.id,
                        query: active.job.query.clone(),
                        stage: super::add_index::ReorgStageRequest::BuildMaterializedView(Box::new(build)),
                        table: Box::new(mview.clone_like_go()),
                        index_ids: Vec::new(),
                        merging: false,
                        physical_ids: Vec::new(),
                        batch_size: 1,
                        concurrency: 1,
                        session_vars: tidb_ddl_build::session_pool::ReorgSessionVars::of_job(&active.job),
                        row_count: active.job.row_count,
                        column: None,
                        sub_job: super::add_index::reorg_sub_job(&active.job),
                        clean_up_global_index: false,
                        partition_phase: None,
                        schema_id,
                    }),
                });
            };
            let read_ts = match &outcome.result {
                super::add_index::ReorgResult::Built(read_ts) => *read_ts,
                super::add_index::ReorgResult::Failed { code, message, identity, .. } => {
                    active.job.state = JobState::ROLLINGBACK;
                    return Err(DdlPlanError::from_mysql_error(
                        tidb_terror::MysqlError::sql_error(*code, message.clone()).with_identity_opt(*identity),
                    ));
                }
                _ => {
                    active.job.state = JobState::ROLLINGBACK;
                    return Err(DdlPlanError::Encode("create materialized view: unexpected build outcome".to_owned()));
                }
            };
            active.job.snapshot_version = read_ts;
            if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewPostBuildRetryableErr")
                .is_some_and(|value| value.bool())
            {
                return Err(wait_reorg_timeout());
            }
            if active.job.snapshot_version == 0 {
                active.job.state = JobState::ROLLINGBACK;
                return Err(DdlPlanError::from_mysql_error(invalid_ddl_job(
                    "create materialized view: invalid build read tso",
                )));
            }
            let mut finished = Vec::new();
            let mut source_mlogs = Vec::new();
            for &id in &base_ids {
                let base = match on_create_materialized_view_base_check(txn, schema_id, id, &schema_name) {
                    Ok(base) => base,
                    Err(error) => {
                        active.job.state = JobState::ROLLINGBACK;
                        return Err(error);
                    }
                };
                source_mlogs.push(base.materialized_view_base.as_ref().map_or(0, |info| info.read().mlog_id));
                finished.push(base);
            }
            if let Err(error) = upsert_create_materialized_view_refresh_info(txn, &mview, read_ts) {
                active.job.state = JobState::ROLLINGBACK;
                return Err(error);
            }
            if let Err(error) = insert_mview_refresh_source_info(txn, mview.id, &source_mlogs) {
                active.job.state = JobState::ROLLINGBACK;
                return Err(error);
            }
            tidb_failpoint::inject_call(
                "github.com/pingcap/tidb/pkg/ddl/afterCreateMaterializedViewSuccessRefreshInfoUpsert",
                &mut (),
            );
            if tidb_failpoint::eval(
                "github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewPostBuildAfterRefreshInfoUpsertRetryableErr",
            )
            .is_some_and(|value| value.bool())
            {
                return Err(wait_reorg_timeout());
            }
            if let Some(info) = &mview.materialized_view {
                info.write().init_build_state = tidb_model::table::MViewInitBuildState::READY;
            }
            if let Err(error) = update_table(txn, schema_id, &mut mview, start_ts) {
                active.job.state = JobState::ROLLINGBACK;
                return Err(error);
            }
            let diff = SchemaDiff {
                action_type: active.job.type_,
                schema_id,
                table_id: mview.id,
                ..SchemaDiff::default()
            };
            let (schema_version, diff) = match update_schema_version(&txn.meta(), active, diff) {
                Ok(updated) => updated,
                Err(error) => {
                    active.job.state = JobState::ROLLINGBACK;
                    return Err(error);
                }
            };
            finished.push(mview.clone_like_go());
            active.job.finish_multiple_table_job(
                JobState::DONE,
                SchemaState::PUBLIC,
                schema_version,
                &GoSharedPointerSlice::from(finished),
            );
            let mut write = job_only_write(active.job.id, Vec::new());
            write.schema_version = schema_version;
            write.diff = diff;
            Ok(PersistedDdlActionStep { write, update_raw_args: true, run_error: None, reorg: None })
        }
        state => Err(DdlPlanError::from(
            tidb_dbterror::ERR_INVALID_DDL_STATE.gen_with_stack(format!("invalid create materialized view schema state {state}")),
        )),
    }
}

/// Go master `rollingbackCreateMaterializedView`: before the view exists the
/// job is cancelled; during its build it rolls back.
pub(super) fn rollingback_create_materialized_view(
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    if let Err(error) = tidb_model::get_create_materialized_view_args(&mut active.job) {
        return Err(cancel_ddl_job(active, DdlPlanError::Encode(error.to_string())));
    }
    match active.job.schema_state {
        SchemaState::NONE => Err(super::cancelled_ddl_job(active)),
        SchemaState::WRITE_REORGANIZATION => {
            active.job.state = JobState::ROLLINGBACK;
            Ok(PersistedDdlActionStep {
                write: job_only_write(active.job.id, Vec::new()),
                update_raw_args: true,
                run_error: Some(DdlPlanError::from(
                    tidb_dbterror::ERR_CANCELLED_DDL_JOB.generate(tidb_errno::errname::ErrCancelledDDLJob.raw),
                )),
                reorg: None,
            })
        }
        _ => Err(DdlPlanError::from(tidb_dbterror::ERR_CANNOT_CANCEL_DDL_JOB.gen_with_stack_by_args(&[active.job.id.into()]))),
    }
}

/// Go master `checkBaseTableDependentMViewMinMaxIndexConstraintsInOwner`:
/// the owner's recheck against the latest schema, cancelling the job on a
/// refusal. A multi-schema change validates its final table shape instead.
pub(crate) fn check_base_table_dependent_mview_min_max_index_constraints_in_owner(
    catalog: &SessionExtendedInfoSchema,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    base: &TableInfo,
    effective: &TableInfo,
    index_name: &str,
    op: &str,
) -> Result<(), DdlPlanError> {
    if active.job.multi_schema_info.is_some()
        || base.materialized_view_base.as_ref().is_none_or(|info| info.read().mview_ids.is_empty())
    {
        return Ok(());
    }
    let schema_name = active.job.schema_name.to_string();
    crate::worker::executor_mview::check_base_table_dependent_mview_min_max_index_constraints_with_effective_table(
        None,
        &|id| catalog.table_info_by_id(id).map(|table| table.read().clone_like_go()),
        &schema_name,
        base,
        effective,
        "",
        index_name,
        op,
    )
    .map_err(|error| cancel_ddl_job(active, DdlPlanError::from_mysql_error(error)))
}

/// Go master `checkBaseTableDependentMViewMinMaxIndexConstraints` at the
/// statement: the table as it is, without the named index.
pub(crate) fn check_base_table_dependent_mview_min_max_index_constraints(
    context: &dyn DdlSessionContext,
    catalog: &SessionExtendedInfoSchema,
    schema_name: &str,
    base: &TableInfo,
    index_name: &str,
    op: &str,
) -> Result<(), DdlPlanError> {
    crate::worker::executor_mview::check_base_table_dependent_mview_min_max_index_constraints_with_effective_table(
        Some(context),
        &|id| catalog.table_info_by_id(id).map(|table| table.read().clone_like_go()),
        schema_name,
        base,
        base,
        index_name,
        index_name,
        op,
    )
    .map_err(DdlPlanError::from_mysql_error)
}

/// Go `dbterror.ErrWaitReorgTimeout`: the job runs this state again.
fn wait_reorg_timeout() -> DdlPlanError {
    DdlPlanError::from(tidb_dbterror::ERR_WAIT_REORG_TIMEOUT.gen_with_stack_by_args(&[]))
}

/// Go master `(*worker).rollbackCreateMaterializedView`.
fn rollback_create_materialized_view(
    txn: &mut JobTxn<'_>,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    mview: &TableInfo,
    mlog_ids: &[i64],
    start_ts: u64,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let schema_id = active.job.schema_id;
    let table_id = active.job.table_id;
    let actual = match get_table_info(txn, table_id, schema_id) {
        Ok(table) => Some(table),
        Err(error) if is_not_exists(&error) => None,
        Err(error) => return Err(error),
    };
    let dropping = actual.clone().unwrap_or_else(|| mview.clone_like_go());
    let extra = update_materialized_view_base_info_on_drop(txn, &active.job, &dropping)?;
    for (extra_schema, table) in &extra {
        update_table(txn, *extra_schema, &mut table.clone_like_go(), start_ts)?;
    }
    if actual.is_some() {
        let meta = txn.meta();
        meta.drop_table_or_view(schema_id, table_id).map_err(|error| job_meta_error(&meta, error))?;
        meta.auto_ids(schema_id, table_id).delete().map_err(|error| job_meta_error(&meta, error))?;
    }
    delete_create_materialized_view_refresh_info(txn, table_id)?;
    if let Err(error) = delete_create_materialized_view_refresh_alert(txn, table_id) {
        tidb_ddl_logutil::ddl_logger().warn(
            "create materialized view rollback: failed to delete refresh alert",
            &[
                tidb_log::Field::new("schemaName", tidb_log::Value::Str(active.job.schema_name.to_string())),
                tidb_log::Field::new("tableName", tidb_log::Value::Str(mview.name.original().to_owned())),
                tidb_log::Field::new("mviewID", tidb_log::Value::I64(table_id)),
                tidb_log::Field::new("error", tidb_log::Value::Str(error.to_string())),
            ],
        );
    }
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
    active.job.fill_args(Some(GoShared::new(tidb_model::CreateMaterializedViewArgs {
        table_info: Some(GoShared::new(mview.clone_like_go())),
        mlog_table_ids: mlog_ids.to_vec(),
    })));
    let mut write = job_only_write(active.job.id, Vec::new());
    write.schema_version = schema_version;
    write.diff = diff;
    Ok(PersistedDdlActionStep { write, update_raw_args: true, run_error: None, reorg: None })
}

/// Go master `onAlterMaterializedViewRefresh` / `onAlterMaterializedViewAttributes`:
/// the view's refresh schedule or alert attributes, with a notifier event.
fn on_alter_materialized_view(
    txn: &mut JobTxn<'_>,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
    apply: impl FnOnce(&mut tidb_model::table::MaterializedViewInfo),
    event: fn(TableInfo, TableInfo) -> SchemaChangeEvent,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let mut table = checked_job_table(txn, active, true)?.read().clone_like_go();
    let Some(info) = table.materialized_view.clone() else {
        return Err(cancel_ddl_job(
            active,
            DdlPlanError::from(tidb_dbterror::ERR_WRONG_OBJECT.gen_with_stack_by_args(&[
                active.job.schema_name.to_string().into(),
                active.job.table_name.to_string().into(),
                "MATERIALIZED VIEW".into(),
            ])),
        ));
    };
    if is_revertible_sub_job(&active.job) {
        active.job.mark_non_revertible();
        return Ok(super::add_index::action_step(job_only_write(active.job.id, Vec::new())));
    }
    let old_table = table.clone_like_go();
    apply(&mut info.write());
    table.materialized_view = Some(info);
    let schema_id = active.job.schema_id;
    let mut write = super::add_index::update_version_and_table_info(txn, active, schema_id, &table, true, start_ts)?;
    let schema_version = write.schema_version;
    let mut mutations = Vec::new();
    append_schema_change_mutations(txn, active.job.id, &[(-1, event(table.clone_like_go(), old_table))])?;
    active.job.finish_table_job(JobState::DONE, SchemaState::PUBLIC, schema_version, Some(GoShared::new(table)));
    write.mutations.extend(mutations);
    Ok(super::add_index::action_step(write))
}

/// Go master `onAlterMaterializedViewRefresh`.
pub(super) fn on_alter_materialized_view_refresh(
    txn: &mut JobTxn<'_>,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let args = tidb_model::get_alter_materialized_view_refresh_args(&mut active.job)
        .map_err(|error| cancel_ddl_job(active, DdlPlanError::Encode(error.to_string())))?
        .unwrap_or_default();
    let args = args.read().clone();
    on_alter_materialized_view(
        txn,
        active,
        start_ts,
        |info| {
            info.refresh_method = args.refresh_method;
            info.refresh_start_with = args.refresh_start_with;
            info.refresh_next = args.refresh_next;
            if args.update_refresh_schedule {
                info.refresh_schedule_sql_mode = args.refresh_schedule_sql_mode;
            }
        },
        SchemaChangeEvent::alter_materialized_view_refresh,
    )
}

/// Go master `onAlterMaterializedViewAttributes`.
pub(super) fn on_alter_materialized_view_attributes(
    txn: &mut JobTxn<'_>,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let args = tidb_model::get_alter_materialized_view_attributes_args(&mut active.job)
        .map_err(|error| cancel_ddl_job(active, DdlPlanError::Encode(error.to_string())))?
        .unwrap_or_default();
    let args = args.read().clone();
    on_alter_materialized_view(
        txn,
        active,
        start_ts,
        |info| {
            info.alert_warning_sec = args.alert_warning_sec;
            info.alert_overdue_sec = args.alert_overdue_sec;
            info.alert_refresh_failed = args.alert_refresh_failed;
        },
        SchemaChangeEvent::alter_materialized_view_attributes,
    )
}

/// Go master `replaceMaterializedViewID`: `old` replaced by `new`, deduped
/// in order; whether `old` was there.
fn replace_materialized_view_id(ids: &[i64], old: i64, new: i64) -> (Vec<i64>, bool) {
    let mut replaced = false;
    let mut result: Vec<i64> = Vec::with_capacity(ids.len());
    for &id in ids {
        let id = if id == old {
            replaced = true;
            new
        } else {
            id
        };
        if !result.contains(&id) {
            result.push(id);
        }
    }
    (result, replaced)
}

/// Go master `convertMViewRefreshInfoTableNotExistsErrOnOutOfPlaceCutover`.
fn convert_cutover_refresh_info_table_not_exists(error: tidb_terror::MysqlError) -> DdlPlanError {
    if error.equal(&tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS) {
        return DdlPlanError::from(tidb_dbterror::ERR_INVALID_DDL_JOB.gen_with_stack_by_args(&[
            "refresh materialized view complete OUT OF PLACE cutover: required system table mysql.tidb_mview_refresh_info does not exist".into(),
        ]));
    }
    DdlPlanError::from_mysql_error(error)
}

fn job_sql_rows(
    txn: &JobTxn<'_>,
    sql: &str,
    label: &str,
    args: &[tidb_sqlescape::SqlArg<'_>],
) -> Result<Vec<Vec<tidb_datatype::Datum>>, tidb_terror::MysqlError> {
    txn.execute(sql, label, args)
}

/// Go master `migrateMViewRefreshInfoForOutOfPlaceCutover`: the view's
/// refresh-info row moves to the shadow table's ID with the build's read
/// TSO, after Go's staleness check of `LAST_SUCCESS_READ_TSO`.
fn migrate_mview_refresh_info_for_out_of_place_cutover(
    txn: &JobTxn<'_>,
    args: &tidb_model::RefreshMaterializedViewCompleteOutOfPlaceCutoverArgs,
) -> Result<(), DdlPlanError> {
    use tidb_sqlescape::SqlArg;
    let invalid = |message: &str| {
        DdlPlanError::from(tidb_dbterror::ERR_INVALID_DDL_JOB.gen_with_stack_by_args(&[format!(
            "refresh materialized view complete OUT OF PLACE cutover: {message}"
        )
        .into()]))
    };
    txn.set_must_rollback_txn_on_error();
    let rows = job_sql_rows(
        txn,
        "SELECT MVIEW_ID, LAST_SUCCESS_READ_TSO, LAST_SUCCESS_REFRESH_END_UNIX_SECONDS, NEXT_REFRESH_UNIX_SECONDS FROM mysql.tidb_mview_refresh_info WHERE MVIEW_ID = %?",
        "mview-refresh-cutover-read-refresh-info",
        &[SqlArg::Signed(args.old_mview_id)],
    )
    .map_err(convert_cutover_refresh_info_table_not_exists)?;
    if rows.is_empty() {
        return Err(invalid("refresh info row missing in mysql.tidb_mview_refresh_info"));
    }
    if rows.len() != 1 {
        return Err(invalid("inconsistent refresh info row in mysql.tidb_mview_refresh_info"));
    }
    let read_tso = &rows[0][1];
    let stale = if args.expected_last_success_read_tso_null {
        !read_tso.is_null()
    } else {
        let value = match read_tso {
            tidb_datatype::Datum::UInt(value) => Some(*value),
            tidb_datatype::Datum::Int(value) => u64::try_from(*value).ok(),
            _ => None,
        };
        value != Some(args.expected_last_success_read_tso)
    };
    if stale {
        return Err(invalid("stale LAST_SUCCESS_READ_TSO detected before cutover"));
    }
    let mut sql = "UPDATE mysql.tidb_mview_refresh_info SET MVIEW_ID = %?, LAST_SUCCESS_READ_TSO = %?, LAST_SUCCESS_REFRESH_END_UNIX_SECONDS = %?".to_owned();
    let mut build_read_tso = args.build_read_tso;
    if let Some(value) =
        tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/mockMViewRefreshOutOfPlacePersistReadTSO")
    {
        if let Ok(ts) = value.string().parse::<u64>() {
            build_read_tso = ts;
        }
    }
    let mut params = vec![
        SqlArg::Signed(args.shadow_table_id),
        SqlArg::Unsigned(build_read_tso),
        SqlArg::Signed(chrono::Utc::now().timestamp()),
    ];
    if args.should_update_next_refresh_unix_seconds {
        sql.push_str(", NEXT_REFRESH_UNIX_SECONDS = %?");
        params.push(args.next_refresh_unix_seconds.map_or(SqlArg::Null, SqlArg::Signed));
    }
    sql.push_str(" WHERE MVIEW_ID = %?");
    params.push(SqlArg::Signed(args.old_mview_id));
    job_sql_rows(txn, &sql, "mview-refresh-cutover-migrate-refresh-info", &params).map(|_| ()).map_err(convert_cutover_refresh_info_table_not_exists)
}

/// Go master `migrateMViewRefreshSourceInfo`: the view's registered input
/// logs move to the new view, or are replaced by the build's when they
/// changed.
fn migrate_mview_refresh_source_info(
    txn: &JobTxn<'_>,
    old_mview_id: i64,
    new_mview_id: i64,
    build_sources: &[tidb_model::MViewRefreshSource],
) -> Result<(), DdlPlanError> {
    use tidb_sqlescape::SqlArg;
    let table = tidb_metadef::system_tables_def::TIDB_MVIEW_FAST_REFRESH_SOURCE_INFO_TABLE_NAME;
    // "Keep this migration within the DDL step's full-transaction error
    // handling."
    txn.set_must_rollback_txn_on_error();
    let read = |mview_id: i64| -> Result<Vec<i64>, DdlPlanError> {
        tidb_mviewutil::refresh_source::read_refresh_source_ids(&mut job_sql_exec(txn), mview_id)
            .map_err(DdlPlanError::from_mysql_error)
    };
    let invalid = |message: &str| {
        DdlPlanError::from(tidb_dbterror::ERR_INVALID_DDL_JOB.gen_with_stack_by_args(&[format!(
            "refresh materialized view complete OUT OF PLACE cutover: {message}"
        )
        .into()]))
    };
    let mlog_ids = read(old_mview_id)?;
    if mlog_ids.is_empty() {
        return Err(invalid("input log records are missing"));
    }
    if !read(new_mview_id)?.is_empty() {
        return Err(invalid("shadow already has input log records"));
    }
    let build_log_ids = tidb_mviewutil::refresh_source::refresh_source_log_ids(build_sources);
    if mlog_ids == build_log_ids {
        return job_sql_rows(
            txn,
            &format!("UPDATE mysql.{table} SET MVIEW_ID = %? WHERE MVIEW_ID = %?"),
            "mview-refresh-cutover-migrate-source-info",
            &[SqlArg::Signed(new_mview_id), SqlArg::Signed(old_mview_id)],
        )
        .map(|_| ())
        .map_err(DdlPlanError::from_mysql_error);
    }
    tidb_mviewutil::refresh_source::delete_refresh_source_info(&mut job_sql_exec(txn), &[old_mview_id])
        .map_err(DdlPlanError::from_mysql_error)?;
    tidb_mviewutil::refresh_source::insert_refresh_source_ids(&mut job_sql_exec(txn), new_mview_id, &build_log_ids)
        .map_err(DdlPlanError::from_mysql_error)
}

/// Go master `(*worker).onRefreshMaterializedViewCompleteOutOfPlaceCutover`:
/// the built shadow table takes the view's name, comment and definition;
/// the old view is dropped (its keys go to GC); the base table and its log
/// point at the new view; the refresh rows move with it.
pub(super) fn on_refresh_materialized_view_complete_out_of_place_cutover(
    txn: &mut JobTxn<'_>,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
    worker: &DdlStepWorker,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let args = tidb_model::get_refresh_materialized_view_complete_out_of_place_cutover_args(&mut active.job)
        .map_err(|error| cancel_ddl_job(active, DdlPlanError::Encode(error.to_string())))?
        .ok_or_else(|| cancel_ddl_job(active, DdlPlanError::Encode("cutover job has nil args".to_owned())))?;
    let args = args.read().clone();
    let invalid = |active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob, message: String| {
        cancel_ddl_job(active, DdlPlanError::from(tidb_dbterror::ERR_INVALID_DDL_JOB.gen_with_stack(message)))
    };
    if args.old_mview_id != active.job.table_id {
        return Err(invalid(active, "refresh materialized view complete OUT OF PLACE cutover: mismatched old materialized view id".into()));
    }
    if args.old_mview_id == args.shadow_table_id {
        return Err(invalid(
            active,
            "refresh materialized view complete OUT OF PLACE cutover: shadow table id equals old materialized view id".into(),
        ));
    }
    if args.build_read_tso == 0 {
        return Err(invalid(active, "refresh materialized view complete OUT OF PLACE cutover: invalid build read tso".into()));
    }
    let schema_id = active.job.schema_id;
    // Go `GetTableInfoAndCancelFaultJob`.
    let old = match get_table_info(txn, args.old_mview_id, schema_id) {
        Ok(old) => old,
        Err(error) if is_not_exists(&error) => return Err(cancel_ddl_job(active, error)),
        Err(error) => return Err(error),
    };
    if old.state != SchemaState::PUBLIC {
        let message = format!("table {} is not in public, but {}", old.name, old.state);
        return Err(cancel_ddl_job(active, DdlPlanError::from(tidb_dbterror::ERR_INVALID_DDL_STATE.gen_with_stack(message))));
    }
    let Some(old_mview) = old.materialized_view.as_ref().map(|info| info.read().clone()) else {
        return Err(cancel_ddl_job(
            active,
            DdlPlanError::from(tidb_dbterror::ERR_WRONG_OBJECT.gen_with_stack_by_args(&[
                active.job.schema_name.to_string().into(),
                active.job.table_name.to_string().into(),
                "MATERIALIZED VIEW".into(),
            ])),
        ));
    };
    let base_ids: Vec<i64> = old_mview.base_table_ids.iter().copied().collect();
    if base_ids.len() != 1 {
        let message = format!(
            "refresh materialized view complete OUT OF PLACE cutover: materialized view must reference exactly one base table in Stage-1, got {}",
            base_ids.len()
        );
        return Err(cancel_ddl_job(active, DdlPlanError::from(tidb_dbterror::ERR_WRONG_OBJECT.gen_with_stack(message))));
    }
    if let Some(expected) = args.expected_old_mview_revision {
        if old.revision != expected {
            return Err(invalid(
                active,
                format!(
                    "refresh materialized view complete OUT OF PLACE cutover: stale old materialized view revision detected before cutover, expected {expected} but got {}",
                    old.revision
                ),
            ));
        }
    }
    let shadow = match get_table_info(txn, args.shadow_table_id, schema_id) {
        Ok(shadow) => shadow,
        Err(error) if is_not_exists(&error) => return Err(cancel_ddl_job(active, error)),
        Err(error) => return Err(error),
    };
    if shadow.materialized_view.is_some()
        || shadow.materialized_view_log.is_some()
        || shadow.view.is_some()
        || shadow.sequence.is_some()
    {
        return Err(invalid(
            active,
            "refresh materialized view complete OUT OF PLACE cutover: shadow table is not a protected physical table".into(),
        ));
    }
    let Some(source_mview_id) = shadow.materialized_view_shadow.as_ref().map(|info| info.read().source_mview_id) else {
        return Err(invalid(active, "refresh materialized view complete OUT OF PLACE cutover: shadow table is missing shadow metadata".into()));
    };
    if source_mview_id != args.old_mview_id {
        return Err(invalid(
            active,
            "refresh materialized view complete OUT OF PLACE cutover: shadow table source materialized view id mismatch".into(),
        ));
    }
    if shadow.state != SchemaState::PUBLIC {
        return Err(invalid(
            active,
            format!("refresh materialized view complete OUT OF PLACE cutover: shadow table is not in public, but {}", shadow.state),
        ));
    }
    if args.old_mlog_id != 0 {
        // "Drain old purges before taking the shared schema-version mutex.
        // The advisory lock also excludes new purges until this job
        // finishes."
        if let Err(error) = acquire_mlog_purge_advisory_lock_for_cutover(worker, active, args.old_mlog_id) {
            if error.equal(&tidb_exeerrors::ERR_MV_ADVISORY_LOCK_CONFLICT) {
                // "Retrying this job would reset the total purge-lock wait
                // budget."
                return Err(cancel_ddl_job(active, error));
            }
            return Err(error);
        }
    }

    // Go `getMViewOutOfPlaceCutoverTable`.
    let cutover_table = |txn: &mut JobTxn<'_>,
                         active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
                         id: i64|
     -> Result<TableInfo, DdlPlanError> {
        get_table_info_and_cancel_non_exist_job(txn, &mut active.job, id, schema_id)
    };
    let mut infos: Vec<TableInfo> = Vec::new();
    for &id in &base_ids {
        let mut base = cutover_table(txn, active, id)?;
        if let Err(error) = rewrite_materialized_view_base_for_out_of_place_cutover(&mut base, args.old_mview_id, args.shadow_table_id) {
            return Err(cancel_ddl_job(active, error));
        }
        let log_id = base.materialized_view_base.as_ref().map_or(0, |info| info.read().mlog_id);
        infos.push(base);
        if log_id != 0 {
            let mut mlog = cutover_table(txn, active, log_id)?;
            let Some(log_info) = mlog.materialized_view_log.clone().filter(|info| info.read().base_table_id == id) else {
                return Err(cancel_ddl_job(
                    active,
                    DdlPlanError::from(tidb_dbterror::ERR_INVALID_DDL_JOB.gen_with_stack_by_args(&[
                        "refresh materialized view complete OUT OF PLACE cutover: materialized view log metadata is invalid".into(),
                    ])),
                ));
            };
            let dependents: Vec<i64> = log_info.read().dependent_mview_ids.iter().copied().collect();
            let (ids, replaced) = replace_materialized_view_id(&dependents, args.old_mview_id, args.shadow_table_id);
            if replaced {
                let mut info = log_info.read().clone();
                info.dependent_mview_ids = ids.into();
                mlog.materialized_view_log = Some(GoShared::new(info));
                infos.push(mlog);
            }
        }
    }

    let mut old_mlog: Option<TableInfo> = None;
    let mut new_mlog: Option<TableInfo> = None;
    if let Some(base) = old.materialized_view_base.as_ref().map(|info| info.read().clone()) {
        if base.mlog_id != args.old_mlog_id {
            return Err(cancel_ddl_job(
                active,
                DdlPlanError::from(tidb_dbterror::ERR_INVALID_DDL_JOB.gen_with_stack_by_args(&[
                    "refresh materialized view complete OUT OF PLACE cutover: output log changed; retry refresh".into(),
                ])),
            ));
        }
        if args.old_mlog_id != 0 {
            let old_log = cutover_table(txn, active, args.old_mlog_id)?;
            let mut replacement = old_log.clone_like_go();
            replacement.id = args.new_mlog_id;
            replacement.revision = 0;
            replacement.update_ts = start_ts;
            if let Some(log) = &replacement.materialized_view_log {
                let mut log = log.read().clone();
                log.base_table_id = args.shadow_table_id;
                replacement.materialized_view_log = Some(GoShared::new(log));
            }
            if let Some(replica) = &replacement.tiflash_replica {
                let mut replica = replica.read().clone();
                replica.available = false;
                replica.available_partition_ids = Default::default();
                replacement.tiflash_replica = Some(GoShared::new(replica));
            }
            old_mlog = Some(old_log);
            new_mlog = Some(replacement);
        }
        for &id in base.mview_ids.iter() {
            let mut child = cutover_table(txn, active, id)?;
            if let Some(info) = &child.materialized_view {
                let mut info = info.read().clone();
                let ids: Vec<i64> = info.base_table_ids.iter().copied().collect();
                info.base_table_ids = replace_materialized_view_id(&ids, args.old_mview_id, args.shadow_table_id).0.into();
                child.materialized_view = Some(GoShared::new(info));
            }
            infos.push(child);
        }
    }

    if let Err(error) = migrate_mview_refresh_info_for_out_of_place_cutover(txn, &args) {
        return Err(cancel_ddl_job(active, error));
    }
    if let Err(error) = migrate_mview_refresh_source_info(
        txn,
        args.old_mview_id,
        args.shadow_table_id,
        args.build_sources.as_deref().unwrap_or_default(),
    ) {
        return Err(cancel_ddl_job(active, error));
    }
    let mut placement_bundles = Vec::new();
    if let Some(replacement) = &mut new_mlog {
        // "Retire the runtime row atomically with the metadata switch, so
        // delayed tasks resolved to the old log fail before issuing any
        // DELETE."
        tidb_failpoint::inject_call(
            "github.com/pingcap/tidb/pkg/ddl/mViewOutOfPlaceBeforeRetireMLog",
            &mut args.old_mlog_id.clone(),
        );
        delete_materialized_view_log_purge_info(txn, args.old_mlog_id)?;
        upsert_create_materialized_view_log_purge_info(txn, replacement)?;
        {
            let meta = txn.meta();
            meta.drop_table_or_view(schema_id, args.old_mlog_id)
                .map_err(|error| job_meta_error(&meta, error))?;
            meta.auto_ids(schema_id, args.old_mlog_id)
                .delete()
                .map_err(|error| job_meta_error(&meta, error))?;
        }
        // Go `createTableOrViewWithCheck`.
        if let Err(error) = check_table_info_valid(replacement) {
            return Err(cancel_ddl_job(active, error));
        }
        {
            let meta = txn.meta();
            meta.create_table_or_view(schema_id, replacement)
                .map_err(|error| job_meta_error(&meta, error))?;
        }
        if let Some((count, labels)) = crate::cluster_ddl::tiflash_replica_spec(replacement) {
            crate::cluster_ddl::configure_tiflash_pd_for_table(replacement.id, count, &labels)
                .map_err(|error| cancel_ddl_job(active, error))?;
        }
        // "Publish the inherited placement policy for the replacement log's
        // new key range."
        if replacement.placement_policy_ref.is_some() {
            let policies = load_policies(txn).map_err(|error| cancel_ddl_job(active, error))?;
            placement_bundles = tidb_placement::new_full_table_bundles(&policies, replacement).map_err(|error| {
                cancel_ddl_job(active, DdlPlanError::Admission(DdlAdmissionError::plain(error.to_string())))
            })?;
        }
    }
    if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/mockMViewRefreshOutOfPlaceCutoverAfterMigrateRefreshInfoError").is_some() {
        return Err(DdlPlanError::plain_refusal(
            "mock refresh materialized view complete OUT OF PLACE cutover error after migrating refresh info",
        ));
    }
    if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/mockMViewRefreshOutOfPlaceCutoverBeforeCommitError").is_some() {
        // Go rolls the step's session back and cancels the job with this
        // error (here the step's `mustRollbackTxnOnError` discards it).
        active.job.state = JobState::CANCELLED;
        return Err(DdlPlanError::plain_refusal(
            "mock refresh materialized view complete OUT OF PLACE cutover error before commit",
        ));
    }
    {
        let meta = txn.meta();
        meta.drop_table_or_view(schema_id, args.old_mview_id)
            .map_err(|error| job_meta_error(&meta, error))?;
        meta.auto_ids(schema_id, args.old_mview_id)
            .delete()
            .map_err(|error| job_meta_error(&meta, error))?;
    }
    let mut new_mview = shadow.clone_like_go();
    new_mview.name = old.name.clone();
    new_mview.comment = old.comment.clone();
    new_mview.materialized_view = Some(GoShared::new(old_mview.clone()));
    new_mview.materialized_view_base = old.materialized_view_base.as_ref().map(|info| {
        let mut info = info.read().clone();
        if let Some(replacement) = &new_mlog {
            info.mlog_id = replacement.id;
        }
        GoShared::new(info)
    });
    new_mview.materialized_view_shadow = None;
    // Go `repairTableOrViewWithCheck`.
    for table in std::iter::once(&mut new_mview).chain(infos.iter_mut()) {
        if let Err(error) = check_table_info_valid(table) {
            return Err(cancel_ddl_job(active, error));
        }
        update_table(txn, schema_id, table, start_ts)?;
    }
    if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/mockMViewRefreshOutOfPlaceCutoverAfterMetadataError").is_some() {
        return Err(DdlPlanError::plain_refusal("mock materialized view cutover error after metadata changes"));
    }
    // Go `SetSchemaDiffForMViewRefreshOutOfPlaceCutover`, then
    // `SetSchemaDiffForMultiInfos`.
    let mut diff = SchemaDiff {
        action_type: active.job.type_,
        schema_id,
        table_id: args.shadow_table_id,
        old_table_id: args.old_mview_id,
        ..SchemaDiff::default()
    };
    if args.old_mlog_id != 0 {
        diff.affected_options = vec![AffectedOption {
            schema_id,
            old_schema_id: schema_id,
            table_id: args.new_mlog_id,
            old_table_id: args.old_mlog_id,
        }]
        .into();
    }
    let info_ids: Vec<(i64, i64)> = infos.iter().map(|table| (schema_id, table.id)).collect();
    set_schema_diff_for_multi_infos(&mut diff, &info_ids);
    let (schema_version, diff) = update_schema_version(&txn.meta(), active, diff)?;
    let mut mutations = Vec::new();
    append_schema_change_mutations(txn, active.job.id, &[(
            -1,
            SchemaChangeEvent::mview_refresh_out_of_place_cutover(
                new_mview.clone_like_go(),
                old.clone_like_go(),
                new_mlog.as_ref().map(TableInfo::clone_like_go),
                old_mlog.as_ref().map(TableInfo::clone_like_go),
            ),
        )])?;
    active.job.finish_table_job(JobState::DONE, SchemaState::PUBLIC, schema_version, Some(GoShared::new(new_mview.clone_like_go())));
    let mut write = job_only_write(active.job.id, mutations);
    write.schema_version = schema_version;
    write.diff = diff;
    write.placement_bundles = placement_bundles;
    Ok(PersistedDdlActionStep { write, update_raw_args: true, run_error: None, reorg: None })
}

/// Go master `rewriteMaterializedViewBaseForOutOfPlaceCutover`: the source
/// lists the shadow table in the old view's place.
fn rewrite_materialized_view_base_for_out_of_place_cutover(
    base: &mut TableInfo,
    old_mview_id: i64,
    shadow_table_id: i64,
) -> Result<(), DdlPlanError> {
    let invalid = |message: &str| {
        DdlPlanError::from(tidb_dbterror::ERR_INVALID_DDL_JOB.gen_with_stack_by_args(&[message.to_owned().into()]))
    };
    let Some(info) = base.materialized_view_base.as_ref().map(|info| info.read().clone()) else {
        return Err(invalid("refresh materialized view complete OUT OF PLACE cutover: base table materialized view metadata missing"));
    };
    let ids: Vec<i64> = info.mview_ids.iter().copied().collect();
    let (updated, found) = replace_materialized_view_id(&ids, old_mview_id, shadow_table_id);
    if !found {
        return Err(invalid(
            "refresh materialized view complete OUT OF PLACE cutover: old materialized view id is missing in base table metadata",
        ));
    }
    let mut info = info;
    info.mview_ids = updated.into();
    base.materialized_view_base = Some(GoShared::new(info));
    Ok(())
}

/// Go master `(*worker).acquireMLogPurgeAdvisoryLockForCutover`: cancels the
/// log's running purge (committed on its own session so the purge monitor
/// sees it) and takes the purge advisory lock, retrying within a one-minute
/// budget.
fn acquire_mlog_purge_advisory_lock_for_cutover(
    worker: &DdlStepWorker,
    active: &tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    mlog_id: i64,
) -> Result<(), DdlPlanError> {
    let mut timeout = MVIEW_CUTOVER_PURGE_LOCK_TIMEOUT;
    if let Some(value) = tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/mockMViewOutOfPlacePurgeAdvisoryLockTimeout") {
        timeout = std::time::Duration::from_millis(u64::try_from(value.int()).unwrap_or(0));
    }
    let deadline = std::time::Instant::now() + timeout;
    let timed_out = || {
        DdlPlanError::from(tidb_exeerrors::ERR_MV_ADVISORY_LOCK_CONFLICT.fast_gen_by_args(&[format!(
            "refresh materialized view complete OUT OF PLACE cutover for {}.{}: timed out waiting for materialized view log purge; retry refresh",
            active.job.schema_name, active.job.table_name
        )
        .into()]))
    };
    let request = worker.sess_pool.get().map_err(DdlPlanError::from_mysql_error)?;
    let kctx = tikv_client::util::with_internal_source_type(
        &tikv_client::trace::TraceContext::new(),
        tidb_txnkv::INTERNAL_TXN_DDL,
    );
    let lock_name = tidb_mviewutil::purge_advisory_lock_name(mlog_id);
    loop {
        if std::time::Instant::now() >= deadline {
            return Err(timed_out());
        }
        request
            .get_restricted_sql_executor()
            .exec_restricted_sql(
                &kctx,
                &[std::sync::Arc::new(tidb_sqlexec::exec_option_use_cur_session)],
                "UPDATE mysql.tidb_mlog_purge_hist
SET CANCEL_REQUEST_TIME = NOW(6),
	CANCEL_REQUESTED_BY = 'materialized view cutover'
WHERE MLOG_ID = %?
	AND PURGE_STATUS = 'running'
	AND CANCEL_REQUEST_TIME IS NULL",
                &[tidb_sqlescape::SqlArg::Signed(mlog_id)],
            )
            .map_err(|error| DdlPlanError::plain_refusal(error.to_string()))?;
        match worker.get_advisory_lock(&lock_name, MVIEW_CUTOVER_PURGE_LOCK_WAIT_TIMEOUT_SEC) {
            // "The scheduler releases job-scoped advisory locks after schema
            // sync, including on cancellation or failure. Retain this lock
            // across retries." Go's deferred check reports a spent budget even
            // when the acquisition succeeds.
            Ok(()) if std::time::Instant::now() >= deadline => return Err(timed_out()),
            Ok(()) => return Ok(()),
            Err(tidb_sessionctx::advisory_lock_state::AdvisoryLockError::Timeout) => {}
            Err(error) => return Err(DdlPlanError::plain_refusal(error.to_string())),
        }
        // "A purge can acquire its lock before publishing running history, or
        // a new purge can start between cancellation and acquisition. Cancel
        // again."
        tidb_failpoint::inject_call(
            "github.com/pingcap/tidb/pkg/ddl/mViewOutOfPlacePurgeAdvisoryLockRetry",
            &mut mlog_id.clone(),
        );
    }
}

/// Go master `mviewCutoverPurgeLockWaitTimeoutSec`.
const MVIEW_CUTOVER_PURGE_LOCK_WAIT_TIMEOUT_SEC: u64 = 5;
/// Go master `mviewCutoverPurgeLockTimeout`.
const MVIEW_CUTOVER_PURGE_LOCK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// Go master `(*worker).cleanupMViewOutOfPlaceCutoverAfterCommit`, as a
/// done cutover job finishes: best effort, each failure only logged -- the
/// old view's refresh alert row, its affinity group and its TiFlash sync
/// progress.
pub(super) fn cleanup_mview_out_of_place_cutover_after_commit(
    job: &tidb_model::Job,
    session: &crate::job_txn::StepSession,
    start_ts: u64,
) {
    let warn = |message: &str, fields: Vec<tidb_log::Field>| tidb_ddl_logutil::ddl_logger().warn(message, &fields);
    let args = match tidb_model::get_refresh_materialized_view_complete_out_of_place_cutover_args(&mut job.clone()) {
        Ok(Some(args)) => args.read().clone(),
        Ok(None) => return,
        Err(error) => {
            warn(
                "failed to decode materialized view cutover args for post-commit cleanup",
                vec![
                    tidb_log::Field::new("jobID", tidb_log::Value::I64(job.id)),
                    tidb_log::Field::new("error", tidb_log::Value::Str(error.to_string())),
                ],
            );
            return;
        }
    };
    let old = args.old_mview_id;
    if let Err(error) = session.execute(
        start_ts,
        "DELETE FROM mysql.tidb_mview_refresh_alert WHERE MVIEW_ID = %?",
        "mview-refresh-cutover-delete-refresh-alert",
        &[tidb_sqlescape::SqlArg::Signed(old)],
    ) {
        warn(
            "failed to delete stale materialized view refresh alert after cutover",
            vec![
                tidb_log::Field::new("tableID", tidb_log::Value::I64(old)),
                tidb_log::Field::new("error", tidb_log::Value::Str(error.to_string())),
            ],
        );
    }
    let group = crate::ddl_affinity::get_table_affinity_group_id(old);
    if let Err(error) = tidb_domain_affinity::delete_groups_with_retry(&[group]) {
        warn(
            "failed to delete old materialized view affinity group after cutover",
            vec![
                tidb_log::Field::new("tableID", tidb_log::Value::I64(old)),
                tidb_log::Field::new("error", tidb_log::Value::Str(error.to_string())),
            ],
        );
    }
    // "Stage-1 MVs and MLogs are non-partitioned. Retain their progress
    // until cutover commits, then remove the entries for both retired
    // physical IDs."
    for id in [old, args.old_mlog_id] {
        if id == 0 {
            continue;
        }
        tidb_domain_infosync::tiflash_manager::delete_tiflash_table_sync_progress(&TableInfo { id, ..TableInfo::default() });
    }
}

#[cfg(test)]
mod shadow_expiration_tests {
    /// Go master `TestMaterializedViewShadowTableExpiration`.
    #[test]
    fn test_materialized_view_shadow_table_expiration() {
        let now = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let cutoff = now - chrono::Duration::days(7);
        let name = |created: chrono::DateTime<chrono::Utc>| format!("__mv_shadow_1_{}", created.timestamp_nanos_opt().unwrap());
        let nanosecond = chrono::Duration::nanoseconds(1);
        for (table, expired) in [
            (name(cutoff - nanosecond), true),
            (name(cutoff), true),
            (name(cutoff + nanosecond), false),
            (name(now + chrono::Duration::hours(1)), false),
            ("__mv_shadow_1".to_owned(), false),
            ("__mv_shadow_1_invalid".to_owned(), false),
            ("__mv_shadow_1_0".to_owned(), false),
            ("__mv_shadow_1_-1".to_owned(), false),
            ("__mv_shadow_1_9223372036854775808".to_owned(), false),
            (name(cutoff) + "_suffix", false),
            (name(cutoff).trim_start_matches("__").to_owned(), false),
        ] {
            assert_eq!(super::is_materialized_view_shadow_table_expired(&table, now), expired, "{table}");
        }
    }
}

#[cfg(test)]
mod build_sql_tests {
    /// Go master `TestBuildCreateMaterializedViewImportSQL` (`ddl_test.go`).
    #[test]
    fn test_build_create_materialized_view_import_sql() {
        let mut mview = super::TableInfo { name: tidb_ast::CiString::new("mv"), ..Default::default() };
        mview.materialized_view = Some(tidb_model::GoShared::new(tidb_model::table::MaterializedViewInfo {
            sql_content: "select a, count(1) from t group by a".to_owned(),
            ..Default::default()
        }));
        let sql = super::build_create_materialized_view_import_sql("test", &mview, 0, "").unwrap();
        assert!(sql.contains("IMPORT INTO `test`.`mv` FROM ("), "{sql}");
        assert!(sql.contains("WITH disable_precheck"), "{sql}");
        assert!(!sql.to_uppercase().contains("AS OF TIMESTAMP"), "{sql}");
        let sql = super::build_create_materialized_view_import_sql("test", &mview, 0, "100gib").unwrap();
        assert!(sql.contains("WITH disable_precheck, disk_quota='100gib'"), "{sql}");
        let sql = super::build_create_materialized_view_import_sql("test", &mview, 12, "64gib").unwrap();
        assert!(sql.contains("WITH disable_precheck, thread=12, disk_quota='64gib'"), "{sql}");
    }
}

/// Go master `pkg/ddl/mview_worker_test.go` (its internal-function tests).
#[cfg(test)]
mod mview_worker_tests {
    use super::FieldTypeFlags;
    use tidb_datatype::{FieldType, FieldTypeCode};

    fn usage(sql: &str, column: &str) -> super::MvColumnUsage {
        super::analyze_mv_column_usage(&super::parse_select_from_sql(sql).unwrap(), column)
    }

    #[test]
    fn test_analyze_mv_column_usage_group_by_alias() {
        let usage = usage("select a as k, count(1) from t group by k", "a");
        assert_eq!(usage.unsupported_reason, "");
        assert_eq!(usage.direct_output_offsets, vec![0]);
        assert!(usage.is_group_key);
    }

    #[test]
    fn test_analyze_mv_column_usage_where_reference_unsupported() {
        let usage = usage("select a, count(1) from t where b > 0 group by a", "b");
        assert_eq!(usage.unsupported_reason, "WHERE clause");
        assert!(usage.direct_output_offsets.is_empty());
        assert!(!usage.is_group_key);
    }

    #[test]
    fn test_analyze_mv_column_usage_group_by_ordinal() {
        let usage = usage("select a, count(1) from t group by 1", "a");
        assert_eq!(usage.unsupported_reason, "");
        assert_eq!(usage.direct_output_offsets, vec![0]);
        assert!(usage.is_group_key);
    }

    #[test]
    fn test_field_type_for_mv_related_column_clears_base_only_flags() {
        let mut old_related_ft = FieldType::new(FieldTypeCode::Long);
        old_related_ft.add_flags(FieldTypeFlags::UNIQUE_KEY);
        let old_related = tidb_model::ColumnInfo { field_type: old_related_ft, ..Default::default() };
        let mut base_new_ft = FieldType::new(FieldTypeCode::LongLong);
        base_new_ft.add_flags(
            FieldTypeFlags::NOT_NULL
                | FieldTypeFlags::AUTO_INCREMENT
                | FieldTypeFlags::ON_UPDATE_NOW
                | FieldTypeFlags::PREVENT_NULL_INSERT
                | FieldTypeFlags::GENERATED_COLUMN
                | FieldTypeFlags::PRI_KEY,
        );
        let base_new = tidb_model::ColumnInfo { field_type: base_new_ft, ..Default::default() };
        let new_field_type = super::field_type_for_mv_related_column(&old_related, &base_new);
        assert!(new_field_type.has_flag(FieldTypeFlags::NOT_NULL));
        assert!(new_field_type.has_flag(FieldTypeFlags::UNIQUE_KEY));
        for cleared in [
            FieldTypeFlags::PRI_KEY,
            FieldTypeFlags::AUTO_INCREMENT,
            FieldTypeFlags::ON_UPDATE_NOW,
            FieldTypeFlags::PREVENT_NULL_INSERT,
            FieldTypeFlags::GENERATED_COLUMN,
        ] {
            assert!(!new_field_type.has_flag(cleared), "{cleared:#x}");
        }
    }

    /// Go master `TestInitCreateMaterializedViewBuildSessionAppliesDefinitionDivPrecisionIncrement`.
    #[test]
    fn test_init_create_materialized_view_build_session_applies_definition_div_precision_increment() {
        use tidb_vardef::tidb_vars;
        let mut sess_vars = tidb_variable::SessionVars::new();
        sess_vars.set_system_var("div_precision_increment", "2".to_owned()).unwrap();
        sess_vars.current_db = "before".to_owned();
        let mview = tidb_model::TableInfo {
            materialized_view: Some(tidb_model::GoShared::new(tidb_model::table::MaterializedViewInfo {
                definition_div_precision_increment: 9,
                definition_sql_mode: i64::try_from(tidb_mysql::consts::ModePadCharToFullLength.0).unwrap(),
                definition_time_zone: tidb_model::TimeZoneLocation::new("UTC", 0),
                ..Default::default()
            })),
            ..Default::default()
        };
        // The submitter has loaded the global variables that track and spill
        // the memory of aggregations, while the internal session has not.
        let mut submitter = tidb_variable::SessionVars::new();
        submitter.set_system_var(tidb_vars::TIDB_TRACK_AGGREGATE_MEMORY_USAGE, "ON".to_owned()).unwrap();
        submitter.set_system_var(tidb_vars::TIDB_ENABLE_PARALLEL_HASHAGG_SPILL, "ON".to_owned()).unwrap();
        let job_session_vars = tidb_variable::mview_execution::capture_m_view_execution_session_vars(&submitter)
            .job_system_vars()
            .into_iter()
            .map(|(name, value)| (name.to_owned(), value))
            .collect();
        sess_vars.set_system_var(tidb_vars::TIDB_TRACK_AGGREGATE_MEMORY_USAGE, "OFF".to_owned()).unwrap();
        sess_vars.set_system_var(tidb_vars::TIDB_ENABLE_PARALLEL_HASHAGG_SPILL, "OFF".to_owned()).unwrap();
        let build = super::MViewBuild {
            job_id: 1,
            schema_name: "test".to_owned(),
            mview: Box::new(mview),
            job_session_vars,
            resource_group: "rg1".to_owned(),
        };
        let names = ["div_precision_increment", "sql_mode", "time_zone"];
        let before: Vec<String> = names.iter().map(|name| sess_vars.get_system(name).unwrap()).collect();
        let sql_mode = sess_vars.sql_mode();
        let original_in_mview_maintenance = sess_vars.in_mview_maintenance;
        let original_resource_group = sess_vars.resource_group.clone();

        let restore = super::init_create_materialized_view_build_session(&mut sess_vars, &build, "test").unwrap();
        assert!(sess_vars.in_mview_maintenance);
        assert_eq!("test", sess_vars.current_db);
        assert_eq!("9", sess_vars.get_system("div_precision_increment").unwrap());
        assert_eq!("PAD_CHAR_TO_FULL_LENGTH", sess_vars.get_system("sql_mode").unwrap());
        assert_eq!("UTC", sess_vars.get_system("time_zone").unwrap());
        assert_eq!("rg1", sess_vars.resource_group);
        assert_eq!("ON", sess_vars.get_system(tidb_vars::TIDB_TRACK_AGGREGATE_MEMORY_USAGE).unwrap());
        assert_eq!("ON", sess_vars.get_system(tidb_vars::TIDB_ENABLE_PARALLEL_HASHAGG_SPILL).unwrap());

        restore.restore(&mut sess_vars);
        assert_eq!(original_in_mview_maintenance, sess_vars.in_mview_maintenance);
        assert_eq!("2", sess_vars.get_system("div_precision_increment").unwrap());
        assert_eq!("before", sess_vars.current_db);
        assert_eq!(sql_mode, sess_vars.sql_mode());
        assert_eq!(original_resource_group, sess_vars.resource_group);
        assert_eq!("OFF", sess_vars.get_system(tidb_vars::TIDB_TRACK_AGGREGATE_MEMORY_USAGE).unwrap());
        assert_eq!("OFF", sess_vars.get_system(tidb_vars::TIDB_ENABLE_PARALLEL_HASHAGG_SPILL).unwrap());
        // Restore the original cached values independently of the session fields.
        for (name, value) in names.iter().zip(&before) {
            assert_eq!(value, &sess_vars.get_system(name).unwrap(), "{name}");
        }

        // The cached value is synchronized even when it already differs from
        // the definition's.
        let mut sess_vars = tidb_variable::SessionVars::new();
        sess_vars.set_system_var("div_precision_increment", "4".to_owned()).unwrap();
        let restore = super::init_create_materialized_view_build_session(&mut sess_vars, &build, "test").unwrap();
        assert_eq!("9", sess_vars.get_system("div_precision_increment").unwrap());
        restore.restore(&mut sess_vars);
    }

    #[test]
    fn test_init_m_view_session() {
        let mut sess_vars = tidb_variable::SessionVars::new();
        let original = [("sql_mode", ""), ("time_zone", "+00:00"), ("div_precision_increment", "2")];
        for (name, value) in original {
            sess_vars.set_system_var(name, value.to_owned()).unwrap();
        }
        sess_vars.current_db = "before".to_owned();
        sess_vars.resource_group = "before_group".to_owned();
        let info = tidb_model::table::MaterializedViewInfo {
            definition_sql_mode: i64::try_from(tidb_mysql::consts::ModeNoBackslashEscapes.0).unwrap(),
            definition_div_precision_increment: 9,
            definition_time_zone: tidb_model::TimeZoneLocation::new("Asia/Shanghai", 0),
            ..Default::default()
        };
        let restore = super::init_m_view_session(&mut sess_vars, &info, "mv_db", "mv_group").unwrap();
        assert!(sess_vars.enable_mview());
        assert_eq!("mv_db", sess_vars.current_db);
        assert_eq!("mv_group", sess_vars.resource_group);
        assert_eq!("9", sess_vars.get_system("div_precision_increment").unwrap());
        assert!(sess_vars.sql_mode().has_no_backslash_escapes_mode());
        let mut worker_vars = tidb_variable::SessionVars::new();
        for (name, expected) in [("sql_mode", "NO_BACKSLASH_ESCAPES"), ("time_zone", "Asia/Shanghai"), ("div_precision_increment", "9")] {
            let (value, keep) = sess_vars.get_session_states_system_var(name).unwrap();
            assert!(keep, "{name}");
            assert_eq!(expected, value, "{name}");
            worker_vars.set_system_var(name, value).unwrap();
        }
        assert_eq!(sess_vars.sql_mode(), worker_vars.sql_mode());
        assert_eq!(sess_vars.session_time_zone(), worker_vars.session_time_zone());
        assert_eq!(
            sess_vars.get_system("div_precision_increment").unwrap(),
            worker_vars.get_system("div_precision_increment").unwrap()
        );
        restore.restore(&mut sess_vars);
        assert!(!sess_vars.enable_mview());
        assert_eq!("before", sess_vars.current_db);
        assert_eq!("before_group", sess_vars.resource_group);
        assert_eq!("2", sess_vars.get_system("div_precision_increment").unwrap());
        assert!(!sess_vars.sql_mode().has_no_backslash_escapes_mode());
        for (name, expected) in original {
            let (value, keep) = sess_vars.get_session_states_system_var(name).unwrap();
            assert!(keep, "{name}");
            assert_eq!(expected, value, "{name}");
        }
    }

    #[test]
    fn test_format_sql_mode() {
        for s in ["", tidb_mysql::consts::DefaultSQLMode, "ANSI", "TRADITIONAL", "PAD_CHAR_TO_FULL_LENGTH,NO_BACKSLASH_ESCAPES"] {
            let mode = tidb_mysql::consts::get_sql_mode(&tidb_mysql::consts::format_sql_mode_str(s)).unwrap();
            let got = tidb_mysql::consts::get_sql_mode(&super::format_sql_mode(i64::try_from(mode.0).unwrap())).unwrap();
            assert_eq!(mode, got, "{s}");
        }
    }

    struct EmptySnapshot;

    impl tidb_infoschema::cluster_catalog::MetaSnapshot for EmptySnapshot {
        fn get(&mut self, _: &[u8]) -> Result<Option<Vec<u8>>, tidb_infoschema::cluster_catalog::ClusterCatalogError> {
            Ok(None)
        }

        fn scan_prefix(
            &mut self,
            _: &[u8],
        ) -> Result<tidb_infoschema::cluster_catalog::MetaPairs, tidb_infoschema::cluster_catalog::ClusterCatalogError> {
            Ok(Vec::new())
        }
    }

    /// Go `mockstore.NewMockStore()` + `kv.RunInNewTxn` + `meta.NewMutator`.
    fn run_in_new_txn(f: impl FnOnce(&mut super::JobTxn<'_>)) {
        let mut store = EmptySnapshot;
        let mut txn = super::JobTxn::new(&mut store, 1, crate::job_txn::unused_step_session(crate::StartMode::Normal));
        f(&mut txn);
    }

    fn create_test_database(txn: &mut super::JobTxn<'_>) {
        let db = tidb_model::db::DBInfo { id: 1, name: tidb_ast::CiString::new("test"), ..Default::default() };
        txn.meta().create_database(&db).unwrap();
    }

    fn table(id: i64, name: &str) -> super::TableInfo {
        super::TableInfo { id, name: tidb_ast::CiString::new(name), ..Default::default() }
    }

    fn new_job(schema_id: i64, table_id: i64, state: super::JobState) -> super::Job {
        let mut job = super::Job::default();
        job.schema_id = schema_id;
        job.table_id = table_id;
        job.state = state;
        job
    }

    fn mview_of(id: i64, base_table_ids: Vec<i64>) -> super::TableInfo {
        let mut mview = table(id, "");
        mview.materialized_view = Some(tidb_model::GoShared::new(tidb_model::table::MaterializedViewInfo {
            base_table_ids: base_table_ids.into(),
            ..Default::default()
        }));
        mview
    }

    #[test]
    fn test_update_materialized_view_base_info_on_create_missing_base_table() {
        run_in_new_txn(|txn| {
            create_test_database(txn);
            let mut job = new_job(1, 0, super::JobState::RUNNING);
            let created_table = mview_of(2, vec![3]);
            let err = super::update_materialized_view_base_info_on_create(txn, &mut job, &created_table, &[], 0).unwrap_err();
            assert!(err.equal(&tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS), "{err:?}");
            assert_eq!(job.state, super::JobState::CANCELLED);
        });
    }

    #[test]
    fn test_get_table_info_and_cancel_non_exist_job() {
        run_in_new_txn(|txn| {
            create_test_database(txn);
            for (name, table_id, schema_id) in [("missing table", 2, 1), ("missing database", 2, 2)] {
                let mut job = new_job(0, 0, super::JobState::RUNNING);
                assert!(super::get_table_info_and_cancel_non_exist_job(txn, &mut job, table_id, schema_id).is_err(), "{name}");
                assert_eq!(job.state, super::JobState::CANCELLED, "{name}");
            }
        });
    }

    #[test]
    fn test_update_materialized_view_base_info_on_drop_propagates_get_table_error() {
        run_in_new_txn(|txn| {
            let job = new_job(1, 0, super::JobState::NONE);
            let dropping_table = mview_of(2, vec![3]);
            let err = super::update_materialized_view_base_info_on_drop(txn, &job, &dropping_table).unwrap_err();
            assert!(err.equal(&tidb_meta::meta_errors::ERR_DB_NOT_EXISTS), "{err:?}");
        });
    }

    #[test]
    fn test_update_materialized_view_base_info_on_drop_without_base_tables() {
        run_in_new_txn(|txn| {
            let dropping_table = mview_of(2, Vec::new());
            let job = new_job(0, dropping_table.id, super::JobState::NONE);
            let extra_infos = super::update_materialized_view_base_info_on_drop(txn, &job, &dropping_table).unwrap();
            assert!(extra_infos.is_empty());
        });
    }

    #[test]
    fn test_update_materialized_view_base_info_on_drop_m_log_dependency() {
        use tidb_model::table::{MaterializedViewBaseInfo, MaterializedViewLogInfo};
        let run_case = |dependent: bool, base_exists: bool, mlog_exists: bool, wrong_base_table_id: bool| {
            run_in_new_txn(|txn| {
                create_test_database(txn);
                let mut base = table(1, "base");
                base.materialized_view_base = Some(tidb_model::GoShared::new(MaterializedViewBaseInfo {
                    mlog_id: 2,
                    mview_ids: vec![3].into(),
                    ..Default::default()
                }));
                let mut log = MaterializedViewLogInfo { base_table_id: 1, ..Default::default() };
                if dependent {
                    log.dependent_mview_ids = vec![3].into();
                }
                if wrong_base_table_id {
                    log.base_table_id = 4;
                }
                let mut mlog = table(2, "$mlog$base");
                mlog.materialized_view_log = Some(tidb_model::GoShared::new(log));
                if base_exists {
                    txn.meta().create_table_or_view(1, &base).unwrap();
                }
                if mlog_exists {
                    txn.meta().create_table_or_view(1, &mlog).unwrap();
                }
                let dropping_table = mview_of(3, vec![1]);
                let job = new_job(1, dropping_table.id, super::JobState::NONE);
                let extra_infos = super::update_materialized_view_base_info_on_drop(txn, &job, &dropping_table).unwrap();
                if !base_exists {
                    assert!(extra_infos.is_empty());
                    return;
                }
                if !dependent {
                    assert_eq!(extra_infos.len(), 1);
                    return;
                }
                if !mlog_exists || wrong_base_table_id {
                    assert_eq!(extra_infos.len(), 1);
                    assert!(extra_infos[0].1.materialized_view_base.as_ref().unwrap().read().mview_ids.is_empty());
                    return;
                }
                assert_eq!(extra_infos.len(), 2);
                assert!(extra_infos[1].1.materialized_view_log.as_ref().unwrap().read().dependent_mview_ids.is_empty());
            });
        };
        // "dependent mlog is updated"
        run_case(true, true, true, false);
        // "unrelated mlog is not updated"
        run_case(false, true, true, false);
        // "missing base table is skipped"
        run_case(true, false, true, false);
        // "missing mlog does not retry a started drop"
        run_case(true, true, false, false);
        // "mlog with wrong base table does not retry a started drop"
        run_case(true, true, true, true);
    }
}
