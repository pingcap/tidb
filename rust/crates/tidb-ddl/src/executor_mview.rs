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

//! Go master `pkg/ddl/materialized_view.go`'s executor half: the jobs the
//! materialized view statements submit.

use tidb_ast::CreateMaterializedViewLogStmt;
use tidb_ddl_build::session_context::DdlSessionContext;
use tidb_goruntime::go_runtime::GoSharedSlice;
use tidb_infoschema::SessionExtendedInfoSchema;
use tidb_model::{ActionType, GoShared, JobArgsValue};
use tidb_terror::MysqlError;

/// Go `checkMaterializedViewEnabled`.
///
/// # Errors
///
/// `tidb_mview_enable` is off.
pub(crate) fn check_materialized_view_enabled(context: &dyn DdlSessionContext) -> Result<(), MysqlError> {
    if !context.enable_mview() {
        return Err(MysqlError::from_terror(&tidb_dbterror::ERR_GENERAL_UNSUPPORTED_DDL.gen_with_stack(
            "Materialized View is disabled, please set `tidb_mview_enable` to `ON` to enable it",
        )));
    }
    Ok(())
}

/// The expression context Go's DDL builds a schedule expression in: no
/// columns, the session's time zone.
struct SessionExprContext<'a>(&'a dyn DdlSessionContext);

impl tidb_expr::rewriter::ColumnResolver for SessionExprContext<'_> {
    fn resolve(&self, _path: &[String]) -> Option<(usize, tidb_datatype::FieldType, i64)> {
        None
    }
    fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
        self.0.session_zone()
    }
}

/// Go `executor.CreateMaterializedViewLog` up to `DoDDLJobWrapper`: the job,
/// and the schema it creates the log in.
///
/// # Errors
///
/// Go's refusals, in Go's order.
pub(crate) fn create_materialized_view_log_job(
    catalog: &SessionExtendedInfoSchema,
    stmt: &CreateMaterializedViewLogStmt,
    current_db: &str,
    context: &dyn DdlSessionContext,
) -> Result<(crate::JobWrapper, String), MysqlError> {
    check_materialized_view_enabled(context)?;
    let (schema_name, table_name) = match stmt.table.as_slice() {
        [schema, table] => (schema.clone(), table.clone()),
        [table] => {
            if current_db.is_empty() {
                return Err(MysqlError::from_terror(&tidb_plannererrors::ERR_NO_DB.gen_with_stack_by_args(&[])));
            }
            (current_db.to_owned(), table.clone())
        }
        _ => return Err(MysqlError::unknown("invalid materialized view log table name".to_owned())),
    };
    let Some(schema) = catalog.schema_by_name(&schema_name) else {
        return Err(MysqlError::from_terror(
            &tidb_infoschema::infoschema_errors::ERR_DATABASE_NOT_EXISTS.gen_with_stack_by_args(&[schema_name.into()]),
        ));
    };
    let schema = schema.read().clone();
    let Some((_, base)) = catalog.table_info_by_name(&schema_name, &table_name) else {
        return Err(MysqlError::from_terror(
            &tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                .gen_with_stack_by_args(&[schema_name.clone().into(), table_name.clone().into()]),
        ));
    };
    let base = base.read().clone();
    if !crate::cluster_ddl::materialized_view::is_valid_materialized_view_log_base_table(
        &tidb_hack::go_to_lower(&schema_name),
        &base,
    ) {
        return Err(MysqlError::from_terror(&tidb_dbterror::ERR_WRONG_OBJECT.gen_with_stack_by_args(&[
            schema_name.clone().into(),
            table_name.clone().into(),
            "BASE TABLE".into(),
        ])));
    }
    if base.get_partition_info().is_some() {
        return Err(MysqlError::from_terror(&tidb_dbterror::ERR_GENERAL_UNSUPPORTED_DDL.gen_with_stack_by_args(&[
            "CREATE MATERIALIZED VIEW LOG on partition table".into(),
        ])));
    }
    let mlog_name = tidb_model::table::materialized_view_log_table_name(&base.name);
    if catalog.table_info_by_name(&schema_name, mlog_name.original()).is_some() {
        return Err(MysqlError::from_terror(&tidb_infoschema::infoschema_errors::ERR_TABLE_EXISTS.gen_with_stack_by_args(
            &[format!("{schema_name}.{}", mlog_name.original()).into()],
        )));
    }
    let mlog = tidb_ddl_build::materialized_view::build_materialized_view_log_table_info(
        &context.metabuild_context(),
        &SessionExprContext(context),
        &schema_name,
        &schema.charset,
        &schema.collate,
        &base,
        stmt,
        context.enable_check_constraint(),
    )
    .map_err(|error| error.to_mysql_error())?;

    let mut job = crate::cluster_ddl::session_job(context, ActionType::ACTION_CREATE_MATERIALIZED_VIEW_LOG);
    job.schema_id = schema.id;
    job.schema_name = schema.name.lowercase().to_owned().into();
    job.table_name = mlog.name.lowercase().to_owned().into();
    job.involving_schema_info = GoSharedSlice::from_vec(vec![
        tidb_model::InvolvingSchemaInfo {
            database: schema.name.lowercase().to_owned().into(),
            table: mlog.name.lowercase().to_owned().into(),
            ..tidb_model::InvolvingSchemaInfo::default()
        },
        tidb_model::InvolvingSchemaInfo {
            database: schema.name.lowercase().to_owned().into(),
            table: base.name.lowercase().to_owned().into(),
            ..tidb_model::InvolvingSchemaInfo::default()
        },
    ]);
    job.session_vars = Some(GoShared::new(std::collections::BTreeMap::from([(
        tidb_vardef::tidb_vars::TIDB_SCATTER_REGION.to_owned(),
        context.ddl_reorg_vars().scatter_region.clone().into(),
    )])));
    let args = tidb_model::CreateMaterializedViewLogArgs { table_info: Some(GoShared::new(mlog)) };
    Ok((
        crate::JobWrapper {
            job,
            args: JobArgsValue::CreateMaterializedViewLog(Some(GoShared::new(args))),
            id_allocated: false,
        },
        schema_name,
    ))
}

/// Go master `getMaterializedViewSchema`: the statement's schema, the
/// current database for an unqualified name.
fn materialized_view_schema(
    catalog: &SessionExtendedInfoSchema,
    path: &[String],
    current_db: &str,
) -> Result<(String, String), MysqlError> {
    let (schema, table) = match path {
        [schema, table] => (schema.clone(), table.clone()),
        [table] => {
            if current_db.is_empty() {
                return Err(MysqlError::from_terror(&tidb_plannererrors::ERR_NO_DB.gen_with_stack_by_args(&[])));
            }
            (current_db.to_owned(), table.clone())
        }
        _ => return Err(MysqlError::unknown("invalid materialized view table name".to_owned())),
    };
    if catalog.schema_by_name(&schema).is_none() {
        return Err(MysqlError::from_terror(
            &tidb_infoschema::infoschema_errors::ERR_DATABASE_NOT_EXISTS.gen_with_stack_by_args(&[schema.into()]),
        ));
    }
    Ok((schema, table))
}

/// What Go master's `executor.DropMaterializedViewLog` does before
/// `dropTableObject`: the log table to drop, or `None` for a missing log
/// under IF EXISTS, after its note.
pub(crate) fn drop_materialized_view_log_target(
    catalog: &SessionExtendedInfoSchema,
    stmt: &tidb_ast::DropMaterializedViewLogStmt,
    current_db: &str,
    context: &dyn DdlSessionContext,
) -> Result<Option<(String, String)>, MysqlError> {
    let (schema, table) = materialized_view_schema(catalog, &stmt.table, current_db)?;
    let Some((_, base)) = catalog.table_info_by_name(&schema, &table) else {
        return Err(MysqlError::from_terror(
            &tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                .gen_with_stack_by_args(&[schema.clone().into(), table.clone().into()]),
        ));
    };
    let base = base.read().clone();
    if !crate::cluster_ddl::materialized_view::is_valid_materialized_view_log_base_table(
        &tidb_hack::go_to_lower(&schema),
        &base,
    ) {
        return Err(MysqlError::from_terror(&tidb_dbterror::ERR_WRONG_OBJECT.gen_with_stack_by_args(&[
            schema.clone().into(),
            table.clone().into(),
            "BASE TABLE".into(),
        ])));
    }
    let mlog_name = tidb_model::table::materialized_view_log_table_name(&base.name).original().to_owned();
    let Some((_, mlog)) = catalog.table_info_by_name(&schema, &mlog_name) else {
        if stmt.if_exists {
            let note = tidb_infoschema::infoschema_errors::ERR_TABLE_DROP_EXISTS
                .gen_with_stack_by_args(&[format!("{schema}.{table}").into()]);
            let note = MysqlError::from_terror(&note);
            context.append_note_parts(note.code, &note.message);
            return Ok(None);
        }
        return Err(MysqlError::from_terror(
            &tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                .gen_with_stack_by_args(&[schema.clone().into(), mlog_name.into()]),
        ));
    };
    let mlog = mlog.read().clone();
    let log = mlog.materialized_view_log.as_ref().map(|log| log.read().clone());
    if log.as_ref().is_none_or(|log| log.base_table_id != base.id) {
        return Err(MysqlError::from_terror(&tidb_dbterror::ERR_WRONG_OBJECT.gen_with_stack_by_args(&[
            schema.clone().into(),
            mlog_name.into(),
            "MATERIALIZED VIEW LOG".into(),
        ])));
    }
    if log.is_some_and(|log| !log.dependent_mview_ids.is_empty()) {
        return Err(MysqlError::unknown(format!(
            "cannot drop materialized view log on {schema}.{}: dependent materialized views exist",
            table
        )));
    }
    Ok(Some((schema, mlog_name)))
}
