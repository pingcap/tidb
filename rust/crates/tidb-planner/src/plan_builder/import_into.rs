//! Go `pkg/planner/core/planbuilder.go`: `PlanBuilder.buildImportInto`.

use super::{
    PlanBuilder,
    insert_values::InsertTarget,
    visit_info::{VisitAuthError, VisitInfo},
};
use crate::common_plans::{ImportInto, LoadDataOpt, Plan};
use std::collections::BTreeMap;
use tidb_ast::{ImportIntoStmt, ImportSource, TableRef};
use tidb_exprctx::Columns;
use tidb_infoschema::table_source::TableSource;
use tidb_planner_core_base::PlanError;

impl<S: TableSource, C: Columns> PlanBuilder<'_, S, C> {
    /// Go `PlanBuilder.buildImportInto`. The bridge optimizes `SelectPlan`
    /// after this build, with the same `optFlag`, as for other query plans.
    pub fn build_import_into(&mut self, stmt: &ImportIntoStmt) -> Result<Plan, PlanError> {
        let (path, format, local) = match &stmt.source {
            ImportSource::File { path, format } => {
                let local = crate::import_into::import_from_server(path);
                (path.clone(), format.clone(), local)
            }
            ImportSource::Select { .. } => (String::new(), None, false),
        };
        let mock = self.build_table_dual();
        let mut options = Vec::with_capacity(stmt.options.len());
        for option in &stmt.options {
            options.push(LoadDataOpt {
                name: option.name.clone(),
                value: option
                    .value
                    .as_ref()
                    .map(|value| self.rewrite_scalar_with_plan(value, &mock, &BTreeMap::new()))
                    .transpose()?,
            });
        }
        crate::import_into::check_import_into_input_vars(stmt)?;
        let (database, name) = match stmt.table.as_slice() {
            [name] if !self.source.current_database().is_empty() => {
                (self.source.current_database(), name.as_str())
            }
            [database, name] => (database.as_str(), name.as_str()),
            _ => {
                return Err(PlanError::from(
                    tidb_plannererrors::ERR_NO_DB.gen_with_stack_by_args(&[]),
                ));
            }
        };
        let table = self
            .source
            .find_table(database, name)
            .ok_or_else(|| {
                PlanError::from(
                    tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                        .fast_gen_by_args(&[database.into(), name.into()]),
                )
            })?
            .clone();
        if table.is_temporary() {
            return Err(PlanError::internal(
                "IMPORT INTO does not support temporary table",
            ));
        }
        if table.is_cached() {
            return Err(PlanError::internal(
                "IMPORT INTO does not support cached table",
            ));
        }
        for (privilege, command) in [
            (tidb_mysql::SelectPriv, "SELECT"),
            (tidb_mysql::UpdatePriv, "UPDATE"),
            (tidb_mysql::InsertPriv, "INSERT"),
            (tidb_mysql::DeletePriv, "DELETE"),
            (tidb_mysql::AlterPriv, "ALTER"),
        ] {
            self.visit_info.push(VisitInfo {
                privilege,
                database: tidb_hack::go_to_lower(database),
                table: tidb_hack::go_to_lower(name),
                column: String::new(),
                error: VisitAuthError::TableAccessDenied {
                    command,
                    table: name.to_owned(),
                },
                dynamic_privs: Vec::new(),
                dynamic_with_grant: false,
            });
        }
        if local {
            self.visit_info.push(VisitInfo {
                privilege: tidb_mysql::FilePriv,
                database: String::new(),
                table: String::new(),
                column: String::new(),
                error: VisitAuthError::SpecificAccessDenied { privilege: "FILE" },
                dynamic_privs: Vec::new(),
                dynamic_with_grant: false,
            });
        }
        if self
            .source
            .latest_index_schema()
            .is_some_and(|latest| !latest.table_indexes.contains_key(&table.table_id()))
        {
            if self.session_ctx.is_some_and(|ctx| ctx.snapshot_ts() != 0) {
                return Err(PlanError::internal(
                    "can not execute IMPORT statement when 'tidb_snapshot' is set",
                ));
            }
            return Err(PlanError::from(
                tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                    .fast_gen_by_args(&[self.source.current_database().into(), name.into()]),
            ));
        }
        let target = InsertTarget {
            database: table.db_name.clone(),
            table: table.table_name(),
            columns: Vec::new(),
            generated: Vec::new(),
            offsets: Vec::new(),
            extra_handle_offset: None,
        };
        let gen_cols = self
            .resolve_generated_columns(&target)?
            .into_iter()
            .flatten()
            .collect();
        let (schema, names, select_plan) = match &stmt.source {
            ImportSource::Select { query, .. } => {
                let select = self.build_query_stmt(query, false)?;
                let expected = if stmt.columns_and_user_vars.is_empty() {
                    table
                        .columns
                        .iter()
                        .filter(|column| {
                            let column = column.read();
                            !column.hidden && column.state == tidb_model::SchemaState::PUBLIC
                        })
                        .count()
                } else {
                    stmt.columns_and_user_vars.len()
                };
                if select.schema().map_or(0, |schema| schema.columns.len()) != expected {
                    return Err(PlanError::from(
                        tidb_plannererrors::ERR_WRONG_VALUE_COUNT_ON_ROW
                            .gen_with_stack_by_args(&[1.into()]),
                    ));
                }
                (
                    Default::default(),
                    Vec::new(),
                    Some(Box::new(Plan::Logical(select))),
                )
            }
            ImportSource::File { .. } => {
                let (schema, names) = crate::show_schema::build_import_into_schema();
                (schema, names, None)
            }
        };
        Ok(Plan::ImportInto(Box::new(ImportInto {
            table: tidb_resolve::TableNameW {
                table_name: TableRef {
                    identity: Default::default(),
                    name: vec![table.db_name.clone(), table.table_name()],
                    partitions: Vec::new(),
                    alias: None,
                    as_of: None,
                    hints: Vec::new(),
                    sample: None,
                },
                db_info: table.db_info,
                table_info: table.table_info,
            },
            column_assignments: stmt.column_assignments.clone(),
            columns_and_user_vars: stmt.columns_and_user_vars.clone(),
            path,
            format,
            options,
            gen_cols,
            select_plan,
            schema,
            names,
            stmt: tidb_ast::Stmt::Dml(tidb_ast::NodeBox::new(tidb_ast::DmlStmt::ImportInto(
                Box::new(stmt.clone()),
            )))
            .restore(),
        })))
    }
}
