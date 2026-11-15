// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go's masking-policy DDL executor functions (`ddl/executor.go`):
//! `CreateMaskingPolicy`, `AddMaskingPolicy`, `AlterTableMaskingPolicyState`,
//! `DropMaskingPolicy`, `ModifyMaskingPolicyExpression`,
//! `ModifyMaskingPolicyRestrictOn` and `checkMaskingPolicyOnModifyColumn`,
//! with `buildMaskingPolicyInfo` (`ddl/masking_policy.go`), over the
//! executor's latest schema and its masking-policy cache.

use crate::cluster_ddl::masking_policy::{
    check_masking_policy_column, check_masking_policy_table, is_masking_policy_supported_type,
};
use crate::OnExist;
use tidb_ast::{
    AlterMaskingPolicyAction, AlterTableAction, AlterTableStmt, CiString, CreateMaskingPolicyStmt,
    Expr, MaskingPolicyRestrictOps, MaskingPolicyState,
};
use tidb_ddl_build::session_context::DdlSessionContext;
use tidb_expr::EvalErrorSql;
use tidb_hack::GoToLower;
use tidb_infoschema::SessionExtendedInfoSchema;
use tidb_model::{
    ColumnInfo, MaskingPolicyInfo, MaskingPolicyStatus, MaskingPolicyType, SchemaState, TableInfo,
};
use tidb_table::generated_column::TableColumnResolver;
use tidb_terror::MysqlError;

/// Go's `errors.Errorf` messages, which reach the client as 1105.
fn plain_error(message: String) -> MysqlError {
    MysqlError::unknown(message)
}

/// Go `maskingPolicyStatusFromState`.
fn masking_policy_status_from_state(state: MaskingPolicyState) -> MaskingPolicyStatus {
    match state {
        MaskingPolicyState::Disabled => MaskingPolicyStatus::DISABLE,
        MaskingPolicyState::ImplicitEnabled | MaskingPolicyState::Enabled => {
            MaskingPolicyStatus::ENABLE
        }
    }
}

/// Go `maskingPolicyTypeFromExpr`.
fn masking_policy_type_from_expr(expr: &Expr) -> MaskingPolicyType {
    let Expr::Func { name, .. } = expr else {
        return MaskingPolicyType::CUSTOM;
    };
    match name.go_to_lower().as_str() {
        "mask_full" => MaskingPolicyType::FULL,
        "mask_partial" => MaskingPolicyType::PARTIAL,
        "mask_null" => MaskingPolicyType::NULL,
        "mask_date" => MaskingPolicyType::DATE,
        _ => MaskingPolicyType::CUSTOM,
    }
}

/// Go `table.FindCol(tbl.Cols(), name)`: the public column of that name.
fn find_col(table: &TableInfo, name: &str) -> Option<ColumnInfo> {
    let name = name.go_to_lower();
    table
        .columns
        .iter_deref()
        .map(|column| column.read().clone_like_go())
        .find(|column| column.state == SchemaState::PUBLIC && column.name.lowercase() == name)
}

/// Go `getMaskingPolicyByNameForTable`: the policy of that name on one of
/// the table's columns.
fn masking_policy_by_name_for_table<'a>(
    policies: &'a [MaskingPolicyInfo],
    table: &TableInfo,
    name: &str,
) -> Option<&'a MaskingPolicyInfo> {
    let name = name.go_to_lower();
    table.columns.iter_deref().find_map(|column| {
        let column_id = column.read().id;
        policies.iter().find(|policy| {
            policy.table_id == table.id
                && policy.column_id == column_id
                && policy.name.lowercase() == name
        })
    })
}

fn masking_policy_not_exists(name: &str) -> MysqlError {
    plain_error(format!("masking policy {name} doesn't exist"))
}

/// Go's masking-policy executor functions over the executor's latest
/// schema (`e.infoCache.GetLatest()`) and the statement's session context.
struct MaskingExec<'a> {
    catalog: &'a SessionExtendedInfoSchema,
    current_db: &'a str,
    context: &'a dyn DdlSessionContext,
}

impl MaskingExec<'_> {
    /// Go `executor.getSchemaAndTableByIdent` over the latest schema.
    fn masking_policy_schema_and_table(
        &self,
        path: &[String],
    ) -> Result<(CiString, TableInfo), MysqlError> {
        let (database, name) = tidb_infoschema::split_table_path_pub(path, &self.current_db)
            .map_err(|error| error.to_mysql_error())?;
        let catalog = self.catalog;
        let Some(schema) = catalog.schema_by_name(database) else {
            return Err(MysqlError::from_terror(
                &tidb_infoschema::infoschema_errors::ERR_DATABASE_NOT_EXISTS
                    .gen_with_stack_by_args(&[database.into()]),
            ));
        };
        let Some((_, table)) = catalog.table_info_by_name(database, name) else {
            return Err(MysqlError::from_terror(
                &tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                    .gen_with_stack_by_args(&[database.into(), name.into()]),
            ));
        };
        let schema_name = schema.read().name.clone();
        let table = table.read().clone_like_go();
        Ok((schema_name, table))
    }

    /// Go `InfoSchema.AllMaskingPolicies`: load once at the schema snapshot.
    fn masking_policies(&self) -> Result<Vec<MaskingPolicyInfo>, MysqlError> {
        Ok(self.catalog.all_masking_policies())
    }

    /// Go `validateMaskingPolicyExpression`: the expression builds against
    /// the table and names no column but its target.
    fn validate_masking_policy_expression(
        &self,
        table: &TableInfo,
        target: &ColumnInfo,
        expr: &Expr,
    ) -> Result<(), MysqlError> {
        let (names, types): (Vec<String>, Vec<tidb_datatype::FieldType>) = table
            .columns
            .iter_deref()
            .map(|column| {
                let column = column.read();
                (column.name.original().to_owned(), column.field_type.clone())
            })
            .unzip();
        let resolver = TableColumnResolver::new(&names, &types, self.context.session_zone())
            .with_current_database(&self.current_db)
            .with_source_table(table.name.original());
        tidb_expr::rewriter::rewrite_expr_resolved(expr, &resolver)
            .map_err(|error| error.to_mysql_error())?;
        if resolver
            .dependency_names()
            .iter()
            .any(|name| name.go_to_lower() != target.name.lowercase())
        {
            return Err(MysqlError::from_terror(
                &tidb_meta::meta_errors::ERR_MASKING_POLICY_EXPR_INVALID_COLUMN
                    .fast_gen_by_args(&[target.name.original().into()]),
            ));
        }
        Ok(())
    }

    /// Go `buildMaskingPolicyInfo`.
    #[allow(clippy::too_many_arguments)]
    fn build_masking_policy_info(
        &self,
        schema: &CiString,
        table: &TableInfo,
        policy_name: &str,
        column_name: &str,
        expr: &Expr,
        restrict_ops: MaskingPolicyRestrictOps,
        state: MaskingPolicyState,
    ) -> Result<MaskingPolicyInfo, MysqlError> {
        check_masking_policy_table(schema, table)
            .map_err(|error| MysqlError::from_terror(&error))?;
        let Some(column) = find_col(table, column_name) else {
            return Err(MysqlError::from_terror(
                &tidb_infoschema::infoschema_errors::ERR_COLUMN_NOT_EXISTS
                    .gen_with_stack_by_args(&[column_name.into(), table.name.original().into()]),
            ));
        };
        check_masking_policy_column(&column).map_err(|error| MysqlError::from_terror(&error))?;
        self.validate_masking_policy_expression(table, &column, expr)?;
        let now = chrono::Local::now().fixed_offset();
        Ok(MaskingPolicyInfo {
            id: 0,
            name: CiString::new(policy_name),
            db_name: schema.clone(),
            table_name: table.name.clone(),
            table_id: table.id,
            column_name: column.name.clone(),
            column_id: column.id,
            expression: expr.restore(),
            status: masking_policy_status_from_state(state),
            masking_type: masking_policy_type_from_expr(expr),
            restrict_ops,
            created_at: now,
            updated_at: now,
            created_by: self.context.ddl_current_user().unwrap_or_default(),
            updated_by: String::new(),
            state: SchemaState::NONE,
        })
    }

    /// Go `createMaskingPolicyWithInfo` up to its job; `None` where
    /// `IF NOT EXISTS` filed the note.
    fn create_masking_policy_with_info(
        &self,
        table: &TableInfo,
        policy: MaskingPolicyInfo,
        on_exist: OnExist,
        context: &dyn DdlSessionContext,
    ) -> Result<Option<crate::JobWrapper>, MysqlError> {
        let policies = self.masking_policies()?;
        if let Some(exist) =
            masking_policy_by_name_for_table(&policies, table, policy.name.lowercase())
        {
            if exist.column_id != policy.column_id {
                return Err(plain_error(format!(
                    "masking policy {} already exists on another column",
                    exist.name.original()
                )));
            }
            let error = plain_error(format!(
                "masking policy {} already exists",
                policy.name.original()
            ));
            match on_exist {
                OnExist::Ignore => {
                    self.context.append_note_parts(error.code, &error.message);
                    return Ok(None);
                }
                OnExist::Error => return Err(error),
                OnExist::Replace => {}
            }
        }
        if let Some(exist) = policies.iter().find(|exist| {
            exist.table_id == policy.table_id
                && exist.column_id == policy.column_id
                && exist.name.lowercase() != policy.name.lowercase()
        }) {
            return Err(plain_error(format!(
                "masking policy already exists on column {}",
                exist.column_name.original()
            )));
        }
        Ok(Some(
            crate::cluster_ddl::masking_policy::create_masking_policy_with_info(
                &policy,
                on_exist == OnExist::Replace,
                context,
            ),
        ))
    }

    /// Go `executor.CreateMaskingPolicy`.
    fn create_masking_policy(
        &self,
        stmt: &CreateMaskingPolicyStmt,
        context: &dyn DdlSessionContext,
    ) -> Result<Option<crate::JobWrapper>, MysqlError> {
        if stmt.or_replace && stmt.if_not_exists {
            return Err(MysqlError::from_terror(
                &tidb_dbterror::ERR_WRONG_USAGE
                    .gen_with_stack_by_args(&["OR REPLACE".into(), "IF NOT EXISTS".into()]),
            ));
        }
        let (schema, table) = self.masking_policy_schema_and_table(&stmt.table)?;
        let policy = self.build_masking_policy_info(
            &schema,
            &table,
            &stmt.name,
            &stmt.column,
            &stmt.expr,
            stmt.restrict_ops,
            stmt.state,
        )?;
        let on_exist = if stmt.if_not_exists {
            OnExist::Ignore
        } else if stmt.or_replace {
            OnExist::Replace
        } else {
            OnExist::Error
        };
        self.create_masking_policy_with_info(&table, policy, on_exist, context)
    }

    /// Go's `ALTER TABLE ... MASKING POLICY` executor functions.
    fn alter_table_masking_policy(
        &self,
        alter: &AlterTableStmt,
        action: &AlterMaskingPolicyAction,
        context: &dyn DdlSessionContext,
    ) -> Result<Option<crate::JobWrapper>, MysqlError> {
        let (schema, table) = self.masking_policy_schema_and_table(&alter.name)?;
        let name = match action {
            AlterMaskingPolicyAction::Add {
                name,
                column,
                expr,
                restrict_ops,
                state,
            } => {
                // Go `AddMaskingPolicy`.
                let policy = self.build_masking_policy_info(
                    &schema,
                    &table,
                    name,
                    column,
                    expr,
                    *restrict_ops,
                    *state,
                )?;
                return self.create_masking_policy_with_info(
                    &table,
                    policy,
                    OnExist::Error,
                    context,
                );
            }
            AlterMaskingPolicyAction::Enable(name)
            | AlterMaskingPolicyAction::Disable(name)
            | AlterMaskingPolicyAction::Drop(name)
            | AlterMaskingPolicyAction::ModifyExpression { name, .. }
            | AlterMaskingPolicyAction::ModifyRestrict { name, .. } => name,
        };
        let policies = self.masking_policies()?;
        let Some(policy) = masking_policy_by_name_for_table(&policies, &table, name) else {
            return Err(masking_policy_not_exists(name));
        };
        let mut new_policy = policy.clone();
        let mut schema_name = policy.db_name.lowercase().to_owned();
        match action {
            AlterMaskingPolicyAction::Add { .. } => unreachable!("returned above"),
            // Go `DropMaskingPolicy`.
            AlterMaskingPolicyAction::Drop(_) => {
                return Ok(Some(
                    crate::cluster_ddl::masking_policy::drop_masking_policy(&new_policy, context),
                ));
            }
            // Go `AlterTableMaskingPolicyState`.
            AlterMaskingPolicyAction::Enable(_) => new_policy.status = MaskingPolicyStatus::ENABLE,
            AlterMaskingPolicyAction::Disable(_) => {
                new_policy.status = MaskingPolicyStatus::DISABLE;
            }
            // Go `ModifyMaskingPolicyExpression`.
            AlterMaskingPolicyAction::ModifyExpression { expr, .. } => {
                let Some(column) = find_col(&table, policy.column_name.lowercase()) else {
                    return Err(MysqlError::from_terror(
                        &tidb_infoschema::infoschema_errors::ERR_COLUMN_NOT_EXISTS
                            .gen_with_stack_by_args(&[
                                policy.column_name.original().into(),
                                table.name.original().into(),
                            ]),
                    ));
                };
                self.validate_masking_policy_expression(&table, &column, expr)?;
                new_policy.expression = expr.restore();
                new_policy.masking_type = masking_policy_type_from_expr(expr);
                schema_name = schema.lowercase().to_owned();
            }
            // Go `ModifyMaskingPolicyRestrictOn`.
            AlterMaskingPolicyAction::ModifyRestrict { restrict_ops, .. } => {
                new_policy.restrict_ops = *restrict_ops;
            }
        }
        new_policy.updated_at = chrono::Local::now().fixed_offset();
        Ok(Some(
            crate::cluster_ddl::masking_policy::alter_masking_policy_with_info(
                &new_policy,
                policy.id,
                &schema_name,
                context,
            ),
        ))
    }

    /// Go `checkMaskingPolicyOnModifyColumn`, which `ModifyColumn` and
    /// `ChangeColumn` run once the job is built: a column that carries a
    /// policy keeps a type the policy can mask.
    ///
    /// # Errors
    ///
    /// `ErrGeneralUnsupportedDDL` for a type no policy masks.
    fn check_masking_policy_on_modify_column(
        &self,
        alter: &AlterTableStmt,
    ) -> Result<(), MysqlError> {
        for action in &alter.actions {
            let (original, column) = match action {
                AlterTableAction::ModifyColumn { column, .. } => (column.name.as_str(), column),
                AlterTableAction::ChangeColumn {
                    old_name, column, ..
                } => match old_name.last() {
                    Some(old_name) => (old_name.as_str(), column),
                    None => continue,
                },
                _ => continue,
            };
            let Ok(code) = tidb_ddl_build::column_field_type::column_type_code(&column.ty) else {
                continue;
            };
            // Go: a table or column that is not found is a later check's.
            let Ok((_, table)) = self.masking_policy_schema_and_table(&alter.name) else {
                continue;
            };
            let original = original.go_to_lower();
            let Some(column_id) = table.columns.iter_deref().find_map(|column| {
                let column = column.read();
                (column.state == SchemaState::PUBLIC
                    && !column.hidden
                    && column.name.lowercase() == original)
                    .then_some(column.id)
            }) else {
                continue;
            };
            let policies = self.masking_policies()?;
            if !is_masking_policy_supported_type(code)
                && policies
                    .iter()
                    .any(|policy| policy.table_id == table.id && policy.column_id == column_id)
            {
                return Err(MysqlError::from_terror(
                    &tidb_dbterror::ERR_GENERAL_UNSUPPORTED_DDL.gen_with_stack_by_args(&[
                        "masking policy on unsupported column type".into(),
                    ]),
                ));
            }
        }
        Ok(())
    }
}

/// Go `executor.CreateMaskingPolicy`: its job, `None` for an `IF NOT EXISTS`
/// that filed its note.
pub(crate) fn create_masking_policy_job(
    catalog: &SessionExtendedInfoSchema,
    stmt: &CreateMaskingPolicyStmt,
    current_db: &str,
    context: &dyn DdlSessionContext,
) -> Result<Option<crate::JobWrapper>, MysqlError> {
    MaskingExec {
        catalog,
        current_db,
        context,
    }
    .create_masking_policy(stmt, context)
}

/// Go `executor.AlterTable`'s masking-policy specs: their job.
pub(crate) fn alter_table_masking_policy_job(
    catalog: &SessionExtendedInfoSchema,
    alter: &AlterTableStmt,
    action: &AlterMaskingPolicyAction,
    current_db: &str,
    context: &dyn DdlSessionContext,
) -> Result<Option<crate::JobWrapper>, MysqlError> {
    MaskingExec {
        catalog,
        current_db,
        context,
    }
    .alter_table_masking_policy(alter, action, context)
}

/// Go `checkMaskingPolicyOnModifyColumn` for an `ALTER TABLE`'s
/// `MODIFY COLUMN` / `CHANGE COLUMN` specs.
pub(crate) fn check_masking_policy_on_modify_column(
    catalog: &SessionExtendedInfoSchema,
    alter: &AlterTableStmt,
    current_db: &str,
    context: &dyn DdlSessionContext,
) -> Result<(), MysqlError> {
    MaskingExec {
        catalog,
        current_db,
        context,
    }
    .check_masking_policy_on_modify_column(alter)
}
