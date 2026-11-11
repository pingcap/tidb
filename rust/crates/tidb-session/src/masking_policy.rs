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

//! Go's masking-policy DDL up to its job: `executor.CreateMaskingPolicy`,
//! `AddMaskingPolicy`, `AlterTableMaskingPolicyState`, `DropMaskingPolicy`,
//! `ModifyMaskingPolicyExpression`, `ModifyMaskingPolicyRestrictOn` and
//! `checkMaskingPolicyOnModifyColumn` (`ddl/executor.go`) with
//! `buildMaskingPolicyInfo` (`ddl/masking_policy.go`). Go's infoschema loads
//! the policies of `mysql.tidb_masking_policy` lazily and caches them; this
//! session reads the table for each statement.

use tidb_ast::{
    AlterMaskingPolicyAction, AlterTableAction, AlterTableStmt, CiString, CreateMaskingPolicyStmt,
    DdlStmt, Expr, MaskingPolicyRestrictOps, MaskingPolicyState,
};
use tidb_ddl::cluster_ddl::masking_policy::{
    check_masking_policy_column, check_masking_policy_table, is_masking_policy_supported_type,
    masking_policy_from_sys_table_row, SELECT_MASKING_POLICY_SQL,
};
use tidb_ddl::cluster_ddl::DdlStatement;
use tidb_driver_error::{DriverError, ExecError};
use tidb_hack::GoToLower;
use tidb_model::{
    ColumnInfo, MaskingPolicyInfo, MaskingPolicyStatus, MaskingPolicyType, SchemaState, TableInfo,
};
use tidb_table::generated_column::TableColumnResolver;
use tidb_terror::MysqlError;

use crate::Session;

/// Go's `errors.Errorf` messages, which reach the client as 1105.
fn plain_error(message: String) -> DriverError {
    DriverError::Mysql(MysqlError::unknown(message))
}

/// Go `OnExist`.
#[derive(Clone, Copy, PartialEq, Eq)]
enum OnExist {
    Error,
    Ignore,
    Replace,
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

fn masking_policy_not_exists(name: &str) -> DriverError {
    plain_error(format!("masking policy {name} doesn't exist"))
}

impl Session {
    /// Go `executor.getSchemaAndTableByIdent` over the latest schema.
    fn masking_policy_schema_and_table(
        &self,
        path: &[String],
    ) -> Result<(CiString, TableInfo), DriverError> {
        let (database, name) = tidb_infoschema::split_table_path_pub(path, &self.vars.current_db)?;
        let catalog = self.lock_catalog()?;
        let Some(schema) = catalog.schema_by_name(database) else {
            return Err(DriverError::from(
                tidb_infoschema::infoschema_errors::ERR_DATABASE_NOT_EXISTS
                    .gen_with_stack_by_args(&[database.into()]),
            ));
        };
        let Some((_, table)) = catalog.table_info_by_name(database, name) else {
            return Err(DriverError::from(
                tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                    .gen_with_stack_by_args(&[database.into(), name.into()]),
            ));
        };
        let schema_name = schema.read().name.clone();
        let table = table.read().clone_like_go();
        Ok((schema_name, table))
    }

    /// The policies Go's infoschema loads from `mysql.tidb_masking_policy`;
    /// none while the table does not exist yet.
    fn masking_policies(&mut self) -> Result<Vec<MaskingPolicyInfo>, DriverError> {
        let rows = match self.user_table_rows(SELECT_MASKING_POLICY_SQL) {
            Ok(rows) => rows,
            Err(error) => {
                let error = error.to_mysql_error();
                if error.code == tidb_errno::errcode::ErrNoSuchTable {
                    return Ok(Vec::new());
                }
                return Err(DriverError::Mysql(error));
            }
        };
        rows.iter()
            .map(|row| masking_policy_from_sys_table_row(row).map_err(plain_error))
            .collect()
    }

    /// Go `validateMaskingPolicyExpression`: the expression builds against
    /// the table and names no column but its target.
    fn validate_masking_policy_expression(
        &self,
        table: &TableInfo,
        target: &ColumnInfo,
        expr: &Expr,
    ) -> Result<(), DriverError> {
        let (names, types): (Vec<String>, Vec<tidb_datatype::FieldType>) = table
            .columns
            .iter_deref()
            .map(|column| {
                let column = column.read();
                (column.name.original().to_owned(), column.field_type.clone())
            })
            .unzip();
        let resolver = TableColumnResolver::new(&names, &types, self.vars.session_time_zone())
            .with_current_database(&self.vars.current_db)
            .with_source_table(table.name.original());
        tidb_expr::rewriter::rewrite_expr_resolved(expr, &resolver)
            .map_err(|error| DriverError::Exec(ExecError::Eval(error)))?;
        if resolver
            .dependency_names()
            .iter()
            .any(|name| name.go_to_lower() != target.name.lowercase())
        {
            return Err(DriverError::from(
                tidb_meta::meta_errors::ERR_MASKING_POLICY_EXPR_INVALID_COLUMN
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
    ) -> Result<MaskingPolicyInfo, DriverError> {
        check_masking_policy_table(schema, table)?;
        let Some(column) = find_col(table, column_name) else {
            return Err(DriverError::from(
                tidb_infoschema::infoschema_errors::ERR_COLUMN_NOT_EXISTS
                    .gen_with_stack_by_args(&[column_name.into(), table.name.original().into()]),
            ));
        };
        check_masking_policy_column(&column)?;
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
            created_by: self.vars.current_user.clone().unwrap_or_default(),
            updated_by: String::new(),
            state: SchemaState::NONE,
        })
    }

    /// Go `createMaskingPolicyWithInfo` up to its job; `None` where
    /// `IF NOT EXISTS` filed the note.
    fn create_masking_policy_with_info(
        &mut self,
        table: &TableInfo,
        policy: MaskingPolicyInfo,
        on_exist: OnExist,
    ) -> Result<Option<DdlStatement>, DriverError> {
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
                    self.append_suppressed(error);
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
        Ok(Some(DdlStatement::CreateMaskingPolicy {
            policy: Box::new(policy),
            replace_on_exist: on_exist == OnExist::Replace,
            ignore_on_exist: on_exist == OnExist::Ignore,
        }))
    }

    /// Go `executor.CreateMaskingPolicy`.
    fn create_masking_policy(
        &mut self,
        stmt: &CreateMaskingPolicyStmt,
    ) -> Result<Option<DdlStatement>, DriverError> {
        if stmt.or_replace && stmt.if_not_exists {
            return Err(DriverError::from(
                tidb_dbterror::ERR_WRONG_USAGE
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
        self.create_masking_policy_with_info(&table, policy, on_exist)
    }

    /// Go's `ALTER TABLE ... MASKING POLICY` executor functions.
    fn alter_table_masking_policy(
        &mut self,
        alter: &AlterTableStmt,
        action: &AlterMaskingPolicyAction,
    ) -> Result<Option<DdlStatement>, DriverError> {
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
                return self.create_masking_policy_with_info(&table, policy, OnExist::Error);
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
                return Ok(Some(DdlStatement::DropMaskingPolicy {
                    policy: Box::new(new_policy),
                }));
            }
            // Go `AlterTableMaskingPolicyState`.
            AlterMaskingPolicyAction::Enable(_) => new_policy.status = MaskingPolicyStatus::ENABLE,
            AlterMaskingPolicyAction::Disable(_) => {
                new_policy.status = MaskingPolicyStatus::DISABLE;
            }
            // Go `ModifyMaskingPolicyExpression`.
            AlterMaskingPolicyAction::ModifyExpression { expr, .. } => {
                let Some(column) = find_col(&table, policy.column_name.lowercase()) else {
                    return Err(DriverError::from(
                        tidb_infoschema::infoschema_errors::ERR_COLUMN_NOT_EXISTS
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
        Ok(Some(DdlStatement::AlterMaskingPolicy {
            policy_id: policy.id,
            policy: Box::new(new_policy),
            schema_name,
        }))
    }

    /// Whether `ddl` is a statement Go's masking-policy executor functions
    /// run: `CREATE MASKING POLICY`, or an `ALTER TABLE` whose only spec is a
    /// masking-policy one (beside another spec `AlterTable` refuses it).
    #[must_use]
    pub fn is_masking_policy_statement(ddl: &DdlStmt) -> bool {
        match ddl {
            DdlStmt::CreateMaskingPolicy(_) => true,
            DdlStmt::AlterTable(alter) => {
                matches!(alter.actions.as_slice(), [AlterTableAction::MaskingPolicy(_)])
            }
            _ => false,
        }
    }

    /// The cluster DDL for a masking-policy statement: Go's executor checks,
    /// then the statement the DDL job runs. `None` is an `IF NOT EXISTS` that
    /// filed its note and submits no job.
    ///
    /// # Errors
    ///
    /// The refusal Go's executor function returns before its job.
    pub fn cluster_masking_policy_statement(
        &mut self,
        ddl: &DdlStmt,
    ) -> Result<Option<DdlStatement>, DriverError> {
        match ddl {
            DdlStmt::CreateMaskingPolicy(create) => self.create_masking_policy(create),
            DdlStmt::AlterTable(alter) => match alter.actions.as_slice() {
                [AlterTableAction::MaskingPolicy(action)] => {
                    self.alter_table_masking_policy(alter, action)
                }
                _ => Ok(None),
            },
            _ => Ok(None),
        }
    }

    /// Whether `ddl` has a `MODIFY COLUMN` / `CHANGE COLUMN` spec, the ones
    /// Go's `checkMaskingPolicyOnModifyColumn` follows.
    #[must_use]
    pub fn modifies_column_type(ddl: &DdlStmt) -> bool {
        let DdlStmt::AlterTable(alter) = ddl else {
            return false;
        };
        alter.actions.iter().any(|action| {
            matches!(
                action,
                AlterTableAction::ModifyColumn { .. } | AlterTableAction::ChangeColumn { .. }
            )
        })
    }

    /// Go `checkMaskingPolicyOnModifyColumn`, which `ModifyColumn` and
    /// `ChangeColumn` run once the job is built: a column that carries a
    /// policy keeps a type the policy can mask.
    ///
    /// # Errors
    ///
    /// `ErrGeneralUnsupportedDDL` for a type no policy masks.
    pub fn check_masking_policy_on_modify_column(&mut self, ddl: &DdlStmt) -> Result<(), DriverError> {
        let DdlStmt::AlterTable(alter) = ddl else {
            return Ok(());
        };
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
            if is_masking_policy_supported_type(code) {
                continue;
            }
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
            if policies
                .iter()
                .any(|policy| policy.table_id == table.id && policy.column_id == column_id)
            {
                return Err(DriverError::from(
                    tidb_dbterror::ERR_GENERAL_UNSUPPORTED_DDL
                        .gen_with_stack_by_args(&["masking policy on unsupported column type".into()]),
                ));
            }
        }
        Ok(())
    }
}
