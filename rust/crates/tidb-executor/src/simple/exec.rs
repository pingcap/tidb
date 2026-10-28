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

//! Go `SimpleExec` (`executor/simple.go`): `Next`'s dispatch over the
//! statements the planner builds as `Simple` plans.

use crate::{DriverError, StmtOutput};

/// Go `SimpleExec`.
pub trait SimpleExec: crate::admin::AdminExec {
    /// Go `sessionctx.Context.DecodeStates`.
    fn decode_session_states(&mut self, text: &str) -> Result<(), DriverError>;
    /// Go `executeAlterInstance`: `util.LoadTLSCertificates` and the
    /// server's TLS config swap.
    fn reload_tls(&mut self, no_rollback_on_error: bool) -> Result<(), DriverError>;
    /// The ADMIN arms this tier still answers in the session.
    fn dispatch_admin_stmt(
        &mut self,
        admin: &tidb_ast::AdminStmt,
    ) -> Result<Option<StmtOutput>, DriverError>;

    /// Go `SimpleExec.Next`: one arm per statement type.
    fn simple_next(&mut self, stmt: &tidb_ast::Stmt) -> Result<Option<StmtOutput>, DriverError> {
        match stmt {
            tidb_ast::Stmt::Session(session_stmt) => match &**session_stmt {
                tidb_ast::SessionStmt::Use(name) => {
                    let domain = self.domain_info_schema();
                    let catalog = domain.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                    super::execute_use(self, &catalog, name)?;
                    Ok(Some(StmtOutput::Affected(0)))
                }
                tidb_ast::SessionStmt::SetRole(set_role) => {
                    super::execute_set_role(self, set_role)?;
                    Ok(Some(StmtOutput::Affected(0)))
                }
                tidb_ast::SessionStmt::SetDefaultRole(set_default) => {
                    self.set_default_role_stmt(set_default)?;
                    Ok(Some(StmtOutput::Affected(0)))
                }
                // `SET PASSWORD` writes `mysql.user`, not a variable.
                tidb_ast::SessionStmt::SetPassword(set_password) => {
                    self.set_password_stmt(set_password)?;
                    Ok(Some(StmtOutput::Affected(0)))
                }
                tidb_ast::SessionStmt::SetSessionStates(set) => {
                    self.session_vars_mut().in_set_session_states_stmt = true;
                    self.decode_session_states(&set.session_states)?;
                    Ok(Some(StmtOutput::Affected(0)))
                }
                tidb_ast::SessionStmt::SetResourceGroup(resource_group) => {
                    let domain = self.domain_info_schema();
                    let catalog = domain.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                    super::execute_set_resource_group_name(self, &catalog, resource_group)?;
                    Ok(Some(StmtOutput::Affected(0)))
                }
                _ => Ok(None),
            },
            tidb_ast::Stmt::Admin(admin) => match &**admin {
                tidb_ast::AdminStmt::CreateUser {
                    if_not_exists,
                    users,
                    tls_options,
                    resource_options,
                    password_options,
                    comment_or_attribute,
                    resource_group,
                } => {
                    self.create_user_stmt(
                        *if_not_exists,
                        users,
                        tls_options,
                        resource_options,
                        password_options,
                        comment_or_attribute,
                        resource_group,
                    )?;
                    Ok(Some(StmtOutput::Affected(0)))
                }
                tidb_ast::AdminStmt::DropUser {
                    is_role,
                    if_exists,
                    users,
                } => {
                    self.drop_user_stmt(*is_role, *if_exists, users)?;
                    Ok(Some(StmtOutput::Affected(0)))
                }
                tidb_ast::AdminStmt::AlterInstance(alter) => {
                    self.reload_tls(alter.no_rollback_on_error)?;
                    Ok(Some(StmtOutput::Affected(0)))
                }
                tidb_ast::AdminStmt::AlterUser(alter) => {
                    self.alter_user_stmt(alter)?;
                    Ok(Some(StmtOutput::Affected(0)))
                }
                tidb_ast::AdminStmt::RenameUser { pairs } => {
                    self.rename_user_stmt(pairs)?;
                    Ok(Some(StmtOutput::Affected(0)))
                }
                tidb_ast::AdminStmt::CreateRole {
                    if_not_exists,
                    roles,
                } => {
                    self.create_role_stmt(*if_not_exists, roles)?;
                    Ok(Some(StmtOutput::Affected(0)))
                }
                // Go `SimpleExec.Next` has no arm for `DropProcedureStmt`
                // (and `autoNewTxn` does not list it): `buildSimple` already
                // refused it, or noted `IF EXISTS`; the executor does nothing
                // and the open transaction stays.
                tidb_ast::AdminStmt::DropProcedure(_) => Ok(Some(StmtOutput::Affected(0))),
                _ => self.dispatch_admin_stmt(admin),
            },
            _ => Ok(None),
        }
    }
}
