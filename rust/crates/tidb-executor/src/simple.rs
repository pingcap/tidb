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

//! Go `pkg/executor/simple.go`: the executors of Go's `core.Simple` plans.

pub mod account;
pub mod account_password;
pub mod account_tables;

use crate::sessionctx::SessionContext;
use crate::{Catalog, DriverError};

/// Go `SimpleExec.executeUse`: an invisible schema is refused (1044) before
/// it is looked up, so it cannot be told apart from a missing one; an unknown
/// schema is `ErrDatabaseNotExists`; the switch sets `CurrentDB`,
/// `CurrentDBChanged` and `collation_database`, whose hook mirrors the
/// charset into `character_set_database`.
pub fn execute_use(
    ctx: &mut dyn SessionContext,
    is: &Catalog,
    name: &str,
) -> Result<(), DriverError> {
    // An empty schema name (`USE ``) is `ErrNoDB`, as Go's preprocessor
    // answers it, not an unknown database.
    if name.is_empty() {
        return Err(DriverError::from(
            tidb_error::plannererrors::ERR_NO_DB.generate_with_stack_by_args(&[]),
        ));
    }
    if !ctx.privilege_manager().db_is_visible(name) {
        let vars = ctx.session_vars();
        let (user, host) = vars
            .current_user
            .as_deref()
            .and_then(|identity| identity.split_once('@'))
            .unwrap_or_default();
        return Err(DriverError::from(
            tidb_error::plannererrors::ERR_DBACCESS_DENIED.generate_with_stack_by_args(&[
                user.to_owned().into(),
                host.to_owned().into(),
                name.to_owned().into(),
            ]),
        ));
    }
    let Some(charset) = is.database_charset(name) else {
        return Err(DriverError::from(
            tidb_error::catalog::infoschema::ERR_DATABASE_NOT_EXISTS
                .generate_with_stack_by_args(&[(name.to_owned()).into()]),
        ));
    };
    let vars = ctx.session_vars_mut();
    vars.current_db_changed = name != vars.current_db;
    vars.current_db = name.to_owned();
    // If new collations are enabled, the default collation stands in for one
    // they do not support.
    let collation =
        tidb_datatype::substitute_missing_collation_to_default(charset.collation.name());
    vars.set_system("collation_database", collation)?;
    Ok(())
}

/// Go `SimpleExec.executeSetResourceGroupName`: a named group must exist in
/// the statement's infoschema (`ErrResourceGroupNotExists`, naming it as
/// written); the empty name is `default`.
pub fn execute_set_resource_group_name(
    ctx: &mut dyn SessionContext,
    is: &Catalog,
    stmt: &tidb_ast::SetResourceGroupStmt,
) -> Result<(), DriverError> {
    use tidb_hack::GoToLower;
    let name = if stmt.name.is_empty() {
        "default".to_owned()
    } else {
        if is.resource_group(&stmt.name).is_none() {
            return Err(DriverError::Mysql(crate::MysqlError::from_terror(
                &tidb_error::catalog::infoschema::ERR_RESOURCE_GROUP_NOT_EXISTS
                    .generate_with_stack_by_args(&[stmt.name.clone().into()]),
            )));
        }
        stmt.name.go_to_lower()
    };
    ctx.session_vars_mut().resource_group = name;
    Ok(())
}

/// Go `SimpleExec.executeSetRole`: the roles named, all granted ones, all
/// but some, the default ones, or none, become the session's active roles.
/// Activation is direct-only: a role named or excluded must be granted to
/// the account itself (`FindEdge`), else `ErrRoleNotGranted` (3530), and a
/// rejected statement leaves the previous set untouched.
pub fn execute_set_role(
    ctx: &mut dyn SessionContext,
    stmt: &tidb_ast::SetRoleStmt,
) -> Result<(), DriverError> {
    if ctx.privilege_checks_bypassed() {
        return Ok(());
    }
    if !ctx.has_privilege_registry() {
        return Err(DriverError::unsupported(
            "SET ROLE requires a server front end with a privilege registry",
        ));
    }
    let account = ctx
        .session_vars()
        .current_user
        .as_deref()
        .and_then(|identity| identity.split_once('@'))
        .map(|(user, host)| (user.to_owned(), host.to_owned()))
        .ok_or_else(|| {
            DriverError::unsupported("CURRENT_USER requires a session with an authenticated identity")
        })?;
    let checker = ctx.privilege_manager();
    let granted = |roles: &[tidb_ast::RoleSpec]| -> Result<Vec<(String, String)>, DriverError> {
        roles
            .iter()
            .map(|spec| {
                let role = role_identity(spec);
                if checker.find_edge(&role, &account) {
                    Ok(role)
                } else {
                    Err(DriverError::from(
                        tidb_util::dbterror::exeerrors::ERR_ROLE_NOT_GRANTED
                            .generate_with_stack_by_args(&[
                                format!("`{}`@`{}`", role.0, role.1).into(),
                                format!("{}@{}", account.0.clone(), account.1.clone()).into(),
                            ]),
                    ))
                }
            })
            .collect()
    };
    let active = match &stmt.selection {
        tidb_ast::SetRoleSelection::None => Vec::new(),
        tidb_ast::SetRoleSelection::All => checker.get_all_roles(&account.0, &account.1),
        tidb_ast::SetRoleSelection::Default => checker.get_default_roles(&account.0, &account.1),
        tidb_ast::SetRoleSelection::AllExcept(excluded) => {
            let excluded = granted(excluded)?;
            checker
                .get_all_roles(&account.0, &account.1)
                .into_iter()
                .filter(|role| !excluded.contains(role))
                .collect()
        }
        tidb_ast::SetRoleSelection::Roles(roles) => granted(roles)?,
    };
    ctx.session_vars_mut().active_roles = std::sync::Arc::new(active);
    Ok(())
}

/// Go `auth.RoleIdentity` of a written role: an omitted host is `%`.
fn role_identity(spec: &tidb_ast::RoleSpec) -> (String, String) {
    let host = if spec.host.is_empty() {
        "%".to_owned()
    } else {
        spec.host.clone()
    };
    (spec.role.clone(), host)
}

