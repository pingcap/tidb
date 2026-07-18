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

//! Role-membership AST payloads from Go's `GrantRoleStmt` and `RevokeRoleStmt`.

use crate::{RoleSpec, UserSpec};

/// Go `ast.GrantRoleStmt`: add roles to accounts in source order.
#[derive(Debug, Clone, PartialEq)]
pub struct GrantRoleStmt {
    /// Roles to grant, each using strict role identity grammar.
    pub roles: Vec<RoleSpec>,
    /// Accounts receiving the roles.
    pub users: Vec<UserSpec>,
}

impl GrantRoleStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("GRANT ");
        restore_roles(out, &self.roles);
        out.push_str(" TO ");
        restore_users(out, &self.users);
    }
}

/// Go `ast.RevokeRoleStmt`: remove roles from accounts in source order.
#[derive(Debug, Clone, PartialEq)]
pub struct RevokeRoleStmt {
    /// Roles to revoke, each using strict role identity grammar.
    pub roles: Vec<RoleSpec>,
    /// Accounts losing the roles.
    pub users: Vec<UserSpec>,
}

impl RevokeRoleStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("REVOKE ");
        restore_roles(out, &self.roles);
        out.push_str(" FROM ");
        restore_users(out, &self.users);
    }
}

fn restore_roles(out: &mut String, roles: &[RoleSpec]) {
    for (index, role) in roles.iter().enumerate() {
        if index != 0 {
            out.push_str(", ");
        }
        role.restore_into(out);
    }
}

fn restore_users(out: &mut String, users: &[UserSpec]) {
    for (index, user) in users.iter().enumerate() {
        if index != 0 {
            out.push_str(", ");
        }
        user.restore_into(out);
    }
}
