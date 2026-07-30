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

//! `GRANT` / `REVOKE` / `SHOW GRANTS` and the privilege level they name,
//! mirroring Go's `GrantStmt`, `RevokeStmt`, and `GrantLevel` in
//! `pkg/parser/ast/misc.go`.

use super::*;

/// Go's `ShowStmt{Tp: ShowGrants}` payload.
#[derive(Debug, Clone, PartialEq)]
pub struct ShowGrantsStmt {
    /// Optional account after `FOR`; absent means the current session user.
    pub user: Option<crate::UserSpec>,
    /// Optional active-role override list after `USING`.
    pub roles: Vec<crate::UserSpec>,
}

impl ShowGrantsStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("SHOW GRANTS");
        if let Some(user) = &self.user {
            out.push_str(" FOR ");
            user.restore_into(out);
        }
        if !self.roles.is_empty() {
            out.push_str(" USING ");
            for (index, role) in self.roles.iter().enumerate() {
                if index != 0 {
                    out.push_str(", ");
                }
                role.restore_into(out);
            }
        }
    }
}

/// TiDB's standard privilege-revoke statement, transliterated from Go's
/// `ast.RevokeStmt` and sharing its privilege/object/level payload types with
/// [`GrantStmt`].
#[derive(Debug, Clone, PartialEq)]
pub struct RevokeStmt {
    /// Standard privileges in their written order.
    pub privileges: Vec<GrantPrivilege>,
    /// Optional object class after `ON`.
    pub object_type: Option<GrantObjectType>,
    /// Scope from which the privileges are revoked.
    pub level: GrantLevel,
    /// Accounts in their written order.
    pub users: Vec<crate::CreateUserSpec>,
}

impl RevokeStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("REVOKE ");
        for (index, privilege) in self.privileges.iter().enumerate() {
            if index != 0 {
                out.push_str(", ");
            }
            privilege.restore_into(out);
        }
        out.push_str(" ON ");
        if let Some(object_type) = self.object_type {
            out.push_str(match object_type {
                GrantObjectType::Table => "TABLE ",
                GrantObjectType::Function => "FUNCTION ",
                GrantObjectType::Procedure => "PROCEDURE ",
            });
        }
        self.level.restore_into(out);
        out.push_str(" FROM ");
        for (index, user) in self.users.iter().enumerate() {
            if index != 0 {
                out.push_str(", ");
            }
            user.restore_into(out);
        }
    }
}

/// TiDB's core privilege-grant statement, transliterated from Go's
/// `ast.GrantStmt`, including each grantee's typed authentication payload.
#[derive(Debug, Clone, PartialEq)]
pub struct GrantStmt {
    /// Privileges in their written order.
    pub privileges: Vec<GrantPrivilege>,
    /// Optional object class after `ON`.
    pub object_type: Option<GrantObjectType>,
    /// Scope to which the privileges apply.
    pub level: GrantLevel,
    /// Grantee accounts in their written order.
    pub users: Vec<crate::CreateUserSpec>,
    /// Optional `REQUIRE` TLS/authentication constraints in Go source order.
    pub tls_options: Vec<crate::AlterUserTlsOption>,
    /// `WITH GRANT OPTION`.
    pub with_grant: bool,
}

/// TiDB's special proxy-user grant, which has no privilege list or object
/// level and therefore cannot be represented by [`GrantStmt`].
#[derive(Debug, Clone, PartialEq)]
pub struct GrantProxyStmt {
    /// Account whose identity may be assumed.
    pub local_user: UserSpec,
    /// Accounts receiving proxy access.
    pub external_users: Vec<UserSpec>,
    /// Whether recipients may grant the proxy privilege onward.
    pub with_grant: bool,
}

impl GrantProxyStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("GRANT PROXY ON ");
        self.local_user.restore_into(out);
        out.push_str(" TO ");
        for (index, user) in self.external_users.iter().enumerate() {
            if index > 0 {
                out.push_str(", ");
            }
            user.restore_into(out);
        }
        if self.with_grant {
            out.push_str(" WITH GRANT OPTION");
        }
    }
}

impl GrantStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("GRANT ");
        for (index, privilege) in self.privileges.iter().enumerate() {
            if index != 0 {
                out.push_str(", ");
            }
            privilege.restore_into(out);
        }
        out.push_str(" ON ");
        if let Some(object_type) = self.object_type {
            out.push_str(match object_type {
                GrantObjectType::Table => "TABLE ",
                GrantObjectType::Function => "FUNCTION ",
                GrantObjectType::Procedure => "PROCEDURE ",
            });
        }
        self.level.restore_into(out);
        out.push_str(" TO ");
        for (index, user) in self.users.iter().enumerate() {
            if index != 0 {
                out.push_str(", ");
            }
            user.restore_into(out);
        }
        if !self.tls_options.is_empty() {
            out.push_str(" REQUIRE ");
            for (index, option) in self.tls_options.iter().enumerate() {
                if index > 0 {
                    out.push_str(" AND ");
                }
                option.restore_into(out);
            }
        }
        if self.with_grant {
            out.push_str(" WITH GRANT OPTION");
        }
    }
}

/// One `ast.PrivElem`: its canonical privilege spelling and optional columns.
#[derive(Debug, Clone, PartialEq)]
pub struct GrantPrivilege {
    /// Go-restored uppercase privilege spelling, including dynamic privileges.
    pub name: String,
    /// Optional column list attached to this privilege.
    pub columns: Vec<String>,
    /// Whether Go parsed this privilege through its identifier-only
    /// `ExtendedPriv` branch. Keeping the distinction typed lets REVOKE
    /// accept dynamic privileges without widening role or special no-`ON`
    /// forms, while preserving the same canonical restore text.
    pub dynamic: bool,
}

impl GrantPrivilege {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str(&self.name);
        if !self.columns.is_empty() {
            out.push_str(" (");
            for (index, column) in self.columns.iter().enumerate() {
                if index != 0 {
                    out.push(',');
                }
                out.push_str(&crate::util::back_quote(column));
            }
            out.push(')');
        }
    }
}

/// Optional object class after `ON`, matching Go's `ObjectTypeType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrantObjectType {
    /// `TABLE`.
    Table,
    /// `FUNCTION`.
    Function,
    /// `PROCEDURE`.
    Procedure,
}

/// Go's three ordinary `GrantLevel` restore forms.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GrantLevel {
    /// `*.*`.
    Global,
    /// `*` or `` `database`.* ``.
    Database(Option<String>),
    /// `` `table` `` or `` `database`.`table` ``.
    Table {
        /// The optional database qualifier.
        database: Option<String>,
        /// The table name.
        table: String,
    },
}

impl GrantLevel {
    pub(crate) fn restore_into(&self, out: &mut String) {
        match self {
            Self::Global => out.push_str("*.*"),
            Self::Database(None) => out.push('*'),
            Self::Database(Some(database)) => {
                out.push_str(&crate::util::back_quote(database));
                out.push_str(".*");
            }
            Self::Table { database, table } => {
                if let Some(database) = database {
                    out.push_str(&crate::util::back_quote(database));
                    out.push('.');
                }
                out.push_str(&crate::util::back_quote(table));
            }
        }
    }
}

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for ShowGrantsStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { user, roles } = self;
        if let Some(value) = user.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        for value in roles.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = user;
        let _ = roles;
        visitor.leave(self)
    }
}

impl crate::Visitable for RevokeStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            privileges,
            object_type,
            level,
            users,
        } = self;
        for value in privileges.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        if let Some(value) = object_type.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        if !crate::Visitable::accept(level, visitor) {
            return false;
        }
        for value in users.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = privileges;
        let _ = object_type;
        let _ = level;
        let _ = users;
        visitor.leave(self)
    }
}

impl crate::Visitable for GrantStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            privileges,
            object_type,
            level,
            users,
            tls_options,
            with_grant,
        } = self;
        for value in privileges.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        if let Some(value) = object_type.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        if !crate::Visitable::accept(level, visitor) {
            return false;
        }
        for value in users.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        for value in tls_options.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = privileges;
        let _ = object_type;
        let _ = level;
        let _ = users;
        let _ = tls_options;
        let _ = with_grant;
        visitor.leave(self)
    }
}

impl crate::Visitable for GrantProxyStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            local_user,
            external_users,
            with_grant,
        } = self;
        if !crate::Visitable::accept(local_user, visitor) {
            return false;
        }
        for value in external_users.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = local_user;
        let _ = external_users;
        let _ = with_grant;
        visitor.leave(self)
    }
}

impl crate::Visitable for GrantPrivilege {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            name,
            columns,
            dynamic,
        } = self;
        let _ = name;
        let _ = columns;
        let _ = dynamic;
        visitor.leave(self)
    }
}

impl crate::Visitable for GrantObjectType {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Table => {}
            Self::Function => {}
            Self::Procedure => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for GrantLevel {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Global => {}
            Self::Database(field_0) => {
                let _ = field_0;
            }
            Self::Table { database, table } => {
                let _ = database;
                let _ = table;
            }
        }
        visitor.leave(self)
    }
}
// END GENERATED AST VISITOR IMPLEMENTATIONS
