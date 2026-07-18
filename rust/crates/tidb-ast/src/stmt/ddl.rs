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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Data-definition statements and their shared restore boundary.

use crate::util::{back_quote, push_name_path};
use crate::{
    AlterInstanceStmt, AlterRangeStmt, AlterSequenceStmt, AlterTableStmt, CreateIndexStmt,
    CreateSequenceStmt, CreateTableStmt, CreateUserCommentOrAttribute, CreateUserPasswordOption,
    CreateUserSpec, CreateViewStmt, DropIndexStmt, DropSequenceStmt, DropTableStmt,
    RenameTableStmt, RestoreContext, RoleSpec, UserSpec,
};

/// One `old_user TO new_user` pair in `RENAME USER`.
#[derive(Debug, Clone, PartialEq)]
pub struct RenameUserPair {
    /// Account identity before the rename.
    pub old_user: UserSpec,
    /// Account identity after the rename.
    pub new_user: UserSpec,
}

/// One table lock mode carried by Go's `ast.TableLock`.
///
/// `None` is intentionally a real payload, not a parser error: the Go hand
/// parser accepts `LOCK TABLE name` and its AST restore materializes the
/// otherwise-unwritten `NONE` mode. Keeping that behavior typed prevents an
/// execution implementation from mistaking an omitted mode for `READ`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum TableLockType {
    /// Go's zero-value `TableLockNone`, restored as `NONE`.
    #[default]
    None,
    /// `READ`.
    Read,
    /// `READ LOCAL`.
    ReadLocal,
    /// `WRITE`.
    Write,
    /// `WRITE LOCAL`.
    WriteLocal,
}

impl TableLockType {
    fn restore(self) -> &'static str {
        match self {
            Self::None => "NONE",
            Self::Read => "READ",
            Self::ReadLocal => "READ LOCAL",
            Self::Write => "WRITE",
            Self::WriteLocal => "WRITE LOCAL",
        }
    }
}

/// One `table_name lock_mode` entry in `LOCK TABLE[S]`.
///
/// A table lock is syntactically a DDL statement in Go, but the seed executor
/// cannot faithfully represent TiDB's session lock manager. It therefore
/// remains a typed parse/restore payload until a future executor owns the
/// lock lifecycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableLock {
    /// The target table name, including an optional schema component.
    pub table: Vec<String>,
    /// The exact Go AST lock kind, including its visible `NONE` zero value.
    pub lock_type: TableLockType,
}

/// One option attached to `CREATE DATABASE`.
///
/// These are statement-owned rather than catalog-owned: Go's
/// `ast.CreateDatabaseStmt` keeps the source-order option list so its restore
/// path can make the option spelling canonical before the DDL executor sees
/// it. The seed executor does not model database namespaces, but retaining
/// the complete parser payload keeps that limitation explicit instead of
/// silently discarding SQL-visible metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatabaseOption {
    /// `CHARACTER SET = name`.
    CharacterSet(String),
    /// `COLLATE = name`.
    Collate(String),
    /// `ENCRYPTION = 'Y' | 'N'`.
    Encryption(String),
    /// `PLACEMENT POLICY = name`.
    PlacementPolicy(String),
    /// `SET TIFLASH REPLICA count [LOCATION LABELS 'label', ...]`.
    SetTiFlashReplica {
        /// Replica count.
        count: u64,
        /// Location-label strings in source order.
        labels: Vec<String>,
    },
}

impl DatabaseOption {
    fn restore_into(&self, out: &mut String) {
        match self {
            Self::CharacterSet(name) => {
                out.push_str("CHARACTER SET = ");
                out.push_str(name);
            }
            Self::Collate(name) => {
                out.push_str("COLLATE = ");
                out.push_str(name);
            }
            Self::Encryption(value) => {
                out.push_str("ENCRYPTION = '");
                out.push_str(&crate::util::escape_string_literal(value));
                out.push('\'');
            }
            Self::PlacementPolicy(name) => {
                out.push_str("PLACEMENT POLICY = ");
                out.push_str(&back_quote(name));
            }
            Self::SetTiFlashReplica { count, labels } => {
                out.push_str("SET TIFLASH REPLICA ");
                out.push_str(&count.to_string());
                if !labels.is_empty() {
                    out.push_str(" LOCATION LABELS ");
                    for (index, label) in labels.iter().enumerate() {
                        if index > 0 {
                            out.push_str(", ");
                        }
                        out.push('\'');
                        out.push_str(&crate::util::escape_string_literal(label));
                        out.push('\'');
                    }
                }
            }
        }
    }
}

/// A statement that defines, alters, or removes schema objects.
#[derive(Debug, Clone, PartialEq)]
pub enum DdlStmt {
    /// A `CREATE TABLE` statement.
    CreateTable(Box<CreateTableStmt>),
    /// A `CREATE [OR REPLACE] VIEW` statement.
    CreateView(Box<CreateViewStmt>),
    /// A standalone `CREATE [UNIQUE] INDEX` statement.
    CreateIndex(Box<CreateIndexStmt>),
    /// A standalone `DROP INDEX` statement.
    DropIndex(Box<DropIndexStmt>),
    /// A `CREATE DATABASE [IF NOT EXISTS] name [options]` statement.
    CreateDatabase {
        /// Whether duplicate-database errors are suppressed.
        if_not_exists: bool,
        /// The database name.
        name: String,
        /// Canonical database options, in their Go AST source order.
        options: Vec<DatabaseOption>,
    },
    /// An `ALTER DATABASE [name] option [, option ...]` statement.
    ///
    /// Go permits the name to be omitted, which means the current default
    /// database. The option list is nevertheless required and is kept in
    /// source order just like [`Self::CreateDatabase`].
    AlterDatabase {
        /// The named database, or `None` for the current default database.
        name: Option<String>,
        /// Canonical database options in Go AST source order.
        options: Vec<DatabaseOption>,
    },
    /// A `CREATE [OR REPLACE] PLACEMENT POLICY [IF NOT EXISTS]` statement.
    CreatePlacementPolicy(Box<crate::CreatePlacementPolicyStmt>),
    /// An `ALTER PLACEMENT POLICY [IF EXISTS] name` statement.
    AlterPlacementPolicy(Box<crate::AlterPlacementPolicyStmt>),
    /// An `ALTER TABLE` statement.
    AlterTable(Box<AlterTableStmt>),
    /// A `RENAME TABLE` statement.
    RenameTable(Box<RenameTableStmt>),
    /// A `RENAME USER old_user TO new_user [, ...]` statement.
    RenameUser {
        /// The user-identity rename pairs in source order.
        pairs: Vec<RenameUserPair>,
    },
    /// A `LOCK TABLE[S] table_name [READ|WRITE [LOCAL]] [, ...]` statement.
    LockTables(Box<Vec<TableLock>>),
    /// An `UNLOCK TABLE[S]` statement.
    UnlockTables,
    /// A `DROP TABLE` statement.
    DropTable(Box<DropTableStmt>),
    /// A `DROP VIEW [IF EXISTS] name [, ...]` statement.
    DropView {
        /// Whether missing views are ignored.
        if_exists: bool,
        /// The view names in source order.
        names: Vec<Vec<String>>,
    },
    /// A `DROP {DATABASE | SCHEMA} [IF EXISTS] name` statement.
    DropDatabase {
        /// Whether missing databases are ignored.
        if_exists: bool,
        /// The database name.
        name: String,
    },
    /// A `DROP PLACEMENT POLICY [IF EXISTS] name` statement.
    DropPlacementPolicy(Box<crate::DropPlacementPolicyStmt>),
    /// A `DROP RESOURCE GROUP [IF EXISTS] name` statement.
    DropResourceGroup(Box<crate::DropResourceGroupStmt>),
    /// A `CREATE RESOURCE GROUP [IF NOT EXISTS] name option [, option ...]` statement.
    CreateResourceGroup(Box<crate::CreateResourceGroupStmt>),
    /// An `ALTER RESOURCE GROUP [IF EXISTS] name option [, option ...]` statement.
    AlterResourceGroup(Box<crate::AlterResourceGroupStmt>),
    /// A `CREATE [OR REPLACE] MASKING POLICY` statement.
    CreateMaskingPolicy(Box<crate::CreateMaskingPolicyStmt>),
    /// A `CREATE USER [IF NOT EXISTS] user_spec [, ...]` statement.
    CreateUser {
        /// Whether duplicate-account errors are suppressed.
        if_not_exists: bool,
        /// The account specifications.
        users: Vec<CreateUserSpec>,
        /// Statement-global `REQUIRE` TLS/token options, in Go parser order.
        tls_options: Vec<crate::AlterUserTlsOption>,
        /// Statement-global `WITH MAX_*` resource limits, in Go parser order.
        resource_options: Vec<crate::AlterUserResourceOption>,
        /// Statement-global password/account-lock policies, in source order.
        password_options: Vec<CreateUserPasswordOption>,
        /// The optional statement-global account annotation.
        comment_or_attribute: Option<CreateUserCommentOrAttribute>,
        /// The optional statement-global resource group, parsed after the
        /// comment/attribute option and restored as an identifier.
        resource_group: Option<String>,
    },
    /// A `CREATE ROLE [IF NOT EXISTS] role_spec [, ...]` statement.
    ///
    /// Roles deliberately use [`RoleSpec`] rather than [`UserSpec`]: Go's
    /// `Rolename` grammar rejects a bare non-reserved keyword unless it is in
    /// the composed `role@host` form, and it never permits `CURRENT_USER`.
    CreateRole {
        /// Whether duplicate-role errors are suppressed.
        if_not_exists: bool,
        /// The role identities in source order.
        roles: Vec<RoleSpec>,
    },
    /// An `ALTER USER [IF EXISTS] user_spec [, ...] [PASSWORD EXPIRE ...]`
    /// statement.
    AlterUser(Box<crate::AlterUserStmt>),
    /// A `DROP USER` or `DROP ROLE` statement.
    DropUser {
        /// Whether this is `DROP ROLE` rather than `DROP USER`.
        is_role: bool,
        /// Whether missing-account errors are suppressed.
        if_exists: bool,
        /// The accounts or roles.
        users: Vec<UserSpec>,
    },
    /// A `TRUNCATE [TABLE] name` statement.
    TruncateTable(Box<Vec<String>>),
    /// A `CREATE SEQUENCE` statement.
    CreateSequence(Box<CreateSequenceStmt>),
    /// An `ALTER SEQUENCE` statement.
    AlterSequence(Box<AlterSequenceStmt>),
    /// A `DROP SEQUENCE` statement.
    DropSequence(Box<DropSequenceStmt>),
    /// `ALTER INSTANCE RELOAD TLS [NO ROLLBACK ON ERROR]`.
    AlterInstance(Box<AlterInstanceStmt>),
    /// `ALTER RANGE name placement_option`.
    AlterRange(Box<AlterRangeStmt>),
}

impl DdlStmt {
    pub(crate) fn restore_into_bytes(&self, out: &mut Vec<u8>, context: RestoreContext) {
        match self {
            Self::CreateTable(table) => table.restore_into_bytes(out, context),
            _ => {
                let mut text = String::new();
                self.restore_into_with_context(&mut text, context);
                out.extend_from_slice(text.as_bytes());
            }
        }
    }

    /// Appends the ordinary canonical SQL used by [`crate::Stmt::restore`].
    /// Context-sensitive callers enter through [`Self::restore_into_with_context`].
    pub(crate) fn restore_into(&self, out: &mut String) {
        match self {
            Self::CreateTable(table) => table.restore_into(out),
            Self::CreateView(view) => view.restore_into(out),
            Self::CreateIndex(index) => index.restore_into(out),
            Self::DropIndex(index) => index.restore_into(out),
            Self::AlterTable(table) => table.restore_into(out),
            _ => self.restore_into_with_context(out, RestoreContext::default()),
        }
    }

    /// Appends this DDL statement using the shared AST restore context.
    pub(crate) fn restore_into_with_context(&self, out: &mut String, context: RestoreContext) {
        match self {
            Self::CreateTable(table) => table.restore_into_with_context(out, context),
            Self::CreateView(view) => view.restore_into(out),
            Self::CreateIndex(index) => index.restore_into_with_context(out, context),
            Self::DropIndex(index) => index.restore_into_with_context(out, context),
            Self::CreateDatabase {
                if_not_exists,
                name,
                options,
            } => {
                out.push_str("CREATE DATABASE ");
                if *if_not_exists {
                    out.push_str("IF NOT EXISTS ");
                }
                out.push_str(&back_quote(name));
                for option in options {
                    out.push(' ');
                    option.restore_into(out);
                }
            }
            Self::AlterDatabase { name, options } => {
                out.push_str("ALTER DATABASE");
                if let Some(name) = name {
                    out.push(' ');
                    out.push_str(&back_quote(name));
                }
                for option in options {
                    out.push(' ');
                    option.restore_into(out);
                }
            }
            Self::CreatePlacementPolicy(statement) => statement.restore_into(out),
            Self::AlterPlacementPolicy(statement) => statement.restore_into(out),
            Self::AlterTable(table) => table.restore_into_with_context(out, context),
            Self::RenameTable(table) => table.restore_into(out),
            Self::RenameUser { pairs } => {
                out.push_str("RENAME USER ");
                for (index, pair) in pairs.iter().enumerate() {
                    if index > 0 {
                        out.push_str(", ");
                    }
                    pair.old_user.restore_into(out);
                    out.push_str(" TO ");
                    pair.new_user.restore_into(out);
                }
            }
            Self::LockTables(locks) => {
                out.push_str("LOCK TABLES ");
                for (index, lock) in locks.iter().enumerate() {
                    if index > 0 {
                        out.push_str(", ");
                    }
                    push_name_path(out, &lock.table);
                    out.push(' ');
                    out.push_str(lock.lock_type.restore());
                }
            }
            Self::UnlockTables => out.push_str("UNLOCK TABLES"),
            Self::DropTable(table) => table.restore_into(out),
            Self::DropView { if_exists, names } => {
                out.push_str("DROP VIEW ");
                if *if_exists {
                    out.push_str("IF EXISTS ");
                }
                for (index, name) in names.iter().enumerate() {
                    if index > 0 {
                        out.push_str(", ");
                    }
                    push_name_path(out, name);
                }
            }
            Self::DropDatabase { if_exists, name } => {
                out.push_str("DROP DATABASE ");
                if *if_exists {
                    out.push_str("IF EXISTS ");
                }
                out.push_str(&back_quote(name));
            }
            Self::DropPlacementPolicy(statement) => statement.restore_into(out),
            Self::DropResourceGroup(statement) => statement.restore_into(out),
            Self::CreateResourceGroup(statement) => statement.restore_into(out),
            Self::AlterResourceGroup(statement) => statement.restore_into(out),
            Self::CreateMaskingPolicy(statement) => statement.restore_into(out),
            Self::CreateUser {
                if_not_exists,
                users,
                tls_options,
                resource_options,
                password_options,
                comment_or_attribute,
                resource_group,
            } => {
                out.push_str("CREATE USER ");
                if *if_not_exists {
                    out.push_str("IF NOT EXISTS ");
                }
                for (index, user) in users.iter().enumerate() {
                    if index > 0 {
                        out.push_str(", ");
                    }
                    user.restore_into(out);
                }
                if !tls_options.is_empty() {
                    out.push_str(" REQUIRE ");
                    for (index, option) in tls_options.iter().enumerate() {
                        if index > 0 {
                            out.push_str(" AND ");
                        }
                        option.restore_into(out);
                    }
                }
                if !resource_options.is_empty() {
                    out.push_str(" WITH");
                    for option in resource_options {
                        out.push(' ');
                        out.push_str(option.kind.as_sql());
                        out.push(' ');
                        out.push_str(&option.count.to_string());
                    }
                }
                for option in password_options {
                    out.push(' ');
                    option.restore_into(out);
                }
                if let Some(option) = comment_or_attribute {
                    out.push(' ');
                    option.restore_into(out);
                }
                if let Some(resource_group) = resource_group {
                    out.push_str(" RESOURCE GROUP ");
                    out.push_str(&back_quote(resource_group));
                }
            }
            Self::CreateRole {
                if_not_exists,
                roles,
            } => {
                out.push_str("CREATE ROLE ");
                if *if_not_exists {
                    out.push_str("IF NOT EXISTS ");
                }
                for (index, role) in roles.iter().enumerate() {
                    if index > 0 {
                        out.push_str(", ");
                    }
                    role.restore_into(out);
                }
            }
            Self::AlterUser(statement) => statement.restore_into(out),
            Self::DropUser {
                is_role,
                if_exists,
                users,
            } => {
                out.push_str(if *is_role { "DROP ROLE " } else { "DROP USER " });
                if *if_exists {
                    out.push_str("IF EXISTS ");
                }
                for (index, user) in users.iter().enumerate() {
                    if index > 0 {
                        out.push_str(", ");
                    }
                    user.restore_into(out);
                }
            }
            Self::TruncateTable(name) => {
                out.push_str("TRUNCATE TABLE ");
                push_name_path(out, name);
            }
            Self::CreateSequence(sequence) => sequence.restore_into(out),
            Self::AlterSequence(sequence) => sequence.restore_into(out),
            Self::DropSequence(sequence) => sequence.restore_into(out),
            Self::AlterInstance(instance) => instance.restore_into(out),
            Self::AlterRange(range) => range.restore_into(out),
        }
    }
}
