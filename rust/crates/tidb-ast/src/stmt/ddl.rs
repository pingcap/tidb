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
    CreateProcedureStmt, CreateSequenceStmt, CreateTableStmt, CreateUserCommentOrAttribute,
    CreateUserPasswordOption, CreateUserSpec, CreateViewStmt, DropIndexStmt, DropProcedureStmt,
    DropSequenceStmt, DropTableStmt, Expr, RenameTableStmt, RestoreContext, RoleSpec,
    TableLockType, UserSpec,
};

/// One `old_user TO new_user` pair in `RENAME USER`.
#[derive(Debug, Clone, PartialEq)]
pub struct RenameUserPair {
    /// Account identity before the rename.
    pub old_user: UserSpec,
    /// Account identity after the rename.
    pub new_user: UserSpec,
}

/// `FLASHBACK DATABASE name [TO new_name]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlashbackDatabaseStmt {
    /// Original database name.
    pub name: String,
    /// Optional restored database name.
    pub new_name: Option<String>,
}

/// `RECOVER TABLE table [job_num]` or `RECOVER TABLE BY JOB job_id`.
///
/// The optional table preserves Go's constructible partial AST: absence picks
/// the BY JOB branch, while a present table takes precedence over `job_id`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoverTableStmt {
    /// DDL job identifier used by the BY JOB form.
    pub job_id: i64,
    /// Table restored by name; `None` selects BY JOB.
    pub table: Option<Vec<String>>,
    /// Optional historical job count following a table name.
    pub job_num: i64,
}

/// `FLASHBACK {TABLE ... | DATABASE ... | CLUSTER} TO {TIMESTAMP expr | TSO n}`.
///
/// Field precedence exactly follows Go restore: non-empty `tables`, then a
/// non-empty database name, otherwise CLUSTER. A nonzero TSO selects the TSO
/// branch; otherwise `flashback_ts` is required and restore panics if absent,
/// matching Go's nil-interface dereference for an invalid hand-built node.
#[derive(Debug, Clone, PartialEq)]
pub struct FlashbackToTimestampStmt {
    /// Timestamp expression used when `flashback_tso == 0`.
    pub flashback_ts: Option<Expr>,
    /// Numeric TSO; zero selects the timestamp-expression branch.
    pub flashback_tso: u64,
    /// Tables in source order; non-empty takes target precedence.
    pub tables: Vec<Vec<String>>,
    /// Database target, or an empty string for CLUSTER.
    pub database_name: String,
}

/// `FLASHBACK TABLE table [TO new_name]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlashbackTableStmt {
    /// Table to restore. Invalid hand-built absence panics during restore,
    /// matching Go's required pointer dereference.
    pub table: Option<Vec<String>>,
    /// New table name; empty means retain the original name.
    pub new_name: String,
}

/// `OPTIMIZE [NO_WRITE_TO_BINLOG] TABLE table [, ...]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptimizeTableStmt {
    /// Suppress binary-log recording (`LOCAL` is the input alias).
    pub no_write_to_binlog: bool,
    /// Tables in source order.
    pub tables: Vec<Vec<String>>,
}

/// `ADMIN REPAIR TABLE table CREATE TABLE ...`.
#[derive(Debug, Clone, PartialEq)]
pub struct RepairTableStmt {
    /// Table being repaired.
    pub table: Vec<String>,
    /// Replacement table definition.
    pub create: CreateTableStmt,
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
    /// A `CREATE PROCEDURE` statement.
    CreateProcedure(Box<CreateProcedureStmt>),
    /// An `ALTER SEQUENCE` statement.
    AlterSequence(Box<AlterSequenceStmt>),
    /// A `DROP SEQUENCE` statement.
    DropSequence(Box<DropSequenceStmt>),
    /// A `DROP PROCEDURE` statement.
    DropProcedure(Box<DropProcedureStmt>),
    /// `ALTER INSTANCE RELOAD TLS [NO ROLLBACK ON ERROR]`.
    AlterInstance(Box<AlterInstanceStmt>),
    /// `ALTER RANGE name placement_option`.
    AlterRange(Box<AlterRangeStmt>),
    /// Restore a dropped database under its original or a new name.
    FlashbackDatabase(Box<FlashbackDatabaseStmt>),
    /// Recover a dropped table by table name or DDL job identifier.
    RecoverTable(Box<RecoverTableStmt>),
    /// Restore tables, a database, or the cluster to a timestamp or TSO.
    FlashbackToTimestamp(Box<FlashbackToTimestampStmt>),
    /// Restore a dropped or truncated table, optionally under a new name.
    FlashbackTable(Box<FlashbackTableStmt>),
    /// Optimize one or more tables.
    OptimizeTable(Box<OptimizeTableStmt>),
    /// Repair a table using an explicit replacement definition.
    RepairTable(Box<RepairTableStmt>),
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
                let skip_placement = context.flags().has_skip_placement_rule_for_restore();
                if skip_placement
                    && !options.is_empty()
                    && options
                        .iter()
                        .all(|option| matches!(option, DatabaseOption::PlacementPolicy(_)))
                {
                    return;
                }
                out.push_str("ALTER DATABASE");
                if let Some(name) = name {
                    out.push(' ');
                    out.push_str(&back_quote(name));
                }
                for option in options {
                    out.push(' ');
                    if skip_placement && matches!(option, DatabaseOption::PlacementPolicy(_)) {
                        continue;
                    }
                    option.restore_into(out);
                }
            }
            Self::CreatePlacementPolicy(statement) => statement.restore_into(out),
            Self::AlterPlacementPolicy(statement) => {
                if !context.flags().has_skip_placement_rule_for_restore() {
                    statement.restore_into(out);
                }
            }
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
                    out.push_str(lock.lock_type.sql());
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
            Self::CreateProcedure(procedure) => procedure.restore_into(out),
            Self::AlterSequence(sequence) => sequence.restore_into(out),
            Self::DropSequence(sequence) => sequence.restore_into(out),
            Self::DropProcedure(procedure) => procedure.restore_into(out),
            Self::AlterInstance(instance) => instance.restore_into(out),
            Self::AlterRange(range) => range.restore_into(out),
            Self::FlashbackDatabase(statement) => {
                out.push_str("FLASHBACK DATABASE ");
                out.push_str(&back_quote(&statement.name));
                if let Some(new_name) = &statement.new_name {
                    out.push_str(" TO ");
                    out.push_str(&back_quote(new_name));
                }
            }
            Self::RecoverTable(statement) => {
                out.push_str("RECOVER TABLE ");
                if let Some(table) = &statement.table {
                    push_name_path(out, table);
                    if statement.job_num > 0 {
                        out.push(' ');
                        out.push_str(&statement.job_num.to_string());
                    }
                } else {
                    out.push_str("BY JOB ");
                    out.push_str(&statement.job_id.to_string());
                }
            }
            Self::FlashbackToTimestamp(statement) => {
                out.push_str("FLASHBACK ");
                if !statement.tables.is_empty() {
                    out.push_str("TABLE ");
                    for (index, table) in statement.tables.iter().enumerate() {
                        if index > 0 {
                            out.push_str(", ");
                        }
                        push_name_path(out, table);
                    }
                } else if !statement.database_name.is_empty() {
                    out.push_str("DATABASE ");
                    out.push_str(&back_quote(&statement.database_name));
                } else {
                    out.push_str("CLUSTER");
                }
                if statement.flashback_tso == 0 {
                    out.push_str(" TO TIMESTAMP ");
                    statement
                        .flashback_ts
                        .as_ref()
                        .expect("FLASHBACK TO TIMESTAMP requires an expression")
                        .restore_into(out);
                } else {
                    out.push_str(" TO TSO ");
                    out.push_str(&statement.flashback_tso.to_string());
                }
            }
            Self::FlashbackTable(statement) => {
                out.push_str("FLASHBACK TABLE ");
                push_name_path(
                    out,
                    statement
                        .table
                        .as_ref()
                        .expect("FLASHBACK TABLE requires a table"),
                );
                if !statement.new_name.is_empty() {
                    out.push_str(" TO ");
                    out.push_str(&back_quote(&statement.new_name));
                }
            }
            Self::OptimizeTable(statement) => {
                out.push_str("OPTIMIZE ");
                if statement.no_write_to_binlog {
                    out.push_str("NO_WRITE_TO_BINLOG ");
                }
                out.push_str("TABLE ");
                for (index, table) in statement.tables.iter().enumerate() {
                    if index > 0 {
                        out.push_str(", ");
                    }
                    push_name_path(out, table);
                }
            }
            Self::RepairTable(statement) => {
                out.push_str("ADMIN REPAIR TABLE ");
                push_name_path(out, &statement.table);
                out.push(' ');
                statement.create.restore_into_with_context(out, context);
            }
        }
    }
}

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for RenameUserPair {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { old_user, new_user } = self;
        if !crate::Visitable::accept(old_user, visitor) {
            return false;
        }
        if !crate::Visitable::accept(new_user, visitor) {
            return false;
        }
        let _ = old_user;
        let _ = new_user;
        visitor.leave(self)
    }
}

impl crate::Visitable for FlashbackDatabaseStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { name, new_name } = self;
        let _ = name;
        let _ = new_name;
        visitor.leave(self)
    }
}

impl crate::Visitable for RecoverTableStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            job_id,
            table,
            job_num,
        } = self;
        let _ = job_id;
        let _ = table;
        let _ = job_num;
        visitor.leave(self)
    }
}

impl crate::Visitable for FlashbackToTimestampStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            flashback_ts,
            flashback_tso,
            tables,
            database_name,
        } = self;
        if *flashback_tso == 0 {
            if let Some(expression) = flashback_ts.as_mut() {
                if !crate::Visitable::accept(expression, visitor) {
                    return false;
                }
            }
        }
        let _ = flashback_ts;
        let _ = flashback_tso;
        let _ = tables;
        let _ = database_name;
        visitor.leave(self)
    }
}

impl crate::Visitable for FlashbackTableStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { table, new_name } = self;
        let _ = table;
        let _ = new_name;
        visitor.leave(self)
    }
}

impl crate::Visitable for OptimizeTableStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            no_write_to_binlog,
            tables,
        } = self;
        let _ = no_write_to_binlog;
        let _ = tables;
        visitor.leave(self)
    }
}

impl crate::Visitable for RepairTableStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { table, create } = self;
        if !crate::Visitable::accept(create, visitor) {
            return false;
        }
        let _ = table;
        let _ = create;
        visitor.leave(self)
    }
}

impl crate::Visitable for TableLock {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { table, lock_type } = self;
        if !crate::Visitable::accept(lock_type, visitor) {
            return false;
        }
        let _ = table;
        let _ = lock_type;
        visitor.leave(self)
    }
}

impl crate::Visitable for DatabaseOption {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::CharacterSet(field_0) => {
                let _ = field_0;
            }
            Self::Collate(field_0) => {
                let _ = field_0;
            }
            Self::Encryption(field_0) => {
                let _ = field_0;
            }
            Self::PlacementPolicy(field_0) => {
                let _ = field_0;
            }
            Self::SetTiFlashReplica { count, labels } => {
                let _ = count;
                let _ = labels;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for DdlStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::CreateTable(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::CreateView(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::CreateIndex(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::DropIndex(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::CreateDatabase {
                if_not_exists,
                name,
                options,
            } => {
                for value in options.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                let _ = if_not_exists;
                let _ = name;
                let _ = options;
            }
            Self::AlterDatabase { name, options } => {
                for value in options.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                let _ = name;
                let _ = options;
            }
            Self::CreatePlacementPolicy(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::AlterPlacementPolicy(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::AlterTable(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::RenameTable(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::RenameUser { pairs } => {
                for value in pairs.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                let _ = pairs;
            }
            Self::LockTables(field_0) => {
                for value in field_0.as_mut().iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                let _ = field_0;
            }
            Self::UnlockTables => {}
            Self::DropTable(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::DropView { if_exists, names } => {
                let _ = if_exists;
                let _ = names;
            }
            Self::DropDatabase { if_exists, name } => {
                let _ = if_exists;
                let _ = name;
            }
            Self::DropPlacementPolicy(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::DropResourceGroup(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::CreateResourceGroup(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::AlterResourceGroup(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::CreateMaskingPolicy(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::CreateUser {
                if_not_exists,
                users,
                tls_options,
                resource_options,
                password_options,
                comment_or_attribute,
                resource_group,
            } => {
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
                for value in resource_options.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                for value in password_options.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                if let Some(value) = comment_or_attribute.as_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                let _ = if_not_exists;
                let _ = users;
                let _ = tls_options;
                let _ = resource_options;
                let _ = password_options;
                let _ = comment_or_attribute;
                let _ = resource_group;
            }
            Self::CreateRole {
                if_not_exists,
                roles,
            } => {
                for value in roles.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                let _ = if_not_exists;
                let _ = roles;
            }
            Self::AlterUser(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::DropUser {
                is_role,
                if_exists,
                users,
            } => {
                for value in users.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                let _ = is_role;
                let _ = if_exists;
                let _ = users;
            }
            Self::TruncateTable(field_0) => {
                let _ = field_0;
            }
            Self::CreateSequence(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::CreateProcedure(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::AlterSequence(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::DropSequence(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::DropProcedure(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::AlterInstance(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::AlterRange(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::FlashbackDatabase(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::RecoverTable(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::FlashbackToTimestamp(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::FlashbackTable(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::OptimizeTable(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::RepairTable(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}
// END GENERATED AST VISITOR IMPLEMENTATIONS

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DdlStmt, Stmt};

    fn ddl(statement: DdlStmt) -> String {
        Stmt::Ddl(crate::NodeBox::new(statement)).restore()
    }

    #[test]
    fn recover_and_flashback_statements_preserve_go_field_precedence() {
        assert_eq!(
            ddl(DdlStmt::RecoverTable(Box::new(RecoverTableStmt {
                job_id: 99,
                table: Some(vec!["db".into(), "t".into()]),
                job_num: 3,
            }))),
            "RECOVER TABLE `db`.`t` 3"
        );
        assert_eq!(
            ddl(DdlStmt::RecoverTable(Box::new(RecoverTableStmt {
                job_id: 99,
                table: None,
                job_num: 3,
            }))),
            "RECOVER TABLE BY JOB 99"
        );
        assert_eq!(
            ddl(DdlStmt::FlashbackToTimestamp(Box::new(
                FlashbackToTimestampStmt {
                    flashback_ts: Some(Expr::String("2026-07-23".into())),
                    flashback_tso: 0,
                    tables: vec![vec!["db".into(), "t".into()], vec!["t2".into()]],
                    database_name: "ignored".into(),
                },
            ))),
            "FLASHBACK TABLE `db`.`t`, `t2` TO TIMESTAMP _UTF8MB4'2026-07-23'"
        );
        assert_eq!(
            ddl(DdlStmt::FlashbackToTimestamp(Box::new(
                FlashbackToTimestampStmt {
                    flashback_ts: None,
                    flashback_tso: 123,
                    tables: Vec::new(),
                    database_name: "db".into(),
                },
            ))),
            "FLASHBACK DATABASE `db` TO TSO 123"
        );
        assert_eq!(
            ddl(DdlStmt::FlashbackToTimestamp(Box::new(
                FlashbackToTimestampStmt {
                    flashback_ts: None,
                    flashback_tso: 123,
                    tables: Vec::new(),
                    database_name: String::new(),
                },
            ))),
            "FLASHBACK CLUSTER TO TSO 123"
        );
        assert_eq!(
            ddl(DdlStmt::FlashbackTable(Box::new(FlashbackTableStmt {
                table: Some(vec!["old".into()]),
                new_name: "new".into(),
            }))),
            "FLASHBACK TABLE `old` TO `new`"
        );
    }
}
