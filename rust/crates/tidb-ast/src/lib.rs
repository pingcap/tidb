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

//! TiDB SQL abstract syntax tree (Phase 0 subset) with SQL restore.
//!
//! `restore()` regenerates canonical SQL matching the Go AST's
//! `Restore(DefaultRestoreFlags)` output (uppercase keywords, back-quoted
//! names, `_UTF8MB4'…'` string literals, tight symbolic operators). Restore
//! equality against the Go parser is the differential-correctness signal for
//! the Rust parser (see the `difftest` crate).
//!
//! ## Module layout
//!
//! Split by concern, mirroring `tidb-parser`'s own module boundaries
//! (confirmed by checking where each statement's parsing function lives
//! there, so an agent already oriented in one crate's layout finds the same
//! shape in the other): [`ddl`] (`CREATE`/`ALTER`/`RENAME`/`DROP TABLE`),
//! [`dml`] (`INSERT`/`UPDATE`/`DELETE`), [`binding`] (SQL-binding payloads),
//! [`resource_group`] (typed CREATE/ALTER/DROP RESOURCE GROUP payloads and
//! options), [`show`] (ordinary metadata inspection), [`select`] (`SELECT`/set
//! operations/`FROM` join tree), [`expr`] (scalar expressions, `CAST`/
//! `CONVERT`, operators), and [`util`] (shared low-level restore
//! primitives — identifier quoting, name-path formatting, literal
//! normalization — with no domain of their own). Each domain module holds
//! BOTH its types AND their `restore_into` impls together, so working on
//! one domain never requires opening a second file. [`Stmt`] itself and the
//! root statement envelope stays here; session/system-variable payloads live
//! under their source-owned statement modules.

#[path = "stmt/admin.rs"]
mod admin;
#[path = "stmt/analyze.rs"]
mod analyze;
#[path = "stmt/binding.rs"]
mod binding;
mod ddl;
#[path = "stmt/ddl.rs"]
mod ddl_stmt;
mod dml;
#[path = "stmt/dml.rs"]
mod dml_stmt;
#[path = "stmt/explain.rs"]
mod explain;
mod expr;
#[path = "stmt/flush.rs"]
mod flush;
mod format;
#[path = "stmt/load_data.rs"]
mod load_data;
#[path = "stmt/masking.rs"]
mod masking;
pub mod opcode;
#[path = "stmt/placement.rs"]
mod placement;
#[path = "stmt/query.rs"]
mod query;
#[path = "stmt/resource_group.rs"]
mod resource_group;
mod select;
#[path = "stmt/sequence.rs"]
mod sequence;
#[path = "stmt/session.rs"]
mod session;
#[path = "stmt/set.rs"]
mod set;
#[path = "stmt/show.rs"]
mod show;
#[path = "stmt/traffic.rs"]
mod traffic;
#[path = "stmt/transaction.rs"]
mod transaction;
#[path = "stmt/user.rs"]
mod user;
#[path = "stmt/user_variable.rs"]
mod user_variable;
mod util;

pub use admin::{
    AdminAlterDdlJobOption, AdminAlterDdlJobsStmt, AdminCheckHandleRange, AdminCheckStmt,
    AdminChecksumStmt, AdminCleanupTableLockStmt, AdminDdlJobControlKind, AdminDdlJobControlStmt,
    AdminPlanCacheScope, AdminRecoverIndexStmt, AdminReloadKind, AdminShowDdlJobQueriesStmt,
    AdminShowDdlJobsStmt, AdminShowNextRowIdStmt, AdminShowSlowMode, AdminShowSlowStmt,
    AdminShowSlowTopScope, AdminStmt, BdrRole, DropStatsStmt, GrantLevel, GrantObjectType,
    GrantPrivilege, GrantRoleStmt, GrantStmt, LoadStatsStmt, PlanReplayerDumpExplainStmt,
    RevokeRoleStmt, RevokeStmt, ShowGrantsStmt,
};
pub use analyze::{
    AnalyzeIncrementalStmt, AnalyzeIncrementalTarget, AnalyzeOption, AnalyzeOptionKind,
    AnalyzeTableStmt, AnalyzeTarget,
};
pub use binding::{
    BindingScope, BindingStatementTarget, BindingStatus, BindingValue, CreateBindingSource,
    CreateBindingStmt, DropBindingStmt, DropBindingTarget, SetBindingStmt, SetBindingTarget,
    ShowBindingsFilter, ShowBindingsStmt,
};
pub use ddl::*;
pub use ddl_stmt::{DatabaseOption, DdlStmt, RenameUserPair, TableLock, TableLockType};
pub use dml::*;
pub use dml_stmt::{BatchDml, BatchDmlDryRun, BatchDmlStmt, DmlStmt};
pub use explain::{DescribeTableStmt, ExplainStmt, StatsLockStmt, StatsLockTable};
pub use expr::*;
pub use flush::FlushStmt;
pub use format::{CteRestorer, CteScope, RestoreContext, RestoreCtx, RestoreFlags, RestoreWriter};
pub use load_data::{
    ColumnOrUserVar, LoadDataFields, LoadDataLines, LoadDataOnDuplicate, LoadDataOption,
    LoadDataStmt,
};
pub use masking::{
    AlterMaskingPolicyAction, CreateMaskingPolicyStmt, MaskingPolicyRestrictOps, MaskingPolicyState,
};
pub use opcode::Op;
pub use placement::{
    AlterPlacementPolicyStmt, CreatePlacementPolicyStmt, DropPlacementPolicyStmt, PlacementOption,
    PlacementRestoreMode,
};
pub use query::QueryStmt;
pub use resource_group::{
    AlterResourceGroupStmt, CreateResourceGroupStmt, DropResourceGroupStmt,
    ResourceGroupBackgroundOption, ResourceGroupBurstable, ResourceGroupOption,
    ResourceGroupPriority, ResourceGroupRate, ResourceGroupRunawayAction,
    ResourceGroupRunawayOption, ResourceGroupRunawayRule, ResourceGroupRunawayWatch,
    ResourceGroupRunawayWatchType,
};
pub use select::*;
pub use sequence::{
    AlterInstanceStmt, AlterRangeStmt, AlterSequenceStmt, CreateSequenceStmt, DropSequenceStmt,
    SequenceOption,
};
pub use session::{
    DefaultRoleSelection, RoleSpec, SessionStmt, SetDefaultRoleStmt, SetPasswordStmt,
    SetRoleSelection, SetRoleStmt,
};
pub use set::{
    CharsetSetKind, SetResourceGroupStmt, SetSessionStatesStmt, SetStmt, SetVariableValue,
    SystemVariableAssignment, SystemVariableScope,
};
pub use show::{
    ShowCharsetFilter, ShowCharsetStmt, ShowCollationFilter, ShowCollationStmt, ShowColumnsFilter,
    ShowColumnsStmt, ShowCreateKind, ShowDatabasesFilter, ShowDatabasesStmt, ShowEnginesFilter,
    ShowEnginesStmt, ShowErrorsFilter, ShowErrorsStmt, ShowIndexFilter, ShowIndexStmt,
    ShowOpenTablesStmt, ShowStatsBucketsFilter, ShowStatsBucketsStmt, ShowStatsHistogramsFilter,
    ShowStatsHistogramsStmt, ShowStatsLockedFilter, ShowStatsLockedStmt, ShowStatsTopNFilter,
    ShowStatsTopNStmt, ShowStatusFilter, ShowStatusStmt, ShowTableNextRowIdStmt,
    ShowTableStatusFilter, ShowTableStatusStmt, ShowTablesFilter, ShowTablesStmt,
    ShowWarningsFilter, ShowWarningsStmt,
};
pub use traffic::{
    RefreshStatsMode, RefreshStatsStmt, StatsObject, TrafficCaptureOption, TrafficReplayOption,
    TrafficStmt,
};
pub use transaction::BeginStmt;
pub use user::{
    AlterUserDualPassword, AlterUserPasswordExpire, AlterUserResourceKind, AlterUserResourceOption,
    AlterUserStmt, AlterUserTlsOption, CreateUserAuth, CreateUserCommentOrAttribute,
    CreateUserCredential, CreateUserPasswordOption, CreateUserSpec, UserSpec,
};
pub use user_variable::{SetUserVarStmt, UserVariableAssignment};
pub use util::restore_string_literal;

/// A parsed statement. Variants are boxed so the enum stays small regardless of
/// the per-statement payload size.
#[derive(Debug, Clone, PartialEq)]
pub enum Stmt {
    /// A query (`SELECT` or a set operation).
    Query(Box<QueryStmt>),
    /// A data-manipulation statement (`INSERT`, `UPDATE`, or `DELETE`).
    Dml(Box<DmlStmt>),
    /// A data-definition statement.
    Ddl(Box<DdlStmt>),
    /// An administrative, inspection, or diagnostics command.
    Admin(Box<AdminStmt>),
    /// A session-scoped command such as `USE`, `SET`, or transaction control.
    Session(Box<SessionStmt>),
}

/// The transaction mode carried by Go's `ast.BeginStmt.Mode`.
///
/// `Default` represents both a bare `BEGIN` and `START TRANSACTION`; Go's
/// restore canonicalizes those forms to `START TRANSACTION`. The explicit
/// variants must not collapse into the default because parser restore is part
/// of the compatibility contract even where an executor lacks TiKV's distinct
/// transaction implementations.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum TransactionMode {
    /// A bare `BEGIN` or `START TRANSACTION`.
    #[default]
    Default,
    /// `BEGIN OPTIMISTIC`.
    Optimistic,
    /// `BEGIN PESSIMISTIC`.
    Pessimistic,
}

impl Stmt {
    /// Restores this statement to canonical SQL.
    pub fn restore(&self) -> String {
        self.restore_with_context(RestoreContext::default())
    }

    /// Restores this statement using an explicit source-formatting context.
    ///
    /// In particular, [`RestoreFlags::TIDB_SPECIAL_COMMENT`] emits TiDB-only
    /// DDL fragments as `/*T![feature] ... */`, exactly as Go's
    /// `format.RestoreCtx` does.  Callers that need only a flag set can use
    /// [`Stmt::restore_with_flags`].
    pub fn restore_with_context(&self, context: RestoreContext) -> String {
        let mut out = String::new();
        self.restore_into_with_context(&mut out, context);
        out
    }

    /// Restores this statement using `flags`.
    pub fn restore_with_flags(&self, flags: RestoreFlags) -> String {
        self.restore_with_context(RestoreContext::new(flags))
    }

    /// Restores this statement into a byte-preserving buffer. This is the
    /// lossless counterpart to [`Stmt::restore`] for Go AST payloads such as
    /// GBK ENUM/SET members that are not valid UTF-8.
    pub fn restore_bytes(&self) -> Vec<u8> {
        self.restore_bytes_with_context(RestoreContext::default())
    }

    /// Byte-preserving restore with an explicit formatting context.
    pub fn restore_bytes_with_context(&self, context: RestoreContext) -> Vec<u8> {
        let mut out = Vec::new();
        match self {
            Stmt::Ddl(ddl) => ddl.restore_into_bytes(&mut out, context),
            _ => {
                let mut text = String::new();
                self.restore_into_with_context(&mut text, context);
                out.extend_from_slice(text.as_bytes());
            }
        }
        out
    }

    /// Like [`Stmt::restore`], but appends into an existing buffer —
    /// used by [`Expr::InSubquery`]'s own restore, the only `Expr`
    /// variant whose subquery may be a full [`Stmt`] (`Select` or
    /// `SetOpr`) rather than always a plain [`SelectStmt`].
    pub(crate) fn restore_into(&self, out: &mut String) {
        match self {
            Stmt::Query(query) => query.restore_into(out),
            Stmt::Dml(dml) => dml.restore_into(out),
            Stmt::Ddl(ddl) => ddl.restore_into(out),
            Stmt::Admin(admin) => admin.restore_into(out),
            Stmt::Session(session) => session.restore_into(out),
        }
    }

    /// Context-aware internal restore path.  Most statement families do not
    /// yet own a context-sensitive source feature, so they retain their
    /// existing narrow restore implementations until they do.
    pub(crate) fn restore_into_with_context(&self, out: &mut String, context: RestoreContext) {
        match self {
            Stmt::Query(query) => query.restore_into(out),
            Stmt::Dml(dml) => dml.restore_into(out),
            Stmt::Ddl(ddl) => ddl.restore_into_with_context(out, context),
            Stmt::Admin(admin) => admin.restore_into(out),
            Stmt::Session(session) => session.restore_into(out),
        }
    }
}

/// The SQL source of a `PREPARE` statement: a string literal or a user
/// variable holding the SQL text.
#[derive(Debug, Clone, PartialEq)]
pub enum PrepareSource {
    /// `PREPARE ... FROM 'sql text'` — restored as a plain single-quoted
    /// string (no `_UTF8MB4` charset introducer, confirmed via `godump`).
    Sql(String),
    /// `PREPARE ... FROM @var` — the user-variable name (without `@`).
    Var(String),
}
