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
mod base;
#[path = "stmt/binding.rs"]
mod binding;
#[path = "stmt/brie.rs"]
mod brie;
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
mod label;
#[path = "stmt/load_data.rs"]
mod load_data;
#[path = "stmt/masking.rs"]
mod masking;
#[path = "stmt/misc.rs"]
mod misc;
mod model;
pub mod opcode;
#[path = "stmt/placement.rs"]
mod placement;
mod procedure;
#[path = "stmt/query.rs"]
mod query;
#[path = "stmt/resource_group.rs"]
mod resource_group;
mod select;
mod sem;
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
mod visitor;

pub use admin::{
    AdminAlterDdlJobOption, AdminAlterDdlJobsStmt, AdminBindingControlKind, AdminCheckHandleRange,
    AdminCheckStmt, AdminChecksumStmt, AdminCleanupTableLockStmt, AdminDdlJobControlKind,
    AdminDdlJobControlStmt, AdminPlanCacheScope, AdminRecoverIndexStmt, AdminReloadKind,
    AdminShowDdlJobQueriesStmt, AdminShowDdlJobsStmt, AdminShowNextRowIdStmt, AdminShowSlowMode,
    AdminShowSlowStmt, AdminShowSlowTopScope, AdminStmt, BdrRole, DropStatsStmt, GrantLevel,
    GrantObjectType, GrantPrivilege, GrantProxyStmt, GrantRoleStmt, GrantStmt, LoadStatsStmt,
    PlanReplayerStmt, PlanReplayerTarget, RevokeRoleStmt, RevokeStmt, ShowGrantsStmt,
    ShowImportGroupsStmt, ShowImportJobsStmt,
};
pub use analyze::{
    AnalyzeIncrementalStmt, AnalyzeOption, AnalyzeOptionKind, AnalyzeTableStmt, AnalyzeTarget,
    HistogramOperation,
};
pub use base::{NodeBox, NodeText};
pub use binding::{
    BindingScope, BindingStatementTarget, BindingStatus, BindingValue, CreateBindingSource,
    CreateBindingStmt, DropBindingStmt, DropBindingTarget, SetBindingStmt, SetBindingTarget,
    ShowBindingsFilter, ShowBindingsStmt,
};
pub use brie::{BrieKind, BrieOption, BrieOptionLevel, BrieOptionValue, BrieStmt};
pub use ddl::*;
pub use ddl_stmt::{
    DatabaseOption, DdlStmt, FlashbackDatabaseStmt, FlashbackTableStmt, FlashbackToTimestampStmt,
    OptimizeTableStmt, RecoverTableStmt, RenameUserPair, RepairTableStmt, TableLock,
};
pub use dml::*;
pub use dml_stmt::{BatchDml, BatchDmlDryRun, BatchDmlStmt, CallStmt, DmlStmt};
pub use explain::{DescribeTableStmt, ExplainStmt, ExplainTarget, StatsLockStmt, StatsLockTable};
pub use expr::*;
pub use flush::{FlushLogType, FlushStmt, FlushTarget};
pub use format::{
    CteRestorer, CteScope, RestoreContext, RestoreCtx, RestoreFlags, RestoreWriter,
    GO_SIMPLE_CASE_UNICODE_VERSION,
};
pub use load_data::{
    ColumnOrUserVar, LoadDataFields, LoadDataLines, LoadDataOnDuplicate, LoadDataOption,
    LoadDataStmt,
};
pub use masking::{
    AlterMaskingPolicyAction, CreateMaskingPolicyStmt, MaskingPolicyRestrictOps, MaskingPolicyState,
};
pub use misc::{
    BinlogStmt, CalibrateResourceOption, CalibrateResourceStmt, CalibrateWorkload,
    CreateStatisticsStmt, ExplainForStmt, ExtendedStatsType, KillStmt, KillTarget,
    RecommendIndexOption, RecommendIndexStmt, ServerControlStmt, SetConfigStmt, SetConfigTarget,
    TraceStmt,
};
pub use model::*;
pub use opcode::Op;
pub use placement::{
    AlterPlacementPolicyStmt, CreatePlacementPolicyStmt, DropPlacementPolicyStmt, PlacementOption,
    PlacementRestoreMode,
};
pub use procedure::*;
pub use query::QueryStmt;
pub use resource_group::{
    AddQueryWatchStmt, AlterResourceGroupStmt, CreateResourceGroupStmt, DropQueryWatchStmt,
    DropResourceGroupStmt, QueryWatchOption, QueryWatchRemoveTarget, QueryWatchTextOption,
    ResourceGroupBackgroundOption, ResourceGroupBurstable, ResourceGroupOption,
    ResourceGroupPriority, ResourceGroupRate, ResourceGroupRunawayAction,
    ResourceGroupRunawayOption, ResourceGroupRunawayRule, ResourceGroupRunawayWatch,
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
    CharsetSetKind, SetItem, SetResourceGroupStmt, SetSessionStatesStmt, SetStmt, SetVariableValue,
    SystemVariableAssignment, SystemVariableScope,
};
pub use show::{
    ShowCharsetFilter, ShowCharsetStmt, ShowCollationFilter, ShowCollationStmt, ShowColumnsFilter,
    ShowColumnsStmt, ShowCreateKind, ShowDatabasesFilter, ShowDatabasesStmt,
    ShowDistributionJobsStmt, ShowEnginesFilter, ShowEnginesStmt, ShowErrorsFilter, ShowErrorsStmt,
    ShowIndexFilter, ShowIndexStmt, ShowInspectionFilter, ShowInspectionKind, ShowInspectionStmt,
    ShowMaskingPoliciesStmt, ShowOpenTablesStmt, ShowPlacementStmt, ShowPlacementTarget,
    ShowProfileStmt, ShowProfileType, ShowStatsBucketsFilter, ShowStatsBucketsStmt,
    ShowStatsHistogramsFilter, ShowStatsHistogramsStmt, ShowStatsLockedFilter, ShowStatsLockedStmt,
    ShowStatsTopNFilter, ShowStatsTopNStmt, ShowStatusFilter, ShowStatusStmt,
    ShowTableNextRowIdStmt, ShowTablePlacementKind, ShowTablePlacementStmt, ShowTableStatusFilter,
    ShowTableStatusStmt, ShowTablesFilter, ShowTablesStmt, ShowVariablesStmt, ShowWarningsFilter,
    ShowWarningsStmt,
};
pub use traffic::{
    RefreshStatsMode, RefreshStatsStmt, StatsObject, TrafficCaptureOption, TrafficReplayOption,
    TrafficStmt,
};
pub use transaction::{BeginStmt, CompletionType};
pub use user::{
    AlterUserDualPassword, AlterUserPasswordExpire, AlterUserResourceKind, AlterUserResourceOption,
    AlterUserStmt, AlterUserTlsOption, CreateUserAuth, CreateUserCommentOrAttribute,
    CreateUserCredential, CreateUserPasswordOption, CreateUserSpec, UserSpec,
};
pub use user_variable::{SetUserVarStmt, UserVariableAssignment};
pub use util::{redact_url, restore_string_literal};
pub use visitor::{Visitable, Visitor};

/// Parser payload for an optional `BINARY` modifier.
///
/// This is the direct Rust representation of Go `ast.OptBinary`. It remains a
/// plain value rather than an AST node because Go uses it only while reducing
/// field-type grammar productions.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OptBinary {
    /// Whether the `BINARY` modifier was present.
    pub is_binary: bool,
    /// The associated charset name, or an empty string when unspecified.
    pub charset: String,
}

/// Element type selected by the vector-type grammar.
///
/// Go intentionally stores the lexer byte directly and validates that it is
/// FLOAT or DOUBLE in the parser. Keeping `u8` preserves its zero value and
/// avoids inventing an AST state that the source type cannot represent.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct VectorElementType {
    /// Source token byte; only FLOAT and DOUBLE are accepted by the parser.
    pub tp: u8,
}

/// A parsed statement. Each family uses [`NodeBox`] so Go's shared AST-node
/// source metadata travels with the already heap-owned payload.
#[derive(Debug, Clone, PartialEq)]
pub enum Stmt {
    /// A query (`SELECT` or a set operation).
    Query(NodeBox<QueryStmt>),
    /// A data-manipulation statement (`INSERT`, `UPDATE`, or `DELETE`).
    Dml(NodeBox<DmlStmt>),
    /// A data-definition statement.
    Ddl(NodeBox<DdlStmt>),
    /// An administrative, inspection, or diagnostics command.
    Admin(NodeBox<AdminStmt>),
    /// A session-scoped command such as `USE`, `SET`, or transaction control.
    Session(NodeBox<SessionStmt>),
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
    /// Returns the shared source metadata carried by this statement.
    pub fn node_text(&self) -> &NodeText {
        match self {
            Self::Query(value) => value.node_text(),
            Self::Dml(value) => value.node_text(),
            Self::Ddl(value) => value.node_text(),
            Self::Admin(value) => value.node_text(),
            Self::Session(value) => value.node_text(),
        }
    }

    /// Returns mutable shared source metadata carried by this statement.
    pub fn node_text_mut(&mut self) -> &mut NodeText {
        match self {
            Self::Query(value) => value.node_text_mut(),
            Self::Dml(value) => value.node_text_mut(),
            Self::Ddl(value) => value.node_text_mut(),
            Self::Admin(value) => value.node_text_mut(),
            Self::Session(value) => value.node_text_mut(),
        }
    }

    /// Replaces the statement's original source text.
    pub fn set_text(
        &mut self,
        encoding: Option<tidb_datatype::Encoding>,
        text: impl Into<Vec<u8>>,
    ) {
        self.node_text_mut().set_text(encoding, text);
    }

    /// Returns the statement text decoded to UTF-8.
    pub fn text(&self) -> &[u8] {
        self.node_text().text()
    }

    /// Returns the statement's exact original source bytes.
    pub fn original_text(&self) -> &[u8] {
        self.node_text().original_text()
    }

    /// Sets the statement's byte offset in the original SQL input.
    pub fn set_origin_text_position(&mut self, offset: usize) {
        self.node_text_mut().set_origin_text_position(offset);
    }

    /// Returns the statement's byte offset in the original SQL input.
    pub fn origin_text_position(&self) -> usize {
        self.node_text().origin_text_position()
    }

    /// Validates source AST states that Go accepts during parsing but rejects
    /// during `Restore`.
    ///
    /// TiDB deliberately keeps parsing and restoration as separate failure
    /// boundaries. Walking the complete tree here preserves that distinction
    /// without making every ordinary restore call thread a `Result` through
    /// nodes whose restoration is infallible.
    pub fn validate_restore(&self) -> Result<(), String> {
        struct RestoreValidator {
            error: Option<String>,
        }

        impl Visitor for RestoreValidator {
            fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
                if self.error.is_some() {
                    return true;
                }
                if let Some(expression) = node.downcast_ref::<Expr>() {
                    if let Err(error) = expression.try_restore() {
                        self.error = Some(error);
                        return true;
                    }
                }
                if let Some(SessionStmt::Prepare {
                    source: PrepareSource::Sql(sql),
                    ..
                }) = node.downcast_ref::<SessionStmt>()
                {
                    if sql.is_empty() {
                        self.error =
                            Some("An error occurred while restore PrepareStmt".to_string());
                        return true;
                    }
                }
                false
            }

            fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
                self.error.is_none()
            }
        }

        let mut statement = self.clone();
        let mut validator = RestoreValidator { error: None };
        let _ = Visitable::accept(&mut statement, &mut validator);
        match validator.error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// Fallible canonical restoration matching Go's distinct `Restore` error
    /// boundary.
    pub fn try_restore(&self) -> Result<String, String> {
        self.try_restore_with_context(&RestoreContext::default())
    }

    /// Fallible canonical restoration with an explicit formatting context.
    pub fn try_restore_with_context(&self, context: &RestoreContext) -> Result<String, String> {
        self.validate_restore()?;
        Ok(self.restore_with_context(context))
    }

    /// Fallible byte-preserving restoration.
    pub fn try_restore_bytes(&self) -> Result<Vec<u8>, String> {
        self.validate_restore()?;
        Ok(self.restore_bytes())
    }

    /// Whether Go exposes this statement through `SensitiveStmtNode`.
    pub fn is_sensitive(&self) -> bool {
        match self {
            Self::Dml(dml) => matches!(dml.as_ref(), DmlStmt::ImportInto(_)),
            Self::Ddl(ddl) => matches!(
                ddl.as_ref(),
                DdlStmt::CreateUser { .. } | DdlStmt::AlterUser(_)
            ),
            Self::Admin(admin) => matches!(
                admin.as_ref(),
                AdminStmt::Grant(_)
                    | AdminStmt::GrantRole(_)
                    | AdminStmt::Brie(_)
                    | AdminStmt::Traffic(_)
            ),
            Self::Session(session) => matches!(
                session.as_ref(),
                SessionStmt::Set(_) | SessionStmt::SetPassword(_)
            ),
            Self::Query(_) => false,
        }
    }

    /// Restores this statement to canonical SQL.
    pub fn restore(&self) -> String {
        self.restore_with_context(&RestoreContext::default())
    }

    /// Restores this statement using an explicit source-formatting context.
    ///
    /// In particular, [`RestoreFlags::TIDB_SPECIAL_COMMENT`] emits TiDB-only
    /// DDL fragments as `/*T![feature] ... */`, exactly as Go's
    /// `format.RestoreCtx` does.  Callers that need only a flag set can use
    /// [`Stmt::restore_with_flags`].
    pub fn restore_with_context(&self, context: &RestoreContext) -> String {
        let mut out = String::new();
        self.restore_into_with_context(&mut out, context);
        out
    }

    /// Restores this statement using `flags`.
    pub fn restore_with_flags(&self, flags: RestoreFlags) -> String {
        self.restore_with_context(&RestoreContext::new(flags))
    }

    /// Restores this statement into a byte-preserving buffer. This is the
    /// lossless counterpart to [`Stmt::restore`] for Go AST payloads such as
    /// GBK ENUM/SET members that are not valid UTF-8.
    pub fn restore_bytes(&self) -> Vec<u8> {
        self.restore_bytes_with_context(&RestoreContext::default())
    }

    /// Byte-preserving restore with an explicit formatting context.
    pub fn restore_bytes_with_context(&self, context: &RestoreContext) -> Vec<u8> {
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
    pub(crate) fn restore_into_with_context(&self, out: &mut String, context: &RestoreContext) {
        match self {
            Stmt::Query(query) => query.restore_into_with_context(out, context),
            Stmt::Dml(dml) => dml.restore_into_with_context(out, context),
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

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for Stmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Query(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Dml(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Ddl(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Admin(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Session(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for TransactionMode {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Default => {}
            Self::Optimistic => {}
            Self::Pessimistic => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for PrepareSource {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Sql(field_0) => {
                let _ = field_0;
            }
            Self::Var(field_0) => {
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}
// END GENERATED AST VISITOR IMPLEMENTATIONS
