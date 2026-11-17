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

//! Go master's materialized view statements (`ast/ddl.go`'s
//! `CreateMaterializedViewStmt` .. `DropMaterializedViewLogStmt`, and
//! `ast/misc.go`'s purge, cancel, refresh and compare statements).

use crate::util::{back_quote, escape_string_literal, push_name_path};
use crate::{Expr, NodeBox, QueryStmt, TableOption};

/// Writes Go's `ctx.WriteString`: a single-quoted, escaped string.
fn push_string(out: &mut String, value: &str) {
    out.push('\'');
    out.push_str(&escape_string_literal(value));
    out.push('\'');
}

/// Writes `(a, b)`, each name back-quoted.
fn push_column_list(out: &mut String, cols: &[String]) {
    out.push('(');
    for (i, col) in cols.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        out.push_str(&back_quote(col));
    }
    out.push(')');
}

/// Writes ` START WITH start NEXT next`, each part only when present.
fn push_start_with_next(out: &mut String, start_with: Option<&Expr>, next: Option<&Expr>) {
    if let Some(start_with) = start_with {
        out.push_str(" START WITH ");
        start_with.restore_into(out);
    }
    if let Some(next) = next {
        out.push_str(" NEXT ");
        next.restore_into(out);
    }
}

/// Go `MViewRefreshMethod`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MViewRefreshMethod {
    /// Go `MViewRefreshMethodFast`.
    Fast,
}

impl MViewRefreshMethod {
    /// Go `MViewRefreshMethod.String`.
    #[must_use]
    pub const fn sql(self) -> &'static str {
        match self {
            Self::Fast => "REFRESH FAST",
        }
    }
}

/// Go `MViewRefreshClause`: the refresh clause of CREATE / ALTER
/// MATERIALIZED VIEW.
#[derive(Debug, Clone, PartialEq)]
pub struct MViewRefreshClause {
    /// Go `Method`.
    pub method: MViewRefreshMethod,
    /// Go `StartWith`.
    pub start_with: Option<Expr>,
    /// Go `Next`.
    pub next: Option<Expr>,
}

impl MViewRefreshClause {
    /// Go `MViewRefreshClause.Restore`.
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str(self.method.sql());
        push_start_with_next(out, self.start_with.as_ref(), self.next.as_ref());
    }
}

/// Go `CreateMaterializedViewStmt`.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateMaterializedViewStmt {
    /// Go `ViewName`.
    pub view_name: Vec<String>,
    /// Go `Cols`.
    pub cols: Vec<String>,
    /// Go `Comment`.
    pub comment: String,
    /// Go `Refresh`.
    pub refresh: Option<MViewRefreshClause>,
    /// Go `Attributes`.
    pub attributes: String,
    /// Go `Options`: `SHARD_ROW_ID_BITS` and `PRE_SPLIT_REGIONS`.
    pub options: Vec<TableOption>,
    /// Go `Select`.
    pub select: NodeBox<QueryStmt>,
}

impl CreateMaterializedViewStmt {
    /// Go `CreateMaterializedViewStmt.Restore`.
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("CREATE MATERIALIZED VIEW ");
        push_name_path(out, &self.view_name);
        out.push(' ');
        push_column_list(out, &self.cols);
        if !self.comment.is_empty() {
            out.push_str(" COMMENT = ");
            push_string(out, &self.comment);
        }
        for option in &self.options {
            out.push(' ');
            option.restore_into(out);
        }
        if let Some(refresh) = &self.refresh {
            out.push(' ');
            refresh.restore_into(out);
        }
        if !self.attributes.is_empty() {
            out.push_str(" ATTRIBUTES = ");
            push_string(out, &self.attributes);
        }
        out.push_str(" AS ");
        self.select.restore_into(out);
    }
}

/// Go `MLogPurgeClause`: the purge clause of CREATE / ALTER MATERIALIZED
/// VIEW LOG.
#[derive(Debug, Clone, PartialEq)]
pub struct MLogPurgeClause {
    /// Go `Immediate`.
    pub immediate: bool,
    /// Go `StartWith`.
    pub start_with: Option<Expr>,
    /// Go `Next`.
    pub next: Option<Expr>,
}

impl MLogPurgeClause {
    /// Go `MLogPurgeClause.Restore`.
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("PURGE");
        if self.immediate {
            out.push_str(" IMMEDIATE");
            return;
        }
        push_start_with_next(out, self.start_with.as_ref(), self.next.as_ref());
    }
}

/// Go `MLogAccumulationAlertClause`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MLogAccumulationAlertClause {
    /// Go `Rows`.
    pub rows: i64,
}

/// Go `CreateMaterializedViewLogStmt`.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateMaterializedViewLogStmt {
    /// Go `Table`.
    pub table: Vec<String>,
    /// Go `Cols`.
    pub cols: Vec<String>,
    /// Go `Options`: `SHARD_ROW_ID_BITS` and `PRE_SPLIT_REGIONS`.
    pub options: Vec<TableOption>,
    /// Go `Purge`.
    pub purge: Option<MLogPurgeClause>,
    /// Go `AccumulationAlert`.
    pub accumulation_alert: Option<MLogAccumulationAlertClause>,
}

impl CreateMaterializedViewLogStmt {
    /// Go `CreateMaterializedViewLogStmt.Restore`.
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("CREATE MATERIALIZED VIEW LOG ON ");
        push_name_path(out, &self.table);
        out.push(' ');
        push_column_list(out, &self.cols);
        for option in &self.options {
            out.push(' ');
            option.restore_into(out);
        }
        if let Some(purge) = &self.purge {
            out.push(' ');
            purge.restore_into(out);
        }
        if let Some(alert) = &self.accumulation_alert {
            out.push_str(" ALERT ROWS ");
            out.push_str(&alert.rows.to_string());
        }
    }
}

/// Go `AlterMaterializedViewAction`, one variant per
/// `AlterMaterializedViewActionType`.
#[derive(Debug, Clone, PartialEq)]
pub enum AlterMaterializedViewAction {
    /// Go `AlterMaterializedViewActionComment`.
    Comment(String),
    /// Go `AlterMaterializedViewActionRefresh`: the grammar always sets a
    /// fast refresh clause.
    Refresh(MViewRefreshClause),
    /// Go `AlterMaterializedViewActionAttributes`.
    Attributes(String),
}

impl AlterMaterializedViewAction {
    /// Go `AlterMaterializedViewAction.Restore`.
    pub(crate) fn restore_into(&self, out: &mut String) {
        match self {
            Self::Comment(comment) => {
                out.push_str("COMMENT = ");
                push_string(out, comment);
            }
            Self::Refresh(refresh) => {
                out.push_str("REFRESH");
                push_start_with_next(out, refresh.start_with.as_ref(), refresh.next.as_ref());
            }
            Self::Attributes(attributes) => {
                out.push_str("ATTRIBUTES = ");
                push_string(out, attributes);
            }
        }
    }
}

/// Go `AlterMaterializedViewStmt`.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterMaterializedViewStmt {
    /// Go `ViewName`.
    pub view_name: Vec<String>,
    /// Go `Actions`.
    pub actions: Vec<AlterMaterializedViewAction>,
}

impl AlterMaterializedViewStmt {
    /// Go `AlterMaterializedViewStmt.Restore`.
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("ALTER MATERIALIZED VIEW ");
        push_name_path(out, &self.view_name);
        out.push(' ');
        for (i, action) in self.actions.iter().enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            action.restore_into(out);
        }
    }
}

/// Go `AlterMaterializedViewLogAction`, one variant per
/// `AlterMaterializedViewLogActionType`.
#[derive(Debug, Clone, PartialEq)]
pub enum AlterMaterializedViewLogAction {
    /// Go `AlterMaterializedViewLogActionPurge`.
    Purge(MLogPurgeClause),
    /// Go `AlterMaterializedViewLogActionAddColumn`.
    AddColumn(Vec<String>),
}

impl AlterMaterializedViewLogAction {
    /// Go `AlterMaterializedViewLogAction.Restore`.
    pub(crate) fn restore_into(&self, out: &mut String) {
        match self {
            Self::Purge(purge) => purge.restore_into(out),
            Self::AddColumn(cols) => {
                out.push_str("ADD COLUMN ");
                push_column_list(out, cols);
            }
        }
    }
}

/// Go `AlterMaterializedViewLogStmt`.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterMaterializedViewLogStmt {
    /// Go `Table`.
    pub table: Vec<String>,
    /// Go `Actions`.
    pub actions: Vec<AlterMaterializedViewLogAction>,
}

impl AlterMaterializedViewLogStmt {
    /// Go `AlterMaterializedViewLogStmt.Restore`.
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("ALTER MATERIALIZED VIEW LOG ON ");
        push_name_path(out, &self.table);
        out.push(' ');
        for (i, action) in self.actions.iter().enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            action.restore_into(out);
        }
    }
}

/// Go `DropMaterializedViewStmt`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropMaterializedViewStmt {
    /// Go `IfExists`.
    pub if_exists: bool,
    /// Go `ViewName`.
    pub view_name: Vec<String>,
}

impl DropMaterializedViewStmt {
    /// Go `DropMaterializedViewStmt.Restore`.
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("DROP MATERIALIZED VIEW ");
        if self.if_exists {
            out.push_str("IF EXISTS ");
        }
        push_name_path(out, &self.view_name);
    }
}

/// Go `DropMaterializedViewLogStmt`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropMaterializedViewLogStmt {
    /// Go `IfExists`.
    pub if_exists: bool,
    /// Go `Table`.
    pub table: Vec<String>,
}

impl DropMaterializedViewLogStmt {
    /// Go `DropMaterializedViewLogStmt.Restore`.
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("DROP MATERIALIZED VIEW LOG ");
        if self.if_exists {
            out.push_str("IF EXISTS ");
        }
        out.push_str("ON ");
        push_name_path(out, &self.table);
    }
}

/// Go `PurgeMaterializedViewLogStmt`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PurgeMaterializedViewLogStmt {
    /// Go `Table`.
    pub table: Vec<String>,
}

impl PurgeMaterializedViewLogStmt {
    /// Go `PurgeMaterializedViewLogStmt.Restore`.
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("PURGE MATERIALIZED VIEW LOG ON ");
        push_name_path(out, &self.table);
    }
}

/// Go `CancelMaterializedViewJobType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelMaterializedViewJobType {
    /// Go `CancelMaterializedViewJobTypeLogPurge`.
    LogPurge,
    /// Go `CancelMaterializedViewJobTypeRefresh`.
    Refresh,
}

/// Go `CancelMaterializedViewRefreshJobCommand`.
pub const CANCEL_MATERIALIZED_VIEW_REFRESH_JOB_COMMAND: &str = "CANCEL MATERIALIZED VIEW REFRESH JOB";

/// Go `CancelMaterializedViewJobStmt`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CancelMaterializedViewJobStmt {
    /// Go `Tp`.
    pub tp: CancelMaterializedViewJobType,
    /// Go `JobID`.
    pub job_id: i64,
}

impl CancelMaterializedViewJobStmt {
    /// Go `CancelMaterializedViewJobStmt.Restore`.
    pub(crate) fn restore_into(&self, out: &mut String) {
        match self.tp {
            CancelMaterializedViewJobType::Refresh => out.push_str(CANCEL_MATERIALIZED_VIEW_REFRESH_JOB_COMMAND),
            CancelMaterializedViewJobType::LogPurge => out.push_str("CANCEL MATERIALIZED VIEW LOG PURGE JOB"),
        }
        out.push(' ');
        out.push_str(&self.job_id.to_string());
    }
}

/// Go `RefreshMaterializedViewMode`: Go's `Type` and `CompleteType` pair,
/// as `RefreshMaterializedViewStmt.Mode` resolves it. A COMPLETE refresh
/// without its mode, which Go's `Restore` and `Mode` refuse, cannot be built.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefreshMaterializedViewMode {
    /// Go `RefreshMaterializedViewModeFast`.
    Fast,
    /// Go `RefreshMaterializedViewModeCompleteInPlace`.
    CompleteInPlace,
    /// Go `RefreshMaterializedViewModeCompleteOutOfPlace`.
    CompleteOutOfPlace,
    /// Go `RefreshMaterializedViewModeCompleteDeltaApply`.
    CompleteDeltaApply,
}

impl RefreshMaterializedViewMode {
    /// Go `RefreshMaterializedViewMode.String`, which is also what Go's
    /// `Restore` writes for the type and its complete mode.
    #[must_use]
    pub const fn sql(self) -> &'static str {
        match self {
            Self::Fast => "FAST",
            Self::CompleteInPlace => "COMPLETE IN PLACE",
            Self::CompleteOutOfPlace => "COMPLETE OUT OF PLACE",
            Self::CompleteDeltaApply => "COMPLETE DELTA APPLY",
        }
    }
}

/// Go `RefreshMaterializedViewObserveType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefreshMaterializedViewObserveType {
    /// Go `RefreshMaterializedViewObserveNone`.
    None,
    /// Go `RefreshMaterializedViewObserveDryRun`.
    DryRun,
    /// Go `RefreshMaterializedViewObserveProfile`.
    Profile,
}

/// Go `RefreshMaterializedViewStmt`.
#[derive(Debug, Clone, PartialEq)]
pub struct RefreshMaterializedViewStmt {
    /// Go `ViewName`.
    pub view_name: Vec<String>,
    /// Go `WithAsyncMode`.
    pub with_async_mode: bool,
    /// Go `Type` and `CompleteType`, as `Mode` resolves them.
    pub mode: RefreshMaterializedViewMode,
    /// Go `ObserveType`.
    pub observe_type: RefreshMaterializedViewObserveType,
    /// Go `AsOf`: the `AS OF TIMESTAMP` expression of a FAST refresh.
    pub as_of: Option<Expr>,
}

impl RefreshMaterializedViewStmt {
    /// Go `RefreshMaterializedViewStmt.Restore`.
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("REFRESH MATERIALIZED VIEW ");
        push_name_path(out, &self.view_name);
        if self.with_async_mode {
            out.push_str(" WITH ASYNC MODE");
        }
        out.push(' ');
        out.push_str(self.mode.sql());
        if let Some(as_of) = &self.as_of {
            out.push_str(" AS OF TIMESTAMP ");
            as_of.restore_into(out);
        }
        match self.observe_type {
            RefreshMaterializedViewObserveType::None => {}
            RefreshMaterializedViewObserveType::DryRun => out.push_str(" DRY RUN"),
            RefreshMaterializedViewObserveType::Profile => out.push_str(" WITH PROFILE"),
        }
    }
}

/// Go `RefreshMaterializedViewImplementStmt`: the internal statement a
/// refresh runs, never parsed.
#[derive(Debug, Clone, PartialEq)]
pub struct RefreshMaterializedViewImplementStmt {
    /// Go `RefreshStmt`.
    pub refresh_stmt: RefreshMaterializedViewStmt,
    /// Go `LastSuccessfulRefreshReadTSO`.
    pub last_successful_refresh_read_tso: u64,
    /// Go `TargetRefreshReadTSO`.
    pub target_refresh_read_tso: u64,
    /// Go `MLogRetainedLowerTSO`.
    pub mlog_retained_lower_tso: u64,
    /// Go `RegisteredMLogIDs`.
    pub registered_mlog_ids: Vec<i64>,
}

impl RefreshMaterializedViewImplementStmt {
    /// Go `RefreshMaterializedViewImplementStmt.Restore`.
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("IMPLEMENT FOR ");
        self.refresh_stmt.restore_into(out);
        out.push_str(" USING TIMESTAMP ");
        out.push_str(&self.last_successful_refresh_read_tso.to_string());
        if self.target_refresh_read_tso > 0 {
            out.push_str(" UP TO TIMESTAMP ");
            out.push_str(&self.target_refresh_read_tso.to_string());
        }
        if self.mlog_retained_lower_tso > 0 {
            out.push_str(" MLOG RETAINED LOWER TIMESTAMP ");
            out.push_str(&self.mlog_retained_lower_tso.to_string());
        }
    }
}

/// Go `CompareMaterializedViewStmt`.
#[derive(Debug, Clone, PartialEq)]
pub struct CompareMaterializedViewStmt {
    /// Go `ViewName`.
    pub view_name: Vec<String>,
    /// Go `AsOf`: the `AS OF TIMESTAMP` expression.
    pub as_of: Expr,
    /// Go `OutputTable`.
    pub output_table: Option<Vec<String>>,
}

impl CompareMaterializedViewStmt {
    /// Go `CompareMaterializedViewStmt.Restore`.
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("COMPARE MATERIALIZED VIEW ");
        push_name_path(out, &self.view_name);
        out.push_str(" AS OF TIMESTAMP ");
        self.as_of.restore_into(out);
        if let Some(output) = &self.output_table {
            out.push_str(" OUTPUT INTO TABLE ");
            push_name_path(out, output);
        }
    }
}

// Go's `Accept`: the expressions, table options and query are the children.

fn accept_exprs<V: crate::Visitor>(visitor: &mut V, start_with: &mut Option<Expr>, next: &mut Option<Expr>) -> bool {
    for expr in [start_with, next].into_iter().flatten() {
        if !crate::Visitable::accept(expr, visitor) {
            return false;
        }
    }
    true
}

impl crate::Visitable for CreateMaterializedViewStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        for option in &mut self.options {
            if !crate::Visitable::accept(option, visitor) {
                return false;
            }
        }
        if let Some(refresh) = &mut self.refresh {
            if !accept_exprs(visitor, &mut refresh.start_with, &mut refresh.next) {
                return false;
            }
        }
        if !crate::Visitable::accept(self.select.as_mut(), visitor) {
            return false;
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for CreateMaterializedViewLogStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        for option in &mut self.options {
            if !crate::Visitable::accept(option, visitor) {
                return false;
            }
        }
        if let Some(purge) = &mut self.purge {
            if !accept_exprs(visitor, &mut purge.start_with, &mut purge.next) {
                return false;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for AlterMaterializedViewAction {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        if let Self::Refresh(refresh) = self {
            if !accept_exprs(visitor, &mut refresh.start_with, &mut refresh.next) {
                return false;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for AlterMaterializedViewStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        for action in &mut self.actions {
            if !crate::Visitable::accept(action, visitor) {
                return false;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for AlterMaterializedViewLogAction {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        if let Self::Purge(purge) = self {
            if !accept_exprs(visitor, &mut purge.start_with, &mut purge.next) {
                return false;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for AlterMaterializedViewLogStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        for action in &mut self.actions {
            if !crate::Visitable::accept(action, visitor) {
                return false;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for RefreshMaterializedViewStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        if let Some(as_of) = &mut self.as_of {
            if !crate::Visitable::accept(as_of, visitor) {
                return false;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for RefreshMaterializedViewImplementStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        if !crate::Visitable::accept(&mut self.refresh_stmt, visitor) {
            return false;
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for CompareMaterializedViewStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        if !crate::Visitable::accept(&mut self.as_of, visitor) {
            return false;
        }
        visitor.leave(self)
    }
}

/// Go's leaf statements: `Accept` visits the node alone.
macro_rules! leaf_visitable {
    ($($ty:ty),*) => {$(
        impl crate::Visitable for $ty {
            fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
                if visitor.enter(self) {
                    return visitor.leave(self);
                }
                visitor.leave(self)
            }
        }
    )*};
}

leaf_visitable!(
    DropMaterializedViewStmt,
    DropMaterializedViewLogStmt,
    PurgeMaterializedViewLogStmt,
    CancelMaterializedViewJobStmt
);
