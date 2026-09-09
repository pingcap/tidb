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

//! Materialized-view DDL syntax and canonical restore.
//!
//! Go's materialized-view statements are parser-owned today: the DDL executor
//! has no dependency-closed refresh/log worker. Keeping their complete typed
//! payload here lets parser, digest, privilege inspection, and restore retain
//! the SQL contract without pretending that execution is available.

use crate::util::{back_quote, push_name_path};
use crate::{Expr, QueryStmt, RestoreContext, TableOption};

/// The refresh method currently accepted by TiDB's grammar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MViewRefreshMethod {
    /// `FAST`.
    Fast,
}

impl MViewRefreshMethod {
    fn restore_into(self, out: &mut String) {
        out.push_str("REFRESH FAST");
    }
}

/// The schedule attached to `REFRESH`.
#[derive(Debug, Clone, PartialEq)]
pub struct MViewRefreshClause {
    /// Refresh implementation.
    pub method: MViewRefreshMethod,
    /// Optional `START WITH` expression.
    pub start_with: Option<Box<Expr>>,
    /// Optional `NEXT` expression.
    pub next: Option<Box<Expr>>,
}

impl MViewRefreshClause {
    fn restore_into(&self, out: &mut String) {
        self.method.restore_into(out);
        if let Some(expression) = &self.start_with {
            out.push_str(" START WITH ");
            expression.restore_into(out);
        }
        if let Some(expression) = &self.next {
            out.push_str(" NEXT ");
            expression.restore_into(out);
        }
    }
}

/// `CREATE MATERIALIZED VIEW` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateMaterializedViewStmt {
    /// View name, including an optional schema qualifier.
    pub view_name: Vec<String>,
    /// Stored output columns in source order.
    pub columns: Vec<String>,
    /// Optional `COMMENT` text.
    pub comment: Option<String>,
    /// Optional refresh schedule.
    pub refresh: Option<MViewRefreshClause>,
    /// Optional `ATTRIBUTES` text.
    pub attributes: Option<String>,
    /// `SHARD_ROW_ID_BITS` / `PRE_SPLIT_REGIONS` options.
    pub options: Vec<TableOption>,
    /// The query that populates the materialized view.
    pub query: Box<QueryStmt>,
    /// Whether the query was enclosed in `AS (...)`.
    pub query_parenthesized: bool,
}

/// `PURGE` schedule attached to a materialized-view log.
#[derive(Debug, Clone, PartialEq)]
pub struct MLogPurgeClause {
    /// Whether the source used `PURGE IMMEDIATE`.
    pub immediate: bool,
    /// Optional `START WITH` expression.
    pub start_with: Option<Box<Expr>>,
    /// Optional `NEXT` expression.
    pub next: Option<Box<Expr>>,
}

impl MLogPurgeClause {
    fn restore_into(&self, out: &mut String) {
        out.push_str("PURGE");
        if self.immediate {
            out.push_str(" IMMEDIATE");
            return;
        }
        if let Some(expression) = &self.start_with {
            out.push_str(" START WITH ");
            expression.restore_into(out);
        }
        if let Some(expression) = &self.next {
            out.push_str(" NEXT ");
            expression.restore_into(out);
        }
    }
}

/// `ALERT ROWS n` attached to a materialized-view log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MLogAccumulationAlertClause {
    /// Alert threshold.
    pub rows: i64,
}

/// `CREATE MATERIALIZED VIEW LOG` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateMaterializedViewLogStmt {
    /// Base table name.
    pub table: Vec<String>,
    /// Logged columns in source order.
    pub columns: Vec<String>,
    /// `SHARD_ROW_ID_BITS` / `PRE_SPLIT_REGIONS` options.
    pub options: Vec<TableOption>,
    /// Optional purge policy.
    pub purge: Option<MLogPurgeClause>,
    /// Optional accumulation alert.
    pub accumulation_alert: Option<MLogAccumulationAlertClause>,
}

/// One `ALTER MATERIALIZED VIEW` action.
#[derive(Debug, Clone, PartialEq)]
pub enum AlterMaterializedViewAction {
    /// `COMMENT = '...'`.
    Comment(String),
    /// `REFRESH [START WITH expr] [NEXT expr]`.
    Refresh {
        /// Optional schedule fields; method is always FAST in this grammar.
        schedule: Option<MViewRefreshClause>,
    },
    /// `ATTRIBUTES = '...'`.
    Attributes(String),
}

/// `ALTER MATERIALIZED VIEW` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterMaterializedViewStmt {
    /// View name.
    pub view_name: Vec<String>,
    /// Actions in source order.
    pub actions: Vec<AlterMaterializedViewAction>,
}

/// One `ALTER MATERIALIZED VIEW LOG` action.
#[derive(Debug, Clone, PartialEq)]
pub enum AlterMaterializedViewLogAction {
    /// `PURGE [IMMEDIATE | ...]` (the bare form is also accepted).
    Purge(MLogPurgeClause),
    /// `ADD [COLUMN] (columns...)`.
    AddColumn(Vec<String>),
}

/// `ALTER MATERIALIZED VIEW LOG ON` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterMaterializedViewLogStmt {
    /// Base table name.
    pub table: Vec<String>,
    /// Actions in source order.
    pub actions: Vec<AlterMaterializedViewLogAction>,
}

/// `DROP MATERIALIZED VIEW` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropMaterializedViewStmt {
    /// Whether missing views are ignored.
    pub if_exists: bool,
    /// View name.
    pub view_name: Vec<String>,
}

/// `DROP MATERIALIZED VIEW LOG` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropMaterializedViewLogStmt {
    /// Whether missing logs are ignored.
    pub if_exists: bool,
    /// Base table name.
    pub table: Vec<String>,
}

fn restore_columns(out: &mut String, columns: &[String]) {
    out.push_str(" (");
    for (index, column) in columns.iter().enumerate() {
        if index > 0 {
            out.push_str(", ");
        }
        out.push_str(&back_quote(column));
    }
    out.push(')');
}

fn restore_options(out: &mut String, options: &[TableOption], context: &RestoreContext) {
    for option in options {
        out.push(' ');
        option.restore_into_with_context(out, context);
    }
}

impl CreateMaterializedViewStmt {
    pub(crate) fn restore_into(&self, out: &mut String, context: &RestoreContext) {
        out.push_str("CREATE MATERIALIZED VIEW ");
        push_name_path(out, &self.view_name);
        restore_columns(out, &self.columns);
        if let Some(comment) = &self.comment {
            out.push_str(" COMMENT = '");
            out.push_str(&crate::util::escape_string_literal(comment));
            out.push('\'');
        }
        restore_options(out, &self.options, context);
        if let Some(refresh) = &self.refresh {
            out.push(' ');
            refresh.restore_into(out);
        }
        if let Some(attributes) = &self.attributes {
            out.push_str(" ATTRIBUTES = '");
            out.push_str(&crate::util::escape_string_literal(attributes));
            out.push('\'');
        }
        out.push_str(" AS ");
        if self.query_parenthesized {
            out.push('(');
        }
        self.query.restore_into_with_context(out, context);
        if self.query_parenthesized {
            out.push(')');
        }
    }
}

impl CreateMaterializedViewLogStmt {
    pub(crate) fn restore_into(&self, out: &mut String, context: &RestoreContext) {
        out.push_str("CREATE MATERIALIZED VIEW LOG ON ");
        push_name_path(out, &self.table);
        restore_columns(out, &self.columns);
        restore_options(out, &self.options, context);
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

impl AlterMaterializedViewAction {
    fn restore_into(&self, out: &mut String) {
        match self {
            Self::Comment(comment) => {
                out.push_str("COMMENT = '");
                out.push_str(&crate::util::escape_string_literal(comment));
                out.push('\'');
            }
            Self::Refresh { schedule } => {
                out.push_str("REFRESH");
                if let Some(schedule) = schedule {
                    if let Some(expression) = &schedule.start_with {
                        out.push_str(" START WITH ");
                        expression.restore_into(out);
                    }
                    if let Some(expression) = &schedule.next {
                        out.push_str(" NEXT ");
                        expression.restore_into(out);
                    }
                }
            }
            Self::Attributes(attributes) => {
                out.push_str("ATTRIBUTES = '");
                out.push_str(&crate::util::escape_string_literal(attributes));
                out.push('\'');
            }
        }
    }
}

impl AlterMaterializedViewStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("ALTER MATERIALIZED VIEW ");
        push_name_path(out, &self.view_name);
        out.push(' ');
        for (index, action) in self.actions.iter().enumerate() {
            if index > 0 {
                out.push_str(", ");
            }
            action.restore_into(out);
        }
    }
}

impl AlterMaterializedViewLogAction {
    fn restore_into(&self, out: &mut String) {
        match self {
            Self::Purge(purge) => purge.restore_into(out),
            Self::AddColumn(columns) => {
                out.push_str("ADD COLUMN");
                restore_columns(out, columns);
            }
        }
    }
}

impl AlterMaterializedViewLogStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("ALTER MATERIALIZED VIEW LOG ON ");
        push_name_path(out, &self.table);
        out.push(' ');
        for (index, action) in self.actions.iter().enumerate() {
            if index > 0 {
                out.push_str(", ");
            }
            action.restore_into(out);
        }
    }
}

impl DropMaterializedViewStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("DROP MATERIALIZED VIEW ");
        if self.if_exists {
            out.push_str("IF EXISTS ");
        }
        push_name_path(out, &self.view_name);
    }
}

impl DropMaterializedViewLogStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("DROP MATERIALIZED VIEW LOG ");
        if self.if_exists {
            out.push_str("IF EXISTS ");
        }
        out.push_str("ON ");
        push_name_path(out, &self.table);
    }
}

macro_rules! visit_expr_options {
    ($self:expr, $visitor:expr) => {
        if let Some(value) = $self.start_with.as_mut() {
            if !crate::Visitable::accept(value.as_mut(), $visitor) {
                return false;
            }
        }
        if let Some(value) = $self.next.as_mut() {
            if !crate::Visitable::accept(value.as_mut(), $visitor) {
                return false;
            }
        }
    };
}

impl crate::Visitable for MViewRefreshClause {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        visit_expr_options!(self, visitor);
        visitor.leave(self)
    }
}

impl crate::Visitable for MLogPurgeClause {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        visit_expr_options!(self, visitor);
        visitor.leave(self)
    }
}

impl crate::Visitable for MViewRefreshMethod {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for MLogAccumulationAlertClause {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for CreateMaterializedViewStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        if let Some(value) = self.refresh.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        for value in &mut self.options {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        if !crate::Visitable::accept(self.query.as_mut(), visitor) {
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
        for value in &mut self.options {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        if let Some(value) = self.purge.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        if let Some(value) = self.accumulation_alert.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
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
        if let Self::Refresh {
            schedule: Some(value),
        } = self
        {
            if !crate::Visitable::accept(value, visitor) {
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
        for value in &mut self.actions {
            if !crate::Visitable::accept(value, visitor) {
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
        if let Self::Purge(value) = self {
            if !crate::Visitable::accept(value, visitor) {
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
        for value in &mut self.actions {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for DropMaterializedViewStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for DropMaterializedViewLogStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        visitor.leave(self)
    }
}
