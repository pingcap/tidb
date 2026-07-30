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

//! The `ADMIN SHOW ...` / `SHOW IMPORT ...` inspection payloads and the reload
//! and binding-cache scopes, mirroring Go's `AdminStmt` show variants in
//! `pkg/parser/ast/misc.go`.

use super::*;

/// Go's import-job inspection payload on `ShowStmt`.
#[derive(Debug, Clone, PartialEq)]
pub struct ShowImportJobsStmt {
    /// Whether `RAW` was written before `IMPORT`.
    pub raw: bool,
    /// Singular job ID; absent for the plural listing form.
    pub job_id: Option<i64>,
    /// Optional plural-list filter.
    pub where_clause: Option<Expr>,
}

impl ShowImportJobsStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("SHOW ");
        if self.raw {
            out.push_str("RAW ");
        }
        if let Some(job_id) = self.job_id {
            out.push_str("IMPORT JOB ");
            out.push_str(&job_id.to_string());
        } else {
            out.push_str("IMPORT JOBS");
            if let Some(where_clause) = &self.where_clause {
                out.push_str(" WHERE ");
                where_clause.restore_into(out);
            }
        }
    }
}

/// Go's import-group inspection payload on `ShowStmt`.
#[derive(Debug, Clone, PartialEq)]
pub struct ShowImportGroupsStmt {
    /// Singular group key; absent for the plural listing form.
    pub group_key: Option<String>,
    /// Optional plural/singular filter.
    pub where_clause: Option<Expr>,
}

impl ShowImportGroupsStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("SHOW IMPORT ");
        if let Some(group_key) = &self.group_key {
            out.push_str("GROUP '");
            out.push_str(&escape_string_literal(group_key));
            out.push('\'');
        } else {
            out.push_str("GROUPS");
        }
        if let Some(where_clause) = &self.where_clause {
            out.push_str(" WHERE ");
            where_clause.restore_into(out);
        }
    }
}

/// Go's `ADMIN SHOW table NEXT_ROW_ID` payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminShowNextRowIdStmt {
    /// The table whose next row ID is requested.
    pub table: Vec<String>,
}

impl AdminShowNextRowIdStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("ADMIN SHOW ");
        push_name_path(out, &self.table);
        out.push_str(" NEXT_ROW_ID");
    }
}

/// Go's `AdminShowDDLJobs` payload.
#[derive(Debug, Clone, PartialEq)]
pub struct AdminShowDdlJobsStmt {
    /// Optional positive-looking integer token represented by Go's zero-value
    /// field. A source `0` is restored as omitted, matching Go's AST restore.
    pub job_number: i64,
    /// Optional DDL-job metadata predicate.
    pub where_clause: Option<Expr>,
}

impl AdminShowDdlJobsStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("ADMIN SHOW DDL JOBS");
        if self.job_number != 0 {
            out.push(' ');
            out.push_str(&self.job_number.to_string());
        }
        if let Some(where_clause) = &self.where_clause {
            out.push_str(" WHERE ");
            where_clause.restore_into(out);
        }
    }
}

/// The distinct Go payload alternatives for `ADMIN SHOW DDL JOB QUERIES`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdminShowDdlJobQueriesStmt {
    /// `ADMIN SHOW DDL JOB QUERIES id [, id ...]`.
    JobIds(Vec<i64>),
    /// `ADMIN SHOW DDL JOB QUERIES LIMIT {count | offset, count | count OFFSET offset}`.
    ///
    /// Go restores every spelling as `LIMIT offset, count`.
    Limit {
        /// Row offset, defaulting to zero for the one-number form.
        offset: u64,
        /// Number of job-query rows to return.
        count: u64,
    },
}

impl AdminShowDdlJobQueriesStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("ADMIN SHOW DDL JOB QUERIES ");
        match self {
            Self::JobIds(job_ids) => {
                for (index, job_id) in job_ids.iter().enumerate() {
                    if index > 0 {
                        out.push_str(", ");
                    }
                    out.push_str(&job_id.to_string());
                }
            }
            Self::Limit { offset, count } => {
                out.push_str("LIMIT ");
                out.push_str(&offset.to_string());
                out.push_str(", ");
                out.push_str(&count.to_string());
            }
        }
    }
}

/// Go's `ADMIN SHOW SLOW` payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminShowSlowStmt {
    /// The selected recent/top result set.
    pub mode: AdminShowSlowMode,
    /// Maximum number of slow statements to list.
    pub count: u64,
}

/// The mutually exclusive `ADMIN SHOW SLOW` modes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdminShowSlowMode {
    /// `RECENT count`.
    Recent,
    /// `TOP [INTERNAL | ALL] count`.
    Top(AdminShowSlowTopScope),
}

/// The optional scope after `ADMIN SHOW SLOW TOP`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminShowSlowTopScope {
    /// Omitted scope.
    Default,
    /// `INTERNAL` statements only.
    Internal,
    /// Both internal and user statements.
    All,
}

impl AdminShowSlowStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("ADMIN SHOW SLOW ");
        match self.mode {
            AdminShowSlowMode::Recent => out.push_str("RECENT "),
            AdminShowSlowMode::Top(scope) => {
                out.push_str("TOP ");
                match scope {
                    AdminShowSlowTopScope::Default => {}
                    AdminShowSlowTopScope::Internal => out.push_str("INTERNAL "),
                    AdminShowSlowTopScope::All => out.push_str("ALL "),
                }
            }
        }
        out.push_str(&self.count.to_string());
    }
}

/// Go's value-less `AdminReload*` variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminReloadKind {
    /// `STATISTICS` or `STATS_EXTENDED`, restoring as `STATS_EXTENDED`.
    Statistics,
    /// `OPT_RULE_BLACKLIST`.
    OptRuleBlacklist,
    /// `EXPR_PUSHDOWN_BLACKLIST`.
    ExprPushdownBlacklist,
    /// `BINDINGS`.
    Bindings,
    /// `CLUSTER [BINDINGS]`, restoring as `CLUSTER BINDINGS`.
    ClusterBindings,
}

/// Go's value-less ADMIN binding maintenance operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminBindingControlKind {
    /// Flush in-memory binding state.
    Flush,
    /// Capture statements into bindings.
    Capture,
    /// Evolve existing bindings.
    Evolve,
}

impl AdminBindingControlKind {
    pub(crate) fn restore_name(self) -> &'static str {
        match self {
            Self::Flush => "FLUSH",
            Self::Capture => "CAPTURE",
            Self::Evolve => "EVOLVE",
        }
    }
}

impl AdminReloadKind {
    pub(crate) fn restore_name(self) -> &'static str {
        match self {
            Self::Statistics => "STATS_EXTENDED",
            Self::OptRuleBlacklist => "OPT_RULE_BLACKLIST",
            Self::ExprPushdownBlacklist => "EXPR_PUSHDOWN_BLACKLIST",
            Self::Bindings => "BINDINGS",
            Self::ClusterBindings => "CLUSTER BINDINGS",
        }
    }
}

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for ShowImportJobsStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            raw,
            job_id,
            where_clause,
        } = self;
        if let Some(value) = where_clause.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = raw;
        let _ = job_id;
        let _ = where_clause;
        visitor.leave(self)
    }
}

impl crate::Visitable for ShowImportGroupsStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            group_key,
            where_clause,
        } = self;
        if let Some(value) = where_clause.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = group_key;
        let _ = where_clause;
        visitor.leave(self)
    }
}

impl crate::Visitable for AdminShowNextRowIdStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { table } = self;
        let _ = table;
        visitor.leave(self)
    }
}

impl crate::Visitable for AdminShowDdlJobsStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            job_number,
            where_clause,
        } = self;
        if let Some(value) = where_clause.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = job_number;
        let _ = where_clause;
        visitor.leave(self)
    }
}

impl crate::Visitable for AdminShowDdlJobQueriesStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::JobIds(field_0) => {
                let _ = field_0;
            }
            Self::Limit { offset, count } => {
                let _ = offset;
                let _ = count;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for AdminShowSlowStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { mode, count } = self;
        if !crate::Visitable::accept(mode, visitor) {
            return false;
        }
        let _ = mode;
        let _ = count;
        visitor.leave(self)
    }
}

impl crate::Visitable for AdminShowSlowMode {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Recent => {}
            Self::Top(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for AdminShowSlowTopScope {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Default => {}
            Self::Internal => {}
            Self::All => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for AdminReloadKind {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Statistics => {}
            Self::OptRuleBlacklist => {}
            Self::ExprPushdownBlacklist => {}
            Self::Bindings => {}
            Self::ClusterBindings => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for AdminBindingControlKind {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Flush => {}
            Self::Capture => {}
            Self::Evolve => {}
        }
        visitor.leave(self)
    }
}
// END GENERATED AST VISITOR IMPLEMENTATIONS
