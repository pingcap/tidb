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

//! SQL-binding AST payloads and restore translated from
//! `pkg/parser/ast/misc.go`.

use crate::util::{back_quote, escape_string_literal};
use crate::{Expr, Stmt};

/// Binding scope. Go's parser treats an omitted scope as `SESSION`, and its
/// restore code makes that default explicit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindingScope {
    /// Bindings shared by all sessions.
    Global,
    /// Bindings owned by the current session (also the omitted-scope form).
    Session,
}

impl BindingScope {
    fn restore_into(self, out: &mut String) {
        out.push_str(match self {
            Self::Global => "GLOBAL ",
            Self::Session => "SESSION ",
        });
    }
}

/// A string literal or user variable accepted by TiDB's binding-digest list
/// grammar. The user-variable name does not include its leading `@`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BindingValue {
    /// A decoded SQL/plan digest literal.
    String(String),
    /// A user variable which supplies a digest at execution time.
    UserVar(String),
}

impl BindingValue {
    fn restore_into(&self, out: &mut String) {
        match self {
            // Go's `ast.StringOrUserVar` uses its plain-string restore path
            // here (unlike a general expression literal), so binding digests
            // do not gain a `_UTF8MB4` introducer.
            Self::String(value) => {
                out.push('\'');
                out.push_str(&escape_string_literal(value));
                out.push('\'');
            }
            Self::UserVar(name) => {
                out.push('@');
                out.push_str(&back_quote(name));
            }
        }
    }
}

/// A parsed origin SQL statement and its optional hinted counterpart. Keeping
/// nested statements typed means the parser cannot accept unmodelled inner SQL
/// merely because it appeared inside a binding command.
#[derive(Debug, Clone, PartialEq)]
pub struct BindingStatementTarget {
    /// SQL whose plan is bound.
    pub origin: Box<Stmt>,
    /// Optional SQL carrying optimizer hints.
    pub hinted: Option<Box<Stmt>>,
}

impl BindingStatementTarget {
    fn restore_into(&self, out: &mut String) {
        self.origin.restore_into(out);
        if let Some(hinted) = &self.hinted {
            out.push_str(" USING ");
            hinted.restore_into(out);
        }
    }
}

/// The source of a `CREATE ... BINDING`: either two parsed statements or a
/// plan-digest history lookup.
#[derive(Debug, Clone, PartialEq)]
pub enum CreateBindingSource {
    /// `FOR origin USING hinted`.
    Statement {
        /// The origin/hinted pair. `hinted` is always present for CREATE.
        target: BindingStatementTarget,
    },
    /// `FROM HISTORY USING PLAN DIGEST value [, ...]`.
    History {
        /// Plan digests in source order.
        plan_digests: Vec<BindingValue>,
    },
}

/// `CREATE [GLOBAL|SESSION] BINDING`.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateBindingStmt {
    /// Scope of the new binding.
    pub scope: BindingScope,
    /// Statement or history source.
    pub source: CreateBindingSource,
}

impl CreateBindingStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("CREATE ");
        self.scope.restore_into(out);
        match &self.source {
            CreateBindingSource::Statement { target } => {
                out.push_str("BINDING FOR ");
                target.restore_into(out);
            }
            CreateBindingSource::History { plan_digests } => {
                out.push_str("BINDING FROM HISTORY USING PLAN DIGEST ");
                for (index, digest) in plan_digests.iter().enumerate() {
                    if index > 0 {
                        out.push_str(", ");
                    }
                    digest.restore_into(out);
                }
            }
        }
    }
}

/// The target of a `DROP ... BINDING`: a parsed statement or one or more SQL
/// digests.
#[derive(Debug, Clone, PartialEq)]
pub enum DropBindingTarget {
    /// `FOR origin [USING hinted]`.
    Statement(BindingStatementTarget),
    /// `FOR SQL DIGEST digest [, ...]`.
    SqlDigests(Vec<BindingValue>),
}

/// `DROP [GLOBAL|SESSION] BINDING`.
#[derive(Debug, Clone, PartialEq)]
pub struct DropBindingStmt {
    /// Scope of the binding to remove.
    pub scope: BindingScope,
    /// Statement or digest target.
    pub target: DropBindingTarget,
}

impl DropBindingStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("DROP ");
        self.scope.restore_into(out);
        out.push_str("BINDING FOR ");
        match &self.target {
            DropBindingTarget::Statement(target) => target.restore_into(out),
            DropBindingTarget::SqlDigests(digests) => {
                out.push_str("SQL DIGEST ");
                for (index, digest) in digests.iter().enumerate() {
                    if index > 0 {
                        out.push_str(", ");
                    }
                    digest.restore_into(out);
                }
            }
        }
    }
}

/// The binding status selected by `SET BINDING`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindingStatus {
    /// Makes a binding eligible for plan selection.
    Enabled,
    /// Makes a binding ineligible for plan selection.
    Disabled,
}

/// The target of `SET BINDING`: a parsed statement or the grammar's one
/// required string SQL digest (unlike DROP's list of string-or-variable
/// values).
#[derive(Debug, Clone, PartialEq)]
pub enum SetBindingTarget {
    /// `FOR origin [USING hinted]`.
    Statement(BindingStatementTarget),
    /// `FOR SQL DIGEST 'digest'`.
    SqlDigest(String),
}

/// `SET BINDING ENABLED|DISABLED FOR ...`.
#[derive(Debug, Clone, PartialEq)]
pub struct SetBindingStmt {
    /// New status.
    pub status: BindingStatus,
    /// Binding to change.
    pub target: SetBindingTarget,
}

impl SetBindingStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("SET BINDING ");
        out.push_str(match self.status {
            BindingStatus::Enabled => "ENABLED FOR ",
            BindingStatus::Disabled => "DISABLED FOR ",
        });
        match &self.target {
            SetBindingTarget::Statement(target) => target.restore_into(out),
            SetBindingTarget::SqlDigest(digest) => {
                out.push_str("SQL DIGEST ");
                out.push('\'');
                out.push_str(&escape_string_literal(digest));
                out.push('\'');
            }
        }
    }
}

/// `SHOW [GLOBAL|SESSION] BINDINGS [LIKE expr | WHERE expr]`.
#[derive(Debug, Clone, PartialEq)]
pub struct ShowBindingsStmt {
    /// Scope requested by the statement; omitted scope restores as `SESSION`.
    pub scope: BindingScope,
    /// Optional filtering clause.
    pub filter: Option<ShowBindingsFilter>,
}

/// The two mutually exclusive filters shared by TiDB's SHOW binding grammar.
#[derive(Debug, Clone, PartialEq)]
pub enum ShowBindingsFilter {
    /// `LIKE <simple expression>`.
    Like(Expr),
    /// `WHERE <expression>`.
    Where(Expr),
}

impl ShowBindingsStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("SHOW ");
        self.scope.restore_into(out);
        out.push_str("BINDINGS");
        match &self.filter {
            None => {}
            Some(ShowBindingsFilter::Like(expr)) => {
                out.push_str(" LIKE ");
                expr.restore_into(out);
            }
            Some(ShowBindingsFilter::Where(expr)) => {
                out.push_str(" WHERE ");
                expr.restore_into(out);
            }
        }
    }
}

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for BindingScope {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Global => {}
            Self::Session => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for BindingValue {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::String(field_0) => {
                let _ = field_0;
            }
            Self::UserVar(field_0) => {
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for BindingStatementTarget {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { origin, hinted } = self;
        if !crate::Visitable::accept(origin.as_mut(), visitor) {
            return false;
        }
        if let Some(value) = hinted.as_mut() {
            if !crate::Visitable::accept(value.as_mut(), visitor) {
                return false;
            }
        }
        let _ = origin;
        let _ = hinted;
        visitor.leave(self)
    }
}

impl crate::Visitable for CreateBindingSource {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Statement { target } => {
                if !crate::Visitable::accept(target, visitor) {
                    return false;
                }
                let _ = target;
            }
            Self::History { plan_digests } => {
                for value in plan_digests.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                let _ = plan_digests;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for CreateBindingStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { scope, source } = self;
        if !crate::Visitable::accept(scope, visitor) {
            return false;
        }
        if !crate::Visitable::accept(source, visitor) {
            return false;
        }
        let _ = scope;
        let _ = source;
        visitor.leave(self)
    }
}

impl crate::Visitable for DropBindingTarget {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Statement(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::SqlDigests(field_0) => {
                for value in field_0.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for DropBindingStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { scope, target } = self;
        if !crate::Visitable::accept(scope, visitor) {
            return false;
        }
        if !crate::Visitable::accept(target, visitor) {
            return false;
        }
        let _ = scope;
        let _ = target;
        visitor.leave(self)
    }
}

impl crate::Visitable for BindingStatus {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Enabled => {}
            Self::Disabled => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for SetBindingTarget {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Statement(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::SqlDigest(field_0) => {
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for SetBindingStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { status, target } = self;
        if !crate::Visitable::accept(status, visitor) {
            return false;
        }
        if !crate::Visitable::accept(target, visitor) {
            return false;
        }
        let _ = status;
        let _ = target;
        visitor.leave(self)
    }
}

impl crate::Visitable for ShowBindingsStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { scope, filter } = self;
        if !crate::Visitable::accept(scope, visitor) {
            return false;
        }
        if let Some(value) = filter.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = scope;
        let _ = filter;
        visitor.leave(self)
    }
}

impl crate::Visitable for ShowBindingsFilter {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Like(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Where(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}
// END GENERATED AST VISITOR IMPLEMENTATIONS
