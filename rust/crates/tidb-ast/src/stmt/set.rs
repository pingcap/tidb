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

//! Non-account, non-user-variable SET payloads translated from
//! `pkg/parser/set_explain_parser.go` and `pkg/parser/ast/misc.go`.

use crate::util::{back_quote, escape_string_literal};
use crate::Expr;

/// A generic system-variable `SET` statement.
///
/// TiDB's `ast.SetStmt` is a list of `VariableAssignment`s. An unscoped name
/// and `LOCAL` are session scoped; `GLOBAL` and `INSTANCE` remain AST-visible
/// so execution can reject unsupported distributed scopes honestly.
#[derive(Debug, Clone, PartialEq)]
pub struct SetStmt {
    /// Assignments in source order.
    pub assignments: Vec<SystemVariableAssignment>,
}

impl SetStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("SET ");
        for (index, assignment) in self.assignments.iter().enumerate() {
            if index > 0 {
                out.push_str(", ");
            }
            assignment.restore_into(out);
        }
    }
}

/// A single system-variable assignment within [`SetStmt`].
#[derive(Debug, Clone, PartialEq)]
pub struct SystemVariableAssignment {
    /// Scope selected by the input. `LOCAL` canonicalizes to `Session`.
    pub scope: SystemVariableScope,
    /// Variable name, including at most one non-scope dot component.
    pub name: String,
    /// The syntactically distinct SET-value form.
    pub value: SetVariableValue,
}

impl SystemVariableAssignment {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("@@");
        out.push_str(match self.scope {
            SystemVariableScope::Session => "SESSION",
            SystemVariableScope::Global => "GLOBAL",
            SystemVariableScope::Instance => "INSTANCE",
        });
        out.push('.');
        out.push_str(&back_quote(&self.name));
        out.push('=');
        self.value.restore_into(out);
    }
}

/// System-variable assignment scope accepted by TiDB's generic SET loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SystemVariableScope {
    /// Unqualified, `SESSION`, and `LOCAL` assignments.
    Session,
    /// A cluster-wide `GLOBAL` assignment.
    Global,
    /// An `INSTANCE` assignment.
    Instance,
}

/// RHS grammar for a system-variable assignment.
#[derive(Debug, Clone, PartialEq)]
pub enum SetVariableValue {
    /// The `DEFAULT` SET-value production.
    Default,
    /// A special SET string literal or ordinary expression.
    Expr(Expr),
}

impl SetVariableValue {
    fn restore_into(&self, out: &mut String) {
        match self {
            Self::Default => out.push_str("DEFAULT"),
            Self::Expr(expr) => expr.restore_into(out),
        }
    }
}

/// The two distinguished session charset command families.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CharsetSetKind {
    /// `SET NAMES charset [COLLATE collation]`.
    Names,
    /// `SET CHARACTER SET`, `SET CHAR SET`, or `SET CHARSET`.
    Charset,
}

/// One comma-separated item in a heterogeneous `SET` statement.
#[derive(Debug, Clone, PartialEq)]
pub enum SetItem {
    /// An ordinary system-variable assignment.
    System(SystemVariableAssignment),
    /// `NAMES` or `CHARSET` connection configuration.
    Charset {
        /// Charset command family.
        kind: CharsetSetKind,
        /// Canonical charset name, or `None` for `DEFAULT`.
        charset: Option<String>,
        /// Optional `NAMES ... COLLATE ...` value.
        collation: Option<String>,
    },
}

impl SetItem {
    pub(crate) fn restore_into(&self, out: &mut String) {
        match self {
            Self::System(assignment) => assignment.restore_into(out),
            Self::Charset {
                kind,
                charset,
                collation,
            } => restore_set_charset_item(out, *kind, charset, collation),
        }
    }
}

pub(crate) fn restore_set_charset(
    out: &mut String,
    kind: CharsetSetKind,
    charset: &Option<String>,
    collation: &Option<String>,
) {
    out.push_str("SET ");
    restore_set_charset_item(out, kind, charset, collation);
}

pub(crate) fn restore_set_charset_item(
    out: &mut String,
    kind: CharsetSetKind,
    charset: &Option<String>,
    collation: &Option<String>,
) {
    out.push_str(match kind {
        CharsetSetKind::Names => "NAMES ",
        CharsetSetKind::Charset => "CHARSET ",
    });
    match charset {
        Some(charset) => {
            out.push('\'');
            out.push_str(&escape_string_literal(charset));
            out.push('\'');
        }
        None => out.push_str("DEFAULT"),
    }
    if let Some(collation) = collation {
        out.push_str(" COLLATE '");
        out.push_str(&escape_string_literal(collation));
        out.push('\'');
    }
}

/// The parser-visible payload of `SET RESOURCE GROUP name`.
#[derive(Debug, Clone, PartialEq)]
pub struct SetResourceGroupStmt {
    /// The decoded resource-group name.
    pub name: String,
}

impl SetResourceGroupStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("SET RESOURCE GROUP ");
        out.push_str(&back_quote(&self.name));
    }
}

/// The parser-visible payload of `SET SESSION_STATES 'serialized state'`.
#[derive(Debug, Clone, PartialEq)]
pub struct SetSessionStatesStmt {
    /// The decoded serialized session state.
    pub session_states: String,
}

impl SetSessionStatesStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("SET SESSION_STATES '");
        out.push_str(&escape_string_literal(&self.session_states));
        out.push('\'');
    }
}

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for SetStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { assignments } = self;
        for value in assignments.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = assignments;
        visitor.leave(self)
    }
}

impl crate::Visitable for SetItem {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::System(assignment) => {
                if !assignment.accept(visitor) {
                    return false;
                }
            }
            Self::Charset { kind, .. } => {
                if !kind.accept(visitor) {
                    return false;
                }
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for SystemVariableAssignment {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { scope, name, value } = self;
        if !crate::Visitable::accept(scope, visitor) {
            return false;
        }
        if !crate::Visitable::accept(value, visitor) {
            return false;
        }
        let _ = scope;
        let _ = name;
        let _ = value;
        visitor.leave(self)
    }
}

impl crate::Visitable for SystemVariableScope {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Session => {}
            Self::Global => {}
            Self::Instance => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for SetVariableValue {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Default => {}
            Self::Expr(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for CharsetSetKind {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Names => {}
            Self::Charset => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for SetResourceGroupStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { name } = self;
        let _ = name;
        visitor.leave(self)
    }
}

impl crate::Visitable for SetSessionStatesStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { session_states } = self;
        let _ = session_states;
        visitor.leave(self)
    }
}
// END GENERATED AST VISITOR IMPLEMENTATIONS
