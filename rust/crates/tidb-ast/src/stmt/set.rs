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

//! SET payloads translated from
//! `pkg/parser/set_explain_parser.go` and `pkg/parser/ast/misc.go`.

use crate::util::{back_quote, escape_string_literal, redact_url};
use crate::Expr;

/// Go `ast.SetNames`: the [`VariableAssignment::name`] of a `SET NAMES`
/// item, whose value is the charset and whose `extend_value` the collation.
pub const SET_NAMES: &str = "SetNAMES";

/// Go `ast.SetCharset`: the [`VariableAssignment::name`] of a
/// `SET {CHARACTER SET | CHAR SET | CHARSET}` item.
pub const SET_CHARSET: &str = "SetCharset";

/// Go `ast.TiDBCloudStorageURI`.
const TIDB_CLOUD_STORAGE_URI: &str = "tidb_cloud_storage_uri";

/// Go `ast.SetStmt`: every `SET` that assigns variables -- user variables,
/// system variables, `NAMES`, `CHARSET`, and `SET TRANSACTION`'s lowered
/// `tx_*` assignments -- in source order, mixed freely.
#[derive(Debug, Clone, PartialEq)]
pub struct SetStmt {
    /// Go `Variables`.
    pub variables: Vec<VariableAssignment>,
}

impl SetStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("SET ");
        for (index, variable) in self.variables.iter().enumerate() {
            if index > 0 {
                out.push_str(", ");
            }
            variable.restore_into(out);
        }
    }

    /// Go `SetStmt.SecureText`: the restore, which redacts the credentials
    /// in a `tidb_cloud_storage_uri` URL, for processlist, logging, and
    /// audit consumers.
    pub fn secure_text(&self) -> String {
        let mut out = String::new();
        self.restore_into(&mut out);
        out
    }
}

/// Go `ast.VariableAssignment`.
#[derive(Debug, Clone, PartialEq)]
pub struct VariableAssignment {
    /// Go `Name`: a user variable's name without its `@`, a system
    /// variable's lowercased name, or [`SET_NAMES`] / [`SET_CHARSET`].
    pub name: String,
    /// Go `Value`: `DEFAULT` is [`Expr::Default`]`(None)`, Go's `DefaultExpr`.
    pub value: Expr,
    /// Go `IsInstance`.
    pub is_instance: bool,
    /// Go `IsGlobal`.
    pub is_global: bool,
    /// Go `IsSystem`.
    pub is_system: bool,
    /// Go `ExtendValue`: `SET NAMES cs COLLATE co`'s collation literal.
    pub extend_value: Option<Expr>,
}

impl VariableAssignment {
    /// A session-scoped system-variable assignment, as Go's parser builds
    /// for an unqualified name.
    #[must_use]
    pub fn system(name: impl Into<String>, value: Expr) -> Self {
        Self {
            name: name.into(),
            value,
            is_instance: false,
            is_global: false,
            is_system: true,
            extend_value: None,
        }
    }

    /// Go `VariableAssignment.Restore`.
    pub(crate) fn restore_into(&self, out: &mut String) {
        if self.is_system {
            out.push_str("@@");
            out.push_str(if self.is_global {
                "GLOBAL"
            } else if self.is_instance {
                "INSTANCE"
            } else {
                "SESSION"
            });
            out.push('.');
        } else if self.name != SET_NAMES && self.name != SET_CHARSET {
            out.push('@');
        }
        if self.name == SET_NAMES {
            out.push_str("NAMES ");
        } else if self.name == SET_CHARSET {
            out.push_str("CHARSET ");
        } else {
            out.push_str(&back_quote(&self.name));
            out.push('=');
        }
        match &self.value {
            // Go writes the URL plain, its credentials redacted, for `show
            // processlist`. Go asserts a literal there (any other value
            // panics its restore); this restores such a value as written.
            Expr::String(text)
            | Expr::RawString(text)
            | Expr::CharsetString { value: text, .. }
                if self.name == TIDB_CLOUD_STORAGE_URI =>
            {
                out.push_str(&redact_url(text));
            }
            value => value.restore_into(out),
        }
        if let Some(extend_value) = &self.extend_value {
            out.push_str(" COLLATE ");
            extend_value.restore_into(out);
        }
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
        for variable in &mut self.variables {
            if !crate::Visitable::accept(variable, visitor) {
                return false;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for VariableAssignment {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        // Go `VariableAssignment.Accept` visits `Value` only.
        if !crate::Visitable::accept(&mut self.value, visitor) {
            return false;
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
