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

//! User-variable assignment statement payloads.

use crate::util::back_quote;
use crate::Expr;

/// A homogeneous `SET @name = value [, @name = value ...]` statement (see
/// [`SessionStmt::SetUserVar`] for why it is separate from [`SetStmt`]). Assignments
/// are retained in source order because TiDB evaluates and writes each one
/// before evaluating the next: a later right-hand side observes earlier
/// writes, and a later error does not roll back those writes.
#[derive(Debug, Clone, PartialEq)]
pub struct SetUserVarStmt {
    /// User-variable assignments in source order.
    pub assignments: Vec<UserVariableAssignment>,
}

impl SetUserVarStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("SET @");
        for (index, assignment) in self.assignments.iter().enumerate() {
            if index > 0 {
                out.push_str(", @");
            }
            assignment.restore_into(out);
        }
    }
}

/// One user-variable assignment within [`SetUserVarStmt`]. `name` omits its
/// leading `@` and preserves source case for restore. Quoted and special
/// user-variable names remain outside this parser's supported grammar.
#[derive(Debug, Clone, PartialEq)]
pub struct UserVariableAssignment {
    /// The user variable's name, without its leading `@`.
    pub name: String,
    /// The assigned value.
    pub value: Expr,
}

impl UserVariableAssignment {
    fn restore_into(&self, out: &mut String) {
        out.push_str(&back_quote(&self.name));
        out.push('=');
        self.value.restore_into(out);
    }
}

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for SetUserVarStmt {
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

impl crate::Visitable for UserVariableAssignment {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { name, value } = self;
        if !crate::Visitable::accept(value, visitor) {
            return false;
        }
        let _ = name;
        let _ = value;
        visitor.leave(self)
    }
}
// END GENERATED AST VISITOR IMPLEMENTATIONS
