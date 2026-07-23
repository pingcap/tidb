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

//! Shared table-, column-, and `ALTER TABLE ADD` CHECK-constraint payload.

use crate::util::back_quote;
use crate::{Expr, RestoreContext};

/// Go's one semantic `CHECK`-constraint payload, shared by table-level,
/// column-level, and `ALTER TABLE ADD` grammar productions. The enclosing
/// enum retains where the constraint was declared; this type carries only the
/// source contract those declarations have in common.
#[derive(Debug, Clone, PartialEq)]
pub struct CheckConstraintDefinition {
    /// The optional explicitly supplied constraint name.
    pub name: Option<String>,
    /// The boolean expression to check.
    pub expression: Expr,
    /// Whether `ENFORCED` (the default) or `NOT ENFORCED` applies.
    pub enforced: bool,
}

impl CheckConstraintDefinition {
    pub(super) fn restore_into(&self, out: &mut String) {
        self.restore_into_with_context(out, &RestoreContext::default());
    }

    pub(super) fn restore_into_with_context(&self, out: &mut String, context: &RestoreContext) {
        if let Some(name) = &self.name {
            out.push_str("CONSTRAINT ");
            out.push_str(&back_quote(name));
            out.push(' ');
        }
        out.push_str("CHECK(");
        self.expression.restore_into_with_context(out, context);
        out.push(')');
        out.push_str(if self.enforced {
            " ENFORCED"
        } else {
            " NOT ENFORCED"
        });
    }
}

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for CheckConstraintDefinition {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            name,
            expression,
            enforced,
        } = self;
        if !crate::Visitable::accept(expression, visitor) {
            return false;
        }
        let _ = name;
        let _ = expression;
        let _ = enforced;
        visitor.leave(self)
    }
}
// END GENERATED AST VISITOR IMPLEMENTATIONS
