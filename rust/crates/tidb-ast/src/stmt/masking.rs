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

//! Typed payload and canonical restore for the grammar owned by Go's
//! `ddl_masking_parser.go` source domain.

use crate::{util::back_quote, util::push_name_path, Expr};

/// Whether the optional policy state was absent, `ENABLE`, or `DISABLE`.
///
/// Go stores this as two booleans. The enum removes the invalid fourth state
/// while preserving the observable default: omitted means enabled, but does
/// not restore an `ENABLE` suffix.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum MaskingPolicyState {
    /// Enabled by default, with no source-visible suffix.
    #[default]
    ImplicitEnabled,
    /// Explicit `ENABLE`.
    Enabled,
    /// Explicit `DISABLE`.
    Disabled,
}

/// A canonical bit set of operations named by `RESTRICT ON (...)`.
///
/// A bit set matches Go's AST contract: duplicate names collapse and restore
/// always follows the declaration order, independent of source order.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MaskingPolicyRestrictOps(u8);

impl MaskingPolicyRestrictOps {
    const INSERT_INTO_SELECT: u8 = 1 << 0;
    const UPDATE_SELECT: u8 = 1 << 1;
    const DELETE_SELECT: u8 = 1 << 2;
    const CTAS: u8 = 1 << 3;

    /// Whether no operations are restricted.
    pub fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// Adds one Go-supported operation name, case-insensitively.
    pub fn insert_name(&mut self, name: &str) -> bool {
        let bit = match name.to_ascii_uppercase().as_str() {
            "INSERT_INTO_SELECT" => Self::INSERT_INTO_SELECT,
            "UPDATE_SELECT" => Self::UPDATE_SELECT,
            "DELETE_SELECT" => Self::DELETE_SELECT,
            "CTAS" => Self::CTAS,
            _ => return false,
        };
        self.0 |= bit;
        true
    }

    fn names(self) -> impl Iterator<Item = &'static str> {
        [
            (Self::INSERT_INTO_SELECT, "INSERT_INTO_SELECT"),
            (Self::UPDATE_SELECT, "UPDATE_SELECT"),
            (Self::DELETE_SELECT, "DELETE_SELECT"),
            (Self::CTAS, "CTAS"),
        ]
        .into_iter()
        .filter_map(move |(bit, name)| (self.0 & bit != 0).then_some(name))
    }

    fn restore_into(self, out: &mut String, write_none: bool) {
        if self.is_empty() {
            if write_none {
                out.push_str("RESTRICT ON NONE");
            }
            return;
        }
        out.push_str("RESTRICT ON (");
        for (index, name) in self.names().enumerate() {
            if index > 0 {
                out.push_str(", ");
            }
            out.push_str(name);
        }
        out.push(')');
    }
}

/// A `CREATE [OR REPLACE] MASKING POLICY` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateMaskingPolicyStmt {
    /// Whether an existing policy is replaced.
    pub or_replace: bool,
    /// Whether duplicate-policy errors are suppressed.
    pub if_not_exists: bool,
    /// The policy name.
    pub name: String,
    /// The policy's target table.
    pub table: Vec<String>,
    /// The target column.
    pub column: String,
    /// The masking expression.
    pub expr: Expr,
    /// Operations prevented from selecting masked data.
    pub restrict_ops: MaskingPolicyRestrictOps,
    /// Optional explicit policy state.
    pub state: MaskingPolicyState,
}

/// One masking-policy action inside Go's `ALTER TABLE` envelope.
#[derive(Debug, Clone, PartialEq)]
pub enum AlterMaskingPolicyAction {
    /// `ADD MASKING POLICY ...`.
    Add {
        /// Policy name.
        name: String,
        /// Target column.
        column: String,
        /// Masking expression.
        expr: Expr,
        /// Restricted operations.
        restrict_ops: MaskingPolicyRestrictOps,
        /// Optional explicit state.
        state: MaskingPolicyState,
    },
    /// `ENABLE MASKING POLICY name`.
    Enable(String),
    /// `DISABLE MASKING POLICY name`.
    Disable(String),
    /// `DROP MASKING POLICY name`.
    Drop(String),
    /// `MODIFY MASKING POLICY name SET EXPRESSION = expr`.
    ModifyExpression {
        /// Policy name.
        name: String,
        /// Replacement masking expression.
        expr: Expr,
    },
    /// `MODIFY MASKING POLICY name SET RESTRICT ON ...`.
    ModifyRestrict {
        /// Policy name.
        name: String,
        /// Replacement operation set; empty restores as explicit `NONE`.
        restrict_ops: MaskingPolicyRestrictOps,
    },
}

impl CreateMaskingPolicyStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("CREATE ");
        if self.or_replace {
            out.push_str("OR REPLACE ");
        }
        out.push_str("MASKING POLICY ");
        if self.if_not_exists {
            out.push_str("IF NOT EXISTS ");
        }
        out.push_str(&back_quote(&self.name));
        out.push_str(" ON ");
        push_name_path(out, &self.table);
        out.push_str(" (");
        out.push_str(&back_quote(&self.column));
        out.push_str(") AS ");
        self.expr.restore_into(out);
        restore_optional_restrict_and_state(out, self.restrict_ops, self.state);
    }
}

impl AlterMaskingPolicyAction {
    pub(crate) fn restore_into(&self, out: &mut String) {
        match self {
            Self::Add {
                name,
                column,
                expr,
                restrict_ops,
                state,
            } => {
                out.push_str("ADD MASKING POLICY ");
                out.push_str(&back_quote(name));
                out.push_str(" ON (");
                out.push_str(&back_quote(column));
                out.push_str(") AS ");
                expr.restore_into(out);
                restore_optional_restrict_and_state(out, *restrict_ops, *state);
            }
            Self::Enable(name) => {
                out.push_str("ENABLE MASKING POLICY ");
                out.push_str(&back_quote(name));
            }
            Self::Disable(name) => {
                out.push_str("DISABLE MASKING POLICY ");
                out.push_str(&back_quote(name));
            }
            Self::Drop(name) => {
                out.push_str("DROP MASKING POLICY ");
                out.push_str(&back_quote(name));
            }
            Self::ModifyExpression { name, expr } => {
                out.push_str("MODIFY MASKING POLICY ");
                out.push_str(&back_quote(name));
                out.push_str(" SET EXPRESSION = ");
                expr.restore_into(out);
            }
            Self::ModifyRestrict { name, restrict_ops } => {
                out.push_str("MODIFY MASKING POLICY ");
                out.push_str(&back_quote(name));
                out.push_str(" SET ");
                restrict_ops.restore_into(out, true);
            }
        }
    }
}

fn restore_optional_restrict_and_state(
    out: &mut String,
    restrict_ops: MaskingPolicyRestrictOps,
    state: MaskingPolicyState,
) {
    if !restrict_ops.is_empty() {
        out.push(' ');
        restrict_ops.restore_into(out, false);
    }
    match state {
        MaskingPolicyState::ImplicitEnabled => {}
        MaskingPolicyState::Enabled => out.push_str(" ENABLE"),
        MaskingPolicyState::Disabled => out.push_str(" DISABLE"),
    }
}

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for MaskingPolicyState {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::ImplicitEnabled => {}
            Self::Enabled => {}
            Self::Disabled => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for MaskingPolicyRestrictOps {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self(field_0) = self;
        let _ = field_0;
        visitor.leave(self)
    }
}

impl crate::Visitable for CreateMaskingPolicyStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            or_replace,
            if_not_exists,
            name,
            table,
            column,
            expr,
            restrict_ops,
            state,
        } = self;
        if !crate::Visitable::accept(expr, visitor) {
            return false;
        }
        if !crate::Visitable::accept(restrict_ops, visitor) {
            return false;
        }
        if !crate::Visitable::accept(state, visitor) {
            return false;
        }
        let _ = or_replace;
        let _ = if_not_exists;
        let _ = name;
        let _ = table;
        let _ = column;
        let _ = expr;
        let _ = restrict_ops;
        let _ = state;
        visitor.leave(self)
    }
}

impl crate::Visitable for AlterMaskingPolicyAction {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Add {
                name,
                column,
                expr,
                restrict_ops,
                state,
            } => {
                if !crate::Visitable::accept(expr, visitor) {
                    return false;
                }
                if !crate::Visitable::accept(restrict_ops, visitor) {
                    return false;
                }
                if !crate::Visitable::accept(state, visitor) {
                    return false;
                }
                let _ = name;
                let _ = column;
                let _ = expr;
                let _ = restrict_ops;
                let _ = state;
            }
            Self::Enable(field_0) => {
                let _ = field_0;
            }
            Self::Disable(field_0) => {
                let _ = field_0;
            }
            Self::Drop(field_0) => {
                let _ = field_0;
            }
            Self::ModifyExpression { name, expr } => {
                if !crate::Visitable::accept(expr, visitor) {
                    return false;
                }
                let _ = name;
                let _ = expr;
            }
            Self::ModifyRestrict { name, restrict_ops } => {
                if !crate::Visitable::accept(restrict_ops, visitor) {
                    return false;
                }
                let _ = name;
                let _ = restrict_ops;
            }
        }
        visitor.leave(self)
    }
}
// END GENERATED AST VISITOR IMPLEMENTATIONS
