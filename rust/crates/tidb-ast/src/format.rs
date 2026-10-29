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

//! The AST's restore policy: Go `ast/expressions.go`'s parent-operator
//! precedence (`restoreBinaryPrecedence`) carried through a copyable context.

use std::sync::Arc;

use tidb_mysql::to_lowercase as identifier_to_lower;
use tidb_parser_format::RestoreFlags;

/// The operator an expression restore is nested under (Go `RestoreCtx.
/// ParentBinaryOp`), which decides whether a parenthesized child keeps its
/// parentheses under `RestoreSkipRedundantParentheses`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RestoreParentOp {
    /// An infix operator.
    Binary(crate::BinaryOp),
    /// `BETWEEN` (Go's restore-only opcode).
    Between,
    /// `IN`.
    In,
    /// `LIKE` / `ILIKE`.
    Like,
    /// `REGEXP`.
    Regexp,
    /// `IS [NOT] NULL`.
    IsNull,
    /// `IS [NOT] TRUE`.
    IsTruth,
    /// `IS [NOT] FALSE`.
    IsFalsity,
    /// `MEMBER OF`.
    MemberOf,
    /// `COLLATE`.
    Collate,
}

/// Which side of its parent operator a child sits on (Go
/// `RestoreCtx.ParentBinarySide`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RestoreParentSide {
    /// The left operand.
    Left,
    /// The right operand.
    Right,
}

impl RestoreParentOp {
    /// Go `restoreBinaryPrecedence`: MySQL operator precedence, larger binds
    /// tighter, 0 unknown.
    fn precedence(self) -> u8 {
        use crate::BinaryOp as B;
        match self {
            Self::Binary(op) => match op {
                B::LogicOr => 1,
                B::LogicXor => 2,
                B::LogicAnd => 3,
                B::Eq | B::Ne | B::NullEq | B::Lt | B::Le | B::Gt | B::Ge => 5,
                B::BitOr => 6,
                B::BitAnd => 7,
                B::LeftShift | B::RightShift => 8,
                B::Plus | B::Minus => 9,
                B::Mul | B::Div | B::IntDiv | B::Mod => 10,
                B::BitXor => 11,
            },
            Self::In
            | Self::Like
            | Self::Regexp
            | Self::IsNull
            | Self::IsTruth
            | Self::IsFalsity
            | Self::MemberOf => 5,
            Self::Between => 4,
            Self::Collate => 12,
        }
    }

    /// Go `isAssociativeRestoreOp`.
    fn associative_with(self, child: Self) -> bool {
        use crate::BinaryOp as B;
        self == child
            && matches!(
                self,
                Self::Binary(B::LogicAnd | B::LogicOr | B::BitAnd | B::BitOr | B::BitXor)
            )
    }

    /// Go `canRestoreBinaryChildWithoutParentheses`.
    fn child_can_drop_parentheses(self, child: Self, side: RestoreParentSide) -> bool {
        let (parent_precedence, child_precedence) = (self.precedence(), child.precedence());
        if parent_precedence == 0 || child_precedence == 0 {
            return false;
        }
        if child_precedence > parent_precedence {
            return true;
        }
        if child_precedence < parent_precedence {
            return false;
        }
        side == RestoreParentSide::Left || self.associative_with(child)
    }
}

/// Copyable restore policy passed through the existing AST restore tree.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RestoreContext {
    flags: RestoreFlags,
    default_db: Arc<str>,
    cte_names: Arc<Vec<String>>,
    /// Go `ParentBinaryOp` and `ParentBinarySide`.
    parent: Option<(RestoreParentOp, RestoreParentSide)>,
    /// Go `InUnaryOperation`.
    in_unary: bool,
    /// The charset a plain string literal carries: Go's parser types every
    /// `ValueExpr` string with the session's `character_set_connection`
    /// (`ast.NewValueExpr(lit, parser.charset, parser.collation)`), and
    /// `ValueExpr.Restore` writes it back as the `_charset` introducer.
    string_charset: Arc<str>,
}

impl Default for RestoreContext {
    fn default() -> Self {
        Self::new(RestoreFlags::DEFAULT)
    }
}

impl RestoreContext {
    /// Creates a restore policy with the supplied flags.
    pub fn new(flags: RestoreFlags) -> Self {
        Self {
            flags,
            default_db: Arc::from(""),
            cte_names: Arc::new(Vec::new()),
            parent: None,
            in_unary: false,
            string_charset: Arc::from("utf8mb4"),
        }
    }

    /// Returns a context whose plain string literals carry `charset`: the
    /// `character_set_connection` of the session that parsed the statement.
    #[must_use]
    pub fn with_string_charset(mut self, charset: &str) -> Self {
        self.string_charset = Arc::from(charset);
        self
    }

    /// The charset a plain string literal restores with.
    #[must_use]
    pub fn string_charset(&self) -> &str {
        &self.string_charset
    }

    /// Go `restoreExprWithBinaryOpParent`: the context a child restores under.
    #[must_use]
    pub fn with_parent(&self, op: RestoreParentOp, side: RestoreParentSide) -> Self {
        let mut context = self.clone();
        context.parent = Some((op, side));
        context
    }

    /// Go `restoreExprWithUnaryOpParent`.
    #[must_use]
    pub fn with_unary_parent(&self) -> Self {
        let mut context = self.clone();
        context.in_unary = true;
        context
    }

    /// Go `restoreWithResetParentContext`.
    #[must_use]
    pub fn with_reset_parent(&self) -> Self {
        let mut context = self.clone();
        context.parent = None;
        context.in_unary = false;
        context
    }

    /// Go `canRestoreWithoutParentheses`, for a parenthesized `expr` whose
    /// own operator is `child` (`None` when it has none) and whether it is a
    /// unary operation.
    pub fn can_restore_without_parentheses(
        &self,
        child: Option<RestoreParentOp>,
        child_is_unary: bool,
    ) -> bool {
        if self.in_unary {
            return false;
        }
        // Dropping a unary child's parentheses could expose a different parse
        // boundary to the surrounding binary operator.
        if child_is_unary && self.parent.is_some() {
            return false;
        }
        let Some(child) = child else {
            return true;
        };
        let Some((parent, side)) = self.parent else {
            return true;
        };
        parent.child_can_drop_parentheses(child, side)
    }

    /// Returns a context that qualifies unqualified table names with `database`.
    pub fn with_default_db(mut self, database: impl Into<Arc<str>>) -> Self {
        self.default_db = database.into();
        self
    }

    /// Returns a nested context in which `name` resolves as a CTE table.
    pub fn with_cte(&self, name: &str) -> Self {
        let mut names = self.cte_names.as_ref().clone();
        names.push(identifier_to_lower(name));
        Self {
            flags: self.flags,
            default_db: Arc::clone(&self.default_db),
            cte_names: Arc::new(names),
            parent: self.parent,
            in_unary: self.in_unary,
            string_charset: Arc::clone(&self.string_charset),
        }
    }

    /// Returns the same restore policy with selected formatting flags removed.
    pub fn without_flags(&self, flags: RestoreFlags) -> Self {
        Self {
            flags: self.flags.without(flags),
            default_db: Arc::clone(&self.default_db),
            cte_names: Arc::clone(&self.cte_names),
            parent: self.parent,
            in_unary: self.in_unary,
            string_charset: Arc::clone(&self.string_charset),
        }
    }

    /// Go `CTERestorer.IsCTETableName`: whether `name` is a CTE visible in
    /// this restore scope.
    pub fn is_cte_table_name(&self, name: &str) -> bool {
        let name = identifier_to_lower(name);
        self.cte_names.iter().any(|cte| *cte == name)
    }

    /// Database prefix for an unqualified table, following Go TableName.Restore.
    pub fn default_db_for_table(&self, name: &str, is_alias: bool) -> Option<&str> {
        (!self.flags.has_without_schema_name()
            && !self.default_db.is_empty()
            && !is_alias
            && !self
                .cte_names
                .iter()
                .any(|cte| *cte == identifier_to_lower(name)))
        .then_some(self.default_db.as_ref())
    }

    /// Returns the active restore flags.
    pub const fn flags(&self) -> RestoreFlags {
        self.flags
    }

    /// Writes `write` plainly or inside the source special-comment envelope.
    pub fn write_with_tidb_special_comment(
        &self,
        out: &mut String,
        feature_id: &str,
        write: impl FnOnce(&mut String),
    ) {
        if self.flags.has_tidb_special_comment() {
            out.push_str("/*T!");
            if !feature_id.is_empty() {
                out.push('[');
                out.push_str(feature_id);
                out.push(']');
            }
            out.push(' ');
            write(out);
            out.push_str(" */");
        } else {
            write(out);
        }
    }
}
