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
// See the License for the specific language governing permissions and
// limitations under the License.

//! What Go's operator dispatch reads off an ARGUMENT EXPRESSION, as opposed
//! to off the evaluated value.
//!
//! Every `getFunction`, `GetAccurateCmpType` and `evalInt` body quoted in
//! `ops.rs` consults exactly two things about `args[i]`, and neither survives
//! into a `Datum`:
//!
//!  * `args[i].GetType(ctx)` -- the operand's `*types.FieldType`. Its
//!    `UnsignedFlag` is the ONLY place a `DOUBLE UNSIGNED` differs from a
//!    `DOUBLE` and a `YEAR` from a `SMALLINT`; both pairs read back as the
//!    same `Datum` kind carrying the same bits.
//!  * the Go TYPE SWITCH `args[i].(*Constant)` / `args[i].(*Column)`. Nothing
//!    in a `Datum` says whether the value came from a literal, and
//!    `GetAccurateCmpType` changes the COMPARISON DOMAIN on exactly that
//!    question.
//!
//! So the descriptor carries the ARGUMENT ITSELF, which is what Go has, and
//! answers each question the way Go's dispatch asks it. One parameter, no
//! pre-derived boolean per rule.

use tidb_datatype::{EvalType, FieldType, FieldTypeCode};

use crate::expression::Expression;

/// One operand of an operator, as its dispatch sees it.
#[derive(Clone, Copy)]
pub(crate) enum Operand<'a> {
    /// A real expression-tree argument -- Go's `args[i]`.
    Expr(&'a Expression),
    /// An operand the caller evaluated from a parsed `tidb_ast::Expr` with no
    /// built expression tree behind it: the AST tier's `eval_in`, and every
    /// hand-assembled call into [`crate::apply_binary`]/[`crate::apply_unary`].
    ///
    /// It answers [`Operand::is_constant`] TRUE, and that is faithful rather
    /// than a fallback: that tier only ever walks literals and operators over
    /// literals, which is precisely what Go's `foldConstant` collapses into a
    /// `*Constant` before any signature is built. Its field type is genuinely
    /// absent, so every type-driven rule below declines instead of guessing.
    Literal,
}

impl<'a> Operand<'a> {
    /// Go `args[i].GetType(ctx)`.
    fn field_type(self) -> Option<&'a FieldType> {
        match self {
            Operand::Expr(expr) => expr.static_type(),
            Operand::Literal => None,
        }
    }

    /// Go `mysql.HasUnsignedFlag(args[i].GetType(ctx).GetFlag())`.
    pub(crate) fn is_unsigned(self) -> bool {
        self.field_type().is_some_and(FieldType::is_unsigned)
    }

    /// Go `args[i].GetType(ctx).EvalType()`.
    pub(crate) fn eval_type(self) -> Option<EvalType> {
        self.field_type().map(FieldType::eval_type)
    }

    /// Go `args[i].GetType(ctx).EvalType().IsStringKind()`.
    pub(crate) fn is_string_kind(self) -> bool {
        self.eval_type().is_some_and(EvalType::is_string_kind)
    }

    /// Go `_, ok := args[i].(*Constant)`, asked of the tree Go's dispatch
    /// actually sees.
    ///
    /// That distinction is the whole subtlety. Go runs `foldConstant`
    /// (`pkg/expression/constant_fold.go`) as each `NewFunction` is built, so
    /// by the time `GetAccurateCmpType` or `unaryMinusFunctionClass.typeInfer`
    /// performs its type switch, every wholly-constant subtree has ALREADY
    /// been replaced by a `*Constant`. This rewriter deliberately keeps such
    /// subtrees as `ScalarFunction` nodes and evaluates them
    /// (`constant_fold::derive_constant_null_flag` computes the same fold but
    /// keeps only its NOT NULL flag), so testing the node kind alone would
    /// answer FALSE where Go answers TRUE -- and Go's answers differ:
    /// `time_col = CONCAT('1:00',':00')` is 1 in Go (folded, so a DURATION
    /// comparison) and `time_col = varchar_col` is 0 (a STRING comparison).
    ///
    /// [`crate::constant_fold::folds_to_constant`] is therefore the predicate,
    /// not the variant test.
    pub(crate) fn is_constant(self) -> bool {
        match self {
            Operand::Expr(expr) => crate::constant_fold::folds_to_constant(expr),
            Operand::Literal => true,
        }
    }

    /// Go `isTemporalColumn(ctx, expr) && col.GetType(ctx).GetType() ==
    /// mysql.TypeDuration`, which is the ONLY case in which
    /// `GetAccurateCmpType`'s temporal-column arm actually changes the
    /// comparison type -- the arm tests date/datetime/timestamp columns too,
    /// then narrows to `TypeDuration` before assigning `cmpType`. A DATETIME
    /// column against text was already upgraded to ETDatetime by the earlier,
    /// UNCONDITIONAL arm (`builtin_compare.go:1441`), which is why only the
    /// duration half needs the constant-ness gate.
    ///
    /// Unlike [`Self::is_constant`] this is a genuine node test -- folding
    /// never turns a subtree INTO a column.
    pub(crate) fn is_duration_column(self) -> bool {
        let Operand::Expr(expr) = self else {
            return false;
        };
        matches!(expr, Expression::Column(_))
            && expr
                .static_type()
                .is_some_and(|ft| ft.code() == FieldTypeCode::Duration)
    }
}

/// The pair of descriptors a binary operator's dispatch needs, kept together
/// so threading them costs one parameter rather than two.
#[derive(Clone, Copy)]
pub(crate) struct Operands<'a> {
    pub(crate) lhs: Operand<'a>,
    pub(crate) rhs: Operand<'a>,
}

impl Operands<'_> {
    /// Both operands as AST-tier literals; see [`Operand::Literal`].
    pub(crate) const LITERALS: Operands<'static> = Operands {
        lhs: Operand::Literal,
        rhs: Operand::Literal,
    };

    /// The descriptors for a two-argument expression-tree node.
    pub(crate) fn of<'b>(lhs: &'b Expression, rhs: &'b Expression) -> Operands<'b> {
        Operands {
            lhs: Operand::Expr(lhs),
            rhs: Operand::Expr(rhs),
        }
    }
}
