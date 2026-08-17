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

//! Go `pkg/planner/util.IsNullRejected` for a WRITTEN predicate: whether it
//! can be TRUE while the named column is NULL.
//!
//! A predicate that cannot proves the column NOT NULL for every surviving row,
//! which is what promotes a nullable UNIQUE key to a candidate key in the
//! `ONLY_FULL_GROUP_BY` rule and what lets an outer join be simplified to an
//! inner one. The distinction is exact, not conservative in the direction that
//! matters: `WHERE a > 3` proves it, `WHERE a <=> NULL` does not, and the
//! recording refuses the second with 1055.
//!
//! # The proof lives elsewhere; this is only the translation
//!
//! Go runs `IsNullRejected` on `expression.Expression`, AFTER the expression
//! rewriter and after `PushDownNot`. `tidb-funcdep` carries that transcreation
//! (delegating in turn to `tidb-expr`'s own port, which holds Go's complete
//! `null_misc_builtins.go` NULL-preserving table). This tier still works on
//! the written `tidb_ast::Expr`, so what remains here is exactly the missing
//! step: the shape translation from written syntax into the operator tree the
//! proof reads, standing in for Go's rewriter. No proof bit is computed here.
//!
//! Two consequences of standing in for the rewriter:
//!
//!  * NEGATION IS PUSHED DOWN during translation ([`translate`]'s `negated`
//!    flag), because Go's proof assumes `PushDownNot` already ran. De Morgan
//!    is observable: `NOT (outer OR inner)` is rejected when the inner
//!    disjunct becomes NULL, while `NOT (outer AND inner)` is not.
//!  * A NODE WITH NO REWRITTEN COUNTERPART HERE BECOMES OPAQUE -- an unnamed
//!    column that is never the nullified one. Go likewise treats an
//!    unclassified builtin as proving nothing, and proving nothing only costs
//!    a refusal this tier already makes, while a wrong proof would ACCEPT a
//!    query TiDB refuses.

use tidb_ast::CiString;
use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_expr::expression::{Column, Constant, Expression, ScalarFunction};

/// The unique id standing for every nullified column. The proof only ever
/// asks whether a column is in the nullified set, so one shared id is enough.
const NULLIFIED: i64 = 1;
/// The unique id standing for every column, subquery, or unmodelled node that
/// is NOT nullified: an unknown value, which proves nothing.
const OPAQUE: i64 = 2;

/// Whether `predicate` rejects every row in which the column at `offset` is
/// NULL, where `resolve` maps a written column path to its scope offset.
///
/// Go `IsNullRejected` for a single nullified column.
pub(crate) fn is_null_rejected(
    predicate: &tidb_ast::Expr,
    offset: usize,
    resolve: &dyn Fn(&[String]) -> Option<usize>,
) -> bool {
    is_null_rejected_by(predicate, &|path| resolve(path) == Some(offset))
}

/// Whether `predicate` rejects a row after every column selected by
/// `nullified` becomes SQL NULL.
///
/// A UNIQUE-key proof nullifies one column; outer-join simplification
/// nullifies a whole child schema. Keeping that distinction in the selector
/// avoids pretending a multi-column child is one synthetic column.
pub(crate) fn is_null_rejected_by(
    predicate: &tidb_ast::Expr,
    nullified: &dyn Fn(&[String]) -> bool,
) -> bool {
    tidb_funcdep::null_reject::is_null_rejected_by(
        &translate(predicate, false, nullified),
        &[NULLIFIED],
    )
}

fn int_type() -> FieldType {
    FieldType::new(FieldTypeCode::LongLong)
}

fn call(name: &str, args: Vec<Expression>) -> Expression {
    Expression::ScalarFunction(ScalarFunction::new(CiString::new(name), int_type(), args))
}

fn constant(value: Datum) -> Expression {
    Expression::Constant(Constant::new(value, int_type()))
}

/// An unknown value: neither NULL nor a definite FALSE.
fn opaque() -> Expression {
    Expression::Column(Column::new(OPAQUE, int_type()))
}

/// Wraps `expr` in Go's `not` builtin when `negated`.
fn negate(expr: Expression, negated: bool) -> Expression {
    if negated {
        call("not", vec![expr])
    } else {
        expr
    }
}

/// The written predicate as the rewritten operator tree Go's proof reads,
/// with any enclosing `NOT` already pushed down (`negated`).
fn translate(
    expr: &tidb_ast::Expr,
    negated: bool,
    nullified: &dyn Fn(&[String]) -> bool,
) -> Expression {
    let sub = |expr: &tidb_ast::Expr| translate(expr, false, nullified);
    match expr {
        tidb_ast::Expr::Paren(inner) => translate(inner, negated, nullified),

        // The target column reads as NULL; any other column is an unknown
        // value, which proves nothing.
        tidb_ast::Expr::Column(path) => negate(
            if nullified(path) {
                Expression::Column(Column::new(NULLIFIED, int_type()))
            } else {
                opaque()
            },
            negated,
        ),
        tidb_ast::Expr::Null => negate(constant(Datum::Null), negated),
        // A written boolean or integer literal is a constant the proof reads
        // directly: `FALSE` / `0` is never TRUE without being NULL.
        tidb_ast::Expr::Bool(value) => negate(constant(Datum::Int(i64::from(*value))), negated),
        tidb_ast::Expr::Int(digits) => negate(
            digits
                .parse::<i64>()
                .map_or_else(|_| opaque(), |value| constant(Datum::Int(value))),
            negated,
        ),

        // De Morgan, which is Go's `PushDownNot`.
        tidb_ast::Expr::Binary(
            op @ (tidb_ast::BinaryOp::LogicAnd | tidb_ast::BinaryOp::LogicOr),
            lhs,
            rhs,
        ) => {
            let is_and = (*op == tidb_ast::BinaryOp::LogicAnd) != negated;
            call(
                if is_and { "and" } else { "or" },
                vec![
                    translate(lhs, negated, nullified),
                    translate(rhs, negated, nullified),
                ],
            )
        }
        tidb_ast::Expr::Binary(op, lhs, rhs) => {
            negate(call(binary_name(*op), vec![sub(lhs), sub(rhs)]), negated)
        }

        tidb_ast::Expr::Unary(tidb_ast::UnaryOp::Not | tidb_ast::UnaryOp::NotKeyword, inner) => {
            translate(inner, !negated, nullified)
        }
        // A unary `+` is notation; the other unary operators are the
        // NULL-preserving builtins Go names.
        tidb_ast::Expr::Unary(tidb_ast::UnaryOp::Plus, inner) => {
            translate(inner, negated, nullified)
        }
        tidb_ast::Expr::Unary(op, inner) => negate(
            call(
                match op {
                    tidb_ast::UnaryOp::BitNeg => "bitneg",
                    _ => "unaryminus",
                },
                vec![sub(inner)],
            ),
            negated,
        ),

        // Go names each SQL test as a builtin: `IS NULL` and `IS UNKNOWN`
        // share `isnull`, and the negated forms are that builtin under `not`.
        // `not(isnull(x))` is the one shape Go's proof special-cases, which is
        // why `x IS NOT NULL` rejects while `x IS NOT TRUE` does not.
        tidb_ast::Expr::Is { expr, target, not } => {
            let name = match target {
                tidb_ast::IsTarget::Null | tidb_ast::IsTarget::Unknown => "isnull",
                tidb_ast::IsTarget::True => "istrue",
                tidb_ast::IsTarget::False => "isfalse",
            };
            negate(call(name, vec![sub(expr)]), *not != negated)
        }

        // Go `proveNullRejectedIn`: `IN` answers NULL for a NULL value, and
        // for a value with an all-NULL list.
        tidb_ast::Expr::In { expr, list, not } => {
            let mut args = vec![sub(expr)];
            args.extend(list.iter().map(sub));
            negate(call("in", args), *not != negated)
        }
        // `a IN (SELECT ...)` compares `a` against the subquery's rows, and a
        // NULL `a` matches nothing and answers NULL. The row set itself is
        // unknown, so it stands in as one opaque list member.
        tidb_ast::Expr::InSubquery { expr, not, .. } => {
            negate(call("in", vec![sub(expr), opaque()]), *not != negated)
        }

        // Go rewrites `BETWEEN` into the conjunction it is defined as.
        tidb_ast::Expr::Between {
            expr,
            low,
            high,
            not,
        } => {
            let range = call(
                "and",
                vec![
                    call("ge", vec![sub(expr), sub(low)]),
                    call("le", vec![sub(expr), sub(high)]),
                ],
            );
            negate(range, *not != negated)
        }

        tidb_ast::Expr::Like {
            expr,
            pattern,
            not,
            ilike,
            escape,
        } => negate(
            call(
                if *ilike { "ilike" } else { "like" },
                vec![
                    sub(expr),
                    sub(pattern),
                    constant(Datum::Int(i64::from(escape.unwrap_or(b'\\')))),
                ],
            ),
            *not != negated,
        ),
        tidb_ast::Expr::Regexp { expr, pattern, not } => negate(
            call("regexp", vec![sub(expr), sub(pattern)]),
            *not != negated,
        ),

        // Every other node is opaque, exactly as Go treats an unlisted
        // builtin: it proves nothing about the nullified column, and a `NOT`
        // over it proves nothing either.
        _ => opaque(),
    }
}

/// The builtin name Go's rewriter gives each written binary operator.
fn binary_name(op: tidb_ast::BinaryOp) -> &'static str {
    match op {
        tidb_ast::BinaryOp::Plus => "plus",
        tidb_ast::BinaryOp::Minus => "minus",
        tidb_ast::BinaryOp::Mul => "mul",
        tidb_ast::BinaryOp::Div => "div",
        tidb_ast::BinaryOp::Mod => "mod",
        tidb_ast::BinaryOp::IntDiv => "intdiv",
        tidb_ast::BinaryOp::BitOr => "bitor",
        tidb_ast::BinaryOp::BitAnd => "bitand",
        tidb_ast::BinaryOp::BitXor => "bitxor",
        tidb_ast::BinaryOp::LeftShift => "leftshift",
        tidb_ast::BinaryOp::RightShift => "rightshift",
        tidb_ast::BinaryOp::Eq => "eq",
        // `a <=> NULL` is the whole point of the NULL-safe operator: it
        // answers TRUE for a NULL operand, so Go's table leaves `nulleq` out
        // of the NULL-preserving set and it proves nothing.
        tidb_ast::BinaryOp::NullEq => "nulleq",
        tidb_ast::BinaryOp::Ge => "ge",
        tidb_ast::BinaryOp::Gt => "gt",
        tidb_ast::BinaryOp::Le => "le",
        tidb_ast::BinaryOp::Lt => "lt",
        tidb_ast::BinaryOp::Ne => "ne",
        tidb_ast::BinaryOp::LogicXor => "xor",
        // Handled before this function is reached.
        tidb_ast::BinaryOp::LogicAnd => "and",
        tidb_ast::BinaryOp::LogicOr => "or",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parses a `WHERE` predicate over a one-column scope named `a`.
    fn rejects(text: &str) -> bool {
        let sql = format!("SELECT 1 FROM t WHERE {text}");
        let statement = tidb_parser::parse(&sql).expect("parses");
        let tidb_ast::Stmt::Query(query) = &statement else {
            panic!("not a query");
        };
        let tidb_ast::QueryStmt::Select(select) = &**query else {
            panic!("not a select");
        };
        let predicate = select.where_clause.as_ref().expect("has a where clause");
        is_null_rejected(predicate, 0, &|path: &[String]| {
            (path.last().map(String::as_str) == Some("a")).then_some(0)
        })
    }

    /// The recording's own boundary, from
    /// `tests/integrationtest/t/planner/funcdep/only_full_group_by.test`:
    /// each accepted statement's `WHERE` proves `a` NOT NULL, and each
    /// refused one does not.
    #[test]
    fn matches_the_recorded_boundary() {
        for predicate in [
            "a IS NOT NULL",
            "NOT (a IS NULL)",
            "a > 3",
            "a = 3",
            "a BETWEEN 3 AND 6",
            "a <> 3",
            "a IN (3,4)",
            "a IN (SELECT b FROM t)",
            "a IS TRUE",
            "(a <> 3) IS TRUE",
            "a IS FALSE",
            "(a <> 3) IS FALSE",
            "a LIKE \"%abc%\"",
        ] {
            assert!(rejects(predicate), "{predicate} should prove a NOT NULL");
        }
        for predicate in ["a<=>NULL", "a IS NOT TRUE", "a IS NULL", "b > 3", "1 = 1"] {
            assert!(!rejects(predicate), "{predicate} should prove nothing");
        }
    }

    /// A conjunction inherits any one side's proof; a disjunction needs both.
    #[test]
    fn combines_over_and_or() {
        assert!(rejects("a > 3 AND b < 1"));
        assert!(rejects("b < 1 AND a > 3"));
        assert!(rejects("a > 3 OR a < 1"));
        assert!(!rejects("a > 3 OR b < 1"));
    }

    /// Go's `PushDownNot` is observable through the translation: De Morgan
    /// turns `NOT (x OR y)` into a conjunction, so one rejected disjunct is
    /// enough, while `NOT (x AND y)` needs both.
    #[test]
    fn pushes_negation_down() {
        assert!(rejects("NOT (a > 3 OR b < 1)"));
        assert!(!rejects("NOT (a > 3 AND b < 1)"));
        assert!(rejects("NOT (a > 3 AND a < 1)"));
        assert!(rejects("NOT NOT (a > 3)"));
        // `NOT (a IS NOT TRUE)` is `a IS TRUE`, which rejects.
        assert!(rejects("NOT (a IS NOT TRUE)"));
        assert!(!rejects("NOT (a IS TRUE)"));
    }
}
