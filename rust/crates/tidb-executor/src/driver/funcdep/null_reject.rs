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

//! Go `pkg/planner/util.IsNullRejected`: whether a predicate can be TRUE while
//! the named column is NULL.
//!
//! A predicate that cannot proves the column NOT NULL for every surviving row,
//! which is what promotes a nullable UNIQUE key to a candidate key in the
//! `ONLY_FULL_GROUP_BY` rule. The distinction is exact, not conservative in
//! the direction that matters: `WHERE a > 3` proves it, `WHERE a <=> NULL`
//! does not, and the recording refuses the second with 1055.
//!
//! Go proves it with two bits per subexpression, over the column substituted
//! by SQL NULL:
//!
//!  * `must_null` -- the subexpression evaluates to NULL.
//!  * `non_true`  -- the subexpression cannot evaluate to TRUE.
//!
//! `non_true` at the root IS null-rejection. The two bits are needed
//! separately because the SQL tests (`IS TRUE`, `IS FALSE`) turn NULL into a
//! definite FALSE: they are `non_true` without being `must_null`, so a `NOT`
//! above them does NOT reject -- exactly why `a IS NOT TRUE` is refused while
//! `a IS TRUE` is accepted.

/// The two proof bits for one subexpression, over the nullified column.
#[derive(Clone, Copy, Default)]
struct Proof {
    /// The subexpression cannot be TRUE.
    non_true: bool,
    /// The subexpression is NULL.
    must_null: bool,
}

impl Proof {
    /// A subexpression that is NULL is also never TRUE.
    fn null() -> Self {
        Proof {
            non_true: true,
            must_null: true,
        }
    }
}

/// Whether `predicate` rejects every row in which the column at `offset` is
/// NULL, where `resolve` maps a written column path to its scope offset.
///
/// Go `IsNullRejected`, with Go's `PushDownNot` normalization not needed here:
/// the written AST still carries `IS NOT NULL` and `IS NOT TRUE` as their own
/// nodes, which [`prove`] classifies directly rather than after a rewrite.
pub(crate) fn is_null_rejected(
    predicate: &tidb_ast::Expr,
    offset: usize,
    resolve: &dyn Fn(&[String]) -> Option<usize>,
) -> bool {
    prove(predicate, offset, resolve).non_true
}

/// The two proof bits for `expr` with the target column read as NULL.
fn prove(
    expr: &tidb_ast::Expr,
    offset: usize,
    resolve: &dyn Fn(&[String]) -> Option<usize>,
) -> Proof {
    match expr {
        tidb_ast::Expr::Paren(inner) => prove(inner, offset, resolve),
        // The target column reads as NULL; any other column is an unknown
        // value, which proves nothing.
        tidb_ast::Expr::Column(path) => {
            if resolve(path) == Some(offset) {
                Proof::null()
            } else {
                Proof::default()
            }
        }
        tidb_ast::Expr::Null => Proof::null(),
        // A written FALSE / 0 is never TRUE but is not NULL.
        tidb_ast::Expr::Bool(false) => Proof {
            non_true: true,
            must_null: false,
        },
        tidb_ast::Expr::Int(digits) if digits.chars().all(|c| c == '0') => Proof {
            non_true: true,
            must_null: false,
        },

        tidb_ast::Expr::Binary(op, lhs, rhs) => {
            let left = prove(lhs, offset, resolve);
            let right = prove(rhs, offset, resolve);
            match op {
                // AND is TRUE only if both are, so one non-TRUE side is
                // enough; it is NULL only if both are.
                tidb_ast::BinaryOp::LogicAnd => Proof {
                    non_true: left.non_true || right.non_true,
                    must_null: left.must_null && right.must_null,
                },
                tidb_ast::BinaryOp::LogicOr => Proof {
                    non_true: left.non_true && right.non_true,
                    must_null: left.must_null && right.must_null,
                },
                // `a <=> NULL` is the whole point of the NULL-safe operator:
                // it answers TRUE for a NULL operand, so it proves nothing.
                tidb_ast::BinaryOp::NullEq => Proof::default(),
                // Every other operator here propagates NULL.
                _ if left.must_null || right.must_null => Proof::null(),
                _ => Proof::default(),
            }
        }

        // `NOT (x IS NULL)` is FALSE when x is NULL -- never TRUE, but not
        // NULL either. A general `NOT` needs its child to be NULL, since
        // `NOT FALSE` is TRUE.
        tidb_ast::Expr::Unary(tidb_ast::UnaryOp::Not | tidb_ast::UnaryOp::NotKeyword, inner) => {
            if let tidb_ast::Expr::Is {
                expr,
                target: tidb_ast::IsTarget::Null,
                not: false,
            } = strip_parens(inner)
            {
                return Proof {
                    non_true: prove(expr, offset, resolve).must_null,
                    must_null: false,
                };
            }
            let child = prove(inner, offset, resolve);
            Proof {
                non_true: child.must_null,
                must_null: child.must_null,
            }
        }
        tidb_ast::Expr::Unary(_, inner) => {
            let child = prove(inner, offset, resolve);
            if child.must_null {
                Proof::null()
            } else {
                Proof::default()
            }
        }

        // `x IS NULL` turns NULL into TRUE, so it proves nothing; `x IS NOT
        // NULL` is FALSE for a NULL `x` -- never TRUE, but not NULL either,
        // so a `NOT` above it would prove nothing in turn.
        //
        // Go `nullRejectRejectNullTests`: `IS TRUE` / `IS FALSE` likewise
        // answer a definite FALSE for a NULL input, while `IS UNKNOWN` keeps
        // NULL. `x IS NOT TRUE` is the case the recording pins: FALSE IS NOT
        // TRUE is TRUE, so the predicate survives a NULL `x` and rejects
        // nothing, and `WHERE a IS NOT TRUE GROUP BY a,b` stays 1055.
        tidb_ast::Expr::Is { expr, target, not } => {
            let child = prove(expr, offset, resolve);
            match (target, not) {
                (tidb_ast::IsTarget::Null, false) => Proof::default(),
                (tidb_ast::IsTarget::Null, true)
                | (tidb_ast::IsTarget::True | tidb_ast::IsTarget::False, false) => Proof {
                    non_true: child.must_null,
                    must_null: false,
                },
                // `IS UNKNOWN` is TRUE for a NULL input; `IS NOT UNKNOWN` is
                // FALSE for it, which is non-TRUE but not NULL.
                (tidb_ast::IsTarget::Unknown, false) => Proof::default(),
                (tidb_ast::IsTarget::Unknown, true) => Proof {
                    non_true: child.must_null,
                    must_null: false,
                },
                (tidb_ast::IsTarget::True | tidb_ast::IsTarget::False, true) => Proof::default(),
            }
        }

        // Go `proveNullRejectedIn`: `IN` answers NULL for a NULL value, and
        // for a value with an all-NULL list.
        tidb_ast::Expr::In { expr, list, .. } => {
            if prove(expr, offset, resolve).must_null {
                return Proof::null();
            }
            if list
                .iter()
                .all(|item| prove(item, offset, resolve).must_null)
            {
                return Proof::null();
            }
            Proof::default()
        }
        // `a IN (SELECT ...)` compares `a` against the subquery's rows, and a
        // NULL `a` matches nothing and answers NULL.
        tidb_ast::Expr::InSubquery { expr, .. } => {
            if prove(expr, offset, resolve).must_null {
                Proof::null()
            } else {
                Proof::default()
            }
        }

        // `BETWEEN` is `x >= lo AND x <= hi`, so it is NULL-preserving in
        // every operand.
        tidb_ast::Expr::Between {
            expr, low, high, ..
        } => {
            if [expr, low, high]
                .into_iter()
                .any(|part| prove(part, offset, resolve).must_null)
            {
                Proof::null()
            } else {
                Proof::default()
            }
        }

        // `LIKE` / `REGEXP` answer NULL for a NULL operand.
        tidb_ast::Expr::Like { expr, pattern, .. } => {
            if prove(expr, offset, resolve).must_null || prove(pattern, offset, resolve).must_null {
                Proof::null()
            } else {
                Proof::default()
            }
        }
        tidb_ast::Expr::Regexp { expr, pattern, .. } => {
            if prove(expr, offset, resolve).must_null || prove(pattern, offset, resolve).must_null {
                Proof::null()
            } else {
                Proof::default()
            }
        }

        // Go classifies each builtin explicitly and treats an unlisted one as
        // opaque. An unlisted function here is likewise opaque: proving
        // nothing only costs a refusal this tier already makes, while a wrong
        // proof would ACCEPT a query TiDB refuses.
        _ => Proof::default(),
    }
}

/// Parentheses are notation, not an operator.
fn strip_parens(expr: &tidb_ast::Expr) -> &tidb_ast::Expr {
    match expr {
        tidb_ast::Expr::Paren(inner) => strip_parens(inner),
        other => other,
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
}
