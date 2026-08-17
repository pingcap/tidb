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
//! `ONLY_FULL_GROUP_BY` rule, and what lets an outer join be simplified to an
//! inner one. The distinction is exact, not conservative in the direction that
//! matters: `WHERE a > 3` proves it, `WHERE a <=> NULL` does not, and the
//! recording refuses the second with 1055.
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
//! `a IS TRUE` is accepted. `FALSE IS NOT TRUE` is TRUE, so `a IS NOT TRUE`
//! survives a NULL `a` and rejects nothing, and
//! `WHERE a IS NOT TRUE GROUP BY a,b` stays 1055.
//!
//! # Where the proof itself lives
//!
//! Go's function takes `expression.Expression`, and `tidb-expr` already
//! carries that transcreation in full -- including Go's complete
//! `pkg/planner/util/null_misc_builtins.go` NULL-preserving builtin table, the
//! `nullRejectRejectNullTests` table (`istrue` / `istrue_with_null` /
//! `isfalse`), and Go's nullify-then-constant-fold bridge. Reimplementing that
//! table here would be a second, silently diverging copy of one Go function,
//! so this module delegates to it and supplies only the two shapes the
//! functional-dependency graph asks for. This is where the earlier
//! `tidb_ast::Expr`-typed port under `tidb-executor` moves TO: retargeting it
//! onto the expression tree moves it toward Go, not away from it.

use tidb_expr::expression::{is_null_rejected as prove_null_rejected, Expression};

/// Whether `predicate` rejects every row in which the column with unique id
/// `column_id` is NULL.
///
/// This is the single-column shape: a UNIQUE-key promotion asks about one
/// nullable key member at a time (Go `FDSet.MakeNotNull` fed from
/// `ExtractNotNullFromConds`).
#[must_use]
pub fn is_null_rejected(predicate: &Expression, column_id: i64) -> bool {
    prove_null_rejected(&[column_id], predicate)
}

/// Whether `predicate` rejects a row after EVERY column in `nullified` becomes
/// SQL NULL.
///
/// This is the multi-column shape: outer-join simplification nullifies a whole
/// child schema at once (Go `IsNullRejected(ctx, innerSchema.Columns, expr)`).
/// Keeping it distinct from [`is_null_rejected`] avoids pretending a
/// multi-column child is one synthetic column -- the two are not equivalent,
/// because a predicate may reject the pair while rejecting neither member
/// alone.
#[must_use]
pub fn is_null_rejected_by(predicate: &Expression, nullified: &[i64]) -> bool {
    prove_null_rejected(nullified, predicate)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_ast::CiString;
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};
    use tidb_expr::expression::{Column, Constant, ScalarFunction};

    /// The nullified column `a`.
    const A: i64 = 1;
    /// A second, unrelated column `b`.
    const B: i64 = 2;

    fn int_type() -> FieldType {
        FieldType::new(FieldTypeCode::LongLong)
    }

    fn column(unique_id: i64) -> Expression {
        Expression::Column(Column::new(unique_id, int_type()))
    }

    fn int(value: i64) -> Expression {
        Expression::Constant(Constant::new(Datum::Int(value), int_type()))
    }

    fn text(value: &str) -> Expression {
        Expression::Constant(Constant::new(
            Datum::Bytes(value.as_bytes().to_vec()),
            FieldType::new(FieldTypeCode::VarString),
        ))
    }

    fn null() -> Expression {
        Expression::Constant(Constant::new(Datum::Null, int_type()))
    }

    /// Go's rewriter names every operator as a builtin; the proof reads the
    /// name and the arguments, exactly as Go's does.
    fn call(name: &str, args: Vec<Expression>) -> Expression {
        Expression::ScalarFunction(ScalarFunction::new(CiString::new(name), int_type(), args))
    }

    /// Does the predicate prove `a` NOT NULL?
    fn rejects(predicate: &Expression) -> bool {
        is_null_rejected(predicate, A)
    }

    /// The recording's own boundary, from
    /// `tests/integrationtest/t/planner/funcdep/only_full_group_by.test`:
    /// each accepted statement's `WHERE` proves `a` NOT NULL, and each refused
    /// one does not. The fixtures are the rewritten expression trees Go's
    /// optimizer hands `IsNullRejected`, not the written syntax.
    #[test]
    fn matches_the_recorded_boundary() {
        let accepted = [
            // `a IS NOT NULL`, which is also the rewrite of `NOT (a IS NULL)`.
            call("not", vec![call("isnull", vec![column(A)])]),
            // `a > 3`
            call("gt", vec![column(A), int(3)]),
            // `a = 3`
            call("eq", vec![column(A), int(3)]),
            // `a BETWEEN 3 AND 6`
            call(
                "and",
                vec![
                    call("ge", vec![column(A), int(3)]),
                    call("le", vec![column(A), int(6)]),
                ],
            ),
            // `a <> 3`
            call("ne", vec![column(A), int(3)]),
            // `a IN (3,4)`
            call("in", vec![column(A), int(3), int(4)]),
            // `a IS TRUE`
            call("istrue", vec![column(A)]),
            // `(a <> 3) IS TRUE`
            call("istrue", vec![call("ne", vec![column(A), int(3)])]),
            // `a IS FALSE`
            call("isfalse", vec![column(A)]),
            // `(a <> 3) IS FALSE`
            call("isfalse", vec![call("ne", vec![column(A), int(3)])]),
            // `a LIKE "%abc%"`, whose third argument is the escape character.
            call(
                "like",
                vec![column(A), text("%abc%"), int(i64::from(b'\\'))],
            ),
        ];
        for predicate in &accepted {
            assert!(rejects(predicate), "{predicate:?} should prove a NOT NULL");
        }

        let refused = [
            // `a <=> NULL`: the NULL-safe operator answers TRUE for a NULL
            // operand, which is the whole point of it.
            call("nulleq", vec![column(A), null()]),
            // `a IS NOT TRUE`: NULL IS NOT TRUE is TRUE, so nothing is rejected.
            call("not", vec![call("istrue", vec![column(A)])]),
            // `a IS NULL` turns NULL into TRUE.
            call("isnull", vec![column(A)]),
            // A predicate over another column says nothing about `a`.
            call("gt", vec![column(B), int(3)]),
            // A predicate over no column at all.
            call("eq", vec![int(1), int(1)]),
        ];
        for predicate in &refused {
            assert!(!rejects(predicate), "{predicate:?} should prove nothing");
        }
    }

    /// A conjunction inherits any one side's proof; a disjunction needs both.
    #[test]
    fn combines_over_and_or() {
        let a_gt_3 = call("gt", vec![column(A), int(3)]);
        let a_lt_1 = call("lt", vec![column(A), int(1)]);
        let b_lt_1 = call("lt", vec![column(B), int(1)]);

        assert!(rejects(&call("and", vec![a_gt_3.clone(), b_lt_1.clone()])));
        assert!(rejects(&call("and", vec![b_lt_1.clone(), a_gt_3.clone()])));
        assert!(rejects(&call("or", vec![a_gt_3.clone(), a_lt_1])));
        assert!(!rejects(&call("or", vec![a_gt_3, b_lt_1])));
    }

    /// The multi-column shape an outer join uses: `a` and `b` are the whole
    /// nullified child schema, and only the predicate reading one of them
    /// rejects.
    #[test]
    fn rejects_over_a_whole_nullified_schema() {
        let schema = [A, B];
        assert!(is_null_rejected_by(
            &call("gt", vec![column(B), int(3)]),
            &schema
        ));
        assert!(!is_null_rejected_by(
            &call("gt", vec![column(B), int(3)]),
            &[A]
        ));
        assert!(!is_null_rejected_by(
            &call("isnull", vec![column(B)]),
            &schema
        ));
    }
}
