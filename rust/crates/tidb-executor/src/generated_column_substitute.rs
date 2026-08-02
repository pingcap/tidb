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

//! Rewriting a predicate's expression into the indexed generated column that
//! already stores it, so the index can serve the query.
//!
//! Mirrors Go `pkg/planner/core/rule_generate_column_substitute.go`
//! (`GcSubstituter`). `WHERE a+1 = 3` over a table carrying
//! `c AS (a+1)` with `KEY(c)` -- or the hidden column an expression index
//! `KEY((a+1))` was rewritten into -- becomes `WHERE c = 3`, which the ranger
//! can turn into `range:[3,3]` on that index. The substitution changes which
//! PLAN runs, never which rows come back.
//!
//! # One namespace, one equality
//!
//! Go compares two `expression.Expression`s with `Equal`, and gets that for
//! free because both sides are already columns of `ds.Schema()`. This tier
//! cannot: a [`GeneratedColumn::expr`](crate::generated_column::
//! GeneratedColumn::expr)'s `Column` nodes index that expression's OWN
//! dependency name list (deliberately -- so no `ALTER TABLE` that reorders
//! the table's columns can re-point it), while a `WHERE` condition's columns
//! are AST names in the query's own namespace. Comparing those two directly
//! would need a mapping between namespaces, and a mapping is a second
//! equality definition to keep honest across pruning and derived tables.
//!
//! There is no mapping here, because the two expressions never have to meet
//! in a positional namespace at all. The access-path choice this rule feeds
//! ([`crate::access_cost::enumerate_paths`]) consumes the `WHERE` as an
//! `tidb_ast::Expr` with column NAMES, and a generated column already carries
//! its expression as `expr_text` -- the canonically RESTORED text of the same
//! AST, which is what `SHOW CREATE TABLE` prints back. So both sides are
//! reduced to one canonical string by one function
//! ([`crate::generated_column::generated_restore_flags`], the flag set BOTH
//! the column path and the expression-index path already store their text
//! under), and equality is string equality on it. One definition, computed on
//! the table side at DDL time and on the query side here.
//!
//! `WITHOUT_TABLE_NAME` is what makes the query side land in the same
//! namespace: `WHERE t.a+1 = 3` and `WHERE a+1 = 3` both restore to
//! `` `a` + 1 ``, which is exactly what the column stored.
//!
//! # It narrows the source; it never replaces the filter
//!
//! Go rewrites the condition in the plan. This does not: the rewritten copy
//! is handed to the range builder alone, and the ORIGINAL `WHERE` stays in
//! the pipeline above the source ([`crate::driver::access`]'s contract). That
//! is not a shortcut -- a substituted reference names a hidden column that is
//! not in the source's output schema at all, so it could not be evaluated up
//! there -- and it makes the row-identity claim structural: the only way a
//! substitution could change an answer is by producing a range that is too
//! NARROW, never by evaluating anything differently.
//!
//! # What is NOT substituted, and why each is Go's own rule
//!
//! * A STORED generated column. Go's `collectGenerateColumn` collects only
//!   virtual ones ("we can't get their expressions directly").
//! * A generation expression that reads no column at all. Go skips these
//!   because replacing a literal with a generated column is not neutral
//!   across an outer join, where null-augmentation turns the column NULL
//!   while the literal stays constant.
//! * A column whose declared type is not
//!   [`FieldType::partial_equal`](tidb_datatype::FieldType::partial_equal) to
//!   its expression's type. This is the gate the `float(24)`/`float(25)` half
//!   of `explain_generate_column_substitute` exists to pin: `c0 float(25)`
//!   IS a double, so `c1 double AS (c0)` substitutes, while `c0 float(24)`
//!   is a float and the same column does not.
//! * An invisible index's column: it is not in
//!   [`KvTable::plan_indexes`](crate::kv_table::KvTable::plan_indexes), so it
//!   contributes no candidate, exactly as Go never plans one.
//! * Anything outside a comparison's operand position. Go walks `=`, `<`,
//!   `<=`, `>`, `>=`, `IN` and `LIKE`, recursing through `AND`/`OR`/`NOT`,
//!   and substitutes nowhere else.
//!
//! # NOT MODELLED
//!
//! A DECLARED generated column (`c BIGINT AS (a+1)` with `KEY(c)`) does not
//! substitute yet, and the cause is upstream of this rule: this tier's
//! `CREATE TABLE` does not normalize a declared numeric column's type, so
//! `BIGINT` is stored as `flen: -1, charset: utf8mb4` while the same
//! expression infers `flen: 20, charset: binary`, and the `PartialEqual` gate
//! refuses the pair -- correctly, given the two types it is handed. Go fills
//! those in at DDL time (`setCharsetCollationFlenDecimal`). An expression
//! index is unaffected because its hidden column takes its type FROM the
//! expression, so the two agree by construction. See the `#[ignore]`d
//! `a_declared_generated_column_offers_the_same_key`, which asserts the Go
//! answer and turns green the day the type is normalized.
//!
//! Go also substitutes in the projection, `ORDER BY` and `GROUP BY`, which is
//! what lets an index scan supply an ordering (`SELECT a+1 FROM t ORDER BY
//! a+1` reads `IndexFullScan ... keep order:true`, captured). Those wins
//! depend on an order property this tier's path choice does not offer an
//! index path for, so porting the rewrite alone would not change the plan.
//! The predicate half above is the half whose wins are order-free.

use tidb_ast::Expr;

use crate::generated_column::generated_restore_flags;
use crate::kv_table::KvTable;

/// The indexed generated columns of one table, keyed by the canonical text of
/// the expression each one stores.
///
/// Go's `ExprColumnMap`, with the key reduced to the canonical string this
/// module's doc explains rather than an `expression.Expression`.
#[derive(Debug, Default)]
pub struct SubstitutionMap {
    /// `(canonical expression text, the column that stores it)`.
    ///
    /// A `Vec` rather than a map: a table's indexed generated columns are a
    /// handful, and Go's own iteration order over its map is unspecified, so
    /// nothing may depend on ordering beyond first-match.
    candidates: Vec<(String, String)>,
}

impl SubstitutionMap {
    /// Collects every candidate `table` offers: Go's `collectGenerateColumn`.
    #[must_use]
    pub fn collect(table: &KvTable) -> Self {
        let mut candidates: Vec<(String, String)> = Vec::new();
        for index in table.plan_indexes() {
            for offset in &index.column_offsets {
                let Some(column) = table.columns.get(*offset) else {
                    continue;
                };
                let Some(generated) = &column.generated else {
                    continue;
                };
                // Go collects VIRTUAL generated columns only.
                if generated.stored {
                    continue;
                }
                // Go: `len(expression.ExtractColumns(col.VirtualExpr)) == 0`
                // is skipped. `dependencies` is this tier's record of the
                // same set, built by the one resolver walk.
                if generated.dependencies.is_empty() {
                    continue;
                }
                let Some(expression_type) = generated.expr.static_type() else {
                    continue;
                };
                // Go's `col.GetType().PartialEqual(col.VirtualExpr.GetType(),
                // EnableUnsafeSubstitute)`. The session switch defaults off
                // and this tier has no setter for it, so the safe form is the
                // only one reachable -- stated as a constant rather than
                // threaded as a parameter no caller could vary.
                if !column.field_type.partial_equal(expression_type, false) {
                    continue;
                }
                let key = canonical_key(&generated.expr_text);
                if candidates.iter().any(|(text, _)| *text == key) {
                    continue;
                }
                candidates.push((key, column.name.clone()));
            }
        }
        SubstitutionMap { candidates }
    }

    /// Whether the table offers nothing to substitute, which lets the caller
    /// skip the walk entirely (Go's `len(exprToColumn) == 0` early return).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.candidates.is_empty()
    }

    /// The column storing `expr`, if one does.
    fn column_for(&self, expr: &Expr) -> Option<&str> {
        // Parentheses are punctuation, not structure: Go's expression builder
        // unwraps `ast.ParenthesesExpr` outright, so `(a+1) = 3` and
        // `a+1 = 3` reach `Equal` as the same expression. Peeling here is
        // what puts them in the same canonical text.
        let expr = peel(expr);
        // Only a computed expression can be stored by a generated column: a
        // bare column reference or a literal is never a candidate's key, and
        // restoring one to compare would be wasted work on every operand.
        if !is_computed(expr) {
            return None;
        }
        let text = expr.restore_with_flags(generated_restore_flags());
        self.candidates
            .iter()
            .find(|(candidate, _)| *candidate == text)
            .map(|(_, column)| column.as_str())
    }

    /// Rewrites `condition` where a comparison operand is an expression an
    /// indexed generated column stores, returning `None` when nothing
    /// matched.
    ///
    /// Go's `substituteExpression`, over the same operator set.
    #[must_use]
    pub fn substitute_condition(&self, condition: &Expr) -> Option<Expr> {
        if self.is_empty() {
            return None;
        }
        self.rewrite(condition)
    }

    /// `rewrite` returns `None` for "unchanged", so an untouched subtree is
    /// never cloned and a caller can tell a real substitution from a copy.
    fn rewrite(&self, condition: &Expr) -> Option<Expr> {
        match condition {
            // A parenthesized predicate is the same predicate. It is rebuilt
            // WITH its parentheses rather than peeled, so the rewritten copy
            // reattaches to its parent at the precedence it was written at.
            Expr::Paren(inner) => self
                .rewrite(inner)
                .map(|inner| Expr::Paren(Box::new(inner))),
            // Go recurses through the logical connectives and substitutes in
            // neither operand of one directly.
            Expr::Binary(
                op @ (tidb_ast::BinaryOp::LogicAnd | tidb_ast::BinaryOp::LogicOr),
                l,
                r,
            ) => {
                let (left, right) = (self.rewrite(l), self.rewrite(r));
                (left.is_some() || right.is_some()).then(|| {
                    Expr::Binary(
                        *op,
                        Box::new(left.unwrap_or_else(|| (**l).clone())),
                        Box::new(right.unwrap_or_else(|| (**r).clone())),
                    )
                })
            }
            // Go matches `ast.UnaryNot`, the ONE function both spellings of
            // the negation build: `!x` and `NOT x` differ in precedence at
            // parse time and in nothing afterwards.
            Expr::Unary(op @ (tidb_ast::UnaryOp::Not | tidb_ast::UnaryOp::NotKeyword), inner) => {
                self.rewrite(inner)
                    .map(|inner| Expr::Unary(*op, Box::new(inner)))
            }
            // Go's `ast.EQ, ast.LT, ast.LE, ast.GT, ast.GE`: both operands are
            // offered, so `3 = a+1` substitutes exactly as `a+1 = 3` does.
            Expr::Binary(
                op @ (tidb_ast::BinaryOp::Eq
                | tidb_ast::BinaryOp::Lt
                | tidb_ast::BinaryOp::Le
                | tidb_ast::BinaryOp::Gt
                | tidb_ast::BinaryOp::Ge),
                l,
                r,
            ) => {
                let (left, right) = (self.substituted(l), self.substituted(r));
                (left.is_some() || right.is_some()).then(|| {
                    Expr::Binary(
                        *op,
                        Box::new(left.unwrap_or_else(|| (**l).clone())),
                        Box::new(right.unwrap_or_else(|| (**r).clone())),
                    )
                })
            }
            // Go's `ast.In` and `ast.Like` substitute the TESTED expression
            // only, never a list element or a pattern.
            Expr::In { expr, list, not } => self.substituted(expr).map(|expr| Expr::In {
                expr: Box::new(expr),
                list: list.clone(),
                not: *not,
            }),
            Expr::Like {
                expr,
                pattern,
                not,
                ilike,
                escape,
            } => self.substituted(expr).map(|expr| Expr::Like {
                expr: Box::new(expr),
                pattern: pattern.clone(),
                not: *not,
                ilike: *ilike,
                escape: *escape,
            }),
            _ => None,
        }
    }

    /// One operand, replaced by its column reference when a candidate stores
    /// it: Go's `tryToSubstituteExpr`.
    fn substituted(&self, operand: &Expr) -> Option<Expr> {
        self.column_for(operand)
            .map(|column| Expr::Column(vec![column.to_owned()]))
    }
}

/// The canonical key for an expression a generated column stores.
///
/// `expr_text` is the expression as the DDL WROTE it, parentheses included:
/// `c BIGINT AS ((a+1))` stores `` (`a` + 1) `` and the expression index
/// `KEY((a+1))` stores `` `a` + 1 ``, because one spelling was written inside
/// a second pair of parentheses and the other was not. That difference is
/// punctuation, and Go never sees it -- its expression builder unwraps
/// `ast.ParenthesesExpr`, so both reach `Equal` as the same `plus(a, 1)`.
///
/// Re-parsing is what removes it here, and it is always possible: `expr_text`
/// is restored SQL by construction, which is the same reason `SHOW CREATE
/// TABLE`'s output can be fed back to the parser. A text that somehow does
/// not parse keeps its raw form rather than being guessed at -- the effect is
/// a candidate that matches nothing, never one that matches wrongly.
fn canonical_key(expr_text: &str) -> String {
    let Ok(tidb_ast::Stmt::Query(query)) = tidb_parser::parse(&format!("select {expr_text}"))
    else {
        return expr_text.to_owned();
    };
    let tidb_ast::QueryStmt::Select(select) = &*query else {
        return expr_text.to_owned();
    };
    match select.fields.fields().first() {
        Some(tidb_ast::SelectField::Expr { expr, .. }) => {
            peel(expr).restore_with_flags(generated_restore_flags())
        }
        _ => expr_text.to_owned(),
    }
}

/// An expression with its parentheses removed, which Go's expression builder
/// does for every `ast.ParenthesesExpr` before anything compares it.
fn peel(expr: &Expr) -> &Expr {
    let mut expr = expr;
    while let Expr::Paren(inner) = expr {
        expr = inner;
    }
    expr
}

/// Whether `expr` computes a value rather than naming or spelling one.
///
/// Go reaches the same set by construction: `collectGenerateColumn` keys its
/// map on a `VirtualExpr` that passed `ExtractColumns != 0`, and a candidate
/// that is a bare `expression.Column` cannot exist because
/// `BuildHiddenColumnInfo` refuses `KEY((a))` outright (3762) and a
/// `c AS (a)` column is compared by `Equal` against an operand that would
/// have to be the very same column.
fn is_computed(expr: &Expr) -> bool {
    !matches!(
        expr,
        Expr::Column(_)
            | Expr::Int(_)
            | Expr::Decimal(_)
            | Expr::Float(_)
            | Expr::Hex(_)
            | Expr::Bit(_)
            | Expr::String(_)
            | Expr::RawString(_)
            | Expr::Null
            | Expr::ParamMarker { .. }
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_ast::BinaryOp;

    /// Parses one expression out of a `SELECT ... WHERE <expr>`.
    fn condition(sql: &str) -> Expr {
        let parsed = tidb_parser::parse(&format!("select 1 from t where {sql}"))
            .expect("the test condition parses");
        let tidb_ast::Stmt::Query(query) = parsed else {
            panic!("expected a query");
        };
        let tidb_ast::QueryStmt::Select(select) = &*query else {
            panic!("expected a select");
        };
        select
            .where_clause
            .clone()
            .expect("the test condition has a where clause")
    }

    /// A map built by hand, so the walk can be tested without a table.
    fn map(candidates: &[(&str, &str)]) -> SubstitutionMap {
        SubstitutionMap {
            candidates: candidates
                .iter()
                .map(|(text, column)| ((*text).to_owned(), (*column).to_owned()))
                .collect(),
        }
    }

    /// The canonical text a table stores for `a+1`, spelled the way both DDL
    /// paths restore it.
    const A_PLUS_ONE: &str = "`a` + 1";

    #[test]
    fn a_matching_operand_becomes_the_column_that_stores_it() {
        let map = map(&[(A_PLUS_ONE, "c")]);
        let substituted = map
            .substitute_condition(&condition("a+1 = 3"))
            .expect("a+1 is stored by c");
        let Expr::Binary(BinaryOp::Eq, left, _) = &substituted else {
            panic!("the comparison survives the rewrite: {substituted:?}");
        };
        assert_eq!(**left, Expr::Column(vec!["c".to_owned()]));
    }

    /// The accept-control: a near miss must NOT substitute, or the ranger
    /// would build a range for a value the index never stored.
    #[test]
    fn a_near_miss_expression_is_left_alone() {
        let map = map(&[(A_PLUS_ONE, "c")]);
        assert!(map.substitute_condition(&condition("a+2 = 3")).is_none());
        assert!(map.substitute_condition(&condition("b+1 = 3")).is_none());
        // Go's `Equal` is not commutative-aware either: `1+a` is a different
        // expression from `a+1` and neither side substitutes it.
        assert!(map.substitute_condition(&condition("1+a = 3")).is_none());
    }

    /// The query side may spell the column qualified; `WITHOUT_TABLE_NAME` is
    /// what puts both spellings in the table's own namespace.
    #[test]
    fn a_qualified_reference_restores_into_the_same_namespace() {
        let map = map(&[(A_PLUS_ONE, "c")]);
        assert!(map.substitute_condition(&condition("t.a+1 = 3")).is_some());
        assert!(map
            .substitute_condition(&condition("test.t.a+1 = 3"))
            .is_some());
    }

    /// Go offers BOTH operands of a comparison.
    #[test]
    fn either_side_of_a_comparison_substitutes() {
        let map = map(&[(A_PLUS_ONE, "c")]);
        let substituted = map
            .substitute_condition(&condition("3 = a+1"))
            .expect("the right operand is offered too");
        let Expr::Binary(BinaryOp::Eq, _, right) = &substituted else {
            panic!("the comparison survives the rewrite: {substituted:?}");
        };
        assert_eq!(**right, Expr::Column(vec!["c".to_owned()]));
    }

    /// The logical connectives are walked through, and a conjunct that did
    /// not match is carried over unchanged.
    #[test]
    fn a_conjunction_substitutes_the_matching_half_only() {
        let map = map(&[(A_PLUS_ONE, "c")]);
        let substituted = map
            .substitute_condition(&condition("a+1 = 3 and b = 7"))
            .expect("the left conjunct matches");
        assert_eq!(
            substituted.restore_with_flags(generated_restore_flags()),
            "`c` = 3 AND `b` = 7"
        );
        let substituted = map
            .substitute_condition(&condition("not (a+1 = 3)"))
            .expect("NOT is walked through");
        assert_eq!(
            substituted.restore_with_flags(generated_restore_flags()),
            "NOT (`c` = 3)"
        );
    }

    /// Parentheses are punctuation. Go's expression builder unwraps them
    /// before `Equal` ever runs, so a written-out `(a+1)` has to match the
    /// same candidate the bare `a+1` matches.
    #[test]
    fn parentheses_around_an_operand_do_not_hide_it() {
        let map = map(&[(A_PLUS_ONE, "c")]);
        for written in ["(a+1) = 3", "((a+1)) = 3", "(a+1) in (1, 2)"] {
            assert!(
                map.substitute_condition(&condition(written)).is_some(),
                "{written} names the same expression as `a` + 1"
            );
        }
    }

    /// `IN` and `LIKE` substitute the tested expression, never the list or
    /// the pattern.
    #[test]
    fn in_and_like_substitute_the_tested_expression() {
        let map = map(&[(A_PLUS_ONE, "c"), ("md5(`b`)", "m")]);
        let substituted = map
            .substitute_condition(&condition("a+1 in (1, 2, 3)"))
            .expect("IN's tested expression is offered");
        assert_eq!(
            substituted.restore_with_flags(generated_restore_flags()),
            "`c` IN (1,2,3)"
        );
        let substituted = map
            .substitute_condition(&condition("md5(b) like 'ab%'"))
            .expect("LIKE's tested expression is offered");
        assert_eq!(
            substituted.restore_with_flags(generated_restore_flags()),
            "`m` LIKE _utf8mb4'ab%'"
        );
    }

    /// Nothing outside a comparison operand is touched: Go walks no other
    /// position, and a substitution there would name a column the source
    /// cannot produce.
    #[test]
    fn no_other_position_is_rewritten() {
        let map = map(&[(A_PLUS_ONE, "c")]);
        // An operand of an arithmetic operator, not of a comparison.
        assert!(map
            .substitute_condition(&condition("(a+1) + 2 = 5"))
            .is_none());
        // A function argument.
        assert!(map
            .substitute_condition(&condition("abs(a+1) = 5"))
            .is_none());
    }

    /// The candidates a real table offers, collected through the real DDL --
    /// the only way to prove the two sides agree, since the table side's text
    /// is written by `CREATE TABLE` and never by this module.
    fn candidates_of(create: &str) -> Vec<(String, String)> {
        let mut catalog = crate::driver::Catalog::default();
        crate::ddl::run_create_table_on(create, &mut catalog).expect("the table is created");
        let Some(crate::TableEntry::Kv(table)) = catalog.get_table_for_test("t") else {
            panic!("expected a kv table");
        };
        SubstitutionMap::collect(table).candidates
    }

    /// An expression index's hidden column is a candidate under the
    /// expression it indexes.
    #[test]
    fn an_expression_index_offers_its_expression() {
        let candidates = candidates_of("CREATE TABLE t (a INT, b INT, KEY k ((a+1)))");
        assert_eq!(
            candidates
                .iter()
                .map(|(text, _)| text.as_str())
                .collect::<Vec<_>>(),
            vec![A_PLUS_ONE],
            "the key is the expression, canonically restored"
        );
    }

    /// A DECLARED generated column offers the same key as the expression
    /// index does -- Go substitutes both, and the recording's
    /// `desc select * from t where a+1=3` over `c BIGINT AS ((a+1))` with
    /// `KEY idx_c (c)` reads `IndexRangeScan ... index:idx_c(c)`.
    ///
    /// It does not here, and the cause is NOT in this rule: a declared
    /// column's `FieldType` is not normalized by this tier's `CREATE TABLE`.
    /// `BIGINT` is stored as `flen: -1, charset: utf8mb4, collate:
    /// utf8mb4_bin`, while the same expression's inferred type is `flen: 20,
    /// charset: binary, collate: binary` -- so the `PartialEqual` gate that
    /// exists to REFUSE `float(24)` vs `double` refuses this too, correctly
    /// given the two types it was handed. Go's `setCharsetCollationFlenDecimal`
    /// fills a numeric column's charset, collation and flen at DDL time, and
    /// until that runs here the two sides cannot agree.
    ///
    /// The parenthesis half of the same case IS fixed and is asserted:
    /// `AS ((a+1))` stores `` (`a` + 1) `` and still keys as `` `a` + 1 ``.
    #[test]
    #[ignore = "blocked on CREATE TABLE not normalizing a declared numeric column's \
                charset/collation/flen (Go setCharsetCollationFlenDecimal)"]
    fn a_declared_generated_column_offers_the_same_key() {
        let candidates =
            candidates_of("CREATE TABLE t (a INT, c BIGINT AS ((a+1)) VIRTUAL, KEY idx_c (c))");
        assert_eq!(
            candidates,
            vec![(A_PLUS_ONE.to_owned(), "c".to_owned())],
            "parentheses are punctuation on the table side too"
        );
    }

    /// The half of the case above that this rule DOES own: the table side's
    /// parentheses are gone from the key, whatever the type gate then does
    /// with the candidate.
    #[test]
    fn a_declared_columns_ddl_parentheses_leave_the_key() {
        assert_eq!(canonical_key("(`a` + 1)"), A_PLUS_ONE);
        assert_eq!(canonical_key("((`a` + 1))"), A_PLUS_ONE);
        assert_eq!(canonical_key(A_PLUS_ONE), A_PLUS_ONE);
        // A text that does not parse keeps its raw form: a candidate that
        // matches nothing, never one that matches wrongly.
        assert_eq!(canonical_key("not an expression ("), "not an expression (");
    }

    /// A STORED column and an unindexed one are both collected by nobody.
    #[test]
    fn only_an_indexed_virtual_column_is_a_candidate() {
        assert!(
            candidates_of("CREATE TABLE t (a INT, c BIGINT AS (a+1) STORED, KEY idx_c (c))")
                .is_empty(),
            "Go collects virtual generated columns only"
        );
        assert!(
            candidates_of("CREATE TABLE t (a INT, c BIGINT AS (a+1) VIRTUAL)").is_empty(),
            "a column no index covers can serve no access path"
        );
    }

    /// An empty map short-circuits, which is Go's own early return.
    #[test]
    fn a_table_with_no_candidate_substitutes_nothing() {
        let map = SubstitutionMap::default();
        assert!(map.is_empty());
        assert!(map.substitute_condition(&condition("a+1 = 3")).is_none());
    }
}
