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

//! Ports of `pkg/parser/ast/expressions_test.go` (origin/master).
//!
//! Go wraps each case in `select %s`, parses it, extracts the field
//! expression, and restores the extracted subtree with
//! `format.DefaultRestoreFlags`. This crate owns the AST/restore contract,
//! so each case constructs the typed AST state Go's parser would produce and
//! asserts the same restored text through [`Expr::restore`].

use tidb_ast::{
    BinaryOp, Expr, IsTarget, MatchModifier, NodeBox, QueryStmt, RestoreFlags, SelectField,
    SelectStatementKind, SelectStmt, Stmt, SysVarScope, UnaryOp,
};

fn column(path: &[&str]) -> Expr {
    Expr::Column(path.iter().map(|name| name.to_string()).collect())
}

fn int(value: &str) -> Expr {
    Expr::Int(value.to_string())
}

fn string(value: &str) -> Expr {
    Expr::String(value.to_string())
}

/// Builds `QueryStmt::Select(...)` with a single field list.
fn select_query(fields: Vec<SelectField>) -> NodeBox<QueryStmt> {
    let mut select = empty_select();
    for field in fields {
        select.fields.push(field);
    }
    NodeBox::new(QueryStmt::Select(Box::new(select)))
}

fn empty_select() -> SelectStmt {
    SelectStmt {
        kind: SelectStatementKind::Select,
        is_in_braces: false,
        with: None,
        hints: Vec::new(),
        priority: Default::default(),
        sql_small_result: false,
        sql_big_result: false,
        sql_buffer_result: false,
        sql_no_cache: false,
        straight_join: false,
        calc_found_rows: false,
        distinct: false,
        all: false,
        fields: Default::default(),
        values: Vec::new(),
        from: None,
        where_clause: None,
        group_by: Vec::new(),
        rollup: false,
        having: None,
        windows: Vec::new(),
        order_by: Vec::new(),
        limit: None,
        lock: None,
        into_outfile: None,
        into_vars: Vec::new(),
    }
}

fn scalar_select(expr: Expr) -> NodeBox<QueryStmt> {
    select_query(vec![SelectField::Expr { expr, alias: None }])
}

/// Counts visits of marked leaf expressions to replicate Go's
/// `checkVisitor` enter/leave counting.
#[derive(Default)]
struct MarkCounter {
    visited: Vec<String>,
}

impl tidb_ast::Visitor for MarkCounter {
    fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
        if let Some(Expr::Column(path)) = node.downcast_ref::<Expr>() {
            if path.first().is_some_and(|name| name.starts_with("mark")) {
                self.visited.push(path[0].clone());
            }
        }
        false
    }

    fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
        true
    }
}

impl MarkCounter {
    fn counts_of(&self, name: &str) -> usize {
        self.visited.iter().filter(|visited| *visited == name).count()
    }
}

/// A marker leaf whose single path component starts with `mark`.
fn mark(name: &str) -> Expr {
    column(&[&format!("mark{name}")])
}

/// `pkg/parser/ast/expressions_test.go::TestExpresionsVisitorCover`.
///
/// Go injects one shared `checkExpr` node at several child positions and
/// asserts per-node Enter/Leave totals (3 between, 2 binary, 6 case, ...).
/// Rust cannot inject an extra expression type into closed [`Expr`]
/// variants, so identical positions carry distinguishable marker leaves:
/// this pins that every Go-counted position is entered exactly once by
/// `Visitable::accept`, reproducing the same traversal coverage. Where Go
/// carries planner-vestige children (`PatternInExpr.Sel`, nil until a
/// planning rewrite fills it), Rust models no such pointer; comments cite
/// each divergence.
#[test]
fn expresions_visitor_cover() {
    // BetweenExpr{Expr, Left, Right}: 3 enters / 3 leaves.
    let mut tree = Expr::Between {
        expr: Box::new(mark("e")),
        low: Box::new(mark("l")),
        high: Box::new(mark("h")),
        not: false,
    };
    let mut counter = MarkCounter::default();
    assert!(tidb_ast::Visitable::accept(&mut tree, &mut counter));
    assert_eq!(
        (
            counter.counts_of("marke"),
            counter.counts_of("markl"),
            counter.counts_of("markh")
        ),
        (1, 1, 1),
        "BetweenExpr must visit all three children"
    );

    // BinaryOperationExpr{L, R}: 2/2.
    let mut tree = Expr::Binary(BinaryOp::Plus, Box::new(mark("l")), Box::new(mark("r")));
    let mut counter = MarkCounter::default();
    assert!(tidb_ast::Visitable::accept(&mut tree, &mut counter));
    assert_eq!(
        (counter.counts_of("markl"), counter.counts_of("markr")),
        (1, 1),
        "BinaryOperationExpr must visit both sides"
    );

    // CaseExpr{Value, WhenClauses(2 x [Expr, Result]), ElseClause}: 6/6.
    let mut tree = Expr::Case {
        value: Some(Box::new(mark("v"))),
        when_clauses: vec![(mark("w1c"), mark("w1r")), (mark("w2c"), mark("w2r"))],
        else_clause: Some(Box::new(mark("e"))),
    };
    let mut counter = MarkCounter::default();
    assert!(tidb_ast::Visitable::accept(&mut tree, &mut counter));
    for name in ["markv", "markw1c", "markw1r", "markw2c", "markw2r", "marke"] {
        assert_eq!(counter.counts_of(name), 1, "{name} must be visited");
    }

    // ColumnNameExpr / DefaultExpr / NewParamMarkerExpr / PositionExpr /
    // ValueExpr / ValuesExpr: zero checkExpr children in Go — nothing to
    // count beyond the walk completing.
    let markless_trees = [
        column(&["plain"]),
        Expr::Default(Some(vec!["i".to_string()])),
        int("42"),
    ];
    for mut tree in markless_trees {
        let mut counter = MarkCounter::default();
        assert!(tidb_ast::Visitable::accept(&mut tree, &mut counter));
        assert!(counter.visited.is_empty(), "no marked children expected");
    }

    // CompareSubqueryExpr{L, R}: 2/2. The left operand is an in-tree
    // expression; the subquery body is reachable as well, which Go models
    // by walking `Sel` — its own parser keeps `Sel == R` after rewriting.
    let mut tree = Expr::CompareSubquery {
        op: BinaryOp::Eq,
        left: Box::new(mark("l")),
        all: false,
        subquery: scalar_select(mark("sub")),
    };
    let mut counter = MarkCounter::default();
    assert!(tidb_ast::Visitable::accept(&mut tree, &mut counter));
    assert_eq!(counter.counts_of("markl"), 1);
    assert_eq!(counter.counts_of("marksub"), 1);

    // ExistsSubqueryExpr{Sel}: 1/1 — reached through the subquery envelope.
    let mut tree = Expr::Exists {
        not: false,
        subquery: scalar_select(mark("sel")),
    };
    let mut counter = MarkCounter::default();
    assert!(tidb_ast::Visitable::accept(&mut tree, &mut counter));
    assert_eq!(counter.counts_of("marksel"), 1);

    // IsNullExpr / IsTruthExpr{Expr}: 1/1 each.
    for target in [IsTarget::Null, IsTarget::True] {
        let mut tree = Expr::Is {
            expr: Box::new(mark("e")),
            target,
            not: false,
        };
        let mut counter = MarkCounter::default();
        assert!(tidb_ast::Visitable::accept(&mut tree, &mut counter));
        assert_eq!(counter.counts_of("marke"), 1);
    }

    // ParenthesesExpr{Expr}: 1/1.
    let mut tree = Expr::Paren(Box::new(mark("p")));
    let mut counter = MarkCounter::default();
    assert!(tidb_ast::Visitable::accept(&mut tree, &mut counter));
    assert_eq!(counter.counts_of("markp"), 1);

    // PatternInExpr{Expr, List(3), Sel} => 5/5 in Go. `Sel` is planner
    // vestige (Go's own parser leaves it nil until rewriten); Rust models
    // the four parse-time children.
    let mut tree = Expr::In {
        expr: Box::new(mark("e")),
        list: vec![mark("l1"), mark("l2"), mark("l3")],
        not: false,
    };
    let mut counter = MarkCounter::default();
    assert!(tidb_ast::Visitable::accept(&mut tree, &mut counter));
    for name in ["marke", "markl1", "markl2", "markl3"] {
        assert_eq!(counter.counts_of(name), 1, "{name}");
    }

    // PatternLikeOrIlikeExpr / PatternRegexpExpr{Expr, Pattern}: 2/2 each.
    for maker in [
        |e: Expr, p: Expr| Expr::Like {
            expr: Box::new(e),
            pattern: Box::new(p),
            not: false,
            ilike: false,
            escape: None,
        },
        |e: Expr, p: Expr| Expr::Regexp {
            expr: Box::new(e),
            pattern: Box::new(p),
            not: false,
        },
    ] {
        let mut tree = maker(mark("e"), mark("p"));
        let mut counter = MarkCounter::default();
        assert!(tidb_ast::Visitable::accept(&mut tree, &mut counter));
        assert_eq!(counter.counts_of("marke"), 1);
        assert_eq!(counter.counts_of("markp"), 1);
    }

    // RowExpr{Values(2)}: 2/2.
    let mut tree = Expr::Row(vec![mark("r1"), mark("r2")]);
    let mut counter = MarkCounter::default();
    assert!(tidb_ast::Visitable::accept(&mut tree, &mut counter));
    assert_eq!(counter.counts_of("markr1"), 1);
    assert_eq!(counter.counts_of("markr2"), 1);

    // UnaryOperationExpr{V}: 1/1.
    let mut tree = Expr::Unary(UnaryOp::Minus, Box::new(mark("v")));
    let mut counter = MarkCounter::default();
    assert!(tidb_ast::Visitable::accept(&mut tree, &mut counter));
    assert_eq!(counter.counts_of("markv"), 1);

    // VariableExpr{Value}: 1/1 — Go stores the assigned value on the same
    // VariableExpr node via `IsSystem=false`; Rust spells assignments as
    // `Expr::Assign`, keeping the value child visited exactly once.
    let mut tree = Expr::Assign {
        name: "v".to_string(),
        value: Box::new(mark("v")),
    };
    let mut counter = MarkCounter::default();
    assert!(tidb_ast::Visitable::accept(&mut tree, &mut counter));
    assert_eq!(counter.counts_of("markv"), 1);
}

/// `pkg/parser/ast/expressions_test.go::TestUnaryOperationExprRestore`.
#[test]
fn unary_operation_expr_restore() {
    let double_unary =
        |outer: UnaryOp, inner: UnaryOp| Expr::Unary(outer, Box::new(Expr::Unary(inner, Box::new(int("1")))));
    let cases: Vec<(Expr, &str)> = vec![
        (double_unary(UnaryOp::Plus, UnaryOp::Plus), "++1"),
        (double_unary(UnaryOp::Minus, UnaryOp::Minus), "--1"),
        (double_unary(UnaryOp::Minus, UnaryOp::Plus), "-+1"),
        (Expr::Unary(UnaryOp::Minus, Box::new(int("1"))), "-1"),
        (
            Expr::Unary(UnaryOp::NotKeyword, Box::new(Expr::Bool(true))),
            "NOT TRUE",
        ),
        (Expr::Unary(UnaryOp::BitNeg, Box::new(int("3"))), "~3"),
        (Expr::Unary(UnaryOp::Not, Box::new(Expr::Bool(true))), "!TRUE"),
    ];
    for (expr, want) in cases {
        assert_eq!(expr.restore(), want);
    }
}

/// `pkg/parser/ast/expressions_test.go::TestColumnNameExprRestore`.
#[test]
fn column_name_expr_restore() {
    // Backquote doubling and per-part case preservation come straight from
    // the transcreated restore; identical rows in Go confirm each shape.
    let cases: Vec<(&[&str], &str)> = vec![
        (&["abc"], "`abc`"),
        (&["abc"], "`abc`"),
        (&["ab`c"], "`ab``c`"),
        (&["sabc", "tABC"], "`sabc`.`tABC`"),
        (&["dabc", "sabc", "tabc"], "`dabc`.`sabc`.`tabc`"),
        (&["dabc", "sabc", "tabc"], "`dabc`.`sabc`.`tabc`"),
        (&["dABC", "sabc", "tabc"], "`dABC`.`sabc`.`tabc`"),
    ];
    for (path, want) in cases {
        assert_eq!(column(path).restore(), want, "{path:?}");
    }
}

/// `pkg/parser/ast/expressions_test.go::TestIsNullExprRestore`.
#[test]
fn is_null_expr_restore() {
    let cases: [(bool, &str); 2] = [(false, "`a` IS NULL"), (true, "`a` IS NOT NULL")];
    for (not, want) in cases {
        let expr = Expr::Is {
            expr: Box::new(column(&["a"])),
            target: IsTarget::Null,
            not,
        };
        assert_eq!(expr.restore(), want);
    }
}

/// `pkg/parser/ast/expressions_test.go::TestIsTruthRestore`.
#[test]
fn is_truth_restore() {
    let cases: [(IsTarget, bool, &str); 4] = [
        (IsTarget::True, false, "`a` IS TRUE"),
        (IsTarget::True, true, "`a` IS NOT TRUE"),
        (IsTarget::False, false, "`a` IS FALSE"),
        (IsTarget::False, true, "`a` IS NOT FALSE"),
    ];
    for (target, not, want) in cases {
        let expr = Expr::Is {
            expr: Box::new(column(&["a"])),
            target,
            not,
        };
        assert_eq!(expr.restore(), want);
    }
}

/// `pkg/parser/ast/expressions_test.go::TestBetweenExprRestore`.
#[test]
fn between_expr_restore() {
    let cases: [(Expr, Expr, bool, &str); 5] = [
        (int("1"), int("2"), false, "`b` BETWEEN 1 AND 2"),
        (int("1"), int("2"), true, "`b` NOT BETWEEN 1 AND 2"),
        (column(&["a"]), column(&["b"]), false, "`b` BETWEEN `a` AND `b`"),
        (
            string(""),
            string("b"),
            false,
            "`b` BETWEEN _UTF8MB4'' AND _UTF8MB4'b'",
        ),
        (
            string("2018-11-01"),
            string("2018-11-02"),
            false,
            "`b` BETWEEN _UTF8MB4'2018-11-01' AND _UTF8MB4'2018-11-02'",
        ),
    ];
    for (low, high, not, want) in cases {
        let expr = Expr::Between {
            expr: Box::new(column(&["b"])),
            low: Box::new(low),
            high: Box::new(high),
            not,
        };
        assert_eq!(expr.restore(), want);
    }
}

/// `pkg/parser/ast/expressions_test.go::TestCaseExpr`.
#[test]
fn case_expr() {
    let cases: [(Option<Expr>, Vec<(Expr, Expr)>, Option<Expr>, &str); 5] = [
        (None, vec![(int("1"), int("2"))], None, "CASE WHEN 1 THEN 2 END"),
        (
            None,
            vec![(int("1"), string("a")), (int("2"), string("b"))],
            None,
            "CASE WHEN 1 THEN _UTF8MB4'a' WHEN 2 THEN _UTF8MB4'b' END",
        ),
        (
            None,
            vec![(int("1"), string("a")), (int("2"), string("b"))],
            Some(string("c")),
            "CASE WHEN 1 THEN _UTF8MB4'a' WHEN 2 THEN _UTF8MB4'b' ELSE _UTF8MB4'c' END",
        ),
        (
            None,
            vec![(
                Expr::Binary(BinaryOp::Ne, Box::new(string("a")), Box::new(int("1"))),
                Expr::Bool(true),
            )],
            Some(Expr::Bool(false)),
            "CASE WHEN _UTF8MB4'a'!=1 THEN TRUE ELSE FALSE END",
        ),
        (
            Some(column(&["a"])),
            vec![(string("a"), Expr::Bool(true))],
            Some(Expr::Bool(false)),
            "CASE `a` WHEN _UTF8MB4'a' THEN TRUE ELSE FALSE END",
        ),
    ];
    for (value, when_clauses, else_clause, want) in cases {
        let expr = Expr::Case {
            value: value.map(Box::new),
            when_clauses,
            else_clause: else_clause.map(Box::new),
        };
        assert_eq!(expr.restore(), want);
    }
}

/// `pkg/parser/ast/expressions_test.go::TestBinaryOperationExpr`.
#[test]
fn binary_operation_expr() {
    let cases: [(BinaryOp, Expr, Expr, &str); 18] = [
        (BinaryOp::Ne, string("a"), int("1"), "_UTF8MB4'a'!=1"),
        (BinaryOp::Ne, column(&["a"]), int("1"), "`a`!=1"),
        (BinaryOp::Lt, int("3"), int("5"), "3<5"),
        (BinaryOp::Gt, int("10"), int("5"), "10>5"),
        (BinaryOp::Plus, int("3"), int("5"), "3+5"),
        (BinaryOp::Minus, int("3"), int("5"), "3-5"),
        (BinaryOp::Ne, column(&["a"]), int("5"), "`a`!=5"),
        (BinaryOp::Eq, column(&["a"]), int("1"), "`a`=1"),
        (BinaryOp::Mod, column(&["a"]), int("2"), "`a`%2"),
        (BinaryOp::IntDiv, column(&["a"]), int("2"), "`a` DIV 2"),
        (BinaryOp::LogicAnd, Expr::Bool(true), Expr::Bool(true), "TRUE AND TRUE"),
        (
            BinaryOp::LogicOr,
            Expr::Bool(false),
            Expr::Bool(false),
            "FALSE OR FALSE",
        ),
        (
            BinaryOp::LogicXor,
            Expr::Bool(true),
            Expr::Bool(false),
            "TRUE XOR FALSE",
        ),
        (BinaryOp::BitAnd, int("3"), int("4"), "3&4"),
        (BinaryOp::BitOr, int("5"), int("6"), "5|6"),
        (BinaryOp::BitXor, int("7"), int("8"), "7^8"),
        (BinaryOp::LeftShift, int("9"), int("10"), "9<<10"),
        (BinaryOp::RightShift, int("11"), int("12"), "11>>12"),
    ];
    for (op, l, r, want) in cases {
        assert_eq!(Expr::Binary(op, Box::new(l), Box::new(r)).restore(), want);
    }
}

/// `pkg/parser/ast/expressions_test.go::TestBinaryOperationExprWithFlags`.
#[test]
fn binary_operation_expr_with_flags() {
    let flags = RestoreFlags::DEFAULT | RestoreFlags::SPACES_AROUND_BINARY_OPERATION;
    let cases: [(BinaryOp, Expr, Expr, &str); 8] = [
        (BinaryOp::Ne, string("a"), int("1"), "_UTF8MB4'a' != 1"),
        (BinaryOp::Ne, column(&["a"]), int("1"), "`a` != 1"),
        (BinaryOp::Lt, int("3"), int("5"), "3 < 5"),
        (BinaryOp::Gt, int("10"), int("5"), "10 > 5"),
        (BinaryOp::Plus, int("3"), int("5"), "3 + 5"),
        (BinaryOp::Minus, int("3"), int("5"), "3 - 5"),
        (BinaryOp::Ne, column(&["a"]), int("5"), "`a` != 5"),
        (BinaryOp::Eq, column(&["a"]), int("1"), "`a` = 1"),
    ];
    for (op, l, r, want) in cases {
        assert_eq!(
            Expr::Binary(op, Box::new(l), Box::new(r)).restore_with_flags(flags),
            want
        );
    }
}

/// `pkg/parser/ast/expressions_test.go::TestParenthesesExpr`.
#[test]
fn parentheses_expr() {
    // `(1+2)*3` keeps its explicit parentheses; `1+2*3` relies purely on
    // precedence — both are the source states Go's parser builds.
    let parenthesized = Expr::Binary(
        BinaryOp::Mul,
        Box::new(Expr::Paren(Box::new(Expr::Binary(
            BinaryOp::Plus,
            Box::new(int("1")),
            Box::new(int("2")),
        )))),
        Box::new(int("3")),
    );
    assert_eq!(parenthesized.restore(), "(1+2)*3");

    let precedence = Expr::Binary(
        BinaryOp::Plus,
        Box::new(int("1")),
        Box::new(Expr::Binary(BinaryOp::Mul, Box::new(int("2")), Box::new(int("3")))),
    );
    assert_eq!(precedence.restore(), "1+2*3");
}

/// `pkg/parser/ast/expressions_test.go::TestWhenClause`.
///
/// Go extracts `CaseExpr.WhenClauses[0]` and restores it standalone. The
/// Rust AST represents when-clauses as inline `(condition, result)` pairs
/// inside [`Expr::Case`] rather than as a free-standing node type, so the
/// pair is pinned through the smallest enclosing case expression (`WHEN …`
/// plus the mandatory `END` keyword).
#[test]
fn when_clause() {
    let pairs: [((Expr, Expr), &str); 3] = [
        ((int("1"), int("2")), "CASE WHEN 1 THEN 2 END"),
        ((int("1"), string("a")), "CASE WHEN 1 THEN _UTF8MB4'a' END"),
        (
            (
                Expr::Binary(BinaryOp::Ne, Box::new(string("a")), Box::new(int("1"))),
                Expr::Bool(true),
            ),
            "CASE WHEN _UTF8MB4'a'!=1 THEN TRUE END",
        ),
    ];
    for ((condition, result), want) in pairs {
        let expr = Expr::Case {
            value: None,
            when_clauses: vec![(condition, result)],
            else_clause: None,
        };
        assert_eq!(expr.restore(), want);
    }
}

/// `pkg/parser/ast/expressions_test.go::TestDefaultExpr`.
///
/// Go builds `insert into t values(%s)` and extracts `Lists[0][0]`; both
/// extracted expressions restore independently of their wrapper here.
#[test]
fn default_expr() {
    assert_eq!(Expr::Default(None).restore(), "DEFAULT");
    assert_eq!(
        Expr::Default(Some(vec!["i".to_string()])).restore(),
        "DEFAULT(`i`)"
    );
}

/// `pkg/parser/ast/expressions_test.go::TestPatternInExprRestore`.
#[test]
fn pattern_in_expr_restore() {
    let in_cases: [(Expr, Vec<Expr>, bool, &str); 3] = [
        (string("a"), vec![string("b")], false, "_UTF8MB4'a' IN (_UTF8MB4'b')"),
        (int("2"), vec![int("0"), int("3"), int("7")], false, "2 IN (0,3,7)"),
        (int("2"), vec![int("0"), int("3"), int("7")], true, "2 NOT IN (0,3,7)"),
    ];
    for (expr, list, not, want) in in_cases {
        let restored = Expr::In {
            expr: Box::new(expr),
            list,
            not,
        }
        .restore();
        assert_eq!(restored, want);
    }
    // Subquery membership uses the dedicated InSubquery variant.
    for (not, want) in [(false, "2 IN (SELECT 2)"), (true, "2 NOT IN (SELECT 2)")] {
        let restored = Expr::InSubquery {
            expr: Box::new(int("2")),
            subquery: scalar_select(int("2")),
            not,
        }
        .restore();
        assert_eq!(restored, want);
    }
}

/// `pkg/parser/ast/expressions_test.go::TestPatternLikeExprRestore`.
#[test]
fn pattern_like_expr_restore() {
    let cases: [(bool, &str); 8] = [
        (false, "t1"),
        (false, "t1%"),
        (false, "%t1%"),
        (false, "%t1_|"),
        (true, "t1"),
        (true, "t1%"),
        (true, "%D%v%"),
        (true, "%t1_|"),
    ];
    for (not, pattern) in cases {
        let expr = Expr::Like {
            expr: Box::new(column(&["a"])),
            pattern: Box::new(string(pattern)),
            not,
            ilike: false,
            escape: None,
        };
        let prefix = if not { "NOT LIKE" } else { "LIKE" };
        let want = format!("`a` {prefix} _UTF8MB4'{pattern}'");
        assert_eq!(expr.restore(), want);
    }
}

/// `pkg/parser/ast/expressions_test.go::TestValuesExpr`.
///
/// Go models `VALUES(a)` as `ValuesExpr{Column}`; this crate spells the
/// same restore contract as `` Expr::Func{name: "VALUES"} `` — see
/// `InsertStmt::on_duplicate`'s own doc for the mapping authority.
#[test]
fn values_expr() {
    let values = |name: &str| Expr::Func {
        name: "VALUES".to_string(),
        args: vec![column(&[name])],
        origin_position: 0,
    };
    assert_eq!(values("a").restore(), "VALUES(`a`)");
    let sum = Expr::Binary(BinaryOp::Plus, Box::new(values("a")), Box::new(values("b")));
    assert_eq!(sum.restore(), "VALUES(`a`)+VALUES(`b`)");
}

/// `pkg/parser/ast/expressions_test.go::TestPatternRegexpExprRestore`.
#[test]
fn pattern_regexp_expr_restore() {
    // `rlike` normalizes to REGEXP at parse time, so both spellings share
    // the four pinned states below.
    let cases: [(bool, &str); 4] = [
        (false, "t1"),
        (false, "^[abc][0-9]{11}|ok$"),
        (true, "t1"),
        (true, "^[abc][0-9]{11}|ok$"),
    ];
    for (not, pattern) in cases {
        let expr = Expr::Regexp {
            expr: Box::new(column(&["a"])),
            pattern: Box::new(string(pattern)),
            not,
        };
        let prefix = if not { "NOT REGEXP" } else { "REGEXP" };
        let want = format!("`a` {prefix} _UTF8MB4'{pattern}'");
        assert_eq!(expr.restore(), want);
    }
}

/// `pkg/parser/ast/expressions_test.go::TestRowExprRestore`.
#[test]
fn row_expr_restore() {
    // The bare-paren and ROW-keyword grammar forms build the SAME node and
    // restore identically, hence duplicated expectations.
    let cases: [(Vec<Expr>, &str); 4] = [
        (vec![int("1"), int("2")], "ROW(1,2)"),
        (
            vec![column(&["col1"]), column(&["col2"])],
            "ROW(`col1`,`col2`)",
        ),
        (vec![int("1"), int("2")], "ROW(1,2)"),
        (
            vec![column(&["col1"]), column(&["col2"])],
            "ROW(`col1`,`col2`)",
        ),
    ];
    for (values, want) in cases {
        assert_eq!(Expr::Row(values).restore(), want);
    }
}

/// `pkg/parser/ast/expressions_test.go::TestMaxValueExprRestore`.
///
/// Go walks `AlterTableStmt.Specs[0].PartDefinitions[0].Clause.
/// (*PartitionDefinitionClauseLessThan).Exprs[0]`. Rust carries the same
/// state as `PartitionValue::MaxValue` inside the partition grammar; the
/// partition payload has no standalone public restore, so the assertion
/// goes through the full ALTER TABLE statement text.
#[test]
fn max_value_expr_restore() {
    use tidb_ast::{
        AddPartitionSpec, AlterPartitionAction, AlterTableAction, AlterTableStmt, DdlStmt,
        PartitionDefinition, PartitionDefinitionClause, PartitionValue,
    };
    let stmt = Stmt::Ddl(NodeBox::new(DdlStmt::AlterTable(Box::new(AlterTableStmt {
        name: vec!["posts".to_string()],
        actions: vec![AlterTableAction::Partition(AlterPartitionAction::Add {
            if_not_exists: false,
            no_write_to_binlog: false,
            spec: AddPartitionSpec::Definitions(vec![PartitionDefinition {
                name: "p1".to_string(),
                clause: PartitionDefinitionClause::LessThan(vec![PartitionValue::MaxValue]),
                options: Vec::new(),
                sub_partitions: Vec::new(),
            }]),
        })],
    }))));
    assert_eq!(
        stmt.restore(),
        "ALTER TABLE `posts` ADD PARTITION (PARTITION `p1` VALUES LESS THAN (MAXVALUE))"
    );
}

/// `pkg/parser/ast/expressions_test.go::TestPositionExprRestore`.
///
/// Go parses `order by 1` into a dedicated `PositionExpr` node whose
/// restore prints the ordinal. Rust keeps order-by items as their literal
/// expressions (`SelectStmt::order_by: Vec<OrderItem>` around
/// `Expr::Int("1")`), which prints the same text — pinned here.
#[test]
fn position_expr_restore() {
    assert_eq!(int("1").restore(), "1");
}

/// `pkg/parser/ast/expressions_test.go::TestExistsSubqueryExprRestore`.
///
/// Repeated `NOT`s collapse through the boolean flag at parse time (the
/// Go AST state keeps only `Not != parity`).
#[test]
fn exists_subquery_expr_restore() {
    let exists_query = || scalar_select(int("2"));
    let cases: [(bool, &str); 4] = [
        (false, "EXISTS (SELECT 2)"),
        (true, "NOT EXISTS (SELECT 2)"),
        (false, "EXISTS (SELECT 2)"),
        (true, "NOT EXISTS (SELECT 2)"),
    ];
    for (not, want) in cases {
        let expr = Expr::Exists {
            not,
            subquery: exists_query(),
        };
        assert_eq!(expr.restore(), want);
    }
}

/// `pkg/parser/ast/expressions_test.go::TestVariableExpr`.
#[test]
fn variable_expr() {
    let user_var = |name: &str| Expr::UserVar(name.to_string());
    let sys_var = |scope: Option<SysVarScope>, name: &str| Expr::SysVar {
        scope,
        name: name.to_string(),
    };
    let cases: [(Expr, &str); 13] = [
        (
            Expr::Binary(BinaryOp::Gt, Box::new(user_var("a")), Box::new(int("1"))),
            "@`a`>1",
        ),
        (
            Expr::Binary(BinaryOp::Plus, Box::new(user_var("aB")), Box::new(int("1"))),
            "@`aB`+1",
        ),
        (
            Expr::Assign {
                name: "a".to_string(),
                value: Box::new(int("1")),
            },
            "@`a`:=1",
        ),
        (
            Expr::Binary(
                BinaryOp::Eq,
                Box::new(user_var("a`b")),
                Box::new(int("4")),
            ),
            "@`a``b`=4",
        ),
        (
            Expr::Binary(BinaryOp::Gt, Box::new(user_var("aBC")), Box::new(int("1"))),
            "@`aBC`>1",
        ),
        (
            Expr::Binary(BinaryOp::Plus, Box::new(user_var("a")), Box::new(int("1"))),
            "@`a`+1",
        ),
        (user_var(""), "@``"),
        (user_var(""), "@``"),
        (sys_var(None, ""), "@@``"),
        (sys_var(None, "var"), "@@`var`"),
        (
            Expr::Binary(
                BinaryOp::Eq,
                Box::new(sys_var(Some(SysVarScope::Global), "b")),
                Box::new(string("foo")),
            ),
            "@@GLOBAL.`b`=_UTF8MB4'foo'",
        ),
        (
            sys_var(Some(SysVarScope::Session), "c"),
            "@@SESSION.`c`",
        ),
        (
            sys_var(Some(SysVarScope::Session), "abc"),
            "@@SESSION.`abc`",
        ),
    ];
    for (expr, want) in cases {
        assert_eq!(expr.restore(), want, "{want}");
    }
}

/// `pkg/parser/ast/expressions_test.go::TestMatchAgainstExpr`.
#[test]
fn match_against_expr() {
    let hex_literal = |digits: &str| Expr::Hex(digits.to_string());
    let against_body: [&str; 4] = [
        "_UTF8MB4'search for'",
        "_UTF8MB4'search for' IN BOOLEAN MODE",
        "_UTF8MB4'search for' WITH QUERY EXPANSION",
        "_UTF8MB4'search for' WITH QUERY EXPANSION",
    ];
    let columns_per_case: [&[&str]; 4] = [
        &["content", "title"],
        &["content"],
        &["content", "title"],
        &["content"],
    ];
    let modifiers = [
        MatchModifier::None,
        MatchModifier::BooleanMode,
        MatchModifier::QueryExpansion,
        // `IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION` restores the
        // query-expansion spelling only.
        MatchModifier::QueryExpansion,
    ];
    for index in 0..4 {
        let expr = Expr::MatchAgainst {
            columns: columns_per_case[index]
                .iter()
                .map(|name| vec![name.to_string()])
                .collect(),
            against: Box::new(string("search for")),
            modifier: modifiers[index],
        };
        let columns_text = columns_per_case[index]
            .iter()
            .map(|name| format!("`{name}`"))
            .collect::<Vec<_>>()
            .join(",");
        let want = format!("MATCH ({columns_text}) AGAINST ({})", against_body[index]);
        assert_eq!(expr.restore(), want, "case {index}");
    }

    // WHERE composition keeps bare operator punctuation.
    for (op, joiner) in [(BinaryOp::LogicAnd, "AND"), (BinaryOp::LogicOr, "OR")] {
        let match_expr = Expr::MatchAgainst {
            columns: vec![vec!["content".to_string()]],
            against: Box::new(string("search")),
            modifier: MatchModifier::None,
        };
        let id_predicate = Expr::Binary(BinaryOp::Eq, Box::new(column(&["id"])), Box::new(int("1")));
        let where_expr = Expr::Binary(op, Box::new(match_expr), Box::new(id_predicate));
        let want = format!("MATCH (`content`) AGAINST (_UTF8MB4'search') {joiner} `id`=1");
        assert_eq!(where_expr.restore(), want);
    }

    // Hex disjunction inside AGAINST restores lowercase x'' literals.
    let against = Expr::MatchAgainst {
        columns: vec![vec!["content".to_string()]],
        against: Box::new(Expr::Binary(
            BinaryOp::BitOr,
            Box::new(hex_literal("40404040")),
            Box::new(hex_literal("01020304")),
        )),
        modifier: MatchModifier::None,
    };
    let id_predicate = Expr::Binary(BinaryOp::Eq, Box::new(column(&["id"])), Box::new(int("1")));
    let where_expr = Expr::Binary(BinaryOp::LogicOr, Box::new(against), Box::new(id_predicate));
    assert_eq!(
        where_expr.restore(),
        "MATCH (`content`) AGAINST (x'40404040'|x'01020304') OR `id`=1"
    );
}
