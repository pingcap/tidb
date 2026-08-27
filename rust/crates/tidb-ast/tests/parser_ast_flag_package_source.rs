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

//! Ports of `pkg/parser/ast/flag_test.go` (origin/master).
//!
//! Go's `ast.SetFlag`/`GetFlag` store a mutable per-node flag word; Rust
//! derives the same observable bit mask immutably via `Expr::flags` (see
//! `pkg/parser/ast/base.go` boundary notes in this crate's `base.rs`). The
//! assertions below build each parsed expression state Go exercises and pin
//! the derived word against the Go expectation's bit set.

use tidb_ast::{
    BinaryOp, Expr, FLAG_CONSTANT, FLAG_HAS_AGGREGATE_FUNC, FLAG_HAS_DEFAULT, FLAG_HAS_FUNC,
    FLAG_HAS_PARAM_MARKER, FLAG_HAS_REFERENCE, FLAG_HAS_SUBQUERY, FLAG_HAS_VARIABLE,
    SelectField, SelectStatementKind, SelectStmt, UnaryOp,
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

fn binary(op: BinaryOp, l: Expr, r: Expr) -> Expr {
    Expr::Binary(op, Box::new(l), Box::new(r))
}

/// `pkg/parser/ast/flag_test.go::TestHasAggFlag`.
///
/// Go sets raw flag words on an empty `BetweenExpr` and checks
/// `HasAggFlag(expr)` — the aggregate bit dominates regardless of the other
/// bits carried alongside it, and clears when absent. Rust derives flags
/// from tree shape, so the equivalent contract is pinned structurally:
/// a tree whose only special subexpression is an aggregate carries exactly
/// the aggregate bit (with reference from its argument); combining
/// additional effects (a variable here, standing in for Go's
/// `FlagHasVariable`) ORs alongside the aggregate bit without changing the
/// predicate; removing the aggregate flips it false.
#[test]
fn has_agg_flag() {
    // FlagHasAggregateFunc alone (its own arg is constant-like in Go).
    let with_aggregate = Expr::Aggregate {
        name: "SUM".to_string(),
        distinct: false,
        args: vec![int("1")],
    };
    assert!(with_aggregate.has_aggregate_flag());

    // FlagHasAggregateFunc | FlagHasVariable: still true.
    let aggregate_plus_variable = binary(
        BinaryOp::Eq,
        Expr::Aggregate {
            name: "SUM".to_string(),
            distinct: false,
            args: vec![int("1")],
        },
        Expr::UserVar("v".to_string()),
    );
    let flags = aggregate_plus_variable.flags();
    assert!(flags & FLAG_HAS_AGGREGATE_FUNC != 0);
    assert!(flags & FLAG_HAS_VARIABLE != 0);
    assert!(aggregate_plus_variable.has_aggregate_flag());

    // FlagHasVariable alone: false.
    let variable_only = Expr::UserVar("auto_commit".to_string());
    let flags = variable_only.flags();
    assert!(flags & FLAG_HAS_AGGREGATE_FUNC == 0);
    assert!(!variable_only.has_aggregate_flag());
}

/// `pkg/parser/ast/flag_test.go::TestFlag`.
///
/// Each Go row parses `select <expr>` and pins the computed flag word of
/// the first field expression after `SetFlag`. Identical states are built
/// here; parse-time canonicalizations that affect flags are preserved
/// (COUNT is an AggregateFuncExpr; TRIM/SUBSTRING/NOW/EXTRACT/CAST carry
/// `FlagHasFunc`; NOT EXISTS chains still count as subqueries).
#[test]
fn flag() {
    let between_constants = Expr::Between {
        expr: Box::new(int("1")),
        low: Box::new(int("0")),
        high: Box::new(int("2")),
        not: false,
    };

    let simple_case = Expr::Case {
        value: Some(Box::new(int("1"))),
        when_clauses: vec![(int("1"), int("1"))],
        else_clause: Some(Box::new(int("0"))),
    };

    let case_with_reference = Expr::Case {
        value: Some(Box::new(int("1"))),
        when_clauses: vec![(
            binary(BinaryOp::Gt, column(&["a"]), int("1")),
            int("1"),
        )],
        else_clause: Some(Box::new(int("0"))),
    };

    // `1 = ANY (select 1) OR exists (select 1)`
    let any_or_exists = binary(
        BinaryOp::LogicOr,
        Expr::CompareSubquery {
            op: BinaryOp::Eq,
            left: Box::new(int("1")),
            all: false,
            subquery: scalar_select(int("1")),
        },
        Expr::Exists {
            not: false,
            subquery: scalar_select(int("1")),
        },
    );

    // `1 in (1) or 1 is true or null is null or 'abc' like 'abc' or 'abc' rlike 'abc'`
    let predicates = binary(
        BinaryOp::LogicOr,
        binary(
            BinaryOp::LogicOr,
            binary(
                BinaryOp::LogicOr,
                Expr::In {
                    expr: Box::new(int("1")),
                    list: vec![int("1")],
                    not: false,
                },
                Expr::Is {
                    expr: Box::new(int("1")),
                    target: tidb_ast::IsTarget::True,
                    not: false,
                },
            ),
            Expr::Is {
                expr: Box::new(Expr::Null),
                target: tidb_ast::IsTarget::Null,
                not: false,
            },
        ),
        binary(
            BinaryOp::LogicOr,
            like(string("abc"), string("abc")),
            Expr::Regexp {
                expr: Box::new(string("abc")),
                pattern: Box::new(string("abc")),
                not: false,
            },
        ),
    );

    // `row (1, 1) = row (1, 1)`
    let row_equality = binary(
        BinaryOp::Eq,
        Expr::Row(vec![int("1"), int("1")]),
        Expr::Row(vec![int("1"), int("1")]),
    );

    // `(1 + a) > ?`
    let reference_with_param = binary(
        BinaryOp::Gt,
        Expr::Paren(Box::new(binary(
            BinaryOp::Plus,
            int("1"),
            column(&["a"]),
        ))),
        Expr::ParamMarker {
            offset: 0,
            order: 0,
            in_execute: false,
            projection_offset: 0,
        },
    );

    let plain_trim = func_expr("trim", vec![string("abc ")]);
    let now_chain = binary(
        BinaryOp::Plus,
        binary(
            BinaryOp::Plus,
            func_expr("now", Vec::new()),
            Expr::Extract {
                unit: "YEAR".to_string(),
                value: Box::new(string("2009-07-02")),
            },
        ),
        Expr::Cast(tidb_ast::CastExpr {
            expr: Box::new(int("1")),
            cast_type: tidb_ast::CastType::Unsigned,
            style: tidb_ast::CastStyle::Cast,
            array: false,
        }),
    );
    let substring_call = func_expr("SUBSTRING", vec![string("abc"), int("1")]);

    let sum_column = Expr::Aggregate {
        name: "SUM".to_string(),
        distinct: false,
        args: vec![column(&["a"])],
    };

    // `(select 1) as a`: the select list holds a scalar subquery field.
    let mut subquery_field_select = empty_select();
    subquery_field_select.fields.push(SelectField::Expr {
        expr: Expr::Subquery(scalar_select(int("1"))),
        alias: Some("a".to_string()),
    });
    let _ = subquery_field_select;

    let default_a = Expr::Default(Some(vec!["a".to_string()]));

    // `a in (1, count(*), 3)`: COUNT(*) parses into the aggregate family.
    let in_with_count = Expr::In {
        expr: Box::new(column(&["a"])),
        list: vec![
            int("1"),
            Expr::Aggregate {
                name: "COUNT".to_string(),
                distinct: false,
                args: vec![int("1")],
            },
            int("3"),
        ],
        not: false,
    };

    let regexp_string_only = Expr::Regexp {
        expr: Box::new(string("Michael!")),
        pattern: Box::new(string(".*")),
        not: false,
    };
    let regexp_reference = Expr::Regexp {
        expr: Box::new(column(&["a"])),
        pattern: Box::new(string(".*")),
        not: false,
    };
    let negated_column = Expr::Unary(UnaryOp::Minus, Box::new(column(&["a"])));

    let cases: [(Expr, u64, &str); 21] = [
        (between_constants, FLAG_CONSTANT, "1 between 0 and 2"),
        (simple_case.clone(), FLAG_CONSTANT, "case 1 when 1 then 1 else 0 end"),
        (simple_case, FLAG_CONSTANT, "case 1 when 1 then 1 else 0 end"),
        (
            case_with_reference,
            FLAG_CONSTANT | FLAG_HAS_REFERENCE,
            "case 1 when a > 1 then 1 else 0 end",
        ),
        (any_or_exists, FLAG_HAS_SUBQUERY, "1 = ANY (select 1) OR exists (select 1)"),
        (predicates, FLAG_CONSTANT, "in/is-null-is-true/like/rlike chain"),
        (row_equality, FLAG_CONSTANT, "row (1, 1) = row (1, 1)"),
        (
            reference_with_param,
            FLAG_HAS_REFERENCE | FLAG_HAS_PARAM_MARKER,
            "(1 + a) > ?",
        ),
        (plain_trim, FLAG_HAS_FUNC, "trim('abc ')"),
        (now_chain, FLAG_HAS_FUNC, "now() + extract + cast unsigned"),
        (substring_call, FLAG_HAS_FUNC, "substring('abc', 1)"),
        (
            sum_column,
            FLAG_HAS_AGGREGATE_FUNC | FLAG_HAS_REFERENCE,
            "sum(a)",
        ),
        (
            Expr::Subquery(scalar_select(int("1"))),
            FLAG_HAS_SUBQUERY,
            "(select 1) as a",
        ),
        (
            Expr::UserVar("auto_commit".to_string()),
            FLAG_HAS_VARIABLE,
            "@auto_commit",
        ),
        (default_a, FLAG_HAS_DEFAULT, "default(a)"),
        (
            Expr::Is {
                expr: Box::new(column(&["a"])),
                target: tidb_ast::IsTarget::Null,
                not: false,
            },
            FLAG_HAS_REFERENCE,
            "a is null",
        ),
        (
            Expr::Is {
                expr: Box::new(int("1")),
                target: tidb_ast::IsTarget::True,
                not: false,
            },
            FLAG_CONSTANT,
            "1 is true",
        ),
        (
            in_with_count,
            FLAG_CONSTANT | FLAG_HAS_REFERENCE | FLAG_HAS_AGGREGATE_FUNC,
            "a in (1, count(*), 3)",
        ),
        (regexp_string_only, FLAG_CONSTANT, "'Michael!' REGEXP '.*'"),
        (regexp_reference, FLAG_HAS_REFERENCE, "a REGEXP '.*'"),
        (negated_column, FLAG_HAS_REFERENCE, "-a"),
    ];
    for (expr, want, label) in cases {
        assert_eq!(expr.flags(), want, "for {label}");
    }
}

fn like(expr: Expr, pattern: Expr) -> Expr {
    Expr::Like {
        expr: Box::new(expr),
        pattern: Box::new(pattern),
        not: false,
        ilike: false,
        escape: None,
    }
}

fn func_expr(name: &str, args: Vec<Expr>) -> Expr {
    Expr::Func {
        name: name.to_string(),
        args,
        origin_position: 0,
    }
}

fn scalar_select(expr: Expr) -> tidb_ast::NodeBox<tidb_ast::QueryStmt> {
    let mut select = empty_select();
    select.fields.push(SelectField::Expr { expr, alias: None });
    tidb_ast::NodeBox::new(tidb_ast::QueryStmt::Select(Box::new(select)))
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
