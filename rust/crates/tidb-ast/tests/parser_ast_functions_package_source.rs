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

//! Ports of `pkg/parser/ast/functions_test.go` (origin/master).
//!
//! Go parses each case inside `select %s`, extracts the field expression,
//! and restores it under default flags. This crate owns the AST state, so
//! cases hand-build the typed expression Go's parser would produce
//! (`STD*`/`VAR*` canonicalization, `TRIM`'s defaulted single-space
//! `remstr`, `SUBSTRING .. FROM n FOR m` flattening, INTERVAL promotion,
//! `CHARACTER` → `CHAR`, ...) and assert [`Expr::restore`] against the
//! identical expectations.

use tidb_ast::{
    CastExpr, CastStyle, CastType, Expr, GetFormatSelector, OrderItem, TrimDirection,
    TypedString, WeightStringType, WindowDef, WindowOver, WindowSpec,
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

fn func(name: &str, args: Vec<Expr>) -> Expr {
    Expr::Func {
        name: name.to_string(),
        args,
        origin_position: 0,
    }
}

fn generic(schema: &str, name: &str, args: Vec<Expr>) -> Expr {
    Expr::GenericFuncCall {
        schema: schema.to_string(),
        name: name.to_string(),
        args,
        origin_position: 0,
    }
}

fn interval(value: Expr, unit: &str) -> Expr {
    Expr::Interval {
        value: Box::new(value),
        unit: unit.to_string(),
    }
}

/// A counting visitor proving traversal reaches every child of the built
/// function nodes (Go's `visitor{}`/`visitor1{}` pair).
#[derive(Default)]
struct FuncCounter {
    entered: usize,
    left: usize,
    deep_ints: usize,
}

impl tidb_ast::Visitor for FuncCounter {
    fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
        self.entered += 1;
        if let Some(Expr::Int(_)) = node.downcast_ref::<Expr>() {
            self.deep_ints += 1;
        }
        false
    }

    fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
        self.left += 1;
        true
    }
}

#[allow(clippy::too_many_arguments)]
fn window(
    name: &str,
    args: Vec<Expr>,
    distinct: bool,
    ignore_nulls: bool,
    from_last: bool,
    over: WindowOver,
) -> Expr {
    Expr::Window {
        name: name.to_string(),
        args,
        distinct,
        ignore_nulls,
        from_last,
        over,
    }
}

/// `pkg/parser/ast/functions_test.go::TestFunctionsVisitorCover`.
///
/// Go accepts an aggregate, a plain call, a cast, and a window function,
/// each carrying one value-expression argument, through both no-op
/// visitors. The same four families are walked here with a counting
/// visitor; enter and leave counts stay balanced and the walk reaches the
/// nested literal argument.
#[test]
fn functions_visitor_cover() {
    let value = int("42");
    let mut trees = [
        Expr::Aggregate {
            name: "SUM".to_string(),
            distinct: false,
            args: vec![value.clone()],
        },
        func("ABS", vec![value.clone()]),
        Expr::Cast(CastExpr {
            expr: Box::new(value.clone()),
            cast_type: CastType::Signed,
            style: CastStyle::Cast,
            array: false,
        }),
        rank_over_partition_a_with_arg(value),
    ];
    for tree in &mut trees {
        let mut counter = FuncCounter::default();
        assert!(tidb_ast::Visitable::accept(tree, &mut counter));
        assert!(counter.entered > 1, "expected nested visits");
        assert_eq!(counter.entered, counter.left);
        assert!(counter.deep_ints >= 1, "argument must be reached");
    }
}

fn rank_over_partition_a_with_arg(arg: Expr) -> Expr {
    window(
        "RANK",
        vec![arg],
        false,
        false,
        false,
        WindowOver::Def(WindowDef {
            base: None,
            spec: WindowSpec {
                partition_by: vec![column(&["a"])],
                order_by: Vec::new(),
                frame: None,
            },
        }),
    )
}

/// `pkg/parser/ast/functions_test.go::TestFuncCallExprRestore`.
#[test]
fn func_call_expr_restore() {
    let cases: Vec<(Expr, &str)> = vec![
        // JSON_*AGG parse into the aggregate family in Go as well.
        (
            Expr::Aggregate {
                name: "JSON_ARRAYAGG".to_string(),
                distinct: false,
                args: vec![column(&["attribute"])],
            },
            "JSON_ARRAYAGG(`attribute`)",
        ),
        (
            Expr::Aggregate {
                name: "JSON_OBJECTAGG".to_string(),
                distinct: false,
                args: vec![column(&["attribute"]), column(&["value"])],
            },
            "JSON_OBJECTAGG(`attribute`, `value`)",
        ),
        (
            func(
                "ABS",
                vec![Expr::Unary(tidb_ast::UnaryOp::Minus, Box::new(int("1024")))],
            ),
            "ABS(-1024)",
        ),
        (
            func("ACOS", vec![Expr::Decimal("3.14".to_string())]),
            "ACOS(3.14)",
        ),
        (func("CONV", vec![string("a"), int("16"), int("2")]), "CONV(_UTF8MB4'a', 16, 2)"),
        (func("COS", vec![func("PI", Vec::new())]), "COS(PI())"),
        (func("RAND", Vec::new()), "RAND()"),
        // ADDDATE's bare numeric interval is promoted to INTERVAL .. DAY at
        // parse time.
        (
            func("ADDDATE", vec![string("2000-01-01"), interval(int("1"), "DAY")]),
            "ADDDATE(_UTF8MB4'2000-01-01', INTERVAL 1 DAY)",
        ),
        (
            func(
                "DATE_ADD",
                vec![string("2000-01-01"), interval(int("1"), "DAY")],
            ),
            "DATE_ADD(_UTF8MB4'2000-01-01', INTERVAL 1 DAY)",
        ),
        (
            func(
                "DATE_ADD",
                vec![
                    string("2000-01-01"),
                    interval(string("1 1:12:23.100000"), "DAY_MICROSECOND"),
                ],
            ),
            "DATE_ADD(_UTF8MB4'2000-01-01', INTERVAL _UTF8MB4'1 1:12:23.100000' DAY_MICROSECOND)",
        ),
        (
            Expr::Extract {
                unit: "DAY".to_string(),
                value: Box::new(string("2000-01-01")),
            },
            "EXTRACT(DAY FROM _UTF8MB4'2000-01-01')",
        ),
        (
            Expr::Extract {
                unit: "DAY".to_string(),
                value: Box::new(string("1999-01-01")),
            },
            "EXTRACT(DAY FROM _UTF8MB4'1999-01-01')",
        ),
        (
            Expr::GetFormat {
                selector: GetFormatSelector::Date,
                expr: Box::new(string("EUR")),
            },
            "GET_FORMAT(DATE, _UTF8MB4'EUR')",
        ),
        (
            Expr::Position {
                substr: Box::new(string("a")),
                str: Box::new(string("abc")),
            },
            "POSITION(_UTF8MB4'a' IN _UTF8MB4'abc')",
        ),
        (
            Expr::Trim {
                expr: Box::new(string("  bar   ")),
                remstr: None,
                direction: None,
            },
            "TRIM(_UTF8MB4'  bar   ')",
        ),
        (
            Expr::Trim {
                expr: Box::new(string("  bar   ")),
                remstr: Some(Box::new(string("a"))),
                direction: None,
            },
            "TRIM(_UTF8MB4'a' FROM _UTF8MB4'  bar   ')",
        ),
        // A written direction without remstr defaults remstr to a
        // single-space string literal that RESTORES explicitly.
        (
            trim("  bar   ", " ", TrimDirection::Leading),
            "TRIM(LEADING _UTF8MB4' ' FROM _UTF8MB4'  bar   ')",
        ),
        (
            trim("  bar   ", " ", TrimDirection::Both),
            "TRIM(BOTH _UTF8MB4' ' FROM _UTF8MB4'  bar   ')",
        ),
        (
            trim("  bar   ", " ", TrimDirection::Trailing),
            "TRIM(TRAILING _UTF8MB4' ' FROM _UTF8MB4'  bar   ')",
        ),
        (
            trim("xxxyxxx", "x", TrimDirection::Leading),
            "TRIM(LEADING _UTF8MB4'x' FROM _UTF8MB4'xxxyxxx')",
        ),
        (
            trim("xxxyxxx", "x", TrimDirection::Both),
            "TRIM(BOTH _UTF8MB4'x' FROM _UTF8MB4'xxxyxxx')",
        ),
        (
            trim("xxxyxxx", "x", TrimDirection::Trailing),
            "TRIM(TRAILING _UTF8MB4'x' FROM _UTF8MB4'xxxyxxx')",
        ),
        (
            func(
                "DATE_ADD",
                vec![
                    string("2008-01-02"),
                    interval(
                        func("INTERVAL", vec![int("1"), int("0"), int("1")]),
                        "DAY",
                    ),
                ],
            ),
            "DATE_ADD(_UTF8MB4'2008-01-02', INTERVAL INTERVAL(1, 0, 1) DAY)",
        ),
        (
            func(
                "BENCHMARK",
                vec![
                    int("1000000"),
                    func(
                        "AES_ENCRYPT",
                        vec![
                            string("text"),
                            func("UNHEX", vec![string("F3229A0B371ED2D9441B830D21A390C3")]),
                        ],
                    ),
                ],
            ),
            "BENCHMARK(1000000, AES_ENCRYPT(_UTF8MB4'text', UNHEX(_UTF8MB4'F3229A0B371ED2D9441B830D21A390C3')))",
        ),
        // SUBSTRING .. FROM n [FOR m] restores as the comma form.
        (
            func("SUBSTRING", vec![string("Quadratically"), int("5")]),
            "SUBSTRING(_UTF8MB4'Quadratically', 5)",
        ),
        (
            func("SUBSTRING", vec![string("Quadratically"), int("5")]),
            "SUBSTRING(_UTF8MB4'Quadratically', 5)",
        ),
        (
            func("SUBSTRING", vec![string("Quadratically"), int("5"), int("6")]),
            "SUBSTRING(_UTF8MB4'Quadratically', 5, 6)",
        ),
        (
            func("SUBSTRING", vec![string("Quadratically"), int("5"), int("6")]),
            "SUBSTRING(_UTF8MB4'Quadratically', 5, 6)",
        ),
        (
            func("JSON_TYPE", vec![string("[123]")]),
            "JSON_TYPE(_UTF8MB4'[123]')",
        ),
        // `all c1`: ALL/DISTINCT markers collapse to plain form.
        (
            agg("BIT_AND", false, vec![column(&["c1"])]),
            "BIT_AND(`c1`)",
        ),
        (func("NEXTVAL", vec![column(&["seq"])]), "NEXTVAL(`seq`)"),
        (
            func("NEXTVAL", vec![column(&["test", "seq"])]),
            "NEXTVAL(`test`.`seq`)",
        ),
        (func("LASTVAL", vec![column(&["seq"])]), "LASTVAL(`seq`)"),
        (
            func("LASTVAL", vec![column(&["test", "seq"])]),
            "LASTVAL(`test`.`seq`)",
        ),
        (
            func("SETVAL", vec![column(&["seq"]), int("100")]),
            "SETVAL(`seq`, 100)",
        ),
        (
            func("SETVAL", vec![column(&["test", "seq"]), int("100")]),
            "SETVAL(`test`.`seq`, 100)",
        ),
        // `next value for seq` parses into the NEXTVAL function call.
        (func("NEXTVAL", vec![column(&["seq"])]), "NEXTVAL(`seq`)"),
        (
            func("NEXTVAL", vec![column(&["test", "seq"])]),
            "NEXTVAL(`test`.`seq`)",
        ),
        (
            func("NEXTVAL", vec![column(&["sequence"])]),
            "NEXTVAL(`sequence`)",
        ),
        (
            func("NEXTVAL", vec![column(&["seQuEncE2"])]),
            "NEXTVAL(`seQuEncE2`)",
        ),
        (
            func("NEXTVAL", vec![column(&["test", "seQuEncE2"])]),
            "NEXTVAL(`test`.`seQuEncE2`)",
        ),
        (
            Expr::WeightString {
                expr: Box::new(column(&["a"])),
                as_type: None,
            },
            "WEIGHT_STRING(`a`)",
        ),
        (
            Expr::WeightString {
                expr: Box::new(column(&["test", "a"])),
                as_type: None,
            },
            "WEIGHT_STRING(`test`.`a`)",
        ),
        (
            weight_str(string("a"), None),
            "WEIGHT_STRING(_UTF8MB4'a')",
        ),
        // Chained COLLATE keeps every collation in written order.
        (
            weight_str(
                Expr::Collate {
                    expr: Box::new(Expr::Collate {
                        expr: Box::new(string("a")),
                        collation: "utf8_general_ci".to_string(),
                    }),
                    collation: "utf8mb4_general_ci".to_string(),
                },
                None,
            ),
            "WEIGHT_STRING(_UTF8MB4'a' COLLATE utf8_general_ci COLLATE utf8mb4_general_ci)",
        ),
        (
            weight_str(
                collated(
                    Expr::CharsetString {
                        charset: "UTF8".to_string(),
                        value: "a".to_string(),
                    },
                    "utf8_general_ci",
                ),
                None,
            ),
            "WEIGHT_STRING(_UTF8'a' COLLATE utf8_general_ci)",
        ),
        (
            weight_str(
                Expr::CharsetString {
                    charset: "UTF8".to_string(),
                    value: "a".to_string(),
                },
                None,
            ),
            "WEIGHT_STRING(_UTF8'a')",
        ),
        (
            weight_str(column(&["a"]), Some((WeightStringType::Char, 5))),
            "WEIGHT_STRING(`a` AS CHAR(5))",
        ),
        (
            // CHARACTER(n) normalizes to CHAR(n).
            weight_str(column(&["a"]), Some((WeightStringType::Char, 5))),
            "WEIGHT_STRING(`a` AS CHAR(5))",
        ),
        (
            weight_str(column(&["a"]), Some((WeightStringType::Binary, 5))),
            "WEIGHT_STRING(`a` AS BINARY(5))",
        ),
        (
            func(
                "HEX",
                vec![weight_str(string("abc"), Some((WeightStringType::Binary, 5)))],
            ),
            "HEX(WEIGHT_STRING(_UTF8MB4'abc' AS BINARY(5)))",
        ),
        (func("SOUNDEX", vec![column(&["attr"])]), "SOUNDEX(`attr`)"),
        (
            func("SOUNDEX", vec![string("string")]),
            "SOUNDEX(_UTF8MB4'string')",
        ),
    ];
    // The BOTH `col1` FROM `col2` row needs column operands directly.
    let col_trim = Expr::Trim {
        expr: Box::new(column(&["col2"])),
        remstr: Some(Box::new(column(&["col1"]))),
        direction: Some(TrimDirection::Both),
    };
    assert_eq!(col_trim.restore(), "TRIM(BOTH `col1` FROM `col2`)");

    for (index, (expr, want)) in cases.into_iter().enumerate() {
        assert_eq!(expr.restore(), want, "case {index}");
    }
}

fn trim(expr: &str, remstr: &str, direction: TrimDirection) -> Expr {
    Expr::Trim {
        expr: Box::new(string(expr)),
        remstr: Some(Box::new(string(remstr))),
        direction: Some(direction),
    }
}

fn agg(name: &str, distinct: bool, args: Vec<Expr>) -> Expr {
    Expr::Aggregate {
        name: name.to_string(),
        distinct,
        args,
    }
}

fn weight_str(expr: Expr, as_type: Option<(WeightStringType, u64)>) -> Expr {
    Expr::WeightString {
        expr: Box::new(expr),
        as_type,
    }
}

fn collated(expr: Expr, collation: &str) -> Expr {
    Expr::Collate {
        expr: Box::new(expr),
        collation: collation.to_string(),
    }
}

/// `pkg/parser/ast/functions_test.go::TestFuncCastExprRestore`.
#[test]
fn func_cast_expr_restore() {
    let cast_expr = |expr: Expr, style: CastStyle, ty: CastType| Expr::Cast(CastExpr {
        expr: Box::new(expr),
        cast_type: ty,
        style,
        array: false,
    });
    let cases: [(Expr, &str); 5] = [
        (
            Expr::ConvertUsing {
                expr: Box::new(string("Müller")),
                charset: "utf8".to_string(),
            },
            "CONVERT(_UTF8MB4'Müller' USING 'utf8')",
        ),
        (
            Expr::ConvertUsing {
                expr: Box::new(string("Müller")),
                charset: "utf8mb4".to_string(),
            },
            "CONVERT(_UTF8MB4'Müller' USING 'utf8mb4')",
        ),
        (
            cast_expr(
                string("Müller"),
                CastStyle::Convert,
                CastType::Char {
                    len: Some(32),
                    charset: Some("UTF8".to_string()),
                },
            ),
            "CONVERT(_UTF8MB4'Müller', CHAR(32) CHARSET UTF8)",
        ),
        (
            cast_expr(
                string("test"),
                CastStyle::Cast,
                CastType::Char {
                    len: None,
                    charset: Some("UTF8".to_string()),
                },
            ),
            "CAST(_UTF8MB4'test' AS CHAR CHARSET UTF8)",
        ),
        (
            cast_expr(
                string("New York"),
                CastStyle::BinaryOperator,
                CastType::Binary { len: None },
            ),
            "BINARY _UTF8MB4'New York'",
        ),
    ];
    for (expr, want) in cases {
        assert_eq!(expr.restore(), want);
    }
}

/// `pkg/parser/ast/functions_test.go::TestAggregateFuncExprRestore`.
///
/// Name normalization happens at parse time (`STD`/`STDDEV` →
/// `STDDEV_POP`, `VARIANCE` → `VAR_POP`, duplicate-ALL collapse), so the
/// canonical names are stored exactly like Go's parsed states carry them.
/// The four GROUP_CONCAT rows follow below the aggregate table because the
/// crate gives them their own node shape.
#[test]
fn aggregate_func_expr_restore() {
    let score = || column(&["test_score"]);
    let cases: [(Expr, &str); 22] = [
        (agg("AVG", false, vec![score()]), "AVG(`test_score`)"),
        (agg("AVG", true, vec![score()]), "AVG(DISTINCT `test_score`)"),
        (agg("BIT_AND", false, vec![score()]), "BIT_AND(`test_score`)"),
        (agg("BIT_OR", false, vec![score()]), "BIT_OR(`test_score`)"),
        (agg("BIT_XOR", false, vec![score()]), "BIT_XOR(`test_score`)"),
        (agg("COUNT", false, vec![score()]), "COUNT(`test_score`)"),
        // COUNT(*) is modelled as COUNT(1).
        (agg("COUNT", false, vec![int("1")]), "COUNT(1)"),
        (
            agg(
                "COUNT",
                true,
                vec![column(&["scores"]), column(&["results"])],
            ),
            "COUNT(DISTINCT `scores`, `results`)",
        ),
        (agg("MIN", false, vec![score()]), "MIN(`test_score`)"),
        (agg("MIN", true, vec![score()]), "MIN(DISTINCT `test_score`)"),
        (agg("MAX", false, vec![score()]), "MAX(`test_score`)"),
        (agg("MAX", true, vec![score()]), "MAX(DISTINCT `test_score`)"),
        (agg("STDDEV_POP", false, vec![score()]), "STDDEV_POP(`test_score`)"),
        (agg("STDDEV_POP", false, vec![score()]), "STDDEV_POP(`test_score`)"),
        (agg("STDDEV_POP", false, vec![score()]), "STDDEV_POP(`test_score`)"),
        (agg("STDDEV_SAMP", false, vec![score()]), "STDDEV_SAMP(`test_score`)"),
        (agg("SUM", false, vec![score()]), "SUM(`test_score`)"),
        (agg("SUM", true, vec![score()]), "SUM(DISTINCT `test_score`)"),
        (agg("VAR_POP", false, vec![score()]), "VAR_POP(`test_score`)"),
        (agg("VAR_SAMP", false, vec![score()]), "VAR_SAMP(`test_score`)"),
        (agg("VAR_POP", false, vec![score()]), "VAR_POP(`test_score`)"),
        (
            agg(
                "JSON_OBJECTAGG",
                false,
                vec![column(&["test_score"]), column(&["results"])],
            ),
            "JSON_OBJECTAGG(`test_score`, `results`)",
        ),
    ];
    for (expr, want) in cases {
        assert_eq!(expr.restore(), want);
    }

    let order_b_c = vec![
        OrderItem {
            expr: column(&["b"]),
            desc: true,
        },
        OrderItem {
            expr: column(&["c"]),
            desc: false,
        },
    ];
    let group_cases: [(Vec<OrderItem>, Option<&str>, &str); 4] = [
        (Vec::new(), None, "GROUP_CONCAT(`a` SEPARATOR ',')"),
        (Vec::new(), Some("--"), "GROUP_CONCAT(`a` SEPARATOR '--')"),
        (
            order_b_c.clone(),
            None,
            "GROUP_CONCAT(`a` ORDER BY `b` DESC,`c` SEPARATOR ',')",
        ),
        (
            order_b_c,
            Some("--"),
            "GROUP_CONCAT(`a` ORDER BY `b` DESC,`c` SEPARATOR '--')",
        ),
    ];
    for (order_by, separator, want) in group_cases {
        let typed = TypedString {
            value: separator.unwrap_or(",").to_string(),
            charset: String::new(),
            collation: String::new(),
        };
        let expr = Expr::GroupConcat {
            distinct: false,
            args: vec![column(&["a"])],
            order_by,
            separator: typed,
        };
        assert_eq!(expr.restore(), want);
    }
}

// go-parity-gap: TestConvert and TestChar pin PARSER-side charset
// validation (`[parser:1115] Unknown character set: ...`) plus the exact
// value string stored on the extracted ValueExpr; those behaviors belong
// to tidb-parser's grammar actions, not this AST crate.
#[test]
#[ignore = "go-parity-gap: CONVERT charset validation ([parser:1115]) lives in tidb-parser grammar"]
fn convert_charset_validation() {}

#[test]
#[ignore = "go-parity-gap: CHAR charset validation ([parser:1115]) lives in tidb-parser grammar"]
fn char_charset_validation() {}

/// `pkg/parser/ast/functions_test.go::TestWindowFuncExprRestore`.
#[test]
fn window_func_expr_restore() {
    let partition_a_spec = || WindowSpec {
        partition_by: vec![column(&["a"])],
        order_by: Vec::new(),
        frame: None,
    };
    let named_base_w =
        || WindowOver::Def(WindowDef {
            base: Some("w".to_string()),
            spec: WindowSpec {
                partition_by: Vec::new(),
                order_by: Vec::new(),
                frame: None,
            },
        });
    let cases: [(Expr, &str); 10] = [
        (
            window("RANK", Vec::new(), false, false, false, WindowOver::Name("w".to_string())),
            "RANK() OVER `w`",
        ),
        (
            window(
                "RANK",
                Vec::new(),
                false,
                false,
                false,
                WindowOver::Def(WindowDef {
                    base: None,
                    spec: partition_a_spec(),
                }),
            ),
            "RANK() OVER (PARTITION BY `a`)",
        ),
        // DISTINCT / DISTINCTROW / DISTINCT ALL store the same flag.
        (
            window("MAX", vec![column(&["a"])], true, false, false, WindowOver::Def(WindowDef { base: None, spec: partition_a_spec() })),
            "MAX(DISTINCT `a`) OVER (PARTITION BY `a`)",
        ),
        (
            window("MAX", vec![column(&["a"])], true, false, false, WindowOver::Def(WindowDef { base: None, spec: partition_a_spec() })),
            "MAX(DISTINCT `a`) OVER (PARTITION BY `a`)",
        ),
        (
            window("MAX", vec![column(&["a"])], true, false, false, WindowOver::Def(WindowDef { base: None, spec: partition_a_spec() })),
            "MAX(DISTINCT `a`) OVER (PARTITION BY `a`)",
        ),
        // A bare ALL collapses away entirely.
        (
            window("MAX", vec![column(&["a"])], false, false, false, WindowOver::Def(WindowDef { base: None, spec: partition_a_spec() })),
            "MAX(`a`) OVER (PARTITION BY `a`)",
        ),
        (
            window(
                "FIRST_VALUE",
                vec![column(&["val"])],
                false,
                true,
                false,
                named_base_w(),
            ),
            "FIRST_VALUE(`val`) IGNORE NULLS OVER (`w`)",
        ),
        // RESPECT NULLS is the silent default.
        (
            window(
                "FIRST_VALUE",
                vec![column(&["val"])],
                false,
                false,
                false,
                WindowOver::Name("w".to_string()),
            ),
            "FIRST_VALUE(`val`) OVER `w`",
        ),
        (
            window("NTH_VALUE", vec![column(&["val"]), int("233")], false, true, true, WindowOver::Name("w".to_string())),
            "NTH_VALUE(`val`, 233) FROM LAST IGNORE NULLS OVER `w`",
        ),
        // `FROM FIRST` is the silent default.
        (
            window("NTH_VALUE", vec![column(&["val"]), int("233")], false, true, false, named_base_w()),
            "NTH_VALUE(`val`, 233) IGNORE NULLS OVER (`w`)",
        ),
    ];
    for (expr, want) in cases {
        assert_eq!(expr.restore(), want);
    }
}

/// `pkg/parser/ast/functions_test.go::TestGenericFuncRestore`.
///
/// Go's row `generic_func()` covers a bare non-reserved identifier call,
/// which this crate's grammar models only through the qualified generic
/// shape; the pinning rows here keep every schema-qualified spelling plus
/// the builtin NOW() contrast.
#[test]
fn generic_func_restore() {
    let cases: [(Expr, &str); 5] = [
        (generic("s", "a", Vec::new()), "`s`.`a`()"),
        (generic("s", "a", Vec::new()), "`s`.`a`()"),
        (func("NOW", Vec::new()), "NOW()"),
        (generic("s", "now", Vec::new()), "`s`.`now`()"),
        (generic("ident.1", "ident.2", Vec::new()), "`ident.1`.`ident.2`()"),
    ];
    for (expr, want) in cases {
        assert_eq!(expr.restore(), want);
    }
}

/// `pkg/parser/ast/functions_test.go::TestRestoreWithError`.
///
/// Go requires `Restore` to FAIL for `json_memberof()` whose two operand
/// children are nil. Rust's closed constructor set cannot represent missing
/// operands; the transcreated error boundary lives in `Expr::try_restore`,
/// which rejects a JSON_MEMBEROF call not carrying exactly two arguments.
#[test]
fn restore_with_error() {
    let error = func("json_memberof", Vec::new())
        .try_restore()
        .expect_err("JSON_MEMBEROF arity check must fail restore");
    assert_eq!(
        error,
        "Incorrect parameter count in the call to native function 'json_memberof'"
    );
}
