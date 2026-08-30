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

//! GO PORTS of `pkg/expression/constant_test.go` on `origin/master`:
//! `TestConstantFolding` (:198), `TestConstantFoldingCharsetConvert` (:274),
//! plus the deferred/propagation members whose carrier is absent.
//!
//! Go drives each condition through `FoldConstant(ctx, expr)` and asserts the
//! `StringWithCtx` rendering. The renderings embed the casts `NewFunction`
//! inference inserts (`cast(Column#0, double BINARY)`), so the ports assert
//! the FOLD ITSELF structurally: which node collapsed to which constant, and
//! which unfoldable function survived. Function construction goes through
//! [`crate::expr_util::RealFunctionBuilder`] -- the crate's full `NewFunction`
//! -- exactly where Go's test used `newFunction(ctx, ...)`.

use super::*;
use crate::column::Column;
use crate::constant::Constant;
use crate::evaluator::EvaluatorSuite;
use crate::expr_util::FunctionBuilder;
use crate::expr_util::{
    fold_constant_with, FoldOptions, PreservingFunctionBuilder, RealFunctionBuilder,
    SubstituteOptions,
};
use crate::expression::Expression;
use crate::scalar_function::ScalarFunction;
use tidb_ast::CiString;
use tidb_datatype::{FieldType, FieldTypeCode};

fn longlong_type() -> tidb_datatype::FieldType {
    FieldType::new(FieldTypeCode::LongLong)
}

fn varchar_type() -> tidb_datatype::FieldType {
    FieldType::new(FieldTypeCode::VarString)
}

/// Go `newColumn`: a LongLong column keyed by UniqueID.
fn column(unique_id: i64) -> Expression {
    Expression::Column(Column::new(unique_id, longlong_type()))
}

/// Go `newLonglong`.
fn int_const(value: i64) -> Expression {
    Expression::Constant(Constant::new(Datum::Int(value), longlong_type()))
}

/// Go `newFunctionWithMockCtx(funcName, args...)` built through the crate's
/// real `NewFunction` over the column-free context.
fn build(name: &str, args: Vec<Expression>) -> Expression {
    RealFunctionBuilder::new(&NoColumns)
        .new_function(name, None, args)
        .unwrap_or_else(|err| panic!("NewFunction({name}) must construct: {err:?}"))
}

/// Go `FoldConstant(ctx, expr)` over a freshly built tree.
fn fold(expr: &Expression) -> Expression {
    let opts = SubstituteOptions::new(&PreservingFunctionBuilder);
    crate::expr_util::fold_constant(expr, &NoColumns, &opts)
}

/// The integer a wholly-folded subtree collapsed to.
fn expect_int_constant<'a>(expr: &'a Expression, context: &str) -> i64 {
    match expr {
        Expression::Constant(constant) => match &constant.value {
            Datum::Int(value) => *value,
            other => panic!("{context}: expected INT constant, got {other:?}"),
        },
        other => panic!("{context}: expected a Constant, got {other:?}"),
    }
}

/// The scalar-function arm the fold kept alive.
fn expect_scalar_function<'a>(
    expr: &'a Expression,
    name: &str,
    arity: usize,
) -> &'a ScalarFunction {
    match expr {
        Expression::ScalarFunction(function) => {
            assert_eq!(function.func_name.lowercase(), name, "{name} root");
            assert_eq!(function.args.len(), arity, "{name} arity");
            function
        }
        other => panic!("expected {name} to survive folding, got {other:?}"),
    }
}

/// /// `pkg/expression/constant_test.go:198 TestConstantFolding` rows 1-2 and 6:
/// wholly-constant operator arguments collapse into one literal even when the
/// surrounding comparison keeps a column (`lt(col#0, 1+2)` -> `lt(col#0, 3)`,
/// `greatest(1,2)` -> `2`), and a partially-constant nested tree folds only
/// its innermost constant arm (`plus(col#1, 2+1)` -> `plus(col#1, 3)`).
#[test]
fn constant_folding_operator_arguments_reduce_in_place() {
    // lt(Column#0, plus(1, 2)) -> lt(Column#0, 3).
    let expr = build(
        "lt",
        vec![column(0), build("plus", vec![int_const(1), int_const(2)])],
    );
    let folded = fold(&expr);
    let root = expect_scalar_function(&folded, "lt", 2);
    assert!(
        matches!(&root.args[0], Expression::Column(_)),
        "column argument stays a column"
    );
    assert_eq!(expect_int_constant(&root.args[1], "plus(1,2)"), 3);

    // lt(Column#0, greatest(1, 2)) -> lt(Column#0, 2).
    let expr = build(
        "lt",
        vec![
            column(0),
            build("greatest", vec![int_const(1), int_const(2)]),
        ],
    );
    let folded = fold(&expr);
    let root = expect_scalar_function(&folded, "lt", 2);
    assert_eq!(expect_int_constant(&root.args[1], "greatest(1,2)"), 2);

    // lt(Column#0, plus(Column#1, plus(2, 1))) -> lt(Column#0, plus(Column#1, 3)).
    let expr = build(
        "lt",
        vec![
            column(0),
            build(
                "plus",
                vec![column(1), build("plus", vec![int_const(2), int_const(1)])],
            ),
        ],
    );
    let folded = fold(&expr);
    let root = expect_scalar_function(&folded, "lt", 2);
    let inner_arg = root.args[1].clone();
    let inner = expect_scalar_function(&inner_arg, "plus", 2);
    assert!(matches!(&inner.args[0], Expression::Column(_)));
    assert_eq!(expect_int_constant(&inner.args[1], "plus(2,1)"), 3);
}

/// Row 3 of `pkg/expression/constant_test.go:198 TestConstantFolding`:
/// `eq(Column#0, rand())` keeps RAND unfolded (Go's rendering
/// `eq(cast(Column#0, double BINARY), rand())`; the cast spelling is the
/// builder-inference facet recorded in this module's header).
#[test]
fn constant_folding_keeps_rand_unfolded() {
    let expr = build("eq", vec![column(0), build("rand", vec![])]);
    let folded = fold(&expr);
    let root = expect_scalar_function(&folded, "eq", 2);
    expect_scalar_function(&root.args[1].clone(), "rand", 0);
}

/// Rows 4-5 of `pkg/expression/constant_test.go:198 TestConstantFolding`:
/// `isnull(1)` folds to `0`, and `unarynot(plus(1,1))` inside `eq` folds to
/// `eq(Column#0, 0)`. (`not` is the registry spelling of Go's ast.UnaryNot.)
#[test]
fn constant_folding_isnull_and_unary_not_reduce() {
    // Rows 4-5 of the master table: isnull(1) folds to 0, and
    // not(plus(1,1)) inside eq folds to eq(Column#0, 0). `not` is Go's
    // ast.UnaryNot name in the registry.
    let expr = build("isnull", vec![int_const(1)]);
    assert_eq!(expect_int_constant(&fold(&expr), "isnull(1)"), 0);

    let expr = build(
        "eq",
        vec![
            column(0),
            build("not", vec![build("plus", vec![int_const(1), int_const(1)])]),
        ],
    );
    let folded = fold(&expr);
    let root = expect_scalar_function(&folded, "eq", 2);
    assert_eq!(expect_int_constant(&root.args[1], "not(2)"), 0);
}

/// Rows 7-8 of `pkg/expression/constant_test.go:198 TestConstantFolding`
/// (`nullRejectCheck` builds): `concat_ws(Column#0, NULL)` and
/// `field(Column#0, 0.0, NULL)` RETAIN their shape both inside and outside the
/// null-reject-check context -- neither folds to a bare NULL because a NULL
/// argument does not force a NULL outcome for these two functions.
#[test]
fn null_reject_conditions_survive_both_fold_modes() {
    let mut nullable_column = Column::new(0, varchar_type());
    nullable_column.index = 0;
    let concat_args = vec![
        Expression::Column(nullable_column.clone()),
        Expression::Constant(Constant::new(Datum::Null, varchar_type())),
    ];
    // Built manually rather than through NewFunction: CONCAT_WS's build-time
    // inference rewrites it to ETString internals that are not part of this
    // table's subject (the subject is what survives FoldConstant).
    let mut concat_ws_ft = varchar_type();
    concat_ws_ft.set_charset_name("binary");
    let concat_ws = |args: Vec<Expression>| -> Expression {
        Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("concat_ws"),
            concat_ws_ft.clone(),
            args,
        ))
    };

    for in_null_reject_check in [false, true] {
        let opts = FoldOptions {
            builder: &PreservingFunctionBuilder,
            use_plan_cache: false,
            in_null_reject_check,
        };
        let folded = fold_constant_with(&concat_ws(concat_args.clone()), &NoColumns, &opts);
        let root = expect_scalar_function(&folded, "concat_ws", 2);
        assert!(matches!(&root.args[0], Expression::Column(_)));
        assert!(
            matches!(&root.args[1], Expression::Constant(c) if c.value == Datum::Null),
            "the NULL separator argument must be retained verbatim"
        );
    }

    // field(Column#0, Double(0), NULL): same retention contract.
    let mut double_column = Column::new(
        0,
        tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::Double),
    );
    double_column.index = 0;
    let field_args = vec![
        Expression::Column(double_column),
        Expression::Constant(Constant::new(
            Datum::Real(0.0),
            tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::Double),
        )),
        Expression::Constant(Constant::new(Datum::Null, varchar_type())),
    ];
    let mut field_ft = longlong_type();
    field_ft.set_charset_name("binary");
    let field_expr = Expression::ScalarFunction(ScalarFunction::new(
        CiString::new("field"),
        field_ft,
        field_args,
    ));
    let opts = FoldOptions {
        builder: &PreservingFunctionBuilder,
        use_plan_cache: false,
        in_null_reject_check: true,
    };
    let folded = fold_constant_with(&field_expr, &NoColumns, &opts);
    let root = expect_scalar_function(&folded, "field", 3);
    assert!(matches!(&root.args[0], Expression::Column(_)));
    assert!(matches!(&root.args[2], Expression::Constant(c) if c.value == Datum::Null));
}

/// Go `newString`: a VARCHAR-typed constant tagged with an explicit
/// charset/collation and flen 255.
fn tagged_string_const(text: &str, charset: &str, collation: &str) -> Expression {
    let mut field_type = FieldType::new(FieldTypeCode::VarString);
    field_type.set_flen(255);
    field_type.set_charset_name(charset);
    field_type.set_collation_name(collation);
    Expression::Constant(Constant::new(Datum::new_string(text), field_type))
}

/// A VARBINARY-shaped `to_binary` / `from_binary` internal-transcode node,
/// spelled exactly like `pkg/expression/rewriter.go`'s `wrapBinaryLiterals`
/// and `pkg/expression/scalar_function.rs:1041`'s evaluator arm.
fn internal_binary_call(name: &str, target_charset: Option<&str>, arg: Expression) -> Expression {
    let mut ret_type = FieldType::new(FieldTypeCode::VarString);
    if let Some(charset) = target_charset {
        ret_type.set_charset_name(charset);
        // Match the FieldTypes Go's test builders produce for these targets.
        match charset {
            "gbk" => ret_type.set_collation_name("gbk_bin"),
            _ => ret_type.set_collation_name(format!("{charset}_bin")),
        }
    } else {
        ret_type.set_charset_name("binary");
        ret_type.set_collation_name("binary");
    }
    Expression::ScalarFunction(ScalarFunction::new(
        CiString::new(name),
        ret_type,
        vec![arg],
    ))
}

/// A BINARY-tagged string constant holding arbitrary bytes -- the shape a
/// KindString datum with the binary collation carries.
fn raw_binary_const(bytes: &[u8]) -> Expression {
    let mut field_type = FieldType::new(FieldTypeCode::VarString);
    field_type.set_flen(255);
    field_type.set_charset_name("binary");
    field_type.set_collation_name("binary");
    let value = Datum::new_collation_string(bytes.to_vec(), tidb_datatype::Collation::Binary);
    Expression::Constant(Constant::new(value, field_type))
}

fn folded_datum_of(expr: Expression) -> Datum {
    match fold(&expr) {
        Expression::Constant(constant) => constant.value,
        other => panic!("expected the fold to reach a Constant, got {other:?}"),
    }
}

/// `pkg/expression/constant_test.go:274 TestConstantFoldingCharsetConvert`:
/// folding sees THROUGH the internal `tidb_binary` transcodes the rewriter
/// plants. LENGTH of a GBK-tagged constant transcoded to binary counts GBK
/// bytes (`中文` -> 4), LENGTH after a utf8mb4 pass counts UTF-8 bytes (6),
/// FROM_BINARY into a character set turns opaque bytes back into text, and a
/// GBK constant concatenated with a BINARY constant yields BINARY bytes with
/// the GBK encoding of `中文` (`\xd6\xd0\xce\xc4`) -- NOT UTF-8.
///
/// Go builds every node through `NewFunctionInternal`, which folds AT
/// CONSTRUCTION bottom-up. The manual InternalFunc nodes therefore receive one
/// construction-time fold before entering an outer build call here too.
#[test]
fn constant_folding_sees_through_internal_charset_transcodes() {
    // Go's newFunctionWithType(InternalFunc*, ...) construction-time fold for
    // the internal transcode calls this table plants by hand.
    let construct_folded = |node: Expression| fold(&node);

    // length(to_binary('中文' @gbk_bin)) -> 4.
    let length_gbk = build(
        "length",
        vec![construct_folded(internal_binary_call(
            "to_binary",
            None,
            tagged_string_const("中文", "gbk", "gbk_bin"),
        ))],
    );
    assert_eq!(
        folded_datum_of(length_gbk),
        Datum::Int(4),
        "GBK encodes 中文 as two 2-byte characters"
    );

    // length(to_binary('中文' @utf8mb4_bin)) -> 6.
    let length_utf8 = build(
        "length",
        vec![construct_folded(internal_binary_call(
            "to_binary",
            None,
            tagged_string_const("中文", "utf8mb4", "utf8mb4_bin"),
        ))],
    );
    assert_eq!(folded_datum_of(length_utf8), Datum::Int(6));

    // concat(from_binary('中文' @binary)) -> '中文'.
    let plain_concat = build(
        "concat",
        vec![construct_folded(internal_binary_call(
            "from_binary",
            Some("utf8mb4"),
            tagged_string_const("中文", "binary", "binary"),
        ))],
    );
    assert_eq!(folded_datum_of(plain_concat), Datum::new_string("中文"));

    // concat(from_binary('\xd2\xbb' @binary -> gbk_bin flen -1), '中文' @gbk_bin)
    // -> '一中文'.
    // Rows 4/5 store Go's raw payload \xd2\xbb -- a VALID GBK byte pair whose
    // meaning under the gbk target charset is 一 -- as a BINARY-tagged value,
    // exactly what types.NewStringDatum("\xd2\xbb") holds.
    let gbk_from_bytes = {
        let mut ret_type = FieldType::new(FieldTypeCode::VarString);
        ret_type.set_flen(-1);
        ret_type.set_charset_name("gbk");
        ret_type.set_collation_name("gbk_bin");
        construct_folded(Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("from_binary"),
            ret_type,
            vec![raw_binary_const(&[0xd2, 0xbb])],
        )))
    };
    let leading_gbk = build(
        "concat",
        vec![
            gbk_from_bytes,
            tagged_string_const("中文", "gbk", "gbk_bin"),
        ],
    );
    assert_eq!(
        folded_datum_of(leading_gbk).label(),
        "STR:一中文",
        "from_binary contributes 一 ahead of the tagged GBK constant"
    );

    // ... and reversed argument order reads '中文一'.
    let trailing_gbk = build(
        "concat",
        vec![
            tagged_string_const("中文", "gbk", "gbk_bin"),
            construct_folded(internal_binary_call(
                "from_binary",
                Some("gbk"),
                raw_binary_const(&[0xd2, 0xbb]),
            )),
        ],
    );
    assert_eq!(folded_datum_of(trailing_gbk).label(), "STR:中文一");
}

/// Master row 6 of `pkg/expression/constant_test.go:274
/// TestConstantFoldingCharsetConvert`: `concat('中文' @gbk_bin,
/// '\xd2\xbb' @binary)` must yield BINARY bytes [D6,D0,CE,C4,D2,BB] -- the
/// character-set half transcoded to ITS OWN encoding. This crate's CONCAT
/// keeps the UTF-8 payload ([228,184,173,230,150,135,210,187]) instead.
#[test]
#[ignore = "go-parity-gap: folded CONCAT over a GBK-tagged constant plus a BINARY constant keeps UTF-8 bytes instead of transcoding the character half to its own encoding"]
fn constant_folding_charset_binary_result_diverges() {}

/// First two tables of `pkg/expression/constant_test.go:72
/// TestConstantPropagation`: constants walk through an equality class and an
/// unrelated predicate remains unchanged.
#[test]
fn test_constant_propagation() {
    let eq = |left, right| build("eq", vec![left, right]);
    fn shape(expression: &Expression) -> String {
        match expression {
            Expression::Column(column) => format!("c{}", column.unique_id),
            Expression::Constant(constant) => match constant.value {
                Datum::Int(value) => value.to_string(),
                Datum::UInt(value) => value.to_string(),
                Datum::Null => "null".to_owned(),
                ref value => format!("{value:?}"),
            },
            Expression::ScalarFunction(function) => format!(
                "{}({})",
                function.func_name.lowercase(),
                function
                    .get_args()
                    .iter()
                    .map(shape)
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            Expression::CorrelatedColumn(column) => format!("cor{}", column.column.unique_id),
        }
    }
    fn solve(builder: &dyn FunctionBuilder, conditions: Vec<Expression>) -> Vec<String> {
        let mut result =
            crate::constant_propagation::propagate_constant(builder, false, conditions, None)
                .conditions
                .iter()
                .map(shape)
                .collect::<Vec<_>>();
        result.sort();
        result
    }
    let conditions = vec![
        eq(column(0), column(1)),
        eq(column(1), column(2)),
        eq(column(2), column(3)),
        eq(column(3), int_const(1)),
        build("or", vec![int_const(1), column(0)]),
    ];
    let builder = RealFunctionBuilder::new(&NoColumns);
    let result = crate::constant_propagation::propagate_constant(&builder, false, conditions, None);
    assert_eq!(result.conditions.len(), 5);
    assert!(result.conditions.iter().any(
        |condition| matches!(condition, Expression::Constant(constant) if matches!(constant.value, Datum::Int(1)))
    ));
    for id in 0..4 {
        assert!(result.conditions.iter().any(|condition| {
            let Expression::ScalarFunction(function) = condition else {
                return false;
            };
            matches!(
                function.get_args(),
                [Expression::Column(column), Expression::Constant(constant)]
                    if function.func_name.lowercase() == "eq"
                        && column.unique_id == id
                        && matches!(constant.value, Datum::Int(1))
            )
        }));
    }

    let unrelated = build("ne", vec![column(2), int_const(2)]);
    let result = crate::constant_propagation::propagate_constant(
        &builder,
        false,
        vec![
            eq(column(0), column(1)),
            eq(column(1), int_const(1)),
            unrelated.clone(),
        ],
        None,
    );
    assert!(result
        .conditions
        .iter()
        .any(|condition| condition.equal(&unrelated)));

    let scalar = |name: &str, left, right| build(name, vec![left, right]);
    assert_eq!(
        solve(
            &builder,
            vec![
                eq(column(0), column(1)),
                eq(column(1), int_const(1)),
                eq(column(2), column(3)),
                scalar("ge", column(2), int_const(2)),
                scalar("ne", column(2), int_const(4)),
                scalar("ne", column(3), int_const(5)),
            ],
        ),
        [
            "eq(c0,1)",
            "eq(c1,1)",
            "eq(c2,c3)",
            "ge(c2,2)",
            "ge(c3,2)",
            "ne(c2,4)",
            "ne(c2,5)",
            "ne(c3,4)",
            "ne(c3,5)",
        ]
    );
    assert_eq!(
        solve(
            &builder,
            vec![
                eq(column(0), column(1)),
                eq(column(0), column(2)),
                scalar("ge", column(1), int_const(0)),
            ],
        ),
        ["eq(c0,c1)", "eq(c0,c2)", "ge(c0,0)", "ge(c1,0)", "ge(c2,0)",]
    );
    assert_eq!(
        solve(
            &builder,
            vec![
                eq(column(0), column(1)),
                scalar("gt", column(0), int_const(2)),
                scalar("gt", column(1), int_const(3)),
                scalar("lt", column(0), int_const(1)),
                scalar("gt", int_const(2), column(1)),
            ],
        ),
        [
            "eq(c0,c1)",
            "gt(2,c0)",
            "gt(2,c1)",
            "gt(c0,2)",
            "gt(c0,3)",
            "gt(c1,2)",
            "gt(c1,3)",
            "lt(c0,1)",
            "lt(c1,1)",
        ]
    );
    assert_eq!(
        solve(&builder, vec![eq(int_const(1), column(0)), int_const(0)]),
        ["0"]
    );
    assert_eq!(
        solve(
            &builder,
            vec![
                eq(column(0), column(1)),
                build("in", vec![column(0), int_const(1), int_const(2)]),
                build("in", vec![column(1), int_const(3), int_const(4)]),
            ],
        ),
        [
            "eq(c0,c1)",
            "in(c0,1,2)",
            "in(c0,3,4)",
            "in(c1,1,2)",
            "in(c1,3,4)",
        ]
    );
}

#[test]
fn test_constant_propagation_for_outer_join() {
    let builder = RealFunctionBuilder::new(&NoColumns);
    let outer = crate::schema::Schema::new(vec![Column::new(0, longlong_type())]);
    let inner = crate::schema::Schema::new(vec![Column::new(1, longlong_type())]);
    let eq = build("eq", vec![column(0), column(1)]);
    let outer_filter = build("gt", vec![column(0), int_const(1)]);
    let result = crate::constant_propagation::propagate_constant_for_outer_join(
        &builder,
        false,
        vec![eq.clone()],
        vec![outer_filter.clone()],
        &outer,
        &inner,
        true,
        false,
        None,
    );
    assert!(result
        .filter_conditions
        .iter()
        .any(|item| item.equal(&outer_filter)));
    assert!(result.join_conditions.iter().any(|item| item.equal(&eq)));
    assert!(result.join_conditions.iter().any(|condition| {
        let Expression::ScalarFunction(function) = condition else {
            return false;
        };
        matches!(
            function.get_args(),
            [Expression::Column(column), Expression::Constant(constant)]
                if function.func_name.lowercase() == "gt"
                    && column.unique_id == 1
                    && matches!(constant.value, Datum::Int(1))
        )
    }));
    assert!(result.join_conditions.iter().any(|condition| {
        let Expression::ScalarFunction(function) = condition else {
            return false;
        };
        function.func_name.lowercase() == "not"
    }));

    let sensitive = crate::constant_propagation::propagate_constant_for_outer_join(
        &builder,
        false,
        vec![eq],
        vec![outer_filter],
        &outer,
        &inner,
        true,
        true,
        None,
    );
    assert_eq!(sensitive.join_conditions.len(), 1);

    // Go's disjoint set derives through the complete equality class, not only
    // through an equality that directly names the source column.
    let outer = crate::schema::Schema::new(vec![
        Column::new(0, longlong_type()),
        Column::new(2, longlong_type()),
    ]);
    let inner = crate::schema::Schema::new(vec![
        Column::new(1, longlong_type()),
        Column::new(3, longlong_type()),
    ]);
    let transitive = crate::constant_propagation::propagate_constant_for_outer_join(
        &builder,
        false,
        vec![
            build("eq", vec![column(0), column(1)]),
            build("eq", vec![column(2), column(1)]),
            build("eq", vec![column(2), column(3)]),
        ],
        vec![build("gt", vec![column(0), int_const(5)])],
        &outer,
        &inner,
        true,
        false,
        None,
    );
    assert!(transitive.join_conditions.iter().any(|condition| {
        let Expression::ScalarFunction(function) = condition else {
            return false;
        };
        matches!(
            function.get_args(),
            [Expression::Column(column), Expression::Constant(constant)]
                if function.func_name.lowercase() == "gt"
                    && column.unique_id == 3
                    && matches!(constant.value, Datum::Int(5))
        )
    }));
}

/// `pkg/expression/constant_test.go:336 TestDeferredParamNotNull` reads
/// `PlanCacheParams.GetParamValue(order)` through each typed evaluator.
/// Rust's cache owner refreshes `Constant.value` before construction and
/// `Constant::eval` now reads that current value. The per-EvalType table is
/// still pending because this crate exposes a single Datum evaluator here.
#[test]
#[ignore = "go-parity-gap: the eleven typed EvalInt/EvalReal/EvalDecimal/EvalString/EvalTime/EvalDuration/EvalJSON/EvalVectorFloat32 rows are not exposed as separate Rust evaluator APIs"]
fn test_deferred_param_not_null() {}

/// Typed-evaluation half of `pkg/expression/constant_test.go:403
/// TestDeferredExprNotNull`: a deferred expression surfaces its inner error
/// through every `EvalXxx`, collapses to NULL when the inner expression does,
/// and forwards the inner value otherwise. Go injects those behaviors with a
/// hand-written `MockExpr`; this crate evaluates deferred constants by
/// evaluating REAL expressions (see below) and has no error-valued datum, so
/// the injected-error rows remain boundary evidence.
///
/// The CLONE half IS portable: `cst.Clone()` keeps the SAME DeferredExpr, and
/// that identity drives hash/equality (`optional_expression_equals`).
#[test]
fn deferred_constant_clone_preserves_the_deferred_expression() {
    fn int_field_type() -> tidb_datatype::FieldType {
        tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong)
    }
    // Two distinct deferred trees...
    let inner_a = build("rand", vec![]);
    let inner_b = build("plus", vec![int_const(1), int_const(2)]);
    // The hashcode cache is crate-private, so grow the constant through the
    // public constructor and then plant the deferred expression.
    let const_with = |inner: Expression| {
        let mut constant = Constant::new(Datum::Null, int_field_type());
        constant.deferred_expr = Some(Box::new(inner));
        constant
    };
    let cst = const_with(inner_a.clone());
    // ...clone keeps the same deferred expression...
    let clone = cst.clone();
    assert!(clone.deferred_expr.is_some());
    assert!(cst.equals(&clone));
    assert_eq!(cst.hash64(), clone.hash64());
    // ...a DIFFERENT deferred expression breaks both hashes and equality...
    let different = const_with(inner_b);
    assert_ne!(cst.hash64(), different.hash64());
    assert!(!cst.equals(&different));
    // ...and the deferred branch dominates the stored value, exactly as the
    // hash treats a deferred constant as its expression (`hash_constant`).
    assert_ne!(
        cst.hash64(),
        Constant::new(Datum::Int(9), int_field_type()).hash64()
    );
}

/// Value-forwarding half of `pkg/expression/constant_test.go:403
/// TestDeferredExprNotNull`: reading a deferred constant evaluates the INNER
/// expression against the live context (`2333`, `'abc'`, decimal/time/duration/
/// JSON forwards in the source table) instead of returning the placeholder.
/// Those MockExpr seams stay Go-side; nothing here pretends otherwise.
#[test]
#[ignore = "go-parity-gap: deferred-constant EvalXxx forwarding needs an error-valued/mock expression seam; evaluation is reported Unsupported today"]
fn test_deferred_expr_not_null() {}

/// `pkg/expression/constant_test.go:533 TestGetTypeThreadSafe`: calling
/// `GetType` twice on a ParamMarker-backed constant must return independent
/// FieldType values (Go `require.NotSame`). `Constant.get_static_type` in this
/// crate returns the STORED type and defers param-marker inference, so the
/// allocation-freshness contract has no carrier.
#[test]
#[ignore = "go-parity-gap: param-marker GetType inference is deferred in constant.rs; no per-call FieldType derivation exists to prove thread safety on"]
fn test_get_type_thread_safe() {}

/// `pkg/expression/constant_test.go:478 TestVectorizedConstant`: a literal
/// Constant fills a whole output chunk -- 1024 INT rows and 1024 VARCHAR rows
/// all read back the same value, through the `EvaluatorSuite` Go drives the
/// same way. The `SetSel([2,3,5,...])` halves of each loop need
/// selection-buffer support this crate's chunk evaluator does not carry; the
/// deferred-literal halves are covered by the ignored sibling below.
#[test]
fn vectorized_constant_fills_whole_output_chunks() {
    use tidb_chunk::chunk::Chunk;

    let rows = 1024;
    let int_type = tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
    let string_type = tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);

    // Fixed-length type: int 2333 over 1024 integer input rows.
    for expression in [Expression::Constant(Constant::new(
        Datum::Int(2333),
        int_type.clone(),
    ))] {
        let mut input = Chunk::new_with_capacity(std::slice::from_ref(&int_type), rows);
        for row in 0..rows as i64 {
            input.append_datum(0, &Datum::Int(row));
        }
        let mut output = Chunk::new_with_capacity(std::slice::from_ref(&int_type), rows);
        EvaluatorSuite::new(vec![expression], false)
            .run(&NoColumns, &mut input, &mut output)
            .expect("constant vector fill");
        assert_eq!(output.num_rows(), rows, "every input row is answered");
        for row in 0..rows {
            assert_eq!(
                output.get_row(row).get_datum(0, &int_type),
                Datum::Int(2333),
                "row {row}"
            );
        }
    }

    // Var-length type with/without Sel: 'hello' over 1024 string cells.
    let expression = Expression::Constant(Constant::new(
        Datum::new_string("hello"),
        string_type.clone(),
    ));
    let int_input_type = tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
    let mut input = Chunk::new_with_capacity(std::slice::from_ref(&int_input_type), rows);
    for row in 0..rows as i64 {
        input.append_datum(0, &Datum::Int(row));
    }
    let mut output = Chunk::new_with_capacity(std::slice::from_ref(&string_type), rows);
    EvaluatorSuite::new(vec![expression], false)
        .run(&NoColumns, &mut input, &mut output)
        .expect("string constant vector fill");
    for row in 0..rows {
        assert_eq!(
            output.get_row(row).get_datum(0, &string_type),
            Datum::new_string("hello"),
            "row {row}"
        );
    }
}

/// Deferred-literal halves of `pkg/expression/constant_test.go:478
/// TestVectorizedConstant`: `{RetType: newIntFieldType(), DeferredExpr:
/// &Constant{...2333}}` must fill the output chunk with 2333 exactly like its
/// literal sibling, for fixed-length AND var-length types. Reading a deferred
/// constant is reported Unsupported by `constant.rs` today, so the fill
/// cannot be driven; nothing here fakes a value.
#[test]
#[ignore = "go-parity-gap: Constant-with-DeferredExpr evaluation is unported; parameter-marker constants are evaluated through their live marker values"]
fn vectorized_constant_deferred_forms_fill_like_literals() {}
