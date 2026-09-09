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

//! Tests for [`super`].
//!
//! Tests labelled GO PORT come from `pkg/expression/util_test.go`.
//! Their expression-building helpers use [`PreservingFunctionBuilder`]
//! instead of Go's `newFunctionWithMockCtx`.
//!
//! Tree equality is asserted through `HashCode`, not `Expression::equal`:
//! `expression.rs` documents the latter as context-free, reporting `false` for
//! two constants and for two scalar functions, which would make most of these
//! assertions vacuous. `HashCode` is structural and context-free by
//! construction, which is exactly what these shape assertions need.

use super::builder::PreservingFunctionBuilder;
use super::*;
use crate::column::{Column, CorrelatedColumn};
use crate::constant::Constant;
use crate::context::NoColumns;
use crate::expression::Expression;
use crate::scalar_function::ScalarFunction;
use tidb_ast::CiString;
use tidb_datatype::{Datum, FieldType, FieldTypeCode, FieldTypeFlags};

fn int_type() -> FieldType {
    FieldType::new(FieldTypeCode::LongLong)
}

fn string_type() -> FieldType {
    FieldType::new(FieldTypeCode::VarString)
}

/// Go `newColumn(id)`: a column whose `UniqueID` and `Index` are both `id`.
fn col(id: i64) -> Expression {
    let mut column = Column::new(id, int_type());
    column.index = id;
    column.orig_name = format!("t.c{id}");
    Expression::Column(column)
}

fn cor_col(id: i64, data: Option<Datum>) -> Expression {
    let mut column = Column::new(id, int_type());
    column.index = id;
    let correlated = match data {
        Some(value) => CorrelatedColumn::with_value(column, value),
        None => CorrelatedColumn { column, data: None },
    };
    Expression::CorrelatedColumn(correlated)
}

/// Go `newLonglong(v)`.
fn int_const(v: i64) -> Expression {
    Expression::Constant(Constant::new(Datum::Int(v), int_type()))
}

fn str_const(v: &str) -> Expression {
    Expression::Constant(Constant::new(
        Datum::Bytes(v.as_bytes().to_vec()),
        string_type(),
    ))
}

/// Go `newFunctionWithMockCtx(name, args...)`, minus the type inference the
/// real `NewFunction` performs -- see this module's header.
fn func(name: &str, args: Vec<Expression>) -> Expression {
    Expression::ScalarFunction(ScalarFunction::new(CiString::new(name), int_type(), args))
}

fn shape(expr: &Expression) -> Vec<u8> {
    expr.clone().hash_code().to_vec()
}

fn same_shape(left: &Expression, right: &Expression) -> bool {
    shape(left) == shape(right)
}

fn opts<'a>(builder: &'a PreservingFunctionBuilder) -> SubstituteOptions<'a> {
    SubstituteOptions::new(builder)
}

/// GO PORT of `TestFilterOutInPlace` (`util_test.go:344`).
#[test]
fn go_filter_out_in_place() {
    let conditions = vec![
        func("eq", vec![col(0), col(1)]),
        func("eq", vec![col(1), col(2)]),
        func("or", vec![int_const(1), col(0)]),
    ];
    let is_logic_or = |e: &Expression| matches!(e, Expression::ScalarFunction(f) if f.func_name.lowercase() == "or");
    let (remained, filtered) = filter_out_in_place(conditions, &is_logic_or);
    assert_eq!(remained.len(), 2);
    for expr in &remained {
        let Expression::ScalarFunction(f) = expr else {
            panic!("expected a scalar function")
        };
        assert_eq!(f.func_name.lowercase(), "eq");
    }
    assert_eq!(filtered.len(), 1);
    let Expression::ScalarFunction(f) = &filtered[0] else {
        panic!("expected a scalar function")
    };
    assert_eq!(f.func_name.lowercase(), "or");
}

/// GO PORT of `TestSetExprColumnInOperand` (`util_test.go:190`).
#[test]
fn go_set_expr_column_in_operand() {
    let marked = set_expr_column_in_operand(col(0));
    assert!(marked.as_column().expect("a column").in_operand);

    let fun = set_expr_column_in_operand(func("abs", vec![col(0)]));
    let Expression::ScalarFunction(f) = &fun else {
        panic!("expected a scalar function")
    };
    assert!(
        f.get_args()[0]
            .as_column()
            .expect("the argument is a column")
            .in_operand
    );
}

/// GO PORT of `TestSubstituteCorCol2Constant` (`util_test.go:240`), reduced to
/// the parts that do not need the real `NewFunction`.
///
/// Go builds `((cast(corCol1) + corCol2) + 1)` from two correlated columns
/// bound to 1 and asserts it folds to the constant 3. That fold needs
/// arithmetic evaluation, which this crate has; what it does NOT have is Go's
/// cast construction, so the `cast` wrapper is dropped and the sum is over the
/// two correlated columns directly.
#[test]
fn go_substitute_cor_col_2_constant() {
    let builder = PreservingFunctionBuilder;
    let opts = opts(&builder);
    let cor1 = cor_col(1, Some(Datum::Int(1)));
    let cor2 = cor_col(2, Some(Datum::Int(1)));
    let plus = func("plus", vec![cor1, cor2]);
    let plus2 = func("plus", vec![plus, int_const(1)]);

    let result = substitute_cor_col_2_constant(&plus2, &NoColumns, &opts)
        .expect("a wholly correlated tree folds");
    let Expression::Constant(constant) = &result else {
        panic!("expected the tree to fold to a constant, got {result:?}")
    };
    assert_eq!(constant.value, Datum::Int(3));

    // A plain column is returned unchanged -- it is not correlated.
    let column = col(1);
    let result = substitute_cor_col_2_constant(&column, &NoColumns, &opts).expect("no failure");
    assert!(same_shape(&result, &column));

    // With a real column mixed in, the tree cannot fold to a constant.
    let plus3 = func("plus", vec![plus2, col(1)]);
    let result = substitute_cor_col_2_constant(&plus3, &NoColumns, &opts).expect("no failure");
    assert!(matches!(result, Expression::ScalarFunction(_)));
}

/// GO PORT of `TestPopRowFirstArg` (`util_test.go:202`).
#[test]
fn go_pop_row_first_arg() {
    let builder = PreservingFunctionBuilder;
    let opts = opts(&builder);
    let row = func("row", vec![col(1), col(2), col(3)]);
    let popped = pop_row_first_arg(&row, &opts)
        .expect("no build failure")
        .expect("a row pops");
    let Expression::ScalarFunction(f) = &popped else {
        panic!("expected a scalar function")
    };
    assert_eq!(f.get_args().len(), 2);
}

/// GO PORT of `TestPushDownNot` (`util_test.go:265`), first case:
/// `!((a=1||a=1)&&a=1)` becomes `(a!=1&&a!=1)||a!=1`.
///
/// Go's expected value reads `orFunc2 = (a!=1&&a!=1)||a!=1`; the comment above
/// it says `||a=1`, but the code builds `neFunc`.
#[test]
fn go_push_down_not_de_morgan() {
    let builder = PreservingFunctionBuilder;
    let eq = func("eq", vec![col(1), int_const(1)]);
    let or = func("or", vec![eq.clone(), eq.clone()]);
    let and = func("and", vec![or, eq]);
    let not = func("not", vec![and]);
    let original = not.clone();

    let ne = func("ne", vec![col(1), int_const(1)]);
    let and2 = func("and", vec![ne.clone(), ne.clone()]);
    let expected = func("or", vec![and2, ne]);

    let result = push_down_not(&not, &builder);
    assert!(
        same_shape(&result, &expected),
        "expected De Morgan's law to flip the tree"
    );
    // Go asserts the input is not mutated.
    assert!(same_shape(&not, &original));
}

/// GO PORT of `TestPushDownNot`, the double-negation cases (issue 15725).
#[test]
fn go_push_down_not_double_negation() {
    let builder = PreservingFunctionBuilder;

    // `not not (a=1)` optimizes to `a=1`.
    let eq = func("eq", vec![col(1), int_const(1)]);
    let not2 = func("not", vec![func("not", vec![eq.clone()])]);
    assert!(same_shape(&push_down_not(&not2, &builder), &eq));

    // `not not not (a > 1)` optimizes to `a <= 1`.
    let gt = func("gt", vec![col(1), int_const(1)]);
    let not3 = func("not", vec![func("not", vec![func("not", vec![gt])])]);
    let expected = func("le", vec![col(1), int_const(1)]);
    assert!(same_shape(&push_down_not(&not3, &builder), &expected));

    // `not not not not (a <= 1)` optimizes back to `a <= 1`.
    let le = func("le", vec![col(1), int_const(1)]);
    let mut not4 = le.clone();
    for _ in 0..4 {
        not4 = func("not", vec![not4]);
    }
    assert!(same_shape(&push_down_not(&not4, &builder), &le));
}

/// GO PORT of `TestPushDownNot`, the cases where the truth wrapper survives:
/// `not not a` becomes `a is true`, because `a` is not itself a truth value.
#[test]
fn go_push_down_not_wraps_non_predicate_in_is_true() {
    let builder = PreservingFunctionBuilder;

    let not2 = func("not", vec![func("not", vec![col(1)])]);
    let expected = func("istrue_with_null", vec![col(1)]);
    assert!(same_shape(&push_down_not(&not2, &builder), &expected));

    // `not not not a` becomes `not (a is true)`.
    let not3 = func("not", vec![func("not", vec![func("not", vec![col(1)])])]);
    let expected = func("not", vec![func("istrue_with_null", vec![col(1)])]);
    assert!(same_shape(&push_down_not(&not3, &builder), &expected));
}

/// GO PORT of `TestDisableParseJSONFlag4Expr` (`util_test.go:400`).
#[test]
fn go_disable_parse_json_flag_4_expr() {
    // A column keeps the flag: its RetType points into the infoschema.
    let mut column = Column::new(1, int_type());
    column
        .ret_type
        .as_mut()
        .expect("a typed column")
        .add_flags(FieldTypeFlags::PARSE_TO_JSON);
    let mut expr = Expression::Column(column);
    disable_parse_json_flag_4_expr(&mut expr);
    assert!(
        expr.static_type().expect("a type").flags() & FieldTypeFlags::PARSE_TO_JSON != 0,
        "a column keeps the flag"
    );

    // A correlated column keeps it too.
    let mut column = Column::new(1, int_type());
    column
        .ret_type
        .as_mut()
        .expect("a typed column")
        .add_flags(FieldTypeFlags::PARSE_TO_JSON);
    let mut expr = Expression::CorrelatedColumn(CorrelatedColumn {
        column,
        data: Default::default(),
    });
    disable_parse_json_flag_4_expr(&mut expr);
    assert!(expr.static_type().expect("a type").flags() & FieldTypeFlags::PARSE_TO_JSON != 0);

    // A scalar function loses it.
    let mut ret_type = int_type();
    ret_type.add_flags(FieldTypeFlags::PARSE_TO_JSON);
    let mut expr = Expression::ScalarFunction(ScalarFunction::new(
        CiString::new("abs"),
        ret_type,
        vec![col(1)],
    ));
    disable_parse_json_flag_4_expr(&mut expr);
    assert!(expr.static_type().expect("a type").flags() & FieldTypeFlags::PARSE_TO_JSON == 0);
}

/// GO PORT of `TestGetUint64FromConstant` (`util_test.go:155`), minus the
/// `ParamMarker` case -- see the narrowing on
/// [`super::predicates::get_uint64_from_constant`].
#[test]
fn go_get_uint64_from_constant() {
    let null = Expression::Constant(Constant::new(Datum::Null, int_type()));
    assert_eq!(get_uint64_from_constant(&null, &NoColumns), Some((0, true)));

    // A negative signed value is not usable.
    let negative = Expression::Constant(Constant::new(Datum::Int(-1), int_type()));
    assert_eq!(get_uint64_from_constant(&negative, &NoColumns), None);

    let one = Expression::Constant(Constant::new(Datum::Int(1), int_type()));
    assert_eq!(get_uint64_from_constant(&one, &NoColumns), Some((1, false)));

    let unsigned = Expression::Constant(Constant::new(Datum::UInt(1), int_type()));
    assert_eq!(
        get_uint64_from_constant(&unsigned, &NoColumns),
        Some((1, false))
    );

    // A deferred expression is evaluated.
    let mut deferred = Constant::new(Datum::Null, int_type());
    deferred.deferred_expr = Some(Box::new(int_const(1)));
    assert_eq!(
        get_uint64_from_constant(&Expression::Constant(deferred), &NoColumns),
        Some((1, false))
    );
}

/// GO PORT of `TestGetStrIntFromConstant` (`util_test.go:213`).
#[test]
fn go_get_str_int_from_constant() {
    // A non-constant is an error.
    assert!(get_string_from_constant(&col(1), &NoColumns).is_err());

    // NULL reads as "is null".
    let null = Expression::Constant(Constant::new(
        Datum::Null,
        FieldType::new(FieldTypeCode::Null),
    ));
    assert_eq!(
        get_string_from_constant(&null, &NoColumns).expect("no error"),
        None
    );

    assert_eq!(
        get_string_from_constant(&int_const(1), &NoColumns).expect("no error"),
        Some("1".to_owned())
    );

    // `GetIntFromConstant` goes through the string form, so a non-numeric
    // string is "is null" and NOT an error.
    assert_eq!(
        get_int_from_constant(&str_const("abc"), &NoColumns).expect("no error"),
        None
    );
    assert_eq!(
        get_int_from_constant(&str_const("123"), &NoColumns).expect("no error"),
        Some(123)
    );
}

/// GO PORT of `TestProjectionBenefitsFromPushedDown` (`util_test.go:483`).
#[test]
fn go_projection_benefits_from_pushed_down() {
    // Pure column refs benefit only when they PRUNE.
    assert!(projection_benefits_from_pushed_down(&[col(0), col(1)], 5));
    assert!(!projection_benefits_from_pushed_down(&[col(0), col(1)], 2));

    // The JSON functions TiKV evaluates well.
    let json_exprs = vec![
        col(0),
        func("json_extract", vec![col(1), str_const("$.a")]),
        func("json_depth", vec![col(1)]),
        func("json_length", vec![col(1)]),
        func("json_type", vec![col(1)]),
        func("json_valid", vec![col(1)]),
        func("json_contains", vec![col(1), str_const("1")]),
        func("json_contains_path", vec![col(1), str_const("one")]),
        func("json_keys", vec![col(1)]),
        func("json_search", vec![col(1), str_const("one")]),
        func("json_memberof", vec![str_const("1"), col(1)]),
        func("json_overlaps", vec![col(1), col(2)]),
    ];
    assert!(projection_benefits_from_pushed_down(&json_exprs, 3));

    // A bare JSON_UNQUOTE does not benefit...
    let bare_unquote = vec![func("json_unquote", vec![col(1)])];
    assert!(!projection_benefits_from_pushed_down(&bare_unquote, 3));

    // ...but the `->>` spelling does.
    let arrow = vec![func(
        "json_unquote",
        vec![func(
            "cast",
            vec![func("json_extract", vec![col(1), str_const("$.a")])],
        )],
    )];
    assert!(projection_benefits_from_pushed_down(&arrow, 3));
}

/// NEW COVERAGE: `IsColOpCol` needs both sides to be columns.
#[test]
fn is_col_op_col_needs_two_columns() {
    let Expression::ScalarFunction(both) = func("eq", vec![col(1), col(2)]) else {
        panic!("expected a scalar function")
    };
    assert!(is_col_op_col(&both).is_some());
    assert!(extract_columns_from_col_op_col(&both).is_some());

    let Expression::ScalarFunction(mixed) = func("eq", vec![col(1), int_const(2)]) else {
        panic!("expected a scalar function")
    };
    assert!(is_col_op_col(&mixed).is_none());
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        extract_columns_from_col_op_col(&mixed)
    }))
    .is_err());

    let Expression::ScalarFunction(one) = func("eq", vec![col(1)]) else {
        panic!("expected a scalar function")
    };
    assert!(extract_columns_from_col_op_col(&one).is_none());
}

/// NEW COVERAGE: `GetFuncArg` returns nil only for a non-function; Go's
/// direct argument indexing panics when a function index is out of range.
#[test]
fn get_func_arg_panics_on_an_out_of_range_function_index_like_go() {
    let row = func("row", vec![col(1), col(2)]);
    let Expression::Column(first) = get_func_arg(&row, 0).expect("function argument") else {
        panic!("expected a column")
    };
    assert_eq!(first.unique_id, 1);
    assert!(get_func_arg(&int_const(1), 0).is_none());
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| get_func_arg(&row, 2))).is_err()
    );
}

/// Go `compareFunctionClass.generateCmpSigs` declares both arguments in the
/// comparison domain. INT against VARCHAR is ETReal, so rebuilding an EQ must
/// produce the same pair of implicit DOUBLE casts as initial AST rewriting.
#[test]
fn real_function_builder_casts_comparison_arguments() {
    use super::builder::FunctionBuilder;
    let real = super::builder::RealFunctionBuilder::new(&NoColumns);
    let mut string_column = Column::new(2, string_type());
    string_column.index = 2;
    let built = real
        .new_function("eq", None, vec![col(1), Expression::Column(string_column)])
        .unwrap();
    let Expression::ScalarFunction(equality) = built else {
        panic!("EQ must remain a scalar function")
    };
    assert!(equality.args.iter().all(|argument| matches!(
        argument,
        Expression::ScalarFunction(cast) if cast.func_name.lowercase() == "cast_double"
    )));
}
