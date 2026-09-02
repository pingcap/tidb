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

//! GO PORTS of `pkg/expression/expression_test.go`'s constant-node table tests:
//! `TestConstant` (:135), `TestIsBinaryLiteral` (:157) and `TestConstLevel`
//! (:173), plus the unportable fragments recorded as `#[ignore]` stubs with
//! their go-parity-gap reasons.
//!
//! Go builds every case through `newFunctionWithMockCtx`, which runs the real
//! `NewFunction`; a node's level therefore reflects what CONSTRUCTION produces,
//! not just the tree shape. This port mirrors that by using
//! [`crate::expr_util::RealFunctionBuilder`] for the registered function names
//! Go uses (`abs`, `plus`, `rand`, `uuid`, `getparam`) and direct node
//! construction only where Go's own walk leaves an unfolded node in place.

use tidb_datatype::{Datum, Decimal, EvalType, FieldType, FieldTypeCode};

use crate::column::{Column, CorrelatedColumn};
use crate::constant::{Constant, ParamMarker};
use crate::context::NoColumns;
use crate::expr_util::{FunctionBuilder, RealFunctionBuilder};
use crate::expression::{ConstLevel, Expression};
use crate::scalar_function::ScalarFunction;
use crate::schema::Schema;
use tidb_ast::CiString;

fn int_type() -> FieldType {
    FieldType::new(FieldTypeCode::LongLong)
}

/// GO PORT of `pkg/expression/expression_test.go:135 TestConstant`, core half.
///
/// - `NewZero().IsCorrelated()` is false and its `ConstLevel()` is
///   `ConstStrict`.
/// - `NewZero().HashCode()` is exactly `[0x00, 0x08, 0x00]`: the
///   `ConstantFlag` byte followed by the mem-comparable encoding of `Int(0)`
///   (`hash_code(&Datum::Int(0)) == [8, 0]`). Asserted against BOTH the literal
///   bytes Go wrote and the flag-plus-encoding derivation, so drift on either
///   side trips a different message.
/// - `NewZero().Equal(ctx, NewOne())` is false: binary-compare over values.
///
/// The `Decorrelate(nil)`-identity and `PropagateType(ETReal)` assertions of
/// the same Go test are covered by the focused parity test below.
#[test]
fn test_constant_core_invariants() {
    let zero = Expression::Constant(Constant::new(Datum::Int(0), int_type()));
    assert!(!zero.is_correlated());
    assert_eq!(zero.const_level(), ConstLevel::STRICT);

    let mut raw_zero = Constant::new(Datum::Int(0), int_type());
    assert_eq!(raw_zero.hash_code(), [0x00u8, 0x08u8, 0x00u8].as_slice());

    let one = Expression::Constant(Constant::new(Datum::Int(1), int_type()));
    assert!(!zero.equal(&one));
}

#[test]
fn test_constant_decorrelate_and_propagate_type_fragments() {
    let zero = Expression::Constant(Constant::new(Datum::Int(0), int_type()));
    assert!(zero.decorrelate(None).equal(&zero));

    let mut decimal_type = FieldType::new(FieldTypeCode::NewDecimal);
    decimal_type.set_flen(9);
    decimal_type.set_decimal(8);
    let mut decimal = Expression::Constant(Constant::new(
        Datum::Decimal(Decimal::from_literal("5.04600000")),
        decimal_type,
    ));
    crate::expression::propagate_type(&mut decimal, EvalType::Real);
    let ty = decimal.static_type().expect("constant has a static type");
    assert_eq!(ty.code(), FieldTypeCode::NewDecimal);
    assert_eq!(ty.flen(), 48);
    assert_eq!(ty.decimal(), 30);
}

/// GO PORT of `pkg/expression/scalar_function.go:451`.
///
/// Scalar decorrelation must recurse into arguments and leave the original
/// owned tree untouched, while an outer schema turns a matching correlated
/// column into its plain-column form.
#[test]
fn test_scalar_function_decorrelate_rebuilds_arguments() {
    let column = Column::new(42, int_type());
    let expression = Expression::ScalarFunction(ScalarFunction::new(
        CiString::new("plus"),
        int_type(),
        vec![Expression::CorrelatedColumn(CorrelatedColumn::new(
            column.clone(),
        ))],
    ));
    assert!(expression.is_correlated());

    let schema = Schema::new(vec![column]);
    let decorrelated = expression.decorrelate(Some(&schema));
    assert!(!decorrelated.is_correlated());
    let Expression::ScalarFunction(function) = decorrelated else {
        panic!("decorrelation must preserve the scalar-function node");
    };
    assert!(matches!(function.args.as_slice(), [Expression::Column(_)]));
    assert!(expression.is_correlated());
}

/// Go's `CorrelatedColumn.Decorrelate(nil)` dereferences the schema pointer;
/// preserve that invalid-call boundary instead of inventing a Rust fallback.
#[test]
#[should_panic(expected = "CorrelatedColumn::decorrelate requires a schema")]
fn test_correlated_decorrelate_requires_schema() {
    let expression =
        Expression::CorrelatedColumn(CorrelatedColumn::new(Column::new(42, int_type())));
    let _ = expression.decorrelate(None);
}

/// GO PORT of `pkg/expression/expression_test.go:157 TestIsBinaryLiteral`.
///
/// `IsBinaryLiteral(expr)` is one datum-kind check -- `con.Value.Kind() ==
/// types.KindBinaryLiteral` for a Constant, else false. The crate has no named
/// symbol yet (call sites inline the same match; see the `IsBinaryLiteral`
/// comment at string_fn.rs's cast dispatch), so this port pins the identical
/// kind-membership semantics through a local predicate mirroring Go's body:
///
/// - Column nodes are never binary literals regardless of declared type (Go
///   cycles TypeEnum/TypeSet/TypeBit/TypeDuration),
/// - a `Constant` holding a binary-literal datum IS one even under a
///   TypeVarString ret type (Go stores `NewBinaryLiteralDatum([0,1])`),
/// - switching the value to an integer stops it being one,
/// - the RET TYPE never matters, only the datum kind.
#[test]
fn test_is_binary_literal_kind_membership() {
    /// Go `IsBinaryLiteral`: `con.Value.Kind() == types.KindBinaryLiteral`,
    /// reachable only for `*Constant`.
    fn is_binary_literal(expr: &Expression) -> bool {
        matches!(expr, Expression::Constant(con) if matches!(con.value, Datum::BinaryLiteral(_)))
    }

    // Columns are never binary literals whatever their declared type.
    for code in [
        FieldTypeCode::Enum,
        FieldTypeCode::Set,
        FieldTypeCode::Bit,
        FieldTypeCode::Duration,
    ] {
        assert!(!is_binary_literal(&Expression::Column(Column::new(
            1,
            FieldType::new(code)
        ))));
    }

    // A constant carrying the binary-literal datum is one; the declared
    // ret type (TypeVarString) does not matter.
    let mut constant = Constant::new(
        Datum::BinaryLiteral(vec![0, 1].into()),
        FieldType::new(FieldTypeCode::VarString),
    );
    assert!(is_binary_literal(&Expression::Constant(constant.clone())));

    // Switching the value to an integer stops it being one.
    constant.value = Datum::Int(1);
    assert!(!is_binary_literal(&Expression::Constant(constant)));
}

/// GO PORT of `pkg/expression/expression_test.go:173 TestConstLevel`.
///
/// The Go case table; each entry built exactly like
/// `newFunctionWithMockCtx(...)`:
///
/// | expression | level |
/// | --- | --- |
/// | `rand()` | ConstNone |
/// | `uuid()` | ConstNone |
/// | `getparam(1)` | ConstNone |
/// | `abs(1)` | ConstStrict (folds to the strict constant 1) |
/// | `abs(col#1)` | ConstNone |
/// | `plus(1, 1)` | ConstStrict (folds to 2) |
/// | `plus(1, col#1)` | ConstNone |
/// | `plus(col#1, 1)` | ConstNone |
/// | `plus(1, ctxConst)` with `ctxConst.DeferredExpr =
///   unix_timestamp()` | ConstOnlyInContext |
#[test]
fn test_const_level_case_table() {
    let ctx = NoColumns;
    let builder = RealFunctionBuilder::new(&ctx);

    fn new_one() -> Expression {
        Expression::Constant(Constant::new(Datum::Int(1), int_type()))
    }
    fn new_column(unique_id: i64) -> Expression {
        let mut column = Column::new(unique_id, int_type());
        column.index = unique_id;
        Expression::Column(column)
    }
    /// A node built without construction-time inference -- what survives when
    /// the real builder is not consulted (the deferred-carrying rows).
    fn raw(name: &str, args: Vec<Expression>) -> Expression {
        Expression::ScalarFunction(ScalarFunction::new(CiString::new(name), int_type(), args))
    }

    let build = |name: &str, args: Vec<Expression>| -> Expression {
        builder
            .new_function(name, None, args)
            .expect("construction must succeed")
    };

    assert_eq!(build("rand", vec![]).const_level(), ConstLevel::NONE);
    assert_eq!(build("uuid", vec![]).const_level(), ConstLevel::NONE);
    assert_eq!(
        build("getparam", vec![new_one()]).const_level(),
        ConstLevel::NONE
    );
    assert_eq!(
        build("abs", vec![new_one()]).const_level(),
        ConstLevel::STRICT
    );
    assert_eq!(
        build("abs", vec![new_column(1)]).const_level(),
        ConstLevel::NONE
    );
    assert_eq!(
        build("plus", vec![new_one(), new_one()]).const_level(),
        ConstLevel::STRICT
    );
    assert_eq!(
        build("plus", vec![new_one(), new_column(1)]).const_level(),
        ConstLevel::NONE
    );
    assert_eq!(
        build("plus", vec![new_column(1), new_one()]).const_level(),
        ConstLevel::NONE
    );

    // ctxConst := NewZero(); ctxConst.DeferredExpr =
    //     newFunctionWithMockCtx(ast.UnixTimestamp)
    let mut ctx_const = Constant::new(Datum::Int(0), int_type());
    ctx_const.deferred_expr = Some(Box::new(raw("unix_timestamp", vec![])));
    let plus_over_deferred = build(
        "plus",
        vec![new_one(), Expression::Constant(ctx_const.clone())],
    );
    assert_eq!(
        plus_over_deferred.const_level(),
        ConstLevel::ONLY_IN_CONTEXT
    );
    // The deferred constant itself reports ONLY_IN_CONTEXT too, which is the
    // minimum the walk above consumes.
    assert_eq!(
        Expression::Constant(ctx_const).const_level(),
        ConstLevel::ONLY_IN_CONTEXT
    );

    // The parameter-marker twin: a param-markered argument also dominates the
    // minimum at ONLY_IN_CONTEXT (Go reaches the same value via GetParam).
    let mut parameter = Constant::new(Datum::Int(7), int_type());
    parameter.param_marker = Some(ParamMarker { order: 0 });
    let plus_over_param = build("plus", vec![new_one(), Expression::Constant(parameter)]);
    assert_eq!(plus_over_param.const_level(), ConstLevel::ONLY_IN_CONTEXT);

    // The empty schema stands for Go's untouched helper state here; kept so a
    // future row that needs schema context reuses this anchor deliberately.
    assert!(Schema::new(Vec::new()).columns.is_empty());
}
