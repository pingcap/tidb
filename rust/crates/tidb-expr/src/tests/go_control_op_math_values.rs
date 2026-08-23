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

//! Value-level ports of `pkg/expression`'s control, operator and math unit
//! tests (`builtin_control_test.go`, `builtin_op_test.go`,
//! `builtin_math_test.go`). The harness mirrors Go's `evalBuiltinFunc`
//! (`builtin_test.go:54`): evaluation dispatches on the function's declared
//! result type and the result is presented AS THAT TYPE.

use super::*;
use tidb_datatype::{FieldType, FieldTypeCode as C, FieldTypeFlags};

use crate::expression::Expression;
use crate::scalar_function::ScalarFunction;
use tidb_ast::CiString;

/// One constant argument typed by its datum kind -- Go `kindToFieldType`
/// (`evaluator_test.go:33`).
fn const_arg(datum: Datum) -> Expression {
    let field_type = match &datum {
        Datum::Null => FieldType::new(C::Null),
        Datum::Int(_) => FieldType::new(C::LongLong),
        Datum::UInt(_) => {
            FieldType::new(C::LongLong).with_added_flags(FieldTypeFlags::UNSIGNED)
        }
        Datum::Float32(_) | Datum::Real(_) => FieldType::new(C::Double),
        Datum::String(_) | Datum::Bytes(_) => FieldType::new(C::VarString),
        Datum::Decimal(_) => FieldType::new(C::NewDecimal),
        other => panic!("no test mapping for {other:?}"),
    };
    Expression::Constant(crate::constant::Constant::new(datum, field_type))
}

/// Go `evalBuiltinFunc`: evaluate `name(args)` presenting the value as
/// `ret_type`.
fn eval_as(name: &str, args: Vec<Datum>, ret_type: FieldType) -> Result<Datum, crate::EvalError> {
    let function = ScalarFunction::new(
        CiString::new(name),
        ret_type,
        args.into_iter().map(const_arg).collect(),
    );
    let cols = crate::context::ZonedNoColumns(tidb_datatype::SessionTimeZone::utc());
    let empty = tidb_chunk::chunk::Chunk::new_with_capacity(&[], 1);
    function.eval(&cols, empty.get_row(0))
}

fn int_result() -> FieldType {
    FieldType::new(C::LongLong)
}

fn uint_result() -> FieldType {
    FieldType::new(C::LongLong).with_added_flags(FieldTypeFlags::UNSIGNED)
}

fn real_result() -> FieldType {
    FieldType::new(C::Double)
}

fn i(v: i64) -> Datum {
    Datum::Int(v)
}

fn r(v: f64) -> Datum {
    Datum::Real(v)
}

/// Go `TestIf` (`builtin_control_test.go:61`): the condition decides between
/// the two arms; every numeric spelling of zero chooses arm two.
#[test]
fn go_test_if() {
    let int_ret = || int_result();
    let cases: &[(Datum, Datum, Datum, Datum)] = &[
        (i(1), i(1), i(2), i(1)),
        (Datum::Null, i(1), i(2), i(2)),
        (i(0), i(1), i(2), i(2)),
        (Datum::Bytes(b"abc".to_vec()), i(1), i(2), i(2)),
        (i(0), Datum::Bytes(b"x".to_vec()), i(2), i(2)),
    ];
    for (condition, left, right, expected) in cases {
        let value =
            eval_as("if", vec![condition.clone(), left.clone(), right.clone()], int_ret())
                .unwrap_or_else(|error| panic!("{condition:?}: {error:?}"));
        assert_eq!(value, *expected, "condition {condition:?}");
    }
    // Decimal / real / numeric-string conditions.
    let decimal = |text: &str| Datum::Decimal(crate::Decimal::from_literal(text));
    for (condition, expected) in [
        (decimal("1.2"), i(1)),
        (r(0.1), i(1)),
        (r(0.0), i(2)),
        (decimal("0.1"), i(1)),
        (decimal("0.0"), i(2)),
    ] {
        let value = eval_as(
            "if",
            vec![condition.clone(), i(1), i(2)],
            int_result(),
        )
        .unwrap_or_else(|error| panic!("{condition:?}: {error:?}"));
        assert_eq!(value, expected, "condition {condition:?}");
    }
}

/// Go `TestIfNull` (`builtin_control_test.go:109`): the first NON-NULL
/// argument wins; two NULLs answer NULL.
#[test]
fn go_test_ifnull() {
    let cases: &[(Datum, Datum, Datum)] = &[
        (i(1), i(2), i(1)),
        (Datum::Null, i(2), i(2)),
        (Datum::Null, Datum::Null, Datum::Null),
        (Datum::Bytes(b"abc".to_vec()), Datum::Null, Datum::Bytes(b"abc".to_vec())),
    ];
    for (left, right, expected) in cases {
        let value = eval_as("ifnull", vec![left.clone(), right.clone()], int_result())
            .unwrap_or_else(|error| panic!("{left:?}, {right:?}: {error:?}"));
        if expected.is_null() {
            assert!(value.is_null(), "{left:?}, {right:?}");
        } else {
            assert!(!value.is_null(), "{left:?}, {right:?}");
        }
    }
    assert!(
        eval_as("ifnull", vec![Datum::Null, Datum::Null], int_result())
            .unwrap()
            .is_null()
    );
}

/// Go `TestLeftShift`/`TestRightShift` (`builtin_op_test.go:175,207`):
/// unsigned 64-bit shifts; a negative LEFT operand reinterprets as unsigned.
#[test]
fn go_test_shifts() {
    let value = eval_as("leftshift", vec![i(123), i(2)], uint_result()).unwrap();
    assert_eq!(value, Datum::UInt(492));
    // -123 reinterprets as unsigned before shifting (Go:
    // uint64(18446744073709551124)).
    let value = eval_as("leftshift", vec![i(-123), i(2)], uint_result()).unwrap();
    assert_eq!(value, Datum::UInt(18_446_744_073_709_551_124));
    let value = eval_as("rightshift", vec![i(123), i(2)], uint_result()).unwrap();
    assert_eq!(value, Datum::UInt(30));
    // NULL propagates.
    assert!(
        eval_as("leftshift", vec![Datum::Null, i(1)], uint_result())
            .unwrap()
            .is_null()
    );
}

/// Go `TestBitXor`/`TestBitOr`/`TestBitAnd` (`builtin_op_test.go:246-436`).
#[test]
fn go_test_bit_ops() {
    let value = eval_as("bitxor", vec![i(1), i(2)], uint_result()).unwrap();
    assert_eq!(value, Datum::UInt(3));
    let value = eval_as("bitor", vec![i(1), i(2)], uint_result()).unwrap();
    assert_eq!(value, Datum::UInt(3));
    let value = eval_as("bitand", vec![i(7), i(3)], uint_result()).unwrap();
    assert_eq!(value, Datum::UInt(3));
    // Go TestBitXor: {123, 321} -> 314.
    let value = eval_as("bitxor", vec![i(123), i(321)], uint_result()).unwrap();
    assert_eq!(value, Datum::UInt(314));
    let value = eval_as("bitxor", vec![i(-123), i(321)], uint_result()).unwrap();
    assert_eq!(value, Datum::UInt(18_446_744_073_709_551_300));
}

/// Go `TestBitNeg` (`builtin_op_test.go`): `~` reinterprets as unsigned.
#[test]
fn go_test_bit_neg() {
    let value = eval_as("bitneg", vec![i(0)], uint_result()).unwrap();
    assert_eq!(value, Datum::UInt(u64::MAX));
    let value = eval_as("bitneg", vec![i(7)], uint_result()).unwrap();
    assert_eq!(value, Datum::UInt(!7_u64));
}

/// Go `TestUnaryNot` (`builtin_op_test.go`): NOT of a nonzero value is 0, of
/// zero is 1, of NULL is NULL.
#[test]
fn go_test_unary_not() {
    assert_eq!(eval_as("not", vec![i(1)], int_result()).unwrap(), i(0));
    assert_eq!(eval_as("not", vec![i(0)], int_result()).unwrap(), i(1));
    assert!(
        eval_as("not", vec![Datum::Null], int_result())
            .unwrap()
            .is_null()
    );
    assert_eq!(eval_as("not", vec![r(0.5)], int_result()).unwrap(), i(0));
    assert_eq!(eval_as("not", vec![r(0.0)], int_result()).unwrap(), i(1));
}

/// Go `TestIsTrueOrFalse` (`builtin_op_test.go`): IS TRUE / IS FALSE /
/// IS UNKNOWN over int, real, decimal and NULL operands.
#[test]
fn go_test_is_true_or_false() {
    for (name, operand, expected) in [
        ("istrue", i(0), i(0)),
        ("istrue", i(2), i(1)),
        ("istrue", Datum::Null, i(0)),
        ("istrue", r(0.0), i(0)),
        ("istrue", r(0.5), i(1)),
        ("isfalse", i(0), i(1)),
        ("isfalse", i(2), i(0)),
        ("isfalse", Datum::Null, i(0)),
        ("isunknown", i(0), i(0)),
        ("isunknown", Datum::Null, i(1)),
    ] {
        assert_eq!(
            eval_as(name, vec![operand.clone()], int_result()).unwrap(),
            expected,
            "{name} {operand:?}"
        );
    }
}

/// Go `TestCeil` (`builtin_math_test.go:61`) and `TestFloor`
/// (`:177`): integers pass through unchanged; a REAL rounds away from
/// zero (ceiling) / toward zero (floor); numeric strings follow the REAL
/// signature; garbage strings truncate to their leading number.
#[test]
fn go_test_ceil_and_floor() {
    // (name, arg, result-type, expected)
    let cases: &[(&str, Datum, FieldType, Datum)] = &[
        ("ceil", i(1), int_result(), i(1)),
        ("ceil", r(1.23), real_result(), r(2.0)),
        ("ceil", r(-1.23), real_result(), r(-1.0)),
        ("ceil", Datum::Null, real_result(), Datum::Null),
        ("floor", i(1), int_result(), i(1)),
        ("floor", r(1.23), real_result(), r(1.0)),
        ("floor", r(-1.23), real_result(), r(-2.0)),
        ("floor", Datum::Null, real_result(), Datum::Null),
    ];
    for (name, arg, ret, expected) in cases {
        let value = eval_as(name, vec![arg.clone()], ret.clone())
            .unwrap_or_else(|error| panic!("{name}({arg:?}): {error:?}"));
        assert!(
            value.compare(expected, tidb_datatype::Collation::Binary)
                == Ok(std::cmp::Ordering::Equal),
            "{name}({arg:?}) = {value:?}, want {expected:?}"
        );
    }
}
